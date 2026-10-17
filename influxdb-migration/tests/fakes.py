"""A fake InfluxDB (1.x and 2.x) and a fake Tag Historian, as real HTTP servers.

Real servers on 127.0.0.1 rather than mocked ``requests`` calls, so the code
under test goes through everything it goes through in use: URL building,
query strings, form and JSON bodies, gzip, status codes, Retry-After, a
connection that closes without an answer.

THE FAKE INFLUXDB IS STRICT ABOUT WHAT IT IS ASKED. It answers exactly the
InfluxQL and Flux shapes the reader sends, implemented with InfluxDB's own
semantics (``GROUP BY *`` reporting every tag key of a measurement with ""
for the ones a series lacks, Flux returning one table per series and field,
annotated CSV with a header per schema), and it answers anything else with a
400. A query that changes shape therefore fails a test here instead of
silently asking a real InfluxDB something it answers differently.

THE FAKE TAG HISTORIAN implements the contract of ``POST /api/v2/write`` that
the copy depends on - Home Assistant naming, friendly name and unit, the daily
allowance, the tag limit, 422/429/503/402 and their bodies - from the
product's controller and middleware, written independently of this package's
own mirror of those rules, so the two can disagree and a test can notice.
"""

from __future__ import annotations

import base64
import gzip
import json
import re
import threading
from collections.abc import Callable
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, unquote, urlparse

from taghistorian_influx_migrate.timeutil import format_time_ns, parse_time_ns, utc_day


@dataclass
class Point:
    measurement: str
    tags: dict[str, str]
    fields: dict[str, object]
    time: int


class _Server:
    """A ThreadingHTTPServer on a free port, with a handler bound to an owner."""

    def __init__(self, owner, handler_cls):
        handler = type("Handler", (handler_cls,), {"owner": owner})
        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), handler)
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)
        self.thread.start()

    @property
    def url(self) -> str:
        host, port = self.httpd.server_address[:2]
        return f"http://{host}:{port}"

    def close(self) -> None:
        self.httpd.shutdown()
        self.httpd.server_close()


class _Handler(BaseHTTPRequestHandler):
    owner = None

    def log_message(self, *args):  # silence the default stderr log
        pass

    def _body(self) -> bytes:
        length = int(self.headers.get("Content-Length") or 0)
        data = self.rfile.read(length) if length else b""
        if self.headers.get("Content-Encoding") == "gzip":
            data = gzip.decompress(data)
        return data

    def _send(
        self, status: int, body: object = None, *, content_type="application/json", headers=None
    ):
        payload = b""
        if body is not None:
            payload = body.encode("utf-8") if isinstance(body, str) else json.dumps(body).encode()
        self.send_response(status)
        for key, value in (headers or {}).items():
            self.send_header(key, value)
        if payload:
            self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        if payload:
            self.wfile.write(payload)


# ---------------------------------------------------------------------------
# InfluxDB
# ---------------------------------------------------------------------------


class FakeInflux:
    """The data, and both query languages over it."""

    def __init__(
        self,
        points: list[Point],
        *,
        username: str | None = None,
        password: str | None = None,
        token: str | None = None,
        max_row_limit: int = 0,
    ):
        self.points = points
        self.username = username
        self.password = password
        self.token = token
        self.max_row_limit = max_row_limit
        self.queries: list[str] = []
        # Rows each InfluxQL read of a series' points answered with.
        self.rows_read: list[int] = []
        self._server = _Server(self, _InfluxHandler)

    @property
    def url(self) -> str:
        return self._server.url

    def close(self) -> None:
        self._server.close()

    # -- shared semantics ----------------------------------------------------

    def tag_keys(self, measurement: str) -> list[str]:
        return sorted({k for p in self.points if p.measurement == measurement for k in p.tags})

    def select(
        self,
        measurements: list[str] | None,
        start: int,
        stop: int,
        where_tags: dict[str, str],
    ) -> list[Point]:
        return [
            p
            for p in self.points
            if (measurements is None or p.measurement in measurements)
            and start <= p.time < stop
            and all(p.tags.get(k) == v for k, v in where_tags.items())
        ]


class _InfluxHandler(_Handler):
    # -- 1.x -----------------------------------------------------------------

    def do_POST(self):
        owner: FakeInflux = self.owner
        path = urlparse(self.path)
        if path.path == "/query":
            return self._influxql(owner, parse_qs(path.query), parse_qs(self._body().decode()))
        if path.path == "/api/v2/query":
            return self._flux(owner, parse_qs(path.query), json.loads(self._body() or b"{}"))
        return self._send(404, {"error": "not found"})

    def _authorised_v1(self, owner: FakeInflux) -> bool:
        if owner.username is None:
            return True
        header = self.headers.get("Authorization", "")
        if not header.startswith("Basic "):
            return False
        user, _, password = base64.b64decode(header[6:]).decode().partition(":")
        return user == owner.username and password == owner.password

    _FROM = r"(?P<from>.+?)"
    _COUNT = re.compile(
        r'^SELECT COUNT\("value"\) FROM '
        + _FROM
        + r" WHERE time >= (?P<s>\d+) AND time < (?P<e>\d+) GROUP BY \*$"
    )
    _LAST = re.compile(
        r'^SELECT LAST\("friendly_name_str"\) AS "friendly_name", LAST\("unit_of_measurement_str"\) AS "unit" FROM '
        + _FROM
        + r" WHERE time >= (?P<s>\d+) AND time < (?P<e>\d+) GROUP BY \*$"
    )
    _SERIES = (
        r" FROM "
        + _FROM
        + r""" WHERE "domain" = '(?P<domain>(?:[^'\\]|\\.)*)' AND "entity_id" = '(?P<entity>(?:[^'\\]|\\.)*)' AND time >= (?P<s>\d+) AND time < (?P<e>\d+) GROUP BY \*$"""
    )
    _FIRST = re.compile(r'^SELECT FIRST\("value"\)' + _SERIES)
    _COUNT_SERIES = re.compile(r'^SELECT COUNT\("value"\)' + _SERIES)
    _RAW = re.compile(r'^SELECT "value", "friendly_name_str", "unit_of_measurement_str"' + _SERIES)

    @staticmethod
    def _measurements(owner: FakeInflux, text: str) -> list[str] | None:
        # Strip a retention policy prefix: "rp"./.*/ or "rp"."m"
        names = []
        for part in re.findall(r'(?:"(?:[^"\\]|\\.)*"\.)?(/\.\*/|"(?:[^"\\]|\\.)*")', text):
            if part == "/.*/":
                return None
            names.append(part[1:-1].replace('\\"', '"').replace("\\\\", "\\"))
        return names

    @staticmethod
    def _unescape(text: str) -> str:
        return re.sub(r"\\(.)", r"\1", text)

    def _influxql(self, owner: FakeInflux, query: dict, form: dict):
        if not self._authorised_v1(owner):
            return self._send(401, {"error": "authorization failed"})
        if query.get("epoch") != ["ns"]:
            return self._send(400, {"error": "the reader always asks for epoch=ns"})
        statement = form.get("q", [""])[0]
        owner.queries.append(statement)

        found = [
            (kind, pattern.match(statement))
            for pattern, kind in (
                (self._COUNT, "count"),
                (self._COUNT_SERIES, "count_series"),
                (self._LAST, "last"),
                (self._FIRST, "first"),
                (self._RAW, "raw"),
            )
        ]
        found = [(kind, match) for kind, match in found if match]
        if not found:
            return self._send(
                400, {"error": f"fake InfluxDB does not know this statement: {statement}"}
            )
        kind, match = found[0]

        measurements = self._measurements(owner, match.group("from"))
        where = {}
        if kind in ("first", "raw", "count_series"):
            where = {
                "domain": self._unescape(match.group("domain")),
                "entity_id": self._unescape(match.group("entity")),
            }
        points = owner.select(measurements, int(match.group("s")), int(match.group("e")), where)

        groups: dict[tuple, list[Point]] = {}
        for p in points:
            keys = owner.tag_keys(p.measurement)
            group_tags = tuple((k, p.tags.get(k, "")) for k in keys)
            groups.setdefault((p.measurement, group_tags), []).append(p)

        series = []
        partial = False
        for (measurement, group_tags), members in sorted(groups.items()):
            members.sort(key=lambda p: p.time)
            tags = dict(group_tags)
            if kind in ("count", "count_series"):
                n = sum(1 for p in members if "value" in p.fields)
                if n:
                    series.append(
                        {
                            "name": measurement,
                            "tags": tags,
                            "columns": ["time", "count"],
                            "values": [[int(match.group("s")), n]],
                        }
                    )
            elif kind == "last":
                names = [
                    p.fields["friendly_name_str"]
                    for p in members
                    if "friendly_name_str" in p.fields
                ]
                units = [
                    p.fields["unit_of_measurement_str"]
                    for p in members
                    if "unit_of_measurement_str" in p.fields
                ]
                if names or units:
                    series.append(
                        {
                            "name": measurement,
                            "tags": tags,
                            "columns": ["time", "friendly_name", "unit"],
                            "values": [
                                [0, names[-1] if names else None, units[-1] if units else None]
                            ],
                        }
                    )
            elif kind == "first":
                with_value = [p for p in members if "value" in p.fields]
                if with_value:
                    series.append(
                        {
                            "name": measurement,
                            "tags": tags,
                            "columns": ["time", "first"],
                            "values": [[with_value[0].time, with_value[0].fields["value"]]],
                        }
                    )
            else:
                columns = ["time", "value", "friendly_name_str", "unit_of_measurement_str"]
                rows = [
                    [p.time, *[p.fields.get(c) for c in columns[1:]]]
                    for p in members
                    if any(c in p.fields for c in columns[1:])
                ]
                entry = {"name": measurement, "tags": tags, "columns": columns, "values": rows}
                if owner.max_row_limit and len(rows) > owner.max_row_limit:
                    entry["values"] = rows[: owner.max_row_limit]
                    entry["partial"] = True
                    partial = True
                if rows:
                    series.append(entry)

        if kind == "raw":
            owner.rows_read.append(sum(len(s["values"]) for s in series))
        result: dict = {"statement_id": 0}
        if series:
            result["series"] = series
        if partial:
            result["partial"] = True
        return self._send(200, {"results": [result]})

    # -- 2.x -----------------------------------------------------------------

    _STR = r'"((?:[^"\\]|\\.)*)"'
    _RANGE = re.compile(
        r"^from\(bucket: " + _STR + r"\)\n  \|> range\(start: (\S+), stop: (\S+)\)\n"
    )
    _MEAS_FILTER = re.compile(
        r"^  \|> filter\(fn: \(r\) => ((?:r\._measurement == " + _STR + r"(?: or )?)+)\)\n"
    )
    _SERIES_FILTER = re.compile(
        r"^  \|> filter\(fn: \(r\) => r\._measurement == "
        + _STR
        + r" and r\.domain == "
        + _STR
        + r" and r\.entity_id == "
        + _STR
        + r"\)\n"
    )
    _FIELD_FILTER = re.compile(
        r"^  \|> filter\(fn: \(r\) => ((?:r\._field == " + _STR + r"(?: or )?)+)\)\n"
    )
    _TAIL = re.compile(r"^(?:  \|> (count|last|first)\(\)\n)?$")

    @staticmethod
    def _flux_unescape(text: str) -> str:
        return re.sub(
            r"\\(.)", lambda m: {"n": "\n", "r": "\r", "t": "\t"}.get(m.group(1), m.group(1)), text
        )

    def _flux(self, owner: FakeInflux, query: dict, body: dict):
        if owner.token is not None and self.headers.get("Authorization") != f"Token {owner.token}":
            return self._send(401, {"code": "unauthorized", "message": "unauthorized access"})
        if self.headers.get("Accept") != "application/csv":
            return self._send(400, {"code": "invalid", "message": "the reader always asks for CSV"})
        dialect = body.get("dialect") or {}
        if (
            dialect.get("annotations") != ["datatype", "group", "default"]
            or body.get("type") != "flux"
        ):
            return self._send(400, {"code": "invalid", "message": f"unexpected dialect {dialect}"})
        flux = body.get("query", "")
        owner.queries.append(flux)

        def fail():
            return self._send(
                400,
                {"code": "invalid", "message": f"fake InfluxDB does not know this query: {flux!r}"},
            )

        rest = flux
        match = self._RANGE.match(rest)
        if not match:
            return fail()
        start, stop = parse_time_ns(match.group(2)), parse_time_ns(match.group(3))
        rest = rest[match.end() :]

        measurements = None
        where: dict[str, str] = {}
        series_match = self._SERIES_FILTER.match(rest)
        if series_match:
            measurements = [self._flux_unescape(series_match.group(1))]
            where = {
                "domain": self._flux_unescape(series_match.group(2)),
                "entity_id": self._flux_unescape(series_match.group(3)),
            }
            rest = rest[series_match.end() :]
        else:
            meas_match = self._MEAS_FILTER.match(rest)
            if meas_match and "r._field" not in meas_match.group(0):
                measurements = [
                    self._flux_unescape(m) for m in re.findall(self._STR, meas_match.group(1))
                ]
                rest = rest[meas_match.end() :]

        field_match = self._FIELD_FILTER.match(rest)
        if not field_match:
            return fail()
        fields = [self._flux_unescape(f) for f in re.findall(self._STR, field_match.group(1))]
        rest = rest[field_match.end() :]
        tail = self._TAIL.match(rest)
        if not tail:
            return fail()
        function = tail.group(1)

        points = owner.select(measurements, start, stop, where)
        tables: dict[tuple, list[tuple[int, object]]] = {}
        for p in sorted(points, key=lambda p: p.time):
            for name in fields:
                if name in p.fields:
                    key = (p.measurement, tuple(sorted(p.tags.items())), name)
                    tables.setdefault(key, []).append((p.time, p.fields[name]))

        blocks = []
        for index, ((measurement, tags, field_name), rows) in enumerate(sorted(tables.items())):
            if function == "count":
                rows = [(None, len(rows))]
            elif function == "last":
                rows = rows[-1:]
            elif function == "first":
                rows = rows[:1]
            blocks.append((index, measurement, dict(tags), field_name, function, rows, start, stop))
        return self._send(200, _annotated_csv(blocks), content_type="text/csv; charset=utf-8")


def _datatype(value: object) -> str:
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "long"
    if isinstance(value, float):
        return "double"
    return "string"


def _annotated_csv(blocks) -> str:
    """Flux's annotated CSV: a header per schema, consecutive same-schema tables sharing one."""
    out: list[str] = []
    previous_schema = None
    for index, measurement, tags, field_name, function, rows, start, stop in blocks:
        tag_names = sorted(tags)
        value_type = _datatype(rows[0][1])
        aggregate = function == "count"
        columns = ["result", "table", "_start", "_stop"]
        types = ["string", "long", "dateTime:RFC3339", "dateTime:RFC3339"]
        groups = ["false", "false", "true", "true"]
        if not aggregate:
            columns.append("_time")
            types.append("dateTime:RFC3339Nano")
            groups.append("false")
        columns += ["_value", "_field", "_measurement", *tag_names]
        types += [value_type, "string", "string", *["string"] * len(tag_names)]
        groups += ["false", "true", "true", *["true"] * len(tag_names)]
        schema = (tuple(columns), tuple(types))
        if schema != previous_schema:
            if out:
                out.append("")
            out.append("#datatype," + ",".join(types))
            out.append("#group," + ",".join(groups))
            out.append("#default,_result," + "," * (len(columns) - 2))
            out.append("," + ",".join(columns))
            previous_schema = schema
        for time, value in rows:
            cells = ["", str(index), format_time_ns(start), format_time_ns(stop)]
            if not aggregate:
                cells.append(format_time_ns(time))
            rendered = ("true" if value else "false") if isinstance(value, bool) else str(value)
            if isinstance(value, str) and ("," in value or '"' in value):
                rendered = '"' + value.replace('"', '""') + '"'
            cells += [rendered, field_name, measurement, *[tags[t] for t in tag_names]]
            out.append("," + ",".join(cells))
    return "\r\n".join(out) + "\r\n\r\n"


# ---------------------------------------------------------------------------
# Tag Historian
# ---------------------------------------------------------------------------


def _sanitize(raw: str) -> str:
    # Deliberately written apart from naming.sanitize_tag_name: short names only.
    out = re.sub(r"[^A-Za-z0-9_.\-]", "_", raw)
    return out if re.match(r"[A-Za-z]", out) else "t" + out


def _unescape_lp(text: str) -> str:
    return re.sub(r"\\(.)", r"\1", text)


def _split_unescaped(text: str, separator: str) -> list[str]:
    parts, current, escaped, quoted = [], "", False, False
    for c in text:
        if escaped:
            current += "\\" + c
            escaped = False
            continue
        if c == "\\":
            escaped = True
            continue
        if c == '"':
            quoted = not quoted
        if c == separator and not quoted:
            parts.append(current)
            current = ""
            continue
        current += c
    parts.append(current)
    return parts


def parse_line(line: str) -> tuple[str, dict[str, str], dict[str, object], int]:
    """Enough of a line protocol parser for the lines the copy sends."""
    head, fields_text, timestamp = _split_unescaped(line, " ")
    measurement, *tag_parts = _split_unescaped(head, ",")
    tags = {}
    for part in tag_parts:
        key, value = _split_unescaped(part, "=")
        tags[_unescape_lp(key)] = _unescape_lp(value)
    fields: dict[str, object] = {}
    for part in _split_unescaped(fields_text, ","):
        key, raw = part.split("=", 1) if "\\=" not in part else _split_unescaped(part, "=")
        if raw.startswith('"'):
            value: object = raw[1:-1].replace('\\"', '"').replace("\\\\", "\\")
        elif raw in ("true", "false"):
            value = raw == "true"
        elif raw.endswith("i"):
            value = int(raw[:-1])
        else:
            value = float(raw)
        fields[_unescape_lp(key)] = value
    return _unescape_lp(measurement), tags, fields, int(timestamp)


@dataclass
class StoredTag:
    readings: list[tuple[int, object]] = field(default_factory=list)
    description: str | None = None
    units: str | None = None
    # (timestamp, when it arrived) for every reading the write endpoint took:
    # what the write buffer holds until it is flushed.
    arrivals: list[tuple[int, int]] = field(default_factory=list)


class FakeTagHistorian:
    """Enough of Tag Historian's API for the copy, with scriptable trouble.

    ``full_resolution_days`` and ``flush_after_s`` model the server's TIERED
    READ (TieredMeasurementQueryService and MemoryBufferService.GetRange):
    ``GET /api/measurements`` sees raw readings only inside the plan's
    full-resolution period; older ones only while they are still in the write
    buffer, and after the flush only as hourly summaries, which the nightly
    roll-up builds and this fake never does. Left at None, every stored
    reading reads back - a server that had never flushed.
    """

    API_KEY = "th-test-key"
    CUSTOMER_ID = "11111111-2222-3333-4444-555555555555"

    def __init__(
        self,
        *,
        clock: Callable[[], int],
        tag_limit: int = 50,
        readings_per_day: int = 1_000_000,
        full_resolution_days: int | None = None,
        flush_after_s: float = 300.0,
    ):
        self.clock = clock
        self.tag_limit = tag_limit
        self.readings_per_day = readings_per_day
        self.full_resolution_days = full_resolution_days
        self.flush_after_ns = int(flush_after_s * 1_000_000_000)
        self.customer_id = self.CUSTOMER_ID
        self.tags: dict[str, StoredTag] = {}
        self.usage: dict[str, int] = {}
        self.write_requests: list[list[str]] = []
        # Each entry decides what the next write request gets instead of normal
        # handling: ("status", code, retry_after, body), "drop-after-store" or
        # "drop-before-store".
        self.script: list[object] = []
        self.reads: list[str] = []
        self._lock = threading.Lock()
        self._server = _Server(self, _TagHistorianHandler)

    @property
    def url(self) -> str:
        return self._server.url

    def close(self) -> None:
        self._server.close()

    def today(self) -> str:
        return utc_day(self.clock())

    def add_live(self, tag: str, readings: list[tuple[int, float]], description=None, units=None):
        stored = self.tags.setdefault(tag, StoredTag())
        stored.readings.extend(readings)
        stored.readings.sort(key=lambda r: r[0])
        stored.description = description or stored.description
        stored.units = units or stored.units

    def values(self, tag: str) -> list[tuple[int, object]]:
        return sorted(self.tags[tag].readings, key=lambda r: r[0])

    def ingest(self, lines: list[str]):
        """The controller and the ingestion service, for the lines the copy sends."""
        rows = []
        metadata: dict[str, tuple[str | None, str | None]] = {}
        for line in lines:
            measurement, tags, fields, timestamp = parse_line(line)
            if "domain" in tags and "entity_id" in tags:
                extras = [
                    f"{k}-{v}" for k, v in sorted(tags.items()) if k not in ("domain", "entity_id")
                ]
                base = ".".join([f"{tags['domain']}.{tags['entity_id']}", *extras])
            else:
                base = measurement
            value = fields.get("value")
            if isinstance(value, (int, float)):
                name = _sanitize(base)
                rows.append((name, timestamp, value))
                description = fields.get("friendly_name_str")
                units = fields.get("unit_of_measurement_str")
                metadata[name] = (description, units)  # type: ignore[assignment]

        today = self.today()
        if self.usage.get(today, 0) >= self.readings_per_day:
            return (
                429,
                {"Retry-After": "3600"},
                {"code": "too many requests", "message": "daily measurement quota exceeded"},
            )

        skipped = []
        stored = 0
        for name in dict.fromkeys(r[0] for r in rows):
            if name not in self.tags and len(self.tags) >= self.tag_limit:
                skipped.append(name)
                continue
            tag = self.tags.setdefault(name, StoredTag())
            for row in rows:
                if row[0] == name:
                    tag.readings.append((row[1], row[2]))
                    tag.arrivals.append((row[1], self.clock()))
                    stored += 1
            description, units = metadata[name]
            tag.description = description or tag.description
            tag.units = units or tag.units
        self.usage[today] = self.usage.get(today, 0) + stored
        if skipped:
            dropped = sum(1 for r in rows if r[0] in skipped)
            return (
                422,
                {},
                {
                    "code": "unprocessable entity",
                    "message": f"{dropped} points dropped: tag quota exceeded (new tags: {', '.join(skipped)}); points for existing tags were written",
                },
            )
        return 204, {}, None


class _TagHistorianHandler(_Handler):
    def _authorised(self) -> bool:
        return self.headers.get("Authorization") == f"Token {FakeTagHistorian.API_KEY}"

    def do_GET(self):
        owner: FakeTagHistorian = self.owner
        if not self._authorised():
            return self._send(401)
        url = urlparse(self.path)
        query = {k: v[0] for k, v in parse_qs(url.query).items()}
        owner.reads.append(url.path)
        with owner._lock:
            if url.path == "/api/customers/me":
                return self._send(200, {"customerId": owner.customer_id})
            if url.path == "/api/usage/limits":
                return self._send(
                    200,
                    {
                        "tagLimit": owner.tag_limit,
                        "currentTagCount": len(owner.tags),
                        "measurementsPerDayLimit": owner.readings_per_day,
                        "tier": "Professional",
                    },
                )
            if url.path == "/api/usage/daily":
                days = int(query.get("days", 30))
                dates = sorted(set(owner.usage) | {owner.today()})[-days:]
                return self._send(
                    200,
                    {
                        "dailyUsage": [
                            {"date": d, "measurementCount": owner.usage.get(d, 0)} for d in dates
                        ]
                    },
                )
            if url.path == "/api/tags":
                names = sorted(owner.tags)
                if query.get("search"):
                    # HotTagRepository.Search: a case-insensitive substring.
                    names = [n for n in names if query["search"].lower() in n.lower()]
                skip, take = int(query.get("skip", 0)), int(query.get("take", 100))
                page = names[skip : skip + take]
                tags = []
                for name in page:
                    readings = owner.tags[name].readings
                    times = [r[0] for r in readings]
                    tags.append(
                        {
                            "tagName": name,
                            "description": owner.tags[name].description,
                            "units": owner.tags[name].units,
                            "totalMeasurements": len(readings),
                            # .NET DateTime: 100 ns ticks, seven fraction digits.
                            "firstTimestamp": _ticks(min(times))
                            if times
                            else "0001-01-01T00:00:00",
                            "lastTimestamp": _ticks(max(times)) if times else "0001-01-01T00:00:00",
                        }
                    )
                return self._send(
                    200, {"totalCount": len(names), "skip": skip, "take": take, "tags": tags}
                )
            match = re.match(r"^/api/measurements/([^/]+)/([^/]+)$", url.path)
            if match:
                name = unquote(match.group(2))
                start, stop = parse_time_ns(query["from"]), parse_time_ns(query["to"])
                tag = owner.tags.get(name, StoredTag())
                now = owner.clock()
                cutoff = (
                    None
                    if owner.full_resolution_days is None
                    else now - owner.full_resolution_days * 86_400 * 1_000_000_000
                )
                buffered = {t for t, at in tag.arrivals if now - at < owner.flush_after_ns}
                readings = [
                    r
                    for r in tag.readings
                    if start <= r[0] <= stop
                    and (cutoff is None or r[0] >= cutoff or r[0] in buffered)
                ]
                return self._send(200, {"count": min(len(readings), int(query.get("limit", 1000)))})
        return self._send(404, {"error": "not found"})

    def do_POST(self):
        owner: FakeTagHistorian = self.owner
        url = urlparse(self.path)
        if url.path != "/api/v2/write":
            return self._send(404)
        if not self._authorised():
            return self._send(401)
        if parse_qs(url.query).get("precision") != ["ns"]:
            return self._send(
                400, {"code": "invalid", "message": "the copy always writes precision=ns"}
            )
        lines = [line for line in self._body().decode("utf-8").split("\n") if line]
        with owner._lock:
            owner.write_requests.append(lines)
            if len(lines) > 5000:
                return self._send(413, {"code": "request too large", "maxLength": 5000})
            action = owner.script.pop(0) if owner.script else None
            if callable(action):
                # Something that happens on the server between plan and write.
                action()
                action = None
            if action == "drop-before-store":
                self.close_connection = True
                self.connection.close()
                return None
            if action == "drop-after-store":
                owner.ingest(lines)
                self.close_connection = True
                self.connection.close()
                return None
            if isinstance(action, tuple):
                _, status, retry_after, body = action
                headers = {"Retry-After": str(retry_after)} if retry_after is not None else {}
                return self._send(status, body, headers=headers)
            status, headers, body = owner.ingest(lines)
        return self._send(status, body, headers=headers)


def _ticks(ns: int) -> str:
    text = format_time_ns((ns // 100) * 100)
    whole, _, fraction = text[:-1].partition(".")
    return f"{whole}.{fraction.ljust(7, '0')[:7]}Z"
