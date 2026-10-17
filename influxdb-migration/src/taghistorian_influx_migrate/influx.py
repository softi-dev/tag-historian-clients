"""Reading Home Assistant's history out of InfluxDB 1.x and 2.x, over HTTP.

Two readers with one shape: :meth:`discover` lists the Home Assistant series
in a time range with how many readings each holds, and :meth:`read` returns
one series' points in a window. Nothing else is asked of InfluxDB, and both
readers keep to the plainest parts of its query languages on purpose -
``SELECT ... GROUP BY *`` and ``COUNT``/``LAST`` in InfluxQL, and ``from``,
``range``, ``filter``, ``count`` and ``last`` in Flux - because those are the
parts every release of each version answers the same way.

InfluxDB 3 is NOT supported: it has no Flux, and its InfluxQL compatibility
layer has never been run against this reader.

A series is what InfluxDB calls one: a measurement and its full tag set. For
Home Assistant that is normally the unit (``°C``) with ``domain`` and
``entity_id`` tags, and the reading is the ``value`` field. Only series that
carry both ``domain`` and ``entity_id`` are listed - the shape Home
Assistant's influxdb integration writes - and only their ``value`` readings
are copied, with the friendly name and unit beside them.
"""

from __future__ import annotations

import csv
import io
import re
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field

import requests

from .naming import FRIENDLY_NAME_FIELD, UNIT_FIELD, VALUE_FIELD, FieldValue
from .timeutil import format_time_ns, parse_time_ns

# The three fields a Home Assistant point is read for. Everything else on the
# line is an attribute, which live forwarding ignores beside a numeric value.
READ_FIELDS = (VALUE_FIELD, FRIENDLY_NAME_FIELD, UNIT_FIELD)


class InfluxError(Exception):
    """InfluxDB refused a query, or answered with something that is not a result."""


class PartialResult(InfluxError):
    """InfluxDB 1.x cut a result short at its ``max-row-limit``."""


@dataclass(frozen=True, order=True)
class SeriesKey:
    """A measurement and its complete tag set - InfluxDB's identity of a series.

    Tags with an empty value are dropped: InfluxDB 1.x reports every tag key a
    measurement has on every series of it, with ``""`` for the ones a series
    does not carry, and a series without a tag must not be named as if it had
    one.
    """

    measurement: str
    tags: tuple[tuple[str, str], ...]

    @staticmethod
    def of(measurement: str, tags: Mapping[str, str | None]) -> SeriesKey:
        return SeriesKey(
            measurement, tuple(sorted((k, v) for k, v in tags.items() if v not in (None, "")))
        )

    @property
    def tag_dict(self) -> dict[str, str]:
        return dict(self.tags)

    def to_json(self) -> dict:
        return {"measurement": self.measurement, "tags": self.tag_dict}

    @staticmethod
    def from_json(data: Mapping) -> SeriesKey:
        return SeriesKey.of(data["measurement"], data["tags"])


@dataclass
class SeriesSummary:
    """What discovery knows about a series without reading it."""

    key: SeriesKey
    readings: int
    friendly_name: str | None = None
    unit_field: str | None = None


@dataclass
class SourcePoint:
    timestamp_ns: int
    fields: dict[str, FieldValue] = field(default_factory=dict)


def _is_home_assistant(tags: Mapping[str, str]) -> bool:
    return "domain" in tags and "entity_id" in tags


class InfluxSource:
    """The interface both readers implement."""

    def describe(self) -> str:  # pragma: no cover - interface
        raise NotImplementedError

    def identity(self) -> dict:  # pragma: no cover - interface
        raise NotImplementedError

    def discover(
        self, start_ns: int, end_ns: int, measurements: Sequence[str] = ()
    ) -> list[SeriesSummary]:  # pragma: no cover - interface
        raise NotImplementedError

    def first_reading(
        self, key: SeriesKey, start_ns: int, end_ns: int
    ) -> int | None:  # pragma: no cover - interface
        """When the series' first ``value`` in the range is, or None if it has none.

        Asked before reading, so the copy starts where the data does rather
        than walking a day at a time from the start of the range through years
        of nothing.
        """
        raise NotImplementedError

    def read(
        self, key: SeriesKey, start_ns: int, end_ns: int
    ) -> list[SourcePoint]:  # pragma: no cover - interface
        raise NotImplementedError

    def close(self) -> None:  # pragma: no cover - interface
        pass


# ---------------------------------------------------------------------------
# InfluxDB 1.x - InfluxQL over GET/POST /query
# ---------------------------------------------------------------------------


def influxql_identifier(name: str) -> str:
    """A double-quoted InfluxQL identifier."""
    return '"' + name.replace("\\", "\\\\").replace('"', '\\"') + '"'


def influxql_string(value: str) -> str:
    """A single-quoted InfluxQL string literal."""
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


class InfluxV1Source(InfluxSource):
    """InfluxDB 1.x (and the Home Assistant InfluxDB app, which is 1.8).

    Queries go to ``/query`` with ``epoch=ns``, so every timestamp arrives as
    an exact integer rather than a string to parse. Credentials, if the
    database has authentication on, are sent as HTTP basic auth, which 1.x
    accepts on every endpoint.
    """

    def __init__(
        self,
        url: str,
        database: str,
        *,
        username: str | None = None,
        password: str | None = None,
        retention_policy: str | None = None,
        timeout: float = 120.0,
        session: requests.Session | None = None,
        verify_tls: bool = True,
    ) -> None:
        self._url = url.rstrip("/")
        self._database = database
        self._rp = retention_policy
        self._timeout = timeout
        self._session = session or requests.Session()
        self._session.verify = verify_tls
        if username:
            self._session.auth = (username, password or "")

    def describe(self) -> str:
        rp = f", retention policy {self._rp}" if self._rp else ""
        return f'InfluxDB 1.x at {self._url}, database "{self._database}"{rp}'

    def identity(self) -> dict:
        return {
            "kind": "influxdb1",
            "url": self._url,
            "database": self._database,
            "retention_policy": self._rp,
        }

    def close(self) -> None:
        self._session.close()

    def _from(self, measurements: Sequence[str]) -> str:
        prefix = f"{influxql_identifier(self._rp)}." if self._rp else ""
        if not measurements:
            return f"{prefix}/.*/"
        return ",".join(prefix + influxql_identifier(m) for m in measurements)

    def _query(self, statement: str) -> list[dict]:
        """The series of one statement's result. Raises on any error."""
        try:
            response = self._session.post(
                f"{self._url}/query",
                params={"db": self._database, "epoch": "ns"},
                data={"q": statement},
                timeout=self._timeout,
            )
        except requests.RequestException as exc:
            raise InfluxError(f"could not reach InfluxDB at {self._url}: {exc}") from exc

        if response.status_code == 401:
            raise InfluxError(
                "InfluxDB refused the username or password (401). Pass --influx-username and "
                "--influx-password, or set INFLUX_USERNAME and INFLUX_PASSWORD."
            )
        try:
            body = response.json()
        except ValueError as exc:
            raise InfluxError(
                f"InfluxDB answered {response.status_code} with something that is not JSON: "
                f"{response.text[:200]!r}"
            ) from exc
        if response.status_code >= 400 or "error" in body:
            raise InfluxError(f"InfluxDB refused the query: {body.get('error', response.text)}")

        results = body.get("results") or []
        if not results:
            return []
        result = results[0]
        if "error" in result:
            raise InfluxError(f"InfluxDB refused the query: {result['error']}")
        series = result.get("series") or []
        if result.get("partial") or any(s.get("partial") for s in series):
            raise PartialResult("InfluxDB returned a partial result")
        return series

    def discover(
        self, start_ns: int, end_ns: int, measurements: Sequence[str] = ()
    ) -> list[SeriesSummary]:
        where = f"time >= {start_ns} AND time < {end_ns}"
        source = self._from(measurements)

        counts: dict[SeriesKey, int] = {}
        for series in self._query(f'SELECT COUNT("value") FROM {source} WHERE {where} GROUP BY *'):
            key = SeriesKey.of(series["name"], series.get("tags") or {})
            if not _is_home_assistant(key.tag_dict):
                continue
            column = series["columns"].index("count")
            total = sum(int(row[column] or 0) for row in series.get("values") or [])
            if total > 0:
                counts[key] = counts.get(key, 0) + total

        names: dict[SeriesKey, tuple[str | None, str | None]] = {}
        try:
            described = self._query(
                f'SELECT LAST("{FRIENDLY_NAME_FIELD}") AS "friendly_name", '
                f'LAST("{UNIT_FIELD}") AS "unit" FROM {source} WHERE {where} GROUP BY *'
            )
        except InfluxError:
            # Only labels for the dry run. A database where neither field was
            # ever written refuses the statement on some releases; the copy
            # reads them per point regardless.
            described = []
        for series in described:
            key = SeriesKey.of(series["name"], series.get("tags") or {})
            columns = series["columns"]
            row = (series.get("values") or [[None] * len(columns)])[-1]
            values = dict(zip(columns, row))
            names[key] = (values.get("friendly_name"), values.get("unit"))

        return [
            SeriesSummary(key, readings, *names.get(key, (None, None)))
            for key, readings in sorted(counts.items())
        ]

    @staticmethod
    def _where(key: SeriesKey, start_ns: int, end_ns: int) -> str:
        tags = key.tag_dict
        return " AND ".join(
            [
                f"{influxql_identifier('domain')} = {influxql_string(tags['domain'])}",
                f"{influxql_identifier('entity_id')} = {influxql_string(tags['entity_id'])}",
                f"time >= {start_ns}",
                f"time < {end_ns}",
            ]
        )

    def first_reading(self, key: SeriesKey, start_ns: int, end_ns: int) -> int | None:
        statement = (
            f'SELECT FIRST("{VALUE_FIELD}") FROM {self._from([key.measurement])} '
            f"WHERE {self._where(key, start_ns, end_ns)} GROUP BY *"
        )
        for series in self._query(statement):
            if SeriesKey.of(series["name"], series.get("tags") or {}) != key:
                continue
            column = series["columns"].index("time")
            times = [int(row[column]) for row in series.get("values") or []]
            if times:
                return min(times)
        return None

    def read(self, key: SeriesKey, start_ns: int, end_ns: int) -> list[SourcePoint]:
        fields = ", ".join(influxql_identifier(f) for f in READ_FIELDS)
        statement = (
            f"SELECT {fields} FROM {self._from([key.measurement])} "
            f"WHERE {self._where(key, start_ns, end_ns)} GROUP BY *"
        )

        points: list[SourcePoint] = []
        for series in self._query(statement):
            # GROUP BY * splits off any series that shares domain and
            # entity_id but carries another tag; that one is a different tag
            # in Tag Historian and is read on its own.
            if SeriesKey.of(series["name"], series.get("tags") or {}) != key:
                continue
            columns = series["columns"]
            for row in series.get("values") or []:
                values = dict(zip(columns, row))
                point = SourcePoint(int(values.pop("time")))
                for name, value in values.items():
                    if value is None:
                        continue
                    # JSON cannot tell 21 from 21.0; see lineprotocol.number_field.
                    if isinstance(value, int) and not isinstance(value, bool):
                        value = float(value)
                    point.fields[name] = value
                points.append(point)
        points.sort(key=lambda p: p.timestamp_ns)
        return points


# ---------------------------------------------------------------------------
# InfluxDB 2.x - Flux over POST /api/v2/query, annotated CSV back
# ---------------------------------------------------------------------------


def flux_string(value: str) -> str:
    """A Flux string literal. ``${`` starts interpolation in Flux, so ``$`` is escaped too."""
    escaped = value.replace("\\", "\\\\").replace('"', '\\"').replace("$", "\\$")
    return '"' + escaped.replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t") + '"'


# Columns Flux adds to every row that are not tags of the series.
_FLUX_SYSTEM_COLUMNS = frozenset(
    {"", "result", "table", "_start", "_stop", "_time", "_value", "_field", "_measurement"}
)


@dataclass
class FluxRow:
    values: dict[str, object]


def _convert(datatype: str, text: str) -> object:
    if text == "":
        return None
    if datatype == "double":
        return float(text)
    if datatype in ("long", "unsignedLong"):
        return int(text)
    if datatype == "boolean":
        return text == "true"
    if datatype.startswith("dateTime"):
        return parse_time_ns(text)
    return text


def parse_annotated_csv(text: str) -> list[FluxRow]:
    """Every data row of a Flux annotated-CSV response, typed by ``#datatype``.

    A response is several tables, each with its own annotation rows and
    header, separated by blank lines - tables with different columns (a series
    with an extra tag, a string field beside a float one) cannot share a
    header. An ``error`` table, which is how Flux reports a failure that
    happens after the response has started, raises.
    """
    rows: list[FluxRow] = []
    datatypes: list[str] = []
    header: list[str] | None = None

    for record in csv.reader(io.StringIO(text)):
        if not record or all(cell == "" for cell in record):
            datatypes, header = [], None
            continue
        if record[0] == "#datatype":
            datatypes, header = record, None
            continue
        if record[0].startswith("#"):
            continue
        if header is None:
            header = record
            if "error" in header and "reference" in header:
                # The next row carries the message.
                continue
            continue
        if "error" in header and "reference" in header:
            message = record[header.index("error")]
            raise InfluxError(f"InfluxDB failed the query: {message}")
        values = {}
        for index, name in enumerate(header):
            if name == "" or index >= len(record):
                continue
            datatype = datatypes[index] if index < len(datatypes) else "string"
            values[name] = _convert(datatype, record[index])
        rows.append(FluxRow(values))
    return rows


def _series_of(row: FluxRow) -> SeriesKey:
    tags = {
        name: value
        for name, value in row.values.items()
        if name not in _FLUX_SYSTEM_COLUMNS and isinstance(value, str)
    }
    return SeriesKey.of(str(row.values.get("_measurement", "")), tags)


class InfluxV2Source(InfluxSource):
    """InfluxDB 2.x: a bucket in an organization, an API token, Flux queries.

    The token needs read access to the bucket and nothing else.
    """

    def __init__(
        self,
        url: str,
        org: str,
        bucket: str,
        token: str,
        *,
        timeout: float = 120.0,
        session: requests.Session | None = None,
        verify_tls: bool = True,
    ) -> None:
        self._url = url.rstrip("/")
        self._org = org
        self._bucket = bucket
        self._timeout = timeout
        self._session = session or requests.Session()
        self._session.verify = verify_tls
        self._session.headers.update(
            {
                "Authorization": f"Token {token}",
                "Accept": "application/csv",
                "Content-Type": "application/json",
            }
        )

    def describe(self) -> str:
        return f'InfluxDB 2.x at {self._url}, organization "{self._org}", bucket "{self._bucket}"'

    def identity(self) -> dict:
        return {"kind": "influxdb2", "url": self._url, "org": self._org, "bucket": self._bucket}

    def close(self) -> None:
        self._session.close()

    def _query(self, flux: str) -> list[FluxRow]:
        try:
            response = self._session.post(
                f"{self._url}/api/v2/query",
                params={"org": self._org},
                json={
                    "query": flux,
                    "type": "flux",
                    "dialect": {
                        "header": True,
                        "delimiter": ",",
                        "annotations": ["datatype", "group", "default"],
                    },
                },
                timeout=self._timeout,
            )
        except requests.RequestException as exc:
            raise InfluxError(f"could not reach InfluxDB at {self._url}: {exc}") from exc

        if response.status_code == 401:
            raise InfluxError(
                "InfluxDB refused the token (401). Pass --influx-token or set INFLUX_TOKEN, with "
                "a token that can read the bucket."
            )
        if response.status_code >= 400:
            try:
                message = response.json().get("message", response.text)
            except ValueError:
                message = response.text
            raise InfluxError(f"InfluxDB refused the query ({response.status_code}): {message}")
        return parse_annotated_csv(response.text)

    def _range(self, start_ns: int, end_ns: int) -> str:
        return (
            f"from(bucket: {flux_string(self._bucket)})\n"
            f"  |> range(start: {format_time_ns(start_ns)}, stop: {format_time_ns(end_ns)})\n"
        )

    @staticmethod
    def _measurement_filter(measurements: Sequence[str]) -> str:
        if not measurements:
            return ""
        clause = " or ".join(f"r._measurement == {flux_string(m)}" for m in measurements)
        return f"  |> filter(fn: (r) => {clause})\n"

    def discover(
        self, start_ns: int, end_ns: int, measurements: Sequence[str] = ()
    ) -> list[SeriesSummary]:
        scope = self._range(start_ns, end_ns) + self._measurement_filter(measurements)

        counts: dict[SeriesKey, int] = {}
        for row in self._query(
            scope + f'  |> filter(fn: (r) => r._field == "{VALUE_FIELD}")\n  |> count()\n'
        ):
            key = _series_of(row)
            if not _is_home_assistant(key.tag_dict):
                continue
            counts[key] = counts.get(key, 0) + int(row.values.get("_value") or 0)

        names: dict[SeriesKey, dict[str, str | None]] = {}
        for row in self._query(
            scope + f'  |> filter(fn: (r) => r._field == "{FRIENDLY_NAME_FIELD}" or '
            f'r._field == "{UNIT_FIELD}")\n  |> last()\n'
        ):
            value = row.values.get("_value")
            names.setdefault(_series_of(row), {})[str(row.values.get("_field"))] = (
                value if isinstance(value, str) else None
            )

        return [
            SeriesSummary(
                key,
                readings,
                names.get(key, {}).get(FRIENDLY_NAME_FIELD),
                names.get(key, {}).get(UNIT_FIELD),
            )
            for key, readings in sorted(counts.items())
            if readings > 0
        ]

    def _series_filter(self, key: SeriesKey, start_ns: int, end_ns: int) -> str:
        tags = key.tag_dict
        return (
            self._range(start_ns, end_ns)
            + f"  |> filter(fn: (r) => r._measurement == {flux_string(key.measurement)} and "
            f"r.domain == {flux_string(tags['domain'])} and "
            f"r.entity_id == {flux_string(tags['entity_id'])})\n"
        )

    def first_reading(self, key: SeriesKey, start_ns: int, end_ns: int) -> int | None:
        flux = (
            self._series_filter(key, start_ns, end_ns)
            + f'  |> filter(fn: (r) => r._field == "{VALUE_FIELD}")\n  |> first()\n'
        )
        times = [
            int(row.values["_time"])  # type: ignore[call-overload]
            for row in self._query(flux)
            if _series_of(row) == key and isinstance(row.values.get("_time"), int)
        ]
        return min(times) if times else None

    def read(self, key: SeriesKey, start_ns: int, end_ns: int) -> list[SourcePoint]:
        flux = (
            self._series_filter(key, start_ns, end_ns)
            + "  |> filter(fn: (r) => "
            + " or ".join(f'r._field == "{f}"' for f in READ_FIELDS)
            + ")\n"
        )

        by_time: dict[int, SourcePoint] = {}
        for row in self._query(flux):
            # Flux returns one table per series AND field; a series with an
            # extra tag is a different Tag Historian tag and is read on its own.
            if _series_of(row) != key:
                continue
            timestamp = row.values.get("_time")
            value = row.values.get("_value")
            if not isinstance(timestamp, int) or value is None:
                continue
            point = by_time.setdefault(timestamp, SourcePoint(timestamp))
            point.fields[str(row.values.get("_field"))] = value  # type: ignore[assignment]
        return [by_time[t] for t in sorted(by_time)]


def filter_series(
    series: Iterable[SeriesSummary], include: Sequence[str], exclude: Sequence[str]
) -> list[SeriesSummary]:
    """Keep the series whose Home Assistant entity id matches the patterns.

    Patterns are shell-style globs on ``domain.entity_id`` (``sensor.*_power``),
    the same spelling Home Assistant's own ``include``/``exclude`` takes. No
    ``include`` means everything.
    """
    from fnmatch import fnmatchcase

    def entity(summary: SeriesSummary) -> str:
        tags = summary.key.tag_dict
        return f"{tags['domain']}.{tags['entity_id']}"

    kept = []
    for summary in series:
        name = entity(summary)
        if include and not any(fnmatchcase(name, p) for p in include):
            continue
        if any(fnmatchcase(name, p) for p in exclude):
            continue
        kept.append(summary)
    return kept


_ENTITY_PATTERN = re.compile(r"^[a-z_*?\[\]0-9]+\.[^\s]+$")


def looks_like_entity_pattern(pattern: str) -> bool:
    """A cheap check that a pattern is ``domain.object_id``-shaped."""
    return bool(_ENTITY_PATTERN.match(pattern))
