"""The Tag Historian side: what the account allows, what it holds, and the write.

Every number this tool states about a plan - how many tags are free, how many
readings a day the account takes - is read from the account at run time
(``GET /api/usage/limits``), the same source the server's own quota
enforcement reads. Nothing about a plan is written into this package.

THE WRITE is ``POST /api/v2/write``, the InfluxDB-compatible endpoint Home
Assistant's integration writes to live. Its answers are mapped onto
:class:`WriteOutcome` by what they MEAN for a history copy, which is not
always what they mean for a live writer:

* ``204`` - stored (or deadbanded as a repeat, which is the same decision live
  data gets);
* ``422`` - the batch named a NEW tag the plan has no room for, and its
  readings were dropped. A batch here only ever holds one tag, so that is the
  whole batch;
* ``429`` with "daily measurement quota exceeded" - the account's readings
  for the UTC day are spent, until midnight UTC (``Retry-After`` says how
  long). It counts the copy AND live forwarding together, which is why the
  copy stops short of it on its own (see ``migrate.DailyBudget``);
* any other ``429``, and ``503`` - wait as long as ``Retry-After`` says and
  send the same batch again. Refused before anything was stored, so a resend
  cannot duplicate;
* ``402`` - the account's storage is full. Stops the copy: it lifts only as
  full-resolution data ages out, or with a bigger plan;
* ``401``/``403`` - the key is wrong, revoked or Read-scoped, or the account
  has not confirmed its email address in time. Stops the copy;
* ``400`` - the line was refused as unparsable, which is a bug here, or a tag
  name the server cannot store. Stops that tag;
* ``413`` - too many points. Cannot happen at the batch size used, and is
  answered by splitting the batch if it does.

Anything without an answer - a timeout, a dropped connection, a 500 - may or
may not have been stored. See ``migrate.Runner._settle`` for how that is
resolved without writing anything twice.
"""

from __future__ import annotations

import gzip
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from urllib.parse import quote

import requests

from . import __version__
from .timeutil import NS_PER_SECOND, format_time_ns, parse_time_ns

DEFAULT_URL = "https://api.taghistorian.com"

# InfluxWriteController.MaxPointsPerRequest. The server answers 413 above it.
MAX_POINTS_PER_REQUEST = 5000

# The tag list's page size: TagsController clamps take to 1000.
_TAG_PAGE = 1000

# How often one read is tried against a 429/503 before the copy gives up on it.
_READ_ATTEMPTS = 5


class TagHistorianError(Exception):
    """The account cannot be used for the copy, and the message says why."""


@dataclass(frozen=True)
class Limits:
    tag_limit: int
    tags_used: int
    readings_per_day: int

    @property
    def tags_free(self) -> int:
        return max(0, self.tag_limit - self.tags_used)


@dataclass(frozen=True)
class ExistingTag:
    """What Tag Historian already holds for a tag.

    ``last_ns`` is the newest reading the tag was ever SENT - kept by ingest
    for every reading of any age, stored or dropped as a repeat - not the
    newest one a read of its stored data would find.
    """

    readings: int
    first_ns: int | None
    last_ns: int | None
    units: str = ""
    description: str = ""


@dataclass(frozen=True)
class WriteOutcome:
    kind: str  # stored | tag_refused | daily_quota | retry | storage_full | denied | invalid | too_large | unanswered
    status: int | None = None
    retry_after: float | None = None
    message: str = ""


def _message(response: requests.Response) -> str:
    try:
        body = response.json()
    except ValueError:
        return response.text.strip()[:500]
    if isinstance(body, dict):
        for key in ("message", "error"):
            if body.get(key):
                return str(body[key])
    return response.text.strip()[:500]


def _retry_after(response: requests.Response) -> float | None:
    value = response.headers.get("Retry-After")
    if value is None:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        return None


def ticks_text(ns: int, *, round_up: bool) -> str:
    """A timestamp the API's DateTime binder takes, at its 100 ns resolution.

    Nanoseconds do not survive a .NET DateTime: the server keeps 100 ns ticks.
    A window for "the points of this batch" is therefore widened to whole
    ticks - down at the start, up at the end - so it can never miss one.
    """
    ticks, remainder = divmod(ns, 100)
    if round_up and remainder:
        ticks += 1
    seconds, sub = divmod(ticks, NS_PER_SECOND // 100)
    whole = format_time_ns(seconds * NS_PER_SECOND)[:-1]  # drop the Z
    return f"{whole}.{sub:07d}Z"


class TagHistorian:
    def __init__(
        self,
        api_key: str,
        url: str = DEFAULT_URL,
        *,
        timeout: float = 60.0,
        session: requests.Session | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        if not api_key:
            raise ValueError("an API key is required")
        self._url = url.rstrip("/")
        self._timeout = timeout
        self._sleep = sleep
        self._session = session or requests.Session()
        self._session.headers.update(
            {
                # The header InfluxDB v2 clients send; the API reads it on every
                # endpoint, not only on the Influx-compatible one.
                "Authorization": f"Token {api_key}",
                "User-Agent": f"taghistorian-influx-migrate/{__version__}",
            }
        )
        self._customer_id: str | None = None

    @property
    def url(self) -> str:
        return self._url

    def close(self) -> None:
        self._session.close()

    # -- reads ---------------------------------------------------------------

    def _get(self, path: str, params: dict | None = None) -> requests.Response:
        # The account and tag reads sit on the API's smaller per-minute budget,
        # which everything else holding a key for the account draws on too. A
        # 429 there is a request to wait, and says how long.
        for attempt in range(_READ_ATTEMPTS):
            try:
                response = self._session.get(
                    f"{self._url}{path}", params=params, timeout=self._timeout
                )
            except requests.RequestException as exc:
                raise TagHistorianError(
                    f"could not reach Tag Historian at {self._url}: {exc}"
                ) from exc
            if response.status_code not in (429, 503) or attempt == _READ_ATTEMPTS - 1:
                break
            self._sleep(_retry_after(response) or 2.0 ** (attempt + 1))
        if response.status_code == 401:
            raise TagHistorianError(
                "Tag Historian refused the API key (401): it is mistyped or has been revoked."
            )
        if response.status_code == 403:
            raise TagHistorianError(
                "Tag Historian refused the API key for this (403). The copy needs a key whose "
                "Scope is Write; a Read key can neither write nor see the account's limits."
            )
        if response.status_code >= 400:
            raise TagHistorianError(
                f"Tag Historian answered {response.status_code} to {path}: {_message(response)}"
            )
        return response

    def customer_id(self) -> str:
        if self._customer_id is None:
            self._customer_id = str(self._get("/api/customers/me").json()["customerId"])
        return self._customer_id

    def limits(self) -> Limits:
        body = self._get("/api/usage/limits").json()
        return Limits(
            tag_limit=int(body["tagLimit"]),
            tags_used=int(body["currentTagCount"]),
            readings_per_day=int(body["measurementsPerDayLimit"]),
        )

    def daily_readings(self, days: int = 1) -> dict[str, int]:
        """Readings the account was sent per UTC day, by everything, newest last.

        The same counter the daily allowance is enforced against - including
        what is still in memory on the server - so today's figure is what the
        next write is measured by, and yesterday's is the best guess at what
        live forwarding alone sends in a day.
        """
        body = self._get("/api/usage/daily", {"days": days}).json()
        return {
            str(day.get("date")): int(day.get("measurementCount") or 0)
            for day in body.get("dailyUsage") or []
        }

    def existing_tags(self) -> dict[str, ExistingTag]:
        """Every tag on the account with what it holds - one page of a thousand at a time."""
        tags: dict[str, ExistingTag] = {}
        skip = 0
        while True:
            body = self._get("/api/tags", {"skip": skip, "take": _TAG_PAGE}).json()
            page = body.get("tags") or []
            for tag in page:
                tags[tag["tagName"]] = _existing(tag)
            skip += len(page)
            if not page or skip >= int(body.get("totalCount") or 0):
                return tags

    def existing_tag(self, tag_name: str) -> ExistingTag | None:
        """What the account holds for one tag right now; None if it has no such tag.

        From the tag list, searched by name, rather than from
        ``/api/tags/single``: the list's ``lastTimestamp`` is the newest
        reading the tag was ever sent, which is the question asked of it here,
        and the single-tag answer carries the same value under the name
        ``lastStoredTimestamp`` - a name a fix on the server could one day make
        true, and then it would leave out a last reading the deadband dropped.
        The search is a substring match, so the exact name is picked out.
        """
        skip = 0
        while True:
            body = self._get(
                "/api/tags", {"search": tag_name, "skip": skip, "take": _TAG_PAGE}
            ).json()
            page = body.get("tags") or []
            for tag in page:
                if tag.get("tagName") == tag_name:
                    return _existing(tag)
            skip += len(page)
            if not page or skip >= int(body.get("totalCount") or 0):
                return None

    def stored_between(self, tag_name: str, first_ns: int, last_ns: int) -> bool:
        """Whether the tag holds at least one stored reading in the window.

        Read from storage and from the server's write buffer both - the same
        read a chart makes - so a batch that has been accepted but not yet
        flushed to disk counts as there.

        Exact only inside the plan's full-resolution period. Older than that
        the server answers from hourly summaries, which the nightly roll-up
        builds, so a window of old history that has left the write buffer
        reads as empty whether or not it was stored. ``migrate.Runner._settle``
        asks the tag's newest reading first for that reason.
        """
        path = f"/api/measurements/{self.customer_id()}/{quote(tag_name, safe='')}"
        body = self._get(
            path,
            {
                "from": ticks_text(first_ns, round_up=False),
                "to": ticks_text(last_ns, round_up=True),
                "limit": 1,
            },
        ).json()
        return int(body.get("count") or 0) > 0

    # -- the write -----------------------------------------------------------

    def write(self, lines: Sequence[str]) -> WriteOutcome:
        if len(lines) > MAX_POINTS_PER_REQUEST:
            raise ValueError(f"at most {MAX_POINTS_PER_REQUEST} lines per request")
        body = gzip.compress("\n".join(lines).encode("utf-8"))
        try:
            response = self._session.post(
                f"{self._url}/api/v2/write",
                params={"precision": "ns"},
                data=body,
                headers={
                    "Content-Type": "text/plain; charset=utf-8",
                    "Content-Encoding": "gzip",
                },
                timeout=self._timeout,
            )
        except requests.RequestException as exc:
            return WriteOutcome("unanswered", message=str(exc))

        status = response.status_code
        message = _message(response) if status != 204 else ""
        if status in (200, 204):
            return WriteOutcome("stored", status)
        if status == 422:
            return WriteOutcome("tag_refused", status, message=message)
        if status == 429 and "daily measurement quota" in message:
            return WriteOutcome("daily_quota", status, _retry_after(response), message)
        if status in (429, 503):
            return WriteOutcome("retry", status, _retry_after(response), message)
        if status == 402:
            return WriteOutcome("storage_full", status, message=message)
        if status in (401, 403):
            return WriteOutcome("denied", status, message=message)
        if status == 413:
            return WriteOutcome("too_large", status, message=message)
        if status == 400:
            return WriteOutcome("invalid", status, message=message)
        # A 500 is the endpoint's catch-all, and it can come after the batch
        # was committed. Not knowing is not the same as not stored.
        return WriteOutcome("unanswered", status, message=message)


def _existing(tag: dict) -> ExistingTag:
    readings = int(tag.get("totalMeasurements") or 0)
    return ExistingTag(
        readings=readings,
        first_ns=_time_or_none(tag.get("firstTimestamp")) if readings else None,
        last_ns=_time_or_none(tag.get("lastTimestamp")) if readings else None,
        units=str(tag.get("units") or ""),
        description=str(tag.get("description") or ""),
    )


def _time_or_none(value: object) -> int | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        return parse_time_ns(value)
    except ValueError:
        return None
