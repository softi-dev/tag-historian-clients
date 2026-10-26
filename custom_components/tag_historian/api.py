"""Thin async client for the Tag Historian API.

No pip dependency: this is hand-written over the aiohttp session Home
Assistant already runs, so the integration installs instantly and adds nothing
to anybody's supply chain. An InfluxDB client library would have bought about
fifty lines of value and cost ARM wheel builds on every Raspberry Pi install.

The important work in this file is :meth:`TagHistorianClient.async_write`
turning the endpoint's status codes into distinguishable outcomes. Home
Assistant's built-in influxdb integration treats every non-2xx alike and never
reads ``Retry-After``; the whole reason a user would install this component
instead is that 401, 403, 422 and 429 are four different problems with four
different fixes, and it can say which one happened.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from typing import Any

import aiohttp
from yarl import URL

from .const import HTTP_TIMEOUT_SECONDS, IMPORT_TIMEOUT_SECONDS, LOGGER


class TagHistorianError(Exception):
    """Base error."""


class TagHistorianConnectionError(TagHistorianError):
    """The host did not answer, or answered with something unusable."""


class TagHistorianServerError(TagHistorianConnectionError):
    """Tag Historian answered, with an error of its own (a 500) that it did not explain.

    Retried like a lost connection - nothing is known to be stored - but it is
    not a sign that Tag Historian cannot be reached, and the same batch may
    well get the same answer every time (importer.STOP_FAILED).
    """


# The ``reason`` of the 503 that Tag Historian answers when it could not CHECK a
# key at all - a moment of trouble on its side - on every endpoint
# that needs a sign-in. A key it checked and refused is a 401, never this.
SIGN_IN_UNAVAILABLE = "sign_in_unavailable"


class TagHistorianUnavailableError(TagHistorianConnectionError):
    """503 - Tag Historian answered, but could not serve this just now.

    A connection error to every caller that only asks "did it go through",
    because the answer is the same - nothing happened, ask again later - but
    the screens a person is looking at must not call it an unreachable host
    or a rejected key. ``reason`` is the body's: ``sign_in_unavailable``
    (:data:`SIGN_IN_UNAVAILABLE`) when the key could not be checked just now,
    which says nothing about the key. ``Retry-After`` is 30 to 45 seconds
    then.
    """

    def __init__(self, what: str, *, reason: str = "", retry_after: int = 0) -> None:
        super().__init__(f"{what} answered 503" + (f" ({reason})" if reason else ""))
        self.reason = reason
        self.retry_after = retry_after

    @property
    def key_not_checked(self) -> bool:
        """The key could not be checked just now - not refused, not checked at all."""
        return self.reason == SIGN_IN_UNAVAILABLE


class TagHistorianAuthError(TagHistorianError):
    """401 - the key is wrong, revoked, or the account is inactive.

    The API's key handler fails authentication for an inactive customer as well
    as for an unknown key, so both arrive here. Reauthentication is the right
    response either way: it is the only screen that can accept a new key, and
    an inactive account will keep failing it with the same message until it is
    reactivated.
    """


class TagHistorianForbiddenError(TagHistorianError):
    """403 - a valid key that is not allowed to do this.

    Two causes, both with a body written for a human to read: an unconfirmed
    email address past its grace period, and an account scheduled for erasure.
    The message is surfaced verbatim rather than paraphrased - the API already
    says what to do about each, and a paraphrase is a fifth public surface that
    can drift out of step with the code.
    """

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message


class TagHistorianPermissionError(TagHistorianError):
    """The key authenticated but lacks the scope this call needs.

    A Read-scoped key can call ``/api/customers/me`` and fails everything else,
    which is exactly what makes "wrong key" and "read-only key" separable at
    setup instead of at 3am.
    """


class WriteOutcome(Enum):
    """What the write endpoint said, in the vocabulary of what to do next.

    Note the two 429s. The endpoint answers 429 for two unrelated things and
    they are NOT one outcome: ``DAILY_QUOTA`` is the account's reading
    allowance being spent and is a billing fact, ``BACK_PRESSURE`` is our own
    write buffer refusing a customer's fair share for the next few seconds and
    is nobody's business but ours. See :func:`_classify_429`.
    """

    OK = "ok"
    PARTIAL_TAG_QUOTA = "partial_tag_quota"  # 422 - do NOT retry
    DAILY_QUOTA = "daily_quota"  # 429 (middleware) - wait for Retry-After
    BACK_PRESSURE = "back_pressure"  # 429 (controller) - our saturation
    UNAVAILABLE = "unavailable"  # 503 - wait for Retry-After
    FORBIDDEN = "forbidden"  # 403/402 - permanent until a human acts
    REJECTED = "rejected"  # 400/413 - our bug; the batch is unsendable


@dataclass
class WriteResult:
    """The classified result of one write."""

    outcome: WriteOutcome
    retry_after: int = 0
    skipped_tags: list[str] = field(default_factory=list)
    # Points the API says it dropped, from the 422 body. ``None`` means the
    # body did not say - which is not the same as zero, and the difference is
    # the whole of :meth:`StateForwarder._handle_result`'s honesty.
    dropped_points: int | None = None
    message: str = ""

    @property
    def retryable(self) -> bool:
        """Whether re-sending THIS BODY could ever succeed.

        422 is deliberately not retryable. It means part of the batch WAS
        written and the rest was refused for a reason resending cannot change;
        the API chose that shape precisely because an Influx client retries a
        failed request forever with the same body. Retrying it would duplicate
        the points that landed - the endpoint does not deduplicate.
        """
        return self.outcome in (
            WriteOutcome.DAILY_QUOTA,
            WriteOutcome.BACK_PRESSURE,
            WriteOutcome.UNAVAILABLE,
        )


class ImportOutcome(Enum):
    """What the import endpoint said about one batch, in terms of what to do next.

    Unlike the live write endpoint, every refusal that will not change by
    waiting ends the import: the allowance spent, the storage cap reached, an
    account that may not write. Waiting is only for the answers that say so -
    a busy write buffer, a server between two processes - and a batch refused
    for a retry stored nothing, so sending it again is safe (and would be safe
    anyway: the import endpoint skips a reading it already has).
    """

    TAKEN = "taken"  # 200 - counts in the result
    RETRY = "retry"  # 429/503 - wait Retry-After, send the same batch again
    ALLOWANCE_SPENT = "allowance_spent"  # 402 - the account's import allowance is used up
    STORAGE_FULL = "storage_full"  # 402 - the plan's storage cap
    # 402 with code hours_allowance - the hourly history allowance, which is
    # not the readings' and comes back by itself (resumes_at).
    HOURS_ALLOWANCE_SPENT = "hours_allowance_spent"
    FORBIDDEN = "forbidden"  # 403 - unconfirmed address, account being erased
    UNKNOWN_IMPORT = "unknown_import"  # 404 - the import is not this account's (any more)
    # 404 with no body: the route is not there - a Tag Historian older than
    # the hours import. Only the hours endpoint can answer it.
    NOT_SUPPORTED = "not_supported"
    REJECTED = "rejected"  # 400/413 - a body this integration should not have built


@dataclass(frozen=True)
class ImportAllowance:
    """``GET /api/import/allowance``: how much history the account may still import.

    The total is the plan's daily reading allowance times its full-resolution
    window in days - both numbers from the API, like every other limit this
    integration shows.
    """

    total: int
    used: int
    remaining: int
    readings_per_day: int
    window_days: int
    window_start: datetime | None = None

    @classmethod
    def from_json(cls, payload: dict[str, Any], window_start: Any = None) -> ImportAllowance:
        return cls(
            total=int(payload.get("total", 0)),
            used=int(payload.get("used", 0)),
            remaining=int(payload.get("remaining", 0)),
            readings_per_day=int(payload.get("readingsPerDay", 0)),
            window_days=int(payload.get("windowDays", 0)),
            window_start=_parse_time(
                window_start if window_start is not None else payload.get("windowStart")
            ),
        )


@dataclass(frozen=True)
class ImportSession:
    """``POST /api/import``: an import, and the line its readings must stay behind."""

    import_id: str
    started_at: datetime
    window_start: datetime
    max_readings_per_batch: int
    allowance: ImportAllowance


@dataclass
class ImportBatchResult:
    """One batch of an import, as the endpoint answered it."""

    outcome: ImportOutcome
    retry_after: int = 0
    stored: int = 0
    compressed: int = 0
    duplicates: int = 0
    outside_window: int = 0
    refused_not_history: int = 0
    refused_tag_limit: int = 0
    refused_allowance: int = 0
    refused_storage: int = 0
    refused_tags: list[str] = field(default_factory=list)
    # Per tag in the batch: readings must be older than this to be imported -
    # the import's start, or the tag's first live reading if that is earlier.
    importable_before: dict[str, datetime] = field(default_factory=dict)
    allowance_remaining: int | None = None
    message: str = ""

    @property
    def refused(self) -> int:
        return (
            self.refused_not_history
            + self.refused_tag_limit
            + self.refused_allowance
            + self.refused_storage
        )


@dataclass
class HoursBatchResult:
    """One batch of hourly statistics (``POST /api/import/{id}/hours``), as the endpoint answered it."""

    outcome: ImportOutcome
    retry_after: int = 0
    stored: int = 0
    duplicates: int = 0
    outside_retention: int = 0
    # Meter hours stored with a negative change, as sent: a meter that was
    # swapped or reset in a way Home Assistant did not see as a reset.
    negative_change: int = 0
    refused_not_history: int = 0
    refused_tag_limit: int = 0
    refused_unit_mismatch: int = 0
    refused_invalid: int = 0
    refused_unsupported: int = 0
    refused_hours_allowance: int = 0
    refused_unwritable: int = 0
    refused_tags: list[str] = field(default_factory=list)
    # Per tag in the batch: an hour must END by this to be imported - the
    # import's start, or the tag's first reading or first import, if earlier.
    # The readings endpoint's name for the same line.
    importable_before: dict[str, datetime] = field(default_factory=dict)
    # The first hour Tag Historian keeps; older hours are skipped.
    oldest_allowed: datetime | None = None
    # While the hourly history allowance is used up: when more goes in.
    resumes_at: datetime | None = None
    message: str = ""
    # Why a 503 asked for the batch again; "busy" and "account_busy" are
    # turns, not trouble (importer._TURN_REASONS).
    reason: str = ""

    @property
    def refused(self) -> int:
        return (
            self.refused_not_history
            + self.refused_tag_limit
            + self.refused_unit_mismatch
            + self.refused_invalid
            + self.refused_unsupported
            + self.refused_hours_allowance
            + self.refused_unwritable
        )

    @property
    def skipped(self) -> int:
        """Hours this batch did not store, whatever the reason."""
        return self.duplicates + self.outside_retention + self.refused


class TagHistorianImportRefused(TagHistorianError):
    """An import could not be started: the allowance is spent, or the account cannot write.

    Carries the API's own sentence, which says which and what to do about it.
    """

    def __init__(self, outcome: ImportOutcome, message: str) -> None:
        super().__init__(message)
        self.outcome = outcome
        self.message = message


class LineageOutcome(Enum):
    """What Tag Historian said to a rename or a merge of a tag.

    In the vocabulary of what this integration does next, like
    :class:`WriteOutcome`. The API's own answers are richer (TagsController's
    ``code``); only the ones that change what happens here get a value of
    their own.
    """

    DONE = "done"  # 200 - done now, or by an earlier request
    TARGET_EXISTS = "target_exists"  # 409 - the new name is a tag already: merge
    NOT_FOUND = "not_found"  # 404 - no tag under the old name: nothing to carry
    REFUSED = "refused"  # anything else refused - the message says why
    UNAVAILABLE = "unavailable"  # 503, or no answer - ask again later


@dataclass
class LineageResult:
    """The classified answer to a rename or a merge."""

    outcome: LineageOutcome
    message: str = ""


@dataclass(frozen=True)
class QuotaSnapshot:
    """Everything the UI is allowed to say about plans and limits.

    Every field here comes from ``GET /api/usage/limits`` and
    ``GET /api/usage/daily``, which the API's own comment describes as reading
    "the canonical Quotas configuration - the same source
    QuotaEnforcementMiddleware enforces". Nothing in this integration invents a
    limit, and the ``tier`` field is not even parsed: the enum spellings are one
    step out of line with the names on the pricing page, so rendering one would
    tell every customer they are on a plan by a name the page does not sell.
    """

    tag_limit: int
    current_tag_count: int
    measurements_per_day_limit: int
    measurements_today: int
    storage_limit_bytes: int
    current_storage_bytes: int

    @property
    def tags_available(self) -> int:
        """Tag slots still free on this account, never below zero."""
        return max(0, self.tag_limit - self.current_tag_count)


class TagHistorianClient:
    """Calls Tag Historian on behalf of one config entry."""

    def __init__(
        self,
        session: aiohttp.ClientSession,
        host: str,
        api_key: str,
        *,
        answer_within: float = HTTP_TIMEOUT_SECONDS,
    ) -> None:
        """``answer_within`` bounds each read (``_get_json``) once connected.

        The screens pass DIALOG_TIMEOUT_SECONDS, so a 503 the API takes a
        while to send is read as its answer rather than as a host that could
        not be reached; connecting is always bounded by HTTP_TIMEOUT_SECONDS.
        Writes, imports and proposals keep HTTP_TIMEOUT_SECONDS whatever this
        says: the forwarder's bounds are built on it.
        """
        self._session = session
        self._base = self._normalise_host(host)
        self._api_key = api_key
        self._answer_within = answer_within

    @staticmethod
    def _normalise_host(host: str) -> URL:
        """Accept what a user is likely to paste.

        The Home Assistant guide (taghistorian.com/docs/home-assistant) has to
        tell YAML users "host only - no https://" because the built-in
        integration builds the URL itself. Nobody reads
        that twice, so accept both spellings here and normalise, rather than
        making a pasted "https://api.taghistorian.com" a setup error.
        """
        host = host.strip().rstrip("/")
        if "://" not in host:
            host = f"https://{host}"
        return URL(host)

    @property
    def host(self) -> str:
        return self._base.host or ""

    def _headers(self) -> dict[str, str]:
        # 'Token <key>' is the InfluxDB v2 spelling and the one the write
        # endpoint is written against; the same key works as X-API-Key on the
        # JSON endpoints, so one header covers every call in this file.
        return {"Authorization": f"Token {self._api_key}"}

    async def _get_json(self, path: str, **params: Any) -> dict[str, Any]:
        try:
            async with self._session.get(
                self._base.join(URL(path)).update_query(params),
                headers=self._headers(),
                timeout=aiohttp.ClientTimeout(
                    total=self._answer_within, connect=HTTP_TIMEOUT_SECONDS
                ),
            ) as response:
                if response.status == 401:
                    raise TagHistorianAuthError(path)
                if response.status == 403:
                    raise TagHistorianPermissionError(path)
                if response.status == 503:
                    raise await self._unavailable(path, response)
                if response.status >= 400:
                    raise TagHistorianConnectionError(
                        f"{path} answered {response.status}"
                    )
                return await response.json()
        except (TimeoutError, aiohttp.ClientError) as err:
            raise TagHistorianConnectionError(str(err)) from err

    async def async_validate(self) -> dict[str, Any]:
        """Prove the key works and return the account it belongs to.

        One call does three jobs: it proves the credential, it yields the
        stable customer id the config entry keys itself on, and it reports
        whether the account is in a state that can still write.
        """
        return await self._get_json("/api/customers/me")

    async def async_get_quota(self) -> QuotaSnapshot:
        """Read the account's limits and today's usage.

        Two calls, because ``/limits`` carries the daily LIMIT but not the
        daily COUNT. Both endpoints sit under ``/api/usage``, which
        QuotaEnforcementMiddleware exempts from enforcement - so an account
        that is already over quota can still be told by how much.
        """
        limits = await self._get_json("/api/usage/limits")
        daily = await self._get_json("/api/usage/daily", days=1)

        days = daily.get("dailyUsage") or []
        today = int(days[-1].get("measurementCount", 0)) if days else 0

        return QuotaSnapshot(
            tag_limit=int(limits.get("tagLimit", 0)),
            current_tag_count=int(limits.get("currentTagCount", 0)),
            measurements_per_day_limit=int(limits.get("measurementsPerDayLimit", 0)),
            measurements_today=today,
            storage_limit_bytes=int(limits.get("storageLimitBytes", 0)),
            current_storage_bytes=int(limits.get("currentStorageBytes", 0)),
        )

    async def async_get_tag_names(self) -> set[str]:
        """Names that already exist on the account: its tags and their former names.

        Lets the selection screens say how many of the chosen entities are
        genuinely NEW tags rather than assuming all of them are - which is the
        difference between "you need 12 free slots" and "you need 47". A
        former name (a tag renamed or merged away from it) counts as existing:
        readings sent under it are stored in the tag it belongs to now, so
        selecting it costs no slot.
        """
        return set(await self.async_get_tag_index())

    async def async_get_tag_index(self) -> dict[str, str]:
        """Every name on the account to the tag it belongs to now.

        A tag maps to itself; each of its ``formerNames`` maps to it. The
        rename handling needs the difference: an entity's old id that is a
        former name of its new id has been renamed already (in Explorer, or by
        a request whose answer never arrived), and one that is no name at all
        never stored anything. The endpoint clamps ``take`` to 1000,
        comfortably above every tier's tag limit, so one page is always the
        whole list.
        """
        payload = await self._get_json("/api/tags", take=1000)
        index: dict[str, str] = {}
        for tag in payload.get("tags", []):
            if not isinstance(tag, dict) or not tag.get("tagName"):
                continue
            name = str(tag["tagName"])
            index[name] = name
            for former in tag.get("formerNames") or []:
                if isinstance(former, str) and former:
                    index.setdefault(former, name)
        return index

    async def async_rename_tag(self, old: str, new: str) -> LineageResult:
        """Give the tag ``old`` the name ``new``, keeping all of its history.

        ``POST /api/tags/rename``. Nothing is copied on the server: the history
        stays where it is stored and reads under the new name, and readings
        still sent under the old one are stored in the renamed tag. A name
        another tag has already is refused with ``target_exists`` - that is a
        merge (:meth:`async_merge_tags`). Asking again for a rename that is
        done answers 200, so a retry is always safe.
        """
        return await self._post_lineage("/api/tags/rename", {"from": old, "to": new})

    async def async_merge_tags(self, source: str, target: str) -> LineageResult:
        """Make ``source``'s history part of ``target``'s (``POST /api/tags/merge``)."""
        return await self._post_lineage(
            "/api/tags/merge", {"source": source, "target": target}
        )

    async def _post_lineage(self, path: str, body: dict[str, str]) -> LineageResult:
        try:
            async with self._session.post(
                self._base.join(URL(path)),
                headers=self._headers(),
                json=body,
                timeout=aiohttp.ClientTimeout(total=HTTP_TIMEOUT_SECONDS),
            ) as response:
                if response.status == 200:
                    return LineageResult(LineageOutcome.DONE)
                if response.status == 401:
                    raise TagHistorianAuthError(path)

                error = await self._read_error_body(response)
                code = str(error.get("code") or "")
                # ``details`` is the sentence written for a person - what
                # happened and what to do about it. ``error`` is its title.
                message = str(
                    error.get("details") or error.get("error") or f"{path} answered {response.status}"
                )

                if response.status == 409 and code == "target_exists":
                    return LineageResult(LineageOutcome.TARGET_EXISTS, message)
                if response.status == 404 and code == "not_found":
                    return LineageResult(LineageOutcome.NOT_FOUND, message)
                if response.status == 429 or response.status >= 500:
                    # Rate limited, the deploy handover or a key that could
                    # not be checked just now (503), or a gateway
                    # that lost the backend for a moment (502/504): all of
                    # them pass. A REFUSED here used to become a permanent
                    # card quoting "answered 502" for a rename that would
                    # have gone through a minute later.
                    return LineageResult(LineageOutcome.UNAVAILABLE, message)
                if response.status == 404 and not code:
                    # No such route: a Tag Historian older than this
                    # integration. The bare "answered 404" told nobody that.
                    message = (
                        "This Tag Historian cannot rename or merge tags yet. Nothing was "
                        "changed; the readings go on into the old tag."
                    )
                elif response.status == 403 and not error:
                    message = (
                        "The API key this integration uses may not rename or merge tags. "
                        "A key with the Write scope can."
                    )
                return LineageResult(LineageOutcome.REFUSED, message)
        except (TimeoutError, aiohttp.ClientError) as err:
            return LineageResult(LineageOutcome.UNAVAILABLE, str(err))

    async def async_propose_place(self, lat: float, lon: float) -> bool | None:
        """Propose the account's coarse place (``POST /api/account/settings/proposal``).

        A proposal is taken only while the account has no place AND has never
        had one removed: a place an Admin set in Settings, one proposed
        before, and a place an Admin removed all stand - a removed place stays
        removed, and no proposal fills it again. Sending it again is therefore
        harmless. True when the API answered, and None when it could not be
        asked or does not know the endpoint (a Tag Historian older than the
        weather) - nothing here depends on it, so neither is an error.
        """
        return await self._propose({"place": {"lat": lat, "lon": lon}, "source": "ha"}, "place")

    async def async_propose_time_zone(self, time_zone: str) -> bool | None:
        """Propose the account's time zone (``POST /api/account/settings/proposal``).

        Taken only while nobody has chosen one: neither an Admin in Settings,
        nor anybody for the owner's email channel under Alarms, nor an earlier
        proposal. The zone is what Tag Historian's increases per day and
        month, the Monday summary and alert times are in. True when the API
        answered, None when it could not be asked or is older than account
        settings - as for the place, neither is an error.
        """
        return await self._propose({"timeZone": time_zone, "source": "ha"}, "time zone")

    async def _propose(self, body: dict[str, Any], what: str) -> bool | None:
        path = "/api/account/settings/proposal"
        try:
            async with self._session.post(
                self._base.join(URL(path)),
                headers=self._headers(),
                json=body,
                timeout=aiohttp.ClientTimeout(total=HTTP_TIMEOUT_SECONDS),
            ) as response:
                if response.status == 401:
                    raise TagHistorianAuthError(path)
                if response.status >= 400:
                    LOGGER.debug("%s answered %s; the %s was not proposed", path, response.status, what)
                    return None
                return True
        except (TimeoutError, aiohttp.ClientError) as err:
            LOGGER.debug("%s could not be reached (%s); the %s was not proposed", path, err, what)
            return None

    async def async_get_import_allowance(self) -> ImportAllowance:
        """How much history this account may still import, and where the window starts."""
        return ImportAllowance.from_json(await self._get_json("/api/import/allowance"))

    async def async_start_import(
        self, source: str = "home-assistant", *, hours: bool = False
    ) -> ImportSession:
        """Start an import. Its readings must be older than ``started_at``.

        Raises :class:`TagHistorianImportRefused` when the account cannot
        import at all right now - the allowance spent (402), the storage cap
        reached (402), or an account that may not write (403).

        ``hours``: the import brings hourly statistics too. The readings
        allowance does not come back once it is spent, and the hours have one
        of their own, so such an import is started even then - its readings
        are refused instead, batch by batch, and ``allowance.remaining`` says
        so from the start.
        """
        body: dict[str, Any] = {"source": source}
        if hours:
            body["hours"] = True
        try:
            async with self._session.post(
                self._base.join(URL("/api/import")),
                headers=self._headers(),
                json=body,
                timeout=aiohttp.ClientTimeout(total=HTTP_TIMEOUT_SECONDS),
            ) as response:
                if response.status == 401:
                    raise TagHistorianAuthError("import")
                if response.status in (402, 403):
                    body = await self._read_error_body(response)
                    raise TagHistorianImportRefused(
                        _classify_import_refusal(response.status, body),
                        str(body.get("message") or body.get("error") or ""),
                    )
                if response.status == 503:
                    raise await self._unavailable("/api/import", response)
                if response.status != 201:
                    raise TagHistorianConnectionError(f"/api/import answered {response.status}")
                payload = await response.json()
        except (TimeoutError, aiohttp.ClientError) as err:
            raise TagHistorianConnectionError(str(err)) from err

        return ImportSession(
            import_id=str(payload["importId"]),
            started_at=_parse_time(payload.get("startedAt")) or datetime.now(UTC),
            window_start=_parse_time(payload.get("windowStart")) or datetime.now(UTC),
            max_readings_per_batch=int(payload.get("maxReadingsPerBatch", 0)),
            allowance=ImportAllowance.from_json(
                payload.get("allowance") or {}, payload.get("windowStart")
            ),
        )

    async def async_import_batch(
        self, import_id: str, readings: list[dict[str, Any]]
    ) -> ImportBatchResult:
        """Send one batch of history and classify the answer.

        Raises :class:`TagHistorianAuthError` on a 401 and
        :class:`TagHistorianConnectionError` when nothing usable came back;
        everything the endpoint says on purpose is an :class:`ImportOutcome`.
        """
        try:
            async with self._session.post(
                self._base.join(URL(f"/api/import/{import_id}/readings")),
                headers=self._headers(),
                json={"readings": readings},
                timeout=aiohttp.ClientTimeout(total=IMPORT_TIMEOUT_SECONDS),
            ) as response:
                if response.status == 401:
                    raise TagHistorianAuthError("import")
                body = await self._read_error_body(response)
                return _classify_import(response, body)
        except (TimeoutError, aiohttp.ClientError) as err:
            raise TagHistorianConnectionError(str(err)) from err

    async def async_import_hours(self, import_id: str, gzipped: bytes) -> HoursBatchResult:
        """Send one batch of hourly statistics, already gzipped, and classify the answer.

        ``POST /api/import/{id}/hours`` takes at most 4 MB once decompressed
        and 5 000 hours, and gzip is what keeps years of hours off a home
        connection's upload: the body is mostly the same keys over and over.
        The body is compressed by the caller, off the event loop
        (statistics.py). Raises like :meth:`async_import_batch`.
        """
        try:
            async with self._session.post(
                self._base.join(URL(f"/api/import/{import_id}/hours")),
                headers={
                    **self._headers(),
                    "Content-Type": "application/json",
                    "Content-Encoding": "gzip",
                },
                data=gzipped,
                timeout=aiohttp.ClientTimeout(total=IMPORT_TIMEOUT_SECONDS),
            ) as response:
                if response.status == 401:
                    raise TagHistorianAuthError("import")
                # Read first, as it came: only a body that is empty on the
                # wire means the route is not there (_classify_hours_import).
                # aiohttp keeps it, so the JSON is read from the same bytes.
                empty = not (await response.read()).strip()
                body = await self._read_error_body(response)
                return _classify_hours_import(response, body, empty=empty)
        except (TimeoutError, aiohttp.ClientError) as err:
            raise TagHistorianConnectionError(str(err)) from err

    async def async_write(self, body: str) -> WriteResult:
        """POST line protocol and classify the answer.

        ``precision=s`` because Home Assistant state changes are timestamped to
        the second and sending nanoseconds would be nine digits of invented
        precision on every line.
        """
        try:
            async with self._session.post(
                self._base.join(URL("/api/v2/write")).update_query(
                    {"precision": "s"}
                ),
                headers={**self._headers(), "Content-Type": "text/plain"},
                data=body.encode("utf-8"),
                timeout=aiohttp.ClientTimeout(total=HTTP_TIMEOUT_SECONDS),
            ) as response:
                return await self._classify(response)
        except (TimeoutError, aiohttp.ClientError) as err:
            raise TagHistorianConnectionError(str(err)) from err

    async def _classify(self, response: aiohttp.ClientResponse) -> WriteResult:
        if response.status == 204:
            return WriteResult(WriteOutcome.OK)

        if response.status == 401:
            raise TagHistorianAuthError("write")

        body = await self._read_error_body(response)
        message = str(body.get("message") or body.get("error") or "")

        if response.status == 422:
            # Partial success. Points for tags that already exist WERE written;
            # only points that would have created a new tag beyond the quota
            # were dropped. Never retry - see WriteResult.retryable.
            return WriteResult(
                WriteOutcome.PARTIAL_TAG_QUOTA,
                skipped_tags=_parse_skipped_tags(message),
                dropped_points=_parse_dropped_points(message),
                message=message,
            )

        if response.status == 429:
            return WriteResult(
                _classify_429(message),
                retry_after=_parse_retry_after(response),
                message=message,
            )

        if response.status == 503:
            return WriteResult(
                WriteOutcome.UNAVAILABLE,
                retry_after=_parse_retry_after(response),
                message=message,
            )

        if response.status in (402, 403):
            # 403 is an unverified email address or a pending erasure; 402 is
            # the storage quota. All three are permanent until a person does
            # something, and all three arrive with a body already written for
            # that person to read.
            return WriteResult(WriteOutcome.FORBIDDEN, message=message)

        # 400 and 413 mean this integration built a body the endpoint cannot
        # take. That is a bug here, not a user problem, so it is logged loudly
        # and the batch is dropped rather than retried into a loop.
        LOGGER.error(
            "Tag Historian refused a write with %s: %s", response.status, message
        )
        return WriteResult(WriteOutcome.REJECTED, message=message)

    @classmethod
    async def _unavailable(
        cls, what: str, response: aiohttp.ClientResponse
    ) -> TagHistorianUnavailableError:
        """The 503 as an error that keeps its reason and its Retry-After."""
        body = await cls._read_error_body(response)
        return TagHistorianUnavailableError(
            what,
            reason=str(body.get("reason") or ""),
            retry_after=_parse_retry_after(response),
        )

    @staticmethod
    async def _read_error_body(response: aiohttp.ClientResponse) -> dict[str, Any]:
        try:
            payload = await response.json(content_type=None)
        except (aiohttp.ClientError, ValueError):
            return {}
        return payload if isinstance(payload, dict) else {}


# QuotaEnforcementMiddleware's own words when the DAILY reading allowance is
# spent. The controller's 429 says "write buffer fair-share exceeded" instead,
# and both carry code "too many requests", so the message is the only thing
# that tells them apart.
_DAILY_QUOTA_MARKER = "daily measurement quota"


def _classify_429(message: str) -> WriteOutcome:
    """Tell the daily quota apart from our own write-buffer back-pressure.

    Two entirely different 429s share the status code:

    * ``QuotaEnforcementMiddleware`` - the account has spent its reading
      allowance for the day. ``Retry-After`` counts down to UTC midnight, and
      the honest things to say are "wait" and "a bigger plan".
    * ``InfluxWriteController.RejectWrite`` - the write buffer is refusing this
      customer's fair share right now. ``MemoryBufferService`` sets
      ``Retry-After`` to five seconds plus jitter. It is our saturation, it
      clears itself, and it has nothing to do with what anybody is paying.

    The default is deliberately back-pressure. Reading an unfamiliar 429 as a
    quota problem is how a five-second hiccup becomes a Repairs card telling a
    customer their allowance is gone and offering them a larger plan - our own
    code inventing a reason to charge more. Being wrong the other way costs a
    card that says "too busy" during a real quota pause, and the pause itself
    still happens, because Retry-After is honoured either way.
    """
    if _DAILY_QUOTA_MARKER in message.casefold():
        return WriteOutcome.DAILY_QUOTA
    return WriteOutcome.BACK_PRESSURE


def _parse_retry_after(response: aiohttp.ClientResponse) -> int:
    """Read ``Retry-After`` as RFC 9110 delta-seconds.

    The endpoint always sends the integer form - on the daily-quota 429 it is
    the seconds to the UTC midnight when the quota resets, on the
    back-pressure 429 and the write buffer's 503 it is the buffer's own
    estimate, and on the 503 for a key that could not be checked just now it
    is 30 to 45 seconds. Honouring it is half the reason this component
    exists. Home Assistant's built-in influxdb integration never looks at the
    header: set up for InfluxDB 1.x it sends a refused batch again only when
    ``max_retries`` is set (it is 0 unless configured), 20 seconds apart,
    and then drops it; set up for 2.x it writes without waiting for the
    answer, so a refused batch is simply gone. Either way a day over quota
    loses every reading until midnight, and a minute of 503s loses that
    minute.
    """
    raw = response.headers.get("Retry-After")
    if not raw:
        return 0
    try:
        return max(0, int(raw.strip()))
    except ValueError:
        LOGGER.debug("Ignoring non-integer Retry-After %r", raw)
        return 0


_SKIPPED_TAGS_RE = re.compile(r"new tags: ([^)]*)\)")

# ``InfluxWriteController`` opens the 422 body with the number that matters:
# "{SkippedNewTagPoints} points dropped: tag quota exceeded (new tags: ...)".
# SkippedNewTagPoints counts POINTS. The tag names after it are DISTINCT
# NAMES, and two refused entities contributing thirty lines each is 2 names
# and 60 points - so counting the names is off by a factor of thirty and every
# one of those points gets reported as delivered.
_DROPPED_POINTS_RE = re.compile(r"^\s*(\d+)\s+points dropped\b", re.IGNORECASE)


def _parse_skipped_tags(message: str) -> list[str]:
    """Pull the refused tag names out of the 422 body.

    The names live inside an English sentence
    ("... tag quota exceeded (new tags: a, b, c); ...") rather than in a field
    of their own, so this is prose-scraping and it is fragile by construction.
    It fails SOFT: an empty list still raises the repair issue, just without
    naming the entities. Adding a ``skippedTags`` array to the 422 body would
    retire this function.
    """
    match = _SKIPPED_TAGS_RE.search(message)
    if not match:
        return []
    return [name.strip() for name in match.group(1).split(",") if name.strip()]


def _parse_dropped_points(message: str) -> int | None:
    """How many POINTS the 422 says it dropped, or ``None`` if it did not say.

    ``None`` rather than 0. A zero here would mean "the whole batch landed",
    which is the one thing a 422 never means, and the caller has to be able to
    tell "the API said none" from "the API did not tell us".
    """
    match = _DROPPED_POINTS_RE.match(message)
    if not match:
        LOGGER.warning(
            "Tag Historian answered 422 without a leading point count: %r. "
            "Treating the whole batch as refused - see _parse_dropped_points",
            message,
        )
        return None
    return int(match.group(1))


def _parse_time(value: Any) -> datetime | None:
    """An ISO 8601 instant from the API, as an aware UTC datetime, or ``None``."""
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _classify_import_refusal(status: int, body: dict[str, Any]) -> ImportOutcome:
    """402 is two different things on the import path; the body says which.

    ``ImportController`` answers "Import allowance used up" when the account
    has imported everything its plan allows; ``QuotaEnforcementMiddleware``
    answers "Storage quota exceeded" for the storage cap, which holds for
    imports as it does for live data. 403 is the account itself.
    """
    if status == 403:
        return ImportOutcome.FORBIDDEN
    if "storage" in str(body.get("error") or "").casefold():
        return ImportOutcome.STORAGE_FULL
    return ImportOutcome.ALLOWANCE_SPENT


def _import_status(
    response: aiohttp.ClientResponse, body: dict[str, Any], message: str
) -> tuple[ImportOutcome, int]:
    """What a refusal from either batch endpoint of an import means: the outcome, and how long to wait.

    One reading of the statuses for readings and hours alike, so the two
    phases of an import stop, wait and retry for the same answers.
    """
    status = response.status

    if status in (402, 403):
        return _classify_import_refusal(status, body), 0

    if status == 404:
        if not body:
            # "No such import" is a 404 with the API's JSON that says so. One
            # without it - an HTML page from a proxy or the ingress in front
            # of a stopped API - is not Tag Historian's answer at all, and is
            # retried as an unreachable one is rather than ending the import.
            raise TagHistorianConnectionError("import answered 404 without Tag Historian's answer")
        return ImportOutcome.UNKNOWN_IMPORT, 0

    if status in (429, 503):
        return ImportOutcome.RETRY, _parse_retry_after(response)

    if status in (400, 413):
        LOGGER.error("Tag Historian refused an import batch with %s: %s", status, message)
        return ImportOutcome.REJECTED, 0

    if status in (502, 504):
        # The gateway lost the API for a moment: as good as unreachable.
        raise TagHistorianConnectionError(f"import answered {status}")
    # A 500 and anything else unexpected: nothing is known to be stored, and
    # the endpoint skips what it already has, so it is retried like a socket
    # error rather than ending the import - but Tag Historian did answer, and
    # an import that meets it every time is not carried on by itself.
    raise TagHistorianServerError(f"import answered {status}")


def _classify_import(response: aiohttp.ClientResponse, body: dict[str, Any]) -> ImportBatchResult:
    status = response.status
    message = str(body.get("message") or body.get("error") or "")

    if status == 200:
        refused = body.get("refused") or {}
        allowance = body.get("allowance") or {}
        importable = {
            str(tag): parsed
            for tag, value in (body.get("importableBefore") or {}).items()
            if (parsed := _parse_time(value)) is not None
        }
        return ImportBatchResult(
            ImportOutcome.TAKEN,
            stored=int(body.get("stored", 0)),
            compressed=int(body.get("compressed", 0)),
            duplicates=int(body.get("duplicates", 0)),
            outside_window=int(body.get("outsideWindow", 0)),
            refused_not_history=int(refused.get("notHistory", 0)),
            refused_tag_limit=int(refused.get("tagLimit", 0)),
            refused_allowance=int(refused.get("allowance", 0)),
            refused_storage=int(refused.get("storage", 0)),
            refused_tags=[str(t) for t in body.get("refusedTags") or []],
            importable_before=importable,
            allowance_remaining=(
                int(allowance["remaining"]) if "remaining" in allowance else None
            ),
            message=message,
        )

    outcome, retry_after = _import_status(response, body, message)
    return ImportBatchResult(outcome, retry_after=retry_after, message=message)


def _count(source: dict[str, Any], key: str) -> int:
    """A count from an answer, 0 when it is absent or not a number."""
    try:
        return int(source.get(key) or 0)
    except (TypeError, ValueError):
        return 0


def _classify_hours_import(
    response: aiohttp.ClientResponse, body: dict[str, Any], *, empty: bool = False
) -> HoursBatchResult:
    """The answer to one hours batch: the import endpoint's statuses, and two of its own.

    * 402 with ``code: hours_allowance`` is the hourly history allowance - the
      plan's tag limit times ten years of hours per rolling 30 days - and not
      the readings' allowance. It ends the statistics phase only, and comes
      back by itself at ``resumesAt``.
    * 404 with nothing at all on the wire (``empty``) is a route the server
      does not have: a Tag Historian from before the hours import. That ends
      the statistics phase and leaves the readings to theirs. "No such import"
      is a 404 with the API's JSON that says so, and ends the session as on
      the readings endpoint. Any other 404 without the API's JSON is read as
      on the readings endpoint (_import_status): not Tag Historian's answer,
      and retried as an unreachable one is. Only the one that is empty on the
      wire is the hours endpoint's own.

    A 503 can carry ``stored``: some months went in and some could not yet
    (the server's deploy, a file a reader held). Sent again, the batch stores
    the rest, and what went in now comes back as duplicates; the importer
    counts it from here. It also carries ``reason``, which tells the hours
    endpoint's turns (``busy``, ``account_busy``) from trouble: the importer
    waits for a turn differently (importer._TURN_REASONS).
    """
    status = response.status
    message = str(body.get("message") or body.get("error") or "")

    if status == 200:
        refused = body.get("refused") or {}
        importable = {
            str(tag): parsed
            for tag, value in (body.get("importableBefore") or {}).items()
            if (parsed := _parse_time(value)) is not None
        }
        return HoursBatchResult(
            ImportOutcome.TAKEN,
            stored=_count(body, "stored"),
            duplicates=_count(body, "duplicates"),
            outside_retention=_count(body, "outsideRetention"),
            negative_change=_count(body, "negativeChange"),
            refused_not_history=_count(refused, "notHistory"),
            refused_tag_limit=_count(refused, "tagLimit"),
            refused_unit_mismatch=_count(refused, "unitMismatch"),
            refused_invalid=_count(refused, "invalid"),
            refused_unsupported=_count(refused, "unsupported"),
            refused_hours_allowance=_count(refused, "hoursAllowance"),
            refused_unwritable=_count(refused, "unwritable"),
            refused_tags=[str(t) for t in body.get("refusedTags") or []],
            importable_before=importable,
            oldest_allowed=_parse_time(body.get("oldestAllowed")),
            resumes_at=_parse_time((body.get("hoursAllowance") or {}).get("resumesAt")),
            message=message,
        )

    if status == 402 and str(body.get("code") or "") == "hours_allowance":
        return HoursBatchResult(
            ImportOutcome.HOURS_ALLOWANCE_SPENT,
            resumes_at=_parse_time(body.get("resumesAt")),
            message=message,
        )

    if status == 404 and empty:
        return HoursBatchResult(ImportOutcome.NOT_SUPPORTED, message=message)

    outcome, retry_after = _import_status(response, body, message)
    retry = outcome is ImportOutcome.RETRY
    return HoursBatchResult(
        outcome,
        retry_after=retry_after,
        stored=_count(body, "stored") if retry else 0,
        message=message,
        reason=str(body.get("reason") or "") if retry else "",
    )
