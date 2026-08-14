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
from enum import Enum
from typing import Any

import aiohttp
from yarl import URL

from .const import HTTP_TIMEOUT_SECONDS, LOGGER


class TagHistorianError(Exception):
    """Base error."""


class TagHistorianConnectionError(TagHistorianError):
    """The host did not answer, or answered with something unusable."""


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
        self, session: aiohttp.ClientSession, host: str, api_key: str
    ) -> None:
        self._session = session
        self._base = self._normalise_host(host)
        self._api_key = api_key

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
                timeout=aiohttp.ClientTimeout(total=HTTP_TIMEOUT_SECONDS),
            ) as response:
                if response.status == 401:
                    raise TagHistorianAuthError(path)
                if response.status == 403:
                    raise TagHistorianPermissionError(path)
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
        """Tag names that already exist on the account.

        Lets the selection screens say how many of the chosen entities are
        genuinely NEW tags rather than assuming all of them are - which is the
        difference between "you need 12 free slots" and "you need 47". The
        endpoint clamps ``take`` to 1000, comfortably above every tier's tag
        limit, so one page is always the whole list.
        """
        payload = await self._get_json("/api/tags", take=1000)
        return {
            tag["tagName"]
            for tag in payload.get("tags", [])
            if isinstance(tag, dict) and tag.get("tagName")
        }

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
    the seconds to the UTC midnight when the quota resets, and on the
    back-pressure 429 and the 503 it is the write buffer's own estimate.
    Honouring it is half the reason this component exists: the built-in
    influxdb integration has a fixed 20/60 second retry schedule and never
    looks at the header at all, so it spends a whole day re-sending into a
    quota that will not reset until midnight.
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
