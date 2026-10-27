"""Exception hierarchy for the Tag Historian client.

Four distinct failure shapes, each needing a different response from the
caller, so they are four distinct catchable types rather than one bag:

- :class:`TagHistorianAPIError` - the server looked at the request and said
  no (a 4xx other than 429, or a 5xx other than 503). Retrying the exact
  same request will not help; the caller has to change something.
- :class:`TagHistorianRateLimitError` - a 429 that the client's own retry
  loop gave up on after honouring every ``Retry-After`` it was given. The
  account is sending faster than it may; the request was never rejected on
  its merits, only postponed past the retry budget, and the right response
  is "send less often, or later", not "fix the request".
- :class:`TagHistorianUnavailableError` - a 503 that outlasted the client's
  retry budget for it. Tag Historian was briefly unable to serve the
  request - its write buffer full for a moment, or an API key it could not
  check just now - which is not a rate limit and says nothing about the
  request or the key. The right response is "try again in a few minutes".
- :class:`TagHistorianConnectionError` - the request never got a response at
  all (DNS failure, connection refused, timeout with no reply). There is no
  status code and no server-authored message to show, because the server
  was never reached.
"""

from __future__ import annotations


class TagHistorianError(Exception):
    """Base class for every error this client raises.

    :param message: Human-readable description of what went wrong.
    :param status_code: The HTTP status code that caused this error, or
        ``None`` when the failure happened before any response arrived
        (a :class:`TagHistorianConnectionError`).
    """

    def __init__(self, message: str, status_code: int | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.status_code = status_code

    def __str__(self) -> str:
        if self.status_code is not None:
            return f"[{self.status_code}] {self.message}"
        return self.message


class TagHistorianAPIError(TagHistorianError):
    """The server rejected the request outright.

    Covers every non-2xx response the client did not retry: 400 (bad
    request - e.g. an invalid tag name, or a validation failure carrying a
    ``violations`` list), 401/403 (auth), 402 (quota), 404, and any 5xx that
    is not a 503. A 429 raises :class:`TagHistorianRateLimitError` and a 503
    :class:`TagHistorianUnavailableError` once their retries are spent; both
    subclass this one.

    :param violations: The optional per-field violation list the API sends
        on some 400s from the tag-name validator. ``None`` on every other
        error - callers should treat its absence as "not applicable", not
        "empty list of problems".
    """

    def __init__(
        self,
        message: str,
        status_code: int,
        violations: list | None = None,
    ) -> None:
        super().__init__(message, status_code)
        self.violations = violations


class TagHistorianRateLimitError(TagHistorianAPIError):
    """Retries were exhausted against a 429 (rate limited) response.

    Subclasses :class:`TagHistorianAPIError` so a caller who only wants "did
    the server refuse this" can catch the parent, while a caller who wants
    to treat throttling differently from a hard rejection can catch this
    more specific type first.

    :param retry_after: The ``Retry-After`` value (seconds) from the last
        response that caused a retry, if the server sent one.
    :param attempts: How many requests were actually made, including the
        first one.
    """

    def __init__(
        self,
        message: str,
        status_code: int,
        retry_after: float | None,
        attempts: int,
    ) -> None:
        super().__init__(message, status_code)
        self.retry_after = retry_after
        self.attempts = attempts


class TagHistorianUnavailableError(TagHistorianAPIError):
    """Tag Historian answered 503 Service Unavailable for longer than the
    client's retry budget for it (``RetryConfig.unavailable_budget``).

    Not a rate limit, and not a refusal: Tag Historian could not serve the
    request just now. Its ``reason`` says why when the body names one -
    ``"sign_in_unavailable"`` when the API key or token could not be CHECKED
    for a moment (a key that was checked and rejected is a 401, never this),
    or the write path's own, such as a write buffer that is momentarily
    full. Every ``Retry-After`` was honoured on the way here; the right
    response is to try again in a few minutes, without changing the request
    or the key.

    Subclasses :class:`TagHistorianAPIError`, like
    :class:`TagHistorianRateLimitError`, so ``except TagHistorianAPIError``
    still catches it.

    :param retry_after: The ``Retry-After`` value (seconds) of the last 503,
        if the server sent one.
    :param attempts: How many requests were actually made, including the
        first one.
    :param reason: The body's ``reason``, or ``None`` when it had none.
    :param waited: Seconds this call spent waiting out 503s before giving
        up.
    """

    def __init__(
        self,
        message: str,
        status_code: int,
        retry_after: float | None,
        attempts: int,
        reason: str | None = None,
        waited: float = 0.0,
    ) -> None:
        super().__init__(message, status_code)
        self.retry_after = retry_after
        self.attempts = attempts
        self.reason = reason
        self.waited = waited


class TagHistorianConnectionError(TagHistorianError):
    """The server could not be reached at all - no HTTP response was ever
    received, after exhausting the configured retries (if any applied).

    ``status_code`` is always ``None`` here: there is no status to report
    when there was no response.
    """

    def __init__(self, message: str, attempts: int) -> None:
        super().__init__(message, status_code=None)
        self.attempts = attempts
