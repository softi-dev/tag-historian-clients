"""Retry/backoff policy for :class:`taghistorian.client.TagHistorianClient`.

Kept as its own small, callable-holding object (not hidden constants inside
the client) so a caller can see exactly what will happen on a 429/503/
network blip, tune it, or turn it off with ``RetryConfig(max_retries=0)`` -
rather than a hidden magic number baked into the request loop.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass


@dataclass
class RetryConfig:
    """How the client retries a 429, a 503, or a transient network error.

    Nothing else is retried: a 400/401/402/403/404, or a 5xx other than a
    503, means the request was looked at and refused on its merits (or
    failed in a way that sending the identical bytes again will not change).

    A 429 and a 503 are budgeted differently, because they are different
    answers. A 429 is a rate limit: the account is sending faster than it
    may, and a few retries at the server's ``Retry-After`` are the right
    amount of patience before the caller is told. A 503 is Tag Historian
    being briefly unavailable - its write buffer momentarily full, a deploy
    handing over, or an API key it could not check just now because of a
    moment's trouble on its side (``reason: "sign_in_unavailable"``
    in the body, with a ``Retry-After`` of 30 to 45 seconds). Those last a
    minute or two, and three retries at that ``Retry-After`` (90 to 135
    seconds) end about when such an outage does, with no margin, so a 503 is
    retried for as long as its waits fit in ``unavailable_budget`` instead of
    a count: a few minutes that ride it out.

    :param max_retries: How many extra attempts to make after the first one
        fails with a 429 or a transient network error. Default 3 (so up to 4
        requests total for one call). Kept small and explicit rather than
        "retry forever": a write is not always safe to repeat blindly on an
        ambiguous network failure (see the module docstring in
        ``client.py``), so a caller who wants different behaviour - none, or
        many more - sets this rather than fighting a hidden default. ``0``
        disables automatic retry entirely, a 503's included.
    :param backoff_base: Seconds to wait before the FIRST retry when the
        server did not send a ``Retry-After`` header (only network-level
        failures and, in principle, a 429/503 without the header, hit this -
        every 429/503 this API sends carries one). Doubles on each
        subsequent retry, capped at ``backoff_max``.
    :param backoff_max: Ceiling for the exponential backoff described above.
    :param sleep: The function called to wait between retries. Defaults to
        ``time.sleep``; overridable so tests (or a caller with their own
        event loop / pacing needs) don't have to actually block.
    :param unavailable_budget: Seconds one call may spend waiting out 503s,
        all of them together. Default 300 (five minutes): long enough to ride
        out a short outage at the server's own ``Retry-After``, short enough
        that a longer one reaches the caller as
        :class:`~taghistorian.exceptions.TagHistorianUnavailableError`
        rather than a call that never returns. A wait that would go past the
        budget is not started; the error is raised instead. A
        ``Retry-After`` under a second (a proxy's ``0``) is read as no
        header, so the backoff applies; and each wait between 503s is at
        least one second, even when ``backoff_base`` is 0, so the budget
        always runs out and the call always returns. The budget counts the
        waits only, not the requests between them.
    """

    max_retries: int = 3
    backoff_base: float = 1.0
    backoff_max: float = 30.0
    sleep: Callable[[float], None] = time.sleep
    unavailable_budget: float = 300.0

    def backoff_delay(self, attempt: int) -> float:
        """Delay before retry number ``attempt`` (1-indexed) when no
        ``Retry-After`` header was available to honour instead."""
        # The exponent is capped: a long unavailable_budget at the one-second
        # floor (backoff_base 0, or a proxy's Retry-After 0) can reach retry
        # number 1025, where 2 ** 1024 no longer converts to a float and the
        # multiplication would raise OverflowError instead of waiting.
        delay = self.backoff_base * (2 ** min(attempt - 1, 62))
        return min(delay, self.backoff_max)
