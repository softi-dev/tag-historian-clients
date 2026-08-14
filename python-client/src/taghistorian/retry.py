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

    Nothing else is retried: a 400/401/402/403/404, or a 5xx other than the
    ingest-buffer-saturated 503, means the request was looked at and refused
    on its merits, and sending the identical bytes again will not change
    that answer.

    :param max_retries: How many extra attempts to make after the first one
        fails with a retryable error. Default 3 (so up to 4 requests total
        for one call). Kept small and explicit rather than "retry forever":
        a write is not always safe to repeat blindly on an ambiguous
        network failure (see the module docstring in ``client.py``), so a
        caller who wants different behaviour - none, or many more - sets
        this rather than fighting a hidden default. ``0`` disables
        automatic retry entirely.
    :param backoff_base: Seconds to wait before the FIRST retry when the
        server did not send a ``Retry-After`` header (only network-level
        failures and, in principle, a 429/503 without the header, hit this -
        every 429/503 this API actually sends does carry one, per
        ``Program.cs``'s ``RetryAfter`` handling). Doubles on each
        subsequent retry, capped at ``backoff_max``.
    :param backoff_max: Ceiling for the exponential backoff described above.
    :param sleep: The function called to wait between retries. Defaults to
        ``time.sleep``; overridable so tests (or a caller with their own
        event loop / pacing needs) don't have to actually block.
    """

    max_retries: int = 3
    backoff_base: float = 1.0
    backoff_max: float = 30.0
    sleep: Callable[[float], None] = time.sleep

    def backoff_delay(self, attempt: int) -> float:
        """Delay before retry number ``attempt`` (1-indexed) when no
        ``Retry-After`` header was available to honour instead."""
        delay = self.backoff_base * (2 ** (attempt - 1))
        return min(delay, self.backoff_max)
