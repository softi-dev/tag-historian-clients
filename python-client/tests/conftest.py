from __future__ import annotations

import pytest

from taghistorian import RetryConfig, TagHistorianClient

BASE_URL = "https://api.test.local"


@pytest.fixture
def sleeps() -> list[float]:
    """Captures every delay the client's retry loop would have slept for,
    without actually blocking the test suite."""
    return []


@pytest.fixture
def make_client(sleeps):
    """Factory for a client pointed at BASE_URL with a fast, spy-able retry
    policy. Tests that care about retry counts/timing pass their own
    ``max_retries``; everyone else gets a small default so a
    forgotten-mock bug fails fast instead of hanging.
    """

    def _make(max_retries: int = 3, backoff_base: float = 1.0) -> TagHistorianClient:
        retry = RetryConfig(
            max_retries=max_retries,
            backoff_base=backoff_base,
            sleep=sleeps.append,
        )
        return TagHistorianClient(api_key="test-key", base_url=BASE_URL, retry=retry)

    return _make
