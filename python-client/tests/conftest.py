from __future__ import annotations

import pytest

from taghistorian import RetryConfig, TagHistorianClient

BASE_URL = "https://api.test.local"

# Far more waits than any one call here should make (the default
# unavailable_budget of 300 s at the one-second floor is 300). A retry loop
# that never ends - a wait of 0 s spends no budget - fails the test at once
# instead of holding the run until the CI job is killed.
_MAX_SLEEPS = 1000


class _Sleeps(list):
    def append(self, delay: float) -> None:
        super().append(delay)
        if len(self) > _MAX_SLEEPS:
            raise AssertionError(
                f"more than {_MAX_SLEEPS} retry waits: a retry loop that never ends"
            )


@pytest.fixture
def sleeps() -> list[float]:
    """Captures every delay the client's retry loop would have slept for,
    without actually blocking the test suite, and stops a loop that would
    never end."""
    return _Sleeps()


@pytest.fixture
def make_client(sleeps):
    """Factory for a client pointed at BASE_URL with a fast, spy-able retry
    policy. Tests that care about retry counts/timing pass their own
    ``max_retries``; everyone else gets a small default so a
    forgotten-mock bug fails fast instead of hanging.
    """

    def _make(
        max_retries: int = 3,
        backoff_base: float = 1.0,
        unavailable_budget: float = 300.0,
    ) -> TagHistorianClient:
        retry = RetryConfig(
            max_retries=max_retries,
            backoff_base=backoff_base,
            sleep=sleeps.append,
            unavailable_budget=unavailable_budget,
        )
        return TagHistorianClient(api_key="test-key", base_url=BASE_URL, retry=retry)

    return _make
