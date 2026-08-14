"""Shared fixtures.

``enable_custom_integrations`` is what makes Home Assistant look in
``custom_components/`` at all; without it every test that sets up an entry
fails with "integration not found", which reads like a packaging problem
rather than a missing fixture.
"""

from __future__ import annotations

import asyncio
import sys
from unittest.mock import AsyncMock

import pytest
import pytest_socket

from custom_components.tag_historian.api import QuotaSnapshot

pytest_plugins = "pytest_homeassistant_custom_component"


if sys.platform == "win32":
    # Let the suite run on Windows, where CI does not but the author does.
    #
    # pytest-homeassistant-custom-component blocks sockets with
    # allow_unix_socket=True. On Linux that still permits the AF_UNIX
    # socketpair asyncio creates for its own self-pipe. Windows has no AF_UNIX
    # socketpair, so ProactorEventLoop falls back to an AF_INET pair and the
    # guard fires while the event loop is being built - before any test body
    # runs, with an error that looks nothing like its cause.
    #
    # There is no hook ordering that fits: their call happens inside the same
    # pytest_runtest_setup chain that resolves fixtures, so "after them but
    # before the fixtures" is not a position a hook can occupy. Neutralising
    # the call is the remaining option.
    #
    # What this costs: on Windows a test that reached for the real network
    # would not be caught here. CI runs ubuntu-latest, where the guard stays
    # on, so nothing merges without that check having run.
    pytest_socket.disable_socket = lambda **_kwargs: None

    # And the same story one layer down: aiodns, which aiohttp's resolver
    # pulls in, refuses to run on a ProactorEventLoop at all. Home Assistant's
    # own policy subclasses the platform default, so pointing its loop factory
    # at the selector loop is the one-line equivalent of what Home Assistant
    # would be doing anyway if it supported Windows.
    asyncio.DefaultEventLoopPolicy._loop_factory = asyncio.SelectorEventLoop

# Deliberately NOT the numbers from any real plan. These are a stubbed API
# response, and if a test ever passes only because the fixture happens to match
# production quotas, that test is asserting the wrong thing.
STUB_QUOTA = QuotaSnapshot(
    tag_limit=12,
    current_tag_count=2,
    measurements_per_day_limit=120000,
    measurements_today=1234,
    storage_limit_bytes=1_000_000,
    current_storage_bytes=1000,
)

STUB_ACCOUNT = {
    "customerId": "11111111-2222-3333-4444-555555555555",
    "name": "Test House",
    "isActive": True,
    "emailVerified": True,
}


@pytest.fixture(autouse=True)
def auto_enable_custom_integrations(enable_custom_integrations):
    """Load custom_components/ for every test in this suite."""
    yield


def make_client(
    account: dict | None = None,
    quota: QuotaSnapshot | None = None,
    tags: set[str] | None = None,
) -> AsyncMock:
    """A stand-in for TagHistorianClient with everything wired to succeed."""
    client = AsyncMock()
    client.async_validate.return_value = STUB_ACCOUNT if account is None else account
    client.async_get_quota.return_value = STUB_QUOTA if quota is None else quota
    client.async_get_tag_names.return_value = set() if tags is None else tags
    return client
