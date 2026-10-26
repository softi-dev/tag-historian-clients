"""The client this server builds for itself at startup, and how long it waits."""

from __future__ import annotations

import io
import json

import requests

from taghistorian_mcp import __main__ as entry
from taghistorian_mcp.server import build_server

from .conftest import call_tool, make_config

# What the API answers when it could not CHECK a key for a moment.
_SIGN_IN_UNAVAILABLE = {
    "error": "Service Unavailable",
    "message": "Your sign-in could not be checked just now.",
    "traceId": "0HN-test",
    "reason": "sign_in_unavailable",
    "retryAfterSeconds": 40,
}


class _AlwaysUnavailable:
    """A stand-in for the client's requests.Session: every call is the 503."""

    def __init__(self) -> None:
        self.headers: dict[str, str] = {}
        self.calls = 0

    def request(self, method, url, **kwargs) -> requests.Response:
        self.calls += 1
        response = requests.Response()
        response.status_code = 503
        response.headers["Retry-After"] = "40"
        response.headers["Content-Type"] = "application/json"
        response._content = json.dumps(_SIGN_IN_UNAVAILABLE).encode()
        response.raw = io.BytesIO()  # the client closes it before it waits
        response.url = url
        return response

    def close(self) -> None:
        pass


def test_the_client_waits_out_a_503_for_one_retry_after_at_most():
    """Not the library's five minutes: an MCP host gives a tool call about one."""
    client = entry._build_client(make_config())

    assert client._retry.unavailable_budget == 45.0
    assert client._retry.unavailable_budget < 60
    # 429s and network errors keep the library's own retries.
    assert client._retry.max_retries == 3


async def test_a_503_that_lasts_comes_back_to_the_llm_after_one_wait():
    client = entry._build_client(make_config())
    sleeps: list[float] = []
    client._retry.sleep = sleeps.append
    session = _AlwaysUnavailable()
    client._session = session
    mcp = build_server(client, make_config())

    result = await call_tool(mcp, "list_active_alerts", {})

    # One wait at the API's Retry-After; a second (80 s in all) would pass the
    # budget, so the error comes back instead of the host's own timeout.
    assert sleeps == [40.0]
    assert session.calls == 2
    assert result.isError is True
    text = result.content[0].text
    assert "temporarily unavailable" in text
    assert "not a rate limit" in text
    assert "40s of waiting" in text
