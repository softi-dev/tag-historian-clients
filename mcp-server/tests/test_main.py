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

# What the write endpoints answer when they cannot take a write just now.
_WRITE_UNAVAILABLE = {
    "error": "ingest_unavailable",
    "reason": "buffer_saturated",
    "retryAfterSeconds": 5,
}


class _AlwaysUnavailable:
    """A stand-in for the client's requests.Session: every call is the 503."""

    def __init__(self, retry_after: str = "40", body: dict = _SIGN_IN_UNAVAILABLE) -> None:
        self.headers: dict[str, str] = {}
        self.calls = 0
        self._retry_after = retry_after
        self._body = body

    def request(self, method, url, **kwargs) -> requests.Response:
        self.calls += 1
        response = requests.Response()
        response.status_code = 503
        response.headers["Retry-After"] = self._retry_after
        response.headers["Content-Type"] = "application/json"
        response._content = json.dumps(self._body).encode()
        response.raw = io.BytesIO()  # the client closes it before it waits
        response.url = url
        return response

    def close(self) -> None:
        pass


def test_the_client_waits_out_503s_for_45_seconds_of_waiting_at_most():
    """Not the library's five minutes: an MCP host gives a tool call about one.
    The budget counts waits, not requests."""
    client = entry._build_client(make_config())

    assert client._retry.unavailable_budget == 45.0
    assert client._retry.unavailable_budget < 60
    # 429s and network errors keep the library's own retries.
    assert client._retry.max_retries == 3
    # Ten seconds to answer, not the library's 45: with a 45 s wait on top,
    # that could pass a host's 60 s.
    assert client._timeout == (10.0, 10.0)


def test_main_starts_the_server_on_the_client_that_waits_45_seconds(monkeypatch):
    """main() itself, not only the helper: the shipped server waits 45 s, not five minutes."""
    monkeypatch.setenv("TAGHISTORIAN_API_KEY", "test-key")
    monkeypatch.setenv("TAGHISTORIAN_BASE_URL", "https://api.test.local")
    monkeypatch.delenv("TAGHISTORIAN_ENABLE_WRITE", raising=False)
    started: list = []

    class _Server:
        def run(self, transport: str) -> None:
            started.append(transport)

    def fake_build_server(client, config):
        started.append(client)
        return _Server()

    monkeypatch.setattr(entry, "build_server", fake_build_server)

    entry.main()

    client, transport = started
    assert transport == "stdio"
    assert client._retry.unavailable_budget == 45.0
    assert client._timeout == (10.0, 10.0)


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


async def test_a_short_retry_after_is_waited_out_a_few_times_within_the_budget():
    """A write the API could not take just now asks for a few seconds: several
    waits, 45 s of waiting in all, not one."""
    client = entry._build_client(make_config())
    sleeps: list[float] = []
    client._retry.sleep = sleeps.append
    session = _AlwaysUnavailable(retry_after="5", body=_WRITE_UNAVAILABLE)
    client._session = session
    mcp = build_server(client, make_config())

    result = await call_tool(mcp, "list_active_alerts", {})

    assert sleeps == [5.0] * 9
    assert sum(sleeps) == 45.0
    assert session.calls == 10
    assert result.isError is True
    text = result.content[0].text
    assert "temporarily unavailable" in text
    assert "45s of waiting" in text
