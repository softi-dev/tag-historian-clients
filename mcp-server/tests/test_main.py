"""The client this server builds for itself at startup, and how long it waits."""

from __future__ import annotations

import importlib
import io
import json
import sys
import types

import pytest
import requests
import taghistorian

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
    "reason": "shutting_down",
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


class _NeverAnswers:
    """A stand-in session that connects and then gets no answer in time."""

    def __init__(self, exc: Exception) -> None:
        self.headers: dict[str, str] = {}
        self.calls = 0
        self._exc = exc

    def request(self, method, url, **kwargs) -> requests.Response:
        self.calls += 1
        raise self._exc

    def close(self) -> None:
        pass


async def test_an_answer_slower_than_ten_seconds_is_not_said_as_unreachable():
    """The API may take up to about half a minute to say it could not check a
    key, longer than a request may wait here. It was reached, so the LLM is not
    told it could not be, and is told to try again later."""
    client = entry._build_client(make_config())
    sleeps: list[float] = []
    client._retry.sleep = sleeps.append
    session = _NeverAnswers(requests.exceptions.ReadTimeout("Read timed out. (read timeout=10.0)"))
    client._session = session
    mcp = build_server(client, make_config())

    result = await call_tool(mcp, "list_active_alerts", {})

    # Four tries of ten seconds and 1 + 2 + 4 s between them: about 47 s.
    assert sleeps == [1.0, 2.0, 4.0]
    assert session.calls == 4
    assert result.isError is True
    text = result.content[0].text
    assert "did not answer in time after 4 attempt(s)" in text
    assert "Could not reach" not in text
    assert "Try again in a few minutes" in text


async def test_a_host_that_cannot_be_connected_to_is_still_unreachable():
    client = entry._build_client(make_config())
    client._retry.sleep = lambda seconds: None
    client._session = _NeverAnswers(requests.exceptions.ConnectTimeout("Connection timed out"))
    mcp = build_server(client, make_config())

    result = await call_tool(mcp, "list_active_alerts", {})

    assert result.isError is True
    assert "Could not reach Tag Historian after 4 attempt(s)" in result.content[0].text


async def test_a_short_retry_after_is_waited_out_a_few_times_within_the_budget():
    """A restart asks for five seconds: several waits, 45 s of waiting in all, not one."""
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


def test_a_client_older_than_0_2_0_is_named_rather_than_a_traceback(monkeypatch, capsys):
    """0.1.x has no TagHistorianUnavailableError. errors.py imports it, and the
    entry point's import of the server is what turns that into the message."""
    older = types.ModuleType("taghistorian")
    for name in dir(taghistorian):
        if name != "TagHistorianUnavailableError":
            setattr(older, name, getattr(taghistorian, name))
    monkeypatch.setitem(sys.modules, "taghistorian", older)
    package = sys.modules["taghistorian_mcp"]
    for name in [m for m in sys.modules if m.startswith("taghistorian_mcp.")]:
        # The fresh import below rebinds the package's attributes too; both
        # are put back afterwards.
        short = name.rpartition(".")[2]
        if hasattr(package, short):
            monkeypatch.setattr(package, short, getattr(package, short))
        monkeypatch.delitem(sys.modules, name)

    with pytest.raises(SystemExit) as raised:
        importlib.import_module("taghistorian_mcp.__main__")

    assert raised.value.code == 1
    assert "0.2.0 or newer" in capsys.readouterr().err
