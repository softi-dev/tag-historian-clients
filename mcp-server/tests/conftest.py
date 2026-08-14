"""Shared fixtures for taghistorian_mcp tests.

None of these tests touch a real API - every test builds the server around a
``unittest.mock.MagicMock`` standing in for ``TagHistorianClient``, and
asserts on what the mock was called with / what it returns to the tool.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from mcp import types
from mcp.server.fastmcp import FastMCP

from taghistorian_mcp.config import ServerConfig


@pytest.fixture
def mock_client() -> MagicMock:
    return MagicMock(name="TagHistorianClient")


def make_config(*, enable_write: bool = False) -> ServerConfig:
    return ServerConfig(
        api_key="test-key", base_url="https://api.test.local", enable_write=enable_write
    )


async def call_tool(mcp: FastMCP, name: str, arguments: dict) -> types.CallToolResult:
    """Invoke a tool the way an MCP host actually would: through the
    low-level protocol handler registered in ``_setup_handlers``, not
    ``FastMCP.call_tool()`` directly.

    This distinction matters and is not just plumbing-for-plumbing's-sake:
    ``FastMCP.call_tool()`` lets a raised ``ToolError`` propagate straight out
    as a Python exception (confirmed by reading the SDK - it is only the
    *low-level* ``Server.call_tool()`` decorator, registered as the actual
    ``tools/call`` handler, that wraps the call in ``try/except Exception`` and
    turns it into ``CallToolResult(isError=True, ...)`` - the mechanism that
    guarantees a failed call comes back as a clear error the LLM can see,
    never as an unhandled exception that kills the server process).
    Calling through ``FastMCP.call_tool()`` in a test would silently test a
    code path a real MCP host never takes and let an error-handling
    regression through undetected.
    """
    request = types.CallToolRequest(
        method="tools/call",
        params=types.CallToolRequestParams(name=name, arguments=arguments),
    )
    server_result = await mcp._mcp_server.request_handlers[types.CallToolRequest](request)
    return server_result.root
