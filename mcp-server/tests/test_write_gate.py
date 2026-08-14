"""The core safety property this whole package is built around: with the
write gate off, write tools do not merely refuse when called - they never
appear in tools/list at all. See server.py's module docstring for why that
distinction (nonexistent vs. present-but-blocked) is the actual point.
"""

from __future__ import annotations

from taghistorian_mcp.server import build_server

from .conftest import make_config

_WRITE_TOOL_NAMES = {"write_measurement", "write_batch_measurements", "create_tag"}
_READ_TOOL_NAMES = {
    "list_tags",
    "read_measurements",
    "read_last_value",
    "read_aggregated",
    "list_active_alerts",
    "list_alert_rules",
}


async def test_write_gate_off_by_default_excludes_write_tools_from_list(mock_client):
    mcp = build_server(mock_client, make_config(enable_write=False))

    tools = await mcp.list_tools()
    names = {t.name for t in tools}

    assert names == _READ_TOOL_NAMES
    assert names.isdisjoint(_WRITE_TOOL_NAMES)


async def test_write_gate_on_includes_both_read_and_write_tools(mock_client):
    mcp = build_server(mock_client, make_config(enable_write=True))

    tools = await mcp.list_tools()
    names = {t.name for t in tools}

    assert names == _READ_TOOL_NAMES | _WRITE_TOOL_NAMES


async def test_every_write_tool_description_states_it_is_a_real_write(mock_client):
    mcp = build_server(mock_client, make_config(enable_write=True))
    tools = {t.name: t for t in await mcp.list_tools()}

    for name in _WRITE_TOOL_NAMES:
        description = (tools[name].description or "").lower()
        # Not "writes data" alone - specifically that it is real and billable
        # against the customer's own account. A write tool must never be
        # undersold as harmless.
        assert "real" in description, f"{name} description does not say 'real': {description!r}"
        assert "quota" in description or "billable" in description, (
            f"{name} description does not mention quota/billing: {description!r}"
        )


async def test_every_read_tool_description_states_it_is_read_only(mock_client):
    mcp = build_server(mock_client, make_config(enable_write=False))
    tools = {t.name: t for t in await mcp.list_tools()}

    for name in _READ_TOOL_NAMES:
        description = (tools[name].description or "").lower()
        assert "read-only" in description, (
            f"{name} description does not say 'read-only': {description!r}"
        )
