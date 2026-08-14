"""Parses the ISO 8601 timestamp strings tool arguments arrive as.

MCP tool arguments are JSON, which has no native datetime type, so every
``from``/``to`` parameter here is declared as a plain string and parsed on
this side rather than asking the ``mcp`` SDK to do it. A bad value (an LLM
occasionally hallucinates "yesterday" instead of an actual timestamp) must
turn into a clear tool error, not a ``ValueError`` traceback escaping into
FastMCP's generic exception handling - hence raising ``ToolError`` directly
here rather than letting ``datetime.fromisoformat`` fail unexplained.
"""

from __future__ import annotations

from datetime import datetime, timezone

from mcp.server.fastmcp.exceptions import ToolError


def parse_optional_timestamp(value: str | None, *, param_name: str) -> datetime | None:
    """``None`` passes through (meaning "use the server's own default range"
    for ``from``/``to`` - see ``TagHistorianClient.read``'s docstring).
    Anything else must be a real ISO 8601 timestamp.
    """
    if value is None:
        return None

    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise ToolError(
            f"{param_name}={value!r} is not a valid ISO 8601 timestamp "
            f'(e.g. "2026-08-14T09:00:00Z" or "2026-08-14"). {exc}'
        ) from exc

    if parsed.tzinfo is None:
        # Same assumption taghistorian._time makes for a naive datetime -
        # treat it as already UTC rather than guessing an offset. Stated once
        # here rather than imported from there because that module's
        # docstring is about *writes* specifically; this one only ever feeds
        # read-range parameters.
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed
