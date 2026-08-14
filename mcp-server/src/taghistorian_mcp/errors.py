"""Turns the client's own exceptions into clean MCP tool-level errors.

WHY THIS EXISTS AT ALL: FastMCP already catches every exception a tool
function raises and turns it into an ``isError`` tool result on its own (see
``Tool.run`` in the SDK) - a bug here cannot crash the long-lived MCP host
session by itself. But that generic safety net produces
``f"Error executing tool {name}: {exc}"`` from whatever ``str(exc)`` the
underlying exception happens to give, and ``str()`` of a
``TagHistorianRateLimitError`` does not mention ``retry_after`` or how many
attempts were already made, and a bare ``requests`` connection failure reads
like a stack trace fragment, not a sentence an LLM can usefully relay to the
user ("your boiler.temperature request failed, try again in a bit" versus
"ConnectionError(...)"). This module is what turns "a tool call did not
crash the process" into "a tool call failed with a message actually worth
showing someone" - the two are not the same bar, and this server deliberately
holds itself to the second one.

``@handle_client_errors`` wraps a tool function and re-raises the three
``taghistorian`` exception types (plus ``ValueError``, for
``write_batch_measurements``' over-1000-item check) as
``mcp.server.fastmcp.exceptions.ToolError`` with a purpose-written message.
Anything else escapes unchanged and still hits FastMCP's own generic
catch-all - this module does not need to be exhaustive to be safe, only to
improve the common, expected failure modes.
"""

from __future__ import annotations

import functools
from collections.abc import Callable
from typing import TypeVar

from mcp.server.fastmcp.exceptions import ToolError
from taghistorian import (
    TagHistorianAPIError,
    TagHistorianConnectionError,
    TagHistorianRateLimitError,
)

_F = TypeVar("_F", bound=Callable)


def _describe(exc: Exception) -> str:
    if isinstance(exc, TagHistorianRateLimitError):
        # Distinct from a plain TagHistorianAPIError (checked first below,
        # since this is a subclass): the server never refused the request on
        # its merits, it just never got un-throttled within this client's
        # retry budget. "try later" is the actionable advice here, not "the
        # request was wrong".
        retry_hint = (
            f"; the server asked to wait {exc.retry_after:g}s and was still "
            f"busy after {exc.attempts} attempt(s)"
            if exc.retry_after is not None
            else f"; still busy after {exc.attempts} attempt(s)"
        )
        return f"Tag Historian is rate-limited or temporarily overloaded (HTTP {exc.status_code}){retry_hint}. Try again shortly."

    if isinstance(exc, TagHistorianAPIError):
        detail = f" Details: {'; '.join(exc.violations)}" if exc.violations else ""
        return (
            f"Tag Historian rejected the request (HTTP {exc.status_code}): {exc.message}.{detail}"
        )

    if isinstance(exc, TagHistorianConnectionError):
        return f"Could not reach Tag Historian after {exc.attempts} attempt(s): {exc.message}"

    # ValueError, currently only write_batch's own >1000-item guard - see
    # TagHistorianClient.write_batch's module-level comment on _MAX_BATCH_SIZE
    # for why the client raises rather than silently chunking. Surfaced
    # as-is: the message it raises is already written for a human to read.
    return str(exc)


def handle_client_errors(fn: _F) -> _F:
    """Decorator for a tool function that calls the ``taghistorian`` client.

    Put this closest to the function definition (innermost of any stack of
    decorators) so it sees the real client exception, not something another
    decorator already rewrapped.
    """

    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except (
            TagHistorianAPIError,
            TagHistorianConnectionError,
            ValueError,
        ) as exc:
            raise ToolError(_describe(exc)) from exc

    return wrapper  # type: ignore[return-value]
