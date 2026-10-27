"""Process entry point: ``taghistorian-mcp`` (see pyproject.toml's
``[project.scripts]``) or ``python -m taghistorian_mcp``.

This is what an MCP host actually launches as a subprocess and talks to over
stdin/stdout, so what happens here before ``mcp.run()`` is called matters a
lot: stdout must never carry anything but MCP protocol frames (the SDK's
stdio transport owns it exclusively), which is why every log line - startup,
errors, everything - goes to stderr, never ``print()``.
"""

from __future__ import annotations

import logging
import sys

from .config import ConfigError, ServerConfig

# taghistorian (python-client/) is not on PyPI yet - see pyproject.toml's TODO
# on the missing `dependencies` entry for the full story. A plain
# ModuleNotFoundError here would otherwise be the very first thing a new user
# sees, with no indication of what to actually run; this exists to turn that
# into a one-line, actionable fix instead of a bare traceback. Kept at true
# module scope (not inside main()) because that is the only place early
# enough to catch it - the console-script entry point imports this module
# before calling main(), so an uncaught ImportError here happens before any
# of our own error handling would ever run.
try:
    from taghistorian import RetryConfig, TagHistorianClient

    # Through errors.py, this is also the check that the client is 0.2.0 or
    # newer: it imports TagHistorianUnavailableError, which 0.1.x does not
    # have, so an older one gets the message below rather than a traceback.
    from .server import build_server
except ImportError as exc:
    print(
        "taghistorian-mcp requires the 'taghistorian' package 0.2.0 or newer, which "
        "is not yet on PyPI. Install it from the sibling python-client/ directory "
        "first (again, if an older one is installed):\n"
        "    pip install -e ../python-client\n"
        "(path relative to mcp-server/; see this package's README for the full "
        f"story). Original import error: {exc}",
        file=sys.stderr,
    )
    raise SystemExit(1) from exc

logger = logging.getLogger("taghistorian_mcp")

# How long one tool call may spend waiting out 503s in all - not the client
# library's default of five minutes. A tool call runs on this server's event
# loop, so a wait inside it holds the whole server (other calls, cancellations
# and pings wait with it), and many MCP hosts give up on a tool call after
# 60 s. The budget counts the waits, not the requests between them. When the
# API could not check the key (Retry-After 30-45 s) that is one wait and two
# requests, so the "temporarily unavailable" message, which the LLM then gets
# to see, usually comes back well inside 60 s - not if the requests are slow
# too. A write the API could not take just now asks for anything from a
# second (a full buffer) or five (a restart) to about half a minute, so
# shorter waits can repeat within the same 45 s.
_UNAVAILABLE_BUDGET_SECONDS = 45.0

# Each request gets ten seconds to connect and ten to answer - not the
# library's 45 to answer, which with a 45 s wait could pass the hosts' 60 s.
# A request that times out is a network error, retried on its own terms
# outside the budget above; so a sign-in answer slower than ten seconds comes
# back to the LLM after about 47 s (four tries of ten seconds, and 1 + 2 + 4 s
# between them) as an answer that did not come in time (errors.py), not as
# "temporarily unavailable" with its 503's reason.
_REQUEST_TIMEOUT_SECONDS = (10.0, 10.0)


def _build_client(config: ServerConfig) -> TagHistorianClient:
    return TagHistorianClient(
        api_key=config.api_key,
        base_url=config.base_url,
        timeout=_REQUEST_TIMEOUT_SECONDS,
        retry=RetryConfig(unavailable_budget=_UNAVAILABLE_BUDGET_SECONDS),
    )


def _configure_logging() -> None:
    # Explicit stream=sys.stderr rather than relying on logging's own default
    # (which does happen to be stderr) - this is the one line in this whole
    # package where getting it wrong means silently corrupting every MCP
    # message the host tries to parse, so it is spelled out rather than
    # inherited.
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stderr,
    )


def main() -> None:
    _configure_logging()

    try:
        config = ServerConfig.from_env()
    except ConfigError as exc:
        logger.error(str(exc))
        raise SystemExit(1) from exc

    client = _build_client(config)
    mcp = build_server(client, config)

    # THE loud, unmissable statement of mode. Not a debug-level log line: a
    # user watching this process start (or checking their MCP host's server
    # log after the fact, wondering "wait, can this actually write to my
    # account?") needs this to be impossible to miss, per the write gate's
    # whole reason for existing - see config.py's module docstring.
    if config.enable_write:
        logger.warning(
            "WRITE TOOLS ENABLED (TAGHISTORIAN_ENABLE_WRITE is set) - "
            "write_measurement, write_batch_measurements, and create_tag are "
            "available and will make real, billable changes against %s.",
            config.base_url,
        )
    else:
        logger.info(
            "Write tools DISABLED (default). Only read tools are registered - "
            "set TAGHISTORIAN_ENABLE_WRITE=true to enable writing against %s.",
            config.base_url,
        )

    logger.info("Starting taghistorian-mcp on stdio transport.")
    try:
        mcp.run(transport="stdio")
    finally:
        client.close()


if __name__ == "__main__":
    main()
