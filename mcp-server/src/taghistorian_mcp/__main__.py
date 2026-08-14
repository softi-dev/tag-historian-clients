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
    from taghistorian import TagHistorianClient

    from .server import build_server
except ImportError as exc:
    print(
        "taghistorian-mcp requires the 'taghistorian' package, which is not yet "
        "on PyPI. Install it from the sibling python-client/ directory first:\n"
        "    pip install -e ../python-client\n"
        "(path relative to mcp-server/; see this package's README for the full "
        f"story). Original import error: {exc}",
        file=sys.stderr,
    )
    raise SystemExit(1) from exc

logger = logging.getLogger("taghistorian_mcp")


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

    client = TagHistorianClient(api_key=config.api_key, base_url=config.base_url)
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
