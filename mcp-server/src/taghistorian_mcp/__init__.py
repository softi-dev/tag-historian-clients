"""MCP (Model Context Protocol) server exposing Tag Historian to an LLM host.

Thin on purpose: everything that talks HTTP to the Tag Historian API lives in
the ``taghistorian`` package (``python-client/``); this package's only job is
translating between that client's typed methods and the MCP tool-call
protocol - argument schemas in, formatted text or a clean tool-level error
out. See ``server.py`` for the tool definitions and ``config.py`` for how the
write gate is decided.
"""

from __future__ import annotations

__version__ = "0.1.0"
