"""Startup configuration, read once from the process environment.

A dataclass rather than reading ``os.environ`` scattered through ``server.py``
for the same reason the write gate needs its own named function: the gate is
the single most security-relevant line in this whole package, and it is much
easier to audit (and to unit test in isolation, with no server/client
machinery involved) as one small pure function than as an inline
``os.environ.get(...) in (...)`` buried among tool registrations.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

_DEFAULT_BASE_URL = "https://api.taghistorian.com"

# Deliberately a small, explicit allow-list rather than "the variable is set
# to anything, even empty string". An env var that is merely present-but-empty
# is a common accident of shell scripting and Docker/Compose templating
# (TAGHISTORIAN_ENABLE_WRITE= with nothing after the "=", a templated value
# that resolved to ""); treating that as "write enabled" would silently turn
# on the ability to write real measurements and create real tags against a
# customer's quota because of a blank template variable, which is exactly the
# accidental-opt-in this gate exists to prevent. Case-insensitive because
# there is no meaningful difference in intent between "true" and "TRUE" typed
# into a host's config JSON.
_TRUTHY_VALUES = frozenset({"1", "true", "yes"})


class ConfigError(Exception):
    """Raised when required configuration is missing or invalid at startup.

    Distinct from any ``taghistorian`` client exception - this is a
    before-the-client-even-exists failure (no API key, an unreachable base
    URL string), always caught in ``main()`` and reported to stderr with a
    clean message and non-zero exit rather than a Python traceback, since
    whoever is watching this process start is very likely the person who
    just mistyped an env var in their MCP host's config, not a Python
    developer.
    """


def _is_write_enabled(raw: str | None) -> bool:
    """Implements the write gate's truthy set. See module docstring."""
    return raw is not None and raw.strip().lower() in _TRUTHY_VALUES


@dataclass(frozen=True)
class ServerConfig:
    """Everything read from the environment at process startup.

    :param api_key: The customer's own Tag Historian API key. Its own scope
        (Read/Write/Admin) is enforced server-side on every call - this
        config's ``enable_write`` gate is an independent, additional
        restriction layered in front of that, not a replacement for it. A
        Read-scope key with ``enable_write=True`` still gets a normal 403
        from the API the first time a write tool is actually invoked; the
        gate controls whether that tool is offered to the LLM at all, not
        whether the key is capable of writing.
    :param base_url: Overridable for a self-hosted deployment or staging,
        matching :class:`taghistorian.TagHistorianClient`'s own default.
    :param enable_write: The write gate. ``True`` only for one of the
        recognised truthy strings (see ``_TRUTHY_VALUES``) - unset, empty,
        "0", "false", "no", or any other value all mean ``False``.
    """

    api_key: str
    base_url: str
    enable_write: bool

    @classmethod
    def from_env(cls) -> ServerConfig:
        api_key = os.environ.get("TAGHISTORIAN_API_KEY", "").strip()
        if not api_key:
            raise ConfigError(
                "TAGHISTORIAN_API_KEY is required but was not set (or was empty). "
                "Set it to the customer's own Tag Historian API key - see this "
                "package's README for the exact MCP host config snippet."
            )

        base_url = os.environ.get("TAGHISTORIAN_BASE_URL", "").strip() or _DEFAULT_BASE_URL
        enable_write = _is_write_enabled(os.environ.get("TAGHISTORIAN_ENABLE_WRITE"))

        return cls(api_key=api_key, base_url=base_url, enable_write=enable_write)
