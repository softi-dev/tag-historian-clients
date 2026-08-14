"""Tests for the write gate and the rest of startup config parsing."""

from __future__ import annotations

import pytest

from taghistorian_mcp.config import ConfigError, ServerConfig, _is_write_enabled


@pytest.mark.parametrize("raw", ["1", "true", "TRUE", "True", "yes", "YES", " true ", "\ttrue\n"])
def test_write_gate_recognises_documented_truthy_values(raw):
    assert _is_write_enabled(raw) is True


@pytest.mark.parametrize(
    "raw",
    [
        None,
        "",
        " ",
        "0",
        "false",
        "no",
        "on",  # close to "yes"/"true" in spirit, but not in the documented set
        "enabled",
        "TAGHISTORIAN_ENABLE_WRITE",  # a plausible copy-paste-the-var-name accident
    ],
)
def test_write_gate_rejects_everything_else_including_empty_string(raw):
    # The empty-but-set case is the one this gate exists specifically to
    # reject - see config.py's module docstring on _TRUTHY_VALUES for why
    # "the variable merely exists" must not be enough.
    assert _is_write_enabled(raw) is False


def test_from_env_requires_api_key(monkeypatch):
    monkeypatch.delenv("TAGHISTORIAN_API_KEY", raising=False)
    with pytest.raises(ConfigError, match="TAGHISTORIAN_API_KEY"):
        ServerConfig.from_env()


def test_from_env_rejects_whitespace_only_api_key(monkeypatch):
    monkeypatch.setenv("TAGHISTORIAN_API_KEY", "   ")
    with pytest.raises(ConfigError, match="TAGHISTORIAN_API_KEY"):
        ServerConfig.from_env()


def test_from_env_defaults_base_url_and_write_gate(monkeypatch):
    monkeypatch.setenv("TAGHISTORIAN_API_KEY", "k")
    monkeypatch.delenv("TAGHISTORIAN_BASE_URL", raising=False)
    monkeypatch.delenv("TAGHISTORIAN_ENABLE_WRITE", raising=False)

    config = ServerConfig.from_env()

    assert config.base_url == "https://api.taghistorian.com"
    assert config.enable_write is False


def test_from_env_reads_overrides(monkeypatch):
    monkeypatch.setenv("TAGHISTORIAN_API_KEY", "k")
    monkeypatch.setenv("TAGHISTORIAN_BASE_URL", "https://staging.example.com")
    monkeypatch.setenv("TAGHISTORIAN_ENABLE_WRITE", "true")

    config = ServerConfig.from_env()

    assert config.base_url == "https://staging.example.com"
    assert config.enable_write is True
