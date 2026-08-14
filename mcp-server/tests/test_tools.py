"""Behavioural tests for individual tools: happy paths and, more importantly,
that a client-side failure turns into a clean tool-level error rather than
an exception escaping the server process.
"""

from __future__ import annotations

from datetime import datetime, timezone

from taghistorian import (
    Alert,
    AlertRule,
    Measurement,
    ReadResult,
    Tag,
    TagHistorianAPIError,
    TagHistorianConnectionError,
    TagHistorianRateLimitError,
    TagListResult,
    WriteResult,
)

from taghistorian_mcp.server import build_server

from .conftest import call_tool, make_config

UTC = timezone.utc


# -- read tools happy paths -------------------------------------------------


async def test_read_last_value_formats_the_measurement(mock_client):
    mock_client.read_last.return_value = Measurement(
        timestamp=datetime(2026, 8, 14, 9, 0, tzinfo=UTC),
        value=84.2,
        status="Good",
        storage_tier="Hot",
    )
    mcp = build_server(mock_client, make_config())

    result = await call_tool(mcp, "read_last_value", {"tag_name": "boiler.temperature"})

    assert result.isError is False
    text = result.content[0].text
    assert "boiler.temperature" in text
    assert "84.2" in text
    mock_client.read_last.assert_called_once_with("boiler.temperature")


async def test_read_measurements_parses_from_and_to_into_datetimes(mock_client):
    mock_client.read.return_value = ReadResult(
        customer_id="c1",
        tag_name="boiler.temperature",
        from_=datetime(2026, 8, 14, 0, 0, tzinfo=UTC),
        to=datetime(2026, 8, 14, 12, 0, tzinfo=UTC),
        count=0,
        measurements=[],
    )
    mcp = build_server(mock_client, make_config())

    result = await call_tool(
        mcp,
        "read_measurements",
        {
            "tag_name": "boiler.temperature",
            "from_": "2026-08-14T00:00:00Z",
            "to": "2026-08-14T12:00:00Z",
        },
    )

    assert result.isError is False
    _, kwargs = mock_client.read.call_args
    assert kwargs["from_"] == datetime(2026, 8, 14, 0, 0, tzinfo=UTC)
    assert kwargs["to"] == datetime(2026, 8, 14, 12, 0, tzinfo=UTC)


async def test_read_measurements_bad_timestamp_is_a_clean_tool_error_not_a_crash(mock_client):
    mcp = build_server(mock_client, make_config())

    result = await call_tool(
        mcp, "read_measurements", {"tag_name": "t", "from_": "not-a-timestamp"}
    )

    assert result.isError is True
    assert "not-a-timestamp" in result.content[0].text
    # The client was never reached - this fails during argument parsing.
    mock_client.read.assert_not_called()


async def test_list_tags_formats_results(mock_client):
    mock_client.list_tags.return_value = TagListResult(
        total_count=1,
        skip=0,
        take=100,
        tags=[
            Tag(
                tag_name="boiler.temperature",
                description="",
                units="C",
                tag_type="Analog",
                enable_compression=True,
                compression_deadband=0.5,
                total_measurements=100,
                compressed_measurements=10,
                compression_ratio=0.9,
                last_value=84.2,
                last_status="Good",
                last_stored_timestamp=datetime(2026, 8, 14, 9, 0, tzinfo=UTC),
            )
        ],
    )
    mcp = build_server(mock_client, make_config())

    result = await call_tool(mcp, "list_tags", {"search": "boiler"})

    assert result.isError is False
    assert "boiler.temperature" in result.content[0].text
    mock_client.list_tags.assert_called_once_with(
        skip=0, take=100, search="boiler", site=None, area=None, equipment=None
    )


async def test_list_active_alerts_formats_results(mock_client):
    mock_client.list_active_alerts.return_value = [
        Alert(
            alert_id="a1",
            rule_id="r1",
            customer_id="c1",
            tag_name="boiler.temperature",
            rule_name="Boiler overtemp",
            message="boiler.temperature exceeded 90",
            value=94.5,
            threshold=90.0,
            severity="Critical",
            state="Triggered",
            triggered_at=datetime(2026, 8, 14, 9, 0, tzinfo=UTC),
        )
    ]
    mcp = build_server(mock_client, make_config())

    result = await call_tool(mcp, "list_active_alerts", {})

    assert result.isError is False
    text = result.content[0].text
    assert "Critical" in text
    assert "boiler.temperature" in text


async def test_list_alert_rules_formats_results_without_leaking_a_secret(mock_client):
    mock_client.list_alert_rules.return_value = [
        AlertRule(
            rule_id="r1",
            customer_id="c1",
            tag_name="boiler.temperature",
            name="Boiler overtemp",
            condition="AboveThreshold",
            threshold=90.0,
            has_webhook_secret=True,
            is_enabled=True,
            cooldown_seconds=300,
            severity="Critical",
            created_at=datetime(2026, 6, 1, tzinfo=UTC),
        )
    ]
    mcp = build_server(mock_client, make_config())

    result = await call_tool(mcp, "list_alert_rules", {})

    text = result.content[0].text
    assert "Boiler overtemp" in text
    assert "signed" in text  # has_webhook_secret rendered, not a secret value


# -- error handling: every client exception type becomes a clean tool error --


async def test_api_error_404_surfaces_as_clean_tool_error(mock_client):
    mock_client.read_last.side_effect = TagHistorianAPIError("Tag not found", 404)
    mcp = build_server(mock_client, make_config())

    result = await call_tool(mcp, "read_last_value", {"tag_name": "does.not.exist"})

    assert result.isError is True
    text = result.content[0].text
    assert "404" in text
    assert "Tag not found" in text


async def test_403_from_a_read_scoped_key_surfaces_as_clean_tool_error(mock_client):
    mock_client.list_active_alerts.side_effect = TagHistorianAPIError("Forbidden", 403)
    mcp = build_server(mock_client, make_config())

    result = await call_tool(mcp, "list_active_alerts", {})

    assert result.isError is True
    assert "403" in result.content[0].text


async def test_rate_limit_error_surfaces_retry_after_in_the_message(mock_client):
    mock_client.read.side_effect = TagHistorianRateLimitError(
        "Rate limit exceeded", 429, retry_after=7.0, attempts=4
    )
    mcp = build_server(mock_client, make_config())

    result = await call_tool(mcp, "read_measurements", {"tag_name": "t"})

    assert result.isError is True
    text = result.content[0].text
    assert "7" in text
    assert "4 attempt" in text


async def test_connection_error_surfaces_as_clean_tool_error_not_a_crash(mock_client):
    mock_client.read_last.side_effect = TagHistorianConnectionError(
        "Could not reach api.taghistorian.com", attempts=3
    )
    mcp = build_server(mock_client, make_config())

    result = await call_tool(mcp, "read_last_value", {"tag_name": "t"})

    assert result.isError is True
    assert "Could not reach" in result.content[0].text


# -- write tools (only meaningful with the gate on) --------------------------


async def test_write_measurement_happy_path(mock_client):
    mock_client.write.return_value = WriteResult(
        success=True,
        was_compressed=False,
        storage_tier="Hot",
        message="Measurement stored successfully",
        timestamp=datetime(2026, 8, 14, 9, 0, tzinfo=UTC),
        compression_ratio=1.0,
    )
    mcp = build_server(mock_client, make_config(enable_write=True))

    result = await call_tool(
        mcp, "write_measurement", {"tag_name": "boiler.temperature", "value": 84.2, "units": "C"}
    )

    assert result.isError is False
    mock_client.write.assert_called_once()
    args, kwargs = mock_client.write.call_args
    assert args[0] == "boiler.temperature"
    assert args[1] == 84.2
    assert kwargs["units"] == "C"


async def test_write_batch_measurements_over_cap_surfaces_clients_value_error_cleanly(mock_client):
    # The real 1000-item cap lives in TagHistorianClient.write_batch itself
    # (see _MAX_BATCH_SIZE in client.py, and python-client/'s own test for
    # the real over-limit behaviour) - this test only proves the MCP layer
    # does not duplicate that check and does not let the client's ValueError
    # escape as an unhandled exception.
    mock_client.write_batch.side_effect = ValueError(
        "write_batch received 1001 measurements, but the server accepts at most 1000 per call."
    )
    mcp = build_server(mock_client, make_config(enable_write=True))

    result = await call_tool(
        mcp,
        "write_batch_measurements",
        {"measurements": [{"tag_name": "t", "value": 1.0}, {"tag_name": "t", "value": 2.0}]},
    )

    assert result.isError is True
    assert "1000" in result.content[0].text


async def test_create_tag_happy_path(mock_client):
    mock_client.create_tag.return_value = Tag(
        tag_name="boiler.temperature",
        description="Boiler outlet temperature",
        units="C",
        tag_type="Analog",
        enable_compression=True,
        compression_deadband=0.5,
        total_measurements=0,
        compressed_measurements=0,
        compression_ratio=0.0,
    )
    mcp = build_server(mock_client, make_config(enable_write=True))

    result = await call_tool(
        mcp,
        "create_tag",
        {"tag_name": "boiler.temperature", "units": "C", "tag_type": "Analog"},
    )

    assert result.isError is False
    assert "boiler.temperature" in result.content[0].text
    mock_client.create_tag.assert_called_once()
