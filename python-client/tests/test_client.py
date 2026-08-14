"""Tests for TagHistorianClient, HTTP mocked with the `responses` library.

Each test is written to prove something about THIS client's behaviour -
request shape, retry/backoff decisions, caching, exception mapping - not
merely that `responses` intercepts a call.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest
import responses

from taghistorian import (
    MeasurementInput,
    RetryConfig,
    TagHistorianAPIError,
    TagHistorianClient,
    TagHistorianRateLimitError,
)
from taghistorian._time import serialize_timestamp

from .conftest import BASE_URL

MEASUREMENTS_URL = f"{BASE_URL}/api/measurements"
BATCH_URL = f"{BASE_URL}/api/measurements/batch"
ME_URL = f"{BASE_URL}/api/customers/me"
ACTIVE_ALERTS_URL = f"{BASE_URL}/api/alerts/active"
ALERT_RULES_URL = f"{BASE_URL}/api/alerts/rules"

CUSTOMER_ID = "11111111-1111-1111-1111-111111111111"


def _write_response_body(**overrides) -> dict:
    body = {
        "success": True,
        "wasCompressed": False,
        "storageTier": "Hot",
        "message": "Measurement stored successfully",
        "timestamp": "2026-01-01T00:00:00Z",
        "compressionRatio": 1.0,
    }
    body.update(overrides)
    return body


# -- single write ---------------------------------------------------------


@responses.activate
def test_write_single_success_sends_expected_body_and_parses_response(make_client):
    responses.add(responses.POST, MEASUREMENTS_URL, json=_write_response_body(), status=200)

    client = make_client()
    result = client.write("boiler.temperature", 84.2, units="C")

    assert result.success is True
    assert result.was_compressed is False
    assert result.storage_tier == "Hot"

    sent = json.loads(responses.calls[0].request.body)
    assert sent == {"tagName": "boiler.temperature", "value": 84.2, "units": "C"}
    # No timestamp key at all when none was given - the server's own "absent
    # means now" behaviour (MeasurementsController.TryCreateMeasurement)
    # should not be second-guessed by the client inventing a timestamp.
    assert "timestamp" not in sent

    assert responses.calls[0].request.headers["X-Api-Key"] == "test-key"


@responses.activate
def test_write_includes_serialized_timestamp_when_given(make_client):
    responses.add(responses.POST, MEASUREMENTS_URL, json=_write_response_body(), status=200)

    client = make_client()
    ts = datetime(2026, 3, 1, 8, 30, tzinfo=timezone.utc)
    client.write("t", 1.0, timestamp=ts)

    sent = json.loads(responses.calls[0].request.body)
    assert sent["timestamp"] == "2026-03-01T08:30:00+00:00".replace("+00:00", "Z")


# -- batch: over-limit raises before any network call ----------------------


def test_batch_over_1000_raises_value_error_before_any_network_call(make_client):
    # No responses.add() at all: if the client tried to make an HTTP call
    # in this block, `responses` (via the decorator on other tests) would
    # normally intercept it, but here we deliberately do NOT activate
    # `responses` - a stray real network call would then hang/fail loudly
    # rather than being silently swallowed by a mock, which is exactly the
    # guarantee this test needs: no request is attempted at all.
    client = make_client()
    too_many = [MeasurementInput(tag_name="t", value=float(i)) for i in range(1001)]

    with pytest.raises(ValueError, match="1000"):
        client.write_batch(too_many)


@responses.activate
def test_batch_at_exactly_1000_is_sent_in_one_call(make_client):
    responses.add(
        responses.POST,
        BATCH_URL,
        json={
            "totalCount": 1000,
            "storedCount": 1000,
            "compressedCount": 0,
            "processingTimeMs": 12,
            "results": [],
        },
        status=200,
    )

    client = make_client()
    exactly_1000 = [MeasurementInput(tag_name="t", value=float(i)) for i in range(1000)]
    result = client.write_batch(exactly_1000)

    assert result.total_count == 1000
    assert len(responses.calls) == 1
    sent = json.loads(responses.calls[0].request.body)
    assert len(sent["measurements"]) == 1000


# -- 429 retry honours Retry-After -----------------------------------------


@responses.activate
def test_429_retry_honours_retry_after_header(make_client, sleeps):
    responses.add(
        responses.POST,
        MEASUREMENTS_URL,
        json={"error": "Rate limit exceeded. Please try again later.", "retryAfterSeconds": "7"},
        status=429,
        headers={"Retry-After": "7"},
    )
    responses.add(responses.POST, MEASUREMENTS_URL, json=_write_response_body(), status=200)

    client = make_client(max_retries=3)
    result = client.write("t", 1.0)

    assert result.success is True
    assert len(responses.calls) == 2
    # The client must wait exactly what the server said, not its own
    # backoff schedule (which would be 1.0s for attempt 1) - this is the
    # whole point of honouring Retry-After.
    assert sleeps == [7.0]


@responses.activate
def test_503_retries_exhausted_raises_rate_limit_error_with_details(make_client, sleeps):
    # max_retries=2 => 3 total attempts, all refused.
    for _ in range(3):
        responses.add(
            responses.POST,
            MEASUREMENTS_URL,
            json={
                "error": "ingest_unavailable",
                "reason": "buffer_saturated",
                "retryAfterSeconds": 3,
            },
            status=503,
            headers={"Retry-After": "3"},
        )

    client = make_client(max_retries=2)

    with pytest.raises(TagHistorianRateLimitError) as excinfo:
        client.write("t", 1.0)

    assert excinfo.value.status_code == 503
    assert excinfo.value.attempts == 3
    assert excinfo.value.retry_after == 3.0
    assert len(responses.calls) == 3
    # Slept before the 2nd and 3rd attempts, each honouring the header - not
    # zero (would mean no retry happened) and not more than the attempts
    # actually made.
    assert sleeps == [3.0, 3.0]


# -- non-retryable errors ---------------------------------------------------


@pytest.mark.parametrize("status_code", [400, 401, 403, 404])
@responses.activate
def test_client_errors_raise_immediately_without_retry(make_client, sleeps, status_code):
    responses.add(
        responses.POST,
        MEASUREMENTS_URL,
        json={"error": f"failure for {status_code}"},
        status=status_code,
    )

    client = make_client(max_retries=3)

    with pytest.raises(TagHistorianAPIError) as excinfo:
        client.write("t", 1.0)

    assert excinfo.value.status_code == status_code
    assert excinfo.value.message == f"failure for {status_code}"
    # Exactly one call: a retry loop that ignored the "never retry 4xx"
    # rule would have made up to 4.
    assert len(responses.calls) == 1
    assert sleeps == []


@responses.activate
def test_400_with_violations_surfaces_them_on_the_exception(make_client):
    responses.add(
        responses.POST,
        MEASUREMENTS_URL,
        json={
            "error": "Invalid tag name",
            "violations": ["must not contain spaces", "must not start with a digit"],
        },
        status=400,
    )

    client = make_client()
    with pytest.raises(TagHistorianAPIError) as excinfo:
        client.write("1 bad tag", 1.0)

    assert excinfo.value.violations == ["must not contain spaces", "must not start with a digit"]


# -- customerId cached across reads -----------------------------------------


@responses.activate
def test_customer_id_fetched_once_and_reused_across_reads(make_client):
    responses.add(
        responses.GET, ME_URL, json={"customerId": CUSTOMER_ID, "name": "Acme"}, status=200
    )
    responses.add(
        responses.GET,
        f"{BASE_URL}/api/measurements/{CUSTOMER_ID}/temp/last",
        json={
            "timestamp": "2026-01-01T00:00:00Z",
            "value": 1.0,
            "status": "Good",
            "storageTier": "Hot",
        },
        status=200,
    )
    responses.add(
        responses.GET,
        f"{BASE_URL}/api/measurements/{CUSTOMER_ID}/temp",
        json={
            "customerId": CUSTOMER_ID,
            "tagName": "temp",
            "from": "2026-01-01T00:00:00Z",
            "to": "2026-01-02T00:00:00Z",
            "count": 0,
            "measurements": [],
        },
        status=200,
    )

    client = make_client()
    client.read_last("temp")
    client.read("temp")
    client.read_last("temp")

    me_calls = [c for c in responses.calls if c.request.url == ME_URL]
    assert len(me_calls) == 1, "GET /api/customers/me must be called exactly once, not per read"


# -- interpolation echoes what the server actually did -----------------------


@responses.activate
def test_read_aggregated_reports_actual_interpolation_not_requested_one(make_client):
    responses.add(responses.GET, ME_URL, json={"customerId": CUSTOMER_ID}, status=200)
    responses.add(
        responses.GET,
        f"{BASE_URL}/api/measurements/{CUSTOMER_ID}/temp/aggregated",
        json={
            "customerId": CUSTOMER_ID,
            "tagName": "temp",
            "from": "2020-01-01T00:00:00Z",
            "to": "2026-01-01T00:00:00Z",
            "interval": "1h",
            "aggregationType": "Average",
            # The archive path always answers "None" regardless of what was
            # requested below.
            "interpolationType": "None",
            "count": 0,
            "measurements": [],
        },
        status=200,
    )

    client = make_client()
    result = client.read_aggregated("temp", interval="1h", interpolation="Linear")

    assert result.interpolation_type == "None"

    sent_params = responses.calls[1].request.url
    assert "interpolation=Linear" in sent_params, "the request should still ask for Linear"


# -- timestamp: naive vs aware serialize identically -----------------------


def test_naive_and_aware_utc_timestamps_serialize_identically():
    naive = datetime(2026, 6, 15, 13, 45, 30)
    aware = datetime(2026, 6, 15, 13, 45, 30, tzinfo=timezone.utc)

    assert serialize_timestamp(naive) == serialize_timestamp(aware) == "2026-06-15T13:45:30Z"


def test_non_utc_aware_timestamp_converts_to_utc_before_serializing():
    # 15:45:30+02:00 is 13:45:30Z - if the client forgot to convert, this
    # would come out as "15:45:30Z", silently three hours wrong.
    plus_two = datetime(2026, 6, 15, 15, 45, 30, tzinfo=timezone(timedelta(hours=2)))
    assert serialize_timestamp(plus_two) == "2026-06-15T13:45:30Z"


@responses.activate
def test_write_serializes_naive_and_aware_timestamps_identically_over_http(make_client):
    responses.add(responses.POST, MEASUREMENTS_URL, json=_write_response_body(), status=200)
    responses.add(responses.POST, MEASUREMENTS_URL, json=_write_response_body(), status=200)

    client = make_client()
    naive = datetime(2026, 6, 15, 13, 45, 30)
    aware = datetime(2026, 6, 15, 13, 45, 30, tzinfo=timezone.utc)

    client.write("t", 1.0, timestamp=naive)
    client.write("t", 1.0, timestamp=aware)

    body_1 = json.loads(responses.calls[0].request.body)
    body_2 = json.loads(responses.calls[1].request.body)
    assert body_1["timestamp"] == body_2["timestamp"] == "2026-06-15T13:45:30Z"


# -- export ------------------------------------------------------------


@responses.activate
def test_export_measurements_returns_bytes_when_no_path_given(make_client):
    responses.add(
        responses.GET,
        f"{BASE_URL}/api/export/measurements/temp",
        body=b"timestamp,value\n2026-01-01T00:00:00Z,1.0\n",
        status=200,
        headers={
            "Content-Type": "text/csv",
            "Content-Disposition": 'attachment; filename="temp-export.csv"',
        },
    )

    client = make_client()
    result = client.export_measurements("temp", format="Csv")

    assert result.path is None
    assert result.content == b"timestamp,value\n2026-01-01T00:00:00Z,1.0\n"
    assert result.filename == "temp-export.csv"
    assert result.content_type == "text/csv"


@responses.activate
def test_export_measurements_streams_to_path(make_client, tmp_path):
    payload = b'{"measurements": []}'
    responses.add(
        responses.GET,
        f"{BASE_URL}/api/export/measurements/temp",
        body=payload,
        status=200,
        headers={
            "Content-Type": "application/json",
            "Content-Disposition": 'attachment; filename="temp-export.json"',
        },
    )

    client = make_client()
    dest = tmp_path / "out.json"
    result = client.export_measurements("temp", path=str(dest))

    assert result.content is None
    assert result.path == str(dest)
    assert dest.read_bytes() == payload


# -- connection-level failure never leaks a requests exception -------------


def test_connection_error_is_wrapped_after_retries_exhausted(make_client, sleeps, monkeypatch):
    import requests

    calls = {"n": 0}

    def _boom(*args, **kwargs):
        calls["n"] += 1
        raise requests.exceptions.ConnectionError("connection refused")

    client = make_client(max_retries=2)
    monkeypatch.setattr(client._session, "request", _boom)

    from taghistorian import TagHistorianConnectionError

    with pytest.raises(TagHistorianConnectionError) as excinfo:
        client.write("t", 1.0)

    assert excinfo.value.status_code is None
    assert excinfo.value.attempts == 3
    assert calls["n"] == 3
    assert len(sleeps) == 2


# -- retries disabled entirely -----------------------------------------


@responses.activate
def test_max_retries_zero_disables_retrying(sleeps):
    responses.add(
        responses.POST,
        MEASUREMENTS_URL,
        json={"error": "Rate limit exceeded", "retryAfterSeconds": "5"},
        status=429,
        headers={"Retry-After": "5"},
    )

    retry = RetryConfig(max_retries=0, sleep=sleeps.append)
    client = TagHistorianClient(api_key="test-key", base_url=BASE_URL, retry=retry)

    with pytest.raises(TagHistorianRateLimitError) as excinfo:
        client.write("t", 1.0)

    assert excinfo.value.attempts == 1
    assert len(responses.calls) == 1
    assert sleeps == []


# -- alerts ------------------------------------------------------------


@responses.activate
def test_list_active_alerts_parses_entity_list_without_customer_id_call(make_client):
    responses.add(
        responses.GET,
        ACTIVE_ALERTS_URL,
        json=[
            {
                "alertId": "aaaaaaaa-0000-0000-0000-000000000001",
                "ruleId": "bbbbbbbb-0000-0000-0000-000000000001",
                "customerId": CUSTOMER_ID,
                "tagName": "boiler.temperature",
                "ruleName": "Boiler overtemp",
                "message": "boiler.temperature exceeded 90",
                "value": 94.5,
                "threshold": 90.0,
                "severity": "Critical",
                "state": "Triggered",
                "triggeredAt": "2026-01-01T00:00:00Z",
                "acknowledgedAt": None,
                "resolvedAt": None,
            }
        ],
        status=200,
    )

    client = make_client()
    alerts = client.list_active_alerts()

    assert len(alerts) == 1
    alert = alerts[0]
    assert alert.alert_id == "aaaaaaaa-0000-0000-0000-000000000001"
    assert alert.tag_name == "boiler.temperature"
    assert alert.severity == "Critical"
    assert alert.state == "Triggered"
    assert alert.value == 94.5
    assert alert.acknowledged_at is None
    assert alert.resolved_at is None

    # Neither alert endpoint takes a customerId path segment (AlertsController
    # resolves it from the auth context, not the URL) - GET /api/customers/me
    # must never be called for this method.
    me_calls = [c for c in responses.calls if c.request.url == ME_URL]
    assert me_calls == []


@responses.activate
def test_list_active_alerts_parses_acknowledged_timestamp_when_present(make_client):
    responses.add(
        responses.GET,
        ACTIVE_ALERTS_URL,
        json=[
            {
                "alertId": "aaaaaaaa-0000-0000-0000-000000000002",
                "ruleId": "bbbbbbbb-0000-0000-0000-000000000002",
                "customerId": CUSTOMER_ID,
                "tagName": "boiler.pressure",
                "ruleName": "Low pressure",
                "message": "boiler.pressure below 1.0",
                "value": 0.8,
                "threshold": 1.0,
                "severity": "Warning",
                "state": "Acknowledged",
                "triggeredAt": "2026-01-01T00:00:00Z",
                "acknowledgedAt": "2026-01-01T00:05:00Z",
                "resolvedAt": None,
            }
        ],
        status=200,
    )

    client = make_client()
    alert = client.list_active_alerts()[0]

    assert alert.state == "Acknowledged"
    from datetime import datetime, timezone

    assert alert.acknowledged_at == datetime(2026, 1, 1, 0, 5, 0, tzinfo=timezone.utc)


@responses.activate
def test_list_alert_rules_parses_response_and_never_exposes_a_secret_field(make_client):
    responses.add(
        responses.GET,
        ALERT_RULES_URL,
        json=[
            {
                "ruleId": "cccccccc-0000-0000-0000-000000000001",
                "customerId": CUSTOMER_ID,
                "tagName": "boiler.temperature",
                "name": "Boiler overtemp",
                "description": "Fires when the boiler runs hot",
                "condition": "AboveThreshold",
                "threshold": 90.0,
                "threshold2": None,
                "webhookUrl": "https://example.com/hook",
                "hasWebhookSecret": True,
                "isEnabled": True,
                "cooldownSeconds": 300,
                "severity": "Critical",
                "createdAt": "2025-06-01T00:00:00Z",
                "lastTriggeredAt": "2026-01-01T00:00:00Z",
            }
        ],
        status=200,
    )

    client = make_client()
    rules = client.list_alert_rules()

    assert len(rules) == 1
    rule = rules[0]
    assert rule.rule_id == "cccccccc-0000-0000-0000-000000000001"
    assert rule.name == "Boiler overtemp"
    assert rule.condition == "AboveThreshold"
    assert rule.has_webhook_secret is True
    assert rule.is_enabled is True
    assert rule.cooldown_seconds == 300
    # The dataclass has no field a webhook secret could land in even if the
    # (never-sent-by-the-server) key showed up in the payload - only the
    # boolean flag.
    assert not hasattr(rule, "webhook_secret")

    me_calls = [c for c in responses.calls if c.request.url == ME_URL]
    assert me_calls == []


@responses.activate
def test_list_alert_rules_handles_no_webhook_and_no_last_triggered(make_client):
    responses.add(
        responses.GET,
        ALERT_RULES_URL,
        json=[
            {
                "ruleId": "cccccccc-0000-0000-0000-000000000002",
                "customerId": CUSTOMER_ID,
                "tagName": "boiler.pressure",
                "name": "Low pressure",
                "description": None,
                "condition": "BelowThreshold",
                "threshold": 1.0,
                "threshold2": None,
                "webhookUrl": None,
                "hasWebhookSecret": False,
                "isEnabled": False,
                "cooldownSeconds": 300,
                "severity": "Warning",
                "createdAt": "2025-06-01T00:00:00Z",
                "lastTriggeredAt": None,
            }
        ],
        status=200,
    )

    client = make_client()
    rule = client.list_alert_rules()[0]

    assert rule.webhook_url is None
    assert rule.has_webhook_secret is False
    assert rule.is_enabled is False
    assert rule.last_triggered_at is None


@responses.activate
def test_list_active_alerts_403_from_a_scope_that_lacks_read_raises_api_error(make_client):
    responses.add(
        responses.GET,
        ACTIVE_ALERTS_URL,
        json={"error": "Forbidden"},
        status=403,
    )

    client = make_client()
    with pytest.raises(TagHistorianAPIError) as excinfo:
        client.list_active_alerts()

    assert excinfo.value.status_code == 403


# -- context manager closes the session -------------------------------------


def test_context_manager_closes_session(make_client):
    client = make_client()
    closed = {"value": False}
    original_close = client._session.close

    def _tracking_close():
        closed["value"] = True
        original_close()

    client._session.close = _tracking_close

    with client:
        pass

    assert closed["value"] is True
