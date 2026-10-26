"""Status-code classification, and the two bits of parsing it depends on."""

from __future__ import annotations

import gzip
import json

import pytest
from homeassistant.core import HomeAssistant
from homeassistant.helpers.aiohttp_client import async_get_clientsession

from custom_components.tag_historian.api import (
    ImportOutcome,
    LineageOutcome,
    TagHistorianAuthError,
    TagHistorianClient,
    TagHistorianConnectionError,
    TagHistorianImportRefused,
    TagHistorianServerError,
    WriteOutcome,
    _classify_429,
    _parse_dropped_points,
    _parse_skipped_tags,
)

WRITE_URL = "https://api.taghistorian.com/api/v2/write?precision=s"
BODY = "°C,domain=sensor,entity_id=outdoor_temperature value=21.5 1786528800"


@pytest.fixture
def client(hass: HomeAssistant, aioclient_mock) -> TagHistorianClient:
    """A client on the session aioclient_mock actually intercepts."""
    return TagHistorianClient(
        async_get_clientsession(hass), "api.taghistorian.com", "not-a-real-key"
    )


@pytest.mark.parametrize(
    "given",
    [
        "api.taghistorian.com",
        "https://api.taghistorian.com",
        "https://api.taghistorian.com/",
        "  api.taghistorian.com  ",
    ],
)
def test_the_host_field_accepts_what_people_actually_paste(given) -> None:
    """The public Home Assistant guide has to say "host only - no https://"
    because the built-in integration builds the URL itself. Nobody reads that
    twice."""
    assert TagHistorianClient(None, given, "k").host == "api.taghistorian.com"


async def test_204_is_a_clean_write(client, aioclient_mock) -> None:
    aioclient_mock.post(WRITE_URL, status=204)

    result = await client.async_write(BODY)

    assert result.outcome is WriteOutcome.OK
    assert not result.retryable
    # precision=s, because Home Assistant timestamps to the second and
    # nanoseconds would be nine digits of invented precision per line.
    assert aioclient_mock.mock_calls[0][1].query["precision"] == "s"


async def test_422_names_the_refused_tags_and_is_not_retryable(
    client, aioclient_mock
) -> None:
    aioclient_mock.post(
        WRITE_URL,
        status=422,
        json={
            "code": "unprocessable entity",
            "message": (
                "2 points dropped: tag quota exceeded "
                "(new tags: sensor.a, sensor.b); points for existing tags were written"
            ),
        },
    )

    result = await client.async_write(BODY)

    assert result.outcome is WriteOutcome.PARTIAL_TAG_QUOTA
    assert result.skipped_tags == ["sensor.a", "sensor.b"]
    # Two names, and the API says two POINTS here. The two numbers agreeing in
    # this fixture is a coincidence of one line per tag; see
    # test_the_422_point_count_is_points_and_not_names.
    assert result.dropped_points == 2
    assert not result.retryable, "retrying a 422 duplicates the points that landed"


async def test_the_422_point_count_is_points_and_not_names(
    client, aioclient_mock
) -> None:
    """SkippedNewTagPoints counts points; SkippedTagNames counts names.

    Two refused entities contributing thirty lines each is 2 names and 60
    points. Counting the names reported 58 readings as delivered that never
    reached anything.
    """
    aioclient_mock.post(
        WRITE_URL,
        status=422,
        json={
            "code": "unprocessable entity",
            "message": (
                "60 points dropped: tag quota exceeded "
                "(new tags: sensor.a, sensor.b); points for existing tags were written"
            ),
        },
    )

    result = await client.async_write(BODY)

    assert len(result.skipped_tags) == 2
    assert result.dropped_points == 60


async def test_the_daily_quota_429_is_the_one_from_the_middleware(
    client, aioclient_mock
) -> None:
    """QuotaEnforcementMiddleware: Retry-After counts down to UTC midnight."""
    aioclient_mock.post(
        WRITE_URL,
        status=429,
        headers={"Retry-After": "3600"},
        json={"code": "too many requests", "message": "daily measurement quota exceeded"},
    )

    result = await client.async_write(BODY)

    assert result.outcome is WriteOutcome.DAILY_QUOTA
    assert result.retry_after == 3600
    assert result.retryable


async def test_the_back_pressure_429_is_not_a_quota_at_all(
    client, aioclient_mock
) -> None:
    """The other 429. Same status code, same ``code`` field, different thing.

    ``InfluxWriteController.RejectWrite`` answers this when the write buffer is
    refusing a customer's fair share, and ``MemoryBufferService`` sets
    Retry-After to five seconds plus jitter. Reading it as the daily quota is
    what turned a seven-second hiccup into a card about billing.
    """
    aioclient_mock.post(
        WRITE_URL,
        status=429,
        headers={"Retry-After": "7"},
        json={
            "code": "too many requests",
            "message": "write buffer fair-share exceeded; retry after 7s",
        },
    )

    result = await client.async_write(BODY)

    assert result.outcome is WriteOutcome.BACK_PRESSURE
    assert result.outcome is not WriteOutcome.DAILY_QUOTA
    assert result.retry_after == 7
    assert result.retryable


def test_an_unrecognised_429_is_read_as_back_pressure() -> None:
    """The default has to fall away from the billing claim.

    Being wrong towards "busy" costs a card that says busy during a real quota
    pause - and the pause still happens, because Retry-After is honoured
    either way. Being wrong towards "quota" tells a paying customer their
    allowance is gone and points them at a bigger plan.
    """
    assert _classify_429("some future wording") is WriteOutcome.BACK_PRESSURE
    assert _classify_429("") is WriteOutcome.BACK_PRESSURE
    assert _classify_429("Daily Measurement Quota Exceeded") is WriteOutcome.DAILY_QUOTA


async def test_503_is_retryable_and_carries_its_own_retry_after(
    client, aioclient_mock
) -> None:
    aioclient_mock.post(
        WRITE_URL,
        status=503,
        headers={"Retry-After": "30"},
        json={"code": "unavailable", "message": "retry after 30s"},
    )

    result = await client.async_write(BODY)

    assert result.outcome is WriteOutcome.UNAVAILABLE
    assert result.retry_after == 30
    assert result.retryable


async def test_403_is_permanent_and_keeps_the_apis_own_wording(
    client, aioclient_mock
) -> None:
    aioclient_mock.post(
        WRITE_URL,
        status=403,
        json={
            "error": "Email address not verified",
            "message": "Confirm your email address to keep writing data.",
        },
    )

    result = await client.async_write(BODY)

    assert result.outcome is WriteOutcome.FORBIDDEN
    assert not result.retryable
    assert result.message == "Confirm your email address to keep writing data."


async def test_401_raises_rather_than_returning(client, aioclient_mock) -> None:
    """It has to reach the coordinator and the forwarder as a distinct thing:
    only reauth can fix it, and only Home Assistant can show reauth."""
    aioclient_mock.post(WRITE_URL, status=401)

    with pytest.raises(TagHistorianAuthError):
        await client.async_write(BODY)


async def test_quota_comes_from_the_api_and_nowhere_else(
    client, aioclient_mock
) -> None:
    """Both calls, because /limits carries the daily LIMIT but not the COUNT."""
    aioclient_mock.get(
        "https://api.taghistorian.com/api/usage/limits",
        json={
            "tagLimit": 12,
            "currentTagCount": 5,
            "measurementsPerDayLimit": 120000,
            "storageLimitBytes": 1000,
            "currentStorageBytes": 10,
        },
    )
    aioclient_mock.get(
        "https://api.taghistorian.com/api/usage/daily?days=1",
        json={"dailyUsage": [{"date": "2026-08-12", "measurementCount": 4321}]},
    )

    quota = await client.async_get_quota()

    assert quota.tag_limit == 12
    assert quota.current_tag_count == 5
    assert quota.tags_available == 7
    assert quota.measurements_per_day_limit == 120000
    assert quota.measurements_today == 4321


async def test_existing_tag_names_come_back_as_a_set(client, aioclient_mock) -> None:
    """One page covers every tier - the endpoint clamps take to 1000 and the
    largest plan holds fewer tags than that."""
    aioclient_mock.get(
        "https://api.taghistorian.com/api/tags?take=1000",
        json={"totalCount": 2, "tags": [{"tagName": "sensor.a"}, {"tagName": "sensor.b"}]},
    )

    assert await client.async_get_tag_names() == {"sensor.a", "sensor.b"}


async def test_former_names_exist_too_and_say_which_tag_they_belong_to(
    client, aioclient_mock
) -> None:
    """A former name is not a tag, but readings sent under it are stored in
    the tag it belongs to - so it costs no slot to select, and an entity whose
    old id is a former name of its new id has been renamed already."""
    aioclient_mock.get(
        "https://api.taghistorian.com/api/tags?take=1000",
        json={
            "totalCount": 2,
            "tags": [
                {"tagName": "sensor.energy", "formerNames": ["sensor.energy_2", "sensor.meter"]},
                {"tagName": "sensor.b", "formerNames": []},
            ],
        },
    )

    assert await client.async_get_tag_index() == {
        "sensor.energy": "sensor.energy",
        "sensor.energy_2": "sensor.energy",
        "sensor.meter": "sensor.energy",
        "sensor.b": "sensor.b",
    }
    assert await client.async_get_tag_names() == {
        "sensor.energy", "sensor.energy_2", "sensor.meter", "sensor.b"
    }


def test_skipped_tag_parsing_fails_soft() -> None:
    """It is scraping prose out of an English sentence, so it will break one
    day. When it does the repair issue must still appear, just unnamed."""
    assert _parse_skipped_tags("(new tags: a.b, c.d); rest written") == ["a.b", "c.d"]
    assert _parse_skipped_tags("some future wording") == []
    assert _parse_skipped_tags("") == []


def test_a_missing_point_count_is_none_and_not_zero() -> None:
    """Zero would mean "the whole batch landed", which a 422 never means.

    The caller has to be able to tell "the API said none" from "the API did
    not say", because those two lead to opposite counters.
    """
    assert _parse_dropped_points("7 points dropped: tag quota exceeded") == 7
    assert _parse_dropped_points("0 points dropped: tag quota exceeded") == 0
    assert _parse_dropped_points("tag quota exceeded (new tags: a.b)") is None
    assert _parse_dropped_points("") is None


# --------------------------------------------------------------------------
# The history import endpoints
# --------------------------------------------------------------------------

IMPORT_URL = "https://api.taghistorian.com/api/import"
BATCH_URL = "https://api.taghistorian.com/api/import/imp-1/readings"
READING = {"tagName": "sensor.house_power", "timestamp": "2026-10-15T12:00:00Z", "value": 1500.0}


async def test_starting_an_import_reads_its_line_and_the_allowance(client, aioclient_mock) -> None:
    aioclient_mock.post(
        IMPORT_URL,
        status=201,
        json={
            "importId": "imp-1",
            "startedAt": "2026-10-17T12:00:00.1234567Z",
            "windowStart": "2026-10-03T12:00:00Z",
            "maxReadingsPerBatch": 5000,
            "allowance": {"total": 900, "used": 100, "remaining": 800, "readingsPerDay": 90, "windowDays": 10},
        },
    )

    started = await client.async_start_import()

    assert started.import_id == "imp-1"
    assert started.started_at.year == 2026 and started.started_at.tzinfo is not None
    assert started.allowance.remaining == 800
    assert started.allowance.window_days == 10
    assert aioclient_mock.mock_calls[0][2] == {"source": "home-assistant"}


@pytest.mark.parametrize(
    ("status", "error", "outcome"),
    [
        (402, "Import allowance used up", ImportOutcome.ALLOWANCE_SPENT),
        (402, "Storage quota exceeded", ImportOutcome.STORAGE_FULL),
        (403, "Email address not verified", ImportOutcome.FORBIDDEN),
    ],
)
async def test_an_import_that_cannot_start_says_why(
    client, aioclient_mock, status, error, outcome
) -> None:
    aioclient_mock.post(IMPORT_URL, status=status, json={"error": error, "message": f"{error}."})

    with pytest.raises(TagHistorianImportRefused) as refused:
        await client.async_start_import()

    assert refused.value.outcome is outcome
    assert refused.value.message == f"{error}."


async def test_a_batch_answer_is_read_field_by_field(client, aioclient_mock) -> None:
    aioclient_mock.post(
        BATCH_URL,
        status=200,
        json={
            "received": 10,
            "stored": 4,
            "compressed": 1,
            "duplicates": 2,
            "outsideWindow": 1,
            "refusedTotal": 2,
            "refused": {"notHistory": 1, "tagLimit": 1, "allowance": 0, "storage": 0},
            "refusedTags": ["sensor.extra"],
            "importableBefore": {"sensor.house_power": "2026-10-16T08:00:00Z"},
            "allowance": {"remaining": 796},
            "message": "2 of 10 readings were refused",
        },
    )

    result = await client.async_import_batch("imp-1", [READING])

    assert result.outcome is ImportOutcome.TAKEN
    assert (result.stored, result.compressed, result.duplicates, result.outside_window) == (4, 1, 2, 1)
    assert result.refused == 2
    assert result.refused_tags == ["sensor.extra"]
    assert result.importable_before["sensor.house_power"].hour == 8
    assert result.allowance_remaining == 796
    assert aioclient_mock.mock_calls[0][2] == {"readings": [READING]}


@pytest.mark.parametrize(
    ("status", "body", "outcome"),
    [
        (402, {"error": "Import allowance used up"}, ImportOutcome.ALLOWANCE_SPENT),
        (402, {"error": "Storage quota exceeded"}, ImportOutcome.STORAGE_FULL),
        (403, {"error": "Account scheduled for deletion"}, ImportOutcome.FORBIDDEN),
        (404, {"error": "No import with that id on this account"}, ImportOutcome.UNKNOWN_IMPORT),
        (400, {"error": "Every imported reading needs its timestamp"}, ImportOutcome.REJECTED),
    ],
)
async def test_every_batch_refusal_is_its_own_outcome(
    client, aioclient_mock, status, body, outcome
) -> None:
    aioclient_mock.post(BATCH_URL, status=status, json=body)

    result = await client.async_import_batch("imp-1", [READING])

    assert result.outcome is outcome


@pytest.mark.parametrize("status", [429, 503])
async def test_a_busy_import_endpoint_is_a_retry_with_its_wait(client, aioclient_mock, status) -> None:
    aioclient_mock.post(
        BATCH_URL, status=status, json={"error": "import_unavailable"}, headers={"Retry-After": "15"}
    )

    result = await client.async_import_batch("imp-1", [READING])

    assert result.outcome is ImportOutcome.RETRY
    assert result.retry_after == 15


async def test_a_500_from_the_import_endpoint_is_retried_like_a_socket_error(
    client, aioclient_mock
) -> None:
    aioclient_mock.post(BATCH_URL, status=500, json={"error": "An internal error occurred."})

    with pytest.raises(TagHistorianConnectionError):
        await client.async_import_batch("imp-1", [READING])


async def test_an_import_that_brings_hours_says_so_when_it_starts(client, aioclient_mock) -> None:
    """The readings allowance does not come back once spent; an import with hours starts anyway."""
    aioclient_mock.post(
        IMPORT_URL,
        status=201,
        json={
            "importId": "imp-1",
            "startedAt": "2026-10-17T12:00:00Z",
            "windowStart": "2026-10-03T12:00:00Z",
            "allowance": {"total": 900, "used": 900, "remaining": 0},
        },
    )

    started = await client.async_start_import(hours=True)

    assert aioclient_mock.mock_calls[0][2] == {"source": "home-assistant", "hours": True}
    assert started.allowance.remaining == 0


# --------------------------------------------------------------------------
# The hours endpoint (the statistics phase)
# --------------------------------------------------------------------------

HOURS_URL = "https://api.taghistorian.com/api/import/imp-1/hours"


async def test_an_hours_batch_goes_gzipped_and_its_answer_is_read_field_by_field(
    client, aioclient_mock
) -> None:
    """What the endpoint says about each hour, under the names ImportController gives them."""
    aioclient_mock.post(
        HOURS_URL,
        status=200,
        json={
            "importId": "imp-1",
            "received": 20,
            "stored": 9,
            "duplicates": 2,
            "outsideRetention": 1,
            "negativeChange": 1,
            "refused": {
                "notHistory": 1,
                "tagLimit": 0,
                "unitMismatch": 3,
                "invalid": 1,
                "unsupported": 0,
                "hoursAllowance": 2,
                "unwritable": 1,
            },
            "refusedTags": [],
            "importableBefore": {"sensor.house_power": "2026-10-16T08:00:00Z"},
            "oldestAllowed": "2016-11-01T00:00:00Z",
            "hoursAllowance": {"limit": 100, "usedLast30Days": 100, "remaining": 0, "resumesAt": "2026-11-02T00:00:00Z"},
        },
    )
    body = {"series": [{"tagName": "sensor.house_power", "hours": [{"t": 3600, "mean": 1, "min": 0, "max": 2}]}]}

    result = await client.async_import_hours("imp-1", gzip.compress(json.dumps(body).encode()))

    assert result.outcome is ImportOutcome.TAKEN
    assert (result.stored, result.duplicates, result.outside_retention, result.negative_change) == (9, 2, 1, 1)
    assert result.refused_unit_mismatch == 3
    assert result.refused_hours_allowance == 2
    assert result.refused == 1 + 3 + 1 + 2 + 1
    assert result.skipped == 2 + 1 + result.refused
    assert result.importable_before["sensor.house_power"].hour == 8
    assert result.oldest_allowed is not None and result.oldest_allowed.year == 2016
    assert result.resumes_at is not None and result.resumes_at.month == 11

    _, _, sent, headers = aioclient_mock.mock_calls[0]
    assert headers["Content-Encoding"] == "gzip"
    assert json.loads(gzip.decompress(sent)) == body, "gzip on the wire, the batch inside"


async def test_the_hourly_allowance_is_its_own_402(client, aioclient_mock) -> None:
    """Not the readings' allowance: it ends the statistics phase only, and says when more goes in."""
    aioclient_mock.post(
        HOURS_URL,
        status=402,
        json={
            "error": "Hourly history allowance used up",
            "code": "hours_allowance",
            "resumesAt": "2026-11-02T00:00:00Z",
            "message": "This account has stored 100 hours of hourly history in the last 30 days.",
        },
    )

    result = await client.async_import_hours("imp-1", b"")

    assert result.outcome is ImportOutcome.HOURS_ALLOWANCE_SPENT
    assert result.resumes_at is not None and result.resumes_at.day == 2


@pytest.mark.parametrize(
    ("status", "body", "outcome"),
    [
        (402, {"error": "Import allowance used up"}, ImportOutcome.ALLOWANCE_SPENT),
        (403, {"error": "Account scheduled for deletion"}, ImportOutcome.FORBIDDEN),
        (404, {"error": "No import with that id on this account"}, ImportOutcome.UNKNOWN_IMPORT),
        (400, {"error": "series is empty"}, ImportOutcome.REJECTED),
        (413, {"error": "at most 5000 hours per request"}, ImportOutcome.REJECTED),
    ],
)
async def test_every_hours_refusal_is_read_as_the_readings_endpoints_are(
    client, aioclient_mock, status, body, outcome
) -> None:
    aioclient_mock.post(HOURS_URL, status=status, json=body)

    assert (await client.async_import_hours("imp-1", b"")).outcome is outcome


async def test_a_tag_historian_without_the_hours_endpoint_is_not_an_unknown_import(
    client, aioclient_mock
) -> None:
    """A route that is not there answers 404 with no body; "no such import" has one."""
    aioclient_mock.post(HOURS_URL, status=404)

    assert (await client.async_import_hours("imp-1", b"")).outcome is ImportOutcome.NOT_SUPPORTED


async def test_a_404_page_from_something_in_front_of_tag_historian_is_not_its_answer(
    client, aioclient_mock
) -> None:
    """An ingress's HTML 404 for a stopped API is retried as unreachable; it does not end the phase for good."""
    aioclient_mock.post(
        HOURS_URL,
        status=404,
        text="<html><body>404 - this container app is stopped</body></html>",
        headers={"Content-Type": "text/html"},
    )

    with pytest.raises(TagHistorianConnectionError):
        await client.async_import_hours("imp-1", b"")


@pytest.mark.parametrize("status", [502, 504])
async def test_a_gateway_that_lost_the_api_is_unreachable_not_a_server_error(
    client, aioclient_mock, status
) -> None:
    aioclient_mock.post(BATCH_URL, status=status)

    with pytest.raises(TagHistorianConnectionError) as raised:
        await client.async_import_batch("imp-1", [READING])
    assert not isinstance(raised.value, TagHistorianServerError)


async def test_a_500_from_the_hours_endpoint_is_a_server_error(client, aioclient_mock) -> None:
    """Retried as a lost connection is, but told apart: the importer does not carry it on by itself."""
    aioclient_mock.post(HOURS_URL, status=500, json={"error": "An internal error occurred."})

    with pytest.raises(TagHistorianServerError):
        await client.async_import_hours("imp-1", b"")


async def test_a_503_that_stored_part_of_the_batch_says_how_much(client, aioclient_mock) -> None:
    """The resend finds those hours as duplicates; the importer counts them as stored, once."""
    aioclient_mock.post(
        HOURS_URL,
        status=503,
        json={"error": "import_unavailable", "reason": "months_not_ready", "stored": 24},
        headers={"Retry-After": "60"},
    )

    result = await client.async_import_hours("imp-1", b"")

    assert result.outcome is ImportOutcome.RETRY
    assert result.retry_after == 60
    assert result.stored == 24


# --------------------------------------------------------------------------
# Renaming and merging tags (renames.py)
# --------------------------------------------------------------------------

RENAME_URL = "https://api.taghistorian.com/api/tags/rename"
MERGE_URL = "https://api.taghistorian.com/api/tags/merge"


async def test_a_rename_sends_the_two_names_and_200_is_done(client, aioclient_mock) -> None:
    """200 is also what a rename that is already done answers, so a retry is safe."""
    aioclient_mock.post(RENAME_URL, status=200, json={"applied": False, "alreadyDone": True})

    result = await client.async_rename_tag("sensor.energy_2", "sensor.energy")

    assert result.outcome is LineageOutcome.DONE
    assert aioclient_mock.mock_calls[0][2] == {"from": "sensor.energy_2", "to": "sensor.energy"}


async def test_a_merge_sends_source_and_target(client, aioclient_mock) -> None:
    aioclient_mock.post(MERGE_URL, status=200, json={"applied": True})

    result = await client.async_merge_tags("sensor.energy_2", "sensor.energy")

    assert result.outcome is LineageOutcome.DONE
    assert aioclient_mock.mock_calls[0][2] == {"source": "sensor.energy_2", "target": "sensor.energy"}


@pytest.mark.parametrize(
    ("status", "body", "expected"),
    [
        # The new name is a tag already: what turns a rename into a merge.
        (409, {"error": "exists", "code": "target_exists", "details": "A tag named 'x' already exists."},
         LineageOutcome.TARGET_EXISTS),
        # No tag under the old name: nothing was ever stored, nothing to carry.
        (404, {"error": "Tag not found", "code": "not_found", "details": "There is no tag named 'x'."},
         LineageOutcome.NOT_FOUND),
        # The deploy handover, or any other moment the API asks to wait.
        (503, {"error": "switching over", "code": "switching_over", "details": "try again"},
         LineageOutcome.UNAVAILABLE),
        # Everything else is the user's to decide, with the API's own words.
        (409, {"error": "former name", "code": "former_name_of_another_tag", "details": "It is a former name."},
         LineageOutcome.REFUSED),
        (402, {"error": "quota", "code": "tag_quota_exceeded", "details": "No tag free."},
         LineageOutcome.REFUSED),
        (403, {"error": "Forbidden"}, LineageOutcome.REFUSED),
    ],
)
async def test_a_rename_answer_is_classified_by_what_happens_next(
    client, aioclient_mock, status, body, expected
) -> None:
    aioclient_mock.post(RENAME_URL, status=status, json=body)

    result = await client.async_rename_tag("sensor.energy_2", "sensor.energy")

    assert result.outcome is expected
    # The sentence written for a person, where there is one.
    assert result.message == body.get("details", body.get("error"))


async def test_a_rename_with_a_revoked_key_asks_for_a_new_one(client, aioclient_mock) -> None:
    aioclient_mock.post(RENAME_URL, status=401)

    with pytest.raises(TagHistorianAuthError):
        await client.async_rename_tag("sensor.energy_2", "sensor.energy")


async def test_a_rename_that_gets_no_answer_is_tried_again_later(client, aioclient_mock) -> None:
    aioclient_mock.post(RENAME_URL, exc=TimeoutError())

    result = await client.async_rename_tag("sensor.energy_2", "sensor.energy")

    assert result.outcome is LineageOutcome.UNAVAILABLE


@pytest.mark.parametrize("status", [429, 500, 502, 504])
async def test_a_busy_or_broken_gateway_is_tried_again_and_never_a_refusal(
    client, aioclient_mock, status
) -> None:
    """These were REFUSED: a gateway hiccup during a deploy became a permanent
    card saying "/api/tags/rename answered 502", and with "Rename tags when
    entities are renamed" on, the rename was never asked for again."""
    aioclient_mock.post(RENAME_URL, status=status)

    result = await client.async_rename_tag("sensor.energy_2", "sensor.energy")

    assert result.outcome is LineageOutcome.UNAVAILABLE


async def test_a_tag_historian_without_renames_says_so_in_words(client, aioclient_mock) -> None:
    """A bare 404 - no code - is a route that does not exist: an API older
    than this integration. The card quotes the message, so it is a sentence."""
    aioclient_mock.post(RENAME_URL, status=404)

    result = await client.async_rename_tag("sensor.energy_2", "sensor.energy")

    assert result.outcome is LineageOutcome.REFUSED
    assert "cannot rename or merge tags yet" in result.message
    assert "404" not in result.message


async def test_a_key_that_may_not_rename_says_which_key_can(client, aioclient_mock) -> None:
    aioclient_mock.post(MERGE_URL, status=403)

    result = await client.async_merge_tags("sensor.energy_2", "sensor.energy")

    assert result.outcome is LineageOutcome.REFUSED
    assert "Write scope" in result.message


PROPOSAL_URL = "https://api.taghistorian.com/api/account/settings/proposal"


async def test_the_place_is_proposed_as_given_and_named_as_home_assistants(
    client, aioclient_mock
) -> None:
    aioclient_mock.post(PROPOSAL_URL, status=200, json={"place": {"lat": 59.3, "lon": 18.1}})

    assert await client.async_propose_place(59.3, 18.1) is True

    body = aioclient_mock.mock_calls[0][2]
    assert body == {"place": {"lat": 59.3, "lon": 18.1}, "source": "ha"}


async def test_a_tag_historian_without_the_weather_is_not_an_error(client, aioclient_mock) -> None:
    """An older server answers 404; nothing depends on the proposal."""
    aioclient_mock.post(PROPOSAL_URL, status=404)

    assert await client.async_propose_place(59.3, 18.1) is None


async def test_a_refused_key_on_the_proposal_is_the_auth_error(client, aioclient_mock) -> None:
    aioclient_mock.post(PROPOSAL_URL, status=401)

    with pytest.raises(TagHistorianAuthError):
        await client.async_propose_place(59.3, 18.1)


async def test_the_time_zone_is_proposed_as_home_assistants(client, aioclient_mock) -> None:
    """The same endpoint as the place, and like the place only ever a proposal."""
    aioclient_mock.post(PROPOSAL_URL, status=200, json={"timeZone": {"id": "Europe/Stockholm"}})

    assert await client.async_propose_time_zone("Europe/Stockholm") is True

    assert aioclient_mock.mock_calls[0][2] == {"timeZone": "Europe/Stockholm", "source": "ha"}
