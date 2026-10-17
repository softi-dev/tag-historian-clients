"""Status-code classification, and the two bits of parsing it depends on."""

from __future__ import annotations

import pytest
from homeassistant.core import HomeAssistant
from homeassistant.helpers.aiohttp_client import async_get_clientsession

from custom_components.tag_historian.api import (
    LineageOutcome,
    TagHistorianAuthError,
    TagHistorianClient,
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
