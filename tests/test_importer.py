"""Importing the history the recorder already holds.

What the import sends is what the live forwarder would have sent, oldest
first, with the original timestamps; it stops at each entity's line (the API
says where history ends), at the allowance, and at any refusal that waiting
will not change; and a restart carries it on under the same import.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest
from homeassistant.core import HomeAssistant, State
from homeassistant.exceptions import HomeAssistantError, ServiceValidationError
from homeassistant.helpers.service import async_get_all_descriptions
from homeassistant.helpers.storage import Store
from homeassistant.helpers.translation import async_get_translations
from homeassistant.util import dt as dt_util
from pytest_homeassistant_custom_component.common import (
    MockConfigEntry,
    async_fire_time_changed,
)

from custom_components.tag_historian import importer as importer_module
from custom_components.tag_historian.api import (
    ImportAllowance,
    ImportBatchResult,
    ImportOutcome,
    ImportSession,
    TagHistorianConnectionError,
    TagHistorianImportRefused,
    WriteOutcome,
    WriteResult,
)
from custom_components.tag_historian.const import (
    ATTR_DAYS,
    CONF_API_KEY,
    CONF_ENTITIES,
    CONF_HOST,
    CONF_MIN_INTERVAL,
    DOMAIN,
    SERVICE_IMPORT_HISTORY,
    STORAGE_VERSION,
)
from custom_components.tag_historian.forwarder import StateForwarder
from custom_components.tag_historian.importer import (
    STATUS_FINISHED,
    STATUS_RUNNING,
    STATUS_STOPPED,
    STOP_ALLOWANCE,
    STOP_UNREACHABLE,
    HistoryImporter,
    ImportAlreadyRunning,
)

from .conftest import make_client, without_statistics

POWER = "sensor.house_power"
TEMP = "sensor.outdoor_temperature"
NOW = datetime(2026, 10, 17, 12, 0, tzinfo=UTC)
WINDOW_START = NOW - timedelta(days=14)
KEY = "tag_historian.import_entry.import"
REAL_SLEEP = asyncio.sleep


class FakeRecorder:
    def __init__(self) -> None:
        self.states: dict[str, list[State]] = {}

    def record(self, entity_id: str, value: str, when: datetime, **attributes: Any) -> None:
        self.states.setdefault(entity_id, []).append(
            State(entity_id, value, attributes, last_changed=when, last_updated=when)
        )

    async def read(self, hass, entity_id, start, end) -> list[State]:
        return [s for s in self.states.get(entity_id, []) if start < s.last_updated < end]


@pytest.fixture
def recorder(monkeypatch) -> FakeRecorder:
    fake = FakeRecorder()
    monkeypatch.setattr(
        "custom_components.tag_historian.history.recorder_ready", lambda hass: True
    )
    monkeypatch.setattr(
        "custom_components.tag_historian.history.async_recorded_states", fake.read
    )
    without_statistics(monkeypatch)
    return fake


@pytest.fixture(autouse=True)
def no_waiting(monkeypatch) -> list[float]:
    """Retries sleep; the tests record how long instead of waiting it out."""
    slept: list[float] = []

    async def _sleep(seconds: float) -> None:
        slept.append(seconds)

    monkeypatch.setattr(importer_module.asyncio, "sleep", _sleep)
    return slept


@pytest.fixture
def entry(hass: HomeAssistant) -> MockConfigEntry:
    entry = MockConfigEntry(
        domain=DOMAIN,
        entry_id="import_entry",
        data={CONF_HOST: "api.taghistorian.com", CONF_API_KEY: "not-a-real-key"},
        options={CONF_ENTITIES: [POWER, TEMP], CONF_MIN_INTERVAL: 10},
        unique_id="11111111-2222-3333-4444-555555555555",
    )
    entry.add_to_hass(hass)
    return entry


def session(import_id: str = "imp-1") -> ImportSession:
    return ImportSession(
        import_id=import_id,
        started_at=NOW,
        window_start=WINDOW_START,
        max_readings_per_batch=5000,
        allowance=ImportAllowance(total=1000, used=0, remaining=1000, readings_per_day=10, window_days=14),
    )


def taken(stored: int, **extra: Any) -> ImportBatchResult:
    return ImportBatchResult(ImportOutcome.TAKEN, stored=stored, allowance_remaining=500, **extra)


class ImportClient:
    """Answers start with a session and every batch with what the script says."""

    def __init__(self, *answers: Any) -> None:
        self.batches: list[list[dict[str, Any]]] = []
        self._answers = list(answers)
        self.async_start_import = AsyncMock(return_value=session())

    async def async_import_batch(self, import_id: str, readings: list[dict[str, Any]]):
        self.batches.append(readings)
        answer = self._answers.pop(0) if self._answers else taken(len(readings))
        if callable(answer):
            answer = answer(readings)
        if isinstance(answer, Exception):
            raise answer
        return answer

    @property
    def sent(self) -> list[tuple[str, str, float]]:
        return [(r["tagName"], r["timestamp"], r["value"]) for batch in self.batches for r in batch]


def make_importer(hass, entry, client, min_interval: int = 10) -> HistoryImporter:
    return HistoryImporter(hass, entry, client, Store(hass, STORAGE_VERSION, KEY), min_interval)


async def _run(importer: HistoryImporter) -> None:
    task = importer._task
    assert task is not None
    await task


# --------------------------------------------------------------------------


@pytest.fixture
def notifications(monkeypatch) -> list[str]:
    """What the import tells the user, in order."""
    said: list[str] = []
    monkeypatch.setattr(
        importer_module.persistent_notification,
        "async_create",
        lambda hass, message, **kwargs: said.append(message),
    )
    return said


async def test_the_import_sends_what_the_live_path_would_have_oldest_first(
    hass: HomeAssistant, entry, recorder, freezer, notifications
) -> None:
    freezer.move_to(NOW)
    base = NOW - timedelta(days=2)
    recorder.record(POWER, "1500", base, friendly_name="House power", unit_of_measurement="W")
    recorder.record(POWER, "1510", base + timedelta(seconds=4))  # inside the minimum interval
    recorder.record(POWER, "unavailable", base + timedelta(seconds=20))  # a gap, not a zero
    recorder.record(POWER, "1520", base + timedelta(seconds=30, microseconds=700000))
    recorder.record(TEMP, "21.5", base + timedelta(hours=30))
    recorder.record(POWER, "1530", NOW - timedelta(days=20))  # outside the window

    client = ImportClient()
    importer = make_importer(hass, entry, client)
    await importer.async_start([POWER, TEMP], None)
    await _run(importer)

    assert client.sent == [
        ("sensor.house_power", "2026-10-15T12:00:00Z", 1500.0),
        ("sensor.house_power", "2026-10-15T12:00:30Z", 1520.0),
        ("sensor.outdoor_temperature", "2026-10-16T18:00:00Z", 21.5),
    ], "to the second, through the same throttle and the same idea of a reading as live"
    assert client.batches[0][0]["description"] == "House power"
    assert client.batches[0][0]["units"] == "W"
    assert [len(batch) for batch in client.batches[:1]] == [1], (
        "the first batch of an entity is its oldest reading alone: the answer says where its history ends"
    )

    progress = importer.progress
    assert progress.status == STATUS_FINISHED
    assert progress.done == [POWER, TEMP]
    assert progress.stored == 3

    assert "3 readings imported for 2 of 2 entities" in notifications[-1]
    assert "Progress is on" in notifications[0]


async def test_an_entity_already_live_ends_where_its_first_live_reading_is(
    hass: HomeAssistant, entry, recorder, freezer
) -> None:
    """The API says, in its first answer, where this entity's history ends."""
    freezer.move_to(NOW)
    first_live = NOW - timedelta(days=1)
    for hours in range(48, 0, -6):
        recorder.record(POWER, str(hours), NOW - timedelta(hours=hours))

    def answer(readings):
        return taken(len(readings), importable_before={POWER: first_live})

    client = ImportClient(*[answer] * 10)
    importer = make_importer(hass, entry, client)
    await importer.async_start([POWER], None)
    await _run(importer)

    stamps = [timestamp for _, timestamp, _ in client.sent]
    assert stamps, "the history before the line is imported"
    assert all(timestamp < "2026-10-16T12:00:00Z" for timestamp in stamps), (
        "nothing at or after the first live reading is even sent"
    )
    assert importer.progress.status == STATUS_FINISHED


async def test_an_entity_the_plan_has_no_tag_for_is_left_after_its_first_refusal(
    hass: HomeAssistant, entry, recorder, freezer, notifications
) -> None:
    """The tag limit refuses the probe; the rest of that entity is not sent to be refused too."""
    freezer.move_to(NOW)
    for hours in range(40, 0, -1):
        recorder.record(POWER, str(hours), NOW - timedelta(hours=hours))
    recorder.record(TEMP, "21.5", NOW - timedelta(hours=5))

    def refused(readings):
        if readings[0]["tagName"] == POWER:
            return taken(0, refused_tag_limit=len(readings), refused_tags=[POWER])
        return taken(len(readings))

    client = ImportClient(*[refused] * 10)
    importer = make_importer(hass, entry, client)
    await importer.async_start([POWER, TEMP], None)
    await _run(importer)

    assert [len(batch) for batch in client.batches] == [1, 1], "one probe each, nothing more for POWER"
    assert importer.progress.status == STATUS_FINISHED
    assert importer.progress.refused_tags == [POWER]
    assert "No tag could be created for: sensor.house_power" in notifications[-1]


async def test_readings_that_were_already_live_are_said_to_be_so(
    hass: HomeAssistant, entry, recorder, freezer, notifications
) -> None:
    freezer.move_to(NOW)
    recorder.record(POWER, "1", NOW - timedelta(hours=3))

    client = ImportClient(taken(0, refused_not_history=1, importable_before={POWER: NOW - timedelta(days=30)}))
    importer = make_importer(hass, entry, client)
    await importer.async_start([POWER], None)
    await _run(importer)

    assert importer.progress.not_history == 1
    assert "already your live data" in notifications[-1]


async def test_a_used_up_allowance_stops_the_import_and_says_why(
    hass: HomeAssistant, entry, recorder, freezer, notifications
) -> None:
    freezer.move_to(NOW)
    for minutes in range(10):
        recorder.record(POWER, str(minutes), NOW - timedelta(hours=5, minutes=-minutes))

    client = ImportClient(
        taken(1),
        taken(3, refused_allowance=6, message="6 of 9 readings were refused"),
    )
    importer = make_importer(hass, entry, client)
    await importer.async_start([POWER, TEMP], None)
    await _run(importer)

    progress = importer.progress
    assert progress.status == STATUS_STOPPED
    assert progress.stop_reason == STOP_ALLOWANCE
    assert progress.done == [], "stopped, not finished: neither entity is done"
    assert len(client.batches) == 2, "nothing more is sent once the allowance is spent"
    assert "import allowance is used up" in notifications[-1]
    assert "Live readings are not affected" in notifications[-1]


async def test_a_402_from_the_endpoint_stops_the_import_too(
    hass: HomeAssistant, entry, recorder, freezer
) -> None:
    freezer.move_to(NOW)
    recorder.record(POWER, "1", NOW - timedelta(hours=3))

    client = ImportClient(ImportBatchResult(ImportOutcome.ALLOWANCE_SPENT, message="Import allowance used up"))
    importer = make_importer(hass, entry, client)
    await importer.async_start([POWER], None)
    await _run(importer)

    assert importer.progress.status == STATUS_STOPPED
    assert importer.progress.stop_reason == STOP_ALLOWANCE


async def test_busy_answers_are_waited_out_and_the_same_batch_is_sent_again(
    hass: HomeAssistant, entry, recorder, freezer, no_waiting
) -> None:
    freezer.move_to(NOW)
    recorder.record(POWER, "1", NOW - timedelta(hours=3))

    client = ImportClient(
        ImportBatchResult(ImportOutcome.RETRY, retry_after=7),
        TagHistorianConnectionError("blip"),
        taken(1),
    )
    importer = make_importer(hass, entry, client)
    await importer.async_start([POWER], None)
    await _run(importer)

    assert importer.progress.status == STATUS_FINISHED
    assert len(client.batches) == 3
    assert client.batches[0] == client.batches[2]
    assert 7 in no_waiting, "Retry-After is honoured"


async def test_an_unreachable_api_stops_the_import_rather_than_spinning(
    hass: HomeAssistant, entry, recorder, freezer
) -> None:
    freezer.move_to(NOW)
    recorder.record(POWER, "1", NOW - timedelta(hours=3))

    client = ImportClient(*[TagHistorianConnectionError("down")] * 50)
    importer = make_importer(hass, entry, client)
    await importer.async_start([POWER], None)
    await _run(importer)

    assert importer.progress.status == STATUS_STOPPED
    assert importer.progress.stop_reason == STOP_UNREACHABLE


async def test_a_restart_carries_the_import_on_under_the_same_import(
    hass: HomeAssistant, entry, recorder, freezer, hass_storage
) -> None:
    """Where it was is saved after every batch; the next load picks it up."""
    freezer.move_to(NOW)
    stamps = [NOW - timedelta(hours=10, minutes=-m) for m in range(5)]
    for i, when in enumerate(stamps):
        recorder.record(POWER, str(i), when)

    hass_storage[KEY] = {
        "version": STORAGE_VERSION,
        "minor_version": 1,
        "key": KEY,
        "data": {
            "status": STATUS_RUNNING,
            "import_id": "imp-before-restart",
            "started_at": NOW.isoformat(),
            "window_start": WINDOW_START.isoformat(),
            "since": WINDOW_START.isoformat(),
            "entities": [POWER],
            "done": [],
            "cursor": {POWER: stamps[1].timestamp()},
            "stored": 2,
        },
    }

    client = ImportClient()
    importer = make_importer(hass, entry, client, min_interval=1)
    await importer.async_load()
    await _run(importer)

    client.async_start_import.assert_not_awaited()
    assert [value for _, _, value in client.sent] == [2.0, 3.0, 4.0], "after the cursor, not from the start"
    assert importer.progress.import_id == "imp-before-restart"
    assert importer.progress.stored == 5
    assert importer.progress.status == STATUS_FINISHED


async def test_what_the_live_forwarder_still_has_to_deliver_is_not_imported(
    hass: HomeAssistant, entry, recorder, freezer, monkeypatch
) -> None:
    """Readings waiting in the forwarder are the forwarder's, not the import's.

    The case: an account over its daily allowance (or with Tag Historian
    down) adds an entity, whose live readings then wait in the forwarder, and
    runs the import, which the daily allowance does not hold up. To Tag
    Historian the entity has no first live reading yet, so it holds the import
    only to the import's own start. Had the import sent those readings, the
    forwarder would have delivered each a second time at midnight, through an
    endpoint that does not deduplicate.
    """
    # The live path needs the loop to turn, and Home Assistant turns it with
    # asyncio.sleep(0) - which no_waiting has made return without yielding.
    # Nothing here retries, so nothing here waits.
    monkeypatch.setattr(importer_module.asyncio, "sleep", REAL_SLEEP)
    listening = NOW - timedelta(hours=2)
    freezer.move_to(listening)
    over = make_client()
    over.async_write.return_value = WriteResult(
        WriteOutcome.DAILY_QUOTA, retry_after=36000, message="spent"
    )
    forwarder = StateForwarder(hass, entry, over, [POWER], 10)
    forwarder.async_start()

    recorder.record(POWER, "1", NOW - timedelta(days=1))  # history from before
    for minutes in (10, 50, 90):
        freezer.move_to(listening + timedelta(minutes=minutes))
        hass.states.async_set(POWER, str(minutes))
        recorder.record(POWER, str(minutes), dt_util.utcnow())
        await hass.async_block_till_done()
        async_fire_time_changed(hass, dt_util.utcnow() + timedelta(seconds=2))
        await hass.async_block_till_done()
    assert forwarder.stats.queued == 3, "held: today's allowance is spent"

    freezer.move_to(NOW)
    client = ImportClient()
    importer = HistoryImporter(
        hass, entry, client, Store(hass, STORAGE_VERSION, KEY), 10, forwarder.history_boundary
    )
    await importer.async_start([POWER], None)
    await _run(importer)
    forwarder.async_stop()

    assert [value for _, _, value in client.sent] == [1.0], (
        "the history from before the forwarder started listening, and nothing it holds"
    )
    assert importer.progress.ends == {POWER: listening.timestamp()}
    assert importer.progress.status == STATUS_FINISHED


async def test_one_import_at_a_time(hass: HomeAssistant, entry, recorder, freezer) -> None:
    freezer.move_to(NOW)
    importer = make_importer(hass, entry, ImportClient())
    importer._task = hass.loop.create_future()  # an import in progress

    with pytest.raises(ImportAlreadyRunning):
        await importer.async_start([POWER], None)
    importer._task.cancel()


async def test_how_far_back_is_never_before_the_window(
    hass: HomeAssistant, entry, recorder, freezer
) -> None:
    freezer.move_to(NOW)
    importer = make_importer(hass, entry, ImportClient())

    await importer.async_start([POWER], 365)
    await _run(importer)
    assert importer.progress.since == WINDOW_START.isoformat()

    await importer.async_start([POWER], 2)
    await _run(importer)
    assert importer.progress.since == (NOW - timedelta(days=2)).isoformat()


# --------------------------------------------------------------------------
# The action
# --------------------------------------------------------------------------


@pytest.fixture
async def loaded(hass: HomeAssistant, entry, recorder):
    """The entry, set up for real, with a client that can import."""
    client = make_client()
    client.async_start_import.return_value = session()
    client.async_import_batch.return_value = taken(0)

    with patch("custom_components.tag_historian.TagHistorianClient", return_value=client):
        assert await hass.config_entries.async_setup(entry.entry_id)
        await hass.async_block_till_done()
        yield client
        assert await hass.config_entries.async_unload(entry.entry_id)
        await hass.async_block_till_done()


async def test_the_action_imports_the_selected_entities_by_default(
    hass: HomeAssistant, entry, loaded
) -> None:
    await hass.services.async_call(DOMAIN, SERVICE_IMPORT_HISTORY, {}, blocking=True)
    await hass.async_block_till_done()

    loaded.async_start_import.assert_awaited_once_with(hours=True)
    assert entry.runtime_data.importer.progress.entities == [POWER, TEMP]
    assert entry.runtime_data.importer.progress.statistics, "the long-term statistics first"
    sensor = hass.states.get("sensor.tag_historian_history_import")
    assert sensor is not None
    assert sensor.state == STATUS_FINISHED
    assert sensor.attributes["hours_stored"] == 0
    assert sensor.attributes["hours_skipped"] == 0


async def test_the_action_can_leave_the_statistics_out(hass: HomeAssistant, entry, loaded) -> None:
    await hass.services.async_call(
        DOMAIN, SERVICE_IMPORT_HISTORY, {"statistics": False, ATTR_DAYS: 3650}, blocking=True
    )
    await hass.async_block_till_done()

    loaded.async_start_import.assert_awaited_once_with(hours=False)
    assert not entry.runtime_data.importer.progress.statistics


async def test_the_action_refuses_an_entity_nobody_chose(
    hass: HomeAssistant, entry, loaded
) -> None:
    """An imported entity costs a tag like a forwarded one; it is chosen in the picker."""
    with pytest.raises(ServiceValidationError) as raised:
        await hass.services.async_call(
            DOMAIN,
            SERVICE_IMPORT_HISTORY,
            {"entity_id": ["sensor.not_chosen"], ATTR_DAYS: 3},
            blocking=True,
        )
    assert raised.value.translation_key == "not_selected"
    loaded.async_start_import.assert_not_awaited()


async def test_the_action_passes_on_the_apis_refusal(
    hass: HomeAssistant, entry, loaded
) -> None:
    loaded.async_start_import.side_effect = TagHistorianImportRefused(
        ImportOutcome.ALLOWANCE_SPENT, "This account has imported all its plan allows."
    )
    with pytest.raises(HomeAssistantError) as raised:
        await hass.services.async_call(DOMAIN, SERVICE_IMPORT_HISTORY, {}, blocking=True)
    assert raised.value.translation_key == "import_refused"
    assert raised.value.translation_placeholders == {
        "message": "This account has imported all its plan allows."
    }


async def test_without_a_recorder_there_is_nothing_to_import(
    hass: HomeAssistant, entry, loaded, monkeypatch
) -> None:
    monkeypatch.setattr(
        "custom_components.tag_historian.history.recorder_ready", lambda hass: False
    )
    with pytest.raises(ServiceValidationError) as raised:
        await hass.services.async_call(DOMAIN, SERVICE_IMPORT_HISTORY, {}, blocking=True)
    assert raised.value.translation_key == "no_recorder"


async def test_the_action_is_described_from_services_yaml_and_the_translations(
    hass: HomeAssistant, entry, loaded
) -> None:
    """What the Actions panel shows: a broken services.yaml shows nothing, silently.

    The name comes from the translations, which newer Home Assistants no
    longer copy into the descriptions - the frontend asks for them itself -
    so it is read where the panel reads it, on every version.
    """
    descriptions = await async_get_all_descriptions(hass)
    action = descriptions[DOMAIN][SERVICE_IMPORT_HISTORY]
    strings = await async_get_translations(hass, "en", "services", [DOMAIN])

    assert strings[f"component.{DOMAIN}.services.{SERVICE_IMPORT_HISTORY}.name"] == "Import history"
    assert set(action["fields"]) == {"config_entry_id", "entity_id", "statistics", "days"}
    assert action["fields"]["days"]["selector"]["number"]["min"] == 1
    assert action["fields"]["days"]["selector"]["number"]["max"] == 3650
