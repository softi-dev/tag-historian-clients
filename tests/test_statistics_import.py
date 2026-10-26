"""The history import's first phase: Home Assistant's long-term statistics.

Statistics first, newest month first, in whole UTC months; each phase stops
for reasons of its own and never skips the other; a restart in either phase
carries on where it was; and the summary says which entities had hourly
history, since when, and which had none and why. The recorder's statistics
are a stand-in here (FakeStatistics) - tests/test_statistics.py holds the
stand-in to a real recorder.
"""

from __future__ import annotations

import gzip
import json
from datetime import UTC, datetime, timedelta
from itertools import pairwise
from typing import Any
from unittest.mock import AsyncMock

import pytest
from homeassistant.core import HomeAssistant, State
from homeassistant.helpers.storage import Store
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.tag_historian import importer as importer_module
from custom_components.tag_historian.api import (
    HoursBatchResult,
    ImportAllowance,
    ImportBatchResult,
    ImportOutcome,
    ImportSession,
    TagHistorianConnectionError,
)
from custom_components.tag_historian.const import (
    CONF_API_KEY,
    CONF_ENTITIES,
    CONF_HOST,
    CONF_MIN_INTERVAL,
    DOMAIN,
    MAX_HOURS_PER_REQUEST,
    STORAGE_VERSION,
)
from custom_components.tag_historian.importer import (
    NO_STATISTICS_SENTENCE,
    PHASE_READINGS,
    PHASE_STATISTICS,
    STATUS_FINISHED,
    STATUS_RUNNING,
    STATUS_STOPPED,
    STOP_ALLOWANCE,
    STOP_HOURS_ALLOWANCE,
    STOP_HOURS_UNSUPPORTED,
    STOP_UNREACHABLE,
    HistoryImporter,
)
from custom_components.tag_historian.statistics import month_start

POWER = "sensor.house_power"
ENERGY = "sensor.energy_meter"
DOOR = "binary_sensor.front_door"
NOW = datetime(2026, 10, 17, 12, 0, tzinfo=UTC)
KEY = "tag_historian.statistics_entry.import"
HOUR = 3600


class FakeRecorder:
    """The recorder's states, as test_importer.py fakes them."""

    def __init__(self) -> None:
        self.states: dict[str, list[State]] = {}

    def record(self, entity_id: str, value: str, when: datetime) -> None:
        self.states.setdefault(entity_id, []).append(
            State(entity_id, value, {}, last_changed=when, last_updated=when)
        )

    async def read(self, hass, entity_id, start, end) -> list[State]:
        return [s for s in self.states.get(entity_id, []) if start < s.last_updated < end]


class FakeStatistics:
    """The recorder's long-term statistics: metadata and hourly rows per statistic id."""

    def __init__(self) -> None:
        self.meta: dict[str, dict[str, Any]] = {}
        self.rows: dict[str, list[dict[str, Any]]] = {}
        self.reads: list[tuple[str, datetime, datetime]] = []

    def add_mean(self, statistic_id: str, first: datetime, last: datetime, unit: str = "W") -> None:
        """Hourly means from ``first`` up to (not including) ``last``."""
        self.meta[statistic_id] = {
            "statistic_id": statistic_id,
            "source": "recorder",
            "mean_type": 1,
            "has_sum": False,
            "unit_of_measurement": unit,
        }
        rows = self.rows.setdefault(statistic_id, [])
        stamp = first
        while stamp < last:
            value = float(stamp.hour)
            rows.append({"start": stamp.timestamp(), "mean": value, "min": value - 1, "max": value + 1})
            stamp += timedelta(hours=1)

    def add_sum(self, statistic_id: str, first: datetime, last: datetime, unit: str = "kWh") -> None:
        self.meta[statistic_id] = {
            "statistic_id": statistic_id,
            "source": "recorder",
            "mean_type": 0,
            "has_sum": True,
            "unit_of_measurement": unit,
        }
        rows = self.rows.setdefault(statistic_id, [])
        stamp, state = first, 100.0
        while stamp < last:
            state += 0.5
            rows.append({"start": stamp.timestamp(), "state": state, "sum": state, "change": 0.5})
            stamp += timedelta(hours=1)

    async def metadata(self, hass, statistic_ids: set[str]):
        from custom_components.tag_historian.statistics import info_from_metadata

        return {i: info_from_metadata(self.meta[i]) for i in statistic_ids if i in self.meta}

    async def read(self, hass, statistic_id, start, end, units, types):
        self.reads.append((statistic_id, start, end))
        return [
            row
            for row in self.rows.get(statistic_id, [])
            if start.timestamp() <= row["start"] < end.timestamp()
        ]


@pytest.fixture
def recorder(monkeypatch) -> FakeRecorder:
    fake = FakeRecorder()
    monkeypatch.setattr("custom_components.tag_historian.history.recorder_ready", lambda hass: True)
    monkeypatch.setattr("custom_components.tag_historian.history.async_recorded_states", fake.read)
    return fake


@pytest.fixture
def stats(monkeypatch, recorder) -> FakeStatistics:
    fake = FakeStatistics()
    monkeypatch.setattr("custom_components.tag_historian.statistics.async_metadata", fake.metadata)
    monkeypatch.setattr(
        "custom_components.tag_historian.statistics.async_recorded_statistics", fake.read
    )
    return fake


@pytest.fixture(autouse=True)
def no_waiting(monkeypatch) -> list[float]:
    slept: list[float] = []

    async def _sleep(seconds: float) -> None:
        slept.append(seconds)

    monkeypatch.setattr(importer_module.asyncio, "sleep", _sleep)
    return slept


@pytest.fixture
def notifications(monkeypatch) -> list[str]:
    said: list[str] = []
    monkeypatch.setattr(
        importer_module.persistent_notification,
        "async_create",
        lambda hass, message, **kwargs: said.append(message),
    )
    return said


@pytest.fixture
def entry(hass: HomeAssistant) -> MockConfigEntry:
    entry = MockConfigEntry(
        domain=DOMAIN,
        entry_id="statistics_entry",
        data={CONF_HOST: "api.taghistorian.com", CONF_API_KEY: "not-a-real-key"},
        options={CONF_ENTITIES: [POWER, ENERGY, DOOR], CONF_MIN_INTERVAL: 10},
        unique_id="11111111-2222-3333-4444-555555555555",
    )
    entry.add_to_hass(hass)
    return entry


def session(remaining: int = 1000) -> ImportSession:
    return ImportSession(
        import_id="imp-1",
        started_at=NOW,
        window_start=NOW - timedelta(days=14),
        max_readings_per_batch=5000,
        allowance=ImportAllowance(total=1000, used=0, remaining=remaining, readings_per_day=10, window_days=14),
    )


def hours_taken(stored: int, tag: str, line: datetime = NOW, **extra: Any) -> HoursBatchResult:
    return HoursBatchResult(
        ImportOutcome.TAKEN,
        stored=stored,
        importable_before={tag: line},
        oldest_allowed=datetime(2016, 11, 1, tzinfo=UTC),
        **extra,
    )


class Client:
    """Starts an import, and answers hours and readings batches as scripted.

    Every call is recorded in ``calls`` as ("hours", series) or ("readings",
    readings), in the order they were made: the order of the two phases is
    the first thing these tests are about.
    """

    def __init__(self, hours: list[Any] | None = None, readings: list[Any] | None = None) -> None:
        self.calls: list[tuple[str, Any]] = []
        self._hours = list(hours or [])
        self._readings = list(readings or [])
        self.async_start_import = AsyncMock(return_value=session())

    async def async_import_hours(self, import_id: str, gzipped: bytes):
        body = json.loads(gzip.decompress(gzipped))
        self.calls.append(("hours", body["series"]))
        answer = self._hours.pop(0) if self._hours else None
        if callable(answer):
            answer = answer(body["series"])
        if answer is None:
            series = body["series"][0]
            answer = hours_taken(len(series["hours"]), series["tagName"])
        if isinstance(answer, Exception):
            raise answer
        return answer

    async def async_import_batch(self, import_id: str, readings: list[dict[str, Any]]):
        self.calls.append(("readings", readings))
        answer = self._readings.pop(0) if self._readings else None
        if answer is None:
            answer = ImportBatchResult(ImportOutcome.TAKEN, stored=len(readings))
        if isinstance(answer, Exception):
            raise answer
        return answer

    def hours_batches(self, tag: str | None = None) -> list[list[dict[str, Any]]]:
        return [
            series["hours"]
            for kind, all_series in self.calls
            if kind == "hours"
            for series in all_series
            if tag is None or series["tagName"] == tag
        ]


def make_importer(hass, entry, client) -> HistoryImporter:
    return HistoryImporter(hass, entry, client, Store(hass, STORAGE_VERSION, KEY), 10)


async def _run(importer: HistoryImporter) -> None:
    assert importer._task is not None
    await importer._task


def _month_of(t: int) -> datetime:
    return month_start(datetime.fromtimestamp(t, UTC))


# --------------------------------------------------------------------------


async def test_the_statistics_go_first_newest_month_first_in_whole_months(
    hass: HomeAssistant, entry, stats, recorder, freezer
) -> None:
    """Eight months of a sensor's hours, then its readings: the years first, the newest of them first."""
    freezer.move_to(NOW)
    first = datetime(2026, 2, 1, tzinfo=UTC)
    stats.add_mean(POWER, first, NOW)
    hass.states.async_set(POWER, "1500", {"unit_of_measurement": "W", "state_class": "measurement"})
    recorder.record(POWER, "1500", NOW - timedelta(hours=5))

    client = Client()
    importer = make_importer(hass, entry, client)
    await importer.async_start([POWER], None, statistics=True)
    await _run(importer)

    kinds = [kind for kind, _ in client.calls]
    assert kinds.index("readings") > max(i for i, kind in enumerate(kinds) if kind == "hours"), (
        "every hours batch before the first readings batch"
    )
    client.async_start_import.assert_awaited_once_with(hours=True)

    batches = client.hours_batches(POWER)
    probe, *rest = batches
    assert [hour["t"] for hour in probe] == [int((NOW - timedelta(hours=1)).timestamp())], (
        "the newest hour alone first: its answer says where the history ends"
    )
    assert len(rest) >= 2, "eight months are more than one request holds"
    months_per_batch = [{_month_of(hour["t"]) for hour in batch} for batch in rest]
    for batch in rest:
        assert len(batch) <= MAX_HOURS_PER_REQUEST
    for earlier, later in pairwise(months_per_batch):
        assert min(earlier) > max(later), "newest months first, and no month in two requests"
    sent = sorted(hour["t"] for batch in rest for hour in batch)
    expected = [int(row["start"]) for row in stats.rows[POWER]]
    assert sent == expected, "every hour of every month, once"

    series = client.calls[0][1][0]
    assert series["tagName"] == POWER
    assert series["units"] == "W"
    assert series["statistic"] == {
        "statisticId": POWER,
        "source": "recorder",
        "stateClass": "measurement",
        "meanType": "arithmetic",
        "hasSum": False,
        "statisticsUnit": "W",
    }
    assert importer.progress.status == STATUS_FINISHED
    assert importer.progress.statistics_done == [POWER]
    assert importer.progress.hours_from[POWER] == first.timestamp()


async def test_a_meters_hours_carry_its_reading_and_change(
    hass: HomeAssistant, entry, stats, freezer
) -> None:
    freezer.move_to(NOW)
    stats.add_sum(ENERGY, NOW - timedelta(days=3), NOW)
    hass.states.async_set(ENERGY, "120", {"unit_of_measurement": "kWh", "state_class": "total_increasing"})

    client = Client()
    importer = make_importer(hass, entry, client)
    await importer.async_start([ENERGY], None, statistics=True)
    await _run(importer)

    hour = client.hours_batches(ENERGY)[-1][0]
    assert set(hour) == {"t", "state", "sum", "change"}
    assert hour["change"] == 0.5
    assert client.calls[0][1][0]["statistic"]["hasSum"] is True
    assert client.calls[0][1][0]["statistic"]["stateClass"] == "total_increasing"


async def test_an_entity_live_for_years_sends_only_what_is_older_than_its_line(
    hass: HomeAssistant, entry, stats, freezer
) -> None:
    """The probe's answer moves the line back; nothing that ends after it is sent."""
    freezer.move_to(NOW)
    stats.add_mean(POWER, datetime(2025, 1, 1, tzinfo=UTC), NOW)
    hass.states.async_set(POWER, "1500", {"unit_of_measurement": "W"})
    line = datetime(2025, 6, 10, 5, tzinfo=UTC)

    client = Client(hours=[hours_taken(0, POWER, line, refused_not_history=1)])
    importer = make_importer(hass, entry, client)
    await importer.async_start([POWER], None, statistics=True)
    await _run(importer)

    after_probe = client.hours_batches(POWER)[1:]
    sent = [hour["t"] for batch in after_probe for hour in batch]
    assert max(sent) + HOUR <= line.timestamp(), "nothing ending after the line"
    assert min(sent) == datetime(2025, 1, 1, tzinfo=UTC).timestamp()
    assert importer.progress.hours_skipped == 0, "the probe's refusal is the question, not a lost hour"


async def test_entities_without_statistics_or_in_a_unit_that_cannot_be_converted_are_named(
    hass: HomeAssistant, entry, stats, freezer, notifications
) -> None:
    freezer.move_to(NOW)
    stats.add_mean(POWER, NOW - timedelta(days=2), NOW, unit="kWh")
    hass.states.async_set(POWER, "21", {"unit_of_measurement": "°C"})  # changed since
    hass.states.async_set(DOOR, "off")

    client = Client()
    importer = make_importer(hass, entry, client)
    await importer.async_start([POWER, DOOR], None, statistics=True)
    await _run(importer)

    assert client.hours_batches() == [], "neither has statistics Tag Historian can take"
    assert importer.progress.no_statistics == [DOOR]
    assert "kWh" in importer.progress.statistics_left_out[POWER]
    summary = notifications[-1]
    assert f"No long-term statistics for: {DOOR}. {NO_STATISTICS_SENTENCE}" in summary
    assert f"The statistics of {POWER} were left out: its long-term statistics are in kWh" in summary


async def test_the_hourly_allowance_stops_the_statistics_but_not_the_readings(
    hass: HomeAssistant, entry, stats, recorder, freezer, notifications
) -> None:
    freezer.move_to(NOW)
    stats.add_mean(POWER, NOW - timedelta(days=60), NOW)
    hass.states.async_set(POWER, "1500", {"unit_of_measurement": "W"})
    recorder.record(POWER, "1500", NOW - timedelta(hours=5))

    client = Client(
        hours=[
            None,
            HoursBatchResult(
                ImportOutcome.HOURS_ALLOWANCE_SPENT,
                resumes_at=datetime(2026, 11, 2, tzinfo=UTC),
                message="Hourly history allowance used up",
            ),
        ]
    )
    importer = make_importer(hass, entry, client)
    await importer.async_start([POWER], None, statistics=True)
    await _run(importer)

    progress = importer.progress
    assert progress.statistics_stop_reason == STOP_HOURS_ALLOWANCE
    assert progress.phase == PHASE_READINGS
    assert progress.status == STATUS_FINISHED, "the readings went in all the same"
    assert progress.stored == 1
    assert (
        "Home Assistant's hourly history goes in at up to ten years per tag every 30 days; the "
        "rest can go in after 2026-11-02, by running the import again."
    ) in notifications[-1]


async def test_a_readings_stop_does_not_take_the_statistics_with_it(
    hass: HomeAssistant, entry, stats, recorder, freezer, notifications
) -> None:
    freezer.move_to(NOW)
    stats.add_mean(POWER, NOW - timedelta(days=20), NOW)
    hass.states.async_set(POWER, "1500", {"unit_of_measurement": "W"})
    recorder.record(POWER, "1500", NOW - timedelta(hours=5))

    client = Client(readings=[ImportBatchResult(ImportOutcome.ALLOWANCE_SPENT, message="spent")])
    importer = make_importer(hass, entry, client)
    await importer.async_start([POWER], None, statistics=True)
    await _run(importer)

    progress = importer.progress
    assert progress.status == STATUS_STOPPED
    assert progress.stop_reason == STOP_ALLOWANCE
    assert progress.statistics_done == [POWER]
    assert progress.hours_stored == 20 * 24 + 1, "the probe hour, then every hour - once each"
    assert "hours of Home Assistant's long-term statistics imported" in notifications[-1]
    assert "import allowance is used up" in notifications[-1]


async def test_a_tag_historian_without_the_hours_endpoint_goes_on_with_the_readings(
    hass: HomeAssistant, entry, stats, recorder, freezer, notifications
) -> None:
    freezer.move_to(NOW)
    stats.add_mean(POWER, NOW - timedelta(days=2), NOW)
    hass.states.async_set(POWER, "1500", {"unit_of_measurement": "W"})
    recorder.record(POWER, "1500", NOW - timedelta(hours=5))

    client = Client(hours=[HoursBatchResult(ImportOutcome.NOT_SUPPORTED)])
    importer = make_importer(hass, entry, client)
    await importer.async_start([POWER], None, statistics=True)
    await _run(importer)

    assert importer.progress.statistics_stop_reason == STOP_HOURS_UNSUPPORTED
    assert importer.progress.status == STATUS_FINISHED
    assert [kind for kind, _ in client.calls] == ["hours", "readings"]
    assert "cannot take them yet" in notifications[-1]


async def test_a_503_that_stored_part_of_a_batch_is_counted_once(
    hass: HomeAssistant, entry, stats, freezer, no_waiting
) -> None:
    freezer.move_to(NOW)
    stats.add_mean(POWER, NOW - timedelta(days=2), NOW)
    hass.states.async_set(POWER, "1500", {"unit_of_measurement": "W"})
    hours = 2 * 24

    client = Client(
        hours=[
            None,
            HoursBatchResult(ImportOutcome.RETRY, retry_after=60, stored=10),
            lambda series: hours_taken(hours - 10, POWER, duplicates=10),
        ]
    )
    importer = make_importer(hass, entry, client)
    await importer.async_start([POWER], None, statistics=True)
    await _run(importer)

    assert importer.progress.hours_stored == 1 + hours, "the probe, and the month: each hour once"
    assert importer.progress.hours_skipped == 0
    assert 60 in no_waiting


async def test_a_restart_in_the_statistics_phase_carries_on_from_the_month_before_its_cursor(
    hass: HomeAssistant, entry, stats, recorder, freezer, hass_storage
) -> None:
    freezer.move_to(NOW)
    stats.add_mean(POWER, datetime(2026, 6, 1, tzinfo=UTC), NOW)
    hass.states.async_set(POWER, "1500", {"unit_of_measurement": "W"})
    answered = datetime(2026, 8, 1, tzinfo=UTC)
    hass_storage[KEY] = {
        "version": STORAGE_VERSION,
        "minor_version": 1,
        "key": KEY,
        "data": {
            "status": STATUS_RUNNING,
            "import_id": "imp-before-restart",
            "started_at": NOW.isoformat(),
            "window_start": (NOW - timedelta(days=14)).isoformat(),
            "since": (NOW - timedelta(days=14)).isoformat(),
            "entities": [POWER],
            "statistics": True,
            "phase": PHASE_STATISTICS,
            "statistics_since": (NOW - timedelta(days=3650)).isoformat(),
            "hours_cursor": {POWER: answered.timestamp()},
            "hours_stored": 1500,
            "statistics_started_at": NOW.isoformat(),
        },
    }

    client = Client()
    importer = make_importer(hass, entry, client)
    await importer.async_load()
    await _run(importer)

    client.async_start_import.assert_not_awaited()
    sent = [hour["t"] for batch in client.hours_batches(POWER) for hour in batch]
    assert sent, "the rest of the statistics"
    assert max(sent) < answered.timestamp(), "no probe, and nothing Tag Historian answered for"
    assert min(sent) == datetime(2026, 6, 1, tzinfo=UTC).timestamp()
    assert importer.progress.import_id == "imp-before-restart"
    assert importer.progress.hours_stored == 1500 + len(sent)
    assert importer.progress.status == STATUS_FINISHED


async def test_a_restart_in_the_readings_phase_does_not_send_the_statistics_again(
    hass: HomeAssistant, entry, stats, recorder, freezer, hass_storage
) -> None:
    freezer.move_to(NOW)
    stats.add_mean(POWER, NOW - timedelta(days=30), NOW)
    hass.states.async_set(POWER, "1500", {"unit_of_measurement": "W"})
    recorder.record(POWER, "1", NOW - timedelta(hours=3))
    hass_storage[KEY] = {
        "version": STORAGE_VERSION,
        "minor_version": 1,
        "key": KEY,
        "data": {
            "status": STATUS_RUNNING,
            "import_id": "imp-before-restart",
            "started_at": NOW.isoformat(),
            "window_start": (NOW - timedelta(days=14)).isoformat(),
            "since": (NOW - timedelta(days=14)).isoformat(),
            "entities": [POWER],
            "statistics": True,
            "phase": PHASE_READINGS,
            "statistics_done": [POWER],
        },
    }

    client = Client()
    importer = make_importer(hass, entry, client)
    await importer.async_load()
    await _run(importer)

    assert [kind for kind, _ in client.calls] == ["readings"]
    assert importer.progress.status == STATUS_FINISHED


async def test_an_unreachable_tag_historian_pauses_the_import_and_the_next_start_carries_it_on(
    hass: HomeAssistant, entry, stats, freezer, notifications
) -> None:
    """Unreachable stops the whole import, in either phase; it is not the end of it."""
    freezer.move_to(NOW)
    stats.add_mean(POWER, NOW - timedelta(days=2), NOW)
    hass.states.async_set(POWER, "1500", {"unit_of_measurement": "W"})

    down = Client(hours=[TagHistorianConnectionError("down")] * 20)
    importer = make_importer(hass, entry, down)
    await importer.async_start([POWER], None, statistics=True)
    await _run(importer)
    assert importer.progress.status == STATUS_STOPPED
    assert importer.progress.stop_reason == STOP_UNREACHABLE
    assert importer.progress.phase == PHASE_STATISTICS
    assert "carries on where it stopped the next time Home Assistant starts" in notifications[-1]

    back = Client()
    restarted = make_importer(hass, entry, back)
    await restarted.async_load()
    await _run(restarted)

    back.async_start_import.assert_not_awaited()
    assert back.hours_batches(POWER), "carried on, under the same import"
    assert restarted.progress.status == STATUS_FINISHED
    assert restarted.progress.import_id == "imp-1"


async def test_a_readings_only_import_sends_no_statistics_and_keeps_when_they_were_brought(
    hass: HomeAssistant, entry, stats, recorder, freezer
) -> None:
    freezer.move_to(NOW)
    stats.add_mean(POWER, NOW - timedelta(days=2), NOW)
    hass.states.async_set(POWER, "1500", {"unit_of_measurement": "W"})

    importer = make_importer(hass, entry, Client())
    await importer.async_start([POWER], None, statistics=True)
    await _run(importer)
    brought = importer.progress.statistics_started_at
    assert brought is not None

    readings_only = Client()
    importer._client = readings_only
    await importer.async_start([POWER], None)
    await _run(importer)

    assert readings_only.hours_batches() == []
    readings_only.async_start_import.assert_awaited_once_with(hours=False)
    assert importer.progress.statistics_started_at == brought, (
        "kept from one import to the next, so the offer is not made again"
    )


async def test_an_import_saved_before_phases_loads_as_it_was(
    hass: HomeAssistant, entry, stats, recorder, freezer, hass_storage
) -> None:
    """A store an earlier version wrote has none of the statistics fields; it loads, as it was."""
    freezer.move_to(NOW)
    hass_storage[KEY] = {
        "version": STORAGE_VERSION,
        "minor_version": 1,
        "key": KEY,
        "data": {
            "status": STATUS_FINISHED,
            "import_id": "imp-old",
            "started_at": NOW.isoformat(),
            "entities": [POWER],
            "done": [POWER],
            "stored": 42,
        },
    }

    importer = make_importer(hass, entry, Client())
    await importer.async_load()

    progress = importer.progress
    assert progress.status == STATUS_FINISHED
    assert progress.stored == 42
    assert progress.statistics is False
    assert progress.hours_cursor == {}
    assert progress.statistics_started_at is None
    assert not importer.running
