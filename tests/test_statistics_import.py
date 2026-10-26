"""The history import's first phase: Home Assistant's long-term statistics.

Statistics first, newest month first, in whole UTC months; each phase stops
for reasons of its own and never skips the other; a restart in either phase
carries on where it was; and the summary says which entities had hourly
history, since when, and which had none and why. A renamed entity's
statistics go where its readings go, and what the phase saved follows the
rename. The recorder's statistics are a stand-in here (FakeStatistics) -
tests/test_statistics.py holds the stand-in to a real recorder.
"""

from __future__ import annotations

import gzip
import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from itertools import pairwise
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from homeassistant.const import EVENT_HOMEASSISTANT_STARTED
from homeassistant.core import CoreState, HomeAssistant, State
from homeassistant.helpers import entity_registry as er
from homeassistant.helpers.storage import Store
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.tag_historian import importer as importer_module
from custom_components.tag_historian.api import (
    HoursBatchResult,
    ImportAllowance,
    ImportBatchResult,
    ImportOutcome,
    ImportSession,
    TagHistorianAuthError,
    TagHistorianConnectionError,
    TagHistorianServerError,
)
from custom_components.tag_historian.const import (
    CONF_API_KEY,
    CONF_ENTITIES,
    CONF_HOST,
    CONF_MIN_INTERVAL,
    CONF_SEND_AS,
    DOMAIN,
    IMPORT_BATCH_SIZE,
    IMPORT_MAX_CONNECTION_FAILURES,
    IMPORT_MAX_RESUMES_ON_LOAD,
    IMPORT_MAX_RETRY_SECONDS,
    IMPORT_MAX_RETRY_WAIT_SECONDS,
    MAX_HOURS_PER_REQUEST,
    SERVICE_IMPORT_HISTORY,
    STORAGE_VERSION,
)
from custom_components.tag_historian.importer import (
    HOURS_UNAFFECTED_SENTENCE,
    KEY_REFUSED_AGAIN_SENTENCE,
    NO_STATISTICS_SENTENCE,
    OFFERED_AGAIN_SENTENCE,
    PHASE_READINGS,
    PHASE_STATISTICS,
    STATUS_FINISHED,
    STATUS_RUNNING,
    STATUS_STOPPED,
    STOP_ALLOWANCE,
    STOP_AUTH,
    STOP_FAILED,
    STOP_FORBIDDEN,
    STOP_HOURS_ALLOWANCE,
    STOP_HOURS_UNSUPPORTED,
    STOP_STORAGE,
    STOP_UNREACHABLE,
    UNREACHABLE_AGAIN_SENTENCE,
    HistoryImporter,
)
from custom_components.tag_historian.statistics import month_start

from .conftest import make_client

POWER = "sensor.house_power"
ENERGY = "sensor.energy_meter"
# The ids two of them have once renamed.
RENAMED = "sensor.house_power_2"
ENERGY_2 = "sensor.energy_meter_2"
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
    the first thing these tests are about. Unscripted, an hours batch is
    answered as the endpoint answers it: an hour it already holds for the
    tag is a duplicate, every other one is stored.
    """

    def __init__(self, hours: list[Any] | None = None, readings: list[Any] | None = None) -> None:
        self.calls: list[tuple[str, Any]] = []
        self._hours = list(hours or [])
        self._readings = list(readings or [])
        self.held: set[tuple[str, int]] = set()
        self.async_start_import = AsyncMock(return_value=session())

    async def async_import_hours(self, import_id: str, gzipped: bytes):
        body = json.loads(gzip.decompress(gzipped))
        self.calls.append(("hours", body["series"]))
        answer = self._hours.pop(0) if self._hours else None
        if callable(answer):
            answer = answer(body["series"])
        if answer is None:
            series = body["series"][0]
            tag = series["tagName"]
            new = {(tag, hour["t"]) for hour in series["hours"]} - self.held
            self.held |= new
            answer = hours_taken(len(new), tag, duplicates=len(series["hours"]) - len(new))
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
    hass: HomeAssistant, entry, stats, recorder, freezer, notifications
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
    assert sent == expected[:-1], "every hour of every month, once - the probe's went in alone"

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
    assert importer.progress.hours_skipped == 0, "a first import skips nothing"
    assert f"Hourly history back to: {POWER} (February 2026)." in notifications[-1], (
        "the summary says since when each entity's hourly history goes back"
    )


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
    assert progress.hours_stored == 20 * 24, "the probe hour, then every other hour - each once"
    assert progress.hours_skipped == 0, "a first import skips nothing"
    summary = notifications[-1]
    assert "hours of Home Assistant's long-term statistics imported" in summary
    assert "were skipped" not in summary
    assert "The readings stopped because the account's import allowance is used up" in summary
    assert HOURS_UNAFFECTED_SENTENCE in summary, "the hours above it are not what stopped"


async def test_a_full_storage_stops_the_readings_and_says_the_hours_stand(
    hass: HomeAssistant, entry, stats, recorder, freezer, notifications
) -> None:
    freezer.move_to(NOW)
    stats.add_mean(POWER, NOW - timedelta(days=2), NOW)
    hass.states.async_set(POWER, "1500", {"unit_of_measurement": "W"})
    recorder.record(POWER, "1500", NOW - timedelta(hours=5))

    client = Client(readings=[ImportBatchResult(ImportOutcome.STORAGE_FULL, message="full")])
    importer = make_importer(hass, entry, client)
    await importer.async_start([POWER], None, statistics=True)
    await _run(importer)

    assert importer.progress.stop_reason == STOP_STORAGE
    assert "The readings stopped because the account reached its plan's storage cap" in notifications[-1]
    assert HOURS_UNAFFECTED_SENTENCE in notifications[-1]


@pytest.mark.parametrize(
    ("outcome", "reason"),
    [(ImportOutcome.ALLOWANCE_SPENT, STOP_ALLOWANCE), (ImportOutcome.STORAGE_FULL, STOP_STORAGE)],
)
@pytest.mark.parametrize("statistics", [False, True])
async def test_a_readings_stop_says_nothing_of_hours_that_were_not_brought(
    hass: HomeAssistant, entry, stats, recorder, freezer, notifications, outcome, reason, statistics
) -> None:
    """A readings-only import, or one whose entity had no statistics: there is no hourly history above it."""
    freezer.move_to(NOW)
    hass.states.async_set(POWER, "1500", {"unit_of_measurement": "W"})
    recorder.record(POWER, "1500", NOW - timedelta(hours=5))

    importer = make_importer(hass, entry, Client(readings=[ImportBatchResult(outcome, message="no")]))
    await importer.async_start([POWER], None, statistics=statistics)
    await _run(importer)

    assert importer.progress.stop_reason == reason
    assert HOURS_UNAFFECTED_SENTENCE not in notifications[-1], "there are no hours above it"


async def test_a_tag_historian_without_the_hours_endpoint_goes_on_with_the_readings(
    hass: HomeAssistant, entry, stats, recorder, freezer, notifications
) -> None:
    freezer.move_to(NOW)
    stats.add_mean(POWER, NOW - timedelta(days=2), NOW)
    hass.states.async_set(POWER, "1500", {"unit_of_measurement": "W"})
    recorder.record(POWER, "1500", NOW - timedelta(hours=5))

    client = Client(hours=[HoursBatchResult(ImportOutcome.NOT_SUPPORTED)])
    # A server's start is its own clock's, never this integration's: the stamp must be the session's.
    client.async_start_import.return_value = replace(session(), started_at=NOW - timedelta(seconds=7))
    importer = make_importer(hass, entry, client)
    await importer.async_start([POWER], None, statistics=True)
    await _run(importer)

    assert importer.progress.statistics_stop_reason == STOP_HOURS_UNSUPPORTED
    assert importer.progress.status == STATUS_FINISHED
    assert [kind for kind, _ in client.calls] == ["hours", "readings"]
    assert "cannot take them yet" in notifications[-1]
    assert importer.progress.statistics_started_at is None, (
        "none of the years were brought, so the offer stands"
    )
    assert OFFERED_AGAIN_SENTENCE in notifications[-1]


async def test_years_brought_earlier_in_the_same_import_stay_brought_when_tag_historian_stops_taking_them(
    hass: HomeAssistant, entry, stats, recorder, freezer, notifications
) -> None:
    """A rollback under a running import: the first entity's hours went in, so the years were brought."""
    freezer.move_to(NOW)
    stats.add_mean(POWER, NOW - timedelta(days=2), NOW)
    stats.add_sum(ENERGY, NOW - timedelta(days=2), NOW)
    hass.states.async_set(POWER, "1500", {"unit_of_measurement": "W"})
    hass.states.async_set(ENERGY, "120", {"unit_of_measurement": "kWh"})

    client = Client(hours=[None, None, HoursBatchResult(ImportOutcome.NOT_SUPPORTED)])
    importer = make_importer(hass, entry, client)
    await importer.async_start([POWER, ENERGY], None, statistics=True)
    await _run(importer)

    assert importer.progress.statistics_stop_reason == STOP_HOURS_UNSUPPORTED
    assert importer.progress.hours_stored > 0
    assert importer.progress.statistics_started_at is not None
    assert OFFERED_AGAIN_SENTENCE not in notifications[-1]


async def test_years_brought_by_an_earlier_import_stay_brought_when_a_later_one_cannot_take_them(
    hass: HomeAssistant, entry, stats, recorder, freezer, notifications
) -> None:
    freezer.move_to(NOW)
    stats.add_mean(POWER, NOW - timedelta(days=2), NOW)
    hass.states.async_set(POWER, "1500", {"unit_of_measurement": "W"})

    importer = make_importer(hass, entry, Client())
    await importer.async_start([POWER], None, statistics=True)
    await _run(importer)
    brought = importer.progress.statistics_started_at
    assert brought is not None

    later = Client(hours=[HoursBatchResult(ImportOutcome.NOT_SUPPORTED)])
    later.async_start_import.return_value = replace(session(), started_at=NOW + timedelta(days=1))
    importer._client = later
    await importer.async_start([POWER], None, statistics=True)
    await _run(importer)

    assert importer.progress.statistics_stop_reason == STOP_HOURS_UNSUPPORTED
    assert importer.progress.statistics_started_at == brought
    assert OFFERED_AGAIN_SENTENCE not in notifications[-1], "no card is raised for years already brought"


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
            # The two days less the probe's hour, which went in alone.
            lambda series: hours_taken(hours - 1 - 10, POWER, duplicates=10),
        ]
    )
    importer = make_importer(hass, entry, client)
    await importer.async_start([POWER], None, statistics=True)
    await _run(importer)

    assert importer.progress.hours_stored == hours, "the probe, and the rest of the two days: each hour once"
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


def _store(hass_storage, **data: Any) -> None:
    """An import as this version saves it, running in its statistics phase unless said otherwise."""
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
            "statistics_started_at": NOW.isoformat(),
            **data,
        },
    }


# --------------------------------------------------------------------------
# Where it gets to, and carrying on from there
# --------------------------------------------------------------------------


async def test_an_import_stopped_between_two_hours_batches_sends_every_month_once_when_it_carries_on(
    hass: HomeAssistant, entry, stats, recorder, freezer
) -> None:
    """The cursor the importer saves itself: the oldest month answered for, so a restart neither skips a month nor sends one twice."""
    freezer.move_to(NOW)
    stats.add_mean(POWER, datetime(2026, 2, 1, tzinfo=UTC), NOW)
    hass.states.async_set(POWER, "1500", {"unit_of_measurement": "W", "state_class": "measurement"})

    # The probe and the first full batch are taken; the second never is.
    down = Client(
        hours=[None, None, *[TagHistorianConnectionError("down")] * IMPORT_MAX_CONNECTION_FAILURES]
    )
    importer = make_importer(hass, entry, down)
    await importer.async_start([POWER], None, statistics=True)
    await _run(importer)

    assert importer.progress.status == STATUS_STOPPED
    assert importer.progress.stop_reason == STOP_UNREACHABLE
    probe, taken, *_ = down.hours_batches(POWER)
    assert importer.progress.hours_cursor[POWER] == _month_of(min(hour["t"] for hour in taken)).timestamp(), (
        "the start of the oldest month Tag Historian answered for"
    )

    back = Client()
    restarted = make_importer(hass, entry, back)
    await restarted.async_load()
    await _run(restarted)

    assert restarted.progress.status == STATUS_FINISHED
    sent = sorted(hour["t"] for batch in [probe, taken, *back.hours_batches(POWER)] for hour in batch)
    expected = [int(row["start"]) for row in stats.rows[POWER]]
    assert sent == expected, "every hour of every month, once, across the two runs"


# The three answers that leave the rest of an entity's statistics out, each
# refusing every hour it was sent, and what the import then says of the
# entity: a reason of its own, or - for a tag the plan has no room for - its
# name among the refused tags.
LEFT_OUT_ANSWERS = [
    pytest.param(lambda hours: {"refused_unit_mismatch": hours}, "another unit", id="another-unit"),
    pytest.param(
        lambda hours: {"refused_unsupported": hours},
        "does not take this kind of statistic",
        id="unsupported",
    ),
    pytest.param(
        lambda hours: {"refused_tag_limit": hours, "refused_tags": [POWER]}, None, id="no-room-for-its-tag"
    ),
]


def _refusing(refusal) -> Any:
    return lambda series: HoursBatchResult(ImportOutcome.TAKEN, **refusal(len(series[0]["hours"])))


def _assert_said_why(importer: HistoryImporter, summary: str, reason: str | None) -> None:
    progress = importer.progress
    assert progress.status == STATUS_FINISHED
    if reason is None:
        assert progress.refused_tags == [POWER]
        assert POWER not in progress.statistics_left_out, "the refused tags name it already"
        assert f"No tag could be created for: {POWER}" in summary
    else:
        assert reason in progress.statistics_left_out[POWER]
        assert f"The statistics of {POWER} were left out: " in summary


@pytest.mark.parametrize(("refusal", "reason"), LEFT_OUT_ANSWERS)
async def test_an_entity_whose_probe_is_refused_for_good_sends_no_batch_after_it(
    hass: HomeAssistant, entry, stats, recorder, freezer, notifications, refusal, reason
) -> None:
    """Every month after the probe would be refused the same way, and not one is sent."""
    freezer.move_to(NOW)
    stats.add_mean(POWER, datetime(2024, 1, 1, tzinfo=UTC), NOW)
    hass.states.async_set(POWER, "1500", {"unit_of_measurement": "W"})

    client = Client(hours=[_refusing(refusal)])
    importer = make_importer(hass, entry, client)
    await importer.async_start([POWER], None, statistics=True)
    await _run(importer)

    assert len(client.hours_batches(POWER)) == 1, "the probe alone"
    _assert_said_why(importer, notifications[-1], reason)


@pytest.mark.parametrize(("refusal", "reason"), LEFT_OUT_ANSWERS)
async def test_a_resumed_entity_refused_for_good_stops_at_its_first_batch(
    hass: HomeAssistant, entry, stats, recorder, freezer, hass_storage, notifications, refusal, reason
) -> None:
    """A carried-on entity sends no probe: its first batch's answer is asked what the probe's would be."""
    freezer.move_to(NOW)
    stats.add_mean(POWER, datetime(2024, 1, 1, tzinfo=UTC), NOW)
    hass.states.async_set(POWER, "1500", {"unit_of_measurement": "W"})
    _store(hass_storage, hours_cursor={POWER: datetime(2026, 8, 1, tzinfo=UTC).timestamp()})

    client = Client(hours=[_refusing(refusal)])
    importer = make_importer(hass, entry, client)
    await importer.async_load()
    await _run(importer)

    assert len(client.hours_batches(POWER)) == 1, "nothing more after the refusal"
    _assert_said_why(importer, notifications[-1], reason)


async def test_a_start_that_carries_an_import_on_reads_no_statistics_until_home_assistant_has_started(
    hass: HomeAssistant, entry, stats, recorder, freezer, hass_storage
) -> None:
    """Until then an entity whose integration is still loading has no state, and no unit to send in."""
    freezer.move_to(NOW)
    stats.add_mean(POWER, NOW - timedelta(days=2), NOW, unit="kW")
    _store(hass_storage)
    hass.set_state(CoreState.starting)

    client = Client()
    importer = make_importer(hass, entry, client)
    await importer.async_load()
    await hass.async_block_till_done()
    assert stats.reads == [] and client.calls == [], "waiting for Home Assistant to have started"

    # The meter's integration has loaded, and its state reads in W.
    hass.states.async_set(POWER, "1500", {"unit_of_measurement": "W"})
    hass.set_state(CoreState.running)
    hass.bus.async_fire(EVENT_HOMEASSISTANT_STARTED)
    await _run(importer)

    assert {series[0]["units"] for kind, series in client.calls if kind == "hours"} == {"W"}
    assert importer.progress.status == STATUS_FINISHED


# --------------------------------------------------------------------------
# How far back
# --------------------------------------------------------------------------


async def test_asked_for_30_days_the_statistics_go_back_30_days_and_no_further(
    hass: HomeAssistant, entry, stats, recorder, freezer, notifications
) -> None:
    """Three months of hours, 30 days asked for: the rest is neither read nor charged."""
    freezer.move_to(NOW)
    stats.add_mean(POWER, NOW - timedelta(days=90), NOW)
    hass.states.async_set(POWER, "1500", {"unit_of_measurement": "W"})
    since = NOW - timedelta(days=30)

    client = Client()
    importer = make_importer(hass, entry, client)
    await importer.async_start([POWER], 30, statistics=True)
    started = notifications[0]
    await _run(importer)

    sent = [hour["t"] for batch in client.hours_batches(POWER) for hour in batch]
    assert min(sent) == since.timestamp(), "every hour of the 30 days, and none before"
    assert all(start >= since for _, start, _ in stats.reads), "nothing older is even read"
    assert importer.progress.hours_from[POWER] == since.timestamp()
    assert f"first its hourly long-term statistics, back to {since:%Y-%m-%d}," in started
    assert "as far back as they go" not in started


@pytest.mark.parametrize("days", [None, 3650])
async def test_an_import_left_unbounded_says_the_statistics_go_as_far_back_as_they_go(
    hass: HomeAssistant, entry, stats, recorder, freezer, notifications, days
) -> None:
    freezer.move_to(NOW)
    hass.states.async_set(POWER, "1500", {"unit_of_measurement": "W"})

    importer = make_importer(hass, entry, Client())
    await importer.async_start([POWER], days, statistics=True)
    await _run(importer)

    assert "first its hourly long-term statistics, as far back as they go, up to ten years," in notifications[0]


async def test_nothing_older_than_tag_historian_keeps_is_read_or_sent(
    hass: HomeAssistant, entry, stats, freezer
) -> None:
    """The answer's oldestAllowed raises the floor above the ten years the import asked for."""
    freezer.move_to(NOW)
    stats.add_mean(POWER, datetime(2026, 1, 1, tzinfo=UTC), NOW)
    hass.states.async_set(POWER, "1500", {"unit_of_measurement": "W"})
    oldest = datetime(2026, 6, 19, 12, tzinfo=UTC)

    def keeps(series: list[dict[str, Any]]) -> HoursBatchResult:
        # Every answer says it: each one sets where Tag Historian's hours start.
        return HoursBatchResult(
            ImportOutcome.TAKEN,
            stored=len(series[0]["hours"]),
            importable_before={POWER: NOW},
            oldest_allowed=oldest,
        )

    client = Client(hours=[keeps] * 10)
    importer = make_importer(hass, entry, client)
    await importer.async_start([POWER], None, statistics=True)
    await _run(importer)

    assert all(start >= oldest for _, start, _ in stats.reads), "no month Tag Historian would refuse is read"
    sent = [hour["t"] for batch in client.hours_batches(POWER) for hour in batch]
    assert min(sent) == oldest.timestamp(), "every hour it keeps, and none older"
    assert importer.progress.hours_skipped == 0


async def test_an_hourly_allowance_that_runs_out_inside_a_batch_ends_the_statistics_there(
    hass: HomeAssistant, entry, stats, recorder, freezer, notifications
) -> None:
    """A batch taken in part, the rest refused for the hourly allowance: no further hours, the readings still go in."""
    freezer.move_to(NOW)
    stats.add_mean(POWER, datetime(2026, 2, 1, tzinfo=UTC), NOW)  # more than one request holds
    stats.add_sum(ENERGY, NOW - timedelta(days=3), NOW)
    hass.states.async_set(POWER, "1500", {"unit_of_measurement": "W"})
    hass.states.async_set(ENERGY, "120", {"unit_of_measurement": "kWh", "state_class": "total_increasing"})
    recorder.record(POWER, "1500", NOW - timedelta(hours=5))

    client = Client(
        hours=[
            None,
            lambda series: hours_taken(
                100,
                POWER,
                refused_hours_allowance=len(series[0]["hours"]) - 100,
                resumes_at=datetime(2026, 11, 2, tzinfo=UTC),
            ),
        ]
    )
    importer = make_importer(hass, entry, client)
    await importer.async_start([POWER, ENERGY], None, statistics=True)
    await _run(importer)

    progress = importer.progress
    assert len(client.hours_batches()) == 2, "the probe and the batch the allowance ran out in - nothing after"
    assert client.hours_batches(ENERGY) == [], "the phase ended, not just this entity's part of it"
    assert progress.statistics_stop_reason == STOP_HOURS_ALLOWANCE
    assert progress.hours_stored == 1 + 100
    assert progress.phase == PHASE_READINGS
    assert progress.status == STATUS_FINISHED, "the readings went in all the same"
    assert progress.stored == 1
    assert "the rest can go in after 2026-11-02, by running the import again." in notifications[-1]


# --------------------------------------------------------------------------
# What stops the whole import, and what of it carries on by itself
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("answer", "reason"),
    [
        (TagHistorianAuthError("revoked"), STOP_AUTH),
        (HoursBatchResult(ImportOutcome.FORBIDDEN, message="Email address not verified"), STOP_FORBIDDEN),
    ],
    ids=["refused key", "account that cannot write"],
)
async def test_a_refused_key_or_an_account_that_cannot_write_stops_the_whole_import_in_the_statistics_phase(
    hass: HomeAssistant, entry, stats, recorder, freezer, notifications, monkeypatch, answer, reason
) -> None:
    """Both end the session, not only the phase: the readings are not sent.

    Only a refused key is carried on at the next setup - a refused key never
    loads, so a load after one has a new key; a 403 waits for the user to run
    the import again rather than meeting it at every restart.
    """
    freezer.move_to(NOW)
    stats.add_mean(POWER, NOW - timedelta(days=2), NOW)
    hass.states.async_set(POWER, "1500", {"unit_of_measurement": "W"})
    recorder.record(POWER, "1500", NOW - timedelta(hours=5))
    reauth = MagicMock()
    monkeypatch.setattr(entry, "async_start_reauth", reauth)

    client = Client(hours=[answer])
    importer = make_importer(hass, entry, client)
    await importer.async_start([POWER], None, statistics=True)
    await _run(importer)

    assert importer.progress.status == STATUS_STOPPED
    assert importer.progress.stop_reason == reason
    assert importer.progress.phase == PHASE_STATISTICS
    assert [kind for kind, _ in client.calls] == ["hours"], "the readings phase did not run"
    assert reauth.called is (reason == STOP_AUTH)
    if reason == STOP_AUTH:
        assert "carries on where it stopped once it is accepted" in notifications[-1]

    back = Client()
    restarted = make_importer(hass, entry, back)
    await restarted.async_load()
    if reason == STOP_AUTH:
        await _run(restarted)
        back.async_start_import.assert_not_awaited()
        assert back.hours_batches(POWER), "carried on, under the same import"
        assert restarted.progress.import_id == "imp-1"
        assert restarted.progress.status == STATUS_FINISHED
    else:
        assert restarted._task is None
        assert restarted.progress.status == STATUS_STOPPED
        assert restarted.progress.stop_reason == STOP_FORBIDDEN
        assert back.calls == []


async def test_statistics_that_cannot_be_read_are_left_out_and_the_readings_still_go(
    hass: HomeAssistant, entry, stats, recorder, freezer, notifications, monkeypatch
) -> None:
    """Recorder drift, or one entity's rows it cannot read: those statistics are left out with why, never the import."""
    freezer.move_to(NOW)
    stats.add_mean(POWER, NOW - timedelta(days=2), NOW)
    hass.states.async_set(POWER, "1500", {"unit_of_measurement": "W"})
    recorder.record(POWER, "1500", NOW - timedelta(hours=5))

    async def drifted(*args: Any, **kwargs: Any) -> list[dict[str, Any]]:
        raise TypeError("statistics_during_period() got an unexpected keyword argument")

    monkeypatch.setattr(
        "custom_components.tag_historian.statistics.async_recorded_statistics", drifted
    )
    client = Client()
    importer = make_importer(hass, entry, client)
    await importer.async_start([POWER], None, statistics=True)
    await _run(importer)

    progress = importer.progress
    assert progress.status == STATUS_FINISHED
    assert [kind for kind, _ in client.calls] == ["readings"], "the readings went in all the same"
    assert progress.statistics_done == [POWER], "done, so the next start does not meet it again"
    assert f"The statistics of {POWER} were left out: they could not be read" in notifications[-1]


async def test_an_error_of_its_own_stops_the_import_as_failed_and_the_next_start_leaves_it(
    hass: HomeAssistant, entry, stats, recorder, freezer, notifications, monkeypatch
) -> None:
    """Not "unreachable": the same error would come back at every start, and say Tag Historian was down."""
    freezer.move_to(NOW)
    hass.states.async_set(POWER, "1500", {"unit_of_measurement": "W"})

    async def broken(*args: Any, **kwargs: Any) -> list[State]:
        raise RuntimeError("the recorder answered something this integration cannot read")

    monkeypatch.setattr("custom_components.tag_historian.history.async_recorded_states", broken)
    importer = make_importer(hass, entry, Client())
    await importer.async_start([POWER], None, statistics=True)
    await _run(importer)

    assert importer.progress.stop_reason == STOP_FAILED
    assert "does not carry on by itself" in notifications[-1]
    assert "The error was: the recorder answered something" in notifications[-1]
    assert "could not be reached" not in notifications[-1]

    back = Client()
    restarted = make_importer(hass, entry, back)
    await restarted.async_load()
    assert restarted._task is None
    assert restarted.progress.status == STATUS_STOPPED
    assert back.calls == []


@pytest.mark.parametrize("where", ["building the batch", "reading the answer"])
async def test_an_error_of_its_own_in_the_statistics_phase_stops_the_import_as_failed(
    hass: HomeAssistant, entry, stats, recorder, freezer, notifications, monkeypatch, where
) -> None:
    """Only the recorder's side leaves an entity's statistics out; an error in sending them is the integration's own."""
    freezer.move_to(NOW)
    stats.add_mean(POWER, NOW - timedelta(days=2), NOW)
    hass.states.async_set(POWER, "1500", {"unit_of_measurement": "W"})
    recorder.record(POWER, "1500", NOW - timedelta(hours=5))
    client = Client()
    if where == "building the batch":

        def broken(*args: Any, **kwargs: Any) -> bytes:
            raise RuntimeError("a batch this integration could not build")

        monkeypatch.setattr("custom_components.tag_historian.statistics.compress", broken)
    else:
        client = Client(hours=[ValueError("an answer this integration could not read")])
    importer = make_importer(hass, entry, client)
    await importer.async_start([POWER], None, statistics=True)
    await _run(importer)

    progress = importer.progress
    assert progress.stop_reason == STOP_FAILED
    assert progress.phase == PHASE_STATISTICS
    assert "readings" not in [kind for kind, _ in client.calls]
    assert POWER not in progress.statistics_done
    assert POWER not in progress.statistics_left_out
    assert "does not carry on by itself" in notifications[-1]
    assert "could not be read" not in notifications[-1]

    back = Client()
    restarted = make_importer(hass, entry, back)
    await restarted.async_load()
    assert restarted._task is None
    assert back.calls == []


async def test_an_error_that_prints_as_nothing_is_still_named(
    hass: HomeAssistant, entry, stats, recorder, freezer, notifications
) -> None:
    freezer.move_to(NOW)
    stats.add_mean(POWER, NOW - timedelta(days=2), NOW)
    hass.states.async_set(POWER, "1500", {"unit_of_measurement": "W"})

    importer = make_importer(hass, entry, Client(hours=[TimeoutError()]))
    await importer.async_start([POWER], None, statistics=True)
    await _run(importer)

    assert importer.progress.stop_reason == STOP_FAILED
    assert "The error was: TimeoutError()" in notifications[-1]


@pytest.mark.parametrize("phase", [PHASE_STATISTICS, PHASE_READINGS])
async def test_a_batch_tag_historian_answers_with_a_500_every_time_is_not_carried_on_by_itself(
    hass: HomeAssistant, entry, stats, recorder, freezer, notifications, phase
) -> None:
    freezer.move_to(NOW)
    stats.add_mean(POWER, NOW - timedelta(days=2), NOW)
    hass.states.async_set(POWER, "1500", {"unit_of_measurement": "W"})
    recorder.record(POWER, "1500", NOW - timedelta(hours=5))
    failing = [TagHistorianServerError("import answered 500")] * IMPORT_MAX_CONNECTION_FAILURES

    client = Client(hours=failing) if phase == PHASE_STATISTICS else Client(readings=failing)
    importer = make_importer(hass, entry, client)
    await importer.async_start([POWER], None, statistics=phase == PHASE_STATISTICS)
    await _run(importer)

    assert importer.progress.stop_reason == STOP_FAILED
    assert importer.progress.phase == phase
    assert f"server error {IMPORT_MAX_CONNECTION_FAILURES} times in a row" in notifications[-1]
    assert "The error was: import answered 500" in notifications[-1]

    back = Client()
    restarted = make_importer(hass, entry, back)
    await restarted.async_load()
    assert restarted._task is None, "it would be sent the same batch, and answer it the same way"
    assert back.calls == []


@pytest.mark.parametrize("phase", [PHASE_STATISTICS, PHASE_READINGS])
@pytest.mark.parametrize(
    "failing",
    [
        pytest.param(
            [
                *[TagHistorianConnectionError("timed out")] * (IMPORT_MAX_CONNECTION_FAILURES - 1),
                TagHistorianServerError("import answered 500"),
            ],
            id="ending-in-a-500",
        ),
        pytest.param(
            [
                *[TagHistorianServerError("import answered 500")] * (IMPORT_MAX_CONNECTION_FAILURES - 1),
                TagHistorianConnectionError("timed out"),
            ],
            id="ending-in-a-timeout",
        ),
    ],
)
async def test_a_run_of_failures_with_one_that_was_not_a_500_is_unreachable_and_carried_on(
    hass: HomeAssistant, entry, stats, recorder, freezer, notifications, phase, failing
) -> None:
    """"Failed" says Tag Historian answered the batch the same way every time; here at least once it did not answer."""
    freezer.move_to(NOW)
    stats.add_mean(POWER, NOW - timedelta(days=2), NOW)
    hass.states.async_set(POWER, "1500", {"unit_of_measurement": "W"})
    recorder.record(POWER, "1500", NOW - timedelta(hours=5))

    client = Client(hours=failing) if phase == PHASE_STATISTICS else Client(readings=failing)
    importer = make_importer(hass, entry, client)
    await importer.async_start([POWER], None, statistics=phase == PHASE_STATISTICS)
    await _run(importer)

    assert importer.progress.stop_reason == STOP_UNREACHABLE
    assert "does not carry on by itself" not in notifications[-1]

    restarted = make_importer(hass, entry, Client())
    await restarted.async_load()
    assert restarted._task is not None, "an outage is carried on"
    await _run(restarted)
    assert restarted.progress.status == STATUS_FINISHED


async def test_an_import_saved_unreachable_before_phases_is_not_carried_on(
    hass: HomeAssistant, entry, stats, recorder, freezer, hass_storage
) -> None:
    """2026.11 called every failure unreachable, and said to run the import again; it is left as it said."""
    freezer.move_to(NOW)
    hass_storage[KEY] = {
        "version": STORAGE_VERSION,
        "minor_version": 1,
        "key": KEY,
        "data": {
            "status": STATUS_STOPPED,
            "stop_reason": STOP_UNREACHABLE,
            "import_id": "imp-2026-10",
            "started_at": (NOW - timedelta(days=20)).isoformat(),
            "since": (NOW - timedelta(days=34)).isoformat(),
            "entities": [POWER],
        },
    }

    client = Client()
    importer = make_importer(hass, entry, client)
    await importer.async_load()

    assert importer._task is None
    assert importer.progress.status == STATUS_STOPPED
    assert client.calls == []


async def test_an_import_that_gets_no_further_is_carried_on_a_few_starts_and_then_left(
    hass: HomeAssistant, entry, stats, recorder, freezer, notifications
) -> None:
    """Each start the probe is taken and the next batch never: the probe alone is not getting further."""
    freezer.move_to(NOW)
    stats.add_mean(POWER, NOW - timedelta(days=40), NOW)
    hass.states.async_set(POWER, "1500", {"unit_of_measurement": "W"})

    def down() -> Client:
        return Client(hours=[None, *[TagHistorianConnectionError("down")] * IMPORT_MAX_CONNECTION_FAILURES])

    importer = make_importer(hass, entry, down())
    await importer.async_start([POWER], None, statistics=True)
    await _run(importer)

    for load in range(1, IMPORT_MAX_RESUMES_ON_LOAD + 1):
        again = make_importer(hass, entry, down())
        await again.async_load()
        assert again._task is not None, "carried on by itself"
        await _run(again)
        assert again.progress.stop_reason == STOP_UNREACHABLE
        if load < IMPORT_MAX_RESUMES_ON_LOAD:
            assert UNREACHABLE_AGAIN_SENTENCE not in notifications[-1], "the next start carries it on"
            assert "carries on where it stopped the next time Home Assistant starts" in notifications[-1]

    assert UNREACHABLE_AGAIN_SENTENCE in notifications[-1], "it says it will not carry on again"
    assert f"at {IMPORT_MAX_RESUMES_ON_LOAD} starts in a row" in notifications[-1]
    last = Client()
    left = make_importer(hass, entry, last)
    await left.async_load()
    assert left._task is None
    assert last.calls == []


async def test_an_import_that_gets_further_at_every_start_keeps_being_carried_on(
    hass: HomeAssistant, entry, stats, recorder, freezer
) -> None:
    freezer.move_to(NOW)
    stats.add_mean(POWER, datetime(2023, 1, 1, tzinfo=UTC), NOW)  # several requests' worth
    hass.states.async_set(POWER, "1500", {"unit_of_measurement": "W"})
    failures = [TagHistorianConnectionError("down")] * IMPORT_MAX_CONNECTION_FAILURES

    importer = make_importer(hass, entry, Client(hours=[None, None, *failures]))
    await importer.async_start([POWER], None, statistics=True)
    await _run(importer)

    for _ in range(IMPORT_MAX_RESUMES_ON_LOAD + 1):
        cursor = importer.progress.hours_cursor[POWER]
        importer = make_importer(hass, entry, Client(hours=[None, *failures]))
        await importer.async_load()
        assert importer._task is not None
        await _run(importer)
        assert importer.progress.hours_cursor[POWER] < cursor, "one more batch answered for"
        assert importer.progress.resumed_on_load == 0, "counted afresh once it got further"


async def test_an_import_that_gets_further_in_its_readings_at_every_start_keeps_being_carried_on(
    hass: HomeAssistant, entry, recorder, freezer
) -> None:
    """The readings' twin of the test above: each start one batch of readings is taken, then the network goes."""
    freezer.move_to(NOW)
    first = NOW - timedelta(days=3)
    for i in range(1 + (IMPORT_MAX_RESUMES_ON_LOAD + 2) * IMPORT_BATCH_SIZE):
        recorder.record(POWER, "1500", first + timedelta(seconds=20 * i))
    failures = [TagHistorianConnectionError("down")] * IMPORT_MAX_CONNECTION_FAILURES

    importer = make_importer(hass, entry, Client(readings=[None, *failures]))
    await importer.async_start([POWER], None)
    await _run(importer)
    assert importer.progress.stop_reason == STOP_UNREACHABLE

    for _ in range(IMPORT_MAX_RESUMES_ON_LOAD + 1):
        cursor = importer.progress.cursor[POWER]
        importer = make_importer(hass, entry, Client(readings=[None, *failures]))
        await importer.async_load()
        assert importer._task is not None
        await _run(importer)
        assert importer.progress.stop_reason == STOP_UNREACHABLE
        assert importer.progress.cursor[POWER] > cursor, "one more batch answered for"
        assert importer.progress.resumed_on_load == 0, "counted afresh once it got further"


@pytest.mark.parametrize("phase", [PHASE_STATISTICS, PHASE_READINGS])
async def test_an_entity_done_with_nothing_to_send_counts_as_getting_further(
    hass: HomeAssistant, entry, stats, recorder, freezer, hass_storage, notifications, phase
) -> None:
    """Carried on for the last time allowed: the door, with nothing to bring, is done, so the next start carries on again."""
    freezer.move_to(NOW)
    stats.add_mean(POWER, NOW - timedelta(days=2), NOW)
    hass.states.async_set(POWER, "1500", {"unit_of_measurement": "W"})
    recorder.record(POWER, "1500", NOW - timedelta(hours=5))
    _store(
        hass_storage,
        status=STATUS_STOPPED,
        stop_reason=STOP_UNREACHABLE,
        import_id="imp-1",
        entities=[DOOR, POWER],
        statistics=phase == PHASE_STATISTICS,
        phase=phase,
        resumed_on_load=IMPORT_MAX_RESUMES_ON_LOAD - 1,
    )
    failures = [TagHistorianConnectionError("down")] * IMPORT_MAX_CONNECTION_FAILURES

    def down() -> Client:
        return Client(hours=failures) if phase == PHASE_STATISTICS else Client(readings=failures)

    importer = make_importer(hass, entry, down())
    await importer.async_load()
    assert importer._task is not None, "carried on, the last time it would be without getting further"
    await _run(importer)

    done = importer.progress.statistics_done if phase == PHASE_STATISTICS else importer.progress.done
    assert done == [DOOR]
    assert importer.progress.stop_reason == STOP_UNREACHABLE
    assert importer.progress.resumed_on_load == 0, "the door being done is getting further"
    assert UNREACHABLE_AGAIN_SENTENCE not in notifications[-1]

    again = make_importer(hass, entry, down())
    await again.async_load()
    assert again._task is not None, "carried on again"
    await _run(again)


async def test_a_refused_key_after_the_last_carry_on_says_it_waits_to_be_run_again(
    hass: HomeAssistant, entry, stats, recorder, freezer, hass_storage, notifications, monkeypatch
) -> None:
    """A new key is entered, but the next start does not carry the import on any more: it says so."""
    freezer.move_to(NOW)
    stats.add_mean(POWER, NOW - timedelta(days=2), NOW)
    hass.states.async_set(POWER, "1500", {"unit_of_measurement": "W"})
    monkeypatch.setattr(entry, "async_start_reauth", MagicMock())
    _store(
        hass_storage,
        status=STATUS_STOPPED,
        stop_reason=STOP_UNREACHABLE,
        import_id="imp-1",
        resumed_on_load=IMPORT_MAX_RESUMES_ON_LOAD - 1,
    )

    importer = make_importer(hass, entry, Client(hours=[TagHistorianAuthError("revoked")]))
    await importer.async_load()
    await _run(importer)

    assert importer.progress.stop_reason == STOP_AUTH
    assert importer.progress.resumed_on_load == IMPORT_MAX_RESUMES_ON_LOAD
    assert KEY_REFUSED_AGAIN_SENTENCE in notifications[-1]
    assert "carries on where it stopped once it is accepted" not in notifications[-1]

    left = make_importer(hass, entry, Client())
    await left.async_load()
    assert left._task is None, "as it said"


async def test_a_refused_key_before_the_last_carry_on_says_it_carries_on(
    hass: HomeAssistant, entry, stats, recorder, freezer, notifications, monkeypatch
) -> None:
    freezer.move_to(NOW)
    stats.add_mean(POWER, NOW - timedelta(days=2), NOW)
    hass.states.async_set(POWER, "1500", {"unit_of_measurement": "W"})
    monkeypatch.setattr(entry, "async_start_reauth", MagicMock())

    importer = make_importer(hass, entry, Client(hours=[TagHistorianAuthError("revoked")]))
    await importer.async_start([POWER], None, statistics=True)
    await _run(importer)

    assert importer.progress.stop_reason == STOP_AUTH
    assert "carries on where it stopped once it is accepted" in notifications[-1]
    assert KEY_REFUSED_AGAIN_SENTENCE not in notifications[-1]


@pytest.mark.parametrize("phase", [PHASE_STATISTICS, PHASE_READINGS])
async def test_a_batch_tag_historian_keeps_asking_to_send_later_stops_the_import_rather_than_spinning(
    hass: HomeAssistant, entry, stats, recorder, freezer, no_waiting, phase
) -> None:
    """503 every time, for hours: it waits longer and longer, then stops - carried on at the next start."""
    freezer.move_to(NOW)
    stats.add_mean(POWER, NOW - timedelta(days=2), NOW)
    hass.states.async_set(POWER, "1500", {"unit_of_measurement": "W"})
    recorder.record(POWER, "1500", NOW - timedelta(hours=5))

    if phase == PHASE_STATISTICS:
        client = Client(hours=[HoursBatchResult(ImportOutcome.RETRY, retry_after=15)] * 50)
    else:
        client = Client(readings=[ImportBatchResult(ImportOutcome.RETRY, retry_after=15)] * 50)
    importer = make_importer(hass, entry, client)
    await importer.async_start([POWER], None, statistics=phase == PHASE_STATISTICS)
    await _run(importer)

    assert importer.progress.status == STATUS_STOPPED
    assert importer.progress.stop_reason == STOP_UNREACHABLE
    assert no_waiting[:4] == [15, 30, 60, 120], "Tag Historian's own wait first, then doubling"
    assert max(no_waiting) == IMPORT_MAX_RETRY_WAIT_SECONDS
    assert sum(no_waiting) >= IMPORT_MAX_RETRY_SECONDS
    assert len(client.calls) < 20, "not every 15 seconds for two hours"


@pytest.mark.parametrize(("reason", "retry_after"), [("busy", 3), ("account_busy", 5)])
async def test_an_hours_batch_waiting_for_its_turn_comes_back_when_its_turn_is_kept(
    hass: HomeAssistant, entry, stats, recorder, freezer, no_waiting, reason, retry_after
) -> None:
    """The API keeps a refused batch's turn until its Retry-After and a second; a doubled wait came back after it was gone."""
    freezer.move_to(NOW)
    stats.add_mean(POWER, NOW - timedelta(days=2), NOW)
    hass.states.async_set(POWER, "1500", {"unit_of_measurement": "W"})
    turns = [HoursBatchResult(ImportOutcome.RETRY, retry_after=retry_after, reason=reason)] * 6

    client = Client(hours=[None, *turns])
    importer = make_importer(hass, entry, client)
    await importer.async_start([POWER], None, statistics=True)
    await _run(importer)

    assert importer.progress.status == STATUS_FINISHED
    assert no_waiting == [retry_after] * 6, "at its Retry-After every time, never doubled"


async def test_an_hours_batch_that_never_gets_its_turn_still_stops_after_two_hours(
    hass: HomeAssistant, entry, stats, recorder, freezer, no_waiting
) -> None:
    freezer.move_to(NOW)
    stats.add_mean(POWER, NOW - timedelta(days=2), NOW)
    hass.states.async_set(POWER, "1500", {"unit_of_measurement": "W"})
    turns = IMPORT_MAX_RETRY_SECONDS // 3 + 1

    client = Client(hours=[None, *[HoursBatchResult(ImportOutcome.RETRY, retry_after=3, reason="busy")] * turns])
    importer = make_importer(hass, entry, client)
    await importer.async_start([POWER], None, statistics=True)
    await _run(importer)

    assert importer.progress.stop_reason == STOP_UNREACHABLE
    assert max(no_waiting) == 3
    assert sum(no_waiting) >= IMPORT_MAX_RETRY_SECONDS


@pytest.mark.parametrize("reason", ["months_not_ready", "read_failed", "ingest_busy"])
async def test_an_hours_batch_asked_again_for_any_other_reason_waits_longer_each_time(
    hass: HomeAssistant, entry, stats, recorder, freezer, no_waiting, reason
) -> None:
    freezer.move_to(NOW)
    stats.add_mean(POWER, NOW - timedelta(days=2), NOW)
    hass.states.async_set(POWER, "1500", {"unit_of_measurement": "W"})

    client = Client(hours=[None, *[HoursBatchResult(ImportOutcome.RETRY, retry_after=5, reason=reason)] * 3])
    importer = make_importer(hass, entry, client)
    await importer.async_start([POWER], None, statistics=True)
    await _run(importer)

    assert no_waiting == [5, 10, 20]


async def test_a_retry_after_longer_than_the_longest_wait_is_held_to_it(
    hass: HomeAssistant, entry, stats, recorder, freezer, no_waiting
) -> None:
    freezer.move_to(NOW)
    stats.add_mean(POWER, NOW - timedelta(days=2), NOW)
    hass.states.async_set(POWER, "1500", {"unit_of_measurement": "W"})

    client = Client(hours=[None, HoursBatchResult(ImportOutcome.RETRY, retry_after=24 * 3600)])
    importer = make_importer(hass, entry, client)
    await importer.async_start([POWER], None, statistics=True)
    await _run(importer)

    assert no_waiting == [IMPORT_MAX_RETRY_WAIT_SECONDS]


# --------------------------------------------------------------------------
# Renamed entities
# --------------------------------------------------------------------------


@pytest.fixture
def moved(monkeypatch) -> None:
    """Home Assistant moved the history to the new id: nothing is recorded under the old one."""
    monkeypatch.setattr(
        "custom_components.tag_historian.history.async_last_recorded",
        AsyncMock(return_value=None),
    )


def _importer(hass, entry, client, **kwargs: Any) -> HistoryImporter:
    return HistoryImporter(hass, entry, client, Store(hass, STORAGE_VERSION, KEY), 10, **kwargs)


def _register(hass: HomeAssistant, entity_id: str) -> str:
    domain, object_id = entity_id.split(".")
    registered = er.async_get(hass).async_get_or_create(
        domain, "test", f"unique-{object_id}", suggested_object_id=object_id
    )
    assert registered.entity_id == entity_id
    return registered.id


def _move_statistics(stats: FakeStatistics, old: str, new: str) -> None:
    """Home Assistant renames the statistic with the entity."""
    stats.meta[new] = {**stats.meta.pop(old), "statistic_id": new}
    stats.rows[new] = stats.rows.pop(old)


async def test_a_renamed_entitys_hours_go_to_the_tag_its_history_is_in(
    hass: HomeAssistant, entry, stats, recorder, freezer, moved
) -> None:
    freezer.move_to(NOW)
    stats.add_mean(RENAMED, NOW - timedelta(days=40), NOW)
    hass.states.async_set(RENAMED, "1500", {"unit_of_measurement": "W"})

    client = Client()
    importer = _importer(hass, entry, client, send_as={RENAMED: POWER}, selected=[RENAMED])
    await importer.async_start([RENAMED], None, statistics=True)
    await _run(importer)

    assert client.hours_batches(RENAMED) == [], "no second tag for the renamed entity"
    assert client.hours_batches(POWER), "its years go where its readings go"
    assert importer.progress.statistics_done == [RENAMED]


async def test_an_entity_given_a_tag_of_its_own_sends_no_hours(
    hass: HomeAssistant, entry, stats, recorder, freezer
) -> None:
    freezer.move_to(NOW)
    stats.add_mean(RENAMED, NOW - timedelta(days=40), NOW)
    hass.states.async_set(RENAMED, "1500", {"unit_of_measurement": "W"})

    client = Client()
    importer = _importer(
        hass, entry, client, own_tag_since={RENAMED: NOW.isoformat()}, selected=[RENAMED]
    )
    await importer.async_start([RENAMED], None, statistics=True)
    await _run(importer)

    assert client.hours_batches() == [], "its history is the old tag's"
    assert importer.progress.statistics_done == [RENAMED]


async def test_a_rename_while_the_statistics_are_read_carries_on_under_the_new_id(
    hass: HomeAssistant, entry, stats, recorder, freezer, moved
) -> None:
    freezer.move_to(NOW)
    _register(hass, POWER)
    hass.config_entries.async_update_entry(entry, options={**entry.options, CONF_ENTITIES: [POWER]})
    first = datetime(2026, 1, 1, tzinfo=UTC)
    stats.add_mean(POWER, first, NOW)
    hass.states.async_set(POWER, "1500", {"unit_of_measurement": "W"})

    def renamed_after_the_first_batch(series: list[dict[str, Any]]) -> HoursBatchResult:
        er.async_get(hass).async_update_entity(POWER, new_entity_id=RENAMED)
        _move_statistics(stats, POWER, RENAMED)
        hass.states.async_remove(POWER)
        hass.states.async_set(RENAMED, "1500", {"unit_of_measurement": "W"})
        hass.config_entries.async_update_entry(
            entry, options={**entry.options, CONF_ENTITIES: [RENAMED], CONF_SEND_AS: {RENAMED: POWER}}
        )
        return hours_taken(len(series[0]["hours"]), POWER)

    client = Client(hours=[None, renamed_after_the_first_batch])
    importer = _importer(hass, entry, client, send_as={}, selected=[POWER])
    await importer.async_start([POWER], None, statistics=True)
    await _run(importer)

    progress = importer.progress
    assert progress.statistics_done == [], "its reads came up empty because its history moved"
    assert progress.status == STATUS_RUNNING
    assert progress.phase == PHASE_STATISTICS
    answered = progress.hours_cursor[POWER]

    after = Client()
    resumed = _importer(hass, entry, after, send_as={RENAMED: POWER}, selected=[RENAMED])
    await resumed.async_load()
    await _run(resumed)

    after.async_start_import.assert_not_awaited()
    assert after.hours_batches(RENAMED) == []
    sent = [hour["t"] for batch in after.hours_batches(POWER) for hour in batch]
    assert sent, "the rest of its statistics"
    assert max(sent) < answered, "no second probe, and nothing answered for sent again"
    assert min(sent) == first.timestamp()
    assert resumed.progress.statistics_done == [RENAMED]
    assert resumed.progress.status == STATUS_FINISHED


async def test_what_the_statistics_phase_saved_follows_a_rename(
    hass: HomeAssistant, entry, stats, recorder, freezer, moved, hass_storage
) -> None:
    freezer.move_to(NOW)
    power_id, energy_id = _register(hass, POWER), _register(hass, ENERGY)
    stats.add_mean(POWER, NOW - timedelta(days=40), NOW)
    left_out = "its long-term statistics are in kWh, and it is in W now"
    _store(
        hass_storage,
        entities=[POWER, ENERGY, DOOR],
        registry_ids={POWER: power_id, ENERGY: energy_id},
        statistics_done=[POWER, ENERGY],
        no_statistics=[ENERGY],
        statistics_left_out={POWER: left_out},
    )
    er.async_get(hass).async_update_entity(POWER, new_entity_id=RENAMED)
    er.async_get(hass).async_update_entity(ENERGY, new_entity_id=ENERGY_2)
    _move_statistics(stats, POWER, RENAMED)
    hass.states.async_set(RENAMED, "1500", {"unit_of_measurement": "W"})
    hass.config_entries.async_update_entry(
        entry, options={**entry.options, CONF_ENTITIES: [RENAMED, ENERGY_2, DOOR]}
    )

    client = Client()
    resumed = _importer(hass, entry, client, send_as={}, selected=[RENAMED, ENERGY_2, DOOR])
    await resumed.async_load()
    await _run(resumed)

    progress = resumed.progress
    assert client.hours_batches() == [], "done before the rename is done after it"
    assert progress.statistics_done == [RENAMED, ENERGY_2, DOOR]
    assert progress.no_statistics == [ENERGY_2, DOOR]
    assert progress.statistics_left_out == {RENAMED: left_out}
    assert progress.status == STATUS_FINISHED


async def test_a_meter_renamed_onto_an_id_with_its_own_statistics_sends_its_former_ids_years(
    hass: HomeAssistant, entry, stats, recorder, freezer, monkeypatch
) -> None:
    freezer.move_to(NOW)
    until = NOW - timedelta(days=20)
    monkeypatch.setattr(
        "custom_components.tag_historian.history.async_last_recorded",
        AsyncMock(return_value=until),
    )
    stats.add_mean(POWER, NOW - timedelta(days=40), until)
    stats.add_mean(RENAMED, NOW - timedelta(days=10), NOW)
    hass.states.async_set(RENAMED, "1500", {"unit_of_measurement": "W"})

    client = Client()
    importer = _importer(hass, entry, client, send_as={RENAMED: POWER}, selected=[RENAMED])
    await importer.async_start([RENAMED], None, statistics=True)
    await _run(importer)

    sent = [hour["t"] for batch in client.hours_batches(POWER) for hour in batch]
    assert min(sent) == (NOW - timedelta(days=40)).timestamp(), "the former id's years, up to the rename"


# --------------------------------------------------------------------------
# What the user sees
# --------------------------------------------------------------------------


async def test_the_import_sensor_shows_the_phase_and_counts_the_hours(
    hass: HomeAssistant, entry, stats, recorder, freezer
) -> None:
    """While the hours go in it says "statistics", then "readings", and it counts the hours."""
    freezer.move_to(NOW)
    stats.add_mean(POWER, NOW - timedelta(days=2), NOW)
    hass.states.async_set(POWER, "1500", {"unit_of_measurement": "W"})
    recorder.record(POWER, "1500", NOW - timedelta(hours=5))

    def shown() -> dict[str, Any]:
        return hass.states.get("sensor.tag_historian_history_import").attributes

    # The phase the sensor showed when each batch went out: the hours, then the readings.
    phases: list[str | None] = []
    scripted = Client(
        hours=[None, lambda series: hours_taken(len(series[0]["hours"]) - 3, POWER, duplicates=3)]
    )

    async def hours(import_id: str, gzipped: bytes) -> HoursBatchResult:
        phases.append(shown()["phase"])
        return await scripted.async_import_hours(import_id, gzipped)

    async def readings(import_id: str, batch: list[dict[str, Any]]) -> ImportBatchResult:
        phases.append(shown()["phase"])
        return await scripted.async_import_batch(import_id, batch)

    client = make_client()
    client.async_start_import.return_value = session()
    client.async_import_hours.side_effect = hours
    client.async_import_batch.side_effect = readings

    with patch("custom_components.tag_historian.TagHistorianClient", return_value=client):
        assert await hass.config_entries.async_setup(entry.entry_id)
        await hass.async_block_till_done()
        await hass.services.async_call(
            DOMAIN, SERVICE_IMPORT_HISTORY, {"entity_id": [POWER]}, blocking=True
        )
        # The import runs as a background task; async_block_till_done does not wait for it.
        await _run(entry.runtime_data.importer)
        await hass.async_block_till_done()

        progress = entry.runtime_data.importer.progress
        assert progress.status == STATUS_FINISHED
        assert phases[0] == PHASE_STATISTICS, "the hours go out while the sensor says statistics"
        assert phases[-1] == PHASE_READINGS, "the readings go out while it says readings"
        assert shown()["hours_stored"] == progress.hours_stored == 2 * 24 - 3, (
            "the probe, and the rest of the two days less the three already there"
        )
        assert shown()["hours_skipped"] == progress.hours_skipped == 3

        assert await hass.config_entries.async_unload(entry.entry_id)
        await hass.async_block_till_done()
