"""The buffer across a restart, and gaps read back from the recorder.

Two promises, and the one rule they must not break between them:

* a reading the forwarder could not deliver when Home Assistant stopped is
  delivered after it starts again (saved to .storage, bounded);
* a reading the buffer had to let go of - evicted during an outage, or never
  seen because the entry was not running - is read back from the recorder and
  sent, oldest first, counting against the daily allowance like any live one;
* and neither ever sends a reading Tag Historian may already have, because the
  live endpoint does not deduplicate.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import patch

import pytest
from homeassistant.core import HomeAssistant, State
from homeassistant.helpers.storage import Store
from homeassistant.util import dt as dt_util
from pytest_homeassistant_custom_component.common import (
    MockConfigEntry,
    async_fire_time_changed,
)

from custom_components.tag_historian import forwarder as forwarder_module
from custom_components.tag_historian.api import (
    TagHistorianConnectionError,
    WriteOutcome,
    WriteResult,
)
from custom_components.tag_historian.const import (
    CONF_API_KEY,
    CONF_ENTITIES,
    CONF_HOST,
    CONF_MIN_INTERVAL,
    DOMAIN,
    STORAGE_VERSION,
)
from custom_components.tag_historian.forwarder import (
    CONNECTION_RETRY_SECONDS,
    Queued,
    StateForwarder,
)

from .conftest import make_client

KEY = "tag_historian.test_entry.forwarder"
POWER = "sensor.house_power"
DOOR = "binary_sensor.back_door"
T0 = datetime(2026, 10, 17, 8, 0, tzinfo=UTC)


class StubClient:
    """Records bodies and answers with a scripted sequence of results."""

    def __init__(self, *results) -> None:
        self.bodies: list[str] = []
        # Only the bodies answered with a delivery, in order.
        self.delivered: list[str] = []
        self._results = list(results)

    async def async_write(self, body: str):
        self.bodies.append(body)
        result = self._results.pop(0) if self._results else WriteResult(WriteOutcome.OK)
        if isinstance(result, Exception):
            raise result
        if result.outcome is WriteOutcome.OK:
            self.delivered.append(body)
        return result

    def delivered_values(self, entity: str) -> list[str]:
        """The values delivered for one entity (its object id), in order."""
        return [
            value
            for body in self.delivered
            for line, value in zip(body.splitlines(), _values(body), strict=True)
            if f"entity_id={entity}" in line
        ]

    @property
    def lines(self) -> list[str]:
        return [line for body in self.bodies for line in body.splitlines()]

    @property
    def values(self) -> list[str]:
        """The value field of every line sent, in order."""
        return [line.split(" ")[1].split(",")[0].removeprefix("value=") for line in self.lines]


class FakeRecorder:
    """Stands in for the recorder: states by entity, read back by time."""

    def __init__(self) -> None:
        self.states: dict[str, list[State]] = {}
        self.reads: list[tuple[str, datetime, datetime]] = []

    def record(self, entity_id: str, value: str, when: datetime, **attributes: Any) -> None:
        self.states.setdefault(entity_id, []).append(
            State(entity_id, value, attributes, last_changed=when, last_updated=when)
        )

    async def read(self, hass, entity_id, start, end) -> list[State]:
        self.reads.append((entity_id, start, end))
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
    return fake


@pytest.fixture
def entry(hass: HomeAssistant) -> MockConfigEntry:
    entry = MockConfigEntry(
        domain=DOMAIN,
        entry_id="test_entry",
        data={CONF_HOST: "api.taghistorian.com", CONF_API_KEY: "not-a-real-key"},
        unique_id="11111111-2222-3333-4444-555555555555",
    )
    entry.add_to_hass(hass)
    return entry


@pytest.fixture
def started(hass: HomeAssistant, entry: MockConfigEntry):
    """Build, restore and start forwarders on the entry's store; stop them all after."""
    built: list[StateForwarder] = []

    async def _start(client, entities=None, min_interval=0) -> StateForwarder:
        forwarder = StateForwarder(
            hass,
            entry,
            client,
            entities if entities is not None else [POWER],
            min_interval,
            store=Store(hass, STORAGE_VERSION, KEY),
        )
        await forwarder.async_restore()
        forwarder.async_start()
        built.append(forwarder)
        return forwarder

    yield _start

    for forwarder in built:
        forwarder.async_stop()


async def _tick(hass: HomeAssistant, seconds: float) -> None:
    async_fire_time_changed(hass, dt_util.utcnow() + timedelta(seconds=seconds))
    await hass.async_block_till_done()


def _values(body: str) -> list[str]:
    return [line.split(" ")[1].split(",")[0].removeprefix("value=") for line in body.splitlines()]


def _saved(hass_storage) -> dict[str, Any]:
    return hass_storage[KEY]["data"]


def _seed(hass_storage, **data: Any) -> None:
    hass_storage[KEY] = {"version": STORAGE_VERSION, "minor_version": 1, "key": KEY, "data": data}


# --------------------------------------------------------------------------
# The buffer across a restart
# --------------------------------------------------------------------------


async def test_what_the_last_flush_could_not_deliver_is_saved_and_sent_after_a_restart(
    hass: HomeAssistant, started, hass_storage
) -> None:
    """The case that used to end in dropped_on_shutdown: now it ends in delivery."""
    down = StubClient(*[TagHistorianConnectionError("down")] * 10)
    forwarder = await started(down)

    hass.states.async_set(POWER, "1500")
    hass.states.async_set(POWER, "1600")
    await hass.async_block_till_done()

    await forwarder.async_shutdown()

    assert forwarder.stats.dropped_shutdown == 0, "saved, not lost"
    saved = _saved(hass_storage)
    assert saved["clean"] is True
    assert [item[0] for item in saved["queue"]] == [POWER, POWER]

    up = StubClient()
    again = await started(up)
    assert again.stats.restored == 2
    await _tick(hass, 2)

    assert up.values == ["1500.0", "1600.0"]
    assert again.stats.sent == 2


async def test_a_started_run_is_marked_on_disk_so_a_crash_cannot_send_the_buffer_twice(
    hass: HomeAssistant, started, hass_storage
) -> None:
    """Restored once, then forgotten on disk: a crash after this must not bring it back."""
    _seed(
        hass_storage,
        clean=True,
        sending_ok=True,
        acked={},
        queue=[[POWER, T0.timestamp(), "W,domain=sensor,entity_id=house_power value=1.0 1"]],
    )

    client = StubClient()
    await started(client)

    saved = _saved(hass_storage)
    assert saved["clean"] is False
    assert saved["queue"] == []
    await _tick(hass, 2)
    assert len(client.lines) == 1


async def test_a_crash_while_writes_went_through_reads_nothing_back(
    hass: HomeAssistant, started, hass_storage, recorder, caplog
) -> None:
    """Some readings after the saved watermark may already be in Tag Historian.

    The live endpoint does not deduplicate, so after a crash with a healthy
    connection nothing is read back: the last moments may be missing, and
    nothing is doubled - and the log says which of the two was chosen.
    """
    _seed(hass_storage, clean=False, sending_ok=True, acked={POWER: T0.timestamp()}, queue=[])
    recorder.record(POWER, "1500", T0 + timedelta(seconds=30))
    recorder.record(POWER, "1600", T0 + timedelta(seconds=60))

    client = StubClient()
    await started(client)
    await hass.async_block_till_done()
    await _tick(hass, 2)

    assert client.lines == []
    assert "did not stop cleanly" in caplog.text


async def test_after_a_crash_during_an_outage_the_outage_is_read_back(
    hass: HomeAssistant, started, hass_storage, recorder
) -> None:
    """While writes were failing nothing was acknowledged, so the watermark is exact."""
    _seed(hass_storage, clean=False, sending_ok=False, acked={POWER: T0.timestamp()}, queue=[])
    recorder.record(POWER, "1400", T0)  # the acknowledged reading itself
    recorder.record(POWER, "1500", T0 + timedelta(seconds=5))  # inside the minimum interval
    recorder.record(POWER, "1600", T0 + timedelta(seconds=30))
    recorder.record(POWER, "unavailable", T0 + timedelta(seconds=40))
    recorder.record(POWER, "1700", T0 + timedelta(seconds=90))

    client = StubClient()
    forwarder = await started(client, min_interval=10)
    await hass.async_block_till_done()
    await _tick(hass, 2)

    assert client.values == ["1600.0", "1700.0"], (
        "after the acknowledged reading, by the live path's own rules: the minimum "
        "interval holds and an unavailable state is a gap"
    )
    assert forwarder.stats.backfilled == 2
    assert forwarder.stats.sent == 2


async def test_a_gap_that_only_held_throttled_readings_stays_empty(
    hass: HomeAssistant, started, hass_storage, recorder
) -> None:
    """In ordinary running a backfill pass finds nothing: everything between two
    sent readings was skipped on purpose."""
    held = T0 + timedelta(seconds=12)
    _seed(
        hass_storage,
        clean=True,
        sending_ok=True,
        acked={POWER: T0.timestamp()},
        queue=[[POWER, held.timestamp(), "W,domain=sensor,entity_id=house_power value=3.0 1"]],
    )
    recorder.record(POWER, "1", T0)
    recorder.record(POWER, "2", T0 + timedelta(seconds=3))
    recorder.record(POWER, "3", held)

    client = StubClient()
    await started(client, min_interval=10)
    await hass.async_block_till_done()
    await _tick(hass, 2)

    assert client.values == ["3.0"], "only the held reading; the throttled one stays skipped"


# --------------------------------------------------------------------------
# An outage that overflows the buffer
# --------------------------------------------------------------------------


async def test_readings_the_buffer_evicted_are_read_back_and_sent_before_the_live_ones(
    hass: HomeAssistant, started, recorder, monkeypatch, freezer
) -> None:
    """The whole point of the watermark.

    Three readings fit, five arrive during the outage, the two oldest are
    evicted. Once Tag Historian answers again the two come back from the
    recorder FIRST - a live reading delivered before them would move the
    watermark past them - and every reading arrives once, in order.
    """
    monkeypatch.setattr(forwarder_module, "MAX_QUEUE_LENGTH", 3)
    freezer.move_to(T0)
    client = StubClient(
        WriteResult(WriteOutcome.OK),
        *[TagHistorianConnectionError("down")] * 3,
    )
    forwarder = await started(client)

    # One delivered reading: the watermark.
    hass.states.async_set(POWER, "10")
    recorder.record(POWER, "10", dt_util.utcnow())
    await hass.async_block_till_done()
    await _tick(hass, 2)
    assert client.values == ["10.0"]

    # Five during the outage; the recorder keeps them all.
    for value in ("11", "12", "13", "14", "15"):
        freezer.tick(timedelta(seconds=20))
        hass.states.async_set(POWER, value)
        recorder.record(POWER, value, dt_util.utcnow())
        await hass.async_block_till_done()
        await _tick(hass, 2)

    assert forwarder.stats.dropped_overflow == 2

    # And back.
    for _ in range(6):
        await _tick(hass, CONNECTION_RETRY_SECONDS + 1)

    assert client.values[:1] == ["10.0"]
    delivered = [value for body in client.bodies[1:] for value in _values(body)]
    assert delivered[-5:] == ["11.0", "12.0", "13.0", "14.0", "15.0"], client.values
    assert forwarder.stats.sent == 6, "every reading once - none of them twice"
    assert forwarder.stats.backfilled == 2


async def test_an_evicted_reading_of_an_entity_with_nothing_else_held_is_read_back(
    hass: HomeAssistant, started, recorder, monkeypatch, freezer
) -> None:
    """The slow entity a chatty one pushes out of the buffer.

    The door reports once during the outage; power readings then evict it,
    and the door holds nothing else. Its gap used to end where the forwarder
    started listening - before its own watermark - so no pass ever read it,
    and the door's next reading moved the watermark past it for good.
    """
    monkeypatch.setattr(forwarder_module, "MAX_QUEUE_LENGTH", 3)
    freezer.move_to(T0)
    client = StubClient(
        WriteResult(WriteOutcome.OK),
        *[TagHistorianConnectionError("down")] * 4,
    )
    forwarder = await started(client, entities=[POWER, DOOR])

    # Both delivered once: the watermarks.
    for entity_id, value in ((POWER, "10"), (DOOR, "on")):
        hass.states.async_set(entity_id, value)
        recorder.record(entity_id, value, dt_util.utcnow())
    await hass.async_block_till_done()
    await _tick(hass, 2)

    # The outage: the door reports once, then power evicts it.
    for entity_id, value in ((DOOR, "off"), (POWER, "11"), (POWER, "12"), (POWER, "13")):
        freezer.tick(timedelta(seconds=20))
        hass.states.async_set(entity_id, value)
        recorder.record(entity_id, value, dt_util.utcnow())
        await hass.async_block_till_done()
        await _tick(hass, 2)
    assert forwarder.stats.dropped_overflow == 1

    for _ in range(6):
        await _tick(hass, CONNECTION_RETRY_SECONDS + 1)

    # The door reports again, later, and is delivered as usual.
    freezer.tick(timedelta(minutes=10))
    hass.states.async_set(DOOR, "on")
    recorder.record(DOOR, "on", dt_util.utcnow())
    await hass.async_block_till_done()
    await _tick(hass, 2)

    assert client.delivered_values("back_door") == ["1.0", "0.0", "1.0"], (
        "the evicted reading is read back, and before the door's next one"
    )
    assert client.delivered_values("house_power") == ["10.0", "11.0", "12.0", "13.0"]
    assert forwarder.stats.backfilled == 1


async def test_readings_let_go_of_while_the_allowance_is_spent_are_not_read_back(
    hass: HomeAssistant, started, recorder, monkeypatch, freezer
) -> None:
    """Over the allowance is not an outage.

    Read back at midnight, the evening's evictions went in front of the new
    day's readings and spent its allowance first, so an account that produces
    more than its allowance every day fell further behind live every day. Now
    the held buffer goes at midnight, as before read-back existed, and what
    was let go of while the allowance was spent stays counted, not re-sent.
    """
    monkeypatch.setattr(forwarder_module, "MAX_QUEUE_LENGTH", 3)
    freezer.move_to(T0)
    client = StubClient(
        WriteResult(WriteOutcome.OK),
        WriteResult(WriteOutcome.DAILY_QUOTA, retry_after=600, message="spent"),
    )
    forwarder = await started(client)

    hass.states.async_set(POWER, "10")
    recorder.record(POWER, "10", dt_util.utcnow())
    await hass.async_block_till_done()
    await _tick(hass, 2)

    for value in ("11", "12", "13", "14", "15"):
        freezer.tick(timedelta(seconds=20))
        hass.states.async_set(POWER, value)
        recorder.record(POWER, value, dt_util.utcnow())
        await hass.async_block_till_done()
        await _tick(hass, 2)
    assert forwarder.stats.dropped_overflow == 2

    await _tick(hass, 601)  # the allowance resets
    for _ in range(3):
        await _tick(hass, 2)

    assert client.delivered_values("house_power") == ["10.0", "13.0", "14.0", "15.0"], (
        "the held buffer, and not the two let go of over the allowance"
    )
    assert forwarder.stats.backfilled == 0


async def test_an_outage_backlog_waiting_when_the_allowance_runs_out_is_still_delivered(
    hass: HomeAssistant, started, recorder, monkeypatch, freezer
) -> None:
    """The exception to the rule above, and why it exists.

    A reading read back from an outage is waiting when the allowance runs
    out. Letting go of a newer reading of that entity for good would move its
    watermark past the waiting one, and a restart - which drops what was read
    back, because the recorder still has it - would then lose it with no
    counter moving. So that entity's eviction is read back like an outage's.
    """
    monkeypatch.setattr(forwarder_module, "MAX_QUEUE_LENGTH", 3)
    freezer.move_to(T0)
    client = StubClient(
        WriteResult(WriteOutcome.OK),
        TagHistorianConnectionError("down"),
        WriteResult(WriteOutcome.DAILY_QUOTA, retry_after=600, message="spent"),
    )
    forwarder = await started(client)

    hass.states.async_set(POWER, "10")
    recorder.record(POWER, "10", dt_util.utcnow())
    await hass.async_block_till_done()
    await _tick(hass, 2)

    # The outage, short enough that no retry falls inside it: 11 is evicted
    # and read back.
    for value in ("11", "12", "13", "14"):
        freezer.tick(timedelta(seconds=5))
        hass.states.async_set(POWER, value)
        recorder.record(POWER, value, dt_util.utcnow())
        await hass.async_block_till_done()
        await _tick(hass, 2)
    assert forwarder.stats.backfilled == 1

    # Tag Historian answers again - with "today's allowance is spent".
    await _tick(hass, CONNECTION_RETRY_SECONDS + 1)
    freezer.tick(timedelta(seconds=5))
    hass.states.async_set(POWER, "15")
    recorder.record(POWER, "15", dt_util.utcnow())
    await hass.async_block_till_done()
    await _tick(hass, 2)

    await _tick(hass, 601)
    for _ in range(3):
        await _tick(hass, 2)

    assert client.delivered_values("house_power") == ["10.0", "11.0", "12.0", "13.0", "14.0", "15.0"]
    assert forwarder.stats.backfilled == 2


async def test_a_crash_before_the_first_answer_reads_the_restored_buffer_and_the_gap_back(
    hass: HomeAssistant, entry, hass_storage, recorder, freezer
) -> None:
    """A run that has had no answer yet has acknowledged nothing.

    The clean stop saved one reading and the watermark before it; the next run
    restored them and died before its first write was answered - a boot loop,
    a second power flicker. That run used to be saved as "writes going
    through" from its first moment, so the run after it dropped the watermark
    and read nothing back: the saved reading and the gap before it, gone with
    no counter moving.
    """
    freezer.move_to(T0 + timedelta(minutes=5))
    held = T0 + timedelta(seconds=60)
    _seed(
        hass_storage,
        clean=True,
        sending_ok=False,
        acked={POWER: T0.timestamp()},
        queue=[[POWER, held.timestamp(), "W,domain=sensor,entity_id=house_power value=3.0 1"]],
    )
    recorder.record(POWER, "2", T0 + timedelta(seconds=30))
    recorder.record(POWER, "3", held)

    crashed = StateForwarder(hass, entry, StubClient(), [POWER], 0, store=Store(hass, STORAGE_VERSION, KEY))
    await crashed.async_restore()
    assert _saved(hass_storage)["sending_ok"] is False, "nothing answered yet"
    # ... and Home Assistant dies here.

    client = StubClient()
    forwarder = StateForwarder(hass, entry, client, [POWER], 0, store=Store(hass, STORAGE_VERSION, KEY))
    await forwarder.async_restore()
    forwarder.async_start()
    await hass.async_block_till_done()
    await _tick(hass, 2)
    forwarder.async_stop()

    assert client.delivered_values("house_power") == ["2.0", "3.0"]


async def test_the_first_answer_marks_writes_as_going_through(
    hass: HomeAssistant, started, hass_storage
) -> None:
    client = StubClient()
    await started(client)
    assert _saved(hass_storage)["sending_ok"] is False

    hass.states.async_set(POWER, "1500")
    await hass.async_block_till_done()
    await _tick(hass, 2)

    assert _saved(hass_storage)["sending_ok"] is True


async def test_a_batch_on_the_wire_is_saved_as_answered_so_it_is_never_read_back(
    hass: HomeAssistant, entry, hass_storage
) -> None:
    """Its answer may never come; it may have landed. Overstate the loss, never double."""
    forwarder = StateForwarder(
        hass, entry, StubClient(), [POWER], 0, store=Store(hass, STORAGE_VERSION, KEY)
    )
    forwarder._acked = {POWER: T0.timestamp()}
    forwarder._inflight = [Queued(POWER, (T0 + timedelta(minutes=1)).timestamp(), "x")]

    assert await forwarder._async_save(clean=True)

    assert _saved(hass_storage)["acked"][POWER] == (T0 + timedelta(minutes=1)).timestamp()


async def test_without_a_recorder_the_live_path_is_not_held_up(
    hass: HomeAssistant, started
) -> None:
    """No recorder, nothing to read back: evicted readings are counted, live ones flow."""
    client = StubClient()
    await started(client)

    hass.states.async_set(POWER, "1500")
    await hass.async_block_till_done()
    await _tick(hass, 2)

    assert client.values == ["1500.0"]


# --------------------------------------------------------------------------
# Through Home Assistant's own reload
# --------------------------------------------------------------------------


async def test_a_reload_hands_the_undelivered_buffer_to_the_next_forwarder(
    hass: HomeAssistant, hass_storage
) -> None:
    """An options change is an unload and a load. What the first could not send, the second does."""
    entry = MockConfigEntry(
        domain=DOMAIN,
        data={CONF_HOST: "api.taghistorian.com", CONF_API_KEY: "not-a-real-key"},
        options={CONF_ENTITIES: [POWER], CONF_MIN_INTERVAL: 10},
        unique_id="11111111-2222-3333-4444-555555555555",
    )
    entry.add_to_hass(hass)
    client = make_client()
    client.async_write.side_effect = TagHistorianConnectionError("down")

    with patch("custom_components.tag_historian.TagHistorianClient", return_value=client):
        assert await hass.config_entries.async_setup(entry.entry_id)
        await hass.async_block_till_done()
        hass.states.async_set(POWER, "1500")
        await hass.async_block_till_done()

        assert await hass.config_entries.async_unload(entry.entry_id)
        await hass.async_block_till_done()
        key = f"tag_historian.{entry.entry_id}.forwarder"
        assert len(hass_storage[key]["data"]["queue"]) == 1

        client.async_write.side_effect = None
        client.async_write.return_value = WriteResult(WriteOutcome.OK)
        assert await hass.config_entries.async_setup(entry.entry_id)
        await hass.async_block_till_done()
        await _tick(hass, 2)

        sent = [call.args[0] for call in client.async_write.await_args_list]
        assert "value=1500.0" in sent[-1]
        assert entry.runtime_data.forwarder.stats.restored == 1

        assert await hass.config_entries.async_unload(entry.entry_id)
        await hass.async_block_till_done()

    await hass.config_entries.async_remove(entry.entry_id)
    await hass.async_block_till_done()
    assert key not in hass_storage, "a removed account's saved buffer goes with it"
