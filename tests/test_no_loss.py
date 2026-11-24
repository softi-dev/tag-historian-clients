"""Every reading once, whatever stops the forwarder and whenever.

The stand-in here is a Tag Historian that keeps what it takes, as the real one
does - without deduplicating - and says per tag how new the newest reading it
has is. Each test ends Home Assistant, the entry or the connection at an
awkward moment and then asks one question: is every reading there, once.
"""

# ruff: noqa: F811
from __future__ import annotations

import asyncio
import copy
import random
from collections import Counter
from datetime import timedelta
from unittest.mock import patch

import pytest
from homeassistant.config_entries import ConfigEntryState
from homeassistant.core import HomeAssistant
from homeassistant.util import dt as dt_util
from pytest_homeassistant_custom_component.common import async_fire_time_changed

from custom_components.tag_historian import forwarder as forwarder_module
from custom_components.tag_historian.api import (
    TagHistorianConnectionError,
    TagHistorianPermissionError,
    TagHistorianUnansweredError,
    WriteOutcome,
    WriteResult,
)
from custom_components.tag_historian.const import CONF_ENTITIES, CONF_MIN_INTERVAL

from .conftest import make_client, without_statistics
from .test_persistence import (  # noqa: F401 - the fixtures are used by name
    DOOR,
    KEY,
    POWER,
    T0,
    _saved,
    _seed,
    _values,
    entry,
    recorder,
    started,
)

OK = WriteResult(WriteOutcome.OK)
DOWN = TagHistorianConnectionError("down")
ERROR = WriteResult(WriteOutcome.SERVER_ERROR, message="internal error")


async def spin(n: int = 60) -> None:
    for _ in range(n):
        await asyncio.sleep(0)


class Server:
    """A Tag Historian: keeps every line it takes, and knows its newest per tag."""

    def __init__(self) -> None:
        self.down = False
        self.stored: list[str] = []
        self.requests = 0
        self.asked = 0
        # Called with the body before it is stored; may raise, wait or answer.
        self.before = None
        self.cannot_tell: Exception | None = None

    async def async_write(self, body: str) -> WriteResult:
        self.requests += 1
        await spin(5)
        if self.down:
            raise DOWN
        if self.before is not None:
            answer = await self.before(body)
            if answer is not None:
                return answer
        self.stored.extend(body.splitlines())
        return OK

    async def async_get_newest_readings(self) -> dict[str, float | None]:
        self.asked += 1
        await spin(2)
        if self.down:
            raise DOWN
        if self.cannot_tell is not None:
            raise self.cannot_tell
        newest: dict[str, float | None] = {}
        for line in self.stored:
            tags = dict(part.split("=", 1) for part in line.split(" ")[0].split(",")[1:])
            name = f"{tags['domain']}.{tags['entity_id']}"
            stamp = float(line.rsplit(" ", 1)[1])
            newest[name] = max(stamp, newest.get(name) or 0.0)
        return newest

    def values(self, entity: str = "house_power") -> list[float]:
        return [float(_values(line)[0]) for line in self.stored if f"entity_id={entity} " in line]

    def everything_once(self, first: int, last: int) -> None:
        got = self.values()
        assert sorted(set(got)) == [float(v) for v in range(first, last + 1)], got
        assert [v for v, n in Counter(got).items() if n > 1] == [], "sent twice"


async def put(hass, recorder, freezer, entity_id, value, seconds: float = 20) -> None:
    freezer.tick(timedelta(seconds=seconds))
    hass.states.async_set(entity_id, str(value))
    recorder.record(entity_id, str(value), dt_util.utcnow())
    await spin()


def missed(recorder, freezer, entity_id, value, seconds: float = 20) -> None:
    """A reading the recorder has and no forwarder was there for."""
    freezer.tick(timedelta(seconds=seconds))
    recorder.record(entity_id, str(value), dt_util.utcnow())


async def later(hass, seconds: float = 2) -> None:
    async_fire_time_changed(hass, dt_util.utcnow() + timedelta(seconds=seconds))
    await spin(100)


async def drain(hass, freezer, rounds: int = 30) -> None:
    for _ in range(rounds):
        freezer.tick(timedelta(seconds=31))
        async_fire_time_changed(hass)
        await spin(100)


def kill(forwarder, hass_storage) -> None:
    """Home Assistant is gone: what is on disk stays as it is, and nothing is flushed."""
    disk = copy.deepcopy(hass_storage.get(KEY))
    forwarder.async_stop()
    forwarder._closed = True
    if forwarder._flush_task is not None:
        forwarder._flush_task.cancel()
    if disk is not None:
        hass_storage[KEY] = disk


@pytest.fixture(autouse=True)
def at_t0(freezer) -> None:
    freezer.move_to(T0)


# --------------------------------------------------------------------------
# P1 - a stop in an outage, and a start that comes late
# --------------------------------------------------------------------------


async def test_what_came_after_the_saved_buffer_and_before_listening_is_read_back(
    hass: HomeAssistant, started, recorder, freezer
) -> None:
    server = Server()
    forwarder = await started(server)
    await put(hass, recorder, freezer, POWER, 10)
    await later(hass)
    server.down = True
    for value in (11, 12, 13):
        await put(hass, recorder, freezer, POWER, value)
        await later(hass)
    await forwarder.async_shutdown()

    for value in (14, 15, 16):
        missed(recorder, freezer, POWER, value)
    freezer.tick(timedelta(seconds=20))

    server.down = False
    await started(server)
    await put(hass, recorder, freezer, POWER, 17)
    await drain(hass, freezer)

    server.everything_once(10, 17)
    assert server.values() == sorted(server.values()), "oldest first"


async def test_stops_in_one_outage_leave_several_holes_and_all_are_read_back(
    hass: HomeAssistant, started, recorder, freezer
) -> None:
    server = Server()
    value = 10
    forwarder = await started(server)
    await put(hass, recorder, freezer, POWER, value)
    await later(hass)
    server.down = True
    for _ in range(3):
        for _ in range(2):
            value += 1
            await put(hass, recorder, freezer, POWER, value)
            await later(hass)
        await forwarder.async_shutdown()
        for _ in range(2):
            value += 1
            missed(recorder, freezer, POWER, value)
        freezer.tick(timedelta(seconds=20))
        forwarder = await started(server)
    server.down = False
    value += 1
    await put(hass, recorder, freezer, POWER, value)
    await drain(hass, freezer)

    server.everything_once(10, value)


async def test_the_real_entry_listens_and_buffers_before_tag_historian_can_be_reached(
    hass: HomeAssistant, recorder, freezer, entry, monkeypatch
) -> None:
    without_statistics(monkeypatch)
    hass.config_entries.async_update_entry(
        entry,
        options={CONF_ENTITIES: [POWER, DOOR], CONF_MIN_INTERVAL: 0, "import_on_setup": False},
    )
    server = Server()
    client = make_client()
    client.async_write = server.async_write
    client.async_get_newest_readings = server.async_get_newest_readings

    with patch("custom_components.tag_historian.TagHistorianClient", return_value=client):
        assert await hass.config_entries.async_setup(entry.entry_id)
        await hass.async_block_till_done()
        await put(hass, recorder, freezer, POWER, 10)
        await put(hass, recorder, freezer, DOOR, "on", 1)
        await later(hass)
        server.down = True
        for value in (11, 12, 13):
            await put(hass, recorder, freezer, POWER, value)
            await later(hass)

        client.async_get_quota.side_effect = DOWN
        await hass.config_entries.async_reload(entry.entry_id)
        await spin()
        assert entry.state is ConfigEntryState.LOADED
        assert server.values() == [10.0], "nothing is written before it is reachable"

        # Not in the recorder at all: only a forwarder that listened has them.
        for value in (14, 15):
            freezer.tick(timedelta(seconds=20))
            hass.states.async_set(POWER, str(value))
            await spin()
        await put(hass, recorder, freezer, DOOR, "off", 1)

        server.down = False
        client.async_get_quota.side_effect = None
        await drain(hass, freezer)
        await put(hass, recorder, freezer, POWER, 16)
        await drain(hass, freezer)

        server.everything_once(10, 16)
        assert server.values("back_door") == [1.0, 0.0]
        await hass.config_entries.async_unload(entry.entry_id)
        await hass.async_block_till_done()


async def test_an_entity_never_delivered_is_read_back_from_where_the_first_run_listened(
    hass: HomeAssistant, started, recorder, freezer
) -> None:
    server = Server()
    server.down = True
    forwarder = await started(server)
    await forwarder.async_shutdown()
    for value in (1, 2):
        missed(recorder, freezer, POWER, value)
    freezer.tick(timedelta(seconds=20))
    server.down = False
    again = await started(server)
    assert again.history_boundary(POWER) == T0.timestamp(), "the import stops where this begins"
    await drain(hass, freezer)
    server.everything_once(1, 2)


# --------------------------------------------------------------------------
# P2, P5 - killed
# --------------------------------------------------------------------------


async def test_killed_during_a_drain_the_rest_of_the_backlog_is_sent(
    hass: HomeAssistant, started, recorder, freezer, monkeypatch, hass_storage
) -> None:
    monkeypatch.setattr(forwarder_module, "MAX_POINTS_PER_REQUEST", 3)
    server = Server()
    forwarder = await started(server)
    await put(hass, recorder, freezer, POWER, 10)
    await later(hass)
    server.down = True
    for value in range(11, 21):
        await put(hass, recorder, freezer, POWER, value)
    await later(hass)
    server.down = False
    gate = asyncio.Event()

    async def hang_after_three(_body):
        if len(server.stored) >= 4:
            await gate.wait()
        return None

    server.before = hang_after_three
    for seconds in (40, 45, 50):
        await later(hass, seconds)
    assert 1 < len(server.stored) < 11
    kill(forwarder, hass_storage)

    server.before = None
    freezer.tick(timedelta(seconds=60))
    await started(server)
    await put(hass, recorder, freezer, POWER, 21)
    await drain(hass, freezer)
    server.everything_once(10, 21)


async def test_killed_while_healthy_and_started_long_before_the_api_is_reachable(
    hass: HomeAssistant, started, recorder, freezer, hass_storage
) -> None:
    server = Server()
    forwarder = await started(server)
    for value in (10, 11, 12):
        await put(hass, recorder, freezer, POWER, value)
        await later(hass)
    kill(forwarder, hass_storage)
    for value in (13, 14, 15):
        missed(recorder, freezer, POWER, value, 600)
    server.down = True
    freezer.tick(timedelta(seconds=5))
    await started(server)
    await put(hass, recorder, freezer, POWER, 16)
    await drain(hass, freezer, 3)
    server.down = False
    await drain(hass, freezer)
    server.everything_once(10, 16)


async def test_killed_twice_before_tag_historian_could_be_asked(
    hass: HomeAssistant, started, recorder, freezer, hass_storage
) -> None:
    server = Server()
    forwarder = await started(server)
    for value in (10, 11):
        await put(hass, recorder, freezer, POWER, value)
        await later(hass)
    for value in (12, 13):
        kill(forwarder, hass_storage)
        server.down = True
        missed(recorder, freezer, POWER, value)
        freezer.tick(timedelta(seconds=10))
        forwarder = await started(server)
        await later(hass)
    await forwarder.async_shutdown()
    server.down = False
    freezer.tick(timedelta(seconds=60))
    await started(server)
    await drain(hass, freezer)
    server.everything_once(10, 13)


async def test_killed_the_restored_buffer_is_still_on_disk(
    hass: HomeAssistant, started, freezer, hass_storage, recorder
) -> None:
    """Of an entity the recorder leaves out the saved buffer is the only copy."""
    server = Server()
    server.down = True
    forwarder = await started(server)
    for value in (1, 2):
        freezer.tick(timedelta(seconds=20))
        hass.states.async_set(POWER, str(value))
        await spin()
    await forwarder.async_shutdown()
    second = await started(server)
    kill(second, hass_storage)
    assert _saved(hass_storage)["queue"] == [], "an older version must not send it twice"
    server.down = False
    await started(server)
    await drain(hass, freezer)
    server.everything_once(1, 2)


async def test_when_tag_historian_cannot_say_a_bounded_stretch_is_counted_not_sent_twice(
    hass: HomeAssistant, started, recorder, freezer, hass_storage
) -> None:
    server = Server()
    forwarder = await started(server)
    await put(hass, recorder, freezer, POWER, 10)
    await later(hass)
    await put(hass, recorder, freezer, POWER, 11)
    await later(hass)
    kill(forwarder, hass_storage)
    # Well past anything the killed run can have sent without saving it.
    missed(recorder, freezer, POWER, 12, 3600)
    missed(recorder, freezer, POWER, 13)
    freezer.tick(timedelta(seconds=5))
    server.cannot_tell = TagHistorianPermissionError("/api/tags")
    again = await started(server)
    await drain(hass, freezer)
    server.everything_once(10, 13)
    assert again.stats.dropped_inflight == 1, "11 may be there or not: counted, not sent again"


async def test_the_file_of_an_older_version_loads_after_a_crash(
    hass: HomeAssistant, started, recorder, freezer, hass_storage
) -> None:
    server = Server()
    server.stored = [f"W,domain=sensor,entity_id=house_power value=5.0 {int(T0.timestamp()) + 60}"]
    _seed(hass_storage, clean=False, sending_ok=True, acked={POWER: T0.timestamp()}, queue=[])
    recorder.record(POWER, "4", T0 + timedelta(seconds=30))
    recorder.record(POWER, "5", T0 + timedelta(seconds=60, milliseconds=400))
    recorder.record(POWER, "6", T0 + timedelta(seconds=90))
    freezer.tick(timedelta(seconds=120))
    await started(server)
    await drain(hass, freezer)
    assert server.values() == [5.0, 6.0]


@pytest.mark.parametrize(
    "extra",
    [
        {},
        {"segments": "x", "doubt": [1], "since": 5, "counters": {"dropped_quota": "many"}},
        {"segments": [7, -1], "doubt": {POWER: "x"}, "held": {}, "doubt_until": "never"},
    ],
)
async def test_a_file_with_nonsense_in_it_still_gives_its_readings(
    hass: HomeAssistant, started, recorder, freezer, hass_storage, extra
) -> None:
    _seed(
        hass_storage,
        clean=True,
        sending_ok=False,
        acked={POWER: T0.timestamp()},
        queue=[[POWER, T0.timestamp() + 5, "W,domain=sensor,entity_id=house_power value=3.0 5"]],
        **extra,
    )
    server = Server()
    await started(server)
    await drain(hass, freezer, 5)
    assert server.values() == [3.0]


# --------------------------------------------------------------------------
# P3 - a stop while a read-back is unfinished
# --------------------------------------------------------------------------


async def test_a_clean_stop_during_a_read_back_leaves_the_rest_to_the_next_start(
    hass: HomeAssistant, started, recorder, freezer, monkeypatch, hass_storage
) -> None:
    monkeypatch.setattr(forwarder_module, "BACKFILL_BATCH_SIZE", 2)
    _seed(
        hass_storage,
        clean=True,
        sending_ok=False,
        acked={POWER: T0.timestamp(), DOOR: T0.timestamp()},
        queue=[],
    )
    for value in range(11, 25):
        missed(recorder, freezer, POWER, value)
        recorder.record(DOOR, "on" if value % 2 else "off", dt_util.utcnow())
    freezer.tick(timedelta(seconds=20))
    server = Server()
    forwarder = await started(server, entities=[POWER, DOOR])
    await put(hass, recorder, freezer, POWER, 25)
    await put(hass, recorder, freezer, DOOR, "on", 1)
    await forwarder.async_shutdown()
    await spin()

    await started(server, entities=[POWER, DOOR])
    await drain(hass, freezer)
    server.everything_once(11, 25)
    assert len(server.values("back_door")) == 15


# --------------------------------------------------------------------------
# P4 - a batch whose fate is unknown
# --------------------------------------------------------------------------


@pytest.mark.parametrize("landed", [False, True])
async def test_a_stop_with_a_write_open_keeps_the_batch_and_asks_before_sending_it(
    hass: HomeAssistant, started, recorder, freezer, monkeypatch, hass_storage, landed
) -> None:
    monkeypatch.setattr(forwarder_module, "INFLIGHT_WAIT_SECONDS", -1)
    server = Server()
    forwarder = await started(server)
    await put(hass, recorder, freezer, POWER, 10)
    await later(hass)
    gate = asyncio.Event()

    async def hang(body):
        if landed:
            server.stored.extend(body.splitlines())
        await gate.wait()
        raise TagHistorianUnansweredError("gone")

    server.before = hang
    for value in range(11, 15):
        await put(hass, recorder, freezer, POWER, value, 1.1 if value == 11 else 0.2)
    await later(hass)
    inflight = len(forwarder._inflight or [])
    await forwarder.async_shutdown()
    gate.set()
    assert inflight == 4
    await spin()
    assert len(_saved(hass_storage)["queue"]) == 4
    assert forwarder.stats.dropped_shutdown == 0
    assert forwarder.stats.dropped_inflight == 0

    server.before = None
    again = await started(server)
    await put(hass, recorder, freezer, POWER, 15, 5)
    await later(hass, 2)
    assert server.asked == 0, "a request cut short a moment ago is given time to land"
    await drain(hass, freezer)
    server.everything_once(10, 15)
    assert again.stats.dropped_inflight == 0


@pytest.mark.parametrize("landed", [False, True])
async def test_a_write_that_times_out_is_sent_again_only_if_it_did_not_land(
    hass: HomeAssistant, started, recorder, freezer, landed
) -> None:
    server = Server()
    forwarder = await started(server)

    async def time_out(body):
        server.before = None
        if landed:
            server.stored.extend(body.splitlines())
        raise TagHistorianUnansweredError("no answer within 10 seconds")

    server.before = time_out
    for value in (1, 2, 3):
        await put(hass, recorder, freezer, POWER, value)
    await drain(hass, freezer, 5)
    await put(hass, recorder, freezer, POWER, 4)
    await drain(hass, freezer, 3)
    server.everything_once(1, 4)
    assert forwarder.stats.sent == 4


async def test_an_unknown_batch_tag_historian_cannot_say_about_is_counted_and_the_count_survives(
    hass: HomeAssistant, started, recorder, freezer
) -> None:
    server = Server()
    server.cannot_tell = TagHistorianPermissionError("/api/tags")
    forwarder = await started(server)

    async def time_out(_body):
        server.before = None
        raise TagHistorianUnansweredError("no answer within 10 seconds")

    server.before = time_out
    for value in (1, 2, 3):
        await put(hass, recorder, freezer, POWER, value, 0.05)
    await drain(hass, freezer, 5)
    assert forwarder.stats.dropped_inflight == 3
    await put(hass, recorder, freezer, POWER, 4)
    await drain(hass, freezer, 3)
    server.everything_once(4, 4)
    await forwarder.async_shutdown()

    # A new process: the counters start from nothing but the file.
    again = await started(server)
    assert again.stats is not forwarder.stats
    assert again.stats.dropped_inflight == 3


async def test_a_reload_does_not_add_the_saved_counters_to_the_ones_still_in_memory(
    hass: HomeAssistant, started, recorder
) -> None:
    server = Server()
    first = await started(server)
    first.stats.dropped_quota = 7
    await first.async_shutdown()
    again = await started(server)
    assert again.stats.dropped_quota == 7
    await again.async_restore()
    assert again.stats.dropped_quota == 7


# --------------------------------------------------------------------------
# P6 - an error of Tag Historian's own
# --------------------------------------------------------------------------


async def test_an_unexpected_500_does_not_drop_the_batch(
    hass: HomeAssistant, started, recorder, freezer
) -> None:
    server = Server()
    forwarder = await started(server)
    left = 4

    async def fail_four_times(_body):
        nonlocal left
        left -= 1
        return ERROR if left >= 0 else None

    server.before = fail_four_times
    for value in range(1, 9):
        await put(hass, recorder, freezer, POWER, value, 0.05)
    await drain(hass, freezer, 60)
    server.everything_once(1, 8)
    assert forwarder.stats.dropped_quota == 0


async def test_a_500_after_the_batch_was_stored_does_not_send_it_twice(
    hass: HomeAssistant, started, recorder, freezer
) -> None:
    server = Server()
    await started(server)

    async def stored_then_error(body):
        server.before = None
        server.stored.extend(body.splitlines())
        return ERROR

    server.before = stored_then_error
    for value in range(1, 9):
        await put(hass, recorder, freezer, POWER, value)
    await drain(hass, freezer, 20)
    server.everything_once(1, 8)


@pytest.mark.parametrize("others", [True, False])
async def test_a_reading_that_always_gets_a_500_is_let_go_of_alone(
    hass: HomeAssistant, started, recorder, freezer, others, caplog
) -> None:
    server = Server()
    server.down = True
    forwarder = await started(server, entities=[POWER, DOOR])

    async def poison(body):
        return ERROR if "value=5.0" in body else None

    server.before = poison
    for value in range(1, 13):
        await put(hass, recorder, freezer, POWER, value)
        if others:
            await put(hass, recorder, freezer, DOOR, "on" if value % 2 else "off", 1)
    server.down = False
    await drain(hass, freezer, 100)
    assert server.values() == [float(v) for v in range(1, 13) if v != 5]
    assert len(server.values("back_door")) == (12 if others else 0)
    assert forwarder.stats.dropped_quota == 1
    assert f"one reading of {POWER}" in caplog.text
    assert forwarder.stats.queued == 0


async def test_a_long_500_outage_drops_nothing_and_goes_back_to_whole_batches(
    hass: HomeAssistant, started, recorder, freezer
) -> None:
    server = Server()
    forwarder = await started(server, entities=[POWER, DOOR])
    failing = True

    async def broken(_body):
        return ERROR if failing else None

    server.before = broken
    for value in range(1, 13):
        await put(hass, recorder, freezer, POWER, value)
        await put(hass, recorder, freezer, DOOR, "on" if value % 2 else "off", 1)
    await drain(hass, freezer, 200)  # well over an hour
    assert forwarder.stats.dropped_quota == 0
    failing = False
    await drain(hass, freezer, 60)
    server.everything_once(1, 12)
    assert len(server.values("back_door")) == 12
    for value in range(13, 40):
        await put(hass, recorder, freezer, POWER, value)
        await drain(hass, freezer, 1)
    assert forwarder._error_limit is None
    server.everything_once(1, 39)


async def test_nothing_but_500s_for_a_day_lets_go_of_the_batch_counted(
    hass: HomeAssistant, started, recorder, freezer, monkeypatch
) -> None:
    monkeypatch.setattr(forwarder_module, "SERVER_ERROR_GIVE_UP_SECONDS", 600)
    server = Server()
    server.down = True
    forwarder = await started(server)

    async def broken(_body):
        return ERROR

    server.before = broken
    for value in range(1, 5):
        await put(hass, recorder, freezer, POWER, value)
    server.down = False
    await drain(hass, freezer, 40)
    assert forwarder.stats.dropped_quota == 4
    server.before = None
    await put(hass, recorder, freezer, POWER, 5)
    await drain(hass, freezer, 5)
    server.everything_once(5, 5)


async def test_a_write_that_raises_something_unforeseen_is_kept_too(
    hass: HomeAssistant, started, recorder, freezer
) -> None:
    server = Server()
    forwarder = await started(server)

    async def boom(_body):
        server.before = None
        raise RuntimeError("unforeseen")

    server.before = boom
    for value in (1, 2):
        await put(hass, recorder, freezer, POWER, value)
    await drain(hass, freezer, 5)
    server.everything_once(1, 2)
    assert forwarder.stats.dropped_inflight == 0


# --------------------------------------------------------------------------
# What it must not cost
# --------------------------------------------------------------------------


async def test_a_healthy_run_saves_its_watermark_now_and_then_and_never_asks(
    hass: HomeAssistant, started, recorder, freezer, hass_storage
) -> None:
    server = Server()
    await started(server)
    for value in range(1, 40):
        await put(hass, recorder, freezer, POWER, value)
        await later(hass)
    assert server.asked == 0
    newest = recorder.states[POWER][-1].last_updated.timestamp()
    assert newest - _saved(hass_storage)["acked"][POWER] <= 330
    assert len(recorder.reads) <= 1, "nothing to fill, nothing read"


async def test_the_buffer_stays_bounded_with_restored_readings_in_it(
    hass: HomeAssistant, started, recorder, freezer, monkeypatch
) -> None:
    monkeypatch.setattr(forwarder_module, "MAX_QUEUE_LENGTH", 3)
    server = Server()
    server.down = True
    value = 0
    for _ in range(3):
        forwarder = await started(server)
        for _ in range(4):
            value += 1
            await put(hass, recorder, freezer, POWER, value)
        assert len(forwarder._queue) <= 3
        assert sum(forwarder._segments) <= 3
        await forwarder.async_shutdown()
    server.down = False
    await started(server)
    await drain(hass, freezer)
    server.everything_once(1, value)


async def test_what_the_recorder_has_not_written_yet_is_waited_for_but_not_for_ever(
    hass: HomeAssistant, monkeypatch
) -> None:
    from custom_components.tag_historian import history

    class Stuck:
        async def async_block_till_done(self):
            await asyncio.Event().wait()

    monkeypatch.setattr(history, "recorder_ready", lambda hass: True)
    monkeypatch.setattr("homeassistant.components.recorder.get_instance", lambda hass: Stuck())
    assert await history.async_written(hass, 0) is False


# --------------------------------------------------------------------------
# All of it at once
# --------------------------------------------------------------------------


@pytest.mark.parametrize("seed", range(40))
async def test_any_order_of_outages_stops_and_kills_gives_every_reading_once(
    hass: HomeAssistant, started, recorder, freezer, monkeypatch, hass_storage, seed
) -> None:
    """Everything the recorder has, once and nothing twice - whatever happened on the way."""
    monkeypatch.setattr(forwarder_module, "MAX_QUEUE_LENGTH", 8)
    monkeypatch.setattr(forwarder_module, "MAX_POINTS_PER_REQUEST", 3)
    monkeypatch.setattr(forwarder_module, "BACKFILL_BATCH_SIZE", 2)
    monkeypatch.setattr(forwarder_module, "INFLIGHT_WAIT_SECONDS", -1)
    dice = random.Random(seed)
    server = Server()
    gates: list[asyncio.Event] = []

    async def trouble(body):
        kind = dice.choice(["ok"] * 5 + ["lost", "landed", "error", "busy", "hang", "hang-landed"])
        if kind in ("landed", "hang-landed"):
            server.stored.extend(body.splitlines())
        if kind in ("lost", "landed"):
            raise TagHistorianUnansweredError("cut off")
        if kind.startswith("hang"):
            if forwarder._stopping:  # the real client gives up after ten seconds
                raise TagHistorianUnansweredError("no answer")
            gate = asyncio.Event()
            gates.append(gate)
            await gate.wait()
            raise TagHistorianUnansweredError("hung")
        if kind == "error":
            return ERROR
        if kind == "busy":
            return WriteResult(WriteOutcome.UNAVAILABLE, retry_after=5)
        return None

    server.before = trouble
    value = 0
    door = False
    entities = [POWER, DOOR]
    forwarder = await started(server, entities=entities)
    for _ in range(60):
        step = dice.choice(["put"] * 5 + ["time"] * 4 + ["down", "up", "stop", "kill", "answer"])
        if step == "put":
            value += 1
            if dice.random() < 0.3:
                door = not door  # Home Assistant says nothing of a state that stays
                await put(hass, recorder, freezer, DOOR, "on" if door else "off", 7)
            await put(hass, recorder, freezer, POWER, value, dice.choice([2, 20, 400]))
        elif step == "time":
            await drain(hass, freezer, 1)
        elif step in ("down", "up"):
            server.down = step == "down"
        elif step == "answer" and gates:
            gates.pop().set()
            await spin()
        elif step in ("stop", "kill"):
            if step == "stop":
                await forwarder.async_shutdown()
            else:
                kill(forwarder, hass_storage)
            for _ in range(dice.randrange(3)):
                value += 1
                missed(recorder, freezer, POWER, value, dice.choice([2, 90]))
            freezer.tick(timedelta(seconds=dice.choice([1, 45])))
            forwarder = await started(server, entities=entities)
            await spin()
        assert len(forwarder._queue) <= 8
    for gate in gates:
        gate.set()
    server.before = None
    server.down = False
    await drain(hass, freezer, 60)
    assert (forwarder.stats.dropped_quota, forwarder.stats.dropped_inflight) == (0, 0)
    assert forwarder.stats.queued == 0
    server.everything_once(1, value)
    doors = [line.rsplit(" ", 1)[1] for line in server.stored if "back_door" in line]
    assert len(doors) == len(set(doors)) == len(recorder.states.get(DOOR, []))
