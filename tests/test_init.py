"""Setting an entry up, and the sensors it brings with it."""

from __future__ import annotations

import asyncio
from datetime import timedelta
from unittest.mock import patch

from homeassistant.config_entries import ConfigEntryState
from homeassistant.core import HomeAssistant
from homeassistant.util import dt as dt_util
from pytest_homeassistant_custom_component.common import (
    MockConfigEntry,
    async_fire_time_changed,
)

from custom_components.tag_historian import async_get_stats
from custom_components.tag_historian.api import (
    TagHistorianAuthError,
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
)

from .conftest import STUB_QUOTA, make_client

CLIENT_PATH = "custom_components.tag_historian.TagHistorianClient"


def make_entry(
    hass: HomeAssistant, entities: list[str], min_interval: int = 10
) -> MockConfigEntry:
    entry = MockConfigEntry(
        domain=DOMAIN,
        data={CONF_HOST: "api.taghistorian.com", CONF_API_KEY: "not-a-real-key"},
        options={CONF_ENTITIES: entities, CONF_MIN_INTERVAL: min_interval},
        unique_id="11111111-2222-3333-4444-555555555555",
    )
    entry.add_to_hass(hass)
    return entry


async def test_setup_creates_sensors_whose_values_come_from_the_api(
    hass: HomeAssistant,
) -> None:
    """Not one of these numbers exists anywhere in this package."""
    entry = make_entry(hass, ["sensor.house_power"])

    with patch(CLIENT_PATH, return_value=make_client()):
        assert await hass.config_entries.async_setup(entry.entry_id)
        await hass.async_block_till_done()

    assert entry.state is ConfigEntryState.LOADED

    tags_used = hass.states.get("sensor.tag_historian_tags_used")
    assert tags_used is not None
    assert tags_used.state == str(STUB_QUOTA.current_tag_count)
    assert tags_used.attributes["tag_limit"] == STUB_QUOTA.tag_limit
    assert tags_used.attributes["tags_available"] == STUB_QUOTA.tags_available

    readings = hass.states.get("sensor.tag_historian_readings_today")
    assert readings.state == str(STUB_QUOTA.measurements_today)
    assert (
        readings.attributes["readings_per_day_limit"]
        == STUB_QUOTA.measurements_per_day_limit
    )

    assert hass.states.get("sensor.tag_historian_readings_dropped").state == "0"

    assert await hass.config_entries.async_unload(entry.entry_id)
    await hass.async_block_till_done()
    assert entry.state is ConfigEntryState.NOT_LOADED


async def test_a_dead_api_defers_setup_rather_than_loading_a_silent_entry(
    hass: HomeAssistant,
) -> None:
    """ConfigEntryNotReady means Home Assistant retries with backoff.

    A loaded entry that forwards nothing looks identical to a working one, so
    failing the setup is the honest outcome.
    """
    entry = make_entry(hass, ["sensor.house_power"])
    client = make_client()
    client.async_get_quota.side_effect = TagHistorianConnectionError("down")

    with patch(CLIENT_PATH, return_value=client):
        assert not await hass.config_entries.async_setup(entry.entry_id)
        await hass.async_block_till_done()

    assert entry.state is ConfigEntryState.SETUP_RETRY


async def test_a_revoked_key_at_setup_asks_for_a_new_one(
    hass: HomeAssistant,
) -> None:
    entry = make_entry(hass, ["sensor.house_power"])
    client = make_client()
    client.async_get_quota.side_effect = TagHistorianAuthError("revoked")

    with patch(CLIENT_PATH, return_value=client):
        assert not await hass.config_entries.async_setup(entry.entry_id)
        await hass.async_block_till_done()

    assert entry.state is ConfigEntryState.SETUP_ERROR
    flows = hass.config_entries.flow.async_progress_by_handler(DOMAIN)
    assert [flow["context"]["source"] for flow in flows] == ["reauth"]


async def test_unloading_flushes_the_buffer_rather_than_dropping_it(
    hass: HomeAssistant,
) -> None:
    """An unload is what an options change and the repair flow both arrive as.

    ``entry.async_on_unload(forwarder.async_stop)`` unsubscribed and cancelled
    the timer, and the buffer went with the object - up to MAX_QUEUE_LENGTH
    readings, no counter anywhere.
    """
    entry = make_entry(hass, ["sensor.house_power"])
    client = make_client()
    client.async_write.return_value = WriteResult(WriteOutcome.OK)

    with patch(CLIENT_PATH, return_value=client):
        assert await hass.config_entries.async_setup(entry.entry_id)
        await hass.async_block_till_done()

        hass.states.async_set("sensor.house_power", "1500")
        await hass.async_block_till_done()
        forwarder = entry.runtime_data.forwarder
        assert forwarder.stats.queued == 1, "still inside the batch window"

        assert await hass.config_entries.async_unload(entry.entry_id)
        await hass.async_block_till_done()

    assert client.async_write.await_count == 1
    assert forwarder.stats.sent == 1
    assert forwarder.stats.dropped_shutdown == 0


async def test_the_delivery_counters_survive_a_reload(hass: HomeAssistant) -> None:
    """Otherwise "readings dropped" resets at the moment somebody looks at it.

    The tag-quota repair flow reloads the entry as its last act, and it is
    reached BY readings being dropped. Counters that restarted from zero there
    erased the evidence as part of the fix.
    """
    entry = make_entry(hass, ["sensor.house_power"])

    with patch(CLIENT_PATH, return_value=make_client()):
        assert await hass.config_entries.async_setup(entry.entry_id)
        await hass.async_block_till_done()

        entry.runtime_data.forwarder.stats.dropped_quota = 41
        entry.runtime_data.forwarder.stats.sent = 900

        await hass.config_entries.async_reload(entry.entry_id)
        await hass.async_block_till_done()

        assert entry.runtime_data.forwarder.stats.dropped_quota == 41
        assert entry.runtime_data.forwarder.stats.sent == 900
        assert hass.states.get("sensor.tag_historian_readings_dropped").state == "41"

        assert await hass.config_entries.async_unload(entry.entry_id)
        await hass.async_block_till_done()


async def test_a_published_state_stops_changing_once_it_is_published(
    hass: HomeAssistant,
) -> None:
    """A Home Assistant state is one consistent moment, or it is nothing.

    ``entities_storing_nothing`` and ``refused_tags`` were the forwarder's own
    lists, handed straight to the state machine. Home Assistant snapshots a
    state and keeps it until the next write, so those two attributes went on
    changing inside a snapshot that everything else in had been frozen: what
    somebody saw was a populated ``entities_storing_nothing`` sitting beside
    ``dropped_not_a_number: 0``, in the same dict, at the same instant. The
    names had arrived after the number was written.
    """
    entry = make_entry(hass, ["sensor.house_mode"])

    with patch(CLIENT_PATH, return_value=make_client()):
        assert await hass.config_entries.async_setup(entry.entry_id)
        await hass.async_block_till_done()

        published = hass.states.get("sensor.tag_historian_readings_dropped")
        assert published.attributes["entities_storing_nothing"] == []
        assert published.attributes["refused_tags"] == []
        assert published.attributes["dropped_not_a_number"] == 0

        # Everything the forwarder does after this moment belongs to the NEXT
        # state, not to the one already on the bus.
        stats = async_get_stats(hass, entry.entry_id)
        stats.dropped_not_numeric += 1
        stats.unstorable_entities.append("sensor.house_mode")
        stats.refused_tags.append("sensor.house_mode")

        assert published.attributes["entities_storing_nothing"] == [], (
            "a published state named an entity its own counter says nothing about"
        )
        assert published.attributes["refused_tags"] == []

        assert await hass.config_entries.async_unload(entry.entry_id)
        await hass.async_block_till_done()


async def test_removing_the_entry_clears_its_counters(hass: HomeAssistant) -> None:
    """A reload is the same account; a removal is not."""
    entry = make_entry(hass, ["sensor.house_power"])

    with patch(CLIENT_PATH, return_value=make_client()):
        assert await hass.config_entries.async_setup(entry.entry_id)
        await hass.async_block_till_done()
        entry.runtime_data.forwarder.stats.dropped_quota = 7

        await hass.config_entries.async_remove(entry.entry_id)
        await hass.async_block_till_done()

    assert hass.data.get(DOMAIN, {}) == {}


# --------------------------------------------------------------------------
# G1 - the batch that had already left the queue
# --------------------------------------------------------------------------

PROBES = [f"sensor.probe_{index:02d}" for index in range(40)]


class SlowWriter:
    """A write endpoint that is still thinking when the entry reloads.

    ``hold`` is real time on purpose. The event this reproduces is a race
    between two coroutines - a request awaiting a socket, and Home Assistant
    tearing the entry down around it - and mocking the clock removes the very
    thing being tested. A fifth of a second is orders of magnitude longer than
    the in-memory platform unload that has to happen in between.
    """

    def __init__(self, hold: float = 0.2) -> None:
        self._hold = hold
        self.on_the_wire = asyncio.Event()
        self.stored: list[str] = []

    async def __call__(self, body: str) -> WriteResult:
        self.on_the_wire.set()
        await asyncio.sleep(self._hold)
        self.stored.extend(body.splitlines())
        return WriteResult(WriteOutcome.OK)


async def _queue_and_open_a_write(hass: HomeAssistant, writer: SlowWriter) -> None:
    """Fill the buffer, let the batch timer fire, wait for the socket to open."""
    for index, entity_id in enumerate(PROBES):
        hass.states.async_set(entity_id, str(index))
    await hass.async_block_till_done()

    async_fire_time_changed(hass, dt_util.utcnow() + timedelta(seconds=2))
    async with asyncio.timeout(10):
        await writer.on_the_wire.wait()


async def test_an_options_change_mid_write_does_not_lose_the_batch(
    hass: HomeAssistant,
) -> None:
    """G1 - forty readings, a request open, and the user presses Save.

    This is the event, not a simulation of it: the readings are queued through
    the real state bus, the real batch timer opens the real flush task, and
    then ``async_update_entry`` fires the real update listener, which reloads
    the entry - the same path the tag-quota repair flow takes when it finishes.

    What used to happen: the flush is a background task tied to the config
    entry, so the unload cancelled it. The batch had already been popped off
    the queue, so there was nothing left in the buffer for the shutdown flush
    to find and nothing anywhere to count. Thirty of forty readings gone,
    ``sent`` zero, every dropped counter zero, and a README that said that
    could not happen.
    """
    entry = make_entry(hass, PROBES, min_interval=0)
    client = make_client()
    writer = SlowWriter()
    client.async_write = writer

    with patch(CLIENT_PATH, return_value=client):
        assert await hass.config_entries.async_setup(entry.entry_id)
        await hass.async_block_till_done()

        await _queue_and_open_a_write(hass, writer)

        # The user changes the minimum interval. Nothing about that is unusual,
        # and it lands while forty readings are on the wire.
        hass.config_entries.async_update_entry(
            entry, options={CONF_ENTITIES: PROBES, CONF_MIN_INTERVAL: 30}
        )
        await hass.async_block_till_done()

        assert entry.state is ConfigEntryState.LOADED, "the reload finished"

        stats = async_get_stats(hass, entry.entry_id)
        assert len(writer.stored) == 40, (
            f"the batch on the wire has to land: {len(writer.stored)} of 40 stored"
        )
        assert stats.sent == 40
        assert stats.dropped_inflight == 0
        assert stats.dropped_shutdown == 0
        assert stats.queued == 0

        assert await hass.config_entries.async_unload(entry.entry_id)
        await hass.async_block_till_done()


async def test_a_batch_that_cannot_be_saved_is_counted_rather_than_vanishing(
    hass: HomeAssistant,
) -> None:
    """The counterfactual, kept as a test: take the wait away and look.

    ``_async_settle_inflight`` is what turns the test above green - it holds
    the unload for up to INFLIGHT_WAIT_SECONDS so a request that is going to
    answer can. Neutralise it and the readings really are gone: Home Assistant
    cancels the entry's background tasks straight after ``async_unload_entry``
    returns, and that cancellation lands inside the await on the socket.

    What must NOT come back is the silence. The batch is counted where it can
    be seen, named as its own kind of loss, because it is the only one that
    can overstate: the endpoint may have taken those readings before the
    socket went, and it does not deduplicate, so re-sending them would risk
    doubling somebody's history instead of restoring it.
    """
    entry = make_entry(hass, PROBES, min_interval=0)
    client = make_client()
    # Long enough that nothing can finish inside the unload, with or without
    # a wait - the point here is what happens when the wait cannot save it.
    writer = SlowWriter(hold=30)
    client.async_write = writer

    async def no_wait_at_all(self) -> None:
        return None

    with patch(CLIENT_PATH, return_value=client), patch(
        "custom_components.tag_historian.forwarder.StateForwarder."
        "_async_settle_inflight",
        no_wait_at_all,
    ):
        assert await hass.config_entries.async_setup(entry.entry_id)
        await hass.async_block_till_done()

        await _queue_and_open_a_write(hass, writer)

        hass.config_entries.async_update_entry(
            entry, options={CONF_ENTITIES: PROBES, CONF_MIN_INTERVAL: 30}
        )
        await hass.async_block_till_done()

        stats = async_get_stats(hass, entry.entry_id)
        assert writer.stored == [], "the request never came back"
        assert stats.sent == 0
        # The whole point. Forty readings left the buffer and did not arrive,
        # and exactly forty are accounted for.
        assert stats.dropped_inflight == 40
        assert stats.dropped_shutdown == 0
        assert (
            hass.states.get("sensor.tag_historian_readings_dropped").state == "40"
        ), "and a person can see it without reading the log"

        assert await hass.config_entries.async_unload(entry.entry_id)
        await hass.async_block_till_done()


# --------------------------------------------------------------------------
# G5 - the same batch, on the shutdown people actually do
# --------------------------------------------------------------------------
#
# An unload is what an options change arrives as. A STOP is what happens every
# time somebody restarts Home Assistant, updates it, or reboots the machine it
# runs on, and it is NOT an unload - config entries are left loaded and only
# the stop sequence runs. The two paths reach the same shutdown code through
# completely different doors, and homeassistant/core.py 2025.4.4 opens them in
# an order that matters:
#
#     # Stage 1 - Run shutdown jobs
#     ...
#         for job in self._shutdown_jobs:
#             task_or_none = self.async_run_hass_job(job.job, *job.args)
#     ...
#     # Cancel all background tasks
#     for task in self._background_tasks:
#         ...
#         task.cancel("Home Assistant is stopping")
#     self._cancel_cancellable_timers()
#     ...
#     self.bus.async_fire_internal(EVENT_HOMEASSISTANT_STOP)
#
# A shutdown job runs while the loop is still willing to await. A listener on
# EVENT_HOMEASSISTANT_STOP runs after every background task has already been
# cancelled - and the flush carrying a batch is a background task. So these
# tests drive ``hass.async_stop()``, the real sequence, rather than firing the
# event on the bus: firing the event alone skips the cancellation and would
# pass against either implementation.


async def test_stopping_home_assistant_mid_write_does_not_lose_the_batch(
    hass: HomeAssistant,
) -> None:
    """G5 - forty readings, a request open, and somebody restarts Home Assistant.

    The same forty readings as G1, on the shutdown that happens far more often
    than an options change. This ran the whole way through with 0.0 seconds of
    grace: the STOP listener woke up on a request that Home Assistant had
    already cancelled, so ten of the forty were stored and thirty were reported
    as dropped - readings the endpoint would have taken.
    """
    entry = make_entry(hass, PROBES, min_interval=0)
    client = make_client()
    writer = SlowWriter()
    client.async_write = writer

    with patch(CLIENT_PATH, return_value=client):
        assert await hass.config_entries.async_setup(entry.entry_id)
        await hass.async_block_till_done()

        await _queue_and_open_a_write(hass, writer)

        # Not hass.bus.async_fire(EVENT_HOMEASSISTANT_STOP). The whole defect
        # lives in what Home Assistant does BEFORE it fires that event.
        await hass.async_stop()

        stats = async_get_stats(hass, entry.entry_id)
        assert len(writer.stored) == 40, (
            f"the batch on the wire has to land: {len(writer.stored)} of 40 stored"
        )
        assert stats.sent == 40
        assert stats.dropped_inflight == 0
        assert stats.dropped_shutdown == 0
        assert stats.queued == 0


async def test_stopping_home_assistant_on_a_write_that_will_not_answer_counts_it(
    hass: HomeAssistant,
) -> None:
    """And when the grace cannot save it, the readings are still accounted for.

    The stop path's half of the invariant. A write that will not answer inside
    the wait is abandoned rather than re-sent - Tag Historian does not
    deduplicate and the endpoint may already have taken the batch - so the
    honest ending is the counter that can overstate, moving by exactly the
    number of readings that left the buffer.
    """
    entry = make_entry(hass, PROBES, min_interval=0)
    client = make_client()
    # Never answers. The wait is shortened rather than removed, so the real
    # settle still runs - the question here is what happens when it expires.
    writer = SlowWriter(hold=30)
    client.async_write = writer

    with patch(CLIENT_PATH, return_value=client), patch(
        "custom_components.tag_historian.forwarder.INFLIGHT_WAIT_SECONDS", 0.1
    ):
        assert await hass.config_entries.async_setup(entry.entry_id)
        await hass.async_block_till_done()

        await _queue_and_open_a_write(hass, writer)

        await hass.async_stop()

        stats = async_get_stats(hass, entry.entry_id)
        assert writer.stored == [], "the request never came back"
        assert stats.sent == 0
        assert stats.dropped_inflight == 40
        assert stats.dropped_shutdown == 0


async def test_changing_the_options_reloads_the_entry(hass: HomeAssistant) -> None:
    """Otherwise the forwarder keeps its old subscription list.

    A selection change that does not re-subscribe is the worst kind of no-op:
    the screen says the new entities are being historised and nothing is.
    """
    entry = make_entry(hass, ["sensor.house_power"])

    with patch(CLIENT_PATH, return_value=make_client()):
        assert await hass.config_entries.async_setup(entry.entry_id)
        await hass.async_block_till_done()

        with patch(
            "custom_components.tag_historian.async_setup_entry", return_value=True
        ) as reloaded:
            hass.config_entries.async_update_entry(
                entry,
                options={
                    CONF_ENTITIES: ["sensor.outdoor_temperature"],
                    CONF_MIN_INTERVAL: 30,
                },
            )
            await hass.async_block_till_done()

    assert reloaded.called
