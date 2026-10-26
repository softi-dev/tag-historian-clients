"""A renamed entity and the history that is not live: held, read back, imported.

Release 2026.11.0 brought three things together. The forwarder keeps what it
could not deliver across a restart and reads gaps back from the recorder; the
history import sends what the recorder holds; and a renamed entity is
followed, its readings going on into the tag its history is in (``send_as``)
until Tag Historian has the new name too. Each was right on its own branch.
Together, a rename met state that the other two had SAVED under the entity's
old id - and the rename's own reload is the moment that state is read back:

* the saved buffer and watermark came back under the old id, so the renamed
  entity had no watermark (its gap was read from when the new forwarder
  started, and what was evicted before was lost) and no held readings (the
  line the history import stops at passed over readings still to be
  delivered, which were then imported AND delivered);
* an import running at the rename carried on reading the old id, which the
  recorder no longer holds anything under - Home Assistant moves an entity's
  recorded history to its new id - and said it had finished;
* the import sent under the entity's new name, a tag of its own beside the
  one the live readings were going to;
* a held reading saved under the old tag was sent there even after the
  rename had found nothing under the old name and sent the entity under its
  new one: the second tag the rename handling exists to prevent;
* and the replaced meter - its successor given its id, which renames.py
  merges - had the recorder hold the OLD meter's readings under that id, so
  the new meter's read-back and import were the old meter's, and the old
  meter's saved readings and import progress were taken for the new one's.

Reviewed together, three more:

* "Start a new tag" on the rename card, or a replaced meter kept apart, left
  the history in the old tag, as the card says - and the import then read it
  from the new id Home Assistant had moved it to, and sent all of it into the
  new tag as well;
* a rename landing while the import read the entity, or while the entry was
  being set up, was left to the reload it causes - but the reads under the
  old id went on until then, came up empty, and the entity was saved as done,
  which that reload carried over to the new id;
* once Tag Historian has the new name the import goes under it, and how far
  it goes is Tag Historian's line for the renamed tag: one that has to take
  in the tag's former names, which only the API can (importer.py).

The recorder here moves an entity's states to its new id on a rename, as Home
Assistant's does (``recorder/entity_registry.py``, the ``states_meta`` row is
renamed) - unless the new id has states of its own, which it then leaves
where they are, as Home Assistant's does too. test_history.py holds both to
the real recorder.
"""

from __future__ import annotations

import asyncio
import inspect
import json
import re
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest
import yaml
from homeassistant.core import Event, HomeAssistant, State, callback
from homeassistant.helpers import entity_registry as er
from homeassistant.helpers import issue_registry as ir
from homeassistant.helpers.storage import Store
from homeassistant.util import dt as dt_util
from pytest_homeassistant_custom_component.common import (
    MockConfigEntry,
    async_fire_time_changed,
)

from custom_components import tag_historian
from custom_components.tag_historian import forwarder as forwarder_module
from custom_components.tag_historian.api import (
    ImportAllowance,
    ImportBatchResult,
    ImportOutcome,
    ImportSession,
    TagHistorianConnectionError,
    WriteOutcome,
    WriteResult,
)
from custom_components.tag_historian.const import (
    CONF_API_KEY,
    CONF_ENTITIES,
    CONF_FOLLOW_RENAMES,
    CONF_HOST,
    CONF_MIN_INTERVAL,
    CONF_OWN_TAG_SINCE,
    CONF_SEND_AS,
    DOMAIN,
    SERVICE_IMPORT_HISTORY,
    STORAGE_KEY_IMPORT,
    STORAGE_VERSION,
)
from custom_components.tag_historian.forwarder import (
    CONNECTION_RETRY_SECONDS,
    StateForwarder,
)
from custom_components.tag_historian.importer import (
    STATUS_FINISHED,
    STATUS_RUNNING,
    HistoryImporter,
)
from custom_components.tag_historian.line_protocol import encode_point
from custom_components.tag_historian.renames import renamed_issue_id
from custom_components.tag_historian.repairs import (
    EntityRenamedRepairFlow,
    async_create_fix_flow,
)

from .conftest import make_client, without_statistics

COMPONENT = Path(tag_historian.__file__).parent
CLIENT_PATHS = (
    "custom_components.tag_historian.TagHistorianClient",
    "custom_components.tag_historian.renames.TagHistorianClient",
)
OLD = "sensor.energy_2"
NEW = "sensor.energy"
T0 = datetime(2026, 10, 17, 8, 0, tzinfo=UTC)


class Writes:
    """The live endpoint: up or down, and what it took."""

    def __init__(self) -> None:
        self.up = True
        self.bodies: list[str] = []
        self.delivered: list[str] = []

    async def write(self, body: str) -> WriteResult:
        self.bodies.append(body)
        if not self.up:
            raise TagHistorianConnectionError("down")
        self.delivered.append(body)
        return WriteResult(WriteOutcome.OK)

    @property
    def delivered_lines(self) -> list[str]:
        return [line for body in self.delivered for line in body.splitlines()]

    def delivered_values(self, object_id: str) -> list[str]:
        """The values delivered under one tag (the entity_id it carries), in order."""
        return [
            line.split(" ")[1].split(",")[0].removeprefix("value=")
            for line in self.delivered_lines
            if f",entity_id={object_id} " in line
        ]


class Recorder:
    """Home Assistant's recorder, as far as the integration reads it - renames included."""

    def __init__(self) -> None:
        self.states: dict[str, list[State]] = {}
        self.reads: list[str] = []

    def record(self, entity_id: str, value: str, when: datetime) -> None:
        self.states.setdefault(entity_id, []).append(
            State(entity_id, value, {}, last_changed=when, last_updated=when)
        )

    @callback
    def renamed(self, event: Event) -> None:
        old, new = event.data["old_entity_id"], event.data["entity_id"]
        if self.states.get(new):
            return  # "the new entity_id is already in use": the history stays put
        self.states[new] = [
            State(new, s.state, s.attributes, s.last_changed, last_updated=s.last_updated)
            for s in self.states.pop(old, [])
        ]

    async def read(self, hass, entity_id, start, end) -> list[State]:
        self.reads.append(entity_id)
        return [s for s in self.states.get(entity_id, []) if start < s.last_updated < end]

    async def last(self, hass, entity_id) -> datetime | None:
        states = self.states.get(entity_id)
        return states[-1].last_updated if states else None


@callback
def _is_a_rename(data: Mapping[str, Any]) -> bool:
    return data.get("action") == "update" and "old_entity_id" in data


@pytest.fixture
def recorder(hass: HomeAssistant, monkeypatch) -> Recorder:
    fake = Recorder()
    monkeypatch.setattr("custom_components.tag_historian.history.recorder_ready", lambda hass: True)
    monkeypatch.setattr("custom_components.tag_historian.history.async_recorded_states", fake.read)
    monkeypatch.setattr("custom_components.tag_historian.history.async_last_recorded", fake.last)
    without_statistics(monkeypatch)
    hass.bus.async_listen(
        er.EVENT_ENTITY_REGISTRY_UPDATED, fake.renamed, event_filter=_is_a_rename
    )
    return fake


@pytest.fixture
def writes() -> Writes:
    return Writes()


@pytest.fixture
def client(writes: Writes):
    stub = make_client(tags={OLD})
    stub.async_write.side_effect = writes.write
    return stub


@pytest.fixture
async def loaded(hass: HomeAssistant, client):
    """Set an entry up with the stub client, and unload it afterwards."""
    entries: list[MockConfigEntry] = []

    async def _load(entry: MockConfigEntry) -> MockConfigEntry:
        assert await hass.config_entries.async_setup(entry.entry_id)
        await hass.async_block_till_done()
        entries.append(entry)
        return entry

    with patch(CLIENT_PATHS[0], return_value=client), patch(CLIENT_PATHS[1], return_value=client):
        yield _load
        for entry in entries:
            if entry.state.recoverable:
                await hass.config_entries.async_unload(entry.entry_id)
        await hass.async_block_till_done()


def make_entry(hass: HomeAssistant, entities: list[str], follow: bool = False) -> MockConfigEntry:
    entry = MockConfigEntry(
        domain=DOMAIN,
        data={CONF_HOST: "api.taghistorian.com", CONF_API_KEY: "not-a-real-key"},
        options={CONF_ENTITIES: entities, CONF_MIN_INTERVAL: 0, CONF_FOLLOW_RENAMES: follow},
        unique_id="11111111-2222-3333-4444-555555555555",
    )
    entry.add_to_hass(hass)
    return entry


def register(hass: HomeAssistant, object_id: str) -> str:
    return (
        er.async_get(hass)
        .async_get_or_create("sensor", "test", f"unique-{object_id}", suggested_object_id=object_id)
        .entity_id
    )


async def settle(hass: HomeAssistant) -> None:
    await hass.async_block_till_done(wait_background_tasks=True)
    await hass.async_block_till_done(wait_background_tasks=True)


async def rename(hass: HomeAssistant, old: str, new: str) -> None:
    """What Settings > Entities does when a person changes the entity id.

    The registry fires the event, and the entity's platform takes its state
    away from the old id (Entity._async_registry_updated) - done here by hand,
    since these entities have no platform, and first: in Home Assistant that
    is long done by the time the reload the rename causes has talked to Tag
    Historian, and here, with nothing to wait for, the reload can run to the
    end inside the registry update.
    """
    hass.states.async_remove(old)
    er.async_get(hass).async_update_entity(old, new_entity_id=new)
    async with asyncio.timeout(30):
        await settle(hass)


async def tick(hass: HomeAssistant, seconds: float) -> None:
    async_fire_time_changed(hass, dt_util.utcnow() + timedelta(seconds=seconds))
    await hass.async_block_till_done()


async def reading(hass: HomeAssistant, recorder: Recorder, entity_id: str, value: str) -> None:
    """A state change: recorded, and handed to the forwarder, whose flush is then due."""
    hass.states.async_set(entity_id, value)
    recorder.record(entity_id, value, dt_util.utcnow())
    await hass.async_block_till_done()
    await tick(hass, 2)


def session(started_at: datetime) -> ImportSession:
    return ImportSession(
        import_id="imp-1",
        started_at=started_at,
        window_start=started_at - timedelta(days=14),
        max_readings_per_batch=5000,
        allowance=ImportAllowance(
            total=1000, used=0, remaining=1000, readings_per_day=10, window_days=14
        ),
    )


def taken(readings: list[dict[str, Any]]) -> ImportBatchResult:
    return ImportBatchResult(ImportOutcome.TAKEN, stored=len(readings), allowance_remaining=500)


# --------------------------------------------------------------------------
# What the forwarder saved, read back after the rename
# --------------------------------------------------------------------------

KEY = "tag_historian.renamed_entry.forwarder"


@pytest.fixture
def entry(hass: HomeAssistant) -> MockConfigEntry:
    entry = MockConfigEntry(
        domain=DOMAIN,
        entry_id="renamed_entry",
        data={CONF_HOST: "api.taghistorian.com", CONF_API_KEY: "not-a-real-key"},
        unique_id="11111111-2222-3333-4444-555555555555",
    )
    entry.add_to_hass(hass)
    return entry


def _down():
    down = make_client()
    down.async_write.side_effect = TagHistorianConnectionError("down")
    return down


async def _saved_before_the_rename(hass: HomeAssistant, entry, hass_storage) -> list[float]:
    """A run of the entity under its old id that stopped with two readings undelivered."""
    register(hass, "energy_2")
    forwarder = StateForwarder(
        hass, entry, _down(), [OLD], 0, store=Store(hass, STORAGE_VERSION, KEY)
    )
    await forwarder.async_restore()
    forwarder._acked[OLD] = T0.timestamp()
    forwarder.async_start()
    stamps = []
    for minutes, value in ((1, "1.5"), (2, "2.5")):
        when = T0 + timedelta(minutes=minutes)
        hass.states.async_set(OLD, value, timestamp=when.timestamp())
        stamps.append(when.timestamp())
    await hass.async_block_till_done()
    await forwarder.async_shutdown()
    assert [item[0] for item in hass_storage[KEY]["data"]["queue"]] == [OLD, OLD]
    return stamps


async def test_a_renamed_entitys_saved_readings_and_watermark_carry_on_under_its_new_id(
    hass: HomeAssistant, entry, hass_storage
) -> None:
    """The reload a rename causes reads back what the run before it saved - under the old id.

    Before: the restored readings and the watermark stayed the old id's. The
    renamed entity had no watermark, so its gap was read from when the new
    forwarder started; and its line for the history import was that moment
    too, later than the two readings still waiting to be delivered.
    """
    held = await _saved_before_the_rename(hass, entry, hass_storage)
    er.async_get(hass).async_update_entity(OLD, new_entity_id=NEW)
    await hass.async_block_till_done()

    forwarder = StateForwarder(
        hass,
        entry,
        make_client(),
        [NEW],
        0,
        store=Store(hass, STORAGE_VERSION, KEY),
        send_as={NEW: OLD},
    )
    await forwarder.async_restore()

    assert [item.entity_id for item in forwarder._queue] == [NEW, NEW]
    assert all(",entity_id=energy_2 " in item.line for item in forwarder._queue), (
        "still bound for the tag its history is in, as its live readings are"
    )
    assert forwarder._acked == {NEW: T0.timestamp()}
    assert forwarder.history_boundary(NEW) == held[0], "where its undelivered readings begin"
    # And saved under the new id, for the next run.
    assert hass_storage[KEY]["data"]["registry_ids"] == {
        NEW: er.async_get(hass).async_get(NEW).id
    }


async def test_a_held_reading_goes_to_the_tag_the_entity_sends_to_by_then(
    hass: HomeAssistant, entry, hass_storage
) -> None:
    """The rename settled while the reading waited: sent under the new name.

    The case is the rename finding nothing under the old name in Tag
    Historian ("no history to carry", or a new tag chosen on the card): the
    entity sends under its own name from then on. A held reading sent under
    the old one would have started a second tag for it.
    """
    await _saved_before_the_rename(hass, entry, hass_storage)
    er.async_get(hass).async_update_entity(OLD, new_entity_id=NEW)
    await hass.async_block_till_done()

    up = make_client()
    up.async_write.return_value = WriteResult(WriteOutcome.OK)
    forwarder = StateForwarder(
        hass, entry, up, [NEW], 0, store=Store(hass, STORAGE_VERSION, KEY), send_as={}
    )
    await forwarder.async_restore()
    forwarder.async_start()
    await tick(hass, 2)
    forwarder.async_stop()

    body = up.async_write.await_args.args[0]
    assert body.splitlines() == [
        encode_point(NEW, 1.5, T0 + timedelta(minutes=1)),
        encode_point(NEW, 2.5, T0 + timedelta(minutes=2)),
    ]


async def test_a_rename_this_setups_options_do_not_know_yet_is_left_to_its_own_reload(
    hass: HomeAssistant, entry, hass_storage
) -> None:
    """A rename that lands while the entry is being set up.

    The registry has the new id; the options this forwarder was built from
    do not, and neither do its tags. Taken up now, the readings would go out
    under the new name - a tag of their own - until the rename's own reload a
    moment later. Left under the old id, they go where they were going, and
    that reload takes them up.
    """
    await _saved_before_the_rename(hass, entry, hass_storage)
    er.async_get(hass).async_update_entity(OLD, new_entity_id=NEW)
    await hass.async_block_till_done()

    stale = StateForwarder(
        hass, entry, _down(), [OLD], 0, store=Store(hass, STORAGE_VERSION, KEY)
    )
    await stale.async_restore()
    assert [item.entity_id for item in stale._queue] == [OLD, OLD]
    assert all(",entity_id=energy_2 " in item.line for item in stale._queue)
    await stale.async_shutdown()

    forwarder = StateForwarder(
        hass,
        entry,
        make_client(),
        [NEW],
        0,
        store=Store(hass, STORAGE_VERSION, KEY),
        send_as={NEW: OLD},
    )
    await forwarder.async_restore()
    assert [item.entity_id for item in forwarder._queue] == [NEW, NEW], (
        "the registry id was kept across the run that could not follow it"
    )
    assert forwarder._acked == {NEW: T0.timestamp()}


# --------------------------------------------------------------------------
# The whole integration: a rename while readings are held or read back
# --------------------------------------------------------------------------


async def test_readings_evicted_before_a_rename_are_read_back_under_the_new_id(
    hass: HomeAssistant, client, loaded, recorder, writes, monkeypatch, freezer
) -> None:
    """An outage evicts a reading; the entity is renamed; Tag Historian comes back.

    The evicted reading is in the recorder - under the new id by now - and
    the watermark it is read back from came through the rename's reload. It
    is sent first, and into the tag the entity's history is in, like the rest.
    """
    monkeypatch.setattr(forwarder_module, "MAX_QUEUE_LENGTH", 2)
    freezer.move_to(T0)
    old = register(hass, "energy_2")
    entry = await loaded(make_entry(hass, [old]))

    await reading(hass, recorder, old, "1")
    writes.up = False
    for value in ("2", "3", "4"):
        freezer.tick(timedelta(seconds=20))
        await reading(hass, recorder, old, value)
    assert entry.runtime_data.forwarder.stats.dropped_overflow == 1
    reads_before_the_rename = len(recorder.reads)

    await rename(hass, old, NEW)
    assert entry.options[CONF_SEND_AS] == {NEW: OLD}

    writes.up = True
    for _ in range(3):
        await tick(hass, CONNECTION_RETRY_SECONDS + 1)

    assert writes.delivered_values("energy_2") == ["1.0", "2.0", "3.0", "4.0"], (
        "every reading once, in order, the evicted one read back first"
    )
    assert writes.delivered_values("energy") == [], "no second tag"
    assert NEW in recorder.reads[reads_before_the_rename:], (
        "read back from where Home Assistant keeps the history now"
    )


async def test_held_readings_follow_a_rename_that_found_nothing_under_the_old_name(
    hass: HomeAssistant, client, loaded, recorder, writes, freezer
) -> None:
    """Nothing ever reached Tag Historian under the old name, so there is no history to carry.

    The entity goes on under its new name - and so do the readings it could
    not deliver before, instead of being the first ever stored under the old
    name: a second tag for one entity, on a plan that may have no room for it.
    """
    client.async_get_tag_index.return_value = {}
    freezer.move_to(T0)
    old = register(hass, "energy_2")
    writes.up = False
    entry = await loaded(make_entry(hass, [old]))

    for value in ("1", "2"):
        freezer.tick(timedelta(seconds=20))
        await reading(hass, recorder, old, value)

    await rename(hass, old, NEW)
    assert entry.options[CONF_SEND_AS] == {}, "settled: the entity sends under its own name"

    writes.up = True
    for _ in range(3):
        await tick(hass, CONNECTION_RETRY_SECONDS + 1)

    assert writes.delivered_values("energy") == ["1.0", "2.0"]
    assert writes.delivered_values("energy_2") == []


# --------------------------------------------------------------------------
# The history import of a renamed entity
# --------------------------------------------------------------------------


async def test_an_import_running_when_its_entity_is_renamed_carries_on_under_the_new_id(
    hass: HomeAssistant, client, loaded, recorder, freezer
) -> None:
    """The rename's reload stops the import; the next setup carries it on.

    Before: it carried on under the old id. The recorder holds nothing under
    that any more, so the rest of the entity's history was never read, and
    the import said it had finished.
    """
    freezer.move_to(T0)
    old = register(hass, "energy_2")
    for days in (5, 4, 3, 2, 1):
        recorder.record(old, str(days), T0 - timedelta(days=days))
    entry = await loaded(make_entry(hass, [old]))

    batches: list[list[dict[str, Any]]] = []
    on_the_wire = asyncio.Event()

    async def import_batch(import_id: str, readings: list[dict[str, Any]]):
        batches.append(readings)
        if len(batches) == 2:
            on_the_wire.set()
            await asyncio.Event().wait()  # until the rename's reload cancels it
        return taken(readings)

    client.async_start_import.return_value = session(T0)
    client.async_import_batch.side_effect = import_batch
    await entry.runtime_data.importer.async_start([old], None)
    async with asyncio.timeout(10):
        await on_the_wire.wait()

    await rename(hass, old, NEW)

    importer = entry.runtime_data.importer
    assert importer.progress.status == STATUS_FINISHED
    assert importer.progress.entities == [NEW]
    assert importer.progress.done == [NEW]
    assert [r["timestamp"] for r in batches[2]] == [
        f"{(T0 - timedelta(days=days)):%Y-%m-%dT%H:%M:%S}Z" for days in (4, 3, 2, 1)
    ], "everything after the last batch Tag Historian answered, read under the new id"
    assert {r["tagName"] for batch in batches for r in batch} == {OLD}, (
        "into the tag the entity's live readings go to while the rename waits"
    )


async def test_the_import_of_a_renamed_entity_stops_where_its_held_readings_begin(
    hass: HomeAssistant, client, loaded, recorder, writes, freezer
) -> None:
    """Renamed first, imported after, with live readings still waiting to be delivered.

    Tag Historian has no live reading of the tag yet, so the only line between
    history and live data is the forwarder's (history_boundary). Before, for
    the renamed entity it was the moment the rename's reload started
    listening - after the two held readings, which the import then sent and
    the forwarder would have delivered a second time.
    """
    freezer.move_to(T0)
    old = register(hass, "energy_2")
    for days in (3, 2, 1):
        recorder.record(old, str(days), T0 - timedelta(days=days))
    writes.up = False
    entry = await loaded(make_entry(hass, [old]))
    held = []
    for value in ("7", "8"):
        freezer.tick(timedelta(minutes=1))
        held.append(dt_util.utcnow().timestamp())
        await reading(hass, recorder, old, value)

    await rename(hass, old, NEW)
    freezer.tick(timedelta(minutes=1))
    assert entry.runtime_data.forwarder.history_boundary(NEW) == held[0]

    imported: list[dict[str, Any]] = []

    async def import_batch(import_id: str, readings: list[dict[str, Any]]):
        imported.extend(readings)
        return taken(readings)

    client.async_start_import.return_value = session(dt_util.utcnow())
    client.async_import_batch.side_effect = import_batch
    await hass.services.async_call(DOMAIN, SERVICE_IMPORT_HISTORY, {}, blocking=True)
    await settle(hass)

    assert [r["value"] for r in imported] == [3.0, 2.0, 1.0], "the history, and not what is held"
    assert {r["tagName"] for r in imported} == {OLD}
    assert entry.runtime_data.importer.progress.entities == [NEW]


# --------------------------------------------------------------------------
# A replaced meter: renamed onto an id the recorder has a history for already
# --------------------------------------------------------------------------


async def _replace_the_meter(hass: HomeAssistant, recorder: Recorder) -> None:
    """Remove the old meter, give the new one its id - the swap renames.py merges.

    Home Assistant cannot move the new meter's recorded history to an id that
    has one of its own, so both stay where they are.
    """
    hass.states.async_remove(NEW)
    er.async_get(hass).async_remove(NEW)
    await hass.async_block_till_done()
    await rename(hass, OLD, NEW)
    assert recorder.states[OLD], "the new meter's history, still under its old id"


async def test_a_replaced_meter_imports_its_own_history_and_not_the_old_meters(
    hass: HomeAssistant, client, loaded, recorder, freezer
) -> None:
    """While the merge waits on its card, the import reads the new meter's own history.

    Under its new id the recorder holds the OLD meter's readings. Imported
    from there, they would have gone into the new meter's tag - and, once the
    card's merge was chosen, into the one history twice: the old meter's tag
    has them already.
    """
    client.async_get_tag_index.return_value = {NEW: NEW, OLD: OLD}
    freezer.move_to(T0)
    register(hass, "energy")
    register(hass, "energy_2")
    for days in (9, 8, 7):
        recorder.record(NEW, f"10{days}", T0 - timedelta(days=days))  # the old meter
    for days in (3, 2, 1):
        recorder.record(OLD, str(days), T0 - timedelta(days=days))  # the new one
    entry = await loaded(make_entry(hass, [OLD]))

    await _replace_the_meter(hass, recorder)
    assert entry.options[CONF_SEND_AS] == {NEW: OLD}, "the merge waits for the card"

    imported: list[dict[str, Any]] = []

    async def import_batch(import_id: str, readings: list[dict[str, Any]]):
        imported.extend(readings)
        return taken(readings)

    client.async_start_import.return_value = session(dt_util.utcnow())
    client.async_import_batch.side_effect = import_batch
    await hass.services.async_call(DOMAIN, SERVICE_IMPORT_HISTORY, {}, blocking=True)
    await settle(hass)

    assert [r["value"] for r in imported] == [3.0, 2.0, 1.0]
    assert {r["tagName"] for r in imported} == {OLD}


async def test_an_import_of_both_meters_carries_on_with_the_new_one_after_the_swap(
    hass: HomeAssistant, client, loaded, recorder, freezer
) -> None:
    """Both meters were being imported when the one was swapped for the other.

    The old meter's progress was saved under the id the new one has now. Taken
    for the new meter's, the old meter being done made the new one done too,
    and the rest of its history was never imported.
    """
    client.async_get_tag_index.return_value = {NEW: NEW, OLD: OLD}
    freezer.move_to(T0)
    register(hass, "energy")
    register(hass, "energy_2")
    for days in (9, 8, 7):
        recorder.record(NEW, f"10{days}", T0 - timedelta(days=days))  # the old meter
    for days in (3, 2, 1):
        recorder.record(OLD, str(days), T0 - timedelta(days=days))  # the new one
    entry = await loaded(make_entry(hass, [NEW, OLD]))

    batches: list[list[dict[str, Any]]] = []
    on_the_wire = asyncio.Event()

    async def import_batch(import_id: str, readings: list[dict[str, Any]]):
        batches.append(readings)
        if len(batches) == 3:  # the new meter's first
            on_the_wire.set()
            await asyncio.Event().wait()
        return taken(readings)

    client.async_start_import.return_value = session(T0)
    client.async_import_batch.side_effect = import_batch
    await entry.runtime_data.importer.async_start([NEW, OLD], None)
    async with asyncio.timeout(10):
        await on_the_wire.wait()

    await _replace_the_meter(hass, recorder)

    importer = entry.runtime_data.importer
    assert importer.progress.status == STATUS_FINISHED
    assert importer.progress.entities == [NEW]
    after_the_swap = [r for batch in batches[3:] for r in batch]
    assert [r["value"] for r in after_the_swap] == [3.0, 2.0, 1.0]
    assert {r["tagName"] for r in after_the_swap} == {OLD}


async def test_a_gap_across_a_meter_swap_is_read_back_from_the_new_meters_own_history(
    hass: HomeAssistant, client, loaded, recorder, writes, monkeypatch, freezer
) -> None:
    """An outage spans the swap: the evicted reading is under the old id, the old meter's under the new.

    Read back from the new id alone, the gap held the old meter's last
    reading - sent as the new meter's - and not the new meter's own.
    """
    monkeypatch.setattr(forwarder_module, "MAX_QUEUE_LENGTH", 2)
    client.async_get_tag_index.return_value = {NEW: NEW, OLD: OLD}
    freezer.move_to(T0)
    register(hass, "energy")
    register(hass, "energy_2")
    entry = await loaded(make_entry(hass, [OLD]))

    await reading(hass, recorder, OLD, "1")
    writes.up = False
    freezer.tick(timedelta(seconds=20))
    await reading(hass, recorder, OLD, "2")
    freezer.tick(timedelta(seconds=5))
    recorder.record(NEW, "999", dt_util.utcnow())  # the old meter, on its way out
    for value in ("3", "4"):
        freezer.tick(timedelta(seconds=20))
        await reading(hass, recorder, OLD, value)
    assert entry.runtime_data.forwarder.stats.dropped_overflow == 1

    await _replace_the_meter(hass, recorder)
    writes.up = True
    for _ in range(3):
        await tick(hass, CONNECTION_RETRY_SECONDS + 1)

    assert writes.delivered_values("energy_2") == ["1.0", "2.0", "3.0", "4.0"]
    assert "999" not in "".join(writes.delivered_lines), "nothing of the old meter's"


async def test_both_meters_held_readings_go_to_their_own_meters_tags(
    hass: HomeAssistant, client, loaded, recorder, writes, freezer
) -> None:
    """Both meters selected, both holding readings, when the one is swapped for the other.

    The old meter's were saved under the id the new one has now. Taken for
    the new meter's, they went to its tag - another meter's readings in it -
    and the new meter's line for the history import began at them.
    """
    client.async_get_tag_index.return_value = {NEW: NEW, OLD: OLD}
    freezer.move_to(T0)
    register(hass, "energy")
    register(hass, "energy_2")
    writes.up = False
    entry = await loaded(make_entry(hass, [NEW, OLD]))
    freezer.tick(timedelta(seconds=20))
    await reading(hass, recorder, NEW, "100")  # the old meter's last
    freezer.tick(timedelta(seconds=20))
    new_meters = dt_util.utcnow().timestamp()
    await reading(hass, recorder, OLD, "1")

    await _replace_the_meter(hass, recorder)
    assert entry.options[CONF_ENTITIES] == [NEW]
    assert entry.runtime_data.forwarder.history_boundary(NEW) == new_meters

    writes.up = True
    for _ in range(3):
        await tick(hass, CONNECTION_RETRY_SECONDS + 1)

    assert writes.delivered_values("energy") == ["100.0"], "into the old meter's own tag"
    assert writes.delivered_values("energy_2") == ["1.0"], "and the new meter's into its"


# --------------------------------------------------------------------------
# What the card decided, and a rename while the import reads
# --------------------------------------------------------------------------


async def _import_everything(hass: HomeAssistant, client) -> list[dict[str, Any]]:
    """The action with its defaults - every selected entity - run to the end."""
    imported: list[dict[str, Any]] = []

    async def import_batch(import_id: str, readings: list[dict[str, Any]]):
        imported.extend(readings)
        return taken(readings)

    client.async_start_import.return_value = session(dt_util.utcnow())
    client.async_import_batch.side_effect = import_batch
    await hass.services.async_call(DOMAIN, SERVICE_IMPORT_HISTORY, {}, blocking=True)
    await settle(hass)
    return imported


@pytest.mark.parametrize(
    ("tags", "choice"),
    [
        ({OLD: OLD}, "new_tag"),
        # A replaced meter whose tag outlived its recorded history, so Home
        # Assistant moved the new meter's history to the id: the same thing.
        ({OLD: OLD, NEW: NEW}, "keep_apart"),
    ],
)
async def test_a_new_tag_chosen_on_the_card_leaves_the_history_out_of_the_import(
    hass: HomeAssistant, client, loaded, recorder, freezer, tags, choice
) -> None:
    """The card: start a new tag "and leave the history where it is, under {old_tag}".

    Home Assistant had moved that history to the new id. Before, the import
    read it there and sent all of it into the new tag - with no live reading
    of its own yet, the new tag took every one - so the history was in both
    tags, and the import allowance paid for the copy.
    """
    client.async_get_tag_index.return_value = tags
    freezer.move_to(T0)
    old = register(hass, "energy_2")
    for days in (5, 4, 3, 2, 1):
        recorder.record(old, str(days), T0 - timedelta(days=days))
    entry = await loaded(make_entry(hass, [old]))
    await rename(hass, old, NEW)
    assert recorder.states[NEW], "the history went with the entity"

    issue = ir.async_get(hass).async_get_issue(DOMAIN, renamed_issue_id(entry, NEW))
    flow = await async_create_fix_flow(hass, issue.issue_id, issue.data)
    flow.hass = hass
    await getattr(flow, f"async_step_{choice}")()
    await settle(hass)
    assert entry.options[CONF_SEND_AS] == {}
    assert NEW in entry.options[CONF_OWN_TAG_SINCE]

    assert await _import_everything(hass, client) == [], "the old tag's history stays the old tag's"
    progress = entry.runtime_data.importer.progress
    assert progress.status == STATUS_FINISHED
    assert progress.done == [NEW]


async def test_a_rename_with_nothing_under_the_old_name_still_imports_the_history(
    hass: HomeAssistant, client, loaded, recorder, freezer
) -> None:
    """No tag holds the history, so the new one is where it goes - no card, no mark."""
    client.async_get_tag_index.return_value = {}
    freezer.move_to(T0)
    old = register(hass, "energy_2")
    for days in (3, 2, 1):
        recorder.record(old, str(days), T0 - timedelta(days=days))
    entry = await loaded(make_entry(hass, [old]))
    await rename(hass, old, NEW)
    assert entry.options[CONF_SEND_AS] == {}
    assert CONF_OWN_TAG_SINCE not in entry.options

    imported = await _import_everything(hass, client)
    assert [r["value"] for r in imported] == [3.0, 2.0, 1.0]
    assert {r["tagName"] for r in imported} == {NEW}


async def test_a_tag_of_its_own_is_still_its_own_after_the_next_rename(
    hass: HomeAssistant, client, loaded, recorder, freezer
) -> None:
    """The choice is about the history, not the id: renamed again, the import still leaves it."""
    freezer.move_to(T0)
    old = register(hass, "energy_2")
    for days in (3, 2, 1):
        recorder.record(old, str(days), T0 - timedelta(days=days))
    entry = await loaded(make_entry(hass, [old]))
    await rename(hass, old, NEW)
    issue = ir.async_get(hass).async_get_issue(DOMAIN, renamed_issue_id(entry, NEW))
    flow = await async_create_fix_flow(hass, issue.issue_id, issue.data)
    flow.hass = hass
    await flow.async_step_new_tag()
    await settle(hass)

    await rename(hass, NEW, "sensor.energy_3")

    assert set(entry.options[CONF_OWN_TAG_SINCE]) == {"sensor.energy_3"}
    assert await _import_everything(hass, client) == []


@pytest.fixture
def import_store(hass: HomeAssistant, entry) -> Store:
    return Store(hass, STORAGE_VERSION, STORAGE_KEY_IMPORT.format(entry_id=entry.entry_id))


def _renamed_in_home_assistant(hass: HomeAssistant, entry) -> None:
    """The rename, as far as the importer sees it: the registry, the recorder - which
    moves the history on the registry's event - and the options, which
    renames.async_follow_rename changes before anything else runs, and which
    queue the reload that will carry the import on."""
    er.async_get(hass).async_update_entity(OLD, new_entity_id=NEW)
    hass.config_entries.async_update_entry(
        entry, options={**entry.options, CONF_ENTITIES: [NEW], CONF_SEND_AS: {NEW: OLD}}
    )


async def _the_reload(hass: HomeAssistant, entry, client, import_store) -> HistoryImporter:
    """The next setup's importer, from the options the rename left: it carries the import on."""
    importer = HistoryImporter(
        hass, entry, client, import_store, 0, None, send_as={NEW: OLD}, selected=[NEW]
    )
    await importer.async_load()
    assert importer._task is not None, "still running, so the reload carries it on"
    await importer._task
    return importer


async def test_a_rename_while_the_import_reads_the_entity_does_not_make_it_done(
    hass: HomeAssistant, entry, recorder, freezer, import_store
) -> None:
    """Renamed between two batches, before the rename's reload has stopped the import.

    The reads under the old id came up empty from then on - Home Assistant
    had moved the history - and the entity was saved as done. The reload
    carried "done" over to the new id: the rest of the history was never
    imported, and the import said it had finished.
    """
    freezer.move_to(T0)
    register(hass, "energy_2")
    for days in (5, 4, 3, 2, 1):
        recorder.record(OLD, str(days), T0 - timedelta(days=days))
    hass.config_entries.async_update_entry(entry, options={CONF_ENTITIES: [OLD]})

    batches: list[list[dict[str, Any]]] = []

    async def import_batch(import_id: str, readings: list[dict[str, Any]]):
        batches.append(readings)
        if len(batches) == 1:
            _renamed_in_home_assistant(hass, entry)
        return taken(readings)

    client = make_client()
    client.async_start_import.return_value = session(T0)
    client.async_import_batch.side_effect = import_batch
    importer = HistoryImporter(
        hass, entry, client, import_store, 0, None, send_as={}, selected=[OLD]
    )
    await importer.async_start([OLD], None)
    await importer._task

    assert importer.progress.status == STATUS_RUNNING
    assert importer.progress.done == []

    resumed = await _the_reload(hass, entry, client, import_store)
    assert resumed.progress.status == STATUS_FINISHED
    assert resumed.progress.done == [NEW]
    assert [r["value"] for batch in batches for r in batch] == [5.0, 4.0, 3.0, 2.0, 1.0]
    assert {r["tagName"] for batch in batches for r in batch} == {OLD}


async def test_a_rename_while_the_entry_is_set_up_does_not_make_a_running_import_done(
    hass: HomeAssistant, entry, recorder, freezer, import_store
) -> None:
    """The rename lands after the setup read its options and before the import was loaded.

    The import is carried on under the old id - renamed_since leaves a rename
    the options it was built from do not know to that rename's own reload -
    and the recorder holds nothing there any more. Before, that was done.
    """
    freezer.move_to(T0)
    registry_id = er.async_get(hass).async_get(register(hass, "energy_2")).id
    for days in (5, 4, 3, 2, 1):
        recorder.record(OLD, str(days), T0 - timedelta(days=days))
    hass.config_entries.async_update_entry(entry, options={CONF_ENTITIES: [OLD]})
    await import_store.async_save(
        {
            "status": STATUS_RUNNING,
            "import_id": "imp-1",
            "started_at": T0.isoformat(),
            "window_start": (T0 - timedelta(days=14)).isoformat(),
            "since": (T0 - timedelta(days=14)).isoformat(),
            "entities": [OLD],
            "cursor": {OLD: (T0 - timedelta(days=5)).timestamp()},
            "registry_ids": {OLD: registry_id},
        }
    )

    client = make_client()
    client.async_import_batch.side_effect = lambda import_id, readings: taken(readings)
    setting_up = HistoryImporter(
        hass, entry, client, import_store, 0, None, send_as={}, selected=[OLD]
    )
    _renamed_in_home_assistant(hass, entry)
    await setting_up.async_load()
    await setting_up._task
    assert setting_up.progress.done == []
    assert setting_up.progress.status == STATUS_RUNNING

    resumed = await _the_reload(hass, entry, client, import_store)
    assert resumed.progress.done == [NEW]
    sent = [r for call in client.async_import_batch.await_args_list for r in call.args[1]]
    assert [r["value"] for r in sent] == [4.0, 3.0, 2.0, 1.0], "the rest of it, after the cursor"


async def test_once_the_tag_has_the_new_name_the_import_stops_at_tag_historians_line_for_it(
    hass: HomeAssistant, client, loaded, recorder, freezer
) -> None:
    """Renamed, the tag renamed with it (follow_renames), imported after.

    The import goes under the new name, and only Tag Historian knows where
    the renamed tag's live data began: under its old name, before the
    rename. This holds the client to stopping wherever the answer for the
    name it sent says - which is where the API has to take in the tag's
    former names, or the history the old name holds already is taken again.
    """
    freezer.move_to(T0)
    old = register(hass, "energy_2")
    for days in (5, 4, 3, 2, 1):
        recorder.record(old, str(days), T0 - timedelta(days=days))
    entry = await loaded(make_entry(hass, [old], follow=True))
    await rename(hass, old, NEW)
    client.async_rename_tag.assert_awaited_once_with(OLD, NEW)
    assert entry.options[CONF_SEND_AS] == {}

    went_live_under_the_old_name = T0 - timedelta(days=3, hours=12)
    imported: list[dict[str, Any]] = []

    async def import_batch(import_id: str, readings: list[dict[str, Any]]):
        imported.extend(readings)
        return ImportBatchResult(
            ImportOutcome.TAKEN,
            stored=len(readings),
            importable_before={NEW: went_live_under_the_old_name},
        )

    client.async_start_import.return_value = session(dt_util.utcnow())
    client.async_import_batch.side_effect = import_batch
    await hass.services.async_call(DOMAIN, SERVICE_IMPORT_HISTORY, {}, blocking=True)
    await settle(hass)

    assert [r["value"] for r in imported] == [5.0, 4.0]
    assert {r["tagName"] for r in imported} == {NEW}


# --------------------------------------------------------------------------
# Both features' strings, in the one en.json the merge put together
# --------------------------------------------------------------------------


def test_the_import_and_the_rename_strings_are_where_the_code_looks() -> None:
    """What hassfest and the screens read, for the import and the rename side by side.

    The two branches each changed the same en.json; this holds the merged
    file to what the code of both asks of it.
    """
    strings = json.loads((COMPONENT / "translations" / "en.json").read_text(encoding="utf-8"))
    services = yaml.safe_load((COMPONENT / "services.yaml").read_text(encoding="utf-8"))

    # The action: services.yaml says what it takes, the translations what it is called.
    assert set(services) == set(strings["services"]) == {SERVICE_IMPORT_HISTORY}
    for action, spec in services.items():
        described = strings["services"][action]
        assert described["name"] and described["description"]
        assert set(spec["fields"]) == set(described["fields"])

    # Configure: both things on the menu, and the rename option still on the selection.
    assert set(strings["options"]["step"]["init"]["menu_options"]) == {"choose", "import_history"}
    for section in ("config", "options"):
        select = strings[section]["step"]["select"]
        assert CONF_FOLLOW_RENAMES in select["data"]
        assert CONF_FOLLOW_RENAMES in select["data_description"]

    # The import screen aborts with the action's own refusals, under the same keys.
    refusals = set(
        re.findall(r'translation_key="(\w+)"', inspect.getsource(tag_historian.async_start_import))
    )
    assert refusals, "the pattern still finds them"
    assert refusals <= set(strings["exceptions"])
    assert refusals <= set(strings["options"]["abort"])

    # The rename card: every choice it offers and every way it can end.
    card = strings["issues"]["entity_renamed"]["fix_flow"]
    source = inspect.getsource(EntityRenamedRepairFlow)
    for step, options in re.findall(r'step_id="(\w+)",\s*menu_options=\[([^\]]*)\]', source):
        assert set(re.findall(r'"(\w+)"', options)) == set(card["step"][step]["menu_options"])
    assert set(re.findall(r'reason="(\w+)"', source)) == set(card["abort"])

    # The import screen names the two answers that leave an entity out, as the card words them.
    import_screen = strings["options"]["step"]["import_history"]["description"]
    for step, choice in (("rename_choice", "new_tag"), ("merge_choice", "keep_apart")):
        assert f'"{card["step"][step]["menu_options"][choice]}"' in import_screen
