"""An entity renamed in Home Assistant: followed, and its history carried over.

THE DEFECT. The forwarder subscribes to exactly the selected entity ids, and
the tag is named after the id. Renaming an entity therefore ended its readings
- the subscription was to an id that no longer existed - without a word: no
error, no counter, a chart that simply stopped. And selecting it again under
the new id started a second tag beside the first.

These drive the real entity registry: ``async_update_entity`` with a new
entity id fires exactly the event Home Assistant fires when a person renames
an entity in Settings.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
from unittest.mock import patch

import pytest
from homeassistant.config_entries import ConfigEntryState
from homeassistant.core import HomeAssistant
from homeassistant.data_entry_flow import FlowResultType
from homeassistant.helpers import entity_registry as er
from homeassistant.helpers import issue_registry as ir
from homeassistant.util import dt as dt_util
from pytest_homeassistant_custom_component.common import (
    MockConfigEntry,
    async_fire_time_changed,
)

from custom_components.tag_historian.api import (
    LineageOutcome,
    LineageResult,
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
    CONF_SEND_AS,
    DOMAIN,
    ISSUE_ENTITY_RENAMED,
    RENAME_RETRY_SECONDS,
)
from custom_components.tag_historian.renames import async_carry_history, renamed_issue_id
from custom_components.tag_historian.repairs import async_create_fix_flow

from .conftest import make_client

CLIENT_PATHS = (
    "custom_components.tag_historian.TagHistorianClient",
    "custom_components.tag_historian.renames.TagHistorianClient",
)


def make_entry(hass: HomeAssistant, entities: list[str], follow: bool = False) -> MockConfigEntry:
    entry = MockConfigEntry(
        domain=DOMAIN,
        data={CONF_HOST: "api.taghistorian.com", CONF_API_KEY: "not-a-real-key"},
        options={
            CONF_ENTITIES: entities,
            CONF_MIN_INTERVAL: 0,
            CONF_FOLLOW_RENAMES: follow,
        },
        unique_id="11111111-2222-3333-4444-555555555555",
    )
    entry.add_to_hass(hass)
    return entry


def register(hass: HomeAssistant, object_id: str) -> str:
    """An entity in the registry, as an integration would have created it."""
    return (
        er.async_get(hass)
        .async_get_or_create("sensor", "test", f"unique-{object_id}", suggested_object_id=object_id)
        .entity_id
    )


async def settle(hass: HomeAssistant) -> None:
    """Let the reloads and the requests a rename sets off run to the end."""
    await hass.async_block_till_done(wait_background_tasks=True)
    await hass.async_block_till_done(wait_background_tasks=True)


async def rename(hass: HomeAssistant, old: str, new: str) -> None:
    """What Settings > Entities does when a person changes the entity id."""
    er.async_get(hass).async_update_entity(old, new_entity_id=new)
    await settle(hass)


async def send_reading(hass: HomeAssistant, entity_id: str, value: str) -> None:
    """A state change, and the flush that sends it."""
    hass.states.async_set(entity_id, value)
    await hass.async_block_till_done()
    async_fire_time_changed(hass, dt_util.utcnow() + timedelta(seconds=2))
    await hass.async_block_till_done()


def sent(client) -> str:
    """Every line protocol body written so far."""
    return "\n".join(call.args[0] for call in client.async_write.await_args_list)


def card(hass: HomeAssistant, entry: MockConfigEntry, entity_id: str) -> ir.IssueEntry | None:
    return ir.async_get(hass).async_get_issue(DOMAIN, renamed_issue_id(entry, entity_id))


@pytest.fixture
def client():
    stub = make_client(tags={"sensor.energy_2"})
    stub.async_write.return_value = WriteResult(WriteOutcome.OK)
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


async def test_a_renamed_entity_is_followed_and_its_readings_go_on_into_its_history(
    hass: HomeAssistant, client, loaded
) -> None:
    """The selection follows the new id; the readings stay in the old tag until asked.

    Before: the subscription was to "sensor.energy_2", which no longer
    existed, and nothing more was ever sent for the entity.
    """
    old = register(hass, "energy_2")
    entry = await loaded(make_entry(hass, [old]))

    await rename(hass, old, "sensor.energy")

    assert entry.options[CONF_ENTITIES] == ["sensor.energy"]
    assert entry.options[CONF_SEND_AS] == {"sensor.energy": "sensor.energy_2"}

    hass.states.async_set("sensor.energy", "1234.5")
    await hass.async_block_till_done()
    async_fire_time_changed(hass, dt_util.utcnow() + timedelta(seconds=2))
    await hass.async_block_till_done()

    lines = [call.args[0] for call in client.async_write.await_args_list]
    assert any("entity_id=energy_2 value=1234.5" in body for body in lines), (
        "the renamed entity's readings go on into the tag its history is in"
    )

    issue = ir.async_get(hass).async_get_issue(DOMAIN, renamed_issue_id(entry, "sensor.energy"))
    assert issue is not None and issue.is_fixable
    assert issue.translation_key == ISSUE_ENTITY_RENAMED
    client.async_rename_tag.assert_not_awaited()


async def test_with_follow_renames_the_tag_is_renamed_and_the_readings_follow(
    hass: HomeAssistant, client, loaded
) -> None:
    old = register(hass, "energy_2")
    entry = await loaded(make_entry(hass, [old], follow=True))

    await rename(hass, old, "sensor.energy")

    client.async_rename_tag.assert_awaited_once_with("sensor.energy_2", "sensor.energy")
    assert entry.options[CONF_ENTITIES] == ["sensor.energy"]
    assert entry.options[CONF_SEND_AS] == {}, "renamed: the readings go under the new name"
    assert ir.async_get(hass).async_get_issue(DOMAIN, renamed_issue_id(entry, "sensor.energy")) is None


async def test_a_name_that_is_a_tag_already_is_merged_into(
    hass: HomeAssistant, client, loaded
) -> None:
    client.async_rename_tag.return_value = LineageResult(
        LineageOutcome.TARGET_EXISTS, "A tag named 'sensor.energy' already exists."
    )
    old = register(hass, "energy_2")
    entry = await loaded(make_entry(hass, [old], follow=True))

    await rename(hass, old, "sensor.energy")

    client.async_merge_tags.assert_awaited_once_with("sensor.energy_2", "sensor.energy")
    assert entry.options[CONF_SEND_AS] == {}


async def test_a_replaced_meter_renamed_to_the_old_ones_id_merges_the_two_histories(
    hass: HomeAssistant, client, loaded
) -> None:
    """The way Home Assistant users swap a meter: remove the old entity, then
    give the new one the old id. Both were selected, both have a tag - the new
    meter's few days merge into the long history under the old name."""
    client.async_get_tag_index.return_value = {
        "sensor.energy": "sensor.energy", "sensor.energy_2": "sensor.energy_2"
    }
    old_meter = register(hass, "energy")
    new_meter = register(hass, "energy_2")
    entry = await loaded(make_entry(hass, [old_meter, new_meter], follow=True))

    er.async_get(hass).async_remove(old_meter)
    await hass.async_block_till_done()
    await rename(hass, new_meter, "sensor.energy")

    client.async_merge_tags.assert_awaited_once_with("sensor.energy_2", "sensor.energy")
    client.async_rename_tag.assert_not_awaited()
    assert entry.options[CONF_ENTITIES] == ["sensor.energy"]
    assert entry.options[CONF_SEND_AS] == {}


async def test_an_entity_that_never_stored_anything_simply_follows(
    hass: HomeAssistant, client, loaded
) -> None:
    client.async_get_tag_index.return_value = {}
    old = register(hass, "energy_2")
    entry = await loaded(make_entry(hass, [old]))

    await rename(hass, old, "sensor.energy")

    assert entry.options[CONF_ENTITIES] == ["sensor.energy"]
    assert entry.options[CONF_SEND_AS] == {}, "there is no history to keep sending into"
    assert ir.async_get(hass).async_get_issue(DOMAIN, renamed_issue_id(entry, "sensor.energy")) is None


async def test_when_tag_historian_cannot_be_reached_the_readings_wait_in_the_old_tag(
    hass: HomeAssistant, client, loaded
) -> None:
    client.async_rename_tag.return_value = LineageResult(LineageOutcome.UNAVAILABLE, "timeout")
    old = register(hass, "energy_2")
    entry = await loaded(make_entry(hass, [old], follow=True))

    await rename(hass, old, "sensor.energy")

    assert entry.options[CONF_SEND_AS] == {"sensor.energy": "sensor.energy_2"}
    assert ir.async_get(hass).async_get_issue(DOMAIN, renamed_issue_id(entry, "sensor.energy")) is None

    client.async_rename_tag.return_value = LineageResult(LineageOutcome.DONE)
    async_fire_time_changed(hass, dt_util.utcnow() + timedelta(seconds=RENAME_RETRY_SECONDS + 1))
    await hass.async_block_till_done(wait_background_tasks=True)
    await hass.async_block_till_done(wait_background_tasks=True)

    assert entry.options[CONF_SEND_AS] == {}, "asked again later, and done"


async def test_a_refused_rename_says_why_on_the_card(
    hass: HomeAssistant, client, loaded
) -> None:
    client.async_rename_tag.return_value = LineageResult(
        LineageOutcome.REFUSED, "'sensor.energy' is a former name of 'sensor.meter'."
    )
    old = register(hass, "energy_2")
    entry = await loaded(make_entry(hass, [old], follow=True))

    await rename(hass, old, "sensor.energy")

    issue = ir.async_get(hass).async_get_issue(DOMAIN, renamed_issue_id(entry, "sensor.energy"))
    assert issue is not None
    assert "former name of 'sensor.meter'" in issue.translation_placeholders["refusal"]
    assert entry.options[CONF_SEND_AS] == {"sensor.energy": "sensor.energy_2"}, (
        "nothing changes until the user decides, and nothing is lost meanwhile"
    )


async def test_the_card_renames_the_tag_when_asked(
    hass: HomeAssistant, client, loaded
) -> None:
    old = register(hass, "energy_2")
    entry = await loaded(make_entry(hass, [old]))
    await rename(hass, old, "sensor.energy")
    issue = ir.async_get(hass).async_get_issue(DOMAIN, renamed_issue_id(entry, "sensor.energy"))

    flow = await async_create_fix_flow(hass, issue.issue_id, issue.data)
    flow.hass = hass
    menu = await flow.async_step_init()
    assert menu["type"] is FlowResultType.MENU
    assert menu["menu_options"] == ["rename_tag", "new_tag"]

    result = await flow.async_step_rename_tag()
    await hass.async_block_till_done(wait_background_tasks=True)

    assert result["type"] is FlowResultType.CREATE_ENTRY
    client.async_rename_tag.assert_awaited_once_with("sensor.energy_2", "sensor.energy")
    assert entry.options[CONF_SEND_AS] == {}


async def test_the_card_can_start_a_new_tag_instead(
    hass: HomeAssistant, client, loaded
) -> None:
    old = register(hass, "energy_2")
    entry = await loaded(make_entry(hass, [old]))
    await rename(hass, old, "sensor.energy")
    issue = ir.async_get(hass).async_get_issue(DOMAIN, renamed_issue_id(entry, "sensor.energy"))

    flow = await async_create_fix_flow(hass, issue.issue_id, issue.data)
    flow.hass = hass
    result = await flow.async_step_new_tag()
    await hass.async_block_till_done(wait_background_tasks=True)

    assert result["type"] is FlowResultType.CREATE_ENTRY
    client.async_rename_tag.assert_not_awaited()
    assert entry.options[CONF_SEND_AS] == {}, "the readings go under the new name, as a new tag"


async def test_a_card_that_cannot_reach_tag_historian_changes_nothing(
    hass: HomeAssistant, client, loaded
) -> None:
    client.async_rename_tag.return_value = LineageResult(LineageOutcome.UNAVAILABLE, "timeout")
    old = register(hass, "energy_2")
    entry = await loaded(make_entry(hass, [old]))
    await rename(hass, old, "sensor.energy")
    issue = ir.async_get(hass).async_get_issue(DOMAIN, renamed_issue_id(entry, "sensor.energy"))

    flow = await async_create_fix_flow(hass, issue.issue_id, issue.data)
    flow.hass = hass
    result = await flow.async_step_rename_tag()

    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == "carry_unavailable"
    assert entry.options[CONF_SEND_AS] == {"sensor.energy": "sensor.energy_2"}


async def test_an_unselected_entity_being_renamed_changes_nothing(
    hass: HomeAssistant, client, loaded
) -> None:
    register(hass, "energy_2")
    other = register(hass, "outdoor")
    entry = await loaded(make_entry(hass, [other], follow=True))

    await rename(hass, "sensor.energy_2", "sensor.energy")

    assert entry.options[CONF_ENTITIES] == [other]
    client.async_rename_tag.assert_not_awaited()


async def test_renamed_twice_before_the_tag_was_its_readings_still_go_into_the_first_history(
    hass: HomeAssistant, client, loaded
) -> None:
    old = register(hass, "energy_2")
    entry = await loaded(make_entry(hass, [old]))

    await rename(hass, old, "sensor.energy_new")
    await rename(hass, "sensor.energy_new", "sensor.energy")

    assert entry.options[CONF_ENTITIES] == ["sensor.energy"]
    assert entry.options[CONF_SEND_AS] == {"sensor.energy": "sensor.energy_2"}
    registry = ir.async_get(hass)
    assert registry.async_get_issue(DOMAIN, renamed_issue_id(entry, "sensor.energy_new")) is None, (
        "the card about the middle name is about a name that is gone"
    )
    assert registry.async_get_issue(DOMAIN, renamed_issue_id(entry, "sensor.energy")) is not None


# --------------------------------------------------------------------------
# Renames that arrive together, or while the entry is not loaded
# --------------------------------------------------------------------------

OLD_TAGS = {"sensor.a_old": "sensor.a_old", "sensor.b_old": "sensor.b_old"}


@pytest.mark.parametrize("follow", [False, True])
async def test_renaming_a_device_follows_every_entity_on_it(
    hass: HomeAssistant, client, loaded, follow
) -> None:
    """Renaming a device renames all of its entities at once - one registry
    event each, milliseconds apart.

    Before: each handler read the selection, asked Tag Historian which tags
    exist, and only then wrote the selection back. The handlers overlapped,
    and the last one to finish put the other entity's OLD id back: that entity
    was never subscribed again and its readings stopped without a word, with
    the option on or off. Here the lookup takes 50 ms, as a real one does,
    and nothing is awaited between the two renames.
    """

    async def slow_index() -> dict[str, str]:
        await asyncio.sleep(0.05)
        return dict(OLD_TAGS)

    client.async_get_tag_index.side_effect = slow_index
    a, b = register(hass, "a_old"), register(hass, "b_old")
    entry = await loaded(make_entry(hass, [a, b], follow=follow))

    registry = er.async_get(hass)
    registry.async_update_entity(a, new_entity_id="sensor.a_new")
    registry.async_update_entity(b, new_entity_id="sensor.b_new")
    await settle(hass)

    assert entry.options[CONF_ENTITIES] == ["sensor.a_new", "sensor.b_new"]
    if follow:
        assert sorted(call.args for call in client.async_rename_tag.await_args_list) == [
            ("sensor.a_old", "sensor.a_new"),
            ("sensor.b_old", "sensor.b_new"),
        ], "each tag renamed, once"
        assert entry.options[CONF_SEND_AS] == {}
    else:
        assert entry.options[CONF_SEND_AS] == {
            "sensor.a_new": "sensor.a_old",
            "sensor.b_new": "sensor.b_old",
        }
        assert card(hass, entry, "sensor.a_new") is not None
        assert card(hass, entry, "sensor.b_new") is not None

    await send_reading(hass, "sensor.a_new", "1.5")
    await send_reading(hass, "sensor.b_new", "2.5")
    tag_of = "new" if follow else "old"
    assert f"entity_id=a_{tag_of} value=1.5" in sent(client), "both entities are subscribed"
    assert f"entity_id=b_{tag_of} value=2.5" in sent(client), "both entities are subscribed"


async def test_a_rename_made_while_the_entry_reloads_is_not_lost(
    hass: HomeAssistant, client, loaded
) -> None:
    """Every handled rename changes the options, which reloads the entry.

    Before: the listener was the entry's - removed on unload, back at the end
    of setup - so a second rename made during that reload was never seen.
    Here the reload's first call to Tag Historian takes 300 ms, and the second
    entity is renamed 100 ms into it.
    """
    client.async_get_tag_index.return_value = dict(OLD_TAGS)
    a, b = register(hass, "a_old"), register(hass, "b_old")
    entry = await loaded(make_entry(hass, [a, b]))

    quota = client.async_get_quota.return_value

    async def slow_quota():
        await asyncio.sleep(0.3)
        return quota

    client.async_get_quota.side_effect = slow_quota

    registry = er.async_get(hass)
    registry.async_update_entity(a, new_entity_id="sensor.a_new")
    await asyncio.sleep(0.1)
    assert entry.state is ConfigEntryState.SETUP_IN_PROGRESS, "the window this is about"
    registry.async_update_entity(b, new_entity_id="sensor.b_new")
    await settle(hass)

    assert entry.state is ConfigEntryState.LOADED
    assert entry.options[CONF_ENTITIES] == ["sensor.a_new", "sensor.b_new"]
    assert entry.options[CONF_SEND_AS] == {
        "sensor.a_new": "sensor.a_old",
        "sensor.b_new": "sensor.b_old",
    }
    await send_reading(hass, "sensor.b_new", "7")
    assert "entity_id=b_old value=7" in sent(client)


async def test_a_rename_made_while_tag_historian_is_unreachable_is_followed(
    hass: HomeAssistant, client, loaded
) -> None:
    """An entry whose setup waits for Tag Historian to come back (setup retry)
    is not loaded - and the rename is followed all the same, and settled when
    the setup succeeds."""
    client.async_get_quota.side_effect = TagHistorianConnectionError("unreachable")
    old = register(hass, "energy_2")
    entry = make_entry(hass, [old])
    assert not await hass.config_entries.async_setup(entry.entry_id)
    assert entry.state is ConfigEntryState.SETUP_RETRY

    await rename(hass, old, "sensor.energy")

    assert entry.options[CONF_ENTITIES] == ["sensor.energy"]
    assert entry.options[CONF_SEND_AS] == {"sensor.energy": "sensor.energy_2"}
    client.async_get_tag_index.assert_not_awaited()  # nothing asked of an API that is down

    client.async_get_quota.side_effect = None
    async_fire_time_changed(hass, dt_util.utcnow() + timedelta(minutes=2))
    await settle(hass)

    assert entry.state is ConfigEntryState.LOADED
    assert card(hass, entry, "sensor.energy") is not None, "settled once it could be"
    await send_reading(hass, "sensor.energy", "42")
    assert "entity_id=energy_2 value=42" in sent(client)
    await hass.config_entries.async_unload(entry.entry_id)


async def test_a_merge_that_cannot_be_asked_now_is_asked_again_later(
    hass: HomeAssistant, client, loaded
) -> None:
    """A merge used to be asked for once, and an unreachable API turned it
    into a card - while the README promised it would be asked again and the
    readings would wait in the old series. Both are true now."""
    client.async_get_tag_index.return_value = {
        "sensor.energy": "sensor.energy", "sensor.energy_2": "sensor.energy_2"
    }
    client.async_merge_tags.return_value = LineageResult(LineageOutcome.UNAVAILABLE, "timeout")
    old_meter, new_meter = register(hass, "energy"), register(hass, "energy_2")
    entry = await loaded(make_entry(hass, [old_meter, new_meter], follow=True))

    er.async_get(hass).async_remove(old_meter)
    await hass.async_block_till_done()
    await rename(hass, new_meter, "sensor.energy")

    client.async_merge_tags.assert_awaited_once_with("sensor.energy_2", "sensor.energy")
    assert entry.options[CONF_SEND_AS] == {"sensor.energy": "sensor.energy_2"}, (
        "the readings go on into the old series meanwhile"
    )
    assert card(hass, entry, "sensor.energy") is None

    client.async_merge_tags.return_value = LineageResult(LineageOutcome.DONE)
    async_fire_time_changed(hass, dt_util.utcnow() + timedelta(seconds=RENAME_RETRY_SECONDS + 1))
    await settle(hass)

    assert client.async_merge_tags.await_count == 2
    assert entry.options[CONF_SEND_AS] == {}, "asked again later, and merged"


async def test_keeping_a_replaced_meter_apart_sends_under_the_new_id_from_then_on(
    hass: HomeAssistant, client, loaded
) -> None:
    client.async_get_tag_index.return_value = {
        "sensor.energy": "sensor.energy", "sensor.energy_2": "sensor.energy_2"
    }
    old_meter, new_meter = register(hass, "energy"), register(hass, "energy_2")
    entry = await loaded(make_entry(hass, [old_meter, new_meter]))
    er.async_get(hass).async_remove(old_meter)
    await hass.async_block_till_done()
    await rename(hass, new_meter, "sensor.energy")

    issue = card(hass, entry, "sensor.energy")
    flow = await async_create_fix_flow(hass, issue.issue_id, issue.data)
    flow.hass = hass
    menu = await flow.async_step_init()
    assert menu["menu_options"] == ["merge_tags", "keep_apart"]
    assert entry.options[CONF_SEND_AS] == {"sensor.energy": "sensor.energy_2"}, "until they choose"

    result = await flow.async_step_keep_apart()
    await settle(hass)

    assert result["type"] is FlowResultType.CREATE_ENTRY
    client.async_merge_tags.assert_not_awaited()
    assert entry.options[CONF_SEND_AS] == {}, "from now on, under the new id"


async def test_a_tag_renamed_in_explorer_already_is_left_as_it_is(
    hass: HomeAssistant, client, loaded
) -> None:
    """The old id is a former name of the new one: someone renamed the tag in
    Explorer first. Nothing to ask, nothing to card - and before, the check
    read only current names, called the old id "never stored anything", and
    started the entity on a fresh tag."""
    client.async_get_tag_index.return_value = {
        "sensor.energy": "sensor.energy", "sensor.energy_2": "sensor.energy"
    }
    old = register(hass, "energy_2")
    entry = await loaded(make_entry(hass, [old], follow=True))

    await rename(hass, old, "sensor.energy")

    assert entry.options[CONF_SEND_AS] == {}
    client.async_rename_tag.assert_not_awaited()
    client.async_merge_tags.assert_not_awaited()
    assert card(hass, entry, "sensor.energy") is None


async def test_a_rename_carried_after_the_entity_was_renamed_again_points_the_next_one_at_it(
    hass: HomeAssistant, client, loaded
) -> None:
    """a -> b is being carried when the entity becomes c. The selection then
    says "c, sent as a" - but once a is renamed to b, a is a former name of b
    and "rename a to c" would be refused. The mapping is pointed at b."""
    client.async_get_tag_index.return_value = {"sensor.a": "sensor.a"}
    entry = await loaded(make_entry(hass, [register(hass, "a")]))
    hass.config_entries.async_update_entry(
        entry,
        options={**entry.options, CONF_ENTITIES: ["sensor.c"], CONF_SEND_AS: {"sensor.c": "sensor.a"}},
    )
    await settle(hass)

    result = await async_carry_history(hass, entry, "sensor.b", "sensor.a", merge=False)
    await settle(hass)

    assert result.outcome is LineageOutcome.DONE
    assert entry.options[CONF_SEND_AS] == {"sensor.c": "sensor.b"}
