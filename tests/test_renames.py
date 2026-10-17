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

from datetime import timedelta
from unittest.mock import patch

import pytest
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
from custom_components.tag_historian.renames import renamed_issue_id
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


async def rename(hass: HomeAssistant, old: str, new: str) -> None:
    """What Settings > Entities does when a person changes the entity id."""
    er.async_get(hass).async_update_entity(old, new_entity_id=new)
    await hass.async_block_till_done(wait_background_tasks=True)
    await hass.async_block_till_done(wait_background_tasks=True)


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
    client.async_get_tag_names.return_value = {"sensor.energy", "sensor.energy_2"}
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
    client.async_get_tag_names.return_value = set()
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
