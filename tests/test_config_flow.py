"""The screens a user actually walks through, and every way they can fail."""

from __future__ import annotations

from unittest.mock import patch

import pytest
import voluptuous as vol
from homeassistant.config_entries import SOURCE_REAUTH, SOURCE_USER
from homeassistant.core import HomeAssistant
from homeassistant.data_entry_flow import FlowResultType
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.tag_historian.api import (
    QuotaSnapshot,
    TagHistorianAuthError,
    TagHistorianConnectionError,
    TagHistorianPermissionError,
)
from custom_components.tag_historian.config_flow import CONF_SHOW_ALL
from custom_components.tag_historian.const import (
    CONF_API_KEY,
    CONF_ENTITIES,
    CONF_HOST,
    CONF_MIN_INTERVAL,
    DOMAIN,
)

from .conftest import STUB_ACCOUNT, STUB_QUOTA, make_client

CREDENTIALS = {CONF_HOST: "api.taghistorian.com", CONF_API_KEY: "not-a-real-key"}
CLIENT_PATH = "custom_components.tag_historian.config_flow.TagHistorianClient"
SETUP_PATH = "custom_components.tag_historian.async_setup_entry"


@pytest.fixture
def house(hass: HomeAssistant) -> None:
    """A small but representative Home Assistant."""
    hass.states.async_set(
        "sensor.house_power",
        "1500",
        {"device_class": "power", "state_class": "measurement", "unit_of_measurement": "W"},
    )
    hass.states.async_set(
        "sensor.outdoor_temperature",
        "21.5",
        {"device_class": "temperature", "state_class": "measurement"},
    )
    hass.states.async_set("binary_sensor.heat_pump_running", "on", {"device_class": "running"})
    hass.states.async_set("climate.living_room", "heat", {"current_temperature": 21.0})
    hass.states.async_set("sensor.phone_battery", "88", {"device_class": "battery"})


async def _start(hass: HomeAssistant, client):
    with patch(CLIENT_PATH, return_value=client):
        result = await hass.config_entries.flow.async_init(
            DOMAIN, context={"source": SOURCE_USER}
        )
        return await hass.config_entries.flow.async_configure(
            result["flow_id"], CREDENTIALS
        )


async def test_happy_path_creates_an_entry(hass: HomeAssistant, house) -> None:
    """Connect, choose, review, done."""
    client = make_client()

    with patch(CLIENT_PATH, return_value=client), patch(SETUP_PATH, return_value=True):
        result = await hass.config_entries.flow.async_init(
            DOMAIN, context={"source": SOURCE_USER}
        )
        assert result["step_id"] == "user"

        result = await hass.config_entries.flow.async_configure(
            result["flow_id"], CREDENTIALS
        )
        assert result["step_id"] == "select"

        result = await hass.config_entries.flow.async_configure(
            result["flow_id"],
            {
                CONF_ENTITIES: ["sensor.house_power", "sensor.outdoor_temperature"],
                CONF_MIN_INTERVAL: 10,
            },
        )
        assert result["step_id"] == "review"

        result = await hass.config_entries.flow.async_configure(result["flow_id"], {})

    assert result["type"] is FlowResultType.CREATE_ENTRY
    assert result["title"] == "Test House"
    assert result["data"] == CREDENTIALS
    assert result["options"] == {
        CONF_ENTITIES: ["sensor.house_power", "sensor.outdoor_temperature"],
        CONF_MIN_INTERVAL: 10,
    }
    # The stable customer id, not the host - two entries for one account would
    # send every reading twice and bill for it twice.
    assert result["result"].unique_id == STUB_ACCOUNT["customerId"]


async def test_the_quota_is_visible_while_choosing(hass: HomeAssistant, house) -> None:
    """Every number on the picker screen comes from the API response."""
    result = await _start(hass, make_client())

    placeholders = result["description_placeholders"]
    assert placeholders["tag_limit"] == str(STUB_QUOTA.tag_limit)
    assert placeholders["tags_in_use"] == str(STUB_QUOTA.current_tag_count)
    assert placeholders["tags_available"] == str(STUB_QUOTA.tags_available)
    # climate.living_room is counted as an entity but it is not a measurement,
    # so it is held back behind "show every entity" rather than listed.
    assert placeholders["entity_count"] == "5"
    assert placeholders["offered_count"] == "4"
    assert placeholders["hidden_count"] == "1"


async def test_review_recomputes_the_numbers_after_submission(
    hass: HomeAssistant, house
) -> None:
    """Screen 3 is exact where screen 2 was only a starting point."""
    result = await _start(hass, make_client())
    result = await hass.config_entries.flow.async_configure(
        result["flow_id"],
        {
            CONF_ENTITIES: ["sensor.house_power", "sensor.phone_battery"],
            CONF_MIN_INTERVAL: 10,
        },
    )

    placeholders = result["description_placeholders"]
    assert placeholders["selected"] == "2"
    assert placeholders["new_tags"] == "2"
    assert placeholders["tags_available"] == str(STUB_QUOTA.tags_available)
    # 2 entities x 8640 readings/day at a 10 s floor.
    assert placeholders["readings_per_day"] == "17 280"
    assert placeholders["daily_limit"] == "120 000"
    assert placeholders["percent"] == "14"


async def test_existing_tags_do_not_spend_the_budget_again(
    hass: HomeAssistant, house
) -> None:
    """Re-selecting an entity that already has history costs no new tag.

    The quota counts TAGS, so a name that exists already needs no free slot.
    Counting ``len(selected)`` instead of the set difference against
    ``GET /api/tags`` would tell a user they are over quota when they are not,
    and talk them into deselecting entities that were costing them nothing.
    """
    client = make_client(tags={"sensor.house_power"})
    result = await _start(hass, client)
    result = await hass.config_entries.flow.async_configure(
        result["flow_id"],
        {
            CONF_ENTITIES: ["sensor.house_power", "sensor.outdoor_temperature"],
            CONF_MIN_INTERVAL: 10,
        },
    )

    assert result["description_placeholders"]["selected"] == "2"
    assert result["description_placeholders"]["new_tags"] == "1"


async def test_over_quota_selection_is_trimmed_to_the_available_budget(
    hass: HomeAssistant, house
) -> None:
    """The review screen blocks a selection that would be refused.

    Trimming keeps the best-ranked entities, so 'Trim' removes exactly the ones
    the ranking would have shown at the bottom of the list.
    """
    tight = QuotaSnapshot(
        tag_limit=3,
        current_tag_count=2,
        measurements_per_day_limit=120000,
        measurements_today=0,
        storage_limit_bytes=1,
        current_storage_bytes=0,
    )
    client = make_client(quota=tight)

    with patch(CLIENT_PATH, return_value=client), patch(SETUP_PATH, return_value=True):
        result = await hass.config_entries.flow.async_init(
            DOMAIN, context={"source": SOURCE_USER}
        )
        result = await hass.config_entries.flow.async_configure(
            result["flow_id"], CREDENTIALS
        )
        result = await hass.config_entries.flow.async_configure(
            result["flow_id"],
            {
                CONF_ENTITIES: [
                    "sensor.outdoor_temperature",
                    "sensor.house_power",
                    "binary_sensor.heat_pump_running",
                ],
                CONF_MIN_INTERVAL: 10,
            },
        )
        assert result["step_id"] == "review_trim"
        assert result["description_placeholders"]["overflow"] == "2"

        result = await hass.config_entries.flow.async_configure(
            result["flow_id"], {"trim_to_fit": True}
        )

    assert result["type"] is FlowResultType.CREATE_ENTRY
    assert result["options"][CONF_ENTITIES] == ["sensor.house_power"]


async def test_over_quota_can_be_accepted_deliberately(
    hass: HomeAssistant, house
) -> None:
    """Unticking 'trim' is the user saying they would rather keep the list."""
    tight = QuotaSnapshot(
        tag_limit=3,
        current_tag_count=2,
        measurements_per_day_limit=120000,
        measurements_today=0,
        storage_limit_bytes=1,
        current_storage_bytes=0,
    )

    with patch(CLIENT_PATH, return_value=make_client(quota=tight)), patch(
        SETUP_PATH, return_value=True
    ):
        result = await hass.config_entries.flow.async_init(
            DOMAIN, context={"source": SOURCE_USER}
        )
        result = await hass.config_entries.flow.async_configure(
            result["flow_id"], CREDENTIALS
        )
        result = await hass.config_entries.flow.async_configure(
            result["flow_id"],
            {
                CONF_ENTITIES: ["sensor.house_power", "sensor.outdoor_temperature"],
                CONF_MIN_INTERVAL: 10,
            },
        )
        result = await hass.config_entries.flow.async_configure(
            result["flow_id"], {"trim_to_fit": False}
        )

    assert len(result["options"][CONF_ENTITIES]) == 2


@pytest.mark.parametrize(
    ("side_effect", "expected_error"),
    [
        (TagHistorianAuthError("nope"), "invalid_auth"),
        (TagHistorianConnectionError("dns"), "cannot_connect"),
        (TagHistorianPermissionError("blocked"), "account_blocked"),
    ],
)
async def test_connect_failures_say_which_one_it_was(
    hass: HomeAssistant, side_effect, expected_error
) -> None:
    """Three different problems need three different fixes."""
    client = make_client()
    client.async_validate.side_effect = side_effect

    result = await _start(hass, client)

    assert result["type"] is FlowResultType.FORM
    assert result["step_id"] == "user"
    assert result["errors"] == {"base": expected_error}


async def test_a_read_only_key_is_named_as_such(hass: HomeAssistant) -> None:
    """A Read-scoped key passes /me and fails everything that needs Editor.

    Including the write endpoint, so catching it here turns a silent failure at
    3am into a setup error that names the actual fix.
    """
    client = make_client()
    client.async_get_quota.side_effect = TagHistorianPermissionError("read only")

    result = await _start(hass, client)

    assert result["errors"] == {"base": "read_only_key"}


async def test_an_inactive_account_is_named_as_such(hass: HomeAssistant) -> None:
    client = make_client(account={**STUB_ACCOUNT, "isActive": False})
    result = await _start(hass, client)
    assert result["errors"] == {"base": "account_inactive"}


async def test_an_unverified_account_past_its_grace_period_is_named_as_such(
    hass: HomeAssistant,
) -> None:
    """Detected at setup rather than after a week of missing history."""
    client = make_client(
        account={
            **STUB_ACCOUNT,
            "emailVerified": False,
            "verifyDeadline": "2020-01-01T00:00:00Z",
        }
    )
    result = await _start(hass, client)
    assert result["errors"] == {"base": "email_not_verified"}


async def test_an_unverified_account_inside_its_grace_period_still_works(
    hass: HomeAssistant, house
) -> None:
    """Writing is not blocked yet, so setup must not block either."""
    client = make_client(
        account={
            **STUB_ACCOUNT,
            "emailVerified": False,
            "verifyDeadline": "2099-01-01T00:00:00Z",
        }
    )
    result = await _start(hass, client)
    assert result["step_id"] == "select"


async def test_the_same_account_cannot_be_added_twice(hass: HomeAssistant) -> None:
    """Two entries for one account would double the readings bill."""
    MockConfigEntry(
        domain=DOMAIN, unique_id=STUB_ACCOUNT["customerId"], data=CREDENTIALS
    ).add_to_hass(hass)

    result = await _start(hass, make_client())

    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == "already_configured"


async def test_reauth_replaces_the_key_in_place(hass: HomeAssistant) -> None:
    entry = MockConfigEntry(
        domain=DOMAIN, unique_id=STUB_ACCOUNT["customerId"], data=CREDENTIALS
    )
    entry.add_to_hass(hass)

    with patch(CLIENT_PATH, return_value=make_client()), patch(
        SETUP_PATH, return_value=True
    ):
        result = await hass.config_entries.flow.async_init(
            DOMAIN,
            context={"source": SOURCE_REAUTH, "entry_id": entry.entry_id},
            data=entry.data,
        )
        assert result["step_id"] == "reauth_confirm"

        result = await hass.config_entries.flow.async_configure(
            result["flow_id"], {CONF_API_KEY: "a-different-key"}
        )

    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == "reauth_successful"
    assert entry.data[CONF_API_KEY] == "a-different-key"


async def test_reauth_refuses_a_key_for_a_different_account(
    hass: HomeAssistant,
) -> None:
    """Otherwise this house's history quietly starts filling someone else's tenant."""
    entry = MockConfigEntry(
        domain=DOMAIN, unique_id=STUB_ACCOUNT["customerId"], data=CREDENTIALS
    )
    entry.add_to_hass(hass)

    other = make_client(account={**STUB_ACCOUNT, "customerId": "99999999-0000-0000-0000-000000000000"})

    with patch(CLIENT_PATH, return_value=other), patch(SETUP_PATH, return_value=True):
        result = await hass.config_entries.flow.async_init(
            DOMAIN,
            context={"source": SOURCE_REAUTH, "entry_id": entry.entry_id},
            data=entry.data,
        )
        result = await hass.config_entries.flow.async_configure(
            result["flow_id"], {CONF_API_KEY: "someone-elses-key"}
        )

    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == "wrong_account"
    assert entry.data[CONF_API_KEY] == CREDENTIALS[CONF_API_KEY]


async def test_options_flow_changes_the_selection(hass: HomeAssistant, house) -> None:
    """The selection can be changed without re-adding the integration."""
    entry = MockConfigEntry(
        domain=DOMAIN,
        unique_id=STUB_ACCOUNT["customerId"],
        data=CREDENTIALS,
        options={CONF_ENTITIES: ["sensor.house_power"], CONF_MIN_INTERVAL: 10},
    )
    entry.add_to_hass(hass)

    with patch(CLIENT_PATH, return_value=make_client()), patch(
        SETUP_PATH, return_value=True
    ):
        result = await hass.config_entries.options.async_init(entry.entry_id)
        assert result["step_id"] == "select"

        result = await hass.config_entries.options.async_configure(
            result["flow_id"],
            {
                CONF_ENTITIES: ["sensor.outdoor_temperature", "binary_sensor.heat_pump_running"],
                CONF_MIN_INTERVAL: 30,
            },
        )
        assert result["step_id"] == "review"

        result = await hass.config_entries.options.async_configure(result["flow_id"], {})
        await hass.async_block_till_done()

    assert result["type"] is FlowResultType.CREATE_ENTRY
    assert entry.options == {
        CONF_ENTITIES: ["sensor.outdoor_temperature", "binary_sensor.heat_pump_running"],
        CONF_MIN_INTERVAL: 30,
    }


# --------------------------------------------------------------------------
# D1 - a momentary state must not decide what can be selected
# --------------------------------------------------------------------------


async def test_the_options_flow_opens_when_a_selected_entity_is_unavailable(
    hass: HomeAssistant, house
) -> None:
    """One flat battery must not lock a user out of their own settings.

    The picker's offer list used to be built from entities whose CURRENT state
    was numeric, while the default came from the saved options. EntitySelector
    enforces include_entities server-side with vol.In, so the dialog opened
    pre-filled with a value its own selector rejects: pressing Submit without
    touching anything raised vol.Invalid.

    ``data_schema({})`` here IS pressing Submit without touching anything - it
    applies the schema's own defaults and validates them.
    """
    hass.states.async_set("sensor.house_power", "unavailable")
    hass.states.async_set("sensor.outdoor_temperature", "unknown")

    entry = MockConfigEntry(
        domain=DOMAIN,
        unique_id=STUB_ACCOUNT["customerId"],
        data=CREDENTIALS,
        options={
            CONF_ENTITIES: ["sensor.house_power", "sensor.outdoor_temperature"],
            CONF_MIN_INTERVAL: 10,
        },
    )
    entry.add_to_hass(hass)

    with patch(CLIENT_PATH, return_value=make_client()), patch(
        SETUP_PATH, return_value=True
    ):
        result = await hass.config_entries.options.async_init(entry.entry_id)
        assert result["step_id"] == "select"

        unchanged = result["data_schema"]({})
        assert set(unchanged[CONF_ENTITIES]) == {
            "sensor.house_power",
            "sensor.outdoor_temperature",
        }

        result = await hass.config_entries.options.async_configure(
            result["flow_id"], unchanged
        )
        assert result["step_id"] == "review"

        result = await hass.config_entries.options.async_configure(
            result["flow_id"], {}
        )
        await hass.async_block_till_done()

    assert result["type"] is FlowResultType.CREATE_ENTRY
    assert entry.options[CONF_ENTITIES] == [
        "sensor.house_power",
        "sensor.outdoor_temperature",
    ]


async def test_an_unavailable_entity_is_still_offered_at_first_setup(
    hass: HomeAssistant,
) -> None:
    """The same root cause, silently omitting entities with no explanation."""
    hass.states.async_set(
        "sensor.house_power",
        "unavailable",
        {"device_class": "power", "state_class": "measurement"},
    )

    result = await _start(hass, make_client())

    assert result["step_id"] == "select"
    schema = result["data_schema"]({})
    assert schema[CONF_ENTITIES] == ["sensor.house_power"]


# --------------------------------------------------------------------------
# D5 - the escape hatch
# --------------------------------------------------------------------------


async def test_the_picker_hides_what_you_operate_and_the_toggle_brings_it_back(
    hass: HomeAssistant, house
) -> None:
    """Hiding by default is not the same as forbidding."""
    result = await _start(hass, make_client())

    assert result["description_placeholders"]["offered_count"] == "4"
    assert result["description_placeholders"]["hidden_count"] == "1"

    # climate.living_room is not selectable while it is hidden...
    with pytest.raises(vol.Invalid):
        result["data_schema"](
            {CONF_ENTITIES: ["climate.living_room"], CONF_MIN_INTERVAL: 10}
        )

    # ...ticking "show every entity" redraws the same screen with all of them...
    result = await hass.config_entries.flow.async_configure(
        result["flow_id"],
        {
            CONF_ENTITIES: ["sensor.house_power"],
            CONF_MIN_INTERVAL: 10,
            CONF_SHOW_ALL: True,
        },
    )
    assert result["step_id"] == "select", "the toggle redraws, it does not advance"
    assert result["description_placeholders"]["offered_count"] == "5"
    assert result["description_placeholders"]["hidden_count"] == "0"

    # ...and now it is.
    result["data_schema"](
        {
            CONF_ENTITIES: ["sensor.house_power", "climate.living_room"],
            CONF_MIN_INTERVAL: 10,
            CONF_SHOW_ALL: True,
        }
    )


async def test_what_was_ticked_survives_the_toggle(
    hass: HomeAssistant, house
) -> None:
    """Redrawing the list must not quietly empty the boxes."""
    result = await _start(hass, make_client())
    result = await hass.config_entries.flow.async_configure(
        result["flow_id"],
        {
            CONF_ENTITIES: ["sensor.phone_battery"],
            CONF_MIN_INTERVAL: 25,
            CONF_SHOW_ALL: True,
        },
    )

    unchanged = result["data_schema"]({})
    assert unchanged[CONF_ENTITIES] == ["sensor.phone_battery"]
    assert unchanged[CONF_MIN_INTERVAL] == 25


# --------------------------------------------------------------------------
# D8 - an integration that is configured and inert
# --------------------------------------------------------------------------


async def test_an_empty_selection_is_refused_rather_than_set_up(
    hass: HomeAssistant, house
) -> None:
    """An entry that subscribes to nothing looks exactly like a working one."""
    result = await _start(hass, make_client())
    result = await hass.config_entries.flow.async_configure(
        result["flow_id"], {CONF_ENTITIES: [], CONF_MIN_INTERVAL: 10}
    )

    assert result["type"] is FlowResultType.FORM
    assert result["step_id"] == "select"
    assert result["errors"] == {"base": "select_at_least_one"}


async def test_a_full_account_refuses_to_install_an_inert_integration(
    hass: HomeAssistant, house
) -> None:
    """tags_available == 0, so "trim to fit" fits nothing at all.

    The trim emptied the selection, the entry was created with ``entities:
    []``, the forwarder subscribed to nothing, and no repair and no warning
    said so. The user believed they had set it up.
    """
    full = QuotaSnapshot(
        tag_limit=3,
        current_tag_count=3,
        measurements_per_day_limit=120000,
        measurements_today=0,
        storage_limit_bytes=1,
        current_storage_bytes=0,
    )

    with patch(CLIENT_PATH, return_value=make_client(quota=full)), patch(
        SETUP_PATH, return_value=True
    ):
        result = await hass.config_entries.flow.async_init(
            DOMAIN, context={"source": SOURCE_USER}
        )
        result = await hass.config_entries.flow.async_configure(
            result["flow_id"], CREDENTIALS
        )
        result = await hass.config_entries.flow.async_configure(
            result["flow_id"],
            {CONF_ENTITIES: ["sensor.house_power"], CONF_MIN_INTERVAL: 10},
        )
        assert result["step_id"] == "review_trim"
        assert result["description_placeholders"]["tags_available"] == "0"

        result = await hass.config_entries.flow.async_configure(
            result["flow_id"], {"trim_to_fit": True}
        )

    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == "no_tags_available"
    assert not hass.config_entries.async_entries(DOMAIN)


async def test_a_full_account_still_works_if_the_tags_already_exist(
    hass: HomeAssistant, house
) -> None:
    """Removing and re-adding the integration: no free slots, and none needed."""
    full = QuotaSnapshot(
        tag_limit=3,
        current_tag_count=3,
        measurements_per_day_limit=120000,
        measurements_today=0,
        storage_limit_bytes=1,
        current_storage_bytes=0,
    )
    client = make_client(quota=full, tags={"sensor.house_power"})

    with patch(CLIENT_PATH, return_value=client), patch(SETUP_PATH, return_value=True):
        result = await hass.config_entries.flow.async_init(
            DOMAIN, context={"source": SOURCE_USER}
        )
        result = await hass.config_entries.flow.async_configure(
            result["flow_id"], CREDENTIALS
        )
        result = await hass.config_entries.flow.async_configure(
            result["flow_id"],
            {CONF_ENTITIES: ["sensor.house_power"], CONF_MIN_INTERVAL: 10},
        )
        assert result["description_placeholders"]["new_tags"] == "0"
        result = await hass.config_entries.flow.async_configure(result["flow_id"], {})

    assert result["type"] is FlowResultType.CREATE_ENTRY
    assert result["options"][CONF_ENTITIES] == ["sensor.house_power"]


# --------------------------------------------------------------------------
# D7 - "trim to fit" measured in the wrong unit
# --------------------------------------------------------------------------


async def test_trim_is_measured_in_tags_and_actually_fits(
    hass: HomeAssistant, house
) -> None:
    """One free slot, three selected, the lowest-ranked already has a tag.

    ``overflow`` counts NEW TAGS and the cut was ``len(selected) - overflow``,
    which counts ENTITIES. So the trim removed the one entity that was already
    free, kept both of the ones that needed a slot, and finished still needing
    two tags against one - a 422 at the first write. Worst of both: it took
    away something the user asked for AND did not achieve the fit.
    """
    tight = QuotaSnapshot(
        tag_limit=6,
        current_tag_count=5,
        measurements_per_day_limit=120000,
        measurements_today=0,
        storage_limit_bytes=1,
        current_storage_bytes=0,
    )
    # binary_sensor.heat_pump_running is the lowest-ranked of the three, and it
    # is the one whose tag already exists.
    client = make_client(quota=tight, tags={"binary_sensor.heat_pump_running"})

    with patch(CLIENT_PATH, return_value=client), patch(SETUP_PATH, return_value=True):
        result = await hass.config_entries.flow.async_init(
            DOMAIN, context={"source": SOURCE_USER}
        )
        result = await hass.config_entries.flow.async_configure(
            result["flow_id"], CREDENTIALS
        )
        result = await hass.config_entries.flow.async_configure(
            result["flow_id"],
            {
                CONF_ENTITIES: [
                    "sensor.house_power",
                    "sensor.outdoor_temperature",
                    "binary_sensor.heat_pump_running",
                ],
                CONF_MIN_INTERVAL: 10,
            },
        )
        assert result["description_placeholders"]["new_tags"] == "2"
        assert result["description_placeholders"]["tags_available"] == "1"
        assert result["description_placeholders"]["overflow"] == "1"

        result = await hass.config_entries.flow.async_configure(
            result["flow_id"], {"trim_to_fit": True}
        )

    kept = result["options"][CONF_ENTITIES]
    new_tags = [e for e in kept if e != "binary_sensor.heat_pump_running"]

    assert len(new_tags) <= 1, f"the trim has to actually fit: {kept}"
    assert "binary_sensor.heat_pump_running" in kept, "it was free; removing it bought nothing"
    assert kept == ["sensor.house_power", "binary_sensor.heat_pump_running"]


async def test_options_flow_refuses_to_show_a_stale_budget(
    hass: HomeAssistant, house
) -> None:
    """No live numbers means no screen - a stale budget is how a surface lies."""
    entry = MockConfigEntry(
        domain=DOMAIN,
        unique_id=STUB_ACCOUNT["customerId"],
        data=CREDENTIALS,
        options={CONF_ENTITIES: [], CONF_MIN_INTERVAL: 10},
    )
    entry.add_to_hass(hass)

    client = make_client()
    client.async_get_quota.side_effect = TagHistorianConnectionError("down")

    with patch(CLIENT_PATH, return_value=client), patch(SETUP_PATH, return_value=True):
        result = await hass.config_entries.options.async_init(entry.entry_id)

    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == "cannot_connect"
