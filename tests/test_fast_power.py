"""Power sensors more often, kept inside the plan while the entry runs (fast_power.py)."""

from __future__ import annotations

import dataclasses
import logging
from unittest.mock import patch

import pytest
from homeassistant.core import HomeAssistant
from homeassistant.helpers import issue_registry as ir
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.tag_historian.const import (
    CONF_API_KEY,
    CONF_ENTITIES,
    CONF_FAST_POWER,
    CONF_HOST,
    CONF_MIN_INTERVAL,
    DOMAIN,
    ISSUE_FAST_POWER,
)
from custom_components.tag_historian.fast_power import issue_id

from .conftest import STUB_QUOTA, make_client

CLIENT_PATH = "custom_components.tag_historian.TagHistorianClient"
SELECTED = ["sensor.house_power", "sensor.phone_battery", "sensor.outdoor_temperature"]


def _quota_allowing(per_day: int):
    return dataclasses.replace(STUB_QUOTA, measurements_per_day_limit=per_day)


@pytest.fixture
def house(hass: HomeAssistant) -> None:
    """One power sensor in W and two that are not power sensors."""
    hass.states.async_set(
        "sensor.house_power", "1500", {"device_class": "power", "unit_of_measurement": "W"}
    )
    hass.states.async_set("sensor.phone_battery", "88", {"device_class": "battery"})
    hass.states.async_set(
        "sensor.outdoor_temperature",
        "4.5",
        {"device_class": "temperature", "unit_of_measurement": "°C"},
    )


def _entry(hass: HomeAssistant, fast_power: bool = True) -> MockConfigEntry:
    entry = MockConfigEntry(
        domain=DOMAIN,
        data={CONF_HOST: "api.taghistorian.com", CONF_API_KEY: "not-a-real-key"},
        options={
            CONF_ENTITIES: SELECTED,
            CONF_MIN_INTERVAL: 10,
            CONF_FAST_POWER: fast_power,
        },
        unique_id="11111111-2222-3333-4444-555555555555",
    )
    entry.add_to_hass(hass)
    return entry


def _issue(hass: HomeAssistant, entry) -> ir.IssueEntry | None:
    return ir.async_get(hass).async_get_issue(DOMAIN, issue_id(entry))


async def test_power_sensors_go_fast_when_the_sum_fits(hass: HomeAssistant, house) -> None:
    """43 200 + 2 x 8 640 = 60 480 at most, inside 120 000: two seconds, and no card."""
    entry = _entry(hass)
    with patch(CLIENT_PATH, return_value=make_client(quota=_quota_allowing(120_000))):
        assert await hass.config_entries.async_setup(entry.entry_id)
        await hass.async_block_till_done()

    forwarder = entry.runtime_data.forwarder
    assert forwarder._interval_for("sensor.house_power") == 2
    assert forwarder._interval_for("sensor.phone_battery") == 10
    assert _issue(hass, entry) is None

    assert await hass.config_entries.async_unload(entry.entry_id)
    await hass.async_block_till_done()


async def test_a_plan_that_no_longer_fits_holds_the_power_sensors_and_says_so_once(
    hass: HomeAssistant, house, caplog: pytest.LogCaptureFixture
) -> None:
    """Set up over the allowance: every entity at the minimum interval, one warning, one card.

    And when the plan grows back, the power sensors go back to two seconds by
    themselves and the card goes away.
    """
    entry = _entry(hass)
    client = make_client(quota=_quota_allowing(55_000))
    caplog.set_level(logging.WARNING)
    with patch(CLIENT_PATH, return_value=client):
        assert await hass.config_entries.async_setup(entry.entry_id)
        await hass.async_block_till_done()

        forwarder = entry.runtime_data.forwarder
        coordinator = entry.runtime_data.coordinator
        assert forwarder._interval_for("sensor.house_power") == 10

        issue = _issue(hass, entry)
        assert issue is not None
        assert issue.translation_key == ISSUE_FAST_POWER
        assert issue.translation_placeholders == {
            "power_interval": "2",
            "selected": "3",
            "power_count": "1",
            "readings_per_day": "60 480",
            "daily_limit": "55 000",
            "min_interval": "10",
        }

        # Another poll that still does not fit: no second warning.
        client.async_get_quota.return_value = dataclasses.replace(
            _quota_allowing(55_000), measurements_today=5000
        )
        await coordinator.async_refresh()
        await hass.async_block_till_done()
        warnings = [r for r in caplog.records if "power sensors too" in r.getMessage()]
        assert len(warnings) == 1
        assert forwarder._interval_for("sensor.house_power") == 10

        # The plan grows: two seconds again, and the card is gone.
        client.async_get_quota.return_value = _quota_allowing(500_000)
        await coordinator.async_refresh()
        await hass.async_block_till_done()
        assert forwarder._interval_for("sensor.house_power") == 2
        assert _issue(hass, entry) is None

        # And shrinks again: held, and said again, because it is news again.
        client.async_get_quota.return_value = _quota_allowing(55_000)
        await coordinator.async_refresh()
        await hass.async_block_till_done()
        assert forwarder._interval_for("sensor.house_power") == 10
        assert _issue(hass, entry) is not None

    assert await hass.config_entries.async_unload(entry.entry_id)
    await hass.async_block_till_done()


async def test_a_selection_that_grows_a_power_sensor_is_checked_again(
    hass: HomeAssistant,
) -> None:
    """At setup nothing selected is a power sensor yet; later one is, and it does not fit.

    The sum is done with what Home Assistant says of the entities now, so it
    is done again at the next poll - and an entity that becomes a power
    sensor after the screen is held at the minimum interval like the rest.
    """
    hass.states.async_set("sensor.house_power", "1500", {})
    hass.states.async_set("sensor.phone_battery", "88", {"device_class": "battery"})
    hass.states.async_set("sensor.outdoor_temperature", "4.5", {})
    entry = _entry(hass)
    client = make_client(quota=_quota_allowing(55_000))
    with patch(CLIENT_PATH, return_value=client):
        assert await hass.config_entries.async_setup(entry.entry_id)
        await hass.async_block_till_done()
        assert _issue(hass, entry) is None

        hass.states.async_set(
            "sensor.house_power", "1500", {"device_class": "power", "unit_of_measurement": "kW"}
        )
        client.async_get_quota.return_value = dataclasses.replace(
            _quota_allowing(55_000), measurements_today=99
        )
        await entry.runtime_data.coordinator.async_refresh()
        await hass.async_block_till_done()

        assert entry.runtime_data.forwarder._interval_for("sensor.house_power") == 10
        assert _issue(hass, entry) is not None

    assert await hass.config_entries.async_unload(entry.entry_id)
    await hass.async_block_till_done()


async def test_unticking_the_box_clears_the_card(hass: HomeAssistant, house) -> None:
    """A card about an option that is off is a card about nothing."""
    entry = _entry(hass)
    with patch(CLIENT_PATH, return_value=make_client(quota=_quota_allowing(55_000))):
        assert await hass.config_entries.async_setup(entry.entry_id)
        await hass.async_block_till_done()
        assert _issue(hass, entry) is not None

        hass.config_entries.async_update_entry(
            entry, options={**entry.options, CONF_FAST_POWER: False}
        )
        await hass.async_block_till_done()

        assert _issue(hass, entry) is None
        assert entry.runtime_data.forwarder._interval_for("sensor.house_power") == 10

    assert await hass.config_entries.async_unload(entry.entry_id)
    await hass.async_block_till_done()
