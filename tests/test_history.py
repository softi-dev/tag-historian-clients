"""The one function that reads the recorder, against a real one.

Everything else in the import and the backfill is tested with a stand-in for
this function, so this is where the stand-in is held to the real thing: the
recorder's own API, its own database, Home Assistant's own version of both.
"""

from __future__ import annotations

from datetime import timedelta

import pytest
from homeassistant.core import HomeAssistant
from homeassistant.util import dt as dt_util
from pytest_homeassistant_custom_component.components.recorder.common import (
    async_wait_recording_done,
)

from custom_components.tag_historian import history


@pytest.fixture(autouse=True)
def auto_enable_custom_integrations(recorder_db_url, enable_custom_integrations):
    """The conftest's fixture of this name, with the recorder's database first.

    The recorder fixtures refuse to be set up after Home Assistant is, and the
    conftest's version asks for Home Assistant before anything a test asks
    for. Naming the database here, ahead of it, is the order they need.
    """
    yield


async def test_the_recorder_is_read_for_state_changes_inside_the_window_only(
    recorder_mock, hass: HomeAssistant, freezer
) -> None:
    start = dt_util.utcnow()

    freezer.tick(timedelta(seconds=10))
    hass.states.async_set("sensor.house_power", "1500", {"unit_of_measurement": "W"})
    freezer.tick(timedelta(seconds=10))
    # An attribute changing is not a reading of the sensor.
    hass.states.async_set("sensor.house_power", "1500", {"unit_of_measurement": "W", "battery": 80})
    freezer.tick(timedelta(seconds=10))
    hass.states.async_set("sensor.house_power", "1600", {"unit_of_measurement": "W"})
    hass.states.async_set("sensor.other", "1")
    freezer.tick(timedelta(seconds=10))
    end = dt_util.utcnow()
    freezer.tick(timedelta(seconds=10))
    hass.states.async_set("sensor.house_power", "1700", {"unit_of_measurement": "W"})
    await async_wait_recording_done(hass)

    states = await history.async_recorded_states(hass, "sensor.house_power", start, end)

    assert [state.state for state in states] == ["1500", "1600"], (
        "state changes inside the window, oldest first: no attribute-only rows, nothing after "
        "the window, and no state invented at its start"
    )
    assert states[0].attributes["unit_of_measurement"] == "W"
    assert history.recorder_ready(hass)


async def test_a_window_that_starts_after_a_value_was_set_has_no_reading_at_its_start(
    recorder_mock, hass: HomeAssistant, freezer
) -> None:
    """include_start_time_state=False: the value in force is not a reading at that time."""
    hass.states.async_set("sensor.house_power", "1500")
    freezer.tick(timedelta(minutes=5))
    start = dt_util.utcnow()
    freezer.tick(timedelta(minutes=5))
    end = dt_util.utcnow()
    await async_wait_recording_done(hass)

    assert await history.async_recorded_states(hass, "sensor.house_power", start, end) == []


async def test_without_a_recorder_nothing_is_read(hass: HomeAssistant) -> None:
    now = dt_util.utcnow()
    assert not history.recorder_ready(hass)
    assert await history.async_recorded_states(hass, "sensor.a", now - timedelta(days=1), now) == []
    assert history.recorder_keep_days(hass) is None
