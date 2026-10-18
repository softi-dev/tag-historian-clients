"""The one function that reads the recorder, against a real one.

Everything else in the import and the backfill is tested with a stand-in for
this function, so this is where the stand-in is held to the real thing: the
recorder's own API, its own database, Home Assistant's own version of both -
and what the recorder does with an entity's history when the entity is
renamed, which the stand-in in test_renamed_history.py models.
"""

from __future__ import annotations

from datetime import timedelta

import pytest
from homeassistant.core import HomeAssistant
from homeassistant.helpers import entity_registry as er
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


async def _rename(hass: HomeAssistant, old: str, new: str) -> None:
    """A rename in Settings: the registry, and the platform taking the old id's state away."""
    er.async_get(hass).async_update_entity(old, new_entity_id=new)
    hass.states.async_remove(old)
    await hass.async_block_till_done()
    await async_wait_recording_done(hass)


def _register(hass: HomeAssistant, object_id: str) -> str:
    return (
        er.async_get(hass)
        .async_get_or_create("sensor", "test", f"unique-{object_id}", suggested_object_id=object_id)
        .entity_id
    )


async def test_a_rename_moves_the_recorded_history_to_the_new_id(
    recorder_mock, hass: HomeAssistant, freezer
) -> None:
    """What the backfill and the import of a renamed entity rely on, and the stand-in models.

    Read under the old id after a rename, an import or a gap found nothing.
    """
    start = dt_util.utcnow()
    old = _register(hass, "energy_2")
    for value in ("1", "2"):
        freezer.tick(timedelta(seconds=10))
        hass.states.async_set(old, value)
    await async_wait_recording_done(hass)

    await _rename(hass, old, "sensor.energy")
    freezer.tick(timedelta(seconds=10))
    hass.states.async_set("sensor.energy", "3")
    freezer.tick(timedelta(seconds=10))
    await async_wait_recording_done(hass)
    end = dt_util.utcnow()

    source = await history.async_source(hass, "sensor.energy", old)
    assert source == history.Source("sensor.energy"), "nothing left under the old id"
    states = await history.async_states(hass, source, start, end)
    assert [state.state for state in states] == ["1", "2", "3"]
    assert await history.async_recorded_states(hass, old, start, end) == []


async def test_a_rename_onto_an_id_with_a_history_is_read_from_both_ids(
    recorder_mock, hass: HomeAssistant, freezer
) -> None:
    """A replaced meter given the old meter's id: Home Assistant moves nothing.

    Under the new id the recorder has the OLD meter's readings, then the new
    meter's from the rename on; the new meter's own are under its old id.
    Read from the new id alone, its history was another meter's.
    """
    start = dt_util.utcnow()
    old_meter = _register(hass, "energy")
    new_meter = _register(hass, "energy_2")
    for value in ("100", "101"):
        freezer.tick(timedelta(seconds=10))
        hass.states.async_set(old_meter, value)
    for value in ("1", "2"):
        freezer.tick(timedelta(seconds=10))
        hass.states.async_set(new_meter, value)
    await async_wait_recording_done(hass)
    er.async_get(hass).async_remove(old_meter)
    hass.states.async_remove(old_meter)
    await async_wait_recording_done(hass)

    await _rename(hass, new_meter, "sensor.energy")
    freezer.tick(timedelta(seconds=10))
    hass.states.async_set("sensor.energy", "3")
    freezer.tick(timedelta(seconds=10))
    await async_wait_recording_done(hass)
    end = dt_util.utcnow()

    left = await history.async_recorded_states(hass, new_meter, start, end)
    # The empty one is the row the recorder writes when the state leaves the
    # old id - the rename's own moment, and a gap to both callers.
    assert [state.state for state in left] == ["1", "2", ""], "left where it was"
    source = await history.async_source(hass, "sensor.energy", new_meter)
    assert source == history.Source("sensor.energy", new_meter, left[-1].last_updated)
    states = await history.async_states(hass, source, start, end)
    assert [state.state for state in states if state.state] == ["1", "2", "3"], (
        "the new meter's, and only its"
    )


async def test_without_a_recorder_nothing_is_read(hass: HomeAssistant) -> None:
    now = dt_util.utcnow()
    assert not history.recorder_ready(hass)
    assert await history.async_recorded_states(hass, "sensor.a", now - timedelta(days=1), now) == []
    assert history.recorder_keep_days(hass) is None
    assert await history.async_last_recorded(hass, "sensor.a") is None
