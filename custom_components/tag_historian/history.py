"""Read what Home Assistant's recorder kept, for the import and the backfill.

One function, so that both callers read the recorder the same way and a test
can stand in for it. Two choices in it are load-bearing:

* ``state_changes_during_period`` returns the rows where the STATE changed -
  not the ones where only an attribute did - which is what a reading is. A
  battery level attribute ticking over is not a new reading of the sensor.

* ``include_start_time_state=False``. With it on, the recorder adds a state
  stamped at the start of the window carrying whatever value was in force
  then. That is a real value at an invented time, and stored as a reading it
  would say the sensor reported at a moment it did not. History is imported as
  it was recorded or not at all.

Long-term statistics (the hourly mean/min/max Home Assistant keeps after the
recorder purges its states) are deliberately not read here: they are not
readings, and Tag Historian stores readings. The README says so.
"""

from __future__ import annotations

from datetime import datetime

from homeassistant.core import HomeAssistant, State

RECORDER_DOMAIN = "recorder"


def recorder_ready(hass: HomeAssistant) -> bool:
    """Whether the recorder is set up and can be asked anything."""
    return RECORDER_DOMAIN in hass.config.components


def recorder_keep_days(hass: HomeAssistant) -> int | None:
    """How many days of states the recorder keeps (purge_keep_days), if known."""
    if not recorder_ready(hass):
        return None
    from homeassistant.components.recorder import get_instance

    try:
        return int(get_instance(hass).keep_days)
    except (KeyError, AttributeError, TypeError, ValueError):
        return None


async def async_recorded_states(
    hass: HomeAssistant, entity_id: str, start: datetime, end: datetime
) -> list[State]:
    """Every recorded state change of one entity strictly inside (start, end), oldest first."""
    if not recorder_ready(hass) or end <= start:
        return []

    from homeassistant.components.recorder import get_instance, history

    changes = await get_instance(hass).async_add_executor_job(
        lambda: history.state_changes_during_period(
            hass,
            start,
            end,
            entity_id=entity_id,
            no_attributes=False,
            descending=False,
            include_start_time_state=False,
        )
    )
    return [
        state
        for state in changes.get(entity_id, [])
        if start < state.last_updated < end
    ]
