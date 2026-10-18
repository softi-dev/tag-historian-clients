"""Read what Home Assistant's recorder kept, for the import and the backfill.

One module, so that both callers read the recorder the same way, and one
function that queries it (:func:`async_recorded_states`) for a test to stand
in for. Two choices in it are load-bearing:

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

And one entity is not always one id. Renaming an entity moves its recorded
history to the new id - unless the new id has a recorded history of its own,
and then Home Assistant leaves both where they are (it logs "Cannot migrate
history ... because the new entity_id is already in use"). That is the
replaced meter whose successor was given the old meter's id, the case
renames.py merges: under the new id the recorder then holds the OLD meter's
readings, and the new one's are under the id it had before.
:func:`async_source` says where an entity's history is, for both callers.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

from homeassistant.core import HomeAssistant, State
from homeassistant.helpers import entity_registry as er

RECORDER_DOMAIN = "recorder"


@dataclass(frozen=True)
class Source:
    """Where one entity's recorded history is.

    Under ``entity_id``; or, for an entity renamed onto an id that had a
    recorded history of its own, under ``former_id`` up to ``until`` - the
    last row recorded there, which is the moment the entity left that id:
    the recorder writes an empty state for it then - and under ``entity_id``
    only after it.
    """

    entity_id: str
    former_id: str | None = None
    until: datetime | None = None


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


async def async_source(hass: HomeAssistant, entity_id: str, former_id: str | None) -> Source:
    """Where the history of ``entity_id`` is, given the id it had before a rename, if any.

    ``former_id`` is the id whose tag the entity's readings still go to
    (renames.py ``send_as``), known for as long as the rename waits to be
    settled. When no state is recorded under it any more, Home Assistant
    moved the history to the new id, and it is read there as usual - as it is
    when something else has the old id now, whose states those would be.
    """
    if former_id is None or former_id == entity_id or _in_use(hass, former_id):
        return Source(entity_id)
    until = await async_last_recorded(hass, former_id)
    if until is None:
        return Source(entity_id)
    return Source(entity_id, former_id, until)


def _in_use(hass: HomeAssistant, entity_id: str) -> bool:
    """Whether some entity has this id now - in the registry, or with a state of its own."""
    return (
        er.async_get(hass).async_get(entity_id) is not None
        or hass.states.get(entity_id) is not None
    )


async def async_states(
    hass: HomeAssistant, source: Source, start: datetime, end: datetime
) -> list[State]:
    """Every recorded state change of one entity strictly inside (start, end), oldest first.

    For an entity renamed onto an id with a history of its own, the states up
    to the rename come from its former id, and nothing the new id recorded
    before then is read: that is the other entity's.
    """
    if source.former_id is None or source.until is None:
        return await async_recorded_states(hass, source.entity_id, start, end)
    # The microsecond keeps the former id's last state inside the first read;
    # the second starts after it.
    before = await async_recorded_states(
        hass, source.former_id, start, min(end, source.until + timedelta(microseconds=1))
    )
    after = await async_recorded_states(hass, source.entity_id, max(start, source.until), end)
    return before + after


async def async_recorded_states(
    hass: HomeAssistant, entity_id: str, start: datetime, end: datetime
) -> list[State]:
    """Every recorded state change of one entity id strictly inside (start, end), oldest first."""
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


async def async_last_recorded(hass: HomeAssistant, entity_id: str) -> datetime | None:
    """When the newest state recorded under this id was, if any is."""
    if not recorder_ready(hass):
        return None

    from homeassistant.components.recorder import get_instance, history

    changes = await get_instance(hass).async_add_executor_job(
        history.get_last_state_changes, hass, 1, entity_id
    )
    states = changes.get(entity_id.lower()) or []
    return states[-1].last_updated if states else None
