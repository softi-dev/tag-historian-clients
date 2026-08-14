"""Quota and delivery sensors.

Five numbers, and not one of them is invented here: the two quota sensors read
``GET /api/usage/limits`` and ``GET /api/usage/daily`` through the coordinator,
and the delivery sensors read the forwarder's own counters.

The delivery sensors exist so that "every reading that does not arrive moves a
counter" is true in Home Assistant's own idiom, the same invariant the
Sparkplug collector's health line has. A drop nobody can see is
indistinguishable from a sensor that stopped reporting.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from homeassistant.components.sensor import (
    SensorEntity,
    SensorEntityDescription,
    SensorStateClass,
)
from homeassistant.const import EntityCategory
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .api import QuotaSnapshot
from .const import DOMAIN
from .coordinator import QuotaCoordinator
from .forwarder import ForwarderStats

if TYPE_CHECKING:
    from . import TagHistorianConfigEntry


@dataclass(frozen=True, kw_only=True)
class QuotaSensorDescription(SensorEntityDescription):
    """A sensor whose value is read straight out of the API snapshot."""

    value_fn: Callable[[QuotaSnapshot], int]
    attributes_fn: Callable[[QuotaSnapshot], dict[str, Any]]


@dataclass(frozen=True, kw_only=True)
class DeliverySensorDescription(SensorEntityDescription):
    """A sensor whose value is a local counter."""

    value_fn: Callable[[ForwarderStats], int]


QUOTA_SENSORS: tuple[QuotaSensorDescription, ...] = (
    QuotaSensorDescription(
        key="tags_used",
        name="Tag Historian tags used",
        state_class=SensorStateClass.MEASUREMENT,
        value_fn=lambda q: q.current_tag_count,
        attributes_fn=lambda q: {
            "tag_limit": q.tag_limit,
            "tags_available": q.tags_available,
        },
    ),
    QuotaSensorDescription(
        key="readings_today",
        name="Tag Historian readings today",
        state_class=SensorStateClass.TOTAL_INCREASING,
        value_fn=lambda q: q.measurements_today,
        attributes_fn=lambda q: {
            "readings_per_day_limit": q.measurements_per_day_limit,
        },
    ),
)

DELIVERY_SENSORS: tuple[DeliverySensorDescription, ...] = (
    DeliverySensorDescription(
        key="readings_sent",
        name="Tag Historian readings sent",
        entity_category=EntityCategory.DIAGNOSTIC,
        state_class=SensorStateClass.TOTAL_INCREASING,
        value_fn=lambda s: s.sent,
    ),
    DeliverySensorDescription(
        key="readings_queued",
        name="Tag Historian readings queued",
        entity_category=EntityCategory.DIAGNOSTIC,
        state_class=SensorStateClass.MEASUREMENT,
        value_fn=lambda s: s.queued,
    ),
    DeliverySensorDescription(
        key="readings_dropped",
        name="Tag Historian readings dropped",
        entity_category=EntityCategory.DIAGNOSTIC,
        state_class=SensorStateClass.TOTAL_INCREASING,
        # Every way a reading the user asked for can fail to become history,
        # added up: from the outside they are one question, "how much did not
        # arrive". The breakdown is on the attributes for anyone who needs to
        # tell them apart. Throttled readings are deliberately NOT in here -
        # the user configured those away on purpose - and they are on the
        # attributes so nobody has to guess.
        value_fn=lambda s: (
            s.dropped_quota
            + s.dropped_overflow
            + s.dropped_shutdown
            + s.dropped_inflight
            + s.dropped_not_numeric
        ),
    ),
)


async def async_setup_entry(
    hass: HomeAssistant,
    entry: TagHistorianConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up the sensors for one account."""
    data = entry.runtime_data
    entities: list[SensorEntity] = [
        QuotaSensor(data.coordinator, entry, description)
        for description in QUOTA_SENSORS
    ]
    entities += [
        DeliverySensor(entry, description) for description in DELIVERY_SENSORS
    ]
    async_add_entities(entities)


class QuotaSensor(CoordinatorEntity[QuotaCoordinator], SensorEntity):
    """A number the API reported."""

    entity_description: QuotaSensorDescription
    _attr_should_poll = False

    def __init__(
        self,
        coordinator: QuotaCoordinator,
        entry: TagHistorianConfigEntry,
        description: QuotaSensorDescription,
    ) -> None:
        super().__init__(coordinator)
        self.entity_description = description
        self._attr_unique_id = f"{entry.entry_id}_{description.key}"

    @property
    def native_value(self) -> int | None:
        if self.coordinator.data is None:
            return None
        return self.entity_description.value_fn(self.coordinator.data)

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        if self.coordinator.data is None:
            return {}
        return self.entity_description.attributes_fn(self.coordinator.data)


class DeliverySensor(SensorEntity):
    """A counter this integration keeps itself."""

    entity_description: DeliverySensorDescription
    # Polled, and cheaply: the counters live in memory in the same process, and
    # pushing an update per forwarded reading would put a state write on the
    # hot path of the thing being measured.
    _attr_should_poll = True

    def __init__(
        self,
        entry: TagHistorianConfigEntry,
        description: DeliverySensorDescription,
    ) -> None:
        self.entity_description = description
        self._entry = entry
        self._attr_unique_id = f"{entry.entry_id}_{description.key}"

    @property
    def native_value(self) -> int:
        return self.entity_description.value_fn(self._entry.runtime_data.forwarder.stats)

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        stats = self._entry.runtime_data.forwarder.stats
        return {
            "dropped_over_quota": stats.dropped_quota,
            "dropped_queue_overflow": stats.dropped_overflow,
            "dropped_rate_limited": stats.dropped_throttled,
            "dropped_on_shutdown": stats.dropped_shutdown,
            "dropped_mid_write": stats.dropped_inflight,
            "dropped_not_a_number": stats.dropped_not_numeric,
            # Naming them is the point: "3 readings could not be stored" sends
            # somebody hunting, "sensor.house_mode could not be stored" is a
            # decision they can make in ten seconds.
            #
            # COPIED, both of them. Home Assistant snapshots a state and keeps
            # it until the next update; handing it the forwarder's own list
            # hands it a list that goes on changing afterwards. What that
            # looked like was a published state carrying a populated
            # entities_storing_nothing beside dropped_not_a_number=0 - the
            # names had arrived after the number was written, in a state
            # object that is supposed to be one consistent moment.
            "entities_storing_nothing": list(stats.unstorable_entities),
            "refused_tags": list(stats.refused_tags),
            "last_error": stats.last_error,
        }


# Referenced by the entity ids the platform generates; kept here so a rename
# has to pass through one place.
PLATFORM_DOMAIN = DOMAIN
