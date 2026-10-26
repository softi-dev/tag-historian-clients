"""Reading Home Assistant's long-term statistics, against a real recorder.

The statistics phase is tested with a stand-in for the recorder
(tests/test_statistics_import.py); this is where the stand-in is held to the
real thing: statistics seeded with Home Assistant's own
``async_import_statistics``, read back through ``get_metadata`` and
``statistics_during_period`` - on the pinned Home Assistant and on the
newest one CI runs. And the metadata as each Home Assistant spells it:
``mean_type``, or ``has_mean`` before it existed, which Home Assistant
2026.11 removes.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from homeassistant.components.recorder.models import StatisticMeanType, StatisticMetaData
from homeassistant.components.recorder.statistics import async_import_statistics
from homeassistant.core import HomeAssistant
from homeassistant.util import dt as dt_util
from pytest_homeassistant_custom_component.components.recorder.common import (
    async_wait_recording_done,
)

from custom_components.tag_historian import history, statistics
from custom_components.tag_historian.statistics import (
    StatisticInfo,
    info_from_metadata,
    plan_units,
)


@pytest.fixture(autouse=True)
def auto_enable_custom_integrations(recorder_db_url, enable_custom_integrations):
    """The recorder's database first, as tests/test_history.py explains."""
    yield


def _metadata(statistic_id: str, unit: str, *, has_sum: bool) -> StatisticMetaData:
    """Metadata as this Home Assistant takes it: unit_class only where it has one."""
    meta: dict = {
        "statistic_id": statistic_id,
        "source": "recorder",
        "name": None,
        "has_sum": has_sum,
        "mean_type": StatisticMeanType.NONE if has_sum else StatisticMeanType.ARITHMETIC,
        "unit_of_measurement": unit,
    }
    if "unit_class" in StatisticMetaData.__annotations__:
        meta["unit_class"] = "energy" if unit in ("kWh", "Wh") else None
    return meta  # type: ignore[return-value]


def _hours_back(count: int) -> list[datetime]:
    top = dt_util.utcnow().replace(minute=0, second=0, microsecond=0)
    return [top - timedelta(hours=count - i) for i in range(count)]


# --------------------------------------------------------------------------
# The metadata, as each Home Assistant spells it
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("meta", "mean_type"),
    [
        ({"mean_type": StatisticMeanType.ARITHMETIC}, "arithmetic"),
        ({"mean_type": StatisticMeanType.CIRCULAR}, "circular"),
        ({"mean_type": StatisticMeanType.NONE, "has_sum": True}, "none"),
        ({"mean_type": 1}, "arithmetic"),
        # Before mean_type, and with both: mean_type wins.
        ({"has_mean": True}, "arithmetic"),
        ({"has_mean": False, "has_sum": True}, "none"),
        ({"has_mean": True, "mean_type": StatisticMeanType.CIRCULAR}, "circular"),
        # Neither, as a Home Assistant after 2026.11 may leave a meter's.
        ({"has_sum": True}, "none"),
    ],
)
def test_the_kind_of_mean_is_mean_type_when_present_and_has_mean_when_not(meta, mean_type) -> None:
    info = info_from_metadata({"statistic_id": "sensor.x", "source": "recorder", **meta})
    assert info.mean_type == mean_type


def test_a_meter_is_read_as_a_sum_an_average_as_a_mean_and_an_angle_not_at_all() -> None:
    assert StatisticInfo("sensor.e", "recorder", True, "none", "kWh").kind == statistics.KIND_SUM
    assert StatisticInfo("sensor.t", "recorder", False, "arithmetic", "°C").kind == statistics.KIND_MEAN
    assert StatisticInfo("sensor.w", "recorder", False, "circular", "°").kind is None


def test_the_values_go_in_the_unit_the_entity_reads_in_when_home_assistant_can_convert_them() -> None:
    energy = StatisticInfo("sensor.e", "recorder", True, "none", "kWh")

    assert plan_units(energy, "kWh") == ("kWh", None, "")
    assert plan_units(energy, None) == ("kWh", None, ""), "an entity without a unit: as stored"
    units, convert, reason = plan_units(energy, "Wh")
    assert (units, reason) == ("Wh", "")
    assert convert == {"energy": "Wh"}

    units, convert, reason = plan_units(energy, "°C")
    assert units is None and convert is None
    assert reason == (
        "its long-term statistics are in kWh and it reads in °C now, which Home Assistant cannot convert"
    )


# --------------------------------------------------------------------------
# Against the recorder
# --------------------------------------------------------------------------


async def test_a_measurements_hours_are_its_mean_minimum_and_maximum(
    recorder_mock, hass: HomeAssistant
) -> None:
    hours = _hours_back(3)
    async_import_statistics(
        hass,
        _metadata("sensor.outdoor_temperature", "°C", has_sum=False),
        [{"start": hour, "mean": 5.0 + i, "min": 4.0 + i, "max": 6.0 + i} for i, hour in enumerate(hours)],
    )
    hass.states.async_set("sensor.outdoor_temperature", "7", {"unit_of_measurement": "°C", "state_class": "measurement"})
    await async_wait_recording_done(hass)

    plan = await statistics.async_plan(hass, history.Source("sensor.outdoor_temperature"))
    rows = await statistics.async_rows(hass, plan, hours[0], hours[-1] + timedelta(hours=1))

    assert plan.info is not None and plan.info.mean_type == "arithmetic"
    assert plan.state_class == "measurement"
    assert rows == [
        {"t": int(hour.timestamp()), "mean": 5.0 + i, "min": 4.0 + i, "max": 6.0 + i}
        for i, hour in enumerate(hours)
    ]


async def test_a_meters_hours_are_its_reading_sum_and_change(recorder_mock, hass: HomeAssistant) -> None:
    hours = _hours_back(3)
    async_import_statistics(
        hass,
        _metadata("sensor.energy_meter", "kWh", has_sum=True),
        [{"start": hour, "state": 100.0 + i, "sum": 10.0 + i} for i, hour in enumerate(hours)],
    )
    hass.states.async_set("sensor.energy_meter", "102", {"unit_of_measurement": "kWh"})
    await async_wait_recording_done(hass)

    plan = await statistics.async_plan(hass, history.Source("sensor.energy_meter"))
    rows = await statistics.async_rows(hass, plan, hours[0], hours[-1] + timedelta(hours=1))

    assert plan.info is not None and plan.info.has_sum
    assert [row["state"] for row in rows] == [100.0, 101.0, 102.0]
    assert [row["change"] for row in rows][1:] == [1.0, 1.0], "Home Assistant's own increase"


async def test_a_meter_read_in_another_unit_of_energy_is_converted_by_home_assistant(
    recorder_mock, hass: HomeAssistant
) -> None:
    hours = _hours_back(2)
    async_import_statistics(
        hass,
        _metadata("sensor.energy_meter", "kWh", has_sum=True),
        [{"start": hour, "state": 1.5 + i, "sum": 1.5 + i} for i, hour in enumerate(hours)],
    )
    hass.states.async_set("sensor.energy_meter", "2500", {"unit_of_measurement": "Wh"})
    await async_wait_recording_done(hass)

    plan = await statistics.async_plan(hass, history.Source("sensor.energy_meter"))
    rows = await statistics.async_rows(hass, plan, hours[0], hours[-1] + timedelta(hours=1))

    assert plan.units == "Wh"
    assert [row["state"] for row in rows] == [1500.0, 2500.0]


async def test_an_entity_without_statistics_is_said_to_have_none(recorder_mock, hass: HomeAssistant) -> None:
    hass.states.async_set("binary_sensor.front_door", "off")
    await async_wait_recording_done(hass)

    plan = await statistics.async_plan(hass, history.Source("binary_sensor.front_door"))

    assert plan.missing


async def test_a_renamed_entitys_hours_come_from_its_former_id_up_to_the_rename(
    recorder_mock, hass: HomeAssistant
) -> None:
    """Renamed onto an id with statistics of its own, its own are under its former id until it moved."""
    hours = _hours_back(4)
    moved = hours[2]
    for statistic_id, values in (("sensor.energy_2", 1.0), ("sensor.energy", 50.0)):
        async_import_statistics(
            hass,
            _metadata(statistic_id, "kWh", has_sum=True),
            [{"start": hour, "state": values + i, "sum": values + i} for i, hour in enumerate(hours)],
        )
    hass.states.async_set("sensor.energy", "60", {"unit_of_measurement": "kWh"})
    await async_wait_recording_done(hass)

    plan = await statistics.async_plan(hass, history.Source("sensor.energy", "sensor.energy_2", moved))
    rows = await statistics.async_rows(hass, plan, hours[0], hours[-1] + timedelta(hours=1))

    assert [row["state"] for row in rows] == [1.0, 2.0, 52.0, 53.0], (
        "the former id's hours before the rename, the entity's own after it"
    )


async def test_the_month_helpers_walk_utc_months() -> None:
    march = datetime(2026, 3, 1, tzinfo=UTC)
    assert statistics.previous_month(march) == datetime(2026, 2, 1, tzinfo=UTC)
    assert statistics.next_month(datetime(2026, 1, 1, tzinfo=UTC)) == datetime(2026, 2, 1, tzinfo=UTC)
    assert statistics.month_start(datetime(2026, 12, 31, 23, 59, tzinfo=UTC)) == datetime(2026, 12, 1, tzinfo=UTC)
