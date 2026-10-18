from __future__ import annotations

import io

import pytest

from taghistorian_influx_migrate.timeutil import NS_PER_SECOND, parse_time_ns

from .fakes import FakeInflux, FakeTagHistorian, Point

T0 = parse_time_ns("2026-10-01T00:00:00Z")
MINUTE = 60 * NS_PER_SECOND


class Clock:
    """A settable "now", shared by the copy and the fake server's notion of today."""

    def __init__(self, now: str = "2026-10-17T12:00:00Z"):
        self.ns = parse_time_ns(now)

    def __call__(self) -> int:
        return self.ns

    def advance(self, seconds: float) -> None:
        self.ns += int(seconds * NS_PER_SECOND)


def home_assistant_history() -> list[Point]:
    """The shapes Home Assistant's influxdb integration actually writes.

    With its defaults the measurement is the unit, the tags are domain and the
    object id, the state is ``value`` and every attribute is a field - a text
    attribute as ``{name}_str``, and no ``unit_of_measurement_str`` at all,
    because the measurement already says it.
    """
    points: list[Point] = []

    def add(measurement, tags, fields, minute):
        points.append(Point(measurement, dict(tags), dict(fields), T0 + minute * MINUTE + 123))

    # A temperature, every ten minutes for three days.
    for i in range(0, 3 * 24 * 6):
        add(
            "°C",
            {"domain": "sensor", "entity_id": "living_room_temperature"},
            {
                "value": 20.0 + (i % 17) / 10,
                "friendly_name_str": "Living room temperature",
                "device_class_str": "temperature",
                "state_class_str": "measurement",
            },
            i * 10,
        )
    # The same unit, another HA instance tagged in (a static "tags:" block).
    for i in range(0, 40):
        add(
            "°C",
            {"domain": "sensor", "entity_id": "outdoor_temperature", "instance": "cabin"},
            {"value": 5.0 + i / 10, "friendly_name_str": "Outdoor temperature"},
            i * 60,
        )
    # An entity without a unit: the measurement is the entity id.
    for i in range(0, 30):
        add(
            "binary_sensor.front_door",
            {"domain": "binary_sensor", "entity_id": "front_door"},
            {
                "value": float(i % 2),
                "state": "on" if i % 2 else "off",
                "friendly_name_str": "Front door",
            },
            i * 97,
        )
    # A unit that changed: Wh until the second day, kWh after. One entity, one tag.
    for i in range(0, 48):
        unit = "Wh" if i < 24 else "kWh"
        add(
            unit,
            {"domain": "sensor", "entity_id": "energy_total"},
            {
                "value": 1000.0 + i if unit == "Wh" else 1.0 + i / 1000,
                "friendly_name_str": "Energy total",
            },
            i * 60,
        )
    # measurement_attr: entity_id - the unit then arrives as a field.
    for i in range(0, 12):
        add(
            "sensor.water_used",
            {"domain": "sensor", "entity_id": "water_used"},
            {
                "value": float(i * 3),
                "unit_of_measurement_str": "L",
                "friendly_name_str": "Water used",
            },
            i * 120,
        )
    # A word state: no value field at all, only a string state and attributes.
    for i in range(0, 10):
        add(
            "climate.living_room",
            {"domain": "climate", "entity_id": "living_room"},
            {
                "state": "heat",
                "current_temperature": 21.0,
                "temperature": 22.0,
                "friendly_name_str": "Living room",
            },
            i * 30,
        )
    # Not Home Assistant at all - Telegraf, in the same database.
    for i in range(0, 10):
        add("cpu", {"host": "nas"}, {"usage_idle": 90.0 + i}, i * 10)
    return points


@pytest.fixture
def clock() -> Clock:
    return Clock()


@pytest.fixture
def history() -> list[Point]:
    return home_assistant_history()


@pytest.fixture
def influx(history):
    fake = FakeInflux(history, username="ha", password="secret")
    yield fake
    fake.close()


@pytest.fixture
def influx2(history):
    fake = FakeInflux(history, token="influx-token")
    yield fake
    fake.close()


@pytest.fixture
def tag_historian(clock):
    fake = FakeTagHistorian(clock=clock)
    yield fake
    fake.close()


@pytest.fixture
def out() -> io.StringIO:
    return io.StringIO()
