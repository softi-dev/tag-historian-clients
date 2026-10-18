"""The mirror of the server's naming, pinned against the server's own examples.

Every expectation below is quoted from the product - InfluxWriteController's
doc comments, docs/LINE-PROTOCOL.md's table and InfluxWriteApiTests - not
derived from this package. A dry run that names a tag differently from the
server is a dry run that counts the wrong tags against somebody's plan.
"""

from __future__ import annotations

import hashlib

import pytest

from taghistorian_influx_migrate.naming import (
    TAG_NAME_MAX_LENGTH,
    Row,
    derive_base_name,
    describe,
    entity_of,
    measurement_reads_as_unit,
    rows_for_point,
    sanitize_tag_name,
    usable_text,
)


def tag_names(measurement: str, tags: dict, fields: dict) -> dict[str, object]:
    base = derive_base_name(measurement, tags)
    return {row.tag_name: row.value for row in rows_for_point(base, fields)}


# --- docs/LINE-PROTOCOL.md, "How lines become tags" --------------------------


@pytest.mark.parametrize(
    ("measurement", "tags", "fields", "expected"),
    [
        (
            "state",
            {"domain": "sensor", "entity_id": "temp"},
            {"value": 21.5},
            {"sensor.temp": 21.5},
        ),
        ("cpu", {}, {"usage": 0.93}, {"cpu.usage": 0.93}),
        (
            "meter",
            {},
            {"power": 1500.0, "voltage": 230.0},
            {"meter.power": 1500.0, "meter.voltage": 230.0},
        ),
        ("meter", {}, {"value": 7.0, "power": 1500.0}, {"meter": 7.0}),
        ("status", {}, {"text": "ok"}, {}),
    ],
)
def test_the_documented_table(measurement, tags, fields, expected):
    assert tag_names(measurement, tags, fields) == expected


def test_a_home_assistant_series_is_named_after_the_entity_not_the_unit():
    assert tag_names(
        "°C", {"domain": "sensor", "entity_id": "outdoor_temperature"}, {"value": 4.0}
    ) == {"sensor.outdoor_temperature": 4.0}


def test_other_tags_on_a_home_assistant_series_are_appended_sorted_by_key():
    """Two HA instances writing one entity stay two tags (the controller's comment)."""
    tags = {
        "entity_id": "outdoor_temperature",
        "zone": "garden",
        "domain": "sensor",
        "instance": "home2",
    }
    assert derive_base_name("°C", tags) == "sensor.outdoor_temperature.instance-home2.zone-garden"


def test_telegraf_series_spell_their_tags_into_the_name():
    assert derive_base_name("cpu", {"host": "srv01", "cpu": "cpu0"}) == "cpu.cpu-cpu0.host-srv01"
    assert sanitize_tag_name(derive_base_name("disk", {"path": "/"})) == "disk.path-_"


def test_a_climate_entity_at_heat_has_no_value_and_becomes_its_attributes():
    """What live forwarding does with a word state - documented on the HA page."""
    names = tag_names(
        "climate.living_room",
        {"domain": "climate", "entity_id": "living_room"},
        {"state": "heat", "current_temperature": 21.0, "temperature": 22.0},
    )
    assert names == {
        "climate.living_room.current_temperature": 21.0,
        "climate.living_room.temperature": 22.0,
    }


def test_booleans_are_readings_and_strings_never_are():
    rows = rows_for_point("m", {"on": True, "label": "x", "nan": float("nan")})
    assert rows == [Row("m.on", "on", True)]


# --- SanitizeTagName ---------------------------------------------------------


@pytest.mark.parametrize(
    ("raw", "expected"),
    [("°C", "t_C"), ("disk.path-/", "disk.path-_"), ("1sensor", "t1sensor"), ("", "t")],
)
def test_sanitize_matches_the_documented_rules(raw, expected):
    assert sanitize_tag_name(raw) == expected


def test_a_long_name_is_cut_with_the_servers_stable_hash():
    """InfluxWriteApiTests.SeriesNameOver200Characters_TruncatesWithStableHash."""
    raw = "sensor." + "x" * 260
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest().upper()[:8]
    assert sanitize_tag_name(raw) == raw[:191] + "-" + digest
    assert len(sanitize_tag_name(raw)) == TAG_NAME_MAX_LENGTH


def test_two_long_names_sharing_a_prefix_stay_distinct():
    assert sanitize_tag_name("sensor." + "y" * 260 + "_a") != sanitize_tag_name(
        "sensor." + "y" * 260 + "_b"
    )


# --- HomeAssistantAttributes -------------------------------------------------


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("  Living room  ", "Living room"),
        ("", None),
        ("   ", None),
        ("a\nb", None),
        ("a\u2028b", None),
        ("x" * 500, "x" * 500),
        ("x" * 501, None),
        (21.5, None),
    ],
)
def test_a_name_is_used_whole_or_not_at_all(value, expected):
    assert usable_text(value, 500) == expected


def test_only_the_home_assistant_shape_describes_a_tag():
    tags = {"domain": "sensor", "entity_id": "t"}
    fields = {"value": 1.0, "friendly_name_str": "T", "unit_of_measurement_str": "°C"}
    assert describe("x", tags, fields, unit_from_measurement=True).unit == "°C"
    assert describe("x", {}, fields, unit_from_measurement=True).friendly_name is None
    no_value = {"state": "heat", "friendly_name_str": "T"}
    assert describe("x", tags, no_value, unit_from_measurement=True).friendly_name is None


# --- the one rule that is the migration's own: the measurement as the unit ----

HA = {"domain": "sensor", "entity_id": "living_room_temperature"}


@pytest.mark.parametrize(
    ("measurement", "tags", "reads_as_unit"),
    [
        ("°C", HA, True),
        ("kWh", HA, True),
        ("%", HA, True),
        # measurement_attr: entity_id, or an entity with no unit at all.
        ("sensor.living_room_temperature", HA, False),
        # measurement_attr: domain__device_class.
        ("sensor__temperature", HA, False),
        ("sensor", HA, False),
        ("", HA, False),
    ],
)
def test_the_measurement_reads_as_a_unit_only_in_home_assistants_default_shape(
    measurement, tags, reads_as_unit
):
    assert measurement_reads_as_unit(measurement, tags) is reads_as_unit


def test_a_fixed_measurement_name_is_declared_not_guessed():
    assert measurement_reads_as_unit("state", HA) is True
    assert measurement_reads_as_unit("state", HA, frozenset({"state"})) is False


def test_the_unit_field_wins_and_an_unusable_one_is_not_replaced_by_a_guess():
    with_field = {"value": 3.0, "unit_of_measurement_str": "L"}
    assert describe("sensor.water", HA, with_field, unit_from_measurement=True).unit == "L"
    # Home Assistant only writes the field when the measurement is NOT the
    # unit - so a field too long to keep still means the measurement is not it.
    too_long = {"value": 3.0, "unit_of_measurement_str": "x" * 51}
    assert describe("°C", HA, too_long, unit_from_measurement=True).unit is None


def test_the_measurement_is_never_the_unit_when_switched_off():
    assert describe("°C", HA, {"value": 1.0}, unit_from_measurement=False).unit is None
    assert describe("°C", HA, {"value": 1.0}, unit_from_measurement=True).unit == "°C"


def test_the_entity_a_user_knows_is_domain_and_object_id():
    assert entity_of(HA) == "sensor.living_room_temperature"
    assert entity_of({"host": "nas"}) is None
