"""Both InfluxDB readers against the fake, which answers with InfluxDB's semantics."""

from __future__ import annotations

import pytest

from taghistorian_influx_migrate.influx import (
    InfluxError,
    InfluxV1Source,
    InfluxV2Source,
    PartialResult,
    SeriesKey,
    filter_series,
    flux_string,
    influxql_string,
    parse_annotated_csv,
)

from .conftest import MINUTE, T0
from .fakes import FakeInflux, Point

END = T0 + 10 * 24 * 60 * MINUTE

LIVING_ROOM = SeriesKey.of("°C", {"domain": "sensor", "entity_id": "living_room_temperature"})
CABIN = SeriesKey.of(
    "°C", {"domain": "sensor", "entity_id": "outdoor_temperature", "instance": "cabin"}
)


@pytest.fixture(params=["v1", "v2"])
def source(request, influx, influx2):
    if request.param == "v1":
        reader = InfluxV1Source(influx.url, "homeassistant", username="ha", password="secret")
    else:
        reader = InfluxV2Source(influx2.url, "home", "homeassistant", "influx-token")
    yield reader
    reader.close()


def test_discovery_lists_home_assistant_series_with_their_readings(source):
    found = {s.key: s for s in source.discover(T0, END)}

    entities = sorted(f"{k.tag_dict['domain']}.{k.tag_dict['entity_id']}" for k in found)
    assert entities == [
        "binary_sensor.front_door",
        "sensor.energy_total",
        "sensor.energy_total",
        "sensor.living_room_temperature",
        "sensor.outdoor_temperature",
        "sensor.water_used",
    ], "the climate entity has no numeric state and the Telegraf series is not Home Assistant's"

    assert found[LIVING_ROOM].readings == 3 * 24 * 6
    assert found[LIVING_ROOM].friendly_name == "Living room temperature"
    assert found[LIVING_ROOM].unit_field is None
    assert found[CABIN].readings == 40
    water = SeriesKey.of("sensor.water_used", {"domain": "sensor", "entity_id": "water_used"})
    assert found[water].unit_field == "L"


def test_a_series_without_a_tag_is_not_given_one(influx):
    """1.x reports every tag key of a measurement on every series, '' where absent."""
    reader = InfluxV1Source(influx.url, "homeassistant", username="ha", password="secret")
    keys = [s.key for s in reader.discover(T0, END)]
    assert LIVING_ROOM in keys
    assert all(dict(k.tags).get("instance") != "" for k in keys)


def test_discovery_can_be_limited_to_measurements(source):
    found = source.discover(T0, END, ["°C"])
    assert {s.key for s in found} == {LIVING_ROOM, CABIN}


def test_the_first_reading_is_found_without_walking_the_range(source):
    assert source.first_reading(LIVING_ROOM, 0, END) == T0 + 123
    assert source.first_reading(LIVING_ROOM, END, END + MINUTE) is None


def test_reading_returns_one_series_exactly_with_its_fields(source):
    points = source.read(LIVING_ROOM, T0, T0 + 30 * MINUTE)
    assert [p.timestamp_ns for p in points] == [
        T0 + 123,
        T0 + 10 * MINUTE + 123,
        T0 + 20 * MINUTE + 123,
    ]
    assert points[0].fields == {"value": 20.0, "friendly_name_str": "Living room temperature"}
    assert isinstance(points[0].fields["value"], float)


def test_a_series_with_an_extra_tag_is_not_mixed_into_its_neighbour(history, source):
    plain = SeriesKey.of("°C", {"domain": "sensor", "entity_id": "outdoor_temperature"})
    assert source.read(plain, T0, END) == []
    assert len(source.read(CABIN, T0, END)) == 40


def test_the_range_is_start_inclusive_and_end_exclusive(source):
    first = T0 + 123
    assert len(source.read(LIVING_ROOM, first, first + 1)) == 1
    assert source.read(LIVING_ROOM, first + 1, first + 10 * MINUTE) == []


def test_a_cut_short_result_is_an_error_not_less_history():
    fake = FakeInflux(
        [
            Point("°C", {"domain": "sensor", "entity_id": "t"}, {"value": float(i)}, i)
            for i in range(10)
        ],
        max_row_limit=5,
    )
    try:
        reader = InfluxV1Source(fake.url, "db")
        with pytest.raises(PartialResult):
            reader.read(SeriesKey.of("°C", {"domain": "sensor", "entity_id": "t"}), 0, 100)
    finally:
        fake.close()


def test_wrong_credentials_say_which(influx, influx2):
    with pytest.raises(InfluxError, match="username or password"):
        InfluxV1Source(influx.url, "db", username="ha", password="wrong").discover(T0, END)
    with pytest.raises(InfluxError, match="token"):
        InfluxV2Source(influx2.url, "o", "b", "wrong").discover(T0, END)


def test_quotes_in_names_cannot_break_out_of_a_query():
    assert influxql_string("it's") == "'it\\'s'"
    assert flux_string('a"b\\c${x}') == '"a\\"b\\\\c\\${x}"'


def test_awkward_entity_ids_round_trip_through_both_languages():
    tags = {"domain": "sensor", "entity_id": 'it\'s "quoted" ${x}'}
    points = [Point("°C", tags, {"value": 1.0}, 7)]
    for make in (
        lambda f: InfluxV1Source(f.url, "db"),
        lambda f: InfluxV2Source(f.url, "o", "b", "t"),
    ):
        fake = FakeInflux(points)
        try:
            reader = make(fake)
            key = SeriesKey.of("°C", tags)
            assert [s.key for s in reader.discover(0, 100)] == [key]
            assert [p.fields["value"] for p in reader.read(key, 0, 100)] == [1.0]
        finally:
            fake.close()


def test_patterns_match_the_entity_id_home_assistant_uses(source):
    found = source.discover(T0, END)
    kept = filter_series(found, ["sensor.*_temperature", "binary_sensor.*"], ["sensor.outdoor*"])
    assert {s.key for s in kept} == {
        LIVING_ROOM,
        SeriesKey.of(
            "binary_sensor.front_door", {"domain": "binary_sensor", "entity_id": "front_door"}
        ),
    }


# --- annotated CSV ---------------------------------------------------------


def test_annotated_csv_with_two_schemas_and_a_shared_header():
    text = (
        "#datatype,string,long,dateTime:RFC3339,double,string,string,string\r\n"
        "#group,false,false,false,false,true,true,true\r\n"
        "#default,_result,,,,,,\r\n"
        ",result,table,_time,_value,_field,_measurement,domain\r\n"
        ",,0,2024-01-01T00:00:00Z,1.5,value,m,sensor\r\n"
        ",,1,2024-01-01T00:00:01.5Z,2,value,m,switch\r\n"
        "\r\n"
        "#datatype,string,long,dateTime:RFC3339,string,string,string,string\r\n"
        "#group,false,false,false,false,true,true,true\r\n"
        "#default,_result,,,,,,\r\n"
        ",result,table,_time,_value,_field,_measurement,domain\r\n"
        ',,2,2024-01-01T00:00:00Z,"a, ""b""",friendly_name_str,m,sensor\r\n'
        "\r\n"
    )
    rows = parse_annotated_csv(text)
    assert [r.values["_value"] for r in rows] == [1.5, 2.0, 'a, "b"']
    assert rows[1].values["_time"] == 1704067201500000000


def test_an_error_table_raises():
    text = "#datatype,string,string\r\n#group,true,true\r\n#default,,\r\n,error,reference\r\n,boom,\r\n\r\n"
    with pytest.raises(InfluxError, match="boom"):
        parse_annotated_csv(text)
