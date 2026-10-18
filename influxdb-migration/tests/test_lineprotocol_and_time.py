from __future__ import annotations

import pytest

from taghistorian_influx_migrate.lineprotocol import encode_line, number_field, string_field
from taghistorian_influx_migrate.timeutil import (
    format_human,
    format_time_ns,
    parse_time_ns,
    seconds_until_next_utc_midnight,
    utc_day,
)

from .fakes import parse_line


def test_a_home_assistant_line_has_the_shape_home_assistant_writes():
    line = encode_line(
        "°C",
        {"entity_id": "living_room", "domain": "sensor"},
        [("value", 21.5), ("friendly_name_str", "Living room"), ("unit_of_measurement_str", "°C")],
        1_700_000_000_123_456_789,
    )
    assert line == (
        '°C,domain=sensor,entity_id=living_room value=21.5,friendly_name_str="Living room",'
        'unit_of_measurement_str="°C" 1700000000123456789'
    )


def test_everything_line_protocol_reserves_is_escaped_and_survives_a_parse():
    line = encode_line(
        "my unit, odd=yes",
        {"domain": "sensor", "entity_id": "a b,c=d", "instance": "x\\y"},
        [("value", 1.0), ("friendly_name_str", 'Say "hi" \\ bye')],
        5,
    )
    measurement, tags, fields, timestamp = parse_line(line)
    assert measurement == "my unit, odd=yes"
    assert tags == {"domain": "sensor", "entity_id": "a b,c=d", "instance": "x\\y"}
    assert fields == {"value": 1.0, "friendly_name_str": 'Say "hi" \\ bye'}
    assert timestamp == 5


@pytest.mark.parametrize(
    ("value", "text"),
    [(21.5, "21.5"), (21.0, "21.0"), (1e-05, "1e-05"), (5, "5i"), (True, "true"), (False, "false")],
)
def test_a_reading_keeps_its_type(value, text):
    assert number_field(value) == text


def test_quotes_and_backslashes_are_the_only_escapes_in_a_string_field():
    assert string_field('a "b" \\ c, d=e') == '"a \\"b\\" \\\\ c, d=e"'


def test_an_empty_tag_value_is_left_off_the_line():
    assert encode_line(
        "m", {"domain": "sensor", "entity_id": "x", "extra": ""}, [("value", 1.0)], 1
    ) == ("m,domain=sensor,entity_id=x value=1.0 1")


def test_a_line_without_fields_is_refused():
    with pytest.raises(ValueError):
        encode_line("m", {}, [], 1)


# --- time ------------------------------------------------------------------


@pytest.mark.parametrize(
    ("text", "ns"),
    [
        ("1970-01-01", 0),
        ("2024-01-31", 1706659200 * 10**9),
        ("2024-01-31T12:00:00Z", 1706702400 * 10**9),
        ("2024-01-31T12:00Z", 1706702400 * 10**9),
        ("2024-01-31T14:00:00+02:00", 1706702400 * 10**9),
        ("2024-01-31T12:00:00.000000001Z", 1706702400 * 10**9 + 1),
        ("2024-01-31T12:00:00.1234567Z", 1706702400 * 10**9 + 123456700),
    ],
)
def test_timestamps_parse_to_exact_nanoseconds(text, ns):
    assert parse_time_ns(text) == ns


def test_a_time_without_an_offset_is_utc_not_the_local_zone():
    assert parse_time_ns("2024-01-31T12:00:00") == parse_time_ns("2024-01-31T12:00:00Z")


def test_formatting_round_trips_every_nanosecond():
    for ns in (0, 1, 999_999_999, 1_700_000_000_123_456_789, 1_700_000_000_100_000_000):
        assert parse_time_ns(format_time_ns(ns)) == ns
    assert format_time_ns(1_700_000_000_100_000_000) == "2023-11-14T22:13:20.1Z"


def test_garbage_is_refused_with_an_example():
    with pytest.raises(ValueError, match="2024-01-31"):
        parse_time_ns("last tuesday")


def test_the_day_helpers_count_utc_days():
    ns = parse_time_ns("2026-10-17T23:00:00Z")
    assert utc_day(ns) == "2026-10-17"
    assert seconds_until_next_utc_midnight(ns) == 3600
    assert format_human(ns) == "2026-10-17 23:00 UTC"
