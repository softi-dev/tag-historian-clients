"""The encoder, and its agreement with what the API actually does.

Every expectation in the tag-name tests below comes from the API side, not
from this package: the derivation and sanitisation the line-protocol endpoint
applies server-side, the behaviour written out in the public guide
(taghistorian.com/docs/influxdb-line-protocol), and cases the API's own test
suite already pins. If the two ever disagree the API wins and this file is
the thing that has to change.
"""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime

import pytest

from custom_components.tag_historian import line_protocol
from custom_components.tag_historian.line_protocol import (
    TAG_NAME_MAX_LENGTH,
    encode_batch,
    encode_point,
    is_missing_state,
    sanitize_tag_name,
    state_to_value,
    tag_name_for_entity,
)

TS = datetime(2026, 8, 12, 10, 0, 0, tzinfo=UTC)


def test_no_extra_line_protocol_tags_are_emitted():
    """Exactly ``domain`` and ``entity_id``, and never a third tag.

    This is the one that guards the customer's entire history.
    ``DeriveBaseTagName`` only yields the clean ``{domain}.{entity_id}`` while
    those two are the WHOLE tag set; any additional tag is folded into the name
    as ``.key-value``. InfluxWriteApiTests.HaShapedPointWithExtraTags_
    KeepsDistinctSeries pins that behaviour from the other side - an extra
    ``instance=home1`` there produces ``sensor.<entity>.instance-home1``. Emit
    a third tag here and every tag on the account silently renames itself.
    """
    line = encode_point("sensor.outdoor_temperature", 21.5, TS, "°C")

    tag_section = line.split(" ", 1)[0]
    measurement, *tags = tag_section.split(",")

    assert measurement == "°C"
    assert tags == ["domain=sensor", "entity_id=outdoor_temperature"]


def test_encoded_line_has_the_shape_the_endpoint_documents():
    """One line, one numeric ``value`` field, second-precision timestamp."""
    line = encode_point("sensor.outdoor_temperature", 21.5, TS, "°C")

    assert line == (
        "°C,domain=sensor,entity_id=outdoor_temperature value=21.5 1786528800"
    )


def test_entity_id_is_sent_without_its_domain_prefix():
    """Home Assistant sends the object id, and the API is written against that.

    ``DeriveBaseTagName`` reassembles ``{domain}.{entity_id}``. Sending the
    full entity id in the ``entity_id`` tag would produce
    ``sensor.sensor.outdoor_temperature``.
    """
    line = encode_point("binary_sensor.heat_pump_running", 1.0, TS)
    assert "entity_id=heat_pump_running" in line
    assert "entity_id=binary_sensor.heat_pump_running" not in line


def test_tag_name_matches_api_derivation():
    """The name we show the user is the name the API will create."""
    assert tag_name_for_entity("sensor.outdoor_temperature") == (
        "sensor.outdoor_temperature"
    )
    assert tag_name_for_entity("binary_sensor.heat_pump_running") == (
        "binary_sensor.heat_pump_running"
    )


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        # Both from the doc comment on SanitizeTagName.
        ("°C", "t_C"),
        ("disk.path-/", "disk.path-_"),
        # A leading digit is not in the alphabet's first position.
        ("1sensor", "t1sensor"),
        ("", "t"),
    ],
)
def test_sanitize_matches_the_documented_rules(raw, expected):
    assert sanitize_tag_name(raw) == expected


def test_long_names_truncate_with_the_same_stable_hash():
    """191 characters + '-' + 8 UPPERCASE hex = exactly 200.

    Mirrors InfluxWriteApiTests.SeriesNameOver200Characters_
    TruncatesWithStableHash. Two details are easy to get silently wrong: the
    hash is over the FULL RAW name rather than the sanitized one, and .NET's
    Convert.ToHexString produces upper case.
    """
    raw = "sensor." + "x" * 260
    expected_hash = hashlib.sha256(raw.encode("utf-8")).hexdigest().upper()[:8]

    result = sanitize_tag_name(raw)

    assert len(result) == TAG_NAME_MAX_LENGTH
    assert result == raw[:191] + "-" + expected_hash
    assert result[192:] == expected_hash
    assert result[192:].isupper()


def test_two_long_names_sharing_a_prefix_stay_distinct():
    a = sanitize_tag_name("sensor." + "y" * 260 + "_a")
    b = sanitize_tag_name("sensor." + "y" * 260 + "_b")
    assert a != b


def test_unknown_state_is_never_sent_as_zero():
    """The core promise: a missing reading is a gap, never an invented zero.

    Home Assistant's own ``state_as_number`` maps STATE_UNKNOWN to 0.0, so
    reaching for it without this guard would write a fabricated zero into
    somebody's history every time a sensor dropped off the network.
    """
    for state in ("unknown", "unavailable", "", None, "Unknown", "UNAVAILABLE"):
        assert state_to_value(state, "sensor") is None
        assert state_to_value(state, "binary_sensor") is None


def test_on_off_states_become_one_and_zero_in_a_two_state_domain():
    assert state_to_value("on", "switch") == 1.0
    assert state_to_value("off", "switch") == 0.0
    assert state_to_value("on", "binary_sensor") == 1.0
    assert state_to_value("open", "cover") == 1.0
    assert state_to_value("closed", "cover") == 0.0
    assert state_to_value("above_horizon", "sun") == 1.0


def test_non_numeric_states_produce_nothing():
    assert state_to_value("heat", "climate") is None
    assert state_to_value("playing", "media_player") is None
    assert state_to_value("nan", "sensor") is None
    assert state_to_value("inf", "sensor") is None


def test_numeric_states_survive_intact():
    assert state_to_value("21.5", "sensor") == 21.5
    assert state_to_value("-3", "sensor") == -3.0
    assert state_to_value("0", "sensor") == 0.0
    # And a domain nobody thought about does not stop a number being a number.
    assert state_to_value("21.5") == 21.5


def test_boolean_states_encode_but_do_not_qualify_anything():
    """These exist to encode a choice the user already made.

    ``state_to_value`` accepting "on" is right: a light the user deliberately
    picked should be stored as 1 and 0. Using the same answer to decide what
    the PICKER offers is what put every light, lock, person and the sun on a
    336-row screen - see tests/test_selection.py.
    """
    assert state_to_value("on", "light") == 1.0
    assert state_to_value("above_horizon", "sun") == 1.0
    assert not hasattr(line_protocol, "can_produce_a_reading"), (
        "selection must not be able to reach for a state-shaped predicate again"
    )


# --------------------------------------------------------------------------
# G4 - a word is only a reading where the domain says it is
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("state", "domain"),
    [
        # The entity this rule exists for: sensor.house_mode cycles
        # home/night/away/guest, and "home" used to be the one that wrote a
        # value. One point out of four state changes, shaped like a series.
        ("home", "sensor"),
        ("away", "sensor"),
        ("night", "sensor"),
        ("guest", "sensor"),
        # A person reports the name of a zone as readily as "home".
        ("home", "person"),
        ("not_home", "device_tracker"),
        # A lock has six states, and its "open" means the opposite of a cover's.
        ("locked", "lock"),
        ("open", "lock"),
        ("jammed", "lock"),
        # Three or more resting states each.
        ("off", "climate"),
        ("off", "media_player"),
    ],
)
def test_a_word_outside_a_two_state_domain_is_not_a_reading(state, domain):
    """Mapping half a state machine is worse than mapping none of it.

    A series that fires for one state out of four is not a partial record, it
    is a misleading one: nothing on the chart says the other three happened.
    """
    assert state_to_value(state, domain) is None


def test_the_word_table_and_the_domain_list_stay_in_step():
    """Neither half of the rule is safe on its own.

    "home" back in the table is harmless only while no domain reporting it is
    allowlisted, and ``person`` in the allowlist is harmless only while "home"
    is out of the table. Both were true at once, and the result was
    ``sensor.house_mode`` with a series containing a single point.

    So this asserts the pairing rather than either half: the domains whose
    state machines are known to run past the words we understand are named
    here, and neither the allowlist nor the table may reach them.
    """
    # Every one of these reports something outside the pair: a zone name, a
    # mode, a transient, a jam.
    multi_state_domains = {
        "sensor",
        "person",
        "device_tracker",
        "lock",
        "climate",
        "media_player",
        "water_heater",
        "vacuum",
        "alarm_control_panel",
        "weather",
        "select",
        "input_select",
    }
    trespassers = line_protocol._BOOLEAN_DOMAINS & multi_state_domains
    assert not trespassers, (
        f"{trespassers} report more states than the table maps, so their series "
        "would be mostly holes"
    )

    # And the words only those domains use stay out of the table, because a
    # table entry is one allowlist edit away from being reachable again.
    for word in (
        "home",
        "not_home",
        "locked",
        "unlocked",
        "jammed",
        "heat",
        "cool",
        "playing",
        "paused",
        "idle",
    ):
        assert word not in line_protocol._BOOLEAN_STATES, (
            f"{word!r} belongs to a state machine with more than two states"
        )


def test_a_word_state_is_not_the_same_as_no_state():
    """The two reasons for ``None`` are told apart by ``is_missing_state``.

    ``unavailable`` is Home Assistant saying it has nothing to report, and a
    gap is the honest record of it. ``away`` is the entity reporting something
    its owner can see, which we then fail to store - a loss, and the forwarder
    counts it as one.
    """
    assert is_missing_state("unavailable")
    assert is_missing_state("unknown")
    assert is_missing_state("")
    assert is_missing_state(None)

    assert not is_missing_state("away")
    assert not is_missing_state("heat")
    assert not is_missing_state("21.5")


def test_integers_are_still_float_fields():
    """A sensor reporting 21 now and 21.5 later must not change field type."""
    assert "value=21.0 " in encode_point("sensor.a", 21.0, TS)


def test_special_characters_are_escaped_not_dropped():
    """Spaces, commas and equals signs would otherwise re-parse the line."""
    line = encode_point("sensor.a", 1.0, TS, "kWh per day")
    assert line.startswith("kWh\\ per\\ day,")


def test_batch_is_newline_joined():
    lines = [encode_point("sensor.a", 1.0, TS), encode_point("sensor.b", 2.0, TS)]
    body = encode_batch(lines)
    assert body.count("\n") == 1
    assert body.splitlines() == lines
