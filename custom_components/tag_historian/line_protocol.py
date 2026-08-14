"""Encode Home Assistant states as InfluxDB line protocol.

Pure and synchronous: no I/O, no Home Assistant imports beyond the state
constants. That is deliberate - this module is where being wrong is silent (a
mis-derived tag name does not raise, it just quietly starts a second series
next to the customer's history), so it has to be testable without a running
Home Assistant.

The tag-name functions here are a MIRROR of the derivation and sanitisation
the API's line-protocol endpoint applies server-side. They exist so the
integration can tell the user which tag a selected entity will become BEFORE
anything is written. The API remains the authority; tests/test_line_protocol.py
pins the mirror against fixtures taken from the server's rules.
"""

from __future__ import annotations

import hashlib
import math
from datetime import datetime

# ``[A-Za-z][A-Za-z0-9_.-]*``, max 200 - TagNameValidator.MaxLength. Tag names
# key the on-disk storage layout, which is why the alphabet is this narrow.
TAG_NAME_MAX_LENGTH = 200
_TAG_NAME_ALPHABET = frozenset(
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_.-"
)

# States that are not numbers but mean one - AND ONLY where the domain makes
# them a genuine two-state signal. See ``_BOOLEAN_DOMAINS`` below; the pairing
# is the point, and applying this table to any string that happens to match is
# the bug it was written to end.
#
# Derived from Home Assistant's own ``state_as_number`` with two deliberate
# departures. STATE_UNKNOWN is absent: core maps "unknown" to 0.0, and writing
# that would put an invented zero into somebody's history. And "home"/
# "not_home" are gone entirely - no domain that keeps them keeps ONLY them. A
# ``person`` reports the name of whatever zone they are in as readily as it
# reports "home", and it was "home" mapping to 1.0 that made a plain
# ``sensor.house_mode`` cycling home/night/away/guest write a single 1.0 and
# three silences.
#
# This table encodes a state the user ALREADY CHOSE to historise. It is not a
# test of whether something is worth offering, and it must never be used as
# one: "on", "open" and "above_horizon" all live here, so a picker built on
# "does this parse" offers every light, cover and the sun. See
# selection._classify for the rule that decides the screen.
_BOOLEAN_STATES: dict[str, float] = {
    "on": 1.0,
    "off": 0.0,
    "open": 1.0,
    "closed": 0.0,
    "above_horizon": 1.0,
    "below_horizon": 0.0,
}

# The domains where the words above ARE the state machine, so that 1/0 series
# is a faithful record of the entity rather than a record of the few states we
# happened to recognise.
#
# What is NOT here, and why - every one of these reports three or more resting
# states, so mapping the two we know produces a series that is mostly holes,
# which is worse than storing nothing at all because it looks like data:
#
#   sensor            a sensor whose state is a word is an enum in disguise;
#                     ``sensor.house_mode`` is the entity this whole rule is
#                     about
#   person,           the state is the name of a zone at least as often as it
#   device_tracker    is "home"
#   lock              also locking, unlocking, jammed - and an "open" that
#                     means the opposite of a cover's
#   climate,          heat/cool/auto/dry, playing/paused/idle, and so on
#   media_player,
#   water_heater,
#   vacuum,
#   alarm_control_panel
#
# ``cover`` IS here: open and closed are its resting states, and the two
# transients (opening, closing) are counted as unstorable like anything else.
_BOOLEAN_DOMAINS = frozenset(
    {
        "binary_sensor",
        "switch",
        "input_boolean",
        "light",
        "fan",
        "siren",
        "humidifier",
        "automation",
        "script",
        "cover",
        "sun",
    }
)

# Never sent, never converted. "unavailable" and "unknown" are Home Assistant
# saying it has nothing to report; the honest wire representation of that is
# no point at all, which becomes a gap in the chart.
_NON_VALUES = frozenset({"unknown", "unavailable", "none", ""})


def is_boolean_domain(domain: str | None) -> bool:
    """Whether a word can be a reading at all in this domain.

    True for ``cover``, false for ``climate``, and the difference is a whole
    sentence in the log: in a domain that is here, an unstorable state means
    the WORD was not one of the two - ``cover`` sitting at ``opening``. In a
    domain that is not, no word would have been stored whatever it said.
    """
    return domain in _BOOLEAN_DOMAINS


def is_missing_state(state: str | None) -> bool:
    """True when Home Assistant is saying it has nothing to report.

    The counterpart to :func:`state_to_value` returning ``None``, and the
    distinction the delivery counters are built on. Both answers mean "send
    nothing", but only this one means "there was no reading to lose": an
    ``unavailable`` sensor is a gap, whereas a sensor sitting at ``away`` has
    told its owner something that we then failed to store.
    """
    if state is None:
        return True
    return state.strip().lower() in _NON_VALUES


def state_to_value(state: str | None, domain: str | None = None) -> float | None:
    """Return the numeric reading a state carries, or ``None`` for no reading.

    ``None`` means "send nothing". It is never 0.0, and the difference is the
    whole promise the Home Assistant guide (taghistorian.com/docs/home-assistant)
    makes: a missing reading is a gap, never an invented zero. Home Assistant's ``state_as_number`` maps
    STATE_UNKNOWN to 0.0, so calling it without the guard below would fabricate
    readings for every unavailable sensor in the house.

    ``domain`` gates the word-state table. Omitting it accepts numbers only,
    which is the safe default for a caller that does not know what it is
    looking at: a word stored as 1.0 on the strength of the string alone is
    how ``sensor.house_mode`` came to have a series containing one point.
    """
    if state is None:
        return None

    normalised = state.strip().lower()
    if normalised in _NON_VALUES:
        return None

    try:
        value = float(state)
    except (TypeError, ValueError):
        if domain not in _BOOLEAN_DOMAINS:
            return None
        value = _BOOLEAN_STATES.get(normalised, math.nan)

    # inf/NaN are not storable and would be rejected downstream anyway; drop
    # them here so a single bad sensor cannot fail the batch around it.
    if not math.isfinite(value):
        return None
    return value


def sanitize_tag_name(raw_name: str) -> str:
    """Mirror of ``InfluxWriteController.SanitizeTagName``.

    Three rules, in this order:
      * every character outside the alphabet becomes ``_``;
      * a name not starting with an ASCII letter gains a ``t`` prefix;
      * a name over 200 characters keeps its first 191 and gains
        ``-`` + the first 8 UPPERCASE hex chars of the SHA-256 of the FULL RAW
        name, so two long names sharing a prefix stay distinct.

    The hash is of ``raw_name``, not of the sanitized string, and the hex is
    upper case because .NET's ``Convert.ToHexString`` is. Both details are easy
    to get subtly wrong and neither fails loudly.
    """
    sanitized = "".join(c if c in _TAG_NAME_ALPHABET else "_" for c in raw_name)

    if not sanitized or not (
        "a" <= sanitized[0] <= "z" or "A" <= sanitized[0] <= "Z"
    ):
        sanitized = "t" + sanitized

    if len(sanitized) <= TAG_NAME_MAX_LENGTH:
        return sanitized

    digest = hashlib.sha256(raw_name.encode("utf-8")).hexdigest().upper()[:8]
    return sanitized[: TAG_NAME_MAX_LENGTH - 9] + "-" + digest


def tag_name_for_entity(entity_id: str) -> str:
    """The Tag Historian tag a selected entity will land in.

    ``DeriveBaseTagName`` gives a point carrying both a ``domain`` and an
    ``entity_id`` tag the name ``{domain}.{entity_id}`` - and that is only true
    while those are the ONLY two tags on the line. See :func:`encode_point`.
    """
    return sanitize_tag_name(entity_id)


def _escape(value: str, extra: str = "") -> str:
    """Escape line protocol special characters.

    Backslash first, or the escapes we add get re-escaped on the next pass.
    """
    out = value.replace("\\", "\\\\").replace(" ", "\\ ").replace(",", "\\,")
    if "=" in extra:
        out = out.replace("=", "\\=")
    return out


def _format_value(value: float) -> str:
    """Render a reading as a line protocol float field.

    Always a float field (never the ``123i`` integer form). The endpoint stores
    both, but a sensor that reports 21 now and 21.5 later must not change field
    type mid-series.
    """
    if value == int(value) and abs(value) < 1e15:
        return f"{int(value)}.0"
    return repr(value)


def encode_point(
    entity_id: str,
    value: float,
    timestamp: datetime,
    unit: str | None = None,
) -> str:
    """Encode one reading as one line of InfluxDB line protocol.

    EXACTLY TWO TAGS, ``domain`` and ``entity_id``, and never a third.

    This is the single most dangerous line in the integration to change.
    ``DeriveBaseTagName`` only produces the clean ``{domain}.{entity_id}`` name
    while those two are the whole tag set; ANY additional tag - an instance
    name, a Home Assistant location, a version stamp - is folded into the name
    as ``.key-value``. Adding one would not fail, it would silently rename
    every tag on the account, orphan the customer's entire history behind names
    nothing writes to any more, and spend their tag quota a second time on the
    new spellings.

    ``entity_id`` carries the object id WITHOUT its domain prefix, because that
    is what Home Assistant's built-in influxdb integration sends and what the
    API's derivation is written against. The measurement name is the unit of
    measurement, mirroring the built-in integration; the endpoint ignores it
    entirely when domain and entity_id are present, so it is there for
    recognisability in a packet capture and nothing else.
    """
    domain, _, object_id = entity_id.partition(".")

    measurement = _escape(unit or entity_id)
    line = (
        f"{measurement},"
        f"domain={_escape(domain, extra='=')},"
        f"entity_id={_escape(object_id, extra='=')} "
        f"value={_format_value(value)} "
        f"{int(timestamp.timestamp())}"
    )
    return line


def encode_batch(lines: list[str]) -> str:
    """Join encoded lines into one request body (precision=s)."""
    return "\n".join(lines)
