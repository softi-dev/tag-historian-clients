"""Which Tag Historian tag a point from InfluxDB becomes, and what it says about it.

A MIRROR, not a second opinion. Every rule in this module is the rule
``POST /api/v2/write`` applies on the server (``InfluxWriteController`` and
``HomeAssistantAttributes`` in the product), restated so the migration can tell
somebody which tags their history will land in BEFORE anything is written, and
can count them against their plan. The history itself is sent as line protocol
to that same endpoint, so the server makes the final call with the same code
that names the tags Home Assistant writes live - the mirror only has to be
right for the dry run to be honest, and ``tests/test_naming.py`` pins it
against the server's own examples.

Being wrong here is silent, which is why it is spelt out: a history copied
under ``sensor.living_room_temperature.instance-home`` while Home Assistant
writes ``sensor.living_room_temperature`` is two half-histories, two tags
against the plan, and no error anywhere.
"""

from __future__ import annotations

import hashlib
import math
import unicodedata
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Union

# TagNameValidator: ``[A-Za-z][A-Za-z0-9_.-]*``, at most 200 characters. Tag
# names key the on-disk layout on the server, which is why the alphabet is
# this narrow and why the server sanitises rather than widening it.
TAG_NAME_MAX_LENGTH = 200
_TAG_NAME_ALPHABET = frozenset("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_.-")

# The two string fields Home Assistant's influxdb integration writes for an
# entity's friendly name and unit ("{attribute}_str" is its spelling for any
# text attribute). The server reads exactly these on a line that has a numeric
# ``value`` and makes them the tag's description and unit.
FRIENDLY_NAME_FIELD = "friendly_name_str"
UNIT_FIELD = "unit_of_measurement_str"
VALUE_FIELD = "value"

# CreateTagRequestValidator's limits, which HomeAssistantAttributes applies to
# both: a longer text is IGNORED by the server, never cut short.
MAX_DESCRIPTION_LENGTH = 500
MAX_UNITS_LENGTH = 50

FieldValue = Union[float, int, bool, str]


def sanitize_tag_name(raw_name: str) -> str:
    """Mirror of ``InfluxWriteController.SanitizeTagName``.

    Every character outside the alphabet becomes ``_``; a name not starting
    with an ASCII letter gains a ``t``; a name over 200 characters keeps its
    first 191 and gains ``-`` and the first eight UPPER-case hex digits of the
    SHA-256 of the FULL RAW name. The hash is of the raw name, and upper case
    because .NET's ``Convert.ToHexString`` is - both are easy to get subtly
    wrong and neither fails loudly.
    """
    sanitized = "".join(c if c in _TAG_NAME_ALPHABET else "_" for c in raw_name)

    if not sanitized or not ("a" <= sanitized[0] <= "z" or "A" <= sanitized[0] <= "Z"):
        sanitized = "t" + sanitized

    if len(sanitized) <= TAG_NAME_MAX_LENGTH:
        return sanitized

    digest = hashlib.sha256(raw_name.encode("utf-8")).hexdigest().upper()[:8]
    return sanitized[: TAG_NAME_MAX_LENGTH - 9] + "-" + digest


def is_home_assistant_series(tags: Mapping[str, str]) -> bool:
    """A tag set with BOTH ``domain`` and ``entity_id`` is Home Assistant's."""
    return "domain" in tags and "entity_id" in tags


def entity_of(tags: Mapping[str, str]) -> str | None:
    """``sensor.living_room_temperature`` for a Home Assistant series, else None.

    Home Assistant writes ``entity_id`` WITHOUT its domain, so the entity id a
    user knows from Home Assistant is the two tags joined - which is also how
    ``--entity`` patterns are matched.
    """
    if not is_home_assistant_series(tags):
        return None
    return f"{tags['domain']}.{tags['entity_id']}"


def derive_base_name(measurement: str, tags: Mapping[str, str]) -> str:
    """Mirror of ``InfluxWriteController.DeriveBaseTagName``, before sanitising.

    * ``domain`` and ``entity_id`` both present: ``{domain}.{entity_id}``, with
      every OTHER tag appended as ``.key-value`` sorted by key - so two Home
      Assistant instances tagged ``instance=home1``/``home2`` stay two tags,
      and a static ``tags:`` block in somebody's HA configuration is part of
      the name. The measurement is ignored: for Home Assistant it is the unit.
    * no tags at all: the measurement.
    * anything else: the measurement, then every tag as ``.key-value`` sorted
      by key.

    Sorted by key with Python's ordering, which is .NET's ordinal ordering for
    every key outside the astral planes - nobody's tag keys are emoji.
    """
    if is_home_assistant_series(tags):
        extras = [f"{k}-{tags[k]}" for k in sorted(tags) if k not in ("domain", "entity_id")]
        base = f"{tags['domain']}.{tags['entity_id']}"
        return base if not extras else base + "." + ".".join(extras)

    if not tags:
        return measurement

    return measurement + "." + ".".join(f"{k}-{tags[k]}" for k in sorted(tags))


def is_storable_number(value: object) -> bool:
    """The field types the server turns into a reading: numbers and booleans.

    Strings are never readings. A non-finite float cannot reach the server at
    all - InfluxDB does not store one either - but it is refused here too, so
    one corrupt row cannot fail a whole request around it.
    """
    if isinstance(value, bool):
        return True
    if isinstance(value, int):
        return True
    if isinstance(value, float):
        return math.isfinite(value)
    return False


@dataclass(frozen=True)
class Row:
    """One reading as the server will store it: which tag, from which field."""

    tag_name: str
    field: str
    value: float | int | bool


def rows_for_point(base_name: str, fields: Mapping[str, FieldValue]) -> list[Row]:
    """Mirror of the field rule in ``InfluxWriteController.Write``.

    A numeric ``value`` field wins: ONE reading, under the base name, and every
    other field on the line is ignored as a reading - the Home Assistant shape,
    a value plus a grab bag of attributes. Without one, every numeric field
    becomes a tag of its own, ``{base}.{field}``: how a ``climate`` entity
    sitting at ``heat`` turns into ``climate.living_room.current_temperature``
    and its siblings when Home Assistant writes it live.
    """
    value = fields.get(VALUE_FIELD)
    if value is not None and is_storable_number(value):
        return [Row(sanitize_tag_name(base_name), VALUE_FIELD, value)]  # type: ignore[arg-type]

    return [
        Row(sanitize_tag_name(f"{base_name}.{key}"), key, field_value)  # type: ignore[arg-type]
        for key, field_value in fields.items()
        if is_storable_number(field_value)
    ]


def usable_text(value: object, max_length: int) -> str | None:
    """Mirror of ``HomeAssistantAttributes.Usable``.

    Trimmed, not empty, not longer than the limit, and no control characters
    (nor U+2028/U+2029). Anything else is ignored by the server, never cut -
    half a name is a wrong name - so it is not worth sending either.
    """
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text or len(text) > max_length:
        return None
    if any(unicodedata.category(c) == "Cc" or c in ("\u2028", "\u2029") for c in text):
        return None
    return text


def measurement_reads_as_unit(
    measurement: str, tags: Mapping[str, str], not_units: frozenset[str] = frozenset()
) -> bool:
    """Whether a Home Assistant series' measurement name is its unit.

    The one rule here that is NOT the server's. The server deliberately never
    reads a measurement as a unit, because live it cannot know how Home
    Assistant was configured. The migration can see the history, and Home
    Assistant's influxdb integration has exactly four ways to fill the
    measurement - this tells the default apart from the other three:

    * the unit (``measurement_attr: unit_of_measurement``, the default) - and
      then, by Home Assistant's own rule, no ``unit_of_measurement_str`` field
      is written, because the measurement already says it;
    * the entity id (``measurement_attr: entity_id``, or the fallback for an
      entity without a unit) - ``sensor.door``, recognisable from the tags;
    * ``{domain}__{device_class}`` or the bare domain
      (``measurement_attr: domain__device_class``), likewise;
    * a fixed string (``default_measurement``/``override_measurement``) - which
      cannot be recognised, and is what ``--not-a-unit`` is for.
    """
    domain = tags.get("domain", "")
    entity = entity_of(tags)
    if not measurement or measurement in not_units:
        return False
    if measurement in (entity, domain):
        return False
    if domain and measurement.startswith(domain + "__"):
        return False
    return usable_text(measurement, MAX_UNITS_LENGTH) is not None


@dataclass(frozen=True)
class Description:
    """What a point says about its tag: the friendly name and the unit."""

    friendly_name: str | None
    unit: str | None


def describe(
    measurement: str,
    tags: Mapping[str, str],
    fields: Mapping[str, FieldValue],
    *,
    unit_from_measurement: bool,
    not_units: frozenset[str] = frozenset(),
) -> Description:
    """The friendly name and unit a Home Assistant point should carry.

    Only Home Assistant's shape describes a tag (a numeric ``value``), as on
    the server. The unit field wins when the history has one; otherwise, if
    allowed, the measurement name when it reads as a unit (see
    :func:`measurement_reads_as_unit`).
    """
    if not is_home_assistant_series(tags):
        return Description(None, None)
    value = fields.get(VALUE_FIELD)
    if value is None or not is_storable_number(value):
        return Description(None, None)

    friendly_name = usable_text(fields.get(FRIENDLY_NAME_FIELD), MAX_DESCRIPTION_LENGTH)
    unit = usable_text(fields.get(UNIT_FIELD), MAX_UNITS_LENGTH)
    if (
        unit is None
        and unit_from_measurement
        and UNIT_FIELD not in fields
        and measurement_reads_as_unit(measurement, tags, not_units)
    ):
        unit = measurement
    return Description(friendly_name, unit)
