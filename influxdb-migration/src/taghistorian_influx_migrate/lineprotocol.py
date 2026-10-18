"""Write a point back out as InfluxDB line protocol, for ``POST /api/v2/write``.

The history goes to Tag Historian through the same endpoint Home Assistant's
influxdb integration writes to live, as the same kind of line: the original
measurement, the original tags, the reading in ``value`` and the friendly name
and unit as Home Assistant's own ``*_str`` fields. That is what makes the tags
the copy creates the tags live forwarding writes to - the server names them
with one piece of code for both.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Union

Number = Union[float, int, bool]


def _escape(text: str, *, equals: bool) -> str:
    # Backslash first, or the escapes added below would be escaped again.
    out = text.replace("\\", "\\\\").replace(",", "\\,").replace(" ", "\\ ")
    if equals:
        out = out.replace("=", "\\=")
    return out


def escape_measurement(name: str) -> str:
    """A measurement escapes commas and spaces; ``=`` is literal there."""
    return _escape(name, equals=False)


def escape_key(key: str) -> str:
    """Tag keys, tag values and field keys also escape ``=``."""
    return _escape(key, equals=True)


def string_field(value: str) -> str:
    """A quoted string field: only the quote and the backslash are escaped."""
    return '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'


def number_field(value: Number) -> str:
    """A reading, in the type it had in InfluxDB as far as that can be known.

    A boolean stays a boolean and an integer stays an integer (``5i``) - the
    server keeps both exactly, as it would have live. A float is written with
    ``repr``, which always round-trips and always reads as a float to the
    parser (``21.0``, ``1e-05``). InfluxDB 1.x answers a query in JSON, which
    cannot tell ``21`` the integer from ``21.0`` the float; the 1.x reader
    hands everything over as a float for that reason, and Home Assistant only
    ever writes floats anyway.
    """
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return f"{value}i"
    return repr(float(value))


def encode_line(
    measurement: str,
    tags: Mapping[str, str],
    fields: Iterable[tuple[str, Number | str]],
    timestamp_ns: int,
) -> str:
    """One line. Timestamps are nanoseconds - the request says ``precision=ns``."""
    parts = [escape_measurement(measurement)]
    for key in sorted(tags):
        value = tags[key]
        if value == "":
            # Line protocol has no empty tag value, and InfluxDB never stores
            # one; a series without the tag is simply without it.
            continue
        parts.append(f"{escape_key(key)}={escape_key(value)}")
    rendered_fields = []
    for key, value in fields:
        rendered = string_field(value) if isinstance(value, str) else number_field(value)
        rendered_fields.append(f"{escape_key(key)}={rendered}")
    if not rendered_fields:
        raise ValueError("a line needs at least one field")
    return f"{','.join(parts)} {','.join(rendered_fields)} {timestamp_ns}"
