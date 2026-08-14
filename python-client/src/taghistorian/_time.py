"""Timestamp conversion between Python ``datetime`` and the API's wire format.

THE ASSUMPTION THIS FILE MAKES, STATED ONCE: a naive ``datetime`` (no
``tzinfo``) passed to :meth:`TagHistorianClient.write` or
:meth:`TagHistorianClient.write_batch` is assumed to already be in UTC, not
local time. This matches the server: ``MeasurementIngestionService`` /
``MeasurementsController.TryCreateMeasurement`` normalises an incoming
timestamp with ``FileSystemStorageEngine.NormalizeToUtc``, and a
``DateTimeKind.Unspecified`` value there is treated as UTC, not shifted by
whatever offset the server process happens to be running in. A naive
Python ``datetime`` serialised any other way would silently disagree with
what the server does with it. If your source of timestamps is in local
time, attach the correct zone with ``datetime.replace(tzinfo=...)`` or
``.astimezone()`` before passing it in - do not rely on this client to guess
your offset.
"""

from __future__ import annotations

from datetime import datetime, timezone


def serialize_timestamp(value: datetime | None) -> str | None:
    """Convert a Python ``datetime`` to the ISO 8601 UTC string the API
    expects, or ``None`` (meaning "now", handled server-side - see
    ``MeasurementsController.TryCreateMeasurement``, which defaults an
    absent timestamp to ``DateTime.UtcNow`` before normalising).

    A naive value is assumed to already be UTC (see module docstring); an
    aware value is converted to UTC. Either way the result always ends in
    ``Z``, never a numeric offset, so it is unambiguous on the wire.
    """
    if value is None:
        return None

    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    else:
        value = value.astimezone(timezone.utc)

    iso = value.isoformat()
    # isoformat() on a UTC-aware datetime renders "+00:00", not "Z". Both are
    # valid ISO 8601, but "Z" is what every example in this API's own docs
    # uses, and is unambiguous to a server that only cares about UTC anyway.
    if iso.endswith("+00:00"):
        iso = iso[:-6] + "Z"
    return iso


def parse_timestamp(value: str) -> datetime:
    """Parse a timestamp the API sent back into an aware UTC ``datetime``.

    Handles the trailing ``Z`` ``datetime.fromisoformat`` only learned to
    accept in Python 3.12 - this package supports 3.9+, so it is normalised
    to ``+00:00`` first rather than relying on that.
    """
    text = value[:-1] + "+00:00" if value.endswith("Z") else value
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed
