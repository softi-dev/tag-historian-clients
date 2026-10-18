"""Time as integer nanoseconds since the Unix epoch, UTC, everywhere inside.

InfluxDB stores nanoseconds and Home Assistant writes them, and a
``datetime`` holds microseconds: carried through a ``datetime``, two readings
a few hundred nanoseconds apart would collapse onto one instant and the
resume position could land in the middle of them. So the only conversions to
and from text happen here, at the edges, and none of them goes through a
float.
"""

from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone

NS_PER_SECOND = 1_000_000_000
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)

# RFC 3339 as InfluxDB 2.x writes it in a CSV result (RFC3339Nano: up to nine
# fraction digits, trailing zeros trimmed, always Z), and as a person types it
# on the command line (a date, a date and time, with or without an offset).
_RFC3339 = re.compile(
    r"^(?P<date>\d{4}-\d{2}-\d{2})"
    r"(?:[T ](?P<time>\d{2}:\d{2}(?::\d{2})?)(?:\.(?P<fraction>\d{1,9}))?)?"
    r"(?P<offset>Z|z|[+-]\d{2}:?\d{2})?$"
)


def parse_time_ns(text: str) -> int:
    """Nanoseconds for an RFC 3339 / ISO 8601 timestamp or a bare date.

    A value without an offset is UTC, not the local time zone of whoever runs
    the script: a migration started on a laptop in Stockholm and resumed on a
    server in UTC must mean the same instant both times.
    """
    match = _RFC3339.match(text.strip())
    if match is None:
        raise ValueError(
            f"{text!r} is not a date or timestamp; use e.g. 2024-01-31 or 2024-01-31T12:00:00Z"
        )

    time_part = match.group("time") or "00:00:00"
    if len(time_part) == 5:
        time_part += ":00"
    base = datetime.strptime(f"{match.group('date')} {time_part}", "%Y-%m-%d %H:%M:%S")

    offset = match.group("offset")
    if offset in (None, "Z", "z"):
        tz = timezone.utc
    else:
        sign = -1 if offset.startswith("-") else 1
        digits = offset[1:].replace(":", "")
        tz = timezone(sign * timedelta(hours=int(digits[:2]), minutes=int(digits[2:])))

    whole_seconds = (base.replace(tzinfo=tz) - _EPOCH) // timedelta(seconds=1)
    fraction = (match.group("fraction") or "").ljust(9, "0")
    return whole_seconds * NS_PER_SECOND + int(fraction)


def format_time_ns(ns: int) -> str:
    """RFC 3339 in UTC with as many fraction digits as the value needs.

    Exact - built from integers, so a timestamp written out and read back is
    the same nanosecond. Flux takes this as a time literal as it stands.
    """
    seconds, fraction = divmod(ns, NS_PER_SECOND)
    stamp = (_EPOCH + timedelta(seconds=seconds)).strftime("%Y-%m-%dT%H:%M:%S")
    if fraction:
        stamp += "." + f"{fraction:09d}".rstrip("0")
    return stamp + "Z"


def format_human(ns: int) -> str:
    """``2024-01-31 12:00 UTC`` - for people, to the minute."""
    seconds = ns // NS_PER_SECOND
    return (_EPOCH + timedelta(seconds=seconds)).strftime("%Y-%m-%d %H:%M UTC")


def now_ns() -> int:
    return (datetime.now(timezone.utc) - _EPOCH) // timedelta(microseconds=1) * 1000


def utc_day(ns: int) -> str:
    """The UTC calendar day a moment falls on - the day the daily allowance counts."""
    return (_EPOCH + timedelta(seconds=ns // NS_PER_SECOND)).strftime("%Y-%m-%d")


def seconds_until_next_utc_midnight(ns: int) -> float:
    seconds_into_day = (ns // NS_PER_SECOND) % 86400
    return float(86400 - seconds_into_day)
