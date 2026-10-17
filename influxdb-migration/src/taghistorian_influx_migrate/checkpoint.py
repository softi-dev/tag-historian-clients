"""The resume file: where every tag's copy has got to, written after every batch.

A history copy is long - days, when a plan's daily allowance is what paces it
- and it will be interrupted: a laptop lid, a reboot, a cron job that runs
once a night. Tag Historian does not deduplicate (a reading sent twice is
stored twice), so "start again from the beginning" is not a way to resume,
and neither is "start from roughly where it stopped". This file is how the
copy knows exactly.

WHAT IS IN IT. Per tag: the InfluxDB series it is copied from, and the
timestamp of the newest reading Tag Historian has ACKNOWLEDGED - written only
after a 204, never before the request is sent. Plus the time range, fixed on
the first run so a resume copies the same history, and which InfluxDB and
which Tag Historian it is between, so a resume cannot be pointed at a
different pair by accident.

HOW IT IS WRITTEN. Whole, to a temporary file beside it, then renamed over
the old one (``os.replace``, atomic on every platform this runs on). A crash
mid-write leaves the previous complete file, which is behind by at most one
batch - and that batch is then found by asking Tag Historian, not resent
blindly (see ``migrate.Runner``).

It holds no secrets: the keys and passwords are read from the command line or
the environment on every run and never written here.
"""

from __future__ import annotations

import json
import os
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path

from .influx import SeriesKey

FORMAT_VERSION = 1


class CheckpointError(Exception):
    pass


@dataclass
class TagState:
    sources: list[SeriesKey]
    # Newest timestamp Tag Historian acknowledged for this tag; None = nothing yet.
    written_through_ns: int | None = None
    readings_sent: int = 0
    # pending | done | refused | failed
    status: str = "pending"
    reason: str = ""
    # A batch that was sent and never answered, waiting to be settled.
    unconfirmed: dict | None = None

    def to_json(self) -> dict:
        return {
            "sources": [s.to_json() for s in self.sources],
            "written_through_ns": self.written_through_ns,
            "readings_sent": self.readings_sent,
            "status": self.status,
            "reason": self.reason,
            "unconfirmed": self.unconfirmed,
        }

    @staticmethod
    def from_json(data: Mapping) -> TagState:
        return TagState(
            sources=[SeriesKey.from_json(s) for s in data.get("sources") or []],
            written_through_ns=data.get("written_through_ns"),
            readings_sent=int(data.get("readings_sent") or 0),
            status=str(data.get("status") or "pending"),
            reason=str(data.get("reason") or ""),
            unconfirmed=data.get("unconfirmed"),
        )


@dataclass
class Checkpoint:
    path: Path
    source: dict
    destination: str
    start_ns: int
    end_ns: int
    options: dict = field(default_factory=dict)
    tags: dict[str, TagState] = field(default_factory=dict)
    # Readings this copy sent per UTC day - what it has spent of each day's allowance.
    days: dict[str, int] = field(default_factory=dict)

    def save(self) -> None:
        payload = {
            "format": FORMAT_VERSION,
            "source": self.source,
            "destination": self.destination,
            "start_ns": self.start_ns,
            "end_ns": self.end_ns,
            "options": self.options,
            "tags": {name: state.to_json() for name, state in sorted(self.tags.items())},
            "days": dict(sorted(self.days.items())),
        }
        temporary = self.path.with_name(self.path.name + ".tmp")
        with open(temporary, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, ensure_ascii=False)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, self.path)

    @staticmethod
    def load(path: Path) -> Checkpoint | None:
        if not path.exists():
            return None
        try:
            with open(path, encoding="utf-8") as handle:
                data = json.load(handle)
        except (OSError, ValueError) as exc:
            raise CheckpointError(f"cannot read the resume file {path}: {exc}") from exc
        if data.get("format") != FORMAT_VERSION:
            raise CheckpointError(
                f"{path} was written by a different version of this tool "
                f"(format {data.get('format')!r}); start a new copy with another --state file"
            )
        return Checkpoint(
            path=path,
            source=dict(data["source"]),
            destination=str(data["destination"]),
            start_ns=int(data["start_ns"]),
            end_ns=int(data["end_ns"]),
            options=dict(data.get("options") or {}),
            tags={name: TagState.from_json(t) for name, t in (data.get("tags") or {}).items()},
            days={str(k): int(v) for k, v in (data.get("days") or {}).items()},
        )

    def check_same_copy(self, source: dict, destination: str, options: dict) -> None:
        """Refuse to resume a different copy into this file's positions."""
        if self.source != source:
            raise CheckpointError(
                f"{self.path} belongs to a copy from {self.source}, not from {source}. "
                "Use another --state file for a different source."
            )
        if self.destination != destination:
            raise CheckpointError(
                f"{self.path} belongs to a copy into {self.destination}, not {destination}."
            )
        changed = sorted(
            k for k in set(self.options) | set(options) if self.options.get(k) != options.get(k)
        )
        if changed:
            raise CheckpointError(
                f"{self.path} was started with different options ({', '.join(changed)}). Resume with "
                "the same --entity/--exclude/--measurement/unit options, or start a new copy with "
                "another --state file."
            )
