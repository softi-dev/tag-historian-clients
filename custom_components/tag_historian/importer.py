"""Import the history Home Assistant already has into Tag Historian.

Two phases, in one import, in this order:

1. STATISTICS. Home Assistant's long-term statistics - the hourly mean,
   minimum and maximum of a measurement, a meter's reading and change - kept
   for as long as its database lives (statistics.py). They go into Tag
   Historian's hour tier (``POST /api/import/{id}/hours``) as what they are,
   hourly summaries, and are how the YEARS before this integration was
   installed come along. First, because they are cheap and they are most of
   the history: a decade of a sensor is 87 600 hours, and a few hundred
   requests.
2. READINGS. The recorder's own states, every reading of the last ten days or
   so, with their original timestamps (``POST /api/import/{id}/readings``).
   Where a reading lands on an hour the statistics filled, the reading's hour
   replaces the summary on Tag Historian's side; the overlap is intended.

Each phase stops for its own reasons. The hourly history allowance, an hours
batch refused, or a Tag Historian without the hours endpoint ends the
statistics phase and the readings phase runs anyway; the readings allowance or
the storage cap ends the readings phase and the statistics are already in. A
key that is refused, an account that may not write, or a Tag Historian that
cannot be reached stops the whole import - and an unreachable one, or a key
replaced through the reauthentication card, carries on by itself at the next
start (:meth:`HistoryImporter.async_load`).

THE STATISTICS PHASE, per entity: one hour first - the newest - to learn the
line its history ends at (``importableBefore``, as for readings: the import's
start, or its first reading in Tag Historian), then whole UTC months back from
that line, newest first, at most 5 000 hours in a request, gzipped, down to
the oldest hour Tag Historian keeps or ``days`` back. A request never splits
a month, so where an entity has got to is the start of the oldest month Tag
Historian has answered for (``hours_cursor``), and a restart carries on from
the month before it.

What Tag Historian holds the READINGS phase to, and what it therefore stops at:

* the plan's full-resolution window - older readings are skipped, so the
  import starts there rather than sending what would be thrown away;
* history only: readings older than the moment the import started, older
  than the entity's first live reading if it is already forwarded live, and
  older than the start of the first import that brought readings into it
  (the API says where that line is in every answer, and the import of that
  entity ends there);
* the account's import allowance, which imported readings are counted against
  INSTEAD of the daily one; when it is used up the import stops and says so;
* the tag limit and the storage cap, as for live data.

And one line of its own: where the live forwarder's part of an entity begins
(:meth:`.forwarder.StateForwarder.history_boundary`) - the oldest reading of it
the forwarder still has to deliver, or, for an entity it has not delivered
anything of yet, when it started listening. Tag Historian cannot draw that
line: an entity whose live readings are still waiting in the forwarder - over
the daily allowance, or during an outage - has no first live reading there
yet, so the import would store them, and the forwarder would then deliver them
a second time through the live endpoint, which does not deduplicate. The line
is taken when the import starts and saved with it.

What it sends is what the live forwarder would have sent: the same numbers and
two-state words, ``unavailable`` left as a gap, the same minimum interval
between two readings of one entity, timestamps to the second. Home
Assistant's long-term statistics are not readings and are never sent as
readings: they go in through the statistics phase above, as hours, and Tag
Historian marks them as Home Assistant's statistics wherever it shows them.

RESUMABLE. Where the import is - which phase, which entities are done, and how
far into each - is saved in ``.storage`` after every batch, so a restart picks
it up where it stopped, under the same import (and so behind the same line). A
batch sent twice is harmless: both endpoints skip what they already have.

RENAMED ENTITIES (renames.py). A renamed entity's history goes into the tag
its live readings go to: its old one (``send_as``) until Tag Historian has the
new name too. Imported under its new name instead, it would start the second
tag the rename handling exists to prevent - and Tag Historian draws the line
an import stops at per tag, so the new tag, with no live reading yet, would
take everything the old one already holds live, and the rename would then
make the two one history with those readings in it twice. The recorder is
read under the entity's id as it is now: Home Assistant moves an entity's
recorded history to its new id when it is renamed - unless the new id has a
history of its own, a replaced meter's successor given the old meter's id,
and then the new meter's history is read under its old id and the old
meter's is left alone (history.py). An import that is running when the
entity is renamed is stopped by the rename's reload and carries on under the
new id (:meth:`HistoryImporter._follow_renames`) - and is never told the
entity is done by reads under the old id that came up empty because the
history had moved (:meth:`HistoryImporter._renamed_meanwhile`).

Once Tag Historian has the new name, the import goes under it, and where it
stops is Tag Historian's line for the renamed tag - which only Tag Historian
can draw, because it has to take in the tag's former names: the first live
reading and the first import under the old name. Drawn from the new name
alone, it lets the history from before the rename in a second time, the old
name holding it already. This relies on the API release this one requires
drawing it over the former names, and on its answer being keyed by the name
the batch was sent under.

And an entity whose rename card was answered "Start a new tag" or "Keep them
apart" is not imported at all (renames.own_tag_since). The old tag keeps its
history, as the card says; Home Assistant has moved that history to the new
id, where it would be read and sent into the new tag in full. What the
recorder holds of the entity from after the choice is live data, the
forwarder's to deliver, so nothing of it is history to import.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Callable, Iterable, Mapping
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

from homeassistant.components import persistent_notification
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.dispatcher import async_dispatcher_send
from homeassistant.helpers.storage import Store
from homeassistant.util import dt as dt_util

from . import history, statistics
from .api import (
    HoursBatchResult,
    ImportBatchResult,
    ImportOutcome,
    TagHistorianAuthError,
    TagHistorianClient,
    TagHistorianConnectionError,
)
from .const import (
    CONF_ENTITIES,
    IMPORT_BATCH_SIZE,
    IMPORT_MAX_CONNECTION_FAILURES,
    IMPORT_RETRY_SECONDS,
    LOGGER,
    MAX_HOURS_PER_REQUEST,
    MAX_IMPORT_DAYS,
    RECORDER_WINDOW_HOURS,
    SIGNAL_IMPORT_UPDATED,
)
from .line_protocol import (
    MAX_FRIENDLY_NAME_LENGTH,
    MAX_UNIT_LENGTH,
    _attribute_text,
    state_to_value,
    tag_name_for_entity,
)
from .renames import own_tag_since as own_tag_since_of
from .renames import registry_ids, removed_since, renamed_since
from .renames import send_as as send_as_of

# Statuses the sensor shows. Stable strings: an automation can wait on them.
STATUS_IDLE = "idle"
STATUS_RUNNING = "running"
STATUS_FINISHED = "finished"
STATUS_STOPPED = "stopped"
STATUSES = [STATUS_IDLE, STATUS_RUNNING, STATUS_FINISHED, STATUS_STOPPED]

# Why an import stopped before it was done.
STOP_ALLOWANCE = "allowance_used_up"
STOP_STORAGE = "storage_full"
STOP_FORBIDDEN = "account_cannot_write"
STOP_AUTH = "key_rejected"
STOP_UNREACHABLE = "unreachable"
STOP_UNKNOWN_IMPORT = "import_unknown"
STOP_REJECTED = "rejected"
# The statistics phase's own: the hourly history allowance, and a Tag
# Historian older than the hours import.
STOP_HOURS_ALLOWANCE = "hours_allowance_used_up"
STOP_HOURS_UNSUPPORTED = "hours_not_supported"

# Stops of the whole import that it carries on from by itself, at the next
# start: Tag Historian came back, or the key was replaced (a refused key never
# gets this far - the entry asks for a new one before it loads).
_RESUMED_ON_LOAD = (STOP_UNREACHABLE, STOP_AUTH)

PHASE_STATISTICS = "statistics"
PHASE_READINGS = "readings"

# How one entity's statistics went, for the phase that runs them.
_GO_ON = "go_on"  # the entity is done, or this batch was taken
_PHASE_STOPPED = "phase_stopped"  # the statistics phase ends; the readings phase runs
_SESSION_STOPPED = "session_stopped"  # the whole import has stopped

HOUR = timedelta(hours=1)

NOTIFICATION_ID = "tag_historian_import_{entry_id}"


class ImportAlreadyRunning(Exception):
    """An import for this entry is already under way."""


@dataclass
class ImportProgress:
    """Where one import is. Saved after every batch; what the sensor shows."""

    status: str = STATUS_IDLE
    import_id: str | None = None
    started_at: str | None = None
    window_start: str | None = None
    since: str | None = None
    entities: list[str] = field(default_factory=list)
    done: list[str] = field(default_factory=list)
    current: str | None = None
    # Per entity: the timestamp (epoch seconds) of the last reading of it
    # that Tag Historian has answered for. A restart carries on after it.
    cursor: dict[str, float] = field(default_factory=dict)
    # Per entity: where the live forwarder's part of it began when the import
    # started (epoch seconds); the import of it ends there. Absent for an
    # entity whose only line is Tag Historian's own.
    ends: dict[str, float] = field(default_factory=dict)
    stored: int = 0
    compressed: int = 0
    duplicates: int = 0
    outside_window: int = 0
    refused: int = 0
    # Of ``refused``: readings that were already live data - not older than
    # their entity's first live reading, the first import into it, or the
    # import's start.
    not_history: int = 0
    refused_tags: list[str] = field(default_factory=list)
    allowance_remaining: int | None = None
    stop_reason: str = ""
    message: str = ""
    finished_at: str | None = None
    # Per entity: its entity registry id, which a rename does not change - so
    # an import resumed after one carries on under the entity's new id.
    registry_ids: dict[str, str] = field(default_factory=dict)

    # The statistics phase (module docstring). Every one of these has a
    # default, so an import an earlier version saved loads as it was - as a
    # readings import - and the store keeps its version: STORAGE_VERSION is
    # shared with the forwarder's buffer, and a bump would fail the setup of
    # every existing entry and lose that buffer (const.py).
    #
    # Whether this import brings the long-term statistics, and which phase it
    # is in; "" is an import saved before there were phases: readings.
    statistics: bool = False
    phase: str = ""
    # How far back the statistics go: ``days`` back, at most ten years.
    statistics_since: str | None = None
    # The first hour Tag Historian keeps, from its first hours answer.
    oldest_allowed: str | None = None
    # Per entity: the start (epoch seconds) of the oldest month of its
    # statistics Tag Historian has answered for. A restart carries on from
    # the month before.
    hours_cursor: dict[str, float] = field(default_factory=dict)
    statistics_done: list[str] = field(default_factory=list)
    statistics_stop_reason: str = ""
    statistics_message: str = ""
    # While the hourly history allowance is spent: when more can go in.
    statistics_resumes_at: str | None = None
    hours_stored: int = 0
    # Hours Tag Historian did not store: already there, older than it keeps,
    # or refused - its answer says which.
    hours_skipped: int = 0
    # Per entity: the oldest hour (epoch seconds) of its statistics that went
    # in or was there already - "since when", for the summary.
    hours_from: dict[str, float] = field(default_factory=dict)
    # Entities with no long-term statistics at all, and entities whose
    # statistics were left out, with why.
    no_statistics: list[str] = field(default_factory=list)
    statistics_left_out: dict[str, str] = field(default_factory=dict)
    # When this entry first started bringing the statistics. Carried from one
    # import to the next: once set, the card that offers it is not raised
    # again (repairs.py), whatever later imports did.
    statistics_started_at: str | None = None

    @classmethod
    def from_saved(cls, data: dict[str, Any]) -> ImportProgress:
        known = {name: data[name] for name in cls.__dataclass_fields__ if name in data}
        return cls(**known)


def _iso(value: datetime | None) -> str | None:
    return value.astimezone(UTC).isoformat() if value is not None else None


def _parse(value: str | None) -> datetime | None:
    return dt_util.parse_datetime(value) if value else None


class HistoryImporter:
    """Runs history imports for one config entry, one at a time."""

    def __init__(
        self,
        hass: HomeAssistant,
        entry: ConfigEntry,
        client: TagHistorianClient,
        store: Store[dict[str, Any]],
        min_interval: int,
        boundary: Callable[[str], float | None] | None = None,
        *,
        send_as: Mapping[str, str] | None = None,
        selected: Iterable[str] | None = None,
        own_tag_since: Mapping[str, str] | None = None,
    ) -> None:
        self.hass = hass
        self.entry = entry
        self._client = client
        self._store = store
        self._min_interval = min_interval
        # StateForwarder.history_boundary in Home Assistant; None only in tests
        # that are about something else.
        self._boundary = boundary
        # The options the forwarder was built from, so the two agree on where
        # an entity's readings go (renames.py); the entry's own when not given.
        self._send_as = dict(send_as if send_as is not None else send_as_of(entry.options))
        self._selected = set(
            selected if selected is not None else entry.options.get(CONF_ENTITIES, [])
        )
        self._own_tag_since = dict(
            own_tag_since if own_tag_since is not None else own_tag_since_of(entry.options)
        )
        self.progress = ImportProgress()
        self._task: asyncio.Task[None] | None = None

    @property
    def running(self) -> bool:
        return self._task is not None and not self._task.done()

    async def async_load(self) -> None:
        """Read where the last import got to, and carry on if it was not done."""
        saved = await self._store.async_load()
        if saved:
            self.progress = ImportProgress.from_saved(saved)
            self._follow_renames()
        progress = self.progress
        if (
            progress.status == STATUS_STOPPED
            and progress.stop_reason in _RESUMED_ON_LOAD
            and progress.import_id
        ):
            # Paused, not ended: Tag Historian could not be reached, or the
            # key was refused and has been replaced since - this entry would
            # not be loading with a refused one. Where it was is saved, under
            # the same import, so it carries on from there.
            LOGGER.info(
                "Tag Historian is carrying on with the history import that stopped because %s",
                "it could not be reached" if progress.stop_reason == STOP_UNREACHABLE else "the key was refused",
            )
            progress.status = STATUS_RUNNING
            progress.stop_reason = ""
            progress.message = ""
            progress.finished_at = None
        if self.progress.status == STATUS_RUNNING and self.progress.import_id:
            LOGGER.info(
                "Tag Historian is carrying on with the history import it was running "
                "before Home Assistant stopped"
            )
            self._spawn()

    def _follow_renames(self) -> None:
        """Carry the saved progress over to the ids its entities have now.

        Renaming an entity reloads the entry (renames.py), which stops a
        running import and carries it on from what was saved - under the old
        id, which the recorder no longer holds anything under once Home
        Assistant has moved the entity's history to the new one: the rest of
        it was never imported, and the import said it had finished.
        """
        progress = self.progress
        renamed = renamed_since(self.hass, progress.registry_ids, self._selected)
        # An entity that is gone, under an id a selected entity has now - the
        # replaced meter, whose successor was given its id. What the recorder
        # holds under that id is no longer its history alone, so the import
        # leaves it; the successor's carries on under the id.
        gone = {
            entity_id
            for entity_id in removed_since(self.hass, progress.registry_ids)
            if entity_id in self._selected
        }
        if not renamed and not gone:
            return

        def now(entity_id: str) -> str:
            return renamed.get(entity_id, entity_id)

        for old, new in renamed.items():
            LOGGER.info(
                "Tag Historian: %s was renamed to %s; the history import carries on under %s",
                old,
                new,
                new,
            )
        for entity_id in gone:
            LOGGER.info(
                "Tag Historian: %s is no longer there and its id is another entity's now; "
                "the history import leaves it",
                entity_id,
            )
        progress.entities = list(
            dict.fromkeys(now(e) for e in progress.entities if e not in gone)
        )
        progress.done = list(dict.fromkeys(now(e) for e in progress.done if e not in gone))
        if progress.current in gone:
            progress.current = None
        progress.current = now(progress.current) if progress.current else None
        progress.registry_ids = {
            now(k): v for k, v in progress.registry_ids.items() if k not in gone
        }
        # Should a stale id and a renamed one ever meet, the later cursor (what
        # Tag Historian has answered for) and the earlier line hold: neither
        # sends a reading twice.
        cursor: dict[str, float] = {}
        for entity_id, stamp in progress.cursor.items():
            if entity_id not in gone:
                cursor[now(entity_id)] = max(stamp, cursor.get(now(entity_id), stamp))
        ends: dict[str, float] = {}
        for entity_id, line in progress.ends.items():
            if entity_id not in gone:
                ends[now(entity_id)] = min(line, ends.get(now(entity_id), line))
        progress.cursor, progress.ends = cursor, ends
        # The statistics phase's, the same way: the later month answered for
        # and the earlier hour that went in hold.
        hours_cursor: dict[str, float] = {}
        for entity_id, month in progress.hours_cursor.items():
            if entity_id not in gone:
                hours_cursor[now(entity_id)] = max(month, hours_cursor.get(now(entity_id), month))
        hours_from: dict[str, float] = {}
        for entity_id, hour in progress.hours_from.items():
            if entity_id not in gone:
                hours_from[now(entity_id)] = min(hour, hours_from.get(now(entity_id), hour))
        progress.hours_cursor, progress.hours_from = hours_cursor, hours_from
        progress.statistics_done = list(
            dict.fromkeys(now(e) for e in progress.statistics_done if e not in gone)
        )
        progress.no_statistics = list(
            dict.fromkeys(now(e) for e in progress.no_statistics if e not in gone)
        )
        progress.statistics_left_out = {
            now(k): v for k, v in progress.statistics_left_out.items() if k not in gone
        }

    def _renamed_meanwhile(self, entity_id: str) -> bool:
        """Whether this entity was renamed after the import was loaded, and the rename followed.

        :meth:`_follow_renames` runs once, at load, and only for a new id in
        the selection the setup was built from (renames.renamed_since). A
        rename landing after that - while the import runs, or while the entry
        is being set up - is its own reload's to follow. But the reads under
        the old id go on until that reload stops this task, and once Home
        Assistant has moved the history they come up empty: the entity looked
        done, was saved as done, and the reload carried "done" over to the
        new id - the rest of its history never imported, and the import said
        it had finished. So ask the registry as it is now, against the
        selection as it is now: the new id is in it exactly when this entry
        followed the rename, and then that reload is on its way.
        """
        registry_id = self.progress.registry_ids.get(entity_id)
        if registry_id is None:
            return False
        return bool(
            renamed_since(
                self.hass, {entity_id: registry_id}, self.entry.options.get(CONF_ENTITIES, [])
            )
        )

    async def async_start(
        self, entity_ids: list[str], days: int | None, *, statistics: bool = False
    ) -> ImportProgress:
        """Start importing these entities' history, at most ``days`` back.

        With ``statistics``, Home Assistant's long-term statistics first and
        the recorder's readings second (module docstring); without, the
        readings alone.

        Raises :class:`ImportAlreadyRunning`, and whatever the client raises
        when the import cannot be started (TagHistorianImportRefused with the
        API's own sentence, an auth or connection error).
        """
        if self.running:
            raise ImportAlreadyRunning

        session = await self._client.async_start_import(hours=statistics)

        now = dt_util.utcnow()
        since = session.window_start
        if days is not None:
            since = max(since, now - timedelta(days=days))

        self.progress = ImportProgress(
            status=STATUS_RUNNING,
            import_id=session.import_id,
            started_at=_iso(session.started_at),
            window_start=_iso(session.window_start),
            since=_iso(since),
            entities=list(entity_ids),
            ends=self._live_lines(entity_ids),
            allowance_remaining=session.allowance.remaining,
            registry_ids=registry_ids(self.hass, entity_ids),
            statistics=statistics,
            phase=PHASE_STATISTICS if statistics else PHASE_READINGS,
            statistics_since=_iso(now - timedelta(days=min(days or MAX_IMPORT_DAYS, MAX_IMPORT_DAYS))),
            statistics_started_at=self.progress.statistics_started_at
            or (_iso(now) if statistics else None),
        )
        await self._async_save()
        if statistics:
            started = (
                f"Bringing the history Home Assistant already has for {len(entity_ids)} entities "
                "into Tag Historian: first its hourly long-term statistics, as far back as they "
                f"go, then the recorded readings from {since:%Y-%m-%d %H:%M} UTC."
            )
        else:
            started = (
                f"Importing the recorded history of {len(entity_ids)} entities into Tag Historian, "
                f"from {since:%Y-%m-%d %H:%M} UTC."
            )
        persistent_notification.async_create(
            self.hass,
            f"{started} Progress is on the \"Tag Historian history import\" sensor, and this "
            "notification says when it is done.",
            title="Tag Historian history import",
            notification_id=NOTIFICATION_ID.format(entry_id=self.entry.entry_id),
        )
        self._spawn()
        return self.progress

    def _live_lines(self, entity_ids: list[str]) -> dict[str, float]:
        """Per entity, where the live forwarder's part begins right now (see the module docstring)."""
        if self._boundary is None:
            return {}
        return {
            entity_id: line
            for entity_id in entity_ids
            if (line := self._boundary(entity_id)) is not None
        }

    def _spawn(self) -> None:
        self._task = self.entry.async_create_background_task(
            self.hass, self._async_run(), "tag_historian_history_import"
        )
        self._changed()

    async def async_stop(self) -> None:
        """Stop for an unload. Not the import's end: the next load carries on."""
        if self._task is not None and not self._task.done():
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task

    def _changed(self) -> None:
        async_dispatcher_send(self.hass, SIGNAL_IMPORT_UPDATED.format(entry_id=self.entry.entry_id))

    async def _async_save(self) -> None:
        await self._store.async_save(asdict(self.progress))

    # ------------------------------------------------------------------

    async def _async_run(self) -> None:
        progress = self.progress
        try:
            if progress.statistics and progress.phase == PHASE_STATISTICS:
                if not await self._async_statistics_phase():
                    return
                progress.phase = PHASE_READINGS
                progress.current = None
                await self._async_save()
                self._changed()

            for entity_id in progress.entities:
                if entity_id in progress.done:
                    continue
                progress.current = entity_id
                self._changed()
                if not await self._async_import_entity(entity_id):
                    return
                if self._renamed_meanwhile(entity_id):
                    # Not done: its reads may have come up empty because Home
                    # Assistant moved the history to the new id under them.
                    # The rename's reload stops this task and carries the
                    # import on under the new id, after the last reading Tag
                    # Historian answered for; until then it stays running.
                    LOGGER.info(
                        "Tag Historian: %s was renamed during the history import; "
                        "it carries on under the new id once the rename's reload is done",
                        entity_id,
                    )
                    return
                progress.done.append(entity_id)
                await self._async_save()
                self._changed()

            await self._async_finish(STATUS_FINISHED, "")
        except asyncio.CancelledError:
            raise
        except Exception as err:  # reported on the sensor and in the log
            LOGGER.exception("Tag Historian's history import failed")
            await self._async_finish(STATUS_STOPPED, STOP_UNREACHABLE, str(err))

    # ------------------------------------------------------------------
    # The statistics phase
    # ------------------------------------------------------------------

    async def _async_statistics_phase(self) -> bool:
        """Every entity's long-term statistics. False when the whole import has stopped.

        True when the phase is over - done, or stopped for a reason of its
        own (``statistics_stop_reason``) - and the readings phase is next.
        """
        progress = self.progress
        for entity_id in progress.entities:
            if entity_id in progress.statistics_done:
                continue
            progress.current = entity_id
            self._changed()
            outcome = await self._async_statistics_entity(entity_id)
            if outcome == _SESSION_STOPPED:
                return False
            if outcome == _PHASE_STOPPED:
                return True
            if self._renamed_meanwhile(entity_id):
                # As in the readings phase: not done, because its statistics
                # may have been moved to the new id under the reads. The
                # rename's reload carries the import on under the new id.
                LOGGER.info(
                    "Tag Historian: %s was renamed during the history import; "
                    "it carries on under the new id once the rename's reload is done",
                    entity_id,
                )
                return False
            progress.statistics_done.append(entity_id)
            await self._async_save()
            self._changed()
        return True

    def _statistics_floor(self) -> datetime:
        """The oldest hour the statistics phase reads: ``days`` back, and no older than Tag Historian keeps."""
        progress = self.progress
        floor = _parse(progress.statistics_since) or (
            dt_util.utcnow() - timedelta(days=MAX_IMPORT_DAYS)
        )
        if (oldest := _parse(progress.oldest_allowed)) is not None:
            floor = max(floor, oldest)
        return floor

    async def _async_statistics_entity(self, entity_id: str) -> str:
        """One entity's statistics, newest month first (module docstring)."""
        progress = self.progress
        started_at = _parse(progress.started_at)
        if started_at is None:
            await self._async_finish(STATUS_STOPPED, STOP_UNKNOWN_IMPORT)
            return _SESSION_STOPPED

        if entity_id in self._own_tag_since:
            # Its history is the old tag's, as for its readings.
            return _GO_ON

        # The tag its readings go to, and where Home Assistant keeps its
        # history - both as in the readings phase.
        former = self._send_as.get(entity_id)
        tag = tag_name_for_entity(former or entity_id)
        source = await history.async_source(self.hass, entity_id, former)
        plan = await statistics.async_plan(self.hass, source)
        if plan.missing:
            if entity_id not in progress.no_statistics:
                progress.no_statistics.append(entity_id)
            return _GO_ON
        if plan.reason:
            progress.statistics_left_out[entity_id] = plan.reason
            return _GO_ON

        floor = self._statistics_floor()
        # Where this entity's history ends in Tag Historian: unknown until its
        # first hour has been answered. A resumed entity is past that, and
        # everything older than its cursor is history.
        line: datetime | None = None
        month = statistics.month_start(started_at - HOUR)
        if (resumed := progress.hours_cursor.get(entity_id)) is not None:
            line = datetime.fromtimestamp(resumed, UTC)
            month = statistics.previous_month(line)

        pending: list[dict[str, Any]] = []
        pending_from = month
        while statistics.next_month(month) > floor:
            limit = line or started_at
            hours = await statistics.async_rows(
                self.hass, plan, max(month, floor), min(statistics.next_month(month), limit)
            )
            hours = [hour for hour in hours if hour["t"] + HOUR.total_seconds() <= limit.timestamp()]

            if line is None:
                if not hours:
                    month = statistics.previous_month(month)
                    continue
                # The newest hour alone: the answer says where the entity's
                # history ends, which for one Tag Historian has had live for
                # years is long before this month.
                outcome, result = await self._async_send_hours(entity_id, tag, plan, hours[-1:], probe=True)
                if outcome != _GO_ON or result is None:
                    return outcome
                if tag in result.refused_tags:
                    # The plan has no room for its tag; every hour would be refused.
                    return _GO_ON
                if result.refused_unit_mismatch:
                    progress.statistics_left_out[entity_id] = (
                        f"its statistics are in {plan.units or 'no unit'}, and its tag in Tag "
                        "Historian has another unit"
                    )
                    return _GO_ON
                if result.refused_unsupported:
                    progress.statistics_left_out[entity_id] = (
                        "Tag Historian does not take this kind of statistic"
                    )
                    return _GO_ON
                line = min(started_at, result.importable_before.get(tag, started_at))
                floor = self._statistics_floor()
                month = statistics.month_start(line - HOUR)
                continue

            if hours:
                if pending and len(pending) + len(hours) > MAX_HOURS_PER_REQUEST:
                    outcome, _ = await self._async_send_hours(entity_id, tag, plan, pending)
                    if outcome != _GO_ON:
                        return outcome
                    progress.hours_cursor[entity_id] = pending_from.timestamp()
                    await self._async_save()
                    pending = []
                # Whole months, older in front: a month is never split
                # between two requests.
                pending = hours + pending
                pending_from = month
            month = statistics.previous_month(month)

        if pending:
            outcome, _ = await self._async_send_hours(entity_id, tag, plan, pending)
            if outcome != _GO_ON:
                return outcome
            progress.hours_cursor[entity_id] = pending_from.timestamp()
            await self._async_save()
        return _GO_ON

    async def _async_send_hours(
        self,
        entity_id: str,
        tag: str,
        plan: statistics.EntityStatistics,
        hours: list[dict[str, Any]],
        *,
        probe: bool = False,
    ) -> tuple[str, HoursBatchResult | None]:
        """Send one batch of hours until it is answered for good, and count it.

        The outcome says whether the phase goes on, the phase has stopped, or
        the whole import has; the result is the answer when it was taken.
        """
        progress = self.progress
        body = await self.hass.async_add_executor_job(
            statistics.compress, {"series": [statistics.series(plan, tag, hours)]}
        )
        failures = 0
        # Hours a 503 said went in before it asked for the batch again: the
        # answer to the resend has them as duplicates, and they are counted
        # as stored, once.
        stored_before = 0

        while True:
            try:
                result = await self._client.async_import_hours(progress.import_id or "", body)
            except TagHistorianAuthError:
                progress.hours_stored += stored_before
                self.entry.async_start_reauth(self.hass)
                await self._async_finish(STATUS_STOPPED, STOP_AUTH)
                return _SESSION_STOPPED, None
            except TagHistorianConnectionError as err:
                failures += 1
                if failures >= IMPORT_MAX_CONNECTION_FAILURES:
                    progress.hours_stored += stored_before
                    await self._async_finish(STATUS_STOPPED, STOP_UNREACHABLE, str(err))
                    return _SESSION_STOPPED, None
                await asyncio.sleep(IMPORT_RETRY_SECONDS)
                continue

            if result.outcome is ImportOutcome.RETRY:
                stored_before += result.stored
                await asyncio.sleep(max(1, result.retry_after))
                continue
            break

        progress.hours_stored += stored_before
        if result.outcome is not ImportOutcome.TAKEN:
            session_reason = {
                ImportOutcome.FORBIDDEN: STOP_FORBIDDEN,
                ImportOutcome.UNKNOWN_IMPORT: STOP_UNKNOWN_IMPORT,
            }.get(result.outcome)
            if session_reason is not None:
                await self._async_finish(STATUS_STOPPED, session_reason, result.message)
                return _SESSION_STOPPED, None
            await self._async_stop_statistics(
                {
                    ImportOutcome.HOURS_ALLOWANCE_SPENT: STOP_HOURS_ALLOWANCE,
                    ImportOutcome.NOT_SUPPORTED: STOP_HOURS_UNSUPPORTED,
                }.get(result.outcome, STOP_REJECTED),
                result,
            )
            return _PHASE_STOPPED, None

        progress.hours_stored += result.stored
        # The probe's own hour, refused as not history, is this integration
        # asking where the line is - not an hour of the user's left out.
        skipped = result.skipped - min(result.duplicates, stored_before)
        if probe:
            skipped -= result.refused_not_history
        progress.hours_skipped += max(0, skipped)
        if result.stored + result.duplicates + stored_before > 0:
            oldest = float(min(hour["t"] for hour in hours))
            progress.hours_from[entity_id] = min(oldest, progress.hours_from.get(entity_id, oldest))
        for refused_tag in result.refused_tags:
            if refused_tag not in progress.refused_tags:
                progress.refused_tags.append(refused_tag)
        if result.oldest_allowed is not None:
            progress.oldest_allowed = _iso(result.oldest_allowed)
        await self._async_save()
        self._changed()

        if result.refused_hours_allowance:
            await self._async_stop_statistics(STOP_HOURS_ALLOWANCE, result)
            return _PHASE_STOPPED, result
        return _GO_ON, result

    async def _async_stop_statistics(self, reason: str, result: HoursBatchResult) -> None:
        """The statistics phase ends here, for a reason of its own; the readings phase is next."""
        progress = self.progress
        progress.statistics_stop_reason = reason
        progress.statistics_message = result.message
        progress.statistics_resumes_at = _iso(result.resumes_at)
        LOGGER.info(
            "Tag Historian's history import stopped bringing the long-term statistics (%s); "
            "it carries on with the recorded readings",
            reason,
        )
        await self._async_save()
        self._changed()

    # ------------------------------------------------------------------
    # The readings phase
    # ------------------------------------------------------------------

    async def _async_import_entity(self, entity_id: str) -> bool:
        """One entity, oldest first. False when the whole import has stopped."""
        progress = self.progress
        started_at = _parse(progress.started_at)
        since = _parse(progress.since)
        if started_at is None or since is None:
            await self._async_finish(STATUS_STOPPED, STOP_UNKNOWN_IMPORT)
            return False

        if entity_id in self._own_tag_since:
            # A tag of its own since the rename card said so; its history is
            # the old tag's (module docstring). Imported, the recorder's
            # history of it - all of which the old tag holds - went into the
            # new tag as well, which the card says it would not.
            LOGGER.info(
                "Tag Historian: %s has a tag of its own since %s and its history stays in "
                "the old tag, so the history import leaves it",
                entity_id,
                self._own_tag_since[entity_id],
            )
            return True

        # A restart carries on after the last reading Tag Historian answered
        # for; the minimum interval is measured from it too.
        answered = progress.cursor.get(entity_id)
        start = since
        if answered is not None:
            start = max(start, datetime.fromtimestamp(answered, UTC))
        # Where the forwarder's part begins: as it was when the import
        # started, and as it is now - a resumed import has a new forwarder.
        # Everything after either is the forwarder's to deliver, so the
        # earlier of the two holds.
        end = started_at
        for line in (progress.ends.get(entity_id), *self._live_lines([entity_id]).values()):
            if line is not None:
                end = min(end, datetime.fromtimestamp(line, UTC))
        last_kept = answered
        # The tag the entity's live readings go to - for a renamed entity
        # whose tag has not been renamed yet, its old one (module docstring) -
        # and where the recorder keeps its history, which for an entity
        # renamed onto an id with a history of its own is partly its old id.
        former = self._send_as.get(entity_id)
        tag = tag_name_for_entity(former or entity_id)
        source = await history.async_source(self.hass, entity_id, former)
        pending: list[tuple[float, dict[str, Any]]] = []
        window = timedelta(hours=RECORDER_WINDOW_HOURS)
        cursor = start
        # The first batch of an entity is its oldest reading alone. The answer
        # says where its history ends - the import's start, or its first live
        # reading - and an entity that has been forwarded live since before
        # the window is then done after one reading rather than one full
        # batch of readings that would all be refused.
        batch_size = 1 if answered is None else IMPORT_BATCH_SIZE

        while cursor < end:
            window_end = min(cursor + window, end)
            states = await history.async_states(
                self.hass, source, cursor - timedelta(microseconds=1), window_end
            )
            for state in states:
                stamp = state.last_updated
                epoch = stamp.timestamp()
                if stamp < cursor or stamp >= end or (answered is not None and epoch <= answered):
                    continue
                value = state_to_value(state.state, state.domain)
                if value is None:
                    continue
                if last_kept is not None and epoch - last_kept < self._min_interval:
                    continue
                last_kept = epoch
                pending.append((epoch, _reading(tag, stamp, value, state.attributes)))

                if len(pending) >= batch_size:
                    result = await self._async_send(entity_id, pending)
                    pending = []
                    batch_size = IMPORT_BATCH_SIZE
                    if result is None:
                        return False
                    if tag in result.refused_tags:
                        # The plan has no room for this tag; the rest of its
                        # history would be refused the same way.
                        return True
                    if (line := result.importable_before.get(tag)) is not None and line < end:
                        # Already live in Tag Historian from here on: the rest
                        # of this entity's recorder history is not history to
                        # it, and would only be refused.
                        end = line
                        if end <= cursor:
                            return True
            cursor = window_end

        if pending:
            result = await self._async_send(entity_id, pending)
            if result is None:
                return False
        return True

    async def _async_send(
        self, entity_id: str, pending: list[tuple[float, dict[str, Any]]]
    ) -> ImportBatchResult | None:
        """Send one batch until it is answered for good. None when the import has stopped."""
        progress = self.progress
        readings = [reading for _, reading in pending]
        failures = 0

        while True:
            try:
                result = await self._client.async_import_batch(progress.import_id or "", readings)
            except TagHistorianAuthError:
                self.entry.async_start_reauth(self.hass)
                await self._async_finish(STATUS_STOPPED, STOP_AUTH)
                return None
            except TagHistorianConnectionError as err:
                failures += 1
                if failures >= IMPORT_MAX_CONNECTION_FAILURES:
                    await self._async_finish(STATUS_STOPPED, STOP_UNREACHABLE, str(err))
                    return None
                await asyncio.sleep(IMPORT_RETRY_SECONDS)
                continue

            if result.outcome is ImportOutcome.RETRY:
                await asyncio.sleep(max(1, result.retry_after))
                continue
            break

        if result.outcome is not ImportOutcome.TAKEN:
            reason = {
                ImportOutcome.ALLOWANCE_SPENT: STOP_ALLOWANCE,
                ImportOutcome.STORAGE_FULL: STOP_STORAGE,
                ImportOutcome.FORBIDDEN: STOP_FORBIDDEN,
                ImportOutcome.UNKNOWN_IMPORT: STOP_UNKNOWN_IMPORT,
            }.get(result.outcome, STOP_REJECTED)
            await self._async_finish(STATUS_STOPPED, reason, result.message)
            return None

        progress.stored += result.stored
        progress.compressed += result.compressed
        progress.duplicates += result.duplicates
        progress.outside_window += result.outside_window
        progress.refused += result.refused
        progress.not_history += result.refused_not_history
        for tag in result.refused_tags:
            if tag not in progress.refused_tags:
                progress.refused_tags.append(tag)
        if result.allowance_remaining is not None:
            progress.allowance_remaining = result.allowance_remaining
        progress.cursor[entity_id] = pending[-1][0]
        await self._async_save()
        self._changed()

        if result.refused_allowance:
            await self._async_finish(STATUS_STOPPED, STOP_ALLOWANCE, result.message)
            return None
        if result.refused_storage:
            await self._async_finish(STATUS_STOPPED, STOP_STORAGE, result.message)
            return None
        return result

    async def _async_finish(self, status: str, reason: str, message: str = "") -> None:
        progress = self.progress
        progress.status = status
        progress.stop_reason = reason
        progress.message = message
        progress.current = None
        progress.finished_at = _iso(dt_util.utcnow())
        await self._async_save()
        self._changed()

        persistent_notification.async_create(
            self.hass,
            _summary(progress),
            title="Tag Historian history import",
            notification_id=NOTIFICATION_ID.format(entry_id=self.entry.entry_id),
        )


_STOP_SENTENCES = {
    STOP_ALLOWANCE: (
        "It stopped because the account's import allowance is used up: your plan's daily "
        "reading allowance times its full-resolution window. Live readings are not affected; "
        "a larger plan raises the allowance."
    ),
    STOP_STORAGE: (
        "It stopped because the account reached its plan's storage cap. Live readings are "
        "refused for the same reason until raw data ages out or the plan changes."
    ),
    STOP_FORBIDDEN: "It stopped because Tag Historian is not accepting data from this account.",
    STOP_AUTH: (
        "It stopped because Tag Historian no longer accepts the API key. Enter a new one; the "
        "import carries on where it stopped once it is accepted."
    ),
    STOP_UNREACHABLE: (
        "It stopped because Tag Historian could not be reached. It carries on where it stopped "
        "the next time Home Assistant starts, or run the import again: what Tag Historian "
        "already has is recognised and not stored twice."
    ),
    STOP_UNKNOWN_IMPORT: "It stopped because Tag Historian no longer knows this import. Start a new one.",
    STOP_REJECTED: "It stopped because Tag Historian refused a batch it could not read.",
}

# Why the statistics phase stopped before the readings; the readings phase ran
# all the same. The hourly allowance's sentence is _hours_allowance_sentence.
_STATISTICS_STOP_SENTENCES = {
    STOP_HOURS_UNSUPPORTED: (
        "The hourly statistics were not brought: this Tag Historian cannot take them yet."
    ),
    STOP_REJECTED: "Bringing the hourly statistics stopped: Tag Historian refused a batch it could not read.",
}

# Which entities have no long-term statistics at all, in Home Assistant's own
# terms: selection.py offers each of these, and none of them has a
# state_class. The website and the README say the same.
NO_STATISTICS_SENTENCE = (
    "Binary sensors, number and counter helpers, and sensors without a state_class have no "
    "long-term statistics."
)


def _month(epoch: float) -> str:
    """An hour as the month it is in, the way the summary says since when: "March 2021"."""
    return f"{datetime.fromtimestamp(epoch, UTC):%B %Y}"


def _hours_allowance_sentence(progress: ImportProgress) -> str:
    """The hourly history allowance, spent: what it is, and when the rest can go in."""
    resumes = _parse(progress.statistics_resumes_at)
    after = f"after {resumes:%Y-%m-%d}" if resumes is not None else "later"
    return (
        "Home Assistant's hourly history goes in at up to ten years per tag every 30 days; "
        f"the rest can go in {after}, by running the import again."
    )


def _statistics_summary(progress: ImportProgress) -> list[str]:
    """The statistics phase in words: what came, since when, and what did not and why."""
    lines = [
        f"{progress.hours_stored} hours of Home Assistant's long-term statistics imported."
    ]
    if progress.hours_from:
        lines.append(
            "Hourly history back to: "
            + ", ".join(
                f"{entity_id} ({_month(hour)})"
                for entity_id, hour in sorted(progress.hours_from.items())
            )
            + "."
        )
    if progress.hours_skipped:
        lines.append(
            f"{progress.hours_skipped} hours were already in Tag Historian, older than it keeps, "
            "or not taken, and were skipped."
        )
    if progress.no_statistics:
        lines.append(
            "No long-term statistics for: "
            + ", ".join(progress.no_statistics)
            + ". "
            + NO_STATISTICS_SENTENCE
        )
    for entity_id, reason in sorted(progress.statistics_left_out.items()):
        lines.append(f"The statistics of {entity_id} were left out: {reason}.")
    if progress.statistics_stop_reason == STOP_HOURS_ALLOWANCE:
        lines.append(_hours_allowance_sentence(progress))
    elif progress.statistics_stop_reason:
        lines.append(
            _STATISTICS_STOP_SENTENCES.get(progress.statistics_stop_reason, progress.statistics_message)
        )
        if progress.statistics_message and progress.statistics_stop_reason == STOP_REJECTED:
            lines.append(f"Tag Historian said: {progress.statistics_message}")
    return lines


def _summary(progress: ImportProgress) -> str:
    """What the import did, in the words the notification shows."""
    lines = _statistics_summary(progress) if progress.statistics else []
    lines.append(
        f"{progress.stored} readings imported for {len(progress.done)} of "
        f"{len(progress.entities)} entities."
    )
    if progress.duplicates:
        lines.append(f"{progress.duplicates} were already in Tag Historian and were skipped.")
    if progress.compressed:
        lines.append(
            f"{progress.compressed} were inside their tag's deadband and, as live readings "
            "would be, were not stored."
        )
    if progress.outside_window:
        lines.append(
            f"{progress.outside_window} were older than your plan's full-resolution window and "
            "were skipped."
        )
    if progress.not_history:
        lines.append(
            f"{progress.not_history} were already your live data - not older than their entity's "
            "first reading in Tag Historian, the first import into it, or this import's start - "
            "and were not imported."
        )
    if progress.refused_tags:
        lines.append(
            "No tag could be created for: " + ", ".join(progress.refused_tags) + " - your plan "
            "has no room for more tags."
        )
    if progress.status == STATUS_STOPPED:
        lines.append(_STOP_SENTENCES.get(progress.stop_reason, progress.message))
        if progress.message and progress.stop_reason in (STOP_FORBIDDEN, STOP_REJECTED):
            lines.append(f"Tag Historian said: {progress.message}")
    return "\n\n".join(lines)


def _reading(tag: str, stamp: datetime, value: float, attributes: Any) -> dict[str, Any]:
    """One reading for the import endpoint, in the shape of POST /api/measurements/batch.

    To the second, as the live forwarder sends it (``precision=s``), so an
    imported history and a forwarded one have the same resolution. The friendly
    name and unit ride along as the live ones do; the endpoint only uses them
    for a tag that has none yet.
    """
    reading: dict[str, Any] = {
        "tagName": tag,
        "timestamp": datetime.fromtimestamp(int(stamp.timestamp()), UTC)
        .isoformat()
        .replace("+00:00", "Z"),
        "value": value,
    }
    description = _attribute_text(attributes.get("friendly_name"), MAX_FRIENDLY_NAME_LENGTH)
    units = _attribute_text(attributes.get("unit_of_measurement"), MAX_UNIT_LENGTH)
    if description is not None:
        reading["description"] = description
    if units is not None:
        reading["units"] = units
    return reading
