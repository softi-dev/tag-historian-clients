"""Import the history Home Assistant already recorded into Tag Historian.

The recorder's database - often on an SD card, the part of a Home Assistant
box most likely to fail - holds days of readings from before this integration
was installed. This sends them to Tag Historian's history import
(``POST /api/import``) with their original timestamps.

What Tag Historian holds an import to, and what this therefore stops at:

* the plan's full-resolution window - older readings are skipped, so the
  import starts there rather than sending what would be thrown away;
* history only: readings older than the moment the import started, and for
  an entity that is already forwarded live, older than its first live reading
  (the API says where that line is in every answer, and the import of that
  entity ends there);
* the account's import allowance, which imported readings are counted against
  INSTEAD of the daily one; when it is used up the import stops and says so;
* the tag limit and the storage cap, as for live data.

What it sends is what the live forwarder would have sent: the same numbers and
two-state words, ``unavailable`` left as a gap, the same minimum interval
between two readings of one entity, timestamps to the second. Only the
recorder's own states are read - Home Assistant's long-term statistics (the
hourly mean/min/max it keeps after the recorder purges its states) are not:
they are not readings, and nothing here pretends they are. The README says so.

RESUMABLE. Where the import is - which entities are done, and how far into
each - is saved in ``.storage`` after every batch, so a restart picks it up
where it stopped, under the same import (and so behind the same line). A batch
sent twice is harmless: the import endpoint skips a reading it already has.
"""

from __future__ import annotations

import asyncio
import contextlib
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

from homeassistant.components import persistent_notification
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.dispatcher import async_dispatcher_send
from homeassistant.helpers.storage import Store
from homeassistant.util import dt as dt_util

from . import history
from .api import (
    ImportBatchResult,
    ImportOutcome,
    TagHistorianAuthError,
    TagHistorianClient,
    TagHistorianConnectionError,
)
from .const import (
    IMPORT_BATCH_SIZE,
    IMPORT_MAX_CONNECTION_FAILURES,
    IMPORT_RETRY_SECONDS,
    LOGGER,
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
    stored: int = 0
    compressed: int = 0
    duplicates: int = 0
    outside_window: int = 0
    refused: int = 0
    # Of ``refused``: readings that were already live data - not older than
    # their entity's first live reading or the import's start.
    not_history: int = 0
    refused_tags: list[str] = field(default_factory=list)
    allowance_remaining: int | None = None
    stop_reason: str = ""
    message: str = ""
    finished_at: str | None = None

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
    ) -> None:
        self.hass = hass
        self.entry = entry
        self._client = client
        self._store = store
        self._min_interval = min_interval
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
        if self.progress.status == STATUS_RUNNING and self.progress.import_id:
            LOGGER.info(
                "Tag Historian is carrying on with the history import it was running "
                "before Home Assistant stopped"
            )
            self._spawn()

    async def async_start(self, entity_ids: list[str], days: int | None) -> ImportProgress:
        """Start importing these entities' recorded history, at most ``days`` back.

        Raises :class:`ImportAlreadyRunning`, and whatever the client raises
        when the import cannot be started (TagHistorianImportRefused with the
        API's own sentence, an auth or connection error).
        """
        if self.running:
            raise ImportAlreadyRunning

        session = await self._client.async_start_import()

        since = session.window_start
        if days is not None:
            since = max(since, dt_util.utcnow() - timedelta(days=days))

        self.progress = ImportProgress(
            status=STATUS_RUNNING,
            import_id=session.import_id,
            started_at=_iso(session.started_at),
            window_start=_iso(session.window_start),
            since=_iso(since),
            entities=list(entity_ids),
            allowance_remaining=session.allowance.remaining,
        )
        await self._async_save()
        persistent_notification.async_create(
            self.hass,
            f"Importing the recorded history of {len(entity_ids)} entities into Tag Historian, "
            f"from {since:%Y-%m-%d %H:%M} UTC. Progress is on the "
            f"\"Tag Historian history import\" sensor, and this notification says when it is done.",
            title="Tag Historian history import",
            notification_id=NOTIFICATION_ID.format(entry_id=self.entry.entry_id),
        )
        self._spawn()
        return self.progress

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
            for entity_id in progress.entities:
                if entity_id in progress.done:
                    continue
                progress.current = entity_id
                self._changed()
                if not await self._async_import_entity(entity_id):
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

    async def _async_import_entity(self, entity_id: str) -> bool:
        """One entity, oldest first. False when the whole import has stopped."""
        progress = self.progress
        started_at = _parse(progress.started_at)
        since = _parse(progress.since)
        if started_at is None or since is None:
            await self._async_finish(STATUS_STOPPED, STOP_UNKNOWN_IMPORT)
            return False

        # A restart carries on after the last reading Tag Historian answered
        # for; the minimum interval is measured from it too.
        answered = progress.cursor.get(entity_id)
        start = since
        if answered is not None:
            start = max(start, datetime.fromtimestamp(answered, UTC))
        end = started_at
        last_kept = answered
        tag = tag_name_for_entity(entity_id)
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
            states = await history.async_recorded_states(
                self.hass, entity_id, cursor - timedelta(microseconds=1), window_end
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
    STOP_AUTH: "It stopped because Tag Historian no longer accepts the API key. Enter a new one.",
    STOP_UNREACHABLE: (
        "It stopped because Tag Historian could not be reached. Run the import again when it "
        "can be: readings it already has are recognised and not stored twice."
    ),
    STOP_UNKNOWN_IMPORT: "It stopped because Tag Historian no longer knows this import. Start a new one.",
    STOP_REJECTED: "It stopped because Tag Historian refused a batch it could not read.",
}


def _summary(progress: ImportProgress) -> str:
    """What the import did, in the words the notification shows."""
    lines = [
        f"{progress.stored} readings imported for {len(progress.done)} of "
        f"{len(progress.entities)} entities."
    ]
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
            "first reading in Tag Historian, or than the import's start - and were not imported."
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
