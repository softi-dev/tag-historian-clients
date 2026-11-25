"""Watch the selected entities and write their readings to Tag Historian.

Two things separate this from the built-in influxdb integration, and both are
deliberate:

* it subscribes to EXACTLY the selected entities via
  ``async_track_state_change_event`` rather than filtering the whole
  ``state_changed`` bus in a callback. Home Assistant's own guidance is
  explicit about the unfiltered form being a performance problem, and the
  built-in integration only does it that way because it predates the helper;

* it reads ``Retry-After`` and it tells 422 apart from 429 apart from 503 -
  and apart from the OTHER 429, which is our write buffer being busy rather
  than the customer's allowance being spent. The built-in integration never
  reads the header and has no notion of partial success: a refused batch is
  sent again only if its ``max_retries`` is set, 20 seconds apart, and is
  then dropped - and set up for InfluxDB 2.x it does not wait for the answer
  at all. So an over-quota Home Assistant simply stops producing data with
  nothing anywhere to say why.

The buffer SURVIVES A RESTART. When Home Assistant stops, or the entry is
reloaded, whatever could not be delivered is saved to ``.storage`` (bounded
by MAX_QUEUE_LENGTH, like the buffer itself) and sent when it starts again -
see :meth:`StateForwarder.async_shutdown`, which an unload and a Home
Assistant stop both reach, by different doors and with the same grace.

And a reading the buffer had to let go of is not gone: Home Assistant's
recorder still has it. For every entity the forwarder keeps the timestamp of
the last reading Tag Historian acknowledged, and after an outage - a queue
that overflowed, a restart, an entry that was disabled for a while - it reads
the recorder for what lies between that reading and the oldest one it still
holds, and sends that first (:meth:`StateForwarder._async_backfill`). That is
delayed LIVE data: it goes to the same endpoint and counts against the daily
allowance like any reading. A reading let go of while that allowance was
spent is the exception: it is over the allowance, not lost to an outage, and
is not read back (:meth:`StateForwarder._forfeit`). It does not re-send a
reading Tag Historian may already have, because the live endpoint does not
deduplicate: where that is IN DOUBT - a batch whose answer was lost, or a run
that did not stop cleanly - Tag Historian is asked first how new its newest
reading of each tag is (:meth:`StateForwarder._async_settle_doubt`), and only
what is newer than that is sent.

A RENAMED entity (renames.py) keeps all of that. Its readings - live, read
back or saved - go into the tag its history is in (``send_as``) until Tag
Historian has the new name too; and what was saved under its old id, buffer
and watermark, is taken up under its new one when the rename's own reload
reads it back (:meth:`StateForwarder.async_restore`). Home Assistant moves
the recorded history to the new id at the rename, so that is where a gap is
read back from - except after a rename onto an id with a recorded history of
its own, where the part before the rename is still under the old id
(history.py).

Everything it cannot place still moves a counter, because a reading that
vanishes without one is indistinguishable from a sensor that stopped
reporting. That invariant covers the batch that is already on the wire as
well as the ones still queued - see :meth:`StateForwarder.async_flush`, whose
``finally`` is the only thing standing between a cancelled request and forty
readings nobody can account for.

NO ENTITY is left on a dropped reading. The minimum interval is about how many
readings are sent, never about which value an entity ends on: Home Assistant
says nothing more of a state that stays, so a reading dropped for coming too
soon could be the last one for hours - a power that went to 0 W a moment later
and stood at what it was before, the door that closed again.
So the newest reading held back is sent once the interval is up, with its own
time (:meth:`StateForwarder._hold_trailing`), and the catch-up from the
recorder does the same (:meth:`StateForwarder._async_backfill_entity`). It is
still at most one reading per interval.

Timing note: everything scheduled here goes through ``async_call_later``, and
throttling compares the readings' OWN timestamps rather than a wall clock. Both
choices are about being able to test the behaviour by moving Home Assistant's
clock, which is the only way "backs off for the duration the header asked for"
is provable rather than asserted.
"""

from __future__ import annotations

import asyncio
import math
from collections import deque
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from functools import partial
from typing import Any, NamedTuple

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import CALLBACK_TYPE, Event, HassJob, HomeAssistant, State, callback
from homeassistant.helpers.event import (
    EventStateChangedData,
    async_call_later,
    async_track_state_change_event,
)
from homeassistant.helpers.storage import Store
from homeassistant.util import dt as dt_util

from . import history
from .api import (
    TagHistorianAnsweredError,
    TagHistorianAuthError,
    TagHistorianClient,
    TagHistorianConnectionError,
    TagHistorianError,
    TagHistorianPermissionError,
    TagHistorianUnansweredError,
    WriteOutcome,
    WriteResult,
)
from .const import (
    BACKFILL_BATCH_SIZE,
    BATCH_BUFFER_SIZE,
    BATCH_TIMEOUT_SECONDS,
    LOGGER,
    LOSS_SAVE_SECONDS,
    MAX_POINTS_PER_REQUEST,
    MAX_QUEUE_LENGTH,
    RECORDER_WAIT_SECONDS,
    RECORDER_WINDOW_HOURS,
    WATERMARK_SAVE_SECONDS,
)
from .line_protocol import (
    encode_batch,
    encode_point,
    is_boolean_domain,
    is_missing_state,
    recorded_state_value,
    retag_line,
    state_to_value,
    tag_name_for_entity,
)
from .renames import registry_ids, removed_since, renamed_since
from .selection import is_power_sensor

# How long to wait after a connection error before trying again. Shorter than
# any Retry-After the API sends, because a socket error is usually a blip. The
# same wait is used for a 429 or 503 that names no wait (_answered_wait).
CONNECTION_RETRY_SECONDS = 30


def _answered_wait(retry_after: int) -> float:
    """How long to wait after a 429 or a 503, whether it said how long or not.

    Tag Historian always asks for a second or more, and that is honoured
    exactly. No header, a 0, or one that does not parse is in practice
    something in front of it answering in its place - a gateway with nothing
    behind it - which says no more than a host that did not answer: it is
    waited for as long as one, not asked again every second for as long as
    the outage lasts, by every Home Assistant at once. (The daily quota's
    countdown reaching 0 at midnight costs half a minute at most.)
    """
    return retry_after if retry_after >= 1 else CONNECTION_RETRY_SECONDS


# How many times in a row Tag Historian may answer the question of what it has
# with a list that cannot be used - one that is not the list, or that gives no
# usable time for what is asked about - before it is taken to be unable to say.
# An error is not that: it is waited out for as long as the write path waits
# (SERVER_ERROR_GIVE_UP_SECONDS), and a 403 for this long. A host that cannot
# be reached, a 429 and a 503 do not count at all: those pass.
SETTLE_ATTEMPTS = 5
SETTLE_REFUSED_SECONDS = 60 * 60

# How long after a write whose answer never came "I do not have it" is asked
# about again rather than believed: a gateway may still be holding the request,
# and hand it over after the question.
UNANSWERED_SETTLE_SECONDS = 120

# The most one error adds to how long there has been nothing but errors: the
# time up to it, but not a long stretch in which nothing was asked.
ERROR_STEP_SECONDS = 120

# How often, at most, an error that goes on is in the log as one.
ERROR_LOG_SECONDS = 60 * 60

# How many times, and how far apart, a gap the recorder could not be read for
# is tried again. Live readings wait behind it meanwhile. Then it is let go of,
# counted.
RECORDER_TRIES = 10
RECORDER_RETRY_SECONDS = 30

# How many times one reading by itself is answered with an error, each time
# with another entity's taken after it, before it is let go of. That is what
# tells a reading that cannot be taken from a Tag Historian that takes none,
# or fails now and then (StateForwarder._handle_server_error). Where only its
# own entity has readings waiting, the next of those can be tried once, and is
# after this many errors times as many.
SERVER_ERROR_TRIES = 3

# And where there is none to try either, and other readings wait behind it: how
# many errors, with the question of what Tag Historian has answered before each.
# An hour of them, at CONNECTION_RETRY_SECONDS.
SERVER_ERROR_LONE_TRIES = 120

# How long nothing but errors - of every batch, whatever is in it - is waited
# out before the batch is let go of, counted. A Tag Historian that is broken
# for hours is waited for; readings behind one it can never take are not held
# up for good.
SERVER_ERROR_GIVE_UP_SECONDS = 24 * 60 * 60

# The counters that are saved with the buffer, so that a loss is still visible
# after a restart.
SAVED_COUNTERS = (
    "dropped_overflow",
    "dropped_quota",
    "dropped_shutdown",
    "dropped_inflight",
    "dropped_not_numeric",
    "gaps_not_read",
)

# How long a stop goes on starting requests, from when it began. One started
# then can take HTTP_TIMEOUT_SECONDS more, and Home Assistant cancels what is
# not done in its 20 seconds for every shutdown job put together.
STOP_BUDGET_SECONDS = 6

# How many requests the shutdown flush is allowed to make. Home Assistant is
# waiting on this, so the buffer gets a few batches and not a drain loop; the
# client's own HTTP timeout bounds each one. What they do not deliver is
# saved, not lost.
FINAL_FLUSH_ATTEMPTS = 5

# How long the shutdown flush waits for a request that is ALREADY on the wire
# before it stops holding Home Assistant up.
#
# Half the client's own per-request timeout, and it sits between three hard
# numbers: aiohttp gives a single write HTTP_TIMEOUT_SECONDS (10) to answer;
# Home Assistant's unload waits 10 seconds on the tasks an entry leaves behind
# before it logs that they did not finish; and a Home Assistant STOP gives
# every shutdown job put together STOPPING_STAGE_SHUTDOWN_TIMEOUT (20) before
# it cancels the lot and carries on. Five seconds of waiting plus a few final
# requests fits inside all three. A write that has not answered in five seconds
# is one a restart should not be held up by; a write that is going to answer
# almost always has by then, and then the readings are delivered instead of
# counted.
INFLIGHT_WAIT_SECONDS = 5

# How many entity ids the "nothing storable here" hint keeps. It is an
# attribute on a diagnostic sensor, meant to name the offender, not to be a
# log of every entity in a badly configured house.
MAX_UNSTORABLE_NAMES = 10

# The state classes of a meter: a reading that only grows, or one that grows
# and starts over (Home Assistant's SensorStateClass). A meter's held reading
# goes out even when its value is the one sent last: its last reset may not be.
METER_STATE_CLASSES = frozenset({"total", "total_increasing"})

# The last microsecond of one.
END_OF_SECOND = 1 - 1e-6


def _is_meter(state: State) -> bool:
    """Whether Home Assistant says this entity is a meter (its state_class)."""
    return str(state.attributes.get("state_class") or "") in METER_STATE_CLASSES


def _says_nothing_new(state: State, value: float, last_value: float | None) -> bool:
    """Whether a reading held back is only what was sent last, over again.

    Only an attribute changed, or it went and came back within the interval:
    Tag Historian is on that value already.
    """
    return value == last_value and not _is_meter(state)


def _floats(saved: Any) -> dict[str, float]:
    """What is a number in something read back from the file, whatever the rest of it is."""
    if not isinstance(saved, dict):
        return {}
    return {
        str(key): float(value)
        for key, value in saved.items()
        if isinstance(value, int | float) and not isinstance(value, bool)
    }


def _throttle_from(stamp: float) -> float:
    """What the minimum interval runs from, after a watermark.

    One taken from Tag Historian's own newest reading is the end of the second
    it was sent in (:meth:`StateForwarder._async_settle_doubt`), and the
    reading was made somewhere in it.
    """
    second = math.floor(stamp)
    return second if stamp - second > END_OF_SECOND - 1e-6 else stamp


class Queued(NamedTuple):
    """One encoded reading, with what the acknowledged-watermark needs to know about it."""

    entity_id: str
    # The reading's own timestamp (``State.last_updated``) as epoch seconds,
    # full precision - the recorder is asked "after this" with exactly it.
    timestamp: float
    line: str
    # The entity whose tag ``line`` is encoded for, when it is not this one's
    # own: a renamed entity's old id while its tag has not been renamed yet
    # (renames.py). Saved with the reading, so a reading still held when the
    # rename is settled can be sent where the entity's readings go by then.
    sent_as: str = ""

    @property
    def encoded_for(self) -> str:
        return self.sent_as or self.entity_id


def _in_doubt(batch: list[Queued]) -> dict[str, tuple[float, str, int]]:
    """Per entity of a batch: its newest reading, the entity its tag is named for, and how many."""
    doubt: dict[str, tuple[float, str, int]] = {}
    for item in batch:
        stamp, _, count = doubt.get(item.entity_id, (item.timestamp, "", 0))
        doubt[item.entity_id] = (max(stamp, item.timestamp), item.encoded_for, count + 1)
    return doubt


class ErrorClock:
    """For how long there has been nothing but errors, not counting time nothing was asked in."""

    def __init__(self) -> None:
        self.seconds = 0.0
        self._last: float | None = None

    def error(self) -> float:
        now = dt_util.utcnow().timestamp()
        if self._last is not None:
            self.seconds += min(max(0.0, now - self._last), ERROR_STEP_SECONDS)
        self._last = now
        return self.seconds

    def interrupted(self) -> None:
        """Something that says nothing either way: a host that did not answer."""
        self._last = None

    def reset(self) -> None:
        self.seconds = 0.0
        self._last = None


@dataclass
class ForwarderStats:
    """Counters the diagnostic sensors read.

    Every reading that does not reach Tag Historian moves exactly one of these.
    Silence is the failure mode this design refuses: a drop nobody can see is
    indistinguishable from a sensor that stopped reporting.

    The instance OUTLIVES the forwarder that fills it. A reload - an options
    change, or the user fixing the tag-quota repair - builds a new forwarder,
    and counters that restarted from zero there would turn "how much have I
    lost today" into "how much have I lost since the last time I touched the
    settings", silently, at exactly the moment somebody was investigating a
    loss. See :func:`.async_get_stats`.
    """

    sent: int = 0
    queued: int = 0
    dropped_overflow: int = 0
    dropped_throttled: int = 0
    dropped_quota: int = 0
    # Readings that could neither be delivered nor saved when the forwarder
    # stopped: a write that came back asking to be re-queued after the buffer
    # had already been saved, or a save that failed. Rare by construction -
    # what the last flush cannot deliver is saved to .storage instead.
    dropped_shutdown: int = 0
    # Readings that may have been stored and may not, and will not be sent
    # again because nobody can say: a request that never came back, or the
    # last moments before a crash, when Tag Historian could not tell what it
    # has. Separate from dropped_shutdown because it is the only counter that
    # can overstate. See :meth:`StateForwarder._async_settle_doubt`.
    dropped_inflight: int = 0
    # Readings the entity genuinely produced and this integration cannot
    # store: a state that is a word rather than a number, and not one of the
    # words that IS a two-state signal in that entity's domain - which covers
    # both `climate` at "heat", where no word would have been, and `cover` at
    # "opening", where two words are and this was neither. NOT the same as
    # "unavailable", which is Home Assistant saying it has nothing to report
    # and is correctly a gap. See :func:`.line_protocol.state_to_value`.
    dropped_not_numeric: int = 0
    # Gaps, of one entity each, that the recorder could not be read for, time
    # after time. How many readings were in them nobody knows, so this is not
    # part of the readings dropped.
    gaps_not_read: int = 0
    # Readings read back from the recorder to fill a gap - an overflow, a
    # restart, a stretch with the entry disabled - and queued to send. Part of
    # ``sent`` once delivered; counted here so the gap they filled is visible.
    backfilled: int = 0
    # Readings saved to .storage when the forwarder last stopped and put back
    # in the buffer when it started again.
    restored: int = 0
    last_error: str = ""
    refused_tags: list[str] = field(default_factory=list)
    # Which entities produced the states above, so the diagnostic sensor can
    # name them. Bounded: this is a hint on an attribute, not a log.
    unstorable_entities: list[str] = field(default_factory=list)
    # Whether the saved counters have been added, once per Home Assistant run.
    loaded: bool = False


class StateForwarder:
    """Buffers state changes for one config entry and flushes them."""

    def __init__(
        self,
        hass: HomeAssistant,
        entry: ConfigEntry,
        client: TagHistorianClient,
        entity_ids: list[str],
        min_interval: int,
        stats: ForwarderStats | None = None,
        store: Store[dict[str, Any]] | None = None,
        send_as: dict[str, str] | None = None,
        power_interval: int | None = None,
    ) -> None:
        self.hass = hass
        self.entry = entry
        self.stats = stats if stats is not None else ForwarderStats()
        self._client = client
        self._entity_ids = list(entity_ids)
        # An entity renamed in Home Assistant whose tag has not been renamed
        # yet is sent under its OLD id, so its readings go on into the history
        # they belong to instead of starting a second tag (renames.py).
        self._send_as = dict(send_as or {})
        self._min_interval = min_interval
        # The interval of a power sensor (device class power, W or kW) when
        # the user asked for those to be sent more often (CONF_FAST_POWER);
        # None when they did not, or while their sum does not fit the plan's
        # daily allowance (fast_power.FastPowerGuard, through
        # set_power_interval). Never longer than the minimum interval.
        self._power_interval = power_interval
        # The plan's daily allowance as last read, kept across a restart: the
        # guard goes by it until it is read again.
        self.daily_limit: int | None = None
        # Where the buffer goes when the forwarder stops, and where the
        # acknowledged watermark is kept across a restart. None only in tests
        # that are about something else; Home Assistant always gets one.
        self._store = store
        self._queue: deque[Queued] = deque()
        # Readings read back from the recorder. Sent BEFORE the live queue,
        # and while a backfill pass runs, instead of it: per entity, readings
        # then reach Tag Historian in time order, which is what lets
        # ``_acked`` be a watermark rather than a guess.
        self._backfill: deque[Queued] = deque()
        # Per entity, the timestamp of the newest reading Tag Historian has
        # answered for - delivered, or refused for good (a 422, a 403).
        # Everything up to it is accounted for; a gap is what lies between it
        # and the oldest reading still held.
        self._acked: dict[str, float] = {}
        # {entity_id: entity registry id} for what is saved, so the next run
        # finds a renamed entity's buffer and watermark under its new id.
        # Taken while the ids are current: by the time a rename's reload saves,
        # the registry no longer knows the old one.
        self._registry_ids: dict[str, str] = {}
        self._last_sent: dict[str, datetime] = {}
        # The value of the newest one, to tell a held reading that says no more.
        self._last_value: dict[str, float] = {}
        # Per entity, the newest reading the interval held back and the
        # timer that sends it (_hold_trailing). At most one of each per
        # selected entity; both go when it is sent, let go of, or stopped.
        self._trailing: dict[str, tuple[State, float]] = {}
        self._trailing_timers: dict[str, CALLBACK_TYPE] = {}
        self._unsub_state: CALLBACK_TYPE | None = None
        self._unsub_timer: CALLBACK_TYPE | None = None
        self._flushing = False
        # The batch that is off the queue and on the wire right now, and the
        # task carrying it. Both exist so that a request which never returns
        # can still be accounted for by whoever notices - the request's own
        # ``finally``, or the shutdown that waited for it.
        self._inflight: list[Queued] | None = None
        self._inflight_from_backfill = False
        self._flush_task: asyncio.Task[None] | None = None
        # Set once the shutdown flush has been and gone. After that the queue
        # is a dead end - nothing will ever flush it again - so anything that
        # tries to put readings back into it has to be counted instead.
        self._closed = False
        # Set once a shutdown has begun: from then on only the shutdown itself
        # writes to .storage, so a save started by an outage beginning or
        # ending cannot land after - and over - the buffer it saved.
        self._stopping = False
        # Set while the API has told us to wait. The queue keeps filling (and
        # evicting) meanwhile; only sending is held.
        self._paused = False
        self._unavailable_since: datetime | None = None
        self._unsub_stop: CALLBACK_TYPE | None = None
        # When this forwarder started listening: where the gap of an entity
        # nothing has been acknowledged for begins.
        self._listening_since: float = dt_util.utcnow().timestamp()
        # The same for an entity nothing has been acknowledged for in an
        # earlier run either: when the first of them started listening.
        self._since: dict[str, float] = {}
        # Listening and buffering, sending nothing: until Tag Historian has
        # been reached for the first time.
        self._holding = False
        # How many of the readings at the front of the buffer each earlier run
        # left, oldest run first. Between two of them - and between the last
        # and the live ones - is a stretch nobody listened through, so each is
        # sent by itself and followed by a read-back (_tidy_segments).
        self._segments: deque[int] = deque()
        self._inflight_restored = False
        # What may have been stored and may not: per entity, the newest such
        # reading, the entity its tag is named for, and how many there are.
        # Nothing is sent before Tag Historian has been asked.
        self._doubt: dict[str, tuple[float, str, int]] = {}
        # A crash: everything since the watermark may have been stored - or,
        # when Tag Historian cannot say, everything up to _doubt_until.
        self._doubt_all = False
        self._doubt_until = 0.0
        # The crashed run was one that did not save as it went: how much of
        # what is after its watermark was delivered, nobody can guess at.
        self._doubt_old = False
        self._settle_failures = 0
        self._settle_errors = ErrorClock()
        # Before this, "I do not have it" is not believed yet.
        self._unanswered_until = 0.0
        # Per entity, the end of a stretch that is counted, not sent - and the
        # readings in it that were held as well, and are counted already.
        self._doubtful_until: dict[str, float] = {}
        self._counted: dict[str, set[float]] = {}
        # After a crash, per entity: Tag Historian's newest second, what was
        # acknowledged before, and the held readings of that second. Where
        # there are several, it may have only one of them.
        self._same_second: dict[str, tuple[float, float, set[float]]] = {}
        # Whether a batch on the wire when the buffer was saved is in the save.
        self._inflight_saved = False
        # Stopped with a gap not read back to its end: live readings must not
        # go past it.
        self._cut_short = False
        # The final save is being put together: nothing it says is to change.
        self._sealed = False
        self._stop_by: float | None = None
        self._recorder_written = False
        self._unsub_recorder: CALLBACK_TYPE | None = None
        # Whether the reading just popped came after a stretch nobody listened
        # through and nobody has read back yet.
        self._popped_past_hole = False
        self._unsub_save: CALLBACK_TYPE | None = None
        self._save_due = 0.0
        # What the file says of _sending_ok, and when that last changed.
        self._disk_sending_ok = False
        self._state_saved_at = float("-inf")
        # An answer that was an error of Tag Historian's own, and how far
        # taking it apart has come (_handle_server_error).
        self._errors_since: float | None = None
        self._errors = ErrorClock()
        self._error_logged_at = float("-inf")
        self._error_limit: int | None = None
        self._error_batch = 0
        self._error_tries = 0
        self._error_proofs = 0
        self._error_answered = False
        self._suspect: Queued | None = None
        self._last_failed: Queued | None = None
        self._probed: set[str] = set()
        self._probing = False
        # Whether writes are going through. Saved when it changes, because the
        # watermark on disk is exact only while it is false - see async_restore.
        #
        # FALSE until this run's first write is answered. It started out true,
        # and async_restore saved that at once, before this run had sent
        # anything: a crash in the seconds before the first answer - a boot
        # loop, a second power flicker, Tag Historian still down - then read
        # as "crashed while writes were going through", and the next start
        # dropped the watermark and read nothing back. The buffer the clean
        # stop had saved, and the whole outage before it, were gone with no
        # counter moving. Nothing can have been acknowledged since the saved
        # watermark until something has been answered, so false is the truth.
        self._sending_ok = False
        # Set while Tag Historian's last answer was that today's reading
        # allowance is spent. A reading the full buffer lets go of meanwhile is
        # over the allowance, not lost to an outage, and is not read back -
        # see :meth:`_forfeit`.
        self._over_allowance = False
        self._backfill_task: asyncio.Task[None] | None = None
        self._backfill_running = False
        # The entities a backfill pass has yet to finish with.
        self._unread: set[str] = set()
        # Set when the buffer evicted readings. A backfill pass starts at
        # once - it reads the recorder now and sends when Tag Historian takes
        # data again - and live readings wait behind it, because a live one
        # delivered first would move the watermark past the evicted ones.
        self._backfill_wanted = False
        self._backfill_room = asyncio.Event()
        self._backfill_room.set()
        # Late import so that pulling in homeassistant.components.repairs (and
        # everything it drags with it) waits until an entry is actually being
        # set up, rather than happening when the integration is merely loaded.
        from . import repairs

        self._repairs = repairs

    # ------------------------------------------------------------------
    # Starting: what the last run left, and the gap since
    # ------------------------------------------------------------------

    async def async_restore(self) -> None:
        """Pick up what the previous run saved, before :meth:`async_start`.

        Two kinds of previous run, told apart by the ``clean`` flag:

        * it STOPPED - an unload or a Home Assistant stop - and saved its
          buffer and watermark on the way out. The buffer comes back, and the
          watermark is exact: what is between it and the oldest restored
          reading is a gap the recorder can fill.

        * it did NOT stop: a crash, a power cut, a kill. Its buffer is gone -
          the recorder has it - and the watermark on disk is at most some
          minutes old, so some readings after it may already be in Tag
          Historian. It is asked what it has before anything is sent
          (:meth:`_async_settle_doubt`), and everything newer is read back.

        The store is then marked as a run in progress at once, so a crash
        after this does not bring back - and send twice - a buffer this run
        has already sent. The restored readings stay in the file, under
        another key, until they are delivered: for an entity the recorder
        leaves out they are the only copy.

        An entity renamed since the save (renames.py) is picked up under its
        new id: its watermark, so its gap is read back from where Tag
        Historian's answers ended rather than from now, and its held
        readings, so they are its own when the history import asks where the
        forwarder's part begins - sent to the tag it sends to now, which may
        have changed with the rename settled (:meth:`_restored`).
        """
        if self._store is None:
            return

        try:
            saved = await self._store.async_load() or {}
        except Exception:  # a corrupt file must not stop setup
            LOGGER.exception("Tag Historian could not read its saved buffer; starting empty")
            saved = {}
        if not isinstance(saved, dict):
            saved = {}
        self._add_saved_counters(saved.get("counters"))
        now = dt_util.utcnow().timestamp()
        limit = saved.get("daily_limit")
        if isinstance(limit, int) and not isinstance(limit, bool):
            self.daily_limit = limit

        saved_ids = {str(k): str(v) for k, v in (saved.get("registry_ids") or {}).items()}
        renamed = renamed_since(self.hass, saved_ids, self._entity_ids)
        for old, new in renamed.items():
            LOGGER.info(
                "Tag Historian: %s was renamed to %s; what was saved for it carries on under %s",
                old,
                new,
                new,
            )
        # What was saved for an entity that is gone, under an id a selected
        # entity has now - the replaced meter's, whose successor was given its
        # id - is the gone one's. It is put under a name nothing else has, so
        # it is neither taken for the successor's nor sent to its tag.
        renamed.update(
            {
                entity_id: f"{entity_id} (removed)"
                for entity_id in removed_since(self.hass, saved_ids)
                if entity_id in self._entity_ids
            }
        )
        self._registry_ids = {
            renamed.get(entity_id, entity_id): registry_id
            for entity_id, registry_id in saved_ids.items()
            if entity_id not in renamed or renamed[entity_id] in self._entity_ids
        }
        self._registry_ids.update(registry_ids(self.hass, self._entity_ids))

        acked: dict[str, float] = {}
        for saved_id, stamp in (saved.get("acked") or {}).items():
            entity_id = renamed.get(str(saved_id), str(saved_id))
            # Two watermarks for one id - one saved with no registry id to
            # tell whose it was, beside the renamed entity's. The newer holds:
            # reading back from the older could send what Tag Historian has
            # answered for already.
            acked[entity_id] = max(float(stamp), acked.get(entity_id, float("-inf")))
        clean = bool(saved.get("clean"))
        self._acked = acked
        self._since = {
            renamed.get(entity_id, entity_id): stamp
            for entity_id, stamp in _floats(saved.get("since")).items()
            if renamed.get(entity_id, entity_id) in self._entity_ids and stamp <= now
        }
        for entity_id in self._entity_ids:
            if entity_id not in acked:
                # Before the save below: killed before the next one, the run
                # after this still knows.
                self._since.setdefault(entity_id, now)

        items = saved.get("queue" if clean else "held")
        restored = [
            self._restored(item, renamed)
            for item in (items if isinstance(items, list) else [])
            if isinstance(item, list | tuple) and len(item) in (3, 4)
        ]
        self._queue.extend(restored)
        segments = saved.get("segments")
        if (
            isinstance(segments, list)
            and all(isinstance(n, int) and n > 0 for n in segments)
            and sum(segments) == len(restored)
        ):
            self._segments.extend(segments)
        elif restored:
            self._segments.append(len(restored))
        while len(self._queue) > MAX_QUEUE_LENGTH:
            # A batch that was on the wire is saved on top of a full buffer.
            self._pop_oldest()
            self.stats.dropped_overflow += 1
        while self._segments and self._segments[0] == 0:
            self._segments.popleft()
        self.stats.restored += len(self._queue)
        self.stats.queued = self._held()
        for item in self._queue:
            stamp = datetime.fromtimestamp(item.timestamp, UTC)
            if (last := self._last_sent.get(item.entity_id)) is None or stamp > last:
                self._last_sent[item.entity_id] = stamp

        doubt = saved.get("doubt")
        for entity_id, value in (doubt if isinstance(doubt, dict) else {}).items():
            try:
                stamp, sent_as, count = float(value[0]), str(value[1]), int(value[2])
            except (TypeError, ValueError, IndexError, KeyError):
                continue
            self._doubt[renamed.get(str(entity_id), str(entity_id))] = (stamp, sent_as, count)
        suspect = saved.get("suspect")
        if isinstance(suspect, list) and len(suspect) == 2:
            # What the answer to the question makes of it is all that is left of it.
            entity_id, stamp = str(suspect[0]), _floats({"": suspect[1]}).get("", now)
            self._suspect = Queued(renamed.get(entity_id, entity_id), stamp, "")
        saved_at = dt_util.parse_datetime(str(saved.get("saved_at") or ""))
        since_save = now - saved_at.timestamp() if saved_at is not None else 0.0
        if saved and (not clean or saved.get("doubt_all")):
            self._doubt_all = True
            self._doubt_until = _floats({"": saved.get("doubt_until")}).get("", 0.0)
            if not clean and saved.get("sending_ok") is not False:
                # How far it can have sent without saving that it had. A file
                # from before it saved as it went says nothing about that.
                # And not past now: what this run listens to, it has not sent.
                spacing = _floats({"": saved.get("save_every")}).get("")
                reach = now
                if saved_at is not None and spacing is not None:
                    reach = saved_at.timestamp() + spacing + 60
                else:
                    self._doubt_old = True
                self._doubt_until = max(self._doubt_until, reach)
            self._doubt_until = min(self._doubt_until, now)
            self._doubt_old = self._doubt_old or bool(saved.get("doubt_old"))
        if not clean and saved:
            LOGGER.warning(
                "Home Assistant did not stop cleanly last time. Tag Historian is asked what it "
                "has, and what it does not have is read back from the recorder"
            )
        if self._doubt and since_save < CONNECTION_RETRY_SECONDS:
            # A request cut short a moment ago may be being stored right now.
            self._pause(CONNECTION_RETRY_SECONDS - max(0.0, since_save))
        if self._doubt and saved_at is not None:
            self._unanswered_until = min(now, saved_at.timestamp()) + UNANSWERED_SETTLE_SECONDS

        for entity_id, stamp in self._acked.items():
            self._last_sent.setdefault(
                entity_id, datetime.fromtimestamp(_throttle_from(stamp), UTC)
            )

        await self._async_save(clean=False)

    def _add_saved_counters(self, saved: Any) -> None:
        """What an earlier Home Assistant run lost. Once: a reload's counters are still in memory."""
        if self.stats.loaded:
            return
        self.stats.loaded = True
        for name, count in _floats(saved).items():
            if name in SAVED_COUNTERS:
                setattr(self.stats, name, getattr(self.stats, name) + int(count))

    def _restored(self, item: list[Any] | tuple[Any, ...], renamed: dict[str, str]) -> Queued:
        """One saved reading: under its entity's id as it is now, for the tag it sends to now.

        That tag is the one a reading read back from the recorder now would
        be sent to, and a held reading goes with it rather than with what was
        decided when it was saved. A rename confirmed in Tag Historian since
        then makes no difference to where it lands - the server stores what
        is sent under a former name in the renamed tag - but a rename that
        found nothing under the old name ("no history to carry"), or the new
        tag chosen on the rename card, sends the entity under its own name
        from then on; a held reading sent under the old one then would start
        the second tag the rename handling exists to prevent. A reading of an
        entity that is not selected, or is gone, goes where it was going:
        nothing says where else.
        """
        sent_as = str(item[3] or "") if len(item) > 3 else ""
        held = Queued(str(item[0]), float(item[1]), str(item[2]), sent_as)
        entity_id = renamed.get(held.entity_id, held.entity_id)
        if entity_id in self._entity_ids:
            target = self._send_as.get(entity_id, entity_id)
        else:
            target = held.encoded_for
        line = retag_line(held.line, held.encoded_for, target)
        if line is None:
            # Not encoded as it says it was; better sent as it is than guessed at.
            line, target = held.line, held.encoded_for
        return Queued(entity_id, held.timestamp, line, "" if target == entity_id else target)

    @callback
    def async_listen(self) -> None:
        """Subscribe to the selected entities and buffer. Nothing is sent until :meth:`async_start`.

        An empty selection subscribes to nothing at all rather than to
        everything - that is the correct reading of "the user picked nothing",
        and the opposite reading would put a Free account over quota in the
        first second.
        """
        # A SHUTDOWN JOB, deliberately, and not a listener on
        # EVENT_HOMEASSISTANT_STOP. ``HomeAssistant.async_stop`` runs the
        # shutdown jobs as its stage 1 and awaits them; it then cancels every
        # background task, cancels the timers, and only THEN fires the stop
        # event. The flush carrying a batch is a background task, so a listener
        # would be handed a request Home Assistant had already cancelled - a
        # bounded wait for it measured 0.0 seconds of grace and reported forty
        # readings the endpoint would have taken as dropped.
        #
        # Registered even for an empty selection: this is what makes the
        # shutdown flush happen, and an entry can gain entities through the
        # options flow without this object being the one that started empty.
        self._unsub_stop = self.hass.async_add_shutdown_job(
            HassJob(self._async_handle_stop, "tag_historian_shutdown")
        )
        self._holding = True
        self._listening_since = dt_util.utcnow().timestamp()
        for entity_id in self._entity_ids:
            if entity_id not in self._acked:
                self._since.setdefault(entity_id, self._listening_since)

        if self._entity_ids:
            self._unsub_state = async_track_state_change_event(
                self.hass, self._entity_ids, self._handle_state_change
            )
        else:
            LOGGER.debug("No entities selected; forwarding nothing")

    @callback
    def async_start(self) -> None:
        """Begin to send - and to listen, if it does not yet."""
        if self._stopping:
            return
        if self._unsub_stop is None:
            self.async_listen()
        self._holding = False

        # What a previous run left behind goes out first. The read-back is
        # asked for first, so that no live reading can go before it.
        self._request_backfill()
        self._schedule_flush()

    # ------------------------------------------------------------------
    # Stopping
    # ------------------------------------------------------------------

    @callback
    def async_stop(self) -> None:
        """Unsubscribe, deregister the shutdown job, cancel any pending flush.

        Synchronous, so it can still be used where only a callback fits. It
        does NOT deal with the buffer - see :meth:`async_shutdown`.

        Dropping the shutdown job matters on the unload path: every options
        change and every repair flow builds a new forwarder, and a registration
        left behind would keep a dead one alive to be flushed at the next
        restart, once per reload the account has ever had.
        """
        if self._unsub_state is not None:
            self._unsub_state()
            self._unsub_state = None
        if self._unsub_stop is not None:
            self._unsub_stop()
            self._unsub_stop = None
        self._cancel_timer()
        if self._unsub_save is not None:
            self._unsub_save()
            self._unsub_save = None
        for unsub in self._trailing_timers.values():
            unsub()
        self._trailing_timers.clear()
        if self._backfill_task is not None and not self._backfill_task.done():
            self._backfill_task.cancel()
        if self._unsub_recorder is not None:
            self._unsub_recorder()
            self._unsub_recorder = None
        self._backfill_room.set()

    @property
    def holding(self) -> bool:
        """Listening and buffering, sending nothing yet (:meth:`async_listen`)."""
        return self._holding and not self._stopping

    async def async_shutdown(self) -> None:
        """Stop listening, wait for the wire, deliver what it can, save the rest.

        Two doors reach this, and both had to be argued for separately because
        Home Assistant cancels an entry's background tasks at a different point
        in each:

        * an UNLOAD - an options change, the tag-quota repair flow. Called from
          ``async_unload_entry`` rather than through ``entry.async_on_unload``,
          because Home Assistant runs the on-unload callbacks and THEN cancels
          the entry's background tasks, while it awaits ``async_unload_entry``
          before either;
        * a STOP - every restart, update and reboot. Reached from a shutdown
          job rather than an ``EVENT_HOMEASSISTANT_STOP`` listener, because
          ``async_stop`` runs and awaits the shutdown jobs BEFORE it cancels
          the background tasks and fires that event.

        Both orderings have the same shape and the same consequence: a flush
        already in flight is a background task, and arriving after the
        cancellation means the request is cancelled out from under a batch that
        has already left the queue. Forty readings, no counter, on the unload
        path; forty readings reported lost that the endpoint had taken, on the
        stop path.

        Four things happen here, in order:

        1. stop listening, so nothing new arrives - and save the buffer, all of
           it in doubt: Home Assistant may be killed while it stops, and of an
           entity the recorder leaves out this is the only copy;
        2. wait, bounded, for a request that is already on the wire, so the
           common case (a restart or a reload landing mid-write) DELIVERS
           those readings rather than losing them;
        3. flush what is still buffered, a few requests' worth;
        4. save whatever is left, with the watermark, for the next start -
           also when Home Assistant runs out of patience and cancels this.
        """
        if self._closed:
            return
        self._begin_stop()
        self._stop_by = self.hass.loop.time() + STOP_BUDGET_SECONDS
        # A reading still held back goes with the last flush, or is saved with
        # the buffer: it is the one its burst ended on.
        self._release_trailing()
        try:
            if self._queue or self._inflight:
                await self._async_save(clean=True, doubt_all=not self._in_doubt)
            await self._async_settle_inflight()
            await self._async_final_flush()
        finally:
            await self._async_save_rest()

    async def async_put_back(self) -> None:
        """The entry could not be set up: save what there is, and ask nobody anything."""
        if self._closed:
            return
        self._begin_stop()
        self._release_trailing()
        self._holding = True
        await self._async_save_rest()

    @callback
    def _begin_stop(self) -> None:
        # A gap not read back to its end is the next start's to read. A live
        # reading delivered now would move the watermark past it.
        self._cut_short = self._cut_short or self._backfill_running or (
            self._backfill_wanted and history.recorder_ready(self.hass)
        )
        self._stopping = True
        self.async_stop()

    async def _async_settle_inflight(self) -> None:
        """Give a request that is already on the wire time to answer.

        Deliberately NOT ``wait_for``: a timeout here must not cancel the
        request. Cancelling it would guarantee the loss this method exists to
        avoid, whereas letting it run means one of two honest endings - it
        answers and the readings are counted as sent, or Home Assistant cancels
        it moments later (the unload's own task sweep, or stage 2 of a stop),
        and the batch is in what was saved, in doubt, for the next start to
        ask about.
        """
        task = self._flush_task
        if task is None or task.done():
            return

        LOGGER.debug(
            "Tag Historian waiting up to %ss for a write already in flight",
            INFLIGHT_WAIT_SECONDS,
        )
        await asyncio.wait({task}, timeout=INFLIGHT_WAIT_SECONDS)

    async def _async_handle_stop(self) -> None:
        """Home Assistant is stopping, and it has cancelled nothing yet."""
        # Forget the deregistration handle rather than calling it. Home
        # Assistant is part way through ``for job in self._shutdown_jobs`` at
        # this moment - a shutdown job is created eagerly, so this coroutine
        # starts running inside that loop - and the handle removes this job
        # from the very list being iterated, which would silently skip whatever
        # integration registered next. Leaving the registration behind costs
        # nothing: the process is going away, and ``async_shutdown`` on an
        # already-drained forwarder is a no-op.
        self._unsub_stop = None
        await self.async_shutdown()

    async def _async_final_flush(self) -> None:
        """A few last delivery attempts, bounded.

        The pause is ignored on purpose. If the API asked us to wait until
        midnight this single request will be refused again and the readings are
        saved for the next start, which is what happens either way - but a
        connection that is merely slow, or a back-pressure window that has
        since cleared, gets the batch through.

        ``_flushing`` is NOT cleared the same way. It is only ever true while a
        request is genuinely in flight with a batch already popped off the
        queue, and forcing it false here would let this loop pop the same
        readings a second time and send them twice - the endpoint does not
        deduplicate. A flush already running is left to finish; the batch it
        carries is saved, in doubt. :meth:`_async_settle_inflight` has already
        given it its chance by the time this runs.
        """
        self._paused = False

        for _ in range(FINAL_FLUSH_ATTEMPTS):
            if not self._queue and not self._backfill:
                break
            if self._stop_by is not None and self.hass.loop.time() >= self._stop_by:
                break
            before = self._held()
            try:
                await self.async_flush()
            # Broad on purpose: a shutdown path that raises takes the rest
            # of the unload - or of Home Assistant's stop sequence - with it,
            # and losing the buffer uncounted is the exact failure this method
            # exists to stop.
            except Exception:
                LOGGER.exception("Tag Historian could not flush on shutdown")
                break
            if self._held() >= before:
                # Requeued, refused, or paused again. Another attempt would be
                # the same request against the same answer.
                break

    async def _async_save_rest(self) -> None:
        """Save what is left, for the next start.

        The backfill's read-ahead is not saved: those readings are still in the
        recorder and still past the watermark, so the next start reads them
        again. (:meth:`_may_forfeit` is what keeps the second half true.)
        """
        if self._closed:
            return
        self._sealed = True
        self._cancel_timer()
        self._paused = False
        self._backfill.clear()

        if self._queue:
            if self._store is None or not await self._async_save(clean=True):
                self._count_shutdown_loss(len(self._queue))
            else:
                LOGGER.info(
                    "Tag Historian saved %s undelivered readings; they are sent when it starts again",
                    len(self._queue),
                )
            self._queue.clear()
            self._segments.clear()
        elif self._store is not None:
            await self._async_save(clean=True)

        self.stats.queued = self._held()
        # From here the queue is a dead end. Anything arriving late - a write
        # that outlived _async_settle_inflight and then came back asking to be
        # requeued - has to be counted rather than parked in a buffer that no
        # longer has anything to flush it.
        self._closed = True

    def _count_shutdown_loss(self, lost: int) -> None:
        self.stats.dropped_shutdown += lost
        self.stats.last_error = f"{lost} readings lost on shutdown"
        LOGGER.warning(
            "Tag Historian dropped %s readings that could neither be delivered nor "
            "saved before shutdown",
            lost,
        )

    async def _async_save(self, *, clean: bool, doubt_all: bool = False) -> bool:
        """Write the watermark - and on a clean stop the buffer - to .storage.

        A batch still on the wire is saved as what it is: perhaps stored. Its
        readings go first in the buffer, and the next start asks Tag Historian
        before it sends them. (One read back from the recorder is only marked;
        the recorder still has it.) ``doubt_all``: all of it is about to be sent.
        """
        if self._store is None:
            return False

        inflight = self._inflight or []
        doubt = {**self._doubt, **_in_doubt(inflight)}
        front = [] if self._inflight_from_backfill else inflight
        segments = list(self._segments)
        if front and self._inflight_restored and segments:
            segments[0] += len(front)
        restored = sum(segments)
        items = [*front, *self._queue]
        if clean and len(items) > restored:
            segments.append(len(items) - restored)
        saved = [list(item) for item in (items if clean else items[:restored])]
        # Not taken to be on the wire any more by the time it is written.
        self._inflight_saved = clean and bool(inflight)

        data: dict[str, Any] = {
            "clean": clean,
            "sending_ok": self._sending_ok,
            "acked": dict(self._acked),
            "queue": saved if clean else [],
            # How the next run finds an entity renamed in between (renames.py).
            "registry_ids": self._registry_ids_of(
                {*self._acked, *self._since, *doubt, *(item[0] for item in saved)}
            ),
            "saved_at": dt_util.utcnow().isoformat(),
            "stopped_at": dt_util.utcnow().isoformat() if clean else None,
            # From here on nothing an older version knows of, or reads.
            "held": [] if clean else saved,
            "segments": [n for n in segments if n],
            "doubt": {entity_id: list(value) for entity_id, value in doubt.items()},
            "doubt_all": doubt_all or self._doubt_all or bool(self._doubtful_until),
            "doubt_until": dt_util.utcnow().timestamp() if doubt_all else self._doubt_until,
            "doubt_old": self._doubt_old,
            "suspect": list(self._suspect[:2]) if self._suspect is not None else None,
            "since": {e: s for e, s in self._since.items() if e not in self._acked},
            "save_every": WATERMARK_SAVE_SECONDS,
            "daily_limit": self.daily_limit,
            "counters": {name: getattr(self.stats, name) for name in SAVED_COUNTERS},
        }
        on_disk = self._disk_sending_ok
        if on_disk != self._sending_ok:
            self._disk_sending_ok = self._sending_ok
            self._state_saved_at = dt_util.utcnow().timestamp()
        try:
            await self._store.async_save(data)
        except Exception:  # reported, and the caller counts the loss
            LOGGER.exception("Tag Historian could not save its buffer")
            self._disk_sending_ok = on_disk
            self._inflight_saved = False
            return False
        return True

    def _registry_ids_of(self, entity_ids: set[str]) -> dict[str, str]:
        """The registry ids of these entities: as taken at the start, else as the registry has them now.

        Taken at the start first, because a rename's reload saves after the
        registry has forgotten the old id. Looked up now for an entity that
        was not in the registry yet when this run started.
        """
        known = {
            entity_id: registry_id
            for entity_id, registry_id in self._registry_ids.items()
            if entity_id in entity_ids
        }
        known.update(registry_ids(self.hass, entity_ids - known.keys()))
        return known

    @callback
    def _save_soon(self) -> None:
        """Save the watermark now, without waiting for it (an outage began or ended)."""
        if self._store is None or self._closed or self._stopping:
            return
        self.entry.async_create_background_task(
            self.hass, self._async_save(clean=False), "tag_historian_save"
        )

    @callback
    def _save_within(self, seconds: float) -> None:
        """Save within this long, together with whatever else asks for it meanwhile."""
        if self._store is None or self._closed or self._stopping:
            return
        due = dt_util.utcnow().timestamp() + seconds
        if self._unsub_save is not None:
            if self._save_due <= due:
                return
            self._unsub_save()
        self._save_due = due
        self._unsub_save = async_call_later(
            self.hass,
            seconds,
            HassJob(self._on_save_due, "tag_historian_save_due", cancel_on_shutdown=True),
        )

    @callback
    def _on_save_due(self, _now: object) -> None:
        self._unsub_save = None
        self._save_soon()

    @callback
    def _cancel_timer(self) -> None:
        if self._unsub_timer is not None:
            self._unsub_timer()
            self._unsub_timer = None

    # ------------------------------------------------------------------
    # Live readings
    # ------------------------------------------------------------------

    @callback
    def set_power_interval(self, power_interval: int | None) -> None:
        """Send power sensors every ``power_interval`` seconds from now on, or at the minimum interval (None).

        For fast_power.FastPowerGuard, which keeps the option inside the plan's
        daily allowance. A reading already held is sent as it is; only what
        arrives from now on, and the next catch-up, is judged by the new pace.
        So is when one held back by the interval goes out.
        """
        if power_interval == self._power_interval:
            return
        self._power_interval = power_interval
        for entity_id in list(self._trailing_timers):
            self._time_trailing(entity_id)

    @callback
    def remember_daily_limit(self, limit: int) -> None:
        """The plan's daily allowance, for the next start to go by until it is read again."""
        if limit != self.daily_limit:
            self.daily_limit = limit
            self._save_within(WATERMARK_SAVE_SECONDS)

    def _interval_for(self, entity_id: str, state: State | None = None) -> float:
        """The fewest seconds between two readings of this entity.

        The minimum interval - or, for a power sensor when the user ticked
        "send power sensors more often", the shorter of it and the power
        interval. ``state`` is the reading being judged; without one (the
        backfill, whose recorded states may carry no attributes) the entity is
        judged by what Home Assistant says of it now.
        """
        if self._power_interval is None or self._power_interval >= self._min_interval:
            return self._min_interval
        if state is None:
            state = self.hass.states.get(entity_id)
        if state is not None and is_power_sensor(state.attributes):
            return self._power_interval
        return self._min_interval

    @callback
    def _handle_state_change(self, event: Event[EventStateChangedData]) -> None:
        """Encode one state change, or decide not to."""
        new_state = event.data["new_state"]
        if new_state is None:
            return

        entity_id = new_state.entity_id
        value = state_to_value(new_state.state, new_state.domain)
        if value is None:
            if is_missing_state(new_state.state):
                # unknown / unavailable / empty. Nothing is sent, and nothing
                # is counted as dropped either: Home Assistant is telling us it
                # has nothing to report, and the honest wire representation of
                # that is a gap. There was no reading to lose.
                return
            # But this one IS a reading. The entity reported something a person
            # can see on their dashboard - "away", "jammed", "heat", "1500 W" -
            # and we stored nothing. That used to return here too, so a
            # sensor.house_mode cycling home/night/away/guest wrote a lone 1.0
            # for "home" and three silences, with every counter reading zero.
            self._count_unstorable(entity_id, new_state.state)
            return

        stamp = new_state.last_updated
        last = self._last_sent.get(entity_id)
        interval = self._interval_for(entity_id, new_state)
        if last is not None and entity_id in self._trailing:
            due = last + timedelta(seconds=interval)
            if stamp > due:
                # Its interval was up before this one was made, and its timer
                # has not got to it: a busy loop, a machine that slept.
                self._send_held(entity_id, due)
                last = due
        if last is not None and (stamp - last).total_seconds() < interval:
            # Deliberate. The daily quota counts every point SENT, including
            # ones deadband compression later discards server-side, so the only
            # place a reading can be saved is here.
            self._hold_trailing(new_state, value)
            return
        # A reading of it goes out now, so one held back is older than it and
        # has nothing left to say.
        self._let_trailing_go(entity_id)
        self._last_sent[entity_id] = stamp
        self._enqueue(new_state, value)

    @callback
    def _hold_trailing(self, state: State, value: float) -> None:
        """A reading the interval holds back: sent when the interval is up, if still the newest.

        An entity that goes quiet after a burst - the dishwasher's last kWh,
        the export that stops, a door opened and closed - would otherwise end
        its burst on a reading it was already past, for as long as it is
        quiet. Only the newest held reading is kept, one per entity, and it
        goes out once the interval has passed since the last one sent, with
        its own timestamp - a real value at its real time.
        """
        entity_id = state.entity_id
        if _says_nothing_new(state, value, self._last_value.get(entity_id)):
            self._let_trailing_go(entity_id)
            self.stats.dropped_throttled += 1
            return
        if entity_id in self._trailing:
            # Replaced by a newer one before it went out: throttled after all.
            self.stats.dropped_throttled += 1
        self._trailing[entity_id] = (state, value)
        if entity_id not in self._trailing_timers:
            self._time_trailing(entity_id)

    @callback
    def _time_trailing(self, entity_id: str) -> None:
        """(Re)start the timer of an entity's held reading."""
        if (unsub := self._trailing_timers.get(entity_id)) is not None:
            unsub()
        interval = self._interval_for(entity_id, self._trailing[entity_id][0])
        due = self._last_sent[entity_id] + timedelta(seconds=interval) - dt_util.utcnow()
        self._trailing_timers[entity_id] = async_call_later(
            self.hass,
            max(0.0, due.total_seconds()),
            HassJob(partial(self._send_trailing, entity_id), "tag_historian_trailing_send"),
        )

    @callback
    def _send_trailing(self, entity_id: str, _now: object) -> None:
        """The interval is up: send the entity's newest held reading."""
        self._trailing_timers.pop(entity_id, None)
        if entity_id in self._trailing and not self._closed:
            self._send_held(entity_id, dt_util.utcnow())

    @callback
    def _send_held(self, entity_id: str, at: datetime) -> None:
        if (unsub := self._trailing_timers.pop(entity_id, None)) is not None:
            unsub()
        state, value = self._trailing.pop(entity_id)
        # The throttle runs from the moment it went out rather than from the
        # reading's own time, so an entity sends at most one reading per
        # interval, trailing ones included - the rate the review screen's
        # arithmetic promises.
        self._last_sent[entity_id] = max(state.last_updated, at)
        self._enqueue(state, value)

    @callback
    def _let_trailing_go(self, entity_id: str) -> None:
        """Forget an entity's held reading, counted as throttled; it never went out."""
        if (unsub := self._trailing_timers.pop(entity_id, None)) is not None:
            unsub()
        if self._trailing.pop(entity_id, None) is not None:
            self.stats.dropped_throttled += 1

    @callback
    def _release_trailing(self) -> None:
        """Stopping: every held reading goes into the buffer, for the last flush or the saved buffer."""
        for unsub in self._trailing_timers.values():
            unsub()
        self._trailing_timers.clear()
        held, self._trailing = self._trailing, {}
        for state, value in held.values():
            self._last_sent[state.entity_id] = state.last_updated
            self._enqueue(state, value)

    @callback
    def _enqueue(self, state: State, value: float) -> None:
        """One reading into the buffer, encoded for the tag its entity sends to."""
        entity_id = state.entity_id
        stamp = state.last_updated
        self._last_value[entity_id] = value
        # The unit and the friendly name ride along on every reading, so the
        # tag in Tag Historian carries what Home Assistant shows - "Living room
        # temperature", 21.5 °C - and follows a rename here. The attribute,
        # not State.name: that falls back to a name made from the entity id,
        # and an entity without a friendly name should not be given one. The
        # state class and last reset too, which make a meter a meter there.
        line = encode_point(
            self._send_as.get(entity_id, entity_id),
            value,
            stamp,
            state.attributes.get("unit_of_measurement"),
            state.attributes.get("friendly_name"),
            state.attributes.get("state_class"),
            state.attributes.get("last_reset"),
        )

        if len(self._queue) >= MAX_QUEUE_LENGTH:
            # Oldest first. During a long outage the recent readings are the
            # ones worth keeping, and an unbounded queue is an out-of-memory
            # kill of the whole Home Assistant process rather than a lost hour
            # of history.
            self._let_go(self._pop_oldest())

        self._queue.append(
            Queued(entity_id, stamp.timestamp(), line, self._send_as.get(entity_id, ""))
        )
        self.stats.queued = self._held()
        self._schedule_flush()

    def _count_unstorable(self, entity_id: str, state: str) -> None:
        """A state that is a value to its owner and not a number to us.

        Counted rather than ignored, and the entity is named on the diagnostic
        sensor, because the customer is spending a tag on this series and the
        series is empty. Without this the only symptom is a chart that is
        blank for no stated reason.

        Two different things bring an entity here, and the log line says which.
        ``climate.living_room`` at ``heat`` is a domain where no word is ever
        stored; ``cover.garage`` at ``opening`` is a domain where two words
        are, and this was not one of them. Telling the second person that
        ``cover`` is "not a domain where that word is a two-state signal"
        contradicts the open/closed series they can already see.
        """
        self.stats.dropped_not_numeric += 1
        self._save_within(WATERMARK_SAVE_SECONDS)
        if entity_id not in self.stats.unstorable_entities:
            if len(self.stats.unstorable_entities) < MAX_UNSTORABLE_NAMES:
                self.stats.unstorable_entities.append(entity_id)
            domain = entity_id.partition(".")[0]
            if is_boolean_domain(domain):
                LOGGER.warning(
                    "Tag Historian is storing nothing for %s: its state %r is not a "
                    "number, and it is not one of the words that count as a "
                    "two-state signal for %s. Nothing is written and the dropped "
                    "counter moves",
                    entity_id,
                    state,
                    domain,
                )
            else:
                LOGGER.warning(
                    "Tag Historian is storing nothing for %s: its state %r is not a "
                    "number, and %s is not a domain where a word can be a two-state "
                    "signal. Nothing is written and the dropped counter moves",
                    entity_id,
                    state,
                    domain,
                )

    # ------------------------------------------------------------------
    # The backfill: gaps read back from the recorder
    # ------------------------------------------------------------------

    def _pop_oldest(self) -> Queued:
        """Take the oldest reading out of the buffer for good."""
        self._popped_past_hole = bool(self._segments)
        for index, count in enumerate(self._segments):
            if count:
                self._segments[index] -= 1
                self._popped_past_hole = index > 0
                break
        return self._queue.popleft()

    @callback
    def _tidy_segments(self) -> None:
        """What an earlier run left is delivered: now read what came after it, before anything newer goes."""
        if not self._drop_empty_segments():
            return
        self._backfill_wanted = True
        if self._stopping:
            self._cut_short = self._cut_short or history.recorder_ready(self.hass)
        self._request_backfill()

    def _drop_empty_segments(self) -> bool:
        done = False
        while self._segments and not self._segments[0] and not self._inflight_restored:
            self._segments.popleft()
            done = True
        return done

    def _held(self) -> int:
        return len(self._queue) + len(self._backfill)

    def _oldest_held(self, entity_id: str) -> float | None:
        """The timestamp of the oldest LIVE reading of this entity still held.

        In the buffer or, when none is there, held back by the interval: that
        one is newer than any that is.
        """
        oldest = min(
            (item.timestamp for item in self._queue if item.entity_id == entity_id),
            default=None,
        )
        if oldest is None and (held := self._trailing.get(entity_id)) is not None:
            return held[0].last_updated.timestamp()
        return oldest

    def _newest_accounted(self, entity_id: str) -> float | None:
        """The newest reading of this entity answered for, read back, or on the wire from a read-back."""
        candidates = [
            item.timestamp
            for item in [*self._backfill, *(self._inflight or [])]
            if item.entity_id == entity_id
        ]
        if (acked := self._acked.get(entity_id)) is not None:
            candidates.append(acked)
        return max(candidates, default=None)

    def history_boundary(self, entity_id: str) -> float | None:
        """Where this forwarder's part of an entity's history begins, for the history import.

        From here on, every reading of the entity is this forwarder's to
        deliver: the oldest of it still held - queued, read back and waiting,
        or on the wire - and, for an entity nothing has been acknowledged for
        yet, the moment this forwarder started listening, because its gap is
        read back from there. An import that stored any of that too would
        double it once the forwarder delivers it, since the live endpoint does
        not deduplicate - and Tag Historian cannot stop it, because to Tag
        Historian an entity that has never been delivered live has no live
        line yet. None when Tag Historian's own line (its first live reading)
        is the only one.

        Asked by the entity's id as it is now. For one renamed since the last
        run that is the id its held readings and watermark were restored
        under (:meth:`async_restore`); under its old id they would not be its
        own, and the line would come out later than readings still waiting to
        be delivered.
        """
        candidates = [
            item.timestamp
            for item in [*self._queue, *self._backfill, *(self._inflight or [])]
            if item.entity_id == entity_id
        ]
        # A reading held back for its trailing send is the forwarder's to
        # deliver as well.
        if (held := self._trailing.get(entity_id)) is not None:
            candidates.append(held[0].last_updated.timestamp())
        if entity_id not in self._acked:
            candidates.append(self._since.get(entity_id, self._listening_since))
        return min(candidates, default=None)

    def _let_go(self, item: Queued) -> None:
        """A reading the full buffer has evicted, oldest first. Counted either way."""
        self.stats.dropped_overflow += 1
        self._save_within(WATERMARK_SAVE_SECONDS)
        if self._over_allowance and self._may_forfeit(item):
            self._forfeit(item)
            return
        # The pass asked for below reads what lies between two runs as well.
        self._drop_empty_segments()
        # Not gone: the recorder still has it, and once Tag Historian answers
        # again a backfill pass reads it back.
        self._backfill_wanted = True
        if self._stopping:
            # A held reading released into a full buffer. The next start reads
            # it, and what is newer must not go before then.
            self._cut_short = self._cut_short or history.recorder_ready(self.hass)
        self._request_backfill()

    def _forfeit(self, item: Queued) -> None:
        """Let go of a reading for good: evicted while today's allowance was spent.

        Such a reading is over the allowance, not lost to an outage. Reading
        it back at midnight - which the first version did - put it, and every
        one like it, in front of the new day's readings: an account that
        produces more than its allowance every day then spent each morning's
        allowance on the evening before, ran out earlier, evicted more, and
        fell further behind live every day, for good - its dashboards and its
        alerts showing ever older values, until the forwarder was sending
        what the recorder was about to purge. Before read-back existed the
        overflow was counted and each day started current; this keeps that.

        So the watermark moves over it, as over a reading the API refused, and
        no backfill pass reads it back. It was counted as an eviction. The
        held buffer itself - the newest readings, at most MAX_QUEUE_LENGTH -
        is still sent when the allowance resets, as it always was.
        """
        self._acknowledge([item])

    def _may_forfeit(self, item: Queued) -> bool:
        """Whether its entity's watermark can move over it without stranding read-back readings.

        Readings of it already read back from an outage - waiting, on the
        wire, or being read by a pass right now - are older than any held one.
        A watermark moved past them would make them unreadable again: a
        restart, which drops the read-ahead because the recorder still has
        it, would lose them with no counter moving. Such an entity's eviction
        is read back like an outage's instead; the next day's evictions, with
        that backlog delivered, are let go of.

        And a stretch nobody listened through, between one run's readings and
        the next one's, must have been read back before anything after it.
        """
        entity_id = item.entity_id
        if entity_id in self._unread:
            return False
        if (self._popped_past_hole or self._backfill_wanted) and history.recorder_ready(self.hass):
            return False
        waiting = list(self._backfill)
        if self._inflight and self._inflight_from_backfill:
            waiting.extend(self._inflight)
        return all(other.entity_id != entity_id for other in waiting)

    @callback
    def _request_backfill(self) -> None:
        """Start a backfill pass, unless one is running or there is no recorder to read."""
        if self._closed or self._backfill_running or not history.recorder_ready(self.hass):
            return
        if self._holding or self._stopping or self._in_doubt:
            # Not yet, or not any more: where the gap begins is what Tag
            # Historian is about to be asked.
            self._backfill_wanted = True
            return
        if self._inflight is not None and not self._inflight_from_backfill:
            # A live batch that comes back is older than what would be read
            # now, and would be behind it. When it has been answered for, then.
            self._backfill_wanted = True
            return
        self._backfill_wanted = False
        self._backfill_running = True
        self._backfill_task = self.entry.async_create_background_task(
            self.hass, self._async_backfill(), "tag_historian_backfill"
        )

    async def _async_backfill(self) -> None:
        """Read the recorder for every selected entity's gap, and queue what it finds.

        The gap of an entity runs from the last reading Tag Historian answered
        for - or, for an entity nothing has been answered for yet, from when
        this forwarder started listening - to the oldest live reading still
        held, or, when none is, to the moment this pass began. Inside it, the
        same rules as a live reading: numbers and the two-state words only,
        ``unavailable`` is a gap, and the minimum interval holds, measured
        from the acknowledged reading - and the last one before a silence is
        sent when it is up - so a gap that only ever held throttled readings
        stays empty.

        "When none is held" ended at the moment this forwarder started
        listening, once. That is the same moment at startup, and hours or days
        before an eviction mid-run - older than the entity's watermark, so the
        gap came out empty and was skipped. Every entity that changes less
        often than the buffer's span (a door, a setpoint, a battery: with
        twenty entities at a ten-second interval the buffer spans about eighty
        minutes) lost its evicted reading for good, because its next live one
        was delivered and moved the watermark past it. Ending at the pass's
        start misses nothing: the forwarder handles a state change the moment
        Home Assistant sets it, so a reading newer than that is held by the
        time the pass reaches its entity, and the gap then ends there.

        While this runs, the flush sends nothing but what it reads, oldest
        first per entity: a live reading delivered in the middle would move the
        watermark past the part of the gap not read yet, and a restart then
        would leave that part out for good.
        """
        todo = list(self._entity_ids)
        self._unread = set(todo)
        try:
            for attempt in range(RECORDER_TRIES):
                if attempt:
                    await self._async_recorder_pause()
                todo = await self._async_backfill_pass(todo, attempt)
                if not todo or self._closed:
                    break
            for entity_id in [] if self._closed else todo:
                self._give_up_gap(entity_id, counted=not self._recorder_written)
        finally:
            self._unread.clear()
            self._backfill_running = False
            self._schedule_flush()

        if self._backfill_wanted and not self._closed:
            self._request_backfill()

    async def _async_backfill_pass(self, entity_ids: list[str], attempt: int) -> list[str]:
        """Read each entity's gap. The ones the recorder could not be read for."""
        if not self._recorder_written:
            try:
                self._recorder_written = await history.async_written(
                    self.hass, RECORDER_WAIT_SECONDS
                )
            except Exception:  # the recorder failing must not stop the live path
                LOGGER.exception("Tag Historian could not wait for the recorder")
            if self._closed:
                return entity_ids
            if not self._recorder_written:
                # Not all of any gap is there to be read yet. At the last try
                # what is there is read, and the rest is counted.
                if attempt < RECORDER_TRIES - 1:
                    return entity_ids
                self.stats.gaps_not_read += len(entity_ids)
                self.stats.last_error = "the recorder has not written what it was handed"
                self._save_within(LOSS_SAVE_SECONDS)
        began = dt_util.utcnow().timestamp()
        failed: list[str] = []
        for entity_id in entity_ids:
            if self._closed:
                break
            try:
                await self._async_backfill_gap(entity_id, began)
            except Exception as err:  # nor one entity's the next one's
                if not attempt and not failed:
                    LOGGER.exception("Tag Historian could not read the recorder to fill a gap")
                self.stats.last_error = f"the recorder could not be read: {err}"
                failed.append(entity_id)
            else:
                self._unread.discard(entity_id)
        return failed

    async def _async_recorder_pause(self) -> None:
        waited = asyncio.Event()

        @callback
        def _due(_now: object) -> None:
            self._unsub_recorder = None
            waited.set()

        # What other entities' gaps held goes meanwhile.
        self._schedule_flush()
        self._unsub_recorder = async_call_later(
            self.hass,
            RECORDER_RETRY_SECONDS,
            HassJob(_due, "tag_historian_recorder_retry", cancel_on_shutdown=True),
        )
        await waited.wait()

    def _give_up_gap(self, entity_id: str, *, counted: bool) -> None:
        """The recorder cannot be read for it: nothing waits any longer, and it is counted."""
        self.stats.gaps_not_read += not counted
        self.stats.last_error = f"the recorder could not be read for {entity_id}"
        LOGGER.warning(
            "Tag Historian could not read the recorder for %s in %s attempts; readings it "
            "missed may not be sent",
            entity_id,
            RECORDER_TRIES,
        )
        held = self._same_second.pop(entity_id, (0.0, 0.0, set()))[2]
        self.stats.dropped_inflight += max(0, len(held) - 1)
        self._end_doubtful(entity_id)
        self._save_within(LOSS_SAVE_SECONDS)

    def _end_doubtful(self, entity_id: str) -> None:
        """The stretch nobody could say about is counted: go on from its end."""
        self._counted.pop(entity_id, None)
        until = self._doubtful_until.pop(entity_id, None)
        if until is None:
            return
        self._acked[entity_id] = max(until, self._acked.get(entity_id, float("-inf")))
        if not self._doubtful_until and not self._doubt_all:
            self._doubt_until = 0.0
            self._doubt_old = False
        self._save_within(LOSS_SAVE_SECONDS)

    async def _async_backfill_gap(self, entity_id: str, began: float) -> None:
        await self._async_count_same_second(entity_id)
        # Where the gap begins: the newest reading of this entity that is
        # answered for, or already read back and waiting to go - a second pass
        # while the first one's readings are still held must not read them
        # again.
        acked = self._newest_accounted(entity_id)
        start = acked
        if start is None:
            start = self._since.get(entity_id, self._listening_since)
        doubtful = self._doubtful_until.get(entity_id)
        if doubtful is not None and self._doubt_old:
            # Counted, not sent, and nobody knows since when: a day of it.
            start = max(start, doubtful - RECORDER_WINDOW_HOURS * 3600)
        held = self._oldest_held(entity_id)
        end = held if held is not None else began
        if end > start:
            found = await self._async_backfill_entity(
                entity_id, start, end, held is not None, throttle_from=acked
            )
            if not (found or self._closed) and acked is None and held is None:
                # An entity that has never reported: not all of it again at
                # every start, for as long as it never does. A whole second, as
                # what Tag Historian says it has is, and before the end: a
                # reading made at it is yet to come.
                self._since[entity_id] = max(start, math.ceil(end) - 1)
        self._end_doubtful(entity_id)

    async def _async_count_same_second(self, entity_id: str) -> None:
        """Readings made within the second of Tag Historian's newest, of which it may have only one."""
        if entity_id not in self._same_second:
            return
        second, acked, held = self._same_second[entity_id]
        source = await history.async_source(self.hass, entity_id, self._send_as.get(entity_id))
        begin = datetime.fromtimestamp(second, UTC)
        states = await history.async_states(
            self.hass, source, begin - timedelta(microseconds=1), begin + timedelta(seconds=1)
        )
        stamps = held | {
            state.last_updated.timestamp()
            for state in states
            if state.last_updated.timestamp() > acked
            and recorded_state_value(state.entity_id, state.state) is not None
        }
        del self._same_second[entity_id]
        missing = len(stamps) - (0 if acked >= second else 1)
        if self._interval_for(entity_id) >= 1:
            # The interval held all but one of them back - and the newest of
            # those may or may not have gone out when it was up.
            missing = min(missing, 1)
        self._count_mid_write(missing)

    def _count_mid_write(self, missing: int) -> None:
        if missing <= 0:
            return
        self.stats.dropped_inflight += missing
        self.stats.last_error = f"{missing} readings that may or may not have been stored"
        self._save_within(LOSS_SAVE_SECONDS)

    async def _async_backfill_entity(
        self,
        entity_id: str,
        start: float,
        end: float,
        end_is_held: bool,
        throttle_from: float | None,
    ) -> int:
        """Queue what the recorder has in the gap. How many readings that is.

        Judged as the live path would have judged them, the trailing send of
        a held reading included (:meth:`_hold_trailing`). One whose interval
        is not up yet when the gap has been read is handed to the live path,
        which sends it when it is - or drops it for a newer one.
        """
        window = timedelta(hours=RECORDER_WINDOW_HOURS)
        cursor = datetime.fromtimestamp(start, UTC)
        stop = datetime.fromtimestamp(end, UTC)
        # The minimum interval runs from the acknowledged reading. An entity
        # with none had nothing before its first live reading to be throttled
        # against, and neither has its first recorded one here.
        last = None if throttle_from is None else _throttle_from(throttle_from)
        last_value: float | None = None
        waiting: tuple[State, float] | None = None
        found = 0
        # A power sensor sent more often live is caught up at the same pace,
        # so a gap an outage left has the readings Loads looks for.
        interval = self._interval_for(entity_id)
        # Where the recorder keeps this entity's history: its id - or, for one
        # renamed onto an id that had a history of its own while the rename
        # waits to be settled, its old id up to the rename (history.py).
        source = await history.async_source(self.hass, entity_id, self._send_as.get(entity_id))

        while cursor < stop and not self._closed:
            window_end = min(cursor + window, stop)
            # The window opens one microsecond before the cursor so the
            # recorder's strict "after" does not drop a state stamped exactly
            # on a window boundary; the acknowledged reading itself is
            # excluded by the timestamp test below.
            states = await history.async_states(
                self.hass, source, cursor - timedelta(microseconds=1), window_end
            )
            for state in states:
                stamp = state.last_updated.timestamp()
                if stamp <= start or stamp >= end or stamp < cursor.timestamp():
                    continue
                value = recorded_state_value(state.entity_id, state.state)
                if value is None:
                    continue
                if last is not None:
                    if waiting is not None and stamp - last > interval:
                        # Its interval was up before this one was made.
                        found += await self._async_read_back(entity_id, *waiting)
                        last, last_value, waiting = last + interval, waiting[1], None
                    if stamp - last < interval:
                        waiting = (state, value)
                        if _says_nothing_new(state, value, last_value):
                            waiting = None
                        continue
                last, last_value, waiting = stamp, value, None
                found += await self._async_read_back(entity_id, state, value)
                if self._closed:
                    return found
            cursor = window_end
            self._schedule_flush()

        if last is not None and not self._closed:
            found += await self._async_read_back_last(
                entity_id, waiting, last, interval, end if end_is_held else None
            )
            self._schedule_flush()

        if found:
            LOGGER.info(
                "Tag Historian read %s readings of %s back from the recorder to fill a gap",
                found,
                entity_id,
            )
        return found

    async def _async_read_back_last(
        self,
        entity_id: str,
        waiting: tuple[State, float] | None,
        last: float,
        interval: float,
        held: float | None,
    ) -> int:
        """Where the gap ends: the reading still waiting for its interval, if there is one."""
        due = last + interval
        up = False
        if waiting is not None:
            newer = held if held is not None else self._oldest_held(entity_id)
            up = due < (newer if newer is not None else dt_util.utcnow().timestamp())
            if not up and (newer is not None or self._stopping):
                waiting = None  # a newer one took its place, or the next start reads it
        # The live path goes on from here.
        sent_at = datetime.fromtimestamp(due if up else last, UTC)
        if (known := self._last_sent.get(entity_id)) is None or sent_at > known:
            self._last_sent[entity_id] = sent_at
        if waiting is None:
            return 0
        state, value = waiting
        doubtful = self._doubtful_until.get(entity_id, float("-inf"))
        if up or state.last_updated.timestamp() <= doubtful:
            return await self._async_read_back(entity_id, state, value)
        self._hold_trailing(
            # Under its id as it is now; it may be recorded under a former one.
            State(
                entity_id,
                state.state,
                state.attributes,
                last_changed=state.last_changed,
                last_updated=state.last_updated,
            ),
            value,
        )
        return 0

    async def _async_read_back(self, entity_id: str, state: State, value: float) -> int:
        """Queue one recorded reading, when there is room. 1 if it was, 0 if not."""
        stamp = state.last_updated.timestamp()
        if stamp <= self._doubtful_until.get(entity_id, float("-inf")):
            if stamp not in self._counted.get(entity_id, ()):
                self.stats.dropped_inflight += 1
                self.stats.last_error = "readings from before a crash may be missing"
            return 0

        while len(self._backfill) >= BACKFILL_BATCH_SIZE and not self._closed:
            self._backfill_room.clear()
            self._schedule_flush()
            await self._backfill_room.wait()
        if self._closed:
            return 0

        # Into the tag a live reading of it goes to: a renamed entity's gap is
        # part of the history its live readings carry on.
        self._backfill.append(
            Queued(
                entity_id,
                stamp,
                encode_point(
                    self._send_as.get(entity_id, entity_id),
                    value,
                    state.last_updated,
                    state.attributes.get("unit_of_measurement"),
                    state.attributes.get("friendly_name"),
                    state.attributes.get("state_class"),
                    state.attributes.get("last_reset"),
                ),
                self._send_as.get(entity_id, ""),
            )
        )
        self.stats.backfilled += 1
        self.stats.queued = self._held()
        return 1

    # ------------------------------------------------------------------
    # The wire
    # ------------------------------------------------------------------

    @callback
    def _schedule_flush(self) -> None:
        """Two triggers: a full buffer, or a quiet second.

        The timeout is what stops a house with one slow sensor from holding its
        reading hostage until ninety-nine more arrive; the size trigger is what
        stops a burst being sent one request per reading.
        """
        if self._closed or self._stopping or self._holding or self._paused or self._flushing:
            return
        if not self._sendable():
            return

        if self._backfill or self._in_doubt or len(self._queue) >= BATCH_BUFFER_SIZE:
            # A full buffer overtakes a pending timeout, and recorded readings
            # have waited long enough already.
            self._cancel_timer()
            self._unsub_timer = async_call_later(self.hass, 0, self._on_flush_due)
            return

        if self._unsub_timer is None:
            self._unsub_timer = async_call_later(
                self.hass, BATCH_TIMEOUT_SECONDS, self._on_flush_due
            )

    def _sendable(self) -> bool:
        """Whether there is anything the next flush may take."""
        if self._backfill or self._in_doubt:
            return True
        return bool(self._queue) and not self._backfill_running and not self._cut_short

    @property
    def _in_doubt(self) -> bool:
        """Whether something may have been stored already, and Tag Historian is to be asked first."""
        return self._doubt_all or bool(self._doubt)

    @callback
    def _on_flush_due(self, _now: object) -> None:
        self._unsub_timer = None
        # The handle is kept so that :meth:`async_shutdown` can wait for a
        # request that is already on its way out instead of letting the reload
        # (or the restart) cancel it out from under a batch that has left the
        # queue.
        self._flush_task = self.entry.async_create_background_task(
            self.hass, self.async_flush(), "tag_historian_flush"
        )

    async def async_flush(self) -> None:
        """Send one batch, classify the answer, decide what happens next.

        The ``finally`` is not decoration. Between the ``popleft`` below and
        one of the branches accounting for the result, the batch exists
        nowhere but in a local variable inside a task Home Assistant is free to
        cancel - and it does, on every unload (which is what an options change
        and the tag-quota repair flow both arrive as) and again at every stop
        (which is what a restart, an update and a reboot all arrive as).
        Review of an earlier version measured 30 of 40 readings gone that way
        with every counter still reading zero, while the README promised the
        opposite.

        The batch goes back to the front, IN DOUBT: the request may have been
        taken in full before the socket went, and Tag Historian does not
        deduplicate live writes, so it is asked what it has before any of it
        is sent again (:meth:`_async_settle_doubt`).

        Recorded readings go first, and a batch is never a mix of the two.
        """
        if self._sealed or self._flushing or self._paused or self._holding:
            return
        if not self._sendable():
            return
        if self._in_doubt:
            if self._stopping:
                return  # saved as it is, for the next start to ask
            self._flushing = True
            try:
                if not await self._async_settle_doubt():
                    return
            finally:
                self._flushing = False
            if self._closed or self._paused or self._stopping:
                return
        self._tidy_segments()
        if not self._sendable():
            return

        source = self._backfill if self._backfill else self._queue
        self._flushing = True
        batch = self._take(source)
        # From here until something accounts for it, this batch is the only
        # copy of those readings anywhere.
        self._inflight = batch
        self._inflight_from_backfill = source is self._backfill
        self.stats.queued = self._held()
        if source is self._backfill:
            self._backfill_room.set()

        try:
            try:
                result = await self._client.async_write(
                    encode_batch([item.line for item in batch])
                )
            except TagHistorianAuthError:
                # The key stopped working. Put the batch back and hand the entry
                # to Home Assistant's reauth flow, which is the one screen that
                # can take a replacement key. A repair issue here would be a
                # worse duplicate of a card Home Assistant already shows.
                self._over_allowance = False
                self._requeue(batch)
                self.stats.last_error = "authentication failed"
                self._flushing = False
                self.entry.async_start_reauth(self.hass)
                # And pause, like every other outcome that will be retried. A
                # revoked key is the LEAST likely of them to start working on
                # its own, so retrying it at the full rate of the house's state
                # changes is a request per reading against a door that needs a
                # person.
                self._writes_failing()
                self._pause(CONNECTION_RETRY_SECONDS)
                return
            except TagHistorianConnectionError as err:
                self._over_allowance = False
                self._errors.interrupted()
                unanswered = isinstance(err, TagHistorianUnansweredError)
                self._requeue(batch, in_doubt=unanswered, unanswered=unanswered)
                if unanswered:
                    # It may be more than the connection takes in the time a
                    # request has.
                    self._error_limit = max(1, len(batch) // 2)
                self.stats.last_error = str(err)
                self._flushing = False
                self._writes_failing()
                self._pause(CONNECTION_RETRY_SECONDS)
                return
            except Exception as err:  # nothing is lost to what nobody foresaw
                if self._log_error_now():
                    LOGGER.exception("Tag Historian could not send %s readings", len(batch))
                result = WriteResult(
                    WriteOutcome.SERVER_ERROR, message=str(err) or type(err).__name__
                )

            self._flushing = False
            self._handle_result(result, batch)
            self._inflight = None
            self._inflight_restored = False
            self._tidy_segments()
            if self._backfill_wanted:
                self._request_backfill()
            self.stats.queued = self._held()
            self._schedule_flush()
        finally:
            # However this method leaves - return, raise, or the
            # CancelledError an unload or a stop delivers straight into the
            # await above - a batch still sitting here is one nothing else will
            # ever account for.
            if self._inflight is not None:
                self._abandon_inflight()
                self._flushing = False

    def _take(self, source: deque[Queued]) -> list[Queued]:
        """Take the next batch out of ``source``, and remember where it came from."""
        restored = source is self._queue and bool(self._segments)
        self._inflight_restored = restored
        self._probing = False
        if self._suspect is not None:
            index = self._probe_index(source)
            if index is not None:
                item = source[index]
                del source[index]
                if restored:
                    self._segments[0] -= 1
                self._probing = True
                return [item]
            self._suspect = None
            self._probed.clear()
        limit = BACKFILL_BATCH_SIZE if source is self._backfill else MAX_POINTS_PER_REQUEST
        if restored:
            limit = min(limit, self._segments[0])
        if self._error_limit is not None:
            limit = min(limit, self._error_limit)
        take = min(len(source), limit)
        if restored:
            self._segments[0] -= take
        return [source.popleft() for _ in range(take)]

    def _probe_index(self, source: deque[Queued]) -> int | None:
        """The oldest reading of an entity not tried yet, behind the one under suspicion.

        Another entity's, so that its own readings still go in order. Where
        there is no other, the next one of the same.
        """
        if not source or source[0] is not self._suspect:
            return None
        end = self._front(source)
        alone = {source[index].entity_id for index in range(end)} == {self._suspect.entity_id}
        if alone:
            # Each of them is taken to be stored once the next is, so this is
            # not done lightly.
            if self._error_tries < SERVER_ERROR_TRIES * SERVER_ERROR_TRIES:
                return None
            if end < 2 or self._suspect.entity_id in self._probed:
                return None
            return 1
        for index in range(1, end):
            entity_id = source[index].entity_id
            if entity_id != self._suspect.entity_id and entity_id not in self._probed:
                return index
        return None

    def _front(self, source: deque[Queued]) -> int:
        """How many readings may be sent before something has to be read back."""
        end = self._segments[0] if source is self._queue and self._segments else len(source)
        return min(end, len(source))

    def _acknowledge(self, batch: list[Queued]) -> None:
        """Move the watermark over a batch Tag Historian has answered for, for good."""
        for item in batch:
            if item.timestamp > self._acked.get(item.entity_id, float("-inf")):
                self._acked[item.entity_id] = item.timestamp
        if batch is self._inflight:
            # Answered for: not on the wire any more, whatever is saved next.
            self._inflight = None
        self._save_within(WATERMARK_SAVE_SECONDS)

    def _abandon_inflight(self) -> None:
        """A batch whose request never came back: held, and in doubt."""
        batch = self._inflight or []
        self._inflight = None
        if not batch:
            return
        if not self._closed:
            self._requeue(batch, in_doubt=True, unanswered=True)
            return
        if self._inflight_saved:
            return  # the next start asks about it
        self.stats.dropped_inflight += len(batch)
        self.stats.last_error = f"{len(batch)} readings abandoned mid-write"
        self.stats.queued = self._held()
        LOGGER.warning(
            "Tag Historian abandoned %s readings that were already being "
            "written when the request was cut short, and could not save them; "
            "some or all of them may have been stored",
            len(batch),
        )

    async def _async_settle_doubt(self) -> bool:
        """Ask Tag Historian what it has, and let go of what it has already. True once settled.

        Its newest reading of a tag is moved once a batch is safely taken, and
        readings reach it oldest first per entity, so a reading no newer than
        that has been stored and a newer one has not. A name it has no reading
        in has stored none.

        When it cannot say, nothing in doubt is sent: a loss that is counted
        is asked about, a doubled reading in somebody's history is not. After
        a crash that is what was read or sent up to ``_doubt_until``, counted
        as the recorder is read (``_doubtful_until``) - never what this run
        listened to itself. And an error is not "cannot say" for as long as it
        may pass: the question is asked again meanwhile.
        """
        newest: dict[str, float | None] | None = None
        try:
            newest = await self._client.async_get_newest_readings()
        except TagHistorianAuthError:
            self.stats.last_error = "authentication failed"
            self.entry.async_start_reauth(self.hass)
            self._settle_errors.interrupted()
            self._ask_again(CONNECTION_RETRY_SECONDS)
            return False
        except Exception as err:  # whatever it is, it is not an answer
            self.stats.last_error = str(err) or type(err).__name__
            if not self._cannot_say(err):
                self._ask_again(max(CONNECTION_RETRY_SECONDS, getattr(err, "retry_after", 0)))
                return False
        if self._closed or self._stopping:
            # What has been saved, or is being, is not changed any more. The
            # next start asks.
            return False

        doubt, whole = self._doubt, self._doubt_all
        asked = {(entity_id, sent_as or entity_id) for entity_id, (_, sent_as, _) in doubt.items()}
        pairs: set[tuple[str, str]] = set()
        if whole:
            pairs = {(e, self._send_as.get(e, e)) for e in {*self._entity_ids, *self._acked}}
            pairs |= {(item.entity_id, item.encoded_for) for item in self._queue}
        answers = {pair: self._stored(newest, pair[1]) for pair in asked | pairs}
        now = dt_util.utcnow().timestamp()
        if newest is not None and not all(known for known, _ in answers.values()):
            self._settle_failures += 1
            if self._settle_failures < SETTLE_ATTEMPTS:
                self._ask_again(CONNECTION_RETRY_SECONDS)
                return False
        if now < self._unanswered_until and any(
            known and (at is None or at < int(doubt[entity_id][0]))
            for (entity_id, target) in asked
            for known, at in (answers[entity_id, target],)
        ):
            self._ask_again(min(CONNECTION_RETRY_SECONDS, self._unanswered_until - now))
            return False

        self._settle_failures = 0
        self._settle_errors.reset()
        self._doubt = {}
        self._doubt_all = False
        # Per entity, up to where Tag Historian has it all, and up to where
        # nobody can say.
        taken: dict[str, float] = {}
        unknown: dict[str, float] = {}
        missing = 0
        for entity_id, (stamp, sent_as, count) in doubt.items():
            known, at = answers[entity_id, sent_as or entity_id]
            if not known:
                unknown[entity_id] = stamp
                missing += count
            elif at is not None and int(stamp) <= at:
                if int(stamp) <= self._acked.get(entity_id, float("-inf")):
                    # The second of one already acknowledged: that may be the
                    # one Tag Historian's answer is about.
                    unknown[entity_id] = stamp
                    missing += count
                    continue
                taken[entity_id] = stamp
                if not whole:
                    self.stats.sent += count

        # The same after a crash, for what an earlier run held: per entity and
        # tag, because one that is not selected any more is sent as it was.
        stored = dict(taken)
        before_stored: dict[tuple[str, str], float] = {}
        before_unknown: dict[tuple[str, str], float] = {}
        recorder = history.recorder_ready(self.hass)
        for pair in pairs:
            entity_id, target = pair
            known, at = answers[pair]
            selected = entity_id in self._entity_ids
            current = target == self._send_as.get(entity_id, entity_id)
            if not known:
                if self._doubt_until:
                    before_unknown[pair] = self._doubt_until
                    if selected and current and recorder:
                        self._doubtful_until[entity_id] = self._doubt_until
            elif at is not None and (
                not (selected and current)
                or entity_id in self._acked
                or at >= self._since.get(entity_id, float("inf"))
            ):
                # To the second it is sent in: all of that second.
                before_stored[pair] = at + END_OF_SECOND
                if selected and current:
                    acked = self._acked.get(entity_id, float("-inf"))
                    self._same_second[entity_id] = (at, acked, set())
                if current:
                    stored[entity_id] = max(
                        stored.get(entity_id, float("-inf")), before_stored[pair]
                    )

        suspect = self._suspect
        if (
            suspect is not None
            and suspect.entity_id in doubt
            and suspect.timestamp < taken.get(suspect.entity_id, float("-inf"))
        ):
            # The next of its entity was tried, and is there. This one is not.
            self.stats.dropped_quota += 1
            self._suspect = None
            self._probed.clear()

        missing += self._let_go_up_to(
            taken, unknown, before_stored, before_unknown, counted=doubt
        )
        for entity_id, stamp in stored.items():
            if stamp > self._acked.get(entity_id, float("-inf")):
                self._acked[entity_id] = stamp
        for entity_id, (stamp, _sent_as, _count) in doubt.items():
            if entity_id in unknown and stamp > self._acked.get(entity_id, float("-inf")):
                self._acked[entity_id] = stamp
        if not recorder:
            for at, acked, held in self._same_second.values():
                self._count_mid_write(len(held) - (0 if acked >= at else 1))
            self._same_second.clear()
        if not self._doubtful_until:
            self._doubt_until = 0.0
            self._doubt_old = False
        if missing:
            self._count_mid_write(missing)
            LOGGER.warning(
                "Tag Historian could not say whether %s readings were stored before the "
                "connection or Home Assistant went away; they are not sent again",
                missing,
            )
        self.stats.queued = self._held()
        self._save_within(LOSS_SAVE_SECONDS)
        self._tidy_segments()
        # Now it is known where a gap begins.
        if self._backfill_wanted:
            self._request_backfill()
        return True

    def _ask_again(self, seconds: float) -> None:
        if self._stopping:
            return
        self._writes_failing()
        self._pause(seconds)

    def _cannot_say(self, err: Exception) -> bool:
        """Whether what Tag Historian answers about what it has is not going to become an answer."""
        if isinstance(err, TagHistorianError):
            refused = isinstance(err, TagHistorianPermissionError)
            if not refused and not isinstance(err, TagHistorianAnsweredError):
                self._settle_errors.interrupted()
                self._errors.interrupted()
                return False
            # Errors everywhere are one outage, whatever was asked.
            self._errors.error()
            limit = SETTLE_REFUSED_SECONDS if refused else SERVER_ERROR_GIVE_UP_SECONDS
            now = dt_util.utcnow()
            if self._unavailable_since is None:
                self._unavailable_since = now
            if self._settle_errors.error() < limit:
                self._repairs.async_maybe_raise_unavailable_issue(
                    self.hass, self.entry, (now - self._unavailable_since).total_seconds()
                )
                return False
            # The write path does not count the same day once more.
            self._errors.reset()
            return True
        self._settle_failures += 1
        return self._settle_failures >= SETTLE_ATTEMPTS

    def _stored(
        self, newest: dict[str, float | None] | None, sent_as: str
    ) -> tuple[bool, float | None]:
        """(whether Tag Historian can say, its newest reading of that entity's tag if it has any)."""
        if newest is None:
            return False, None
        name = tag_name_for_entity(sent_as)
        if name not in newest:
            return True, None
        at = newest[name]
        # A reading from the future is not one of ours, and says nothing.
        if at is None or at > dt_util.utcnow().timestamp() + 60:
            return False, None
        return True, at

    def _let_go_up_to(
        self,
        stored: dict[str, float],
        unknown: dict[str, float],
        before_stored: dict[tuple[str, str], float],
        before_unknown: dict[tuple[str, str], float],
        counted: dict[str, tuple[float, str, int]],
    ) -> int:
        """Take what is settled out of the buffers. How many of them nobody can say about.

        Not the ones ``counted`` already knows of. The two ``before`` are about
        what an earlier run held, and only that.
        """
        never = float("-inf")
        lost = 0

        def settled(item: Queued, *, restored: bool) -> bool:
            nonlocal lost
            stamp, entity_id = item.timestamp, item.entity_id
            if stamp <= stored.get(entity_id, never):
                return True
            if restored and stamp <= before_stored.get((entity_id, item.encoded_for), never):
                if (same := self._same_second.get(entity_id)) is not None and (
                    stamp >= same[0] and stamp > same[1]
                ):
                    same[2].add(stamp)
                return True
            known_of = stamp <= counted.get(entity_id, (never,))[0]
            if stamp <= unknown.get(entity_id, never):
                lost += not known_of
                return True
            if restored and stamp <= before_unknown.get((entity_id, item.encoded_for), never):
                lost += not known_of
                # The recorder may have it too; it is counted once.
                self._counted.setdefault(entity_id, set()).add(stamp)
                return True
            return False

        kept: list[Queued] = []
        restored = sum(self._segments)
        bounds: list[int] = []
        total = 0
        for count in self._segments:
            total += count
            bounds.append(total)
        left = [0] * len(bounds)
        for index, item in enumerate(self._queue):
            if settled(item, restored=index < restored):
                continue
            kept.append(item)
            if index < restored:
                left[next(n for n, bound in enumerate(bounds) if index < bound)] += 1
        self._queue = deque(kept)
        self._segments = deque(left)
        lost_before = lost
        self._backfill = deque(
            item for item in self._backfill if not settled(item, restored=False)
        )
        lost = lost_before  # the recorder has these, and counts them itself
        for entity_id in [e for e in self._counted if e not in self._doubtful_until]:
            del self._counted[entity_id]
        self._backfill_room.set()
        return lost

    @callback
    def _writes_failing(self) -> None:
        """Writes stopped going through: the watermark on disk is exact from here."""
        if not self._sending_ok:
            return
        self._sending_ok = False
        # A connection that comes and goes is not written down every time. The
        # file then says that more may have been sent than was, not less.
        if dt_util.utcnow().timestamp() - self._state_saved_at >= WATERMARK_SAVE_SECONDS:
            self._save_soon()
        else:
            self._save_within(WATERMARK_SAVE_SECONDS)

    @callback
    def _writes_going_through(self) -> None:
        """Writes are going through again: say so on disk, and fill any gap still open."""
        if not self._sending_ok:
            self._sending_ok = True
            if not self._disk_sending_ok:
                self._save_soon()
        if self._backfill_wanted:
            self._request_backfill()

    def _handle_result(self, result: WriteResult, batch: list[Queued]) -> None:
        # Set before anything below can requeue and evict: only "today's
        # allowance is spent" makes an eviction a reading over the allowance.
        # Any other answer - a delivery, an outage, a busy buffer - makes it one
        # to read back.
        if result.outcome is not WriteOutcome.SERVER_ERROR:
            self._errors.interrupted()
        self._over_allowance = result.outcome is WriteOutcome.DAILY_QUOTA

        if result.outcome is WriteOutcome.SERVER_ERROR:
            self._handle_server_error(batch, result.message)
            return

        if result.outcome in (WriteOutcome.OK, WriteOutcome.PARTIAL_TAG_QUOTA):
            self._server_answered()

        if result.outcome is WriteOutcome.OK:
            self._acknowledge(batch)
            self.stats.sent += len(batch)
            self.stats.last_error = ""
            self.stats.refused_tags = []
            self._unavailable_since = None
            # An issue that outlives its cause teaches people to ignore the
            # repairs panel, so a clean 204 clears every write-path issue.
            self._repairs.async_clear_write_issues(self.hass, self.entry)
            self._writes_going_through()
            return

        if result.outcome is WriteOutcome.PARTIAL_TAG_QUOTA:
            # Partial success: the points for tags that already exist WERE
            # written. Only the ones that would have created a new tag beyond
            # the quota were dropped, and re-sending this body would duplicate
            # everything that landed - the endpoint does not deduplicate. So
            # the batch is NOT requeued.
            #
            # The count comes from the body, because the body has it:
            # SkippedNewTagPoints counts POINTS. ``skipped_tags`` counts
            # distinct NAMES, and two refused entities contributing thirty
            # lines each would report 2 dropped and 98 of 100 delivered - 58
            # readings that never existed anywhere, reported as history.
            self._acknowledge(batch)
            refused = result.dropped_points
            if refused is None:
                # The API did not say. It never means "none" - a 422 is raised
                # by there being dropped points - so the whole batch is counted
                # as refused. Overstating a loss is visible and gets fixed;
                # understating one is exactly the silence this counter exists
                # to prevent.
                refused = len(batch)
            refused = min(max(0, refused), len(batch))
            self.stats.sent += len(batch) - refused
            self.stats.dropped_quota += refused
            self.stats.refused_tags = result.skipped_tags
            self.stats.last_error = result.message
            self._repairs.async_raise_tag_quota_issue(
                self.hass, self.entry, result.skipped_tags, refused
            )
            self._writes_going_through()
            return

        if result.outcome is WriteOutcome.DAILY_QUOTA:
            self._requeue(batch)
            self.stats.last_error = result.message
            self._repairs.async_raise_daily_quota_issue(
                self.hass, self.entry, result.retry_after
            )
            self._writes_failing()
            self._pause(_answered_wait(result.retry_after))
            return

        if result.outcome in (WriteOutcome.BACK_PRESSURE, WriteOutcome.UNAVAILABLE):
            # Tag Historian unable to take the batch for a while, and nothing
            # to do with the account's allowance: the 429 is its write buffer
            # refusing this account's share for a few seconds; a 503 is the
            # buffer full, or the key not CHECKED just now - trouble on its
            # side for a moment, which says nothing about the key (a key
            # checked and refused is a 401, and goes to reauthentication).
            # They share this branch because they share the honest answer:
            # hold the readings, wait the header out, and say nothing to the
            # user unless it lasts long enough to matter.
            self._requeue(batch)
            self.stats.last_error = result.message
            now = dt_util.utcnow()
            if self._unavailable_since is None:
                self._unavailable_since = now
            self._repairs.async_maybe_raise_unavailable_issue(
                self.hass,
                self.entry,
                (now - self._unavailable_since).total_seconds(),
            )
            self._writes_failing()
            self._pause(_answered_wait(result.retry_after))
            return

        # Answered for good either way below: the watermark moves over the
        # batch, so the recorder is never asked for it again.
        self._acknowledge(batch)

        if result.outcome is WriteOutcome.FORBIDDEN:
            # Permanent until a person acts. The batch is dropped rather than
            # queued: nothing about waiting changes an unconfirmed email
            # address, and holding readings for days would evict the ones that
            # arrive once it IS fixed.
            self.stats.dropped_quota += len(batch)
            self.stats.last_error = result.message
            self._repairs.async_raise_forbidden_issue(
                self.hass, self.entry, result.message
            )
            return

        # REJECTED: a body this integration should never have built. Dropped,
        # loudly, because retrying it would loop forever on the same bytes.
        self.stats.dropped_quota += len(batch)
        self.stats.last_error = result.message

    def _handle_server_error(self, batch: list[Queued], message: str) -> None:
        """An error of Tag Historian's own, or something nobody foresaw: nothing is dropped for it.

        Sent again as it is, a batch with one reading in it that cannot be
        taken would be at the front for good. So it is halved until it goes
        through or is one reading; and that one is not let go of before
        another entity's has been taken meanwhile, which tells a reading that
        cannot be taken from a Tag Historian that takes none. The latter is
        waited out, for SERVER_ERROR_GIVE_UP_SECONDS.
        """
        now = dt_util.utcnow()
        nothing_else_for = self._errors.error()
        if self._log_error_now():
            LOGGER.error(
                "Tag Historian answered a write with an error of its own (%s); the readings are "
                "held and sent again",
                message,
            )
        if self._errors_since is None:
            self._errors_since = now.timestamp()
            self._error_batch = len(batch)
        if self._unavailable_since is None:
            self._unavailable_since = now
        self.stats.last_error = message
        probing = self._probing
        self._requeue(batch, in_doubt=True)
        if self._closed:
            return
        self._writes_failing()

        source = self._backfill if self._inflight_from_backfill else self._queue
        if nothing_else_for >= SERVER_ERROR_GIVE_UP_SECONDS:
            count = min(self._error_batch, len(source))
            for _ in range(count):
                self._refuse(source)
            LOGGER.error(
                "Tag Historian has answered every write with an error for %s hours; "
                "%s readings are let go of",
                SERVER_ERROR_GIVE_UP_SECONDS // 3600,
                count,
            )
            self._forget_errors()
        elif probing:
            self._probed.add(batch[0].entity_id)
        elif len(batch) > 1:
            self._error_limit = len(batch) // 2
            self._error_tries = 0
        else:
            self._error_limit = 1
            if batch[0] is not self._last_failed:
                self._last_failed = batch[0]
                self._error_tries = 0
                self._error_proofs = 0
            self._error_tries += 1
            if source and source[0] is batch[0]:
                end = self._front(source)
                alone = {source[index].entity_id for index in range(end)} == {batch[0].entity_id}
                if alone and self._error_tries >= SERVER_ERROR_LONE_TRIES and self._held() > end:
                    LOGGER.error(
                        "Tag Historian has answered a reading of %s with an error %s times; "
                        "it is let go of",
                        batch[0].entity_id,
                        self._error_tries,
                    )
                    self._refuse(source)
                    self._suspect = None
                elif not alone or self._error_tries >= SERVER_ERROR_TRIES * SERVER_ERROR_TRIES:
                    self._suspect = batch[0]
        self._repairs.async_maybe_raise_unavailable_issue(
            self.hass, self.entry, (now - self._unavailable_since).total_seconds()
        )
        # Quickly only while others are being taken in between.
        failing = now.timestamp() - (self._errors_since or now.timestamp())
        quick = self._error_answered and failing < CONNECTION_RETRY_SECONDS
        self._pause(1 if quick else CONNECTION_RETRY_SECONDS)

    def _server_answered(self) -> None:
        """A batch was taken: what failed before it is not Tag Historian taking nothing."""
        self._errors.reset()
        if self._error_limit is None and self._errors_since is None:
            return
        self._errors_since = None
        self._error_answered = True
        if self._probing and self._suspect is not None:
            source = self._backfill if self._inflight_from_backfill else self._queue
            # The next of its own entity is tried once: Tag Historian's newest
            # reading is past this one now, so its answer about it would be.
            alone = self._inflight is not None and any(
                item.entity_id == self._suspect.entity_id for item in self._inflight
            )
            self._error_proofs += SERVER_ERROR_TRIES if alone else 1
            if self._error_proofs < SERVER_ERROR_TRIES:
                self._suspect = None
                self._probed.clear()
                return
            if source and source[0] is self._suspect:
                LOGGER.error(
                    "Tag Historian answers one reading of %s with an error every time; "
                    "it is let go of",
                    self._suspect.entity_id,
                )
                self._refuse(source)
            self._suspect = None
            self._probed.clear()
            return
        if self._error_limit is not None:
            self._error_limit *= 2
            if self._error_limit >= MAX_POINTS_PER_REQUEST:
                self._forget_errors()

    def _refuse(self, source: deque[Queued]) -> None:
        """Let go of the reading at the front: Tag Historian cannot take it."""
        item = self._pop_oldest() if source is self._queue else source.popleft()
        self._acknowledge([item])
        self._doubt.pop(item.entity_id, None)
        self.stats.dropped_quota += 1

    def _log_error_now(self) -> bool:
        """Whether an error is to be in the log: the first, and now and then while it goes on."""
        now = dt_util.utcnow().timestamp()
        if now - self._error_logged_at < ERROR_LOG_SECONDS:
            return False
        self._error_logged_at = now
        return True

    def _forget_errors(self) -> None:
        if self._error_answered:
            LOGGER.info("Tag Historian takes readings again")
        self._errors.reset()
        self._errors_since = None
        self._error_limit = None
        self._error_batch = 0
        self._error_tries = 0
        self._error_proofs = 0
        self._last_failed = None
        self._error_answered = False
        self._suspect = None
        self._probed.clear()

    def _requeue(
        self, batch: list[Queued], *, in_doubt: bool = False, unanswered: bool = False
    ) -> None:
        """Put an unsent batch back at the FRONT of where it came from, preserving order.

        ``in_doubt`` where it may have been taken all the same: a timeout, a
        connection that broke off, an error of Tag Historian's own.
        ``unanswered`` where it may even be on its way there still.
        """
        # Back in the buffer, so the in-flight guard has nothing left to count.
        self._inflight = None

        if self._closed:
            # The shutdown flush has already finished and the buffer has been
            # saved. A write that took longer than the wait and then came back
            # with "not taken, try again" is asking to be put into a queue
            # nobody will ever drain, which is the silent loss with an extra
            # extra step in it. Unless the buffer was saved with this batch in
            # it: then the next start asks about it.
            if not self._inflight_saved:
                self._count_shutdown_loss(len(batch))
            return

        if in_doubt:
            self._doubt.update(_in_doubt(batch))
        if unanswered:
            self._unanswered_until = dt_util.utcnow().timestamp() + UNANSWERED_SETTLE_SECONDS
        source = self._backfill if self._inflight_from_backfill else self._queue
        if self._probing and source and source[0] is self._suspect:
            source.insert(1, batch[0])
        else:
            source.extendleft(reversed(batch))
        self._probing = False
        if self._inflight_restored and self._segments:
            self._segments[0] += len(batch)
        self._inflight_restored = False
        if source is self._backfill:
            self.stats.queued = self._held()
            return

        while len(self._queue) > MAX_QUEUE_LENGTH:
            self._let_go(self._pop_oldest())
        if self._backfill_wanted:
            self._request_backfill()
        self.stats.queued = self._held()

    @callback
    def _pause(self, seconds: float) -> None:
        """Hold off writing for ``seconds``, then try again.

        The endpoint's ``Retry-After`` on a 429 counts down to the UTC midnight
        when the daily reading quota resets; on a 503 it is the write buffer's
        own estimate, or 30 to 45 seconds when the key could not be checked
        just now. Honouring it is half the reason this component exists -
        the built-in influxdb integration never reads the header: it sends a
        refused batch again only if ``max_retries`` is set, 20 seconds apart,
        then drops it (and set up for InfluxDB 2.x never sees the refusal), so
        an over-quota Home Assistant loses the rest of the day's readings.

        The floor of one second only keeps a zero from becoming a busy loop.
        A 429 or a 503 that names no usable wait has already been given
        CONNECTION_RETRY_SECONDS by :func:`_answered_wait`, so a missing or
        unparsable header never means a request a second.
        """
        self._paused = True
        self._cancel_timer()
        self._unsub_timer = async_call_later(
            self.hass, max(1.0, seconds), self._on_resume
        )

    @callback
    def _on_resume(self, _now: object) -> None:
        """The wait the API asked for has elapsed.

        Flush straight away rather than going back through the one-second batch
        timer: the buffer has already been sitting for however long Retry-After
        said, and adding another second of latency to that would be a strange
        thing to do on purpose.
        """
        self._unsub_timer = None
        self._paused = False
        if self._sendable():
            self._on_flush_due(None)
