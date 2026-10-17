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
  than the customer's allowance being spent. The built-in integration has a
  fixed retry schedule and no notion of partial success, so an over-quota Home
  Assistant simply stops producing data with nothing anywhere to say why.

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
deduplicate: a batch that was on the wire when its answer was lost counts as
delivered, never as a gap.

Everything it cannot place still moves a counter, because a reading that
vanishes without one is indistinguishable from a sensor that stopped
reporting. That invariant covers the batch that is already on the wire as
well as the ones still queued - see :meth:`StateForwarder.async_flush`, whose
``finally`` is the only thing standing between a cancelled request and forty
readings nobody can account for.

Timing note: everything scheduled here goes through ``async_call_later``, and
throttling compares the readings' OWN timestamps rather than a wall clock. Both
choices are about being able to test the behaviour by moving Home Assistant's
clock, which is the only way "backs off for the duration the header asked for"
is provable rather than asserted.
"""

from __future__ import annotations

import asyncio
from collections import deque
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any, NamedTuple

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import CALLBACK_TYPE, Event, HassJob, HomeAssistant, callback
from homeassistant.helpers.event import (
    EventStateChangedData,
    async_call_later,
    async_track_state_change_event,
)
from homeassistant.helpers.storage import Store
from homeassistant.util import dt as dt_util

from . import history
from .api import (
    TagHistorianAuthError,
    TagHistorianClient,
    TagHistorianConnectionError,
    WriteOutcome,
    WriteResult,
)
from .const import (
    BACKFILL_BATCH_SIZE,
    BATCH_BUFFER_SIZE,
    BATCH_TIMEOUT_SECONDS,
    LOGGER,
    MAX_POINTS_PER_REQUEST,
    MAX_QUEUE_LENGTH,
    RECORDER_WINDOW_HOURS,
)
from .line_protocol import (
    encode_batch,
    encode_point,
    is_boolean_domain,
    is_missing_state,
    state_to_value,
)

# How long to wait after a connection error before trying again. Shorter than
# any Retry-After the API sends, because a socket error is usually a blip.
CONNECTION_RETRY_SECONDS = 30

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


class Queued(NamedTuple):
    """One encoded reading, with what the acknowledged-watermark needs to know about it."""

    entity_id: str
    # The reading's own timestamp (``State.last_updated``) as epoch seconds,
    # full precision - the recorder is asked "after this" with exactly it.
    timestamp: float
    line: str


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
    # Readings that had already LEFT the buffer and were in a request that
    # never finished - the entry reloaded underneath it, Home Assistant stopped
    # while it was still open, or the write raised something nobody
    # anticipated. Separate from dropped_shutdown because it is the only
    # counter that can overstate: the endpoint may well have taken the batch
    # before the socket went, and there is no way from here to know. They are
    # not read back from the recorder either, for the same reason.
    # See :meth:`StateForwarder.async_flush`.
    dropped_inflight: int = 0
    # Readings the entity genuinely produced and this integration cannot
    # store: a state that is a word rather than a number, and not one of the
    # words that IS a two-state signal in that entity's domain - which covers
    # both `climate` at "heat", where no word would have been, and `cover` at
    # "opening", where two words are and this was neither. NOT the same as
    # "unavailable", which is Home Assistant saying it has nothing to report
    # and is correctly a gap. See :func:`.line_protocol.state_to_value`.
    dropped_not_numeric: int = 0
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
        # answered for - delivered, or refused for good (a 422, a 403), or on
        # the wire when its answer was lost. Everything up to it is accounted
        # for; a gap is what lies between it and the oldest reading still held.
        self._acked: dict[str, float] = {}
        self._last_sent: dict[str, datetime] = {}
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
        # The entity a backfill pass is reading right now, if any.
        self._reading_back: str | None = None
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

        * it did NOT stop: a crash, a power cut, a kill. Its buffer is gone
          and the watermark on disk is the one saved when it started, or when
          writes last began or stopped failing. If they were failing at the
          end - or that run had not had a single answer yet - nothing was
          acknowledged since that save and the watermark is exact: the whole
          outage is read back, the buffer that run had restored included. (The
          one reading that can then arrive twice is a retry Tag Historian took
          in the very moment of the crash, whose answer never came.) If they
          were going through, some readings after it may already be in Tag
          Historian, and the live endpoint does not deduplicate, so nothing is
          read back: the last second or so before the crash may be missing,
          and nothing is doubled. The recorder itself loses its last few
          seconds to a power cut, so this asks for no more than it can give.

        The store is then marked as a run in progress at once, so a crash
        after this does not bring back - and send twice - a buffer this run
        has already sent.
        """
        if self._store is None:
            return

        try:
            saved = await self._store.async_load() or {}
        except Exception:  # a corrupt file must not stop setup
            LOGGER.exception("Tag Historian could not read its saved buffer; starting empty")
            saved = {}

        acked = {str(k): float(v) for k, v in (saved.get("acked") or {}).items()}
        if saved.get("clean"):
            restored = [
                Queued(str(item[0]), float(item[1]), str(item[2]))
                for item in saved.get("queue") or []
                if isinstance(item, list | tuple) and len(item) == 3
            ][-MAX_QUEUE_LENGTH:]
            self._queue.extend(restored)
            self.stats.restored += len(restored)
            self.stats.queued = self._held()
            for item in restored:
                stamp = datetime.fromtimestamp(item.timestamp, UTC)
                if (last := self._last_sent.get(item.entity_id)) is None or stamp > last:
                    self._last_sent[item.entity_id] = stamp
            self._acked = acked
        elif saved and saved.get("sending_ok") is False:
            self._acked = acked
        elif saved:
            LOGGER.warning(
                "Home Assistant did not stop cleanly last time. Readings from its last "
                "moments may be missing in Tag Historian; they are not read back from the "
                "recorder, because some of them may already have been delivered"
            )

        for entity_id, stamp in self._acked.items():
            self._last_sent.setdefault(entity_id, datetime.fromtimestamp(stamp, UTC))

        await self._async_save(clean=False)

    @callback
    def async_start(self) -> None:
        """Subscribe to the selected entities.

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
        self._listening_since = dt_util.utcnow().timestamp()

        if self._entity_ids:
            self._unsub_state = async_track_state_change_event(
                self.hass, self._entity_ids, self._handle_state_change
            )
        else:
            LOGGER.debug("No entities selected; forwarding nothing")

        # What a previous run left behind goes out first.
        self._schedule_flush()
        self._request_backfill()

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
        if self._backfill_task is not None and not self._backfill_task.done():
            self._backfill_task.cancel()
        self._backfill_room.set()

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

        1. stop listening, so nothing new arrives;
        2. wait, bounded, for a request that is already on the wire, so the
           common case (a restart or a reload landing mid-write) DELIVERS
           those readings rather than losing them;
        3. flush what is still buffered, a few requests' worth;
        4. save whatever is left, with the watermark, for the next start.
        """
        self._stopping = True
        self.async_stop()
        await self._async_settle_inflight()
        await self._async_final_flush()

    async def _async_settle_inflight(self) -> None:
        """Give a request that is already on the wire time to answer.

        Deliberately NOT ``wait_for``: a timeout here must not cancel the
        request. Cancelling it would guarantee the loss this method exists to
        avoid, whereas letting it run means one of two honest endings - it
        answers and the readings are counted as sent, or Home Assistant cancels
        it moments later (the unload's own task sweep, or stage 2 of a stop)
        and :meth:`async_flush`'s ``finally`` counts the batch as abandoned.
        Both move a number.
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
        """A few last delivery attempts, bounded, then save what is left.

        The pause is ignored on purpose. If the API asked us to wait until
        midnight this single request will be refused again and the readings are
        saved for the next start, which is what happens either way - but a
        connection that is merely slow, or a back-pressure window that has
        since cleared, gets the batch through.

        ``_flushing`` is NOT cleared the same way. It is only ever true while a
        request is genuinely in flight with a batch already popped off the
        queue, and forcing it false here would let this loop pop the same
        readings a second time and send them twice - the endpoint does not
        deduplicate. A flush already running is left to finish; whatever it
        does not take is saved below. :meth:`_async_settle_inflight` has
        already given it its chance by the time this runs.

        The backfill's read-ahead is not saved: those readings are still in the
        recorder and still past the watermark, so the next start reads them
        again. (:meth:`_may_forfeit` is what keeps the second half true.)
        """
        self._paused = False

        for _ in range(FINAL_FLUSH_ATTEMPTS):
            if not self._queue and not self._backfill:
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

    async def _async_save(self, *, clean: bool) -> bool:
        """Write the watermark - and on a clean stop the buffer - to .storage.

        A batch still on the wire is saved as ACKNOWLEDGED. Its answer may yet
        say "not taken", and then it is counted as lost; the other way round,
        saving it as a gap, would have the next start read it back from the
        recorder and send it a second time if it HAD landed. Overstating a
        loss is visible; a doubled reading in somebody's history is not.
        """
        if self._store is None:
            return False

        acked = dict(self._acked)
        for item in self._inflight or []:
            if item.timestamp > acked.get(item.entity_id, float("-inf")):
                acked[item.entity_id] = item.timestamp

        data: dict[str, Any] = {
            "clean": clean,
            "sending_ok": self._sending_ok,
            "acked": acked,
            "queue": [list(item) for item in self._queue] if clean else [],
            "saved_at": dt_util.utcnow().isoformat(),
            "stopped_at": dt_util.utcnow().isoformat() if clean else None,
        }
        try:
            await self._store.async_save(data)
        except Exception:  # reported, and the caller counts the loss
            LOGGER.exception("Tag Historian could not save its buffer")
            return False
        return True

    @callback
    def _save_soon(self) -> None:
        """Save the watermark now, without waiting for it (an outage began or ended)."""
        if self._store is None or self._closed or self._stopping:
            return
        self.entry.async_create_background_task(
            self.hass, self._async_save(clean=False), "tag_historian_save"
        )

    @callback
    def _cancel_timer(self) -> None:
        if self._unsub_timer is not None:
            self._unsub_timer()
            self._unsub_timer = None

    # ------------------------------------------------------------------
    # Live readings
    # ------------------------------------------------------------------

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
        if (
            last is not None
            and (stamp - last).total_seconds() < self._min_interval
        ):
            # Deliberate. The daily quota counts every point SENT, including
            # ones deadband compression later discards server-side, so the only
            # place a reading can be saved is here.
            self.stats.dropped_throttled += 1
            return
        self._last_sent[entity_id] = stamp

        # The unit and the friendly name ride along on every reading, so the
        # tag in Tag Historian carries what Home Assistant shows - "Living room
        # temperature", 21.5 °C - and follows a rename here. The attribute,
        # not State.name: that falls back to a name made from the entity id,
        # and an entity without a friendly name should not be given one.
        line = encode_point(
            self._send_as.get(entity_id, entity_id),
            value,
            stamp,
            new_state.attributes.get("unit_of_measurement"),
            new_state.attributes.get("friendly_name"),
        )

        if len(self._queue) >= MAX_QUEUE_LENGTH:
            # Oldest first. During a long outage the recent readings are the
            # ones worth keeping, and an unbounded queue is an out-of-memory
            # kill of the whole Home Assistant process rather than a lost hour
            # of history.
            self._let_go(self._queue.popleft())

        self._queue.append(Queued(entity_id, stamp.timestamp(), line))
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

    def _held(self) -> int:
        return len(self._queue) + len(self._backfill)

    def _oldest_held(self, entity_id: str) -> float | None:
        """The timestamp of the oldest LIVE reading of this entity still in the buffer."""
        return min(
            (item.timestamp for item in self._queue if item.entity_id == entity_id),
            default=None,
        )

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
        """
        candidates = [
            item.timestamp
            for item in [*self._queue, *self._backfill, *(self._inflight or [])]
            if item.entity_id == entity_id
        ]
        if entity_id not in self._acked:
            candidates.append(self._listening_since)
        return min(candidates, default=None)

    def _let_go(self, item: Queued) -> None:
        """A reading the full buffer has evicted, oldest first. Counted either way."""
        self.stats.dropped_overflow += 1
        if self._over_allowance and self._may_forfeit(item.entity_id):
            self._forfeit(item)
            return
        # Not gone: the recorder still has it, and once Tag Historian answers
        # again a backfill pass reads it back.
        self._backfill_wanted = True
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

    def _may_forfeit(self, entity_id: str) -> bool:
        """Whether the watermark of this entity can move without stranding read-back readings.

        Readings of it already read back from an outage - waiting, on the
        wire, or being read by a pass right now - are older than any held one.
        A watermark moved past them would make them unreadable again: a
        restart, which drops the read-ahead because the recorder still has
        it, would lose them with no counter moving. Such an entity's eviction
        is read back like an outage's instead; the next day's evictions, with
        that backlog delivered, are let go of.
        """
        if entity_id == self._reading_back:
            return False
        waiting = list(self._backfill)
        if self._inflight and self._inflight_from_backfill:
            waiting.extend(self._inflight)
        return all(item.entity_id != entity_id for item in waiting)

    @callback
    def _request_backfill(self) -> None:
        """Start a backfill pass, unless one is running or there is no recorder to read."""
        if self._closed or self._backfill_running or not history.recorder_ready(self.hass):
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
        ``unavailable`` is a gap, and the minimum interval holds - measured
        from the acknowledged reading, and kept clear of the held one - so a
        gap that only ever held throttled readings stays empty.

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
        began = dt_util.utcnow().timestamp()
        try:
            for entity_id in list(self._entity_ids):
                if self._closed:
                    break
                # Where the gap begins: the newest reading of this entity that
                # is answered for, or already read back and waiting to go - a
                # second pass while the first one's readings are still held
                # must not read them again.
                acked = self._newest_accounted(entity_id)
                start = acked if acked is not None else self._listening_since
                held = self._oldest_held(entity_id)
                end = held if held is not None else began
                if end <= start:
                    continue
                self._reading_back = entity_id
                await self._async_backfill_entity(
                    entity_id, start, end, held is not None, throttle_from=acked
                )
                self._reading_back = None
        except asyncio.CancelledError:
            raise
        except Exception:  # the recorder failing must not stop the live path
            LOGGER.exception("Tag Historian could not read the recorder to fill a gap")
        finally:
            self._reading_back = None
            self._backfill_running = False
            self._schedule_flush()

        if self._backfill_wanted and not self._closed:
            self._request_backfill()

    async def _async_backfill_entity(
        self,
        entity_id: str,
        start: float,
        end: float,
        end_is_held: bool,
        throttle_from: float | None,
    ) -> None:
        window = timedelta(hours=RECORDER_WINDOW_HOURS)
        cursor = datetime.fromtimestamp(start, UTC)
        stop = datetime.fromtimestamp(end, UTC)
        # The minimum interval runs from the acknowledged reading. An entity
        # with none had nothing before its first live reading to be throttled
        # against, and neither has its first recorded one here.
        last = throttle_from
        found = 0

        while cursor < stop and not self._closed:
            window_end = min(cursor + window, stop)
            # The window opens one microsecond before the cursor so the
            # recorder's strict "after" does not drop a state stamped exactly
            # on a window boundary; the acknowledged reading itself is
            # excluded by the timestamp test below.
            states = await history.async_recorded_states(
                self.hass, entity_id, cursor - timedelta(microseconds=1), window_end
            )
            for state in states:
                stamp = state.last_updated.timestamp()
                if stamp <= start or stamp >= end or stamp < cursor.timestamp():
                    continue
                value = state_to_value(state.state, state.domain)
                if value is None:
                    continue
                if last is not None and stamp - last < self._min_interval:
                    continue
                if end_is_held and end - stamp < self._min_interval:
                    continue
                last = stamp

                while len(self._backfill) >= BACKFILL_BATCH_SIZE and not self._closed:
                    self._backfill_room.clear()
                    self._schedule_flush()
                    await self._backfill_room.wait()
                if self._closed:
                    return

                # Into the tag a live reading of it goes to: a renamed entity's
                # gap is part of the history its live readings carry on.
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
                        ),
                    )
                )
                found += 1
                self.stats.backfilled += 1
                self.stats.queued = self._held()
            cursor = window_end
            self._schedule_flush()

        if found:
            LOGGER.info(
                "Tag Historian read %s readings of %s back from the recorder to fill a gap",
                found,
                entity_id,
            )

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
        if self._closed or self._paused or self._flushing or not self._sendable():
            return

        if self._backfill or len(self._queue) >= BATCH_BUFFER_SIZE:
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
        if self._backfill:
            return True
        return bool(self._queue) and not self._backfill_running

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

        The batch is NOT requeued when that happens. The request may have been
        taken in full before the socket went, and Tag Historian does not
        deduplicate live writes, so putting it back would risk doubling
        readings in somebody's history. It is counted as lost and logged
        instead - which can overstate the loss, and that is the direction this
        project has always chosen: an overstated loss gets asked about, an
        understated one is the silence the counters exist to prevent.

        Recorded readings go first, and a batch is never a mix of the two.
        """
        if self._flushing or self._paused or not self._sendable():
            return

        source = self._backfill if self._backfill else self._queue
        self._flushing = True
        limit = BACKFILL_BATCH_SIZE if source is self._backfill else MAX_POINTS_PER_REQUEST
        take = min(len(source), limit)
        batch = [source.popleft() for _ in range(take)]
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
                self._requeue(batch)
                self.stats.last_error = str(err)
                self._flushing = False
                self._writes_failing()
                self._pause(CONNECTION_RETRY_SECONDS)
                return

            self._flushing = False
            self._handle_result(result, batch)
            self._inflight = None
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

    def _acknowledge(self, batch: list[Queued]) -> None:
        """Move the watermark over a batch Tag Historian has answered for, for good."""
        for item in batch:
            if item.timestamp > self._acked.get(item.entity_id, float("-inf")):
                self._acked[item.entity_id] = item.timestamp

    def _abandon_inflight(self) -> None:
        """Count a batch whose request never came back.

        It also moves the watermark over it: the endpoint may have taken it,
        and reading it back from the recorder later would send it twice.
        """
        batch = self._inflight or []
        self._inflight = None
        if not batch:
            return

        self._acknowledge(batch)
        self.stats.dropped_inflight += len(batch)
        self.stats.last_error = f"{len(batch)} readings abandoned mid-write"
        self.stats.queued = self._held()
        LOGGER.warning(
            "Tag Historian abandoned %s readings that were already being "
            "written when the request was cut short; they are not re-sent, "
            "because the endpoint does not deduplicate and some or all of them "
            "may already have been stored",
            len(batch),
        )

    @callback
    def _writes_failing(self) -> None:
        """Writes stopped going through: the watermark on disk is exact from here."""
        if self._sending_ok:
            self._sending_ok = False
            self._save_soon()

    @callback
    def _writes_going_through(self) -> None:
        """Writes are going through again: say so on disk, and fill any gap still open."""
        if not self._sending_ok:
            self._sending_ok = True
            self._save_soon()
        if self._backfill_wanted:
            self._request_backfill()

    def _handle_result(self, result: WriteResult, batch: list[Queued]) -> None:
        # Set before anything below can requeue and evict: only "today's
        # allowance is spent" makes an eviction a reading over the allowance.
        # Any other answer - a delivery, an outage, a busy buffer - makes it one
        # to read back.
        self._over_allowance = result.outcome is WriteOutcome.DAILY_QUOTA

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
            self._pause(result.retry_after)
            return

        if result.outcome in (WriteOutcome.BACK_PRESSURE, WriteOutcome.UNAVAILABLE):
            # Both are Tag Historian's own write buffer refusing to take more
            # for a few seconds, and neither is anything to do with the
            # account's allowance. They share this branch because they share
            # the honest answer: hold the readings, wait the header out, and
            # say nothing to the user unless it lasts long enough to matter.
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
            self._pause(result.retry_after)
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

    def _requeue(self, batch: list[Queued]) -> None:
        """Put an unsent batch back at the FRONT of where it came from, preserving order.

        Only ever called where the API gave a definite answer that the batch
        was NOT taken - a 401, a socket error, a 429, a 503. That is the whole
        difference from :meth:`_abandon_inflight`, which handles the case where
        there is no answer at all.
        """
        # Back in the buffer, so the in-flight guard has nothing left to count.
        self._inflight = None

        if self._closed:
            # The shutdown flush has already finished and the buffer has been
            # saved. A write that took longer than the wait and then came back
            # with "not taken, try again" is asking to be put into a queue
            # nobody will ever drain, which is the silent loss with an extra
            # step in it. It was saved as acknowledged, so it is not read back
            # from the recorder either: counted, as lost.
            self._count_shutdown_loss(len(batch))
            return

        if self._inflight_from_backfill:
            self._backfill.extendleft(reversed(batch))
            self.stats.queued = self._held()
            return

        self._queue.extendleft(reversed(batch))
        while len(self._queue) > MAX_QUEUE_LENGTH:
            self._let_go(self._queue.popleft())
        self.stats.queued = self._held()

    @callback
    def _pause(self, seconds: float) -> None:
        """Hold off writing for ``seconds``, then try again.

        The endpoint's ``Retry-After`` on a 429 counts down to the UTC midnight
        when the daily reading quota resets; on a 503 it is the write buffer's
        own estimate. Honouring it is half the reason this component exists -
        the built-in influxdb integration has a fixed 20/60 second schedule and
        never reads the header at all, so an over-quota Home Assistant spends
        the rest of the day re-sending into a closed door.

        The floor of one second is for a malformed or absent header only. It is
        deliberately NOT a fallback that shortens a long wait: a 429 with a
        header we failed to parse still gets a real pause.
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
