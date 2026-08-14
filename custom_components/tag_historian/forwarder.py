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

The buffer is memory only, and it is emptied on the way out rather than
abandoned: see :meth:`StateForwarder.async_shutdown`, which an unload and a
Home Assistant stop both reach, by different doors and with the same grace.
Everything it cannot place moves a counter, because a reading that vanishes
without one is indistinguishable from a sensor that stopped reporting. That
invariant covers the batch that is already on the wire as well as the ones
still queued - see :meth:`StateForwarder.async_flush`, whose ``finally`` is the
only thing standing between a cancelled request and forty readings nobody can
account for.

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
from datetime import datetime

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import CALLBACK_TYPE, Event, HassJob, HomeAssistant, callback
from homeassistant.helpers.event import (
    EventStateChangedData,
    async_call_later,
    async_track_state_change_event,
)
from homeassistant.util import dt as dt_util

from .api import (
    TagHistorianAuthError,
    TagHistorianClient,
    TagHistorianConnectionError,
    WriteOutcome,
    WriteResult,
)
from .const import (
    BATCH_BUFFER_SIZE,
    BATCH_TIMEOUT_SECONDS,
    LOGGER,
    MAX_POINTS_PER_REQUEST,
    MAX_QUEUE_LENGTH,
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
# client's own HTTP timeout bounds each one.
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
    # Readings still in the buffer when Home Assistant stopped and the last
    # flush could not place them. The buffer is memory only, so this is where
    # a shutdown backlog goes, and it goes somewhere countable rather than
    # nowhere.
    dropped_shutdown: int = 0
    # Readings that had already LEFT the buffer and were in a request that
    # never finished - the entry reloaded underneath it, Home Assistant stopped
    # while it was still open, or the write raised something nobody
    # anticipated. Separate from dropped_shutdown because it is the only
    # counter that can overstate: the endpoint may well have taken the batch
    # before the socket went, and there is no way from here to know.
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
    ) -> None:
        self.hass = hass
        self.entry = entry
        self.stats = stats if stats is not None else ForwarderStats()
        self._client = client
        self._entity_ids = list(entity_ids)
        self._min_interval = min_interval
        self._queue: deque[str] = deque()
        self._last_sent: dict[str, datetime] = {}
        self._unsub_state: CALLBACK_TYPE | None = None
        self._unsub_timer: CALLBACK_TYPE | None = None
        self._flushing = False
        # The batch that is off the queue and on the wire right now, and the
        # task carrying it. Both exist so that a request which never returns
        # can still be accounted for by whoever notices - the request's own
        # ``finally``, or the shutdown that waited for it.
        self._inflight: list[str] | None = None
        self._flush_task: asyncio.Task[None] | None = None
        # Set once the shutdown flush has been and gone. After that the queue
        # is a dead end - nothing will ever flush it again - so anything that
        # tries to put readings back into it has to be counted instead.
        self._closed = False
        # Set while the API has told us to wait. The queue keeps filling (and
        # evicting) meanwhile; only sending is held.
        self._paused = False
        self._unavailable_since: datetime | None = None
        self._unsub_stop: CALLBACK_TYPE | None = None
        # Late import so that pulling in homeassistant.components.repairs (and
        # everything it drags with it) waits until an entry is actually being
        # set up, rather than happening when the integration is merely loaded.
        from . import repairs

        self._repairs = repairs

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

        if not self._entity_ids:
            LOGGER.debug("No entities selected; forwarding nothing")
            return

        self._unsub_state = async_track_state_change_event(
            self.hass, self._entity_ids, self._handle_state_change
        )

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

    async def async_shutdown(self) -> None:
        """Stop listening, wait for the wire, then place what is left.

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

        Three things happen here, in order:

        1. stop listening, so nothing new arrives;
        2. wait, bounded, for a request that is already on the wire, so the
           common case (a restart or a reload landing mid-write) DELIVERS
           those readings rather than losing them;
        3. flush what is still buffered, and count whatever will not go.

        The buffer is memory only and stays that way (the README says so in as
        many words); what this guarantees is that losing any part of it moves
        a number.
        """
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
        """One last delivery attempt, bounded, then count what is left.

        The pause is ignored on purpose. If the API asked us to wait until
        midnight this single request will be refused again and the readings are
        counted as lost, which is exactly what happens either way - but a
        connection that is merely slow, or a back-pressure window that has
        since cleared, gets the batch through.

        ``_flushing`` is NOT cleared the same way. It is only ever true while a
        request is genuinely in flight with a batch already popped off the
        queue, and forcing it false here would let this loop pop the same
        readings a second time and send them twice - the endpoint does not
        deduplicate. A flush already running is left to finish; whatever it
        does not take is counted below. :meth:`_async_settle_inflight` has
        already given it its chance by the time this runs.
        """
        self._paused = False

        for _ in range(FINAL_FLUSH_ATTEMPTS):
            if not self._queue:
                break
            before = len(self._queue)
            try:
                await self.async_flush()
            # Broad on purpose: a shutdown path that raises takes the rest
            # of the unload - or of Home Assistant's stop sequence - with it,
            # and losing the buffer uncounted is the exact failure this method
            # exists to stop.
            except Exception:
                LOGGER.exception("Tag Historian could not flush on shutdown")
                break
            if len(self._queue) >= before:
                # Requeued, refused, or paused again. Another attempt would be
                # the same request against the same answer.
                break

        self._cancel_timer()
        self._paused = False

        lost = len(self._queue)
        if lost:
            self._count_shutdown_loss(lost)
            self._queue.clear()
        self.stats.queued = len(self._queue)
        # From here the queue is a dead end. Anything arriving late - a write
        # that outlived _async_settle_inflight and then came back asking to be
        # requeued - has to be counted rather than parked in a buffer that no
        # longer has anything to flush it.
        self._closed = True

    def _count_shutdown_loss(self, lost: int) -> None:
        self.stats.dropped_shutdown += lost
        self.stats.last_error = f"{lost} readings lost on shutdown"
        LOGGER.warning(
            "Tag Historian dropped %s buffered readings that could not be "
            "delivered before shutdown; the buffer is held in memory only",
            lost,
        )

    @callback
    def _cancel_timer(self) -> None:
        if self._unsub_timer is not None:
            self._unsub_timer()
            self._unsub_timer = None

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

        line = encode_point(
            entity_id,
            value,
            stamp,
            new_state.attributes.get("unit_of_measurement"),
        )

        if len(self._queue) >= MAX_QUEUE_LENGTH:
            # Oldest first. During a long outage the recent readings are the
            # ones worth keeping, and an unbounded queue is an out-of-memory
            # kill of the whole Home Assistant process rather than a lost hour
            # of history.
            self._queue.popleft()
            self.stats.dropped_overflow += 1

        self._queue.append(line)
        self.stats.queued = len(self._queue)
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

    @callback
    def _schedule_flush(self) -> None:
        """Two triggers: a full buffer, or a quiet second.

        The timeout is what stops a house with one slow sensor from holding its
        reading hostage until ninety-nine more arrive; the size trigger is what
        stops a burst being sent one request per reading.
        """
        if self._closed or self._paused or self._flushing or not self._queue:
            return

        if len(self._queue) >= BATCH_BUFFER_SIZE:
            # A full buffer overtakes a pending timeout.
            self._cancel_timer()
            self._unsub_timer = async_call_later(self.hass, 0, self._on_flush_due)
            return

        if self._unsub_timer is None:
            self._unsub_timer = async_call_later(
                self.hass, BATCH_TIMEOUT_SECONDS, self._on_flush_due
            )

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
        deduplicate, so putting it back would risk doubling readings in
        somebody's history. It is counted as lost and logged instead - which
        can overstate the loss, and that is the direction this project has
        always chosen: an overstated loss gets asked about, an understated one
        is the silence the counters exist to prevent.
        """
        if self._flushing or self._paused or not self._queue:
            return

        self._flushing = True
        take = min(len(self._queue), MAX_POINTS_PER_REQUEST)
        batch = [self._queue.popleft() for _ in range(take)]
        # From here until something accounts for it, this batch is the only
        # copy of those readings anywhere.
        self._inflight = batch
        self.stats.queued = len(self._queue)

        try:
            try:
                result = await self._client.async_write(encode_batch(batch))
            except TagHistorianAuthError:
                # The key stopped working. Put the batch back and hand the entry
                # to Home Assistant's reauth flow, which is the one screen that
                # can take a replacement key. A repair issue here would be a
                # worse duplicate of a card Home Assistant already shows.
                self._requeue(batch)
                self.stats.last_error = "authentication failed"
                self._flushing = False
                self.entry.async_start_reauth(self.hass)
                # And pause, like every other outcome that will be retried. A
                # revoked key is the LEAST likely of them to start working on
                # its own, so retrying it at the full rate of the house's state
                # changes is a request per reading against a door that needs a
                # person.
                self._pause(CONNECTION_RETRY_SECONDS)
                return
            except TagHistorianConnectionError as err:
                self._requeue(batch)
                self.stats.last_error = str(err)
                self._flushing = False
                self._pause(CONNECTION_RETRY_SECONDS)
                return

            self._flushing = False
            self._handle_result(result, batch)
            self._inflight = None
            self.stats.queued = len(self._queue)
            self._schedule_flush()
        finally:
            # However this method leaves - return, raise, or the
            # CancelledError an unload or a stop delivers straight into the
            # await above - a batch still sitting here is one nothing else will
            # ever account for.
            if self._inflight is not None:
                self._abandon_inflight()
                self._flushing = False

    def _abandon_inflight(self) -> None:
        """Count a batch whose request never came back."""
        batch = self._inflight or []
        self._inflight = None
        if not batch:
            return

        self.stats.dropped_inflight += len(batch)
        self.stats.last_error = f"{len(batch)} readings abandoned mid-write"
        self.stats.queued = len(self._queue)
        LOGGER.warning(
            "Tag Historian abandoned %s readings that were already being "
            "written when the request was cut short; they are not re-sent, "
            "because the endpoint does not deduplicate and some or all of them "
            "may already have been stored",
            len(batch),
        )

    def _handle_result(self, result: WriteResult, batch: list[str]) -> None:
        if result.outcome is WriteOutcome.OK:
            self.stats.sent += len(batch)
            self.stats.last_error = ""
            self.stats.refused_tags = []
            self._unavailable_since = None
            # An issue that outlives its cause teaches people to ignore the
            # repairs panel, so a clean 204 clears every write-path issue.
            self._repairs.async_clear_write_issues(self.hass, self.entry)
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
            return

        if result.outcome is WriteOutcome.DAILY_QUOTA:
            self._requeue(batch)
            self.stats.last_error = result.message
            self._repairs.async_raise_daily_quota_issue(
                self.hass, self.entry, result.retry_after
            )
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
            self._pause(result.retry_after)
            return

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

    def _requeue(self, batch: list[str]) -> None:
        """Put an unsent batch back at the FRONT, preserving order.

        Only ever called where the API gave a definite answer that the batch
        was NOT taken - a 401, a socket error, a 429, a 503. That is the whole
        difference from :meth:`_abandon_inflight`, which handles the case where
        there is no answer at all.
        """
        # Back in the buffer, so the in-flight guard has nothing left to count.
        self._inflight = None

        if self._closed:
            # The shutdown flush has already finished. A write that took longer
            # than the wait and then came back with "not taken, try again" is
            # asking to be put into a queue nobody will ever drain, which is
            # the silent loss with an extra step in it.
            self._count_shutdown_loss(len(batch))
            return

        self._queue.extendleft(reversed(batch))
        overflow = len(self._queue) - MAX_QUEUE_LENGTH
        if overflow > 0:
            for _ in range(overflow):
                self._queue.popleft()
            self.stats.dropped_overflow += overflow
        self.stats.queued = len(self._queue)

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
        if self._queue:
            self._on_flush_due(None)
