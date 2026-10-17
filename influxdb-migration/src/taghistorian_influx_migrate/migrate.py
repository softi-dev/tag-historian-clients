"""The plan (which tags, how many readings, what it costs) and the copy itself.

THE ORDER OF THINGS IS THE WHOLE DESIGN, because the write endpoint is the
live one and live data has rules a history copy has to respect:

* Tag Historian compares every reading with the newest reading the tag
  already holds, and drops one that repeats it (the deadband, 0.1% by
  default) as a repeat. Copying a year of history INTO a tag that live
  forwarding has already been writing for a week therefore compares the whole
  year with last week's value, and every historical reading equal to it is
  dropped - for an on/off sensor, every reading in the state it is in now.
  So by default a tag only receives readings NEWER than what it already
  holds: copy the history first, then point Home Assistant at Tag Historian.
  ``--include-older`` overrides it for whoever has already switched and
  accepts the cost, and says so.
* Tag Historian does not deduplicate either: a reading sent twice is stored
  twice. That same "only newer" floor, together with the resume file, is what
  makes running the copy again harmless.
* The daily readings allowance is shared with live forwarding, and Home
  Assistant does not wait for a refused write - it drops it. So the copy stops
  for the day well before the allowance does (``--leave-for-live``), and a big
  history is spread over as many days as that takes.
"""

from __future__ import annotations

import math
import sys
import time
from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass, field
from typing import TextIO

from .checkpoint import Checkpoint, TagState
from .influx import InfluxSource, PartialResult, SeriesKey, SeriesSummary, filter_series
from .lineprotocol import encode_line
from .naming import (
    FRIENDLY_NAME_FIELD,
    MAX_UNITS_LENGTH,
    UNIT_FIELD,
    VALUE_FIELD,
    derive_base_name,
    describe,
    entity_of,
    is_storable_number,
    measurement_reads_as_unit,
    sanitize_tag_name,
    usable_text,
)
from .taghistorian import (
    MAX_POINTS_PER_REQUEST,
    ExistingTag,
    Limits,
    TagHistorian,
    TagHistorianError,
)
from .timeutil import (
    NS_PER_SECOND,
    format_human,
    now_ns,
    seconds_until_next_utc_midnight,
    utc_day,
)

DAY_NS = 86_400 * NS_PER_SECOND

# How much history one read asks InfluxDB for. It starts at a day and follows
# the data: a window that comes back with more than TARGET points halves the
# next one, an empty or thin one doubles it, so a sensor that reports every
# second and one that reports twice a day both cost a sensible number of
# queries - and a sensor that was unplugged for a winter costs a few.
INITIAL_WINDOW_NS = DAY_NS
MIN_WINDOW_NS = 60 * NS_PER_SECOND
MAX_WINDOW_NS = 366 * DAY_NS
TARGET_POINTS_PER_READ = 20_000

# Exit codes, so a scheduled run can tell "finished" from "come back tomorrow".
EXIT_DONE = 0
EXIT_ERROR = 1
EXIT_PAUSED = 3


class MigrationStopped(Exception):
    """The copy cannot continue on this run; the message says why and what to do."""


@dataclass
class Options:
    include: Sequence[str] = ()
    exclude: Sequence[str] = ()
    measurements: Sequence[str] = ()
    unit_from_measurement: bool = True
    not_units: frozenset[str] = frozenset()
    include_older: bool = False
    leave_for_live: float = 0.25
    max_per_day: int | None = None
    batch_size: int = MAX_POINTS_PER_REQUEST
    wait: bool = False
    # Seconds to wait before asking whether an unanswered batch arrived: past
    # the 40-60 s in which a deploy runs two API revisions side by side, so the
    # question reaches the revision that holds the answer.
    settle_delay: float = 90.0
    retry_limit: int = 8

    def frozen(self) -> dict:
        """The options a resume has to repeat: the ones that decide what is copied, and how it is named."""
        return {
            "include": sorted(self.include),
            "exclude": sorted(self.exclude),
            "measurements": sorted(self.measurements),
            "unit_from_measurement": self.unit_from_measurement,
            "not_units": sorted(self.not_units),
        }


@dataclass
class TagPlan:
    tag_name: str
    entity: str
    sources: list[SeriesKey]
    readings: int
    friendly_name: str | None
    # Every unit the history carries. More than one when the entity's unit
    # changed (W to kW): InfluxDB keeps two series, Home Assistant one entity,
    # and the tag ends with the newest - as live forwarding would leave it.
    units: list[str] = field(default_factory=list)
    existing: ExistingTag | None = None
    # Copy only readings strictly newer than this; None = from the start.
    floor_ns: int | None = None
    note: str = ""

    @property
    def is_new(self) -> bool:
        return self.existing is None


@dataclass
class Plan:
    start_ns: int
    end_ns: int
    tags: list[TagPlan]
    limits: Limits | None = None
    readings_yesterday: int | None = None
    readings_today: int | None = None

    @property
    def readings(self) -> int:
        return sum(t.readings for t in self.tags)

    @property
    def new_tags(self) -> list[TagPlan]:
        return [t for t in self.tags if t.is_new]


def _predicted_unit(summary: SeriesSummary, options: Options) -> str | None:
    if summary.unit_field is not None:
        return usable_text(summary.unit_field, MAX_UNITS_LENGTH)
    if options.unit_from_measurement and measurement_reads_as_unit(
        summary.key.measurement, summary.key.tag_dict, options.not_units
    ):
        return summary.key.measurement
    return None


def build_plan(
    source: InfluxSource,
    destination: TagHistorian | None,
    start_ns: int,
    end_ns: int,
    options: Options,
    checkpoint: Checkpoint | None = None,
    clock: Callable[[], int] = now_ns,
) -> Plan:
    """Discover, select, group into Tag Historian tags, and look at the account."""
    summaries = filter_series(
        source.discover(start_ns, end_ns, options.measurements), options.include, options.exclude
    )

    by_tag: dict[str, TagPlan] = {}
    for summary in summaries:
        tags = summary.key.tag_dict
        name = sanitize_tag_name(derive_base_name(summary.key.measurement, tags))
        plan = by_tag.get(name)
        if plan is None:
            plan = by_tag[name] = TagPlan(
                tag_name=name,
                entity=entity_of(tags) or name,
                sources=[],
                readings=0,
                friendly_name=None,
            )
        plan.sources.append(summary.key)
        plan.readings += summary.readings
        plan.friendly_name = plan.friendly_name or summary.friendly_name
        unit = _predicted_unit(summary, options)
        if unit is not None and unit not in plan.units:
            plan.units.append(unit)

    # A tag the resume file already knows keeps every source it was copied
    # from, even one that has since dropped out of the range's discovery.
    if checkpoint is not None:
        for name, state in checkpoint.tags.items():
            if name in by_tag:
                for key in state.sources:
                    if key not in by_tag[name].sources:
                        by_tag[name].sources.append(key)

    plan = Plan(start_ns, end_ns, sorted(by_tag.values(), key=lambda t: t.tag_name))

    if destination is not None:
        plan.limits = destination.limits()
        existing = destination.existing_tags()
        daily = destination.daily_readings(days=2)
        today = utc_day(clock())
        plan.readings_today = daily.get(today, 0)
        earlier = [(day, count) for day, count in sorted(daily.items()) if day < today]
        if earlier:
            # What live forwarding alone sent yesterday: the account's total
            # less whatever this copy sent itself.
            day, count = earlier[-1]
            copied = checkpoint.days.get(day, 0) if checkpoint else 0
            plan.readings_yesterday = max(0, count - copied)

        for tag in plan.tags:
            tag.existing = existing.get(tag.tag_name)
            state = checkpoint.tags.get(tag.tag_name) if checkpoint else None
            ours = state.written_through_ns if state else None
            theirs = tag.existing.last_ns if tag.existing else None
            tag.floor_ns = ours
            # The server keeps time in 100 ns ticks (a .NET DateTime), so the
            # newest reading it reports is the copy's own last one cut down to
            # a tick, and anything up to 99 ns past it may be that very
            # reading. Compared at tick resolution, so the copy's own last
            # batch is not mistaken for somebody else's newer data; and the
            # floor taken from the server covers its whole tick, or the last
            # reading of a copy resumed without its resume file would be sent
            # a second time.
            if (
                theirs is not None
                and not options.include_older
                and (ours is None or theirs > ours // 100 * 100)
            ):
                tag.floor_ns = theirs + 99
                tag.note = f"already holds readings up to {format_human(theirs)}; only newer ones are copied"
            elif ours is not None:
                tag.note = f"copied up to {format_human(ours)} on an earlier run"
            elif tag.existing is not None and tag.existing.readings == 0:
                tag.note = "exists in Tag Historian, still empty"
            elif theirs is not None:
                tag.note = (
                    f"already holds readings up to {format_human(theirs)}; --include-older copies "
                    "all of the history into it, and readings an earlier copy wrote are written again"
                )
            if len(tag.units) > 1:
                tag.note = (tag.note + "; " if tag.note else "") + (
                    "its unit changed in InfluxDB, and the tag keeps the newest"
                )
    return plan


# ---------------------------------------------------------------------------
# Printing the plan
# ---------------------------------------------------------------------------


def _n(value: int) -> str:
    return f"{value:,}"


def describe_plan(plan: Plan, options: Options, out: TextIO, *, source_text: str) -> None:
    print(f"From: {source_text}", file=out)
    start = "the first reading" if plan.start_ns == 0 else format_human(plan.start_ns)
    print(f"Range: {start} to {format_human(plan.end_ns)}", file=out)
    print("", file=out)

    if not plan.tags:
        print(
            "Nothing to copy: no Home Assistant series (domain and entity_id tags, a numeric "
            "'value' field) matched in that range.",
            file=out,
        )
        return

    width = max(len(t.tag_name) for t in plan.tags)
    print(f"{'Tag':<{width}}  {'Readings':>11}  {'Unit':<8}  Friendly name", file=out)
    for tag in plan.tags:
        print(
            f"{tag.tag_name:<{width}}  {_n(tag.readings):>11}  {(', '.join(tag.units) or '-'):<8}  "
            f"{tag.friendly_name or '-'}",
            file=out,
        )
        if tag.note:
            print(f"{'':<{width}}  ^ {tag.note}", file=out)
    print("", file=out)

    print(f"{len(plan.tags)} tags, {_n(plan.readings)} readings in InfluxDB.", file=out)
    if plan.limits is None:
        print(
            "Give --api-key (or TAGHISTORIAN_API_KEY) to check this against your Tag Historian "
            "account: how many of these tags are new, whether your plan has room for them, and "
            "how many days the copy takes.",
            file=out,
        )
        return

    limits = plan.limits
    new = len(plan.new_tags)
    print(
        f"{new} of them are new tags. Your plan has room for {_n(limits.tags_free)} more "
        f"({_n(limits.tags_used)} of {_n(limits.tag_limit)} in use).",
        file=out,
    )
    if new > limits.tags_free:
        print(
            f"That is {new - limits.tags_free} too many. Narrow the selection with --entity or "
            "--exclude; the copy refuses to start a plan that cannot finish.",
            file=out,
        )

    per_day = copy_rate_per_day(plan, options)
    print(
        f"Your plan takes {_n(limits.readings_per_day)} readings a day, live forwarding included. "
        f"The copy stops each UTC day at {100 - round(options.leave_for_live * 100)}% of that "
        f"(--leave-for-live).",
        file=out,
    )
    if plan.readings_yesterday:
        print(
            f"Everything else on the account - live forwarding - sent {_n(plan.readings_yesterday)} "
            "yesterday, and the copy only has what is left of that share.",
            file=out,
        )
    if per_day <= 0:
        print(
            "Live forwarding alone used that share yesterday, so the copy would get nothing a "
            "day. Lower --leave-for-live, or copy before Home Assistant sends here live.",
            file=out,
        )
    else:
        days = max(1, math.ceil(plan.readings / per_day))
        print(
            f"At about {_n(per_day)} readings a day that is up to {days} "
            f"day{'s' if days != 1 else ''} of copying - fewer if some of it is already here. "
            "Run it once a day, or leave it running with --wait.",
            file=out,
        )


def copy_rate_per_day(plan: Plan, options: Options) -> int:
    """Readings a day the copy can count on: its share, less what live forwarding used yesterday."""
    if plan.limits is None:
        return 0
    share = math.floor(plan.limits.readings_per_day * (1 - options.leave_for_live))
    live = plan.readings_yesterday or 0
    rate = share - live
    if options.max_per_day is not None:
        rate = min(rate, options.max_per_day)
    return max(0, rate)


# ---------------------------------------------------------------------------
# The daily budget
# ---------------------------------------------------------------------------


class DailyBudget:
    """How many readings the copy may still send on the current UTC day.

    Two ceilings. The account's: the copy never takes today's total - its own
    readings AND live forwarding's - past ``(1 - leave_for_live)`` of the
    plan's daily allowance, because a Home Assistant write refused for the
    allowance is dropped by Home Assistant, not retried. And, if given, the
    copy's own ``--max-per-day``. Today's total is the server's count, read
    again every few minutes and at every new day; between reads the copy adds
    its own.
    """

    REFRESH_NS = 300 * NS_PER_SECOND

    def __init__(
        self,
        destination: TagHistorian,
        limits: Limits,
        options: Options,
        checkpoint: Checkpoint,
        clock: Callable[[], int],
    ) -> None:
        self._destination = destination
        self._cap = math.floor(limits.readings_per_day * (1 - options.leave_for_live))
        self._max_per_day = options.max_per_day
        self._checkpoint = checkpoint
        self._clock = clock
        self._day: str | None = None
        self._account_today = 0
        self._refreshed_at = 0
        self._exhausted_day: str | None = None

    def _refresh(self, now: int) -> None:
        today = utc_day(now)
        self._day = today
        self._account_today = self._destination.daily_readings(days=1).get(today, 0)
        self._refreshed_at = now

    def remaining(self) -> int:
        now = self._clock()
        today = utc_day(now)
        if self._exhausted_day == today:
            return 0
        if self._day != today or now - self._refreshed_at > self.REFRESH_NS:
            self._refresh(now)
        room = self._cap - self._account_today
        if self._max_per_day is not None:
            room = min(room, self._max_per_day - self._checkpoint.days.get(today, 0))
        return max(0, room)

    def spent(self, readings: int) -> None:
        today = utc_day(self._clock())
        self._account_today += readings
        self._checkpoint.days[today] = self._checkpoint.days.get(today, 0) + readings

    def exhausted(self) -> None:
        """The server said the day's allowance is gone: nothing more today, whatever the sums say."""
        self._exhausted_day = utc_day(self._clock())

    def spent_by_copy_today(self) -> int:
        return self._checkpoint.days.get(utc_day(self._clock()), 0)


# ---------------------------------------------------------------------------
# The copy
# ---------------------------------------------------------------------------


@dataclass
class _Line:
    timestamp_ns: int
    text: str


@dataclass
class Report:
    sent: int = 0
    tags_done: int = 0
    refused: list[tuple[str, str]] = field(default_factory=list)
    failed: list[tuple[str, str]] = field(default_factory=list)


class Runner:
    def __init__(
        self,
        source: InfluxSource,
        destination: TagHistorian,
        checkpoint: Checkpoint,
        options: Options,
        *,
        out: TextIO = sys.stdout,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], int] = now_ns,
    ) -> None:
        self._source = source
        self._destination = destination
        self._checkpoint = checkpoint
        self._options = options
        self._out = out
        self._sleep = sleep
        self._clock = clock
        self._budget: DailyBudget | None = None
        self.report = Report()

    def _say(self, text: str) -> None:
        print(text, file=self._out, flush=True)

    # -- lines ---------------------------------------------------------------

    def _lines(self, tag: TagPlan, start_ns: int, end_ns: int) -> Iterator[_Line]:
        """Every reading of the tag from start (inclusive) to end, as lines, oldest first."""
        cursor: int | None = None
        for key in tag.sources:
            first = self._source.first_reading(key, start_ns, end_ns)
            if first is not None and (cursor is None or first < cursor):
                cursor = first
        if cursor is None:
            return

        window = INITIAL_WINDOW_NS
        while cursor < end_ns:
            stop = min(end_ns, cursor + window)
            try:
                batch: list[_Line] = []
                for key in tag.sources:
                    batch.extend(self._render(key, self._source.read(key, cursor, stop)))
            except PartialResult:
                if window <= MIN_WINDOW_NS:
                    raise MigrationStopped(
                        f"InfluxDB cuts the result for {tag.entity} short even for a "
                        "one-minute window (its max-row-limit setting). Raise max-row-limit "
                        "in the InfluxDB configuration, or set it to 0, and run again."
                    ) from None
                window = max(MIN_WINDOW_NS, window // 2)
                continue

            batch.sort(key=lambda line: line.timestamp_ns)
            yield from batch
            cursor = stop
            if len(batch) > TARGET_POINTS_PER_READ:
                window = max(MIN_WINDOW_NS, window // 2)
            elif len(batch) < TARGET_POINTS_PER_READ // 4:
                window = min(MAX_WINDOW_NS, window * 2)

    def _render(self, key: SeriesKey, points: Sequence) -> Iterator[_Line]:
        tags = key.tag_dict
        for point in points:
            value = point.fields.get(VALUE_FIELD)
            if value is None or not is_storable_number(value):
                # A state that was not a number, or an attribute-only line:
                # live forwarding stores no reading for the entity either.
                continue
            described = describe(
                key.measurement,
                tags,
                point.fields,
                unit_from_measurement=self._options.unit_from_measurement,
                not_units=self._options.not_units,
            )
            fields: list[tuple[str, object]] = [(VALUE_FIELD, value)]
            if described.friendly_name is not None:
                fields.append((FRIENDLY_NAME_FIELD, described.friendly_name))
            if described.unit is not None:
                fields.append((UNIT_FIELD, described.unit))
            yield _Line(
                point.timestamp_ns,
                encode_line(key.measurement, tags, fields, point.timestamp_ns),  # type: ignore[arg-type]
            )

    # -- the run -------------------------------------------------------------

    def run(self, plan: Plan) -> int:
        if plan.limits is None:
            raise MigrationStopped("the copy needs the account's limits; this is a bug")

        new_needed = len([t for t in plan.new_tags if self._state(t).status == "pending"])

        # A tag an earlier run was refused for want of room is tried again
        # once there is room - a plan upgraded, or a tag deleted, since - but
        # only as many as fit, so a refusal that still stands cannot keep the
        # rest of the copy from starting.
        room = plan.limits.tags_free - new_needed
        for tag in plan.new_tags:
            state = self._state(tag)
            if state.status == "refused" and room > 0:
                state.status, state.reason = "pending", ""
                new_needed += 1
                room -= 1

        if new_needed > plan.limits.tags_free:
            raise MigrationStopped(
                f"The selection needs {new_needed} new tags and your plan has room for "
                f"{plan.limits.tags_free}. Narrow it with --entity or --exclude (a dry run "
                "lists them), or move to a plan with more tags."
            )

        self._budget = DailyBudget(
            self._destination, plan.limits, self._options, self._checkpoint, self._clock
        )

        for index, tag in enumerate(plan.tags, 1):
            state = self._state(tag)
            if state.status != "pending":
                continue
            self._say(f"[{index}/{len(plan.tags)}] {tag.tag_name}")
            result = self._copy_tag(tag, state, plan)
            if result is not None:
                return result

        self._summarise(plan)
        return EXIT_DONE

    def _state(self, tag: TagPlan) -> TagState:
        state = self._checkpoint.tags.get(tag.tag_name)
        if state is None:
            state = self._checkpoint.tags[tag.tag_name] = TagState(sources=list(tag.sources))
        else:
            for key in tag.sources:
                if key not in state.sources:
                    state.sources.append(key)
        return state

    def _copy_tag(self, tag: TagPlan, state: TagState, plan: Plan) -> int | None:
        if state.unconfirmed is not None:
            # Left by a run that stopped between sending a batch and hearing
            # back. If it arrived, the position moves past it; if not, it is
            # read from InfluxDB again below like any other.
            self._settle(tag, state)

        floor = tag.floor_ns
        if state.written_through_ns is not None and (
            floor is None or state.written_through_ns > floor
        ):
            floor = state.written_through_ns
        start = plan.start_ns if floor is None else max(plan.start_ns, floor + 1)

        full_batch = min(self._options.batch_size, MAX_POINTS_PER_REQUEST)
        pending: list[_Line] = []
        lines = self._lines(tag, start, plan.end_ns)
        exhausted = False
        while True:
            # Fill a batch: never more than the request limit or what is left
            # of today's share, and never split one instant across two
            # batches - the resume position is a timestamp, and a reading
            # sharing it with the end of a batch would be skipped on resume.
            assert self._budget is not None
            allowance = self._budget.remaining()
            if allowance <= 0:
                paused = self._pause_for_the_day()
                if paused is not None:
                    return paused
                continue
            limit = min(full_batch, allowance)

            while not exhausted and len(pending) <= limit:
                line = next(lines, None)
                if line is None:
                    exhausted = True
                    break
                pending.append(line)
            if not pending:
                break

            cut = min(limit, len(pending))
            if cut < len(pending):
                boundary = pending[cut].timestamp_ns
                while cut > 0 and pending[cut - 1].timestamp_ns == boundary:
                    cut -= 1
                if cut == 0:
                    if limit < full_batch:
                        # Today's remainder is smaller than the readings at
                        # this one instant; tomorrow's is not.
                        paused = self._pause_for_the_day()
                        if paused is not None:
                            return paused
                        continue
                    raise MigrationStopped(
                        f"{tag.entity} has more than {limit} readings at one instant, which "
                        "cannot be sent in one request."
                    )
            batch, pending = pending[:cut], pending[cut:]

            result = self._send(tag, state, batch)
            if result is not None:
                return result
            if state.status != "pending":
                return None

        state.status = "done"
        self._checkpoint.save()
        self.report.tags_done += 1
        self._say(f"    done: {_n(state.readings_sent)} readings copied in total")
        return None

    def _send(self, tag: TagPlan, state: TagState, batch: list[_Line]) -> int | None:
        """Send one batch until it is stored or the run has to stop.

        None means the batch is dealt with - stored, or its tag refused - and
        the copy carries on; an exit code means this run ends here.
        """
        attempts = 0
        while True:
            outcome = self._destination.write([line.text for line in batch])
            kind = outcome.kind

            if kind == "stored":
                self._acknowledge(tag, state, batch)
                return None

            if kind == "unanswered":
                state.unconfirmed = {
                    "first_ns": batch[0].timestamp_ns,
                    "last_ns": batch[-1].timestamp_ns,
                    "readings": len(batch),
                }
                self._checkpoint.save()
                if self._settle(tag, state):
                    return None
                attempts += 1
                if attempts > self._options.retry_limit and not self._options.wait:
                    raise MigrationStopped(
                        f"Tag Historian did not answer {attempts} times in a row ({outcome.message}). "
                        "Nothing was lost; run the same command again later."
                    )
                continue

            if kind == "retry":
                attempts += 1
                if attempts > self._options.retry_limit and not self._options.wait:
                    raise MigrationStopped(
                        f"Tag Historian kept asking the copy to wait ({outcome.status}: "
                        f"{outcome.message}). Nothing was lost; run the same command again later."
                    )
                delay = (
                    outcome.retry_after
                    if outcome.retry_after is not None
                    else min(60.0, 2.0**attempts)
                )
                self._say(
                    f"    Tag Historian asked for a pause ({outcome.status}); waiting {delay:.0f} s"
                )
                self._sleep(delay)
                continue

            if kind == "daily_quota":
                # The account's allowance ran out before the copy's own share
                # did: live forwarding sent more than usual today. Refused
                # before anything was stored, so the same batch goes tomorrow.
                assert self._budget is not None
                self._budget.exhausted()
                paused = self._pause_for_the_day()
                if paused is not None:
                    return paused
                continue

            if kind == "too_large":
                if len(batch) == 1:
                    raise MigrationStopped(
                        f"Tag Historian refused a single reading as too large: {outcome.message}"
                    )
                half = len(batch) // 2
                for part in (batch[:half], batch[half:]):
                    result = self._send(tag, state, part)
                    if result is not None or state.status != "pending":
                        return result
                return None

            if kind == "tag_refused":
                state.status = "refused"
                state.reason = outcome.message or "the plan has no room for another tag"
                self._checkpoint.save()
                self.report.refused.append((tag.tag_name, state.reason))
                self._say(f"    refused: {state.reason}")
                return None

            if kind == "invalid":
                state.status = "failed"
                state.reason = outcome.message
                self._checkpoint.save()
                self.report.failed.append((tag.tag_name, outcome.message))
                self._say(f"    refused as invalid, skipped: {outcome.message}")
                return None

            if kind == "storage_full":
                self._checkpoint.save()
                raise MigrationStopped(
                    "Your account's storage is full (402). Full-resolution data older than your "
                    "plan's window is removed nightly, which frees room by itself; a plan with "
                    "more storage frees it now. Run the same command again then - nothing was lost."
                )

            if kind == "denied":
                self._checkpoint.save()
                raise MigrationStopped(
                    f"Tag Historian refused the key ({outcome.status}): {outcome.message or 'no reason given'}. "
                    "The copy needs an API key whose Scope is Write, on an account whose email "
                    "address is confirmed."
                )

            raise MigrationStopped(f"unexpected answer from Tag Historian: {outcome}")

    def _acknowledge(self, tag: TagPlan, state: TagState, batch: list[_Line]) -> None:
        assert self._budget is not None
        state.written_through_ns = batch[-1].timestamp_ns
        state.readings_sent += len(batch)
        state.unconfirmed = None
        self._budget.spent(len(batch))
        self._checkpoint.save()
        self.report.sent += len(batch)
        span = max(1, self._checkpoint.end_ns - self._checkpoint.start_ns)
        done = (batch[-1].timestamp_ns - self._checkpoint.start_ns) / span
        self._say(
            f"    {_n(state.readings_sent)} readings, through {format_human(batch[-1].timestamp_ns)} "
            f"({max(0.0, min(1.0, done)):.0%} of the range)"
        )

    def _settle(self, tag: TagPlan, state: TagState) -> bool:
        """Find out whether a batch that got no answer was stored, without writing it twice.

        A batch is committed all or nothing, and it holds history: nothing else
        writes to this tag inside its time window. So one reading stored in
        that window means the batch arrived. None means it did not - or that
        every reading in it was a repeat the deadband dropped, and sending
        those again is dropped again just the same. Either way the answer
        decides whether to resend, and resending can no longer double
        anything.

        The question waits ``settle_delay`` first. A batch the server accepted
        sits in that revision's write buffer for a few minutes before it is
        on disk, and while a deploy runs two revisions side by side, the
        question could reach the other one; past the overlap there is only
        the one that took it.
        """
        unconfirmed = state.unconfirmed
        assert unconfirmed is not None
        self._say(
            f"    no answer for {unconfirmed['readings']} readings; asking Tag Historian whether "
            f"they arrived (in {self._options.settle_delay:.0f} s)"
        )
        attempts = 0
        while True:
            self._sleep(self._options.settle_delay)
            try:
                arrived = self._destination.stored_between(
                    tag.tag_name, unconfirmed["first_ns"], unconfirmed["last_ns"]
                )
                break
            except TagHistorianError as exc:
                attempts += 1
                if attempts > self._options.retry_limit and not self._options.wait:
                    raise MigrationStopped(
                        f"Tag Historian cannot be reached to check the last batch ({exc}). Run "
                        "the same command again later: the resume file remembers the batch and "
                        "checks it first."
                    ) from exc

        if arrived:
            assert self._budget is not None
            state.written_through_ns = unconfirmed["last_ns"]
            state.readings_sent += unconfirmed["readings"]
            self._budget.spent(unconfirmed["readings"])
            self.report.sent += unconfirmed["readings"]
            self._say("    it had arrived")
        else:
            self._say("    it had not arrived; it is sent again")
        state.unconfirmed = None
        self._checkpoint.save()
        return arrived

    def _pause_for_the_day(self) -> int | None:
        assert self._budget is not None
        self._checkpoint.save()
        wait_s = seconds_until_next_utc_midnight(self._clock()) + 60
        copied = self._budget.spent_by_copy_today()
        if not self._options.wait:
            hours, minutes = divmod(int(wait_s // 60), 60)
            self._say(
                f"Today's share of the daily allowance is used ({_n(copied)} readings copied today). "
                f"Run the same command again after 00:00 UTC - in about {hours} h {minutes} min - "
                "and it carries on where it stopped. Or add --wait to leave it running."
            )
            return EXIT_PAUSED
        self._say(f"Today's share is used; waiting until 00:00 UTC ({wait_s / 3600:.1f} h).")
        self._sleep(wait_s)
        return None

    def _summarise(self, plan: Plan) -> None:
        self._say("")
        self._say(f"Finished: {_n(self.report.sent)} readings copied on this run.")
        for name, reason in self.report.refused:
            self._say(f"  not copied, {name}: {reason}")
        for name, reason in self.report.failed:
            self._say(f"  not copied, {name}: {reason}")
