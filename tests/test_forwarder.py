"""Batching, throttling, and what happens on each thing the API can answer."""

from __future__ import annotations

from datetime import timedelta

import pytest
from homeassistant.core import HomeAssistant
from homeassistant.helpers import issue_registry as ir
from homeassistant.util import dt as dt_util
from pytest_homeassistant_custom_component.common import (
    MockConfigEntry,
    async_fire_time_changed,
)

from custom_components.tag_historian.api import (
    TagHistorianAuthError,
    TagHistorianConnectionError,
    WriteOutcome,
    WriteResult,
)
from custom_components.tag_historian.const import (
    BATCH_BUFFER_SIZE,
    CONF_API_KEY,
    CONF_HOST,
    DOMAIN,
    ISSUE_DAILY_QUOTA,
    ISSUE_SERVICE_UNAVAILABLE,
    ISSUE_TAG_QUOTA,
    ISSUE_WRITE_FORBIDDEN,
)
from custom_components.tag_historian.forwarder import (
    CONNECTION_RETRY_SECONDS,
    StateForwarder,
)
from custom_components.tag_historian.repairs import issue_id

WATCHED = ["sensor.house_power", "sensor.outdoor_temperature"]


class StubClient:
    """Records bodies and answers with a scripted sequence of results."""

    def __init__(self, *results) -> None:
        self.bodies: list[str] = []
        self._results = list(results)

    async def async_write(self, body: str):
        self.bodies.append(body)
        if not self._results:
            return WriteResult(WriteOutcome.OK)
        result = self._results.pop(0)
        if isinstance(result, Exception):
            raise result
        return result

    @property
    def lines(self) -> list[str]:
        return [line for body in self.bodies for line in body.splitlines()]


@pytest.fixture
def entry(hass: HomeAssistant) -> MockConfigEntry:
    entry = MockConfigEntry(
        domain=DOMAIN,
        data={CONF_HOST: "api.taghistorian.com", CONF_API_KEY: "not-a-real-key"},
        unique_id="11111111-2222-3333-4444-555555555555",
    )
    entry.add_to_hass(hass)
    return entry


@pytest.fixture
def make_forwarder(hass: HomeAssistant, entry: MockConfigEntry):
    """Build forwarders and make sure every one of them is stopped again.

    Home Assistant's test harness fails a test that leaves a timer behind, and
    a forwarder always has one pending while its buffer is non-empty. Stopping
    them here is what a real unload does through ``entry.async_on_unload``.
    """
    built: list[StateForwarder] = []

    def _build(_hass, _entry, client, entities=None, min_interval=0) -> StateForwarder:
        forwarder = StateForwarder(
            _hass,
            _entry,
            client,
            entities if entities is not None else WATCHED,
            min_interval,
        )
        forwarder.async_start()
        built.append(forwarder)
        return forwarder

    yield _build

    for forwarder in built:
        forwarder.async_stop()


async def _tick(hass: HomeAssistant, seconds: float) -> None:
    async_fire_time_changed(hass, dt_util.utcnow() + timedelta(seconds=seconds))
    await hass.async_block_till_done()


async def _advance(hass: HomeAssistant, freezer, seconds: float) -> None:
    """Move the clock as well as firing the timers.

    ``_tick`` fires whatever is due at a future point but leaves utcnow() where
    it was, which is all the batch timer needs. The repair issues that only
    appear after half an hour are measured against utcnow(), so proving one of
    those needs the clock itself to move.
    """
    freezer.tick(timedelta(seconds=seconds))
    async_fire_time_changed(hass)
    await hass.async_block_till_done()


async def test_readings_are_batched_rather_than_sent_one_by_one(
    hass: HomeAssistant, entry, make_forwarder
) -> None:
    client = StubClient()
    make_forwarder(hass, entry, client)

    hass.states.async_set("sensor.house_power", "1500")
    hass.states.async_set("sensor.outdoor_temperature", "21.5")
    await hass.async_block_till_done()
    assert client.bodies == []  # nothing yet - the quiet second has not passed

    await _tick(hass, 2)

    assert len(client.bodies) == 1
    assert len(client.lines) == 2


async def test_a_full_buffer_flushes_without_waiting_for_the_timeout(
    hass: HomeAssistant, entry, make_forwarder
) -> None:
    """A burst must not sit in memory for a second, nor go one request per point."""
    client = StubClient()
    make_forwarder(hass, entry, client)

    for i in range(BATCH_BUFFER_SIZE):
        hass.states.async_set("sensor.house_power", str(i))
    await hass.async_block_till_done()
    await _tick(hass, 0.1)

    assert len(client.bodies) == 1
    assert len(client.lines) == BATCH_BUFFER_SIZE


async def test_unselected_entities_are_never_encoded(
    hass: HomeAssistant, entry, make_forwarder
) -> None:
    """Nothing outside the selection produces bytes.

    This is the quota-safety property: a Home Assistant install has hundreds of
    entities and the smallest plan holds a handful of tags, so forwarding one
    unselected entity is a tag the user did not agree to spend. The subscription
    is asserted as well as the output, because a filter applied after the
    callback would still pass this test's second half on a quiet system.
    """
    client = StubClient()
    make_forwarder(hass, entry, client, entities=["sensor.house_power"])

    hass.states.async_set("sensor.house_power", "1500")
    hass.states.async_set("sensor.outdoor_temperature", "21.5")
    hass.states.async_set("sensor.phone_battery", "88")
    await hass.async_block_till_done()
    await _tick(hass, 2)

    assert len(client.lines) == 1
    assert "entity_id=house_power" in client.lines[0]
    assert not any("outdoor_temperature" in line for line in client.lines)
    assert not any("phone_battery" in line for line in client.lines)


async def test_unavailable_states_produce_no_point_at_all(
    hass: HomeAssistant, entry, make_forwarder
) -> None:
    """A gap, never an invented zero."""
    client = StubClient()
    make_forwarder(hass, entry, client)

    hass.states.async_set("sensor.house_power", "unavailable")
    hass.states.async_set("sensor.outdoor_temperature", "unknown")
    await hass.async_block_till_done()
    await _tick(hass, 2)

    assert client.bodies == []


async def test_the_minimum_interval_skips_readings_and_counts_them(
    hass: HomeAssistant, entry, make_forwarder
) -> None:
    """The daily allowance is spent on everything SENT, so throttle at source."""
    client = StubClient()
    forwarder = make_forwarder(hass, entry, client, min_interval=60)

    hass.states.async_set("sensor.house_power", "1500")
    hass.states.async_set("sensor.house_power", "1501")
    hass.states.async_set("sensor.house_power", "1502")
    await hass.async_block_till_done()
    await _tick(hass, 2)

    assert len(client.lines) == 1
    assert forwarder.stats.dropped_throttled == 2


async def test_422_is_not_retried_and_raises_a_fixable_repair_issue(
    hass: HomeAssistant, entry, make_forwarder
) -> None:
    """Partial success. Re-sending the body would duplicate what landed."""
    client = StubClient(
        WriteResult(
            WriteOutcome.PARTIAL_TAG_QUOTA,
            skipped_tags=["sensor.outdoor_temperature"],
            dropped_points=1,
            message="1 points dropped: tag quota exceeded (new tags: sensor.outdoor_temperature); points for existing tags were written",
        )
    )
    forwarder = make_forwarder(hass, entry, client)

    hass.states.async_set("sensor.house_power", "1500")
    hass.states.async_set("sensor.outdoor_temperature", "21.5")
    await hass.async_block_till_done()
    await _tick(hass, 2)
    await _tick(hass, 60)

    assert len(client.bodies) == 1, "a 422 must never be retried"
    assert forwarder.stats.dropped_quota == 1
    assert forwarder.stats.sent == 1

    issue = ir.async_get(hass).async_get_issue(
        DOMAIN, issue_id(entry, ISSUE_TAG_QUOTA)
    )
    assert issue is not None
    assert issue.is_fixable
    assert (
        issue.translation_placeholders["refused_tags"] == "sensor.outdoor_temperature"
    )


async def test_429_backs_off_for_the_retry_after_duration(
    hass: HomeAssistant, entry, make_forwarder
) -> None:
    """The header is honoured, not replaced with a schedule of our own.

    Home Assistant's built-in influxdb integration never reads Retry-After, so
    an over-quota instance re-sends into a closed door until UTC midnight. Here
    a one-hour Retry-After means one hour of silence, and then a retry.
    """
    client = StubClient(
        WriteResult(WriteOutcome.DAILY_QUOTA, retry_after=3600, message="daily quota"),
        WriteResult(WriteOutcome.OK),
    )
    forwarder = make_forwarder(hass, entry, client)

    hass.states.async_set("sensor.house_power", "1500")
    await hass.async_block_till_done()
    await _tick(hass, 2)
    assert len(client.bodies) == 1

    issue = ir.async_get(hass).async_get_issue(
        DOMAIN, issue_id(entry, ISSUE_DAILY_QUOTA)
    )
    assert issue is not None
    assert not issue.is_fixable

    # Well inside the hour the API asked for: still silent, and the reading is
    # still held rather than thrown away.
    await _tick(hass, 600)
    assert len(client.bodies) == 1
    assert forwarder.stats.queued == 1

    # Past it: one retry, and the issue clears on the clean answer.
    await _tick(hass, 3700)
    assert len(client.bodies) == 2
    assert (
        ir.async_get(hass).async_get_issue(DOMAIN, issue_id(entry, ISSUE_DAILY_QUOTA))
        is None
    )


async def test_503_requeues_and_stays_quiet_until_it_persists(
    hass: HomeAssistant, entry, make_forwarder
) -> None:
    """A blip that clears itself is not something to wake anyone for."""
    client = StubClient(
        WriteResult(WriteOutcome.UNAVAILABLE, retry_after=5, message="saturated"),
        WriteResult(WriteOutcome.OK),
    )
    forwarder = make_forwarder(hass, entry, client)

    hass.states.async_set("sensor.house_power", "1500")
    await hass.async_block_till_done()
    await _tick(hass, 2)

    assert forwarder.stats.queued == 1
    assert not ir.async_get(hass).issues

    await _tick(hass, 10)
    assert len(client.bodies) == 2
    assert forwarder.stats.sent == 1


async def test_403_surfaces_the_apis_own_message_verbatim(
    hass: HomeAssistant, entry, make_forwarder
) -> None:
    """The API already tells the user what to do; paraphrasing it would be a
    fifth customer-facing surface that can drift out of step with the code."""
    message = (
        "Confirm your email address to keep writing data. Your account was "
        "created more than 7 days ago and the address has not been confirmed."
    )
    client = StubClient(WriteResult(WriteOutcome.FORBIDDEN, message=message))
    make_forwarder(hass, entry, client)

    hass.states.async_set("sensor.house_power", "1500")
    await hass.async_block_till_done()
    await _tick(hass, 2)

    issue = ir.async_get(hass).async_get_issue(
        DOMAIN, issue_id(entry, ISSUE_WRITE_FORBIDDEN)
    )
    assert issue is not None
    assert issue.translation_placeholders["message"] == message


async def test_401_starts_reauth_rather_than_raising_an_issue(
    hass: HomeAssistant, entry, make_forwarder
) -> None:
    """A revoked key needs a screen that can accept a new one."""
    client = StubClient(TagHistorianAuthError("revoked"))
    forwarder = make_forwarder(hass, entry, client)

    hass.states.async_set("sensor.house_power", "1500")
    await hass.async_block_till_done()
    await _tick(hass, 2)

    flows = hass.config_entries.flow.async_progress_by_handler(DOMAIN)
    assert [flow["context"]["source"] for flow in flows] == ["reauth"]
    # The reading is kept: it will be sent once a working key is supplied.
    assert forwarder.stats.queued == 1


async def test_a_connection_error_keeps_the_batch_and_retries(
    hass: HomeAssistant, entry, make_forwarder
) -> None:
    client = StubClient(TagHistorianConnectionError("dns"), WriteResult(WriteOutcome.OK))
    forwarder = make_forwarder(hass, entry, client)

    hass.states.async_set("sensor.house_power", "1500")
    await hass.async_block_till_done()
    await _tick(hass, 2)
    assert forwarder.stats.queued == 1

    await _tick(hass, 60)
    assert forwarder.stats.sent == 1
    assert forwarder.stats.queued == 0


async def test_a_clean_write_clears_the_issues_it_raised(
    hass: HomeAssistant, entry, make_forwarder
) -> None:
    """An issue that outlives its cause trains people to ignore the panel."""
    client = StubClient(
        WriteResult(
            WriteOutcome.PARTIAL_TAG_QUOTA,
            skipped_tags=["sensor.outdoor_temperature"],
            dropped_points=1,
            message="1 points dropped: tag quota exceeded (new tags: sensor.outdoor_temperature)",
        ),
        WriteResult(WriteOutcome.OK),
    )
    make_forwarder(hass, entry, client)

    hass.states.async_set("sensor.house_power", "1500")
    await hass.async_block_till_done()
    await _tick(hass, 2)
    assert ir.async_get(hass).async_get_issue(DOMAIN, issue_id(entry, ISSUE_TAG_QUOTA))

    hass.states.async_set("sensor.house_power", "1600")
    await hass.async_block_till_done()
    await _tick(hass, 2)

    assert (
        ir.async_get(hass).async_get_issue(DOMAIN, issue_id(entry, ISSUE_TAG_QUOTA))
        is None
    )


async def test_an_empty_selection_subscribes_to_nothing(
    hass: HomeAssistant, entry, make_forwarder
) -> None:
    """The opposite reading would put a small plan over quota in one second."""
    client = StubClient()
    make_forwarder(hass, entry, client, entities=[])

    hass.states.async_set("sensor.house_power", "1500")
    await hass.async_block_till_done()
    await _tick(hass, 2)

    assert client.bodies == []


# --------------------------------------------------------------------------
# D3 - what a 422 actually costs
# --------------------------------------------------------------------------


async def test_a_422_counts_the_points_the_api_dropped_not_the_tag_names(
    hass: HomeAssistant, entry, make_forwarder
) -> None:
    """The 422 body opens with the number, and the number is POINTS.

    A batch of six lines, two entities refused, three lines each. Counting
    ``len(skipped_tags)`` gives 2 dropped and 4 sent - so four readings that
    reached nothing are reported as delivered, and the counter that exists to
    make a loss visible is the thing hiding it.
    """
    client = StubClient(
        WriteResult(
            WriteOutcome.PARTIAL_TAG_QUOTA,
            skipped_tags=["sensor.outdoor_temperature"],
            dropped_points=3,
            message=(
                "3 points dropped: tag quota exceeded "
                "(new tags: sensor.outdoor_temperature); "
                "points for existing tags were written"
            ),
        )
    )
    forwarder = make_forwarder(hass, entry, client)

    for value in ("1500", "1501", "1502"):
        hass.states.async_set("sensor.house_power", value)
    for value in ("21.5", "21.6", "21.7"):
        hass.states.async_set("sensor.outdoor_temperature", value)
    await hass.async_block_till_done()
    await _tick(hass, 2)

    assert len(client.lines) == 6
    # Three points refused, three delivered. Counting names would say 1 and 5.
    assert forwarder.stats.dropped_quota == 3
    assert forwarder.stats.sent == 3
    assert forwarder.stats.dropped_quota + forwarder.stats.sent == 6


async def test_a_422_we_cannot_read_is_counted_as_a_total_loss(
    hass: HomeAssistant, entry, make_forwarder
) -> None:
    """The prose scrape failing must not report the batch as delivered.

    ``refused = 0`` meant ``sent += len(batch)``: a total loss reported as
    complete success, which is the exact failure mode the counters exist to
    rule out. Overstating a loss is visible and gets asked about.
    """
    client = StubClient(
        WriteResult(
            WriteOutcome.PARTIAL_TAG_QUOTA,
            skipped_tags=[],
            dropped_points=None,
            message="some future wording nobody parsed",
        )
    )
    forwarder = make_forwarder(hass, entry, client)

    hass.states.async_set("sensor.house_power", "1500")
    hass.states.async_set("sensor.outdoor_temperature", "21.5")
    await hass.async_block_till_done()
    await _tick(hass, 2)

    assert forwarder.stats.sent == 0
    assert forwarder.stats.dropped_quota == 2


# --------------------------------------------------------------------------
# D4 - the two 429s
# --------------------------------------------------------------------------


async def test_back_pressure_never_raises_the_daily_quota_card(
    hass: HomeAssistant, entry, make_forwarder
) -> None:
    """A five-second hiccup of ours is not a billing event of theirs.

    ``InfluxWriteController`` answers 429 for write-buffer back-pressure with
    Retry-After around five seconds. Routed to the daily-quota card that
    rendered "your account has used its reading allowance for today", "resets
    in about 1 hour" - because max(1, round(7/3600)) is 1 - and a link to the
    pricing page.
    """
    client = StubClient(
        WriteResult(
            WriteOutcome.BACK_PRESSURE,
            retry_after=7,
            message="write buffer fair-share exceeded; retry after 7s",
        ),
        WriteResult(WriteOutcome.OK),
    )
    forwarder = make_forwarder(hass, entry, client)

    hass.states.async_set("sensor.house_power", "1500")
    await hass.async_block_till_done()
    await _tick(hass, 2)

    registry = ir.async_get(hass)
    assert registry.async_get_issue(DOMAIN, issue_id(entry, ISSUE_DAILY_QUOTA)) is None
    # And nothing else either: it has not lasted long enough to be worth a card.
    assert not registry.issues

    # The reading is held, the header is honoured, and it goes on the retry.
    assert forwarder.stats.queued == 1
    await _tick(hass, 10)
    assert forwarder.stats.sent == 1


async def test_back_pressure_that_persists_becomes_the_busy_card_not_the_quota_one(
    hass: HomeAssistant, entry, make_forwarder, freezer
) -> None:
    """Half an hour of it is worth saying - and still not about the plan."""
    client = StubClient(
        *[
            WriteResult(
                WriteOutcome.BACK_PRESSURE,
                retry_after=7,
                message="write buffer fair-share exceeded; retry after 7s",
            )
            for _ in range(50)
        ]
    )
    make_forwarder(hass, entry, client)

    hass.states.async_set("sensor.house_power", "1500")
    await hass.async_block_till_done()
    await _advance(hass, freezer, 2)
    for _ in range(4):
        await _advance(hass, freezer, 600)

    registry = ir.async_get(hass)
    assert registry.async_get_issue(DOMAIN, issue_id(entry, ISSUE_DAILY_QUOTA)) is None
    issue = registry.async_get_issue(DOMAIN, issue_id(entry, ISSUE_SERVICE_UNAVAILABLE))
    assert issue is not None
    assert not issue.is_fixable
    # No pricing link, because there is nothing to buy that fixes our buffer.
    assert issue.learn_more_url is None


# --------------------------------------------------------------------------
# D9 - a key that will never work again
# --------------------------------------------------------------------------


async def test_a_revoked_key_pauses_instead_of_retrying_at_full_rate(
    hass: HomeAssistant, entry, make_forwarder
) -> None:
    """Reauth needs a person; retrying meanwhile is a request per reading.

    Every other retryable outcome pauses. This one requeued and went straight
    back round, so a busy house hammered a dead credential at the rate of its
    own state changes for as long as nobody looked at the notification.
    """
    client = StubClient(*[TagHistorianAuthError("revoked")] * 50)
    forwarder = make_forwarder(hass, entry, client)

    hass.states.async_set("sensor.house_power", "1500")
    await hass.async_block_till_done()
    await _tick(hass, 2)
    assert len(client.bodies) == 1

    # More readings arrive; none of them provokes another attempt.
    for value in ("1501", "1502", "1503"):
        hass.states.async_set("sensor.house_power", value)
        await hass.async_block_till_done()
        await _tick(hass, 2)

    assert len(client.bodies) == 1, "paused, not spinning"
    assert forwarder.stats.queued == 4

    # And it does come back, once the pause is over.
    await _tick(hass, CONNECTION_RETRY_SECONDS + 5)
    assert len(client.bodies) == 2


# --------------------------------------------------------------------------
# D2 - the buffer at shutdown
# --------------------------------------------------------------------------


async def test_the_buffer_is_flushed_when_the_forwarder_shuts_down(
    hass: HomeAssistant, entry, make_forwarder
) -> None:
    """An options change, the repair flow and a restart all arrive here.

    All three used to drop up to MAX_QUEUE_LENGTH readings with no counter
    moving anywhere - and the repair flow is REACHED by being over quota, so
    the backlog is at its largest exactly when it was thrown away.
    """
    client = StubClient()
    forwarder = make_forwarder(hass, entry, client)

    hass.states.async_set("sensor.house_power", "1500")
    hass.states.async_set("sensor.outdoor_temperature", "21.5")
    await hass.async_block_till_done()
    assert client.bodies == [], "still inside the batch window"

    await forwarder.async_shutdown()

    assert len(client.lines) == 2
    assert forwarder.stats.sent == 2
    assert forwarder.stats.queued == 0
    assert forwarder.stats.dropped_shutdown == 0


async def test_stopping_home_assistant_flushes_the_buffer(
    hass: HomeAssistant, entry, make_forwarder
) -> None:
    """The stop path, not just the unload path.

    ``hass.async_stop()`` and not ``hass.bus.async_fire(...STOP)``. Firing the
    event by hand is what this test used to do, and it is why the stop path
    looked covered while it was cancelling writes: the event on its own skips
    the stage that cancels every background task, which is the only part of a
    real stop that can hurt. See tests/test_init.py, G5, for the batch on the
    wire; this one is about the buffer behind it.
    """
    client = StubClient()
    forwarder = make_forwarder(hass, entry, client)

    hass.states.async_set("sensor.house_power", "1500")
    await hass.async_block_till_done()

    await hass.async_stop()

    assert len(client.lines) == 1
    assert forwarder.stats.sent == 1


async def test_what_cannot_be_flushed_on_shutdown_is_counted(
    hass: HomeAssistant, entry, make_forwarder
) -> None:
    """The buffer is memory only. Losing it is allowed; losing it quietly is not.

    The README's promise is that if data is missing there is always a counter
    that moved, and this is the case that made it false.
    """
    client = StubClient(*[TagHistorianConnectionError("down")] * 10)
    forwarder = make_forwarder(hass, entry, client)

    hass.states.async_set("sensor.house_power", "1500")
    hass.states.async_set("sensor.outdoor_temperature", "21.5")
    await hass.async_block_till_done()

    await forwarder.async_shutdown()

    assert forwarder.stats.sent == 0
    assert forwarder.stats.dropped_shutdown == 2
    assert forwarder.stats.queued == 0
    assert "lost on shutdown" in forwarder.stats.last_error


async def test_a_write_that_comes_back_after_the_shutdown_is_still_counted(
    hass: HomeAssistant, entry, make_forwarder
) -> None:
    """The last hole in the invariant, found by reading the README again.

    A write can outlive the bounded wait and then answer "not taken, try
    again" - a socket error, a 503, a daily-quota 429. Every one of those
    requeues, and requeueing after the final flush has already run puts the
    readings into a buffer that nothing will ever drain: no counter, no log,
    and a queued count that quietly climbs on an object Home Assistant has
    already let go of.
    """
    client = StubClient(WriteResult(WriteOutcome.UNAVAILABLE, retry_after=5))
    forwarder = make_forwarder(hass, entry, client)

    hass.states.async_set("sensor.house_power", "1500")
    hass.states.async_set("sensor.outdoor_temperature", "21.5")
    await hass.async_block_till_done()

    # The shutdown has been and gone with the buffer empty...
    await forwarder.async_shutdown()
    assert forwarder.stats.dropped_shutdown == 2

    # ...and now a straggler asks for its batch to be put back.
    forwarder._requeue(["a", "b", "c"])

    assert forwarder.stats.queued == 0, "there is nothing left to drain it"
    assert forwarder.stats.dropped_shutdown == 5


# --------------------------------------------------------------------------
# G4 - a word is a reading only where the domain says so
# --------------------------------------------------------------------------

HOUSE_MODES = ("home", "night", "away", "guest")


async def test_a_multi_state_text_sensor_stores_nothing_and_says_so(
    hass: HomeAssistant, entry, make_forwarder
) -> None:
    """One point out of four state changes is not a series, it is a trap.

    ``sensor.house_mode`` cycles home/night/away/guest. "home" was in the
    on/off table, so the sensor wrote a lone 1.0 for that one state and
    nothing for the other three - and ``_handle_state_change`` returned early
    on "there was no reading to lose", so no counter moved either. The
    customer spent a tag on a chart that is one dot, and nothing anywhere said
    why.

    Now: nothing is written at all, all four changes are counted, and the
    entity is named where a person can find it.
    """
    client = StubClient()
    forwarder = make_forwarder(
        hass, entry, client, entities=["sensor.house_mode"]
    )

    for mode in HOUSE_MODES:
        hass.states.async_set("sensor.house_mode", mode)
    await hass.async_block_till_done()
    await _tick(hass, 2)

    assert client.bodies == [], "a lone 1.0 for 'home' is worse than nothing"
    assert forwarder.stats.sent == 0
    # Four state changes, four readings this integration could not store, four
    # counted. Not zero, which is what the silence looked like.
    assert forwarder.stats.dropped_not_numeric == 4
    assert forwarder.stats.unstorable_entities == ["sensor.house_mode"]


async def test_the_unstorable_log_line_says_which_kind_of_unstorable_it_is(
    hass: HomeAssistant, entry, make_forwarder, caplog
) -> None:
    """Two different reasons reach the same counter, and the log has to tell.

    ``climate.living_room`` at ``heat`` is a domain where no word is ever
    stored. ``cover.garage`` at ``opening`` is a domain where two words are -
    open and closed, which is the series the customer is already looking at -
    and this was neither of them. One line was being printed for both, and for
    the cover it said ``cover`` "is not a domain where that word is a two-state
    signal", which contradicts the chart on the same screen.
    """
    client = StubClient()
    forwarder = make_forwarder(
        hass, entry, client, entities=["cover.garage", "climate.living_room"]
    )

    hass.states.async_set("cover.garage", "opening")
    hass.states.async_set("climate.living_room", "heat")
    await hass.async_block_till_done()
    await _tick(hass, 2)

    assert client.bodies == []
    assert forwarder.stats.dropped_not_numeric == 2

    cover = next(line for line in caplog.messages if "cover.garage" in line)
    climate = next(line for line in caplog.messages if "climate.living_room" in line)

    assert "not one of the words that count as a two-state signal for cover" in cover
    assert "cover is not a domain" not in cover
    assert "climate is not a domain where a word can be a two-state signal" in climate


async def test_a_genuine_two_state_signal_still_works(
    hass: HomeAssistant, entry, make_forwarder
) -> None:
    """The gate must not have cost the case it was protecting.

    A binary_sensor IS on/off - that is the whole domain - so a heat pump's
    duty cycle is exactly the 1/0 series a historian is for, and it is one of
    the reasons this integration offers binary sensors at all.
    """
    client = StubClient()
    forwarder = make_forwarder(
        hass,
        entry,
        client,
        entities=["binary_sensor.heat_pump_running", "cover.garage", "light.hall"],
    )

    hass.states.async_set("binary_sensor.heat_pump_running", "on")
    hass.states.async_set("binary_sensor.heat_pump_running", "off")
    hass.states.async_set("cover.garage", "open")
    hass.states.async_set("light.hall", "on")
    await hass.async_block_till_done()
    await _tick(hass, 2)

    assert len(client.lines) == 4
    assert forwarder.stats.sent == 4
    assert forwarder.stats.dropped_not_numeric == 0
    values = [line.split("value=")[1].split(" ")[0] for line in client.lines]
    assert values == ["1.0", "0.0", "1.0", "1.0"]


async def test_an_unavailable_sensor_is_still_a_gap_and_not_a_loss(
    hass: HomeAssistant, entry, make_forwarder
) -> None:
    """The two reasons for storing nothing must not be counted as one.

    ``unavailable`` is Home Assistant saying it has nothing to report, and a
    gap in the chart is the honest record of that - counting it as a dropped
    reading would put a permanently climbing number in front of anybody whose
    sensor sleeps overnight.
    """
    client = StubClient()
    forwarder = make_forwarder(
        hass, entry, client, entities=["sensor.house_power"]
    )

    hass.states.async_set("sensor.house_power", "unavailable")
    hass.states.async_set("sensor.house_power", "unknown")
    await hass.async_block_till_done()
    await _tick(hass, 2)

    assert client.bodies == []
    assert forwarder.stats.dropped_not_numeric == 0

    # ...and a state that IS something is counted, from the same sensor.
    hass.states.async_set("sensor.house_power", "1500 W")
    await hass.async_block_till_done()
    await _tick(hass, 2)

    assert forwarder.stats.dropped_not_numeric == 1


async def test_a_bare_sensor_reporting_a_number_is_untouched(
    hass: HomeAssistant, entry, make_forwarder
) -> None:
    """The domain gate is about WORDS. A template sensor is why bare sensors
    are offered in the first place, and they mostly report numbers."""
    client = StubClient()
    forwarder = make_forwarder(hass, entry, client, entities=["sensor.tariff"])

    hass.states.async_set("sensor.tariff", "1.42")
    await hass.async_block_till_done()
    await _tick(hass, 2)

    assert forwarder.stats.sent == 1
    assert forwarder.stats.dropped_not_numeric == 0


async def test_a_shutdown_during_a_quota_pause_still_counts_the_backlog(
    hass: HomeAssistant, entry, make_forwarder
) -> None:
    """A daily-quota pause can hold hours of readings. They still go somewhere."""
    client = StubClient(
        WriteResult(
            WriteOutcome.DAILY_QUOTA,
            retry_after=3600,
            message="daily measurement quota exceeded",
        ),
        *[
            WriteResult(
                WriteOutcome.DAILY_QUOTA,
                retry_after=3600,
                message="daily measurement quota exceeded",
            )
            for _ in range(5)
        ],
    )
    forwarder = make_forwarder(hass, entry, client)

    hass.states.async_set("sensor.house_power", "1500")
    await hass.async_block_till_done()
    await _tick(hass, 2)
    assert forwarder.stats.queued == 1

    await forwarder.async_shutdown()

    assert forwarder.stats.dropped_shutdown == 1
    assert forwarder.stats.queued == 0
