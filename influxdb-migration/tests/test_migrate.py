"""The copy, end to end: the CLI, a fake InfluxDB and a fake Tag Historian.

Each test is one thing a person moving off InfluxDB depends on, and each
asserts on what ended up in Tag Historian - names, values, units, friendly
names, counts - rather than on what the code believes it sent.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from taghistorian_influx_migrate import migrate
from taghistorian_influx_migrate.checkpoint import StateLock
from taghistorian_influx_migrate.cli import DESCRIPTION, main
from taghistorian_influx_migrate.migrate import EXIT_DONE, EXIT_ERROR, EXIT_PAUSED, Runner
from taghistorian_influx_migrate.taghistorian import TagHistorian
from taghistorian_influx_migrate.timeutil import NS_PER_SECOND, parse_time_ns

from .conftest import MINUTE, T0
from .fakes import FakeInflux, FakeTagHistorian, Point

LIVING_ROOM_READINGS = 3 * 24 * 6
ALL_READINGS = LIVING_ROOM_READINGS + 40 + 30 + 48 + 12
END = "2026-10-15T00:00:00Z"


class Harness:
    """Runs the command the way a person would, with time under the test's control."""

    def __init__(self, influx, tag_historian, clock, out, tmp_path: Path, *, version: int = 1):
        self.influx = influx
        self.th = tag_historian
        self.clock = clock
        self.out = out
        self.state = tmp_path / "state.json"
        self.version = version
        self.sleeps: list[float] = []

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.clock.advance(seconds)

    def argv(self, *extra: str) -> list[str]:
        if self.version == 1:
            source = [
                "--influx-url",
                self.influx.url,
                "--database",
                "homeassistant",
                "--influx-username",
                "ha",
                "--influx-password",
                "secret",
            ]
        else:
            source = [
                "--influx-url",
                self.influx.url,
                "--org",
                "home",
                "--bucket",
                "homeassistant",
                "--influx-token",
                "influx-token",
            ]
        return [
            *source,
            "--api-key",
            FakeTagHistorian.API_KEY,
            "--url",
            self.th.url,
            "--state",
            str(self.state),
            *extra,
        ]

    def run(self, *extra: str, settle_delay: float = 90.0) -> int:
        def runner(source, destination, checkpoint, options, *, out, clock):
            options.settle_delay = settle_delay
            return Runner(
                source, destination, checkpoint, options, out=out, sleep=self.sleep, clock=clock
            )

        return main(self.argv(*extra), out=self.out, runner_factory=runner, clock=self.clock)

    def saved(self) -> dict:
        return json.loads(self.state.read_text(encoding="utf-8"))


@pytest.fixture
def harness(influx, tag_historian, clock, out, tmp_path):
    return Harness(influx, tag_historian, clock, out, tmp_path)


# ---------------------------------------------------------------------------


def test_a_dry_run_shows_the_plan_and_writes_nothing(harness):
    assert harness.run("--dry-run", "--end", END) == EXIT_DONE
    text = harness.out.getvalue()

    assert "sensor.living_room_temperature" in text
    assert "Living room temperature" in text
    assert "sensor.outdoor_temperature.instance-cabin" in text
    # Six InfluxDB series, five tags: the energy meter's unit changed, which
    # is two series in InfluxDB and one entity - one tag - in Home Assistant.
    assert "5 tags, " + f"{ALL_READINGS:,}" + " readings" in text
    assert "5 of them are new tags. Your plan has room for 50 more" in text
    assert "Wh, kWh" in text
    assert "its unit changed in InfluxDB, and the tag keeps the newest" in text
    assert "Range: the first reading to 2026-10-15 00:00 UTC" in text
    assert "climate.living_room" not in text
    assert "cpu" not in text.split("Range:")[1]
    assert "Dry run: nothing was written." in text
    assert harness.th.write_requests == []
    assert not harness.state.exists()


def test_a_dry_run_works_without_a_tag_historian_key(influx, out, tmp_path):
    argv = [
        "--influx-url",
        influx.url,
        "--database",
        "homeassistant",
        "--influx-username",
        "ha",
        "--influx-password",
        "secret",
        "--state",
        str(tmp_path / "s.json"),
        "--dry-run",
        "--end",
        END,
    ]
    assert main(argv, out=out) == EXIT_DONE
    assert "Give --api-key" in out.getvalue()


def test_the_copy_lands_under_the_names_live_forwarding_uses(harness):
    assert harness.run("--end", END) == EXIT_DONE
    th = harness.th

    assert sorted(th.tags) == [
        "binary_sensor.front_door",
        "sensor.energy_total",
        "sensor.living_room_temperature",
        "sensor.outdoor_temperature.instance-cabin",
        "sensor.water_used",
    ]
    living = th.values("sensor.living_room_temperature")
    assert len(living) == LIVING_ROOM_READINGS
    assert living[0] == (T0 + 123, 20.0)
    assert sum(len(t.readings) for t in th.tags.values()) == ALL_READINGS


def test_friendly_names_and_units_arrive_with_the_history(harness):
    harness.run("--end", END)
    tags = harness.th.tags

    assert tags["sensor.living_room_temperature"].description == "Living room temperature"
    assert tags["sensor.living_room_temperature"].units == "°C"
    # No unit in Home Assistant: the measurement is the entity id, not a unit.
    assert tags["binary_sensor.front_door"].units is None
    assert tags["binary_sensor.front_door"].description == "Front door"
    # measurement_attr: entity_id - the unit came as a field.
    assert tags["sensor.water_used"].units == "L"
    # A unit that changed: the newest one, as live forwarding would leave it.
    assert tags["sensor.energy_total"].units == "kWh"


def test_the_measurement_is_not_read_as_a_unit_when_told_not_to(harness):
    harness.run("--end", END, "--no-unit-from-measurement")
    assert harness.th.tags["sensor.living_room_temperature"].units is None
    assert harness.th.tags["sensor.water_used"].units == "L"


def test_running_it_again_writes_nothing_twice(harness):
    assert harness.run("--end", END) == EXIT_DONE
    requests = len(harness.th.write_requests)
    assert harness.run("--end", END) == EXIT_DONE
    assert len(harness.th.write_requests) == requests
    assert sum(len(t.readings) for t in harness.th.tags.values()) == ALL_READINGS


def test_a_fresh_resume_file_still_never_writes_a_reading_twice(harness):
    """Somebody deleted the resume file: what Tag Historian holds is the floor."""
    harness.run("--end", END)
    harness.state.unlink()
    assert harness.run("--end", END) == EXIT_DONE
    assert sum(len(t.readings) for t in harness.th.tags.values()) == ALL_READINGS


def test_only_the_selected_entities_are_copied(harness):
    harness.run("--end", END, "--entity", "sensor.*_temperature", "--exclude", "sensor.outdoor*")
    assert sorted(harness.th.tags) == ["sensor.living_room_temperature"]


def test_batches_follow_the_size_and_never_split_one_instant(harness):
    harness.run("--end", END, "--entity", "sensor.living_room_temperature", "--batch-size", "100")
    sizes = [len(r) for r in harness.th.write_requests]
    assert sizes == [100, 100, 100, 100, 32]


def test_the_daily_share_pauses_the_copy_and_the_next_day_resumes_it(influx, clock, out, tmp_path):
    th = FakeTagHistorian(clock=clock, readings_per_day=400)
    try:
        h = Harness(influx, th, clock, out, tmp_path)
        # Live forwarding has already sent 100 today.
        th.usage[th.today()] = 100

        assert h.run("--end", END, "--entity", "sensor.living_room_temperature") == EXIT_PAUSED
        # 400 a day less the 25% kept for live = 300, of which live used 100.
        assert th.usage[th.today()] == 300
        assert "Run the same command again after 00:00 UTC" in out.getvalue()
        assert h.saved()["days"] == {"2026-10-17": 200}

        clock.advance(13 * 3600)  # the next UTC day
        assert h.run("--end", END, "--entity", "sensor.living_room_temperature") == EXIT_DONE
        assert len(th.values("sensor.living_room_temperature")) == LIVING_ROOM_READINGS
    finally:
        th.close()


def test_with_wait_it_sleeps_to_midnight_and_carries_on(influx, clock, out, tmp_path):
    th = FakeTagHistorian(clock=clock, readings_per_day=400)
    try:
        h = Harness(influx, th, clock, out, tmp_path)
        assert (
            h.run("--end", END, "--entity", "sensor.living_room_temperature", "--wait") == EXIT_DONE
        )
        assert len(th.values("sensor.living_room_temperature")) == LIVING_ROOM_READINGS
        # Noon to midnight, plus the minute's margin.
        assert h.sleeps[0] == 12 * 3600 + 60
    finally:
        th.close()


def test_a_per_day_cap_of_its_own(harness):
    assert harness.run("--end", END, "--max-per-day", "150") == EXIT_PAUSED
    assert sum(len(t.readings) for t in harness.th.tags.values()) == 150


def test_a_429_with_retry_after_is_waited_out_and_nothing_doubles(harness):
    harness.th.script = [
        (
            "status",
            429,
            7,
            {"error": "Rate limit exceeded. Please try again later.", "retryAfterSeconds": "7"},
        ),
        ("status", 503, 3, {"code": "unavailable", "message": "retry after 3s"}),
    ]
    assert harness.run("--end", END, "--entity", "sensor.water_used") == EXIT_DONE
    assert harness.sleeps == [7, 3]
    assert len(harness.th.values("sensor.water_used")) == 12


def test_the_servers_daily_quota_answer_pauses_rather_than_loses(harness):
    harness.th.script = [
        (
            "status",
            429,
            43200,
            {"code": "too many requests", "message": "daily measurement quota exceeded"},
        ),
    ]
    assert harness.run("--end", END, "--entity", "sensor.water_used") == EXIT_PAUSED
    assert harness.th.tags == {}
    harness.clock.advance(13 * 3600)
    assert harness.run("--end", END, "--entity", "sensor.water_used") == EXIT_DONE
    assert len(harness.th.values("sensor.water_used")) == 12


def test_a_batch_that_arrived_without_an_answer_is_not_sent_again(harness):
    harness.th.script = ["drop-after-store"]
    assert harness.run("--end", END, "--entity", "sensor.water_used") == EXIT_DONE
    assert len(harness.th.values("sensor.water_used")) == 12
    assert len(harness.th.write_requests) == 1
    assert harness.sleeps == [90.0], "it waits out a deploy's overlap before asking"
    assert "it had arrived" in harness.out.getvalue()


def test_a_batch_that_never_arrived_is_sent_again(harness):
    harness.th.script = ["drop-before-store"]
    assert harness.run("--end", END, "--entity", "sensor.water_used") == EXIT_DONE
    assert len(harness.th.values("sensor.water_used")) == 12
    assert len(harness.th.write_requests) == 2


def test_an_old_batch_that_arrived_and_left_the_buffer_is_not_sent_again(
    influx, clock, out, tmp_path
):
    """History older than the full-resolution period reads back only from the write buffer.

    The water meter's readings are sixteen days old, past a fourteen-day
    full-resolution period, and the buffer is flushed within the 90 seconds
    the copy waits before asking. Reading the window back then finds nothing
    although the batch was stored; the tag's newest reading still says so.
    """
    th = FakeTagHistorian(clock=clock, full_resolution_days=14, flush_after_s=60)
    try:
        h = Harness(influx, th, clock, out, tmp_path)
        th.script = ["drop-after-store"]
        assert h.run("--end", END, "--entity", "sensor.water_used") == EXIT_DONE
        assert len(th.values("sensor.water_used")) == 12
        assert len(th.write_requests) == 1
        assert "it had arrived" in out.getvalue()

        # The read-back alone would have said "not there" and sent it again.
        first, last = th.values("sensor.water_used")[0][0], th.values("sensor.water_used")[-1][0]
        reader = TagHistorian(FakeTagHistorian.API_KEY, th.url)
        try:
            assert not reader.stored_between("sensor.water_used", first, last)
        finally:
            reader.close()
    finally:
        th.close()


def test_an_old_batch_that_never_arrived_is_sent_again(influx, clock, out, tmp_path):
    th = FakeTagHistorian(clock=clock, full_resolution_days=14, flush_after_s=60)
    try:
        h = Harness(influx, th, clock, out, tmp_path)
        th.script = ["drop-before-store"]
        assert h.run("--end", END, "--entity", "sensor.water_used") == EXIT_DONE
        assert len(th.values("sensor.water_used")) == 12
        assert len(th.write_requests) == 2
    finally:
        th.close()


def test_with_include_older_live_readings_are_not_taken_for_the_batch(harness):
    """The tag's newest reading is live data's there, and says nothing about the batch."""
    harness.th.add_live("sensor.water_used", [(parse_time_ns(END), 99.0)])
    harness.th.script = ["drop-before-store"]
    argv = ("--end", END, "--entity", "sensor.water_used", "--include-older")
    assert harness.run(*argv) == EXIT_DONE
    assert len(harness.th.values("sensor.water_used")) == 1 + 12
    assert len(harness.th.write_requests) == 2


def test_an_unanswered_batch_left_by_a_crash_is_settled_first(harness):
    """The resume file says a batch went out and no answer came back."""
    harness.run("--end", END, "--entity", "sensor.water_used", "--dry-run")
    harness.th.script = ["drop-after-store"]
    # The first run's settle check cannot reach the server: simulate a run
    # killed right after the unanswered write by stopping it in the sleep.

    class Killed(Exception):
        pass

    def killed_sleep(seconds):
        raise Killed

    def runner(source, destination, checkpoint, options, *, out, clock):
        return Runner(
            source, destination, checkpoint, options, out=out, sleep=killed_sleep, clock=clock
        )

    with pytest.raises(Killed):
        main(
            harness.argv("--end", END, "--entity", "sensor.water_used"),
            out=harness.out,
            runner_factory=runner,
            clock=harness.clock,
        )
    assert harness.saved()["tags"]["sensor.water_used"]["unconfirmed"]["readings"] == 12

    assert harness.run("--end", END, "--entity", "sensor.water_used") == EXIT_DONE
    assert len(harness.th.values("sensor.water_used")) == 12
    assert len(harness.th.write_requests) == 1


def test_a_tag_the_plan_has_no_room_for_is_refused_and_the_rest_continue(
    influx, clock, out, tmp_path
):
    th = FakeTagHistorian(clock=clock, tag_limit=2)
    try:
        h = Harness(influx, th, clock, out, tmp_path)
        # The plan sees room for two; live forwarding takes one meanwhile.
        th.script = [lambda: th.add_live("sensor.something_else", [(1, 1.0)])]
        argv = (
            "--end",
            END,
            "--entity",
            "sensor.water_used",
            "--entity",
            "binary_sensor.front_door",
        )
        assert h.run(*argv) == EXIT_DONE
        state = h.saved()["tags"]
        assert state["binary_sensor.front_door"]["status"] == "done"
        assert state["sensor.water_used"]["status"] == "refused"
        assert "tag quota exceeded" in state["sensor.water_used"]["reason"]

        # Still no room: the next run leaves it alone rather than refusing to start.
        assert h.run(*argv) == EXIT_DONE
        assert h.saved()["tags"]["sensor.water_used"]["status"] == "refused"

        # Room made - an upgrade - and the next run copies it.
        th.tag_limit = 3
        assert h.run(*argv) == EXIT_DONE
        assert h.saved()["tags"]["sensor.water_used"]["status"] == "done"
        assert len(th.values("sensor.water_used")) == 12
    finally:
        th.close()


def test_a_selection_bigger_than_the_plan_does_not_start(influx, clock, out, tmp_path):
    th = FakeTagHistorian(clock=clock, tag_limit=3)
    try:
        h = Harness(influx, th, clock, out, tmp_path)
        assert h.run("--end", END) == EXIT_ERROR
        assert "needs 5 new tags and your plan has room for 3" in out.getvalue()
        assert th.write_requests == []
    finally:
        th.close()


def test_a_tag_live_forwarding_already_writes_only_gets_newer_readings(harness):
    """History older than what a tag holds would be compared with its newest value."""
    cutoff = T0 + 2 * 24 * 60 * MINUTE
    harness.th.add_live(
        "sensor.living_room_temperature", [(cutoff, 30.0)], "Living room temperature", "°C"
    )

    harness.run("--dry-run", "--end", END, "--entity", "sensor.living_room_temperature")
    assert (
        "already holds readings up to 2026-10-03 00:00 UTC; only newer ones are copied"
        in harness.out.getvalue()
    )

    assert harness.run("--end", END, "--entity", "sensor.living_room_temperature") == EXIT_DONE
    copied = [t for t, _ in harness.th.values("sensor.living_room_temperature")]
    assert min(copied) == cutoff
    assert all(t > cutoff for t in copied[1:])
    assert len(copied) == 1 + 24 * 6


def test_include_older_copies_behind_live_data_when_asked(harness):
    cutoff = T0 + 2 * 24 * 60 * MINUTE
    harness.th.add_live("sensor.living_room_temperature", [(cutoff, 30.0)])
    harness.run("--end", END, "--entity", "sensor.living_room_temperature", "--include-older")
    assert len(harness.th.values("sensor.living_room_temperature")) == 1 + LIVING_ROOM_READINGS


def test_catch_up_moves_the_end_and_copies_what_arrived_since(harness):
    first_end = "2026-10-02T00:00:00Z"
    assert (
        harness.run("--end", first_end, "--entity", "sensor.living_room_temperature") == EXIT_DONE
    )
    assert len(harness.th.values("sensor.living_room_temperature")) == 24 * 6

    assert harness.run("--entity", "sensor.living_room_temperature") == EXIT_DONE
    assert len(harness.th.values("sensor.living_room_temperature")) == 24 * 6, (
        "without --catch-up a resume keeps the range it started with"
    )

    assert harness.run("--entity", "sensor.living_room_temperature", "--catch-up") == EXIT_DONE
    assert len(harness.th.values("sensor.living_room_temperature")) == LIVING_ROOM_READINGS


def test_help_says_catch_up_first_and_switch_after():
    """--help used to say "run once more with --catch-up after you switch"."""
    assert "after you switch" not in DESCRIPTION
    catch_up = DESCRIPTION.index("--catch-up")
    assert DESCRIPTION.index("then point Home Assistant at Tag Historian") > catch_up


def test_a_catch_up_after_the_switch_names_what_it_cannot_copy(harness):
    """Switched first, then --catch-up: InfluxDB's readings in between are not copied, and it says so."""
    entity = ("--entity", "sensor.living_room_temperature")
    assert harness.run("--end", "2026-10-02T00:00:00Z", *entity) == EXIT_DONE
    # Home Assistant was pointed at Tag Historian on the 4th: live readings
    # from then on, and InfluxDB's history stops there.
    switch = T0 + 3 * 24 * 60 * MINUTE
    harness.th.add_live(
        "sensor.living_room_temperature", [(switch + i * 10 * MINUTE, 25.0) for i in range(6)]
    )

    assert harness.run(*entity, "--catch-up") == EXIT_DONE
    text = harness.out.getvalue()
    assert "already holds newer readings, up to 2026-10-04 00:50 UTC" in text
    assert "InfluxDB's readings after 2026-10-01 23:50 UTC are NOT copied" in text
    assert (
        "not copied, sensor.living_room_temperature: InfluxDB's readings after "
        "2026-10-01 23:50 UTC" in text
    )
    assert len(harness.th.values("sensor.living_room_temperature")) == 24 * 6 + 6

    # The way it names: --include-older brings the two days in between.
    assert harness.run(*entity, "--catch-up", "--include-older") == EXIT_DONE
    assert len(harness.th.values("sensor.living_room_temperature")) == 24 * 6 + 2 * 24 * 6 + 6


def test_a_busy_sensor_after_a_long_silence_is_read_in_windows_that_fit(
    clock, out, tmp_path, monkeypatch
):
    """Two months of nothing, then a reading a minute: no read may take the lot at once."""
    monkeypatch.setattr(migrate, "TARGET_POINTS_PER_READ", 200)
    tags = {"domain": "sensor", "entity_id": "heat_pump_power"}
    start = T0 - 70 * 24 * 60 * MINUTE
    back = T0 - 10 * 24 * 60 * MINUTE
    points = [Point("W", tags, {"value": 1.0, "friendly_name_str": "Heat pump"}, start)]
    points += [
        Point(
            "W", tags, {"value": float(i % 50), "friendly_name_str": "Heat pump"}, back + i * MINUTE
        )
        for i in range(2 * 24 * 60)
    ]
    influx = FakeInflux(points)
    th = FakeTagHistorian(clock=clock)
    try:
        h = Harness(influx, th, clock, out, tmp_path)
        assert h.run("--end", END) == EXIT_DONE
        assert len(th.values("sensor.heat_pump_power")) == 1 + 2 * 24 * 60
        assert max(influx.rows_read) <= 200, influx.rows_read
    finally:
        influx.close()
        th.close()


def test_a_unit_tag_historian_lost_after_the_copy_is_named_by_the_next_dry_run(harness):
    entity = ("--entity", "sensor.living_room_temperature")
    assert harness.run("--end", END, *entity) == EXIT_DONE
    assert "--dry-run in ten minutes" in harness.out.getvalue()
    assert harness.saved()["tags"]["sensor.living_room_temperature"]["unit"] == "°C"

    harness.run("--dry-run", "--end", END, *entity)
    assert "no longer has the unit" not in harness.out.getvalue()

    # Tag Historian restarted before it saved the unit; live data from the
    # built-in integration will never send one.
    harness.th.tags["sensor.living_room_temperature"].units = None
    harness.run("--dry-run", "--end", END, *entity)
    assert "Tag Historian no longer has the unit °C this copy gave it" in harness.out.getvalue()


def test_two_runs_cannot_use_one_resume_file_at_once(harness):
    argv = ("--end", END, "--entity", "sensor.water_used")
    with StateLock(harness.state):
        assert harness.run(*argv) == EXIT_ERROR
        assert "Another run of the copy is using" in harness.out.getvalue()
        assert harness.th.write_requests == []
        # A dry run only reads the file, and may look while a copy runs.
        assert harness.run("--dry-run", *argv) == EXIT_DONE
    assert harness.run(*argv) == EXIT_DONE
    assert len(harness.th.values("sensor.water_used")) == 12


def test_a_resume_file_from_another_account_is_refused(harness):
    argv = ("--end", END, "--entity", "sensor.water_used")
    assert harness.run(*argv) == EXIT_DONE
    assert harness.saved()["account"] == FakeTagHistorian.CUSTOMER_ID

    # Same address, a key for another account.
    harness.th.customer_id = "99999999-8888-7777-6666-555555555555"
    assert harness.run(*argv) == EXIT_ERROR
    assert "another Tag Historian account" in harness.out.getvalue()


def test_a_resume_file_from_another_copy_is_refused(harness):
    harness.run("--end", END, "--entity", "sensor.water_used")
    assert harness.run("--end", END, "--entity", "sensor.*") == EXIT_ERROR
    assert "different options" in harness.out.getvalue()


def test_a_full_account_stops_the_copy_and_says_why(harness):
    harness.th.script = [("status", 402, None, {"error": "Storage quota exceeded"})]
    assert harness.run("--end", END, "--entity", "sensor.water_used") == EXIT_ERROR
    assert "storage is full" in harness.out.getvalue()


def test_a_refused_key_stops_the_copy_and_names_the_scope(harness):
    harness.th.script = [("status", 403, None, None)]
    assert harness.run("--end", END, "--entity", "sensor.water_used") == EXIT_ERROR
    assert "Scope is Write" in harness.out.getvalue()


def test_a_small_influxdb_row_limit_is_worked_around_by_smaller_windows(
    history, clock, out, tmp_path
):
    from .fakes import FakeInflux

    influx = FakeInflux(history, username="ha", password="secret", max_row_limit=50)
    th = FakeTagHistorian(clock=clock)
    try:
        h = Harness(influx, th, clock, out, tmp_path)
        assert h.run("--end", END, "--entity", "sensor.living_room_temperature") == EXIT_DONE
        assert len(th.values("sensor.living_room_temperature")) == LIVING_ROOM_READINGS
    finally:
        influx.close()
        th.close()


def test_influxdb_2_end_to_end(influx2, tag_historian, clock, out, tmp_path):
    h = Harness(influx2, tag_historian, clock, out, tmp_path, version=2)
    assert h.run("--end", END) == EXIT_DONE
    assert sum(len(t.readings) for t in tag_historian.tags.values()) == ALL_READINGS
    assert tag_historian.tags["sensor.living_room_temperature"].units == "°C"
    assert tag_historian.tags["sensor.water_used"].units == "L"


def test_progress_is_said_in_readings_and_dates(harness):
    harness.run("--end", END, "--entity", "sensor.water_used")
    text = harness.out.getvalue()
    assert "[1/1] sensor.water_used" in text
    assert "12 readings, through 2026-10-01 22:00 UTC" in text
    assert "Finished: 12 readings copied on this run." in text


def test_the_resume_file_holds_no_secret(harness):
    harness.run("--end", END, "--entity", "sensor.water_used")
    text = harness.state.read_text(encoding="utf-8")
    assert FakeTagHistorian.API_KEY not in text
    assert "secret" not in text


def test_the_range_on_the_first_run_is_kept_for_every_resume(harness):
    harness.run("--start", "2026-10-02", "--end", END, "--entity", "sensor.living_room_temperature")
    saved = harness.saved()
    assert saved["start_ns"] == parse_time_ns("2026-10-02")
    assert saved["end_ns"] == parse_time_ns(END)
    assert len(harness.th.values("sensor.living_room_temperature")) == 2 * 24 * 6
    with pytest.raises(SystemExit, match="starts elsewhere"):
        harness.run("--start", "2026-10-01", "--entity", "sensor.living_room_temperature")


def test_nanoseconds_survive_the_trip(harness):
    harness.run("--end", END, "--entity", "sensor.water_used")
    times = [t for t, _ in harness.th.values("sensor.water_used")]
    assert times[0] == T0 + 123
    assert times[1] - times[0] == 120 * 60 * NS_PER_SECOND
