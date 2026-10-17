"""``taghistorian-influx-migrate``: the command line around the plan and the copy."""

from __future__ import annotations

import argparse
import os
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import TextIO

from . import __version__
from .checkpoint import Checkpoint, CheckpointError
from .influx import InfluxError, InfluxSource, InfluxV1Source, InfluxV2Source
from .migrate import (
    EXIT_DONE,
    EXIT_ERROR,
    MigrationStopped,
    Options,
    Runner,
    build_plan,
    describe_plan,
)
from .taghistorian import DEFAULT_URL, MAX_POINTS_PER_REQUEST, TagHistorian, TagHistorianError
from .timeutil import now_ns, parse_time_ns

DEFAULT_STATE = "taghistorian-influx-migration.json"

DESCRIPTION = """\
Copy Home Assistant history out of InfluxDB 1.x or 2.x into Tag Historian,
named exactly as Home Assistant's live data is named there, with each
entity's friendly name and unit.

Start with --dry-run: it lists the tags the copy would create, how many
readings each holds, and how many days your plan's daily allowance makes
it take. Nothing is written until you run it without --dry-run.

Copy the history BEFORE you point Home Assistant's InfluxDB integration at
Tag Historian, then run once more with --catch-up after you switch. A tag
only receives readings newer than the newest it already holds; see the
README for why that order matters.
"""

EPILOG = """\
exit status: 0 finished (or a dry run), 3 paused until the next UTC day
because today's share of the allowance is used - run it again then - and
1 for an error, with the reason printed.

Keys and passwords can also come from the environment: TAGHISTORIAN_API_KEY,
INFLUX_TOKEN (2.x), INFLUX_USERNAME and INFLUX_PASSWORD (1.x). The resume
file never stores them.
"""


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="taghistorian-influx-migrate",
        description=DESCRIPTION,
        epilog=EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")

    influx = parser.add_argument_group("InfluxDB (the source)")
    influx.add_argument(
        "--influx-url",
        default=os.environ.get("INFLUX_URL"),
        help="for example http://homeassistant.local:8086 (or INFLUX_URL)",
    )
    influx.add_argument("--database", help="InfluxDB 1.x: the database Home Assistant writes to")
    influx.add_argument("--retention-policy", help="InfluxDB 1.x: only if not the default one")
    influx.add_argument(
        "--influx-username", default=os.environ.get("INFLUX_USERNAME"), help="InfluxDB 1.x"
    )
    influx.add_argument(
        "--influx-password", default=os.environ.get("INFLUX_PASSWORD"), help="InfluxDB 1.x"
    )
    influx.add_argument("--org", help="InfluxDB 2.x: the organization")
    influx.add_argument("--bucket", help="InfluxDB 2.x: the bucket Home Assistant writes to")
    influx.add_argument(
        "--influx-token",
        default=os.environ.get("INFLUX_TOKEN"),
        help="InfluxDB 2.x: a token that can read the bucket (or INFLUX_TOKEN)",
    )
    influx.add_argument(
        "--influx-insecure",
        action="store_true",
        help="do not verify InfluxDB's TLS certificate (a self-signed one on your own network)",
    )

    th = parser.add_argument_group("Tag Historian (the destination)")
    th.add_argument(
        "--api-key",
        default=os.environ.get("TAGHISTORIAN_API_KEY"),
        help="an API key whose Scope is Write (or TAGHISTORIAN_API_KEY)",
    )
    th.add_argument(
        "--url",
        default=os.environ.get("TAGHISTORIAN_URL", DEFAULT_URL),
        help=f"default {DEFAULT_URL}",
    )

    what = parser.add_argument_group("what to copy")
    what.add_argument(
        "--entity",
        action="append",
        default=[],
        metavar="PATTERN",
        help="only entities matching, e.g. sensor.*_temperature (repeatable; default: all)",
    )
    what.add_argument(
        "--exclude",
        action="append",
        default=[],
        metavar="PATTERN",
        help="leave these out (repeatable)",
    )
    what.add_argument(
        "--measurement",
        action="append",
        default=[],
        metavar="NAME",
        help="only these InfluxDB measurements, e.g. °C (repeatable; default: all)",
    )
    what.add_argument(
        "--start", help="copy from this date or time, UTC (default: the first reading)"
    )
    what.add_argument(
        "--end", help="copy up to this date or time, UTC (default: now, on the first run)"
    )
    what.add_argument(
        "--no-unit-from-measurement",
        action="store_true",
        help="never read the measurement name as the unit (see README: Units)",
    )
    what.add_argument(
        "--not-a-unit",
        action="append",
        default=[],
        metavar="NAME",
        help="a measurement name that is not a unit, e.g. your default_measurement (repeatable)",
    )

    how = parser.add_argument_group("how")
    how.add_argument("--dry-run", action="store_true", help="show the plan; write nothing")
    how.add_argument(
        "--state",
        default=DEFAULT_STATE,
        metavar="FILE",
        help=f"the resume file (default {DEFAULT_STATE} in the current directory)",
    )
    how.add_argument(
        "--leave-for-live",
        type=float,
        default=25.0,
        metavar="PERCENT",
        help="stop each UTC day with this much of the daily allowance left (default 25)",
    )
    how.add_argument(
        "--max-per-day", type=int, metavar="N", help="copy at most N readings per UTC day"
    )
    how.add_argument(
        "--wait",
        action="store_true",
        help="keep running across UTC midnights instead of stopping when today's share is used",
    )
    how.add_argument(
        "--catch-up",
        action="store_true",
        help="move the end of the range to now and copy what arrived in InfluxDB since",
    )
    how.add_argument(
        "--include-older",
        action="store_true",
        help="also copy readings older than what a tag already holds (see README first)",
    )
    how.add_argument(
        "--batch-size",
        type=int,
        default=MAX_POINTS_PER_REQUEST,
        metavar="N",
        help=f"readings per request, at most {MAX_POINTS_PER_REQUEST}",
    )
    return parser


def _source(args: argparse.Namespace) -> InfluxSource:
    if not args.influx_url:
        raise SystemExit("--influx-url is required (or set INFLUX_URL)")
    if args.database and args.bucket:
        raise SystemExit("give --database (InfluxDB 1.x) or --bucket (InfluxDB 2.x), not both")
    if args.database:
        return InfluxV1Source(
            args.influx_url,
            args.database,
            username=args.influx_username,
            password=args.influx_password,
            retention_policy=args.retention_policy,
            verify_tls=not args.influx_insecure,
        )
    if args.bucket:
        if not args.org:
            raise SystemExit("InfluxDB 2.x needs --org as well as --bucket")
        if not args.influx_token:
            raise SystemExit("InfluxDB 2.x needs --influx-token (or INFLUX_TOKEN)")
        return InfluxV2Source(
            args.influx_url,
            args.org,
            args.bucket,
            args.influx_token,
            verify_tls=not args.influx_insecure,
        )
    raise SystemExit(
        "say which InfluxDB: --database NAME for 1.x (Home Assistant's InfluxDB app is 1.x), "
        "or --org and --bucket for 2.x"
    )


def _options(args: argparse.Namespace) -> Options:
    if not 0 <= args.leave_for_live < 100:
        raise SystemExit("--leave-for-live is a percentage from 0 to below 100")
    if not 1 <= args.batch_size <= MAX_POINTS_PER_REQUEST:
        raise SystemExit(f"--batch-size is from 1 to {MAX_POINTS_PER_REQUEST}")
    if args.max_per_day is not None and args.max_per_day < 1:
        raise SystemExit("--max-per-day must be at least 1")
    return Options(
        include=tuple(args.entity),
        exclude=tuple(args.exclude),
        measurements=tuple(args.measurement),
        unit_from_measurement=not args.no_unit_from_measurement,
        not_units=frozenset(args.not_a_unit),
        include_older=args.include_older,
        leave_for_live=args.leave_for_live / 100.0,
        max_per_day=args.max_per_day,
        batch_size=args.batch_size,
        wait=args.wait,
    )


def main(
    argv: Sequence[str] | None = None,
    *,
    out: TextIO = sys.stdout,
    source_factory=_source,
    destination_factory=None,
    runner_factory=Runner,
    clock=now_ns,
) -> int:
    args = _parser().parse_args(argv)
    options = _options(args)
    source = source_factory(args)
    destination: TagHistorian | None = None
    if args.api_key:
        destination = (destination_factory or TagHistorian)(args.api_key, args.url)
    elif not args.dry_run:
        raise SystemExit(
            "--api-key is required to copy (or set TAGHISTORIAN_API_KEY); a --dry-run works "
            "without one"
        )

    state_path = Path(args.state)
    try:
        checkpoint = Checkpoint.load(state_path)
        destination_url = destination.url if destination else args.url.rstrip("/")
        if checkpoint is not None:
            checkpoint.check_same_copy(source.identity(), destination_url, options.frozen())
            if args.start and parse_time_ns(args.start) != checkpoint.start_ns:
                raise SystemExit(
                    f"{state_path} copies a range that starts elsewhere; drop --start to resume it, "
                    "or start a new copy with another --state file"
                )
            if args.end and not args.catch_up and parse_time_ns(args.end) != checkpoint.end_ns:
                raise SystemExit(
                    f"{state_path} copies a range that ends elsewhere; use --catch-up to move the "
                    "end to now, or start a new copy with another --state file"
                )
            start_ns, end_ns = checkpoint.start_ns, checkpoint.end_ns
            if args.catch_up:
                end_ns = parse_time_ns(args.end) if args.end else clock()
            print(f"Resuming the copy in {state_path}.", file=out)
        else:
            start_ns = parse_time_ns(args.start) if args.start else 0
            end_ns = parse_time_ns(args.end) if args.end else clock()
        if end_ns <= start_ns:
            raise SystemExit("the end of the range has to be after its start")

        plan = build_plan(source, destination, start_ns, end_ns, options, checkpoint, clock)
        describe_plan(plan, options, out, source_text=source.describe())

        if args.dry_run:
            print("\nDry run: nothing was written.", file=out)
            return EXIT_DONE
        if not plan.tags:
            return EXIT_DONE
        assert destination is not None

        if checkpoint is None:
            checkpoint = Checkpoint(
                path=state_path,
                source=source.identity(),
                destination=destination.url,
                start_ns=start_ns,
                end_ns=end_ns,
                options=options.frozen(),
            )
        elif args.catch_up:
            checkpoint.end_ns = end_ns
            for state in checkpoint.tags.values():
                if state.status == "done":
                    state.status = "pending"
        checkpoint.save()

        print("", file=out)
        runner = runner_factory(source, destination, checkpoint, options, out=out, clock=clock)
        return runner.run(plan)
    except (MigrationStopped, CheckpointError, InfluxError, TagHistorianError) as exc:
        print(f"\nStopped: {exc}", file=out)
        return EXIT_ERROR
    except ValueError as exc:
        print(f"\nStopped: {exc}", file=out)
        return EXIT_ERROR
    finally:
        source.close()
        if destination is not None:
            destination.close()


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
