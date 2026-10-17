# Copy Home Assistant history out of InfluxDB into Tag Historian

`taghistorian-influx-migrate` reads the history Home Assistant's InfluxDB
integration wrote — from InfluxDB 1.x, including Home Assistant's own InfluxDB
app, or from InfluxDB 2.x — and writes it into
[Tag Historian](https://taghistorian.com), under exactly the tag names Home
Assistant's live data gets there, with each entity's friendly name and unit.

It is the missing half of leaving InfluxDB. Pointing Home Assistant's InfluxDB
integration at Tag Historian moves your live data in one dialog
([taghistorian.com/docs/home-assistant](https://taghistorian.com/docs/home-assistant)).
This brings the years you already have along with it, so the next database
migration — the end-of-life InfluxDB 1.x app, the next major version — is one
you skip. The whole move, with the order that matters, is at
[taghistorian.com/docs/migrate-from-influxdb](https://taghistorian.com/docs/migrate-from-influxdb).

## What it copies, and what it does not

**Copied:** every Home Assistant entity whose state is a number — the series
Home Assistant writes with a `domain` and an `entity_id` tag and a numeric
`value` field. On/off entities count: Home Assistant writes `on` and `off` as 1
and 0. Each entity becomes one tag, named `{domain}.{entity_id}` — the name
Tag Historian gives the same entity when Home Assistant writes to it live, extra
tags from a `tags:` block in your InfluxDB configuration included — with its
friendly name as the tag's description and its unit as the tag's unit. An entity
whose unit changed (W to kW) is two series in InfluxDB and one tag here, as it is
one entity in Home Assistant.

**Not copied:**

- **Attributes.** Only the state, as live forwarding stores it beside a numeric
  state.
- **Entities whose state is a word** — a `climate` entity at `heat`. Live, Tag
  Historian turns each numeric attribute of such an entity into a tag of its
  own; the copy leaves those out rather than spending tags you did not choose.
  A template sensor in Home Assistant is the way to keep one of them.
- **Anything that is not Home Assistant's** — Telegraf's series in the same
  database, say. Telegraf can write to Tag Historian directly from now on.
- **InfluxDB 3.** Not supported: it has no Flux, and its InfluxQL compatibility
  has never been tried with this tool.

## Install

Python 3.9 or newer, on any machine that can reach both your InfluxDB and the
internet. Not on PyPI; install it from this repository:

```bash
pip install "git+https://github.com/softi-dev/tag-historian-clients.git#subdirectory=influxdb-migration"
```

## Before you start

You need:

- **A Tag Historian API key whose Scope is Write.** In Tag Historian Explorer,
  Settings → API keys → **Create New Key**, and leave **Scope** on **Write**,
  where the dialog opens. A Write key sends readings and creates a tag the first
  time it writes to a new name, against your plan's tag allowance. It cannot
  issue or revoke API keys, delete a tag, change your plan or reach your team;
  those endpoints refuse it with a `403`. Pass it as `--api-key` or in
  `TAGHISTORIAN_API_KEY`.
- **InfluxDB's address and credentials.** The address is in Home Assistant under
  **Settings → Devices & services → InfluxDB → ⋮ → Reconfigure**, along with
  the database (1.x) or organization and bucket (2.x). For the InfluxDB app
  running inside Home Assistant that is usually `http://homeassistant.local:8086`.
  - 1.x: `--database`, and `--influx-username`/`--influx-password` (or
    `INFLUX_USERNAME`/`INFLUX_PASSWORD`) if the database has authentication on.
    A user that can read the database is enough.
  - 2.x: `--org`, `--bucket` and `--influx-token` (or `INFLUX_TOKEN`): a token
    that can read the bucket is enough.

## Step by step

**1. Look before you write.** A dry run lists every tag the copy would write,
how many readings each holds, its unit and friendly name, how many of the
tags are new against how many your plan has room for, and how many days your
plan's daily allowance makes the copy take. It writes nothing.

```bash
taghistorian-influx-migrate --dry-run \
  --influx-url http://homeassistant.local:8086 --database home_assistant \
  --influx-username homeassistant --influx-password '...' \
  --entity 'sensor.*_temperature' --entity 'sensor.*_energy*'
```

Every tag costs one from your plan, so choose with `--entity` (shell-style
patterns on the entity id, repeatable) and `--exclude`. Without `--entity` the
copy takes every entity it finds.

**2. Copy the history — while Home Assistant still writes to InfluxDB.** The
same command without `--dry-run`. It shows progress per tag and writes where
it got to into a resume file (`taghistorian-influx-migration.json`, in the
current directory) after every batch.

The copy uses your plan's **daily readings allowance**, the same one live data
uses, and stops each UTC day with a quarter of it left (`--leave-for-live`),
because Home Assistant drops a reading the allowance refuses rather than
retrying it. A big history therefore takes days: when today's share is used,
the command says so and exits with status 3; run it again after midnight UTC —
by hand, from cron, or leave it running with `--wait` — and it carries on.
Readings older than your plan keeps at full resolution are kept as hourly
summaries, like your own data once it ages; how long each is kept is on
[taghistorian.com/docs/retention](https://taghistorian.com/docs/retention).

**3. Catch up, then switch.** When the copy has finished, run it once more
with `--catch-up`: it moves the end of the range to now and copies what
Home Assistant wrote to InfluxDB in the meantime. Then, straight away, point
Home Assistant's InfluxDB integration at Tag Historian (the Home Assistant guide
above, "Already sending to an InfluxDB of your own?").

**4. Import the Grafana dashboard.** [`../grafana/home-assistant.json`](../grafana/)
is the dashboard people build on InfluxDB — an entity dropdown, a chart per
entity in its tag's unit and under its friendly name — pointed at Tag Historian.
Every entity the copy brought across has its unit; one you add to Home Assistant
later needs it set once on the tag's page in Explorer (see "Units" below).

## Why the order matters

Tag Historian compares every reading with the newest one the tag already holds,
and drops one that repeats it — to within the tag's deadband, 0.1% by default,
and unless an hour has passed since the last one it kept. That is how live data
is stored, and it is right for live data. For a history copied into a tag that
live forwarding has already been writing, it means every old reading is compared
with *last week's* value, no hour ever passes, and the old readings that happen
to equal it are dropped — for an on/off sensor, every reading in the state it is
in now.

So **a tag only receives readings newer than the newest it already holds.**
Copy first, switch after, and nothing is lost but the minutes between the
catch-up and the switch. If you switched first, the dry run says which tags
already hold newer readings; `--include-older` copies everything anyway, with
exactly that cost. It is safest for sensors that keep changing, and worst for
on/off ones. It also takes the resume file's word for what was already copied,
so keep using the same one. If you switched in the middle of the copy, or ran
the catch-up after the switch, every run names the tags whose readings from
where the copy got to up to the switch are not copied; `--include-older` with
the same resume file copies just those.

## What happens when something goes wrong

| What happened | What the copy does |
|---|---|
| Interrupted — Ctrl-C, a reboot, a dropped connection | Run the same command again. The resume file says where every tag got to |
| A request got no answer | Waits 90 seconds, then asks Tag Historian whether that batch arrived — by the newest reading the tag has been sent, and by reading the batch's time window back — before sending it again: Tag Historian does not deduplicate, so a blind resend could double it |
| Run twice, or the resume file was deleted | Nothing is written twice: a tag only receives readings newer than it already holds (with `--include-older`, only the resume file prevents it) |
| Started again while a run is still going — cron firing during a `--wait` run | The second one refuses to start: two runs on one resume file would send the same readings twice |
| The resume file used with a key for another account | Refuses to start: the file's positions belong to the account it was copying into |
| A sensor silent for months, then reporting every few seconds | Counts each stretch of history before reading it, so no single read is bigger than about 20,000 readings |
| `429` or `503` with `Retry-After` | Waits exactly as long as asked, and sends the same batch |
| Today's allowance is spent | Stops for the day (exit status 3), or with `--wait` sleeps until midnight UTC |
| A new tag the plan has no room for (`422`) | Skips that entity, says so, carries on with the rest, and tries it again on a later run once there is room. The dry run warns before you start, and the copy refuses to start a selection your plan cannot hold |
| Storage full (`402`), or the key refused (`401`/`403`) | Stops, says why. Nothing is lost; run it again once it is fixed |
| InfluxDB 1.x cuts a result short (`max-row-limit`) | Asks for a smaller window, down to a minute |

## Units

With its default settings, Home Assistant's InfluxDB integration does not write
an entity's unit as a field — it makes the unit the measurement name (`°C`,
`kWh`). So when a series has no `unit_of_measurement_str` field, the copy reads
the measurement as the unit, except where it is plainly something else: the
entity id (an entity without a unit), the domain or `domain__device_class`
(the `measurement_attr` setting). Live data from the built-in integration
carries no unit, so the copy is what gives these tags theirs, and live data
leaves it as it is.

Tag Historian saves a tag's unit a few minutes after a batch brings it, and a
restart of Tag Historian before then loses it — and the built-in integration
never sends one to bring it back. So when a run finishes, run the same command
with `--dry-run` ten minutes later: it compares every tag with the unit the
copy gave it and names any that no longer has it. Set those on the tag's page
in Tag Historian Explorer, or let a `--catch-up` that still has readings for
the tag bring the unit along again.

If your InfluxDB integration used a fixed measurement name
(`default_measurement` or `override_measurement`), say so with `--not-a-unit
NAME`, or turn the guess off with `--no-unit-from-measurement`. The dry run
shows the unit of every tag before anything is written.

## All options

```text
taghistorian-influx-migrate --help
```

| Option | |
|---|---|
| `--entity PATTERN`, `--exclude PATTERN` | Which entities; repeatable |
| `--measurement NAME` | Only these InfluxDB measurements; repeatable |
| `--start`, `--end` | The range, UTC, as `2024-01-31` or `2024-01-31T12:00:00Z`. Default: everything up to now. Fixed by the first run |
| `--catch-up` | Move the end of the range to now |
| `--leave-for-live PERCENT` | How much of each day's allowance to leave alone; default 25 |
| `--max-per-day N` | Copy at most N readings per UTC day |
| `--wait` | Keep running across midnights |
| `--include-older` | Also copy readings older than a tag already holds — read "Why the order matters" first |
| `--state FILE` | Another resume file, for a second, separate copy |
| `--no-unit-from-measurement`, `--not-a-unit NAME` | See "Units" |
| `--influx-insecure` | Do not check InfluxDB's TLS certificate (a self-signed one at home) |
| `--url` | Another Tag Historian, for a self-hosted one |

## Development

From `influxdb-migration/`:

```bash
python -m venv .venv
.venv/bin/pip install -e ".[dev]"   # .venv\Scripts\pip on Windows
.venv/bin/pytest
.venv/bin/ruff check . && .venv/bin/ruff format --check .
```

The tests run the real command against two fake servers on localhost: an
InfluxDB that answers InfluxQL (`/query`) and Flux (`/api/v2/query`, annotated
CSV) the way 1.x and 2.x do, and refuses any query it does not know, and a Tag
Historian that implements the write endpoint's contract — naming, friendly name
and unit, the daily allowance, the tag limit, and every refusal above.
