# Tag Historian for Home Assistant

Keep long-term history for the sensors you actually care about, in
[Tag Historian](https://taghistorian.com) — a small, cheap time-series
database — without editing `configuration.yaml` and without restarting Home
Assistant.

You pick the entities in a normal Home Assistant dialog. The dialog tells you
how many series your plan allows, how many you have left, and what your
selection will cost against your daily reading allowance — all read live from
your own account, so the numbers on screen are your numbers.

This repository is the public home of Tag Historian's client-side
integrations. Besides this Home Assistant integration it ships the official
[Python client](python-client/), an [MCP server](mcp-server/) for Claude
Desktop and other AI assistants, a tool that [copies your history out of
InfluxDB](influxdb-migration/) and a [Grafana dashboard](grafana/) for Home
Assistant entities, and it carries the documentation and public image name of
the [collector](collector/) for MQTT, OPC UA, Sparkplug B and Modbus TCP —
see [Also in this repository](#also-in-this-repository).

## Running InfluxDB for Home Assistant? Keep it, or drop the server

If Home Assistant writes to an InfluxDB of your own and you are happy running
it, keep it: a local database costs only the machine it is on, and nothing here
asks you to move. Move when you would rather not run a database for this at
all — then Tag Historian is the database, somebody else runs it, and the
history you already have comes along.

**What stays the same.** Each entity keeps its name —
`sensor.outdoor_temperature` is the tag `sensor.outdoor_temperature`, in the
copied history and live alike — and Grafana can still draw it, through Grafana
Labs' Infinity data source and the dashboard in step 3 rather than your old
InfluxDB queries. The live sending moves to this integration, with a picker in
place of your `include` filter. Without HACS, Home Assistant's own InfluxDB
integration can keep doing it: Tag Historian speaks the InfluxDB v2 write API,
so that switch is the integration's own dialog, and the filter stays as it is.

**What you gain.** No database to run, upgrade or back up. Full resolution for
as long as your plan keeps it, then hourly minimum, maximum, average and count
for ten years, on every plan — see
[how long data is kept](https://taghistorian.com/docs/retention). A nightly
backup to a geo-redundant vault whose offsite copy sits in a second Swedish
region. Alerts by email, push and chat when a value crosses a limit or a sensor
goes quiet.

**What you give up.** InfluxQL and Flux: data comes back out through Tag
Historian's own API, its charts, exports and Grafana's Infinity data source, not
through InfluxDB tooling. Text states and attributes are not stored, only
numbers. And it is a paid service past the free plan's tags.

**The move, in three steps** (in full, with the order that matters, at
[taghistorian.com/docs/migrate-from-influxdb](https://taghistorian.com/docs/migrate-from-influxdb)):

1. **Copy the history** with [`influxdb-migration/`](influxdb-migration/) while
   Home Assistant still writes to InfluxDB. It names every tag exactly as live
   forwarding will, carries each entity's friendly name and unit, and spreads a
   big history over as many days as your plan's daily allowance needs — with a
   dry run first and a resume file throughout.
2. **Switch the live data to Tag Historian** straight after the copy's last
   catch-up run, one of two ways — and only one of them, because both would
   send every reading twice:
   - **This integration**, if you can install HACS: [install](#install) and
     [set it up](#set-up), pick the entities the copy took, then delete the
     InfluxDB entry, or leave it writing to your own InfluxDB if you are keeping
     that too. It names a tag after the entity id, as the copy does — unless
     your InfluxDB configuration adds tags of its own, from a `tags:` block for
     instance: the copy puts those in the tag names, as the built-in integration
     does, and this integration does not, so take the second way then.
   - **Home Assistant's built-in InfluxDB integration**, pointed at Tag
     Historian: **Reconfigure** on an InfluxDB 2.x connection; for 1.x, which
     Reconfigure cannot turn into 2.x, delete the entry and add it again as
     v2.x. [taghistorian.com/docs/home-assistant](https://taghistorian.com/docs/home-assistant)
     has both.
3. **Import [`grafana/home-assistant.json`](grafana/)** into Grafana: an entity
   dropdown and a chart per entity, in its tag's unit and under its friendly
   name. The copy gives every entity it copies its unit, and this integration
   sends the unit of every entity that has one. With the built-in integration,
   set the unit of an entity you add to Home Assistant later once on the tag's
   page in Tag Historian Explorer, because on its default settings it does not
   send it in a form Tag Historian reads.

Every limit that decides how long step 1 takes — tags, readings a day — is read
from your own account when the copy runs, and the dry run prints them.

With this integration doing step 2, its [history import](#import-your-history)
adds to the copy and never doubles it: Tag Historian counts the copied readings
as live data, and an import only takes what is older than a series' first live
reading, under any name the series has had — an entity renamed after the copy
included. It brings what the recorder has from before your InfluxDB did, if
anything, and the history of entities your InfluxDB never had.

## This one, or the built-in integration?

This one, if you can install HACS: it is the way Tag Historian recommends
sending Home Assistant history, on
[taghistorian.com](https://taghistorian.com/docs/home-assistant-hacs) and on the
Explorer's first screen. Without HACS, Home Assistant's **built-in `influxdb`
integration works** with Tag Historian
too — its setup is at
[taghistorian.com/docs/home-assistant](https://taghistorian.com/docs/home-assistant) —
and if you already run it and are happy with it, nothing forces a move.

What this integration has that the built-in one does not:

- **No YAML, no restart.** Entity selection is a picker, and changing it later
  is a picker again.
- **The picker is a short list.** A normal Home Assistant has several hundred
  entities and almost none of them are worth a series. What you get is the
  things whose *job* is to report something — sensors, binary sensors, numbers,
  counters — with the best ones already ticked. The list is decided by what
  each entity is, not by what it happens to read at that moment, so a flat
  battery never removes anything from it. Everything else is one toggle away,
  not gone.
- **The quota is visible while you choose.** Tag Historian charges by the
  number of series, not by how much data they hold, so *which* entities you
  send is the decision that matters. This shows you the budget as you spend it,
  and refuses to let you sail past it without saying so.
- **History that survives a rename.** Rename an entity in Home Assistant, or
  replace a meter and give the new one the old one's id, and the series follows
  it with its history intact - nothing is lost, nothing starts over, and it does
  not cost a second series. See [When you rename an entity](#when-you-rename-an-entity).
- **It can tell you why data stopped arriving.** The built-in integration
  cannot: it has one failure mode, it never reads the `Retry-After` header, and
  it has no notion of the API's "some of that batch was refused" answer. This
  one turns each of those into a Repairs card that says which problem it is and
  what to do — a revoked key, entities beyond your plan's series count, the
  daily reading allowance being spent, or Tag Historian simply being busy. The
  last two are different problems and are never described as the same one.
- **It brings the history you already have.** The recorder on your Home
  Assistant box holds days of readings from before you installed this — on an
  SD card, more often than not, the part most likely to fail. One action
  copies them into Tag Historian with their original timestamps, and imported
  history does not count against your daily reading allowance. See
  [Import your history](#import-your-history).
- **A restart or an outage does not cost you your readings.** Readings it
  could not deliver are saved when Home Assistant stops and sent when it
  starts again, and readings it had to let go of during an outage are read
  back from the recorder while the recorder still has them. See [When the
  connection or Home Assistant goes
  away](#when-the-connection-or-home-assistant-goes-away).

What it does **not** do: read data back out of Tag Historian, replace the
recorder, or keep attributes as history. It is a one-way export of entity
states - each with the entity's friendly name and unit, so the series is called
what Home Assistant calls it and its readings carry their unit.

## Install

### Through HACS

[![Open your Home Assistant instance and open the Tag Historian repository inside the Home Assistant Community Store.](https://my.home-assistant.io/badges/hacs_repository.svg)](https://my.home-assistant.io/redirect/hacs_repository/?owner=softi-dev&repository=tag-historian-clients&category=integration)

The button opens this repository in HACS on your own Home Assistant, through
My Home Assistant, which asks for your Home Assistant's address the first time.
HACS asks whether to add it as a custom repository: press **Add**, then
**Download**, and restart Home Assistant.

Without the button:

1. HACS → three-dot menu → **Custom repositories**
2. Repository: the URL of this repository. Category: **Integration**.
3. Install **Tag Historian**, then restart Home Assistant.

### By hand

Copy `custom_components/tag_historian/` into your Home Assistant
`config/custom_components/` directory and restart.

## Set up

**Settings → Devices & services → Add integration → Tag Historian**, or, once
it is installed and Home Assistant has restarted:

[![Open your Home Assistant instance and start setting up the Tag Historian integration.](https://my.home-assistant.io/badges/config_flow_start.svg)](https://my.home-assistant.io/redirect/config_flow_start/?domain=tag_historian)

You will need an API key whose **Scope** is **Write**, created in the Tag
Historian dashboard under API keys: press **Create New Key**, name it, and leave
**Scope** on **Write**, where the dialog opens. A Read key is refused the moment
this integration tries to forward anything; it is detected during setup and
named as such, rather than failing quietly later. The key list shows the scope
beside every key, so you can check one you already have instead of issuing
another.

A Write key forwards readings, and creates a tag the first time it writes to a
name that does not exist yet — against your tag allowance. It cannot issue or
revoke API keys, not even its own; it cannot delete a tag, which is what deletes
that tag's history; it cannot change your plan or cancel your subscription; and
it cannot invite or remove anyone from your team. Those endpoints refuse it with
a `403`; they do not quietly do nothing. It can rename a tag or merge one into
another, which is how this integration carries a renamed entity's history
over. Neither deletes anything: the history stays where it is stored, and both
can be undone in Tag Historian Explorer. The key an account recovery hands you
is **Admin**, which is the whole account, and Home Assistant has no use for any
of it.

Three screens:

1. **Connect.** Host (leave the default alone unless you self-host) and your
   API key. The key is checked against your account before anything is saved,
   so a typo is caught here rather than a week later.
2. **Choose.** The entities whose job is to report something, with the best
   candidates already ticked and spread across your devices, cut off at the
   number of series your plan has left. Things you *operate* rather than
   measure — lights, switches, locks, automations, scripts, update entities,
   the sun — are not in the list until you tick **Show every entity**, which
   puts every entity in the install back in it, unfiltered. There is also a
   minimum interval, which is how you stop a chatty sensor spending your daily
   reading allowance.

   The same screen has **Use my approximate location (about 10 km) for
   outdoor weather from SMHI**, off until you tick it. Ticked, this Home
   Assistant's location is rounded to a tenth of a degree and proposed as your
   account's place, so Tag Historian can draw the outdoor temperature and
   precipitation from the Swedish Meteorological and Hydrological Institute's
   nearest weather stations beside your charts (Sweden only). It is only a
   proposal: a place already set in Tag Historian's Settings stays, and that is
   where you change or remove it. Left off, nothing about your location is
   sent.

   A sensor with no device class and no state class is listed but left
   unticked. Home Assistant knows nothing about those either way: most are
   hand-written template sensors reporting a number, and a few report a word
   instead — see [what one entity costs](#what-one-entity-costs) for what
   happens if you tick one of the latter.
3. **Review.** The same sums, recomputed from what you actually ticked. If
   everything fits, that is all the screen says. If your selection needs more
   series than you have, it says so instead and offers to trim it back for you
   — it removes the lowest-ranked entities that need a *new* series, and never
   one whose series already exists, because that one was costing you nothing.

If your account has no free series at all and everything you picked would need
one, setup stops and says so rather than finishing with an empty selection.

Change any of it later: **Settings → Devices & services → Tag Historian →
Configure → Change what you keep history for.**

## Import your history

**Settings → Devices & services → Tag Historian → Configure → Import the
history Home Assistant has already recorded**, or the action
`tag_historian.import_history` (from **Developer tools → Actions**, a script or
an automation):

```yaml
action: tag_historian.import_history
data:
  entity_id:            # optional: default is every entity you keep history for
    - sensor.house_power
  days: 30              # optional: default is as far back as your plan's window
```

It reads what Home Assistant's recorder holds for those entities and sends it
to Tag Historian with the original timestamps, through the same rules as the
live readings — the same numbers and on/off words, `unavailable` left as a
gap, the same minimum interval, timestamps to the second. It runs in the
background: the **Tag Historian history import** sensor shows how far it is
(`idle`, `running`, `finished` or `stopped`, with the counts as attributes),
and a notification says when it is done and what it did. A restart carries it
on where it stopped.

What Tag Historian holds an import to — every number below is read from your
own account, none of it is written into this integration:

- **Your import allowance, instead of the daily one.** Imported readings do not
  count against your daily reading allowance. Each account has an import
  allowance instead: your plan's daily reading allowance times its
  full-resolution window in days — what the plan could have stored in that
  window — for all imports together. The import screen shows what is left; a
  larger plan raises it. When it is used up the import stops and says so, and
  live readings carry on unaffected.
- **Your plan's full-resolution window.** Readings older than it are skipped,
  so the import starts there.
- **History only.** Readings older than the moment the import started, older
  than the first reading of the entity Tag Historian received live, and older
  than the start of the first import of that entity (for a renamed series,
  under any name it has had). Everything after that is
  your live data, which this integration sends itself — including readings
  still waiting to be sent, so nothing arrives twice. Importing the same
  history again works; feeding an entity fresh readings through imports does
  not.
- **The tag limit and the storage cap**, as for live data. An imported entity
  uses a series like a forwarded one, which is why only entities you keep
  history for can be imported. It is imported into the series its live
  readings go to - for an entity you have renamed, the one its history is in
  (see [When you rename an entity](#when-you-rename-an-entity)).

Running an import twice stores nothing twice: Tag Historian skips a reading it
already has for that series at that timestamp, and a reading it skips costs no
allowance.

**Not imported: long-term statistics.** Once the recorder purges its states
(after 10 days by default), Home Assistant keeps only hourly mean, minimum and
maximum values. Those are averages, not readings, and Tag Historian stores
readings — so they are not imported yet, rather than imported as something
they are not. Import soon after installing to keep the most.

## What one entity costs

One selected entity is exactly one Tag Historian series — no more, no
asterisks. The integration sends at most a single numeric reading per state
change and never a second series for the same entity, so the count on the
review screen is the count you will be billed for.

Only the entity's own state is sent as a reading, never its attributes. Two
attributes go along to describe it: the friendly name, which becomes the
series' description, and the unit of measurement, which becomes its unit -
what Tag Historian Explorer labels the chart with. Rename the entity in Home
Assistant and the description follows with its next reading. A reading is
therefore a number, or one of a small set of words that *are* a number in
disguise: `on`/`off` for a light, a switch, a fan, an automation, a helper or a
binary sensor, `open`/`closed` for a cover, `above_horizon`/`below_horizon` for
the sun. Those become 1 and 0.

Everything else stores nothing at all, and the ones that catch people out are
worth naming:

| Entity | State | What is stored |
|---|---|---|
| `sensor.outdoor_temperature` | `21.5` | 21.5 |
| `binary_sensor.heat_pump_running` | `on` / `off` | 1 and 0 |
| `climate.living_room` | `heat`, `cool`, `off` | nothing |
| `person.you` | `home`, `Work`, `not_home` | nothing |
| `lock.front_door` | `locked`, `jammed`, `open` | nothing |
| `sensor.house_mode` | `home`, `night`, `away` | nothing |

The last three are the reason the rule is about the *domain* rather than the
word. A lock, a person and a plain sensor can each report a word we recognise
and several we do not, so translating the ones we recognise would produce a
chart that jumps to 1 for one state and shows nothing for the rest — a series
that looks like data and is not. Storing nothing at all is the honest answer,
and each of those state changes moves the **readings dropped** counter, which
also names the offending entity in its attributes. If you want a number out of
one of these, make a
[template sensor](https://www.home-assistant.io/integrations/template/) that
exposes it as its own entity, and select that.

An entity that is `unavailable` or `unknown` when you open the picker is still
listed, and one you have already selected stays selected. Its state right now
decides whether there is a reading to send, never whether you are allowed to
choose it.

`unknown` and `unavailable` are never sent, and never counted as dropped
either: they are Home Assistant saying it has nothing to report. A gap in the
chart means exactly that — Tag Historian never stores an invented zero.

## When you rename an entity

A series is named after its entity's id: `sensor.energy_meter` is stored as the
series `sensor.energy_meter`. Rename the entity in Home Assistant and this
integration follows it. The selection is updated to the new id, and readings
keep arriving - before version 2026.11.0 they stopped without a word, because
the integration was listening for an id that no longer existed.

The history follows as well, in one of two ways:

- **With "Rename tags when entities are renamed" switched on** (on the Choose
  screen, and under Configure later), the integration asks Tag Historian to
  rename the series to the new id straight away. Nothing is copied or moved:
  the readings from before stay where they are stored and read under the new
  name, and it is still one series. If the new id already has a series of its
  own - you replaced a meter and gave the new one the old one's id - the two
  are merged into one history under that name, and the merged-in one stops
  counting against your plan. If Tag Historian cannot be reached, the
  integration asks again later; the readings go on into the old series
  meanwhile.
- **With it off** (the default), the readings go on into the series the entity
  already has, so nothing is lost and no second series is started, and a
  Repairs card asks what you want: rename the series and keep its history, or
  start a new series under the new id and leave the history under the old
  name, which takes one more series on your plan. For a replaced meter, the card
  asks whether to merge the two histories or keep them apart.

Tag Historian keeps the old id as a *former name* of the series. Anything still
sent under it - a script, Home Assistant's built-in `influxdb` integration - is
stored in the renamed series, and a chart or a query that asks for the old name
gets the renamed series. Both can be undone in Tag Historian Explorer, under
**Tags**: a rename by renaming the series back, a merge with **Separate** next
to the name under **Former names**.
Readings are never adjusted: a replacement meter that started from zero still
shows the drop where the two histories meet.

What is still on its way when you rename an entity goes with it, into the
series its readings go to: readings held over a restart or an outage, the ones
read back from the recorder afterwards, and a history import that is running,
which carries on under the new id. An entity renamed before you import its
history has it imported into that series as well, so the import does not
start a second series beside it either - and once the series has the new
name, the import still stops where its live readings began under the old one.
If you chose a new series on the card instead, or to keep a replaced meter's
two histories apart, the import leaves that entity out: its history stays in
the old series, as the card says, and what comes after your choice arrives
live. The recorder is read where Home Assistant keeps the entity's history:
under the new id, where it moves the history when you rename an entity - or,
for a replacement meter given the old meter's id, under the id the new meter
had before, because Home Assistant then leaves both histories where they
were. While the merge waits - for your
answer on its card, or for Tag Historian to be reachable - nothing of the old
meter's is read as the new one's.

Renaming a device renames all of its entities at once, and each of them is
followed. So is a rename made while the integration reloads or waits for Tag
Historian to come back. A rename made while this integration is not running -
disabled, or not yet installed - is not seen: the entity simply stops arriving
under its old id.
Select it again under its new id, and rename the old series to the new id in
Tag Historian Explorer (**Tags**, open the series, **Rename**) to keep its
history in one piece - or, if readings under the new id have already started a
series, use **Merge into...** instead.

## Sensors it adds

| Sensor | What it is |
|---|---|
| `sensor.tag_historian_tags_used` | Series in use on your account, with the limit as an attribute |
| `sensor.tag_historian_readings_today` | Readings stored today, against your daily allowance |
| `sensor.tag_historian_readings_sent` | Readings this integration has delivered |
| `sensor.tag_historian_readings_queued` | Readings waiting to be sent right now |
| `sensor.tag_historian_readings_dropped` | Readings that did not arrive, and why, in the attributes |
| `sensor.tag_historian_history_import` | Where a history import is, with its counts as attributes |

The last one is there on purpose: nothing is dropped silently, so if data is
missing there is always a counter that moved. Its state adds up every way a
reading can fail to become history:

| Attribute | What it counts |
|---|---|
| `dropped_over_quota` | Refused because it needed a series your plan has no room for |
| `dropped_queue_overflow` | Evicted from a full buffer, oldest first. During an outage they are read back from the recorder once Tag Historian answers again, while the recorder still has them; while today's reading allowance is spent they are over the allowance and are not |
| `dropped_on_shutdown` | Neither delivered nor saved when the integration stopped: handed back too late to requeue, or a save that failed |
| `dropped_mid_write` | Already in a request that never came back, because Home Assistant stopped or the entry was reloaded underneath it |
| `dropped_not_a_number` | The entity reported a word this integration cannot store |

`entities_storing_nothing` names the entities behind that last one, so a chart
that stays empty says why. `dropped_rate_limited` is on the attributes too but
is deliberately *not* in the total — those are the readings your minimum
interval skipped, which is what you asked it to do. Two more attributes are not
losses but what happened next: `backfilled_from_recorder`, readings read back
from the recorder to fill a gap, and `restored_after_restart`, readings saved
at the last stop and sent after it.

The counters keep their values across a reload, so fixing a Repairs card does
not erase the evidence of what it was telling you about. A Home Assistant
restart clears them — they are held in memory — while the readings they
describe are saved and sent, and anything that could not be is written to the
log as well as counted.

## When something goes wrong

Everything below shows up in **Settings → Repairs**.

| What happened | What the integration does |
|---|---|
| Your API key was revoked | Asks for a new one through Home Assistant's normal re-authentication card, and stops retrying the dead key meanwhile |
| Some readings exceed your plan's series count | Names the series, counts the readings that were lost, keeps sending everything else, and offers to reopen the picker so you can deselect |
| Today's reading allowance is spent | Holds the newest readings — through a restart too — and waits exactly as long as the API asks; it does not hammer a closed door. What no longer fits in the buffer meanwhile is over the allowance: counted, not sent later |
| Tag Historian is busy or briefly unreachable | Holds and retries quietly; only raises a card if it lasts, and that card never mentions your plan, because this is not about your plan |
| Your account cannot write yet | Shows Tag Historian's own explanation, word for word |
| An entity you send was renamed | Follows it and keeps its readings arriving in its history; renames the series in Tag Historian if you switched that on, or asks you on a card - see [When you rename an entity](#when-you-rename-an-entity) |

## When the connection or Home Assistant goes away

When Home Assistant stops — a restart, an update, a reboot — or when the entry
is reloaded because you changed the options or fixed a Repairs card, three
things happen before the buffer is let go of:

- a request that is already on its way out gets up to five seconds to finish,
  so a stop or a reload landing mid-write normally **delivers** those readings
  rather than losing them;
- whatever is still buffered gets a last few delivery attempts;
- and anything that will not go is **saved** to Home Assistant's `.storage`
  (`tag_historian.<entry>.forwarder`), with how far Tag Historian had
  acknowledged each entity, and sent when the integration starts again. The
  file is bounded like the buffer — at most ten thousand readings, a few
  megabytes — and is written when the integration stops and when writes start
  or stop failing, not on every reading, so it does not wear an SD card.

Both of those apply to both ways out, and that is worth stating because the two
are not the same underneath. Home Assistant cancels an integration's background
tasks *before* it announces that it is stopping, and a write in flight is one of
those tasks — so the wait has to be booked earlier than the stop announcement,
not in response to it. An integration that only handled the reload would give
you the full five seconds on the rarer event and nothing at all on the one that
happens every time you restart.

A request that does *not* finish in that window is abandoned rather than
re-sent. The live write endpoint does not deduplicate, and it may already have
taken the batch before the connection went, so re-sending it would risk
doubling readings in your history instead of restoring them. Those readings are
counted as `dropped_mid_write`, which means that counter can *overstate* the
loss — some of them may have landed. That is the direction chosen on purpose:
an overstated loss gets asked about, an understated one is invisible.

The buffer is also bounded — during a long outage the oldest go first and
`dropped_queue_overflow` moves. They are not gone: the recorder still has
them. For every entity the integration keeps the time of the last reading Tag
Historian acknowledged, and after a gap — an outage that overflowed the buffer,
a restart, a stretch with the integration disabled — it reads the recorder for
what lies between that reading and the oldest one it still holds, and sends
that first, oldest first. That is your live data arriving late, so it counts
against your daily reading allowance like any other reading; a long gap can
take more than a day's allowance, and then it waits for the allowance like any
reading does.

What it reads back is what the recorder still has: Home Assistant purges
states after 10 days by default, and an entity you have excluded from the
recorder has nothing to read back. And readings let go of while today's
allowance is spent are not read back at all. Those are over the allowance, not
lost to an outage, and reading them back at midnight would put the new day's
readings behind them — every day, for an account that sends more than its
allowance. The buffer still holds the newest of them and sends those when the
allowance resets. (One exception: an entity that is still catching up on an
earlier outage when the allowance runs out has its readings read back as
usual, so none of that outage is lost.)

It does not re-send a reading Tag Historian may already have. A request that
was abandoned mid-write counts as delivered, never as a gap, and if Home
Assistant did not shut down cleanly — a crash, a power cut — while writes were
going through, nothing is read back: the last moment before it may be missing,
and nothing is doubled. (The recorder loses its own last few seconds to a power
cut too.) If writes were failing when it went down, or it had not yet had an
answer since it started, nothing had been acknowledged since, and the whole
outage is read back — with the one exception of a retry Tag Historian took in
the very moment of the crash, which then arrives twice.

## Development

```bash
pip install -r requirements_test.txt
python -m pytest
python -m ruff check .
```

The tests run against the exact Home Assistant version pinned in
`requirements_test.txt` — the same minor release whose floor `hacs.json`
claims.

On **Windows** the suite needs the two shims in `tests/conftest.py` — Home
Assistant's test harness assumes a Linux event loop, and both shims explain
themselves in place. CI runs Ubuntu, where neither is used.

## Also in this repository

- **[`python-client/`](python-client/)** — `taghistorian`, a small synchronous
  Python client for the Tag Historian API: writes (single and batch), reads,
  aggregated queries, CSV/Parquet export, and read-only listing of alerts and
  alert rules, with automatic retries that honour the server's `Retry-After`.
  Built for cron jobs and PLC bridges; `requests` is its only dependency. Not
  on PyPI yet — its README has the install-from-this-repository one-liner.
- **[`mcp-server/`](mcp-server/)** — `taghistorian-mcp`, an MCP server that
  lets Claude Desktop or any MCP-compatible host query your account: tags,
  history, aggregates, alerts. **Read-only by default** — the three write
  tools don't exist as far as the host is concerned unless you explicitly
  enable them. Its README explains the write gate before you turn it on.
- **[`influxdb-migration/`](influxdb-migration/)** — `taghistorian-influx-migrate`,
  a command-line tool that copies Home Assistant's history out of InfluxDB 1.x
  or 2.x into Tag Historian, under the names live forwarding uses, with each
  entity's friendly name and unit. Dry run first, a resume file throughout, and
  the daily allowance shared politely with live data. `requests` is its only
  dependency; installed from this repository.
- **[`grafana/`](grafana/)** — `home-assistant.json`, a Grafana dashboard for
  Home Assistant entities stored in Tag Historian: an entity dropdown and one
  chart per entity, in its tag's unit and under its friendly name, through
  Grafana Labs' Infinity data source.
- **[`collector/`](collector/)** —
  `ghcr.io/softi-dev/tag-historian-clients/collector`, the container image
  you run next to your MQTT broker, OPC UA server, Sparkplug B namespace or
  Modbus TCP devices. It connects outward only and buffers to disk when the
  uplink drops.
  **Source not included** — the collector is part of the proprietary
  product, built in its private repository and published under this one so
  that its image, documentation and issue tracker are somewhere public. What
  lives here is its README.

The three Python packages each have their own README, test suite and Python
floor, the collector has its README and nothing to build, the dashboard is a
file to import, and everything ships on its own cadence — they only meet in
this repository.

## License

Everything in this repository is licensed under
[Apache-2.0](LICENSE). That covers these clients only — the Tag Historian
service itself is a separate, proprietary product.

---

Made by [Softi AB](https://taghistorian.com). Not affiliated with the Home
Assistant project or with InfluxData.
