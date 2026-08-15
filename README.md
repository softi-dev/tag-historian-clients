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
[Python client](python-client/) and an [MCP server](mcp-server/) for Claude
Desktop and other AI assistants, and it carries the documentation and public
image name of the [collector](collector/) for MQTT, OPC UA and Sparkplug B —
see [Also in this repository](#also-in-this-repository).

## Do I need this? You may not

Home Assistant's **built-in `influxdb` integration already works** with Tag
Historian and is fully supported. If you have it running and you are happy with
it, there is nothing here you have to move to. The setup is documented at
[taghistorian.com/docs/home-assistant](https://taghistorian.com/docs/home-assistant).

What this integration adds:

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
- **It can tell you why data stopped arriving.** The built-in integration
  cannot: it has one failure mode, it never reads the `Retry-After` header, and
  it has no notion of the API's "some of that batch was refused" answer. This
  one turns each of those into a Repairs card that says which problem it is and
  what to do — a revoked key, entities beyond your plan's series count, the
  daily reading allowance being spent, or Tag Historian simply being busy. The
  last two are different problems and are never described as the same one.

What it does **not** do: read data back out of Tag Historian, replace the
recorder, or forward attributes. It is a one-way export of entity states.

## Install

### Through HACS (custom repository)

1. HACS → three-dot menu → **Custom repositories**
2. Repository: the URL of this repository. Category: **Integration**.
3. Install **Tag Historian**, then restart Home Assistant.

### By hand

Copy `custom_components/tag_historian/` into your Home Assistant
`config/custom_components/` directory and restart.

## Set up

**Settings → Devices & services → Add integration → Tag Historian.**

You will need an API key whose **Scope** is **Write**, created in the Tag
Historian dashboard under API keys: press **Create New Key**, name it, and set
**Scope** to **Write**. The dialog opens on **Read**, which is the right default
for the product and the wrong choice here — a Read key is refused the moment
this integration tries to forward anything. It is detected during setup and
named as such, rather than failing quietly later. The key list shows the scope
beside every key, so you can check one you already have instead of issuing
another.

A Write key forwards readings, and creates a tag the first time it writes to a
name that does not exist yet — against your tag allowance. It cannot issue or
revoke API keys, not even its own; it cannot delete a tag, which is what deletes
that tag's history; it cannot change your plan or cancel your subscription; and
it cannot invite or remove anyone from your team. Those endpoints refuse it with
a `403`; they do not quietly do nothing. The key an account recovery hands you
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
Configure.**

## What one entity costs

One selected entity is exactly one Tag Historian series — no more, no
asterisks. The integration sends at most a single numeric reading per state
change and never a second series for the same entity, so the count on the
review screen is the count you will be billed for.

Only the entity's own state is sent, never its attributes. A reading is
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

## Sensors it adds

| Sensor | What it is |
|---|---|
| `sensor.tag_historian_tags_used` | Series in use on your account, with the limit as an attribute |
| `sensor.tag_historian_readings_today` | Readings stored today, against your daily allowance |
| `sensor.tag_historian_readings_sent` | Readings this integration has delivered |
| `sensor.tag_historian_readings_queued` | Readings waiting in memory right now |
| `sensor.tag_historian_readings_dropped` | Readings that did not arrive, and why, in the attributes |

The last one is there on purpose: nothing is dropped silently, so if data is
missing there is always a counter that moved. Its state adds up every way a
reading can fail to become history:

| Attribute | What it counts |
|---|---|
| `dropped_over_quota` | Refused because it needed a series your plan has no room for |
| `dropped_queue_overflow` | Evicted from a full buffer during a long outage, oldest first |
| `dropped_on_shutdown` | Still undelivered when the last flush was done, buffered or handed back too late to requeue |
| `dropped_mid_write` | Already in a request that never came back, because Home Assistant stopped or the entry was reloaded underneath it |
| `dropped_not_a_number` | The entity reported a word this integration cannot store |

`entities_storing_nothing` names the entities behind that last one, so a chart
that stays empty says why. `dropped_rate_limited` is on the attributes too but
is deliberately *not* in the total — those are the readings your minimum
interval skipped, which is what you asked it to do.

The counters keep their values across a reload, so fixing a Repairs card does
not erase the evidence of what it was telling you about. A Home Assistant
restart clears them: they are held in memory, next to the buffer they describe.
Anything the restart could not deliver is written to the log as well as
counted, and the log is the half that is still there afterwards.

## When something goes wrong

Everything below shows up in **Settings → Repairs**.

| What happened | What the integration does |
|---|---|
| Your API key was revoked | Asks for a new one through Home Assistant's normal re-authentication card, and stops retrying the dead key meanwhile |
| Some readings exceed your plan's series count | Names the series, counts the readings that were lost, keeps sending everything else, and offers to reopen the picker so you can deselect |
| Today's reading allowance is spent | Holds readings in memory and waits exactly as long as the API asks — it does not hammer a closed door |
| Tag Historian is busy or briefly unreachable | Holds and retries quietly; only raises a card if it lasts, and that card never mentions your plan, because this is not about your plan |
| Your account cannot write yet | Shows Tag Historian's own explanation, word for word |

The buffer is held **in memory only**. When Home Assistant stops — a restart,
an update, a reboot — or when the entry is reloaded because you changed the
options or fixed a Repairs card, two things happen before it goes:

- a request that is already on its way out gets up to five seconds to finish,
  so a stop or a reload landing mid-write normally **delivers** those readings
  rather than losing them;
- whatever is still buffered gets a last few delivery attempts, and anything
  that will not go is added to the dropped counter and written to the log.

Both of those apply to both ways out, and that is worth stating because the two
are not the same underneath. Home Assistant cancels an integration's background
tasks *before* it announces that it is stopping, and a write in flight is one of
those tasks — so the wait has to be booked earlier than the stop announcement,
not in response to it. An integration that only handled the reload would give
you the full five seconds on the rarer event and nothing at all on the one that
happens every time you restart.

A request that does *not* finish in that window is abandoned rather than
re-sent. Tag Historian does not deduplicate, and the endpoint may already have
taken the batch before the connection went, so re-sending it would risk
doubling readings in your history instead of restoring them. Those readings are
counted as `dropped_mid_write`, which means that counter can *overstate* the
loss — some of them may have landed. That is the direction chosen on purpose:
an overstated loss gets asked about, an understated one is invisible.

The buffer is also bounded — during a long outage the oldest go first and
`dropped_queue_overflow` moves. Nothing is thrown away without being counted.

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
- **[`collector/`](collector/)** —
  `ghcr.io/softi-dev/tag-historian-clients/collector`, the container image
  you run next to your MQTT broker, OPC UA server or Sparkplug B namespace.
  It connects outward only and buffers to disk when the uplink drops.
  **Source not included** — the collector is part of the proprietary
  product, built in its private repository and published under this one so
  that its image, documentation and issue tracker are somewhere public. What
  lives here is its README.

The two Python packages each have their own README, test suite and Python
floor, the collector has its README and nothing to build, and everything
ships on its own cadence — they only meet in this repository.

## License

Everything in this repository is licensed under
[Apache-2.0](LICENSE). That covers these clients only — the Tag Historian
service itself is a separate, proprietary product.

---

Made by [Softi AB](https://taghistorian.com). Not affiliated with the Home
Assistant project or with InfluxData.
