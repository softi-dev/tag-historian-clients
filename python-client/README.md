# taghistorian

A small, synchronous Python client for the [Tag Historian](https://taghistorian.com) time-series
API. Built for the kind of place Tag Historian actually gets used from: a cron job, a quick
script polling a sensor, or a bridge sitting between a PLC and the cloud - not a web framework.
If you need `async`/`await`, this isn't that client; everything here blocks, on purpose.

**Not yet published to PyPI.** Install it straight from this repository (see below) until it is.

## Install

This package isn't on PyPI yet, so install it directly from the repository. Pick whichever of
these matches how you manage dependencies:

```bash
# Straight from a git checkout, editable (good for trying it out or hacking on it):
git clone https://github.com/softi-dev/tag-historian-clients.git
pip install -e tag-historian-clients/python-client

# Or, without cloning, letting pip do it (pin a commit/tag for anything you'll rely on):
pip install "git+https://github.com/softi-dev/tag-historian-clients.git#subdirectory=python-client"
```

Requires Python 3.9 or newer. The only runtime dependency is
[`requests`](https://pypi.org/project/requests/).

## Getting an API key

Sign up at [taghistorian.com](https://taghistorian.com), then create an API key with **Write**
scope (or higher) for anything that sends measurements, and **Read** scope for anything that
only queries data. Keep the key out of source control - an environment variable is the usual
choice, and every example below reads it from one.

## Quickstart: writing a measurement

```python
import os
from taghistorian import TagHistorianClient

client = TagHistorianClient(api_key=os.environ["TAGHISTORIAN_API_KEY"])
result = client.write("boiler.temperature", 84.2, units="C")
print(result.success, result.was_compressed)
```

Five lines, and that's a real write. A couple of things worth knowing about it:

- **Timestamp**: omit it (as above) and the server stamps it with "now" the instant it's
  received - fine for anything reporting live. Pass a `datetime` yourself for backfilling
  historical data. If the `datetime` has no timezone attached, it's assumed to already be UTC -
  attach the right zone yourself if your source is local time.
- **Tags are created implicitly.** The first `write()` for a tag name that doesn't exist yet
  creates it (within your plan's tag limit). Use `create_tag()` first only if you want to set
  metadata - units, compression, a chart axis range - before the first value lands.

Prefer the client as a context manager so its connection pool gets closed cleanly when you're
done with it:

```python
with TagHistorianClient(api_key=os.environ["TAGHISTORIAN_API_KEY"]) as client:
    client.write("boiler.temperature", 84.2, units="C")
```

## Writing in bulk

```python
from taghistorian import MeasurementInput

client.write_batch(
    [
        MeasurementInput(tag_name="boiler.temperature", value=84.2),
        MeasurementInput(tag_name="boiler.pressure", value=1.03),
        MeasurementInput(tag_name="boiler.running", value=1),
    ]
)
```

One call can carry up to 1000 measurements - the API's own limit, not a client guess. Pass more
than that and `write_batch()` raises `ValueError` immediately, before any network call: batching
is one atomic write on the server, so this client won't silently split an over-sized batch into
several smaller, non-atomic ones on your behalf. If you have more than 1000 points to send,
chunk them yourself, in whatever grouping makes sense for your data (e.g. per second, per file).

## Reading data back

```python
from datetime import datetime, timedelta, timezone

# The last known value:
last = client.read_last("boiler.temperature")
print(last.value, last.timestamp)

# Raw measurements over a range (both from/to default to the last 24h if omitted):
now = datetime.now(timezone.utc)
history = client.read("boiler.temperature", from_=now - timedelta(hours=6), to=now)
for m in history.measurements:
    print(m.timestamp, m.value, m.status)
```

You never need to know or paste your own account's customer ID - the client fetches it once
(via `GET /api/customers/me`), the first time you call any read method, and reuses it for the
rest of that client's lifetime.

## Aggregated / downsampled queries

For charts, dashboards, or anything that doesn't need raw resolution:

```python
result = client.read_aggregated(
    "boiler.temperature",
    from_=now - timedelta(days=7),
    to=now,
    interval="1h",
    aggregation="Average",  # Average | Minimum | Maximum | Sum | Count
    interpolation="Linear",  # None | Linear | StepForward
)
for bucket in result.measurements:
    print(bucket.interval_start, bucket.value, bucket.count)

# What interpolation actually happened - read this, don't assume the request held:
print(result.interpolation_type)
```

That last line matters for long ranges: queries reaching far enough back are answered from
pre-aggregated hourly data, and that path always reports back `"None"` for interpolation
regardless of what you asked for (re-bucketing an already-summarised hour isn't interpolation).
`result.interpolation_type` always tells you what the server actually did.

## Tags

```python
# List your tags, optionally filtered/paginated:
tags = client.list_tags(take=50, search="boiler")
for tag in tags.tags:
    print(tag.tag_name, tag.units, tag.last_value)

# Create one up front, with metadata:
client.create_tag(
    "boiler.temperature",
    description="Boiler outlet temperature",
    units="C",
    tag_type="Analog",
    scale_min=0,
    scale_max=150,
)
```

## Alerts

```python
# What's currently firing (Triggered or Acknowledged, never Resolved):
for alert in client.list_active_alerts():
    print(alert.severity, alert.tag_name, alert.message, alert.value)

# What rules exist and their thresholds - enabled or not:
for rule in client.list_alert_rules():
    print(rule.name, rule.tag_name, rule.condition, rule.threshold, rule.is_enabled)
```

Both are read-only and need only a Read-scope key - unlike everything else that touches
`/api/alerts`, they don't require Editor (creating/updating/deleting rules, acknowledging or
resolving an alert, isn't implemented in this client yet). Neither call needs your customer ID:
the alerts endpoints resolve the caller's account from the API key itself, not a URL segment, so
unlike `read`/`read_last`/`read_aggregated` above, calling either of these doesn't trigger the
one-time `GET /api/customers/me` lookup.

If a rule has a webhook signing secret configured, `rule.has_webhook_secret` is `True` - the
secret itself is never sent back over the wire by the API, so there is no field here that could
hold it.

## Exporting data

```python
# Get the file's bytes directly:
export = client.export_measurements("boiler.temperature", format="Csv")
print(export.filename, len(export.content), "bytes")

# Or stream it straight to disk (better for a large export):
client.export_measurements("boiler.temperature", format="Parquet", path="boiler_temp.parquet")
```

Omit both `from_`/`to` to export everything ever stored for the tag.

## Error handling

Every non-2xx response raises one of three exception types, all importable from `taghistorian`,
so you can handle each the way it actually calls for:

```python
from taghistorian import (
    TagHistorianAPIError,  # the server rejected the request - fix the request
    TagHistorianRateLimitError,  # retries were exhausted on a 429/503 - back off more
    TagHistorianConnectionError,  # never got a response at all - a network/DNS problem
)

try:
    client.write("boiler.temperature", 84.2)
except TagHistorianRateLimitError as e:
    print(f"still rate limited after {e.attempts} attempts, last Retry-After was {e.retry_after}s")
except TagHistorianAPIError as e:
    print(f"rejected: HTTP {e.status_code}: {e.message}")
    if e.violations:
        print(e.violations)
except TagHistorianConnectionError as e:
    print(f"could not reach the API after {e.attempts} attempt(s): {e.message}")
```

A call never returns `None` or a half-empty result on failure - it raises one of the above.

## Retries

A `429` (rate limited) or `503` (write buffer momentarily full) is retried automatically,
honouring the server's own `Retry-After` header rather than guessing a backoff. A `400`/`401`/
`403`/`404` is never retried - retrying an unchanged, rejected request cannot succeed. Tune or
disable this with `RetryConfig`:

```python
from taghistorian import RetryConfig, TagHistorianClient

client = TagHistorianClient(
    api_key=os.environ["TAGHISTORIAN_API_KEY"],
    retry=RetryConfig(max_retries=0),  # fail immediately instead of retrying
)
```

See the docstring on `RetryConfig` for the full set of knobs (backoff base/ceiling, and a
`sleep` override for tests or custom pacing).

## Self-hosted / staging

```python
client = TagHistorianClient(api_key="...", base_url="https://staging.example.com")
```

## Development

From `python-client/`:

```bash
python -m venv .venv
.venv/bin/pip install -e ".[dev]"   # .venv\Scripts\pip on Windows
.venv/bin/pytest
.venv/bin/ruff check .
```
