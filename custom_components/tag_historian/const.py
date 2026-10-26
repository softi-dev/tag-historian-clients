"""Constants for the Tag Historian integration.

Deliberately absent from this file: tag limits, reading limits, plan names and
prices. Every one of those is read from the API at runtime
(``GET /api/usage/limits``), because a number typed here is a number that goes
stale the day pricing changes and then lies to the user from inside their own
Home Assistant. tests/test_no_hardcoded_quotas.py enforces that.
"""

from __future__ import annotations

import logging
from typing import Final

DOMAIN: Final = "tag_historian"
LOGGER: Final = logging.getLogger(__package__)

# Config entry data (secret / connection identity - survives an options change).
CONF_HOST: Final = "host"
CONF_API_KEY: Final = "api_key"

# Config entry options (the selection - changed freely from the options flow).
CONF_ENTITIES: Final = "entities"
CONF_MIN_INTERVAL: Final = "min_interval"

# Whether renaming an entity in Home Assistant renames its tag in Tag Historian
# too, so the history goes on under the new name (renames.py). Off by default:
# the user is asked through a repair card instead.
CONF_FOLLOW_RENAMES: Final = "follow_renames"

# {entity_id: entity_id} - readings of the first are sent under the tag of the
# second. Only ever set for an entity renamed in Home Assistant whose tag has
# not been renamed yet, so its readings go on landing in the history they
# belong to, at no extra tag, until the rename is confirmed (renames.py).
CONF_SEND_AS: Final = "send_as"

# {entity_id: ISO timestamp} - a renamed entity whose readings went into a tag
# of its own from that moment, its history left in the old tag: "Start a new
# tag" or "Keep them apart" on the rename card (renames.py). The history import
# leaves such an entity alone (importer.py). Kept when the selection changes:
# deselecting the entity and selecting it again does not make the old tag's
# history its own.
CONF_OWN_TAG_SINCE: Final = "own_tag_since"

# Whether Tag Historian may be told this Home Assistant's approximate location,
# so the account can show the outdoor weather from SMHI's nearest stations
# beside its charts (place.py). OFF by default: a location is the user's to
# give, and they tick it. Rounded to a tenth of a degree - about 10 km - here,
# before it leaves Home Assistant, and again by the API.
CONF_SHARE_LOCATION: Final = "share_approximate_location"

# Whether the first setup of the entry brings the history Home Assistant
# already has - its long-term statistics first, then the recorder's readings
# (importer.py). Asked on the last screen of a new entry, ticked by default.
# An entry made before that screen existed has no such option at all, and is
# offered the same through a repair card instead (repairs.py).
CONF_IMPORT_ON_SETUP: Final = "import_on_setup"

DEFAULT_HOST: Final = "api.taghistorian.com"

# The three write-path limits below are PROTOCOL facts, not plan facts, so they
# are safe to hold as constants. Each mirrors a named constant on the API side.

# InfluxWriteController.MaxPointsPerRequest. Above it the endpoint answers 413.
# Our flush batches are two orders of magnitude smaller, so this is a guard
# against a pathological backlog drain, not a routine code path.
MAX_POINTS_PER_REQUEST: Final = 5000

# Matched to the built-in influxdb integration's BATCH_BUFFER_SIZE/BATCH_TIMEOUT.
# Two triggers, whichever comes first: a full buffer, or a quiet second. The
# second one is what stops a house with one slow sensor from holding a reading
# hostage until 99 more arrive.
BATCH_BUFFER_SIZE: Final = 100
BATCH_TIMEOUT_SECONDS: Final = 1.0

# Bounded, and bounded on purpose. A queue that grows without limit during an
# outage turns a 20-minute API blip into an out-of-memory kill of the whole
# Home Assistant process. When it overflows the OLDEST readings go first and a
# counter moves - and the backfill reads them back from the recorder once Tag
# Historian answers again (see forwarder.py). The same bound holds for what is
# saved to .storage when Home Assistant stops, so that file stays under a few
# megabytes however long the outage was.
MAX_QUEUE_LENGTH: Final = 10000

# How many recorded readings one backfill request carries, and how many the
# backfill holds read-ahead of the wire. Recorder reads are local and quick;
# the cap keeps a day-long gap from being one enormous list in memory.
BACKFILL_BATCH_SIZE: Final = 1000

# The recorder is read in windows of this many hours, per entity, for both
# the backfill and the history import: a chatty sensor writes tens of
# thousands of states a day, and one query for a whole fortnight of it would
# be a fortnight of State objects in memory at once.
RECORDER_WINDOW_HOURS: Final = 24

# Readings per history-import request. Below the endpoint's own per-batch
# ceiling (ImportService.MaxReadingsPerBatch, 5 000) so one request stays well
# inside IMPORT_TIMEOUT_SECONDS: the endpoint checks every reading against the
# day files it would land in before it stores anything.
IMPORT_BATCH_SIZE: Final = 2000

# An import batch does more work per request than a live write (the
# duplicate check reads day files), so it gets longer than a write's
# HTTP_TIMEOUT_SECONDS before it is given up on and retried.
IMPORT_TIMEOUT_SECONDS: Final = 30

# How long the import waits after a socket error before it tries the same
# batch again, and how many times in a row it does before it stops - stopped,
# not finished: running the action again carries on where it left off.
IMPORT_RETRY_SECONDS: Final = 30
IMPORT_MAX_CONNECTION_FAILURES: Final = 10

# How long, in waits added up, one batch is sent again while Tag Historian
# answers 429 or 503, and the longest wait between two sends. Its own waits
# are short - 15 seconds for a file that did not read, minutes for a month not
# ready yet, the deploy's grace at most half an hour - so a batch still not
# taken after two hours is not "a moment": the import stops as unreachable and
# carries on at the next start, rather than asking every 15 seconds for good
# (each ask a warning in Tag Historian's log). Between sends the wait doubles,
# with the answer's Retry-After as its floor.
IMPORT_MAX_RETRY_SECONDS: Final = 2 * 60 * 60
IMPORT_MAX_RETRY_WAIT_SECONDS: Final = 15 * 60

# How many loads in a row carry a paused import on by themselves without it
# getting any further (importer.HistoryImporter.async_load). A Tag Historian
# that is down comes back; a batch that fails the same way every time does
# not, and carried on at every start it would be sent for good.
IMPORT_MAX_RESUMES_ON_LOAD: Final = 3

# Hours per request of the statistics phase. ImportService.MaxHoursPerBatch
# on the API side; above it the hours endpoint answers 413. A batch is whole
# UTC months of one entity, so it holds at most six of them.
MAX_HOURS_PER_REQUEST: Final = 5000

# Services. Stable strings: automations call them by name.
SERVICE_IMPORT_HISTORY: Final = "import_history"
ATTR_CONFIG_ENTRY_ID: Final = "config_entry_id"
ATTR_DAYS: Final = "days"
# Whether the import brings Home Assistant's long-term statistics too, before
# the recorder's readings. On unless said otherwise.
ATTR_STATISTICS: Final = "statistics"

# The furthest back an import may be asked to reach, in days: the ten years
# Tag Historian keeps hourly history for. The statistics phase goes no
# further back than the API's own oldestAllowed either way.
MAX_IMPORT_DAYS: Final = 3650

# .storage keys, one of each per config entry. Version bumps here need a
# migration; the key names must not change, or a restart forgets what it was
# holding.
STORAGE_VERSION: Final = 1
STORAGE_KEY_FORWARDER: Final = "tag_historian.{entry_id}.forwarder"
STORAGE_KEY_IMPORT: Final = "tag_historian.{entry_id}.import"

# Dispatcher signal the import sensor listens on.
SIGNAL_IMPORT_UPDATED: Final = "tag_historian_import_updated_{entry_id}"

# Default minimum seconds between two readings of the SAME entity.
#
# Not a plan number: every tier divides to the same ratio (each tier's daily
# reading allowance divided by its tag allowance is 10 000 readings per tag per
# day, i.e. one every 8.64 s), so 10 s is the rounded-up floor that fits any
# plan at full tag count. The figure the config flow actually SHOWS the user is
# computed from their own MeasurementsPerDayLimit and their own selection size,
# not from this constant.
DEFAULT_MIN_INTERVAL_SECONDS: Final = 10

# Quota is a billing surface, not a live metric. Fifteen minutes is often
# enough to catch "you are at your limit" long before the user goes looking.
QUOTA_UPDATE_INTERVAL_MINUTES: Final = 15

HTTP_TIMEOUT_SECONDS: Final = 10

# Repair issue ids. Stable strings - changing one orphans an issue that is
# already showing in somebody's repairs panel.
ISSUE_TAG_QUOTA: Final = "tag_quota_exceeded"
ISSUE_DAILY_QUOTA: Final = "daily_quota_exceeded"
ISSUE_WRITE_FORBIDDEN: Final = "write_forbidden"
ISSUE_SERVICE_UNAVAILABLE: Final = "service_unavailable"
ISSUE_ENTITY_RENAMED: Final = "entity_renamed"
ISSUE_BRING_YEARS: Final = "bring_years"

# How long to wait before asking Tag Historian again to rename a tag after it
# could not be reached. A rename that waits costs nothing - the readings go on
# being stored under the old name meanwhile - so this is unhurried.
RENAME_RETRY_SECONDS: Final = 900

# A single 503 is our problem and none of the user's business. Only after the
# writes have been failing this long does it become something they should see.
SERVICE_UNAVAILABLE_ISSUE_AFTER_SECONDS: Final = 1800

PRICING_URL: Final = "https://taghistorian.com/pricing"
# This integration's own guide: the same page as "documentation" in
# manifest.json, which is the link Home Assistant shows. /docs/home-assistant
# is the guide to the built-in influxdb integration instead.
# tests/test_user_facing_text.py holds the two together.
DOCS_URL: Final = "https://taghistorian.com/docs/home-assistant-hacs"
