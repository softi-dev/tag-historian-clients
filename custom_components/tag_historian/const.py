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
# counter moves - never a silent drop.
MAX_QUEUE_LENGTH: Final = 10000

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

# A single 503 is our problem and none of the user's business. Only after the
# writes have been failing this long does it become something they should see.
SERVICE_UNAVAILABLE_ISSUE_AFTER_SECONDS: Final = 1800

PRICING_URL: Final = "https://taghistorian.com/pricing"
DOCS_URL: Final = "https://taghistorian.com/docs/home-assistant"
