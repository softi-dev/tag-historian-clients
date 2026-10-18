"""Copy Home Assistant history out of InfluxDB 1.x or 2.x into Tag Historian.

Run ``taghistorian-influx-migrate --help``, and read README.md first: the
order in which the copy and the switch of Home Assistant's live data happen
is what decides whether the history arrives whole.
"""

__version__ = "0.1.0"
