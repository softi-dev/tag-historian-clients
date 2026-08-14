"""Renders ``taghistorian`` client results as text an LLM can read directly.

Plain formatted text, not JSON-dumped dataclasses: an LLM tool result is read
by the model, not parsed by a program, and prose like
"boiler.temperature is currently 84.2 C (Good), as of 2026-08-14T09:03:00Z"
is both more token-efficient and easier for the model to summarise back to a
user than the equivalent JSON blob would be. (FastMCP still attaches a
`structuredContent` block alongside the text automatically for any tool that
returns a plain value - see this SDK's own tool-call handler - so nothing
machine-readable is lost by choosing text as the primary content here.)
"""

from __future__ import annotations

from taghistorian import (
    AggregatedReadResult,
    Alert,
    AlertRule,
    BatchWriteResult,
    Measurement,
    ReadResult,
    Tag,
    TagListResult,
    WriteResult,
)


def _ts(value) -> str:
    return value.isoformat().replace("+00:00", "Z")


def format_tag_list(result: TagListResult) -> str:
    if not result.tags:
        return "No tags matched (total on this page: 0)."

    lines = [f"{result.total_count} tag(s) total, showing {len(result.tags)}:"]
    for tag in result.tags:
        last = (
            f"last={tag.last_value} ({tag.last_status}) at {_ts(tag.last_stored_timestamp)}"
            if tag.last_stored_timestamp is not None
            else "no data yet"
        )
        lines.append(
            f"- {tag.tag_name} [{tag.tag_type or 'unknown type'}] units={tag.units or '-'} {last}"
        )
    return "\n".join(lines)


def format_measurement(tag_name: str, m: Measurement) -> str:
    return f"{tag_name} = {m.value} {m.status or ''} (tier={m.storage_tier}) at {_ts(m.timestamp)}".replace(
        "  ", " "
    )


def format_read_result(result: ReadResult) -> str:
    if not result.measurements:
        return (
            f"No measurements for {result.tag_name} between {_ts(result.from_)} "
            f"and {_ts(result.to)}."
        )

    header = (
        f"{result.count} measurement(s) for {result.tag_name} "
        f"between {_ts(result.from_)} and {_ts(result.to)}:"
    )
    lines = [f"  {_ts(m.timestamp)}  {m.value}  {m.status}" for m in result.measurements]
    return "\n".join([header, *lines])


def format_aggregated_result(result: AggregatedReadResult) -> str:
    if not result.measurements:
        return (
            f"No aggregated data for {result.tag_name} between {_ts(result.from_)} "
            f"and {_ts(result.to)} at interval {result.interval}."
        )

    header = (
        f"{result.count} {result.aggregation_type.lower()} bucket(s) for {result.tag_name}, "
        f"interval={result.interval}, interpolation={result.interpolation_type} "
        f"(requested/actual - see the note on read_aggregated if these differ):"
    )
    lines = []
    for bucket in result.measurements:
        span = f"{_ts(bucket.interval_start)}..{_ts(bucket.interval_end)}"
        minmax = (
            f" min={bucket.min_value} max={bucket.max_value}"
            if bucket.min_value is not None or bucket.max_value is not None
            else ""
        )
        lines.append(f"  {span}  value={bucket.value}{minmax} n={bucket.count}")
    return "\n".join([header, *lines])


def format_active_alerts(alerts: list[Alert]) -> str:
    if not alerts:
        return "No active alerts. Nothing is currently firing."

    lines = [f"{len(alerts)} active alert(s):"]
    for a in alerts:
        ack = f", acknowledged at {_ts(a.acknowledged_at)}" if a.acknowledged_at else ""
        lines.append(
            f"- [{a.severity}] {a.rule_name} on {a.tag_name}: {a.message} "
            f"(value={a.value}, threshold={a.threshold}) state={a.state}, "
            f"triggered at {_ts(a.triggered_at)}{ack}"
        )
    return "\n".join(lines)


def format_alert_rules(rules: list[AlertRule]) -> str:
    if not rules:
        return "No alert rules are configured."

    lines = [f"{len(rules)} alert rule(s):"]
    for r in rules:
        status = "enabled" if r.is_enabled else "disabled"
        webhook = "with webhook" if r.webhook_url else "no webhook"
        secret = " (signed)" if r.has_webhook_secret else ""
        last = f", last triggered {_ts(r.last_triggered_at)}" if r.last_triggered_at else ""
        lines.append(
            f"- {r.name} [{status}] on {r.tag_name}: {r.condition} {r.threshold}"
            + (f"/{r.threshold2}" if r.threshold2 is not None else "")
            + f", severity={r.severity}, cooldown={r.cooldown_seconds}s, {webhook}{secret}{last}"
        )
    return "\n".join(lines)


def format_write_result(tag_name: str, result: WriteResult) -> str:
    compressed = " (compressed away - within the deadband)" if result.was_compressed else ""
    return (
        f"Wrote {tag_name} to Tag Historian: {result.message} "
        f"[tier={result.storage_tier}]{compressed} at {_ts(result.timestamp)}"
    )


def format_batch_write_result(result: BatchWriteResult) -> str:
    return (
        f"Batch write: {result.stored_count}/{result.total_count} stored "
        f"({result.compressed_count} compressed away), {result.processing_time_ms}ms."
    )


def format_tag(tag: Tag) -> str:
    return (
        f"Created/updated tag {tag.tag_name} [{tag.tag_type or 'unknown type'}], "
        f"units={tag.units or '-'}, compression={'on' if tag.enable_compression else 'off'}"
        + (f" (deadband={tag.compression_deadband})" if tag.enable_compression else "")
    )
