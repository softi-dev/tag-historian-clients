"""Typed shapes for everything this client sends and receives.

Dataclasses rather than plain dicts, and rather than attrs/pydantic: the
API's wire shapes are flat and already known ahead of time (they are pinned
by the C# controllers this client is generated against), so there is nothing
here that needs pydantic's runtime validation or attrs' extra machinery.
``dataclasses`` is the standard-library answer to "a fixed set of typed
fields with autocomplete", which is exactly what this is for - it means a
caller who types ``result.stroed_count`` gets an ``AttributeError`` at the
point of the typo instead of a ``KeyError`` three lines later, or worse, a
silently-``None``-shaped bug from ``result.get("storedCount")``.

Field names are snake_case (Python convention) even though the wire is
camelCase (ASP.NET Core's default); the ``_from_json`` classmethods on each
type are the one place that translation happens, so it never leaks into
calling code.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

from ._time import parse_timestamp


@dataclass(frozen=True)
class WriteResult:
    """Response to a single :meth:`TagHistorianClient.write` call."""

    success: bool
    was_compressed: bool
    storage_tier: str
    message: str
    timestamp: datetime
    compression_ratio: float

    @classmethod
    def _from_json(cls, data: dict) -> WriteResult:
        return cls(
            success=data["success"],
            was_compressed=data["wasCompressed"],
            storage_tier=data.get("storageTier", ""),
            message=data.get("message", ""),
            timestamp=parse_timestamp(data["timestamp"]),
            compression_ratio=data.get("compressionRatio", 0.0),
        )


@dataclass(frozen=True)
class BatchWriteResult:
    """Response to :meth:`TagHistorianClient.write_batch`."""

    total_count: int
    stored_count: int
    compressed_count: int
    processing_time_ms: int
    results: list[WriteResult] = field(default_factory=list)

    @classmethod
    def _from_json(cls, data: dict) -> BatchWriteResult:
        return cls(
            total_count=data["totalCount"],
            stored_count=data["storedCount"],
            compressed_count=data["compressedCount"],
            processing_time_ms=data["processingTimeMs"],
            results=[WriteResult._from_json(r) for r in data.get("results", [])],
        )


@dataclass(frozen=True)
class MeasurementInput:
    """One row of a :meth:`TagHistorianClient.write_batch` call.

    Mirrors the fields :meth:`TagHistorianClient.write` accepts as keyword
    arguments; batching just needs them packaged one-per-row instead.
    """

    tag_name: str
    value: float
    status: str | None = None
    timestamp: datetime | None = None
    units: str | None = None
    description: str | None = None


@dataclass(frozen=True)
class Measurement:
    """One stored data point, as returned by ``read``/``read_last``."""

    timestamp: datetime
    value: float
    status: str
    storage_tier: str

    @classmethod
    def _from_json(cls, data: dict) -> Measurement:
        return cls(
            timestamp=parse_timestamp(data["timestamp"]),
            value=data["value"],
            status=data.get("status", ""),
            storage_tier=data.get("storageTier", ""),
        )


@dataclass(frozen=True)
class ReadResult:
    """Response to :meth:`TagHistorianClient.read`."""

    customer_id: str
    tag_name: str
    from_: datetime
    to: datetime
    count: int
    measurements: list[Measurement] = field(default_factory=list)

    @classmethod
    def _from_json(cls, data: dict) -> ReadResult:
        return cls(
            customer_id=data["customerId"],
            tag_name=data["tagName"],
            from_=parse_timestamp(data["from"]),
            to=parse_timestamp(data["to"]),
            count=data["count"],
            measurements=[Measurement._from_json(m) for m in data.get("measurements", [])],
        )


@dataclass(frozen=True)
class AggregatedMeasurement:
    """One bucket of an aggregated query result."""

    interval_start: datetime
    interval_end: datetime
    value: float
    min_value: float | None
    max_value: float | None
    count: int
    status: str | None

    @classmethod
    def _from_json(cls, data: dict) -> AggregatedMeasurement:
        return cls(
            interval_start=parse_timestamp(data["intervalStart"]),
            interval_end=parse_timestamp(data["intervalEnd"]),
            value=data["value"],
            min_value=data.get("minValue"),
            max_value=data.get("maxValue"),
            count=data["count"],
            status=data.get("status"),
        )


@dataclass(frozen=True)
class AggregatedReadResult:
    """Response to :meth:`TagHistorianClient.read_aggregated`.

    ``interpolation_type`` echoes whatever the server actually did, which is
    not always what was requested: the archive query path always answers
    "None" regardless of the ``interpolation`` argument (see the docstring on
    :meth:`TagHistorianClient.read_aggregated`). Read this field rather than
    assuming the request value held.
    """

    customer_id: str
    tag_name: str
    from_: datetime
    to: datetime
    interval: str
    aggregation_type: str
    interpolation_type: str
    count: int
    measurements: list[AggregatedMeasurement] = field(default_factory=list)

    @classmethod
    def _from_json(cls, data: dict) -> AggregatedReadResult:
        return cls(
            customer_id=data["customerId"],
            tag_name=data["tagName"],
            from_=parse_timestamp(data["from"]),
            to=parse_timestamp(data["to"]),
            interval=data.get("interval", ""),
            aggregation_type=data.get("aggregationType", ""),
            interpolation_type=data.get("interpolationType", ""),
            count=data["count"],
            measurements=[
                AggregatedMeasurement._from_json(m) for m in data.get("measurements", [])
            ],
        )


@dataclass(frozen=True)
class Tag:
    """A tag's metadata, as returned by ``list_tags`` and ``create_tag``."""

    tag_name: str
    description: str
    units: str
    tag_type: str
    enable_compression: bool
    compression_deadband: float
    total_measurements: int
    compressed_measurements: int
    compression_ratio: float
    min_value: float | None = None
    max_value: float | None = None
    scale_min: float | None = None
    scale_max: float | None = None
    last_value: float | None = None
    last_status: str | None = None
    last_stored_timestamp: datetime | None = None

    @classmethod
    def _from_json(cls, data: dict) -> Tag:
        last_ts = data.get("lastStoredTimestamp") or data.get("lastTimestamp")
        return cls(
            tag_name=data["tagName"],
            description=data.get("description", ""),
            units=data.get("units", ""),
            tag_type=data.get("tagType", ""),
            enable_compression=data.get("enableCompression", False),
            compression_deadband=data.get("compressionDeadband", 0.0),
            total_measurements=data.get("totalMeasurements", 0),
            compressed_measurements=data.get("compressedMeasurements", 0),
            compression_ratio=data.get("compressionRatio", 0.0),
            min_value=data.get("minValue"),
            max_value=data.get("maxValue"),
            scale_min=data.get("scaleMin"),
            scale_max=data.get("scaleMax"),
            last_value=data.get("lastValue"),
            last_status=data.get("lastStatus"),
            last_stored_timestamp=parse_timestamp(last_ts) if last_ts else None,
        )


@dataclass(frozen=True)
class TagListResult:
    """Response to :meth:`TagHistorianClient.list_tags`."""

    total_count: int
    skip: int
    take: int
    tags: list[Tag] = field(default_factory=list)

    @classmethod
    def _from_json(cls, data: dict) -> TagListResult:
        return cls(
            total_count=data["totalCount"],
            skip=data["skip"],
            take=data["take"],
            tags=[Tag._from_json(t) for t in data.get("tags", [])],
        )


@dataclass(frozen=True)
class Alert:
    """A triggered alert, as returned by :meth:`TagHistorianClient.list_active_alerts`.

    This is the raw ``Alert`` entity (``AlertsController.GetActiveAlerts`` returns
    it directly, unlike ``AlertRuleResponse`` below) - there is no secret field on
    this shape to strip, so no DTO translation happens server-side and none is
    needed here either.
    """

    alert_id: str
    rule_id: str
    customer_id: str
    tag_name: str
    rule_name: str
    message: str
    value: float
    threshold: float
    severity: str
    state: str
    triggered_at: datetime
    acknowledged_at: datetime | None = None
    resolved_at: datetime | None = None

    @classmethod
    def _from_json(cls, data: dict) -> Alert:
        return cls(
            alert_id=data["alertId"],
            rule_id=data["ruleId"],
            customer_id=data["customerId"],
            tag_name=data["tagName"],
            rule_name=data.get("ruleName", ""),
            message=data.get("message", ""),
            value=data["value"],
            threshold=data["threshold"],
            severity=data.get("severity", ""),
            state=data.get("state", ""),
            triggered_at=parse_timestamp(data["triggeredAt"]),
            acknowledged_at=(
                parse_timestamp(data["acknowledgedAt"]) if data.get("acknowledgedAt") else None
            ),
            resolved_at=(parse_timestamp(data["resolvedAt"]) if data.get("resolvedAt") else None),
        )


@dataclass(frozen=True)
class AlertRule:
    """An alert rule's definition, as returned by :meth:`TagHistorianClient.list_alert_rules`.

    This is ``AlertRuleResponse`` on the wire, not the server's internal
    alert-rule entity - ``has_webhook_secret`` is the only trace of a
    configured webhook secret that ever leaves the API; the server never
    serializes the secret itself, so there is no field here for it to land in.
    """

    rule_id: str
    customer_id: str
    tag_name: str
    name: str
    condition: str
    threshold: float
    has_webhook_secret: bool
    is_enabled: bool
    cooldown_seconds: int
    severity: str
    created_at: datetime
    description: str | None = None
    threshold2: float | None = None
    webhook_url: str | None = None
    last_triggered_at: datetime | None = None

    @classmethod
    def _from_json(cls, data: dict) -> AlertRule:
        return cls(
            rule_id=data["ruleId"],
            customer_id=data["customerId"],
            tag_name=data["tagName"],
            name=data.get("name", ""),
            description=data.get("description"),
            condition=data.get("condition", ""),
            threshold=data["threshold"],
            threshold2=data.get("threshold2"),
            webhook_url=data.get("webhookUrl"),
            has_webhook_secret=data.get("hasWebhookSecret", False),
            is_enabled=data.get("isEnabled", True),
            cooldown_seconds=data.get("cooldownSeconds", 300),
            severity=data.get("severity", ""),
            created_at=parse_timestamp(data["createdAt"]),
            last_triggered_at=(
                parse_timestamp(data["lastTriggeredAt"]) if data.get("lastTriggeredAt") else None
            ),
        )


@dataclass(frozen=True)
class ExportResult:
    """Response to :meth:`TagHistorianClient.export_measurements`.

    Exactly one of ``content``/``path`` is populated, matching which mode
    the call was made in: pass ``path=`` to stream the export straight to
    disk (``content`` is then ``None``, so a large export never has to sit
    in memory twice), or omit it to get the raw bytes back.
    """

    filename: str
    content_type: str
    content: bytes | None
    path: str | None
