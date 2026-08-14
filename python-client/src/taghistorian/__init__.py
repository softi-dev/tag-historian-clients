"""Synchronous Python client for the Tag Historian time-series API.

    from taghistorian import TagHistorianClient

    with TagHistorianClient(api_key="...") as client:
        client.write("boiler.temperature", 84.2)

See README.md for a fuller quickstart.
"""

from .client import TagHistorianClient
from .exceptions import (
    TagHistorianAPIError,
    TagHistorianConnectionError,
    TagHistorianError,
    TagHistorianRateLimitError,
)
from .models import (
    AggregatedMeasurement,
    AggregatedReadResult,
    Alert,
    AlertRule,
    BatchWriteResult,
    ExportResult,
    Measurement,
    MeasurementInput,
    ReadResult,
    Tag,
    TagListResult,
    WriteResult,
)
from .retry import RetryConfig

__version__ = "0.1.0"

__all__ = [
    "AggregatedMeasurement",
    "AggregatedReadResult",
    "Alert",
    "AlertRule",
    "BatchWriteResult",
    "ExportResult",
    "Measurement",
    "MeasurementInput",
    "ReadResult",
    "RetryConfig",
    "Tag",
    "TagHistorianAPIError",
    "TagHistorianClient",
    "TagHistorianConnectionError",
    "TagHistorianError",
    "TagHistorianRateLimitError",
    "TagListResult",
    "WriteResult",
]
