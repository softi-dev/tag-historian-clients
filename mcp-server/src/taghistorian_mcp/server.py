"""Tool definitions and the write gate.

Every tool function here is a thin translation layer: parse/validate MCP
arguments -> call one ``taghistorian`` client method -> format the result as
text. None of them talk HTTP directly - that stays entirely inside
``taghistorian.TagHistorianClient`` (``python-client/``), which is also
where retry/backoff and error-shape decisions already live and are already
tested. Duplicating any of that here would be the exact
re-implement-the-HTTP-calls mistake this package is built specifically to
avoid.

THE WRITE GATE: ``build_server`` only calls ``@mcp.tool()`` on
``write_measurement``/``write_batch_measurements``/``create_tag`` when
``config.enable_write`` is true. This is an ``if`` around the registration
call, not a check inside the tool body - FastMCP's ``list_tools()`` (what an
MCP host calls to build its tool picker, and what an LLM sees when deciding
what it can do) only ever lists tools that were actually registered. A tool
that existed-but-refused would still tell an LLM "there is a way to write
here, it's just blocked right now"; that is a materially different, weaker
signal than the tool simply not existing, which is why registration-time
gating was chosen over call-time refusal.
"""

from __future__ import annotations

import logging
from typing import Annotated, Literal

from mcp.server.fastmcp import FastMCP
from pydantic import BaseModel, Field
from taghistorian import MeasurementInput, TagHistorianClient

from .config import ServerConfig
from .errors import handle_client_errors
from .formatting import (
    format_active_alerts,
    format_aggregated_result,
    format_alert_rules,
    format_batch_write_result,
    format_measurement,
    format_read_result,
    format_tag,
    format_tag_list,
    format_write_result,
)
from .timeparse import parse_optional_timestamp

logger = logging.getLogger("taghistorian_mcp")

# The server's own limit that TagHistorianClient.write_batch enforces
# client-side before ever making a network call (see _MAX_BATCH_SIZE's
# comment in client.py). Repeated here only for the tool's own parameter
# description, so an LLM sees the cap before it tries to build an
# over-sized call - the actual enforcement still happens once, in the
# client, via the ValueError that errors.handle_client_errors turns into a
# clean ToolError.
_MAX_BATCH_SIZE = 1000


class MeasurementItem(BaseModel):
    """One row of a ``write_batch_measurements`` call. Mirrors
    ``taghistorian.MeasurementInput`` field-for-field; a separate pydantic
    model exists only because that's what gives FastMCP a real per-item JSON
    Schema to hand the LLM (see ``func_metadata`` in the ``mcp`` SDK) -
    ``MeasurementInput`` itself is a plain dataclass with no schema
    generation of its own.
    """

    tag_name: Annotated[str, Field(description="The tag to write to.")]
    value: Annotated[float, Field(description="The numeric value to store.")]
    timestamp: Annotated[
        str | None,
        Field(
            description=(
                "ISO 8601 timestamp (e.g. '2026-08-14T09:00:00Z'). Omit to use "
                "the server's current time."
            )
        ),
    ] = None
    status: Annotated[
        str | None, Field(description="Quality status, e.g. 'Good' or 'Bad'. Omit for the default.")
    ] = None
    units: Annotated[str | None, Field(description="Units for this value, e.g. 'C' or 'bar'.")] = (
        None
    )
    description: Annotated[str | None, Field(description="Optional free-text note.")] = None

    def to_client_input(self) -> MeasurementInput:
        return MeasurementInput(
            tag_name=self.tag_name,
            value=self.value,
            status=self.status,
            timestamp=parse_optional_timestamp(self.timestamp, param_name="timestamp"),
            units=self.units,
            description=self.description,
        )


def build_server(client: TagHistorianClient, config: ServerConfig) -> FastMCP:
    """Assemble the FastMCP server and register its tools against ``client``.

    Taking ``client`` as a parameter (rather than constructing one from
    ``config`` internally) is what makes this testable without a real API
    key or network access: tests pass a mock/fake client in and assert on
    what tools get called with what arguments, never touching ``requests``.
    """

    mcp = FastMCP(
        name="taghistorian",
        instructions=(
            "Query Tag Historian, an industrial/IoT time-series historian: list "
            "tags, read raw or aggregated measurement history, and check active "
            "alerts and alert rule configuration. All read tools are always "
            "available. Tools that write data only appear when the server "
            "operator has explicitly enabled them - if you don't see a write "
            "tool, writing is disabled for this connection, not merely blocked."
        ),
    )

    _register_read_tools(mcp, client)

    if config.enable_write:
        _register_write_tools(mcp, client)

    return mcp


def _register_read_tools(mcp: FastMCP, client: TagHistorianClient) -> None:
    @mcp.tool()
    @handle_client_errors
    def list_tags(
        search: Annotated[
            str | None, Field(description="Filter to tag names containing this text.")
        ] = None,
        site: Annotated[str | None, Field(description="Filter to this site.")] = None,
        area: Annotated[str | None, Field(description="Filter to this area.")] = None,
        equipment: Annotated[str | None, Field(description="Filter to this equipment.")] = None,
        skip: Annotated[
            int, Field(description="Number of tags to skip, for pagination.", ge=0)
        ] = 0,
        take: Annotated[
            int, Field(description="Max tags to return (server clamps to 1000).", ge=1)
        ] = 100,
    ) -> str:
        """List the customer's tags, optionally filtered by name/site/area/equipment.
        Read-only - does not create or change anything."""
        result = client.list_tags(
            skip=skip, take=take, search=search, site=site, area=area, equipment=equipment
        )
        return format_tag_list(result)

    # NOTE on "from_" rather than "from" in every read-range tool below: this
    # mirrors taghistorian.TagHistorianClient's own parameter name (from is a
    # Python keyword, so the client can't call it that either - see client.py).
    # A pydantic Field(alias="from") looked like the fix for the tool-schema
    # name, but this SDK version's argument dispatch passes the alias straight
    # through as the Python kwarg (confirmed empirically: it raised
    # "read_measurements() got an unexpected keyword argument 'from'"), so
    # aliasing here would break every call rather than just rename a schema
    # field. "from_" in the schema is a minor wart an LLM has no trouble with;
    # a broken tool is not an acceptable trade for a prettier field name.
    @mcp.tool()
    @handle_client_errors
    def read_measurements(
        tag_name: Annotated[str, Field(description="The tag to read.")],
        from_: Annotated[
            str | None,
            Field(
                description="ISO 8601 start of the range, e.g. '2026-08-14T00:00:00Z'. Omit for the last 24h.",
            ),
        ] = None,
        to: Annotated[
            str | None, Field(description="ISO 8601 end of the range. Omit for 'now'.")
        ] = None,
        limit: Annotated[
            int | None, Field(description="Max rows to return (server clamps to 10000).", ge=1)
        ] = None,
    ) -> str:
        """Read a tag's raw stored values over a time range. Read-only.
        Use read_aggregated instead for long ranges or chart-style summaries -
        raw data over weeks/months can be very large."""
        result = client.read(
            tag_name,
            from_=parse_optional_timestamp(from_, param_name="from"),
            to=parse_optional_timestamp(to, param_name="to"),
            limit=limit,
        )
        return format_read_result(result)

    @mcp.tool()
    @handle_client_errors
    def read_last_value(
        tag_name: Annotated[str, Field(description="The tag to read.")],
    ) -> str:
        """The single most recent stored value for a tag. Read-only. Use this
        for "what is X right now" questions instead of read_measurements with
        a tiny range - it's one direct lookup, not a range query."""
        m = client.read_last(tag_name)
        return format_measurement(tag_name, m)

    @mcp.tool()
    @handle_client_errors
    def read_aggregated(
        tag_name: Annotated[str, Field(description="The tag to read.")],
        from_: Annotated[
            str | None,
            Field(description="ISO 8601 start of the range. Omit for the last 24h."),
        ] = None,
        to: Annotated[
            str | None, Field(description="ISO 8601 end of the range. Omit for 'now'.")
        ] = None,
        interval: Annotated[str, Field(description="Bucket size, e.g. '10m', '1h', '1d'.")] = "10m",
        aggregation: Annotated[
            Literal["Average", "Minimum", "Maximum", "Sum", "Count"],
            Field(description="How to summarise each bucket."),
        ] = "Average",
        interpolation: Annotated[
            Literal["None", "Linear", "StepForward"],
            Field(
                description=(
                    "Interpolation hint for gaps. Long ranges are answered from "
                    "pre-aggregated hourly data and always come back as 'None' "
                    "regardless of what was requested - the response says which "
                    "actually happened."
                )
            ),
        ] = "Linear",
        max_results: Annotated[
            int | None, Field(description="Cap on the number of buckets returned.", ge=1)
        ] = None,
    ) -> str:
        """Averaged/min/max/summed/counted values over an interval - the right
        tool for "what has X been doing over the last day/week" style
        questions, and much more compact than read_measurements for long
        ranges. Read-only."""
        result = client.read_aggregated(
            tag_name,
            from_=parse_optional_timestamp(from_, param_name="from"),
            to=parse_optional_timestamp(to, param_name="to"),
            interval=interval,
            aggregation=aggregation,
            interpolation=interpolation,
            max_results=max_results,
        )
        return format_aggregated_result(result)

    @mcp.tool()
    @handle_client_errors
    def list_active_alerts() -> str:
        """What's currently firing: every alert in state Triggered or
        Acknowledged (never Resolved) for this customer. Read-only - does not
        acknowledge or resolve anything. Good for "are there any active
        alarms" style questions."""
        alerts = client.list_active_alerts()
        return format_active_alerts(alerts)

    @mcp.tool()
    @handle_client_errors
    def list_alert_rules() -> str:
        """Every configured alert rule (enabled or not) and its thresholds -
        what would fire, not what is firing right now (use list_active_alerts
        for that). Read-only. Webhook signing secrets, if configured, are
        never exposed here - only whether one is set."""
        rules = client.list_alert_rules()
        return format_alert_rules(rules)


def _register_write_tools(mcp: FastMCP, client: TagHistorianClient) -> None:
    @mcp.tool()
    @handle_client_errors
    def write_measurement(
        tag_name: Annotated[
            str,
            Field(
                description=(
                    "The tag to write to. If it doesn't exist yet, it is created "
                    "implicitly by this write (within the customer's plan limit)."
                )
            ),
        ],
        value: Annotated[float, Field(description="The numeric value to store.")],
        timestamp: Annotated[
            str | None,
            Field(
                description=(
                    "ISO 8601 timestamp for this value. Omit to use the server's "
                    "current time - the normal choice for a live reading."
                )
            ),
        ] = None,
        status: Annotated[
            str | None, Field(description="Quality status, e.g. 'Good' or 'Bad'.")
        ] = None,
        units: Annotated[str | None, Field(description="Units, e.g. 'C' or 'bar'.")] = None,
        description: Annotated[str | None, Field(description="Optional free-text note.")] = None,
    ) -> str:
        """Write ONE real measurement to the customer's Tag Historian account.
        This is a genuine, billable write against their stored data and tag
        quota - it is not a preview, dry run, or sandbox action, and it is not
        reversible through this tool. Only available because the server
        operator explicitly enabled write tools for this connection."""
        result = client.write(
            tag_name,
            value,
            status=status,
            timestamp=parse_optional_timestamp(timestamp, param_name="timestamp"),
            units=units,
            description=description,
        )
        return format_write_result(tag_name, result)

    # No max_length=1000 constraint on the field below, even though the cap
    # is real: TagHistorianClient.write_batch already enforces it (see
    # _MAX_BATCH_SIZE in client.py) and raises a ValueError with a message
    # written for a human before any network call happens. Adding a second,
    # separate enforcement here would mean two places to keep in sync if the
    # server's real limit ever changes, and - worse - two different error
    # shapes depending on which check fires first. One source of truth for
    # the limit; this tool's description just tells the LLM about it, and
    # handle_client_errors is what turns the client's ValueError into a clean
    # tool error if a caller ignores that and sends too many anyway.
    @mcp.tool()
    @handle_client_errors
    def write_batch_measurements(
        measurements: Annotated[
            list[MeasurementItem],
            Field(description=f"Up to {_MAX_BATCH_SIZE} measurements to write in one atomic call."),
        ],
    ) -> str:
        """Write MULTIPLE real measurements to the customer's Tag Historian
        account in one call. Like write_measurement, this is a genuine,
        billable write against their stored data and tag quota, not a
        preview or dry run - it just does several at once, atomically (all
        rows are stored, or none are). More than 1000 measurements in one
        call is rejected outright; split a larger batch into multiple calls
        yourself. Only available because the server operator explicitly
        enabled write tools for this connection."""
        inputs = [m.to_client_input() for m in measurements]
        result = client.write_batch(inputs)
        return format_batch_write_result(result)

    @mcp.tool()
    @handle_client_errors
    def create_tag(
        tag_name: Annotated[str, Field(description="The new tag's name.")],
        description: Annotated[str | None, Field(description="Free-text description.")] = None,
        units: Annotated[str | None, Field(description="Units, e.g. 'C' or 'bar'.")] = None,
        tag_type: Annotated[
            Literal["Analog", "Discrete"] | None, Field(description="The tag's value type.")
        ] = None,
        enable_compression: Annotated[
            bool | None, Field(description="Whether to enable deadband compression.")
        ] = None,
        compression_deadband: Annotated[
            float | None, Field(description="Deadband width, if compression is enabled.")
        ] = None,
        scale_min: Annotated[
            float | None,
            Field(description="Chart axis lower bound. Must be given together with scale_max."),
        ] = None,
        scale_max: Annotated[
            float | None,
            Field(description="Chart axis upper bound. Must be given together with scale_min."),
        ] = None,
    ) -> str:
        """Register a real new tag against the customer's Tag Historian
        account, counting against their tag quota - this is a genuine write,
        not a preview. Only needed to set metadata (units, compression, a
        chart axis range) up front; otherwise the first write_measurement for
        a new tag name creates it implicitly. Only available because the
        server operator explicitly enabled write tools for this connection."""
        tag = client.create_tag(
            tag_name,
            description=description,
            units=units,
            tag_type=tag_type,
            enable_compression=enable_compression,
            compression_deadband=compression_deadband,
            scale_min=scale_min,
            scale_max=scale_max,
        )
        return format_tag(tag)
