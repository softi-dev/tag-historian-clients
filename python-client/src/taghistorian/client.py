"""The synchronous Tag Historian API client.

Synchronous and ``requests``-based on purpose: the target user is a cron
job, a quick script, or a PLC bridge polling on its own timer - not an
asyncio application. The Home Assistant integration elsewhere in this
repository is async because Home Assistant's integration model requires it;
nothing here has that constraint, and adding an async variant nobody asked
for would just double the surface to maintain.

A NOTE ON RETRYING WRITES: ``write``/``write_batch`` are POSTs, and are not
idempotent on the server - two identical calls create two stored
measurements (or, for values inside the compression deadband, two
compression decisions), not one. This client retries a 429/503 (both mean
"try again later", not "something is wrong with your data") and a
transient network error automatically, within the bounds of
:class:`taghistorian.retry.RetryConfig`. For 429/503 this is safe: the
server never accepted the request in the first place. For a network error
it is a judgement call - the request *might* have been accepted and only
the response was lost - which is exactly why the retry budget defaults to a
small, finite number and is fully callable-configurable rather than a silent
infinite loop; a caller for whom an occasional duplicate sample is worse
than a dropped one should pass ``RetryConfig(max_retries=0)`` and handle
:class:`~taghistorian.exceptions.TagHistorianConnectionError` themselves.
"""

from __future__ import annotations

import re
from datetime import datetime
from types import TracebackType
from urllib.parse import quote

import requests

from ._time import serialize_timestamp
from .exceptions import (
    TagHistorianAPIError,
    TagHistorianConnectionError,
    TagHistorianRateLimitError,
)
from .models import (
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

__all__ = ["TagHistorianClient"]

_DEFAULT_BASE_URL = "https://api.taghistorian.com"

# The server's own limit (MeasurementsController.PostMeasurementBatch: "Batch
# size exceeds maximum of 1000 measurements"), not a client guess. Checking it
# here is the one client-side validation this package does, and the reason is
# BEFORE a network call, not auto-chunk-and-retry: MeasurementIngestionService
# commits a batch atomically (all rows or none, see its own comments on
# reservation and rollback) and the response describes that single atomic
# operation (totalCount/storedCount/processingTimeMs for the call that was
# made). Silently splitting an over-limit call into several smaller ones would
# turn one atomic operation the caller is reasoning about into several
# non-atomic ones, and there would be no single honest response left to
# hand back - a synthetic merge of N responses is not what the server did.
# Raising up front means the caller decides how to split their own batch,
# with full knowledge of what "batch 3 of 5 failed" would mean for them.
_MAX_BATCH_SIZE = 1000

_CONTENT_DISPOSITION_FILENAME_RE = re.compile(r"filename\*?=(?:UTF-8'')?\"?([^\";]+)\"?")


class TagHistorianClient:
    """Client for the Tag Historian HTTP API.

    :param api_key: Sent as the ``X-Api-Key`` header on every request. Its
        scope determines what the key can do: Read maps to Viewer (every
        read method here), Write maps to Editor (also required for
        ``write``/``write_batch``/``create_tag``), Admin maps to Admin.
    :param base_url: Overridable for self-hosted deployments or staging;
        defaults to the public API.
    :param timeout: Per-request timeout in seconds, passed straight to
        ``requests``.
    :param retry: Retry/backoff policy; see :class:`taghistorian.retry.RetryConfig`.
        Defaults to a fresh ``RetryConfig()`` when omitted.
    :param session: An existing ``requests.Session`` to use instead of
        creating one (mainly for tests). The client owns whatever session it
        ends up with and closes it in ``close()``/on context-manager exit.

    Use as a context manager to make sure the underlying connection pool is
    closed::

        with TagHistorianClient(api_key="...") as client:
            client.write("boiler.temperature", 84.2)
    """

    def __init__(
        self,
        api_key: str,
        base_url: str = _DEFAULT_BASE_URL,
        *,
        timeout: float = 10.0,
        retry: RetryConfig | None = None,
        session: requests.Session | None = None,
    ) -> None:
        if not api_key:
            raise ValueError("api_key is required")

        self._base_url = base_url.rstrip("/")
        self._timeout = timeout
        self._retry = retry if retry is not None else RetryConfig()
        self._session = session if session is not None else requests.Session()
        self._session.headers.update({"X-Api-Key": api_key, "Accept": "application/json"})

        # Fetched once, lazily, on the first read call - see _customer_id().
        self._customer_id: str | None = None

    # -- context manager -------------------------------------------------

    def __enter__(self) -> TagHistorianClient:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()

    def close(self) -> None:
        """Close the underlying ``requests.Session``."""
        self._session.close()

    # -- writes ------------------------------------------------------------

    def write(
        self,
        tag_name: str,
        value: float,
        *,
        status: str | None = None,
        timestamp: datetime | None = None,
        units: str | None = None,
        description: str | None = None,
    ) -> WriteResult:
        """Write a single measurement. Requires a Write-scope (or higher) key.

        :param timestamp: When the measurement occurred. ``None`` (the
            default) means "now", resolved server-side - the same as
            omitting the field entirely (confirmed against
            ``MeasurementsController.TryCreateMeasurement``, which defaults
            an absent timestamp to ``DateTime.UtcNow``). A naive
            ``datetime`` is assumed to already be UTC; see
            ``taghistorian._time`` for why.
        """
        body = self._measurement_body(tag_name, value, status, timestamp, units, description)
        response = self._request("POST", "/api/measurements", json_body=body)
        return WriteResult._from_json(response.json())

    def write_batch(self, measurements: list[MeasurementInput]) -> BatchWriteResult:
        """Write up to 1000 measurements in one call. Requires Write scope.

        :raises ValueError: if more than 1000 measurements are passed - the
            server's exact limit (see the module-level comment on
            ``_MAX_BATCH_SIZE`` for why this raises instead of
            auto-chunking). Raised before any network call is made.
        """
        if len(measurements) > _MAX_BATCH_SIZE:
            raise ValueError(
                f"write_batch received {len(measurements)} measurements, but the server "
                f"accepts at most {_MAX_BATCH_SIZE} per call. Split this into multiple "
                f"write_batch() calls yourself - see _MAX_BATCH_SIZE's comment in client.py "
                f"for why this client does not auto-chunk a batch that size."
            )

        body = {
            "measurements": [
                self._measurement_body(
                    m.tag_name, m.value, m.status, m.timestamp, m.units, m.description
                )
                for m in measurements
            ]
        }
        response = self._request("POST", "/api/measurements/batch", json_body=body)
        return BatchWriteResult._from_json(response.json())

    @staticmethod
    def _measurement_body(
        tag_name: str,
        value: float,
        status: str | None,
        timestamp: datetime | None,
        units: str | None,
        description: str | None,
    ) -> dict:
        body: dict = {"tagName": tag_name, "value": value}
        if status is not None:
            body["status"] = status
        ts = serialize_timestamp(timestamp)
        if ts is not None:
            body["timestamp"] = ts
        if units is not None:
            body["units"] = units
        if description is not None:
            body["description"] = description
        return body

    # -- reads ---------------------------------------------------------

    def read(
        self,
        tag_name: str,
        *,
        from_: datetime | None = None,
        to: datetime | None = None,
        limit: int | None = None,
    ) -> ReadResult:
        """Raw measurements for ``tag_name`` in ``[from_, to]``.

        ``from_``/``to`` default to the last 24h server-side when omitted.
        ``limit`` is clamped server-side to 10000.
        """
        customer_id = self._customer_id_cached()
        params = self._range_params(from_, to)
        if limit is not None:
            params["limit"] = limit

        path = f"/api/measurements/{customer_id}/{quote(tag_name, safe='')}"
        response = self._request("GET", path, params=params)
        return ReadResult._from_json(response.json())

    def read_last(self, tag_name: str) -> Measurement:
        """The most recent stored value for ``tag_name``."""
        customer_id = self._customer_id_cached()
        path = f"/api/measurements/{customer_id}/{quote(tag_name, safe='')}/last"
        response = self._request("GET", path)
        return Measurement._from_json(response.json())

    def read_aggregated(
        self,
        tag_name: str,
        *,
        from_: datetime | None = None,
        to: datetime | None = None,
        interval: str = "10m",
        aggregation: str = "Average",
        interpolation: str = "Linear",
        max_results: int | None = None,
    ) -> AggregatedReadResult:
        """Aggregated/interpolated measurements for ``tag_name``.

        :param interval: e.g. ``"10m"``, ``"1h"``, ``"5s"``, ``"1d"``.
        :param aggregation: One of ``Average``, ``Minimum``, ``Maximum``,
            ``Sum``, ``Count``.
        :param interpolation: One of ``None``, ``Linear``, ``StepForward``.
            Only a request hint: read
            ``result.interpolation_type`` for what actually happened. The
            archive query path (long ranges answered from pre-aggregated
            hourly data) always reports back ``"None"`` regardless of what
            was requested here - re-bucketing an already-summarised hour
            aggregate is not interpolation, and the response says so
            honestly rather than echoing the request.
        """
        customer_id = self._customer_id_cached()
        params = self._range_params(from_, to)
        params["interval"] = interval
        params["aggregation"] = aggregation
        params["interpolation"] = interpolation
        if max_results is not None:
            params["maxResults"] = max_results

        path = f"/api/measurements/{customer_id}/{quote(tag_name, safe='')}/aggregated"
        response = self._request("GET", path, params=params)
        return AggregatedReadResult._from_json(response.json())

    @staticmethod
    def _range_params(from_: datetime | None, to: datetime | None) -> dict:
        params: dict = {}
        if from_ is not None:
            params["from"] = serialize_timestamp(from_)
        if to is not None:
            params["to"] = serialize_timestamp(to)
        return params

    # -- tags ------------------------------------------------------------

    def list_tags(
        self,
        *,
        skip: int = 0,
        take: int = 100,
        search: str | None = None,
        site: str | None = None,
        area: str | None = None,
        equipment: str | None = None,
    ) -> TagListResult:
        """Paginated tag list. ``take`` is clamped server-side to 1000."""
        params: dict = {"skip": skip, "take": take}
        if search is not None:
            params["search"] = search
        if site is not None:
            params["site"] = site
        if area is not None:
            params["area"] = area
        if equipment is not None:
            params["equipment"] = equipment

        response = self._request("GET", "/api/tags", params=params)
        return TagListResult._from_json(response.json())

    def create_tag(
        self,
        tag_name: str,
        *,
        description: str | None = None,
        units: str | None = None,
        tag_type: str | None = None,
        enable_compression: bool | None = None,
        compression_deadband: float | None = None,
        scale_min: float | None = None,
        scale_max: float | None = None,
    ) -> Tag:
        """Create a tag. Requires Admin scope - unlike every other write in
        this client, a Write-scope key gets a 403 here. Tag creation is the
        one schema-changing operation the API reserves for Admin; if you only
        need a tag to exist before writing to it, :meth:`write` and
        :meth:`write_batch` create one implicitly on first write and only need
        Write scope.

        :param tag_type: ``"Analog"`` or ``"Discrete"``.
        :param scale_min: Configured chart axis lower bound. Must be
            supplied together with ``scale_max`` or not at all - the server
            enforces this (400 naming both fields) and this client
            deliberately does not duplicate that check; see the module
            docstring on why client-side validation here stays minimal.
        """
        body: dict = {"tagName": tag_name}
        if description is not None:
            body["description"] = description
        if units is not None:
            body["units"] = units
        if tag_type is not None:
            body["tagType"] = tag_type
        if enable_compression is not None:
            body["enableCompression"] = enable_compression
        if compression_deadband is not None:
            body["compressionDeadband"] = compression_deadband
        if scale_min is not None:
            body["scaleMin"] = scale_min
        if scale_max is not None:
            body["scaleMax"] = scale_max

        response = self._request("POST", "/api/tags", json_body=body)
        return Tag._from_json(response.json())

    # -- alerts --------------------------------------------------------------
    #
    # Unlike the measurement endpoints above, neither alert endpoint takes a
    # customerId path segment - AlertsController resolves the caller's
    # customer from the authenticated HttpContext, not a URL parameter (see
    # AlertsController.GetCustomerId()). So no _customer_id_cached() call
    # happens here; there is nothing for it to be cached for.

    def list_active_alerts(self) -> list[Alert]:
        """Currently firing alerts (state ``Triggered`` or ``Acknowledged``,
        never ``Resolved``) for the authenticated customer. Requires only
        Read scope - this is a status query, not a mutation.

        Returns the raw ``Alert`` entity list, not a wrapper - matching what
        ``AlertsController.GetActiveAlerts`` actually returns (see the
        docstring on :class:`taghistorian.models.Alert` for why, unlike alert
        rules, there is no separate response DTO here).
        """
        response = self._request("GET", "/api/alerts/active")
        return [Alert._from_json(a) for a in response.json()]

    def list_alert_rules(self) -> list[AlertRule]:
        """Every alert rule configured for the authenticated customer,
        whether currently enabled or not. Requires only Read scope.

        The webhook signing secret, if one is set, never appears here -
        ``has_webhook_secret`` is a bool, not the secret itself (see the
        docstring on :class:`taghistorian.models.AlertRule`).
        """
        response = self._request("GET", "/api/alerts/rules")
        return [AlertRule._from_json(r) for r in response.json()]

    # -- export ------------------------------------------------------------

    def export_measurements(
        self,
        tag_name: str,
        *,
        from_: datetime | None = None,
        to: datetime | None = None,
        format: str = "Json",  # matches the API's own query param name
        limit: int | None = None,
        path: str | None = None,
    ) -> ExportResult:
        """Export everything stored for ``tag_name``, or a date range of it.

        Omit both ``from_``/``to`` to export everything ever stored for the
        tag. ``format`` is one of ``"Json"``, ``"Csv"``, ``"Parquet"``.

        :param path: If given, the export is streamed straight to this file
            path and ``ExportResult.content`` is ``None`` - use this for a
            large export so it never has to sit fully in memory. If omitted,
            the full response body is returned as ``ExportResult.content``.
        """
        params = self._range_params(from_, to)
        params["format"] = format
        if limit is not None:
            params["limit"] = limit

        stream = path is not None
        response = self._request(
            "GET",
            f"/api/export/measurements/{quote(tag_name, safe='')}",
            params=params,
            stream=stream,
        )

        filename = self._filename_from_response(response, tag_name, format)
        content_type = response.headers.get("Content-Type", "application/octet-stream")

        if path is not None:
            with open(path, "wb") as f:
                for chunk in response.iter_content(chunk_size=65536):
                    f.write(chunk)
            return ExportResult(
                filename=filename, content_type=content_type, content=None, path=str(path)
            )

        return ExportResult(
            filename=filename, content_type=content_type, content=response.content, path=None
        )

    @staticmethod
    def _filename_from_response(response: requests.Response, tag_name: str, format: str) -> str:
        header = response.headers.get("Content-Disposition")
        if header:
            match = _CONTENT_DISPOSITION_FILENAME_RE.search(header)
            if match:
                return match.group(1)
        return f"{tag_name}.{format.lower()}"

    # -- customer id -----------------------------------------------------

    def _customer_id_cached(self) -> str:
        """The caller's own customer id, fetched once and cached for the
        life of this client instance.

        Every GET measurement endpoint carries the caller's own customerId
        as a URL segment (the server 403s on a mismatch - see
        ``MeasurementsController``'s ``Forbid()`` check), so a user of this
        library never has to know or paste their own account GUID; this is
        what makes that true.
        """
        if self._customer_id is None:
            response = self._request("GET", "/api/customers/me")
            self._customer_id = response.json()["customerId"]
        return self._customer_id

    # -- request/retry core ------------------------------------------------

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict | None = None,
        json_body: dict | None = None,
        stream: bool = False,
    ) -> requests.Response:
        url = f"{self._base_url}{path}"
        attempt = 0

        while True:
            attempt += 1
            try:
                response = self._session.request(
                    method,
                    url,
                    params=params,
                    json=json_body,
                    timeout=self._timeout,
                    stream=stream,
                )
            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as exc:
                if attempt <= self._retry.max_retries:
                    self._retry.sleep(self._retry.backoff_delay(attempt))
                    continue
                raise TagHistorianConnectionError(
                    f"Could not reach {url} after {attempt} attempt(s): {exc}",
                    attempts=attempt,
                ) from exc
            except requests.exceptions.RequestException as exc:
                # Not a transient condition (bad URL, too many redirects,
                # etc.) - retrying identical input would not help, but this
                # is still "never reached the server", not an HTTP-level
                # rejection, so it is a connection error, raised immediately.
                raise TagHistorianConnectionError(
                    f"Could not reach {url}: {exc}", attempts=attempt
                ) from exc

            if response.status_code < 300:
                return response

            if response.status_code in (429, 503):
                retry_after = self._parse_retry_after(response)
                if attempt <= self._retry.max_retries:
                    delay = (
                        retry_after
                        if retry_after is not None
                        else self._retry.backoff_delay(attempt)
                    )
                    # The body is never read on this branch - no .json()/.text
                    # call between here and the retry - so without an explicit
                    # close() the connection stays checked out of the pool
                    # until garbage collection gets to it. Harmless for a
                    # normal JSON call (small body, GC runs soon), but
                    # export_measurements() sets stream=True, and a rate-limited
                    # or saturated download hitting this branch would otherwise
                    # hold a real connection open for the life of the retry
                    # loop for no reason - we are about to fire a brand new
                    # request on the same session either way.
                    response.close()
                    self._retry.sleep(delay)
                    continue
                message, _ = self._error_details(response)
                raise TagHistorianRateLimitError(
                    message, response.status_code, retry_after, attempt
                )

            # Every other non-2xx is refused on its merits: 400/401/402/403/404,
            # or a 5xx that isn't the buffer-saturated 503 above. Never retried.
            message, violations = self._error_details(response)
            raise TagHistorianAPIError(message, response.status_code, violations)

    @staticmethod
    def _parse_retry_after(response: requests.Response) -> float | None:
        value = response.headers.get("Retry-After")
        if value is None:
            return None
        try:
            return float(value)
        except ValueError:
            return None

    @staticmethod
    def _error_details(response: requests.Response) -> tuple[str, list | None]:
        try:
            body = response.json()
        except ValueError:
            return response.text or f"HTTP {response.status_code}", None

        if isinstance(body, dict):
            message = body.get("error") or response.text or f"HTTP {response.status_code}"
            violations = body.get("violations")
            return message, violations

        return response.text or f"HTTP {response.status_code}", None
