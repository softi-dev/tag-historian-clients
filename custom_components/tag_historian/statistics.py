"""Read Home Assistant's long-term statistics, for the history import's first phase.

Home Assistant keeps two kinds of history. The recorder's states - every
reading - are purged after ten days by default (history.py reads those). Its
LONG-TERM STATISTICS are kept for as long as the database lives: one row per
hour per sensor, with the hour's mean, minimum and maximum for a measurement,
or the meter's reading, running sum and change for a meter. They are
summaries, not readings, and Tag Historian keeps them as such: in its hour
tier, marked as Home Assistant's statistics wherever they are read, under the
tag the entity's readings go to (``POST /api/import/{id}/hours``). That is
how the years before the integration was installed come along.

THREE CHOICES IN HERE ARE LOAD-BEARING:

* What a statistic is comes from its metadata (``get_metadata``): a meter
  ("has_sum") is read as state, sum and change, anything with a mean as mean,
  min and max. The kind of mean is ``mean_type`` - arithmetic, or circular for
  an angle such as a wind direction, which Tag Historian does not take - and
  ``has_mean`` on a Home Assistant from before ``mean_type``. Home Assistant
  2026.11 removes ``has_mean``, so it is only ever the fallback.

* The values are sent in the unit the entity READS IN NOW, because that is
  the unit its tag has from the live readings, and Tag Historian refuses a
  whole series in another unit. Home Assistant converts its statistics into
  any unit of the same kind (kWh into Wh, °F into °C), so the conversion is
  asked for only when its converter takes the entity's unit; otherwise the
  entity is left out, and the import's summary says why.

* ``statistics_during_period`` is the recorder's own query and runs in the
  recorder's executor, one calendar month at a time - about 30 days, 744
  hours at most - so a decade of a sensor is never in memory at once. It is
  internal to Home Assistant rather than a published API; the README names
  the ``recorder.get_statistics`` action as the way to read the same rows
  should it change.

And one entity is not always one statistic id: an entity renamed onto an id
that had a history of its own keeps its statistics from before the rename
under its former id, as it keeps its states (history.py). :func:`async_rows`
reads them from both, split where the entity moved.
"""

from __future__ import annotations

import gzip
import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from homeassistant.core import HomeAssistant

from . import history
from .line_protocol import MAX_FRIENDLY_NAME_LENGTH, MAX_UNIT_LENGTH, _attribute_text

# What one hour of each kind carries, as POST /api/import/{id}/hours reads it.
MEAN_TYPES = {"mean", "min", "max"}
SUM_TYPES = {"state", "sum", "change"}

KIND_MEAN = "mean"
KIND_SUM = "sum"

HOUR_SECONDS = 3600


@dataclass(frozen=True)
class StatisticInfo:
    """What Home Assistant's metadata says about one statistic id."""

    statistic_id: str
    source: str
    has_sum: bool
    # "arithmetic", "circular" or "none" - the API's spelling of mean_type.
    mean_type: str
    statistics_unit: str | None
    unit_class: str | None = None

    @property
    def kind(self) -> str | None:
        """How its hours are read: a meter's sum, a mean - or None, for nothing Tag Historian takes."""
        if self.has_sum:
            return KIND_SUM
        if self.mean_type == "arithmetic":
            return KIND_MEAN
        return None


@dataclass(frozen=True)
class EntityStatistics:
    """How one entity's statistics are read and sent, or why they are not.

    ``reason`` is set when nothing is sent, in words for the import's summary;
    ``missing`` when there is simply nothing - no long-term statistics at all.
    """

    entity_id: str
    info: StatisticInfo | None = None
    # The statistic ids to read: the entity's own, and for one renamed onto
    # an id with a history of its own, its former one up to ``until``.
    source: history.Source | None = None
    # The unit the values are sent in, and the conversion that gets them there.
    units: str | None = None
    convert: dict[str, str] | None = None
    state_class: str | None = None
    friendly_name: str | None = None
    missing: bool = False
    reason: str = ""


def _mean_type(meta: dict[str, Any]) -> str:
    """mean_type as the API spells it; has_mean on a Home Assistant from before mean_type."""
    if "mean_type" in meta and meta["mean_type"] is not None:
        value = meta["mean_type"]
        # StatisticMeanType is an IntEnum: NONE 0, ARITHMETIC 1, CIRCULAR 2.
        number = int(value.value if hasattr(value, "value") else value)
        return {1: "arithmetic", 2: "circular"}.get(number, "none")
    return "arithmetic" if meta.get("has_mean") else "none"


def info_from_metadata(meta: dict[str, Any]) -> StatisticInfo:
    """A statistic's metadata (StatisticMetaData) as this integration reads it."""
    return StatisticInfo(
        statistic_id=str(meta.get("statistic_id") or ""),
        source=str(meta.get("source") or "recorder"),
        has_sum=bool(meta.get("has_sum")),
        mean_type=_mean_type(meta),
        statistics_unit=meta.get("unit_of_measurement"),
        unit_class=meta.get("unit_class"),
    )


async def async_metadata(hass: HomeAssistant, statistic_ids: set[str]) -> dict[str, StatisticInfo]:
    """The metadata of these statistic ids, those that have any, read in the recorder's executor."""
    if not history.recorder_ready(hass) or not statistic_ids:
        return {}

    from homeassistant.components.recorder import get_instance
    from homeassistant.components.recorder import statistics as recorder_statistics

    found = await get_instance(hass).async_add_executor_job(
        lambda: recorder_statistics.get_metadata(hass, statistic_ids=set(statistic_ids))
    )
    return {statistic_id: info_from_metadata(meta) for statistic_id, (_, meta) in found.items()}


def _converter(info: StatisticInfo) -> Any:
    """Home Assistant's unit converter for this statistic's unit, or None when there is none.

    By the statistic's unit class where its metadata has one (Home Assistant
    2025.10 on) and the class takes its unit, else by the unit alone - the
    same order the recorder itself looks them up in. Both tables are read off
    the module, so a Home Assistant without the newer one still works.
    """
    from homeassistant.components.recorder import statistics as recorder_statistics

    unit = info.statistics_unit
    by_class = getattr(recorder_statistics, "UNIT_CLASS_TO_UNIT_CONVERTER", {})
    converter = by_class.get(info.unit_class) if info.unit_class else None
    if converter is not None and unit in converter.VALID_UNITS:
        return converter
    return getattr(recorder_statistics, "STATISTIC_UNIT_TO_UNIT_CONVERTER", {}).get(unit)


def plan_units(info: StatisticInfo, entity_unit: str | None) -> tuple[str | None, dict[str, str] | None, str]:
    """The unit to send in, the conversion asked of Home Assistant, and why it cannot be done.

    The third value is empty unless the entity is to be left out.

    The unit is asked for by name whenever a converter takes it, even when it
    is the statistics' own: asked for nothing, Home Assistant converts each
    read into the unit the read id's live state has at that moment - which
    need not be the unit the series is sent as. A state that appeared after
    the plan, or a former id with no state at all, was sent in the wrong
    unit, a thousand times off for kWh read as Wh.
    """
    statistics_unit = info.statistics_unit
    target = entity_unit if entity_unit is not None else statistics_unit
    converter = _converter(info)
    if converter is not None and target in converter.VALID_UNITS:
        return target, {converter.UNIT_CLASS: target}, ""
    if target == statistics_unit:
        return statistics_unit, None, ""

    return (
        None,
        None,
        f"its long-term statistics are in {statistics_unit or 'no unit'} and it reads in "
        f"{entity_unit} now, which Home Assistant cannot convert",
    )


def _arrives_in(info: StatisticInfo, units: str | None, convert: dict[str, str] | None) -> bool:
    """Whether this statistic's rows come back in ``units``: stored in it, or converted to it as asked."""
    if info.statistics_unit == units:
        return True
    converter = _converter(info)
    return (
        converter is not None
        and convert is not None
        and convert.get(converter.UNIT_CLASS) == units
    )


async def async_plan(hass: HomeAssistant, source: history.Source) -> EntityStatistics:
    """How this entity's long-term statistics are to be read, or why they are not."""
    entity_id = source.entity_id
    ids = {entity_id} | ({source.former_id} if source.former_id else set())
    metadata = await async_metadata(hass, ids)
    info = metadata.get(entity_id) or (metadata.get(source.former_id) if source.former_id else None)
    if info is None:
        return EntityStatistics(entity_id, missing=True)

    if info.kind is None:
        if info.mean_type == "circular":
            return EntityStatistics(
                entity_id,
                info,
                reason="its long-term statistics are an angle's average, which Tag Historian does not take",
            )
        return EntityStatistics(entity_id, info, missing=True)

    state = hass.states.get(entity_id)
    attributes = state.attributes if state is not None else {}
    entity_unit = attributes.get("unit_of_measurement")
    units, convert, reason = plan_units(info, entity_unit if isinstance(entity_unit, str) else None)
    if reason:
        return EntityStatistics(entity_id, info, reason=reason)

    # A former id whose statistics are not the same kind as the entity's, or
    # cannot be read in the unit the series is sent in, are not its history
    # to send; read only the entity's own then.
    former = metadata.get(source.former_id) if source.former_id else None
    if former is not None and (former.kind != info.kind or not _arrives_in(former, units, convert)):
        source = history.Source(entity_id)

    # The name and the unit as the live readings carry them (line_protocol):
    # the API takes them only for a tag that has none yet.
    state_class = attributes.get("state_class")
    return EntityStatistics(
        entity_id,
        info,
        source=source,
        units=_attribute_text(units, MAX_UNIT_LENGTH),
        convert=convert,
        state_class=str(state_class) if state_class else None,
        friendly_name=_attribute_text(attributes.get("friendly_name"), MAX_FRIENDLY_NAME_LENGTH),
    )


def month_start(moment: datetime) -> datetime:
    """The start of the UTC month ``moment`` is in."""
    moment = moment.astimezone(UTC)
    return moment.replace(day=1, hour=0, minute=0, second=0, microsecond=0)


def previous_month(month: datetime) -> datetime:
    """The start of the UTC month before the one starting at ``month``."""
    return month_start(month - timedelta(days=1))


def next_month(month: datetime) -> datetime:
    """The start of the UTC month after the one starting at ``month``."""
    return month_start(month + timedelta(days=32))


async def async_rows(
    hass: HomeAssistant, plan: EntityStatistics, start: datetime, end: datetime
) -> list[dict[str, Any]]:
    """The entity's hours that START in [start, end), oldest first, as the hours endpoint takes them.

    For an entity renamed onto an id with a history of its own, the hours up
    to the rename come from its former id and the rest from its own, as
    history.async_states reads its states.
    """
    source = plan.source or history.Source(plan.entity_id)
    if source.former_id is None or source.until is None:
        return await _async_rows(hass, plan, source.entity_id, start, end)
    before = await _async_rows(hass, plan, source.former_id, start, min(end, source.until))
    after = await _async_rows(hass, plan, source.entity_id, max(start, source.until), end)
    return before + after


async def _async_rows(
    hass: HomeAssistant, plan: EntityStatistics, statistic_id: str, start: datetime, end: datetime
) -> list[dict[str, Any]]:
    """One statistic id's hours that start in [start, end): what async_recorded_statistics read, encoded."""
    if end <= start:
        return []
    rows = await async_recorded_statistics(
        hass,
        statistic_id,
        start,
        end,
        plan.convert,
        SUM_TYPES if plan.info is not None and plan.info.has_sum else MEAN_TYPES,
    )
    first, last = start.timestamp(), end.timestamp()
    hours: list[dict[str, Any]] = []
    for row in rows:
        stamp = _stamp(row.get("start"))
        if stamp is None or not first <= stamp < last:
            continue
        if (hour := _hour(stamp, row, plan.info is not None and plan.info.has_sum)) is not None:
            hours.append(hour)
    return hours


def _stamp(value: Any) -> float | None:
    """A row's start as epoch seconds: a float from Home Assistant 2023.3 on, a datetime before."""
    if isinstance(value, datetime):
        return value.timestamp()
    if isinstance(value, int | float):
        return float(value)
    return None


def _hour(stamp: float, row: dict[str, Any], has_sum: bool) -> dict[str, Any] | None:
    """One row as one hour of the hours endpoint, or None when it lacks what that kind needs.

    A meter's hour is its reading at the end of the hour (state), its running
    sum and its change in the hour - Home Assistant's own increase, which
    already knows about resets. A measurement's is its mean, minimum and
    maximum. Missing values are not filled in: such an hour is not sent.
    """
    if has_sum:
        if row.get("state") is None or row.get("change") is None:
            return None
        return {
            "t": int(stamp),
            "state": row["state"],
            "sum": row.get("sum"),
            "change": row["change"],
        }
    if row.get("mean") is None or row.get("min") is None or row.get("max") is None:
        return None
    return {"t": int(stamp), "mean": row["mean"], "min": row["min"], "max": row["max"]}


async def async_recorded_statistics(
    hass: HomeAssistant,
    statistic_id: str,
    start: datetime,
    end: datetime,
    units: dict[str, str] | None,
    types: set[str],
) -> list[dict[str, Any]]:
    """The recorder's hourly statistics of one id in [start, end), in its executor - the one place it is asked."""
    if not history.recorder_ready(hass):
        return []

    from homeassistant.components.recorder import get_instance
    from homeassistant.components.recorder import statistics as recorder_statistics

    found = await get_instance(hass).async_add_executor_job(
        lambda: recorder_statistics.statistics_during_period(
            hass, start, end, {statistic_id}, "hour", units, set(types)
        )
    )
    return list(found.get(statistic_id, []))


def series(plan: EntityStatistics, tag: str, hours: list[dict[str, Any]]) -> dict[str, Any]:
    """One series of an hours batch: the tag, what the statistic is, and its hours."""
    info = plan.info
    body: dict[str, Any] = {
        "tagName": tag,
        "units": plan.units,
        "statistic": {
            "statisticId": info.statistic_id if info else plan.entity_id,
            "source": info.source if info else "recorder",
            "stateClass": plan.state_class,
            "meanType": info.mean_type if info else None,
            "hasSum": bool(info and info.has_sum),
            "statisticsUnit": info.statistics_unit if info else None,
        },
        "hours": hours,
    }
    if plan.friendly_name:
        body["description"] = plan.friendly_name
    return body


def compress(body: dict[str, Any]) -> bytes:
    """A batch as the gzipped JSON the hours endpoint reads. Called in an executor."""
    return gzip.compress(json.dumps(body, separators=(",", ":")).encode("utf-8"))
