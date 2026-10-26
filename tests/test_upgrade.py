"""An entry set up by an earlier version keeps working after the upgrade.

This release adds the statistics phase to the history import, with fields of
its own in the import's saved progress - and keeps the store's version, which
the forwarder's saved buffer shares (const.STORAGE_VERSION). A version bump
would make Home Assistant's Store refuse both files with no migration to
call: the entry would fail to set up and the buffer would be lost. So these
load version-1 files, written the way the released versions wrote them,
through ``async_setup_entry`` itself - the path an upgraded Home Assistant
takes.

And what the upgrade brings such an entry: a repair card offering the years
Home Assistant already has, raised once, which starts them; a new entry gets
the same from the last screen of its setup instead.
"""

from __future__ import annotations

import gzip
import json
from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import patch

import pytest
from homeassistant.config_entries import ConfigEntryState
from homeassistant.core import HomeAssistant, State
from homeassistant.data_entry_flow import FlowResultType
from homeassistant.helpers import issue_registry as ir
from homeassistant.util import dt as dt_util
from pytest_homeassistant_custom_component.common import (
    MockConfigEntry,
    async_fire_time_changed,
)

from custom_components.tag_historian import importer as importer_module
from custom_components.tag_historian.api import (
    HoursBatchResult,
    ImportAllowance,
    ImportBatchResult,
    ImportOutcome,
    ImportSession,
    TagHistorianConnectionError,
    TagHistorianImportRefused,
    WriteOutcome,
    WriteResult,
)
from custom_components.tag_historian.const import (
    CONF_API_KEY,
    CONF_ENTITIES,
    CONF_HOST,
    CONF_IMPORT_ON_SETUP,
    CONF_MIN_INTERVAL,
    DOMAIN,
    ISSUE_BRING_YEARS,
    STORAGE_VERSION,
)
from custom_components.tag_historian.importer import STATUS_FINISHED
from custom_components.tag_historian.repairs import async_create_fix_flow, issue_id

from .conftest import make_client

CLIENT_PATH = "custom_components.tag_historian.TagHistorianClient"
CONFIG_FLOW_CLIENT_PATH = "custom_components.tag_historian.config_flow.TagHistorianClient"
POWER = "sensor.house_power"
NOW = datetime(2026, 10, 17, 12, 0, tzinfo=UTC)
ENTRY_ID = "upgraded_entry"
FORWARDER_KEY = f"tag_historian.{ENTRY_ID}.forwarder"
IMPORT_KEY = f"tag_historian.{ENTRY_ID}.import"

# A buffer as forwarder.py has saved it since 2026.11.0: a clean stop, two
# readings waiting, the watermark, and the registry ids.
SAVED_BUFFER: dict[str, Any] = {
    "clean": True,
    "sending_ok": False,
    "acked": {POWER: (NOW - timedelta(minutes=30)).timestamp()},
    "queue": [
        [POWER, (NOW - timedelta(minutes=20)).timestamp(), "W,domain=sensor,entity_id=house_power value=1500.0 1792223600", ""],
        [POWER, (NOW - timedelta(minutes=10)).timestamp(), "W,domain=sensor,entity_id=house_power value=1600.0 1792224200", ""],
    ],
    "registry_ids": {},
    "saved_at": (NOW - timedelta(minutes=5)).isoformat(),
    "stopped_at": (NOW - timedelta(minutes=5)).isoformat(),
}

# A finished import as importer.py saved it before the statistics phase: none
# of its fields.
SAVED_IMPORT: dict[str, Any] = {
    "status": "finished",
    "import_id": "imp-2026-10",
    "started_at": (NOW - timedelta(days=9)).isoformat(),
    "window_start": (NOW - timedelta(days=23)).isoformat(),
    "since": (NOW - timedelta(days=23)).isoformat(),
    "entities": [POWER],
    "done": [POWER],
    "current": None,
    "cursor": {POWER: (NOW - timedelta(days=9, hours=1)).timestamp()},
    "ends": {},
    "stored": 1234,
    "compressed": 0,
    "duplicates": 0,
    "outside_window": 0,
    "refused": 0,
    "not_history": 0,
    "refused_tags": [],
    "allowance_remaining": 5000,
    "stop_reason": "",
    "message": "",
    "finished_at": (NOW - timedelta(days=9)).isoformat(),
    "registry_ids": {},
}


def _version_1(key: str, data: dict[str, Any]) -> dict[str, Any]:
    return {"version": 1, "minor_version": 1, "key": key, "data": data}


class FakeStatistics:
    """A month of a sensor's hourly means, and nothing for anything else."""

    def __init__(self) -> None:
        self.rows = [
            {"start": (NOW - timedelta(hours=h)).timestamp(), "mean": 1.0, "min": 0.0, "max": 2.0}
            for h in range(24 * 30, 0, -1)
        ]

    async def metadata(self, hass, statistic_ids):
        from custom_components.tag_historian.statistics import info_from_metadata

        meta = {
            "statistic_id": POWER,
            "source": "recorder",
            "mean_type": 1,
            "has_sum": False,
            "unit_of_measurement": "W",
        }
        return {POWER: info_from_metadata(meta)} if POWER in statistic_ids else {}

    async def read(self, hass, statistic_id, start, end, units, types):
        return [r for r in self.rows if start.timestamp() <= r["start"] < end.timestamp()]


@pytest.fixture(autouse=True)
def recorder_and_statistics(monkeypatch) -> None:
    """A recorder with a month of statistics and no states to read back."""

    async def _no_states(hass, entity_id, start, end) -> list[State]:
        return []

    fake = FakeStatistics()
    monkeypatch.setattr("custom_components.tag_historian.history.recorder_ready", lambda hass: True)
    monkeypatch.setattr("custom_components.tag_historian.history.async_recorded_states", _no_states)
    monkeypatch.setattr("custom_components.tag_historian.statistics.async_metadata", fake.metadata)
    monkeypatch.setattr(
        "custom_components.tag_historian.statistics.async_recorded_statistics", fake.read
    )


@pytest.fixture(autouse=True)
def no_waiting(monkeypatch) -> None:
    async def _sleep(seconds: float) -> None:
        return None

    monkeypatch.setattr(importer_module.asyncio, "sleep", _sleep)


def importing_client() -> Any:
    """make_client, able to import: a session, and every batch taken."""
    client = make_client()
    client.async_write.return_value = WriteResult(WriteOutcome.OK)
    client.async_start_import.return_value = ImportSession(
        import_id="imp-years",
        # The server's clock, a few seconds off the integration's own: the
        # stamp an import leaves is its session's start, never the local now.
        started_at=NOW - timedelta(seconds=7),
        window_start=NOW - timedelta(days=14),
        max_readings_per_batch=5000,
        allowance=ImportAllowance(total=1000, used=0, remaining=1000, readings_per_day=10, window_days=14),
    )
    sent: list[list[dict[str, Any]]] = []

    async def _hours(import_id: str, gzipped: bytes) -> HoursBatchResult:
        series = json.loads(gzip.decompress(gzipped))["series"]
        sent.append(series[0]["hours"])
        return HoursBatchResult(
            ImportOutcome.TAKEN,
            stored=len(series[0]["hours"]),
            importable_before={POWER: NOW},
        )

    client.async_import_hours.side_effect = _hours
    client.async_import_batch.return_value = ImportBatchResult(ImportOutcome.TAKEN)
    client.hours_sent = sent
    return client


def make_entry(hass: HomeAssistant, **options: Any) -> MockConfigEntry:
    entry = MockConfigEntry(
        domain=DOMAIN,
        entry_id=ENTRY_ID,
        title="Test House",
        data={CONF_HOST: "api.taghistorian.com", CONF_API_KEY: "not-a-real-key"},
        options={CONF_ENTITIES: [POWER], CONF_MIN_INTERVAL: 10, **options},
        unique_id="11111111-2222-3333-4444-555555555555",
    )
    entry.add_to_hass(hass)
    return entry


async def test_an_existing_entry_loads_its_version_1_stores_and_keeps_its_buffer(
    hass: HomeAssistant, hass_storage, freezer
) -> None:
    """The upgrade's first start: set up, the saved buffer sent, the import as it was."""
    freezer.move_to(NOW)
    hass_storage[FORWARDER_KEY] = _version_1(FORWARDER_KEY, dict(SAVED_BUFFER))
    hass_storage[IMPORT_KEY] = _version_1(IMPORT_KEY, dict(SAVED_IMPORT))
    entry = make_entry(hass)
    client = importing_client()

    with patch(CLIENT_PATH, return_value=client):
        assert await hass.config_entries.async_setup(entry.entry_id)
        await hass.async_block_till_done()

        assert entry.state is ConfigEntryState.LOADED
        forwarder = entry.runtime_data.forwarder
        assert forwarder.stats.restored == 2, "the buffer the last version saved"
        async_fire_time_changed(hass, dt_util.utcnow() + timedelta(seconds=2))
        await hass.async_block_till_done()
        sent = "\n".join(call.args[0] for call in client.async_write.await_args_list)
        assert "value=1500.0" in sent and "value=1600.0" in sent, "and delivered"

        progress = entry.runtime_data.importer.progress
        assert progress.status == STATUS_FINISHED
        assert progress.stored == 1234
        assert progress.statistics is False
        assert hass_storage[IMPORT_KEY]["version"] == STORAGE_VERSION == 1

        assert await hass.config_entries.async_unload(entry.entry_id)
        await hass.async_block_till_done()

    assert hass_storage[FORWARDER_KEY]["version"] == 1, "saved again under the version it was read as"


async def test_the_offer_to_an_existing_entry_appears_once_and_starts_the_years(
    hass: HomeAssistant, hass_storage, freezer
) -> None:
    freezer.move_to(NOW)
    hass_storage[IMPORT_KEY] = _version_1(IMPORT_KEY, dict(SAVED_IMPORT))
    entry = make_entry(hass)
    client = importing_client()
    registry = ir.async_get(hass)
    card = issue_id(entry, ISSUE_BRING_YEARS)

    with patch(CLIENT_PATH, return_value=client):
        assert await hass.config_entries.async_setup(entry.entry_id)
        await hass.async_block_till_done()

        issue = registry.async_get_issue(DOMAIN, card)
        assert issue is not None and issue.is_fixable
        assert issue.translation_placeholders == {"account": "Test House"}
        client.async_start_import.assert_not_awaited()  # an offer, not a start

        flow = await async_create_fix_flow(hass, issue.issue_id, issue.data)
        flow.hass = hass
        form = await flow.async_step_init()
        assert form["type"] is FlowResultType.FORM
        result = await flow.async_step_confirm({})
        await hass.async_block_till_done(wait_background_tasks=True)

        assert result["type"] is FlowResultType.CREATE_ENTRY
        client.async_start_import.assert_awaited_once_with(hours=True)
        assert client.hours_sent, "the statistics went in"
        progress = entry.runtime_data.importer.progress
        assert progress.statistics_started_at is not None
        assert registry.async_get_issue(DOMAIN, card) is None

        # The next start does not ask again: the years have been brought.
        assert await hass.config_entries.async_reload(entry.entry_id)
        await hass.async_block_till_done()
        assert registry.async_get_issue(DOMAIN, card) is None
        assert client.async_start_import.await_count == 1

        assert await hass.config_entries.async_unload(entry.entry_id)
        await hass.async_block_till_done()


async def test_a_new_entry_left_ticked_brings_the_years_at_its_first_setup_and_only_then(
    hass: HomeAssistant, freezer
) -> None:
    freezer.move_to(NOW)
    entry = make_entry(hass, **{CONF_IMPORT_ON_SETUP: True})
    client = importing_client()

    with patch(CLIENT_PATH, return_value=client):
        assert await hass.config_entries.async_setup(entry.entry_id)
        await hass.async_block_till_done(wait_background_tasks=True)

        client.async_start_import.assert_awaited_once_with(hours=True)
        assert ir.async_get(hass).async_get_issue(DOMAIN, issue_id(entry, ISSUE_BRING_YEARS)) is None

        assert await hass.config_entries.async_reload(entry.entry_id)
        await hass.async_block_till_done(wait_background_tasks=True)
        assert client.async_start_import.await_count == 1, "once brought, not again"

        assert await hass.config_entries.async_unload(entry.entry_id)
        await hass.async_block_till_done()


async def test_a_new_entry_left_unticked_neither_imports_nor_asks(hass: HomeAssistant, freezer) -> None:
    freezer.move_to(NOW)
    entry = make_entry(hass, **{CONF_IMPORT_ON_SETUP: False})
    client = importing_client()

    with patch(CLIENT_PATH, return_value=client):
        assert await hass.config_entries.async_setup(entry.entry_id)
        await hass.async_block_till_done(wait_background_tasks=True)

        client.async_start_import.assert_not_awaited()
        assert ir.async_get(hass).async_get_issue(DOMAIN, issue_id(entry, ISSUE_BRING_YEARS)) is None

        assert await hass.config_entries.async_unload(entry.entry_id)
        await hass.async_block_till_done()


async def test_every_setup_proposes_home_assistants_time_zone(hass: HomeAssistant, freezer) -> None:
    """Disclosed on the setup's last screen; taken only if the account has no zone yet."""
    freezer.move_to(NOW)
    await hass.config.async_update(time_zone="Europe/Stockholm")
    entry = make_entry(hass, **{CONF_IMPORT_ON_SETUP: False})
    client = importing_client()

    with patch(CLIENT_PATH, return_value=client):
        assert await hass.config_entries.async_setup(entry.entry_id)
        await hass.async_block_till_done(wait_background_tasks=True)

        client.async_propose_time_zone.assert_awaited_once_with("Europe/Stockholm")

        assert await hass.config_entries.async_unload(entry.entry_id)
        await hass.async_block_till_done()


def _cannot_take_hours(client: Any) -> None:
    """A Tag Historian from before the hours import: 404 with nothing on the wire."""
    client.async_import_hours.side_effect = None
    client.async_import_hours.return_value = HoursBatchResult(ImportOutcome.NOT_SUPPORTED)


async def test_a_new_entry_left_unticked_is_not_asked_after_changing_its_selection(
    hass: HomeAssistant, freezer
) -> None:
    """The options dialog writes the options anew; the answer to the last screen goes with them."""
    freezer.move_to(NOW)
    hass.states.async_set(
        POWER, "1500", {"device_class": "power", "state_class": "measurement", "unit_of_measurement": "W"}
    )
    entry = make_entry(hass, **{CONF_IMPORT_ON_SETUP: False})
    client = importing_client()

    with patch(CLIENT_PATH, return_value=client), patch(CONFIG_FLOW_CLIENT_PATH, return_value=client):
        assert await hass.config_entries.async_setup(entry.entry_id)
        await hass.async_block_till_done(wait_background_tasks=True)

        result = await hass.config_entries.options.async_init(entry.entry_id)
        result = await hass.config_entries.options.async_configure(
            result["flow_id"], {"next_step_id": "choose"}
        )
        result = await hass.config_entries.options.async_configure(
            result["flow_id"], {CONF_ENTITIES: [POWER], CONF_MIN_INTERVAL: 30}
        )
        result = await hass.config_entries.options.async_configure(result["flow_id"], {})
        # The options listener reloads the entry.
        await hass.async_block_till_done(wait_background_tasks=True)

        assert entry.options[CONF_IMPORT_ON_SETUP] is False
        assert entry.options[CONF_MIN_INTERVAL] == 30, "the change went in, and the entry reloaded"
        assert ir.async_get(hass).async_get_issue(DOMAIN, issue_id(entry, ISSUE_BRING_YEARS)) is None
        client.async_start_import.assert_not_awaited()

        assert await hass.config_entries.async_unload(entry.entry_id)
        await hass.async_block_till_done()


async def test_a_ticked_entry_whose_first_start_failed_tries_again_at_the_next_setup(
    hass: HomeAssistant, freezer
) -> None:
    freezer.move_to(NOW)
    entry = make_entry(hass, **{CONF_IMPORT_ON_SETUP: True})
    client = importing_client()
    session = client.async_start_import.return_value
    client.async_start_import.side_effect = [TagHistorianConnectionError("down"), session]

    with patch(CLIENT_PATH, return_value=client):
        assert await hass.config_entries.async_setup(entry.entry_id)
        await hass.async_block_till_done(wait_background_tasks=True)
        assert client.async_start_import.await_count == 1

        assert await hass.config_entries.async_reload(entry.entry_id)
        await hass.async_block_till_done(wait_background_tasks=True)

        assert client.async_start_import.await_count == 2, "nothing was started, so it is tried again"
        assert client.hours_sent, "and the years went in"
        assert ir.async_get(hass).async_get_issue(DOMAIN, issue_id(entry, ISSUE_BRING_YEARS)) is None

        assert await hass.config_entries.async_unload(entry.entry_id)
        await hass.async_block_till_done()


async def test_an_import_the_last_version_left_unreachable_is_left_as_it_said_at_the_upgrade(
    hass: HomeAssistant, hass_storage, freezer
) -> None:
    """2026.11 called every failure unreachable and said to run it again: not carried on, and the card is offered."""
    freezer.move_to(NOW)
    paused = {**SAVED_IMPORT, "status": "stopped", "stop_reason": "unreachable", "message": "down", "done": []}
    hass_storage[IMPORT_KEY] = _version_1(IMPORT_KEY, paused)
    entry = make_entry(hass)
    client = importing_client()

    with patch(CLIENT_PATH, return_value=client):
        assert await hass.config_entries.async_setup(entry.entry_id)
        await hass.async_block_till_done(wait_background_tasks=True)

        importer = entry.runtime_data.importer
        assert not importer.running
        assert importer.progress.import_id == "imp-2026-10"
        assert importer.progress.status == "stopped"
        client.async_import_batch.assert_not_awaited()
        assert ir.async_get(hass).async_get_issue(DOMAIN, issue_id(entry, ISSUE_BRING_YEARS)) is not None

        assert await hass.config_entries.async_unload(entry.entry_id)
        await hass.async_block_till_done()


async def test_the_offer_comes_back_when_the_tag_historian_could_not_take_the_years_yet(
    hass: HomeAssistant, hass_storage, freezer
) -> None:
    """Answered against a Tag Historian without the hours import, none of the years were brought."""
    freezer.move_to(NOW)
    hass_storage[IMPORT_KEY] = _version_1(IMPORT_KEY, dict(SAVED_IMPORT))
    entry = make_entry(hass)
    client = importing_client()
    _cannot_take_hours(client)
    registry = ir.async_get(hass)
    card = issue_id(entry, ISSUE_BRING_YEARS)

    with patch(CLIENT_PATH, return_value=client):
        assert await hass.config_entries.async_setup(entry.entry_id)
        await hass.async_block_till_done()
        issue = registry.async_get_issue(DOMAIN, card)
        flow = await async_create_fix_flow(hass, issue.issue_id, issue.data)
        flow.hass = hass
        await flow.async_step_confirm({})
        await hass.async_block_till_done(wait_background_tasks=True)
        assert entry.runtime_data.importer.progress.statistics_started_at is None

        assert await hass.config_entries.async_reload(entry.entry_id)
        await hass.async_block_till_done(wait_background_tasks=True)
        assert registry.async_get_issue(DOMAIN, card) is not None, "offered again"

        # Answered again once Tag Historian takes them: brought, and not offered any more.
        fixed = importing_client()
        entry.runtime_data.importer._client = fixed
        flow = await async_create_fix_flow(hass, issue.issue_id, issue.data)
        flow.hass = hass
        await flow.async_step_confirm({})
        await hass.async_block_till_done(wait_background_tasks=True)
        assert fixed.hours_sent
        assert entry.runtime_data.importer.progress.statistics_started_at is not None
        assert registry.async_get_issue(DOMAIN, card) is None

        assert await hass.config_entries.async_unload(entry.entry_id)
        await hass.async_block_till_done()


async def test_a_ticked_entry_that_met_a_tag_historian_without_the_hours_import_is_offered_them(
    hass: HomeAssistant, freezer
) -> None:
    """Not started anew at every restart: from then on the card asks, and the user says when."""
    freezer.move_to(NOW)
    entry = make_entry(hass, **{CONF_IMPORT_ON_SETUP: True})
    client = importing_client()
    _cannot_take_hours(client)

    with patch(CLIENT_PATH, return_value=client):
        assert await hass.config_entries.async_setup(entry.entry_id)
        await hass.async_block_till_done(wait_background_tasks=True)
        client.async_start_import.assert_awaited_once_with(hours=True)
        assert entry.runtime_data.importer.progress.statistics_started_at is None

        assert await hass.config_entries.async_reload(entry.entry_id)
        await hass.async_block_till_done(wait_background_tasks=True)

        assert client.async_start_import.await_count == 1, "not started again by itself"
        assert ir.async_get(hass).async_get_issue(DOMAIN, issue_id(entry, ISSUE_BRING_YEARS)) is not None

        assert await hass.config_entries.async_unload(entry.entry_id)
        await hass.async_block_till_done()


async def test_a_refused_start_from_the_card_says_why_and_leaves_the_card(
    hass: HomeAssistant, hass_storage, freezer
) -> None:
    freezer.move_to(NOW)
    hass_storage[IMPORT_KEY] = _version_1(IMPORT_KEY, dict(SAVED_IMPORT))
    entry = make_entry(hass)
    client = importing_client()
    client.async_start_import.side_effect = TagHistorianImportRefused(
        ImportOutcome.ALLOWANCE_SPENT, "Import allowance used up."
    )
    registry = ir.async_get(hass)
    card = issue_id(entry, ISSUE_BRING_YEARS)

    with patch(CLIENT_PATH, return_value=client):
        assert await hass.config_entries.async_setup(entry.entry_id)
        await hass.async_block_till_done()
        issue = registry.async_get_issue(DOMAIN, card)
        flow = await async_create_fix_flow(hass, issue.issue_id, issue.data)
        flow.hass = hass
        result = await flow.async_step_confirm({})

        assert result["type"] is FlowResultType.ABORT
        assert result["reason"] == "import_refused"
        assert result["description_placeholders"] == {"message": "Import allowance used up."}
        assert registry.async_get_issue(DOMAIN, card) is not None, "for another try"

        assert await hass.config_entries.async_unload(entry.entry_id)
        await hass.async_block_till_done()
