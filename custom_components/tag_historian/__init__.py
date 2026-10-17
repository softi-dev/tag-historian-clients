"""The Tag Historian integration.

Three loops share one HTTP client:

    push    - async_track_state_change_event on exactly the selected entities,
              buffered, flushed to POST /api/v2/write; the buffer is saved
              across a restart, and gaps are read back from the recorder
              (see forwarder.py)
    poll    - a 15-minute DataUpdateCoordinator over GET /api/usage/limits,
              which is where every quota number the UI shows comes from
              (see coordinator.py)
    import  - on request only: the recorder's history of the selected
              entities, sent to POST /api/import (see importer.py), started
              from the options dialog or the tag_historian.import_history
              action

and one listener, for the whole integration rather than any one entry: the
entity registry, so an entity renamed in Home Assistant is followed rather than
silently dropped (see renames.py).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import voluptuous as vol
from homeassistant.config_entries import ConfigEntry, ConfigEntryState
from homeassistant.const import ATTR_ENTITY_ID, Platform
from homeassistant.core import HomeAssistant, ServiceCall
from homeassistant.exceptions import HomeAssistantError, ServiceValidationError
from homeassistant.helpers import config_validation as cv
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.helpers.storage import Store
from homeassistant.helpers.typing import ConfigType

from . import history
from .api import (
    TagHistorianAuthError,
    TagHistorianClient,
    TagHistorianConnectionError,
    TagHistorianImportRefused,
)
from .const import (
    ATTR_CONFIG_ENTRY_ID,
    ATTR_DAYS,
    CONF_API_KEY,
    CONF_ENTITIES,
    CONF_HOST,
    CONF_MIN_INTERVAL,
    DEFAULT_MIN_INTERVAL_SECONDS,
    DOMAIN,
    SERVICE_IMPORT_HISTORY,
    STORAGE_KEY_FORWARDER,
    STORAGE_KEY_IMPORT,
    STORAGE_VERSION,
)
from .coordinator import QuotaCoordinator
from .forwarder import ForwarderStats, StateForwarder
from .importer import HistoryImporter, ImportAlreadyRunning
from .renames import (
    async_clear_renamed_issues,
    async_resume,
    async_setup_rename_tracking,
    send_as,
)
from .repairs import async_clear_write_issues

PLATFORMS: list[Platform] = [Platform.SENSOR]

# Set up from the UI only; async_setup below exists for the import action and
# the rename listener, and hassfest wants an integration that has one to say
# what YAML it takes.
CONFIG_SCHEMA = cv.config_entry_only_config_schema(DOMAIN)

IMPORT_HISTORY_SCHEMA = vol.Schema(
    {
        vol.Optional(ATTR_CONFIG_ENTRY_ID): cv.string,
        vol.Optional(ATTR_ENTITY_ID): cv.entity_ids,
        vol.Optional(ATTR_DAYS): vol.All(vol.Coerce(int), vol.Range(min=1, max=3650)),
    }
)


@dataclass
class TagHistorianData:
    """Everything one config entry owns while it is loaded."""

    client: TagHistorianClient
    coordinator: QuotaCoordinator
    forwarder: StateForwarder
    importer: HistoryImporter


type TagHistorianConfigEntry = ConfigEntry[TagHistorianData]


def async_get_stats(hass: HomeAssistant, entry_id: str) -> ForwarderStats:
    """The delivery counters for this entry, outliving any one forwarder.

    A reload builds a new forwarder, and an options change or the tag-quota
    repair flow triggers one. Counters kept on the forwarder went back to zero
    there, so "readings dropped" silently reset at the exact moment somebody
    was fixing the thing that had been dropping them. These live beside the
    entry instead, and only a Home Assistant restart clears them.
    """
    return hass.data.setdefault(DOMAIN, {}).setdefault(entry_id, ForwarderStats())


async def async_setup(hass: HomeAssistant, config: ConfigType) -> bool:
    """Once per Home Assistant run: the integration's actions, and the rename listener.

    Both here rather than per entry, because an entry is not always loaded
    when they are needed. The action exists - and can say what is wrong -
    while no entry is loaded. And a rename can happen at any time: each
    handled rename changes the options and so reloads the entry, and an entry
    waits out a dead API in setup-retry; a listener that came and went with
    the entry missed every rename made in those windows - the second entity of
    a device being renamed, typically - and that entity's readings stopped
    without a word (renames.py).
    """

    async def _async_import_history(call: ServiceCall) -> None:
        entry = _resolve_entry(hass, call.data.get(ATTR_CONFIG_ENTRY_ID))
        selected = list(entry.options.get(CONF_ENTITIES, []))
        entity_ids = list(call.data.get(ATTR_ENTITY_ID) or selected)
        not_selected = [entity_id for entity_id in entity_ids if entity_id not in selected]
        if not_selected:
            raise ServiceValidationError(
                translation_domain=DOMAIN,
                translation_key="not_selected",
                translation_placeholders={"entities": ", ".join(not_selected)},
            )
        if not entity_ids:
            raise ServiceValidationError(
                translation_domain=DOMAIN, translation_key="nothing_selected"
            )
        await async_start_import(hass, entry, entity_ids, call.data.get(ATTR_DAYS))

    hass.services.async_register(
        DOMAIN, SERVICE_IMPORT_HISTORY, _async_import_history, schema=IMPORT_HISTORY_SCHEMA
    )
    async_setup_rename_tracking(hass)
    return True


def _resolve_entry(hass: HomeAssistant, entry_id: str | None) -> TagHistorianConfigEntry:
    loaded = [
        entry
        for entry in hass.config_entries.async_entries(DOMAIN)
        if entry.state is ConfigEntryState.LOADED
    ]
    if entry_id is not None:
        for entry in loaded:
            if entry.entry_id == entry_id:
                return entry
        raise ServiceValidationError(
            translation_domain=DOMAIN, translation_key="entry_not_loaded"
        )
    if len(loaded) == 1:
        return loaded[0]
    raise ServiceValidationError(
        translation_domain=DOMAIN,
        translation_key="entry_ambiguous" if loaded else "entry_not_loaded",
    )


async def async_start_import(
    hass: HomeAssistant,
    entry: TagHistorianConfigEntry,
    entity_ids: list[str],
    days: int | None,
) -> None:
    """Start a history import, or say in the user's language why it cannot start.

    Shared by the action and the options dialog, so both refuse for the same
    reasons in the same words.
    """
    if not history.recorder_ready(hass):
        raise ServiceValidationError(translation_domain=DOMAIN, translation_key="no_recorder")
    try:
        await entry.runtime_data.importer.async_start(entity_ids, days)
    except ImportAlreadyRunning as err:
        raise ServiceValidationError(
            translation_domain=DOMAIN, translation_key="import_running"
        ) from err
    except TagHistorianImportRefused as err:
        raise HomeAssistantError(
            translation_domain=DOMAIN,
            translation_key="import_refused",
            translation_placeholders={"message": err.message},
        ) from err
    except TagHistorianAuthError as err:
        entry.async_start_reauth(hass)
        raise HomeAssistantError(
            translation_domain=DOMAIN, translation_key="key_rejected"
        ) from err
    except TagHistorianConnectionError as err:
        raise HomeAssistantError(
            translation_domain=DOMAIN, translation_key="import_cannot_connect"
        ) from err


async def async_setup_entry(
    hass: HomeAssistant, entry: TagHistorianConfigEntry
) -> bool:
    """Set up one Tag Historian account."""
    # Reload on an options change so the new selection is what gets subscribed
    # to. OptionsFlowWithReload would replace this listener, but it only exists
    # from 2025.8 and hacs.json claims a lower floor - see the README.
    #
    # FIRST, before anything is awaited. A rename arriving while this setup is
    # under way changes the options (renames.py); registered at the end, the
    # listener missed a change made after the forwarder below was built, which
    # then ran on to the next reload subscribed to an id that no longer exists.
    # Registered here, the change queues a reload, which waits for this setup
    # to finish. A setup that fails runs the unload callbacks, this one too.
    # The saved buffer and the saved import are read further down, so this
    # setup awaits more than it did when that was found, not less.
    entry.async_on_unload(entry.add_update_listener(_async_options_updated))

    client = TagHistorianClient(
        async_get_clientsession(hass), entry.data[CONF_HOST], entry.data[CONF_API_KEY]
    )

    coordinator = QuotaCoordinator(hass, entry, client)
    # Raises ConfigEntryNotReady on a dead API (Home Assistant then retries
    # with backoff) and ConfigEntryAuthFailed on a revoked key (Home Assistant
    # then shows its reauth card). Both are better than a loaded entry that
    # silently forwards nothing.
    await coordinator.async_config_entry_first_refresh()

    min_interval = int(entry.options.get(CONF_MIN_INTERVAL, DEFAULT_MIN_INTERVAL_SECONDS))
    forwarder = StateForwarder(
        hass,
        entry,
        client,
        list(entry.options.get(CONF_ENTITIES, [])),
        min_interval,
        async_get_stats(hass, entry.entry_id),
        store=_store(hass, STORAGE_KEY_FORWARDER, entry),
        send_as=send_as(entry.options),
    )
    # What the last run saved - its undelivered readings and how far Tag
    # Historian had acknowledged each entity - goes back in before anything
    # new arrives.
    await forwarder.async_restore()
    forwarder.async_start()
    # NOT entry.async_on_unload(forwarder.async_shutdown). See
    # async_unload_entry below: the on-unload callbacks run AFTER Home
    # Assistant has begun cancelling the entry's background tasks, and one of
    # those tasks is the flush that may have a batch on the wire. The stop
    # path has the same hazard one layer up and its own answer - a shutdown
    # job, registered in StateForwarder.async_start.

    # The importer asks the forwarder where each entity's live part begins, so
    # it never imports a reading the forwarder is still going to deliver.
    importer = HistoryImporter(
        hass,
        entry,
        client,
        _store(hass, STORAGE_KEY_IMPORT, entry),
        min_interval,
        forwarder.history_boundary,
    )

    entry.runtime_data = TagHistorianData(client, coordinator, forwarder, importer)

    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)

    # An import that was running when Home Assistant stopped carries on.
    await importer.async_load()

    # Every rename waiting to be settled - just made, which is what this
    # reload is for; made while the entry was not loaded; or left from before
    # a restart or an unreachable API - is taken up: a card, or the request to
    # rename the tag in Tag Historian (renames.py).
    async_resume(hass, entry)

    return True


def _store(hass: HomeAssistant, key: str, entry: ConfigEntry) -> Store[dict[str, Any]]:
    return Store(hass, STORAGE_VERSION, key.format(entry_id=entry.entry_id))


async def _async_options_updated(
    hass: HomeAssistant, entry: TagHistorianConfigEntry
) -> None:
    await hass.config_entries.async_reload(entry.entry_id)


async def async_unload_entry(
    hass: HomeAssistant, entry: TagHistorianConfigEntry
) -> bool:
    """Unload one account, and save the buffer on the way out.

    The forwarder is shut down HERE rather than through
    ``entry.async_on_unload``, and the ordering is the reason. Home Assistant's
    unload does three things in this sequence:

        1. await this function;
        2. run the ``async_on_unload`` callbacks;
        3. cancel the entry's background tasks and wait for them.

    A flush with a batch already on the wire is one of those background tasks.
    Shutting down at step 2 meant step 3 cancelled the request out from under a
    batch that had already left the queue: on every options change and every
    tag-quota repair, the readings in that batch vanished and no counter moved,
    while the README promised that could not happen. At step 1 nothing has been
    cancelled yet, so ``async_shutdown`` can wait for the write - and what it
    still cannot deliver, it saves.

    Order within this function matters too: the platforms go first, so a
    failed unload leaves a forwarder that is still running rather than one that
    has been stopped and emptied under a loaded entry. A running history
    import is stopped, not ended: its place is saved, and the next load
    carries on from it.
    """
    unloaded = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    if unloaded:
        await entry.runtime_data.importer.async_stop()
        await entry.runtime_data.forwarder.async_shutdown()
    return unloaded


async def async_remove_entry(
    hass: HomeAssistant, entry: TagHistorianConfigEntry
) -> None:
    """Clear what this entry left behind: repair issues, and its saved state.

    An issue whose config entry no longer exists cannot be fixed and cannot be
    dismissed by fixing it, so removing the entry has to remove them too. The
    saved buffer and import progress belong to an account that is no longer
    set up here; a re-added account must not send them.
    """
    async_clear_write_issues(hass, entry)
    async_clear_renamed_issues(hass, entry)
    # The counters survive a reload on purpose; they must not survive the
    # account being removed and re-added, which is a different account's worth
    # of history as far as anybody reading them is concerned.
    hass.data.get(DOMAIN, {}).pop(entry.entry_id, None)
    for key in (STORAGE_KEY_FORWARDER, STORAGE_KEY_IMPORT):
        await _store(hass, key, entry).async_remove()
