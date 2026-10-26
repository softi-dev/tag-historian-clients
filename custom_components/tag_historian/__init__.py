"""The Tag Historian integration.

Three loops share one HTTP client:

    push    - async_track_state_change_event on exactly the selected entities,
              buffered, flushed to POST /api/v2/write; the buffer is saved
              across a restart, and gaps are read back from the recorder
              (see forwarder.py)
    poll    - a 15-minute DataUpdateCoordinator over GET /api/usage/limits,
              which is where every quota number the UI shows comes from
              (see coordinator.py)
    import  - the history Home Assistant already has of the selected
              entities: its long-term statistics to POST /api/import/{id}/hours,
              then the recorder's readings to POST /api/import/{id}/readings
              (see importer.py). Started by the setup of a new entry when
              its last screen says so, by the repair card that offers it to
              an older entry, from the options dialog, or by the
              tag_historian.import_history action

and one listener, for the whole integration rather than any one entry: the
entity registry, so an entity renamed in Home Assistant is followed rather than
silently dropped (see renames.py). All three loops follow it: a renamed
entity's readings - live, held, read back or imported - go into the tag its
history is in.
"""

from __future__ import annotations

from collections.abc import Mapping
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
    ATTR_STATISTICS,
    CONF_API_KEY,
    CONF_ENTITIES,
    CONF_HOST,
    CONF_IMPORT_ON_SETUP,
    CONF_MIN_INTERVAL,
    CONF_SHARE_LOCATION,
    DEFAULT_MIN_INTERVAL_SECONDS,
    DOMAIN,
    LOGGER,
    MAX_IMPORT_DAYS,
    SERVICE_IMPORT_HISTORY,
    STORAGE_KEY_FORWARDER,
    STORAGE_KEY_IMPORT,
    STORAGE_VERSION,
)
from .coordinator import QuotaCoordinator
from .forwarder import ForwarderStats, StateForwarder
from .importer import STOP_HOURS_UNSUPPORTED, HistoryImporter, ImportAlreadyRunning
from .place import async_propose_place, async_propose_time_zone
from .renames import (
    async_clear_renamed_issues,
    async_resume,
    async_setup_rename_tracking,
    own_tag_since,
    send_as,
)
from .repairs import (
    async_clear_bring_years_issue,
    async_clear_write_issues,
    async_raise_bring_years_issue,
)

PLATFORMS: list[Platform] = [Platform.SENSOR]

# Set up from the UI only; async_setup below exists for the import action and
# the rename listener, and hassfest wants an integration that has one to say
# what YAML it takes.
CONFIG_SCHEMA = cv.config_entry_only_config_schema(DOMAIN)

IMPORT_HISTORY_SCHEMA = vol.Schema(
    {
        vol.Optional(ATTR_CONFIG_ENTRY_ID): cv.string,
        vol.Optional(ATTR_ENTITY_ID): cv.entity_ids,
        vol.Optional(ATTR_DAYS): vol.All(vol.Coerce(int), vol.Range(min=1, max=MAX_IMPORT_DAYS)),
        vol.Optional(ATTR_STATISTICS, default=True): cv.boolean,
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
        await async_start_import(
            hass,
            entry,
            entity_ids,
            call.data.get(ATTR_DAYS),
            statistics=call.data.get(ATTR_STATISTICS, True),
        )

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
    *,
    statistics: bool = True,
) -> None:
    """Start a history import, or say in the user's language why it cannot start.

    Shared by the action, the options dialog and the repair card, so all
    three refuse for the same reasons in the same words. ``statistics``
    brings Home Assistant's long-term statistics first, as every door does
    unless told otherwise.
    """
    if not history.recorder_ready(hass):
        raise ServiceValidationError(translation_domain=DOMAIN, translation_key="no_recorder")
    try:
        await entry.runtime_data.importer.async_start(entity_ids, days, statistics=statistics)
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
    if statistics:
        # The card that offers this is answered, whichever door it came by.
        async_clear_bring_years_issue(hass, entry)


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

    # ONE reading of the options, for the forwarder and the importer both. A
    # rename landing during the awaits below replaces entry.options and queues
    # the reload that takes it up; meanwhile the two must not see different
    # selections, or send the same entity to different tags.
    options = entry.options
    selected = list(options.get(CONF_ENTITIES, []))
    renamed = send_as(options)
    min_interval = int(options.get(CONF_MIN_INTERVAL, DEFAULT_MIN_INTERVAL_SECONDS))
    forwarder = StateForwarder(
        hass,
        entry,
        client,
        selected,
        min_interval,
        async_get_stats(hass, entry.entry_id),
        store=_store(hass, STORAGE_KEY_FORWARDER, entry),
        send_as=renamed,
    )
    # What the last run saved - its undelivered readings and how far Tag
    # Historian had acknowledged each entity - goes back in before anything
    # new arrives: under each entity's id as it is now, for one renamed since,
    # and bound for the tag it sends to now.
    await forwarder.async_restore()
    forwarder.async_start()
    # NOT entry.async_on_unload(forwarder.async_shutdown). See
    # async_unload_entry below: the on-unload callbacks run AFTER Home
    # Assistant has begun cancelling the entry's background tasks, and one of
    # those tasks is the flush that may have a batch on the wire. The stop
    # path has the same hazard one layer up and its own answer - a shutdown
    # job, registered in StateForwarder.async_start.

    # The importer asks the forwarder where each entity's live part begins, so
    # it never imports a reading the forwarder is still going to deliver, and
    # sends a renamed entity's history into the tag the forwarder sends its
    # readings to.
    importer = HistoryImporter(
        hass,
        entry,
        client,
        _store(hass, STORAGE_KEY_IMPORT, entry),
        min_interval,
        forwarder.history_boundary,
        send_as=renamed,
        selected=selected,
        own_tag_since=own_tag_since(options),
    )

    entry.runtime_data = TagHistorianData(client, coordinator, forwarder, importer)

    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)

    # An import that was running when Home Assistant stopped, or when this
    # entry was reloaded - by a rename, too - carries on.
    await importer.async_load()

    # Every rename waiting to be settled - just made, which is what this
    # reload is for; made while the entry was not loaded; or left from before
    # a restart or an unreachable API - is taken up: a card, or the request to
    # rename the tag in Tag Historian (renames.py).
    async_resume(hass, entry)

    # The approximate location for the account's outdoor weather, only when
    # the user ticked it (place.py). In the background: nothing waits on it,
    # and an API that does not answer must not hold up the readings.
    if options.get(CONF_SHARE_LOCATION, False):
        entry.async_create_background_task(
            hass, async_propose_place(hass, client), f"{DOMAIN} propose place"
        )
    # The time zone, at every setup, and taken by Tag Historian only if the
    # account has no zone yet (place.py). A new entry's last screen says so;
    # an entry set up before that screen proposes it from its first load after
    # the update, which the README's upgrade paragraph and its repair card say.
    entry.async_create_background_task(
        hass, async_propose_time_zone(hass, client), f"{DOMAIN} propose time zone"
    )

    _async_bring_years(hass, entry, options)

    return True


def _async_bring_years(
    hass: HomeAssistant, entry: TagHistorianConfigEntry, options: Mapping[str, Any]
) -> None:
    """The history Home Assistant already has: brought now, offered, or neither.

    * A new entry whose last setup screen was left ticked
      (CONF_IMPORT_ON_SETUP) starts the import - statistics, then readings -
      for everything it keeps history for, in the background.
    * An entry set up before that screen existed has no such option, and is
      offered the same on a repair card instead.
    * Unticked: nothing; the options dialog and the action are still there.
    * Whichever it was, an import that met a Tag Historian that could not take
      the statistics yet brought none of them: they are offered on the card
      from then on, rather than started anew at every restart against a Tag
      Historian that may still not take them.

    Each only until the statistics have been brought once
    (``statistics_started_at``, kept from one import to the next), and never
    while an import is running - one carried on after a restart, say.
    """
    importer = entry.runtime_data.importer
    progress = importer.progress
    if importer.running or progress.statistics_started_at:
        return
    if not history.recorder_ready(hass):
        return
    if CONF_IMPORT_ON_SETUP not in options or (
        progress.statistics_stop_reason == STOP_HOURS_UNSUPPORTED
    ):
        async_raise_bring_years_issue(hass, entry)
        return
    if not options[CONF_IMPORT_ON_SETUP]:
        return

    selected = list(options.get(CONF_ENTITIES, []))

    async def _start() -> None:
        try:
            await async_start_import(hass, entry, selected, None, statistics=True)
        except HomeAssistantError as err:
            # Nothing was started, so the next setup asks again.
            LOGGER.warning(
                "Tag Historian could not start bringing the history Home Assistant already has "
                "(%s); it tries again the next time it is set up",
                err.translation_key or err,
            )

    entry.async_create_background_task(hass, _start(), f"{DOMAIN} bring the years")


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
    async_clear_bring_years_issue(hass, entry)
    # The counters survive a reload on purpose; they must not survive the
    # account being removed and re-added, which is a different account's worth
    # of history as far as anybody reading them is concerned.
    hass.data.get(DOMAIN, {}).pop(entry.entry_id, None)
    for key in (STORAGE_KEY_FORWARDER, STORAGE_KEY_IMPORT):
        await _store(hass, key, entry).async_remove()
