"""The Tag Historian integration.

Two independent loops share one HTTP client:

    push  - async_track_state_change_event on exactly the selected entities,
            buffered, flushed to POST /api/v2/write  (see forwarder.py)
    poll  - a 15-minute DataUpdateCoordinator over GET /api/usage/limits,
            which is where every quota number the UI shows comes from
            (see coordinator.py)

and one listener, for the whole integration rather than any one entry: the
entity registry, so an entity renamed in Home Assistant is followed rather than
silently dropped (see renames.py).
"""

from __future__ import annotations

from dataclasses import dataclass

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import Platform
from homeassistant.core import HomeAssistant
from homeassistant.helpers import config_validation as cv
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.helpers.typing import ConfigType

from .api import TagHistorianClient
from .const import (
    CONF_API_KEY,
    CONF_ENTITIES,
    CONF_HOST,
    CONF_MIN_INTERVAL,
    DEFAULT_MIN_INTERVAL_SECONDS,
    DOMAIN,
)
from .coordinator import QuotaCoordinator
from .forwarder import ForwarderStats, StateForwarder
from .renames import (
    async_clear_renamed_issues,
    async_resume,
    async_setup_rename_tracking,
    send_as,
)
from .repairs import async_clear_write_issues

PLATFORMS: list[Platform] = [Platform.SENSOR]

# Set up from the UI only; async_setup below exists for the rename listener,
# and hassfest wants an integration that has one to say what YAML it takes.
CONFIG_SCHEMA = cv.config_entry_only_config_schema(DOMAIN)


@dataclass
class TagHistorianData:
    """Everything one config entry owns while it is loaded."""

    client: TagHistorianClient
    coordinator: QuotaCoordinator
    forwarder: StateForwarder


type TagHistorianConfigEntry = ConfigEntry[TagHistorianData]


def async_get_stats(hass: HomeAssistant, entry_id: str) -> ForwarderStats:
    """The delivery counters for this entry, outliving any one forwarder.

    A reload builds a new forwarder, and an options change or the tag-quota
    repair flow triggers one. Counters kept on the forwarder went back to zero
    there, so "readings dropped" silently reset at the exact moment somebody
    was fixing the thing that had been dropping them. These live beside the
    entry instead, and only a Home Assistant restart clears them - which is
    honest, because a restart is also when the buffer itself goes.
    """
    return hass.data.setdefault(DOMAIN, {}).setdefault(entry_id, ForwarderStats())


async def async_setup(hass: HomeAssistant, config: ConfigType) -> bool:
    """Once per Home Assistant run: listen for renamed entities, for every entry.

    Here and not in async_setup_entry, because an entry is not always loaded
    when a rename happens. Each handled rename changes the options and so
    reloads the entry, and an entry waits out a dead API in setup-retry; a
    listener that came and went with the entry missed every rename made in
    those windows - the second entity of a device being renamed, typically -
    and that entity's readings stopped without a word (renames.py).
    """
    async_setup_rename_tracking(hass)
    return True


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

    forwarder = StateForwarder(
        hass,
        entry,
        client,
        list(entry.options.get(CONF_ENTITIES, [])),
        int(entry.options.get(CONF_MIN_INTERVAL, DEFAULT_MIN_INTERVAL_SECONDS)),
        async_get_stats(hass, entry.entry_id),
        send_as(entry.options),
    )
    forwarder.async_start()
    # NOT entry.async_on_unload(forwarder.async_shutdown). See
    # async_unload_entry below: the on-unload callbacks run AFTER Home
    # Assistant has begun cancelling the entry's background tasks, and one of
    # those tasks is the flush that may have a batch on the wire. The stop
    # path has the same hazard one layer up and its own answer - a shutdown
    # job, registered in StateForwarder.async_start.

    entry.runtime_data = TagHistorianData(client, coordinator, forwarder)

    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)

    # Every rename waiting to be settled - just made, which is what this
    # reload is for; made while the entry was not loaded; or left from before
    # a restart or an unreachable API - is taken up: a card, or the request to
    # rename the tag in Tag Historian (renames.py).
    async_resume(hass, entry)

    return True


async def _async_options_updated(
    hass: HomeAssistant, entry: TagHistorianConfigEntry
) -> None:
    await hass.config_entries.async_reload(entry.entry_id)


async def async_unload_entry(
    hass: HomeAssistant, entry: TagHistorianConfigEntry
) -> bool:
    """Unload one account, and empty the buffer on the way out.

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
    cancelled yet, so ``async_shutdown`` can wait for the write - and if it
    still cannot be saved, count it.

    Order within this function matters too: the platforms go first, so a
    failed unload leaves a forwarder that is still running rather than one that
    has been stopped and emptied under a loaded entry.
    """
    unloaded = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    if unloaded:
        await entry.runtime_data.forwarder.async_shutdown()
    return unloaded


async def async_remove_entry(
    hass: HomeAssistant, entry: TagHistorianConfigEntry
) -> None:
    """Clear any repair issues this entry left behind.

    An issue whose config entry no longer exists cannot be fixed and cannot be
    dismissed by fixing it, so removing the entry has to remove them too.
    """
    async_clear_write_issues(hass, entry)
    async_clear_renamed_issues(hass, entry)
    # The counters survive a reload on purpose; they must not survive the
    # account being removed and re-added, which is a different account's worth
    # of history as far as anybody reading them is concerned.
    hass.data.get(DOMAIN, {}).pop(entry.entry_id, None)
