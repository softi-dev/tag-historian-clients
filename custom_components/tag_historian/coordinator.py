"""Poll the account's quota.

Separate from the write path on purpose. The forwarder learns about quota the
hard way, from a 422 or a 429 after the fact; this coordinator knows the
numbers before anything is refused, which is what lets the options flow say
"47 of 50" while the user is still choosing and what fills in the figures the
repair issues quote back at them.
"""

from __future__ import annotations

from datetime import timedelta

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import ConfigEntryAuthFailed
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed

from .api import (
    QuotaSnapshot,
    TagHistorianAuthError,
    TagHistorianClient,
    TagHistorianError,
)
from .const import DOMAIN, LOGGER, QUOTA_UPDATE_INTERVAL_MINUTES


class QuotaCoordinator(DataUpdateCoordinator[QuotaSnapshot]):
    """Fifteen-minute poll of ``/api/usage/limits`` and ``/api/usage/daily``.

    Fifteen minutes, not one. This is a billing surface: the tag count moves
    when a new tag is created, which for a Home Assistant install happens once
    at setup and then almost never. Polling it a minute would be sixty times
    the requests for a number that changes by the day.
    """

    def __init__(
        self,
        hass: HomeAssistant,
        entry: ConfigEntry,
        client: TagHistorianClient,
    ) -> None:
        super().__init__(
            hass,
            LOGGER,
            name=DOMAIN,
            config_entry=entry,
            update_interval=timedelta(minutes=QUOTA_UPDATE_INTERVAL_MINUTES),
            # The snapshot is a frozen dataclass, so equality is by value:
            # an unchanged quota does not wake every listener.
            always_update=False,
        )
        self._client = client

    async def _async_update_data(self) -> QuotaSnapshot:
        try:
            return await self._client.async_get_quota()
        except TagHistorianAuthError as err:
            # Stops the poll and hands the entry to Home Assistant's own reauth
            # flow, which is the only screen that can actually take a new key.
            raise ConfigEntryAuthFailed(
                "Tag Historian rejected the API key"
            ) from err
        except TagHistorianError as err:
            raise UpdateFailed(f"Could not read quota: {err}") from err
