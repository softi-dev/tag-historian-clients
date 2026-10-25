"""The account's approximate place, for its outdoor weather.

Tag Historian can draw SMHI's outdoor temperature and precipitation beside any
chart, from the weather stations nearest the account's place. When the user
ticks "Use my approximate location (about 10 km) for outdoor weather from
SMHI" (``CONF_SHARE_LOCATION``, off by default), this Home Assistant's
location is proposed as that place at every setup of the entry.

ROUNDED HERE. The latitude and longitude are rounded to a tenth of a degree -
about 11 km north-south, 5 to 6 km east-west in Sweden - before anything is
sent, so a house's position never leaves Home Assistant. The API rounds again
whatever it is sent.

A PROPOSAL, NOT A SETTING. The API takes it only while the account has no
place: one an Admin chose in Tag Historian's Settings, or one proposed before,
stands. Sending it again at the next setup is therefore harmless, and covers a
setup whose proposal did not get through. Unticking the option stops the
proposals; it does not remove a place the account already has - that is done
in Settings, which says where the place came from.
"""

from __future__ import annotations

from homeassistant.core import HomeAssistant

from .api import TagHistorianAuthError, TagHistorianClient
from .const import LOGGER


def coarse_place(hass: HomeAssistant) -> tuple[float, float] | None:
    """This Home Assistant's location to a tenth of a degree, or None when it has none."""
    lat = hass.config.latitude
    lon = hass.config.longitude
    if lat is None or lon is None:
        return None
    return round(float(lat), 1), round(float(lon), 1)


async def async_propose_place(hass: HomeAssistant, client: TagHistorianClient) -> None:
    """Propose the rounded location as the account's place; quietly, since nothing waits on it."""
    place = coarse_place(hass)
    if place is None:
        return
    try:
        await client.async_propose_place(*place)
    except TagHistorianAuthError:
        # The quota coordinator meets the same key at its next refresh, and
        # that is where a revoked key is reported.
        LOGGER.debug("The place was not proposed: the API key was refused")
