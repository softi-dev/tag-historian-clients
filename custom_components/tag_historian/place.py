"""The account's approximate place and its time zone, proposed from Home Assistant.

Tag Historian's account has a clock - a time zone - and, for its outdoor
weather, a coarse place. Home Assistant knows both. This proposes them to
the account (``POST /api/account/settings/proposal``), and a proposal is
only that: what somebody chose in Tag Historian stands.

THE PLACE. Tag Historian can draw SMHI's outdoor temperature and
precipitation beside any chart, from the weather stations nearest the
account's place. When the user ticks "Use my approximate location (about 10
km) for outdoor weather from SMHI" (``CONF_SHARE_LOCATION``, off by default),
this Home Assistant's location is proposed as that place at every setup of
the entry.

ROUNDED HERE. The latitude and longitude are rounded to a tenth of a degree -
about 11 km north-south, 5 to 6 km east-west in Sweden - before anything is
sent, so a house's position never leaves Home Assistant. The API rounds again
whatever it is sent.

A PROPOSAL, NOT A SETTING. The API takes a place only while the account has
none AND has never had one removed: a place an Admin chose in Tag
Historian's Settings, or one proposed before, stands, and a place an Admin
removed stays removed - a proposal does not fill it again. Sending it again
at the next setup is therefore harmless, and covers a setup whose proposal
did not get through. Unticking the option stops the proposals; it does not
remove a place the account already has - that is done in Settings, which
says where the place came from.

THE TIME ZONE. Home Assistant's own (``hass.config.time_zone``) is proposed
at every setup, as the setup's last screen says. It is what Tag Historian's
increases per day and month, Fair winter, the Monday summary and alert times
are in, and Tag Historian takes it only while nobody has chosen one -
not an Admin in Settings, not the owner's email channel under Alarms, not an
earlier proposal. Until then the account is on Swedish time.
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


async def async_propose_time_zone(hass: HomeAssistant, client: TagHistorianClient) -> None:
    """Propose Home Assistant's time zone as the account's; quietly, as the place is."""
    time_zone = hass.config.time_zone
    if not time_zone:
        return
    try:
        await client.async_propose_time_zone(str(time_zone))
    except TagHistorianAuthError:
        LOGGER.debug("The time zone was not proposed: the API key was refused")
