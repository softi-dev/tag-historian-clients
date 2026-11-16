"""Power sensors more often (const.CONF_FAST_POWER), and the rule it keeps.

The rule: the option never lets the account send more readings a day than
its plan allows. The sum is the README's - every selected entity at the
minimum interval, the power sensors among them at FAST_POWER_INTERVAL_SECONDS
- and it is a worst case, which is the point: when even the worst case is
inside the allowance, the option cannot be what pushes the account over it.

It is kept in two places, with the same arithmetic (``FastPowerSum``):

* **The review screen** (config_flow.py). When the sum with the power sensors
  at two seconds is above the plan's daily allowance, the box cannot take
  effect: the screen says so in numbers, the sum it shows is the one at the
  minimum interval, and the entry is saved with the box off - what is saved
  is what happens.
* **At runtime** (``FastPowerGuard``). The selection's power sensors and the
  plan can both change after the screen: a sensor gains a device class, the
  plan is downgraded, Home Assistant was still starting when the entry was
  set up. The guard does the sum at setup, once Home Assistant has started,
  and at every quota poll. When it no longer fits, every entity goes back to
  the minimum interval, a warning is logged once and a repair card says why;
  when it fits again, the power sensors go back to two seconds and the card
  goes away.

Every number in the sum comes from the account (``GET /api/usage/limits``) and
from Home Assistant; nothing about a plan is typed here.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers import issue_registry as ir
from homeassistant.helpers.start import async_at_started

from .const import DOMAIN, FAST_POWER_INTERVAL_SECONDS, ISSUE_FAST_POWER, LOGGER
from .selection import is_power_sensor

if TYPE_CHECKING:
    from .coordinator import QuotaCoordinator
    from .forwarder import StateForwarder


def readings_per_day(
    entity_count: int,
    min_interval: int,
    power_count: int = 0,
    power_interval: int | None = None,
) -> int:
    """Worst-case readings a selection can produce in a day.

    Worst case, not typical: it assumes every selected entity changes at least
    as often as the minimum interval allows. Understating this number would
    turn the review screen into false reassurance, which is the one thing a
    quota screen must not be.

    ``power_count`` of the entities are power sensors sent every
    ``power_interval`` seconds instead (CONF_FAST_POWER) - counted at that
    pace when it is the shorter of the two, exactly as the forwarder sends
    them (StateForwarder._interval_for).
    """
    if min_interval <= 0:
        return 0
    if not power_count or power_interval is None or power_interval >= min_interval:
        return entity_count * (86400 // min_interval)
    power_count = min(power_count, entity_count)
    return (entity_count - power_count) * (86400 // min_interval) + power_count * (
        86400 // power_interval
    )


def power_sensor_count(hass: HomeAssistant, entity_ids: list[str]) -> int:
    """How many of these entities Home Assistant says measure power in W or kW, now."""
    return sum(
        1
        for entity_id in entity_ids
        if (state := hass.states.get(entity_id)) is not None
        and is_power_sensor(state.attributes)
    )


@dataclass(frozen=True)
class FastPowerSum:
    """The day's worst case for one selection, with and without the option.

    ``power_count`` is how many of the selected entities the option would
    send more often: 0 when none is a power sensor, or when the minimum
    interval is already as short as the power interval - then the option
    changes nothing, and nothing about it can fail to fit.
    """

    selected: int
    min_interval: int
    power_count: int
    daily_limit: int

    @property
    def normal(self) -> int:
        """Readings a day at most with every entity at the minimum interval."""
        return readings_per_day(self.selected, self.min_interval)

    @property
    def fast(self) -> int:
        """Readings a day at most with the power sensors at the power interval."""
        return readings_per_day(
            self.selected, self.min_interval, self.power_count, FAST_POWER_INTERVAL_SECONDS
        )

    @property
    def fits(self) -> bool:
        """Whether the option may take effect: it changes nothing, or its sum is inside the plan.

        A daily limit the account did not state (0) is not a plan to fit in:
        the option does not take effect on a number nobody can check.
        """
        if self.power_count == 0:
            return True
        return self.daily_limit > 0 and self.fast <= self.daily_limit


def fast_power_sum(
    hass: HomeAssistant, selected: list[str], min_interval: int, daily_limit: int
) -> FastPowerSum:
    """The sum for these entities as Home Assistant describes them now."""
    faster = min_interval > FAST_POWER_INTERVAL_SECONDS
    return FastPowerSum(
        selected=len(selected),
        min_interval=min_interval,
        power_count=power_sensor_count(hass, selected) if faster else 0,
        daily_limit=daily_limit,
    )


def _spaced(number: int) -> str:
    """A large number the way the review screen prints them: 1 500 000."""
    return f"{number:,}".replace(",", " ")


def issue_id(entry: ConfigEntry) -> str:
    return f"{ISSUE_FAST_POWER}_{entry.entry_id}"


def async_raise_fast_power_issue(
    hass: HomeAssistant, entry: ConfigEntry, total: FastPowerSum
) -> None:
    """The card that says the power sensors are back at the minimum interval, and why.

    Created again with the same id while it lasts, so Home Assistant shows it
    once; dismissing it is an answer Home Assistant keeps.
    """
    ir.async_create_issue(
        hass,
        DOMAIN,
        issue_id(entry),
        is_fixable=False,
        is_persistent=False,
        severity=ir.IssueSeverity.WARNING,
        translation_key=ISSUE_FAST_POWER,
        translation_placeholders={
            "power_interval": str(FAST_POWER_INTERVAL_SECONDS),
            "selected": str(total.selected),
            "power_count": str(total.power_count),
            "readings_per_day": _spaced(total.fast),
            "daily_limit": _spaced(total.daily_limit),
            "min_interval": str(total.min_interval),
        },
    )


def async_clear_fast_power_issue(hass: HomeAssistant, entry: ConfigEntry) -> None:
    ir.async_delete_issue(hass, DOMAIN, issue_id(entry))


class FastPowerGuard:
    """Keeps the option inside the plan while the entry runs.

    Only built for an entry whose box is ticked. It owns the forwarder's
    power interval: two seconds while the sum fits, none (every entity at the
    minimum interval) while it does not.
    """

    def __init__(
        self,
        hass: HomeAssistant,
        entry: ConfigEntry,
        forwarder: StateForwarder,
        coordinator: QuotaCoordinator,
        selected: list[str],
        min_interval: int,
    ) -> None:
        self.hass = hass
        self.entry = entry
        self._forwarder = forwarder
        self._coordinator = coordinator
        self._selected = list(selected)
        self._min_interval = min_interval
        # Whether the power sensors are held at the minimum interval right
        # now - so the warning is logged once when that starts, not at every
        # quota poll while it lasts.
        self._held = False

    @property
    def held(self) -> bool:
        return self._held

    @callback
    def async_check(self) -> None:
        """Do the sum with the plan and the states as they are now, and act on it."""
        quota = self._coordinator.data
        if quota is None:
            return
        total = fast_power_sum(
            self.hass, self._selected, self._min_interval, quota.measurements_per_day_limit
        )
        if total.fits:
            self._forwarder.set_power_interval(FAST_POWER_INTERVAL_SECONDS)
            if self._held:
                LOGGER.info(
                    "Tag Historian sends power sensors every %s seconds again: "
                    "the selection fits the plan's daily readings allowance",
                    FAST_POWER_INTERVAL_SECONDS,
                )
            self._held = False
            async_clear_fast_power_issue(self.hass, self.entry)
            return
        self._forwarder.set_power_interval(None)
        if not self._held:
            LOGGER.warning(
                "Tag Historian sends every entity at most once every %s seconds, power sensors "
                "too: at %s seconds the %s selected entities could send up to %s readings a day, "
                "and the plan allows %s",
                self._min_interval,
                FAST_POWER_INTERVAL_SECONDS,
                total.selected,
                total.fast,
                total.daily_limit,
            )
            async_raise_fast_power_issue(self.hass, self.entry, total)
        self._held = True

    @callback
    def async_start(self) -> Callable[[], None]:
        """Check now, once Home Assistant has started, and at every quota poll.

        Now, before the forwarder starts, so the catch-up after an outage is
        read back at the pace that fits. Again once Home Assistant has
        started, because during a start the entities of other integrations
        may not have states yet, and an uncounted power sensor is a sum that
        fits when it does not. Returns what stops the later checks.
        """
        self.async_check()
        unsubs = [
            self._coordinator.async_add_listener(self.async_check),
            async_at_started(self.hass, self._async_started),
        ]

        @callback
        def _stop() -> None:
            for unsub in unsubs:
                unsub()

        return _stop

    async def _async_started(self, _hass: HomeAssistant) -> None:
        self.async_check()
