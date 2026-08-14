"""Config and options flows.

Four screens, and the middle two are the reason this integration exists at all.
Home Assistant can already write to Tag Historian through the built-in
``influxdb`` integration; what it cannot do is tell the user, while they are
choosing, that their plan holds N tags and they have just ticked N+14.

    1. connect  - host and API key, VALIDATED before an entry is created
    2. select   - the entities whose JOB is to report something, pre-ticked
                  down to the tag allowance and spread across devices, with a
                  toggle that puts the whole install back in the list
    3. review   - the same arithmetic recomputed AFTER submission, so the
                  numbers on the last screen are exact rather than stale. Two
                  step ids, ``review`` and ``review_trim``, because a screen
                  with nothing to decide and a screen offering to cut the
                  selection are not the same screen and must not share a
                  description
    4. (done)

Two rules the screens have to keep, because both were broken by code that
looked reasonable:

* **What the picker OFFERS never depends on what an entity reads right now.**
  ``EntitySelector`` enforces ``include_entities`` server-side with
  ``vol.In``, so an offer list that shrinks when a battery goes flat gives the
  options dialog a default its own selector refuses - and pressing Submit
  without touching anything raises ``vol.Invalid``.

* **The budget is counted in TAGS, everywhere.** ``tags_available``,
  ``_new_tag_count`` and the trim are all in the same unit; an entity count
  substituted for any of them is a screen that says it fits when it does not.

Every number any of these screens prints about plans, tags or readings comes
from ``GET /api/usage/limits`` and ``GET /api/usage/daily``. Nothing about a
plan is typed into this package.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import voluptuous as vol
from homeassistant.config_entries import (
    ConfigFlow,
    ConfigFlowResult,
    OptionsFlow,
)
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers import entity_registry as er
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.helpers.selector import (
    EntitySelector,
    EntitySelectorConfig,
    NumberSelector,
    NumberSelectorConfig,
    NumberSelectorMode,
    TextSelector,
    TextSelectorConfig,
    TextSelectorType,
)

from .api import (
    QuotaSnapshot,
    TagHistorianAuthError,
    TagHistorianClient,
    TagHistorianConnectionError,
    TagHistorianPermissionError,
)
from .const import (
    CONF_API_KEY,
    CONF_ENTITIES,
    CONF_HOST,
    CONF_MIN_INTERVAL,
    DEFAULT_HOST,
    DEFAULT_MIN_INTERVAL_SECONDS,
    DOMAIN,
)
from .line_protocol import tag_name_for_entity
from .selection import (
    EntityFacts,
    Proposal,
    propose,
    rank_candidates,
    trim_to_new_tag_budget,
)

CONF_TRIM = "trim_to_fit"
CONF_SHOW_ALL = "show_all"


def collect_entity_facts(hass: HomeAssistant) -> list[EntityFacts]:
    """Gather what selection needs about every entity in this install.

    The entity registry carries device_class, state_class, entity_category and
    the owning device without touching the state machine, but not every entity
    is registered (YAML template sensors are not), so the state's attributes
    are the fallback. Disabled and hidden entities are skipped: Home Assistant
    already knows the user does not want them.

    The entity's current STATE is deliberately not collected. Reading it here
    is what made one flat battery enough to drop an entity out of the picker
    while it was still sitting in the saved options - and then the options
    dialog opened pre-filled with a value its own selector refuses.
    """
    registry = er.async_get(hass)
    facts: list[EntityFacts] = []

    for state in hass.states.async_all():
        entry = registry.async_get(state.entity_id)
        if entry is not None and (entry.disabled_by or entry.hidden_by):
            continue

        device_class = None
        state_class = None
        entity_category = None
        device_id = None

        if entry is not None:
            device_class = entry.device_class or entry.original_device_class
            entity_category = entry.entity_category
            device_id = entry.device_id
            if entry.capabilities:
                state_class = entry.capabilities.get("state_class")

        device_class = device_class or state.attributes.get("device_class")
        state_class = state_class or state.attributes.get("state_class")

        facts.append(
            EntityFacts(
                entity_id=state.entity_id,
                # StrEnum values arrive from the registry and plain strings from
                # the state machine; normalise so ranking compares like with
                # like.
                device_class=str(device_class) if device_class else None,
                state_class=str(state_class) if state_class else None,
                entity_category=str(entity_category) if entity_category else None,
                friendly_name=state.attributes.get("friendly_name"),
                device_id=device_id,
            )
        )

    return facts


def readings_per_day(entity_count: int, min_interval: int) -> int:
    """Worst-case readings a selection can produce in a day.

    Worst case, not typical: it assumes every selected entity changes at least
    as often as the minimum interval allows. Understating this number would
    turn the review screen into false reassurance, which is the one thing a
    quota screen must not be.
    """
    if min_interval <= 0:
        return 0
    return entity_count * (86400 // min_interval)


class SelectionStepsMixin:
    """The select and review screens, shared by the config and options flows.

    Shared rather than duplicated because the two flows have to agree: an
    options flow that computed the budget differently from the config flow
    would let a user pass a screen that had just refused them.
    """

    hass: HomeAssistant

    _client: TagHistorianClient
    _quota: QuotaSnapshot
    _existing_tags: set[str]
    _proposal: Proposal
    _selected: list[str]
    _min_interval: int = DEFAULT_MIN_INTERVAL_SECONDS
    _show_everything: bool = False
    _selection_touched: bool = False

    def _new_tag_count(self, selected: list[str]) -> int:
        """How many of these entities do NOT already have a tag.

        Re-selecting an entity that already has history costs nothing: the tag
        exists, and the quota counts tags. Only genuinely new names need a free
        slot, which is why this is a set difference against
        ``GET /api/tags`` rather than ``len(selected)``.
        """
        return sum(
            1
            for entity_id in selected
            if tag_name_for_entity(entity_id) not in self._existing_tags
        )

    async def async_step_select(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Screen 2 - choose the entities, with the allowance in view."""
        errors: dict[str, str] = {}

        if user_input is not None:
            self._selected = list(user_input[CONF_ENTITIES])
            self._min_interval = int(user_input[CONF_MIN_INTERVAL])
            self._selection_touched = True
            asked_for_everything = bool(user_input.get(CONF_SHOW_ALL, False))

            if asked_for_everything != self._show_everything:
                # Moving the toggle is a request to redraw this screen with a
                # different list, not an answer to it. Fall through and show
                # the form again; whatever was ticked survives, because it is
                # passed to propose() as ``keep``.
                self._show_everything = asked_for_everything
            elif not self._selected:
                # An entry with an empty selection subscribes to nothing and
                # is indistinguishable from a working one. Refusing here is
                # the whole reason this is a blocking error and not a warning.
                errors["base"] = "select_at_least_one"
            else:
                return await self.async_step_review()

        facts = collect_entity_facts(self.hass)
        self._proposal = propose(
            facts,
            self._quota.tags_available,
            show_everything=self._show_everything,
            keep=self._selected,
        )

        # include_entities is what turns a 336-row checkbox list into a
        # hundred-row one. It also has to contain everything already ticked:
        # EntitySelector enforces this list server-side with vol.In, so a
        # default outside it makes pressing Submit unchanged raise vol.Invalid.
        offered = self._proposal.offered
        default = (
            self._selected
            if self._selection_touched
            else (self._selected or self._proposal.selected)
        )

        schema = vol.Schema(
            {
                vol.Required(CONF_ENTITIES, default=default): EntitySelector(
                    EntitySelectorConfig(multiple=True, include_entities=offered)
                ),
                vol.Required(
                    CONF_MIN_INTERVAL, default=self._min_interval
                ): NumberSelector(
                    NumberSelectorConfig(
                        min=1, max=3600, mode=NumberSelectorMode.BOX,
                        unit_of_measurement="s",
                    )
                ),
                vol.Required(
                    CONF_SHOW_ALL, default=self._show_everything
                ): bool,
            }
        )

        return self.async_show_form(
            step_id="select",
            data_schema=schema,
            errors=errors,
            description_placeholders={
                "tag_limit": str(self._quota.tag_limit),
                "tags_in_use": str(self._quota.current_tag_count),
                "tags_available": str(self._quota.tags_available),
                "entity_count": str(len(facts)),
                "offered_count": str(len(offered)),
                "hidden_count": str(self._proposal.hidden_count),
                "preselected": str(len(default)),
                "breakdown": self._proposal.breakdown(default) or "nothing yet",
                "did_not_fit": str(self._proposal.did_not_fit),
            },
        )

    def _overflow(self) -> int:
        """New tags the selection needs beyond what the account has free."""
        return max(0, self._new_tag_count(self._selected) - self._quota.tags_available)

    def _review_placeholders(self) -> dict[str, str]:
        """The sums both review screens print, recomputed from the submission."""
        per_day = readings_per_day(len(self._selected), self._min_interval)
        daily_limit = self._quota.measurements_per_day_limit
        percent = round(per_day * 100 / daily_limit) if daily_limit else 0
        return {
            "selected": str(len(self._selected)),
            "new_tags": str(self._new_tag_count(self._selected)),
            "tags_available": str(self._quota.tags_available),
            "min_interval": str(self._min_interval),
            "readings_per_day": f"{per_day:,}".replace(",", " "),
            "daily_limit": f"{daily_limit:,}".replace(",", " "),
            "percent": str(percent),
        }

    async def async_step_review(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Screen 3 - the same sums, recomputed after submission.

        Home Assistant config-flow forms do not round-trip to the server while
        the user is ticking boxes, so the counts on screen 2 are the counts as
        of when it was drawn. That is exactly what this screen is for: it is
        recomputed from what was actually submitted, so it is exact, and it is
        the last thing between the user and a selection that will be refused.

        Two steps, not one form with a conditional field. A step id IS the
        translation key, so one step meant one description, and that
        description had to be written for the case where something does not
        fit. On a clean setup it drew no trim checkbox and still said "0 of
        those new tags do not fit ... the readings will be refused and lost" -
        a data-loss warning about a control that was not on the screen, shown
        to somebody who had done nothing wrong. Splitting the step lets each
        case say only what is true of it.
        """
        if self._overflow():
            return await self.async_step_review_trim()

        if user_input is not None:
            return await self.async_step_finish()

        return self.async_show_form(
            step_id="review",
            data_schema=vol.Schema({}),
            description_placeholders=self._review_placeholders(),
        )

    async def async_step_review_trim(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Screen 3, the version with something to decide.

        Reached only when the selection needs more new tags than the account
        has free, which is also the only state in which the trim checkbox
        exists.
        """
        available = self._quota.tags_available

        if user_input is not None:
            if user_input.get(CONF_TRIM, True):
                proposal = getattr(self, "_proposal", None)
                candidates = (
                    proposal.candidates
                    if proposal is not None
                    else rank_candidates(collect_entity_facts(self.hass))
                )
                # The budget handed over is the one the quota is actually
                # counted in - free TAG slots. Passing an entity count here is
                # what let a "trim" finish still over budget.
                trimmed = trim_to_new_tag_budget(
                    self._selected, candidates, self._existing_tags, available
                )
                if not trimmed:
                    # Trimming a selection down to nothing is not a trim, it is
                    # a silent uninstall: the entry would be created, subscribe
                    # to nothing, and look exactly like a working one.
                    return self.async_abort(reason="no_tags_available")
                self._selected = trimmed
            return await self.async_step_finish()

        # "Trim" removes the entities the ranking would have shown at the
        # bottom of the list anyway; unticking it is the user saying they would
        # rather keep the selection and let the extras be refused.
        return self.async_show_form(
            step_id="review_trim",
            data_schema=vol.Schema({vol.Required(CONF_TRIM, default=True): bool}),
            description_placeholders={
                **self._review_placeholders(),
                "overflow": str(self._overflow()),
            },
        )

    async def async_step_finish(self) -> ConfigFlowResult:
        raise NotImplementedError


class TagHistorianConfigFlow(SelectionStepsMixin, ConfigFlow, domain=DOMAIN):
    """Add a Tag Historian account."""

    VERSION = 1
    MINOR_VERSION = 1

    def __init__(self) -> None:
        self._host = DEFAULT_HOST
        self._api_key = ""
        self._account_name = ""
        self._selected = []
        self._existing_tags = set()

    @staticmethod
    @callback
    def async_get_options_flow(config_entry: Any) -> OptionsFlow:
        # No argument, and no ``self.config_entry = ...``: assigning it in an
        # options flow was deprecated in 2024.11 and removed in 2025.12. The
        # base class provides it as a property.
        return TagHistorianOptionsFlow()

    async def async_step_user(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Screen 1 - connect, and say plainly what went wrong if it does."""
        errors: dict[str, str] = {}

        if user_input is not None:
            self._host = user_input[CONF_HOST]
            self._api_key = user_input[CONF_API_KEY]
            errors = await self._async_connect()
            if not errors:
                return await self.async_step_select()

        schema = vol.Schema(
            {
                vol.Required(CONF_HOST, default=self._host): str,
                vol.Required(CONF_API_KEY): TextSelector(
                    TextSelectorConfig(type=TextSelectorType.PASSWORD)
                ),
            }
        )
        return self.async_show_form(
            step_id="user", data_schema=schema, errors=errors
        )

    async def _async_connect(self) -> dict[str, str]:
        """Validate the credential and load the account's quota.

        Three distinguishable failures, because they need three different
        fixes. A single "cannot connect" would send someone hunting for a
        firewall rule when what they actually pasted was a read-only key.
        """
        client = TagHistorianClient(
            async_get_clientsession(self.hass), self._host, self._api_key
        )

        try:
            account = await client.async_validate()
        except TagHistorianAuthError:
            return {"base": "invalid_auth"}
        except TagHistorianPermissionError:
            # /api/customers/me is reachable by every role, so a 403 here is
            # not a scope problem - it is an account the API is refusing.
            return {"base": "account_blocked"}
        except TagHistorianConnectionError:
            return {"base": "cannot_connect"}

        if account.get("isActive") is False:
            return {"base": "account_inactive"}

        if _write_blocked_pending_verification(account):
            return {"base": "email_not_verified"}

        try:
            quota = await client.async_get_quota()
            existing = await client.async_get_tag_names()
        except TagHistorianPermissionError:
            # A Read-scoped key passes /me and fails everything that needs
            # Editor - including the write endpoint. Catching it here turns a
            # silent 3am failure into a setup error naming the actual fix.
            return {"base": "read_only_key"}
        except TagHistorianAuthError:
            return {"base": "invalid_auth"}
        except TagHistorianConnectionError:
            return {"base": "cannot_connect"}

        customer_id = account.get("customerId")
        if customer_id:
            # The customer GUID, not the host: a URL or an IP is explicitly
            # named in Home Assistant's docs as an unacceptable unique id, and
            # two entries for one account would double the reading bill.
            await self.async_set_unique_id(str(customer_id))
            self._abort_if_unique_id_configured()

        self._client = client
        self._quota = quota
        self._existing_tags = existing
        self._account_name = str(account.get("name") or "Tag Historian")
        return {}

    async def async_step_finish(self) -> ConfigFlowResult:
        return self.async_create_entry(
            title=self._account_name,
            data={CONF_HOST: self._host, CONF_API_KEY: self._api_key},
            options={
                CONF_ENTITIES: self._selected,
                CONF_MIN_INTERVAL: self._min_interval,
            },
        )

    async def async_step_reauth(
        self, entry_data: dict[str, Any]
    ) -> ConfigFlowResult:
        """The key stopped working - ask for a new one, nothing else."""
        self._host = entry_data.get(CONF_HOST, DEFAULT_HOST)
        return await self.async_step_reauth_confirm()

    async def async_step_reauth_confirm(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        errors: dict[str, str] = {}
        entry = self._get_reauth_entry()

        if user_input is not None:
            self._api_key = user_input[CONF_API_KEY]
            client = TagHistorianClient(
                async_get_clientsession(self.hass), self._host, self._api_key
            )
            try:
                account = await client.async_validate()
            except TagHistorianAuthError:
                errors["base"] = "invalid_auth"
            except TagHistorianConnectionError:
                errors["base"] = "cannot_connect"
            else:
                # A key for a DIFFERENT account would silently start writing
                # this house's history into somebody else's tenant.
                await self.async_set_unique_id(str(account.get("customerId")))
                self._abort_if_unique_id_mismatch(reason="wrong_account")
                return self.async_update_reload_and_abort(
                    entry, data_updates={CONF_API_KEY: self._api_key}
                )

        return self.async_show_form(
            step_id="reauth_confirm",
            data_schema=vol.Schema(
                {
                    vol.Required(CONF_API_KEY): TextSelector(
                        TextSelectorConfig(type=TextSelectorType.PASSWORD)
                    )
                }
            ),
            errors=errors,
        )


class TagHistorianOptionsFlow(SelectionStepsMixin, OptionsFlow):
    """Change the selection later without re-adding the integration."""

    async def async_step_init(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        entry = self.config_entry
        self._selected = list(entry.options.get(CONF_ENTITIES, []))
        self._min_interval = int(
            entry.options.get(CONF_MIN_INTERVAL, DEFAULT_MIN_INTERVAL_SECONDS)
        )

        client = TagHistorianClient(
            async_get_clientsession(self.hass),
            entry.data[CONF_HOST],
            entry.data[CONF_API_KEY],
        )
        try:
            self._quota = await client.async_get_quota()
            self._existing_tags = await client.async_get_tag_names()
        except (
            TagHistorianAuthError,
            TagHistorianPermissionError,
            TagHistorianConnectionError,
        ):
            # Without live numbers this flow cannot honestly show a budget, and
            # showing a stale one is how a customer-facing surface starts
            # lying. Abort and let the user retry once the API answers.
            return self.async_abort(reason="cannot_connect")

        self._client = client
        return await self.async_step_select()

    async def async_step_finish(self) -> ConfigFlowResult:
        return self.async_create_entry(
            data={
                CONF_ENTITIES: self._selected,
                CONF_MIN_INTERVAL: self._min_interval,
            }
        )


def _write_blocked_pending_verification(account: dict[str, Any]) -> bool:
    """True when the API will refuse writes for an unconfirmed address.

    The account keeps working - reading, signing in, this very call - right up
    until the grace period expires, at which point ingestion starts answering
    403. Detecting it at setup means the user is told before they finish, not
    after a week of missing history.
    """
    if account.get("emailVerified"):
        return False

    deadline = account.get("verifyDeadline")
    if not deadline:
        return False

    try:
        parsed = datetime.fromisoformat(str(deadline).replace("Z", "+00:00"))
    except ValueError:
        return False
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed < datetime.now(UTC)
