"""Surface write-path failures where the user can act on them.

Four things can stop readings reaching Tag Historian, and they are four
different problems with four different fixes. Lumping them into one "cannot
write" notification would be the same mistake the built-in influxdb
integration makes by having only one failure mode.

    revoked key      -> NOT an issue here. ``ConfigEntryAuthFailed`` sends the
                        entry to Home Assistant's own reauth card, which is the
                        only screen that can accept a replacement key.
    tag quota (422)  -> fixable. The fix flow reopens the entity picker so the
                        user can deselect down to their allowance. This is the
                        one failure a dialog can genuinely repair.
    daily quota (429)-> not fixable in a dialog. The answers are "wait for the
                        reset" and "upgrade", neither of which belongs in a
                        Home Assistant form. Says which, and links pricing.
    forbidden (403)  -> not fixable here either, and the API's body is already
                        written for a person to read, so it is quoted verbatim.
    back-pressure    -> our saturation, not the user's problem. Shares the
      (429) and 503     "unavailable" card and is not raised at all below half
                        an hour. A blip that clears itself is not something to
                        wake anyone for.

Two of those are 429s and they must never be confused. Only the middleware's
"daily measurement quota exceeded" is about the account's allowance; the
controller's "write buffer fair-share exceeded" is Tag Historian being busy.
The card for the first names a billing limit and links the pricing page, so
sending the second one there is this code inventing a reason to charge more.

Every issue is cleared on the next clean 204. An issue that outlives its cause
teaches people to ignore the repairs panel.

One more card is not about the write path at all, and is not cleared by a 204:
an entity that was renamed in Home Assistant, whose tag has not been renamed in
Tag Historian. It offers to carry the history over to the new name, or to start
a new tag; until it is answered, the readings go on into the old tag, so
nothing is lost while it waits (renames.py).
"""

from __future__ import annotations

from typing import Any

import voluptuous as vol
from homeassistant.components.repairs import ConfirmRepairFlow, RepairsFlow
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, callback
from homeassistant.data_entry_flow import FlowResult
from homeassistant.helpers import issue_registry as ir
from homeassistant.helpers.selector import (
    EntitySelector,
    EntitySelectorConfig,
)

from .api import LineageOutcome
from .const import (
    CONF_ENTITIES,
    CONF_SEND_AS,
    DOMAIN,
    ISSUE_DAILY_QUOTA,
    ISSUE_ENTITY_RENAMED,
    ISSUE_SERVICE_UNAVAILABLE,
    ISSUE_TAG_QUOTA,
    ISSUE_WRITE_FORBIDDEN,
    PRICING_URL,
    SERVICE_UNAVAILABLE_ISSUE_AFTER_SECONDS,
)
from .renames import (
    async_carry_history,
    async_stop_carrying,
    renamed_placeholders,
    send_as,
)

# Issue ids are per config entry: two Tag Historian accounts on one Home
# Assistant must be able to be over quota independently.
_WRITE_ISSUES = (
    ISSUE_TAG_QUOTA,
    ISSUE_DAILY_QUOTA,
    ISSUE_WRITE_FORBIDDEN,
    ISSUE_SERVICE_UNAVAILABLE,
)


def issue_id(entry: ConfigEntry, kind: str) -> str:
    return f"{kind}_{entry.entry_id}"


def _quota_placeholders(hass: HomeAssistant, entry: ConfigEntry) -> dict[str, str]:
    """Tag counts for the issue text - from the API, never from this package.

    Falls back to "?" rather than to a guess. A repair dialog that invents
    "5 of 5" for an account that is actually on 50 is worse than one that
    admits it has not polled yet.
    """
    # getattr on the entry itself, not just on runtime_data: an entry that is
    # not fully loaded has no runtime_data attribute at all, and an issue that
    # fails to appear because reading its own placeholders raised is the worst
    # possible failure for a mechanism whose whole job is to be visible.
    runtime = getattr(entry, "runtime_data", None)
    coordinator = getattr(runtime, "coordinator", None)
    snapshot = getattr(coordinator, "data", None)
    if snapshot is None:
        return {"tag_count": "?", "tag_limit": "?"}
    return {
        "tag_count": str(snapshot.current_tag_count),
        "tag_limit": str(snapshot.tag_limit),
    }


@callback
def async_raise_tag_quota_issue(
    hass: HomeAssistant,
    entry: ConfigEntry,
    refused_tags: list[str],
    dropped_points: int,
) -> None:
    """422: some points were refused because they needed a new tag."""
    placeholders = _quota_placeholders(hass, entry)
    # Prose-scraped from the 422 body, so it can legitimately be empty. Say so
    # rather than rendering an empty bullet list.
    placeholders["refused_tags"] = (
        ", ".join(refused_tags) if refused_tags else "(the API did not name them)"
    )
    # Readings, not tags. The card used to describe the tag names as though
    # they were the things that were lost, which understates it by however many
    # readings each refused entity had produced in the batch.
    placeholders["dropped_points"] = str(dropped_points)

    ir.async_create_issue(
        hass,
        DOMAIN,
        issue_id(entry, ISSUE_TAG_QUOTA),
        is_fixable=True,
        is_persistent=False,
        severity=ir.IssueSeverity.ERROR,
        translation_key=ISSUE_TAG_QUOTA,
        translation_placeholders=placeholders,
        learn_more_url=PRICING_URL,
        data={"entry_id": entry.entry_id},
    )


def describe_wait(seconds: float) -> str:
    """Render a Retry-After as something a person can act on.

    The previous rendering was ``max(1, round(seconds / 3600))`` hours, which
    could not produce an answer shorter than an hour however short the wait
    actually was. That mattered because this card was also being raised for
    the write-buffer 429, whose Retry-After is five seconds plus jitter: a
    seven-second hiccup rendered as "resets in about 1 hour". The 429s are
    separated now (see api._classify_429), and this function is the other half
    of it - a real quota reset can also be four minutes away, near midnight.
    """
    if seconds < 60:
        return "in less than a minute"

    minutes = round(seconds / 60)
    if minutes == 1:
        return "in about a minute"
    if minutes < 60:
        return f"in about {minutes} minutes"

    hours = round(seconds / 3600)
    if hours == 1:
        return "in about an hour"
    return f"in about {hours} hours"


@callback
def async_raise_daily_quota_issue(
    hass: HomeAssistant, entry: ConfigEntry, retry_after: int
) -> None:
    """The daily-quota 429: the account's reading allowance for today is spent.

    Raised ONLY for QuotaEnforcementMiddleware's 429. The write buffer's 429
    carries the same status code and is not a billing fact at all; routing it
    here produced a card that told a customer their allowance was gone and
    linked them at a larger plan, five seconds into a blip of our own making.
    """
    ir.async_create_issue(
        hass,
        DOMAIN,
        issue_id(entry, ISSUE_DAILY_QUOTA),
        is_fixable=False,
        is_persistent=False,
        severity=ir.IssueSeverity.WARNING,
        translation_key=ISSUE_DAILY_QUOTA,
        translation_placeholders={"when": describe_wait(retry_after)},
        learn_more_url=PRICING_URL,
    )


@callback
def async_raise_forbidden_issue(
    hass: HomeAssistant, entry: ConfigEntry, message: str
) -> None:
    """403/402: a valid key that is not allowed to write right now."""
    ir.async_create_issue(
        hass,
        DOMAIN,
        issue_id(entry, ISSUE_WRITE_FORBIDDEN),
        is_fixable=False,
        is_persistent=False,
        severity=ir.IssueSeverity.ERROR,
        translation_key=ISSUE_WRITE_FORBIDDEN,
        # Verbatim. The API already tells the user which of the two causes it
        # is and exactly what to do; rewording it here would create a fifth
        # customer-facing surface that can drift away from the code.
        translation_placeholders={"message": message or "no reason given"},
    )


@callback
def async_maybe_raise_unavailable_issue(
    hass: HomeAssistant, entry: ConfigEntry, failing_for_seconds: float
) -> None:
    """503 and the write-buffer 429: our saturation. Quiet unless it persists.

    Says nothing about plans, allowances or upgrading, because neither of the
    two answers that land here has anything to do with any of them.
    """
    if failing_for_seconds < SERVICE_UNAVAILABLE_ISSUE_AFTER_SECONDS:
        return

    ir.async_create_issue(
        hass,
        DOMAIN,
        issue_id(entry, ISSUE_SERVICE_UNAVAILABLE),
        is_fixable=False,
        is_persistent=False,
        severity=ir.IssueSeverity.WARNING,
        translation_key=ISSUE_SERVICE_UNAVAILABLE,
        translation_placeholders={
            "minutes": str(int(failing_for_seconds // 60)),
        },
    )


@callback
def async_clear_write_issues(hass: HomeAssistant, entry: ConfigEntry) -> None:
    """A clean write means none of the four problems is still true."""
    for kind in _WRITE_ISSUES:
        ir.async_delete_issue(hass, DOMAIN, issue_id(entry, kind))


class TagQuotaRepairFlow(RepairsFlow):
    """Reopen the entity picker so the user can deselect down to budget."""

    def __init__(self, entry: ConfigEntry) -> None:
        self._entry = entry

    async def async_step_init(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        return await self.async_step_confirm()

    async def async_step_confirm(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        current: list[str] = list(self._entry.options.get(CONF_ENTITIES, []))

        if user_input is not None:
            chosen = list(user_input[CONF_ENTITIES])
            self.hass.config_entries.async_update_entry(
                self._entry,
                options={
                    **self._entry.options,
                    CONF_ENTITIES: chosen,
                    # An entity no longer selected sends nothing, under any name.
                    CONF_SEND_AS: {
                        entity_id: history
                        for entity_id, history in send_as(self._entry.options).items()
                        if entity_id in chosen
                    },
                },
            )
            # The issue describes a state that the new selection may have just
            # ended. Clear it and let the next write decide whether it is true
            # again - guessing here would either leave a stale card or hide a
            # real problem.
            async_clear_write_issues(self.hass, self._entry)
            await self.hass.config_entries.async_reload(self._entry.entry_id)
            return self.async_create_entry(title="", data={})

        return self.async_show_form(
            step_id="confirm",
            data_schema=vol.Schema(
                {
                    vol.Required(CONF_ENTITIES, default=current): EntitySelector(
                        EntitySelectorConfig(multiple=True, include_entities=current)
                    )
                }
            ),
        )


class EntityRenamedRepairFlow(RepairsFlow):
    """An entity was renamed in Home Assistant: carry its history over, or not.

    Two choices, and the card says what each does. Carrying it over renames
    the tag in Tag Historian (or merges it into the tag the new name has
    already) - nothing is copied, the history reads under the new name, and
    the readings go under the new name from then on. Not carrying it over
    sends the readings under the new name from now on as a new tag, and leaves
    the history where it is, under the old one.
    """

    def __init__(
        self, entry: ConfigEntry, entity_id: str, history: str, merge: bool, refusal: str
    ) -> None:
        self._entry = entry
        self._entity_id = entity_id
        self._history = history
        self._merge = merge
        # What Tag Historian said when it was asked automatically and refused,
        # so the card does not offer as new a choice that has been tried.
        self._refusal = refusal

    def _placeholders(self) -> dict[str, str]:
        return renamed_placeholders(self._entity_id, self._history, self._refusal or None)

    async def async_step_init(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        if self._merge:
            return self.async_show_menu(
                step_id="merge_choice",
                menu_options=["merge_tags", "keep_apart"],
                description_placeholders=self._placeholders(),
            )
        return self.async_show_menu(
            step_id="rename_choice",
            menu_options=["rename_tag", "new_tag"],
            description_placeholders=self._placeholders(),
        )

    async def async_step_rename_tag(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        return await self._carry()

    async def async_step_merge_tags(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        return await self._carry()

    async def _carry(self) -> FlowResult:
        result = await async_carry_history(
            self.hass, self._entry, self._entity_id, self._history, merge=self._merge
        )
        if result.outcome in (LineageOutcome.DONE, LineageOutcome.NOT_FOUND):
            return self.async_create_entry(data={})
        if result.outcome is LineageOutcome.UNAVAILABLE:
            # Nothing changed, and nothing is lost meanwhile: the readings go
            # on into the old tag. The card stays for another try.
            return self.async_abort(reason="carry_unavailable")
        return self.async_abort(
            reason="carry_refused",
            description_placeholders={"message": result.message or "no reason given"},
        )

    async def async_step_new_tag(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        await async_stop_carrying(self.hass, self._entry, self._entity_id)
        return self.async_create_entry(data={})

    async def async_step_keep_apart(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        # The readings already go under the new name, into its own history;
        # leaving the old tag as it is takes nothing more than closing the card.
        return self.async_create_entry(data={})


async def async_create_fix_flow(
    hass: HomeAssistant,
    issue_id_: str,
    data: dict[str, str | int | float | None] | None,
) -> RepairsFlow:
    """Home Assistant's entry point for a fixable issue."""
    entry_id = (data or {}).get("entry_id")
    entry = (
        hass.config_entries.async_get_entry(str(entry_id)) if entry_id else None
    )
    if entry is None:
        # The entry was removed while the card was on screen. Confirming is
        # then the only honest option - there is nothing left to reconfigure.
        return ConfirmRepairFlow()
    if issue_id_.startswith(f"{ISSUE_ENTITY_RENAMED}_"):
        return EntityRenamedRepairFlow(
            entry,
            str((data or {}).get("entity_id")),
            str((data or {}).get("history")),
            (data or {}).get("merge") == "yes",
            str((data or {}).get("refusal") or ""),
        )
    return TagQuotaRepairFlow(entry)
