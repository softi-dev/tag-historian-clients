"""Follow an entity that is renamed in Home Assistant, history and all.

A tag's name is its entity's id (line_protocol.tag_name_for_entity), and the
forwarder subscribes to exactly the selected entity ids. Rename an entity and
both break at once: the subscription is to an id that no longer exists, so the
readings simply stopped - silently, with every counter at zero - and if the new
id was ever selected, it started a second tag beside the first, the history
split in two and a second tag slot spent.

This module is the fix, in three parts:

* **The selection follows the rename.** Home Assistant announces it on the
  entity registry (``EVENT_ENTITY_REGISTRY_UPDATED``, action ``update``, with
  ``old_entity_id``); a selected entity's old id is replaced by its new one,
  and the forwarder - rebuilt by the reload the options change causes - is
  subscribed to the entity again.

* **Its readings go on landing in its history.** Until the tag has the new
  name too, readings of the renamed entity are sent under the OLD tag
  (``send_as``): no gap, no second tag, nothing refused on a plan with no slot
  free.

* **The tag is renamed in Tag Historian** - ``POST /api/tags/rename``, or a
  merge when the new name is a tag already - so the history goes on under the
  new name. Automatically when the user opted in (``follow_renames``),
  otherwise offered on a repair card, because renaming a tag is a decision
  about their data that a rename made for tidiness in Home Assistant does not
  have to imply. Either way nothing is lost while it waits: the readings are
  stored under the old name, and the server redirects whatever is still sent
  under an old name once the tag has been renamed.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import CALLBACK_TYPE, Event, HomeAssistant, callback
from homeassistant.helpers import entity_registry as er
from homeassistant.helpers import issue_registry as ir
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.helpers.event import async_call_later

from .api import (
    LineageOutcome,
    LineageResult,
    TagHistorianAuthError,
    TagHistorianClient,
    TagHistorianError,
)
from .const import (
    CONF_API_KEY,
    CONF_ENTITIES,
    CONF_FOLLOW_RENAMES,
    CONF_HOST,
    CONF_SEND_AS,
    DOMAIN,
    ISSUE_ENTITY_RENAMED,
    LOGGER,
    RENAME_RETRY_SECONDS,
)
from .line_protocol import tag_name_for_entity

if TYPE_CHECKING:
    from collections.abc import Mapping


def send_as(options: Mapping[str, Any]) -> dict[str, str]:
    """{entity_id: entity_id whose tag its readings are sent under}, from the options."""
    raw = options.get(CONF_SEND_AS) or {}
    return {str(k): str(v) for k, v in raw.items()}


def renamed_issue_id(entry: ConfigEntry, entity_id: str) -> str:
    """One card per renamed entity, per account."""
    return f"{ISSUE_ENTITY_RENAMED}_{entry.entry_id}_{entity_id}"


def _client(hass: HomeAssistant, entry: ConfigEntry) -> TagHistorianClient:
    """The entry's client while it is loaded, a fresh one over the same session otherwise."""
    runtime = getattr(entry, "runtime_data", None)
    client = getattr(runtime, "client", None)
    if client is not None:
        return client
    return TagHistorianClient(
        async_get_clientsession(hass), entry.data[CONF_HOST], entry.data[CONF_API_KEY]
    )


@callback
def async_track_renames(hass: HomeAssistant, entry: ConfigEntry) -> CALLBACK_TYPE:
    """Listen for selected entities being renamed. Returns the unsubscribe."""

    @callback
    def _is_a_rename_of_ours(event_data: Mapping[str, Any]) -> bool:
        return (
            event_data.get("action") == "update"
            and "old_entity_id" in event_data
            and event_data["old_entity_id"] in entry.options.get(CONF_ENTITIES, [])
        )

    @callback
    def _renamed(event: Event) -> None:
        # NOT an entry background task. The first thing the handler does is
        # change the options, which reloads the entry - and an unload cancels
        # the entry's background tasks, this one included, before it could
        # ask Tag Historian to rename the tag.
        hass.async_create_background_task(
            async_entity_renamed(
                hass, entry, event.data["old_entity_id"], event.data["entity_id"]
            ),
            "tag_historian_entity_renamed",
        )

    return hass.bus.async_listen(
        er.EVENT_ENTITY_REGISTRY_UPDATED, _renamed, event_filter=_is_a_rename_of_ours
    )


async def async_entity_renamed(
    hass: HomeAssistant, entry: ConfigEntry, old: str, new: str
) -> None:
    """A selected entity was renamed from ``old`` to ``new`` in Home Assistant."""
    selected = list(entry.options.get(CONF_ENTITIES, []))
    if old not in selected:
        return

    mapping = send_as(entry.options)
    # The entity whose TAG the readings were going to: the old id, or - for an
    # entity renamed twice before its tag was - wherever the first rename left
    # them going. The card about the first rename is about a name that is gone.
    history = mapping.pop(old, old)
    ir.async_delete_issue(hass, DOMAIN, renamed_issue_id(entry, old))

    # The new id is in the selection already when it was selected for an
    # entity that has since gone - a meter that was replaced, its successor
    # renamed to the old meter's id. Its tag is the history to continue, and
    # this one's is merged into it.
    merge = new in selected
    selected = list(dict.fromkeys(new if entity_id == old else entity_id for entity_id in selected))

    client = _client(hass, entry)
    carry = True
    if new == history:
        # Renamed back to the name its tag already has.
        carry = False
    elif merge and not await _has_a_tag(client, tag_name_for_entity(new)):
        # Selected under that name, but nothing was ever stored under it:
        # there is nothing to merge into, and a rename does the job.
        merge = False

    if carry and not merge:
        if await _has_a_tag(client, tag_name_for_entity(history)):
            mapping[new] = history
        else:
            # Nothing was ever stored under the old name, so there is no
            # history to carry: the entity simply goes on under its new one.
            carry = False

    LOGGER.info(
        "Tag Historian: %s was renamed to %s; %s",
        old,
        new,
        "its readings go on into the tag of " + history if new in mapping else "following it",
    )

    # Last, because it reloads the entry: the forwarder is rebuilt subscribed
    # to the new id and sending under the right tag.
    hass.config_entries.async_update_entry(
        entry,
        options={**entry.options, CONF_ENTITIES: selected, CONF_SEND_AS: mapping},
    )

    if not carry:
        return

    if not entry.options.get(CONF_FOLLOW_RENAMES):
        async_raise_renamed_issue(hass, entry, new, history, merge=merge)
        return

    if not merge:
        # Nothing to do here: the reload the options change above causes
        # finds the entity sent under its old tag and asks Tag Historian to
        # rename it (async_resume) - one place that asks, retries included,
        # rather than two racing each other with the same request.
        return

    # A merge leaves nothing in the options to take it up again, so it is
    # asked for now, and a refusal or an unreachable API becomes the card.
    result = await async_carry_history(hass, entry, new, history, merge=True)
    if result.outcome not in (LineageOutcome.DONE, LineageOutcome.NOT_FOUND):
        async_raise_renamed_issue(
            hass,
            entry,
            new,
            history,
            merge=True,
            refusal=result.message if result.outcome is LineageOutcome.REFUSED else None,
        )


async def _has_a_tag(client: TagHistorianClient, tag_name: str) -> bool:
    """Whether Tag Historian has a tag of that name. Yes when it cannot say:
    sending readings into an existing history is never wrong, starting a new
    one beside it is the thing this module exists to prevent."""
    try:
        return tag_name in await client.async_get_tag_names()
    except TagHistorianError:
        return True


async def async_carry_history(
    hass: HomeAssistant, entry: ConfigEntry, entity_id: str, history: str, *, merge: bool
) -> LineageResult:
    """Ask Tag Historian to put ``history``'s tag under ``entity_id``'s name.

    A rename, or - when the new name is a tag already - a merge. Done (or
    nothing to carry), the readings of ``entity_id`` are sent under its own
    name from now on and the card goes. Anything else is handed back for the
    caller to say.
    """
    client = _client(hass, entry)
    old_tag = tag_name_for_entity(history)
    new_tag = tag_name_for_entity(entity_id)

    try:
        if merge:
            result = await client.async_merge_tags(old_tag, new_tag)
        else:
            result = await client.async_rename_tag(old_tag, new_tag)
            if result.outcome is LineageOutcome.TARGET_EXISTS:
                # Something has written under the new name already - the
                # entity was selected under it once, or a reading got there
                # first. Its history and this one are one thing now.
                result = await client.async_merge_tags(old_tag, new_tag)
    except TagHistorianAuthError:
        entry.async_start_reauth(hass)
        return LineageResult(LineageOutcome.UNAVAILABLE, "authentication failed")

    if result.outcome in (LineageOutcome.DONE, LineageOutcome.NOT_FOUND):
        LOGGER.info("Tag Historian: the history of %s now continues as %s", old_tag, new_tag)
        if hass.config_entries.async_get_entry(entry.entry_id) is None:
            # The entry was removed while the request was out; there is no
            # selection left to update, and its cards went with it.
            return result
        mapping = send_as(entry.options)
        if mapping.get(entity_id) == history:
            del mapping[entity_id]
            hass.config_entries.async_update_entry(
                entry, options={**entry.options, CONF_SEND_AS: mapping}
            )
        ir.async_delete_issue(hass, DOMAIN, renamed_issue_id(entry, entity_id))
    elif result.outcome is LineageOutcome.UNAVAILABLE:
        LOGGER.info(
            "Tag Historian could not rename %s to %s now (%s); readings go on into %s meanwhile",
            old_tag,
            new_tag,
            result.message,
            old_tag,
        )
    else:
        LOGGER.warning(
            "Tag Historian refused to rename %s to %s: %s", old_tag, new_tag, result.message
        )

    return result


async def async_stop_carrying(hass: HomeAssistant, entry: ConfigEntry, entity_id: str) -> None:
    """Send ``entity_id`` under its own name from now on, leaving the old tag as it is."""
    mapping = send_as(entry.options)
    if mapping.pop(entity_id, None) is not None:
        hass.config_entries.async_update_entry(
            entry, options={**entry.options, CONF_SEND_AS: mapping}
        )


@callback
def async_raise_renamed_issue(
    hass: HomeAssistant,
    entry: ConfigEntry,
    entity_id: str,
    history: str,
    *,
    merge: bool,
    refusal: str | None = None,
) -> None:
    """The card that offers to carry the history over - fixable, and kept across restarts.

    Persistent, because the decision it asks for is still open after a
    restart, and the readings are going into the old tag until it is made.
    """
    ir.async_create_issue(
        hass,
        DOMAIN,
        renamed_issue_id(entry, entity_id),
        is_fixable=True,
        is_persistent=True,
        severity=ir.IssueSeverity.WARNING,
        translation_key=ISSUE_ENTITY_RENAMED,
        translation_placeholders=renamed_placeholders(entity_id, history, refusal),
        data={
            "entry_id": entry.entry_id,
            "entity_id": entity_id,
            "history": history,
            "merge": "yes" if merge else "no",
            "refusal": refusal or "",
        },
    )


def renamed_placeholders(entity_id: str, history: str, refusal: str | None = None) -> dict[str, str]:
    """What the card and its fix flow print."""
    return {
        "old": history,
        "new": entity_id,
        "old_tag": tag_name_for_entity(history),
        "new_tag": tag_name_for_entity(entity_id),
        "refusal": f"\n\nTag Historian said, when asked: {refusal}" if refusal else "",
    }


@callback
def async_resume(hass: HomeAssistant, entry: ConfigEntry) -> None:
    """At setup: carry on with renames that were waiting.

    With ``follow_renames`` on, every entity still sent under an old tag is
    a rename Tag Historian has not confirmed - it could not be reached, or the
    option was switched on after the rename - so it is asked again. With it
    off, each one gets its card back if it was lost.
    """
    pending = send_as(entry.options)
    if not pending:
        return

    if not entry.options.get(CONF_FOLLOW_RENAMES):
        for entity_id, history in pending.items():
            if ir.async_get(hass).async_get_issue(DOMAIN, renamed_issue_id(entry, entity_id)) is None:
                async_raise_renamed_issue(hass, entry, entity_id, history, merge=False)
        return

    async def _carry_all() -> None:
        for entity_id, history in pending.items():
            result = await async_carry_history(hass, entry, entity_id, history, merge=False)
            if result.outcome is LineageOutcome.UNAVAILABLE:
                _retry_later(hass, entry)
                return
            if result.outcome is LineageOutcome.REFUSED:
                async_raise_renamed_issue(
                    hass, entry, entity_id, history, merge=False, refusal=result.message
                )

    hass.async_create_background_task(_carry_all(), "tag_historian_resume_renames")


@callback
def _retry_later(hass: HomeAssistant, entry: ConfigEntry) -> None:
    """Ask again in a while (:func:`async_resume`) - unless the entry has gone away by then."""

    @callback
    def _again(_now: object) -> None:
        if entry.state.recoverable and hass.config_entries.async_get_entry(entry.entry_id) is not None:
            async_resume(hass, entry)

    entry.async_on_unload(async_call_later(hass, RENAME_RETRY_SECONDS, _again))


@callback
def async_clear_renamed_issues(hass: HomeAssistant, entry: ConfigEntry) -> None:
    """Every rename card of this entry - for an entry that is being removed."""
    prefix = f"{ISSUE_ENTITY_RENAMED}_{entry.entry_id}_"
    for domain, issue in list(ir.async_get(hass).issues):
        if domain == DOMAIN and issue.startswith(prefix):
            ir.async_delete_issue(hass, DOMAIN, issue)
