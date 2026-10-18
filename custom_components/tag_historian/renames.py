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

* **Its readings go on landing in its history.** Until Tag Historian has
  confirmed that the tag has the new name too, readings of the renamed entity
  are sent under the OLD tag (``send_as``): no gap, no second tag, nothing
  refused on a plan with no slot free. That holds for a merge as well: the
  readings go on into the tag they were going to until the merge is done or
  the user has said to keep the two apart.

* **The tag is renamed in Tag Historian** - ``POST /api/tags/rename``, or a
  merge when the new name is a tag already - so the history goes on under the
  new name. Automatically when the user opted in (``follow_renames``),
  otherwise offered on a repair card, because renaming a tag is a decision
  about their data that a rename made for tidiness in Home Assistant does not
  have to imply. Either way nothing is lost while it waits: the readings are
  stored under the old name, and the server redirects whatever is still sent
  under an old name once the tag has been renamed.

TWO RULES, each the fix of a way renames were lost:

1. **The options change is made synchronously, in the event callback, before
   anything is awaited.** It used to be made at the end of a task that first
   asked Tag Historian which tags exist. Renaming a device renames all of its
   entities within milliseconds, one registry event each; the tasks overlapped,
   each wrote back the selection it had read before its await, and the last
   one to finish put the other entities' OLD ids back. Those entities were
   never subscribed again - the very silence this module exists to end. Now
   each event updates the options it finds, there and then, and everything
   that needs the network happens afterwards (:func:`async_resume`), one entry
   at a time under a lock, from a fresh read of the options.

2. **The listener belongs to the integration, not to a loaded entry.** It
   used to be registered at the end of the entry's setup and removed on
   unload - and every handled rename changes the options, which reloads the
   entry. A second rename arriving during that reload, or while the entry was
   waiting to retry its setup because Tag Historian was unreachable, found
   nobody listening and was dropped. :func:`async_setup_rename_tracking` is
   called once from ``async_setup`` and listens for as long as Home Assistant
   runs, for every entry, whatever state it is in.

And what is SAVED follows too. The forwarder's buffer and watermarks and the
history import's progress outlive the run that wrote them, keyed by entity id
- and the rename's own reload is one of the runs they outlive. Read back
under the old id, the renamed entity had no watermark, so its gap was read
from when the new forwarder started and whatever was evicted before that was
never read back; its held readings were not its own, so the line the history
import stops at (StateForwarder.history_boundary) passed over readings the
forwarder still had to deliver; and a running import carried on reading an
id the recorder no longer holds anything under, because Home Assistant moves
an entity's recorded history to its new id when it is renamed. So both save
the entity registry's own id for each entity beside it
(:func:`registry_ids`), which a rename does not change, and find each one's
id as it is now when they read it back (:func:`renamed_since`) - or that it
is gone, and its id another entity's now (:func:`removed_since`).

One answer on the card outlives the rename it settles. "Start a new tag" and
"Keep them apart" leave the history in the old tag - and Home Assistant has
moved that history to the new id, where the history import read it and sent
all of it again, into the new tag the card had just promised would hold the
readings from now on. The choice is kept (:func:`own_tag_since`), and the
import leaves such an entity alone.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any, Final

from homeassistant.config_entries import ConfigEntry, ConfigEntryState
from homeassistant.core import Event, HomeAssistant, callback
from homeassistant.helpers import entity_registry as er
from homeassistant.helpers import issue_registry as ir
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.helpers.event import async_call_later
from homeassistant.util import dt as dt_util

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
    CONF_OWN_TAG_SINCE,
    CONF_SEND_AS,
    DOMAIN,
    ISSUE_ENTITY_RENAMED,
    LOGGER,
    RENAME_RETRY_SECONDS,
)
from .line_protocol import tag_name_for_entity

if TYPE_CHECKING:
    from collections.abc import Collection, Iterable, Mapping

# hass.data key: {entry_id: asyncio.Lock}. NOT hass.data[DOMAIN], which holds
# one ForwarderStats per entry id and nothing else (async_get_stats).
_LOCKS: Final = f"{DOMAIN}_rename_locks"


def send_as(options: Mapping[str, Any]) -> dict[str, str]:
    """{entity_id: entity_id whose tag its readings are sent under}, from the options."""
    raw = options.get(CONF_SEND_AS) or {}
    return {str(k): str(v) for k, v in raw.items()}


def own_tag_since(options: Mapping[str, Any]) -> dict[str, str]:
    """{entity_id: when (ISO)} its readings went into a tag of their own, from the options.

    Set by the card's "Start a new tag" and "Keep them apart"
    (:func:`async_stop_carrying`). Never by a rename that found no history
    under the old name - the recorder's history of the entity is in no tag
    at all then, and the new one is where it belongs - nor by one whose old
    name is the new one's already, one history under the new name.
    """
    raw = options.get(CONF_OWN_TAG_SINCE) or {}
    return {str(k): str(v) for k, v in raw.items()}


def registry_ids(hass: HomeAssistant, entity_ids: Iterable[str]) -> dict[str, str]:
    """{entity_id: its entity registry id}, for those of them in the registry.

    The registry id is the one thing about an entity a rename does not
    change. An entity without a registry entry (no unique id) cannot be
    renamed in Settings, so there is nothing to follow for it.
    """
    registry = er.async_get(hass)
    return {
        entity_id: entry.id
        for entity_id in entity_ids
        if (entry := registry.async_get(entity_id)) is not None
    }


def renamed_since(
    hass: HomeAssistant, saved: Mapping[str, str], selected: Collection[str]
) -> dict[str, str]:
    """{saved entity id: the id it has now}, for each entity renamed since it was saved.

    ``saved`` is what :func:`registry_ids` gave when it was saved. Only a
    new id in ``selected`` counts - the selection the caller was built from,
    which a rename has updated by the time the reload it causes reads it. A
    rename that lands while an entry is being set up has changed the
    registry but not yet that selection; followed now, the entity would be
    sent under a name no part of this setup knows, and its readings to a tag
    of their own, for the moment until the rename's own reload. Left alone,
    that reload follows it.
    """
    registry = er.async_get(hass)
    renamed: dict[str, str] = {}
    for entity_id, registry_id in saved.items():
        entry = registry.async_get(registry_id)
        if entry is not None and entry.entity_id != entity_id and entry.entity_id in selected:
            renamed[entity_id] = entry.entity_id
    return renamed


def removed_since(hass: HomeAssistant, saved: Mapping[str, str]) -> set[str]:
    """The saved entity ids whose entity is no longer in the registry at all.

    What was saved for one of them belongs to an entity that is gone, not to
    whatever has its id now - the replaced meter, whose id its successor was
    given.
    """
    registry = er.async_get(hass)
    return {
        entity_id
        for entity_id, registry_id in saved.items()
        if registry.async_get(registry_id) is None
    }


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


def _lock(hass: HomeAssistant, entry: ConfigEntry) -> asyncio.Lock:
    """One at a time per entry: what is asked of Tag Historian about its renames."""
    return hass.data.setdefault(_LOCKS, {}).setdefault(entry.entry_id, asyncio.Lock())


@callback
def async_setup_rename_tracking(hass: HomeAssistant) -> None:
    """Listen for renamed entities on behalf of every entry, for as long as Home Assistant runs.

    Called once, from the integration's ``async_setup`` - see rule 2 in the
    module docstring for why it is not tied to an entry being loaded.
    """

    @callback
    def _is_a_rename(event_data: Mapping[str, Any]) -> bool:
        return event_data.get("action") == "update" and "old_entity_id" in event_data

    @callback
    def _renamed(event: Event) -> None:
        old, new = event.data["old_entity_id"], event.data["entity_id"]
        for entry in hass.config_entries.async_entries(DOMAIN):
            async_follow_rename(hass, entry, old, new)

    hass.bus.async_listen(er.EVENT_ENTITY_REGISTRY_UPDATED, _renamed, event_filter=_is_a_rename)


@callback
def async_follow_rename(hass: HomeAssistant, entry: ConfigEntry, old: str, new: str) -> None:
    """A selected entity was renamed from ``old`` to ``new``: follow it, now.

    Synchronous on purpose (rule 1 in the module docstring): it reads the
    options and writes them back with nothing awaited in between, so a second
    rename handled right after it starts from what this one wrote.

    What it does not do is ask Tag Historian anything. The options change
    reloads a loaded entry; an entry that is being set up, or waiting to retry
    its setup, reads the new options when it gets there; and every setup ends
    with :func:`async_resume`, which settles the rename - the card, or the
    request to rename the tag - from the options as they are by then.
    """
    selected = list(entry.options.get(CONF_ENTITIES, []))
    if old not in selected:
        return

    mapping = send_as(entry.options)
    # The entity whose TAG the readings were going to: the old id, or - for an
    # entity renamed twice before its tag was - wherever the first rename left
    # them going.
    history = mapping.pop(old, old)
    selected = list(dict.fromkeys(new if entity_id == old else entity_id for entity_id in selected))

    if new == history:
        # Renamed back to the name its tag already has: nothing to carry.
        mapping.pop(new, None)
    else:
        # Until Tag Historian has the new name too, the readings go on into
        # the history they were going to. That is right whether this turns out
        # to be a rename, a merge (the new id has a tag already) or nothing at
        # all (the old id never stored anything): sending into an existing
        # history is never wrong, starting a new one beside it is the thing
        # this module exists to prevent. async_resume corrects it once it
        # knows which.
        mapping[new] = history

    # Cards about either name describe a state that has just changed.
    ir.async_delete_issue(hass, DOMAIN, renamed_issue_id(entry, old))
    ir.async_delete_issue(hass, DOMAIN, renamed_issue_id(entry, new))

    LOGGER.info(
        "Tag Historian: %s was renamed to %s; %s",
        old,
        new,
        "its readings go on into the tag of " + history if new in mapping else "following it",
    )

    options = {**entry.options, CONF_ENTITIES: selected, CONF_SEND_AS: mapping}
    own = own_tag_since(entry.options)
    if old in own:
        # A tag of its own, chosen on a card before: what the recorder holds
        # of it from before that is still the old tag's, under whichever id.
        own[new] = own.pop(old)
        options[CONF_OWN_TAG_SINCE] = own
    hass.config_entries.async_update_entry(entry, options=options)


@callback
def async_resume(hass: HomeAssistant, entry: ConfigEntry) -> None:
    """At setup: settle every rename that is waiting.

    Every entity still sent under another entity's tag is a rename Tag
    Historian has not confirmed. With ``follow_renames`` on, it is asked to
    rename (or merge) the tag; with it off, the entity gets its card if it has
    none. Either way the first step is to find out what the old name is on
    the server - nothing at all, a tag, or already a former name of the new
    one - because each of those wants something different.

    NOT an entry background task: settling one rename can change the options,
    which reloads the entry, and an unload cancels the entry's background
    tasks - this one included, part-way through the next rename.
    """
    if send_as(entry.options):
        hass.async_create_background_task(
            _settle_all(hass, entry), "tag_historian_settle_renames"
        )


async def _settle_all(hass: HomeAssistant, entry: ConfigEntry) -> None:
    # Under the lock, and from a fresh read: a resume runs after every reload,
    # so several can be queued, and the ones behind the first find its work
    # done (the mapping gone, or the card there) and do nothing.
    async with _lock(hass, entry):
        if hass.config_entries.async_get_entry(entry.entry_id) is None:
            return
        follow = bool(entry.options.get(CONF_FOLLOW_RENAMES))
        issues = ir.async_get(hass)
        pending = {
            entity_id: history
            for entity_id, history in send_as(entry.options).items()
            if follow or issues.async_get_issue(DOMAIN, renamed_issue_id(entry, entity_id)) is None
        }
        if not pending:
            return

        try:
            # Every name on the account, former names included, to the name
            # of the tag it belongs to now.
            names = await _client(hass, entry).async_get_tag_index()
        except TagHistorianAuthError:
            # The quota poll asks for the new key; nothing is lost meanwhile.
            return
        except TagHistorianError:
            _retry_later(hass, entry)
            return

        for entity_id, history in pending.items():
            if send_as(entry.options).get(entity_id) != history:
                continue  # renamed again, or answered on its card, while we asked
            outcome = await _settle_one(hass, entry, entity_id, history, names, follow=follow)
            if outcome is LineageOutcome.UNAVAILABLE:
                _retry_later(hass, entry)
                return


async def _settle_one(
    hass: HomeAssistant,
    entry: ConfigEntry,
    entity_id: str,
    history: str,
    names: Mapping[str, str],
    *,
    follow: bool,
) -> LineageOutcome | None:
    old_tag, new_tag = tag_name_for_entity(history), tag_name_for_entity(entity_id)
    now_called = names.get(old_tag)

    if now_called is None or now_called == new_tag:
        # Nothing was ever stored under the old name - no history to carry,
        # the entity simply goes on under its new one - or the tag is under
        # the new name already: renamed in Explorer, or by a request whose
        # answer this integration never got to write down.
        async_stop_carrying(hass, entry, entity_id)
        ir.async_delete_issue(hass, DOMAIN, renamed_issue_id(entry, entity_id))
        return None

    # The new id has a tag of its own (or is a name of one): a merge, not a
    # rename. A replaced meter, its successor given the old meter's id.
    merge = new_tag in names

    if not follow:
        async_raise_renamed_issue(hass, entry, entity_id, history, merge=merge)
        return None

    result = await _carry(hass, entry, entity_id, history, merge=merge)
    if result.outcome is LineageOutcome.REFUSED:
        async_raise_renamed_issue(
            hass, entry, entity_id, history, merge=merge, refusal=result.message
        )
    return result.outcome


async def async_carry_history(
    hass: HomeAssistant, entry: ConfigEntry, entity_id: str, history: str, *, merge: bool
) -> LineageResult:
    """Ask Tag Historian to put ``history``'s tag under ``entity_id``'s name.

    A rename, or - when the new name is a tag already - a merge. Done (or
    nothing to carry), the readings of ``entity_id`` are sent under its own
    name from now on and the card goes. Anything else is handed back for the
    caller to say. For the repair card; :func:`async_resume` holds the lock
    already and calls :func:`_carry` itself.
    """
    async with _lock(hass, entry):
        return await _carry(hass, entry, entity_id, history, merge=merge)


async def _carry(
    hass: HomeAssistant, entry: ConfigEntry, entity_id: str, history: str, *, merge: bool
) -> LineageResult:
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
        # Read fresh: the entity may have been renamed again while the
        # request was out (a -> b being carried, then b -> c), and then the
        # mapping says c -> a. After a rename or merge the history it means
        # is under b, and asking for a -> c next would be refused - a is a
        # former name of b - so it is pointed at b. (NOT_FOUND moved nothing:
        # such a mapping is left for its own settling, which finds no history.)
        moved = result.outcome is LineageOutcome.DONE
        mapping = {
            renamed: (entity_id if moved and was == history else was)
            for renamed, was in send_as(entry.options).items()
            if not (renamed == entity_id and was == history)
        }
        mapping = {renamed: was for renamed, was in mapping.items() if renamed != was}
        if mapping != send_as(entry.options):
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


@callback
def async_stop_carrying(
    hass: HomeAssistant, entry: ConfigEntry, entity_id: str, *, own_tag: bool = False
) -> None:
    """Send ``entity_id`` under its own name from now on, leaving the old tag as it is.

    ``own_tag`` when that is the user's answer on the card, a new tag or a
    replaced meter kept apart: the old tag keeps the history, and the moment
    is kept with the options (:func:`own_tag_since`) so that the history
    import does not bring it into the new tag after all. Not when the old name
    turned out to hold nothing, or to be the new name's already: then there is
    no other tag for the history to be in.
    """
    mapping = send_as(entry.options)
    if mapping.pop(entity_id, None) is None:
        return
    options = {**entry.options, CONF_SEND_AS: mapping}
    if own_tag:
        options[CONF_OWN_TAG_SINCE] = {
            **own_tag_since(entry.options),
            entity_id: dt_util.utcnow().isoformat(),
        }
    hass.config_entries.async_update_entry(entry, options=options)


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
def _retry_later(hass: HomeAssistant, entry: ConfigEntry) -> None:
    """Ask again in a while (:func:`async_resume`) - if the entry is loaded by then.

    An entry that is not loaded then will be set up again some time, and
    every setup ends with :func:`async_resume`. The timer is the entry's, so
    an unload cancels it.
    """

    @callback
    def _again(_now: object) -> None:
        if entry.state is ConfigEntryState.LOADED:
            async_resume(hass, entry)

    entry.async_on_unload(async_call_later(hass, RENAME_RETRY_SECONDS, _again))


@callback
def async_clear_renamed_issues(hass: HomeAssistant, entry: ConfigEntry) -> None:
    """Every rename card of this entry - for an entry that is being removed."""
    prefix = f"{ISSUE_ENTITY_RENAMED}_{entry.entry_id}_"
    for domain, issue in list(ir.async_get(hass).issues):
        if domain == DOMAIN and issue.startswith(prefix):
            ir.async_delete_issue(hass, DOMAIN, issue)
    hass.data.get(_LOCKS, {}).pop(entry.entry_id, None)
