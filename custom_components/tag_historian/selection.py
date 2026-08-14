"""Decide which entities are worth spending a tag on.

Entity selection is not a setting in this integration, it is the product. A
typical Home Assistant install exposes several hundred entities and the plans
hold far fewer tags than that, so an integration that forwards everything puts
the user over quota before they have finished reading the success message.

The model, and the reason it is this one:

* **One selected entity is exactly one tag.** Each selected entity is encoded
  as a single line with a single ``value`` field, so the counter on the review
  screen is arithmetically true rather than approximately true. Home
  Assistant's built-in influxdb integration cannot make that promise - it sends
  every attribute as a field, and a ``climate`` entity whose state is "heat"
  fans out into one tag per numeric attribute.

* **The screen shows what is plausibly worth historising, not everything.** A
  336-entity house offering 333 rows with five ticked is not better than the
  YAML this replaces, it is the same problem with more clicks. So the default
  list is the measurement-shaped entities and the rest is one toggle away. See
  :func:`_classify` for the rule and why it is stated the way it is.

* **Hidden is not forbidden.** ``show_everything`` offers the whole install,
  and anything already selected is offered whatever the rule says about it, so
  the screen can never refuse a value it is itself displaying.

* **Ranked, then spread, then cut at the budget.** Score the candidates, sort
  deterministically, take one per device in turn, and let the budget decide
  where the line falls. The tiebreak is an ordinal comparison of the entity id,
  so the same house always yields the same pre-selection - a selection that
  reshuffles between two runs of the config flow is impossible to reason about.

Nothing here looks at what an entity's state happens to READ right now. A flat
battery, or a sensor that goes ``unknown`` at midnight, must not change what
the picker will accept: selectability is a property of the entity, and a
momentary state is not.

This module is pure: it takes plain dataclasses and returns plain dataclasses,
so the ranking can be tested without a running Home Assistant.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field

from .line_protocol import tag_name_for_entity

# Rank order. Lower sorts first, so lower gets the tag slot.
RANK_ENERGY = 10
RANK_CLIMATE = 20
RANK_METER = 30
RANK_MEASUREMENT = 40
RANK_OTHER_NUMERIC = 50
RANK_ONOFF = 60
RANK_HELPER = 70
# Offered in the picker, but never pre-ticked.
RANK_NOT_PRESELECTED = 900
# Not in the picker until the user asks for everything.
RANK_HIDDEN = 9000

# The categories exist to be counted back at the user ("18 energy and power,
# 12 temperature and humidity, ..."), so the label is part of the data. When a
# second language appears these move into translations/; with one language,
# a lookup table in translations that only ever resolves to English would be
# ceremony without a reader.
CATEGORY_LABELS: dict[str, str] = {
    "energy": "energy and power",
    "climate": "temperature and humidity",
    "meter": "meters and totals",
    "measurement": "other measurements",
    "numeric": "other numeric sensors",
    "onoff": "on/off states",
    "helper": "helpers",
    "other": "not pre-selected",
    "hidden": "shown only with everything",
}

# The domains that exist to REPORT something. Everything outside this set is a
# thing you operate (light, switch, lock, cover, media_player), a thing that
# runs (automation, script, scene), or a thing that describes the house rather
# than measuring it (person, zone, sun, weather, update).
#
# This is the whole D5 rule and it is deliberately about what the entity IS.
# The previous rule asked whether the current state string parsed as a number,
# and since "on", "home", "open" and "above_horizon" all do, every light,
# lock, automation and person in the house qualified.
_MEASUREMENT_DOMAINS = frozenset(
    {"sensor", "binary_sensor", "number", "input_number", "counter"}
)

# Device classes on a sensor that can never carry a reading, whatever the
# domain says. An enum sensor's states are words by definition, and a
# timestamp is an instant rather than a quantity.
_NEVER_NUMERIC_DEVICE_CLASSES = frozenset({"timestamp", "date", "enum"})

# Worth historising by default. Energy first because it is the reason most
# people go looking for long-term history in the first place.
_ENERGY_DEVICE_CLASSES = frozenset(
    {"energy", "power", "gas", "water", "energy_storage", "apparent_power"}
)
_CLIMATE_DEVICE_CLASSES = frozenset({"temperature", "humidity"})

# binary_sensor is where Home Assistant's own long-term statistics stop: it
# never gets them, whatever its device class. A heat pump's duty cycle is
# precisely what a historian is for, so these are first-class candidates here.
_ONOFF_DEVICE_CLASSES = frozenset(
    {
        "door",
        "window",
        "motion",
        "occupancy",
        "moisture",
        "smoke",
        "gas",
        "problem",
        "running",
        "power",
    }
)

# Numeric domains that can never carry a state_class, and so can never get
# Home Assistant's own long-term statistics either. Ranked last of the
# auto-ticked group rather than excluded.
_HELPER_DOMAINS = frozenset({"input_number", "counter", "number"})

# Offered, never pre-ticked. Not because they are worthless - a battery trend
# is genuinely interesting - but because on a 5-tag plan they would crowd out
# the sensor the user actually came here for, and every one of them is one
# click away in the picker.
_DEMOTED_DEVICE_CLASSES = frozenset({"battery", "signal_strength", "uptime"})


@dataclass(frozen=True)
class EntityFacts:
    """What selection needs to know about one entity.

    Assembled from the entity registry plus the current state machine by
    :mod:`.config_flow`. Kept as a plain dataclass so ranking is testable
    without a registry.

    Note what is NOT here: the entity's current state. Ranking that consulted
    it would hide a sensor for as long as it was unavailable and then offer a
    default the picker itself rejects.
    """

    entity_id: str
    device_class: str | None = None
    state_class: str | None = None
    entity_category: str | None = None
    friendly_name: str | None = None
    device_id: str | None = None

    @property
    def domain(self) -> str:
        return self.entity_id.partition(".")[0]


@dataclass(frozen=True)
class Candidate:
    """One entity that could be historised, with its rank and its tag name."""

    entity_id: str
    tag_name: str
    rank: int
    category: str
    friendly_name: str | None = None
    device_id: str | None = None

    @property
    def preselectable(self) -> bool:
        return self.rank < RANK_NOT_PRESELECTED

    @property
    def offered_by_default(self) -> bool:
        return self.rank < RANK_HIDDEN

    @property
    def device_key(self) -> str:
        """What counts as "one thing" when spreading the pre-selection.

        An entity with no device - a YAML template sensor, a helper - is its
        own thing rather than being lumped in with every other device-less
        entity, which would make the whole group share one slot per round.
        """
        return self.device_id or self.entity_id


@dataclass
class Proposal:
    """The pre-selection, and everything the screens need to explain it."""

    candidates: list[Candidate] = field(default_factory=list)
    offered: list[str] = field(default_factory=list)
    selected: list[str] = field(default_factory=list)
    budget: int = 0
    show_everything: bool = False

    @property
    def hidden_count(self) -> int:
        """Rows the picker is holding back until the user asks for them."""
        return max(0, len(self.candidates) - len(self.offered))

    @property
    def did_not_fit(self) -> int:
        """Auto-tickable candidates that the budget could not reach."""
        wanted = sum(1 for c in self.candidates if c.preselectable)
        return max(0, wanted - len(self.selected))

    def breakdown(self, entity_ids: Iterable[str] | None = None) -> str:
        """A human sentence fragment: '18 energy and power, 9 on/off states'.

        Describes ``entity_ids`` when given, so the screen can count back what
        is actually ticked rather than what was proposed - the two differ the
        moment the user edits the list and the form is redrawn.
        """
        by_category: dict[str, int] = {}
        selected = set(self.selected if entity_ids is None else entity_ids)
        for candidate in self.candidates:
            if candidate.entity_id in selected:
                by_category[candidate.category] = (
                    by_category.get(candidate.category, 0) + 1
                )

        # Category order, not count order: the sentence should read the same
        # way twice even when two categories tie.
        ordered = sorted(
            by_category.items(),
            key=lambda kv: min(
                c.rank for c in self.candidates if c.category == kv[0]
            ),
        )
        return ", ".join(
            f"{count} {CATEGORY_LABELS.get(category, category)}"
            for category, count in ordered
        )


def _classify(facts: EntityFacts) -> tuple[int, str]:
    """Return (rank, category) for one entity.

    Three outcomes, in descending order of how much of the screen they get:
    pre-ticked, offered, and hidden behind "show everything". Every branch
    decides from what the entity IS - its domain, its device class, its state
    class, and whether Home Assistant itself has marked it as plumbing.
    """
    domain = facts.domain

    # A config entity is a SETTING. ``number.heat_pump_curve`` and
    # ``select.mode`` are how you operate the device, not something it
    # reports, and historising a value you type in yourself is a chart of your
    # own keystrokes.
    if facts.entity_category == "config":
        return RANK_HIDDEN, "hidden"

    if domain not in _MEASUREMENT_DOMAINS:
        return RANK_HIDDEN, "hidden"

    if facts.device_class in _NEVER_NUMERIC_DEVICE_CLASSES:
        return RANK_HIDDEN, "hidden"

    # An entity_category marks something Home Assistant itself considers
    # plumbing. Every ESPHome, Shelly and Zigbee device now ships several, and
    # a diagnostic reading IS a reading - an rssi trend explains a dropout - so
    # these stay in the list, they just never spend a slot unasked.
    if facts.entity_category is not None:
        return RANK_NOT_PRESELECTED, "other"

    if facts.device_class in _DEMOTED_DEVICE_CLASSES:
        return RANK_NOT_PRESELECTED, "other"

    if domain == "binary_sensor":
        if facts.device_class in _ONOFF_DEVICE_CLASSES:
            return RANK_ONOFF, "onoff"
        return RANK_NOT_PRESELECTED, "other"

    if domain in _HELPER_DOMAINS:
        return RANK_HELPER, "helper"

    if facts.device_class in _ENERGY_DEVICE_CLASSES:
        return RANK_ENERGY, "energy"

    if facts.device_class in _CLIMATE_DEVICE_CLASSES:
        return RANK_CLIMATE, "climate"

    # state_class is Home Assistant's own answer to "is this worth keeping",
    # and it is on the registry entry, so it needs no guesswork on our part.
    if facts.state_class in ("total", "total_increasing"):
        return RANK_METER, "meter"

    if facts.state_class is not None and facts.device_class is not None:
        return RANK_MEASUREMENT, "measurement"

    if facts.state_class is not None:
        return RANK_OTHER_NUMERIC, "numeric"

    # A bare sensor with no metadata at all. Usually a YAML template sensor,
    # which is exactly the thing somebody hand-wrote because they wanted it -
    # so it is offered, and left for them to tick.
    return RANK_NOT_PRESELECTED, "other"


def rank_candidates(facts: list[EntityFacts]) -> list[Candidate]:
    """Rank every entity in the install, best first.

    Nothing is dropped. An entity that will never produce a reading ranks
    ``RANK_HIDDEN`` and stays out of the default picker, but it remains
    selectable under "show everything" - the screen decides what to show, it
    does not decide what is allowed to exist.
    """
    candidates = []
    for entry in facts:
        rank, category = _classify(entry)
        candidates.append(
            Candidate(
                entity_id=entry.entity_id,
                tag_name=tag_name_for_entity(entry.entity_id),
                rank=rank,
                category=category,
                friendly_name=entry.friendly_name,
                device_id=entry.device_id,
            )
        )

    # Ordinal entity_id as the tiebreak. Deterministic beats optimal here: a
    # pre-selection that comes out differently on a second run of the same
    # config flow is one nobody can check.
    candidates.sort(key=lambda c: (c.rank, c.entity_id))
    return candidates


def spread_across_devices(candidates: list[Candidate], budget: int) -> list[str]:
    """Take ``budget`` entities, one per device per pass, best-ranked first.

    Straight rank order fills the budget from whichever device happens to sort
    first: on five slots a house of metered plugs produced
    ``plug_0_energy, plug_0_power, plug_10_energy, plug_10_power,
    plug_11_energy`` - two and a half devices, one of them cut in half, and
    nothing at all from the other nine. On the tier most people start on, that
    is the first thing they ever see this integration do.

    So: pass over the devices in the order their best entity ranks, take one
    from each, then go round again. Five slots become five different things.
    Still fully deterministic - both the device order and the order within a
    device come from the ranking, which is itself tiebroken on entity id.
    """
    if budget <= 0:
        return []

    buckets: dict[str, list[str]] = {}
    device_order: list[str] = []
    for candidate in candidates:
        key = candidate.device_key
        if key not in buckets:
            buckets[key] = []
            device_order.append(key)
        buckets[key].append(candidate.entity_id)

    picked: list[str] = []
    depth = 0
    deepest = max((len(bucket) for bucket in buckets.values()), default=0)
    while len(picked) < budget and depth < deepest:
        for key in device_order:
            bucket = buckets[key]
            if depth < len(bucket):
                picked.append(bucket[depth])
                if len(picked) == budget:
                    return picked
        depth += 1
    return picked


def propose(
    facts: list[EntityFacts],
    budget: int,
    *,
    show_everything: bool = False,
    keep: Iterable[str] = (),
) -> Proposal:
    """Rank, decide what the picker shows, then pre-tick down to ``budget``.

    ``budget`` is the tag allowance still available on the account -
    ``tagLimit - currentTagCount`` from the API, never a number from this
    package.

    ``keep`` is whatever is already selected. Those entity ids are offered
    whatever the visibility rule says about them, including ids that match no
    entity at all any more: Home Assistant's EntitySelector enforces
    ``include_entities`` server-side, so a screen whose default is not in its
    own offer list raises ``vol.Invalid`` the moment the user presses Submit
    without touching anything.
    """
    candidates = rank_candidates(facts)

    kept = set(keep)
    offered = [
        c.entity_id
        for c in candidates
        if show_everything or c.offered_by_default or c.entity_id in kept
    ]
    # Selected entities whose entity no longer exists anywhere in the install.
    # Still offered, so the dialog opens; unticking one is how it goes away.
    known = {c.entity_id for c in candidates}
    offered += [entity_id for entity_id in keep if entity_id not in known]

    preselectable = [c for c in candidates if c.preselectable]
    return Proposal(
        candidates=candidates,
        offered=offered,
        selected=spread_across_devices(preselectable, budget),
        budget=budget,
        show_everything=show_everything,
    )


def trim_to_new_tag_budget(
    selected: list[str],
    candidates: list[Candidate],
    existing_tags: set[str],
    available: int,
) -> list[str]:
    """Cut a selection until the NEW tags it needs fit in ``available``.

    The unit matters and it is the one thing the previous version got wrong.
    The overflow is counted in new TAG NAMES - an entity whose tag already
    exists costs nothing, because the quota counts tags - but the cut was
    ``selected[:len(selected) - overflow]``, which is counted in ENTITIES. On
    one free slot and three selected entities, the lowest-ranked of which
    already had a tag, that removed the entity that was FREE and kept both of
    the ones that needed a slot: still over budget, and one fewer thing
    historised for it.

    So the loop is written against the number that actually decides the
    outcome, and it removes the lowest-ranked entity that NEEDS a new tag -
    never one that was costing nothing.
    """
    order = {c.entity_id: (c.rank, c.entity_id) for c in candidates}
    # Anything the user picked that ranking does not recognise sorts last,
    # rather than being silently discarded.
    ranked = sorted(
        selected, key=lambda e: order.get(e, (RANK_HIDDEN + 1, e))
    )

    def new_tags(entity_ids: list[str]) -> int:
        return sum(
            1
            for entity_id in entity_ids
            if tag_name_for_entity(entity_id) not in existing_tags
        )

    budget = max(0, available)
    while new_tags(ranked) > budget:
        for index in range(len(ranked) - 1, -1, -1):
            if tag_name_for_entity(ranked[index]) not in existing_tags:
                del ranked[index]
                break
        else:  # pragma: no cover - unreachable while new_tags() > budget >= 0
            break
    return ranked
