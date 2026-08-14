"""Ranking, what the picker shows, the budget cut, and the spread across devices."""

from __future__ import annotations

from custom_components.tag_historian.selection import (
    EntityFacts,
    propose,
    rank_candidates,
    trim_to_new_tag_budget,
)


def facts(entity_id: str, **kwargs) -> EntityFacts:
    return EntityFacts(entity_id=entity_id, **kwargs)


HOUSE = [
    facts("sensor.house_power", device_class="power", state_class="measurement"),
    facts("sensor.outdoor_temperature", device_class="temperature", state_class="measurement"),
    facts("sensor.water_meter", device_class="water", state_class="total_increasing"),
    facts("binary_sensor.heat_pump_running", device_class="running"),
    facts("input_number.setpoint"),
    facts("sensor.wifi_signal", device_class="signal_strength", state_class="measurement"),
    facts("sensor.phone_battery", device_class="battery", state_class="measurement"),
    facts("sensor.uptime_seconds", entity_category="diagnostic", state_class="measurement"),
    facts("sun.sun"),
    facts("climate.living_room"),
    facts("update.esphome_firmware"),
]


def offered(house, **kwargs) -> set[str]:
    return set(propose(house, budget=0, **kwargs).offered)


# --------------------------------------------------------------------------
# What the screen shows
# --------------------------------------------------------------------------


def test_things_you_operate_are_not_offered_by_default():
    """The picker is a list of measurements, not a list of entities.

    ``climate.living_room`` is the interesting case: through Home Assistant's
    built-in influxdb integration it fans out into one tag per numeric
    attribute, which is more than an entire small plan from a single entity.
    Here it is not on the screen unless asked for, which is what keeps "one
    selected entity, one tag" both true and useful.
    """
    shown = offered(HOUSE)
    assert "climate.living_room" not in shown
    assert "sun.sun" not in shown
    assert "update.esphome_firmware" not in shown
    assert "sensor.house_power" in shown


def test_show_everything_offers_everything():
    """Hidden is not forbidden. The escape hatch keeps the principle."""
    shown = offered(HOUSE, show_everything=True)
    assert shown == {entry.entity_id for entry in HOUSE}


def test_a_realistic_house_offers_a_small_fraction_of_itself():
    """336 entities offering 333 rows is the YAML problem with more clicks.

    The reviewer's install: a 336-entity house whose picker offered 333 rows
    with five ticked. This is that house. What makes it a defect rather than a
    long list is that every light, lock, automation, script and person in it
    qualified - not because anyone decided they were worth a tag, but because
    "on", "locked" and "home" happen to parse as numbers.
    """
    house: list[EntityFacts] = []

    # 100 lights, 60 switches, 44 automations, 12 scripts, 8 locks, 6 covers.
    house += [facts(f"light.light_{i}") for i in range(100)]
    house += [facts(f"switch.wall_{i}") for i in range(60)]
    house += [facts(f"automation.rule_{i}") for i in range(44)]
    house += [facts(f"script.script_{i}") for i in range(12)]
    house += [facts(f"lock.lock_{i}") for i in range(8)]
    house += [facts(f"cover.blind_{i}") for i in range(6)]

    # The plumbing every modern device ships with.
    house += [facts(f"update.firmware_{i}", entity_category="config") for i in range(20)]
    house += [facts(f"button.identify_{i}", entity_category="config") for i in range(12)]
    house += [facts(f"select.mode_{i}", entity_category="config") for i in range(10)]
    house += [facts(f"number.calibration_{i}", entity_category="config") for i in range(8)]
    house += [facts("sun.sun"), facts("person.alice"), facts("person.bob")]
    house += [facts(f"device_tracker.phone_{i}") for i in range(3)]
    house += [facts("climate.living_room"), facts("weather.home")]

    # And the reason anybody installed this: genuine measurements, plus the
    # diagnostics that come along with them.
    house += [
        facts(f"sensor.plug_{i}_power", device_class="power",
              state_class="measurement", device_id=f"plug_{i}")
        for i in range(12)
    ]
    house += [
        facts(f"sensor.plug_{i}_energy", device_class="energy",
              state_class="total_increasing", device_id=f"plug_{i}")
        for i in range(12)
    ]
    house += [
        facts(f"sensor.room_{i}_temperature", device_class="temperature",
              state_class="measurement", device_id=f"room_{i}")
        for i in range(8)
    ]
    house += [
        facts(f"sensor.room_{i}_battery", device_class="battery",
              state_class="measurement", entity_category="diagnostic",
              device_id=f"room_{i}")
        for i in range(8)
    ]
    house += [
        facts(f"binary_sensor.door_{i}", device_class="door") for i in range(6)
    ]
    house += [facts("sensor.last_boot", device_class="timestamp")]
    house += [facts("sensor.washer_program", device_class="enum")]

    assert len(house) == 336, "the reviewer's house, to the entity"

    shown = offered(house)
    everything = offered(house, show_everything=True)

    # The escape hatch really does offer the whole install.
    assert everything == {entry.entity_id for entry in house}

    # And the default screen is a small fraction of it. The bound is the point
    # of this test: 333 of 336 must not be reintroducible without failing here.
    assert len(shown) == 46
    assert len(shown) < len(house) / 4

    # Nothing you operate, run or configure.
    assert not [e for e in shown if e.startswith(("light.", "switch.", "automation."))]
    assert not [e for e in shown if e.startswith(("script.", "lock.", "cover."))]
    assert not [e for e in shown if e.startswith(("update.", "button.", "select."))]
    assert not [e for e in shown if e.startswith(("sun.", "person.", "device_tracker."))]
    assert "climate.living_room" not in shown
    assert "number.calibration_0" not in shown  # a setting, not a reading

    # A sensor whose state is words is not a series either.
    assert "sensor.last_boot" not in shown
    assert "sensor.washer_program" not in shown

    # Everything that measures something, including the diagnostics.
    assert "sensor.plug_0_power" in shown
    assert "sensor.room_0_temperature" in shown
    assert "sensor.room_0_battery" in shown
    assert "binary_sensor.door_0" in shown


def test_a_house_of_lights_and_automations_offers_nothing_by_default():
    """66 entities, none of them a measurement, and the picker said all 66."""
    house = (
        [facts(f"light.light_{i}") for i in range(30)]
        + [facts(f"automation.rule_{i}") for i in range(20)]
        + [facts(f"switch.wall_{i}") for i in range(10)]
        + [facts(f"script.script_{i}") for i in range(5)]
        + [facts("sun.sun")]
    )
    assert len(house) == 66

    assert offered(house) == set()
    assert len(offered(house, show_everything=True)) == 66


def test_a_bare_template_sensor_is_offered_but_not_ticked():
    """No unit, no device class, no state class - and hand-written on purpose."""
    house = [facts("sensor.boiler_efficiency")]
    proposal = propose(house, budget=5)
    assert proposal.offered == ["sensor.boiler_efficiency"]
    assert proposal.selected == []


def test_an_already_selected_entity_is_offered_whatever_the_rule_says():
    """The screen can never refuse a value it is itself displaying.

    EntitySelector enforces include_entities server-side with vol.In, so a
    default outside the offer list makes Submit-without-touching-anything
    raise vol.Invalid. That is the options flow refusing to open.
    """
    proposal = propose(HOUSE, budget=5, keep=["climate.living_room", "sun.sun"])
    assert "climate.living_room" in proposal.offered
    assert "sun.sun" in proposal.offered
    # Offered, but still never pre-ticked on its own account.
    assert "climate.living_room" not in proposal.selected


def test_an_entity_that_no_longer_exists_is_still_offered():
    """A selection saved against a device that has since been removed."""
    proposal = propose(HOUSE, budget=5, keep=["sensor.gone_for_good"])
    assert "sensor.gone_for_good" in proposal.offered


def test_hidden_count_is_what_the_toggle_would_add():
    proposal = propose(HOUSE, budget=5)
    assert proposal.hidden_count == len(HOUSE) - len(proposal.offered)
    assert proposal.hidden_count == 3  # sun, climate, update


# --------------------------------------------------------------------------
# Ranking and the budget
# --------------------------------------------------------------------------


def test_energy_outranks_temperature_outranks_helpers():
    ranked = [c.entity_id for c in rank_candidates(HOUSE)]
    assert ranked.index("sensor.house_power") < ranked.index("sensor.outdoor_temperature")
    assert ranked.index("sensor.outdoor_temperature") < ranked.index("input_number.setpoint")


def test_diagnostics_and_batteries_are_offered_but_never_preselected():
    """One click away, but never spending a slot the user did not ask to spend."""
    proposal = propose(HOUSE, budget=99)

    assert "sensor.phone_battery" in proposal.offered
    assert "sensor.wifi_signal" in proposal.offered
    assert "sensor.uptime_seconds" in proposal.offered

    assert "sensor.phone_battery" not in proposal.selected
    assert "sensor.wifi_signal" not in proposal.selected
    assert "sensor.uptime_seconds" not in proposal.selected


def test_preselection_stops_at_the_budget():
    """Both energy-class entities before either climate one, ordinal within rank."""
    proposal = propose(HOUSE, budget=2)
    assert proposal.selected == ["sensor.house_power", "sensor.water_meter"]
    assert proposal.did_not_fit == 3


def test_a_zero_budget_preselects_nothing():
    """An account already at its tag limit gets an empty picker, not a full one."""
    proposal = propose(HOUSE, budget=0)
    assert proposal.selected == []
    assert proposal.did_not_fit == 5


def test_selection_is_deterministic_for_the_same_registry():
    """The same house yields the same answer twice.

    Ordinal entity_id as the tiebreak, the same rule SparkplugSession uses
    before its own budget cut. A pre-selection that reshuffles between two runs
    of the config flow is one nobody can check.
    """
    shuffled = list(reversed(HOUSE))
    assert propose(HOUSE, 3).selected == propose(shuffled, 3).selected


def test_breakdown_counts_only_what_was_selected():
    proposal = propose(HOUSE, budget=3)
    assert proposal.breakdown() == "2 energy and power, 1 temperature and humidity"


def test_breakdown_can_describe_a_list_that_is_not_the_proposal():
    """The screen counts back what is ticked NOW, not what was proposed."""
    proposal = propose(HOUSE, budget=3)
    assert proposal.breakdown(["sensor.outdoor_temperature"]) == (
        "1 temperature and humidity"
    )


def test_candidates_carry_the_tag_name_they_will_become():
    candidates = {c.entity_id: c.tag_name for c in rank_candidates(HOUSE)}
    assert candidates["sensor.house_power"] == "sensor.house_power"


# --------------------------------------------------------------------------
# Spreading the pre-selection across devices
# --------------------------------------------------------------------------


PLUGS = [
    facts(f"sensor.plug_{i}_{kind}", device_class=dc, state_class=sc,
          device_id=f"plug_{i}")
    for i in range(12)
    for kind, dc, sc in (
        ("energy", "energy", "total_increasing"),
        ("power", "power", "measurement"),
    )
]


def test_five_slots_show_five_different_devices():
    """The free tier's first impression, and it used to look broken.

    Straight rank order filled all five slots from whichever device sorted
    first alphabetically: plug_0 twice, plug_10 twice, half of plug_11. Ten
    metered plugs in the house and the pre-selection showed two and a half of
    them, one cut in half.
    """
    selected = propose(PLUGS, budget=5).selected

    assert len(selected) == 5
    devices = [entity_id.rpartition("_")[0] for entity_id in selected]
    assert len(set(devices)) == 5, selected


def test_the_spread_is_still_deterministic():
    """Round-robin, not round-random: two runs of the flow agree."""
    assert propose(PLUGS, 5).selected == propose(list(reversed(PLUGS)), 5).selected
    assert propose(PLUGS, 5).selected == [
        "sensor.plug_0_energy",
        "sensor.plug_10_energy",
        "sensor.plug_11_energy",
        "sensor.plug_1_energy",
        "sensor.plug_2_energy",
    ]


def test_a_second_pass_only_starts_once_every_device_has_one():
    """Twelve devices, thirteen slots: everyone gets one before anyone gets two."""
    selected = propose(PLUGS, budget=13).selected
    assert len(selected) == 13
    assert sum(1 for e in selected if e.endswith("_energy")) == 12
    assert sum(1 for e in selected if e.endswith("_power")) == 1


def test_entities_with_no_device_are_each_their_own_thing():
    """Otherwise every YAML template sensor in the house shares one slot."""
    selected = propose(HOUSE, budget=3).selected
    assert selected == [
        "sensor.house_power",
        "sensor.water_meter",
        "sensor.outdoor_temperature",
    ]


# --------------------------------------------------------------------------
# Trimming
# --------------------------------------------------------------------------


def test_trim_keeps_the_best_ranked():
    candidates = rank_candidates(HOUSE)
    selected = [
        "input_number.setpoint",
        "sensor.house_power",
        "sensor.outdoor_temperature",
    ]
    assert trim_to_new_tag_budget(selected, candidates, set(), 2) == [
        "sensor.house_power",
        "sensor.outdoor_temperature",
    ]


def test_trim_counts_tags_and_never_removes_one_that_is_free():
    """The unit is new TAGS, and an entity whose tag exists costs nothing.

    One free slot, three selected, and the lowest-ranked of the three already
    has a tag. Trimming by entity count removed that free one and kept both of
    the ones needing a slot - still two new tags against one, so the first
    write was a 422 anyway, and the user lost an entity for nothing.
    """
    candidates = rank_candidates(HOUSE)
    selected = [
        "sensor.house_power",
        "sensor.outdoor_temperature",
        "input_number.setpoint",
    ]
    existing = {"input_number.setpoint"}

    trimmed = trim_to_new_tag_budget(selected, candidates, existing, 1)

    assert "input_number.setpoint" in trimmed, "it was free; removing it bought nothing"
    assert len(set(trimmed) - existing) == 1, "and the result has to actually fit"
    assert trimmed == ["sensor.house_power", "input_number.setpoint"]


def test_trim_leaves_a_selection_that_is_already_free_alone():
    """Every tag already exists: nothing needs a slot, so nothing is removed."""
    candidates = rank_candidates(HOUSE)
    selected = ["sensor.house_power", "sensor.outdoor_temperature"]
    existing = set(selected)
    assert trim_to_new_tag_budget(selected, candidates, existing, 0) == selected


def test_trim_can_come_back_empty_when_nothing_fits():
    """The caller has to be able to tell; creating the entry anyway is D8."""
    candidates = rank_candidates(HOUSE)
    assert trim_to_new_tag_budget(["sensor.house_power"], candidates, set(), 0) == []


def test_trim_does_not_silently_discard_unknown_entities():
    """Something the user picked that ranking does not recognise goes last."""
    candidates = rank_candidates(HOUSE)
    selected = ["sensor.mystery", "sensor.house_power"]
    assert trim_to_new_tag_budget(selected, candidates, set(), 2) == [
        "sensor.house_power",
        "sensor.mystery",
    ]
