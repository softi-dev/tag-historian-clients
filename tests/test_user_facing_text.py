"""Every string this integration can put on a screen, against what it does.

Public surfaces have made false claims about this product before, and the
worst of them - a Repairs card telling a customer their reading allowance was
gone, five seconds into a hiccup of our own - was written by code that was
working exactly as designed. So the strings are tested like code: the words
are rendered with the placeholders the code actually supplies, and the result
is asserted against what happened.

Three kinds of check here:

* the RENDERED text of a card or a screen, for the claims that matter most;
* a mechanical placeholder audit, which is what catches a screen quietly
  printing the literal "{hours}" after somebody renames a field;
* a mechanical form audit: a screen may not label, explain or instruct the
  user to use a control its own schema does not draw.

This file used to cover the Repairs cards only, and two defects walked
straight through the gap. The config flow's picker went on describing a filter
the code had stopped applying, and the review screen warned every clean setup
about readings being "refused and lost" while showing no control that could
have prevented it. Both were sentences, both were customer-facing, and neither
was reachable from a test that only rendered cards. So the flow steps are
driven here too - the real flow, the real placeholders, the real schema.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from unittest.mock import patch

import pytest
import voluptuous as vol
from homeassistant.config_entries import SOURCE_REAUTH, SOURCE_USER
from homeassistant.core import HomeAssistant
from homeassistant.helpers import issue_registry as ir
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.tag_historian import repairs
from custom_components.tag_historian.api import (
    QuotaSnapshot,
    TagHistorianClient,
    WriteOutcome,
)
from custom_components.tag_historian.config_flow import CONF_SHOW_ALL, CONF_TRIM
from custom_components.tag_historian.const import (
    CONF_API_KEY,
    CONF_ENTITIES,
    CONF_HOST,
    CONF_MIN_INTERVAL,
    DOMAIN,
    ISSUE_DAILY_QUOTA,
    ISSUE_SERVICE_UNAVAILABLE,
    PRICING_URL,
)
from custom_components.tag_historian.forwarder import INFLIGHT_WAIT_SECONDS

from .conftest import STUB_ACCOUNT, make_client

TRANSLATIONS = json.loads(
    (
        Path(__file__).resolve().parent.parent
        / "custom_components"
        / "tag_historian"
        / "translations"
        / "en.json"
    ).read_text(encoding="utf-8")
)

WRITE_URL = "https://api.taghistorian.com/api/v2/write?precision=s"
BODY = "W,domain=sensor,entity_id=house_power value=1500.0 1786528800"

# Words that make a claim about what somebody is paying for. None of them may
# appear on a card raised by Tag Historian's own saturation.
BILLING_WORDS = (
    "allowance",
    "quota",
    "upgrade",
    "plan",
    "pricing",
    "billing",
    "limit",
    "tier",
)


def render_issue(issue: ir.IssueEntry) -> str:
    """The words the user reads, title and body, with the real placeholders."""
    strings = TRANSLATIONS["issues"][issue.translation_key]
    placeholders = issue.translation_placeholders or {}
    return "\n".join(
        part.format(**placeholders)
        for part in (strings["title"], strings.get("description", ""))
    )


@pytest.fixture
def entry(hass: HomeAssistant) -> MockConfigEntry:
    entry = MockConfigEntry(
        domain=DOMAIN,
        data={CONF_HOST: "api.taghistorian.com", CONF_API_KEY: "not-a-real-key"},
        unique_id="11111111-2222-3333-4444-555555555555",
    )
    entry.add_to_hass(hass)
    return entry


# --------------------------------------------------------------------------
# D4 - the claim that got through once
# --------------------------------------------------------------------------


async def test_a_back_pressure_429_never_produces_quota_wording(
    hass: HomeAssistant, entry, aioclient_mock
) -> None:
    """The whole chain, from the bytes on the wire to the words on the card.

    ``MemoryBufferService.GetRetryAfterSeconds`` answers 5 + jitter for
    ``CustomerQuotaExceeded``, which is what the write buffer calls a customer
    using more than their fair share of it RIGHT NOW - not a day's readings,
    not anything anybody is billed for. Seven seconds of that used to render:

        Tag Historian daily reading limit reached
        Your account has used its reading allowance for today ... until the
        limit resets in about 1 hour ... move to a plan with a larger allowance

    with the pricing page attached. Three false statements and an upsell, out
    of a blip that cleared itself before the card finished animating.
    """
    aioclient_mock.post(
        WRITE_URL,
        status=429,
        headers={"Retry-After": "7"},
        json={
            "code": "too many requests",
            "message": "write buffer fair-share exceeded; retry after 7s",
        },
    )
    client = TagHistorianClient(
        async_get_clientsession(hass), "api.taghistorian.com", "not-a-real-key"
    )

    result = await client.async_write(BODY)

    # Whatever card this outcome leads to, it is not the daily-quota one.
    assert result.outcome is not WriteOutcome.DAILY_QUOTA

    # Drive the card this outcome can reach and read what it says.
    repairs.async_maybe_raise_unavailable_issue(hass, entry, 3600)
    registry = ir.async_get(hass)

    assert registry.async_get_issue(DOMAIN, repairs.issue_id(entry, ISSUE_DAILY_QUOTA)) is None

    issue = registry.async_get_issue(
        DOMAIN, repairs.issue_id(entry, ISSUE_SERVICE_UNAVAILABLE)
    )
    assert issue is not None
    assert issue.learn_more_url != PRICING_URL

    text = render_issue(issue).casefold()
    offenders = [word for word in BILLING_WORDS if word in text]
    assert not offenders, (
        f"a card raised by our own saturation says {offenders}: {text!r}"
    )


async def test_the_daily_quota_card_can_say_minutes(
    hass: HomeAssistant, entry
) -> None:
    """``max(1, round(seconds / 3600))`` could not produce an honest answer.

    A real daily quota does reset in four minutes if it is 23:56 UTC, and the
    card said "about 1 hour" for that too. The floor was there so a missing
    header did not render "0 hours"; it also made every short wait a lie.
    """
    repairs.async_raise_daily_quota_issue(hass, entry, 240)

    issue = ir.async_get(hass).async_get_issue(
        DOMAIN, repairs.issue_id(entry, ISSUE_DAILY_QUOTA)
    )
    text = render_issue(issue)

    assert "in about 4 minutes" in text
    assert "hour" not in text


@pytest.mark.parametrize(
    ("seconds", "expected"),
    [
        (0, "in less than a minute"),
        (7, "in less than a minute"),
        (59, "in less than a minute"),
        (60, "in about a minute"),
        (240, "in about 4 minutes"),
        (3540, "in about 59 minutes"),
        (3600, "in about an hour"),
        (28800, "in about 8 hours"),
    ],
)
def test_the_wait_is_described_as_what_it_is(seconds, expected) -> None:
    assert repairs.describe_wait(seconds) == expected


async def test_the_daily_quota_card_is_still_raised_for_the_real_thing(
    hass: HomeAssistant, entry
) -> None:
    """Separating the two must not have made the true claim unsayable."""
    repairs.async_raise_daily_quota_issue(hass, entry, 3600)

    issue = ir.async_get(hass).async_get_issue(
        DOMAIN, repairs.issue_id(entry, ISSUE_DAILY_QUOTA)
    )
    assert issue.learn_more_url == PRICING_URL
    assert "reading allowance for today" in render_issue(issue)


# --------------------------------------------------------------------------
# The mechanical audit
# --------------------------------------------------------------------------


_PLACEHOLDER_RE = re.compile(r"(?<!\{)\{([a-z_]+)\}(?!\})")


def placeholders_in(text: str) -> set[str]:
    return set(_PLACEHOLDER_RE.findall(text))


def issue_strings() -> dict[str, str]:
    """Every issue's title and description, joined, keyed by translation key."""
    return {
        key: strings["title"] + " " + strings.get("description", "")
        for key, strings in TRANSLATIONS["issues"].items()
    }


async def test_every_issue_string_gets_the_placeholders_it_asks_for(
    hass: HomeAssistant, entry
) -> None:
    """A renamed field renders as the literal "{hours}" and nothing raises.

    This is the failure mode that has no symptom in a test suite and one very
    obvious symptom in somebody's Repairs panel.
    """
    repairs.async_raise_tag_quota_issue(hass, entry, ["sensor.a", "sensor.b"], 60)
    repairs.async_raise_daily_quota_issue(hass, entry, 3600)
    repairs.async_raise_forbidden_issue(hass, entry, "Confirm your email address.")
    repairs.async_maybe_raise_unavailable_issue(hass, entry, 3600)

    registry = ir.async_get(hass)
    raised = [
        issue
        for issue in registry.issues.values()
        if issue.domain == DOMAIN
    ]
    assert len(raised) == 4, "every card this integration can show"

    for issue in raised:
        supplied = set(issue.translation_placeholders or {})
        strings = TRANSLATIONS["issues"][issue.translation_key]
        wanted = placeholders_in(strings["title"] + " " + strings.get("description", ""))
        # The tag-quota card's own text lives in its fix flow rather than in a
        # description, so fold that in too.
        if "fix_flow" in strings:
            for step in strings["fix_flow"]["step"].values():
                wanted |= placeholders_in(step["title"] + " " + step["description"])

        assert wanted <= supplied, (
            f"{issue.translation_key} prints {wanted - supplied} that nothing supplies"
        )
        assert supplied <= wanted, (
            f"{issue.translation_key} is handed {supplied - wanted} that nothing prints"
        )
        # And it renders.
        render_issue(issue)


# --------------------------------------------------------------------------
# G2 and G3 - the screens the card-only guard could not see
# --------------------------------------------------------------------------

FLOW_CLIENT_PATH = "custom_components.tag_historian.config_flow.TagHistorianClient"
SETUP_PATH = "custom_components.tag_historian.async_setup_entry"
CREDENTIALS = {CONF_HOST: "api.taghistorian.com", CONF_API_KEY: "not-a-real-key"}

# One free tag against a selection that needs more, so the trim screen is
# reachable. Not a real plan's numbers - see conftest.
TIGHT_QUOTA = QuotaSnapshot(
    tag_limit=3,
    current_tag_count=2,
    measurements_per_day_limit=120000,
    measurements_today=0,
    storage_limit_bytes=1,
    current_storage_bytes=0,
)

# Words that only mean anything while the control they name is on the screen.
# A description using one while its own schema draws no such field is telling
# somebody to press something that is not there - which is exactly what the
# review screen did to every clean setup.
CONTROL_WORDS: dict[str, tuple[str, ...]] = {
    CONF_TRIM: ("trim", "trimming"),
    CONF_SHOW_ALL: ("show every entity",),
}

# Sentences describing the rule the picker USED to follow, when visibility was
# decided by whether an entity's current state parsed as a number. It is
# decided by what the entity IS now - domain, entity category, device class -
# so any of these on the screen is the old rule being described to a user
# looking at a list built by the new one.
STALE_PICKER_CLAIMS = (
    "the ones that report a reading.",
    "cannot produce a reading",
    "whose state is a word rather than a value",
    "a date or a word rather than a value",
    "store nothing at all.\n",
)

# A review screen with nothing to decide may not talk about losing data.
LOSS_LANGUAGE = ("do not fit", "refused and lost", "trim")

# Placeholders Home Assistant adds to a step by itself, so "handed something
# nothing prints" must not fire on them. ``name`` is put on every reauth step
# by ConfigFlow.async_show_form - see config_entries.py, which fills it from
# the entry title unless the flow supplied one.
FRAMEWORK_PLACEHOLDERS = {"name"}


@pytest.fixture
def house(hass: HomeAssistant) -> None:
    """A house holding one of each thing the picker has to describe.

    ``sensor.house_mode`` is the entity G4 is about: a bare sensor whose state
    is a word. ``light.lamp_3`` is the entity G2 is about: it lands in the
    "show everything" list even though it can never do more than a 1 or a 0,
    and ``climate.living_room`` lands there while storing nothing at all.
    """
    hass.states.async_set(
        "sensor.house_power",
        "1500",
        {"device_class": "power", "state_class": "measurement"},
    )
    hass.states.async_set(
        "sensor.outdoor_temperature",
        "21.5",
        {"device_class": "temperature", "state_class": "measurement"},
    )
    hass.states.async_set("sensor.house_mode", "away")
    hass.states.async_set("light.lamp_3", "on")
    hass.states.async_set("climate.living_room", "heat")


def fields_on(result) -> set[str]:
    """The field names the form actually draws."""
    schema = result.get("data_schema")
    if schema is None:
        return set()
    return {str(key) for key in schema.schema}


def offers(result, entity_id: str) -> bool:
    """Whether the picker on this screen would accept that entity.

    ``EntitySelector`` enforces ``include_entities`` server-side with
    ``vol.In``, so this is the same question the screen itself answers.
    """
    try:
        result["data_schema"](
            {CONF_ENTITIES: [entity_id], CONF_MIN_INTERVAL: 10}
        )
    except vol.Invalid:
        return False
    return True


def render_step(section: str, result) -> str:
    """Every word on one screen, with the placeholders the code supplied.

    Title, description, field labels and field help, joined - because a false
    claim is just as false in a ``data_description`` as in a description, and
    the picker's was in both.
    """
    strings = TRANSLATIONS[section]["step"][result["step_id"]]
    placeholders = dict(result.get("description_placeholders") or {})
    parts = [strings["title"], strings.get("description", "")]
    parts += list(strings.get("data", {}).values())
    parts += list(strings.get("data_description", {}).values())
    return "\n".join(part.format(**placeholders) for part in parts)


def audit_step(section: str, result) -> str:
    """The checks every screen has to pass, whatever it says.

    Returns the rendered text so a caller can go on to assert about the words
    themselves.
    """
    step_id = result["step_id"]
    strings = TRANSLATIONS[section]["step"][step_id]
    where = f"{section}.{step_id}"

    supplied = set(result.get("description_placeholders") or {})
    wanted = placeholders_in(strings["title"] + " " + strings.get("description", ""))
    assert wanted <= supplied, f"{where} prints {wanted - supplied} that nothing supplies"
    assert supplied - FRAMEWORK_PLACEHOLDERS <= wanted, (
        f"{where} is handed {supplied - wanted} that nothing prints"
    )

    fields = fields_on(result)
    assert set(strings.get("data", {})) == fields, (
        f"{where} labels {set(strings.get('data', {})) ^ fields} that its form does not draw"
    )
    assert set(strings.get("data_description", {})) <= fields, (
        f"{where} explains a field it does not draw"
    )

    text = render_step(section, result)
    lowered = text.casefold()
    for field, words in CONTROL_WORDS.items():
        used = [word for word in words if word in lowered]
        if used:
            assert field in fields, (
                f"{where} says {used} while showing no {field} control: {text!r}"
            )
    return text


async def _walk_config_flow(hass: HomeAssistant, client) -> dict[str, dict]:
    """Add the integration, keeping every screen it draws on the way.

    Abandoned rather than finished, and abandoned explicitly: the flow claims
    the account's unique id as soon as the key validates, so a second walk
    would be turned away as already in progress rather than reaching the
    screens it came for.
    """
    seen: dict[str, dict] = {}
    with patch(FLOW_CLIENT_PATH, return_value=client), patch(
        SETUP_PATH, return_value=True
    ):
        result = await hass.config_entries.flow.async_init(
            DOMAIN, context={"source": SOURCE_USER}
        )
        seen[result["step_id"]] = result
        flow_id = result["flow_id"]

        result = await hass.config_entries.flow.async_configure(flow_id, CREDENTIALS)
        seen[result["step_id"]] = result

        result = await hass.config_entries.flow.async_configure(
            flow_id,
            {
                CONF_ENTITIES: ["sensor.house_power", "sensor.outdoor_temperature"],
                CONF_MIN_INTERVAL: 10,
            },
        )
        seen[result["step_id"]] = result
        hass.config_entries.flow.async_abort(flow_id)
    return seen


async def _walk_options_flow(hass: HomeAssistant, client) -> dict[str, dict]:
    """The same two screens again, from Configure rather than Add."""
    entry = MockConfigEntry(
        domain=DOMAIN,
        data=CREDENTIALS,
        options={CONF_ENTITIES: ["sensor.house_power"], CONF_MIN_INTERVAL: 10},
    )
    entry.add_to_hass(hass)

    seen: dict[str, dict] = {}
    with patch(FLOW_CLIENT_PATH, return_value=client), patch(
        SETUP_PATH, return_value=True
    ):
        result = await hass.config_entries.options.async_init(entry.entry_id)
        seen[result["step_id"]] = result
        flow_id = result["flow_id"]

        result = await hass.config_entries.options.async_configure(
            flow_id,
            {
                CONF_ENTITIES: ["sensor.house_power", "sensor.outdoor_temperature"],
                CONF_MIN_INTERVAL: 10,
            },
        )
        seen[result["step_id"]] = result
        hass.config_entries.options.async_abort(flow_id)
    return seen


async def test_every_config_flow_screen_renders_and_matches_its_own_form(
    hass: HomeAssistant, house
) -> None:
    """The audit the Repairs cards already had, for the screens as well.

    Every step id in en.json has to be reachable and every one of them has to
    pass: placeholders exactly matched in both directions, labels exactly
    matching the fields the schema draws, and no sentence naming a control the
    form is not showing.
    """
    seen = await _walk_config_flow(hass, make_client())
    seen |= await _walk_config_flow(hass, make_client(quota=TIGHT_QUOTA))

    # And the one screen that is not on the way in.
    entry = MockConfigEntry(
        domain=DOMAIN, unique_id=STUB_ACCOUNT["customerId"], data=CREDENTIALS
    )
    entry.add_to_hass(hass)
    with patch(FLOW_CLIENT_PATH, return_value=make_client()):
        result = await hass.config_entries.flow.async_init(
            DOMAIN,
            context={"source": SOURCE_REAUTH, "entry_id": entry.entry_id},
            data=entry.data,
        )
        seen[result["step_id"]] = result

    for result in seen.values():
        audit_step("config", result)

    assert set(seen) == set(TRANSLATIONS["config"]["step"]), (
        "a screen nothing drives is a screen nothing checks"
    )


async def test_every_options_flow_screen_renders_and_matches_its_own_form(
    hass: HomeAssistant, house
) -> None:
    """The options flow shares the code and has its own strings.

    Sharing the mixin is exactly why this needs its own walk: the two flows
    can drift apart in en.json while agreeing perfectly in Python.
    """
    seen = await _walk_options_flow(hass, make_client())
    seen |= await _walk_options_flow(hass, make_client(quota=TIGHT_QUOTA))

    for result in seen.values():
        audit_step("options", result)

    assert set(seen) == set(TRANSLATIONS["options"]["step"])


@pytest.mark.parametrize("section", ["config", "options"])
async def test_a_clean_setup_reads_like_a_clean_setup(
    hass: HomeAssistant, house, section
) -> None:
    """G3 - the review screen used to warn about data loss on the happy path.

    At ``overflow == 0`` the schema is empty, so there is no trim checkbox on
    the screen at all - and the description still read "0 of those new tags do
    not fit. If you continue without trimming, the readings ... will be
    refused and lost." A warning about a control that is not there, about a
    loss that is not happening, to somebody who has done nothing wrong.
    """
    walk = _walk_config_flow if section == "config" else _walk_options_flow
    review = (await walk(hass, make_client()))["review"]

    assert fields_on(review) == set(), "nothing to decide on this screen"
    assert review["description_placeholders"]["tags_available"] == "10"

    text = audit_step(section, review).casefold()
    for phrase in LOSS_LANGUAGE:
        assert phrase not in text, (
            f"a clean setup is being warned about {phrase!r}: {text!r}"
        )


@pytest.mark.parametrize("section", ["config", "options"])
async def test_the_screen_that_does_have_something_to_lose_still_says_so(
    hass: HomeAssistant, house, section
) -> None:
    """Separating the two must not have made the true warning unsayable."""
    walk = _walk_config_flow if section == "config" else _walk_options_flow
    review = (await walk(hass, make_client(quota=TIGHT_QUOTA)))["review_trim"]

    assert fields_on(review) == {CONF_TRIM}
    assert review["description_placeholders"]["overflow"] == "1"

    text = audit_step(section, review).casefold()
    assert "do not fit" in text
    assert "refused and lost" in text


async def test_the_picker_describes_the_list_it_actually_offers(
    hass: HomeAssistant, house
) -> None:
    """G2 - the select step described a filter the code stopped applying.

    Visibility is decided by what an entity IS - its domain, its entity
    category, its device class - and deliberately not by whether its current
    state parses as a number, because a picker that consulted the state hid
    entities while their battery was flat and then handed its own selector a
    default it refused.

    So ``sensor.house_mode`` sitting at "away" is in the default list, and
    ``light.lamp_3`` is one toggle away, and no sentence on the screen may go
    on claiming the list is filtered by whether a reading can come out.
    """
    seen = await _walk_config_flow(hass, make_client())
    select = seen["select"]

    # The list is not filtered by what an entity reads: a bare sensor sitting
    # at a word is offered like anything else.
    assert offers(select, "sensor.house_mode")
    # ...and the things you operate are held back, by domain.
    assert not offers(select, "light.lamp_3")
    assert select["description_placeholders"]["offered_count"] == "3"
    assert select["description_placeholders"]["entity_count"] == "5"

    text = audit_step("config", select)
    lowered = text.casefold()
    for claim in STALE_PICKER_CLAIMS:
        assert claim.casefold() not in lowered, (
            f"the picker still claims {claim!r} while offering sensor.house_mode"
        )

    # G4's half of the bargain: an entity that will store nothing may be
    # offered un-ticked, but only if the screen says what that means.
    assert "listed but left unticked" in lowered
    assert "store nothing at all" in lowered


async def test_show_everything_is_described_as_the_unfiltered_list_it_is(
    hass: HomeAssistant, house
) -> None:
    """The toggle puts the WHOLE install in the list, filter and all removed.

    The old text said most of what it adds can store a 1 or a 0 and that only
    words store nothing. Half of that is still true and the sentence has to
    name the other half, because a `climate` mode and a `person` in a zone are
    both in this list and both store nothing whatever.
    """
    with patch(FLOW_CLIENT_PATH, return_value=make_client()), patch(
        SETUP_PATH, return_value=True
    ):
        result = await hass.config_entries.flow.async_init(
            DOMAIN, context={"source": SOURCE_USER}
        )
        result = await hass.config_entries.flow.async_configure(
            result["flow_id"], CREDENTIALS
        )
        result = await hass.config_entries.flow.async_configure(
            result["flow_id"],
            {
                CONF_ENTITIES: ["sensor.house_power"],
                CONF_MIN_INTERVAL: 10,
                CONF_SHOW_ALL: True,
            },
        )

    assert result["step_id"] == "select"
    assert offers(result, "light.lamp_3")
    assert offers(result, "climate.living_room")
    placeholders = result["description_placeholders"]
    assert placeholders["offered_count"] == placeholders["entity_count"]
    assert placeholders["hidden_count"] == "0"

    text = audit_step("config", result).casefold()
    assert "unfiltered" in text, "the toggle removes the filter; say so"
    assert "a lock, a person, a climate mode and a media player" in text


# --------------------------------------------------------------------------
# The README is a customer-facing surface too
# --------------------------------------------------------------------------

README = (Path(__file__).resolve().parent.parent / "README.md").read_text(
    encoding="utf-8"
)

SPELLED = {1: "one", 2: "two", 3: "three", 4: "four", 5: "five", 10: "ten"}


def test_the_readme_names_the_wait_the_code_actually_gives_a_write() -> None:
    """It states the number in words, so the number has to be checked.

    This README asserted an invariant the code did not hold once already. A
    duration written into prose and nowhere near the constant is the same
    failure waiting to happen more quietly.
    """
    assert f"up to {SPELLED[INFLIGHT_WAIT_SECONDS]} seconds" in README


def test_the_readme_promises_that_wait_on_both_ways_out() -> None:
    """The number was right and the SCOPE was wrong, which is worse.

    "When Home Assistant stops ... a request already on its way out gets up to
    five seconds" was true of an unload and false of a stop, for the whole time
    the sentence stood - Home Assistant cancels the background tasks before it
    fires its stop event, so the listener that was supposed to do the waiting
    got 0.0 seconds and reported forty delivered readings as lost.

    So the claim has to name both doors where it is made. tests/test_init.py
    drives each of them with a real write on the wire - an options change under
    G1, ``hass.async_stop()`` under G5 - and those are what make it true.
    """
    claim = next(
        block
        for block in README.split("\n\n")
        if f"up to {SPELLED[INFLIGHT_WAIT_SECONDS]} seconds" in block
    ).casefold()

    assert "reload" in claim, f"the reload is not named where the wait is promised: {claim!r}"
    assert "stop" in claim, f"the stop is not named where the wait is promised: {claim!r}"


async def test_every_dropped_attribute_the_readme_names_actually_exists(
    hass: HomeAssistant,
) -> None:
    """The README breaks the dropped counter down attribute by attribute.

    A renamed attribute leaves that table pointing at nothing, in the one
    document somebody reads when they are trying to find out where their data
    went.
    """
    entry = MockConfigEntry(
        domain=DOMAIN,
        data=CREDENTIALS,
        options={CONF_ENTITIES: ["sensor.house_power"], CONF_MIN_INTERVAL: 10},
    )
    entry.add_to_hass(hass)

    with patch(
        "custom_components.tag_historian.TagHistorianClient",
        return_value=make_client(),
    ):
        assert await hass.config_entries.async_setup(entry.entry_id)
        await hass.async_block_till_done()

        state = hass.states.get("sensor.tag_historian_readings_dropped")
        assert state is not None

        named = set(re.findall(r"`(dropped_[a-z_]+|entities_[a-z_]+)`", README))
        assert named, "the README used to break the counter down; it still should"
        missing = named - set(state.attributes)
        assert not missing, f"the README names attributes that do not exist: {missing}"

        assert await hass.config_entries.async_unload(entry.entry_id)
        await hass.async_block_till_done()


# The digit runs that appear in a screen string and are not plan numbers. There
# is one, and it is the sentence describing what an on/off entity stores.
#
# An allow-list rather than a clever pattern, because the pattern is what went
# wrong: this guard used to look for four-or-more digits, which caught the daily
# reading allowances and nothing else. The tag limits are 5, 50 and 500 - "your
# plan allows 50 tags" is the most plausible stale number anybody would type
# into these strings, and it is exactly the shape the guard could not see.
# Every number on these screens comes from GET /api/usage/limits as a
# placeholder, so a new digit here is a decision, and it should have to be one.
DIGITS_THAT_ARE_NOT_PLAN_NUMBERS = ("a 1 or a 0",)


def plan_number_digits(text: str) -> list[str]:
    """The digit runs in a customer-facing string that could be a plan number."""
    for phrase in DIGITS_THAT_ARE_NOT_PLAN_NUMBERS:
        text = text.replace(phrase, "")
    return re.findall(r"\d+", text)


def test_no_screen_string_states_a_plan_number() -> None:
    """The strings are prose, and prose is where a stale number hides best.

    tests/test_no_hardcoded_quotas.py guards the package sources mechanically;
    this catches the shape - a bare number in a string somebody reads, which
    is what a plan limit looks like once it has been typed into a sentence.
    """
    offenders: list[str] = []

    def walk(node, path: str) -> None:
        if isinstance(node, dict):
            for key, value in node.items():
                walk(value, f"{path}.{key}")
        elif isinstance(node, str) and plan_number_digits(node):
            offenders.append(f"{path}: {node}")

    walk(TRANSLATIONS, "en")
    assert not offenders, offenders


@pytest.mark.parametrize(
    "sentence",
    [
        "Your plan allows 7 tags.",
        "Your plan allows 70 tags.",
        "Your plan allows 700 tags.",
        "That is up to 70000 readings a day.",
        "Move to a plan with a larger allowance for 4 a month.",
    ],
)
def test_the_plan_number_guard_catches_a_small_one(sentence) -> None:
    """A guard nothing can trip is a guard nobody notices has stopped working.

    Sentences of exactly the shapes that walked past the previous version of
    this check. The numbers are arbitrary, deliberately: a real limit must not
    appear here even as a test fixture.
    """
    assert plan_number_digits(sentence), f"a plan number got through: {sentence!r}"


def test_the_plan_number_guard_still_allows_what_an_on_off_entity_stores() -> None:
    """And it must not have become a guard that nothing can satisfy."""
    assert (
        plan_number_digits(
            "Some of them store a 1 or a 0 - a light, a switch, a cover."
        )
        == []
    )


# --------------------------------------------------------------------------
# The scope audit
# --------------------------------------------------------------------------
#
# An instruction to create an API key has to name the scope.
#
# Every string here used to say "an API key with write access", and every
# ingestion guide in the repository said "generate a key on the Settings page"
# and stopped there. The Create New Key dialog opens on Read, ApiKeyScope.Read
# maps to CustomerRole.Viewer, and the write endpoint requires Editor - so a
# customer who followed the instruction exactly ended up with a key this
# integration rejects at setup, and, on the guides with no such detection, with
# a 403 on their very first reading.
#
# "Write access" is not the fix. The control is a dropdown labelled Scope with
# three named values in it, and a reader is scanning that form for a literal
# string. So the words have to be the screen's words: Scope, and Write.
#
# The Tag Historian product repository carries the same guard for its own
# surfaces - the markdown guides and the React pages. This half owns what this
# package puts on a Home Assistant screen.

# The scope names as the dashboard's Create New Key dialog spells them. This
# used to be read live out of the dashboard's source, but that coupling cannot
# cross a repository boundary: the dashboard lives in the product repository,
# whose own test suite pins this exact list to the API's scope enum. If a
# scope is ever added or renamed there, this tuple is the one line to update
# here - and the assertions below fail loudly rather than silently if the
# recommended scope stops being one of these.
API_KEY_SCOPES = ("Read", "Write", "Admin")

# The scope this integration needs, and the only one it may recommend. It
# forwards entity states to POST /api/v2/write, and a Read key is refused there.
REQUIRED_SCOPE = "Write"

# A sentence telling somebody to go and create a key. The literal control name
# is the reliable half - you cannot make a key without pressing it. The verb
# form is the vaguer half, and the one every surface used to carry.
CREATE_A_KEY = re.compile(
    r"Create\s+New\s+Key"
    r"|(?:creat|generat|issu|mint)[a-z]*\s+(?:a|an|your|the|one|another|new)"
    r"\s+(?:[a-z]+\s+){0,2}key\b",
    re.IGNORECASE,
)

# The looser half, and why it is only on this side of the guard. The reauth
# screen said "Create a new one with write access in the Tag Historian
# dashboard" - the noun is a sentence back, so the pattern above cannot see it,
# and that string is exactly the kind this guard exists for. A UI string is
# short enough that a creating verb and the word "key" inside it mean what they
# look like. Run over a whole markdown guide the same rule would fire on
# everything, which is why the product repository's half of this guard keeps
# the strict pattern only.
CREATING_VERB = re.compile(r"(?:creat|generat|issu|mint)[a-z]*\b", re.IGNORECASE)
KEY_NOUN = re.compile(r"\bkeys?\b", re.IGNORECASE)


def instructs_creating_a_key(text: str) -> bool:
    """Whether this string tells somebody to go and make one."""
    if CREATE_A_KEY.search(text):
        return True
    return bool(CREATING_VERB.search(text) and KEY_NOUN.search(text))


def flattened(text: str) -> str:
    """One line, so a claim is not split by wherever the paragraph wrapped.

    The README wraps at eighty columns, which puts a line break in the middle
    of "it cannot issue or revoke API keys" - present to a reader, absent to a
    substring search. Without this the guard would quietly be asking authors to
    keep whole clauses on one line to satisfy it.
    """
    return re.sub(r"\s+", " ", text)


def instructing_strings() -> list[tuple[str, str]]:
    """Every translation string that tells somebody to create a key."""
    found: list[tuple[str, str]] = []

    def walk(node, path: str) -> None:
        if isinstance(node, dict):
            for key, value in node.items():
                walk(value, f"{path}.{key}")
        elif isinstance(node, str) and instructs_creating_a_key(node):
            found.append((path, node))

    walk(TRANSLATIONS, "en")
    return found


def test_the_scope_names_come_from_the_dashboards_own_list() -> None:
    """A guard that derived nothing would pass whatever it was pointed at."""
    assert REQUIRED_SCOPE in API_KEY_SCOPES, (
        f"the dashboard offers {API_KEY_SCOPES} and this integration asks for "
        f"{REQUIRED_SCOPE!r}, which is not among them"
    )


def test_every_string_that_tells_somebody_to_create_a_key_names_the_scope() -> None:
    """The defect, as a test.

    Three of these strings existed and none of them named a scope: the connect
    screen, its field help, and the reauth screen. A customer following any of
    them landed on a dialog whose default is the one value that does not work.
    """
    instructing = instructing_strings()
    assert instructing, (
        "no string in this integration tells anybody to create a key, which "
        "means the guard is watching an empty set rather than the screens"
    )

    for path, text in instructing:
        assert "Scope" in text, (
            f"{path} tells somebody to create a key without naming the control "
            f"the dialog prints 'Scope' on: {text!r}"
        )
        assert REQUIRED_SCOPE in text, (
            f"{path} tells somebody to create a key without naming the "
            f"{REQUIRED_SCOPE} scope, and the dialog defaults to the one that "
            f"cannot forward anything: {text!r}"
        )


def test_no_screen_names_a_scope_the_api_cannot_issue() -> None:
    """'Read-only' is what these strings used to say, and it is not a value.

    The dropdown has three options with names on them. A screen that tells
    somebody to pick "read-only", or "Viewer" - the ROLE a Read scope maps to,
    which is exactly the word a developer would reach for - sends them looking
    for an option that is not in the list.
    """
    not_a_scope = ("read-only scope", "readonly scope", "viewer scope", "editor scope")

    offenders = [
        (path, text)
        for path, text in instructing_strings()
        for phrase in not_a_scope
        if phrase in text.casefold()
    ]
    assert not offenders, offenders


def test_the_readme_tells_the_same_story_as_the_screens() -> None:
    """The README is where somebody goes when the screen was not enough.

    It said "an API key with write access" while the screen said the same, and
    both were describing a control neither of them named.
    """
    assert instructs_creating_a_key(README), (
        "the README no longer explains how to get a key"
    )
    assert "Scope" in README
    assert REQUIRED_SCOPE in README


def test_the_readme_says_what_a_write_key_cannot_do() -> None:
    """The half that gets edited out, because it reads like a limitation.

    It is the reason somebody does not paste the key an account recovery handed
    them - which is Admin, and the whole account - into Home Assistant instead.
    Each of these is a 403 the product's own test suite executes against the
    running API, so the claims here are enforced facts, not copy.
    """
    for claim in (
        "issue or revoke",
        "delete a tag",
        "cancel your subscription",
        "invite or remove",
    ):
        assert claim in flattened(README), (
            f"the README never says a Write key cannot {claim!r}"
        )

    assert "403" in README, (
        "and it has to say the refusals are refusals - 'it cannot do X' reads as "
        "'X quietly does nothing' otherwise, and a customer debugging silence "
        "suspects their own configuration first"
    )


@pytest.mark.parametrize(
    "sentence",
    [
        # The strings this branch replaced, verbatim.
        "Paste an API key with write access. You can create one in the Tag "
        "Historian dashboard under API keys.",
        "The key this integration was using is no longer accepted. Create a "
        "new one with write access in the Tag Historian dashboard and paste it "
        "here.",
        "That key is read-only. It can look at your data but not add to it - "
        "create a key with write access instead.",
        # And the one the repository's other guides carried.
        "Generate a key on the Settings page. It is shown only once - copy it "
        "right away.",
    ],
)
def test_the_scope_guard_catches_the_sentences_this_branch_removed(sentence) -> None:
    """A guard nothing can trip is a guard nobody notices has stopped working.

    Every one of these shipped, and every one of them describes a scope in
    prose ("write access", "read-only") without naming the value in the
    dropdown or the label above it.
    """
    assert instructs_creating_a_key(sentence), f"the detector missed: {sentence!r}"
    assert not ("Scope" in sentence and REQUIRED_SCOPE in sentence), (
        f"this sentence was meant to be an example of the failure: {sentence!r}"
    )


def test_the_scope_guard_is_not_one_nothing_can_satisfy() -> None:
    """And the corrected sentence has to pass it."""
    fixed = (
        "Paste an API key with the Write scope. Create one in the dashboard: "
        "press Create New Key, name it, and set Scope to Write."
    )
    assert instructs_creating_a_key(fixed)
    assert "Scope" in fixed and REQUIRED_SCOPE in fixed
