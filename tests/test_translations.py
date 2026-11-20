"""Every language says what the English says, with the same numbers in it.

The integration's dialogs, repair cards, action and refusals are in
``translations/``, one file per language. English is the one the code and the
other tests are written against (tests/test_user_facing_text.py); this file
holds every other language to it:

- the same keys, so no dialog falls back to English halfway down a screen;
- the same placeholders in every string - Home Assistant drops a translated
  string whose placeholders differ from the English ones and shows the
  English instead, without saying so;
- where a text names a tick box or a menu choice, it names it as that
  language's own screen shows it;
- and the few words Python puts together itself (words.py) are there, are
  used, and come out of Home Assistant in the language it is set to.
"""

from __future__ import annotations

import ast
import json
import re
from typing import Any

import pytest
from homeassistant.core import HomeAssistant
from homeassistant.helpers import issue_registry as ir
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.tag_historian import repairs
from custom_components.tag_historian.config_flow import power_clause
from custom_components.tag_historian.const import DOMAIN, ISSUE_DAILY_QUOTA
from custom_components.tag_historian.importer import STATUSES
from custom_components.tag_historian.selection import CATEGORIES, EntityFacts, propose
from custom_components.tag_historian.words import CATEGORY, async_load_words

from .conftest import TRANSLATIONS_DIR, words_in

COMPONENT = TRANSLATIONS_DIR.parent

# The languages the integration speaks, by Home Assistant's own codes. Home
# Assistant has Portuguese twice, "pt" and "pt-BR", and falls back from either
# straight to English, never to the other - so the one Portuguese text there
# is (Brazilian, as the rest of Tag Historian's) is in both files.
LANGUAGES = ("sv", "de", "es", "fr", "pt-BR", "pt")

_PLACEHOLDER_RE = re.compile(r"\{([^{}]*)\}")


def _load(language: str) -> dict[str, Any]:
    return json.loads((TRANSLATIONS_DIR / f"{language}.json").read_text(encoding="utf-8"))


def _flatten(node: Any, path: str = "") -> dict[str, str]:
    if isinstance(node, dict):
        flat: dict[str, str] = {}
        for key, value in node.items():
            flat.update(_flatten(value, f"{path}.{key}" if path else key))
        return flat
    return {path: node}


ENGLISH = _flatten(_load("en"))


@pytest.fixture
def entry(hass: HomeAssistant) -> MockConfigEntry:
    entry = MockConfigEntry(domain=DOMAIN, data={}, unique_id="11111111-2222-3333-4444-555555555555")
    entry.add_to_hass(hass)
    return entry


def test_the_languages_are_the_ones_the_readme_names() -> None:
    files = {path.stem for path in TRANSLATIONS_DIR.glob("*.json")}
    assert files == {"en", *LANGUAGES}

    readme = (COMPONENT.parent.parent / "README.md").read_text(encoding="utf-8")
    for name in ("English", "Swedish", "German", "Spanish", "French", "Portuguese"):
        assert name in readme, f"the README does not say the integration speaks {name}"


@pytest.mark.parametrize("language", LANGUAGES)
def test_a_language_has_every_string_english_has_and_no_other(language: str) -> None:
    translated = _flatten(_load(language))

    assert set(translated) == set(ENGLISH), (
        f"{language}: missing {sorted(set(ENGLISH) - set(translated))}, "
        f"extra {sorted(set(translated) - set(ENGLISH))}"
    )
    for path, text in translated.items():
        assert isinstance(text, str) and text.strip(), f"{language}: {path} is empty"
        assert text == text.strip(), f"{language}: {path} begins or ends with a space"


@pytest.mark.parametrize("language", LANGUAGES)
def test_a_language_fills_in_exactly_what_english_fills_in(language: str) -> None:
    """A placeholder too many is printed in braces; one too few, or misspelt, loses the string."""
    translated = _flatten(_load(language))

    for path, english in ENGLISH.items():
        assert sorted(_PLACEHOLDER_RE.findall(translated[path])) == sorted(
            _PLACEHOLDER_RE.findall(english)
        ), f"{language}: {path} does not have the placeholders of the English string"


def test_portuguese_is_one_text_under_both_of_home_assistants_codes() -> None:
    assert _load("pt") == _load("pt-BR")


@pytest.mark.parametrize("language", LANGUAGES)
def test_a_language_is_translated(language: str) -> None:
    """Not a copy of en.json under another name.

    A handful of strings are the same word in two languages (Host, Tag
    Historian); a file where most are is English.
    """
    translated = _flatten(_load(language))
    same = [path for path, text in translated.items() if text == ENGLISH[path]]
    assert len(same) <= 4, f"{language}: still English at {same}"


# What a text calls a tick box or a menu choice, by where that label is. A
# sentence that tells somebody to tick "Show every entity" has to say it the
# way their own screen does.
_LABELS = (
    "config.step.select.data.show_all",
    "config.step.select.data.fast_power",
    "options.step.init.menu_options.choose",
    "options.step.init.menu_options.import_history",
    "issues.entity_renamed.fix_flow.step.rename_choice.menu_options.new_tag",
    "issues.entity_renamed.fix_flow.step.merge_choice.menu_options.keep_apart",
)

# The integration's own sensors are named the same in every language
# (sensor.py), so every language points at them by that name.
_SENSOR_NAMES = ("Tag Historian history import", "readings dropped")


@pytest.mark.parametrize("language", LANGUAGES)
def test_a_text_names_a_box_as_that_languages_screen_shows_it(language: str) -> None:
    translated = _flatten(_load(language))

    quoted = 0
    for label_path in _LABELS:
        english_label, label = ENGLISH[label_path], translated[label_path]
        for path, english in ENGLISH.items():
            if path == label_path or english == english_label or english_label not in english:
                continue
            quoted += 1
            assert label in translated[path], (
                f"{language}: {path} does not name {label!r}, which is what {label_path} is called"
            )
            assert english_label not in translated[path], (
                f"{language}: {path} still names the English {english_label!r}"
            )
    assert quoted >= 10, "the labels are quoted in more places than this found"

    for name in _SENSOR_NAMES:
        for path, english in ENGLISH.items():
            if name in english:
                assert name in translated[path], f"{language}: {path} does not name the sensor {name!r}"


@pytest.mark.parametrize("language", ("en", *LANGUAGES))
def test_the_two_screens_that_share_a_box_call_it_the_same(language: str) -> None:
    """Setting up and Configure show the same boxes; one name each."""
    strings = _load(language)
    assert strings["config"]["step"]["select"]["data"] == strings["options"]["step"]["select"]["data"]
    assert (
        strings["config"]["step"]["history"]["title"]
        == strings["config"]["step"]["history"]["data"]["import_on_setup"]
    )


# --------------------------------------------------------------------------
# The words Python puts together itself (words.py)
# --------------------------------------------------------------------------


def _keys_the_code_asks_for() -> set[str]:
    """Every common.<key> a ``say`` or ``counted`` call names, from the source."""
    keys: set[str] = set()
    for source in COMPONENT.glob("*.py"):
        if source.name == "words.py":
            # Where say and counted are: it builds category_<name> and the
            # _one/_other pair, which the caller of this adds itself.
            continue
        for node in ast.walk(ast.parse(source.read_text(encoding="utf-8"))):
            if not (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr in ("say", "counted")
                and node.args
            ):
                continue
            first = node.args[0]
            assert isinstance(first, ast.Constant) and isinstance(first.value, str), (
                f"{source.name}:{node.lineno} asks for a key this test cannot read"
            )
            if node.func.attr == "counted":
                keys |= {f"{first.value}_one", f"{first.value}_other"}
            else:
                keys.add(first.value)
    return keys


def test_the_words_the_code_asks_for_are_the_words_there_are() -> None:
    wanted = _keys_the_code_asks_for() | {f"category_{category}" for category in CATEGORIES}
    there = set(_load("en")[CATEGORY])

    assert wanted - there == set(), "the code asks for words no file has"
    assert there - wanted == set(), "words nothing says"


def test_the_states_of_the_import_sensor_are_the_ones_it_can_be_in() -> None:
    states = _load("en")["entity"]["sensor"]["history_import"]["state"]
    assert set(states) == set(STATUSES)


HOUSE = [
    EntityFacts(entity_id="sensor.house_power", device_class="power", state_class="measurement"),
    EntityFacts(
        entity_id="sensor.outdoor_temperature", device_class="temperature", state_class="measurement"
    ),
    EntityFacts(entity_id="binary_sensor.heat_pump_running", device_class="running"),
]


@pytest.mark.parametrize(
    ("language", "counted", "one_sensor", "an_hour", "minutes"),
    [
        (
            "sv",
            "1 energi och effekt, 1 temperatur och luftfuktighet, 1 på/av-tillstånd",
            ", och ett per 2 sekunder för 1 effektsensor,",
            "om ungefär en timme",
            "om ungefär 4 minuter",
        ),
        (
            "de",
            "1 Energie und Leistung, 1 Temperatur und Luftfeuchtigkeit, 1 Ein/Aus-Zustände",
            ", und einem alle 2 Sekunden für 1 Leistungssensor,",
            "in etwa einer Stunde",
            "in etwa 4 Minuten",
        ),
        (
            "es",
            "1 de energía y potencia, 1 de temperatura y humedad, 1 estados de encendido/apagado",
            ", y una cada 2 segundos para 1 sensor de potencia,",
            "dentro de una hora, más o menos",
            "dentro de unos 4 minutos",
        ),
        (
            "fr",
            "1 énergie et puissance, 1 température et humidité, 1 états marche/arrêt",
            ", et d\u2019une toutes les 2 secondes pour 1 capteur de puissance,",
            "dans une heure environ",
            "dans 4 minutes environ",
        ),
        (
            "pt-BR",
            "1 de energia e potência, 1 de temperatura e umidade, 1 estados de ligado/desligado",
            ", e uma a cada 2 segundos para 1 sensor de potência,",
            "em cerca de uma hora",
            "em cerca de 4 minutos",
        ),
    ],
)
def test_the_pieces_python_builds_read_as_that_language(
    language: str, counted: str, one_sensor: str, an_hour: str, minutes: str
) -> None:
    said = words_in(language)

    assert propose(HOUSE, budget=3).breakdown(said.category_labels(CATEGORIES)) == counted
    assert power_clause(said, 1, asked=True) == one_sensor
    assert "3" in power_clause(said, 3, asked=True)
    assert power_clause(said, 0, asked=True).startswith(" (")
    assert repairs.describe_wait(said, 7) == said.say("wait_under_a_minute")
    assert repairs.describe_wait(said, 60) == said.say("wait_minutes_one")
    assert repairs.describe_wait(said, 240) == minutes
    assert repairs.describe_wait(said, 3600) == an_hour
    assert "8" in repairs.describe_wait(said, 28800)


def test_french_and_portuguese_count_nothing_as_one() -> None:
    """ "0 heure", "0 hora": their singular covers zero, where the others' does not."""
    for language in ("fr", "pt", "pt-BR"):
        said = words_in(language)
        assert said.counted("wait_hours", 0) == said.say("wait_hours_one")
    for language in ("en", "sv", "de", "es"):
        said = words_in(language)
        assert said.counted("wait_hours", 0) == said.say("wait_hours_other", count=0)


async def test_home_assistant_serves_the_words_in_the_language_it_is_set_to(
    hass: HomeAssistant, entry
) -> None:
    """Through Home Assistant's own translation cache, not this suite's reading of the files."""
    hass.config.language = "sv"
    assert (await async_load_words(hass)).say("no_reason_given") == "ingen orsak angavs"

    # A repair card raised where nothing can be awaited finds them loaded.
    repairs.async_raise_daily_quota_issue(hass, entry, 240)
    issue = ir.async_get(hass).async_get_issue(DOMAIN, repairs.issue_id(entry, ISSUE_DAILY_QUOTA))
    assert issue.translation_placeholders == {"when": "om ungefär 4 minuter"}

    hass.config.language = "pt-BR"
    assert (await async_load_words(hass)).say("wait_minutes_other", count=4) == "em cerca de 4 minutos"


async def test_a_language_the_integration_does_not_speak_gets_english(hass: HomeAssistant) -> None:
    hass.config.language = "nl"
    said = await async_load_words(hass)
    assert said.say("no_reason_given") == "no reason given"
    assert said.counted("wait_minutes", 4) == "in about 4 minutes"


async def test_home_assistant_keeps_every_translated_string(hass: HomeAssistant) -> None:
    """Home Assistant checks the placeholders itself and falls back to English where they differ.

    So what it serves is compared with what the file says, for every string of
    every language: one that came back in English was dropped.
    """
    from homeassistant.helpers.translation import async_get_translations

    for language in LANGUAGES:
        translated = _load(language)
        for category in translated:
            served = await async_get_translations(hass, language, category, [DOMAIN])
            for path, text in _flatten(translated[category]).items():
                assert served[f"component.{DOMAIN}.{category}.{path}"] == text, (
                    f"{language}: Home Assistant does not serve {category}.{path} as written"
                )
