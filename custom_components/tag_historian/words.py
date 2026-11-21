"""The words this integration puts together itself, in Home Assistant's language.

Most of what the integration says is a whole string in ``translations/``: the
frontend picks the reader's language and fills in the numbers. A few pieces
cannot be one string, because how many there are or which of them applies is
only known here: the counted categories on the Choose screen ("18 energy and
power, 9 on/off states"), what the review screens say of the power sensors,
how long until the daily allowance resets, and the line that quotes what Tag
Historian answered. Those are put together in Python and handed over as a
placeholder, so Python needs the words.

They live under ``common`` in the same translation files, read through Home
Assistant's own translation cache, which falls back to English key by key and
drops a translation whose placeholders differ from the English ones.

In WHICH language is the one thing that differs from the rest. A dialog is
drawn in the language of the person looking at it, which only the frontend
knows; nothing here is told. So these pieces are in the language Home
Assistant itself is set to (Settings, System, General) - the same language its
own notifications and logbook use. On an install where a user's profile is set
to another language than the system's, a sentence in the profile's language
can carry one of these pieces in the system's.
"""

from __future__ import annotations

from collections.abc import Mapping

from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.translation import (
    async_get_cached_translations,
    async_get_translations,
)

from .const import DOMAIN, LOGGER

# The translation files' section these strings are in. Home Assistant's schema
# for a translation file (hassfest) has this one for strings that belong to no
# dialog, entity or action: a flat map of key to string.
CATEGORY = "common"

# The languages whose "one" covers 0 as well: "0 heure", "0 hora". Everywhere
# else here (en, sv, de, es) only 1 is singular.
_ZERO_IS_SINGULAR = frozenset({"fr", "pt", "pt-BR"})


class Words:
    """The ``common`` strings of one language, with English wherever it has none."""

    def __init__(self, language: str, strings: Mapping[str, str]) -> None:
        self.language = language
        self._strings = strings

    def say(self, key: str, **placeholders: object) -> str:
        """The string under ``key``, with its placeholders filled in.

        Empty, and logged, when there is no such string: a sentence short of
        one clause is better than a dialog that does not open.
        """
        text = self._strings.get(f"component.{DOMAIN}.{CATEGORY}.{key}")
        if text is None:
            LOGGER.warning("Tag Historian has no text for %r in %s", key, self.language)
            return ""
        return text.format(**placeholders) if placeholders else text

    def counted(self, key: str, count: int, **placeholders: object) -> str:
        """``key`` in the singular or the plural, as ``count`` asks for in this language.

        The two are two whole strings, ``<key>_one`` and ``<key>_other``: no
        language here makes its plural by adding a letter to a word.
        """
        singular = count == 1 or (count == 0 and self.language in _ZERO_IS_SINGULAR)
        return self.say(f"{key}_one" if singular else f"{key}_other", count=count, **placeholders)

    def category_labels(self, categories: tuple[str, ...]) -> dict[str, str]:
        """What each selection category is called when it is counted back (selection.py)."""
        return {category: self.say(f"category_{category}") for category in categories}


async def async_load_words(hass: HomeAssistant) -> Words:
    """The words in Home Assistant's language, loading its translation file if need be.

    Awaited where a dialog begins and when an entry is set up, so that the
    pieces built where nothing can be awaited (``words``) find them loaded.
    """
    language = hass.config.language
    return Words(language, await async_get_translations(hass, language, CATEGORY, [DOMAIN]))


@callback
def words(hass: HomeAssistant) -> Words:
    """The words in Home Assistant's language, as far as they are loaded already."""
    language = hass.config.language
    return Words(language, async_get_cached_translations(hass, language, CATEGORY, DOMAIN))
