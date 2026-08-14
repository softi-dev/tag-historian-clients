"""Nothing this integration displays about a plan may be typed into it.

A number copied out of the server's plan configuration into another file is
correct until the day the plans change, and then it is a lie living inside
somebody's Home Assistant with no way of knowing. The pattern, not any
individual number, is the thing to break.

So the rule is mechanical. Every tag limit, reading limit, plan name and price
the UI shows comes from ``GET /api/usage/limits`` at runtime - the server's
single canonical source, the same one its own quota enforcement reads. This
test is what makes the rule enforceable rather than aspirational - and it is
deliberately written without restating any real limit, so that this file can
never become the copied constant it exists to forbid.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

PACKAGE = Path(__file__).resolve().parent.parent / "custom_components" / "tag_historian"

# Currency markers never belong in this package: every price the UI shows is
# the API's to state, and the integration does not show prices at all.
FORBIDDEN = (
    "€",
    "EUR",
)

# A plan limit written out in prose: "50 tags", "500 series". Any digit
# directly quantifying tags or series is a copied constant however small it
# looks - the UI's numbers come from GET /api/usage/limits at runtime. The
# lookbehind keeps it from firing on a digit that is part of a larger token,
# like the "1/0 series" a binary sensor's chart is described as.
PLAN_LIMIT_IN_PROSE = re.compile(r"(?<![\d/.])\b\d+\s+(?:tags?|series)\b", re.IGNORECASE)

# Any numeric literal long enough to be a reading allowance or a storage
# quota. The two below are mechanics, not plan values, and each earns its
# place here or the test fails:
#   86400 - seconds in a day, config_flow's readings-per-day arithmetic;
#   10000 - MAX_QUEUE_LENGTH, the in-memory buffer bound during an outage.
# Adding a number to this allowlist is the review surface: if it came out of
# the server's plan configuration, it does not belong in this package at all.
BIG_LITERAL = re.compile(r"\b\d{5,}\b")
ALLOWED_BIG_LITERALS = {"86400", "10000"}


def _sources() -> list[Path]:
    return sorted(PACKAGE.rglob("*.py")) + sorted(PACKAGE.rglob("*.json"))


def test_the_package_has_sources_to_check() -> None:
    """A path typo here would make every assertion below vacuously true."""
    names = {path.name for path in _sources()}
    assert {"const.py", "config_flow.py", "manifest.json", "en.json"} <= names


@pytest.mark.parametrize("needle", FORBIDDEN)
def test_no_currency_marker_is_written_into_the_package(needle) -> None:
    offenders = [
        str(path.relative_to(PACKAGE))
        for path in _sources()
        if needle in path.read_text(encoding="utf-8")
    ]
    assert not offenders, (
        f"{needle!r} appears in {offenders}. Prices are the API's to state; "
        "this integration never shows one."
    )


def test_no_plan_limit_is_written_out_in_a_sentence() -> None:
    offenders = [
        f"{path.relative_to(PACKAGE)}: {match.group(0)!r}"
        for path in _sources()
        for match in PLAN_LIMIT_IN_PROSE.finditer(path.read_text(encoding="utf-8"))
    ]
    assert not offenders, (
        f"a plan limit is written out in {offenders}. It has to come from "
        "GET /api/usage/limits at runtime."
    )


def test_no_unexplained_large_number_is_written_into_the_package() -> None:
    """A reading allowance or a storage quota is always a large literal.

    Every large number in the package must be a named, explained mechanic in
    the allowlist above - which is exactly where a review will see it.
    """
    offenders = [
        f"{path.relative_to(PACKAGE)}: {match.group(0)}"
        for path in _sources()
        for match in BIG_LITERAL.finditer(path.read_text(encoding="utf-8"))
        if match.group(0) not in ALLOWED_BIG_LITERALS
    ]
    assert not offenders, (
        f"unexplained large literals in {offenders}. If it is a mechanic, add "
        "it to ALLOWED_BIG_LITERALS with a reason; if it came out of the plan "
        "configuration, it must be read from GET /api/usage/limits instead."
    )


def test_the_prose_guard_would_catch_one() -> None:
    """A regex nothing can trip is a regex nobody notices has stopped working."""
    assert PLAN_LIMIT_IN_PROSE.search("Your plan allows 50 tags.")
    assert PLAN_LIMIT_IN_PROSE.search("500 series is this plan's allowance.")
    # And it does not fire on a number that has nothing to do with a plan:
    assert not PLAN_LIMIT_IN_PROSE.search("Batches are capped at 100 points.")
    # nor on a digit that is part of a larger token:
    assert not PLAN_LIMIT_IN_PROSE.search("charted as a 1/0 series")


def test_the_tier_name_is_never_read_out_of_the_response() -> None:
    """The usage-limits response carries a tier field whose spellings are the
    server's internal enum, not the plan names a customer sees on the pricing
    page. The integration does not parse the field at all, so an internal
    spelling can never leak into a screen by accident.
    """
    for path in PACKAGE.rglob("*.py"):
        text = path.read_text(encoding="utf-8")
        for access in ('.get("tier")', '["tier"]', ".get('tier')", "['tier']"):
            assert access not in text, f"{path.name} reads the tier field"
