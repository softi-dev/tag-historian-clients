"""The integration's own brand images, where HACS and Home Assistant look.

A custom integration has shipped its icon in a ``brand/`` folder of its own
since Home Assistant 2026.3, instead of through a pull request to
home-assistant/brands. Three things read that folder, and each fails quietly:

- the HACS action's brands check looks for ``brand/icon.png`` under the
  integration's directory and, finding nothing, falls back to the brands
  repository - where this domain is not listed, so the check fails. It is the
  check validate.yml used to ignore, and the HACS default store accepts no
  repository whose action needs an ignore;
- Home Assistant serves the files to its own frontend, and a name it does not
  know is simply never asked for - the grey placeholder shows instead, with
  nothing in any log;
- the Marketplace reads the same path at the latest release, straight from
  GitHub.

So the names, the sizes and the format are held here, the only place a typo
or a re-export at the wrong size would be noticed before a user's sidebar.
The images themselves are made from the product's own icon; this file holds
their shape, not their look.
"""

from __future__ import annotations

import struct
from pathlib import Path

import pytest

BRAND = Path(__file__).resolve().parent.parent / "custom_components" / "tag_historian" / "brand"

# The names Home Assistant's brands proxy serves (ALLOWED_IMAGES in
# homeassistant/components/brands/const.py). A logo is optional: a missing
# logo.png falls back to icon.png there, as it does on the brands CDN.
ALLOWED = {
    "icon.png",
    "icon@2x.png",
    "logo.png",
    "logo@2x.png",
    "dark_icon.png",
    "dark_icon@2x.png",
    "dark_logo.png",
    "dark_logo@2x.png",
}

PNG_SIGNATURE = b"\x89PNG\r\n\x1a\n"
COLOR_TYPE_RGBA = 6
ADAM7 = 1


def _header(path: Path) -> tuple[int, int, int, int]:
    """Width, height, colour type and interlace method, from the IHDR chunk.

    Read by hand rather than through Pillow, which is not a test requirement
    and would be one more thing to pin for twenty-nine bytes.
    """
    data = path.read_bytes()
    assert data[:8] == PNG_SIGNATURE, f"{path.name} is not a PNG"
    _length, chunk = struct.unpack(">I4s", data[8:16])
    assert chunk == b"IHDR", f"{path.name} does not start with its header chunk"
    width, height, _depth, color_type, _compression, _filter, interlace = struct.unpack(
        ">IIBBBBB", data[16:29]
    )
    return width, height, color_type, interlace


def test_the_icon_hacs_checks_for_is_there() -> None:
    """The exact path the HACS action's brands check asks the tree for."""
    assert (BRAND / "icon.png").is_file()


def test_every_file_in_the_folder_is_one_home_assistant_asks_for() -> None:
    """``Icon.png`` or ``icon-2x.png`` would sit there and never be shown."""
    unknown = sorted(p.name for p in BRAND.iterdir() if p.name not in ALLOWED)
    assert not unknown, f"Home Assistant never asks for {unknown}"


@pytest.mark.parametrize(("name", "size"), [("icon.png", 256), ("icon@2x.png", 512)])
def test_icons_are_square_transparent_and_the_size_home_assistant_wants(
    name: str, size: int
) -> None:
    """256 and 512 pixels square, with an alpha channel, interlaced.

    The sizes are the brands specification's for icons; transparency and
    interlacing are what it prefers. The icon is the product's rounded square,
    so the alpha channel is what keeps its corners from turning into a dark
    box on a light card.
    """
    width, height, color_type, interlace = _header(BRAND / name)
    assert (width, height) == (size, size)
    assert color_type == COLOR_TYPE_RGBA, f"{name} has no alpha channel"
    assert interlace == ADAM7, f"{name} is not interlaced"
