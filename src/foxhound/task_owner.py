"""Safe presentation of a task owner independently of identity storage."""

from __future__ import annotations

import re


UNRESOLVED_DISPLAY = "(unassigned)"
_SPEAKER_TOKEN = r"SPK_\d+(?:/(?:SPK_)?\d+)*"
_PARENTHETICAL_SPEAKER = re.compile(rf"\s*\(\s*{_SPEAKER_TOKEN}\s*\)")
_BARE_SPEAKER = re.compile(rf"(?<![A-Za-z0-9_]){_SPEAKER_TOKEN}(?!\d)")


def canonical_owner_display(
    owner: str | None, owner_kind: str | None
) -> str | None:
    """Return card-safe display text without treating it as identity.

    Version-zero rows may still cache a historical label containing a raw
    speaker token. Removing that token is presentation hygiene only; matching
    and workflow conditions must use the separate structured reference.
    """
    if owner_kind == "unresolved":
        return UNRESOLVED_DISPLAY
    if owner is None:
        return None
    display = _PARENTHETICAL_SPEAKER.sub("", owner)
    display = _BARE_SPEAKER.sub("", display)
    display = re.sub(r"\s{2,}", " ", display).strip(" \t,;:()[]—-")
    return display or UNRESOLVED_DISPLAY
