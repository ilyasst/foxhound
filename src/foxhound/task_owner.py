"""Safe presentation of a task owner independently of identity storage."""

from __future__ import annotations

import re
import unicodedata
from collections.abc import Iterable, Mapping


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


def normalized_owner(value: str) -> str:
    """Fold one owner label to its comparison form.

    Accent-folded, case-folded, and reduced to alphanumeric words, so that
    labels differing only in diacritics, case, or punctuation compare equal.
    Presentation text is the only input; the structured reference decides
    whether comparing at all is legitimate.
    """
    decomposed = unicodedata.normalize("NFKD", value).casefold()
    return " ".join(
        "".join(
            character if character.isalnum() else " "
            for character in decomposed
            if not unicodedata.combining(character)
        ).split()
    )


def normalized_aliases(values: Iterable[str]) -> frozenset[str]:
    """Comparison forms for the reader's own names, ignoring blanks."""
    return frozenset(
        normalized for normalized in (
            normalized_owner(str(value)) for value in values
        ) if normalized
    )


def reader_owned(
    row: Mapping[str, object], reader_aliases: frozenset[str]
) -> bool:
    """Whether this task's owner is confidently the reader.

    Fails closed. An unresolved, group, provisional, or version-zero owner is
    not the reader, because acting on an uncertain label would make a guess
    into authorization. These are the same conditions ADR 0034 requires before
    a card may name an owner, kept in one place so admission and the card
    cannot disagree about who owns a task.
    """
    if not reader_aliases:
        return False
    kind = _column(row, "owner_kind")
    display = canonical_owner_display(
        _text_or_none(_column(row, "owner")), _text_or_none(kind)
    )
    if display is None or display == UNRESOLVED_DISPLAY:
        return False
    if kind not in {"person", "external"}:
        return False
    if _as_int(_column(row, "owner_ref_version")) != 1:
        return False
    if _as_int(_column(row, "owner_provisional")) != 0:
        return False
    return normalized_owner(display) in reader_aliases


def _column(row: Mapping[str, object], name: str) -> object:
    """One column, whether the row is a mapping or a `sqlite3.Row`.

    `sqlite3.Row` supports subscripting but not `get`, and raises rather than
    returning None for a column the query did not select. A selection that
    forgets an owner column must fail closed here, not crash the scheduler.
    """
    try:
        return row[name]
    except (IndexError, KeyError):
        return None


def _text_or_none(value: object) -> str | None:
    return value if isinstance(value, str) else None


def _as_int(value: object) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    return value
