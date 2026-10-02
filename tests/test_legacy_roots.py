"""Tests for legacy root path translations."""

from __future__ import annotations

import pytest

from foxhound.legacy_roots import RootTranslations


def test_validation_rules() -> None:
    # Reject non-absolute prefixes
    with pytest.raises(ValueError):
        RootTranslations([("relative/path", "/var/new")])
    with pytest.raises(ValueError):
        RootTranslations([("/var/old", "relative/path")])

    # Reject unnormalised prefixes (trailing slash or '..')
    with pytest.raises(ValueError):
        RootTranslations([("/var/old/", "/var/new")])
    with pytest.raises(ValueError):
        RootTranslations([("/var/old/../foo", "/var/new")])
    with pytest.raises(ValueError):
        RootTranslations([("/var/old//bar", "/var/new")])

    # Reject identical old and new
    with pytest.raises(ValueError):
        RootTranslations([("/var/old", "/var/old")])

    # Reject duplicates
    with pytest.raises(ValueError):
        RootTranslations([("/var/old", "/var/new1"), ("/var/old", "/var/new2")])

    # Reject > 16 pairs
    too_many = [(f"/old/{i}", f"/new/{i}") for i in range(17)]
    with pytest.raises(ValueError):
        RootTranslations(too_many)


def test_translate_path_and_text_boundaries() -> None:
    trans = RootTranslations([("/srv/data/old_kb", "/srv/data/kb")])

    # Single path translation
    assert trans.translate_path("/srv/data/old_kb/doc.md") == "/srv/data/kb/doc.md"
    assert trans.translate_path("/srv/data/old_kb") == "/srv/data/kb"
    # Should not translate if it is a prefix of a directory name
    assert trans.translate_path("/srv/data/old_kb-2/doc.md") == "/srv/data/old_kb-2/doc.md"

    # Text translation at boundaries
    text = (
        "Check `/srv/data/old_kb/doc.md`, '/srv/data/old_kb' and "
        "/srv/data/old_kb. Also /srv/data/old_kb-2 should stay unchanged."
    )
    expected = (
        "Check `/srv/data/kb/doc.md`, '/srv/data/kb' and "
        "/srv/data/kb. Also /srv/data/old_kb-2 should stay unchanged."
    )
    assert trans.translate_text(text) == expected


def test_longest_prefix_wins() -> None:
    trans = RootTranslations([
        ("/srv/data", "/opt/data"),
        ("/srv/data/nested", "/opt/special"),
    ])

    text = "Visit /srv/data/nested/file.txt and /srv/data/other.txt"
    expected = "Visit /opt/special/file.txt and /opt/data/other.txt"
    assert trans.translate_text(text) == expected


def test_no_double_translation() -> None:
    # A translates to B, B translates to C. Single-pass should not chain A -> C.
    trans = RootTranslations([
        ("/srv/first", "/srv/second"),
        ("/srv/second", "/srv/third"),
    ])

    text = "Source is /srv/first/item"
    expected = "Source is /srv/second/item"
    assert trans.translate_text(text) == expected


def test_home_relative() -> None:
    trans = RootTranslations(
        [
            ("/srv/accounts/example/old_vault", "/srv/accounts/example/new_vault"),
            ("/srv/accounts/example/legacy", "/opt/external"),
        ],
        home="/srv/accounts/example",
    )

    text = "File at ~/old_vault/notes.md and ~/legacy/config.json"
    expected = "File at ~/new_vault/notes.md and /opt/external/config.json"
    assert trans.translate_text(text) == expected

    # Also still translates absolute
    assert (
        trans.translate_text("/srv/accounts/example/old_vault/notes.md")
        == "/srv/accounts/example/new_vault/notes.md"
    )


def test_unchanged_text() -> None:
    trans = RootTranslations([("/srv/old", "/srv/new")])
    text = "No matching paths here /srv/older /other/path"
    assert trans.translate_text(text) == text
    assert trans.translate_text("") == ""
