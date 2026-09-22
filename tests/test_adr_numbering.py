#!/usr/bin/env python3
"""The ADR index is a namespace, and merge order must not be able to assign it.

Three collisions reached `main` before this existed -- 0023, 0036 and 0046 --
because a number is chosen when a branch is written and checked by nobody when
it lands. Two branches opened in the same week pick the same next number, both
suites pass, and the second merge silently makes the first ADR's number
ambiguous. Every reference of the form "ADR 0046" then points at two documents.

These tests fail the merge instead, and they only read the directory, so a new
ADR needs no registration anywhere.
"""

from __future__ import annotations

import re
import unittest
from collections import defaultdict
from pathlib import Path

ARCHITECTURE = Path(__file__).resolve().parents[1] / "docs" / "architecture"

#: `0046-worker-resolved-from-the-running-release.md`
FILENAME = re.compile(r"^(?P<number>\d{4})-(?P<slug>[a-z0-9]+(?:-[a-z0-9]+)*)\.md$")

#: `# ADR 0046: Worker resolved from the running release`
HEADING = re.compile(r"^#\s+ADR\s+(?P<number>\d{4}):\s+\S")

#: A markdown link that names an ADR number and a target: `[ADR 0046](...md)`.
LINK = re.compile(r"\[ADR\s+(?P<number>\d{4})\]\((?P<target>[^)]+\.md)\)")

REPOSITORY = Path(__file__).resolve().parents[1]
SEARCHED = ("*.md", "*.py")


def _adr_files() -> list[Path]:
    return sorted(path for path in ARCHITECTURE.iterdir() if path.suffix == ".md")


class ADRNumberingTests(unittest.TestCase):
    def test_every_adr_filename_is_numbered(self) -> None:
        for path in _adr_files():
            self.assertRegex(path.name, FILENAME, f"{path.name} is not `NNNN-slug.md`")

    def test_no_two_adrs_claim_the_same_number(self) -> None:
        by_number: dict[str, list[str]] = defaultdict(list)
        for path in _adr_files():
            match = FILENAME.match(path.name)
            if match is not None:
                by_number[match.group("number")].append(path.name)
        collisions = {
            number: sorted(names)
            for number, names in by_number.items()
            if len(names) > 1
        }
        self.assertEqual(
            collisions, {},
            "two ADRs claim one number, so every reference to it is ambiguous; "
            "renumber the later one to the next free number and update its "
            "references",
        )

    def test_each_adr_heading_matches_its_filename(self) -> None:
        for path in _adr_files():
            match = FILENAME.match(path.name)
            if match is None:
                continue
            first = path.read_text(encoding="utf-8").splitlines()[0]
            heading = HEADING.match(first)
            self.assertIsNotNone(
                heading, f"{path.name} does not open with `# ADR NNNN: Title`",
            )
            self.assertEqual(
                heading.group("number"), match.group("number"),
                f"{path.name} is titled ADR {heading.group('number')}; a rename "
                "that leaves the heading behind is how a number goes ambiguous",
            )

    def test_every_adr_reference_resolves_to_that_adr(self) -> None:
        """A renumber that misses a reference leaves a link to a deleted file."""
        for pattern in SEARCHED:
            for source in REPOSITORY.rglob(pattern):
                if ".git" in source.parts or ".venv" in source.parts:
                    continue
                if source.resolve() == Path(__file__).resolve():
                    continue  # this file quotes the link shape it checks
                try:
                    text = source.read_text(encoding="utf-8")
                except (OSError, UnicodeDecodeError):
                    continue
                for link in LINK.finditer(text):
                    target = (source.parent / link.group("target")).resolve()
                    relative = source.relative_to(REPOSITORY)
                    self.assertTrue(
                        target.is_file(),
                        f"{relative} links to {link.group('target')}, which "
                        "does not exist",
                    )
                    self.assertTrue(
                        target.name.startswith(link.group("number")),
                        f"{relative} says ADR {link.group('number')} but links "
                        f"to {target.name}",
                    )


if __name__ == "__main__":
    unittest.main()
