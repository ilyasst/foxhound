#!/usr/bin/env python3
"""Synthetic tests for deployed-revision reporting."""

from __future__ import annotations

import subprocess
import tempfile
import unittest
from pathlib import Path

from foxhound import release_revision


def _git(root: Path, *arguments: str) -> None:
    subprocess.run(("git", "-C", str(root), *arguments),
                   capture_output=True, check=True)


class DescribeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        # describe() walks up three parents from a module file, so build that
        # shape rather than asserting against this repository.
        self.module = self.root / "src" / "package" / "module.py"
        self.module.parent.mkdir(parents=True)
        self.module.write_text("", encoding="utf-8")

    def _repository(self) -> None:
        _git(self.root, "init", "-q", "-b", "trunk")
        _git(self.root, "config", "user.email", "synthetic@example.com")
        _git(self.root, "config", "user.name", "Synthetic")
        _git(self.root, "add", "-A")
        _git(self.root, "commit", "-q", "-m", "synthetic")

    def test_a_clean_checkout_reports_commit_branch_and_state(self) -> None:
        self._repository()
        described = release_revision.describe(self.module)
        self.assertIn("(trunk)", described)
        self.assertTrue(described.endswith("clean"))

    def test_an_edited_checkout_is_reported_as_modified(self) -> None:
        """On a release checkout, edits mean the commit no longer names it."""
        self._repository()
        self.module.write_text("# edited\n", encoding="utf-8")
        self.assertTrue(release_revision.describe(self.module).endswith("modified"))

    def test_a_detached_checkout_says_so(self) -> None:
        self._repository()
        head = subprocess.run(
            ("git", "-C", str(self.root), "rev-parse", "HEAD"),
            capture_output=True, text=True, check=True).stdout.strip()
        _git(self.root, "checkout", "-q", head)
        self.assertIn("(detached)", release_revision.describe(self.module))

    def test_no_repository_is_unknown_rather_than_an_error(self) -> None:
        """A missing label must never stop a service from starting."""
        self.assertEqual(release_revision.describe(self.module),
                         release_revision.UNKNOWN)

    def test_the_description_never_contains_the_path(self) -> None:
        self._repository()
        self.assertNotIn(str(self.root), release_revision.describe(self.module))


if __name__ == "__main__":
    unittest.main()
