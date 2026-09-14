#!/usr/bin/env python3
"""Synthetic tests for durable task review files."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from foxhound.task_archive import (
    TaskArchiveError,
    append_result,
    prepare_task_archive,
    preserve_run_files,
)


class TaskArchiveTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.work = self.root / "Project Alpha" / "Tasks"
        self.kb = self.root / "Project Alpha KB" / "Tasks"
        self.run = self.root / "private-run"
        self.run.mkdir(mode=0o700)

    def _paths(self):
        return prepare_task_archive(
            working_root=self.work,
            kb_root=self.kb,
            task_id=7,
            task_text="Review synthetic result",
            run_id="a" * 32,
            phase="plan",
            agent_display_name="Agent Example",
            origin_kind="issue",
            origin_record="github.com/example/project-alpha",
            origin_item="42",
        )

    def test_two_stable_locations_hold_result_and_review_links(self):
        paths = self._paths()
        result = {
            "outcome": "completed",
            "summary": "The synthetic work already exists.",
            "work_markdown": (
                "Verified PR #12 and commit abcdef1234567. "
                "See [verification](https://github.com/example/project-alpha/"
                "actions/runs/123)."
            ),
            "questions": [],
            "external_actions": [],
            "deliverables": [],
        }
        append_result(
            paths,
            result=result,
            origin_kind="issue",
            origin_record="github.com/example/project-alpha",
            origin_item="42",
        )

        self.assertEqual(
            paths.working_directory.name,
            "T7-review-synthetic-result",
        )
        self.assertEqual(paths.task_file.name, "T7-review-synthetic-result.md")
        for document in (
            paths.working_directory / "README.md",
            paths.task_file,
        ):
            text = document.read_text(encoding="utf-8")
            self.assertIn("The synthetic work already exists.", text)
            self.assertIn("/issues/42", text)
            self.assertIn("/pull/12", text)
            self.assertIn("/commit/abcdef1234567", text)
            self.assertIn(str(paths.run_directory), text)

        renamed = prepare_task_archive(
            working_root=self.work,
            kb_root=self.kb,
            task_id=7,
            task_text="A later synthetic title",
            run_id="b" * 32,
            phase="execute",
            agent_display_name="Agent Example",
        )
        self.assertEqual(renamed.working_directory, paths.working_directory)
        self.assertEqual(renamed.task_file, paths.task_file)

    def test_only_explicit_evidence_is_preserved(self):
        paths = self._paths()
        files = {
            "agent-output.log": "Synthetic transcript.\n",
            "result-summary.txt": "Synthetic summary.\n",
            "notes/check.txt": "Synthetic check.\n",
            "run-state.json": "private capability",
            "agent-instructions.json": "private instructions",
            "unlisted.txt": "not requested",
        }
        for name, value in files.items():
            path = self.run / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(value, encoding="utf-8")
            path.chmod(0o600)
        manifest = self.run / "result-artifacts.json"
        manifest.write_text(json.dumps(["notes/check.txt"]), encoding="utf-8")
        manifest.chmod(0o600)

        copied = preserve_run_files(self.run, paths.run_directory)

        self.assertEqual(
            set(copied),
            {
                "agent-output.log",
                "result-summary.txt",
                "result-artifacts.json",
                "notes/check.txt",
            },
        )
        self.assertFalse((paths.run_directory / "run-state.json").exists())
        self.assertFalse(
            (paths.run_directory / "agent-instructions.json").exists()
        )
        self.assertFalse((paths.run_directory / "unlisted.txt").exists())

    def test_manifest_cannot_smuggle_state_or_escape_the_run(self):
        paths = self._paths()
        manifest = self.run / "result-artifacts.json"
        for value in (["../outside.txt"], ["nested/run-state.json"]):
            with self.subTest(value=value):
                manifest.write_text(json.dumps(value), encoding="utf-8")
                manifest.chmod(0o600)
                with self.assertRaises(TaskArchiveError):
                    preserve_run_files(self.run, paths.run_directory)


if __name__ == "__main__":
    unittest.main()
