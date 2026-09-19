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
    clear_missing_runtime_logs,
    prepare_task_archive,
    preserve_run_files,
    publish_deliverables,
    record_runtime_log,
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

    def _run(self, run_id: str, phase: str = "plan"):
        return prepare_task_archive(
            working_root=self.work,
            kb_root=self.kb,
            task_id=7,
            task_text="Review synthetic result",
            run_id=run_id,
            phase=phase,
            agent_display_name="Agent Example",
        )

    def test_repeated_runs_restate_the_task_instead_of_accumulating(self):
        """The document says where the task IS, not how it got there.

        It used to be append-only, so every run added a section and every
        result appended its COMPLETE work text again. Eight runs of one real
        task produced seven hundred lines carrying the same plan four times
        over, with no statement anywhere of what was currently true.
        """
        first = self._run("b" * 32)
        append_result(first, result={
            "outcome": "awaiting_plan",
            "summary": "First pass.",
            "work_markdown": "ORIGINAL PLAN BODY",
        })
        second = self._run("c" * 32)
        append_result(second, result={
            "outcome": "completed",
            "summary": "Second pass supersedes the first.",
            "work_markdown": "REVISED PLAN BODY",
        })

        for path in (first.working_directory / "README.md", first.task_file):
            text = path.read_text(encoding="utf-8")
            # Only the current answer is restated in full.
            self.assertIn("REVISED PLAN BODY", text)
            self.assertNotIn("ORIGINAL PLAN BODY", text)
            # The superseded run is still named, and still on disk.
            self.assertIn("b" * 32, text)
            self.assertIn("First pass.", text)
            self.assertIn("## Objective", text)
            self.assertIn("**Status:** completed", text)
            # One "current result" heading however many runs there were.
            self.assertEqual(text.count("## Current result"), 1)
            self.assertEqual(text.count("## Work"), 1)

    def test_a_corrupt_log_does_not_cost_the_result(self):
        """A bad cache must never block recording; runs/ holds the evidence."""
        paths = self._run("d" * 32)
        (paths.working_directory / ".task-log.json").write_text(
            "{not json", encoding="utf-8"
        )
        append_result(paths, result={
            "outcome": "completed",
            "summary": "Recorded anyway.",
            "work_markdown": "BODY",
        })
        text = (paths.working_directory / "README.md").read_text(
            encoding="utf-8"
        )
        self.assertIn("Recorded anyway.", text)

    def test_ledger_names_the_private_structured_runtime_log(self):
        paths = self._run("f" * 32)

        record_runtime_log(paths, "runtime-session.json")

        text = (paths.working_directory / "README.md").read_text(
            encoding="utf-8"
        )
        self.assertIn(
            "runs/plan-" + "f" * 32 + "/runtime-session.json", text
        )

    def test_rotated_runtime_log_is_removed_from_the_ledger(self):
        paths = self._run("0" * 32)
        record_runtime_log(paths, "runtime-session.json")

        clear_missing_runtime_logs(paths)

        text = (paths.working_directory / "README.md").read_text(
            encoding="utf-8"
        )
        self.assertNotIn("runtime-session.json", text)

    def test_deliverables_are_published_to_the_task_folder(self):
        """The folder is the deliverable surface, not just an evidence store."""
        paths = self._run("e" * 32)
        (self.run / "Cost Breakdown.xlsx").write_bytes(b"synthetic")
        nested = self.run / "nested"
        nested.mkdir()
        (nested / "Draft reply.txt").write_text("synthetic", encoding="utf-8")
        (self.run / "result-artifacts.json").write_text(
            json.dumps(["Cost Breakdown.xlsx", "nested/Draft reply.txt"]),
            encoding="utf-8",
        )

        published = publish_deliverables(paths, self.run)

        self.assertEqual(
            set(published), {"Cost Breakdown.xlsx", "Draft reply.txt"}
        )
        # Flattened to the top level, where the reader opens the folder.
        self.assertTrue(
            (paths.working_directory / "Cost Breakdown.xlsx").is_file()
        )
        self.assertTrue(
            (paths.working_directory / "Draft reply.txt").is_file()
        )
        # And the README is not overwritten by a deliverable of that name.
        (self.run / "README.md").write_text("HOSTILE", encoding="utf-8")
        (self.run / "result-artifacts.json").write_text(
            json.dumps(["README.md"]), encoding="utf-8"
        )
        self.assertEqual(publish_deliverables(paths, self.run), ())
        self.assertNotIn(
            "HOSTILE",
            (paths.working_directory / "README.md").read_text(
                encoding="utf-8"
            ),
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
