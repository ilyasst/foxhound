#!/usr/bin/env python3
"""Synthetic tests for bounded one-shot execution scheduling."""

from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from contextlib import closing, redirect_stderr, redirect_stdout
from io import StringIO
from pathlib import Path
from unittest import mock

from foxhound.candidate_inbox import CandidateInbox
from foxhound.execution_schedule import main
from foxhound.task_execution import TaskExecutionService, WorkflowStatus


class ExecutionScheduleCommandTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.root.chmod(0o700)
        self.database = self.root / "foxhound.sqlite3"
        CandidateInbox(self.database).initialize()
        with closing(sqlite3.connect(self.database)) as connection:
            for task_id in (1, 2):
                connection.execute(
                    "INSERT INTO tasks(id,status,text,owner,due,version,"
                    "created_at,updated_at,closed_at) "
                    "VALUES(?,'open',?,NULL,NULL,1,?,?,NULL)",
                    (
                        task_id,
                        f"Synthetic task {task_id}",
                        "2030-01-01T12:00:00+00:00",
                        "2030-01-01T12:00:00+00:00",
                    ),
                )
            connection.commit()
        self.database.chmod(0o600)

    def test_command_is_bounded_content_free_and_idempotent(self):
        first_stdout = StringIO()
        with redirect_stdout(first_stdout):
            self.assertEqual(main([
                "--database", str(self.database), "--limit", "1"
            ]), 0)
        first = json.loads(first_stdout.getvalue())
        self.assertEqual(first, {"ok": True, "remaining": 1, "scheduled": 1})
        self.assertNotIn("Synthetic task", first_stdout.getvalue())
        self.assertEqual(
            TaskExecutionService(self.database).get(1).status,
            WorkflowStatus.AWAITING_START,
        )

        second_stdout = StringIO()
        with redirect_stdout(second_stdout):
            self.assertEqual(main([
                "--database", str(self.database), "--limit", "10"
            ]), 0)
        self.assertEqual(
            json.loads(second_stdout.getvalue()),
            {"ok": True, "remaining": 0, "scheduled": 1},
        )

        replay_stdout = StringIO()
        with redirect_stdout(replay_stdout):
            self.assertEqual(main([
                "--database", str(self.database), "--limit", "10"
            ]), 0)
        self.assertEqual(
            json.loads(replay_stdout.getvalue()),
            {"ok": True, "remaining": 0, "scheduled": 0},
        )

    def test_unsafe_database_parent_and_invalid_limit_fail_closed(self):
        self.root.chmod(0o755)
        for arguments in (
            ["--database", str(self.database)],
            ["--database", str(self.database), "--limit", "0"],
        ):
            stdout = StringIO()
            stderr = StringIO()
            with redirect_stdout(stdout), redirect_stderr(stderr):
                self.assertEqual(main(arguments), 78)
            self.assertEqual(stdout.getvalue(), "")
            self.assertEqual(
                stderr.getvalue(),
                "foxhound execution schedule: configuration unavailable\n",
            )

    def test_internal_failure_is_content_free(self):
        private_text = "Synthetic private task content"
        stderr = StringIO()
        with mock.patch(
            "foxhound.execution_schedule.run_schedule",
            side_effect=RuntimeError(private_text),
        ), redirect_stderr(stderr):
            self.assertEqual(main(["--database", str(self.database)]), 70)
        self.assertEqual(
            stderr.getvalue(),
            "foxhound execution schedule: scheduling failed\n",
        )
        self.assertNotIn(private_text, stderr.getvalue())


if __name__ == "__main__":
    unittest.main()
