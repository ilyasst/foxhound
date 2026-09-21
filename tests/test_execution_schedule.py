#!/usr/bin/env python3
"""Synthetic tests for bounded one-shot execution scheduling."""

from __future__ import annotations

from foxhound import migrate_database

import json
import sqlite3
import tempfile
import unittest
from contextlib import closing, redirect_stderr, redirect_stdout
from io import StringIO
from pathlib import Path
from unittest import mock

from foxhound.agent_profiles import AgentProfile, WORKER_COMMAND_TOKEN
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
        migrate_database(self.database)
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
        self.assertEqual(
            first, {"ok": True, "capped": 0, "remaining": 1, "scheduled": 1}
        )
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
            {"ok": True, "capped": 0, "remaining": 0, "scheduled": 1},
        )

        replay_stdout = StringIO()
        with redirect_stdout(replay_stdout):
            self.assertEqual(main([
                "--database", str(self.database), "--limit", "10"
            ]), 0)
        self.assertEqual(
            json.loads(replay_stdout.getvalue()),
            {"ok": True, "capped": 0, "remaining": 0, "scheduled": 0},
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

    def test_installed_default_profile_is_bound_before_start(self):
        directory = self.root / "profiles"
        directory.mkdir(mode=0o700)
        profile = self._profile()
        manifest = directory / f"{profile.profile_id}.json"
        manifest.write_text(json.dumps(profile.document()), encoding="utf-8")
        manifest.chmod(0o600)

        stdout = StringIO()
        with redirect_stdout(stdout):
            self.assertEqual(main([
                "--database", str(self.database),
                "--agent-profile-directory", str(directory),
                "--default-agent-profile", profile.profile_id,
                "--limit", "1",
            ]), 0)
        scheduled = TaskExecutionService(self.database).get(1)
        self.assertEqual(scheduled.agent_profile_id, profile.profile_id)
        self.assertEqual(scheduled.agent_profile_revision, profile.revision)
        self.assertEqual(
            json.loads(stdout.getvalue()),
            {"ok": True, "capped": 0, "remaining": 1, "scheduled": 1},
        )

    def test_invalid_default_profile_configuration_schedules_nothing(self):
        missing = self.root / "missing-profile"
        missing.mkdir(mode=0o700)

        execute_only = self.root / "execute-only"
        execute_only.mkdir(mode=0o700)
        profile = self._profile(phases=("execute",))
        manifest = execute_only / f"{profile.profile_id}.json"
        manifest.write_text(json.dumps(profile.document()), encoding="utf-8")
        manifest.chmod(0o600)

        malformed = self.root / "malformed"
        malformed.mkdir(mode=0o700)
        invalid = malformed / "example-specialist.json"
        invalid.write_text("{}", encoding="utf-8")
        invalid.chmod(0o600)

        unsafe = self.root / "unsafe"
        unsafe.mkdir(mode=0o755)

        cases = (
            (missing, "missing"),
            (execute_only, profile.profile_id),
            (malformed, "example-specialist"),
            (unsafe, "general"),
        )
        for directory, profile_id in cases:
            with self.subTest(profile_id=profile_id):
                stdout = StringIO()
                stderr = StringIO()
                with redirect_stdout(stdout), redirect_stderr(stderr):
                    self.assertEqual(main([
                        "--database", str(self.database),
                        "--agent-profile-directory", str(directory),
                        "--default-agent-profile", profile_id,
                    ]), 78)
                self.assertEqual(stdout.getvalue(), "")
                self.assertEqual(
                    stderr.getvalue(),
                    "foxhound execution schedule: configuration unavailable\n",
                )
        with closing(sqlite3.connect(self.database)) as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM task_execution_workflows"
                ).fetchone()[0],
                0,
            )

    def test_a_saturated_cap_is_reported_and_not_mistaken_for_idle(self):
        """A cap that stops admission must not report as an idle pass.

        Both tasks are eligible; a cap of zero admits neither. Before
        `capped` existed this printed `scheduled: 0, remaining: 2` -- the
        same shape a pass with nothing to do prints -- so a deployment whose
        intake had stopped entirely looked healthy on every run.
        """
        stdout, stderr = StringIO(), StringIO()
        with redirect_stdout(stdout), redirect_stderr(stderr):
            self.assertEqual(main([
                "--database", str(self.database),
                "--plan-ready-cap", "0",
                "--awaiting-reader-cap", "0",
            ]), 0)
        self.assertEqual(
            json.loads(stdout.getvalue()),
            {"ok": True, "capped": 2, "remaining": 2, "scheduled": 0},
        )
        self.assertEqual(
            stderr.getvalue(),
            "foxhound execution schedule: "
            "2 eligible task(s) held by a capacity cap\n",
        )
        self.assertNotIn("Synthetic task", stderr.getvalue())
        with closing(sqlite3.connect(self.database)) as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM task_execution_workflows"
                ).fetchone()[0],
                0,
            )

    def test_a_paged_pass_is_not_reported_as_capped(self):
        """`limit` leaving work behind is paging, not starvation.

        The distinction is the whole point: a paged pass drains by itself on
        the next tick, so reporting it the same way a saturated cap is
        reported would make the new signal noise and train a reader to
        ignore it.
        """
        stdout, stderr = StringIO(), StringIO()
        with redirect_stdout(stdout), redirect_stderr(stderr):
            self.assertEqual(main([
                "--database", str(self.database), "--limit", "1",
            ]), 0)
        self.assertEqual(
            json.loads(stdout.getvalue()),
            {"ok": True, "capped": 0, "remaining": 1, "scheduled": 1},
        )
        self.assertEqual(stderr.getvalue(), "")

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

    @staticmethod
    def _profile(
        *, phases: tuple[str, ...] = ("plan", "execute", "external_action")
    ) -> AgentProfile:
        return AgentProfile(
            profile_id="example-specialist",
            display_name="Example Specialist",
            runtime="hermes",
            prompt_template=(
                f"Use {WORKER_COMMAND_TOKEN} and synthetic evidence only."
            ),
            toolsets=("terminal",),
            max_turns=12,
            timeout_seconds=300,
            claim_lease_seconds=600,
            heartbeat_seconds=60,
            kill_grace_seconds=30,
            allowed_phases=phases,
        )


if __name__ == "__main__":
    unittest.main()

    def test_skip_planning_requires_execution_grants_to_start(self):
        stdout = StringIO()
        stderr = StringIO()
        with redirect_stdout(stdout), redirect_stderr(stderr):
            self.assertEqual(main([
                "--database", str(self.database),
                "--skip-planning-for", "issue",
                "--plan-without-asking", "issue",
            ]), 78)
        self.assertEqual(stderr.getvalue(), "foxhound execution schedule: configuration unavailable\n")

    def test_skip_planning_succeeds_with_execute_grants(self):
        # We need to insert a candidate for task 1
        with closing(sqlite3.connect(self.database)) as connection:
            connection.execute(
                "INSERT INTO candidate_inbox(candidate_id, source_kind, "
                "schema_version, created_at, content) "
                "VALUES('test1', 'issue', 1, '2030-01-01T12:00:00', '{}')"
            )
            connection.execute(
                "INSERT INTO task_candidate_bindings(task_id, candidate_id, relation) "
                "VALUES(1, 'test1', 'accepted')"
            )
            connection.commit()

        stdout = StringIO()
        stderr = StringIO()
        with redirect_stdout(stdout), redirect_stderr(stderr):
            self.assertEqual(main([
                "--database", str(self.database),
                "--skip-planning-for", "issue",
                "--plan-without-asking", "issue",
                "--execute-without-asking", "issue",
                "--limit", "1"
            ]), 0)

        with closing(sqlite3.connect(self.database)) as connection:
            phase = connection.execute(
                "SELECT phase FROM task_execution_workflows WHERE task_id=1"
            ).fetchone()[0]
            self.assertEqual(phase, "execute")
