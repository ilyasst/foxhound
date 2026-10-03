#!/usr/bin/env python3
"""Synthetic tests for task deadline problem detection."""

from __future__ import annotations

from contextlib import closing
from datetime import date
from pathlib import Path
import sqlite3
import tempfile
import unittest

from foxhound import migrate_database
from foxhound.task_deadline_problems import DeadlineProblems, find_problems


class TaskDeadlineProblemsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.database = Path(self.temporary.name) / "foxhound.sqlite3"
        migrate_database(self.database)
        self.now = "2026-10-01T12:00:00+00:00"

    def _insert_task(
        self,
        connection: sqlite3.Connection,
        task_id: int,
        status: str = "open",
        due: str | None = None,
        owner: str | None = None,
        owner_kind: str | None = None,
        owner_ref_version: int = 1,
        owner_provisional: int = 0,
    ) -> None:
        connection.execute(
            "INSERT INTO tasks(id, status, text, owner, owner_kind, owner_ref_version, "
            "owner_provisional, due, version, created_at, updated_at, closed_at) "
            "VALUES(?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, NULL)",
            (
                task_id,
                status,
                f"Synthetic task {task_id}",
                owner,
                owner_kind,
                owner_ref_version,
                owner_provisional,
                due,
                self.now,
                self.now,
            ),
        )

    def _insert_change_set(
        self,
        connection: sqlite3.Connection,
        change_set_id: int,
        task_id: int,
    ) -> None:
        connection.execute(
            "INSERT INTO task_scheduling_change_sets("
            "id, target_task_id, target_task_version, expected_workflow_version, "
            "kind, state, research_receipt_id, research_document_digest, "
            "recommendation_digest, recommendation_json, resulting_workflow_version, "
            "created_at, updated_at) "
            "VALUES(?, ?, 1, 1, 'after_task_completed', 'active', 'receipt-test', ?, ?, '{}', 1, ?, ?)",
            (change_set_id, task_id, "0" * 64, "0" * 64, self.now, self.now),
        )

    def _insert_dependency(
        self,
        connection: sqlite3.Connection,
        depends_on_task_id: int,
        task_id: int,
        change_set_id: int,
        state: str = "active",
    ) -> None:
        connection.execute(
            "INSERT INTO task_scheduling_conditions("
            "task_id, task_version, kind, depends_on_task_id, not_before, "
            "state, change_set_id, created_at, updated_at) "
            "VALUES(?, 1, 'after_task_completed', ?, NULL, ?, ?, ?, ?)",
            (task_id, depends_on_task_id, state, change_set_id, self.now, self.now),
        )

    def test_healthy_chain_no_problems(self) -> None:
        today = date(2026, 10, 1)
        reader_aliases = ["Alice"]

        with closing(sqlite3.connect(self.database)) as connection:
            connection.row_factory = sqlite3.Row
            # Task 1 (due 2026-10-10) -> Task 2 (due 2026-10-15)
            # Both owned by Alice
            self._insert_task(connection, 1, due="2026-10-10", owner="Alice", owner_kind="person")
            self._insert_task(connection, 2, due="2026-10-15", owner="Alice", owner_kind="person")
            self._insert_change_set(connection, 1, 2)
            self._insert_dependency(connection, depends_on_task_id=1, task_id=2, change_set_id=1)
            connection.commit()

            problems = find_problems(connection, today=today, reader_aliases=reader_aliases)
            self.assertEqual(problems.cycles, ())
            self.assertEqual(problems.infeasible, ())
            self.assertEqual(problems.foreign_blockers, ())

    def test_dependency_cycle_detected(self) -> None:
        today = date(2026, 10, 1)
        reader_aliases = ["Alice"]

        with closing(sqlite3.connect(self.database)) as connection:
            connection.row_factory = sqlite3.Row
            # Cycle between 1 and 2
            self._insert_task(connection, 1, due="2026-10-10", owner="Alice", owner_kind="person")
            self._insert_task(connection, 2, due="2026-10-15", owner="Alice", owner_kind="person")
            self._insert_change_set(connection, 1, 2)
            self._insert_dependency(connection, depends_on_task_id=1, task_id=2, change_set_id=1)
            self._insert_change_set(connection, 2, 1)
            self._insert_dependency(connection, depends_on_task_id=2, task_id=1, change_set_id=2)
            connection.commit()

            problems = find_problems(connection, today=today, reader_aliases=reader_aliases)
            self.assertEqual(len(problems.cycles), 1)
            self.assertEqual(set(problems.cycles[0]), {1, 2})
            self.assertEqual(problems.infeasible, ())
            self.assertEqual(problems.foreign_blockers, ())

    def test_infeasible_chain_detected(self) -> None:
        # Predecessor open, but dependant deadline is already past
        today = date(2026, 10, 10)
        reader_aliases = ["Alice"]

        with closing(sqlite3.connect(self.database)) as connection:
            connection.row_factory = sqlite3.Row
            # Task 1 open, no due date
            self._insert_task(connection, 1, status="open", due=None, owner="Alice", owner_kind="person")
            # Task 2 open, due 2026-10-05 (past today!)
            self._insert_task(connection, 2, status="open", due="2026-10-05", owner="Alice", owner_kind="person")
            self._insert_change_set(connection, 1, 2)
            self._insert_dependency(connection, depends_on_task_id=1, task_id=2, change_set_id=1)
            connection.commit()

            problems = find_problems(connection, today=today, reader_aliases=reader_aliases)
            # Task 2 has predecessor 1 open and deadline 2026-10-05 < 2026-10-10
            self.assertEqual(problems.cycles, ())
            self.assertIn((2, date(2026, 10, 5)), problems.infeasible)
            self.assertEqual(problems.foreign_blockers, ())

    def test_foreign_blocker_detected(self) -> None:
        today = date(2026, 10, 1)
        reader_aliases = ["Alice"]

        with closing(sqlite3.connect(self.database)) as connection:
            connection.row_factory = sqlite3.Row
            # Task 1 owned by Bob (open)
            self._insert_task(connection, 1, status="open", due="2026-10-10", owner="Bob", owner_kind="person")
            # Task 2 owned by Alice (open), blocked by Task 1
            self._insert_task(connection, 2, status="open", due="2026-10-15", owner="Alice", owner_kind="person")
            self._insert_change_set(connection, 1, 2)
            self._insert_dependency(connection, depends_on_task_id=1, task_id=2, change_set_id=1)
            connection.commit()

            problems = find_problems(connection, today=today, reader_aliases=reader_aliases)
            self.assertEqual(problems.cycles, ())
            self.assertEqual(problems.infeasible, ())
            self.assertEqual(problems.foreign_blockers, ((1, 2),))

    def test_closed_tasks_ignored(self) -> None:
        today = date(2026, 10, 10)
        reader_aliases = ["Alice"]

        with closing(sqlite3.connect(self.database)) as connection:
            connection.row_factory = sqlite3.Row
            # Task 1 owned by Bob but DONE (closed)
            self._insert_task(connection, 1, status="done", due="2026-10-01", owner="Bob", owner_kind="person")
            # Task 2 owned by Alice, due was 2026-10-05 (< today)
            self._insert_task(connection, 2, status="open", due="2026-10-05", owner="Alice", owner_kind="person")
            self._insert_change_set(connection, 1, 2)
            self._insert_dependency(connection, depends_on_task_id=1, task_id=2, change_set_id=1)
            connection.commit()

            problems = find_problems(connection, today=today, reader_aliases=reader_aliases)
            # Since predecessor 1 is closed:
            # - Not a foreign blocker (predecessor is not open)
            # - Not an infeasible dependency problem (predecessor is not open)
            self.assertEqual(problems.cycles, ())
            self.assertEqual(problems.infeasible, ())
            self.assertEqual(problems.foreign_blockers, ())

    def test_inactive_dependencies_ignored(self) -> None:
        today = date(2026, 10, 10)
        reader_aliases = ["Alice"]

        with closing(sqlite3.connect(self.database)) as connection:
            connection.row_factory = sqlite3.Row
            self._insert_task(connection, 1, status="open", due="2026-10-01", owner="Bob", owner_kind="person")
            self._insert_task(connection, 2, status="open", due="2026-10-05", owner="Alice", owner_kind="person")
            self._insert_change_set(connection, 1, 2)
            self._insert_dependency(
                connection, depends_on_task_id=1, task_id=2, change_set_id=1, state="satisfied"
            )
            connection.commit()

            problems = find_problems(connection, today=today, reader_aliases=reader_aliases)
            self.assertEqual(problems.cycles, ())
            self.assertEqual(problems.infeasible, ())
            self.assertEqual(problems.foreign_blockers, ())


if __name__ == "__main__":
    unittest.main()
