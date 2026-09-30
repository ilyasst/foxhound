#!/usr/bin/env python3
"""Synthetic tests for task bulk drop CLI and operations."""

from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path

from foxhound import migrate_database
from foxhound.execution_cards import ExecutionCardService
from foxhound.task_bulk_drop import main, run_bulk_drop
from foxhound.task_cards import TaskCardService
from foxhound.task_execution import (
    ExecutionOutcome,
    ExecutionResultEnvelope,
    TaskExecutionService,
    WorkflowPhase,
    WorkflowStatus,
)
from foxhound.task_ledger import TaskLedger, TaskStatus

NOW = datetime(2030, 4, 1, 12, 0, tzinfo=timezone.utc)
NOW_ISO = NOW.isoformat(timespec="seconds")
CLAIM_TOKEN = "claim-token-synthetic-0000000000000000000000000000000000000000000000"
DELIVERY_TOKEN = "delivery-token-synthetic-000000000000000000000000000000000000000000"


class Clock:
    def __init__(self, start: datetime = NOW):
        self.current = start

    def __call__(self) -> datetime:
        return self.current


class TaskBulkDropTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.database = Path(self.temporary.name) / "foxhound.sqlite3"
        self.clock = Clock()
        migrate_database(self.database)

        self.execution = TaskExecutionService(
            self.database,
            clock=self.clock,
            token_factory=lambda: CLAIM_TOKEN,
        )
        self.cards = ExecutionCardService(
            self.database,
            clock=self.clock,
            token_factory=lambda: DELIVERY_TOKEN,
        )
        self.ledger = TaskLedger(self.database, clock=self.clock)
        self.task_cards = TaskCardService(self.database, clock=self.clock)

    def _task(self, task_id: int, *, status: str = "open", version: int = 1) -> None:
        closed_at = NOW_ISO if status != "open" else None
        with closing(sqlite3.connect(self.database)) as connection:
            connection.execute(
                "INSERT INTO tasks(id,status,text,owner,due,version,"
                "created_at,updated_at,closed_at) "
                "VALUES(?,?,?,?,NULL,?,?,?,?)",
                (
                    task_id,
                    status,
                    f"Synthetic task {task_id}",
                    f"Person {task_id}",
                    version,
                    NOW_ISO,
                    NOW_ISO,
                    closed_at,
                ),
            )
            connection.commit()

    def _plan_review(self, task_id: int, result_id: str) -> None:
        scheduled = self.execution.schedule(task_id, expected_task_version=1)
        self.assertTrue(scheduled.accepted)
        started = self.execution.start_action(
            task_id, expected_version=scheduled.version, action="start"
        )
        self.assertEqual(started.status, WorkflowStatus.QUEUED)
        claim = self.execution.claim_next()
        self.assertIsNotNone(claim)
        self.assertEqual((claim.task_id, claim.phase), (task_id, WorkflowPhase.PLAN))
        self.execution.record_result(
            ExecutionResultEnvelope(
                result_id=result_id,
                task_id=task_id,
                task_version=1,
                workflow_version=claim.workflow_version,
                phase=WorkflowPhase.PLAN,
                claim_token=claim.token,
                outcome=ExecutionOutcome.AWAITING_PLAN,
                summary="Synthetic summary",
                work_markdown="Synthetic plan.",
                questions=("Proceed?",),
            )
        )
        # Schedule the execution review card so it exists and is pending
        scheduled_cards = self.cards.schedule()
        self.assertGreater(scheduled_cards.created, 0)

    def test_bulk_drop_lifecycle_and_filters(self):
        # 1. create tasks:
        # task 1: open with workflow awaiting_review and pending execution card
        self._task(1, status="open")
        self._plan_review(1, "result-1")

        # verify card is pending
        with closing(sqlite3.connect(self.database)) as connection:
            row = connection.execute(
                "SELECT status FROM execution_review_cards WHERE task_id=1"
            ).fetchone()
            self.assertIsNotNone(row)
            self.assertEqual(row[0], "pending")

        # task 2: open plain (no workflow)
        self._task(2, status="open")

        # task 3: done
        self._task(3, status="done")

        # task 4: already dropped
        self._task(4, status="dropped")

        # Assert: dry run changes nothing
        dry_result = run_bulk_drop(
            database_path=self.database,
            reason="Synthetic test reason",
            apply=False,
            now=NOW_ISO,
        )
        self.assertTrue(dry_result["dry_run"])
        self.assertEqual(dry_result["reason"], "Synthetic test reason")
        self.assertEqual(dry_result["seen"], 2)  # tasks 1 and 2
        self.assertEqual(dry_result["dropped"], [1, 2])
        self.assertEqual(dry_result["skipped"], [])

        # Check DB unchanged after dry run
        self.assertEqual(self.ledger.get(1).status, TaskStatus.OPEN)
        self.assertEqual(self.ledger.get(2).status, TaskStatus.OPEN)
        self.assertEqual(self.execution.get(1).status, WorkflowStatus.AWAITING_REVIEW)
        with closing(sqlite3.connect(self.database)) as connection:
            row = connection.execute(
                "SELECT status FROM execution_review_cards WHERE task_id=1"
            ).fetchone()
            self.assertEqual(row[0], "pending")

        # Apply: drops only the open ones
        apply_result = run_bulk_drop(
            database_path=self.database,
            reason="Synthetic test reason",
            apply=True,
            now=NOW_ISO,
        )
        self.assertFalse(apply_result["dry_run"])
        self.assertEqual(apply_result["seen"], 2)
        self.assertEqual(apply_result["dropped"], [1, 2])
        self.assertEqual(apply_result["skipped"], [])

        # Assert task 1 and 2 are dropped
        self.assertEqual(self.ledger.get(1).status, TaskStatus.DROPPED)
        self.assertEqual(self.ledger.get(2).status, TaskStatus.DROPPED)

        # Assert workflow status became cancelled
        self.assertEqual(self.execution.get(1).status, WorkflowStatus.CANCELLED)

        # Assert card is no longer pending (cancelled/retracted)
        with closing(sqlite3.connect(self.database)) as connection:
            row = connection.execute(
                "SELECT status FROM execution_review_cards WHERE task_id=1"
            ).fetchone()
            self.assertEqual(row[0], "cancelled")

        # Done/dropped untouched
        self.assertEqual(self.ledger.get(3).status, TaskStatus.DONE)
        self.assertEqual(self.ledger.get(4).status, TaskStatus.DROPPED)

        # Second apply drops nothing
        second_result = run_bulk_drop(
            database_path=self.database,
            reason="Synthetic test reason",
            apply=True,
            now=NOW_ISO,
        )
        self.assertEqual(second_result["seen"], 0)
        self.assertEqual(second_result["dropped"], [])
        self.assertEqual(second_result["skipped"], [])

    def test_filters(self):
        self._task(10, status="open")
        self._task(11, status="open")
        self._task(12, status="open")

        # Test --task-id filter
        res = run_bulk_drop(
            database_path=self.database,
            reason="filter test",
            task_ids=[10, 12],
            apply=False,
            now=NOW_ISO,
        )
        self.assertEqual(res["dropped"], [10, 12])

        # Test --exclude-task-id filter
        res = run_bulk_drop(
            database_path=self.database,
            reason="filter test",
            exclude_task_ids=[11],
            apply=False,
            now=NOW_ISO,
        )
        self.assertEqual(res["dropped"], [10, 12])

        # Test --limit filter
        res = run_bulk_drop(
            database_path=self.database,
            reason="filter test",
            limit=1,
            apply=False,
            now=NOW_ISO,
        )
        self.assertEqual(len(res["dropped"]), 1)
        self.assertEqual(res["dropped"], [10])

    def test_cli_invocation(self):
        self._task(20, status="open")
        output = []
        code = main([
            "--database", str(self.database),
            "--reason", "CLI synthetic drop",
            "--task-id", "20",
            "--apply",
        ])
        self.assertEqual(code, 0)
        self.assertEqual(self.ledger.get(20).status, TaskStatus.DROPPED)


if __name__ == "__main__":
    unittest.main()
