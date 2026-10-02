"""Ownership proposals from published research hold a plan at its Start card."""

from __future__ import annotations

import sqlite3
import unittest
from contextlib import closing
from unittest import mock

import test_research_gate_planning as base
from foxhound.task_execution import WorkflowStatus
from foxhound.task_research_gate import (
    ownership_review_status,
    record_ownership_decision,
)

READER = base.READER


def _draft_with_owner(owner_text: str, refs: list[str]) -> dict:
    draft = base._draft()
    # A claim without sources cannot be "supported" (receipt validation).
    draft["stakeholders"] = [
        {"text": owner_text, "status": "supported" if refs else "unknown",
         "source_refs": refs},
    ]
    return draft


class OwnershipReviewTests(base.ResearchGatePlanningTests):
    """Reuses the research-gate fixtures; the parent's own tests run there."""

    def _researched(self, owner_text: str, *, owner: str = READER,
                    refs: list[str] | None = None, **owner_columns: object):
        self._task(1, owner, origin_kind="meeting", **owner_columns)
        service = self._service(research_before_planning=["meeting"])
        service.schedule_new(limit=10)
        self.assertIsNone(service.claim_next())  # requests research
        draft = _draft_with_owner(owner_text, ["src-001"] if refs is None else refs)
        with mock.patch.object(base, "_draft", return_value=draft):
            self._publish_research(task_id=1)
        return service

    def _status(self) -> tuple[str, str | None]:
        with closing(sqlite3.connect(self.database)) as connection:
            workflow = connection.execute(
                "SELECT status FROM task_execution_workflows WHERE task_id=1"
            ).fetchone()[0]
            return workflow, ownership_review_status(connection, 1, 1)

    def test_migration_adds_ownership_reviews(self):
        with closing(sqlite3.connect(self.database)) as connection:
            tables = {row[0] for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'")}
            self.assertIn("ownership_reviews", tables)
            self.assertEqual(
                connection.execute("PRAGMA user_version").fetchone()[0], 68)

    def test_reader_task_named_other_is_held_at_start(self):
        service = self._researched("Owner: other:Person B — assigned in the meeting")
        self.assertIsNone(service.claim_next())
        self.assertEqual(self._status(), (WorkflowStatus.AWAITING_START, "pending"))
        # The scheduler must not promote it back while the proposal is open.
        service.schedule_new(limit=10)
        self.assertEqual(self._status()[0], WorkflowStatus.AWAITING_START)
        with closing(sqlite3.connect(self.database)) as connection:
            row = connection.execute(
                "SELECT proposed_owner,proposed_kind,reasoning,source_title "
                "FROM ownership_reviews").fetchone()
        self.assertEqual(row, ("Person B", "other", "assigned in the meeting",
                               "Project Alpha plan"))

    def test_unresolved_owner_named_other_is_held(self):
        self._researched("Owner: other:Person B", owner="(unassigned)",
                         owner_kind="unresolved")
        service = self._service(research_before_planning=["meeting"])
        self.assertIsNone(service.claim_next())
        self.assertEqual(self._status()[1], "pending")

    def test_no_hold_for_agreement_undetermined_or_uncited(self):
        for text, refs in (
            ("Owner: reader", ["src-001"]),
            ("Owner: undetermined", ["src-001"]),
            ("Owner: other:Person B", []),
        ):
            with self.subTest(text=text, refs=refs):
                self.tearDown()
                self.setUp()
                service = self._researched(text, refs=refs)
                self.assertIsNotNone(service.claim_next())
                self.assertIsNone(self._status()[1])

    def test_pinned_owner_is_never_reopened(self):
        service = self._researched("Owner: other:Person B")
        with closing(sqlite3.connect(self.database)) as connection:
            connection.execute("UPDATE tasks SET owner_pinned=1 WHERE id=1")
            connection.commit()
        self.assertIsNotNone(service.claim_next())
        self.assertIsNone(self._status()[1])

    def test_decision_closes_only_a_pending_proposal(self):
        service = self._researched("Owner: other:Person B")
        self.assertIsNone(service.claim_next())
        with closing(sqlite3.connect(self.database)) as connection:
            self.assertTrue(record_ownership_decision(
                connection, 1, 1, "kept", None, "2030-01-01T00:00:00+00:00"))
            self.assertFalse(record_ownership_decision(
                connection, 1, 1, "kept", None, "2030-01-01T00:00:00+00:00"))
            connection.commit()
            self.assertEqual(ownership_review_status(connection, 1, 1), "kept")
        with self.assertRaises(ValueError):
            with closing(sqlite3.connect(self.database)) as connection:
                record_ownership_decision(
                    connection, 1, 1, "maybe", None, "2030-01-01T00:00:00+00:00")


    def test_start_keeps_and_pins_then_planning_proceeds(self):
        service = self._researched("Owner: other:Person B")
        self.assertIsNone(service.claim_next())
        with closing(sqlite3.connect(self.database)) as connection:
            version = connection.execute(
                "SELECT version FROM task_execution_workflows WHERE task_id=1"
            ).fetchone()[0]
        result = service.start_action(1, expected_version=version, action="start")
        self.assertEqual(result.status, WorkflowStatus.QUEUED)
        self.assertEqual(self._status()[1], "kept")
        with closing(sqlite3.connect(self.database)) as connection:
            self.assertEqual(connection.execute(
                "SELECT owner_pinned FROM tasks WHERE id=1").fetchone()[0], 1)
        self.assertIsNotNone(service.claim_next())

    def test_reassignment_confirms_or_reassigns(self):
        from foxhound.execution_cards import _close_ownership_review
        for new_owner, expected in (("person b", "confirmed"), ("Person C", "reassigned")):
            with self.subTest(new_owner=new_owner):
                self.tearDown()
                self.setUp()
                service = self._researched("Owner: other:Person B")
                self.assertIsNone(service.claim_next())
                with closing(sqlite3.connect(self.database)) as connection:
                    _close_ownership_review(
                        connection, 1, 1, new_owner, "2030-01-01T00:00:00+00:00")
                    connection.commit()
                    row = connection.execute(
                        "SELECT status,decided_owner FROM ownership_reviews"
                    ).fetchone()
                self.assertEqual(row, (expected, new_owner))


for _name in dir(base.ResearchGatePlanningTests):
    if _name.startswith("test_") and _name not in vars(OwnershipReviewTests):
        setattr(OwnershipReviewTests, _name, None)


if __name__ == "__main__":
    unittest.main()
