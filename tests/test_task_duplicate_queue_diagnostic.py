"""Synthetic tests for the duplicate queue diagnostic.

Each test plants a database with one proposal of a specific disqualified
kind and asserts the diagnostic reports the correct reason.  Covers
permanent and transient conditions, plus the happy path of an empty
or fully carded queue.
"""

from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from foxhound import migrate_database
from foxhound import task_duplicate_proposals as proposals
from foxhound.task_duplicate_queue_diagnostic import (
    DisqualificationReason,
    diagnose_duplicate_queue,
)


NOW = datetime(2030, 3, 1, 12, 0, tzinfo=timezone.utc).isoformat()


class _DbFixture:
    """Minimal database with schema and helper to insert tasks."""

    def __init__(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.database = Path(self.directory.name) / "foxhound.sqlite3"
        migrate_database(self.database)
        self.connection = sqlite3.connect(self.database)
        self.connection.row_factory = sqlite3.Row

    def close(self) -> None:
        self.connection.close()
        self.directory.cleanup()

    def insert_task(
        self, task_id: int, text: str, status: str = "open",
        version: int = 1, closed_at: str | None = None,
    ) -> None:
        self.connection.execute(
            "INSERT INTO tasks(id,status,text,version,created_at,updated_at,"
            "owner_ref_version,owner_kind,owner_speaker_id,"
            "owner_canonical_speaker_id,owner_speaker_registry_id,"
            "owner_pinned,owner_provisional,closed_at) "
            "VALUES(?, ?, ?, ?, ?, ?, 1, 'person', 'SPK_1', "
            "'SPK_1', 'registry-A', 0, 0, ?)",
            (task_id, status, text, version, NOW, NOW, closed_at),
        )

    def insert_workflow(self, task_id: int, status: str) -> None:
        # The CHECK constraint requires claim fields for 'running',
        # and due_at for 'snoozed'. Provide them.
        claim_digest = "a" * 64 if status == "running" else None
        claimed_at = NOW if status == "running" else None
        heartbeat_at = NOW if status == "running" else None
        expires_at = NOW if status == "running" else None
        due_at = NOW if status == "snoozed" else None
        self.connection.execute(
            "INSERT INTO task_execution_workflows("
            "task_id,task_version,status,phase,version,"
            "due_at,claim_token_digest,claimed_at,"
            "claim_heartbeat_at,claim_expires_at,created_at,updated_at) "
            "VALUES(?, ?, ?, 'execute', 1, ?, ?, ?, ?, ?, ?, ?)",
            (task_id, 1, status, due_at,
             claim_digest, claimed_at, heartbeat_at, expires_at,
             NOW, NOW),
        )

    def insert_card(
        self, task_id: int, status: str = "pending",
        version: int = 1,
    ) -> int:
        cursor = self.connection.execute(
            "INSERT INTO task_review_cards("
            "task_id,task_version,status,version,due_at,updated_at,created_at) "
            "VALUES(?, ?, ?, ?, ?, ?, ?)",
            (task_id, version, status, version, NOW, NOW, NOW),
        )
        assert cursor.lastrowid is not None
        return int(cursor.lastrowid)

    def propose(self, a: int, b: int, detector: str = "synthetic") -> int:
        result = proposals.propose(
            self.connection, task_id_a=a, task_id_b=b,
            basis="Synthetic shared deliverable.",
            detector=detector, now=NOW,
        )
        assert result.proposal_id is not None
        return int(result.proposal_id)

    def commit(self) -> None:
        self.connection.commit()


class DuplicateQueueDiagnosticTests(unittest.TestCase):

    # -----------------------------------------------------------------------
    # Empty queue
    # -----------------------------------------------------------------------

    def test_empty_queue_reports_zero_counts(self) -> None:
        db = _DbFixture()
        try:
            db.commit()
            result = diagnose_duplicate_queue(database_path=db.database)
        finally:
            db.close()

        self.assertEqual(result.total_proposed, 0)
        self.assertEqual(result.carded, 0)
        self.assertEqual(result.uncarded, 0)
        self.assertEqual(result.permanently_blocked, 0)
        self.assertEqual(result.transiently_blocked, 0)
        self.assertEqual(len(result.proposals), 0)

    # -----------------------------------------------------------------------
    # Happy path: proposal is carded — no reasons
    # -----------------------------------------------------------------------

    def test_carded_proposal_has_no_reasons(self) -> None:
        db = _DbFixture()
        try:
            db.insert_task(1, "Task one")
            db.insert_task(2, "Task two")
            pid = db.propose(1, 2)
            # Simulate card binding
            cid = db.insert_card(1, "pending")
            db.connection.execute(
                "UPDATE task_duplicate_proposals SET card_id=? "
                "WHERE id=?", (cid, pid),
            )
            db.commit()
            result = diagnose_duplicate_queue(database_path=db.database)
        finally:
            db.close()

        self.assertEqual(result.total_proposed, 1)
        self.assertEqual(result.carded, 1)
        self.assertEqual(result.uncarded, 0)
        proposal = result.proposals[0]
        self.assertEqual(proposal.card_id, cid)
        self.assertEqual(len(proposal.reasons), 0)

    # -----------------------------------------------------------------------
    # Permanent: version mismatch
    # -----------------------------------------------------------------------

    def test_stale_left_version_is_permanent(self) -> None:
        db = _DbFixture()
        try:
            # Task 1 at version 1 when proposed
            db.insert_task(1, "Task one", version=1)
            db.insert_task(2, "Task two")
            db.propose(1, 2)
            # Advance task 1 to version 2
            db.connection.execute(
                "UPDATE tasks SET version=2, updated_at=? WHERE id=1", (NOW,)
            )
            db.commit()
            result = diagnose_duplicate_queue(database_path=db.database)
        finally:
            db.close()

        proposal = result.proposals[0]
        codes = [r.code for r in proposal.reasons]
        self.assertIn("left_version_stale", codes)
        # Verify it is permanent
        reason = next(r for r in proposal.reasons if r.code == "left_version_stale")
        self.assertTrue(reason.permanent)
        self.assertEqual(result.permanently_blocked, 1)

    def test_stale_right_version_is_permanent(self) -> None:
        db = _DbFixture()
        try:
            db.insert_task(1, "Task one")
            db.insert_task(2, "Task two", version=1)
            db.propose(1, 2)
            db.connection.execute(
                "UPDATE tasks SET version=2, updated_at=? WHERE id=2", (NOW,)
            )
            db.commit()
            result = diagnose_duplicate_queue(database_path=db.database)
        finally:
            db.close()

        codes = [r.code for r in result.proposals[0].reasons]
        self.assertIn("right_version_stale", codes)
        self.assertEqual(result.permanently_blocked, 1)

    def test_duplicate_relation_is_permanent(self) -> None:
        db = _DbFixture()
        try:
            db.insert_task(1, "Task one")
            db.insert_task(2, "Task two")
            db.propose(1, 2)
            db.connection.execute(
                "INSERT INTO task_relations(subject_id,object_id,kind,"
                "basis,asserted_by,actor,created_at) "
                "VALUES(2,1,'duplicate_of','test','reader','reader',?)",
                (NOW,),
            )
            db.commit()
            result = diagnose_duplicate_queue(database_path=db.database)
        finally:
            db.close()

        proposal = result.proposals[0]
        codes = [r.code for r in proposal.reasons]
        self.assertIn("duplicate_relation", codes)
        reason = next(r for r in proposal.reasons if r.code == "duplicate_relation")
        self.assertTrue(reason.permanent)
        self.assertEqual(result.permanently_blocked, 1)

    # -----------------------------------------------------------------------
    # Permanent: both tasks closed
    # -----------------------------------------------------------------------

    def test_both_tasks_closed_is_permanent(self) -> None:
        db = _DbFixture()
        try:
            db.insert_task(1, "Task one")
            db.insert_task(2, "Task two")
            db.propose(1, 2)
            CLOSED = datetime(2030, 3, 1, 13, 0, tzinfo=timezone.utc).isoformat()
            db.connection.execute(
                "UPDATE tasks SET status='done', closed_at=?, updated_at=? "
                "WHERE id=1", (CLOSED, CLOSED),
            )
            db.connection.execute(
                "UPDATE tasks SET status='done', closed_at=?, updated_at=? "
                "WHERE id=2", (CLOSED, CLOSED),
            )
            db.commit()
            result = diagnose_duplicate_queue(database_path=db.database)
        finally:
            db.close()

        codes = [r.code for r in result.proposals[0].reasons]
        self.assertIn("both_tasks_closed", codes)
        reason = next(r for r in result.proposals[0].reasons
                      if r.code == "both_tasks_closed")
        self.assertTrue(reason.permanent)
        self.assertEqual(result.permanently_blocked, 1)

    # -----------------------------------------------------------------------
    # Permanent: settled state
    # -----------------------------------------------------------------------

    def test_settled_proposal_is_reported(self) -> None:
        db = _DbFixture()
        try:
            db.insert_task(1, "Task one")
            db.insert_task(2, "Task two")
            db.propose(1, 2)
            proposals.settle(
                db.connection, proposal_id=1,
                decision=proposals.Decision.CONFIRMED,
                actor="reader", now=NOW,
            )
            db.commit()
            result = diagnose_duplicate_queue(database_path=db.database)
        finally:
            db.close()

        proposal = result.proposals[0]
        self.assertEqual(proposal.state, "confirmed")
        codes = [r.code for r in proposal.reasons]
        self.assertIn("settled", codes)
        # Settled proposals are not counted as "proposed"
        self.assertEqual(result.total_proposed, 0)

    # -----------------------------------------------------------------------
    # Transient: workflow hold
    # -----------------------------------------------------------------------

    def test_workflow_hold_is_transient(self) -> None:
        db = _DbFixture()
        try:
            db.insert_task(1, "Task one")
            db.insert_task(2, "Task two")
            db.propose(1, 2)
            db.insert_workflow(1, "running")
            db.commit()
            result = diagnose_duplicate_queue(database_path=db.database)
        finally:
            db.close()

        codes = [r.code for r in result.proposals[0].reasons]
        self.assertIn("left_workflow_hold", codes)
        reason = next(r for r in result.proposals[0].reasons
                      if r.code == "left_workflow_hold")
        self.assertFalse(reason.permanent)
        self.assertEqual(result.transiently_blocked, 1)

    def test_right_workflow_hold_is_transient(self) -> None:
        db = _DbFixture()
        try:
            db.insert_task(1, "Task one")
            db.insert_task(2, "Task two")
            db.propose(1, 2)
            db.insert_workflow(2, "awaiting_review")
            db.commit()
            result = diagnose_duplicate_queue(database_path=db.database)
        finally:
            db.close()

        codes = [r.code for r in result.proposals[0].reasons]
        self.assertIn("right_workflow_hold", codes)
        self.assertFalse(result.proposals[0].reasons[0].permanent)

    def test_dormant_workflow_does_not_block(self) -> None:
        db = _DbFixture()
        try:
            db.insert_task(1, "Task one")
            db.insert_task(2, "Task two")
            db.propose(1, 2)
            # snoozed workflow does NOT block — mirrors _duplicate_execution_holds
            db.insert_workflow(1, "snoozed")
            db.commit()
            result = diagnose_duplicate_queue(database_path=db.database)
        finally:
            db.close()

        codes = [r.code for r in result.proposals[0].reasons]
        self.assertNotIn("left_workflow_hold", codes)

    # -----------------------------------------------------------------------
    # Transient: card conflict
    # -----------------------------------------------------------------------

    def test_active_card_conflict_is_transient(self) -> None:
        db = _DbFixture()
        try:
            db.insert_task(1, "Task one")
            db.insert_task(2, "Task two")
            db.propose(1, 2)
            # Task 1 already has a delivered card (not pending)
            db.insert_card(1, "delivered")
            db.commit()
            result = diagnose_duplicate_queue(database_path=db.database)
        finally:
            db.close()

        codes = [r.code for r in result.proposals[0].reasons]
        self.assertIn("card_conflict", codes)
        reason = next(r for r in result.proposals[0].reasons
                      if r.code == "card_conflict")
        self.assertFalse(reason.permanent)

    def test_pending_card_with_different_proposal_blocks(self) -> None:
        db = _DbFixture()
        try:
            db.insert_task(1, "Task one")
            db.insert_task(2, "Task two")
            db.insert_task(3, "Task three")
            # Propose 1-2
            pid_12 = db.propose(1, 2, detector="synthetic-a")
            # Propose 1-3 (same left task)
            pid_13 = db.propose(1, 3, detector="synthetic-b")
            # Task 1 has a pending card bound to proposal 1-3
            cid = db.insert_card(1, "pending")
            db.connection.execute(
                "UPDATE task_duplicate_proposals SET card_id=? "
                "WHERE id=?", (cid, pid_13),
            )
            db.commit()
            result = diagnose_duplicate_queue(database_path=db.database)
        finally:
            db.close()

        # Proposal 1-2 should be blocked because its target (task 1)
        # already has a pending card carrying proposal 1-3
        diag_12 = next(
            p for p in result.proposals if p.proposal_id == pid_12
        )
        codes = [r.code for r in diag_12.reasons]
        self.assertIn("card_conflict", codes)

    # -----------------------------------------------------------------------
    # Multiple reasons at once
    # -----------------------------------------------------------------------

    def test_multiple_reasons_reported_together(self) -> None:
        db = _DbFixture()
        try:
            db.insert_task(1, "Task one", version=1)
            db.insert_task(2, "Task two")
            db.propose(1, 2)
            # Advance version AND add workflow hold
            db.connection.execute(
                "UPDATE tasks SET version=2, updated_at=? WHERE id=1", (NOW,)
            )
            db.insert_workflow(2, "running")
            db.commit()
            result = diagnose_duplicate_queue(database_path=db.database)
        finally:
            db.close()

        codes = [r.code for r in result.proposals[0].reasons]
        self.assertIn("left_version_stale", codes)
        self.assertIn("right_workflow_hold", codes)
        self.assertEqual(len(result.proposals[0].reasons), 2)
        # Has permanent reason, so counted as permanently blocked
        self.assertEqual(result.permanently_blocked, 1)

    # -----------------------------------------------------------------------
    # Content-free output
    # -----------------------------------------------------------------------

    def test_output_is_content_free(self) -> None:
        """Output must never contain task text or proposal basis."""
        db = _DbFixture()
        try:
            db.insert_task(1, "Confidential project delivery report Q4")
            db.insert_task(2, "Secret internal review of vendor X")
            db.propose(1, 2)
            db.commit()
            result = diagnose_duplicate_queue(database_path=db.database)
        finally:
            db.close()

        # Convert to JSON and check it contains no task text fragments
        from foxhound.task_duplicate_queue_diagnostic import _diagnostic_to_json
        output_text = json.dumps(_diagnostic_to_json(result))
        self.assertNotIn("Confidential", output_text)
        self.assertNotIn("Secret", output_text)
        self.assertNotIn("Synthetic", output_text)
        self.assertNotIn("vendor", output_text)

    # -----------------------------------------------------------------------
    # Uncarded but eligible (no blocking reasons)
    # -----------------------------------------------------------------------

    def test_eligible_uncarded_proposal(self) -> None:
        """A proposed, uncarded proposal with no blocking reasons."""
        db = _DbFixture()
        try:
            db.insert_task(1, "Task one")
            db.insert_task(2, "Task two")
            db.propose(1, 2)
            db.commit()
            result = diagnose_duplicate_queue(database_path=db.database)
        finally:
            db.close()

        self.assertEqual(result.total_proposed, 1)
        self.assertEqual(result.uncarded, 1)
        # Not blocked at all
        self.assertEqual(len(result.proposals[0].reasons), 0)
        self.assertEqual(result.permanently_blocked, 0)
        self.assertEqual(result.transiently_blocked, 0)


if __name__ == "__main__":
    unittest.main()
