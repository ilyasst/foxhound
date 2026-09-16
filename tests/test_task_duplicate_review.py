#!/usr/bin/env python3
"""Synthetic reader-card tests for cross-source task consolidation."""

from __future__ import annotations

import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from foxhound import task_duplicate_proposals as proposals
from foxhound.candidate_inbox import CandidateInbox
from foxhound.task_cards import CardDisposition, CardStatus, TaskCardService, render_task_review_card


NOW = datetime(2030, 3, 1, 12, 0, tzinfo=timezone.utc)
TOKEN = "a" * 43
CONSUMER = "b" * 64


class DuplicateReviewCardTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.database = Path(self.directory.name) / "foxhound.sqlite3"
        CandidateInbox(self.database).initialize()
        self.connection = sqlite3.connect(self.database)
        self.connection.row_factory = sqlite3.Row
        self.addCleanup(self.connection.close)
        self._task(1, "email", "Prepare the synthetic rollout checklist")
        self._task(2, "meeting", "Draft the synthetic rollout checklist")
        self.proposal = proposals.propose(
            self.connection, task_id_a=1, task_id_b=2,
            basis="Same synthetic deliverable and confirmed owner.",
            detector="synthetic-detector", now=NOW.isoformat(),
        )
        self.connection.commit()
        self.cards = TaskCardService(
            self.database, clock=lambda: NOW, token_factory=lambda: TOKEN
        )

    def _task(self, task_id: int, kind: str, text: str) -> None:
        candidate_id = f"candidate-{task_id}"
        revision = f"{task_id:064x}"
        self.connection.execute(
            "INSERT INTO tasks(id,status,text,version,created_at,updated_at,"
            "owner_ref_version,owner_kind,owner_speaker_id,"
            "owner_canonical_speaker_id,owner_speaker_registry_id,"
            "owner_pinned,owner_provisional) VALUES(?, 'open', ?, 1, ?, ?,"
            "1, 'person', 'SPK_1', 'SPK_1', 'registry-A', 0, 0)",
            (task_id, text, NOW.isoformat(), NOW.isoformat()),
        )
        self.connection.execute(
            "INSERT INTO candidate_inbox(candidate_id,source_system,source_kind,"
            "source_record_id,source_item_id,source_revision,payload_json,"
            "created_at,first_imported_at,updated_at) VALUES(?, 'gw', ?,"
            "'record', ?, ?, '{}', ?, ?, ?)",
            (candidate_id, kind, str(task_id), revision, NOW.isoformat(),
             NOW.isoformat(), NOW.isoformat()),
        )
        self.connection.execute(
            "INSERT INTO task_candidate_bindings(candidate_id,source_revision,"
            "task_id,relation,decided_at) VALUES(?,?,?,'accepted',?)",
            (candidate_id, revision, task_id, NOW.isoformat()),
        )

    def _deliver(self):
        self.cards.schedule()
        claim = self.cards.claim_next(consumer_digest=CONSUMER)
        self.assertIsNotNone(claim)
        self.assertTrue(self.cards.complete_delivery(
            claim.card.id, expected_version=claim.card.version,
            claim_token=claim.token, transport="synthetic",
            delivery_ref="message-1",
        ).accepted)
        return claim

    def test_proposal_becomes_one_side_by_side_reader_card(self) -> None:
        scheduled = self.cards.schedule()
        self.assertEqual((scheduled.created, scheduled.asked), (1, 1))
        claim = self.cards.claim_next(consumer_digest=CONSUMER)
        self.assertIsNotNone(claim)
        self.assertIsNotNone(claim.card.duplicate)
        text, keyboard = render_task_review_card(claim.card)
        self.assertIn("Same task?", text)
        self.assertIn("Task T1", text)
        self.assertIn("Task T2", text)
        self.assertEqual(len(keyboard["inline_keyboard"][0]), 2)

    def test_confirm_records_relation_and_hides_the_noncanonical_task(self) -> None:
        claim = self._deliver()
        result = self.cards.act(
            claim.card.id, expected_version=claim.card.version,
            action="duplicate_confirm",
        )
        self.assertIs(result.disposition, CardDisposition.APPLIED)
        self.assertIs(result.status, CardStatus.CANCELLED)
        self.assertEqual(
            self.connection.execute(
                "SELECT state FROM task_duplicate_proposals WHERE id=?",
                (self.proposal.proposal_id,),
            ).fetchone()[0],
            "confirmed",
        )
        relation = self.connection.execute(
            "SELECT kind,asserted_by FROM task_relations"
        ).fetchone()
        self.assertEqual(tuple(relation), ("duplicate_of", "reader"))
        self.cards.schedule()
        self.assertEqual([card.task_id for card in self.cards.due()], [1])

    def test_rejection_is_durable_and_leaves_both_tasks_open(self) -> None:
        claim = self._deliver()
        result = self.cards.act(
            claim.card.id, expected_version=claim.card.version,
            action="duplicate_reject",
        )
        self.assertTrue(result.accepted)
        self.assertEqual(
            self.connection.execute(
                "SELECT state FROM task_duplicate_proposals WHERE id=?",
                (self.proposal.proposal_id,),
            ).fetchone()[0],
            "rejected",
        )
        self.assertEqual(
            self.connection.execute("SELECT count(*) FROM task_relations").fetchone()[0],
            0,
        )
        self.assertEqual(
            [tuple(row) for row in self.connection.execute(
                "SELECT status FROM tasks ORDER BY id")],
            [("open",), ("open",)],
        )

    def test_recently_closed_comparison_uses_the_open_task_card(self) -> None:
        self.connection.execute(
            "UPDATE tasks SET status='done',closed_at=? WHERE id=1",
            ((NOW - timedelta(days=1)).isoformat(),),
        )
        self.connection.commit()

        self.assertEqual(self.cards.schedule().asked, 1)
        claim = self.cards.claim_next(consumer_digest=CONSUMER)
        self.assertIsNotNone(claim)
        self.assertEqual(claim.card.task_id, 2)
        self.assertEqual(claim.card.duplicate.other_task_id, 1)
        text, keyboard = render_task_review_card(claim.card)
        self.assertIn("recently closed task", text)
        self.assertEqual(keyboard["inline_keyboard"][0][0]["text"], "✅ Already completed")

        self.assertTrue(self.cards.complete_delivery(
            claim.card.id, expected_version=claim.card.version,
            claim_token=claim.token, transport="synthetic",
            delivery_ref="message-1",
        ).accepted)
        result = self.cards.act(
            claim.card.id, expected_version=claim.card.version,
            action="duplicate_confirm",
        )
        self.assertTrue(result.accepted)
        relation = self.connection.execute(
            "SELECT subject_id,object_id FROM task_relations"
        ).fetchone()
        self.assertEqual(tuple(relation), (2, 1))

    def test_stale_right_task_refuses_confirmation(self) -> None:
        claim = self._deliver()
        self.connection.execute("UPDATE tasks SET version=2 WHERE id=2")
        self.connection.commit()
        result = self.cards.act(
            claim.card.id, expected_version=claim.card.version,
            action="duplicate_confirm",
        )
        self.assertIs(result.disposition, CardDisposition.REFUSED)
        self.assertEqual(
            self.connection.execute("SELECT count(*) FROM task_relations").fetchone()[0], 0
        )

    def test_active_execution_refuses_confirmation(self) -> None:
        claim = self._deliver()
        self.connection.execute(
            "INSERT INTO task_execution_workflows("
            "task_id,task_version,status,phase,version,failure_count,"
            "created_at,updated_at) VALUES(1,1,'awaiting_review','plan',1,0,?,?)",
            (NOW.isoformat(), NOW.isoformat()),
        )
        self.connection.commit()
        result = self.cards.act(
            claim.card.id, expected_version=claim.card.version,
            action="duplicate_confirm",
        )
        self.assertIs(result.disposition, CardDisposition.REFUSED)
        self.assertEqual(
            self.connection.execute("SELECT count(*) FROM task_relations").fetchone()[0], 0
        )

    def test_reader_can_reverse_a_confirmation_without_losing_history(self) -> None:
        claim = self._deliver()
        self.assertTrue(self.cards.act(
            claim.card.id, expected_version=claim.card.version,
            action="duplicate_confirm",
        ).accepted)
        relation_id = self.connection.execute(
            "SELECT id FROM task_relations"
        ).fetchone()[0]
        self.assertTrue(self.cards.reverse_duplicate(relation_id))
        self.assertEqual(
            self.connection.execute(
                "SELECT withdrawn_at FROM task_relations WHERE id=?", (relation_id,)
            ).fetchone()[0] is not None,
            True,
        )
        self.assertEqual(
            self.connection.execute(
                "SELECT state FROM task_duplicate_proposals WHERE id=?",
                (self.proposal.proposal_id,),
            ).fetchone()[0],
            "proposed",
        )


if __name__ == "__main__":
    unittest.main()
