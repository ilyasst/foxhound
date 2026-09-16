#!/usr/bin/env python3
"""Synthetic tests for the reader-gated duplicate-proposal ledger."""

from __future__ import annotations

import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path

from foxhound import task_duplicate_proposals as duplicates
from foxhound.candidate_inbox import CandidateInbox, SCHEMA_VERSION


NOW = "2030-03-01T12:00:00+00:00"
LATER = "2030-03-02T12:00:00+00:00"


class DuplicateProposalTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.database = Path(self.directory.name) / "foxhound.sqlite3"
        CandidateInbox(self.database).initialize()
        self.connection = sqlite3.connect(self.database)
        self.connection.row_factory = sqlite3.Row
        self.addCleanup(self.connection.close)
        for task_id in range(1, 9):
            self._task(task_id)

    def _task(self, task_id: int, *, status: str = "open",
              owner_suffix: str = "A", provisional: int = 0) -> None:
        self.connection.execute(
            "INSERT INTO tasks("
            "id,status,text,version,created_at,updated_at,owner_ref_version,"
            "owner_kind,owner_speaker_id,owner_canonical_speaker_id,"
            "owner_speaker_registry_id,owner_pinned,owner_provisional) "
            "VALUES(?,?,?,1,?, ?,1,'person',?,?,?,0,?)",
            (task_id, status, f"Synthetic task {task_id}", NOW, NOW,
             f"SPK_{task_id}{owner_suffix}", f"SPK_{task_id}{owner_suffix}",
             f"registry-{owner_suffix}", provisional),
        )

    def _same_owner(self, *task_ids: int) -> None:
        for task_id in task_ids:
            self.connection.execute(
                "UPDATE tasks SET owner_speaker_id='SPK_1',"
                "owner_canonical_speaker_id='SPK_1',"
                "owner_speaker_registry_id='registry-A' WHERE id=?",
                (task_id,),
            )

    def _propose(self, **overrides):
        values = {
            "task_id_a": 1,
            "task_id_b": 2,
            "basis": "Same synthetic deliverable and confirmed owner.",
            "detector": "synthetic-detector",
            "now": NOW,
        }
        values.update(overrides)
        return duplicates.propose(self.connection, **values)

    def test_records_an_ordered_private_pair_and_machine_event(self) -> None:
        self._same_owner(1, 2)
        result = self._propose(task_id_a=2, task_id_b=1)
        self.assertIs(result.disposition, duplicates.ProposalDisposition.RECORDED)
        proposal = duplicates.get(self.connection, result.proposal_id)
        self.assertEqual((proposal.left_task_id, proposal.right_task_id), (1, 2))
        self.assertEqual((proposal.left_task_version, proposal.right_task_version),
                         (1, 1))
        self.assertTrue(proposal.open)
        self.assertEqual(self._events(result.proposal_id), ["proposed"])

    def test_an_existing_pair_is_never_rephrased_into_a_second_question(self) -> None:
        self._same_owner(1, 2)
        first = self._propose()
        again = self._propose(basis="A differently worded synthetic basis.")
        self.assertIs(again.disposition, duplicates.ProposalDisposition.UNCHANGED)
        self.assertEqual(again.proposal_id, first.proposal_id)
        self.assertEqual(self._rows(), 1)

    def test_refuses_self_unknown_closed_and_incompatible_pairs(self) -> None:
        self.assertIs(
            self._propose(task_id_b=1).refusal,
            duplicates.ProposalRefusal.SAME_TASK,
        )
        self.assertIs(
            self._propose(task_id_b=99).refusal,
            duplicates.ProposalRefusal.UNKNOWN_TASK,
        )
        self._same_owner(1, 2)
        self.connection.execute("UPDATE tasks SET status='done' WHERE id=2")
        self.assertIs(
            self._propose().refusal,
            duplicates.ProposalRefusal.TASK_NOT_OPEN,
        )
        self.connection.execute("UPDATE tasks SET status='open' WHERE id=2")
        self.connection.execute(
            "UPDATE tasks SET owner_speaker_registry_id='registry-B' WHERE id=2")
        self.assertIs(
            self._propose().refusal,
            duplicates.ProposalRefusal.INCOMPATIBLE_OWNER,
        )

    def test_records_a_pair_with_a_recently_closed_task(self) -> None:
        self._same_owner(1, 2)
        self.connection.execute(
            "UPDATE tasks SET status='done',closed_at=? WHERE id=1",
            ("2030-02-15T12:00:00+00:00",),
        )
        result = self._propose()
        self.assertIs(result.disposition, duplicates.ProposalDisposition.RECORDED)

    def test_refuses_a_pair_with_a_task_closed_outside_the_lookback(self) -> None:
        self._same_owner(1, 2)
        self.connection.execute(
            "UPDATE tasks SET status='done',closed_at=? WHERE id=1",
            ("2030-01-01T12:00:00+00:00",),
        )
        self.assertIs(
            self._propose().refusal,
            duplicates.ProposalRefusal.TASK_NOT_OPEN,
        )

    def test_refuses_unconfirmed_and_provisional_owners(self) -> None:
        self._same_owner(1, 2)
        self.connection.execute(
            "UPDATE tasks SET owner_provisional=1 WHERE id=2")
        self.assertIs(
            self._propose().refusal,
            duplicates.ProposalRefusal.INCOMPATIBLE_OWNER,
        )
        self.connection.execute(
            "UPDATE tasks SET owner_provisional=0,owner_kind='external'"
        )
        self.assertIs(
            self._propose().refusal,
            duplicates.ProposalRefusal.INCOMPATIBLE_OWNER,
        )

    def test_runaway_detector_is_bounded_per_task(self) -> None:
        self._same_owner(*range(1, 8))
        for other in range(2, 2 + duplicates.MAX_OPEN_PROPOSALS_PER_TASK):
            self.assertIs(
                self._propose(task_id_b=other).disposition,
                duplicates.ProposalDisposition.RECORDED,
            )
        refused = self._propose(
            task_id_b=2 + duplicates.MAX_OPEN_PROPOSALS_PER_TASK)
        self.assertIs(refused.refusal,
                      duplicates.ProposalRefusal.TOO_MANY_OPEN_PROPOSALS)

    def test_reader_decisions_are_distinct_from_machine_recommendations(self) -> None:
        self._same_owner(1, 2)
        recorded = self._propose()
        self.assertTrue(duplicates.settle(
            self.connection, proposal_id=recorded.proposal_id,
            decision=duplicates.Decision.REJECTED, actor="reader", now=LATER,
        ))
        self.assertFalse(duplicates.settle(
            self.connection, proposal_id=recorded.proposal_id,
            decision=duplicates.Decision.CONFIRMED, actor="reader", now=LATER,
        ))
        self.assertEqual(duplicates.get(self.connection, recorded.proposal_id).state,
                         "rejected")
        self.assertEqual(self._events(recorded.proposal_id),
                         ["proposed", "rejected"])
        self.assertIs(
            self._propose().disposition,
            duplicates.ProposalDisposition.UNCHANGED,
        )

    def test_only_an_explicit_reader_reopening_reconsiders_rejection(self) -> None:
        self._same_owner(1, 2)
        recorded = self._propose()
        duplicates.settle(
            self.connection, proposal_id=recorded.proposal_id,
            decision=duplicates.Decision.REJECTED, actor="reader", now=LATER,
        )
        self.assertTrue(duplicates.reopen(
            self.connection, proposal_id=recorded.proposal_id,
            actor="reader", now="2030-03-03T12:00:00+00:00",
        ))
        self.assertTrue(duplicates.get(self.connection, recorded.proposal_id).open)
        self.assertEqual(self._events(recorded.proposal_id),
                         ["proposed", "rejected", "reopened"])

    def test_counts_are_content_free_and_event_based(self) -> None:
        self._same_owner(1, 2, 3, 4)
        rejected = self._propose()
        confirmed = self._propose(task_id_a=3, task_id_b=4)
        duplicates.settle(
            self.connection, proposal_id=rejected.proposal_id,
            decision=duplicates.Decision.REJECTED, actor="reader", now=LATER,
        )
        duplicates.reopen(
            self.connection, proposal_id=rejected.proposal_id,
            actor="reader", now="2030-03-03T12:00:00+00:00",
        )
        duplicates.settle(
            self.connection, proposal_id=confirmed.proposal_id,
            decision=duplicates.Decision.CONFIRMED, actor="reader", now=LATER,
        )
        count = duplicates.counts(self.connection)[0]
        self.assertEqual(
            (count.detector, count.proposed, count.confirmed, count.rejected,
             count.reopened),
            ("synthetic-detector", 2, 1, 1, 1),
        )

    def test_rows_and_events_are_immutable_except_settlement(self) -> None:
        self._same_owner(1, 2)
        result = self._propose()
        with self.assertRaises(sqlite3.IntegrityError):
            self.connection.execute("DELETE FROM task_duplicate_proposals")
        with self.assertRaises(sqlite3.IntegrityError):
            self.connection.execute(
                "UPDATE task_duplicate_proposals SET basis='rewritten'")
        with self.assertRaises(sqlite3.IntegrityError):
            self.connection.execute("DELETE FROM task_duplicate_proposal_events")
        self.assertTrue(duplicates.settle(
            self.connection, proposal_id=result.proposal_id,
            decision=duplicates.Decision.CONFIRMED, actor="reader", now=LATER,
        ))

    def test_version_twenty_six_migrates_to_the_proposal_ledger(self) -> None:
        self.connection.close()
        with closing(sqlite3.connect(self.database)) as connection:
            for name in (
                "task_duplicate_proposals_settle_only",
                "task_duplicate_proposals_no_delete",
                "task_duplicate_proposal_events_no_delete",
                "task_duplicate_proposal_events_no_update",
            ):
                connection.execute(f"DROP TRIGGER {name}")
            connection.execute("DROP INDEX task_duplicate_proposals_open")
            connection.execute("DROP INDEX task_duplicate_proposals_pair")
            connection.execute("DROP TABLE task_duplicate_proposal_events")
            connection.execute("DROP TABLE task_duplicate_proposals")
            connection.execute("PRAGMA user_version = 26")
            connection.commit()
        CandidateInbox(self.database).initialize()
        with closing(sqlite3.connect(self.database)) as connection:
            version = connection.execute("PRAGMA user_version").fetchone()[0]
            rows = connection.execute(
                "SELECT count(*) FROM task_duplicate_proposals").fetchone()[0]
        self.assertEqual((version, rows), (SCHEMA_VERSION, 0))

    def _events(self, proposal_id: int) -> list[str]:
        return [row[0] for row in self.connection.execute(
            "SELECT kind FROM task_duplicate_proposal_events "
            "WHERE proposal_id=? ORDER BY sequence", (proposal_id,)
        )]

    def _rows(self) -> int:
        return int(self.connection.execute(
            "SELECT count(*) FROM task_duplicate_proposals").fetchone()[0])


if __name__ == "__main__":
    unittest.main()
