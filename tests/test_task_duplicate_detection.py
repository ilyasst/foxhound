#!/usr/bin/env python3
"""Synthetic tests for cross-source duplicate-task recall."""

from __future__ import annotations

from foxhound import migrate_database

import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path

from foxhound import task_duplicate_detection as detection
from foxhound import task_duplicate_proposals as proposals
from foxhound.candidate_inbox import CandidateInbox


NOW = "2030-03-01T12:00:00+00:00"


class CrossSourceDetectionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.database = Path(self.directory.name) / "foxhound.sqlite3"
        migrate_database(self.database)
        self.connection = sqlite3.connect(self.database)
        self.connection.row_factory = sqlite3.Row
        self.addCleanup(self.connection.close)

    def _task(self, task_id: int, *, kind: str, text: str,
              owner: str = "A", status: str = "open",
              closed_at: str | None = None) -> None:
        candidate_id = f"candidate-{task_id}"
        revision = f"{task_id:064x}"
        self.connection.execute(
            "INSERT INTO tasks("
            "id,status,text,version,created_at,updated_at,closed_at,owner_ref_version,"
            "owner_kind,owner_speaker_id,owner_canonical_speaker_id,"
            "owner_speaker_registry_id,owner_pinned,owner_provisional) "
            "VALUES(?,?,?,1,?,?,?,1,'person','SPK_1','SPK_1',?,0,0)",
            (task_id, status, text, NOW, NOW, closed_at, f"registry-{owner}"),
        )
        self.connection.execute(
            "INSERT INTO candidate_inbox("
            "candidate_id,source_system,source_kind,source_record_id,"
            "source_item_id,source_revision,payload_json,created_at,"
            "first_imported_at,updated_at) VALUES(?,'gw',?,'record',?,?,'{}',"
            "?,?,?)",
            (candidate_id, kind, str(task_id), revision, NOW, NOW, NOW),
        )
        self.connection.execute(
            "INSERT INTO task_candidate_bindings("
            "candidate_id,source_revision,task_id,relation,decided_at) "
            "VALUES(?,?,?,'accepted',?)",
            (candidate_id, revision, task_id, NOW),
        )

    def test_distinct_sources_with_a_shared_deliverable_become_a_proposal(self):
        self._task(1, kind="email", text="Prepare the synthetic rollout checklist")
        self._task(2, kind="meeting", text="Draft the rollout checklist")
        result = detection.scan(self.connection, now=NOW)
        self.assertEqual(
            (result.pairs_considered, result.pairs_signalled,
             result.proposals_recorded, result.proposals_unchanged,
             result.proposals_refused),
            (1, 1, 1, 0, 0),
        )
        proposal = proposals.next_open(self.connection)
        self.assertIsNotNone(proposal)
        self.assertEqual((proposal.left_task_id, proposal.right_task_id), (1, 2))
        self.assertIn("email and meeting", proposal.basis)

    def test_same_source_and_different_owner_pairs_are_not_signalled(self):
        self._task(1, kind="email", text="Prepare the synthetic checklist")
        self._task(2, kind="email", text="Draft the synthetic checklist")
        self._task(3, kind="meeting", text="Draft the synthetic checklist",
                   owner="B")
        result = detection.scan(self.connection, now=NOW)
        self.assertEqual(result.pairs_considered, 0)
        self.assertIsNone(proposals.next_open(self.connection))

    def test_unrelated_text_does_not_create_a_proposal(self):
        self._task(1, kind="email", text="Prepare the synthetic checklist")
        self._task(2, kind="meeting", text="Review the unrelated budget")
        result = detection.scan(self.connection, now=NOW)
        self.assertEqual((result.pairs_considered, result.pairs_signalled), (1, 0))
        self.assertIsNone(proposals.next_open(self.connection))

    def test_recently_closed_task_is_compared_with_a_new_open_task(self):
        self._task(
            1, kind="email", text="Prepare the synthetic rollout checklist",
            status="done", closed_at="2030-02-15T12:00:00+00:00",
        )
        self._task(2, kind="meeting", text="Draft the synthetic rollout checklist")
        result = detection.scan(self.connection, now=NOW)
        self.assertEqual((result.pairs_considered, result.proposals_recorded), (1, 1))

    def test_long_closed_task_is_not_compared(self):
        self._task(
            1, kind="email", text="Prepare the synthetic rollout checklist",
            status="done", closed_at="2030-01-01T12:00:00+00:00",
        )
        self._task(2, kind="meeting", text="Draft the synthetic rollout checklist")
        result = detection.scan(self.connection, now=NOW)
        self.assertEqual((result.pairs_considered, result.proposals_recorded), (0, 0))

    def test_repeat_scan_is_idempotent_and_preserves_reader_rejection(self):
        self._task(1, kind="email", text="Prepare the synthetic checklist")
        self._task(2, kind="meeting", text="Draft the synthetic checklist")
        first = detection.scan(self.connection, now=NOW)
        proposal = proposals.next_open(self.connection)
        self.assertTrue(proposals.settle(
            self.connection, proposal_id=proposal.id,
            decision=proposals.Decision.REJECTED, actor="reader", now=NOW,
        ))
        again = detection.scan(self.connection, now="2030-03-02T12:00:00+00:00")
        self.assertEqual((first.proposals_recorded, again.proposals_unchanged),
                         (1, 1))
        self.assertIsNone(proposals.next_open(self.connection))

    def test_database_scan_returns_only_content_free_counts(self):
        self.connection.close()
        with closing(sqlite3.connect(self.database)) as connection:
            connection.row_factory = sqlite3.Row
            self.connection = connection
            self._task(1, kind="email", text="Prepare the synthetic checklist")
            self._task(2, kind="meeting", text="Draft the synthetic checklist")
            connection.commit()
        result = detection.scan_database(self.database, now=NOW)
        self.assertEqual(result.proposals_recorded, 1)


if __name__ == "__main__":
    unittest.main()
