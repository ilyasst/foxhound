"""Synthetic tests for unverified duplicate-candidate reconciliation."""

from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from foxhound import migrate_database
from foxhound import task_duplicate_reconcile as reconcile
from foxhound import task_duplicate_stage1 as stage1


NOW = "2030-03-01T12:00:00+00:00"


class ReconcileTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.database = Path(self.directory.name) / "foxhound.sqlite3"
        migrate_database(self.database)
        self.connection = sqlite3.connect(self.database)
        self.connection.row_factory = sqlite3.Row
        self.addCleanup(self.connection.close)
        for task_id in range(1, 9):
            self.connection.execute(
                "INSERT INTO tasks(id,status,text,version,created_at,updated_at) "
                "VALUES(?,'open',?,1,?,?)",
                (task_id, f"Synthetic task {task_id}", NOW, NOW),
            )
        self.connection.commit()

    def _candidate(
        self, left: int, right: int, routes: dict[str, float], *,
        rank_score: float = 1.0,
    ) -> int:
        cursor = self.connection.execute(
            "INSERT INTO task_duplicate_candidates("
            "left_task_id,right_task_id,left_task_version,right_task_version,"
            "rank_score,state,created_at,updated_at) "
            "VALUES(?,?,1,1,?,'queued',?,?)",
            (left, right, rank_score, NOW, NOW),
        )
        candidate_id = int(cursor.lastrowid)
        self.connection.executemany(
            "INSERT INTO task_duplicate_candidate_routes(candidate_id,route,score) "
            "VALUES(?,?,?)",
            ((candidate_id, route, score) for route, score in routes.items()),
        )
        self.connection.commit()
        return candidate_id

    def test_dry_run_reports_without_writing(self) -> None:
        removed = self._candidate(1, 2, {"reread": 1.0})
        rescored = self._candidate(3, 4, {"words": 0.6, "reread": 1.0})

        result = reconcile.run_database(self.database)

        self.assertFalse(result.applied)
        self.assertEqual(result.supporting_only_removed, 1)
        self.assertEqual(result.score_changes, 1)
        self.assertIsNotNone(self.connection.execute(
            "SELECT 1 FROM task_duplicate_candidates WHERE id=?", (removed,)
        ).fetchone())
        self.assertEqual(self.connection.execute(
            "SELECT rank_score FROM task_duplicate_candidates WHERE id=?",
            (rescored,),
        ).fetchone()[0], 1.0)

    def test_apply_is_bounded_and_second_run_writes_nothing(self) -> None:
        first = self._candidate(1, 2, {"words": 0.6}, rank_score=1.0)
        second = self._candidate(3, 4, {"owner": 1.0})

        bounded = reconcile.run_database(self.database, apply=True, limit=1)
        remaining = reconcile.run_database(self.database, apply=True, limit=10)
        no_op = reconcile.run_database(self.database, apply=True, limit=10)

        self.assertEqual((bounded.examined, bounded.score_changes), (1, 1))
        self.assertIsNotNone(self.connection.execute(
            "SELECT 1 FROM task_duplicate_candidates WHERE id=?", (first,)
        ).fetchone())
        self.assertEqual(remaining.supporting_only_removed, 1)
        self.assertIsNone(self.connection.execute(
            "SELECT 1 FROM task_duplicate_candidates WHERE id=?", (second,)
        ).fetchone())
        self.assertEqual(no_op.examined, 0)

    def test_apply_recalculates_current_combined_score(self) -> None:
        candidate_id = self._candidate(
            1, 2, {"words": 0.6, "reread": 1.0}, rank_score=1.0,
        )

        result = reconcile.run_database(self.database, apply=True)

        expected = stage1._combined_score({"words": 0.6, "reread": 1.0})
        actual = self.connection.execute(
            "SELECT rank_score FROM task_duplicate_candidates WHERE id=?",
            (candidate_id,),
        ).fetchone()[0]
        self.assertEqual(result.score_changes, 1)
        self.assertAlmostEqual(actual, expected)

    def test_active_claim_is_skipped(self) -> None:
        candidate_id = self._candidate(1, 2, {"reread": 1.0})
        self.connection.execute(
            "INSERT INTO task_duplicate_verification_claims("
            "candidate_id,claimed_at,attempts) VALUES(?,?,1)",
            (candidate_id, NOW),
        )
        self.connection.commit()

        result = reconcile.run_database(self.database, apply=True)

        self.assertEqual(result.claimed_skipped, 1)
        self.assertIsNotNone(self.connection.execute(
            "SELECT 1 FROM task_duplicate_candidates WHERE id=?", (candidate_id,)
        ).fetchone())

    def test_apply_removes_participant_only_candidate(self) -> None:
        participant_only = self._candidate(1, 2, {"participant": 0.9})
        words_and_participant = self._candidate(3, 4, {"words": 0.6, "participant": 0.9})

        result = reconcile.run_database(self.database, apply=True)

        self.assertEqual(result.supporting_only_removed, 1)
        self.assertIsNone(self.connection.execute(
            "SELECT 1 FROM task_duplicate_candidates WHERE id=?", (participant_only,)
        ).fetchone())
        self.assertIsNone(self.connection.execute(
            "SELECT 1 FROM task_duplicate_candidate_routes WHERE candidate_id=?", (participant_only,)
        ).fetchone())
        self.assertIsNotNone(self.connection.execute(
            "SELECT 1 FROM task_duplicate_candidates WHERE id=?", (words_and_participant,)
        ).fetchone())

    def test_verified_history_is_outside_the_write_set(self) -> None:
        candidate_id = self._candidate(1, 2, {"reread": 1.0, "participant": 0.9})
        self.connection.execute(
            "INSERT INTO task_duplicate_verifications("
            "candidate_id,verdict,confidence,citations_json,latency_ms,"
            "prompt_tokens,completion_tokens,verified_at,proposal_id) "
            "VALUES(?,'different',0.9,'[]',10,1,1,?,NULL)",
            (candidate_id, NOW),
        )
        self.connection.commit()

        result = reconcile.run_database(self.database, apply=True)

        self.assertEqual(result.examined, 0)
        self.assertIsNotNone(self.connection.execute(
            "SELECT 1 FROM task_duplicate_verifications WHERE candidate_id=?",
            (candidate_id,),
        ).fetchone())


if __name__ == "__main__":
    unittest.main()
