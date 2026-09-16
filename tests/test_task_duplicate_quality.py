#!/usr/bin/env python3
"""Synthetic tests for content-free duplicate-detector quality reporting."""

from __future__ import annotations

import json
import io
import sqlite3
import tempfile
import unittest
from contextlib import closing, redirect_stdout
from pathlib import Path

from foxhound import migrate_database
from foxhound import task_duplicate_proposals as proposals
from foxhound import task_duplicate_quality as quality


NOW = "2030-03-01T12:00:00+00:00"


class QualityReportTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.database = Path(self.directory.name) / "foxhound.sqlite3"
        migrate_database(self.database)

    def _proposal(self, connection, left, right, detector):
        for task_id in (left, right):
            connection.execute(
                "INSERT OR IGNORE INTO tasks("
                "id,status,text,version,created_at,updated_at,owner_ref_version,"
                "owner_kind,owner_speaker_id,owner_canonical_speaker_id,"
                "owner_speaker_registry_id,owner_pinned,owner_provisional) "
                "VALUES(?,'open',?,1,?,?,1,'person','SPK_1','SPK_1','registry',0,0)",
                (task_id, f"Synthetic task {task_id}", NOW, NOW),
            )
        return proposals.propose(
            connection, task_id_a=left, task_id_b=right,
            basis="synthetic basis", detector=detector, now=NOW,
            allow_unconfirmed_owner=True,
        )

    def test_report_separates_detectors_and_withholds_rate_until_answered(self):
        with closing(sqlite3.connect(self.database)) as connection:
            connection.row_factory = sqlite3.Row
            first = self._proposal(connection, 1, 2, "detector-a")
            self._proposal(connection, 3, 4, "detector-b")
            proposals.settle(
                connection, proposal_id=first.proposal_id,
                decision=proposals.Decision.CONFIRMED, actor="reader", now=NOW,
            )
            connection.commit()
        lines = {line["detector"]: line for line in quality.report(self.database)}
        self.assertEqual(lines["detector-a"]["confirmed"], 1)
        self.assertEqual(lines["detector-a"]["confirm_rate"], 1.0)
        self.assertEqual(lines["detector-b"]["awaiting"], 1)
        self.assertIsNone(lines["detector-b"]["confirm_rate"])

    def test_command_output_carries_no_task_text(self):
        with closing(sqlite3.connect(self.database)) as connection:
            connection.row_factory = sqlite3.Row
            self._proposal(connection, 1, 2, "detector-a")
            connection.commit()
        stream = io.StringIO()
        with redirect_stdout(stream):
            code = quality.main(["--database", str(self.database)])
        payload = json.loads(stream.getvalue())
        self.assertEqual(code, 0)
        self.assertTrue(payload["accepted"])
        self.assertNotIn("Synthetic task", stream.getvalue())
        self.assertNotIn("synthetic basis", stream.getvalue())

    def test_missing_database_is_a_content_free_refusal(self):
        stream = io.StringIO()
        with redirect_stdout(stream):
            code = quality.main(["--database", str(self.database.parent / "absent")])
        self.assertEqual(code, 2)
        self.assertEqual(json.loads(stream.getvalue()), {"accepted": False})


if __name__ == "__main__":
    unittest.main()
