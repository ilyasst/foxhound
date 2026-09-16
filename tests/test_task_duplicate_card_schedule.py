"""Synthetic CLI tests for the duplicate-only card scheduler."""

from __future__ import annotations

import contextlib
import io
import json
import sqlite3
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from foxhound import migrate_database
from foxhound import task_duplicate_proposals as proposals
from foxhound.task_duplicate_card_schedule import main


NOW = datetime(2030, 3, 1, 12, 0, tzinfo=timezone.utc).isoformat()


class DuplicateCardScheduleTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.database = Path(self.directory.name) / "foxhound.sqlite3"
        migrate_database(self.database)
        self.connection = sqlite3.connect(self.database)
        self.connection.row_factory = sqlite3.Row
        self.addCleanup(self.connection.close)
        for task_id in (1, 2):
            self.connection.execute(
                "INSERT INTO tasks(id,status,text,version,created_at,updated_at,"
                "owner_ref_version,owner_kind,owner_speaker_id,"
                "owner_canonical_speaker_id,owner_speaker_registry_id,"
                "owner_pinned,owner_provisional) VALUES(?, 'open', ?, 1, ?, ?,"
                "1, 'person', 'SPK_1', 'SPK_1', 'registry-A', 0, 0)",
                (task_id, f"Synthetic duplicate task {task_id}", NOW, NOW),
            )
        proposals.propose(
            self.connection, task_id_a=1, task_id_b=2,
            basis="Synthetic shared deliverable.", detector="synthetic", now=NOW,
        )
        self.connection.commit()

    def test_cli_schedules_only_the_duplicate_question(self) -> None:
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            self.assertEqual(main([
                "--database", str(self.database), "--limit", "5",
            ]), 0)
        document = json.loads(output.getvalue())
        self.assertEqual(document, {
            "asked": 1,
            "cancelled": 0,
            "created": 1,
            "disposition": "applied",
            "ok": True,
            "refusal": None,
        })

    def test_invalid_limit_is_a_content_free_refusal(self) -> None:
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            self.assertEqual(main([
                "--database", str(self.database), "--limit", "0",
            ]), 1)
        document = json.loads(output.getvalue())
        self.assertEqual(document["refusal"], "invalid_argument")
        self.assertNotIn("Synthetic", output.getvalue())


if __name__ == "__main__":
    unittest.main()
