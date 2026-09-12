from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path

from foxhound import CandidateInbox
from foxhound.task_ledger import TaskLedger
from foxhound.task_lifecycle_outcome_export import (
    ExportDisposition,
    TaskLifecycleOutcomeExportError,
    export_outcomes,
)


NOW = datetime(2030, 1, 2, 3, 4, 5, tzinfo=timezone.utc)


class LifecycleOutcomeExportTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.root.chmod(0o700)
        self.database = self.root / "foxhound.sqlite3"
        CandidateInbox(self.database, clock=lambda: NOW).initialize()
        self.database.chmod(0o600)
        self.outbox = self.root / "outcomes"
        self.outbox.mkdir(mode=0o700)
        self.ledger = TaskLedger(self.database, clock=lambda: NOW)
        self._insert_task(1, legacy_task_id=91)

    def _insert_task(
        self, task_id: int, *, legacy_task_id: int | None = None
    ) -> None:
        with closing(sqlite3.connect(self.database)) as connection:
            connection.execute("PRAGMA foreign_keys=ON")
            connection.execute(
                "INSERT INTO tasks(id,status,text,owner,due,version,"
                "created_at,updated_at,closed_at) VALUES(?, 'open', ?, "
                "NULL, NULL, 1, ?, ?, NULL)",
                (
                    task_id,
                    f"Synthetic task {task_id}",
                    NOW.isoformat(),
                    NOW.isoformat(),
                ),
            )
            connection.execute(
                "INSERT INTO task_events(task_id,kind,task_version,"
                "candidate_id,source_revision,from_status,to_status,"
                "occurred_at) "
                "VALUES(?, 'created', 1, NULL, NULL, NULL, 'open', ?)",
                (task_id, NOW.isoformat()),
            )
            if legacy_task_id is not None:
                connection.execute(
                    "INSERT INTO task_bootstrap_correlations(producer,"
                    "legacy_task_id,task_id,created_at) VALUES('gw',?,?,?)",
                    (legacy_task_id, task_id, NOW.isoformat()),
                )
            connection.commit()

    def _pages(self) -> list[Path]:
        return sorted(self.outbox.glob("page-*.json"))

    def test_empty_export_is_content_free_and_unchanged(self):
        result = export_outcomes(
            self.database, outbox_dir=self.outbox, stream_id="pilot-alpha"
        )
        self.assertEqual(result.disposition, ExportDisposition.UNCHANGED)
        self.assertEqual((result.outcomes_seen, result.current_cursor), (0, 0))
        self.assertEqual(self._pages(), [])

    def test_only_correlated_status_events_are_exported(self):
        self._insert_task(2)
        self.assertTrue(
            self.ledger.transition(
                2, expected_version=1, action="done"
            ).accepted
        )
        self.assertTrue(
            self.ledger.transition(
                1, expected_version=1, action="done"
            ).accepted
        )
        result = export_outcomes(
            self.database, outbox_dir=self.outbox, stream_id="pilot-alpha"
        )
        self.assertEqual(result.outcomes_exported, 1)
        page = json.loads(self._pages()[0].read_text(encoding="utf-8"))
        item = page["items"][0]
        self.assertEqual(item["sequence"], 1)
        self.assertGreater(item["outcome"]["event_sequence"], 1)
        self.assertEqual(item["outcome"]["correlation"], {
            "system": "gw", "task_id": 91,
        })
        encoded = json.dumps(page)
        self.assertNotIn("Synthetic task", encoded)
        self.assertNotIn("owner", encoded)

    def test_pagination_replay_and_later_append_are_exact(self):
        self.assertTrue(
            self.ledger.transition(
                1, expected_version=1, action="done"
            ).accepted
        )
        self.assertTrue(
            self.ledger.transition(
                1, expected_version=2, action="reopen"
            ).accepted
        )
        first = export_outcomes(
            self.database,
            outbox_dir=self.outbox,
            stream_id="pilot-alpha",
            max_page_items=1,
        )
        self.assertEqual((first.current_cursor, len(first.pages)), (2, 2))
        replay = export_outcomes(
            self.database, outbox_dir=self.outbox, stream_id="pilot-alpha"
        )
        self.assertEqual(replay.disposition, ExportDisposition.UNCHANGED)
        self.assertEqual(replay.previous_cursor, 2)
        self.assertTrue(
            self.ledger.transition(
                1, expected_version=3, action="drop"
            ).accepted
        )
        later = export_outcomes(
            self.database, outbox_dir=self.outbox, stream_id="pilot-alpha"
        )
        self.assertEqual((later.previous_cursor, later.current_cursor), (2, 3))
        self.assertEqual(len(self._pages()), 3)

    def test_altered_history_and_unknown_entries_fail_closed(self):
        self.assertTrue(
            self.ledger.transition(
                1, expected_version=1, action="done"
            ).accepted
        )
        export_outcomes(
            self.database, outbox_dir=self.outbox, stream_id="pilot-alpha"
        )
        page = self._pages()[0]
        document = json.loads(page.read_text(encoding="utf-8"))
        document["items"][0]["outcome"]["to_status"] = "dropped"
        page.write_text(json.dumps(
            document, ensure_ascii=True, separators=(",", ":"), sort_keys=True
        ) + "\n", encoding="utf-8")
        page.chmod(0o600)
        with self.assertRaises(TaskLifecycleOutcomeExportError):
            export_outcomes(
                self.database, outbox_dir=self.outbox, stream_id="pilot-alpha"
            )
        page.unlink()
        (self.outbox / "unexpected.txt").write_text(
            "synthetic", encoding="utf-8"
        )
        with self.assertRaises(TaskLifecycleOutcomeExportError):
            export_outcomes(
                self.database, outbox_dir=self.outbox, stream_id="pilot-alpha"
            )

    def test_unsafe_locations_and_duplicate_json_are_refused(self):
        self.outbox.chmod(0o755)
        with self.assertRaises(TaskLifecycleOutcomeExportError):
            export_outcomes(
                self.database, outbox_dir=self.outbox, stream_id="pilot-alpha"
            )
        self.outbox.chmod(0o700)
        self.assertTrue(
            self.ledger.transition(
                1, expected_version=1, action="done"
            ).accepted
        )
        export_outcomes(
            self.database, outbox_dir=self.outbox, stream_id="pilot-alpha"
        )
        page = self._pages()[0]
        raw = page.read_text(encoding="utf-8")
        duplicate = raw.replace(
            '{"emitted_at"', '{"emitted_at":"x","emitted_at"', 1
        )
        page.write_text(duplicate, encoding="utf-8")
        page.chmod(0o600)
        with self.assertRaises(TaskLifecycleOutcomeExportError):
            export_outcomes(
                self.database, outbox_dir=self.outbox, stream_id="pilot-alpha"
            )

    def test_cli_reports_only_counts_and_generic_failure(self):
        self.assertTrue(
            self.ledger.transition(
                1, expected_version=1, action="done"
            ).accepted
        )
        command = [
            sys.executable,
            "-m",
            "foxhound.task_lifecycle_outcome_export",
            "--database",
            str(self.database),
            "--outbox",
            str(self.outbox),
            "--stream-id",
            "pilot-alpha",
        ]
        environment = dict(os.environ)
        environment["PYTHONPATH"] = str(
            Path(__file__).resolve().parents[1] / "src"
        )
        completed = subprocess.run(
            command,
            text=True,
            capture_output=True,
            env=environment,
            check=False,
        )
        self.assertEqual(completed.returncode, 0)
        status = json.loads(completed.stdout)
        self.assertEqual(set(status), {
            "current_cursor", "disposition", "outcomes_exported",
            "outcomes_seen", "pages", "previous_cursor",
        })
        self.assertNotIn("Synthetic", completed.stdout)
        failed = subprocess.run(
            command[:-1] + ["bad stream"],
            text=True,
            capture_output=True,
            env=environment,
            check=False,
        )
        self.assertEqual(failed.returncode, 1)
        self.assertEqual(
            failed.stderr.strip(), "task lifecycle outcome export failed"
        )


if __name__ == "__main__":
    unittest.main()
