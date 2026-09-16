"""Synthetic tests for background fused-task display titles."""

from __future__ import annotations

import io
import json
import sqlite3
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock

from foxhound import migrate_database
from foxhound import fused_task_titles as titles


NOW = datetime(2030, 3, 1, 12, 0, tzinfo=timezone.utc)


class _Response(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
        return False


def _reply(content):
    return _Response(json.dumps({
        "choices": [{"message": {"content": content}}]
    }).encode("utf-8"))


def _opener(result):
    calls = []

    def urlopen(request, timeout=None):
        calls.append((request, timeout))
        if isinstance(result, Exception):
            raise result
        return result() if callable(result) else result

    return mock.Mock(urlopen=urlopen), calls


class FusedTaskTitleTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.database = Path(self.directory.name) / "foxhound.sqlite3"
        migrate_database(self.database)
        self.connection = sqlite3.connect(self.database)
        self.connection.row_factory = sqlite3.Row
        self.addCleanup(self.connection.close)
        for task_id, text in ((1, "Prepare the synthetic rollout checklist"),
                              (2, "Draft the synthetic rollout checklist")):
            self.connection.execute(
                "INSERT INTO tasks(id,status,text,version,created_at,updated_at,"
                "owner_ref_version,owner_kind,owner_speaker_id,"
                "owner_canonical_speaker_id,owner_speaker_registry_id,"
                "owner_pinned,owner_provisional) VALUES(?, 'open', ?, 1, ?, ?,"
                "1, 'person', 'SPK_1', 'SPK_1', 'registry-A', 0, 0)",
                (task_id, text, NOW.isoformat(), NOW.isoformat()),
            )
        self.connection.execute(
            "INSERT INTO task_relations(subject_id,object_id,kind,basis,"
            "asserted_by,actor,created_at) VALUES(2,1,'duplicate_of',"
            "'Synthetic reader confirmation.','reader','reader',?)",
            (NOW.isoformat(),),
        )
        titles.enqueue(self.connection, task_id=1, now=NOW.isoformat())
        self.connection.commit()

    def test_success_uses_thinking_no_and_stores_one_line_title(self) -> None:
        opener, calls = _opener(lambda: _reply("Synthetic rollout checklist"))
        result = titles.run_once(
            self.database, opener=opener, clock=lambda: NOW,
        )
        self.assertEqual((result.attempted, result.completed, result.retryable),
                         (1, 1, 0))
        row = self.connection.execute(
            "SELECT state,title,attempts FROM task_fused_title_jobs WHERE task_id=1"
        ).fetchone()
        self.assertEqual(tuple(row), ("ready", "Synthetic rollout checklist", 1))
        request, timeout = calls[0]
        sent = json.loads(request.data)
        self.assertEqual(sent["model"], "thinking_no")
        self.assertEqual(timeout, titles.TIMEOUT_SECONDS)
        self.assertIn("Task 1:", sent["messages"][1]["content"])
        self.assertIn("Task 2:", sent["messages"][1]["content"])

    def test_malformed_title_stays_pending_for_retry(self) -> None:
        opener, _ = _opener(lambda: _reply("# Heading\nsecond line"))
        result = titles.run_once(
            self.database, opener=opener, clock=lambda: NOW,
        )
        self.assertEqual((result.attempted, result.completed, result.retryable),
                         (1, 0, 1))
        row = self.connection.execute(
            "SELECT state,title,attempts FROM task_fused_title_jobs WHERE task_id=1"
        ).fetchone()
        self.assertEqual(tuple(row), ("pending", None, 1))

    def test_transport_failure_stays_pending_without_changing_task_text(self) -> None:
        opener, _ = _opener(OSError("synthetic gateway unavailable"))
        result = titles.run_once(
            self.database, opener=opener, clock=lambda: NOW,
        )
        self.assertEqual((result.attempted, result.completed, result.retryable),
                         (1, 0, 1))
        self.assertEqual(
            self.connection.execute("SELECT text FROM tasks WHERE id=1").fetchone()[0],
            "Prepare the synthetic rollout checklist",
        )
        self.assertEqual(
            self.connection.execute(
                "SELECT state FROM task_fused_title_jobs WHERE task_id=1"
            ).fetchone()[0],
            "pending",
        )


if __name__ == "__main__":
    unittest.main()
