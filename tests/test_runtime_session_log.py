from __future__ import annotations

import json
from pathlib import Path
import sqlite3
import tempfile
import unittest

from foxhound.runtime_session_log import (
    RUNTIME_SESSION_LOG_NAME,
    RuntimeSessionLogError,
    rotate_runtime_session_logs,
    write_runtime_session_log,
)


class RuntimeSessionLogTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.database = self.root / "runtime.sqlite3"
        with sqlite3.connect(self.database) as connection:
            connection.execute(
                "CREATE TABLE sessions (id TEXT PRIMARY KEY, source TEXT, "
                "parent_session_id TEXT, api_call_count INTEGER, "
                "tool_call_count INTEGER, end_reason TEXT)"
            )
            connection.execute(
                "CREATE TABLE messages (id INTEGER PRIMARY KEY, session_id TEXT, "
                "role TEXT, tool_calls TEXT, tool_name TEXT, content TEXT)"
            )
            connection.execute(
                "INSERT INTO sessions VALUES "
                "('session-a', 'foxhound-a', NULL, 3, 2, 'complete'), "
                "('session-b', 'other', 'session-a', 4, 2, 'budget')"
            )
            connection.execute(
                "INSERT INTO messages VALUES "
                "(1, 'session-a', 'assistant', '{\"arguments\":{\"path\":\"x\"}}', NULL, NULL), "
                "(2, 'session-b', 'tool', NULL, 'terminal', 'synthetic failure')"
            )

    def test_a_compression_child_inheriting_the_tag_is_not_ambiguous(self) -> None:
        """The runtime copies a session's source onto its compression child,
        so a tag unique to one run still matches more than one row."""
        with sqlite3.connect(self.database) as connection:
            connection.execute(
                "UPDATE sessions SET source='foxhound-a' WHERE id='session-b'"
            )
        destination = self.root / "task" / "runs" / "plan-shared"
        destination.mkdir(parents=True)

        path = write_runtime_session_log(
            self.database,
            source="foxhound-a",
            destination=destination,
            turn_budget=80,
        )

        document = json.loads(path.read_text(encoding="utf-8"))
        self.assertEqual(
            [row["id"] for row in document["sessions"]],
            ["session-a", "session-b"],
        )

    def test_two_unrelated_sessions_sharing_a_tag_are_refused(self) -> None:
        with sqlite3.connect(self.database) as connection:
            connection.execute(
                "INSERT INTO sessions VALUES "
                "('session-c', 'foxhound-a', NULL, 1, 0, 'complete')"
            )
        destination = self.root / "task" / "runs" / "plan-ambiguous"
        destination.mkdir(parents=True)

        with self.assertRaises(RuntimeSessionLogError):
            write_runtime_session_log(
                self.database,
                source="foxhound-a",
                destination=destination,
                turn_budget=80,
            )

    def test_copies_the_structured_session_and_its_compression_child(self) -> None:
        destination = self.root / "task" / "runs" / "plan-a"
        destination.mkdir(parents=True)

        path = write_runtime_session_log(
            self.database,
            source="foxhound-a",
            destination=destination,
            turn_budget=80,
        )

        self.assertEqual(path.name, RUNTIME_SESSION_LOG_NAME)
        document = json.loads(path.read_text(encoding="utf-8"))
        self.assertEqual(document["source"], "foxhound-a")
        self.assertEqual(document["run"]["turn_budget"], 80)
        self.assertEqual(
            [row["id"] for row in document["sessions"]],
            ["session-a", "session-b"],
        )
        self.assertEqual(document["messages"][0]["tool_calls"], '{"arguments":{"path":"x"}}')
        self.assertEqual(document["messages"][1]["content"], "synthetic failure")
        self.assertEqual(path.stat().st_mode & 0o777, 0o600)

    def test_retention_removes_oldest_completed_log_but_not_the_current_one(self) -> None:
        runs = self.root / "task" / "runs"
        old = runs / "plan-old"
        current = runs / "plan-current"
        old.mkdir(parents=True)
        current.mkdir()
        (old / RUNTIME_SESSION_LOG_NAME).write_bytes(b"a" * 16)
        (current / RUNTIME_SESSION_LOG_NAME).write_bytes(b"b" * 16)

        removed = rotate_runtime_session_logs(
            runs.parent, retain_bytes=16, current_directory=current
        )

        self.assertEqual(removed, (old / RUNTIME_SESSION_LOG_NAME,))
        self.assertFalse((old / RUNTIME_SESSION_LOG_NAME).exists())
        self.assertTrue((current / RUNTIME_SESSION_LOG_NAME).exists())


if __name__ == "__main__":
    unittest.main()
