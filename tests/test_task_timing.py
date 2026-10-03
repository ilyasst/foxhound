"""Tests for task_timing storage and schema v70 migration."""

from __future__ import annotations

import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path

from foxhound import CandidateInbox, migrate_database
from foxhound.candidate_inbox import SCHEMA_VERSION
from foxhound.task_timing import record_research_timing, read_timing


class TaskTimingTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "test.db"
        migrate_database(self.db_path)
        self.inbox = CandidateInbox(self.db_path)

    def tearDown(self):
        self.temp_dir.cleanup()

    def _insert_task(self, connection: sqlite3.Connection, task_id: int = 1) -> None:
        connection.execute(
            """
            INSERT INTO tasks (
                id, status, text, owner, due, version, created_at, updated_at
            ) VALUES (
                ?, 'open', 'Synthetic task text', 'person-a', '2026-10-10', 1,
                '2026-10-01T00:00:00Z', '2026-10-01T00:00:00Z'
            )
            """,
            (task_id,),
        )

    def test_schema_v70_created_and_version_updated(self):
        with closing(sqlite3.connect(self.db_path)) as conn:
            v = conn.execute("PRAGMA user_version").fetchone()[0]
            self.assertEqual(v, SCHEMA_VERSION)
            self.assertEqual(v, SCHEMA_VERSION)
            tables = {
                r[0]
                for r in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }
            self.assertIn("task_timing", tables)

    def test_v69_to_v70_migration(self):
        mig_dir = tempfile.TemporaryDirectory()
        try:
            mig_db = Path(mig_dir.name) / "migrate.db"
            migrate_database(mig_db)
            with closing(sqlite3.connect(mig_db)) as conn:
                conn.execute("DROP TABLE task_timing")
                conn.execute("PRAGMA user_version = 69")
                conn.commit()

            # Now migrate_database(mig_db) will run migration from v69 to v70
            migrate_database(mig_db)

            with closing(sqlite3.connect(mig_db)) as conn:
                v = conn.execute("PRAGMA user_version").fetchone()[0]
                self.assertEqual(v, SCHEMA_VERSION)
                tables = {
                    r[0]
                    for r in conn.execute(
                        "SELECT name FROM sqlite_master WHERE type='table'"
                    ).fetchall()
                }
                self.assertIn("task_timing", tables)
        finally:
            mig_dir.cleanup()

    def test_record_and_read_timing(self):
        with closing(sqlite3.connect(self.db_path)) as conn:
            self._insert_task(conn, 1)
            doc = {
                "report": {
                    "effort": {"size": "day"},
                    "deadline": {"date": "2026-10-15"},
                }
            }
            changed = record_research_timing(
                conn, 1, "job-1", doc, "2026-10-02T12:00:00Z"
            )
            self.assertTrue(changed)

            # Read back
            timing = read_timing(conn, [1, 2])
            self.assertEqual(timing[1], ("day", "2026-10-15"))
            self.assertEqual(timing[2], (None, None))

    def test_partial_update_keeps_other_field(self):
        with closing(sqlite3.connect(self.db_path)) as conn:
            self._insert_task(conn, 1)
            doc1 = {
                "report": {
                    "effort": {"size": "day"},
                    "deadline": {"date": "2026-10-15"},
                }
            }
            record_research_timing(conn, 1, "job-1", doc1, "2026-10-02T12:00:00Z")

            # Update only effort
            doc2 = {
                "report": {
                    "effort": {"size": "week"},
                }
            }
            changed = record_research_timing(
                conn, 1, "job-2", doc2, "2026-10-02T13:00:00Z"
            )
            self.assertTrue(changed)
            timing = read_timing(conn, [1])
            self.assertEqual(timing[1], ("week", "2026-10-15"))

            # Update only deadline
            doc3 = {
                "report": {
                    "deadline": {"date": "2026-10-20"},
                }
            }
            changed = record_research_timing(
                conn, 1, "job-3", doc3, "2026-10-02T14:00:00Z"
            )
            self.assertTrue(changed)
            timing = read_timing(conn, [1])
            self.assertEqual(timing[1], ("week", "2026-10-20"))

            # No changes when re-applying identical doc
            changed_again = record_research_timing(
                conn, 1, "job-3", doc3, "2026-10-02T14:00:00Z"
            )
            self.assertFalse(changed_again)

    def test_read_timing_empty(self):
        with closing(sqlite3.connect(self.db_path)) as conn:
            self.assertEqual(read_timing(conn, []), {})


class WithResearchTimingTests(unittest.TestCase):
    def test_earlier_cited_deadline_and_effort_fold_into_timing(self):
        import tempfile
        from datetime import date
        from pathlib import Path
        from foxhound.task_deadlines import TaskTiming
        from foxhound.task_timing import with_research_timing
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "db.sqlite3"
            migrate_database(path)
            connection = sqlite3.connect(path)
            connection.execute("PRAGMA foreign_keys = OFF")
            connection.execute(
                "INSERT INTO task_timing(task_id,effort,researched_due,source_job_id,updated_at) "
                "VALUES(1,'week','2030-01-05','job-1','2030-01-01T00:00:00+00:00'),"
                "(2,NULL,'2030-03-01','job-2','2030-01-01T00:00:00+00:00')")
            tasks = {
                1: TaskTiming(due=date(2030, 1, 10), effort=None, open=True),
                2: TaskTiming(due=date(2030, 2, 1), effort="day", open=True),
                3: TaskTiming(due=None, effort=None, open=True),
            }
            folded = with_research_timing(connection, tasks)
            connection.close()
        self.assertEqual(folded[1].due, date(2030, 1, 5))
        self.assertEqual(folded[1].effort, "week")
        self.assertEqual(folded[2].due, date(2030, 2, 1))   # later cited date ignored
        self.assertEqual(folded[2].effort, "day")
        self.assertEqual(folded[3], tasks[3])
