"""Synthetic tests for visible pre-structure backlog counts."""

from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from foxhound import migrate_database
from foxhound import task_structure_status as status


class StructureStatusTests(unittest.TestCase):
    def test_reports_unstructured_tasks_without_task_content(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "foxhound.sqlite3"
            migrate_database(database)
            with sqlite3.connect(database) as connection:
                connection.execute(
                    "INSERT INTO tasks(status,text,version,created_at,updated_at) "
                    "VALUES('open','Synthetic older task',1,'2030-01-01','2030-01-01')"
                )
                connection.execute(
                    "INSERT INTO tasks(status,text,object,action,confidence,version,"
                    "created_at,updated_at) VALUES('open','Synthetic newer task',"
                    "'sample','review',0.8,1,'2030-01-01','2030-01-01')"
                )
            self.assertEqual(
                status.report(database),
                {"structured_tasks": 1, "unstructured_tasks": 1},
            )


if __name__ == "__main__":
    unittest.main()
