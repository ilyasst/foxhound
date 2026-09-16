#!/usr/bin/env python3
"""Synthetic tests for explicit Foxhound database lifecycle commands."""

from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from contextlib import closing, redirect_stderr, redirect_stdout
from io import StringIO
from pathlib import Path

from foxhound import CandidateInbox, InboxError
from foxhound.candidate_inbox import SCHEMA_VERSION
from foxhound.database_lifecycle import (
    DatabaseState,
    inspect_database,
    main,
    migrate_database,
)


class DatabaseLifecycleTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.root.chmod(0o700)
        self.database = self.root / "foxhound.sqlite3"

    def test_inspection_does_not_create_a_missing_database(self) -> None:
        inspection = inspect_database(self.database)

        self.assertEqual(inspection.state, DatabaseState.MISSING)
        self.assertFalse(self.database.exists())

    def test_migration_creates_and_inspects_a_current_database(self) -> None:
        inspection = migrate_database(self.database)

        self.assertEqual(inspection.state, DatabaseState.CURRENT)
        self.assertTrue(inspection.compatible)
        self.assertEqual(CandidateInbox(self.database).count(), 0)

    def test_inspection_reports_upgrade_without_changing_state(self) -> None:
        migrate_database(self.database)
        with closing(sqlite3.connect(self.database)) as connection, connection:
            connection.execute("PRAGMA user_version = 30")

        inspection = inspect_database(self.database)

        self.assertEqual(inspection.state, DatabaseState.UPGRADE_REQUIRED)
        with closing(sqlite3.connect(self.database)) as connection:
            self.assertEqual(
                connection.execute("PRAGMA user_version").fetchone()[0], 30
            )

    def test_migration_refuses_a_newer_database(self) -> None:
        migrate_database(self.database)
        with closing(sqlite3.connect(self.database)) as connection, connection:
            connection.execute(f"PRAGMA user_version = {SCHEMA_VERSION + 1}")

        with self.assertRaisesRegex(InboxError, "newer"):
            migrate_database(self.database)

    def test_migration_refuses_an_incomplete_current_database(self) -> None:
        migrate_database(self.database)
        with closing(sqlite3.connect(self.database)) as connection, connection:
            connection.execute("DROP TABLE candidate_inbox")

        self.assertEqual(
            inspect_database(self.database).state, DatabaseState.INCOMPLETE
        )
        with self.assertRaisesRegex(InboxError, "incomplete"):
            migrate_database(self.database)

    def test_cli_is_content_free_for_inspection_and_failure(self) -> None:
        output = StringIO()
        with redirect_stdout(output):
            self.assertEqual(main(["inspect", "--database", str(self.database)]), 1)
        self.assertEqual(
            json.loads(output.getvalue()),
            {
                "compatible": False,
                "expected_schema_version": SCHEMA_VERSION,
                "ok": False,
                "schema_version": None,
                "state": "missing",
            },
        )

        errors = StringIO()
        with redirect_stderr(errors):
            self.assertEqual(
                main(["migrate", "--database", str(self.root / "missing" / "db")]),
                70,
            )
        self.assertEqual(errors.getvalue(), "foxhound database: operation failed\n")


if __name__ == "__main__":
    unittest.main()
