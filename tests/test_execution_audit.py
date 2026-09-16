#!/usr/bin/env python3
"""Synthetic tests for the legacy repository-result audit."""

from __future__ import annotations

from foxhound import migrate_database

import json
import sqlite3
import tempfile
import unittest
from contextlib import closing, redirect_stderr, redirect_stdout
from io import StringIO
from pathlib import Path

from foxhound.candidate_inbox import CandidateInbox
from foxhound.execution_audit import find_empty_repository_completions, main


class ExecutionAuditTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.root.chmod(0o700)
        self.database = self.root / "foxhound.sqlite3"
        migrate_database(self.database)
        self.database.chmod(0o600)

    def _insert_result(
        self, *, task_id: int, kind: str, record: str, phase: str,
        outcome: str, deliverables: str,
    ) -> None:
        now = "2030-01-02T03:04:05+00:00"
        candidate = f"candidate-{task_id}"
        with closing(sqlite3.connect(self.database)) as connection:
            connection.execute(
                "INSERT INTO tasks(id,status,text,owner,due,version,"
                "created_at,updated_at,closed_at) "
                "VALUES(?,'done','Synthetic task',NULL,NULL,1,?,?,?)",
                (task_id, now, now, now),
            )
            connection.execute(
                "INSERT INTO candidate_inbox(candidate_id,source_system,"
                "source_kind,source_record_id,source_item_id,source_revision,"
                "payload_json,created_at,first_imported_at,updated_at) "
                "VALUES(?,'gw',?,?,?,'a','{}',?,?,?)",
                (candidate, kind, record, str(task_id), now, now, now),
            )
            connection.execute(
                "INSERT INTO task_candidate_bindings(candidate_id,"
                "source_revision,task_id,relation,decided_at) "
                "VALUES(?,'a',?,'accepted',?)",
                (candidate, task_id, now),
            )
            connection.execute(
                "INSERT INTO task_execution_results(result_id,task_id,"
                "workflow_version,task_version,phase,outcome,content_digest,"
                "summary,work_markdown,questions_json,external_actions_json,"
                "deliverables_json,created_at) "
                "VALUES(?,?,1,1,?,?,?,'Synthetic','Synthetic','[]','[]',?,?)",
                (
                    f"result-{task_id}", task_id, phase, outcome,
                    "a" * 64, deliverables, now,
                ),
            )
            connection.commit()

    def test_lists_only_completed_github_delivery_failures(self):
        self._insert_result(
            task_id=1, kind="issue", record="github.com/example/repo",
            phase="execute", outcome="completed", deliverables="[]",
        )
        self._insert_result(
            task_id=2, kind="issue", record="github.com/example/repo",
            phase="plan", outcome="completed", deliverables="[]",
        )
        self._insert_result(
            task_id=3, kind="meeting", record="private-record",
            phase="execute", outcome="completed", deliverables="[]",
        )
        self._insert_result(
            task_id=4, kind="issue", record="github.com/example/repo",
            phase="execute", outcome="completed", deliverables='["PR"]',
        )

        findings = find_empty_repository_completions(database_path=self.database)

        self.assertEqual(len(findings), 1)
        self.assertEqual(findings[0].task_id, 1)
        self.assertEqual(findings[0].origin_record_id, "github.com/example/repo")

    def test_cli_is_content_free_and_reports_findings(self):
        self._insert_result(
            task_id=1, kind="review_request", record="github.com/example/repo",
            phase="external_action", outcome="completed", deliverables="[]",
        )
        output = StringIO()
        with redirect_stdout(output):
            self.assertEqual(main(["--database", str(self.database)]), 0)
        document = json.loads(output.getvalue())
        self.assertEqual(document["count"], 1)
        self.assertNotIn("Synthetic task", output.getvalue())

        self.root.chmod(0o755)
        errors = StringIO()
        with redirect_stderr(errors):
            self.assertEqual(main(["--database", str(self.database)]), 70)
        self.assertEqual(errors.getvalue(), "foxhound execution audit: audit unavailable\n")


if __name__ == "__main__":
    unittest.main()
