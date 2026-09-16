#!/usr/bin/env python3
"""Synthetic tests for the one-shot verified shadow bootstrap command."""

from __future__ import annotations

from foxhound import migrate_database

import hashlib
import json
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path
from unittest import mock

from foxhound.candidate_inbox import CandidateInbox
from foxhound.contracts import candidate_id_for, comparable_task_digest
from foxhound.task_bootstrap import main
from foxhound.task_ledger import TaskLedger


NOW = datetime(2030, 3, 1, 12, 0, tzinfo=timezone.utc)
TOKEN = "synthetic-knowledge-token-with-sufficient-length"


def _candidate(index: int) -> dict:
    record_id = f"record-{index:03d}"
    item_id = f"action-{index:03d}"
    text = f"Prepare synthetic summary {index}"
    owner = "Person A"
    revision = hashlib.sha256(
        json.dumps([text, owner, index]).encode("utf-8")
    ).hexdigest()
    return {
        "schema": "foxhound.task-candidate",
        "schema_version": 2,
        "candidate_id": candidate_id_for(
            system="gw",
            kind="meeting",
            record_id=record_id,
            item_id=item_id,
        ),
        "source": {
            "system": "gw",
            "kind": "meeting",
            "record_id": record_id,
            "item_id": item_id,
            "revision": revision,
        },
        "task": {"text": text, "owner": owner, "due": None},
        "evidence": {
            "document_id": record_id,
            "locator": f"action-item-{index:03d}",
        },
        "created_at": "2030-01-01T12:00:00Z",
    }


def _observation(item: dict, legacy_task_id: int) -> dict:
    return {
        "schema": "foxhound.task-shadow-observation",
        "schema_version": 1,
        "candidate": item,
        "disposition": "minted",
        "legacy_task": {
            "task_id": legacy_task_id,
            "comparable_digest": comparable_task_digest(
                text=item["task"]["text"],
                project=None,
                owner=item["task"]["owner"],
            ),
        },
        "reason_code": None,
        "observed_at": "2030-02-01T12:00:00Z",
    }


class TaskBootstrapCommandTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.root.chmod(0o700)
        self.database = self.root / "foxhound.sqlite3"
        self.inbox = CandidateInbox(self.database, clock=lambda: NOW)
        migrate_database(self.database)
        self.database.chmod(0o600)
        self.token = self.root / "knowledge.token"
        self.token.write_text(TOKEN, encoding="utf-8")
        self.token.chmod(0o600)

    def _arguments(self, *, token: Path | None = None) -> list[str]:
        return [
            "--database", str(self.database),
            "--gw-endpoint", "http://127.0.0.1:8787",
            "--gw-alias", "primary",
            "--gw-token-file", str(self.token if token is None else token),
        ]

    def _import_group(self, *items: tuple[dict, int]) -> None:
        observations = []
        for item, legacy_task_id in items:
            self.assertTrue(self.inbox.import_document(item).accepted)
            observations.append(_observation(item, legacy_task_id))
        feed = {
            "schema": "foxhound.task-shadow-observation-feed",
            "schema_version": 1,
            "producer": "gw",
            "stream_id": "primary",
            "from_cursor": 0,
            "to_cursor": len(observations),
            "items": [
                {"sequence": index, "observation": observation}
                for index, observation in enumerate(observations, start=1)
            ],
            "emitted_at": "2030-03-01T12:00:00Z",
        }
        self.assertTrue(self.inbox.import_shadow_feed(feed).accepted)

    def test_command_materializes_agreed_group_and_exact_retry_is_safe(self):
        item = _candidate(1)
        self._import_group((item, 9001))

        first_stdout = StringIO()
        with redirect_stdout(first_stdout):
            self.assertEqual(main(self._arguments()), 0)
        first = json.loads(first_stdout.getvalue())
        self.assertTrue(first["ok"])
        self.assertEqual(first["disposition"], "applied")
        self.assertEqual(first["counts"]["tasks_created"], 1)
        self.assertNotIn(item["task"]["text"], first_stdout.getvalue())

        second_stdout = StringIO()
        with redirect_stdout(second_stdout):
            self.assertEqual(main(self._arguments()), 0)
        second = json.loads(second_stdout.getvalue())
        self.assertEqual(second["disposition"], "unchanged")
        self.assertEqual(second["counts"]["bindings_unchanged"], 1)
        self.assertEqual(TaskLedger(self.database).count(), 1)

    def test_conflicting_group_refuses_atomically_with_aggregate_output(self):
        first = _candidate(1)
        second = _candidate(2)
        self._import_group((first, 9001), (second, 9001))

        stdout = StringIO()
        with redirect_stdout(stdout):
            self.assertEqual(main(self._arguments()), 1)
        result = json.loads(stdout.getvalue())
        self.assertFalse(result["ok"])
        self.assertEqual(result["refusal"], "state_conflict")
        self.assertEqual(TaskLedger(self.database).count(), 0)
        self.assertNotIn(first["task"]["text"], stdout.getvalue())
        self.assertNotIn(second["task"]["text"], stdout.getvalue())

    def test_unsafe_token_parent_fails_before_mutating_the_ledger(self):
        item = _candidate(1)
        self._import_group((item, 9001))
        public = self.root / "public"
        public.mkdir(mode=0o755)
        token = public / "knowledge.token"
        token.write_text(TOKEN, encoding="utf-8")
        token.chmod(0o600)

        stdout = StringIO()
        stderr = StringIO()
        with redirect_stdout(stdout), redirect_stderr(stderr):
            self.assertEqual(main(self._arguments(token=token)), 78)
        self.assertEqual(stdout.getvalue(), "")
        self.assertEqual(
            stderr.getvalue(),
            "foxhound task bootstrap: configuration unavailable\n",
        )
        self.assertEqual(TaskLedger(self.database).count(), 0)

    def test_internal_failure_is_content_free(self):
        private_text = "Synthetic private task content"
        stderr = StringIO()
        with mock.patch(
            "foxhound.task_bootstrap.run_bootstrap",
            side_effect=RuntimeError(private_text),
        ), redirect_stderr(stderr):
            self.assertEqual(main(self._arguments()), 70)
        self.assertEqual(
            stderr.getvalue(),
            "foxhound task bootstrap: bootstrap failed\n",
        )
        self.assertNotIn(private_text, stderr.getvalue())


if __name__ == "__main__":
    unittest.main()
