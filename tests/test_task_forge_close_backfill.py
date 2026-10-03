#!/usr/bin/env python3
"""Synthetic tests for task forge close backfill CLI and operations."""

from __future__ import annotations

import hashlib
import json
import sqlite3
import tempfile
import unittest
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path

from foxhound import migrate_database
from foxhound.candidate_inbox import CandidateInbox
from foxhound.contracts import candidate_id_for
from foxhound.task_execution import (
    ExecutionOutcome,
    ExecutionResultEnvelope,
    TaskExecutionService,
    WorkflowPhase,
    WorkflowStatus,
)
from foxhound.task_forge_close_backfill import main, run_forge_close_backfill
from foxhound.task_ledger import TaskLedger, TaskStatus
from review_card_fixture import raise_review_cards

NOW = datetime(2030, 3, 1, 12, 0, tzinfo=timezone.utc)
NOW_ISO = NOW.isoformat(timespec="seconds")
CLAIM_TOKEN = "claim-token-synthetic-0000000000000000000000000000000000000000000000"


def candidate(
    index: int,
    *,
    text: str | None = None,
    owner: str | None = "Person A",
    due: str | None = None,
) -> dict:
    task_text = text or f"Prepare synthetic summary {index}"
    record_id = f"record-{index:03d}"
    item_id = f"action-{index:03d}"
    revision = hashlib.sha256(
        json.dumps([task_text, owner, due, index]).encode("utf-8")
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
        "task": {"text": task_text, "owner": owner, "due": due},
        "evidence": {
            "document_id": record_id,
            "locator": f"action-item-{index:03d}",
        },
        "created_at": "2030-01-01T12:00:00Z",
    }


def owner_candidate(
    index: int,
    *,
    owner: str = "Person A",
    speaker_id: str = "SPK_101",
    canonical_speaker_id: str = "SPK_001",
) -> dict:
    item = candidate(index, owner=owner)
    item["schema_version"] = 5
    item["task"]["owner_ref"] = {
        "kind": "person",
        "speaker_id": speaker_id,
        "canonical_speaker_id": canonical_speaker_id,
        "speaker_registry_id": "registry-alpha",
        "pinned": False,
        "provisional": False,
    }
    item["source"]["revision"] = hashlib.sha256(
        json.dumps(item["task"], sort_keys=True).encode("utf-8")
    ).hexdigest()
    return item


def cumulative_candidate(index: int, kind: str) -> dict:
    item = owner_candidate(index)
    item["schema_version"] = 7
    item["source"]["kind"] = kind
    item["candidate_id"] = candidate_id_for(
        system="gw",
        kind=kind,
        record_id=item["source"]["record_id"],
        item_id=item["source"]["item_id"],
    )
    item["lifecycle"] = {
        "state": "active",
        "generation": 1,
        "changed_at": "2030-02-01T12:00:00Z",
    }
    item["source"]["revision"] = hashlib.sha256(
        json.dumps(item, sort_keys=True).encode("utf-8")
    ).hexdigest()
    return item


def history_candidate(
    index: int, *, generation: int = 1, text: str | None = None
) -> dict:
    item = cumulative_candidate(index, "email")
    item["schema_version"] = 9
    item["source"]["history"] = {
        "source": "email",
        "stream_id": "primary",
        "item_id": f"message-{index:03d}",
        "position": generation,
        "revision": hashlib.sha256(
            f"history-{index}-{generation}".encode("utf-8")
        ).hexdigest(),
    }
    item["task"].update({
        "text": text or item["task"]["text"],
        "object": f"synthetic summary {index}",
        "action": "create",
        "confidence": 0.8,
    })
    item["lifecycle"].update({
        "generation": generation,
        "changed_at": f"2030-02-{generation:02d}T12:00:00Z",
    })
    item["source"]["revision"] = hashlib.sha256(
        json.dumps(item, sort_keys=True).encode("utf-8")
    ).hexdigest()
    return item


def review_candidate(
    head_oid: str,
    *,
    title: str = "Review synthetic change",
    generation: int = 1,
    index: int = 42,
) -> dict:
    item = cumulative_candidate(index, "review_request")
    item["source"].update({
        "record_id": f"example.com/acme/widget-{index}",
        "item_id": str(index),
    })
    item["candidate_id"] = candidate_id_for(
        system="gw",
        kind="review_request",
        record_id=item["source"]["record_id"],
        item_id=item["source"]["item_id"],
    )
    item["task"]["text"] = title
    item["lifecycle"].update({
        "generation": generation,
        "changed_at": f"2030-02-{generation:02d}T12:00:00Z",
    })
    item["evidence"] = {
        "document_id": item["source"]["record_id"],
        "locator": f"example.com/acme/widget/pull/{index}",
        "sources": [
            {
                "name": f"pull-request-{index}.md",
                "role": "title",
                "extract": title,
            },
            {
                "name": f"pull-request-{index}-head.txt",
                "role": "diff",
                "extract": head_oid,
            },
        ],
    }
    item["source"]["revision"] = hashlib.sha256(
        json.dumps(item, sort_keys=True).encode("utf-8")
    ).hexdigest()
    return item


def feed(from_cursor: int, *items: dict) -> dict:
    return {
        "schema": "foxhound.task-candidate-feed",
        "schema_version": 1,
        "producer": "gw",
        "stream_id": "primary",
        "from_cursor": from_cursor,
        "to_cursor": from_cursor + len(items),
        "items": [
            {"sequence": from_cursor + offset, "candidate": item}
            for offset, item in enumerate(items, start=1)
        ],
        "emitted_at": "2030-03-01T12:00:00Z",
    }


class TaskForgeCloseBackfillTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.root.chmod(0o700)
        self.database = self.root / "foxhound.sqlite3"
        self.inbox = CandidateInbox(self.database, clock=lambda: NOW)
        migrate_database(self.database)
        self.database.chmod(0o600)
        self.ledger = TaskLedger(self.database, clock=lambda: NOW)

    def activate(self, cursor: int = 0):
        return self.ledger.activate_native_intake(
            producer="gw", stream_id="primary", expected_cursor=cursor
        )

    def intake(self, *, limit: int = 100):
        return self.ledger.accept_native_candidates(
            producer="gw", stream_id="primary", limit=limit
        )

    def _withdraw_review(self, generation: int = 2, index: int = 42):
        withdrawn = review_candidate("a" * 40, generation=generation, index=index)
        withdrawn["lifecycle"]["state"] = "withdrawn"
        withdrawn["source"]["revision"] = hashlib.sha256(
            json.dumps(withdrawn, sort_keys=True).encode("utf-8")
        ).hexdigest()
        self.inbox.import_feed(feed(1, withdrawn))
        return self.intake()

    def test_dry_run_and_apply_flow(self):
        self.activate()
        self.inbox.import_feed(feed(0, review_candidate("a" * 40, index=42)))
        self.intake()

        execution = TaskExecutionService(
            self.database,
            clock=lambda: NOW,
            token_factory=lambda: CLAIM_TOKEN,
        )
        scheduled = execution.schedule(1, expected_task_version=1)
        self.assertIsNotNone(scheduled.version)
        assert scheduled.version is not None
        if scheduled.status == WorkflowStatus.AWAITING_START:
            execution.start_action(
                1, expected_version=scheduled.version, action="start"
            )
        claim = execution.claim_next()
        self.assertIsNotNone(claim)
        wf = execution.get(1)
        self.assertIsNotNone(wf)
        assert wf is not None
        self.assertEqual(wf.status, WorkflowStatus.RUNNING)

        # Withdraw while running -> leaves workflow running and produces reader_conflict
        self._withdraw_review(index=42)

        task = self.ledger.get(1)
        self.assertIsNotNone(task)
        assert task is not None
        self.assertEqual(task.status, TaskStatus.OPEN)

        with closing(sqlite3.connect(self.database)) as connection:
            row = connection.execute(
                "SELECT resolution FROM task_candidate_lifecycle"
            ).fetchone()
            self.assertEqual(row[0], "reader_conflict")

        # 1. While running, dry run lists it
        dry_result = run_forge_close_backfill(database_path=self.database)
        self.assertTrue(dry_result["dry_run"])
        self.assertEqual(dry_result["eligible"], [1])
        self.assertEqual(dry_result["skipped"], [])

        # Apply while running skips it
        apply_running = run_forge_close_backfill(
            database_path=self.database, apply=True
        )
        self.assertFalse(apply_running["dry_run"])
        self.assertEqual(apply_running["eligible"], [1])
        self.assertEqual(
            apply_running["skipped"], [{"task_id": 1, "reason": "running"}]
        )
        # Task remains open
        task_still_open = self.ledger.get(1)
        self.assertIsNotNone(task_still_open)
        assert task_still_open is not None
        self.assertEqual(task_still_open.status, TaskStatus.OPEN)

        # Record result so workflow finishes running and moves to awaiting_review
        assert claim is not None
        execution.record_result(
            ExecutionResultEnvelope(
                result_id="res-1",
                task_id=1,
                task_version=1,
                workflow_version=claim.workflow_version,
                phase=WorkflowPhase.PLAN,
                claim_token=claim.token,
                outcome=ExecutionOutcome.AWAITING_PLAN,
                summary="Synthetic summary",
                work_markdown="Synthetic plan.",
                questions=("Proceed?",),
            )
        )
        wf_after_run = execution.get(1)
        self.assertIsNotNone(wf_after_run)
        assert wf_after_run is not None
        self.assertNotEqual(wf_after_run.status, WorkflowStatus.RUNNING)

        # 2. Dry run again: lists task and changes nothing
        dry_result2 = run_forge_close_backfill(database_path=self.database)
        self.assertTrue(dry_result2["dry_run"])
        self.assertEqual(dry_result2["eligible"], [1])
        task_still_open2 = self.ledger.get(1)
        self.assertIsNotNone(task_still_open2)
        assert task_still_open2 is not None
        self.assertEqual(task_still_open2.status, TaskStatus.OPEN)

        # 3. Apply closes it once workflow is no longer running
        apply_result = run_forge_close_backfill(
            database_path=self.database, apply=True
        )
        self.assertFalse(apply_result["dry_run"])
        self.assertEqual(apply_result["eligible"], [1])
        self.assertEqual(apply_result["skipped"], [])

        task_after = self.ledger.get(1)
        self.assertIsNotNone(task_after)
        assert task_after is not None
        self.assertEqual(task_after.status, TaskStatus.DONE)
        with closing(sqlite3.connect(self.database)) as connection:
            lifecycle = connection.execute(
                "SELECT resolution, task_version FROM task_candidate_lifecycle"
            ).fetchone()
            work_item_state = connection.execute(
                "SELECT state FROM work_items WHERE task_id=1"
            ).fetchone()[0]
        self.assertEqual(
            tuple(lifecycle), ("closed_by_source", task_after.version)
        )
        self.assertEqual(work_item_state, "closed")

        # Running backfill again finds no open tasks
        dry_result3 = run_forge_close_backfill(database_path=self.database)
        self.assertEqual(dry_result3["eligible"], [])

    def test_non_forge_email_reader_conflict_is_never_listed(self):
        self.activate()
        email_active = history_candidate(2, generation=1)
        self.inbox.import_feed(feed(0, email_active))
        self.intake()

        # Let the task be closed first by reader
        self.assertTrue(
            self.ledger.transition(1, expected_version=1, action="done").accepted
        )
        email_withdrawn = history_candidate(2, generation=2)
        email_withdrawn["lifecycle"]["state"] = "withdrawn"
        email_withdrawn["source"]["revision"] = hashlib.sha256(
            json.dumps(email_withdrawn, sort_keys=True).encode("utf-8")
        ).hexdigest()
        self.inbox.import_feed(feed(1, email_withdrawn))
        self.intake()

        # reopen or let it stay in reader_conflict while open
        # Wait, if task is open and email candidate is withdrawn with reader_conflict:
        # e.g., task was edited by reader (version 2) while open, or workflow was queued
        with closing(sqlite3.connect(self.database)) as connection:
            # Let's set task status='open' with reader_conflict on lifecycle
            connection.execute("UPDATE tasks SET status='open' WHERE id=1")
            lifecycle = connection.execute(
                "SELECT state, resolution FROM task_candidate_lifecycle"
            ).fetchone()
        self.assertEqual(tuple(lifecycle), ("withdrawn", "reader_conflict"))

        # Should never be listed because source_kind is 'email'
        res = run_forge_close_backfill(database_path=self.database)
        self.assertEqual(res["eligible"], [])
        self.assertEqual(res["skipped"], [])

    def test_cli_invocation(self):
        # Test CLI entry point main()
        ret = main(["--database", str(self.database)])
        self.assertEqual(ret, 0)


if __name__ == "__main__":
    unittest.main()
