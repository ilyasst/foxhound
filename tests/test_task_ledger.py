from __future__ import annotations

import copy
import hashlib
import json
import sqlite3
import tempfile
import unittest
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path

from foxhound import CandidateInbox, InboxError
from foxhound.contracts import candidate_id_for, comparable_task_digest
from foxhound.task_ledger import (
    BootstrapDisposition,
    BootstrapRefusal,
    TaskLedger,
    TaskLedgerError,
    TaskStatus,
    TransitionDisposition,
    TransitionRefusal,
)


NOW = datetime(2030, 3, 1, 12, 0, tzinfo=timezone.utc)


def candidate(
    index: int,
    *,
    text: str = "Prepare the synthetic summary",
    owner: str | None = "Person A",
    due: str | None = None,
) -> dict:
    kind = "meeting" if index % 2 else "email"
    record_id = f"record-{index:03d}"
    item_id = f"action-{index:03d}"
    revision = hashlib.sha256(
        json.dumps([text, owner, due, index]).encode("utf-8")
    ).hexdigest()
    return {
        "schema": "foxhound.task-candidate",
        "schema_version": 2,
        "candidate_id": candidate_id_for(
            system="gw",
            kind=kind,
            record_id=record_id,
            item_id=item_id,
        ),
        "source": {
            "system": "gw",
            "kind": kind,
            "record_id": record_id,
            "item_id": item_id,
            "revision": revision,
        },
        "task": {"text": text, "owner": owner, "due": due},
        "evidence": {
            "document_id": record_id,
            "locator": f"action-item-{index:03d}",
        },
        "created_at": "2030-01-01T12:00:00Z",
    }


def observation(
    item: dict,
    *,
    disposition: str,
    legacy_task_id: int | None = None,
    reason_code: str | None = None,
    divergent: bool = False,
) -> dict:
    legacy = None
    if legacy_task_id is not None:
        digest = comparable_task_digest(
            text=item["task"]["text"],
            project=None,
            owner=item["task"]["owner"],
        )
        legacy = {
            "task_id": legacy_task_id,
            "comparable_digest": "0" * 64 if divergent else digest,
        }
    return {
        "schema": "foxhound.task-shadow-observation",
        "schema_version": 1,
        "candidate": copy.deepcopy(item),
        "disposition": disposition,
        "legacy_task": legacy,
        "reason_code": reason_code,
        "observed_at": "2030-02-01T12:00:00Z",
    }


def shadow_feed(observations: list[dict]) -> dict:
    return {
        "schema": "foxhound.task-shadow-observation-feed",
        "schema_version": 1,
        "producer": "gw",
        "stream_id": "primary",
        "from_cursor": 0,
        "to_cursor": len(observations),
        "items": [
            {"sequence": index, "observation": item}
            for index, item in enumerate(observations, start=1)
        ],
        "emitted_at": "2030-03-01T12:00:00Z",
    }


class TaskLedgerTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.database = Path(self.temporary.name) / "foxhound.sqlite3"
        self.inbox = CandidateInbox(self.database, clock=lambda: NOW)
        self.inbox.initialize()
        self.ledger = TaskLedger(self.database, clock=lambda: NOW)

    def import_candidates(self, *items: dict) -> None:
        for item in items:
            self.assertTrue(self.inbox.import_document(item).accepted)

    def import_observations(self, *items: dict) -> None:
        self.assertTrue(
            self.inbox.import_shadow_feed(shadow_feed(list(items))).accepted
        )

    def test_imports_are_passive_and_bootstrap_preserves_legacy_grouping(self):
        minted = candidate(1, due="2030-03-15")
        folded = candidate(2)
        self.import_candidates(minted, folded)
        self.import_observations(
            observation(minted, disposition="minted", legacy_task_id=9001),
            observation(folded, disposition="folded", legacy_task_id=9001),
        )

        self.assertEqual(self.ledger.count(), 0)
        result = self.ledger.bootstrap_from_shadow()

        self.assertEqual(result.disposition, BootstrapDisposition.APPLIED)
        self.assertEqual((result.tasks_created, result.bindings_created), (1, 2))
        self.assertEqual(self.ledger.count(), 1)
        self.assertEqual(self.ledger.binding_count(), 2)
        task = self.ledger.get(1)
        self.assertIsNotNone(task)
        self.assertNotEqual(task.id, 9001)
        self.assertEqual(task.status, TaskStatus.OPEN)
        self.assertEqual(task.due, "2030-03-15")

        with closing(sqlite3.connect(self.database)) as connection:
            events = connection.execute(
                "SELECT kind,candidate_id FROM task_events ORDER BY sequence"
            ).fetchall()
        self.assertEqual([row[0] for row in events], ["created", "candidate_folded"])
        self.assertEqual(events[0][1], minted["candidate_id"])
        self.assertEqual(events[1][1], folded["candidate_id"])

        replay = self.ledger.bootstrap_from_shadow()
        self.assertEqual(replay.disposition, BootstrapDisposition.UNCHANGED)
        self.assertEqual(replay.bindings_unchanged, 2)

    def test_only_current_agreed_complete_groups_create_tasks(self):
        refused = candidate(1)
        unmapped = candidate(2)
        divergent = candidate(3)
        pending = candidate(4)
        incomplete = candidate(5)
        stale = candidate(6)
        self.import_candidates(
            refused, unmapped, divergent, pending, incomplete, stale
        )
        self.import_observations(
            observation(
                refused,
                disposition="refused",
                reason_code="unaddressable_projection",
            ),
            observation(
                unmapped,
                disposition="unmapped",
                reason_code="legacy_identity_absent",
            ),
            observation(
                divergent,
                disposition="minted",
                legacy_task_id=101,
                divergent=True,
            ),
            observation(
                incomplete,
                disposition="folded",
                legacy_task_id=102,
            ),
            observation(stale, disposition="minted", legacy_task_id=103),
        )
        revised = copy.deepcopy(stale)
        revised["source"]["revision"] = "f" * 64
        revised["task"]["text"] = "Prepare the revised synthetic summary"
        self.assertTrue(self.inbox.import_document(revised).accepted)

        result = self.ledger.bootstrap_from_shadow()

        self.assertEqual(result.disposition, BootstrapDisposition.UNCHANGED)
        self.assertEqual(result.candidates_pending, 2)
        self.assertEqual(result.candidates_refused, 1)
        self.assertEqual(result.candidates_unmapped, 1)
        self.assertEqual(result.candidates_divergent, 1)
        self.assertEqual(result.incomplete_groups, 1)
        self.assertEqual(self.ledger.count(), 0)

    def test_conflicting_group_rolls_back_every_task(self):
        first = candidate(1)
        second = candidate(2)
        self.import_candidates(first, second)
        self.import_observations(
            observation(first, disposition="minted", legacy_task_id=201),
            observation(second, disposition="minted", legacy_task_id=201),
        )

        result = self.ledger.bootstrap_from_shadow()

        self.assertEqual(result.disposition, BootstrapDisposition.REFUSED)
        self.assertEqual(result.refusal, BootstrapRefusal.STATE_CONFLICT)
        self.assertEqual(self.ledger.count(), 0)
        self.assertEqual(self.ledger.binding_count(), 0)

    def test_a_correlated_task_cannot_gain_a_second_minted_candidate(self):
        first = candidate(1)
        self.import_candidates(first)
        self.import_observations(
            observation(first, disposition="minted", legacy_task_id=211)
        )
        self.assertTrue(self.ledger.bootstrap_from_shadow().accepted)

        second = candidate(2)
        self.import_candidates(second)
        next_page = shadow_feed([
            observation(second, disposition="minted", legacy_task_id=211)
        ])
        next_page["from_cursor"] = 1
        next_page["to_cursor"] = 2
        next_page["items"][0]["sequence"] = 2
        self.assertTrue(self.inbox.import_shadow_feed(next_page).accepted)

        result = self.ledger.bootstrap_from_shadow()

        self.assertEqual(result.disposition, BootstrapDisposition.REFUSED)
        self.assertEqual(result.refusal, BootstrapRefusal.STATE_CONFLICT)
        self.assertEqual(self.ledger.count(), 1)
        self.assertEqual(self.ledger.binding_count(), 1)

    def test_transitions_are_version_fenced_and_events_are_append_only(self):
        item = candidate(1)
        self.import_candidates(item)
        self.import_observations(
            observation(item, disposition="minted", legacy_task_id=301)
        )
        self.ledger.bootstrap_from_shadow()

        done = self.ledger.transition(1, expected_version=1, action="done")
        self.assertEqual(done.disposition, TransitionDisposition.APPLIED)
        self.assertEqual((done.status, done.version), (TaskStatus.DONE, 2))
        stale = self.ledger.transition(1, expected_version=1, action="reopen")
        self.assertEqual(stale.refusal, TransitionRefusal.STALE_VERSION)
        reopened = self.ledger.transition(1, expected_version=2, action="reopen")
        self.assertEqual((reopened.status, reopened.version), (TaskStatus.OPEN, 3))
        dropped = self.ledger.transition(1, expected_version=3, action="drop")
        self.assertEqual((dropped.status, dropped.version), (TaskStatus.DROPPED, 4))
        wrong = self.ledger.transition(1, expected_version=4, action="done")
        self.assertEqual(wrong.refusal, TransitionRefusal.INVALID_STATE)
        self.assertEqual(self.ledger.get(1).version, 4)

        with closing(sqlite3.connect(self.database)) as connection:
            count = connection.execute(
                "SELECT COUNT(*) FROM task_events"
            ).fetchone()[0]
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute(
                    "UPDATE task_events SET kind='created' WHERE sequence=1"
                )
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute("DELETE FROM task_events WHERE sequence=1")
        self.assertEqual(count, 4)

    def test_schema_three_migrates_without_losing_inbox_state(self):
        item = candidate(1)
        self.import_candidates(item)
        with closing(sqlite3.connect(self.database)) as connection, connection:
            connection.execute("DROP TRIGGER shadow_import_cycles_no_update")
            connection.execute("DROP TRIGGER shadow_import_cycles_no_delete")
            connection.execute("DROP TABLE shadow_import_cycles")
            connection.execute("DROP TRIGGER task_events_no_update")
            connection.execute("DROP TRIGGER task_events_no_delete")
            connection.execute("DROP TABLE task_events")
            connection.execute("DROP TABLE task_bootstrap_correlations")
            connection.execute("DROP TABLE task_candidate_bindings")
            connection.execute("DROP TABLE tasks")
            connection.execute("PRAGMA user_version = 3")

        self.inbox.initialize()

        self.assertEqual(self.inbox.count(), 1)
        self.assertEqual(self.ledger.count(), 0)
        with closing(sqlite3.connect(self.database)) as connection:
            self.assertEqual(
                connection.execute("PRAGMA user_version").fetchone()[0], 5
            )

    def test_missing_append_only_trigger_is_refused(self):
        with closing(sqlite3.connect(self.database)) as connection, connection:
            connection.execute("DROP TRIGGER task_events_no_update")

        with self.assertRaisesRegex(InboxError, "schema is incomplete"):
            self.inbox.initialize()
        with self.assertRaisesRegex(TaskLedgerError, "schema is incomplete"):
            self.ledger.count()


if __name__ == "__main__":
    unittest.main()
