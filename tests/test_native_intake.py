#!/usr/bin/env python3
"""Synthetic tests for producer-independent ordered candidate intake."""

from __future__ import annotations

import copy
import hashlib
import json
import sqlite3
import tempfile
import unittest
from contextlib import closing, redirect_stderr, redirect_stdout
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path

from foxhound.candidate_inbox import CandidateInbox, SCHEMA_VERSION
from foxhound.contracts import candidate_id_for, comparable_task_digest
from foxhound.native_intake import main
from foxhound.task_cards import CardRefusal, TaskCardService
from foxhound.task_ledger import (
    BootstrapDisposition,
    BootstrapRefusal,
    NativeIntakeDisposition,
    NativeIntakeRefusal,
    TaskLedger,
)


NOW = datetime(2030, 3, 1, 12, 0, tzinfo=timezone.utc)


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


def legacy_candidate(index: int) -> dict:
    """A fictional open task offered only for a bounded cutover."""
    item = candidate(
        index,
        text=f"Prepare migrated synthetic summary {index}",
        owner="Person B",
        due="2030-03-20",
    )
    item["schema_version"] = 1
    item["source"].update({
        "kind": "legacy",
        "record_id": "example-task-ledger",
        "item_id": f"task-{index:03d}",
    })
    item["candidate_id"] = candidate_id_for(
        system="gw",
        kind="legacy",
        record_id=item["source"]["record_id"],
        item_id=item["source"]["item_id"],
    )
    item["task"]["project"] = "Project Alpha"
    item["evidence"] = {
        "document_id": "example-task-ledger",
        "locator": f"task-{index:03d}",
    }
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


def observation(item: dict, *, disposition: str, task_id: int | None) -> dict:
    legacy = None
    if task_id is not None:
        legacy = {
            "task_id": task_id,
            "comparable_digest": comparable_task_digest(
                text=item["task"]["text"],
                project=None,
                owner=item["task"]["owner"],
            ),
        }
    return {
        "schema": "foxhound.task-shadow-observation",
        "schema_version": 1,
        "candidate": copy.deepcopy(item),
        "disposition": disposition,
        "legacy_task": legacy,
        "reason_code": None if task_id is not None else "ambiguous_match",
        "observed_at": "2030-02-01T12:00:00Z",
    }


def shadow_feed(*items: dict) -> dict:
    return {
        "schema": "foxhound.task-shadow-observation-feed",
        "schema_version": 1,
        "producer": "gw",
        "stream_id": "primary",
        "from_cursor": 0,
        "to_cursor": len(items),
        "items": [
            {"sequence": offset, "observation": item}
            for offset, item in enumerate(items, start=1)
        ],
        "emitted_at": "2030-03-01T12:00:00Z",
    }


class NativeCandidateIntakeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.root.chmod(0o700)
        self.database = self.root / "foxhound.sqlite3"
        self.inbox = CandidateInbox(self.database, clock=lambda: NOW)
        self.inbox.initialize()
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

    def test_activation_is_exact_idempotent_and_requires_reconciled_prefix(self):
        item = candidate(1)
        self.assertTrue(self.inbox.import_feed(feed(0, item)).accepted)

        refused = self.activate(1)
        self.assertEqual(refused.disposition, NativeIntakeDisposition.REFUSED)
        self.assertEqual(refused.refusal, NativeIntakeRefusal.UNRECONCILED_PREFIX)

        self.assertTrue(
            self.inbox.import_shadow_feed(
                shadow_feed(observation(item, disposition="refused", task_id=None))
            ).accepted
        )
        activated = self.activate(1)
        self.assertEqual(activated.disposition, NativeIntakeDisposition.APPLIED)
        self.assertEqual(
            self.activate(1).disposition, NativeIntakeDisposition.UNCHANGED
        )
        mismatch = self.activate(0)
        self.assertEqual(mismatch.refusal, NativeIntakeRefusal.CURSOR_MISMATCH)

    def test_activation_accepts_a_historically_bound_prefix(self):
        item = candidate(1)
        self.inbox.import_feed(feed(0, item))
        self.inbox.import_shadow_feed(
            shadow_feed(observation(item, disposition="minted", task_id=1001))
        )
        self.assertEqual(self.ledger.bootstrap_from_shadow().tasks_created, 1)

        activated = self.activate(1)

        self.assertEqual(activated.disposition, NativeIntakeDisposition.APPLIED)
        self.assertEqual(activated.activation_cursor, 1)

    def test_bounded_ordered_intake_creates_once_and_replays_without_writes(self):
        self.assertEqual(self.activate().disposition, NativeIntakeDisposition.APPLIED)
        first = candidate(1)
        second = candidate(2)
        self.assertTrue(self.inbox.import_feed(feed(0, first, second)).accepted)

        one = self.intake(limit=1)
        self.assertEqual(
            (one.tasks_created, one.previous_cursor, one.current_cursor, one.remaining),
            (1, 0, 1, 1),
        )
        two = self.intake(limit=1)
        self.assertEqual(
            (two.tasks_created, two.previous_cursor, two.current_cursor, two.remaining),
            (1, 1, 2, 0),
        )
        before = self._state()
        replay = self.intake()
        self.assertEqual(replay.disposition, NativeIntakeDisposition.UNCHANGED)
        self.assertEqual(self._state(), before)
        self.assertEqual((self.ledger.count(), self.ledger.binding_count()), (2, 2))

    def test_legacy_candidate_uses_ordinary_exactly_once_intake(self):
        self.activate()
        item = legacy_candidate(17)
        self.assertTrue(self.inbox.import_feed(feed(0, item)).accepted)

        applied = self.intake()

        self.assertEqual(applied.tasks_created, 1)
        task = self.ledger.get(1)
        self.assertEqual(
            (task.text, task.owner, task.due),
            (
                "Prepare migrated synthetic summary 17",
                "Person B",
                "2030-03-20",
            ),
        )
        origin = self.ledger.origin(1)
        self.assertEqual(
            (origin.kind, origin.record_id, origin.item_id),
            ("legacy", "example-task-ledger", "task-017"),
        )
        self.assertEqual(
            self.intake().disposition, NativeIntakeDisposition.UNCHANGED
        )
        self.assertEqual(self.ledger.count(), 1)

    def test_exact_historically_bound_candidate_advances_as_unchanged(self):
        item = candidate(1)
        legacy_feed = feed(0, item)
        legacy_feed["stream_id"] = "legacy-shadow"
        self.assertTrue(self.inbox.import_feed(legacy_feed).accepted)
        self.inbox.import_shadow_feed(
            shadow_feed(observation(item, disposition="minted", task_id=1001))
        )
        self.assertEqual(self.ledger.bootstrap_from_shadow().tasks_created, 1)
        self.assertEqual(self.activate().disposition, NativeIntakeDisposition.APPLIED)
        self.assertTrue(self.inbox.import_feed(feed(0, item)).accepted)

        result = self.intake()

        self.assertEqual(result.disposition, NativeIntakeDisposition.APPLIED)
        self.assertEqual(
            (
                result.tasks_created,
                result.tasks_revised,
                result.candidates_unchanged,
                result.previous_cursor,
                result.current_cursor,
            ),
            (0, 0, 1, 0, 1),
        )
        self.assertEqual((self.ledger.count(), self.ledger.binding_count()), (1, 1))

    def test_historically_bound_nonaccepted_candidate_fails_closed(self):
        item = candidate(1)
        legacy_feed = feed(0, item)
        legacy_feed["stream_id"] = "legacy-shadow"
        self.assertTrue(self.inbox.import_feed(legacy_feed).accepted)
        self.inbox.import_shadow_feed(
            shadow_feed(observation(item, disposition="minted", task_id=1001))
        )
        self.assertEqual(self.ledger.bootstrap_from_shadow().tasks_created, 1)
        with closing(sqlite3.connect(self.database)) as connection, connection:
            connection.execute(
                "UPDATE task_candidate_bindings SET relation='folded'"
            )
        self.assertEqual(self.activate().disposition, NativeIntakeDisposition.APPLIED)
        self.assertTrue(self.inbox.import_feed(feed(0, item)).accepted)

        result = self.intake()

        self.assertEqual(result.refusal, NativeIntakeRefusal.STATE_CONFLICT)
        self.assertEqual(self._intake_cursor(), 0)

    def test_revision_updates_only_the_accepted_open_task_and_appends_event(self):
        self.activate()
        initial = candidate(1)
        self.inbox.import_feed(feed(0, initial))
        self.intake()
        revised = candidate(
            1,
            text="Prepare the revised synthetic summary",
            owner="Person B",
            due="2030-03-20",
        )
        self.inbox.import_feed(feed(1, revised))

        result = self.intake()

        self.assertEqual(result.tasks_revised, 1)
        task = self.ledger.get(1)
        self.assertEqual(
            (task.text, task.owner, task.due, task.version),
            (
                "Prepare the revised synthetic summary",
                "Person B",
                "2030-03-20",
                2,
            ),
        )
        with closing(sqlite3.connect(self.database)) as connection:
            binding = connection.execute(
                "SELECT source_revision FROM task_candidate_bindings"
            ).fetchone()[0]
            event = connection.execute(
                "SELECT kind,task_version,source_revision FROM task_events "
                "ORDER BY sequence DESC LIMIT 1"
            ).fetchone()
        self.assertEqual(binding, revised["source"]["revision"])
        self.assertEqual(event, ("candidate_revised", 2, binding))

    def test_revision_invalidates_an_existing_task_card(self):
        self.activate()
        initial = candidate(1)
        self.inbox.import_feed(feed(0, initial))
        self.intake()
        cards = TaskCardService(
            self.database, clock=lambda: NOW, token_factory=lambda: "a" * 43
        )
        self.assertEqual(cards.schedule().created, 1)
        claim = cards.claim_next()
        self.assertIsNotNone(claim)
        delivered = cards.complete_delivery(
            claim.card.id,
            expected_version=claim.card.version,
            claim_token=claim.token,
            transport="synthetic",
            delivery_ref="message-1",
        )
        revised = candidate(1, text="Prepare a newer synthetic summary")
        self.inbox.import_feed(feed(1, revised))
        self.assertEqual(self.intake().tasks_revised, 1)

        stale = cards.act(
            claim.card.id,
            expected_version=delivered.version,
            action="done",
        )

        self.assertEqual(stale.refusal, CardRefusal.STALE_VERSION)
        self.assertEqual(self.ledger.get(1).status.value, "open")

    def test_conflict_rolls_back_complete_pass_and_keeps_cursor(self):
        self.activate()
        first = candidate(1)
        second = candidate(2)
        self.inbox.import_feed(feed(0, first, second))
        self.inbox.import_shadow_feed(
            shadow_feed(observation(second, disposition="refused", task_id=None))
        )

        result = self.intake()

        self.assertEqual(result.refusal, NativeIntakeRefusal.STATE_CONFLICT)
        self.assertEqual((self.ledger.count(), self.ledger.binding_count()), (0, 0))
        self.assertEqual(self._intake_cursor(), 0)

    def test_revision_with_a_post_boundary_producer_decision_fails_closed(self):
        self.activate()
        initial = candidate(1)
        self.inbox.import_feed(feed(0, initial))
        self.intake()
        revised = candidate(1, text="Prepare a disputed synthetic revision")
        self.inbox.import_feed(feed(1, revised))
        self.inbox.import_shadow_feed(
            shadow_feed(observation(revised, disposition="minted", task_id=1001))
        )

        result = self.intake()

        self.assertEqual(result.refusal, NativeIntakeRefusal.STATE_CONFLICT)
        self.assertEqual(self.ledger.get(1).version, 1)
        self.assertEqual(self._intake_cursor(), 1)

    def test_terminal_revision_fails_closed(self):
        self.activate()
        first = candidate(1)
        self.inbox.import_feed(feed(0, first))
        self.intake()
        self.ledger.transition(1, expected_version=1, action="done")
        terminal_revision = candidate(1, text="Revise a closed synthetic task")
        self.inbox.import_feed(feed(1, terminal_revision))
        self.assertEqual(self.intake().refusal, NativeIntakeRefusal.STATE_CONFLICT)
        self.assertEqual(self._intake_cursor(), 1)

    def test_folded_revision_fails_closed(self):
        self.activate()
        first = candidate(1)
        self.inbox.import_feed(feed(0, first))
        self.intake()
        other = candidate(2)
        with closing(sqlite3.connect(self.database)) as connection, connection:
            connection.execute(
                "INSERT INTO candidate_revision_history("
                "candidate_id,source_revision,payload_json,created_at,imported_at) "
                "VALUES(?,?,?,?,?)",
                (
                    other["candidate_id"],
                    other["source"]["revision"],
                    json.dumps(other, sort_keys=True, separators=(",", ":")),
                    other["created_at"],
                    NOW.isoformat(),
                ),
            )
            connection.execute(
                "INSERT INTO candidate_inbox("
                "candidate_id,source_system,source_kind,source_record_id,"
                "source_item_id,source_revision,payload_json,created_at,"
                "first_imported_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
                (
                    other["candidate_id"], "gw", "meeting", "record-002",
                    "action-002", other["source"]["revision"],
                    json.dumps(other, sort_keys=True, separators=(",", ":")),
                    other["created_at"], NOW.isoformat(), NOW.isoformat(),
                ),
            )
            connection.execute(
                "INSERT INTO task_candidate_bindings("
                "candidate_id,source_revision,task_id,relation,decided_at) "
                "VALUES(?,?,1,'folded',?)",
                (other["candidate_id"], other["source"]["revision"], NOW.isoformat()),
            )
        folded_revision = candidate(2, text="Revise a folded synthetic task")
        self.inbox.import_feed(feed(1, folded_revision))
        self.assertEqual(self.intake().refusal, NativeIntakeRefusal.STATE_CONFLICT)
        self.assertEqual(self._intake_cursor(), 1)

    def test_missing_provenance_and_invalid_arguments_fail_closed(self):
        self.activate()
        with closing(sqlite3.connect(self.database)) as connection, connection:
            connection.execute(
                "INSERT INTO candidate_feed_cursors("
                "producer,stream_id,cursor,updated_at) VALUES('gw','primary',1,?) "
                "ON CONFLICT(producer,stream_id) DO UPDATE SET cursor=1",
                (NOW.isoformat(),),
            )
        self.assertEqual(self.intake().refusal, NativeIntakeRefusal.STATE_CONFLICT)
        self.assertEqual(self._intake_cursor(), 0)
        invalid = self.ledger.accept_native_candidates(
            producer="gw", stream_id="primary", limit=True
        )
        self.assertEqual(invalid.refusal, NativeIntakeRefusal.INVALID_ARGUMENT)

    def test_provenance_and_intake_events_are_append_only(self):
        self.activate()
        self.inbox.import_feed(feed(0, candidate(1)))
        self.intake()
        with closing(sqlite3.connect(self.database)) as connection:
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute(
                    "UPDATE candidate_feed_items SET sequence=2 WHERE sequence=1"
                )
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute(
                    "DELETE FROM native_candidate_intake_events WHERE sequence=1"
                )
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute(
                    "UPDATE native_candidate_intakes SET activation_cursor=1"
                )
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute("DELETE FROM native_candidate_intakes")

    def test_activation_permanently_disables_legacy_bootstrap(self):
        self.activate()
        resolver_called = False

        def resolver(*_args, **_kwargs):
            nonlocal resolver_called
            resolver_called = True
            raise AssertionError("legacy resolver must not be called")

        result = self.ledger.bootstrap_from_shadow(owner_resolver=resolver)

        self.assertEqual(result.disposition, BootstrapDisposition.REFUSED)
        self.assertEqual(result.refusal, BootstrapRefusal.INVALID_STATE)
        self.assertFalse(resolver_called)

    def test_cli_is_content_free_and_unsafe_state_is_refused(self):
        stdout = StringIO()
        with redirect_stdout(stdout):
            self.assertEqual(main([
                "activate", "--database", str(self.database),
                "--stream-id", "primary", "--expected-cursor", "0",
            ]), 0)
        activated = json.loads(stdout.getvalue())
        self.assertEqual(activated["disposition"], "applied")

        private_text = "Prepare a private-looking synthetic task"
        self.inbox.import_feed(feed(0, candidate(1, text=private_text)))
        stdout = StringIO()
        with redirect_stdout(stdout):
            self.assertEqual(main([
                "run", "--database", str(self.database),
                "--stream-id", "primary", "--limit", "1",
            ]), 0)
        self.assertNotIn(private_text, stdout.getvalue())
        self.assertEqual(json.loads(stdout.getvalue())["counts"]["tasks_created"], 1)

        alias = self.root / "alias.sqlite3"
        alias.symlink_to(self.database)
        stderr = StringIO()
        with redirect_stderr(stderr):
            self.assertEqual(main([
                "run", "--database", str(alias), "--stream-id", "primary",
            ]), 78)
        self.assertEqual(
            stderr.getvalue().strip(),
            "foxhound native intake: configuration unavailable",
        )

    def _intake_cursor(self) -> int:
        with closing(sqlite3.connect(self.database)) as connection:
            return int(connection.execute(
                "SELECT cursor FROM native_candidate_intakes"
            ).fetchone()[0])

    def _state(self) -> tuple:
        with closing(sqlite3.connect(self.database)) as connection:
            return (
                tuple(connection.execute(
                    "SELECT id,status,text,owner,due,version FROM tasks ORDER BY id"
                )),
                tuple(connection.execute(
                    "SELECT candidate_id,source_revision,task_id,relation "
                    "FROM task_candidate_bindings ORDER BY candidate_id"
                )),
                tuple(connection.execute(
                    "SELECT kind,from_cursor,to_cursor,tasks_created,tasks_revised,"
                    "candidates_unchanged FROM native_candidate_intake_events "
                    "ORDER BY sequence"
                )),
                self._intake_cursor(),
            )


if __name__ == "__main__":
    unittest.main()
