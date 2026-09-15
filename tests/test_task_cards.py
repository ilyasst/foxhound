from __future__ import annotations

import copy
import hashlib
import json
import sqlite3
import tempfile
import unittest
from contextlib import closing
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path

from foxhound import CandidateInbox
from foxhound.card_provenance import (
    MAX_CARD_EXTRACT_CHARS,
    CardSourceEvidence,
)
from foxhound.candidate_inbox import SCHEMA_VERSION
from foxhound.contracts import candidate_id_for, comparable_task_digest
from foxhound.task_cards import (
    CardDisposition,
    CardRefusal,
    CardStatus,
    TaskCardService,
    parse_task_review_callback,
    render_task_review_card,
)
from foxhound.task_ledger import TaskLedger, TaskLedgerError, TaskStatus


NOW = datetime(2030, 3, 1, 12, 0, tzinfo=timezone.utc)
TOKEN = "a" * 43


def _drop_native_intake_schema(connection: sqlite3.Connection) -> None:
    connection.execute("DROP TRIGGER task_owner_events_no_update")
    connection.execute("DROP TRIGGER task_owner_events_no_delete")
    connection.execute("DROP TRIGGER execution_reader_inputs_no_update")
    connection.execute("DROP TRIGGER execution_reader_inputs_no_delete")
    connection.execute("DROP TABLE task_owner_events")
    connection.execute("DROP TABLE execution_reader_inputs")
    connection.execute("DROP TRIGGER native_candidate_intake_events_no_update")
    connection.execute("DROP TRIGGER native_candidate_intake_events_no_delete")
    connection.execute("DROP TRIGGER candidate_feed_items_no_update")
    connection.execute("DROP TRIGGER candidate_feed_items_no_delete")
    connection.execute("DROP TABLE native_candidate_intake_events")
    connection.execute("DROP TABLE native_candidate_intakes")
    connection.execute("DROP TABLE candidate_feed_items")


def _drop_owner_schema(connection: sqlite3.Connection) -> None:
    for column in (
        "owner_provisional",
        "owner_pinned",
        "owner_speaker_registry_id",
        "owner_canonical_speaker_id",
        "owner_speaker_id",
        "owner_kind",
        "owner_ref_version",
    ):
        connection.execute(f"ALTER TABLE tasks DROP COLUMN {column}")


class Clock:
    def __init__(self):
        self.value = NOW

    def __call__(self):
        return self.value

    def advance(self, delta: timedelta):
        self.value += delta


def candidate(index: int) -> dict:
    text = f"Prepare synthetic item {index} < safely"
    owner = f"Person {index}"
    revision = hashlib.sha256(
        json.dumps([text, owner, index]).encode("utf-8")
    ).hexdigest()
    return {
        "schema": "foxhound.task-candidate",
        "schema_version": 7,
        "candidate_id": candidate_id_for(
            system="gw",
            kind="meeting",
            record_id=f"record-{index:03d}",
            item_id=f"action-{index:03d}",
        ),
        "source": {
            "system": "gw",
            "kind": "meeting",
            "record_id": f"record-{index:03d}",
            "item_id": f"action-{index:03d}",
            "revision": revision,
        },
        "task": {
            "text": text,
            "owner": owner,
            "owner_ref": {
                "kind": "person",
                "speaker_id": None,
                "canonical_speaker_id": None,
                "speaker_registry_id": None,
                "pinned": False,
                "provisional": False,
            },
            "due": f"2030-03-{index + 10:02d}",
        },
        "evidence": {
            "document_id": f"record-{index:03d}",
            "locator": f"action-item-{index:03d}",
            "sources": [{
                "name": f"meeting-{index:03d}.md",
                "role": "transcript",
                "extract": "Person A: Please prepare <the synthetic item>.",
            }],
        },
        "lifecycle": {
            "state": "active",
            "generation": 1,
            "changed_at": "2030-02-01T12:00:00Z",
        },
        "created_at": f"2030-01-{index:02d}T12:00:00Z",
    }


def observation(item: dict, legacy_task_id: int) -> dict:
    return {
        "schema": "foxhound.task-shadow-observation",
        "schema_version": 1,
        "candidate": copy.deepcopy(item),
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


class TaskCardTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.database = Path(self.temporary.name) / "foxhound.sqlite3"
        self.clock = Clock()
        inbox = CandidateInbox(self.database, clock=self.clock)
        inbox.initialize()
        items = [candidate(index) for index in range(1, 5)]
        for item in items:
            self.assertTrue(inbox.import_document(item).accepted)
        feed = {
            "schema": "foxhound.task-shadow-observation-feed",
            "schema_version": 1,
            "producer": "gw",
            "stream_id": "synthetic",
            "from_cursor": 0,
            "to_cursor": len(items),
            "items": [
                {
                    "sequence": index,
                    "observation": observation(item, 1000 + index),
                }
                for index, item in enumerate(items, start=1)
            ],
            "emitted_at": "2030-03-01T12:00:00Z",
        }
        self.assertTrue(inbox.import_shadow_feed(feed).accepted)
        self.ledger = TaskLedger(self.database, clock=self.clock)
        self.assertEqual(self.ledger.bootstrap_from_shadow().tasks_created, 4)
        self.cards = TaskCardService(
            self.database,
            clock=self.clock,
            token_factory=lambda: TOKEN,
        )

    def claim_and_deliver(self):
        claim = self.cards.claim_next()
        self.assertIsNotNone(claim)
        delivered = self.cards.complete_delivery(
            claim.card.id,
            expected_version=claim.card.version,
            claim_token=claim.token,
            transport="synthetic",
            delivery_ref=f"message-{claim.card.id}",
        )
        self.assertEqual(delivered.disposition, CardDisposition.APPLIED)
        return claim

    def test_schema_six_migration_is_passive(self):
        with closing(sqlite3.connect(self.database)) as connection:
            _drop_owner_schema(connection)
            _drop_native_intake_schema(connection)
            connection.execute(
                "DROP TRIGGER execution_review_card_events_no_update"
            )
            connection.execute(
                "DROP TRIGGER execution_review_card_events_no_delete"
            )
            connection.execute("DROP INDEX execution_review_cards_one_active")
            connection.execute("DROP TABLE execution_review_card_events")
            connection.execute("DROP TABLE execution_review_cards")
            connection.execute("DROP TRIGGER task_execution_events_no_update")
            connection.execute("DROP TRIGGER task_execution_events_no_delete")
            connection.execute("DROP TRIGGER task_execution_results_no_update")
            connection.execute("DROP TRIGGER task_execution_results_no_delete")
            connection.execute("DROP INDEX task_execution_workflows_ready")
            connection.execute("DROP TABLE task_execution_events")
            connection.execute("DROP TABLE task_execution_results")
            connection.execute("DROP TABLE task_execution_workflows")
            connection.execute("DROP TRIGGER task_review_card_events_no_update")
            connection.execute("DROP TRIGGER task_review_card_events_no_delete")
            connection.execute("DROP TABLE task_review_card_events")
            connection.execute("DROP TABLE task_review_cards")
            connection.execute("PRAGMA user_version = 6")

        CandidateInbox(self.database, clock=self.clock).initialize()

        with closing(sqlite3.connect(self.database)) as connection:
            self.assertEqual(connection.execute(
                "PRAGMA user_version"
            ).fetchone()[0], SCHEMA_VERSION)
            self.assertEqual(connection.execute(
                "SELECT count(*) FROM tasks"
            ).fetchone()[0], 4)
            self.assertEqual(connection.execute(
                "SELECT count(*) FROM task_review_cards"
            ).fetchone()[0], 0)

    def test_explicit_schedule_is_bounded_ordered_and_idempotent(self):
        first = self.cards.schedule(limit=2)
        self.assertEqual(first.disposition, CardDisposition.APPLIED)
        self.assertEqual((first.created, first.cancelled), (2, 0))
        self.assertEqual([card.task_id for card in self.cards.due(limit=20)], [1, 2])

        second = self.cards.schedule(limit=2)
        self.assertEqual(second.created, 2)
        self.assertEqual([card.task_id for card in self.cards.due(limit=20)],
                         [1, 2, 3, 4])
        replay = self.cards.schedule()
        self.assertEqual(replay.disposition, CardDisposition.UNCHANGED)
        self.assertEqual(self.cards.count(), 4)

    def test_stats_are_aggregate_and_from_one_queue_snapshot(self):
        empty = self.cards.stats()
        self.assertEqual(
            (empty.pending, empty.delivering, empty.delivered,
             empty.snoozed, empty.active),
            (0, 0, 0, 0, 0),
        )
        self.cards.schedule()
        first = self.claim_and_deliver()
        self.cards.act(
            first.card.id,
            expected_version=first.card.version,
            action="snooze",
        )
        self.cards.claim_next()

        stats = self.cards.stats()

        self.assertEqual(
            (stats.pending, stats.delivering, stats.delivered,
             stats.snoozed, stats.active),
            (2, 1, 0, 1, 4),
        )

    def test_delivery_claim_render_ack_and_replay_are_fenced(self):
        self.cards.schedule()
        claim = self.cards.claim_next(lease_seconds=60)
        self.assertEqual(claim.card.status, CardStatus.DELIVERING)
        self.assertEqual(claim.card.version, 2)
        body, keyboard = render_task_review_card(claim.card)
        self.assertIn("&lt; safely", body)
        self.assertIn("☑️ <b>Task done?</b>", body)
        self.assertIn("<b>From:</b> Meeting", body)
        self.assertIn("<code>meeting-001.md</code> — Transcript", body)
        self.assertIn(
            "Person A: Please prepare &lt;the synthetic item&gt;.", body
        )
        callbacks = [
            button["callback_data"]
            for row in keyboard["inline_keyboard"] for button in row
        ]
        self.assertEqual(
            [parse_task_review_callback(value)[2] for value in callbacks],
            ["done", "keep_open", "drop", "snooze"],
        )

        no_evidence = replace(claim.card, origin_sources=())
        missing_body, _ = render_task_review_card(no_evidence)
        self.assertIn("Source extract not provided", missing_body)

        # A record identified only by digest is not a source the reader can
        # reach, so neither it nor a warning about it earns a line. The
        # card must not end on the blank separator that block used to sit
        # under, either.
        opaque = replace(
            no_evidence, origin_record="0123456789abcdef", origin_sources=())
        opaque_body, _ = render_task_review_card(opaque)
        self.assertNotIn("0123456789abcdef", opaque_body)
        self.assertNotIn("Source extract not provided", opaque_body)
        self.assertEqual(opaque_body, opaque_body.rstrip())
        self.assertIn("☑️ <b>Task done?</b>", opaque_body)

        bounded = replace(
            claim.card,
            origin_sources=tuple(
                CardSourceEvidence(
                    name=f"source-{index}.txt",
                    role="transcript",
                    extract="<&" * 600,
                )
                for index in range(3)
            ),
        )
        bounded_body, _ = render_task_review_card(bounded)
        self.assertLess(len(bounded_body.encode("utf-8")), 24 * 1024)
        self.assertEqual(bounded_body.count("<blockquote>"), 3)
        # Every source still gets a quote, and no quote gets the card. The
        # contract allows 1,200 characters each and a card may carry three;
        # what the reader needs from an extract is recognition, which the
        # opening gives them.
        self.assertEqual(bounded_body.count("…"), 3)
        for quote in bounded_body.split("<blockquote>")[1:]:
            self.assertLess(
                len(quote.split("</blockquote>")[0]),
                MAX_CARD_EXTRACT_CHARS * 6,
            )

        refused = self.cards.complete_delivery(
            claim.card.id,
            expected_version=claim.card.version,
            claim_token="b" * 43,
            transport="synthetic",
            delivery_ref="message-1",
        )
        self.assertEqual(refused.refusal, CardRefusal.CLAIM_MISMATCH)
        before = self.cards.event_count()
        applied = self.cards.complete_delivery(
            claim.card.id,
            expected_version=claim.card.version,
            claim_token=claim.token,
            transport="synthetic",
            delivery_ref="message-1",
        )
        self.assertEqual(applied.status, CardStatus.DELIVERED)
        replay = self.cards.complete_delivery(
            claim.card.id,
            expected_version=claim.card.version,
            claim_token=claim.token,
            transport="synthetic",
            delivery_ref="message-1",
        )
        self.assertEqual(replay.disposition, CardDisposition.UNCHANGED)
        self.assertEqual(self.cards.event_count(), before + 1)

    def test_actions_are_atomic_and_stale_replays_write_nothing(self):
        self.cards.schedule()
        actions = ("done", "keep_open", "drop", "snooze")
        claims = []
        for action in actions:
            claim = self.claim_and_deliver()
            claims.append(claim)
            before = self.cards.event_count()
            result = self.cards.act(
                claim.card.id,
                expected_version=claim.card.version,
                action=action,
            )
            self.assertEqual(result.disposition, CardDisposition.APPLIED)
            replay = self.cards.act(
                claim.card.id,
                expected_version=claim.card.version,
                action=action,
            )
            self.assertEqual(replay.refusal, CardRefusal.STALE_VERSION)
            self.assertEqual(self.cards.event_count(), before + 1)

        self.assertEqual(self.ledger.get(1).status, TaskStatus.DONE)
        self.assertEqual(self.ledger.get(2).status, TaskStatus.OPEN)
        self.assertEqual(self.ledger.get(3).status, TaskStatus.DROPPED)
        self.assertEqual(self.ledger.get(4).status, TaskStatus.OPEN)
        self.assertEqual(
            self.cards.act(
                claims[3].card.id,
                expected_version=claims[3].card.version,
                action="snooze",
            ).refusal,
            CardRefusal.STALE_VERSION,
        )
        self.assertEqual(self.cards.schedule().created, 0)

        self.clock.advance(timedelta(days=3))
        self.assertEqual([card.task_id for card in self.cards.due()], [4])
        self.clock.advance(timedelta(days=4))
        self.assertEqual(self.cards.schedule().created, 1)
        self.assertEqual([card.task_id for card in self.cards.due()], [4, 2])

    def test_failed_and_expired_delivery_claims_can_be_retried(self):
        self.cards.schedule(limit=1)
        first = self.cards.claim_next(lease_seconds=60)
        failed = self.cards.fail_delivery(
            first.card.id,
            expected_version=first.card.version,
            claim_token=first.token,
        )
        self.assertEqual((failed.status, failed.version), (CardStatus.PENDING, 3))
        self.assertEqual(self.cards.complete_delivery(
            first.card.id,
            expected_version=first.card.version,
            claim_token=first.token,
            transport="synthetic",
            delivery_ref="message-1",
        ).refusal, CardRefusal.STALE_VERSION)

        second = self.cards.claim_next(lease_seconds=60)
        self.assertEqual(second.card.version, 4)
        self.clock.advance(timedelta(seconds=61))
        third = self.cards.claim_next(lease_seconds=60)
        self.assertEqual(third.card.id, second.card.id)
        self.assertEqual(third.card.version, 6)
        self.assertEqual(self.cards.complete_delivery(
            second.card.id,
            expected_version=second.card.version,
            claim_token=second.token,
            transport="synthetic",
            delivery_ref="message-2",
        ).refusal, CardRefusal.STALE_VERSION)

    def test_external_task_change_invalidates_active_card(self):
        self.cards.schedule(limit=1)
        claim = self.claim_and_deliver()
        self.assertTrue(self.ledger.transition(
            claim.card.task_id,
            expected_version=claim.card.task_version,
            action="done",
        ).accepted)
        before = self.cards.event_count()
        result = self.cards.act(
            claim.card.id,
            expected_version=claim.card.version,
            action="drop",
        )
        self.assertEqual(result.refusal, CardRefusal.STALE_VERSION)
        self.assertEqual(self.cards.event_count(), before)
        scheduled = self.cards.schedule()
        self.assertEqual(scheduled.cancelled, 1)
        self.assertEqual(self.ledger.get(claim.card.task_id).status, TaskStatus.DONE)

    def test_invalid_inputs_and_append_only_history_fail_closed(self):
        self.assertEqual(self.cards.schedule(limit=0).refusal,
                         CardRefusal.INVALID_ARGUMENT)
        self.assertEqual(self.cards.act(
            1, expected_version=1, action="invented"
        ).refusal, CardRefusal.INVALID_ACTION)
        self.assertIsNone(parse_task_review_callback("fhc|1|1|invented"))
        with self.assertRaisesRegex(TaskLedgerError, "limit"):
            self.cards.due(limit=0)
        with self.assertRaisesRegex(TaskLedgerError, "lease"):
            self.cards.claim_next(lease_seconds=1)
        self.cards.schedule(limit=1)
        before = self.cards.event_count()
        with self.assertRaises(sqlite3.IntegrityError):
            with closing(sqlite3.connect(self.database)) as connection:
                connection.execute(
                    "UPDATE task_review_card_events SET kind='cancelled'"
                )
        with self.assertRaises(sqlite3.IntegrityError):
            with closing(sqlite3.connect(self.database)) as connection:
                connection.execute("DELETE FROM task_review_card_events")
        self.assertEqual(self.cards.event_count(), before)


if __name__ == "__main__":
    unittest.main()
