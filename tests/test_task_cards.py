from __future__ import annotations

from foxhound import migrate_database

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
    OPEN_REVIEW_INTERVAL,
    ClaimAtCeiling,
    CardDisposition,
    CardRefusal,
    CardStatus,
    TaskCardService,
    parse_task_review_callback,
    render_task_review_card,
)
from foxhound.task_ledger import TaskLedger, TaskLedgerError, TaskStatus
from review_card_fixture import raise_review_cards


NOW = datetime(2030, 3, 1, 12, 0, tzinfo=timezone.utc)
TOKEN = "a" * 43
# Synthetic resolved consumer identities (ADR 0036 decision 1): the digest a
# server would compute from an authenticating bearer token, not a token
# itself. Two distinct values let tests prove a second claim by a different
# consumer is independent of what a card held before.
CONSUMER_A = hashlib.sha256(b"synthetic-consumer-a").hexdigest()
CONSUMER_B = hashlib.sha256(b"synthetic-consumer-b").hexdigest()


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
    def _raise_review_cards(self, *, limit: int = 100) -> int:
        return raise_review_cards(self.database, self.clock(), limit=limit)

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.database = Path(self.temporary.name) / "foxhound.sqlite3"
        self.clock = Clock()
        inbox = CandidateInbox(self.database, clock=self.clock)
        migrate_database(inbox.database_path)
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

    def _consumer_digest(self, card_id: int):
        """Read the raw ``consumer_digest`` column directly -- it is
        content-free server-side bookkeeping (ADR 0036 decision 2), not
        something any service method hands back to a caller.
        """
        with closing(sqlite3.connect(self.database)) as connection:
            row = connection.execute(
                "SELECT consumer_digest FROM task_review_cards WHERE id=?",
                (card_id,),
            ).fetchone()
        return row[0]

    def claim_and_deliver(self, *, consumer_digest=CONSUMER_A):
        claim = self.cards.claim_next(consumer_digest=consumer_digest)
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

    def revise_source_without_task_change(self, task_id: int) -> str:
        """Model an evidence-only #254 refresh already applied by the ledger."""
        with closing(sqlite3.connect(self.database)) as connection:
            row = connection.execute(
                "SELECT candidate_id,source_revision FROM task_candidate_bindings "
                "WHERE task_id=? AND relation='accepted'",
                (task_id,),
            ).fetchone()
            self.assertIsNotNone(row)
            revised = hashlib.sha256(
                (str(row[1]) + "-synthetic-source-update").encode("utf-8")
            ).hexdigest()
            payload = connection.execute(
                "SELECT payload_json FROM candidate_revision_history "
                "WHERE candidate_id=? AND source_revision=?",
                row,
            ).fetchone()[0]
            document = json.loads(payload)
            document["source"]["revision"] = revised
            document["evidence"]["sources"] = [{
                "name": "latest-update.md",
                "role": "transcript",
                "extract": "Synthetic source update after the first review.",
            }]
            payload = json.dumps(document, sort_keys=True, separators=(",", ":"))
            connection.execute(
                "INSERT INTO candidate_revision_history("
                "candidate_id,source_revision,payload_json,created_at,imported_at) "
                "VALUES(?,?,?,?,?)",
                (row[0], revised, payload,
                 "2030-03-02T12:00:00+00:00", "2030-03-02T12:00:00+00:00"),
            )
            connection.execute(
                "UPDATE candidate_inbox SET source_revision=?,payload_json=? "
                "WHERE candidate_id=?",
                (revised, payload, row[0]),
            )
            connection.execute(
                "UPDATE task_candidate_bindings SET source_revision=? "
                "WHERE candidate_id=?",
                (revised, row[0]),
            )
            connection.commit()
        return revised

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

        migrate_database(self.database)

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

    def test_schema_twenty_six_snapshots_current_source_revisions(self):
        self._raise_review_cards(limit=1)
        with closing(sqlite3.connect(self.database)) as connection:
            expected = connection.execute(
                "SELECT b.source_revision FROM task_candidate_bindings AS b "
                "WHERE b.task_id=1 AND b.relation='accepted'"
            ).fetchone()[0]
            connection.execute(
                "ALTER TABLE task_review_cards DROP COLUMN source_revision"
            )
            connection.execute("PRAGMA user_version = 25")
            connection.commit()

        migrate_database(self.database)

        with closing(sqlite3.connect(self.database)) as connection:
            self.assertEqual(
                connection.execute("PRAGMA user_version").fetchone()[0],
                SCHEMA_VERSION,
            )
            self.assertEqual(
                connection.execute(
                    "SELECT source_revision FROM task_review_cards"
                ).fetchall(),
                [(expected,)],
            )

    def set_workflow(self, task_id: int, status: str, *, phase: str = "plan",
                     completed_at: str | None = None) -> None:
        """Model one execution workflow row without running a workflow.

        The card surface only ever reads this table, so a row is enough to
        state the case: execution is holding this task, or has finished with
        it at a given moment.
        """
        stamp = self.clock().isoformat(timespec="seconds")
        # The table ties each status to its companion columns with CHECK
        # constraints, so a synthetic row has to be as consistent as a real
        # one: a running workflow holds a claim, a snoozed one has a wake
        # time, a parked one a parked stamp, a finished one an end stamp.
        claim = "c" * 64 if status == "running" else None
        claimed = stamp if status == "running" else None
        with closing(sqlite3.connect(self.database)) as connection:
            connection.execute(
                "INSERT INTO task_execution_workflows("
                "task_id,task_version,status,phase,version,due_at,"
                "claim_token_digest,claimed_at,claim_heartbeat_at,"
                "claim_expires_at,parked_at,created_at,updated_at,"
                "completed_at) VALUES(?,1,?,?,1,?,?,?,?,?,?,?,?,?) "
                "ON CONFLICT(task_id) DO UPDATE SET status=excluded.status,"
                "phase=excluded.phase,due_at=excluded.due_at,"
                "claim_token_digest=excluded.claim_token_digest,"
                "claimed_at=excluded.claimed_at,"
                "claim_heartbeat_at=excluded.claim_heartbeat_at,"
                "claim_expires_at=excluded.claim_expires_at,"
                "parked_at=excluded.parked_at,updated_at=excluded.updated_at,"
                "completed_at=excluded.completed_at",
                (
                    task_id, status, phase,
                    stamp if status == "snoozed" else None,
                    claim, claimed, claimed, claimed,
                    stamp if status == "parked" else None,
                    stamp, stamp, completed_at,
                ),
            )
            connection.commit()

    def carded_task_ids(self) -> list[int]:
        with closing(sqlite3.connect(self.database)) as connection:
            return [
                row[0] for row in connection.execute(
                    "SELECT task_id FROM task_review_cards WHERE status IN "
                    "('pending','delivering','delivered','snoozed') "
                    "ORDER BY task_id"
                )
            ]

    def test_a_scheduled_card_is_retracted_when_a_workflow_takes_the_task(self):
        """The window between scheduling and answering is not safe either.

        A card already on screen is withdrawn through the ordinary stale
        path, so the completion question it carried goes back to the queue
        rather than down with the card.
        """
        self._raise_review_cards()
        self.assertIn(1, self.carded_task_ids())

        self.set_workflow(1, "queued")
        result = self.cards.schedule()

        self.assertEqual(result.cancelled, 1)
        self.assertNotIn(1, self.carded_task_ids())
        with closing(sqlite3.connect(self.database)) as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT status FROM task_review_cards WHERE task_id=1"
                ).fetchone()[0],
                "cancelled",
            )

    def test_stats_are_aggregate_and_from_one_queue_snapshot(self):
        empty = self.cards.stats(consumer_digest=CONSUMER_A)
        self.assertEqual(
            (empty.pending, empty.delivering, empty.delivered,
             empty.snoozed, empty.elsewhere, empty.active),
            (0, 0, 0, 0, 0, 0),
        )
        self._raise_review_cards()
        first = self.claim_and_deliver()
        self.cards.act(
            first.card.id,
            expected_version=first.card.version,
            action="snooze",
        )
        self.cards.claim_next(consumer_digest=CONSUMER_A)

        stats = self.cards.stats(consumer_digest=CONSUMER_A)

        self.assertEqual(
            (stats.pending, stats.delivering, stats.delivered,
             stats.snoozed, stats.elsewhere, stats.active),
            (2, 1, 0, 1, 0, 4),
        )

    def test_a_dead_lease_counts_as_pending_rather_than_on_screen(self):
        """A consumer must not be told a surface is full of abandoned rows.

        The count feeds `keep - delivering - delivered`, and a consumer
        stops before claiming when that reaches zero. Since the reaper runs
        inside `claim_next`, counting dead leases as `delivering` lets the
        queue wedge with every card waiting and nothing on screen.
        """
        self._raise_review_cards()
        held = self.cards.claim_next(consumer_digest=CONSUMER_A)
        stale = self.cards.claim_next(consumer_digest=CONSUMER_B)
        self.assertIsNotNone(held)
        self.assertIsNotNone(stale)

        live = self.cards.stats(consumer_digest=CONSUMER_A)
        self.assertEqual(
            (live.pending, live.delivering, live.elsewhere, live.active),
            (2, 1, 1, 4),
        )

        self.clock.advance(timedelta(seconds=61))

        expired = self.cards.stats(consumer_digest=CONSUMER_A)
        # Both dead leases are pending again, whoever held them.
        self.assertEqual(
            (expired.pending, expired.delivering, expired.delivered,
             expired.snoozed, expired.elsewhere, expired.active),
            (4, 0, 0, 0, 0, 4),
        )
        # The invariant every consumer checks the response against.
        self.assertEqual(
            expired.active,
            expired.pending + expired.delivering + expired.delivered
            + expired.snoozed + expired.elsewhere,
        )
        # And the promise is real: the reaper agrees with the count, so a
        # consumer that acts on it gets a card rather than an empty claim.
        revived = self.cards.claim_next(consumer_digest=CONSUMER_A)
        self.assertIsNotNone(revived)
        self.assertEqual(
            self.cards.stats_global().pending
            + self.cards.stats_global().delivering,
            4,
        )

    def test_a_full_surface_of_dead_leases_still_leaves_room_to_claim(self):
        """The deadlock itself: depth 2, two dead leases, nothing on screen."""
        self._raise_review_cards()
        for _ in range(2):
            self.assertIsNotNone(
                self.cards.claim_next(consumer_digest=CONSUMER_A))
        keep = 2

        before = self.cards.stats(consumer_digest=CONSUMER_A)
        self.assertEqual(keep - before.delivering - before.delivered, 0)

        self.clock.advance(timedelta(seconds=61))

        after = self.cards.stats(consumer_digest=CONSUMER_A)
        self.assertGreater(keep - after.delivering - after.delivered, 0)

    def test_claim_ceiling_is_distinct_and_scoped_to_role_and_consumer(self):
        self._raise_review_cards()
        first = self.cards.claim_next(
            consumer_digest=CONSUMER_A, consumer_role="queue_view"
        )
        second = self.cards.claim_next(
            consumer_digest=CONSUMER_A, consumer_role="queue_view"
        )
        self.assertIsNotNone(first)
        self.assertIsNotNone(second)
        at_ceiling = self.cards.claim_next(
            consumer_digest=CONSUMER_A, consumer_role="queue_view"
        )
        self.assertIsInstance(at_ceiling, ClaimAtCeiling)
        self.assertEqual((at_ceiling.held_count, at_ceiling.ceiling), (2, 2))
        # A different consumer and a caller under its ceiling remain eligible.
        other = self.cards.claim_next(
            consumer_digest=CONSUMER_B, consumer_role="queue_view"
        )
        self.assertIsNotNone(other)

    def test_stats_scope_claimed_cards_and_hide_legacy_consumer_rows(self):
        self._raise_review_cards()
        mine = self.claim_and_deliver(consumer_digest=CONSUMER_A)
        other = self.cards.claim_next(consumer_digest=CONSUMER_B)
        self.assertIsNotNone(other)

        a = self.cards.stats(consumer_digest=CONSUMER_A)
        b = self.cards.stats(consumer_digest=CONSUMER_B)
        self.assertEqual(
            (a.pending, a.snoozed, a.delivering, a.delivered,
             a.elsewhere, a.active),
            (2, 0, 0, 1, 1, 4),
        )
        self.assertEqual(
            (b.pending, b.snoozed, b.delivering, b.delivered,
             b.elsewhere, b.active),
            (2, 0, 1, 0, 1, 4),
        )

        # A row left by a pre-migration binary has no attributable owner.
        with closing(sqlite3.connect(self.database)) as connection:
            connection.execute(
                "UPDATE task_review_cards SET consumer_digest=NULL WHERE id=?",
                (mine.card.id,),
            )
            connection.commit()
        a = self.cards.stats(consumer_digest=CONSUMER_A)
        self.assertEqual((a.delivered, a.elsewhere, a.active), (0, 1, 4))

    def test_delivery_claim_render_ack_and_replay_are_fenced(self):
        self._raise_review_cards()
        claim = self.cards.claim_next(
            lease_seconds=60, consumer_digest=CONSUMER_A
        )
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

    def test_delivered_card_can_be_repaired_by_local_operator(self):
        self._raise_review_cards(limit=1)
        claim = self.claim_and_deliver()
        before_events = self.cards.event_count()

        repaired = self.cards.retry_delivery(
            claim.card.id, expected_version=claim.card.version
        )

        self.assertEqual(repaired.disposition, CardDisposition.APPLIED)
        self.assertEqual(
            (repaired.status, repaired.version),
            (CardStatus.PENDING, claim.card.version + 1),
        )
        with closing(sqlite3.connect(self.database)) as connection:
            row = connection.execute(
                "SELECT status,version,claim_token_digest,claim_expires_at,"
                "consumer_digest,transport,delivery_ref,delivered_at "
                "FROM task_review_cards WHERE id=?",
                (claim.card.id,),
            ).fetchone()
            event = connection.execute(
                "SELECT kind,card_version FROM task_review_card_events "
                "WHERE card_id=? ORDER BY sequence DESC LIMIT 1",
                (claim.card.id,),
            ).fetchone()
        self.assertEqual(row, (
            "pending", claim.card.version + 1, None, None, None, None, None, None
        ))
        self.assertEqual(event, ("delivery_failed", claim.card.version + 1))
        self.assertEqual(self.cards.event_count(), before_events + 1)

    def test_unanswered_delivered_card_is_represented_after_one_hour(self):
        self._raise_review_cards(limit=1)
        claim = self.claim_and_deliver()

        self.clock.advance(timedelta(hours=1) - timedelta(seconds=1))
        self.assertEqual(self.cards.requeue_unanswered().requeued, 0)
        self.clock.advance(timedelta(seconds=1))
        self.assertEqual(self.cards.requeue_unanswered().requeued, 1)

        stale = self.cards.act(
            claim.card.id, expected_version=claim.card.version, action="done"
        )
        self.assertEqual(stale.refusal, CardRefusal.STALE_VERSION)
        replacement = self.cards.claim_next(consumer_digest=CONSUMER_A)
        self.assertEqual(replacement.card.id, claim.card.id)
        self.assertGreater(replacement.card.version, claim.card.version)
        with closing(sqlite3.connect(self.database)) as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT status,version FROM tasks WHERE id=?",
                    (claim.card.task_id,),
                ).fetchone(),
                ("open", claim.card.task_version),
            )

    def test_represent_only_requeues_current_delivered_cards(self):
        self._raise_review_cards(limit=2)
        first = self.claim_and_deliver()
        self.claim_and_deliver()
        self.cards.act(
            first.card.id, expected_version=first.card.version, action="snooze"
        )

        self.clock.advance(timedelta(hours=1))
        self.assertEqual(self.cards.requeue_unanswered().requeued, 1)
        self.assertEqual(
            self.cards.stats(consumer_digest=CONSUMER_A).pending,
            1,
        )

    def test_delivered_card_repair_refuses_stale_or_noncurrent_cards(self):
        self._raise_review_cards(limit=1)
        claim = self.claim_and_deliver()
        before_events = self.cards.event_count()
        stale = self.cards.retry_delivery(
            claim.card.id, expected_version=claim.card.version - 1
        )
        self.assertEqual(stale.refusal, CardRefusal.STALE_VERSION)
        self.assertEqual(self.cards.event_count(), before_events)
        with closing(sqlite3.connect(self.database)) as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT status,version FROM task_review_cards WHERE id=?",
                    (claim.card.id,),
                ).fetchone(),
                ("delivered", claim.card.version),
            )

        self.cards.act(
            claim.card.id, expected_version=claim.card.version, action="snooze"
        )
        before_events = self.cards.event_count()
        not_delivered = self.cards.retry_delivery(
            claim.card.id, expected_version=claim.card.version + 1
        )
        self.assertEqual(not_delivered.refusal, CardRefusal.INVALID_STATE)
        self.assertEqual(self.cards.event_count(), before_events)

    def test_actions_are_atomic_and_stale_replays_write_nothing(self):
        self._raise_review_cards()
        actions = ("done", "keep_open", "drop", "snooze")
        claims = []
        for action in actions:
            claim = self.claim_and_deliver()
            claims.append(claim)
            self.assertEqual(self._consumer_digest(claim.card.id), CONSUMER_A)
            before = self.cards.event_count()
            result = self.cards.act(
                claim.card.id,
                expected_version=claim.card.version,
                action=action,
            )
            self.assertEqual(result.disposition, CardDisposition.APPLIED)
            # Every act() outcome here -- "done"/"drop"/"keep_open" leave
            # the card `resolved`, "snooze" leaves it `snoozed` -- clears
            # the consumer digest independently of which outcome it was
            # (ADR 0036 invariant 6). Checked per-action, not just once,
            # since each is a distinct code path in `act()`.
            self.assertIsNone(
                self._consumer_digest(claim.card.id),
                f"consumer digest survived action={action!r}",
            )
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
        # A snoozed card still comes back -- that is a card-level promise the
        # reader made. A `keep_open` one does not: task 2 used to return a
        # review interval later, and that periodic rhythm is what was retired.
        self.clock.advance(timedelta(days=4))
        self.assertEqual(self.cards.schedule().created, 0)
        self.assertEqual([card.task_id for card in self.cards.due()], [4])

    def test_failed_and_expired_delivery_claims_can_be_retried(self):
        self._raise_review_cards(limit=1)
        first = self.cards.claim_next(
            lease_seconds=60, consumer_digest=CONSUMER_A
        )
        self.assertEqual(
            self._consumer_digest(first.card.id), CONSUMER_A
        )
        failed = self.cards.fail_delivery(
            first.card.id,
            expected_version=first.card.version,
            claim_token=first.token,
        )
        self.assertEqual((failed.status, failed.version), (CardStatus.PENDING, 3))
        self.assertIsNone(self._consumer_digest(first.card.id))
        self.assertEqual(self.cards.complete_delivery(
            first.card.id,
            expected_version=first.card.version,
            claim_token=first.token,
            transport="synthetic",
            delivery_ref="message-1",
        ).refusal, CardRefusal.STALE_VERSION)

        # A second claim by a different consumer after re-pooling records
        # that consumer's own digest, independent of what the card held
        # before (ADR 0036 decision 2).
        second = self.cards.claim_next(
            lease_seconds=60, consumer_digest=CONSUMER_B
        )
        self.assertEqual(second.card.version, 4)
        self.assertEqual(self._consumer_digest(second.card.id), CONSUMER_B)
        self.clock.advance(timedelta(seconds=61))
        third = self.cards.claim_next(
            lease_seconds=60, consumer_digest=CONSUMER_A
        )
        self.assertEqual(third.card.id, second.card.id)
        self.assertEqual(third.card.version, 6)
        # Lease expiry (inside `claim_next`) cleared consumer B's digest
        # before the third claim recorded consumer A's.
        self.assertEqual(self._consumer_digest(third.card.id), CONSUMER_A)
        self.assertEqual(self.cards.complete_delivery(
            second.card.id,
            expected_version=second.card.version,
            claim_token=second.token,
            transport="synthetic",
            delivery_ref="message-2",
        ).refusal, CardRefusal.STALE_VERSION)

    def test_external_task_change_invalidates_active_card(self):
        self._raise_review_cards(limit=1)
        claim = self.claim_and_deliver()
        self.assertEqual(self._consumer_digest(claim.card.id), CONSUMER_A)
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
        # `_cancel_stale` (driven here by `schedule()`, since the
        # underlying task changed under the delivered card) is a terminal
        # transition out of `delivered` too: the card must carry no
        # consumer affinity afterward (ADR 0036 invariant 6).
        self.assertIsNone(self._consumer_digest(claim.card.id))

    def test_a_source_update_fences_the_card_already_in_flight(self):
        """The fence survives; the re-surface it used to trigger does not.

        Editing a task's source still invalidates a card a reader is holding
        -- answering it would answer about text that has since changed. What
        no longer follows is a fresh card: raising one was part of the
        periodic review rhythm, and that rhythm is gone.
        """
        self._raise_review_cards(limit=1)
        first = self.claim_and_deliver()
        task_id = first.card.task_id

        self.revise_source_without_task_change(task_id)
        stale = self.cards.act(
            first.card.id,
            expected_version=first.card.version,
            action="keep_open",
        )
        self.assertEqual(stale.refusal, CardRefusal.STALE_VERSION)

        # `schedule()` retracts the fenced card and raises nothing to replace
        # it, so the reader is left with one fewer question, not a new one.
        raised = self.cards.schedule()
        self.assertEqual((raised.cancelled, raised.created), (1, 0))
        self.assertEqual(
            [card for card in self.cards.due(limit=20)
             if card.task_id == task_id],
            [],
        )
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
            self.cards.claim_next(lease_seconds=1, consumer_digest=CONSUMER_A)
        for bad_digest in (None, "", "not-hex" * 8, "a" * 63, "A" * 64):
            with self.assertRaisesRegex(TaskLedgerError, "consumer digest"):
                self.cards.claim_next(
                    lease_seconds=60, consumer_digest=bad_digest
                )
        self._raise_review_cards(limit=1)
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

    def test_card_renders_live_participants_and_advisory_confidence(self):
        with closing(sqlite3.connect(self.database)) as connection:
            connection.execute(
                "UPDATE tasks SET confidence=0.75 WHERE id=1"
            )
            connection.execute(
                "INSERT INTO task_participants("
                "task_id,position,kind,speaker_id,canonical_speaker_id,"
                "speaker_registry_id) VALUES(1,0,'person','SPK_002','SPK_002',"
                "'registry-synthetic')"
            )
            connection.execute(
                "INSERT INTO speaker_registry_entries("
                "speaker_registry_id,speaker_id,canonical_speaker_id,"
                "display_name,updated_at) VALUES('registry-synthetic','SPK_002',"
                "'SPK_002','Person B','2030-03-01T00:00:00Z')"
            )
            connection.commit()
        self._raise_review_cards(limit=1)
        card = next(card for card in self.cards.due(limit=20) if card.task_id == 1)
        body, _ = render_task_review_card(replace(card, status=CardStatus.DELIVERING))
        self.assertIn("Participants:</b> Person B", body)
        self.assertIn("Extraction confidence:</b> 75%", body)

        with closing(sqlite3.connect(self.database)) as connection:
            connection.execute(
                "UPDATE speaker_registry_entries SET display_name='Person C' "
                "WHERE speaker_registry_id='registry-synthetic' AND speaker_id='SPK_002'"
            )
            connection.commit()
        refreshed = next(
            card for card in self.cards.due(limit=20) if card.task_id == 1
        )
        self.assertEqual(refreshed.participants, ("Person C",))


if __name__ == "__main__":
    unittest.main()
