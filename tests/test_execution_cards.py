#!/usr/bin/env python3
"""Synthetic tests for durable execution review cards."""

from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from contextlib import closing
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path

import foxhound.candidate_inbox as inbox_schema
from foxhound.agent_profiles import (
    AgentProfileRegistry,
    general_profile,
    parse_profile,
)
from foxhound import task_relations
from foxhound.candidate_inbox import CandidateInbox, SCHEMA_VERSION
from foxhound.contracts import candidate_id_for
from foxhound.execution_cards import (
    CALLBACK_DATA_LIMIT,
    MAX_BRIEF_BYTES,
    MAX_BRIEF_CHARS,
    MAX_CARD_BODY_BYTES,
    MAX_TRUNCATED_CARD_BODY_BYTES,
    ExecutionCardDisposition,
    ExecutionCardKind,
    ExecutionCardRefusal,
    ExecutionCardService,
    ExecutionCardStatus,
    ExecutionReviewCard,
    parse_execution_agent_callback,
    parse_execution_review_callback,
    render_execution_agent_selector,
    render_execution_review_card,
    task_brief,
)
from foxhound.card_provenance import CardSourceEvidence
from foxhound.task_execution import (
    PARK_RETRY_INTERVAL,
    ExecutionOutcome,
    ExecutionResultEnvelope,
    TaskExecutionService,
    WorkflowPhase,
    WorkflowStatus,
)
from foxhound.task_ledger import TaskLedger, TaskStatus
from foxhound.knowledge_client import (
    KnowledgeTransportError,
    OwnerUpcomingMeeting,
)
from foxhound.task_lifecycle_outcome_export import export_outcomes


NOW = datetime(2030, 4, 5, 12, 0, tzinfo=timezone.utc)


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


def _drop_agent_profile_schema(connection: sqlite3.Connection) -> None:
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
    connection.execute(
        "ALTER TABLE task_execution_results DROP COLUMN task_kb_file"
    )
    connection.execute(
        "ALTER TABLE task_execution_results DROP COLUMN task_work_directory"
    )
    for table in (
        "task_execution_workflows",
        "task_execution_results",
        "task_execution_events",
    ):
        connection.execute(
            f"ALTER TABLE {table} DROP COLUMN agent_profile_revision"
        )
        connection.execute(
            f"ALTER TABLE {table} DROP COLUMN agent_profile_id"
        )


DELIVERY_TOKEN = "delivery-token-" + "d" * 32
CLAIM_TOKEN = "execution-token-" + "c" * 32


class Clock:
    def __init__(self) -> None:
        self.value = NOW

    def __call__(self) -> datetime:
        return self.value

    def advance(self, delta: timedelta) -> None:
        self.value += delta


class ExecutionCardTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.database = Path(self.temporary.name) / "foxhound.sqlite3"
        self.clock = Clock()
        CandidateInbox(self.database, clock=self.clock).initialize()
        with closing(sqlite3.connect(self.database)) as connection:
            for task_id in range(1, 7):
                connection.execute(
                    "INSERT INTO tasks(id,status,text,owner,due,version,"
                    "created_at,updated_at,closed_at) "
                    "VALUES(?,'open',?,?,NULL,1,?,?,NULL)",
                    (
                        task_id,
                        f"Synthetic task {task_id} <private>",
                        f"Person {task_id}",
                        f"2030-04-{task_id:02d}T10:00:00+00:00",
                        NOW.isoformat(timespec="seconds"),
                    ),
                )
            connection.commit()
        self.execution = TaskExecutionService(
            self.database,
            clock=self.clock,
            token_factory=lambda: CLAIM_TOKEN,
        )
        self.cards = ExecutionCardService(
            self.database,
            clock=self.clock,
            token_factory=lambda: DELIVERY_TOKEN,
        )
        self.ledger = TaskLedger(self.database, clock=self.clock)

    def _schedule_workflow(self, task_id: int):
        result = self.execution.schedule(task_id, expected_task_version=1)
        self.assertTrue(result.accepted)
        return result

    def _set_structured_owner(
        self,
        task_id: int,
        owner: str,
        *,
        kind: str = "person",
        speaker_id: str | None = "SPK_002",
        canonical_speaker_id: str | None = None,
        provisional: int = 0,
    ) -> None:
        registry_id = "registry-1" if speaker_id is not None else None
        canonical = (
            speaker_id
            if canonical_speaker_id is None
            else canonical_speaker_id
        )
        with closing(sqlite3.connect(self.database)) as connection:
            connection.execute(
                "UPDATE tasks SET owner=?,owner_ref_version=1,owner_kind=?,"
                "owner_speaker_id=?,owner_canonical_speaker_id=?,"
                "owner_speaker_registry_id=?,owner_pinned=0,"
                "owner_provisional=? WHERE id=?",
                (
                    owner, kind, speaker_id, canonical, registry_id,
                    provisional, task_id,
                ),
            )
            connection.commit()

    def _claim_and_deliver(self):
        claim = self.cards.claim_next(lease_seconds=60)
        self.assertIsNotNone(claim)
        delivered = self.cards.complete_delivery(
            claim.card.id,
            expected_version=claim.card.version,
            claim_token=claim.token,
            transport="synthetic",
            delivery_ref=f"message-{claim.card.id}",
        )
        self.assertTrue(delivered.accepted)
        return claim

    def _record(
        self,
        task_id: int,
        *,
        phase: WorkflowPhase,
        outcome: ExecutionOutcome,
        result_id: str,
        long_work: bool = False,
        work_markdown: str | None = None,
    ):
        claim = self.execution.claim_next()
        self.assertIsNotNone(claim)
        self.assertEqual((claim.task_id, claim.phase), (task_id, phase))
        result = self.execution.record_result(ExecutionResultEnvelope(
            result_id=result_id,
            task_id=task_id,
            task_version=1,
            workflow_version=claim.workflow_version,
            phase=phase,
            claim_token=claim.token,
            outcome=outcome,
            summary="Synthetic <summary>",
            work_markdown=(
                work_markdown
                if work_markdown is not None
                else (
                    ("Synthetic plan. " * 8_000).strip()
                    if long_work
                    else "Synthetic plan."
                )
            ),
            questions=("Proceed with Example A?",),
            external_actions=("Publish synthetic draft <alpha>.",),
            deliverables=("Synthetic deliverable",),
        ))
        self.assertTrue(result.accepted)
        return result

    def _plan_review(
        self,
        task_id: int,
        result_id: str,
        *,
        long_work: bool = False,
        work_markdown: str | None = None,
    ):
        scheduled = self._schedule_workflow(task_id)
        started = self.execution.start_action(
            task_id, expected_version=scheduled.version, action="start"
        )
        self.assertEqual(started.status, WorkflowStatus.QUEUED)
        return self._record(
            task_id,
            phase=WorkflowPhase.PLAN,
            outcome=ExecutionOutcome.AWAITING_PLAN,
            result_id=result_id,
            long_work=long_work,
            work_markdown=work_markdown,
        )

    def _external_review(self, task_id: int, prefix: str):
        self._plan_review(task_id, f"{prefix}-plan")
        approved = self.execution.review_action(
            task_id,
            expected_version=self.execution.get(task_id).version,
            action="approve",
        )
        self.assertEqual(
            (approved.status, approved.phase),
            (WorkflowStatus.QUEUED, WorkflowPhase.EXECUTE),
        )
        return self._record(
            task_id,
            phase=WorkflowPhase.EXECUTE,
            outcome=ExecutionOutcome.AWAITING_EXTERNAL,
            result_id=f"{prefix}-execute",
        )

    def test_schema_eight_migration_is_passive_and_preserves_execution(self):
        self._schedule_workflow(1)
        with closing(sqlite3.connect(self.database)) as connection:
            _drop_agent_profile_schema(connection)
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
            connection.execute("PRAGMA user_version = 8")
            connection.commit()

        CandidateInbox(self.database, clock=self.clock).initialize()
        with closing(sqlite3.connect(self.database)) as connection:
            version = connection.execute("PRAGMA user_version").fetchone()[0]
            workflows = connection.execute(
                "SELECT COUNT(*) FROM task_execution_workflows"
            ).fetchone()[0]
            cards = connection.execute(
                "SELECT COUNT(*) FROM execution_review_cards"
            ).fetchone()[0]
        self.assertEqual((version, workflows, cards), (SCHEMA_VERSION, 1, 0))

    def test_schema_ten_migration_preserves_active_cards_and_events(self):
        database = Path(self.temporary.name) / "schema-ten.sqlite3"
        with closing(sqlite3.connect(database)) as connection:
            connection.execute(inbox_schema._SCHEMA_V1)
            for version in range(2, 11):
                for statement in getattr(inbox_schema, f"_SCHEMA_V{version}"):
                    connection.execute(statement)
                connection.commit()
            now = NOW.isoformat(timespec="seconds")
            connection.execute(
                "INSERT INTO tasks(id,status,text,owner,due,version,created_at,"
                "updated_at,closed_at) VALUES(1,'open','Synthetic task',"
                "'Person A',NULL,1,?,?,NULL)",
                (now, now),
            )
            connection.execute(
                "INSERT INTO task_execution_workflows("
                "task_id,task_version,status,phase,version,due_at,"
                "claim_token_digest,claimed_at,claim_heartbeat_at,"
                "claim_expires_at,failure_count,last_failure_reason,"
                "last_failure_at,next_attempt_at,parked_at,last_result_id,"
                "created_at,updated_at,completed_at) VALUES(1,1,"
                "'awaiting_start','plan',1,NULL,NULL,NULL,NULL,NULL,0,NULL,"
                "NULL,NULL,NULL,NULL,?,?,NULL)",
                (now, now),
            )
            connection.execute(
                "INSERT INTO execution_review_cards("
                "id,task_id,task_version,workflow_version,kind,phase,result_id,"
                "status,version,created_at,updated_at) VALUES(1,1,1,1,'start',"
                "'plan',NULL,'delivered',2,?,?)",
                (now, now),
            )
            connection.execute(
                "INSERT INTO execution_review_card_events("
                "card_id,task_id,kind,card_version,workflow_version,action,"
                "occurred_at) VALUES(1,1,'delivered',2,1,NULL,?)",
                (now,),
            )
            connection.execute("PRAGMA user_version = 10")
            connection.commit()

        CandidateInbox(database, clock=self.clock).initialize()

        with closing(sqlite3.connect(database)) as connection:
            version = connection.execute("PRAGMA user_version").fetchone()[0]
            card = connection.execute(
                "SELECT kind,status,version FROM execution_review_cards"
            ).fetchone()
            event = connection.execute(
                "SELECT kind,card_version FROM execution_review_card_events"
            ).fetchone()
            private_tables = tuple(
                connection.execute(
                    "SELECT COUNT(*) FROM " + table
                ).fetchone()[0]
                for table in ("execution_reader_inputs", "task_owner_events")
            )
        self.assertEqual(version, SCHEMA_VERSION)
        self.assertEqual(card, ("start", "delivered", 2))
        self.assertEqual(event, ("delivered", 2))
        self.assertEqual(private_tables, (0, 0))

    def test_schedule_is_explicit_current_bounded_and_idempotent(self):
        self.assertEqual(
            self.cards.schedule().disposition,
            ExecutionCardDisposition.UNCHANGED,
        )
        first = self._schedule_workflow(1)
        second = self._schedule_workflow(2)
        self.execution.start_action(
            2, expected_version=second.version, action="snooze"
        )
        scheduled = self.cards.schedule(limit=1)
        self.assertEqual((scheduled.created, scheduled.cancelled), (1, 0))
        self.assertEqual(self.cards.schedule().created, 0)
        claim = self.cards.claim_next()
        self.assertEqual(
            (claim.card.task_id, claim.card.kind, claim.card.workflow_version),
            (1, ExecutionCardKind.START, first.version),
        )
        self.assertNotIn("Synthetic task", repr(claim.card))
        self.assertNotIn(DELIVERY_TOKEN, repr(claim))

        self.clock.advance(timedelta(days=1))
        self.assertEqual(self.cards.schedule().created, 1)
        self.assertEqual(self.cards.stats().active, 2)
        self.assertEqual(
            self.cards.schedule(limit=0).refusal,
            ExecutionCardRefusal.INVALID_ARGUMENT,
        )

    def test_work_in_flight_does_not_hide_the_work_behind_it(self):
        """A gate is how work gets queued. Suppressing gates while a
        workflow runs means the queue empties and never refills: one
        machine had 143 tasks invisible behind seven in flight, with a free
        card surface and nothing to put on it.

        How many cards a reader sees at once is the drip's business, and it
        already bounds that. It is not this query's job to decide the
        machine is too busy to be asked.
        """
        first = self._schedule_workflow(1)
        self._schedule_workflow(2)

        queued = self.execution.start_action(
            1, expected_version=first.version, action="start")
        self.assertEqual(queued.status, WorkflowStatus.QUEUED)

        # Task 2 has not been asked about, and task 1 being queued is not a
        # reason to keep asking about nothing.
        self.assertEqual(self.cards.schedule(limit=6).created, 1)
        card = self.cards.claim_next().card
        self.assertEqual(
            (card.task_id, card.kind), (2, ExecutionCardKind.START))

        # Still true once the first is actually running.
        claim = self.execution.claim_next()
        self.assertEqual(claim.task_id, 1)
        self.assertEqual(self.cards.schedule(limit=6).created, 0)

    def test_a_review_still_arrives_while_other_work_runs(self):
        # The point is not "more cards"; it is that finishing one piece of
        # work still produces its own card whatever else is in flight.
        first = self._schedule_workflow(1)
        self.execution.start_action(
            1, expected_version=first.version, action="start")
        claim = self.execution.claim_next()
        recorded = self.execution.record_result(ExecutionResultEnvelope(
            result_id="priority-plan",
            task_id=1,
            task_version=1,
            workflow_version=claim.workflow_version,
            phase=WorkflowPhase.PLAN,
            claim_token=claim.token,
            outcome=ExecutionOutcome.AWAITING_PLAN,
            summary="Synthetic priority plan.",
            work_markdown="Review this synthetic plan.",
        ))
        self.assertTrue(recorded.accepted)

        self.assertEqual(self.cards.schedule(limit=6).created, 1)
        review = self._claim_and_deliver()
        self.assertEqual(review.card.kind, ExecutionCardKind.PLAN_REVIEW)
        finished = self.cards.act(
            review.card.id,
            expected_version=review.card.version,
            action="done",
        )
        self.assertEqual(finished.workflow_status, WorkflowStatus.COMPLETED)

    def test_existing_over_capacity_work_does_not_hide_a_start_card(self):
        for task_id in range(1, 7):
            workflow = self._schedule_workflow(task_id)
            queued = self.execution.start_action(
                task_id,
                expected_version=workflow.version,
                action="start",
            )
            self.assertEqual(queued.status, WorkflowStatus.QUEUED)
        with closing(sqlite3.connect(self.database)) as connection:
            connection.execute(
                "INSERT INTO tasks(id,status,text,owner,due,version,"
                "created_at,updated_at,closed_at) "
                "VALUES(7,'open','Synthetic task 7','Person A',NULL,1,?,?,NULL)",
                (self.clock().isoformat(), self.clock().isoformat()),
            )
            connection.commit()
        self._schedule_workflow(7)

        self.assertEqual(self.cards.schedule(limit=1).created, 1)
        with closing(sqlite3.connect(self.database)) as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT task_id,kind FROM execution_review_cards"
                ).fetchone(),
                (7, "start"),
            )

    def test_delivery_retry_expiry_acknowledgement_and_callbacks(self):
        self._schedule_workflow(1)
        self.cards.schedule()
        first = self.cards.claim_next(lease_seconds=60)
        body, keyboard = render_execution_review_card(first.card)
        self.assertIn("&lt;private&gt;", body)
        self.assertNotIn("<private>", body)
        self.assertLessEqual(len(body.encode("utf-8")), MAX_CARD_BODY_BYTES)
        callbacks = [
            button["callback_data"]
            for row in keyboard["inline_keyboard"]
            for button in row
        ]
        self.assertEqual(
            [parse_execution_review_callback(value)[2] for value in callbacks],
            ["done", "start", "drop", "discuss", "snooze", "reassign",
             "agent", "brief"],
        )
        self.assertEqual(
            [[button["text"] for button in row]
             for row in keyboard["inline_keyboard"]],
            [
                ["✅ Done", "▶️ Continue"],
                ["🗑 Drop", "✏️ Update"],
                ["🕓 Snooze", "👥 Reassign"],
                ["🤖 Agent"],
                ["📋 Task brief"],
            ],
        )
        self.assertTrue(all(
            len(value.encode("utf-8")) <= CALLBACK_DATA_LIMIT
            for value in callbacks
        ))

        wrong = self.cards.complete_delivery(
            first.card.id,
            expected_version=first.card.version,
            claim_token="wrong-token-" + "w" * 32,
            transport="synthetic",
            delivery_ref="message-alpha",
        )
        self.assertEqual(wrong.refusal, ExecutionCardRefusal.CLAIM_MISMATCH)
        failed = self.cards.fail_delivery(
            first.card.id,
            expected_version=first.card.version,
            claim_token=first.token,
        )
        self.assertEqual(
            (failed.card_status, failed.card_version),
            (ExecutionCardStatus.PENDING, 3),
        )
        second = self.cards.claim_next(lease_seconds=60)
        self.clock.advance(timedelta(seconds=61))
        third = self.cards.claim_next(lease_seconds=60)
        self.assertEqual(third.card.id, second.card.id)
        self.assertGreater(third.card.version, second.card.version)
        delivered = self.cards.complete_delivery(
            third.card.id,
            expected_version=third.card.version,
            claim_token=third.token,
            transport="synthetic",
            delivery_ref="message-beta",
        )
        self.assertEqual(delivered.card_status, ExecutionCardStatus.DELIVERED)
        replay = self.cards.complete_delivery(
            third.card.id,
            expected_version=third.card.version,
            claim_token=third.token,
            transport="synthetic",
            delivery_ref="message-beta",
        )
        self.assertEqual(replay.disposition, ExecutionCardDisposition.UNCHANGED)
        self.assertIsNone(parse_execution_review_callback("fhe|01|1|start"))
        self.assertIsNone(parse_execution_review_callback("fhc|1|1|start"))

    def test_other_owner_can_be_held_until_meeting_and_wakes_with_fresh_card(self):
        self._set_structured_owner(
            1,
            "Person B",
            speaker_id="SPK_010",
            canonical_speaker_id="SPK_002",
        )
        matches = [False]
        requests = []

        def condition(owner, owner_ref):
            requests.append((owner, owner_ref))
            return OwnerUpcomingMeeting(
                matches[0],
                self.clock().isoformat(timespec="seconds"),
                "a" * 64,
            )

        cards = ExecutionCardService(
            self.database,
            clock=self.clock,
            token_factory=lambda: DELIVERY_TOKEN,
            owner_condition=condition,
            reader_aliases=("Person A", "A. Person"),
        )
        self._schedule_workflow(1)
        self.assertEqual(cards.schedule().created, 1)
        claim = cards.claim_next()
        body, keyboard = render_execution_review_card(claim.card)
        self.assertIn("Person B", body)
        labels = [
            button["text"]
            for row in keyboard["inline_keyboard"]
            for button in row
        ]
        self.assertIn("🗓 Until next meeting with Person B", labels)
        callback = next(
            button["callback_data"]
            for row in keyboard["inline_keyboard"]
            for button in row
            if button["text"].startswith("🗓")
        )
        self.assertEqual(
            parse_execution_review_callback(callback)[2], "until_meeting"
        )
        delivered = cards.complete_delivery(
            claim.card.id,
            expected_version=claim.card.version,
            claim_token=claim.token,
            transport="synthetic",
            delivery_ref="message-hold",
        )
        held = cards.act(
            claim.card.id,
            expected_version=delivered.card_version,
            action="until_meeting",
        )
        self.assertEqual(held.workflow_status, WorkflowStatus.SNOOZED)
        self.assertEqual(
            held.wake_at,
            (NOW + timedelta(days=21)).isoformat(timespec="seconds"),
        )
        self.assertIsNone(self.execution.claim_next())
        with closing(sqlite3.connect(self.database)) as connection:
            hold = connection.execute(
                "SELECT status,owner_display,owner_speaker_id,backstop_at "
                "FROM task_execution_owner_holds"
            ).fetchone()
            events = connection.execute(
                "SELECT kind FROM task_execution_owner_hold_events "
                "ORDER BY sequence"
            ).fetchall()
        self.assertEqual(
            hold,
            (
                "active", "Person B", "SPK_010",
                (NOW + timedelta(days=21)).isoformat(timespec="seconds"),
            ),
        )
        self.assertEqual(events, [("created",)])

        restarted = ExecutionCardService(
            self.database,
            clock=self.clock,
            token_factory=lambda: DELIVERY_TOKEN,
            owner_condition=condition,
            reader_aliases=("Person A",),
        )
        self.assertEqual(restarted.schedule().created, 0)
        self.assertEqual(requests[-1][0], "Person B")
        self.assertEqual(
            requests[-1][1]["canonical_speaker_id"], "SPK_002"
        )
        self.assertEqual(requests[-1][1]["speaker_id"], "SPK_010")
        matches[0] = True
        self.assertEqual(restarted.schedule().created, 1)
        replacement = restarted.claim_next()
        self.assertEqual(replacement.card.task_id, 1)
        self.assertGreater(
            replacement.card.workflow_version, claim.card.workflow_version
        )
        stale = restarted.act(
            claim.card.id,
            expected_version=delivered.card_version,
            action="start",
        )
        self.assertEqual(stale.refusal, ExecutionCardRefusal.STALE_VERSION)
        with closing(sqlite3.connect(self.database)) as connection:
            released = connection.execute(
                "SELECT status,release_reason FROM task_execution_owner_holds"
            ).fetchone()
        self.assertEqual(released, ("released", "meeting"))

    def test_owner_hold_is_conservative_and_backstop_is_exact(self):
        for task_id, owner, kind, speaker, provisional in (
            (1, "Person A", "person", "SPK_001", 0),
            (2, "Team Alpha", "group", None, 0),
            (3, "Person C", "person", "SPK_003", 1),
            (4, "Person B", "person", "SPK_002", 0),
            (5, "Person " + "É" * 180, "person", "SPK_005", 0),
        ):
            self._set_structured_owner(
                task_id,
                owner,
                kind=kind,
                speaker_id=speaker,
                provisional=provisional,
            )
            self._schedule_workflow(task_id)

        def unavailable(_owner, _owner_ref):
            raise KnowledgeTransportError("synthetic unavailable")

        cards = ExecutionCardService(
            self.database,
            clock=self.clock,
            token_factory=lambda: DELIVERY_TOKEN,
            owner_condition=unavailable,
            reader_aliases=("Pérson A",),
        )
        self.assertEqual(cards.schedule(limit=5).created, 5)
        eligibility = {}
        long_label = None
        for _ in range(5):
            claim = cards.claim_next()
            eligibility[claim.card.task_id] = claim.card.owner_hold_eligible
            if claim.card.task_id == 5:
                _body, keyboard = render_execution_review_card(claim.card)
                long_label = next(
                    button["text"]
                    for row in keyboard["inline_keyboard"]
                    for button in row
                    if button["text"].startswith("🗓")
                )
            delivered = cards.complete_delivery(
                claim.card.id,
                expected_version=claim.card.version,
                claim_token=claim.token,
                transport="synthetic",
                delivery_ref=f"message-{claim.card.id}",
            )
            if claim.card.task_id == 4:
                cards.act(
                    claim.card.id,
                    expected_version=delivered.card_version,
                    action="until_meeting",
                )
        self.assertEqual(
            eligibility,
            {1: False, 2: False, 3: False, 4: True, 5: True},
        )
        self.assertLessEqual(len(long_label.encode("utf-8")), 64)
        self.assertNotIn("SPK_", long_label)
        self.clock.advance(timedelta(days=21) - timedelta(seconds=1))
        self.assertEqual(cards.schedule().created, 0)
        self.clock.advance(timedelta(seconds=1))
        self.assertEqual(cards.schedule().created, 1)
        with closing(sqlite3.connect(self.database)) as connection:
            released = connection.execute(
                "SELECT status,release_reason FROM task_execution_owner_holds"
            ).fetchone()
        self.assertEqual(released, ("released", "backstop"))

    def test_operator_can_retry_only_a_current_delivered_presentation(self):
        workflow = self._schedule_workflow(1)
        self.cards.schedule()
        claim = self._claim_and_deliver()

        retried = self.cards.retry_delivery(
            claim.card.id,
            expected_version=claim.card.version,
        )

        self.assertEqual(
            (retried.card_status, retried.card_version),
            (ExecutionCardStatus.PENDING, claim.card.version + 1),
        )
        self.assertEqual(
            (self.execution.get(1).status, self.execution.get(1).version),
            (WorkflowStatus.AWAITING_START, workflow.version),
        )
        old_action = self.cards.act(
            claim.card.id,
            expected_version=claim.card.version,
            action="start",
        )
        self.assertEqual(old_action.refusal, ExecutionCardRefusal.STALE_VERSION)
        duplicate = self.cards.retry_delivery(
            claim.card.id,
            expected_version=claim.card.version + 1,
        )
        self.assertEqual(duplicate.refusal, ExecutionCardRefusal.INVALID_STATE)
        replacement = self.cards.claim_next(lease_seconds=60)
        self.assertEqual(replacement.card.id, claim.card.id)
        self.assertGreater(replacement.card.version, retried.card_version)

    def test_start_actions_atomically_apply_the_expected_task_lifecycle(self):
        expected = {
            1: ("start", WorkflowStatus.QUEUED, TaskStatus.OPEN),
            2: ("snooze", WorkflowStatus.SNOOZED, TaskStatus.OPEN),
            3: ("cancel", WorkflowStatus.CANCELLED, TaskStatus.OPEN),
            4: ("done", WorkflowStatus.COMPLETED, TaskStatus.DONE),
            5: ("drop", WorkflowStatus.CANCELLED, TaskStatus.DROPPED),
        }
        for task_id in expected:
            self._schedule_workflow(task_id)
        self.cards.schedule()
        for task_id, (action, status, task_status) in expected.items():
            claim = self._claim_and_deliver()
            self.assertEqual(claim.card.task_id, task_id)
            before = self.cards.event_count()
            result = self.cards.act(
                claim.card.id,
                expected_version=claim.card.version,
                action=action,
            )
            self.assertEqual(
                (result.card_status, result.workflow_status),
                (ExecutionCardStatus.RESOLVED, status),
            )
            self.assertEqual(
                self.ledger.get(task_id).status, task_status
            )
            stale = self.cards.act(
                claim.card.id,
                expected_version=claim.card.version,
                action=action,
            )
            self.assertEqual(stale.refusal, ExecutionCardRefusal.STALE_VERSION)
            self.assertEqual(self.cards.event_count(), before + 1)

    def test_agent_change_versions_workflow_and_invalidates_start_card(self):
        workflow = self._schedule_workflow(1)
        self.cards.schedule()
        claim = self._claim_and_deliver()
        specialist_document = general_profile().document()
        specialist_document.update({
            "profile_id": "specialist",
            "display_name": "Synthetic Specialist",
        })
        specialist = parse_profile(specialist_document)
        execution = TaskExecutionService(
            self.database,
            clock=self.clock,
            profile_registry=AgentProfileRegistry((
                general_profile(), specialist,
            )),
        )

        selected = execution.select_agent(
            1,
            expected_version=workflow.version,
            profile_id=specialist.profile_id,
            profile_revision=specialist.revision,
        )

        self.assertEqual(selected.version, workflow.version + 1)
        stale = self.cards.act(
            claim.card.id,
            expected_version=claim.card.version,
            action="start",
        )
        self.assertEqual(stale.refusal, ExecutionCardRefusal.STALE_VERSION)
        refreshed = self.cards.schedule()
        self.assertEqual((refreshed.cancelled, refreshed.created), (1, 1))
        self.assertEqual(self.ledger.get(1).status, TaskStatus.OPEN)

    def test_historical_start_card_offers_current_revision_for_reselection(self):
        current = general_profile()
        historical_document = current.document()
        historical_document.update({
            "max_turns": 12,
            "timeout_seconds": 240,
            "claim_lease_seconds": 900,
            "kill_grace_seconds": 10,
        })
        historical = parse_profile(historical_document)
        old_execution = TaskExecutionService(
            self.database,
            clock=self.clock,
            token_factory=lambda: CLAIM_TOKEN,
            profile_registry=AgentProfileRegistry((historical,)),
        )
        workflow = old_execution.schedule(1, expected_task_version=1)
        registry = AgentProfileRegistry(
            (current,), historical_profiles=(historical,)
        )
        cards = ExecutionCardService(
            self.database,
            clock=self.clock,
            token_factory=lambda: DELIVERY_TOKEN,
            profile_registry=registry,
        )
        cards.schedule()
        claim = cards.claim_next()
        self.assertIsNotNone(claim)
        cards.complete_delivery(
            claim.card.id,
            expected_version=claim.card.version,
            claim_token=claim.token,
            transport="synthetic",
            delivery_ref="message-historical-selector",
        )

        choices = cards.agent_options(
            claim.card.id, expected_version=claim.card.version
        )
        self.assertEqual(
            [
                (option.display_name, option.selected)
                for option in choices.options
            ],
            [("General", False)],
        )
        _, keyboard = render_execution_agent_selector(choices)
        callback = keyboard["inline_keyboard"][0][0]["callback_data"]
        parsed = parse_execution_agent_callback(callback)
        selected = cards.select_agent(
            parsed[0],
            expected_version=parsed[1],
            selection_token=parsed[2],
        )

        self.assertEqual(selected.disposition, ExecutionCardDisposition.APPLIED)
        self.assertEqual(selected.card.agent_profile_revision, current.revision)
        self.assertEqual(selected.card.workflow_version, workflow.version + 1)

    def test_start_card_selects_agent_atomically_with_bounded_callbacks(self):
        specialist_document = general_profile().document()
        specialist_document.update({
            "profile_id": "specialist",
            "display_name": "Synthetic Specialist",
            "max_turns": 50,
        })
        specialist = parse_profile(specialist_document)
        execute_only_document = general_profile().document()
        execute_only_document.update({
            "profile_id": "execute-only",
            "display_name": "Synthetic Execute Only",
            "allowed_phases": ["execute"],
        })
        execute_only = parse_profile(execute_only_document)
        registry = AgentProfileRegistry((
            general_profile(), specialist, execute_only,
        ))
        cards = ExecutionCardService(
            self.database,
            clock=self.clock,
            token_factory=lambda: DELIVERY_TOKEN,
            profile_registry=registry,
        )
        workflow = self._schedule_workflow(1)
        cards.schedule()
        claim = cards.claim_next()
        cards.complete_delivery(
            claim.card.id,
            expected_version=claim.card.version,
            claim_token=claim.token,
            transport="synthetic",
            delivery_ref="message-agent-selector",
        )
        choices = cards.agent_options(
            claim.card.id, expected_version=claim.card.version
        )
        choice_body, choice_keyboard = render_execution_agent_selector(choices)
        self.assertIn("Choose the agent for planning", choice_body)
        self.assertEqual(
            [row[0]["text"] for row in choice_keyboard["inline_keyboard"]],
            ["✓ General", "Synthetic Specialist"],
        )
        callbacks = [
            row[0]["callback_data"]
            for row in choice_keyboard["inline_keyboard"]
        ]
        self.assertTrue(all(
            len(value.encode("utf-8")) <= CALLBACK_DATA_LIMIT
            for value in callbacks
        ))
        maximum_callback = (
            "fha|9223372036854775807|9223372036854775807|" + "a" * 20
        )
        self.assertEqual(len(maximum_callback.encode("utf-8")), 64)
        self.assertIsNotNone(parse_execution_agent_callback(maximum_callback))
        specialist_callback = callbacks[1]
        parsed = parse_execution_agent_callback(specialist_callback)
        self.assertEqual(parsed[:2], (claim.card.id, claim.card.version))
        before_task = self.ledger.get(1)
        before_events = cards.event_count()

        selected = cards.select_agent(
            parsed[0],
            expected_version=parsed[1],
            selection_token=parsed[2],
        )

        self.assertEqual(selected.disposition, ExecutionCardDisposition.APPLIED)
        self.assertEqual(selected.card_version, claim.card.version + 1)
        self.assertEqual(selected.card.agent_profile_id, "specialist")
        self.assertNotIn("Synthetic Specialist", repr(selected.card))
        self.assertEqual(
            (selected.card.workflow_status, selected.card.workflow_version),
            (WorkflowStatus.AWAITING_START, workflow.version + 1),
        )
        refreshed_body, refreshed_keyboard = render_execution_review_card(
            selected.card
        )
        self.assertIn("<b>Start this task?</b>", refreshed_body)
        # Choosing an agent shows on the card. Selecting one and seeing no
        # sign of it is indistinguishable from the tap not working.
        self.assertIn("Synthetic Specialist", refreshed_body)
        self.assertTrue(all(
            parse_execution_review_callback(button["callback_data"])[1]
            == selected.card_version
            for row in refreshed_keyboard["inline_keyboard"]
            for button in row
        ))
        after_task = self.ledger.get(1)
        self.assertEqual(
            (after_task.status, after_task.version),
            (before_task.status, before_task.version),
        )
        self.assertEqual(cards.event_count(), before_events + 1)
        with closing(sqlite3.connect(self.database)) as connection:
            card_event = connection.execute(
                "SELECT kind,action,card_version,workflow_version "
                "FROM execution_review_card_events ORDER BY sequence DESC "
                "LIMIT 1"
            ).fetchone()
            workflow_event = connection.execute(
                "SELECT kind,agent_profile_id,agent_profile_revision "
                "FROM task_execution_events ORDER BY sequence DESC LIMIT 1"
            ).fetchone()
        self.assertEqual(
            card_event,
            ("refreshed", "agent", selected.card.version,
             selected.card.workflow_version),
        )
        self.assertEqual(
            workflow_event,
            ("agent_selected", specialist.profile_id, specialist.revision),
        )

        stale = cards.select_agent(
            parsed[0],
            expected_version=parsed[1],
            selection_token=parsed[2],
        )
        forged = cards.select_agent(
            selected.card.id,
            expected_version=selected.card.version,
            selection_token="0" * 20,
        )
        oversized = cards.select_agent(
            selected.card.id,
            expected_version=selected.card.version,
            selection_token="a" * 21,
        )
        self.assertEqual(stale.refusal, ExecutionCardRefusal.STALE_VERSION)
        self.assertEqual(forged.refusal, ExecutionCardRefusal.INVALID_ARGUMENT)
        self.assertEqual(oversized.refusal, ExecutionCardRefusal.INVALID_ARGUMENT)
        self.assertEqual(cards.event_count(), before_events + 1)

        refreshed_choices = cards.agent_options(
            selected.card.id, expected_version=selected.card.version
        )
        selected_option = next(
            option for option in refreshed_choices.options if option.selected
        )
        replay = cards.select_agent(
            selected.card.id,
            expected_version=selected.card.version,
            selection_token=selected_option.selection_token,
        )
        self.assertEqual(replay.disposition, ExecutionCardDisposition.UNCHANGED)
        self.assertEqual(replay.card.version, selected.card.version)

        started = cards.act(
            selected.card.id,
            expected_version=selected.card.version,
            action="start",
        )
        self.assertTrue(started.accepted)
        unavailable = cards.agent_options(
            selected.card.id, expected_version=selected.card.version
        )
        self.assertEqual(unavailable.refusal, ExecutionCardRefusal.STALE_VERSION)

    def test_agent_control_is_absent_from_resumed_snoozed_start_card(self):
        self._schedule_workflow(1)
        self.cards.schedule()
        claim = self._claim_and_deliver()
        snoozed = self.cards.act(
            claim.card.id,
            expected_version=claim.card.version,
            action="snooze",
        )
        self.clock.advance(timedelta(days=1))
        self.assertEqual(self.cards.schedule().created, 1)
        resumed = self._claim_and_deliver()
        _body, keyboard = render_execution_review_card(resumed.card)
        actions = [
            parse_execution_review_callback(button["callback_data"])[2]
            for row in keyboard["inline_keyboard"]
            for button in row
        ]
        self.assertEqual(resumed.card.workflow_status, WorkflowStatus.SNOOZED)
        self.assertNotIn("agent", actions)
        refused = self.cards.agent_options(
            resumed.card.id, expected_version=resumed.card.version
        )
        self.assertEqual(refused.refusal, ExecutionCardRefusal.INVALID_STATE)
        self.assertEqual(self.execution.get(1).version, snoozed.workflow_version)

    def test_plan_review_rendering_approval_revision_and_cancel(self):
        for task_id, action in ((1, "approve"), (2, "revise"), (3, "cancel")):
            self._plan_review(task_id, f"plan-{task_id}")
        self.cards.schedule()
        targets = {
            "approve": (WorkflowStatus.QUEUED, WorkflowPhase.EXECUTE),
            "revise": (WorkflowStatus.QUEUED, WorkflowPhase.PLAN),
            "cancel": (WorkflowStatus.CANCELLED, WorkflowPhase.PLAN),
        }
        for task_id, action in ((1, "approve"), (2, "revise"), (3, "cancel")):
            claim = self._claim_and_deliver()
            self.assertEqual(claim.card.kind, ExecutionCardKind.PLAN_REVIEW)
            body, keyboard = render_execution_review_card(claim.card)
            self.assertIn("Synthetic &lt;summary&gt;", body)
            self.assertNotIn("<summary>", body)
            self.assertLessEqual(len(body.encode("utf-8")), MAX_CARD_BODY_BYTES)
            callbacks = [
                button["callback_data"]
                for row in keyboard["inline_keyboard"]
                for button in row
            ]
            self.assertEqual(
                [parse_execution_review_callback(value)[2]
                 for value in callbacks],
                [
                    "revise", "discuss", "approve", "snooze", "done",
                    "reassign", "drop", "brief",
                ],
            )
            self.assertEqual(
                [[button["text"] for button in row]
                 for row in keyboard["inline_keyboard"]],
                [
                    ["🔎 Investigate further", "💬 Discuss"],
                    ["▶️ Execute plan", "🕒 Snooze"],
                    ["✅ Mark as done"],
                    ["👥 Reassign", "🗑 Drop task"],
                    ["📋 Task brief"],
                ],
            )
            result = self.cards.act(
                claim.card.id,
                expected_version=claim.card.version,
                action=action,
            )
            self.assertEqual(
                (result.workflow_status, result.workflow_phase),
                targets[action],
            )
            self.assertEqual(self.ledger.get(task_id).status, TaskStatus.OPEN)

    def test_truncated_private_content_cannot_be_approved(self):
        self._plan_review(1, "long-plan", long_work=True)
        self.cards.schedule()
        claim = self._claim_and_deliver()
        body, keyboard = render_execution_review_card(claim.card)
        self.assertLessEqual(
            len(body.encode("utf-8")), MAX_TRUNCATED_CARD_BODY_BYTES
        )
        self.assertIn("too long to approve", body)
        actions = [
            parse_execution_review_callback(button["callback_data"])[2]
            for row in keyboard["inline_keyboard"]
            for button in row
        ]
        self.assertEqual(
            actions,
            ["revise", "discuss", "snooze", "reassign", "drop", "brief"],
        )
        before = self.execution.get(1)
        refused = self.cards.act(
            claim.card.id,
            expected_version=claim.card.version,
            action="approve",
        )
        self.assertEqual(refused.refusal, ExecutionCardRefusal.INVALID_STATE)
        self.assertEqual(self.execution.get(1), before)

    def test_an_approval_card_shows_what_it_asks_to_send(self):
        """The card says "approve only if these exact external effects are
        intended", and the effect is usually "send this". Showing a summary
        of a draft instead of the draft asks a reader to authorise text they
        have not read — which is the one thing the gate exists to prevent.

        The plan and result cards already show drafts in full; this one did
        not, so the card asking for authority was the least informative of
        the three.
        """
        task_id = 1
        self._plan_review(task_id, "approve-draft")
        approved = self.execution.review_action(
            task_id,
            expected_version=self.execution.get(task_id).version,
            action="approve",
        )
        self.assertEqual(approved.phase, WorkflowPhase.EXECUTE)
        claim = self.execution.claim_next()
        self.execution.record_result(ExecutionResultEnvelope(
            result_id="e" * 32,
            task_id=task_id,
            task_version=1,
            workflow_version=claim.workflow_version,
            phase=WorkflowPhase.EXECUTE,
            claim_token=claim.token,
            outcome=ExecutionOutcome.AWAITING_EXTERNAL,
            summary="Reviewed it; two things to fix.",
            work_markdown="Synthetic work.",
            questions=(),
            external_actions=(
                {"action": "Post this review on the pull request",
                 "channel": "forge.example/acme/widget"},
            ),
            deliverables=(
                {"label": "review", "body": "Line one.\nSynthetic finding."},
            ),
        ))
        self.assertEqual(self.cards.schedule().created, 1)
        card = self.cards.claim_next().card
        self.assertEqual(card.kind, ExecutionCardKind.EXTERNAL_REVIEW)

        body, _keyboard = render_execution_review_card(card)
        self.assertIn("<b>review</b>", body)
        self.assertIn("Synthetic finding.", body)

    def test_external_review_requires_its_exact_separate_approval(self):
        self._external_review(1, "external-one")
        self._external_review(2, "external-two")
        self._external_review(3, "external-three")
        self.cards.schedule()
        targets = {
            "approve": (WorkflowStatus.QUEUED, WorkflowPhase.EXTERNAL_ACTION),
            "revise": (WorkflowStatus.QUEUED, WorkflowPhase.PLAN),
            "cancel": (WorkflowStatus.CANCELLED, WorkflowPhase.EXECUTE),
        }
        for action in ("approve", "revise", "cancel"):
            claim = self._claim_and_deliver()
            self.assertEqual(claim.card.kind, ExecutionCardKind.EXTERNAL_REVIEW)
            body, keyboard = render_execution_review_card(claim.card)
            self.assertIn(
                "<b>External action awaiting your approval:</b>", body)
            self.assertIn("Publish synthetic draft &lt;alpha&gt;.", body)
            self.assertEqual(
                keyboard["inline_keyboard"][0][0]["text"],
                "✅ Authorize action",
            )
            result = self.cards.act(
                claim.card.id,
                expected_version=claim.card.version,
                action=action,
            )
            self.assertEqual(
                (result.workflow_status, result.workflow_phase),
                targets[action],
            )

    def test_markdown_is_safe_readable_html(self):
        markdown = (
            "# Synthetic heading\n"
            "- **Important** & <unsafe>\n"
            "1. [Safe example](https://example.com/path?a=1&b=2)\n"
            "[Unsafe example](javascript:unsafe) and `sample code`"
        )
        self._plan_review(1, "markdown-plan", work_markdown=markdown)
        self.cards.schedule()
        claim = self._claim_and_deliver()

        body, _keyboard = render_execution_review_card(claim.card)

        self.assertIn("<b>Synthetic heading</b>", body)
        self.assertIn("• <b>Important</b> &amp; &lt;unsafe&gt;", body)
        self.assertIn(
            '<a href="https://example.com/path?a=1&amp;b=2">'
            "Safe example</a>",
            body,
        )
        self.assertIn("[Unsafe example](javascript:unsafe)", body)
        self.assertNotIn('href="javascript:', body)
        self.assertIn("<code>sample code</code>", body)
        self.assertNotIn("**Important**", body)

    def test_markdown_tables_are_safe_aligned_html_in_review_cards(self):
        markdown = (
            "| Option | Cost | State |\n"
            "| :--- | ---: | :---: |\n"
            "| **Example Alpha** | 7 | `Ready` |\n"
            "| [Example Beta](https://example.com) | 12 | Waiting |\n"
            "| <unsafe> | one \\| two | `x|y` |"
        )
        self._plan_review(1, "table-plan", work_markdown=markdown)
        self.cards.schedule()
        plan_claim = self._claim_and_deliver()

        plan_body, _keyboard = render_execution_review_card(plan_claim.card)

        self.assertIn("<pre>", plan_body)
        self.assertIn("│", plan_body)
        self.assertIn("─┼─", plan_body)
        self.assertIn("Example Alpha", plan_body)
        self.assertIn("&lt;unsafe&gt;", plan_body)
        self.assertIn("one | two", plan_body)
        self.assertIn("x|y", plan_body)
        self.assertNotIn("| :--- | ---: | :---: |", plan_body)
        self.assertNotIn("**Example Alpha**", plan_body)

        dropped = self.cards.act(
            plan_claim.card.id,
            expected_version=plan_claim.card.version,
            action="drop",
        )
        self.assertTrue(dropped.accepted)
        scheduled = self._schedule_workflow(2)
        self.execution.start_action(
            2, expected_version=scheduled.version, action="start"
        )
        self._record(
            2,
            phase=WorkflowPhase.PLAN,
            outcome=ExecutionOutcome.COMPLETED,
            result_id="table-result",
            work_markdown=markdown,
        )
        self.assertEqual(self.cards.schedule().created, 1)
        result_claim = self.cards.claim_next()
        self.assertEqual(result_claim.card.kind, ExecutionCardKind.RESULT_REVIEW)

        result_body, _keyboard = render_execution_review_card(result_claim.card)

        self.assertIn("<b>Outcome:</b> completed", result_body)
        self.assertIn("<pre>", result_body)
        self.assertIn("Example Beta", result_body)

    def test_a_card_names_its_task_and_shows_a_draft_in_full(self):
        """A reader approves what the card shows them.

        A deliverable rendered as its own name — "Prepared draft", a file
        path, a one-line summary of itself — asks the reader to authorise
        text they cannot see. The identifier matters for the same reason:
        a card that cannot be named cannot be referred to or found again.
        """
        task_id = 3
        scheduled = self._schedule_workflow(task_id)
        started = self.execution.start_action(
            task_id, expected_version=scheduled.version, action="start")
        claim = self.execution.claim_next()
        self.execution.record_result(ExecutionResultEnvelope(
            result_id="c" * 32,
            task_id=task_id,
            task_version=1,
            workflow_version=claim.workflow_version,
            phase=WorkflowPhase.PLAN,
            claim_token=claim.token,
            outcome=ExecutionOutcome.AWAITING_PLAN,
            summary="Draft prepared, awaiting contact details.",
            work_markdown=(
                "Synthetic plan. Review [PR 12]"
                "(https://github.com/example/project-alpha/pull/12)."
            ),
            questions=("What is their address?",),
            external_actions=(
                {"action": "Add them as a collaborator",
                 "requires": "Their handle"},
            ),
            deliverables=(
                {"label": "email", "recipient": "Someone",
                 "subject": "Synthetic subject",
                 "body": "First line.\nSecond line."},
            ),
            task_work_directory="/srv/example/Tasks/T3-synthetic-task",
            task_kb_file="/srv/example/KB/Tasks/T3-synthetic-task.md",
        ))
        self.assertEqual(self.cards.schedule().created, 1)
        card = self.cards.claim_next().card
        body, _keyboard = render_execution_review_card(card)

        self.assertIn(f"<code>T{task_id}</code>", body)
        self.assertIn("<b>Phase:</b> plan refinement", body)
        self.assertIn("<b>Agent:</b> General", body)
        self.assertIn("<b>Review files:</b>", body)
        self.assertIn("/srv/example/Tasks/T3-synthetic-task", body)
        self.assertIn("/srv/example/KB/Tasks/T3-synthetic-task.md", body)
        self.assertIn(
            'href="https://github.com/example/project-alpha/pull/12"', body
        )
        self.assertIn("<b>Needs your input:</b>", body)
        # The action says what is still missing, not just what it is.
        self.assertIn("Needs: Their handle", body)
        # The draft is readable on the card, headed and addressed.
        self.assertIn("<b>email</b>", body)
        self.assertIn("To: Someone", body)
        self.assertIn("Subject: Synthetic subject", body)
        self.assertIn("<pre>First line.\nSecond line.</pre>", body)

    def test_a_superseded_card_stops_holding_the_surface(self):
        """The reader must get the next card without anyone intervening.

        A delivered card whose work has moved on can no longer be answered
        — its buttons are refused. It used to keep counting as the one
        card on screen anyway, so nothing further was ever scheduled, and
        the sweep that cancels it runs inside the scheduling call the drip
        had already returned from. The surface went quiet permanently.
        """
        task_id = 6
        scheduled = self._schedule_workflow(task_id)
        self.assertEqual(self.cards.schedule().created, 1)
        claim = self.cards.claim_next()
        self.cards.complete_delivery(
            claim.card.id, expected_version=claim.card.version,
            claim_token=claim.token, transport="synthetic",
            delivery_ref="1")
        self.assertEqual(self.cards.stats().delivered, 1)

        # The reader starts the work from somewhere else, so the gate that
        # is still on screen now asks a question that has been answered.
        self.execution.start_action(
            task_id, expected_version=scheduled.version, action="start")

        self.assertEqual(self.cards.stats().delivered, 0)
        self.assertEqual(self.cards.stats().active, 0)
        # And the surface refills on its own, cancelling what it replaced.
        self.assertEqual(self.cards.schedule().cancelled, 1)

    def _park(self, task_id: int):
        """Fail a workflow until it parks, the way a broken run does."""
        self._schedule_workflow(task_id)
        started = self.execution.start_action(
            task_id, expected_version=self.execution.get(task_id).version,
            action="start")
        self.assertEqual(started.status, WorkflowStatus.QUEUED)
        for _ in range(3):
            # Each failure schedules the next attempt with a backoff, so
            # the clock has to reach it before the claim is available.
            self.clock.advance(timedelta(hours=1))
            claim = self.execution.claim_next()
            self.assertIsNotNone(claim)
            self.execution.fail(
                task_id,
                expected_version=claim.workflow_version,
                claim_token=claim.token,
                reason="process_exit",
            )
        return self.execution.get(task_id)

    def test_a_workflow_that_gave_up_says_so_instead_of_going_quiet(self):
        """Parking is the retry limiter, and it used to be terminal and
        silent: the task stayed open, its workflow was abandoned, and no
        card appeared anywhere. A reader waited for something the system
        had already stopped working on.

        Five workflows reached that state on one machine before anyone
        noticed, one of them holding a complete review.
        """
        task_id = 1
        parked = self._park(task_id)
        self.assertEqual(parked.status, WorkflowStatus.PARKED)

        self.assertEqual(self.cards.schedule().created, 1)
        card = self.cards.claim_next().card
        body, keyboard = render_execution_review_card(card)

        self.assertIn("Stopped after 3 failed attempt", body)
        self.assertIn("process_exit", body)
        actions = [
            parse_execution_review_callback(button["callback_data"])[2]
            for row in keyboard["inline_keyboard"] for button in row
        ]
        self.assertIn("start", actions)

    def test_a_workflow_parked_after_a_result_still_produces_a_card(self):
        """A planning pass can succeed and a later attempt still park, so a
        parked workflow may carry a result from earlier.

        Matching the eligibility query while matching no card kind raised,
        and the raise happened inside the scheduling sweep — which took
        down every other card with it. One machine produced no cards at
        all because a single workflow was in this state.
        """
        task_id = 1
        self._plan_review(task_id, "parked-with-result")
        # Send it back for another pass, then fail that pass until it parks.
        self.execution.review_action(
            task_id,
            expected_version=self.execution.get(task_id).version,
            action="revise")
        for _ in range(3):
            self.clock.advance(timedelta(hours=1))
            claim = self.execution.claim_next()
            self.assertIsNotNone(claim)
            self.execution.fail(
                task_id,
                expected_version=claim.workflow_version,
                claim_token=claim.token,
                reason="process_exit")
        workflow = self.execution.get(task_id)
        self.assertEqual(workflow.status, WorkflowStatus.PARKED)
        self.assertIsNotNone(workflow.last_result_id)

        # The sweep must not raise, and must produce the card.
        self.assertEqual(self.cards.schedule().created, 1)
        body, _keyboard = render_execution_review_card(
            self.cards.claim_next().card)
        self.assertIn("Stopped after 3 failed attempt", body)

    def _park_in_execute(self, task_id: int):
        """Plan, get approved, then fail the execute phase until it parks."""
        self._plan_review(task_id, f"plan-{task_id}")
        approved = self.execution.review_action(
            task_id,
            expected_version=self.execution.get(task_id).version,
            action="approve",
        )
        self.assertEqual(approved.phase, WorkflowPhase.EXECUTE)
        for _ in range(3):
            self.clock.advance(timedelta(hours=1))
            claim = self.execution.claim_next()
            self.assertIsNotNone(claim)
            self.execution.fail(
                task_id,
                expected_version=claim.workflow_version,
                claim_token=claim.token,
                reason="process_exit",
            )
        return self.execution.get(task_id)

    def test_a_workflow_that_parks_after_planning_still_says_so(self):
        """Parking during `execute` used to produce no card at all.

        Eligibility only matched a park in `plan`, because the schema
        pinned a start card there, so a workflow that planned, was
        approved, and then gave up went completely quiet. The reader saw a
        task that simply stopped, with nothing to answer.
        """
        task_id = 1
        parked = self._park_in_execute(task_id)
        self.assertEqual(parked.status, WorkflowStatus.PARKED)
        self.assertEqual(parked.phase, WorkflowPhase.EXECUTE)

        self.assertEqual(self.cards.schedule().created, 1)
        card = self.cards.claim_next().card
        body, keyboard = render_execution_review_card(card)

        self.assertIn("Stopped after 3 failed attempt", body)
        self.assertIn("process_exit", body)
        # It planned, was approved and was attempted, so it is not "not
        # started" — saying so would hide the history the reader needs to
        # judge whether another attempt is worth it.
        self.assertNotIn("not started", body)
        actions = [
            parse_execution_review_callback(button["callback_data"])[2]
            for row in keyboard["inline_keyboard"] for button in row
        ]
        self.assertIn("start", actions)

    def test_continuing_a_late_park_does_not_discard_the_approved_plan(self):
        """`start` used to reset the phase to `plan` unconditionally.

        For a workflow parked in `execute` that throws away a plan the
        reader already read and approved, and silently asks the agent to
        redo accepted work.
        """
        task_id = 1
        self._park_in_execute(task_id)
        self.assertEqual(self.cards.schedule().created, 1)
        card = self.cards.claim_next().card

        resumed = self.execution.start_action(
            task_id,
            expected_version=self.execution.get(task_id).version,
            action="start",
        )
        self.assertEqual(resumed.status, WorkflowStatus.QUEUED)
        self.assertEqual(resumed.phase, WorkflowPhase.EXECUTE)
        # And the retry starts with a full set of attempts.
        workflow = self.execution.get(task_id)
        self.assertEqual(workflow.phase, WorkflowPhase.EXECUTE)
        self.assertIsNone(card.result_id)

    def test_a_park_during_planning_still_resumes_planning(self):
        """The phase is preserved, not advanced: a park in `plan` must
        still come back as `plan`."""
        task_id = 1
        parked = self._park(task_id)
        self.assertEqual(parked.phase, WorkflowPhase.PLAN)
        resumed = self.execution.start_action(
            task_id,
            expected_version=self.execution.get(task_id).version,
            action="start",
        )
        self.assertEqual(
            (resumed.status, resumed.phase),
            (WorkflowStatus.QUEUED, WorkflowPhase.PLAN),
        )

    def test_one_unrenderable_workflow_cannot_silence_the_others(self):
        # The sweep classifies every eligible row, so anything that raises
        # mid-sweep costs every card behind it, not just its own.
        self._schedule_workflow(2)
        self._schedule_workflow(3)
        created = self.cards.schedule(limit=6).created
        self.assertGreaterEqual(created, 2)

    def test_a_parked_workflow_tries_again_on_its_own(self):
        """Parking stops the immediate retries; it is not a decision to
        abandon the work. A run of failures is often something passing — an
        unreachable forge, a machine under load — and a reader who never
        answers the card should still have the work attempted.

        The reader is told either way: parking raises a card, and a failed
        second round raises another.
        """
        task_id = 1
        parked = self._park(task_id)
        self.assertEqual(parked.status, WorkflowStatus.PARKED)

        # Not immediately: that would be the retry loop parking prevented.
        self.assertIsNone(self.execution.claim_next())

        self.clock.advance(PARK_RETRY_INTERVAL + timedelta(minutes=1))
        claim = self.execution.claim_next()

        self.assertIsNotNone(claim)
        self.assertEqual(claim.task_id, task_id)
        # A fresh round, or the first slip would park it again at once.
        self.assertEqual(self.execution.get(task_id).failure_count, 0)

    def test_parking_still_raises_a_card_before_it_retries(self):
        # The automatic round must not replace telling the reader. They
        # decide whether the work is still wanted; the retry only means
        # nobody has to notice for it to be attempted again.
        task_id = 1
        self._park(task_id)
        self.assertEqual(self.cards.schedule().created, 1)
        body, _keyboard = render_execution_review_card(
            self.cards.claim_next().card)
        self.assertIn("Stopped after 3 failed attempt", body)

    def test_trying_again_gives_a_full_set_of_attempts(self):
        # Restarting with the count still at its limit would park again on
        # the first slip, which is a retry in name only.
        task_id = 1
        self._park(task_id)
        self.assertEqual(self.cards.schedule().created, 1)
        claim = self._claim_and_deliver()

        restarted = self.cards.act(
            claim.card.id, expected_version=claim.card.version,
            action="start")

        self.assertTrue(restarted.accepted, restarted.refusal)
        self.assertEqual(restarted.workflow_status, WorkflowStatus.QUEUED)
        self.assertEqual(self.execution.get(task_id).failure_count, 0)

    def test_a_brief_describes_the_work_without_naming_this_machine(self):
        """A brief is pasted into some other agent, so it must describe the
        work rather than this pipeline. The prompt this agent runs is
        useless there: it names local commands that do not exist and a
        recording protocol that cannot be followed.
        """
        task_id = 1
        self._plan_review(task_id, "brief-plan")
        self.assertEqual(self.cards.schedule().created, 1)
        claim = self.cards.claim_next()

        result = self.cards.brief(
            claim.card.id, expected_version=claim.card.version)

        self.assertTrue(result.accepted, result.refusal)
        text = result.text
        self.assertIn("Synthetic task 1", text)
        self.assertIn("Proceed with Example A?", text)
        # Nothing only this machine can act on.
        for local in ("foxhound-task-worker", "act worktree", "record ",
                      "claim_token", str(self.database)):
            with self.subTest(local=local):
                self.assertNotIn(local, text)

        enriched = replace(
            claim.card,
            origin_kind="issue",
            origin_record="github.com/example-org/example-repo",
            origin_item="42",
            origin_sources=(CardSourceEvidence(
                "issue-42.md",
                "issue_body",
                "Synthetic acceptance criterion from the issue body.",
            ),),
        )
        enriched_text = task_brief(enriched)
        self.assertIn(
            "https://github.com/example-org/example-repo/issues/42",
            enriched_text,
        )
        self.assertIn("issue-42.md (issue body)", enriched_text)
        self.assertIn("Synthetic acceptance criterion", enriched_text)
        enriched_body, _ = render_execution_review_card(enriched)
        self.assertIn(
            '<a href="https://github.com/example-org/example-repo/issues/42">',
            enriched_body,
        )
        self.assertIn("issue-42.md", enriched_body)

        long_brief = task_brief(replace(
            enriched,
            work_markdown="Synthetic detail. " * MAX_BRIEF_CHARS,
        ))
        self.assertLessEqual(len(long_brief), MAX_BRIEF_CHARS)
        self.assertLessEqual(len(long_brief.encode("utf-8")), MAX_BRIEF_BYTES)
        self.assertTrue(long_brief.endswith("Open the source for the rest.]"))

        multibyte_brief = task_brief(replace(
            enriched,
            work_markdown="Synthetic detail é. " * MAX_BRIEF_CHARS,
        ))
        self.assertLessEqual(len(multibyte_brief), MAX_BRIEF_CHARS)
        self.assertLessEqual(
            len(multibyte_brief.encode("utf-8")), MAX_BRIEF_BYTES
        )
        self.assertTrue(
            multibyte_brief.endswith("Open the source for the rest.]")
        )

        deceptive = replace(
            enriched,
            origin_record="github.com.example/example-org/example-repo",
        )
        deceptive_text = task_brief(deceptive)
        self.assertNotIn("https://github.com.example", deceptive_text)
        self.assertNotIn("github.com.example", deceptive_text)
        self.assertIn("Source: Issue", deceptive_text)

    def test_asking_for_a_brief_answers_nothing(self):
        # A reader deciding to take the work elsewhere has not answered the
        # card, so the card must still be there when they come back.
        task_id = 1
        self._plan_review(task_id, "brief-read")
        self.assertEqual(self.cards.schedule().created, 1)
        claim = self.cards.claim_next()
        before = self.cards.stats()

        self.cards.brief(claim.card.id, expected_version=claim.card.version)

        self.assertEqual(self.cards.stats(), before)

    def test_a_brief_for_a_card_that_has_moved_on_is_refused(self):
        task_id = 1
        self._plan_review(task_id, "brief-stale")
        self.assertEqual(self.cards.schedule().created, 1)
        claim = self.cards.claim_next()

        stale = self.cards.brief(
            claim.card.id, expected_version=claim.card.version + 5)

        self.assertFalse(stale.accepted)
        self.assertEqual(stale.refusal, ExecutionCardRefusal.STALE_VERSION)
        self.assertEqual(stale.text, "")

    def test_a_gate_says_which_issue_it_is_asking_about(self):
        """Naming the task is not naming the thing.

        Two issues can share a title, and the number is what the reader
        searches for afterwards. Without it the reader is asked to
        authorise work they would have to go and look up first.
        """
        task_id = 6
        payload = json.dumps({"task": {"project": "Project Alpha"}})
        with closing(sqlite3.connect(self.database)) as connection:
            connection.execute(
                "INSERT INTO candidate_inbox(candidate_id,source_system,"
                "source_kind,source_record_id,source_item_id,source_revision,"
                "payload_json,created_at,first_imported_at,updated_at) "
                "VALUES('c1','gw','issue','forge.example/acme/widget','42',"
                "?,?,'2030-01-01T00:00:00Z','2030-01-01T00:00:00Z',"
                "'2030-01-01T00:00:00Z')", ("b" * 64, payload))
            connection.execute(
                "INSERT INTO candidate_revision_history(candidate_id,"
                "source_revision,payload_json,created_at,imported_at) "
                "VALUES('c1',?,?,?,?)",
                ("b" * 64, payload, "2030-01-01T00:00:00Z",
                 "2030-01-01T00:00:00Z"),
            )
            connection.execute(
                "INSERT INTO task_candidate_bindings(candidate_id,"
                "source_revision,task_id,relation,decided_at) "
                "VALUES('c1',?,?,'accepted','2030-01-01T00:00:00Z')",
                ("b" * 64, task_id))
            connection.commit()
        self._schedule_workflow(task_id)
        self.assertEqual(self.cards.schedule().created, 1)
        body, _keyboard = render_execution_review_card(
            self.cards.claim_next().card)
        self.assertIn("widget #42", body)
        self.assertNotIn("Project Alpha", body)
        self.assertIn("<b>First raised:</b> 2030-01-01", body)

    def test_the_card_asks_the_policy_which_origins_are_addressable(self):
        """Presentation is declared beside the authority each source has,
        not compared by name where a card happens to be built. A source
        whose origin is not addressable is still named — a reader can
        search for a name, and a link that does not resolve is worse than
        none — but it is never offered as a link.
        """
        from foxhound.execution_cards import ADDRESSABLE_ORIGINS
        from foxhound.source_policy import SOURCE_POLICIES

        self.assertEqual(
            ADDRESSABLE_ORIGINS,
            {kind for kind, policy in SOURCE_POLICIES.items()
             if policy.addressable_origin},
        )
        # The kinds a reader actually sees today, decided deliberately.
        self.assertIn("issue", ADDRESSABLE_ORIGINS)
        for kind in ("meeting", "legacy", "email", "teams"):
            with self.subTest(kind=kind):
                self.assertNotIn(kind, ADDRESSABLE_ORIGINS)

    def test_a_second_look_names_what_it_continues(self):
        """A pull request reviewed twice is two tasks, because reviewing it
        at one state and at a later one are two jobs. Without naming the
        first, an agent starts from nothing every time it moves, and a
        reader cannot tell a second pass from a duplicate card.
        """
        def bind(task_id, candidate_id, item_id):
            with closing(sqlite3.connect(self.database)) as connection:
                connection.execute(
                    "INSERT INTO candidate_inbox(candidate_id,source_system,"
                    "source_kind,source_record_id,source_item_id,"
                    "source_revision,payload_json,created_at,"
                    "first_imported_at,updated_at) "
                    "VALUES(?,'gw','review_request',"
                    "'forge.example/acme/widget',?,?,'{}',"
                    "'2030-01-01T00:00:00Z','2030-01-01T00:00:00Z',"
                    "'2030-01-01T00:00:00Z')",
                    (candidate_id, item_id, "b" * 64))
                connection.execute(
                    "INSERT INTO task_candidate_bindings(candidate_id,"
                    "source_revision,task_id,relation,decided_at) "
                    "VALUES(?,?,?,'accepted','2030-01-01T00:00:00Z')",
                    (candidate_id, "b" * 64, task_id))
                connection.commit()

        # Two states of pull request 7, and one of a different pull request
        # in the same repository, which must not be mistaken for a prior.
        bind(4, "cand-first", "7/2030-01-02T10:00:00Z")
        bind(6, "cand-other", "9/2030-01-02T10:00:00Z")
        bind(5, "cand-second", "7/2030-01-03T09:00:00Z")

        self._schedule_workflow(5)
        self.assertEqual(self.cards.schedule().created, 1)
        card = self.cards.claim_next().card

        self.assertEqual(card.prior_task_id, 4)
        body, _keyboard = render_execution_review_card(card)
        self.assertIn("Continues T4", body)

    def test_a_recorded_relation_reaches_the_card(self):
        # The derived predecessor above is inferred from source identity and
        # cannot reach across sources. A recorded one can, carries a basis,
        # and can be withdrawn — and both render through one path.
        task_id = 6
        self._schedule_workflow(task_id)
        self.assertEqual(self.cards.schedule().created, 1)
        with closing(sqlite3.connect(self.database)) as connection:
            task_relations.assert_relation(
                connection,
                subject_id=task_id,
                object_id=4,
                kind="supersedes",
                basis="the same ask, raised again from a different source",
                asserted_by="reader",
                note="folded in after the second meeting",
            )
            connection.commit()
        card = self.cards.claim_next().card
        body, _keyboard = render_execution_review_card(card)
        self.assertIn("Continues T4", body)
        self.assertIn("folded in after the second meeting", body)

    def test_one_pair_is_named_once(self):
        # A recorded relation and the derived predecessor can name the same
        # pair. Two sentences about it is worse than either alone.
        task_id = 6
        with closing(sqlite3.connect(self.database)) as connection:
            task_relations.assert_relation(
                connection,
                subject_id=task_id,
                object_id=4,
                kind="supersedes",
                basis="recorded, and also derivable",
                asserted_by="machine",
            )
            connection.commit()
        self._schedule_workflow(task_id)
        self.assertEqual(self.cards.schedule().created, 1)
        card = self.cards.claim_next().card
        body, _keyboard = render_execution_review_card(card)
        self.assertEqual(body.count("Continues T4"), 1)

    def test_a_withdrawn_relation_leaves_the_card(self):
        task_id = 6
        with closing(sqlite3.connect(self.database)) as connection:
            relation = task_relations.assert_relation(
                connection,
                subject_id=task_id,
                object_id=4,
                kind="duplicate_of",
                basis="the same ask from two sources",
                asserted_by="machine",
            )
            task_relations.withdraw(
                connection, relation.id, withdrawn_by="reader")
            connection.commit()
        self._schedule_workflow(task_id)
        self.assertEqual(self.cards.schedule().created, 1)
        card = self.cards.claim_next().card
        self.assertEqual(card.relations, ())
        body, _keyboard = render_execution_review_card(card)
        self.assertNotIn("Same task as T4", body)

    def test_a_first_look_continues_nothing(self):
        # An ordinary state, and it must not be reported as a second pass.
        task_id = 6
        self._schedule_workflow(task_id)
        self.assertEqual(self.cards.schedule().created, 1)
        card = self.cards.claim_next().card
        self.assertIsNone(card.prior_task_id)
        body, _keyboard = render_execution_review_card(card)
        self.assertNotIn("Continues", body)

    def test_an_unlinkable_origin_is_still_named(self):
        # A meeting record has no address a reader can open. Naming it is
        # still better than silence, and a wrong link is worse than none.
        task_id = 6
        with closing(sqlite3.connect(self.database)) as connection:
            connection.execute(
                "INSERT INTO candidate_inbox(candidate_id,source_system,"
                "source_kind,source_record_id,source_item_id,source_revision,"
                "payload_json,created_at,first_imported_at,updated_at) "
                "VALUES('c2','gw','meeting','record_synthetic','action-1',"
                "?,'{}','2030-01-01T00:00:00Z','2030-01-01T00:00:00Z',"
                "'2030-01-01T00:00:00Z')", ("b" * 64,))
            connection.execute(
                "INSERT INTO task_candidate_bindings(candidate_id,"
                "source_revision,task_id,relation,decided_at) "
                "VALUES('c2',?,?,'accepted','2030-01-01T00:00:00Z')",
                ("b" * 64, task_id))
            connection.commit()
        self._schedule_workflow(task_id)
        self.assertEqual(self.cards.schedule().created, 1)
        body, _keyboard = render_execution_review_card(
            self.cards.claim_next().card)
        self.assertIn("record_synthetic", body)
        self.assertNotIn("<a href", body)

    def test_bounded_source_evidence_replaces_an_opaque_meeting_record(self):
        task_id = 6
        record_id = "record_" + "a" * 32
        item_id = "action-001"
        candidate_id = candidate_id_for(
            system="gw", kind="meeting", record_id=record_id,
            item_id=item_id,
        )
        revision = "c" * 64
        candidate = {
            "schema": "foxhound.task-candidate",
            "schema_version": 4,
            "candidate_id": candidate_id,
            "source": {
                "system": "gw", "kind": "meeting",
                "record_id": record_id, "item_id": item_id,
                "revision": revision,
            },
            "task": {
                "text": "Prepare the Project Alpha summary",
                "owner": "Person A", "due": None,
            },
            "evidence": {
                "document_id": record_id,
                "locator": "action-item-001",
                "sources": [
                    {
                        "name": "20300102_example_protocol.md",
                        "role": "protocol",
                        "extract": "Summary:\nA report was requested.\n\nAction item:\nPrepare it.",
                    },
                    {
                        "name": "20300102_example_transcript.txt",
                        "role": "transcript",
                        "extract": "[Person A] I will prepare the report.",
                    },
                ],
            },
            "created_at": "2030-01-01T00:00:00Z",
        }
        payload = json.dumps(candidate, separators=(",", ":"), sort_keys=True)
        with closing(sqlite3.connect(self.database)) as connection:
            connection.execute(
                "INSERT INTO candidate_inbox(candidate_id,source_system,"
                "source_kind,source_record_id,source_item_id,source_revision,"
                "payload_json,created_at,first_imported_at,updated_at) "
                "VALUES(?,?,?,?,?,?,?,?,?,?)",
                (candidate_id, "gw", "meeting", record_id, item_id, revision,
                 payload, "2030-01-01T00:00:00Z",
                 "2030-01-01T00:00:00Z", "2030-01-01T00:00:00Z"),
            )
            connection.execute(
                "INSERT INTO candidate_revision_history(candidate_id,"
                "source_revision,payload_json,created_at,imported_at) "
                "VALUES(?,?,?,?,?)",
                (candidate_id, revision, payload,
                 "2030-01-01T00:00:00Z", "2030-01-01T00:00:00Z"),
            )
            connection.execute(
                "INSERT INTO task_candidate_bindings(candidate_id,"
                "source_revision,task_id,relation,decided_at) "
                "VALUES(?,?,?,'accepted','2030-01-01T00:00:00Z')",
                (candidate_id, revision, task_id),
            )
            connection.commit()

        self._schedule_workflow(task_id)
        self.assertEqual(self.cards.schedule().created, 1)
        body, _keyboard = render_execution_review_card(
            self.cards.claim_next().card
        )

        self.assertIn("<b>From:</b> Meeting", body)
        self.assertIn("20300102_example_protocol.md", body)
        self.assertIn("20300102_example_transcript.txt", body)
        self.assertIn("A report was requested", body)
        self.assertIn("I will prepare the report", body)
        self.assertNotIn(record_id, body)

    def test_a_gate_can_be_reassigned_dropped_or_discussed(self):
        """A task is most often noticed as someone else's when offered.

        The gate used to accept only start, snooze and cancel, so "not
        mine" was indistinguishable from "not real" and the task was lost
        for whoever it actually belonged to.
        """
        def delivered_gate(task_id):
            self._schedule_workflow(task_id)
            self.assertEqual(self.cards.schedule().created, 1)
            claim = self.cards.claim_next()
            self.assertEqual(claim.card.kind, ExecutionCardKind.START)
            delivered = self.cards.complete_delivery(
                claim.card.id, expected_version=claim.card.version,
                claim_token=claim.token, transport="synthetic",
                delivery_ref="1")
            return claim.card.id, delivered.card_version

        # One input answers a card, so each is exercised on its own gate.
        card_id, version = delivered_gate(5)
        noted = self.cards.submit_input(
            card_id, expected_version=version, kind="discussion",
            value="Check the deployment story first.")
        self.assertTrue(noted.accepted, noted.refusal)
        self.assertEqual(noted.workflow_status, WorkflowStatus.AWAITING_START)
        self.assertEqual(self.cards.schedule().created, 1)
        replacement = self._claim_and_deliver()
        started = self.cards.act(
            replacement.card.id,
            expected_version=replacement.card.version,
            action="start",
        )
        self.assertEqual(started.workflow_status, WorkflowStatus.QUEUED)
        run = self.execution.claim_next()
        self.assertEqual(
            self.execution.reader_instruction(
                5,
                expected_version=run.workflow_version,
                claim_token=run.token,
            ),
            "Check the deployment story first.",
        )
        self.assertTrue(
            self.ledger.transition(
                5, expected_version=1, action="done"
            ).accepted
        )
        self.assertIsNone(self.execution.claim_next())

        card_id, version = delivered_gate(6)
        moved = self.cards.submit_input(
            card_id, expected_version=version,
            kind="reassignment", value="Person B")
        self.assertTrue(moved.accepted, moved.refusal)

    def test_a_start_card_never_claims_work_has_begun(self):
        # The phase names what WOULD run. On a gate, naming it reads as
        # though it already had.
        task_id = 4
        self._schedule_workflow(task_id)
        self.assertEqual(self.cards.schedule().created, 1)
        body, _keyboard = render_execution_review_card(
            self.cards.claim_next().card)
        self.assertIn("<b>Start this task?</b>", body)
        self.assertNotIn("<b>Phase:</b>", body)
        self.assertNotIn("plan refinement", body)
        self.assertIn("No agent has looked at this yet.", body)
        self.assertIn("<b>Continue</b> starts the investigation.", body)
        # The gate names its agent. A reader who cannot see it cannot tell
        # that a pull request is about to be reviewed by a compatibility
        # profile, which is how one review was lost.
        self.assertIn("<b>Agent:</b>", body)

    def test_every_post_run_card_identifies_its_bound_agent(self):
        self._plan_review(1, "agent-plan")
        self.assertEqual(self.cards.schedule().created, 1)
        plan = self.cards.claim_next().card
        plan_body, _ = render_execution_review_card(plan)
        self.assertIn("<b>Agent:</b> General", plan_body)

        self._external_review(2, "agent-external")
        self.assertEqual(self.cards.schedule().created, 1)
        external = self.cards.claim_next().card
        external_body, _ = render_execution_review_card(external)
        self.assertIn("<b>Agent:</b> General", external_body)

        self._plan_review(3, "agent-result-plan")
        approved = self.execution.review_action(
            3,
            expected_version=self.execution.get(3).version,
            action="approve",
        )
        result = self._record(
            3,
            phase=WorkflowPhase.EXECUTE,
            outcome=ExecutionOutcome.COMPLETED,
            result_id="agent-result",
        )
        self.assertTrue(result.accepted)
        self.assertGreater(result.version, approved.version)
        self.assertEqual(self.cards.schedule().created, 1)
        completed = self.cards.claim_next().card
        completed_body, _ = render_execution_review_card(completed)
        self.assertIn("<b>Agent:</b> General", completed_body)

        oversized = ExecutionReviewCard(
            **{
                **plan.__dict__,
                "work_markdown": "Synthetic plan. " * 4_000,
            }
        )
        oversized_body, _ = render_execution_review_card(oversized)
        self.assertIn("Agent: General", oversized_body)
        self.assertNotIn("<b>Agent:</b>", oversized_body)

    def test_project_metadata_is_absent_from_start_and_review_headers(self):
        payload = json.dumps({"task": {"project": "Project Alpha"}})
        revision = "e" * 64
        with closing(sqlite3.connect(self.database)) as connection:
            connection.execute(
                "INSERT INTO candidate_inbox(candidate_id,source_system,"
                "source_kind,source_record_id,source_item_id,source_revision,"
                "payload_json,created_at,first_imported_at,updated_at) "
                "VALUES('project-card','gw','meeting','record-alpha',"
                "'action-alpha',?,?, '2030-01-01T00:00:00Z',"
                "'2030-01-01T00:00:00Z','2030-01-01T00:00:00Z')",
                (revision, payload),
            )
            connection.execute(
                "INSERT INTO candidate_revision_history(candidate_id,"
                "source_revision,payload_json,created_at,imported_at) "
                "VALUES('project-card',?,?,?,?)",
                (revision, payload, "2030-01-01T00:00:00Z",
                 "2030-01-01T00:00:00Z"),
            )
            connection.execute(
                "INSERT INTO task_candidate_bindings(candidate_id,"
                "source_revision,task_id,relation,decided_at) "
                "VALUES('project-card',?,1,'accepted',"
                "'2030-01-01T00:00:00Z')",
                (revision,),
            )
            connection.commit()

        self._schedule_workflow(1)
        self.assertEqual(self.cards.schedule().created, 1)
        start = self._claim_and_deliver()
        start_body, _keyboard = render_execution_review_card(start.card)
        self.assertNotIn("Project Alpha", start_body)
        started = self.cards.act(
            start.card.id,
            expected_version=start.card.version,
            action="start",
        )
        self._record(
            1,
            phase=WorkflowPhase.PLAN,
            outcome=ExecutionOutcome.AWAITING_PLAN,
            result_id="projectless-header-plan",
        )
        self.assertEqual(self.cards.schedule().created, 1)
        review = self.cards.claim_next()
        review_body, _keyboard = render_execution_review_card(review.card)
        self.assertNotIn("Project Alpha", review_body)
        self.assertEqual(started.workflow_status, WorkflowStatus.QUEUED)

    def test_a_plain_line_still_renders_after_records_arrived(self):
        # Every result written before records existed is still a list of
        # sentences, and must keep rendering as one.
        task_id = 5
        self._plan_review(task_id, "d" * 32)
        self.assertEqual(self.cards.schedule().created, 1)
        body, _keyboard = render_execution_review_card(
            self.cards.claim_next().card)
        self.assertIn("• Publish synthetic draft &lt;alpha&gt;.", body)
        self.assertIn("• Proceed with Example A?", body)
        self.assertNotIn("Needs:", body)
        self.assertNotIn("<pre>Synthetic deliverable</pre>", body)

    def test_unbounded_table_like_markdown_remains_ordinary_text(self):
        header = " | ".join(f"Column {number}" for number in range(13))
        delimiter = " | ".join("---" for _ in range(13))
        self._plan_review(
            1,
            "unbounded-table-plan",
            work_markdown=f"{header}\n{delimiter}",
        )
        self.cards.schedule()
        claim = self.cards.claim_next()

        body, _keyboard = render_execution_review_card(claim.card)

        self.assertNotIn("<pre>", body)
        self.assertIn("Column 12", body)
        self.assertIn("--- | ---", body)

    def test_complete_multi_message_review_retains_approval(self):
        markdown = "\n".join(
            f"- Synthetic review line {index} with **detail**."
            for index in range(220)
        )
        self._plan_review(1, "multi-message-plan", work_markdown=markdown)
        self.cards.schedule()
        claim = self._claim_and_deliver()

        body, keyboard = render_execution_review_card(claim.card)
        actions = [
            parse_execution_review_callback(button["callback_data"])[2]
            for row in keyboard["inline_keyboard"]
            for button in row
        ]

        self.assertGreater(len(body), 4_096)
        self.assertLessEqual(len(body.encode("utf-8")), MAX_CARD_BODY_BYTES)
        self.assertIn("Synthetic review line 219", body)
        self.assertIn("approve", actions)
        self.assertNotIn("Approval is disabled", body)

    def test_complete_review_has_transport_safe_html_lines(self):
        markdown = "**" + ("&" * 1_000) + "**"
        self._plan_review(1, "single-line-plan", work_markdown=markdown)
        self.cards.schedule()
        claim = self._claim_and_deliver()

        body, keyboard = render_execution_review_card(claim.card)

        self.assertLessEqual(max(map(len, body.splitlines())), 3_000)
        self.assertEqual(body.count("<b>"), body.count("</b>"))
        self.assertEqual(body.count("<i>"), body.count("</i>"))
        self.assertTrue(any(
            button["text"] == "▶️ Execute plan"
            for row in keyboard["inline_keyboard"]
            for button in row
        ))

    def test_invalid_or_stale_actions_change_neither_card_nor_workflow(self):
        workflow = self._schedule_workflow(1)
        self.cards.schedule()
        claim = self._claim_and_deliver()
        before = (
            self.execution.get(1),
            self.cards.event_count(),
        )
        wrong = self.cards.act(
            claim.card.id,
            expected_version=claim.card.version,
            action="approve",
        )
        self.assertEqual(wrong.refusal, ExecutionCardRefusal.INVALID_ACTION)
        self.assertEqual(
            (self.execution.get(1), self.cards.event_count()), before
        )

        self.execution.start_action(
            1, expected_version=workflow.version, action="cancel"
        )
        stale_before = self.cards.event_count()
        stale = self.cards.act(
            claim.card.id,
            expected_version=claim.card.version,
            action="start",
        )
        self.assertEqual(stale.refusal, ExecutionCardRefusal.STALE_VERSION)
        self.assertEqual(self.cards.event_count(), stale_before)
        cleanup = self.cards.schedule()
        self.assertEqual(cleanup.cancelled, 1)

        self._schedule_workflow(2)
        self.cards.schedule()
        claim = self._claim_and_deliver()
        self.ledger.transition(2, expected_version=1, action="done")
        stale = self.cards.act(
            claim.card.id,
            expected_version=claim.card.version,
            action="start",
        )
        self.assertEqual(stale.refusal, ExecutionCardRefusal.STALE_VERSION)

    def test_card_update_failure_rolls_back_the_workflow_transition(self):
        self._schedule_workflow(1)
        self.cards.schedule()
        claim = self._claim_and_deliver()
        with closing(sqlite3.connect(self.database)) as connection:
            connection.execute(
                "CREATE TRIGGER synthetic_refuse_execution_card_update "
                "BEFORE UPDATE ON execution_review_cards "
                "BEGIN SELECT RAISE(ABORT, 'synthetic refusal'); END"
            )
            connection.commit()
        with self.assertRaises(sqlite3.IntegrityError):
            self.cards.act(
                claim.card.id,
                expected_version=claim.card.version,
                action="start",
            )
        self.assertEqual(
            self.execution.get(1).status, WorkflowStatus.AWAITING_START
        )
        self.assertEqual(self.cards.stats().delivered, 1)

    def test_events_are_append_only_and_completed_work_gets_result_card(self):
        scheduled = self._schedule_workflow(1)
        self.execution.start_action(
            1, expected_version=scheduled.version, action="start"
        )
        self._record(
            1,
            phase=WorkflowPhase.PLAN,
            outcome=ExecutionOutcome.COMPLETED,
            result_id="completed-plan",
        )
        self.assertEqual(self.cards.schedule().created, 1)
        claim = self.cards.claim_next()
        self.assertEqual(claim.card.kind, ExecutionCardKind.RESULT_REVIEW)
        body, keyboard = render_execution_review_card(claim.card)
        self.assertIn("<b>Task workflow</b>", body)
        self.assertIn("<b>Outcome:</b> completed", body)
        self.assertEqual(
            [
                parse_execution_review_callback(button["callback_data"])[2]
                for row in keyboard["inline_keyboard"]
                for button in row
            ],
            ["done", "discuss", "snooze", "reassign", "drop", "brief"],
        )
        self.cards.complete_delivery(
            claim.card.id,
            expected_version=claim.card.version,
            claim_token=claim.token,
            transport="synthetic",
            delivery_ref="result-message",
        )
        completed = self.cards.act(
            claim.card.id,
            expected_version=claim.card.version,
            action="done",
        )
        self.assertEqual(completed.workflow_status, WorkflowStatus.COMPLETED)
        self.assertEqual(self.ledger.get(1).status, TaskStatus.DONE)

        self._schedule_workflow(2)
        self.cards.schedule()
        before = self.cards.event_count()
        with self.assertRaises(sqlite3.IntegrityError):
            with closing(sqlite3.connect(self.database)) as connection:
                connection.execute(
                    "UPDATE execution_review_card_events SET kind='cancelled'"
                )
        with self.assertRaises(sqlite3.IntegrityError):
            with closing(sqlite3.connect(self.database)) as connection:
                connection.execute("DELETE FROM execution_review_card_events")
        self.assertEqual(self.cards.event_count(), before)

    def test_a_gate_can_be_snoozed_for_a_chosen_interval(self):
        """A gate is asked before any work has been done, which makes it the
        card most likely to be deferred — and "tomorrow" is rarely the right
        answer for something waiting on another person, a release, or a
        month end. The review cards already accept these intervals; only
        the gate did not, so the reader could defer it one day at a time.
        """
        for task_id, action, days in (
            (2, "snooze_7d", 7), (3, "snooze_30d", 30),
        ):
            with self.subTest(action=action):
                self._schedule_workflow(task_id)
                self.cards.schedule()
                claim = self._claim_and_deliver()
                self.assertEqual(claim.card.kind, ExecutionCardKind.START)

                snoozed = self.cards.act(
                    claim.card.id,
                    expected_version=claim.card.version,
                    action=action,
                )

                self.assertTrue(snoozed.accepted, snoozed.refusal)
                self.assertEqual(
                    snoozed.workflow_status, WorkflowStatus.SNOOZED)
                self.assertEqual(
                    snoozed.wake_at,
                    (NOW + timedelta(days=days)).isoformat(
                        timespec="seconds"),
                )

    def test_a_gate_still_takes_the_plain_snooze(self):
        # The control the reader taps sends `snooze`; the interval arrives
        # from the picker that opens. Both have to keep working.
        self._schedule_workflow(4)
        self.cards.schedule()
        claim = self._claim_and_deliver()
        snoozed = self.cards.act(
            claim.card.id,
            expected_version=claim.card.version,
            action="snooze",
        )
        self.assertTrue(snoozed.accepted, snoozed.refusal)
        self.assertEqual(snoozed.workflow_status, WorkflowStatus.SNOOZED)

    def test_review_snooze_is_durable_and_resumes_the_same_gate(self):
        self._plan_review(1, "snooze-plan")
        self.cards.schedule()
        claim = self._claim_and_deliver()

        snoozed = self.cards.act(
            claim.card.id,
            expected_version=claim.card.version,
            action="snooze_7d",
        )

        self.assertEqual(snoozed.workflow_status, WorkflowStatus.SNOOZED)
        self.assertEqual(
            snoozed.wake_at,
            (NOW + timedelta(days=7)).isoformat(timespec="seconds"),
        )
        self.assertEqual(self.cards.schedule().created, 0)
        self.clock.advance(timedelta(days=7))
        self.assertEqual(self.cards.schedule().created, 1)
        resumed = self._claim_and_deliver()
        self.assertEqual(resumed.card.kind, ExecutionCardKind.PLAN_REVIEW)
        approved = self.cards.act(
            resumed.card.id,
            expected_version=resumed.card.version,
            action="approve",
        )
        self.assertEqual(
            (approved.workflow_status, approved.workflow_phase),
            (WorkflowStatus.QUEUED, WorkflowPhase.EXECUTE),
        )

    def test_pre_migration_completed_result_can_be_snoozed_for_review(self):
        scheduled = self._schedule_workflow(1)
        self.execution.start_action(
            1, expected_version=scheduled.version, action="start"
        )
        recorded = self._record(
            1,
            phase=WorkflowPhase.PLAN,
            outcome=ExecutionOutcome.COMPLETED,
            result_id="legacy-completed-result",
        )
        with closing(sqlite3.connect(self.database)) as connection:
            connection.execute(
                "UPDATE task_execution_workflows SET status='completed',"
                "completed_at=? WHERE task_id=1 AND version=?",
                (NOW.isoformat(timespec="seconds"), recorded.version),
            )
            connection.commit()
        self.cards.schedule()
        claim = self._claim_and_deliver()

        result = self.cards.act(
            claim.card.id,
            expected_version=claim.card.version,
            action="snooze_1d",
        )

        self.assertEqual(result.workflow_status, WorkflowStatus.SNOOZED)
        self.assertEqual(self.ledger.get(1).status, TaskStatus.OPEN)

    def test_discussion_is_private_version_bound_and_consumed_by_result(self):
        self._plan_review(1, "discussion-plan")
        self.cards.schedule()
        claim = self._claim_and_deliver()
        before = self.execution.get(1)

        discussed = self.cards.submit_input(
            claim.card.id,
            expected_version=claim.card.version,
            kind="discussion",
            value="Check the synthetic constraint.",
        )

        self.assertEqual(discussed.workflow_status, WorkflowStatus.QUEUED)
        run = self.execution.claim_next()
        self.assertEqual(
            self.execution.reader_instruction(
                1,
                expected_version=run.workflow_version,
                claim_token=run.token,
            ),
            "Check the synthetic constraint.",
        )
        self.execution.record_result(ExecutionResultEnvelope(
            result_id="discussion-result",
            task_id=1,
            task_version=1,
            workflow_version=run.workflow_version,
            phase="plan",
            claim_token=run.token,
            outcome="awaiting_plan",
            summary="Synthetic updated plan.",
            work_markdown="Synthetic updated work.",
        ))
        self.cards.schedule()
        followup = self._claim_and_deliver()
        revised = self.cards.act(
            followup.card.id,
            expected_version=followup.card.version,
            action="revise",
        )
        next_run = self.execution.claim_next()
        self.assertEqual(next_run.workflow_version, revised.workflow_version + 1)
        self.assertIsNone(self.execution.reader_instruction(
            1,
            expected_version=next_run.workflow_version,
            claim_token=next_run.token,
        ))
        self.assertGreater(discussed.workflow_version, before.version)

    def test_reassignment_versions_task_and_restarts_at_start_gate(self):
        self._plan_review(1, "reassignment-plan")
        self.cards.schedule()
        claim = self._claim_and_deliver()

        reassigned = self.cards.submit_input(
            claim.card.id,
            expected_version=claim.card.version,
            kind="reassignment",
            value="Person Example",
        )

        task = self.ledger.get(1)
        workflow = self.execution.get(1)
        self.assertEqual((task.owner, task.version), ("Person Example", 2))
        self.assertEqual(
            (workflow.task_version, workflow.status, workflow.phase,
             workflow.last_result_id),
            (2, WorkflowStatus.AWAITING_START, WorkflowPhase.PLAN, None),
        )
        self.assertEqual(reassigned.workflow_status, WorkflowStatus.AWAITING_START)
        self.assertEqual(self.cards.schedule().created, 1)
        replacement = self.cards.claim_next()
        self.assertEqual(
            (replacement.card.kind, replacement.card.task_version),
            (ExecutionCardKind.START, 2),
        )
        with closing(sqlite3.connect(self.database)) as connection:
            event = connection.execute(
                "SELECT from_owner,to_owner,task_version "
                "FROM task_owner_events WHERE task_id=1"
            ).fetchone()
            owner_identity = connection.execute(
                "SELECT owner_ref_version,owner_kind,owner_pinned,"
                "owner_provisional FROM tasks WHERE id=1"
            ).fetchone()
        self.assertEqual(event, ("Person 1", "Person Example", 2))
        self.assertEqual(owner_identity, (1, "external", 1, 0))

    def test_plan_drop_and_result_done_atomically_close_task_and_workflow(self):
        self._plan_review(1, "drop-plan")
        self._plan_review(2, "done-plan")
        self.cards.schedule()
        dropped_card = self._claim_and_deliver()
        dropped = self.cards.act(
            dropped_card.card.id,
            expected_version=dropped_card.card.version,
            action="drop",
        )
        self.assertEqual(self.ledger.get(1).status, TaskStatus.DROPPED)
        self.assertEqual(dropped.workflow_status, WorkflowStatus.CANCELLED)

        plan_card = self._claim_and_deliver()
        self.cards.act(
            plan_card.card.id,
            expected_version=plan_card.card.version,
            action="done",
        )
        self.assertEqual(self.ledger.get(2).status, TaskStatus.DONE)
        self.assertEqual(self.execution.get(2).status, WorkflowStatus.COMPLETED)

    def test_card_completion_projects_through_lifecycle_outcome_feed(self):
        self._plan_review(1, "outcome-plan")
        self.cards.schedule()
        claim = self._claim_and_deliver()
        with closing(sqlite3.connect(self.database)) as connection:
            connection.execute(
                "INSERT INTO task_bootstrap_correlations("
                "producer,legacy_task_id,task_id,created_at) "
                "VALUES('gw',101,1,?)",
                (NOW.isoformat(timespec="seconds"),),
            )
            connection.commit()
        self.cards.act(
            claim.card.id,
            expected_version=claim.card.version,
            action="done",
        )
        outbox = Path(self.temporary.name) / "outcomes"
        outbox.mkdir(mode=0o700)

        exported = export_outcomes(
            self.database,
            outbox_dir=outbox,
            stream_id="synthetic-pilot",
            clock=self.clock,
        )

        page = json.loads(exported.pages[0].read_text(encoding="utf-8"))
        outcome = page["items"][0]["outcome"]
        self.assertEqual(
            (outcome["task_id"], outcome["task_version"],
             outcome["to_status"]),
            (1, 2, "done"),
        )

    def test_input_card_update_failure_rolls_back_all_state(self):
        self._plan_review(1, "rollback-plan")
        self.cards.schedule()
        claim = self._claim_and_deliver()
        before_task = self.ledger.get(1)
        before_workflow = self.execution.get(1)
        with closing(sqlite3.connect(self.database)) as connection:
            connection.execute(
                "CREATE TRIGGER synthetic_refuse_execution_input_card_update "
                "BEFORE UPDATE ON execution_review_cards "
                "BEGIN SELECT RAISE(ABORT, 'synthetic refusal'); END"
            )
            connection.commit()

        with self.assertRaises(sqlite3.IntegrityError):
            self.cards.submit_input(
                claim.card.id,
                expected_version=claim.card.version,
                kind="reassignment",
                value="Person Example",
            )

        self.assertEqual(self.ledger.get(1), before_task)
        self.assertEqual(self.execution.get(1), before_workflow)
        with closing(sqlite3.connect(self.database)) as connection:
            counts = (
                connection.execute(
                    "SELECT COUNT(*) FROM execution_reader_inputs"
                ).fetchone()[0],
                connection.execute(
                    "SELECT COUNT(*) FROM task_owner_events"
                ).fetchone()[0],
            )
        self.assertEqual(counts, (0, 0))

    def test_lifecycle_card_update_failure_rolls_back_task_and_workflow(self):
        self._plan_review(1, "lifecycle-rollback-plan")
        self.cards.schedule()
        claim = self._claim_and_deliver()
        before_task = self.ledger.get(1)
        before_workflow = self.execution.get(1)
        with closing(sqlite3.connect(self.database)) as connection:
            event_count = connection.execute(
                "SELECT COUNT(*) FROM task_events"
            ).fetchone()[0]
            connection.execute(
                "CREATE TRIGGER synthetic_refuse_lifecycle_card_update "
                "BEFORE UPDATE ON execution_review_cards "
                "BEGIN SELECT RAISE(ABORT, 'synthetic refusal'); END"
            )
            connection.commit()

        with self.assertRaises(sqlite3.IntegrityError):
            self.cards.act(
                claim.card.id,
                expected_version=claim.card.version,
                action="done",
            )

        self.assertEqual(self.ledger.get(1), before_task)
        self.assertEqual(self.execution.get(1), before_workflow)
        with closing(sqlite3.connect(self.database)) as connection:
            after_events = connection.execute(
                "SELECT COUNT(*) FROM task_events"
            ).fetchone()[0]
        self.assertEqual(after_events, event_count)


if __name__ == "__main__":
    unittest.main()
