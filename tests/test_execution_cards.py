#!/usr/bin/env python3
"""Synthetic tests for durable execution review cards."""

from __future__ import annotations

import sqlite3
import tempfile
import unittest
from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path

from foxhound.candidate_inbox import CandidateInbox, SCHEMA_VERSION
from foxhound.execution_cards import (
    CALLBACK_DATA_LIMIT,
    MAX_CARD_BODY_BYTES,
    ExecutionCardDisposition,
    ExecutionCardKind,
    ExecutionCardRefusal,
    ExecutionCardService,
    ExecutionCardStatus,
    parse_execution_review_callback,
    render_execution_review_card,
)
from foxhound.task_execution import (
    ExecutionOutcome,
    ExecutionResultEnvelope,
    TaskExecutionService,
    WorkflowPhase,
    WorkflowStatus,
)
from foxhound.task_ledger import TaskLedger, TaskStatus


NOW = datetime(2030, 4, 5, 12, 0, tzinfo=timezone.utc)


def _drop_native_intake_schema(connection: sqlite3.Connection) -> None:
    connection.execute("DROP TRIGGER native_candidate_intake_events_no_update")
    connection.execute("DROP TRIGGER native_candidate_intake_events_no_delete")
    connection.execute("DROP TRIGGER candidate_feed_items_no_update")
    connection.execute("DROP TRIGGER candidate_feed_items_no_delete")
    connection.execute("DROP TABLE native_candidate_intake_events")
    connection.execute("DROP TABLE native_candidate_intakes")
    connection.execute("DROP TABLE candidate_feed_items")


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
    ):
        claim = self.execution.claim_next(lease_seconds=300)
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
            work_markdown=(("Synthetic plan. " * 8_000).strip() if long_work
                           else "Synthetic plan."),
            questions=("Proceed with Example A?",),
            external_actions=("Publish synthetic draft <alpha>.",),
            deliverables=("Synthetic deliverable",),
        ))
        self.assertTrue(result.accepted)
        return result

    def _plan_review(self, task_id: int, result_id: str, *, long_work=False):
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
            ["start", "snooze", "cancel"],
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

    def test_start_actions_are_atomic_and_do_not_change_task_lifecycle(self):
        expected = {
            1: ("start", WorkflowStatus.QUEUED),
            2: ("snooze", WorkflowStatus.SNOOZED),
            3: ("cancel", WorkflowStatus.CANCELLED),
        }
        for task_id in expected:
            self._schedule_workflow(task_id)
        self.cards.schedule()
        for task_id, (action, status) in expected.items():
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
                self.ledger.get(task_id).status, TaskStatus.OPEN
            )
            stale = self.cards.act(
                claim.card.id,
                expected_version=claim.card.version,
                action=action,
            )
            self.assertEqual(stale.refusal, ExecutionCardRefusal.STALE_VERSION)
            self.assertEqual(self.cards.event_count(), before + 1)

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
                for button in keyboard["inline_keyboard"][0]
            ]
            self.assertEqual(
                [parse_execution_review_callback(value)[2]
                 for value in callbacks],
                ["approve", "revise", "cancel"],
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
        self.assertLessEqual(len(body.encode("utf-8")), MAX_CARD_BODY_BYTES)
        self.assertIn("too long to approve", body)
        actions = [
            parse_execution_review_callback(button["callback_data"])[2]
            for button in keyboard["inline_keyboard"][0]
        ]
        self.assertEqual(actions, ["revise", "cancel"])
        before = self.execution.get(1)
        refused = self.cards.act(
            claim.card.id,
            expected_version=claim.card.version,
            action="approve",
        )
        self.assertEqual(refused.refusal, ExecutionCardRefusal.INVALID_STATE)
        self.assertEqual(self.execution.get(1), before)

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
            self.assertIn("external-action approval", body)
            self.assertIn("Publish synthetic draft &lt;alpha&gt;.", body)
            self.assertEqual(
                keyboard["inline_keyboard"][0][0]["text"],
                "Approve action",
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

    def test_events_are_append_only_and_completed_work_gets_no_card(self):
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
        self.assertEqual(self.cards.schedule().created, 0)

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


if __name__ == "__main__":
    unittest.main()
