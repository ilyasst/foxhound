#!/usr/bin/env python3
"""Synthetic tests for durable execution review cards."""

from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path

import foxhound.candidate_inbox as inbox_schema
from foxhound.agent_profiles import (
    AgentProfileRegistry,
    general_profile,
    parse_profile,
)
from foxhound.candidate_inbox import CandidateInbox, SCHEMA_VERSION
from foxhound.execution_cards import (
    CALLBACK_DATA_LIMIT,
    MAX_CARD_BODY_BYTES,
    MAX_TRUNCATED_CARD_BODY_BYTES,
    ExecutionCardDisposition,
    ExecutionCardKind,
    ExecutionCardRefusal,
    ExecutionCardService,
    ExecutionCardStatus,
    parse_execution_agent_callback,
    parse_execution_review_callback,
    render_execution_agent_selector,
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
            ["start", "agent", "snooze", "cancel"],
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
        body, keyboard = render_execution_review_card(claim.card)
        self.assertIn("<b>Agent:</b> General", body)
        agent_callback = next(
            button["callback_data"]
            for row in keyboard["inline_keyboard"]
            for button in row
            if button["text"] == "🤖 Agent"
        )
        self.assertEqual(
            parse_execution_review_callback(agent_callback),
            (claim.card.id, claim.card.version, "agent"),
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
        self.assertIn("<b>Agent:</b> Synthetic Specialist", refreshed_body)
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
                    "reassign", "drop",
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
            actions, ["revise", "discuss", "snooze", "reassign", "drop"]
        )
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
            work_markdown="Synthetic plan.",
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
        ))
        self.assertEqual(self.cards.schedule().created, 1)
        card = self.cards.claim_next().card
        body, _keyboard = render_execution_review_card(card)

        self.assertIn(f"<code>T{task_id}</code>", body)
        self.assertIn("<b>Phase:</b> plan refinement", body)
        self.assertIn("<b>Needs your input:</b>", body)
        # The action says what is still missing, not just what it is.
        self.assertIn("Needs: Their handle", body)
        # The draft is readable on the card, headed and addressed.
        self.assertIn("<b>email</b>", body)
        self.assertIn("To: Someone", body)
        self.assertIn("Subject: Synthetic subject", body)
        self.assertIn("<pre>First line.\nSecond line.</pre>", body)

    def test_a_start_card_never_claims_work_has_begun(self):
        # The phase names what WOULD run. On a gate, naming it reads as
        # though it already had.
        task_id = 4
        self._schedule_workflow(task_id)
        self.assertEqual(self.cards.schedule().created, 1)
        body, _keyboard = render_execution_review_card(
            self.cards.claim_next().card)
        self.assertIn("<b>Phase:</b> not started", body)
        self.assertNotIn("plan refinement", body)
        self.assertIn("No task work or external action has run.", body)

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
            ["done", "discuss", "snooze", "reassign", "drop"],
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
        self.assertEqual(event, ("Person 1", "Person Example", 2))

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
