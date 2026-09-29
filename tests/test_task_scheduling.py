#!/usr/bin/env python3
"""Synthetic tests for bounded scheduling recommendations and Undo."""

from __future__ import annotations

import sqlite3
import tempfile
import unittest
from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

from foxhound import migrate_database
from foxhound.agent_profiles import AgentProfileRegistry, general_profile
from foxhound.task_execution import TaskExecutionService
from foxhound.task_ledger import TaskLedgerError
from foxhound.task_scheduling import (
    MAX_ACTIVE_CONDITIONS,
    SchedulingDisposition,
    SchedulingKind,
    SchedulingRefusal,
    TaskSchedulingService,
    ValidatedSchedulingRecommendation,
)


class MutableClock:
    def __init__(self) -> None:
        self.value = datetime(2031, 2, 3, 4, 5, 6, tzinfo=timezone.utc)

    def __call__(self) -> datetime:
        return self.value

    def advance(self, **values: int) -> None:
        self.value += timedelta(**values)


class TaskSchedulingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.database = Path(self.temporary.name) / "foxhound.sqlite3"
        self.clock = MutableClock()
        migrate_database(self.database)
        profile = general_profile()
        now = self.now
        with closing(sqlite3.connect(self.database)) as connection:
            for task_id in range(1, 16):
                connection.execute(
                    "INSERT INTO tasks(id,status,text,owner,due,version,created_at,"
                    "updated_at,closed_at) VALUES(?,'open',?,NULL,NULL,1,?,?,NULL)",
                    (task_id, f"Synthetic task {task_id}", now, now),
                )
                connection.execute(
                    "INSERT INTO task_events(task_id,kind,task_version,candidate_id,"
                    "source_revision,from_status,to_status,occurred_at) "
                    "VALUES(?,'created',1,NULL,NULL,NULL,'open',?)",
                    (task_id, now),
                )
                connection.execute(
                    "INSERT INTO task_execution_workflows(task_id,task_version,status,"
                    "phase,version,due_at,failure_count,created_at,updated_at,"
                    "agent_profile_id,agent_profile_revision,queue_priority) "
                    "VALUES(?,?,?,'plan',1,NULL,0,?,?,?,?, 'normal')",
                    (
                        task_id, 1, "queued" if task_id == 1 else "awaiting_start",
                        now, now, profile.profile_id, profile.revision,
                    ),
                )
            connection.commit()
        self.scheduling = TaskSchedulingService(
            self.database,
            clock=self.clock,
            token_factory=lambda: "synthetic-scheduling-token-000000000000000000000",
            provenance_validator=lambda _connection, value: (
                value.research_receipt_id == "synthetic-receipt-001"
                and set(value.source_refs) <= {"src-001", "src-002"}
            ),
        )
        self.execution = TaskExecutionService(
            self.database,
            clock=self.clock,
            token_factory=lambda: "synthetic-claim-token-000000000000000000000000",
            execution_slot_cap=-1,
            profile_registry=AgentProfileRegistry((profile,)),
        )

    @property
    def now(self) -> str:
        return self.clock().isoformat(timespec="seconds")

    def recommendation(self, kind: SchedulingKind, **values: object) -> ValidatedSchedulingRecommendation:
        return ValidatedSchedulingRecommendation(
            kind=kind,
            target_task_id=int(values.pop("target_task_id", 1)),
            target_task_version=1,
            expected_workflow_version=int(values.pop("expected_workflow_version", 1)),
            rationale="Synthetic evidence establishes this queue constraint.",
            source_refs=("src-001",),
            research_receipt_id="synthetic-receipt-001",
            research_document_digest="a" * 64,
            **values,
        )

    def rows(self, table: str) -> list[sqlite3.Row]:
        with closing(sqlite3.connect(self.database)) as connection:
            connection.row_factory = sqlite3.Row
            return connection.execute(f"SELECT * FROM {table} ORDER BY 1").fetchall()

    def deliver(self, card_id: int) -> int:
        claim = self.scheduling.claim_next(consumer_digest="b" * 64)
        self.assertIsNotNone(claim)
        assert claim is not None
        self.assertEqual(card_id, claim.card.card_id)
        delivered = self.scheduling.complete_delivery(
            card_id,
            expected_version=claim.card.version,
            claim_token=claim.token,
            transport="synthetic",
            delivery_ref=f"scheduling-card-{card_id}",
        )
        self.assertTrue(delivered.accepted)
        return claim.card.version

    def test_validated_recommendation_to_card_to_undo_restores_exact_state(self) -> None:
        before = self.rows("task_execution_workflows")[0]
        applied = self.scheduling.apply(self.recommendation(
            SchedulingKind.AFTER_TASK_COMPLETED, related_task_id=2,
        ))
        self.assertEqual(SchedulingDisposition.APPLIED, applied.disposition)
        self.assertEqual("active", self.rows("task_scheduling_conditions")[0]["state"])
        card = self.rows("task_scheduling_review_cards")[0]
        self.assertEqual("pending", card["status"])
        self.assertIsNone(self.execution.claim_next())

        delivered_version = self.deliver(applied.card_id or 0)
        undone = self.scheduling.act(
            applied.card_id or 0, expected_version=delivered_version, action="undo"
        )
        self.assertEqual(SchedulingDisposition.APPLIED, undone.disposition)
        condition = self.rows("task_scheduling_conditions")[0]
        self.assertEqual("canceled", condition["state"])
        after = self.rows("task_execution_workflows")[0]
        self.assertEqual(before["version"], after["version"])
        self.assertEqual(before["queue_priority"], after["queue_priority"])
        card = self.rows("task_scheduling_review_cards")[0]
        self.assertEqual(("resolved", "undo", 3),
                         (card["status"], card["resolution"], card["version"]))
        self.assertEqual("undone", self.rows("task_scheduling_change_sets")[0]["state"])

    def test_after_task_releases_only_when_dependency_is_done(self) -> None:
        self.scheduling.apply(self.recommendation(
            SchedulingKind.AFTER_TASK_COMPLETED, related_task_id=2,
        ))
        with closing(sqlite3.connect(self.database)) as connection:
            connection.execute(
                "UPDATE tasks SET status='done',version=2,closed_at=?,updated_at=? WHERE id=2",
                (self.now, self.now),
            )
            connection.commit()
        claim = self.execution.claim_next()
        self.assertIsNotNone(claim)
        self.assertEqual(1, claim.task_id)
        self.assertEqual("satisfied", self.rows("task_scheduling_conditions")[0]["state"])

    def test_not_before_holds_then_releases(self) -> None:
        wake = (self.clock() + timedelta(hours=2)).isoformat(timespec="seconds")
        self.scheduling.apply(self.recommendation(
            SchedulingKind.NOT_BEFORE, not_before=wake,
        ))
        self.assertIsNone(self.execution.claim_next())
        self.clock.advance(hours=2)
        claim = self.execution.claim_next()
        self.assertIsNotNone(claim)
        self.assertEqual(1, claim.task_id)

    def test_condition_evaluator_error_fails_closed(self) -> None:
        with mock.patch(
            "foxhound.task_execution.evaluate_task_conditions",
            side_effect=RuntimeError("synthetic evaluator failure"),
        ):
            self.assertIsNone(self.execution.claim_next())
        self.assertEqual("queued", self.rows("task_execution_workflows")[0]["status"])

    def test_limits_cycle_and_depth_are_rejected_without_partial_writes(self) -> None:
        for related in (2, 3):
            result = self.scheduling.apply(self.recommendation(
                SchedulingKind.AFTER_TASK_COMPLETED, related_task_id=related,
            ))
            self.assertTrue(result.accepted)
        result = self.scheduling.apply(self.recommendation(
            SchedulingKind.AFTER_TASK_COMPLETED, related_task_id=4,
        ))
        self.assertEqual(SchedulingRefusal.LIMIT_EXCEEDED, result.refusal)

        reverse = self.scheduling.apply(self.recommendation(
            SchedulingKind.AFTER_TASK_COMPLETED,
            target_task_id=2,
            related_task_id=1,
        ))
        self.assertEqual(SchedulingRefusal.CYCLE, reverse.refusal)
        self.assertEqual(2, len(self.rows("task_scheduling_change_sets")))

        # A separate chain reaches the maximum eight edges; the ninth refuses.
        for target in range(5, 13):
            result = self.scheduling.apply(self.recommendation(
                SchedulingKind.AFTER_TASK_COMPLETED,
                target_task_id=target,
                related_task_id=target + 1,
            ))
            self.assertTrue(result.accepted)
        too_deep = self.scheduling.apply(self.recommendation(
            SchedulingKind.AFTER_TASK_COMPLETED,
            target_task_id=13,
            related_task_id=14,
        ))
        self.assertEqual(SchedulingRefusal.DEPTH_EXCEEDED, too_deep.refusal)

    def test_only_one_not_before_and_three_conditions(self) -> None:
        wake = (self.clock() + timedelta(days=1)).isoformat(timespec="seconds")
        self.assertTrue(self.scheduling.apply(self.recommendation(
            SchedulingKind.NOT_BEFORE, not_before=wake,
        )).accepted)
        duplicate = self.scheduling.apply(self.recommendation(
            SchedulingKind.NOT_BEFORE,
            not_before=(self.clock() + timedelta(days=2)).isoformat(timespec="seconds"),
        ))
        self.assertEqual(SchedulingRefusal.LIMIT_EXCEEDED, duplicate.refusal)
        for related in (2, 3):
            self.assertTrue(self.scheduling.apply(self.recommendation(
                SchedulingKind.AFTER_TASK_COMPLETED, related_task_id=related,
            )).accepted)
        self.assertEqual(MAX_ACTIVE_CONDITIONS, len(self.rows("task_scheduling_conditions")))

    def test_automated_raise_never_overwrites_reader_priority_and_undo_is_fenced(self) -> None:
        manual = self.execution.set_priority(1, expected_version=1, action="lower")
        self.assertTrue(manual.accepted)
        refused = self.scheduling.apply(self.recommendation(
            SchedulingKind.RAISE_PRIORITY, expected_workflow_version=2,
        ))
        self.assertEqual(SchedulingRefusal.MANUAL_PRIORITY, refused.refusal)

        self.assertTrue(self.execution.set_priority(1, expected_version=2, action="clear").accepted)
        applied = self.scheduling.apply(self.recommendation(
            SchedulingKind.RAISE_PRIORITY, expected_workflow_version=3,
        ))
        self.assertTrue(applied.accepted)
        current = self.rows("task_execution_workflows")[0]
        self.assertEqual(("raised", 4), (current["queue_priority"], current["version"]))
        delivered_version = self.deliver(applied.card_id or 0)
        self.assertTrue(self.scheduling.act(
            applied.card_id or 0, expected_version=delivered_version, action="undo"
        ).accepted)
        restored = self.rows("task_execution_workflows")[0]
        self.assertEqual(("normal", 5), (restored["queue_priority"], restored["version"]))

    def test_create_prerequisite_is_atomic_and_untouched_creation_can_be_undone(self) -> None:
        applied = self.scheduling.apply(self.recommendation(
            SchedulingKind.CREATE_PREREQUISITE,
            prerequisite_text="Prepare synthetic input.",
        ))
        self.assertTrue(applied.accepted)
        created = applied.created_task_id
        self.assertIsNotNone(created)
        condition = self.rows("task_scheduling_conditions")[0]
        self.assertEqual(created, condition["depends_on_task_id"])
        delivered_version = self.deliver(applied.card_id or 0)
        self.assertTrue(self.scheduling.act(
            applied.card_id or 0, expected_version=delivered_version, action="undo"
        ).accepted)
        with closing(sqlite3.connect(self.database)) as connection:
            task = connection.execute("SELECT status,version FROM tasks WHERE id=?", (created,)).fetchone()
        self.assertEqual(("dropped", 2), task)

    def test_touched_prerequisite_refuses_undo_without_partial_changes(self) -> None:
        applied = self.scheduling.apply(self.recommendation(
            SchedulingKind.CREATE_PREREQUISITE,
            prerequisite_text="Prepare another synthetic input.",
        ))
        created = applied.created_task_id
        with closing(sqlite3.connect(self.database)) as connection:
            connection.execute(
                "UPDATE tasks SET text='Reader revised synthetic prerequisite',"
                "version=2,updated_at=? WHERE id=?",
                (self.now, created),
            )
            connection.commit()
        delivered_version = self.deliver(applied.card_id or 0)
        refused = self.scheduling.act(
            applied.card_id or 0, expected_version=delivered_version, action="undo"
        )
        self.assertEqual(SchedulingRefusal.UNDO_CONFLICT, refused.refusal)
        self.assertEqual("active", self.rows("task_scheduling_conditions")[0]["state"])
        self.assertEqual("delivered", self.rows("task_scheduling_review_cards")[0]["status"])
        self.assertEqual("undo_conflict", self.rows("task_scheduling_change_sets")[0]["state"])

    def test_delivery_claim_ack_failure_and_expiry_are_fenced(self) -> None:
        first = self.scheduling.apply(self.recommendation(
            SchedulingKind.AFTER_TASK_COMPLETED, related_task_id=2,
        ))
        claim = self.scheduling.claim_next(
            consumer_digest="b" * 64, lease_seconds=10,
        )
        self.assertIsNotNone(claim)
        assert claim is not None
        self.assertEqual(first.card_id, claim.card.card_id)
        stored = self.rows("task_scheduling_review_cards")[0]
        self.assertEqual(("delivering", 2), (stored["status"], stored["version"]))
        self.assertNotEqual(claim.token, stored["claim_token_digest"])
        self.assertEqual(64, len(stored["claim_token_digest"]))

        wrong = self.scheduling.complete_delivery(
            claim.card.card_id, expected_version=2,
            claim_token="wrong-synthetic-token-000000000000000000000000",
            transport="synthetic", delivery_ref="message-one",
        )
        self.assertEqual(SchedulingRefusal.CLAIM_MISMATCH, wrong.refusal)
        failed = self.scheduling.fail_delivery(
            claim.card.card_id, expected_version=2, claim_token=claim.token,
        )
        self.assertTrue(failed.accepted)
        self.assertEqual(("pending", 3), tuple(
            self.rows("task_scheduling_review_cards")[0][key]
            for key in ("status", "version")
        ))

        reclaimed = self.scheduling.claim_next(
            consumer_digest="b" * 64, lease_seconds=10,
        )
        self.assertIsNotNone(reclaimed)
        assert reclaimed is not None
        self.clock.advance(seconds=11)
        expired = self.scheduling.complete_delivery(
            reclaimed.card.card_id, expected_version=reclaimed.card.version,
            claim_token=reclaimed.token, transport="synthetic",
            delivery_ref="message-two",
        )
        self.assertEqual(SchedulingRefusal.INVALID_STATE, expired.refusal)
        recovered = self.scheduling.claim_next(consumer_digest="b" * 64)
        self.assertIsNotNone(recovered)
        assert recovered is not None
        self.assertEqual(reclaimed.card.version + 2, recovered.card.version)

    def test_delivery_rejects_invalid_lease_and_actions_before_delivery(self) -> None:
        applied = self.scheduling.apply(self.recommendation(
            SchedulingKind.NOT_BEFORE,
            not_before=(self.clock() + timedelta(days=1)).isoformat(timespec="seconds"),
        ))
        with self.assertRaisesRegex(TaskLedgerError, "lease is invalid"):
            self.scheduling.claim_next(consumer_digest="b" * 64, lease_seconds=True)
        refused = self.scheduling.act(
            applied.card_id or 0, expected_version=1, action="keep",
        )
        self.assertEqual(SchedulingRefusal.INVALID_STATE, refused.refusal)

        version = self.deliver(applied.card_id or 0)
        kept = self.scheduling.act(
            applied.card_id or 0, expected_version=version, action="keep",
        )
        self.assertTrue(kept.accepted)
        self.assertEqual(("resolved", "keep", version + 1), tuple(
            self.rows("task_scheduling_review_cards")[0][key]
            for key in ("status", "resolution", "version")
        ))


if __name__ == "__main__":
    unittest.main()
