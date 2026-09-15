#!/usr/bin/env python3
"""Synthetic tests for Foxhound-owned task execution workflow state."""

from __future__ import annotations

import sqlite3
import tempfile
import unittest
from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path

from foxhound.agent_profiles import (
    AgentProfileRegistry,
    WORKER_COMMAND_TOKEN,
    general_profile,
    parse_profile,
)
from foxhound.candidate_inbox import CandidateInbox, SCHEMA_VERSION
from foxhound.task_execution import (
    AWAITING_READER_CAP,
    EXECUTION_SLOT_CAP,
    PLAN_READY_CAP,
    WORK_IN_PROGRESS_CAP,
    ExecutionOutcome,
    ExecutionResultEnvelope,
    TaskExecutionService,
    WorkflowDisposition,
    WorkflowPhase,
    WorkflowRefusal,
    WorkflowStatus,
)
from foxhound.task_ledger import TaskLedger, TaskLedgerError


TOKEN = "execution-claim-token-000000000000000000000000"
OTHER_TOKEN = "different-claim-token-000000000000000000000"
LEGACY_GENERAL_REVISION = (
    "f0171b0e9e09e547d9b344223d31b6de1bc0e6d13cb5b8c891fda9d0a7b0db94"
)


def _profile(profile_id="specialist", *, phases=("plan", "execute")):
    return parse_profile({
        "schema": "foxhound.agent-profile",
        "schema_version": 1,
        "profile_id": profile_id,
        "display_name": "Synthetic Specialist",
        "runtime": "hermes",
        "prompt_template": f"Use {WORKER_COMMAND_TOKEN} context.",
        "toolsets": ["terminal", "file"],
        "max_turns": 50,
        "timeout_seconds": 1_800,
        "claim_lease_seconds": 2_700,
        "heartbeat_seconds": 60,
        "kill_grace_seconds": 30,
        "allowed_phases": list(phases),
    })


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


def _drop_agent_profile_schema(connection: sqlite3.Connection) -> None:
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


class MutableClock:
    def __init__(self) -> None:
        self.value = datetime(2030, 1, 2, 3, 4, 5, tzinfo=timezone.utc)

    def __call__(self) -> datetime:
        return self.value

    def advance(self, **values) -> None:
        self.value += timedelta(**values)


class TaskExecutionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.root.chmod(0o700)
        self.database = self.root / "foxhound.sqlite3"
        self.clock = MutableClock()
        CandidateInbox(self.database, clock=self.clock).initialize()
        with closing(sqlite3.connect(self.database)) as connection:
            connection.execute(
                "INSERT INTO tasks(id,status,text,owner,due,version,"
                "created_at,"
                "updated_at,closed_at) VALUES(1,'open','Synthetic task',"
                "'Person A',NULL,1,?,?,NULL)",
                (self._now(), self._now()),
            )
            connection.execute(
                "INSERT INTO task_events(task_id,kind,task_version,"
                "candidate_id,source_revision,from_status,to_status,"
                "occurred_at) VALUES(1,'created',1,NULL,NULL,NULL,'open',?)",
                (self._now(),),
            )
            connection.commit()
        self.service = TaskExecutionService(
            self.database,
            clock=self.clock,
            token_factory=lambda: TOKEN,
            max_attempts=3,
            profile_registry=AgentProfileRegistry((
                general_profile(),
                _profile("sigint", phases=("plan", "execute", "external_action")),
            )),
        )

    def _now(self) -> str:
        return self.clock().isoformat(timespec="seconds")

    def _bind_origin(self, task_id: int, kind: str) -> None:
        """Attach one synthetic accepted origin to an existing task."""
        with closing(sqlite3.connect(self.database)) as connection:
            connection.execute(
                "INSERT INTO candidate_inbox(candidate_id,source_system,"
                "source_kind,source_record_id,source_item_id,"
                "source_revision,payload_json,created_at,"
                "first_imported_at,updated_at) "
                "VALUES(?,'gw',?,'forge.example/acme/widget',?,?,'{}',?,?,?)",
                (
                    f"origin-{task_id}", kind, str(task_id), "b" * 64,
                    self._now(), self._now(), self._now(),
                ),
            )
            connection.execute(
                "INSERT INTO task_candidate_bindings(candidate_id,"
                "source_revision,task_id,relation,decided_at) "
                "VALUES(?,?,?,'accepted',?)",
                (f"origin-{task_id}", "b" * 64, task_id, self._now()),
            )
            connection.commit()

    def _schedule_and_start(self):
        scheduled = self.service.schedule(1, expected_task_version=1)
        self.assertEqual(scheduled.status, WorkflowStatus.AWAITING_START)
        started = self.service.start_action(
            1, expected_version=scheduled.version, action="start"
        )
        self.assertEqual(started.status, WorkflowStatus.QUEUED)
        return started

    def _claim(self):
        claim = self.service.claim_next()
        self.assertIsNotNone(claim)
        return claim

    def _result(
        self,
        claim,
        *,
        result_id="result-001",
        outcome=ExecutionOutcome.AWAITING_PLAN,
    ) -> ExecutionResultEnvelope:
        return ExecutionResultEnvelope(
            result_id=result_id,
            task_id=claim.task_id,
            task_version=claim.task_version,
            workflow_version=claim.workflow_version,
            phase=claim.phase,
            claim_token=claim.token,
            outcome=outcome,
            summary="Synthetic result summary",
            work_markdown="# Synthetic work\n\nNo private evidence.",
            questions=("Should Example A proceed?",),
            external_actions=("Prepare a synthetic draft for review.",),
            deliverables=("Synthetic deliverable",),
        )

    def test_schema_seven_migration_is_passive_and_append_only(self):
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
            # ADR 0036 added this at v23; a database at an older version
            # has not got it yet.
            connection.execute(
                "ALTER TABLE task_review_cards DROP COLUMN consumer_digest"
            )
            connection.execute("PRAGMA user_version = 7")
            connection.commit()

        CandidateInbox(self.database, clock=self.clock).initialize()

        with closing(sqlite3.connect(self.database)) as connection:
            self.assertEqual(
                connection.execute("PRAGMA user_version").fetchone()[0],
                SCHEMA_VERSION,
            )
            self.assertEqual(
                connection.execute("SELECT COUNT(*) FROM tasks").fetchone()[0],
                1,
            )
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM task_execution_workflows"
                ).fetchone()[0],
                0,
            )

    def test_schema_eleven_migration_preserves_gates_and_adds_profile_evidence(self):
        started = self._schedule_and_start()
        claim = self._claim()
        recorded = self.service.record_result(self._result(claim))
        self.assertEqual(recorded.status, WorkflowStatus.AWAITING_REVIEW)
        before = self.service.get(1)

        with closing(sqlite3.connect(self.database)) as connection:
            _drop_owner_schema(connection)
            _drop_agent_profile_schema(connection)
            # v21 added this; a database at an older version has
            # not got it yet.
            connection.execute(
                "ALTER TABLE task_execution_results DROP COLUMN work_digest"
            )
            # ADR 0036 added this at v23; a database at an older version
            # has not got it yet.
            connection.execute(
                "ALTER TABLE task_review_cards DROP COLUMN consumer_digest"
            )
            connection.execute("PRAGMA user_version = 11")
            connection.commit()

        CandidateInbox(self.database, clock=self.clock).initialize()

        after = self.service.get(1)
        # The point is that a version-11 database arrives at the current
        # schema, not at one particular number — pinning the number means
        # every later migration edits a test about an earlier one.
        self.assertGreaterEqual(SCHEMA_VERSION, 20)
        self.assertEqual(
            (after.status, after.phase, after.version, after.task_version),
            (before.status, before.phase, before.version, before.task_version),
        )
        self.assertEqual(after.agent_profile_id, "general")
        self.assertEqual(
            after.agent_profile_revision, LEGACY_GENERAL_REVISION
        )
        with closing(sqlite3.connect(self.database)) as connection:
            result_evidence = connection.execute(
                "SELECT DISTINCT agent_profile_id,agent_profile_revision "
                "FROM task_execution_results"
            ).fetchall()
            event_evidence = connection.execute(
                "SELECT DISTINCT agent_profile_id,agent_profile_revision "
                "FROM task_execution_events"
            ).fetchall()
        expected = [("general", LEGACY_GENERAL_REVISION)]
        self.assertEqual(result_evidence, expected)
        self.assertEqual(event_evidence, expected)

    def test_agent_selection_is_explicit_idempotent_and_version_fenced(self):
        specialist = _profile()
        unavailable = _profile("execute-only", phases=("execute",))
        historical = parse_profile({
            **general_profile().document(),
            "max_turns": 49,
        })
        registry = AgentProfileRegistry(
            (general_profile(), specialist, unavailable),
            historical_profiles=(historical,),
        )
        service = TaskExecutionService(
            self.database,
            clock=self.clock,
            token_factory=lambda: TOKEN,
            profile_registry=registry,
        )
        scheduled = service.schedule(1, expected_task_version=1)
        self.assertEqual(scheduled.agent_profile_id, "general")
        self.assertEqual(
            scheduled.agent_profile_revision, general_profile().revision
        )

        selected = service.select_agent(
            1,
            expected_version=scheduled.version,
            profile_id=specialist.profile_id,
            profile_revision=specialist.revision,
        )
        self.assertEqual(selected.disposition, WorkflowDisposition.APPLIED)
        self.assertEqual(selected.version, scheduled.version + 1)
        self.assertEqual(selected.agent_profile_id, specialist.profile_id)
        event_count = service.event_count()

        replay = service.select_agent(
            1,
            expected_version=selected.version,
            profile_id=specialist.profile_id,
            profile_revision=specialist.revision,
        )
        self.assertEqual(replay.disposition, WorkflowDisposition.UNCHANGED)
        self.assertEqual(service.event_count(), event_count)

        stale = service.select_agent(
            1,
            expected_version=scheduled.version,
            profile_id="general",
            profile_revision=general_profile().revision,
        )
        self.assertEqual(stale.refusal, WorkflowRefusal.STALE_WORKFLOW)
        for profile_id, revision in (
            ("missing", specialist.revision),
            (specialist.profile_id, "0" * 64),
            (unavailable.profile_id, unavailable.revision),
            (historical.profile_id, historical.revision),
        ):
            with self.subTest(profile_id=profile_id, revision=revision):
                refused = service.select_agent(
                    1,
                    expected_version=selected.version,
                    profile_id=profile_id,
                    profile_revision=revision,
                )
                self.assertEqual(
                    refused.refusal,
                    WorkflowRefusal.AGENT_PROFILE_UNAVAILABLE,
                )
        self.assertEqual(service.get(1).version, selected.version)

        started = service.start_action(
            1, expected_version=selected.version, action="start"
        )
        refused = service.select_agent(
            1,
            expected_version=started.version,
            profile_id="general",
            profile_revision=general_profile().revision,
        )
        self.assertEqual(refused.refusal, WorkflowRefusal.INVALID_STATE)
        self.assertEqual(service.get(1).agent_profile_id, specialist.profile_id)

    def test_default_profile_is_deterministic_and_not_selected_by_task_content(self):
        specialist = _profile()
        registry = AgentProfileRegistry((general_profile(), specialist))
        with closing(sqlite3.connect(self.database)) as connection:
            connection.execute(
                "UPDATE tasks SET text=? WHERE id=1", (specialist.profile_id,)
            )
            connection.commit()
        service = TaskExecutionService(
            self.database,
            clock=self.clock,
            profile_registry=registry,
            default_profile_id=specialist.profile_id,
        )
        scheduled = service.schedule(1, expected_task_version=1)
        self.assertEqual(scheduled.agent_profile_id, specialist.profile_id)
        self.assertEqual(
            scheduled.agent_profile_revision, specialist.revision
        )
        with self.assertRaises(ValueError):
            TaskExecutionService(
                self.database,
                profile_registry=registry,
                default_profile_id="missing",
            )
        with self.assertRaises(ValueError):
            TaskExecutionService(
                self.database,
                profile_registry=AgentProfileRegistry((
                    _profile("execute-only", phases=("execute",)),
                )),
                default_profile_id="execute-only",
            )

    def test_claim_retry_result_events_and_health_retain_exact_profile(self):
        specialist = _profile()
        registry = AgentProfileRegistry((general_profile(), specialist))
        service = TaskExecutionService(
            self.database,
            clock=self.clock,
            token_factory=lambda: TOKEN,
            profile_registry=registry,
        )
        scheduled = service.schedule(1, expected_task_version=1)
        selected = service.select_agent(
            1,
            expected_version=scheduled.version,
            profile_id=specialist.profile_id,
            profile_revision=specialist.revision,
        )
        started = service.start_action(
            1, expected_version=selected.version, action="start"
        )
        claim = service.claim_next()
        self.assertEqual(claim.agent_profile_id, specialist.profile_id)
        self.assertEqual(claim.agent_profile_revision, specialist.revision)
        failed = service.fail(
            1,
            expected_version=claim.workflow_version,
            claim_token=claim.token,
            reason="process_exit",
        )
        retried = service.retry(1, expected_version=failed.version)
        self.assertEqual(retried.agent_profile_id, specialist.profile_id)
        self.assertEqual(retried.agent_profile_revision, specialist.revision)

        claim = service.claim_next()
        recorded = service.record_result(self._result(claim))
        self.assertEqual(recorded.agent_profile_id, specialist.profile_id)
        with closing(sqlite3.connect(self.database)) as connection:
            result_evidence = connection.execute(
                "SELECT agent_profile_id,agent_profile_revision "
                "FROM task_execution_results WHERE result_id='result-001'"
            ).fetchone()
            latest_events = connection.execute(
                "SELECT agent_profile_id,agent_profile_revision "
                "FROM task_execution_events WHERE kind!='scheduled'"
            ).fetchall()
        expected = (specialist.profile_id, specialist.revision)
        self.assertEqual(result_evidence, expected)
        self.assertTrue(latest_events)
        self.assertTrue(all(event == expected for event in latest_events))

        health = service.profile_health()
        self.assertEqual(len(health), 1)
        self.assertEqual(health[0].agent_profile_id, specialist.profile_id)
        self.assertEqual(health[0].agent_profile_revision, specialist.revision)
        self.assertTrue(health[0].available)
        unavailable = TaskExecutionService(
            self.database, clock=self.clock
        ).profile_health()
        self.assertFalse(unavailable[0].available)
        self.assertNotIn("Synthetic task", repr(unavailable))

    def test_claim_refuses_changed_missing_or_phase_ineligible_profile(self):
        specialist = _profile(phases=("plan",))
        registry = AgentProfileRegistry((general_profile(), specialist))
        service = TaskExecutionService(
            self.database,
            clock=self.clock,
            token_factory=lambda: TOKEN,
            profile_registry=registry,
        )
        scheduled = service.schedule(1, expected_task_version=1)
        selected = service.select_agent(
            1,
            expected_version=scheduled.version,
            profile_id=specialist.profile_id,
            profile_revision=specialist.revision,
        )
        service.start_action(1, expected_version=selected.version, action="start")

        without_profile = TaskExecutionService(
            self.database, clock=self.clock, token_factory=lambda: TOKEN
        )
        with self.assertRaises(TaskLedgerError):
            without_profile.claim_next()
        queued = service.get(1)
        self.assertEqual(queued.status, WorkflowStatus.QUEUED)

        claim = service.claim_next()
        recorded = service.record_result(self._result(claim))
        approved = service.review_action(
            1, expected_version=recorded.version, action="approve"
        )
        self.assertEqual(approved.phase, WorkflowPhase.EXECUTE)
        with self.assertRaises(TaskLedgerError):
            service.claim_next()
        self.assertEqual(service.get(1).status, WorkflowStatus.QUEUED)

    def test_schedule_is_explicit_idempotent_and_task_version_fenced(self):
        scheduled = self.service.schedule(1, expected_task_version=1)
        self.assertEqual(scheduled.disposition, WorkflowDisposition.APPLIED)
        self.assertEqual(scheduled.version, 1)
        self.assertEqual(scheduled.status, WorkflowStatus.AWAITING_START)
        self.assertEqual(self.service.event_count(), 1)

        replay = self.service.schedule(1, expected_task_version=1)
        self.assertEqual(replay.disposition, WorkflowDisposition.UNCHANGED)
        self.assertEqual(self.service.event_count(), 1)
        with closing(sqlite3.connect(self.database)) as connection:
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute(
                    "UPDATE task_execution_workflows "
                    "SET claim_token_digest=? WHERE task_id=1",
                    ("a" * 64,),
                )
        stale = self.service.schedule(1, expected_task_version=2)
        self.assertEqual(stale.refusal, WorkflowRefusal.STALE_TASK)
        missing = self.service.schedule(99, expected_task_version=1)
        self.assertEqual(missing.refusal, WorkflowRefusal.NOT_FOUND)

        closed = TaskLedger(self.database, clock=self.clock).transition(
            1, expected_version=1, action="done"
        )
        self.assertTrue(closed.accepted)
        refused = self.service.schedule(1, expected_task_version=2)
        self.assertEqual(refused.refusal, WorkflowRefusal.INVALID_STATE)

    def test_repository_work_starts_on_an_agent_that_can_do_it(self):
        """A single default sends every kind of work to the same agent. One
        pull-request review went to the compatibility profile, produced
        nothing recordable three times, and parked with the review written.

        A compatibility-only library instance may still fall back while the
        production scheduler is responsible for installing SigInt.
        """
        from foxhound.agent_profiles import AgentProfile, AgentProfileRegistry
        from foxhound.task_execution import SOURCE_KIND_PROFILES

        self.assertEqual(SOURCE_KIND_PROFILES.get("review_request"), "sigint")
        self.assertEqual(SOURCE_KIND_PROFILES.get("issue"), "sigint")
        # A meeting action has no preference and keeps the default.
        self.assertIsNone(SOURCE_KIND_PROFILES.get("meeting"))

        general = self.service._profile_registry.get("general")
        service = TaskExecutionService(
            self.database, clock=self.clock,
            profile_registry=AgentProfileRegistry([general]))
        self.assertEqual(
            service._profile_for("review_request").profile_id, "general")

    def test_an_enrolled_repository_issue_is_planned_without_being_asked(self):
        """The gate asks a question already answered by enrolling the repo.

        Worse, it asks it with a card that can only show the issue's title,
        because nothing has looked at the issue yet. Planning is read-only,
        so going straight to it costs one pass and produces a card a reader
        can actually judge. Everything after the plan is still gated.
        """
        def bind(task_id, kind, record, item):
            with closing(sqlite3.connect(self.database)) as connection:
                connection.execute(
                    "INSERT INTO tasks(id,status,text,owner,due,version,"
                    "created_at,updated_at,closed_at) "
                    "VALUES(?,'open',?,NULL,NULL,1,?,?,NULL)",
                    (task_id, f"Synthetic task {task_id}", self._now(),
                     self._now()))
                connection.execute(
                    "INSERT INTO candidate_inbox(candidate_id,source_system,"
                    "source_kind,source_record_id,source_item_id,"
                    "source_revision,payload_json,created_at,"
                    "first_imported_at,updated_at) "
                    "VALUES(?,'gw',?,?,?,?,'{}',?,?,?)",
                    (f"cand-{task_id}", kind, record, item, "b" * 64,
                     self._now(), self._now(), self._now()))
                connection.execute(
                    "INSERT INTO task_candidate_bindings(candidate_id,"
                    "source_revision,task_id,relation,decided_at) "
                    "VALUES(?,?,?,'accepted',?)",
                    (f"cand-{task_id}", "b" * 64, task_id, self._now()))
                connection.commit()

        bind(2, "issue", "forge.example/acme/widget", "42")
        bind(3, "review_request", "forge.example/acme/widget", "7")
        bind(4, "meeting", "record_synthetic", "action-1")
        # This machine grants repository work and nothing else.
        TaskExecutionService(
            self.database,
            clock=self.clock,
            planning_grants=["issue", "review_request"],
            profile_registry=self.service._profile_registry,
        ).schedule_new(limit=10)

        def status(task_id):
            with closing(sqlite3.connect(self.database)) as connection:
                connection.row_factory = sqlite3.Row
                return connection.execute(
                    "SELECT status,phase FROM task_execution_workflows "
                    "WHERE task_id=?", (task_id,)).fetchone()

        issue = status(2)
        self.assertEqual((issue["status"], issue["phase"]),
                         ("queued", "plan"))

        review = status(3)
        self.assertEqual((review["status"], review["phase"]),
                         ("queued", "plan"))

        # A task nobody granted standing permission for is still asked
        # about. The meeting it came from is not an enrolment.
        meeting = status(4)
        self.assertEqual(meeting["status"], "awaiting_start")

        # And a task bound to nothing at all keeps its gate.
        unbound = status(1)
        self.assertEqual(unbound["status"], "awaiting_start")

    def test_a_granted_repository_origin_skips_an_explicit_start_gate(self):
        """Direct scheduling must honor the same host grant as the runner."""
        self._bind_origin(1, "issue")
        service = TaskExecutionService(
            self.database, clock=self.clock, planning_grants=["issue"],
            profile_registry=self.service._profile_registry,
        )

        scheduled = service.schedule(1, expected_task_version=1)

        self.assertEqual(
            (scheduled.status, scheduled.phase),
            (WorkflowStatus.QUEUED, WorkflowPhase.PLAN),
        )

    def test_a_new_planning_grant_promotes_an_existing_start_gate(self):
        """A previously delivered Start card must not survive the grant."""
        self._bind_origin(1, "review_request")
        gated = self.service.schedule(1, expected_task_version=1)
        self.assertEqual(gated.status, WorkflowStatus.AWAITING_START)
        service = TaskExecutionService(
            self.database, clock=self.clock,
            planning_grants=["review_request"],
            profile_registry=self.service._profile_registry,
        )

        result = service.schedule_new()

        self.assertEqual((result.scheduled, result.remaining), (1, 0))
        promoted = service.get(1)
        self.assertEqual(
            (promoted.status, promoted.phase),
            (WorkflowStatus.QUEUED, WorkflowPhase.PLAN),
        )

    def test_schedule_new_is_bounded_and_never_resets_existing_workflows(self):
        with closing(sqlite3.connect(self.database)) as connection:
            for task_id, status in ((2, "open"), (3, "open"), (4, "open"),
                                    (5, "done")):
                connection.execute(
                    "INSERT INTO tasks(id,status,text,owner,due,version,"
                    "created_at,updated_at,closed_at) VALUES(?,?,?,?,NULL,1,?,?,?)",
                    (
                        task_id,
                        status,
                        f"Synthetic task {task_id}",
                        "Person A",
                        self._now(),
                        self._now(),
                        self._now() if status == "done" else None,
                    ),
                )
            connection.commit()
        existing = self.service.schedule(2, expected_task_version=1)
        cancelled = self.service.start_action(
            2, expected_version=existing.version, action="cancel"
        )

        first = self.service.schedule_new(limit=1)
        self.assertEqual((first.scheduled, first.remaining), (1, 2))
        self.assertEqual(
            self.service.get(1).status, WorkflowStatus.AWAITING_START
        )
        cancelled_state = self.service.get(2)
        self.assertEqual(cancelled_state.status, WorkflowStatus.CANCELLED)
        self.assertEqual(cancelled_state.version, cancelled.version)
        self.assertIsNone(self.service.get(3))

        second = self.service.schedule_new(limit=10)
        self.assertEqual((second.scheduled, second.remaining), (2, 0))
        self.assertIsNotNone(self.service.get(3))
        self.assertIsNotNone(self.service.get(4))
        self.assertIsNone(self.service.get(5))
        cancelled_state = self.service.get(2)
        self.assertEqual(cancelled_state.status, WorkflowStatus.CANCELLED)
        self.assertEqual(cancelled_state.version, cancelled.version)

        replay = self.service.schedule_new(limit=10)
        self.assertEqual((replay.scheduled, replay.remaining), (0, 0))

    def test_new_work_and_reader_waiting_have_separate_gw_caps(self):
        with closing(sqlite3.connect(self.database)) as connection:
            for task_id in range(2, 36):
                connection.execute(
                    "INSERT INTO tasks(id,status,text,owner,due,version,"
                    "created_at,updated_at,closed_at) "
                    "VALUES(?,'open',?,'Person A',NULL,1,?,?,NULL)",
                    (
                        task_id,
                        f"Synthetic task {task_id}",
                        self._now(),
                        self._now(),
                    ),
                )
                kind = "issue" if task_id <= 11 else "meeting"
                connection.execute(
                    "INSERT INTO candidate_inbox(candidate_id,source_system,"
                    "source_kind,source_record_id,source_item_id,"
                    "source_revision,payload_json,created_at,"
                    "first_imported_at,updated_at) "
                    "VALUES(?,'gw',?,?,?,?,'{}',?,?,?)",
                    (
                        f"candidate-{task_id}",
                        kind,
                        f"record-{task_id}",
                        f"item-{task_id}",
                        "c" * 64,
                        self._now(),
                        self._now(),
                        self._now(),
                    ),
                )
                connection.execute(
                    "INSERT INTO task_candidate_bindings(candidate_id,"
                    "source_revision,task_id,relation,decided_at) "
                    "VALUES(?,?,?,'accepted',?)",
                    (
                        f"candidate-{task_id}",
                        "c" * 64,
                        task_id,
                        self._now(),
                    ),
                )
            connection.commit()

        result = TaskExecutionService(
            self.database,
            clock=self.clock,
            planning_grants=["issue"],
            profile_registry=self.service._profile_registry,
        ).schedule_new(limit=100)
        self.assertEqual(
            (EXECUTION_SLOT_CAP, PLAN_READY_CAP, AWAITING_READER_CAP),
            (2, 10, 20),
        )
        self.assertEqual(
            (result.scheduled, result.remaining),
            (PLAN_READY_CAP + AWAITING_READER_CAP, 5),
        )
        with closing(sqlite3.connect(self.database)) as connection:
            counts = dict(connection.execute(
                "SELECT status,COUNT(*) FROM task_execution_workflows "
                "GROUP BY status"
            ).fetchall())
        self.assertEqual(counts, {"awaiting_start": 20, "queued": 10})

        # A capacity-bound pass is passive: no existing row is removed or
        # rewritten merely to make room for another candidate.
        replay = TaskExecutionService(
            self.database,
            clock=self.clock,
            planning_grants=["issue"],
            profile_registry=self.service._profile_registry,
        ).schedule_new(limit=100)
        self.assertEqual((replay.scheduled, replay.remaining), (0, 5))
        with closing(sqlite3.connect(self.database)) as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM task_execution_workflows"
                ).fetchone()[0],
                PLAN_READY_CAP + AWAITING_READER_CAP,
            )

    def test_two_claims_can_run_but_a_third_is_not_claimed(self):
        with closing(sqlite3.connect(self.database)) as connection:
            for task_id in (2, 3):
                connection.execute(
                    "INSERT INTO tasks(id,status,text,owner,due,version,"
                    "created_at,updated_at,closed_at) "
                    "VALUES(?,'open',?,'Person A',NULL,1,?,?,NULL)",
                    (task_id, f"Synthetic task {task_id}", self._now(),
                     self._now()),
                )
            connection.commit()
        for task_id in (1, 2, 3):
            scheduled = self.service.schedule(task_id, expected_task_version=1)
            self.service.start_action(
                task_id, expected_version=scheduled.version, action="start"
            )

        first = self._claim()
        second = self._claim()
        self.assertEqual({first.task_id, second.task_id}, {1, 2})
        self.assertIsNone(self.service.claim_next())
        self.assertEqual(self.service.get(3).status, WorkflowStatus.QUEUED)

    def test_start_gate_snooze_cancel_and_stale_taps_are_fenced(self):
        scheduled = self.service.schedule(1, expected_task_version=1)
        snoozed = self.service.start_action(
            1, expected_version=scheduled.version, action="snooze"
        )
        self.assertEqual(snoozed.status, WorkflowStatus.SNOOZED)
        self.assertIsNotNone(snoozed.wake_at)
        early = self.service.start_action(
            1, expected_version=snoozed.version, action="start"
        )
        self.assertEqual(early.refusal, WorkflowRefusal.INVALID_STATE)
        self.clock.advance(days=1)
        started = self.service.start_action(
            1, expected_version=snoozed.version, action="start"
        )
        self.assertEqual(started.status, WorkflowStatus.QUEUED)
        stale = self.service.start_action(
            1, expected_version=snoozed.version, action="cancel"
        )
        self.assertEqual(stale.refusal, WorkflowRefusal.STALE_WORKFLOW)

        claim = self._claim()
        released = self.service.release(
            1,
            expected_version=claim.workflow_version,
            claim_token=claim.token,
        )
        self.assertEqual(released.status, WorkflowStatus.QUEUED)

    def test_claim_renew_release_and_capability_fences(self):
        self._schedule_and_start()
        claim = self._claim()
        self.assertEqual(claim.phase, WorkflowPhase.PLAN)
        self.assertEqual(claim.text, "Synthetic task")
        self.assertIsNone(self.service.claim_next())

        wrong = self.service.renew(
            1,
            expected_version=claim.workflow_version,
            claim_token=OTHER_TOKEN,
        )
        self.assertEqual(wrong.refusal, WorkflowRefusal.CLAIM_MISMATCH)
        self.clock.advance(seconds=30)
        renewed = self.service.renew(
            1,
            expected_version=claim.workflow_version,
            claim_token=claim.token,
        )
        self.assertEqual(renewed.status, WorkflowStatus.RUNNING)
        self.assertEqual(renewed.version, claim.workflow_version)
        released = self.service.release(
            1,
            expected_version=claim.workflow_version,
            claim_token=claim.token,
        )
        self.assertEqual(released.status, WorkflowStatus.QUEUED)
        repeated = self.service.release(
            1,
            expected_version=claim.workflow_version,
            claim_token=claim.token,
        )
        self.assertEqual(repeated.refusal, WorkflowRefusal.STALE_WORKFLOW)

    def test_claim_phase_allowlist_is_atomic_and_leaves_other_work_queued(self):
        self._schedule_and_start()
        plan_claim = self._claim()
        recorded = self.service.record_result(self._result(plan_claim))
        execute = self.service.review_action(
            1, expected_version=recorded.version, action="approve"
        )
        self.assertEqual(execute.phase, WorkflowPhase.EXECUTE)

        with closing(sqlite3.connect(self.database)) as connection:
            connection.execute(
                "INSERT INTO tasks(id,status,text,owner,due,version,"
                "created_at,updated_at,closed_at) VALUES(2,'open',"
                "'Synthetic task 2','Person A',NULL,1,?,?,NULL)",
                (self._now(), self._now()),
            )
            connection.commit()
        scheduled = self.service.schedule(2, expected_task_version=1)
        self.service.start_action(
            2, expected_version=scheduled.version, action="start"
        )

        execute_before = self.service.get(1)
        plan = self.service.claim_next(
            allowed_phases=(WorkflowPhase.PLAN,),
        )
        self.assertIsNotNone(plan)
        self.assertEqual((plan.task_id, plan.phase), (2, WorkflowPhase.PLAN))
        execute_after = self.service.get(1)
        self.assertEqual(execute_after, execute_before)

        self.service.release(
            plan.task_id,
            expected_version=plan.workflow_version,
            claim_token=plan.token,
        )
        selected = self.service.claim_next(
            allowed_phases=(WorkflowPhase.EXECUTE,),
        )
        self.assertIsNotNone(selected)
        self.assertEqual(
            (selected.task_id, selected.phase),
            (1, WorkflowPhase.EXECUTE),
        )
        waiting = self.service.record_result(self._result(
            selected,
            result_id="result-002",
            outcome=ExecutionOutcome.AWAITING_EXTERNAL,
        ))
        external = self.service.review_action(
            1, expected_version=waiting.version, action="approve"
        )
        self.assertEqual(external.phase, WorkflowPhase.EXTERNAL_ACTION)
        external_before = self.service.get(1)

        plan = self.service.claim_next(
            allowed_phases=(WorkflowPhase.PLAN,),
        )
        self.assertIsNotNone(plan)
        self.assertEqual((plan.task_id, plan.phase), (2, WorkflowPhase.PLAN))
        self.assertEqual(self.service.get(1), external_before)

    def test_claim_phase_allowlist_rejects_invalid_configuration(self):
        self._schedule_and_start()
        for phases in (
            (),
            (WorkflowPhase.PLAN, WorkflowPhase.PLAN),
            ("unknown",),
            "plan",
        ):
            with self.subTest(phases=phases):
                with self.assertRaises(ValueError):
                    self.service.claim_next(allowed_phases=phases)
                self.assertEqual(
                    self.service.get(1).status,
                    WorkflowStatus.QUEUED,
                )

    def test_cancel_is_fenced_and_terminal_workflow_can_be_rescheduled(self):
        scheduled = self.service.schedule(1, expected_task_version=1)
        cancelled = self.service.start_action(
            1, expected_version=scheduled.version, action="cancel"
        )
        self.assertEqual(cancelled.status, WorkflowStatus.CANCELLED)
        stale = self.service.start_action(
            1, expected_version=scheduled.version, action="start"
        )
        self.assertEqual(stale.refusal, WorkflowRefusal.STALE_WORKFLOW)

        rescheduled = self.service.schedule(1, expected_task_version=1)

        self.assertEqual(rescheduled.status, WorkflowStatus.AWAITING_START)
        self.assertGreater(rescheduled.version, cancelled.version)
        self.assertIsNone(self.service.get(1).completed_at)

    def test_result_progression_is_strict_private_and_idempotent(self):
        self._schedule_and_start()
        plan_claim = self._claim()
        recorded = self.service.record_result(self._result(plan_claim))
        self.assertEqual(recorded.status, WorkflowStatus.AWAITING_REVIEW)
        self.assertEqual(recorded.phase, WorkflowPhase.PLAN)
        event_count = self.service.event_count()
        replay = self.service.record_result(self._result(plan_claim))
        self.assertEqual(replay.disposition, WorkflowDisposition.UNCHANGED)
        self.assertEqual(self.service.event_count(), event_count)
        conflict = self.service.record_result(ExecutionResultEnvelope(
            **{
                **self._result(plan_claim).__dict__,
                "summary": "Changed synthetic summary",
            }
        ))
        self.assertEqual(conflict.refusal, WorkflowRefusal.RESULT_CONFLICT)

        approved = self.service.review_action(
            1, expected_version=recorded.version, action="approve"
        )
        self.assertEqual(approved.phase, WorkflowPhase.EXECUTE)
        execute_claim = self._claim()
        waiting = self.service.record_result(self._result(
            execute_claim,
            result_id="result-002",
            outcome=ExecutionOutcome.AWAITING_EXTERNAL,
        ))
        self.assertEqual(waiting.status, WorkflowStatus.AWAITING_REVIEW)
        external = self.service.review_action(
            1, expected_version=waiting.version, action="approve"
        )
        self.assertEqual(external.phase, WorkflowPhase.EXTERNAL_ACTION)
        external_claim = self._claim()
        completed = self.service.record_result(self._result(
            external_claim,
            result_id="result-003",
            outcome=ExecutionOutcome.COMPLETED,
        ))
        self.assertEqual(completed.status, WorkflowStatus.AWAITING_REVIEW)
        self.assertEqual(self.service.result_count(), 3)
        task = TaskLedger(self.database, clock=self.clock).get(1)
        self.assertEqual(task.status, "open")
        self.assertEqual(task.version, 1)

        with closing(sqlite3.connect(self.database)) as connection:
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute(
                    "UPDATE task_execution_results SET summary='changed'"
                )
            connection.rollback()
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute("DELETE FROM task_execution_events")

    def test_invalid_result_cannot_change_a_running_claim(self):
        self._schedule_and_start()
        claim = self._claim()
        invalid_phase = self.service.record_result(self._result(
            claim,
            outcome=ExecutionOutcome.AWAITING_EXTERNAL,
        ))
        self.assertEqual(
            invalid_phase.refusal, WorkflowRefusal.INVALID_ARGUMENT
        )
        oversized = self._result(claim)
        oversized = ExecutionResultEnvelope(
            **{**oversized.__dict__, "summary": "x" * 1201}
        )
        refused = self.service.record_result(oversized)
        self.assertEqual(refused.refusal, WorkflowRefusal.INVALID_ARGUMENT)
        self.assertEqual(self.service.get(1).status, WorkflowStatus.RUNNING)
        self.assertEqual(self.service.result_count(), 0)

    def test_failures_back_off_expire_park_and_retry(self):
        self._schedule_and_start()
        claim = self._claim()
        first = self.service.fail(
            1,
            expected_version=claim.workflow_version,
            claim_token=claim.token,
            reason="process_exit",
        )
        self.assertEqual(first.status, WorkflowStatus.QUEUED)
        self.assertIsNotNone(first.next_attempt_at)
        self.assertEqual(self.service.readiness().cooling, 1)
        self.clock.advance(seconds=60)

        self._claim()
        self.clock.advance(
            seconds=general_profile().claim_lease_seconds + 1
        )
        third_claim = self.service.claim_next()
        self.assertIsNone(third_claim)
        state = self.service.get(1)
        self.assertEqual(state.failure_count, 2)
        self.assertEqual(state.last_failure_reason, "claim_expired")
        self.assertEqual(state.status, WorkflowStatus.QUEUED)
        self.clock.advance(seconds=120)

        final_claim = self._claim()
        parked = self.service.fail(
            1,
            expected_version=final_claim.workflow_version,
            claim_token=final_claim.token,
            reason="timeout",
        )
        self.assertEqual(parked.status, WorkflowStatus.PARKED)
        health = self.service.readiness()
        self.assertEqual((health.parked, health.running), (1, 0))
        retried = self.service.retry(
            1, expected_version=parked.version
        )
        self.assertEqual(retried.status, WorkflowStatus.QUEUED)
        self.assertEqual(self.service.get(1).failure_count, 0)

    def test_task_transition_cancels_stale_work_before_claim(self):
        self._schedule_and_start()
        transitioned = TaskLedger(self.database, clock=self.clock).transition(
            1, expected_version=1, action="done"
        )
        self.assertTrue(transitioned.accepted)

        self.assertIsNone(self.service.claim_next())

        state = self.service.get(1)
        self.assertEqual(state.status, WorkflowStatus.CANCELLED)
        self.assertEqual(state.failure_count, 0)
        self.assertEqual(self.service.readiness().cancelled, 1)

    def test_task_transition_invalidates_an_active_claim(self):
        self._schedule_and_start()
        claim = self._claim()
        transitioned = TaskLedger(self.database, clock=self.clock).transition(
            1, expected_version=1, action="done"
        )
        self.assertTrue(transitioned.accepted)

        released = self.service.release(
            1,
            expected_version=claim.workflow_version,
            claim_token=claim.token,
        )
        failed = self.service.fail(
            1,
            expected_version=claim.workflow_version,
            claim_token=claim.token,
            reason="process_exit",
        )
        self.assertEqual(released.refusal, WorkflowRefusal.STALE_TASK)
        self.assertEqual(failed.refusal, WorkflowRefusal.STALE_TASK)

        self.assertIsNone(self.service.claim_next())
        state = self.service.get(1)
        self.assertEqual(state.status, WorkflowStatus.CANCELLED)
        self.assertEqual(state.failure_count, 0)

    def test_readiness_is_aggregate_only(self):
        self.service.schedule(1, expected_task_version=1)
        health = self.service.readiness()
        self.assertEqual(health.awaiting_start, 1)
        self.assertEqual(sum(health.__dict__.values()), 1)
        self.assertNotIn("Synthetic", repr(health))
        self.assertNotIn(TOKEN, repr(health))


if __name__ == "__main__":
    unittest.main()
