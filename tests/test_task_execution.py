#!/usr/bin/env python3
"""Synthetic tests for Foxhound-owned task execution workflow state."""

from __future__ import annotations

from foxhound import migrate_database

import json
import sqlite3
import tempfile
import threading
import unittest
from contextlib import closing
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path

from foxhound.agent_profiles import (
    AgentProfileRegistry,
    WORKER_COMMAND_TOKEN,
    general_profile,
    parse_profile,
)
from foxhound.candidate_inbox import CandidateInbox, SCHEMA_VERSION
from foxhound.execution_cards import (
    ExecutionCardRefusal,
    ExecutionCardService,
    ExecutionCardStatus,
)
from foxhound.task_execution import (
    AWAITING_READER_CAP,
    EXECUTION_SLOT_CAP,
    PLAN_READY_CAP,
    UNBOUNDED_CAP,
    WORK_IN_PROGRESS_CAP,
    ExecutionOutcome,
    ExecutionResultEnvelope,
    TaskExecutionService,
    WorkflowDisposition,
    WorkflowPhase,
    WorkflowPriority,
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
        migrate_database(self.database)
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
                _profile(
                    "repository-agent",
                    phases=("plan", "execute", "external_action"),
                ),
            )),
        )

    def _now(self) -> str:
        return self.clock().isoformat(timespec="seconds")

    def _add_task(self, task_id: int, text: str) -> None:
        """Seed one more open task so the ready queue has depth."""
        with closing(sqlite3.connect(self.database)) as connection:
            connection.execute(
                "INSERT INTO tasks(id,status,text,owner,due,version,"
                "created_at,updated_at,closed_at) "
                "VALUES(?,'open',?,'Person A',NULL,1,?,?,NULL)",
                (task_id, text, self._now(), self._now()),
            )
            connection.execute(
                "INSERT INTO task_events(task_id,kind,task_version,"
                "candidate_id,source_revision,from_status,to_status,"
                "occurred_at) VALUES(?,'created',1,NULL,NULL,NULL,'open',?)",
                (task_id, self._now()),
            )
            connection.commit()

    def _clear_retry_backoff(self, task_id: int) -> None:
        """Make a deferred workflow ready again without moving the clock."""
        with closing(sqlite3.connect(self.database)) as connection:
            connection.execute(
                "UPDATE task_execution_workflows SET next_attempt_at=NULL "
                "WHERE task_id=?",
                (task_id,),
            )
            connection.commit()

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

    def test_repository_references_are_validated_and_persisted(self):
        scheduled = self.service.schedule(1, expected_task_version=1)
        self.service.start_action(
            1, expected_version=scheduled.version, action="start")
        claim = self._claim()
        result = replace(self._result(claim), repository_references=(
            {"kind": "pull-request",
             "url": "https://github.com/example-org/example-repo/pull/12"},
            {"kind": "commit",
             "url": "https://github.com/example-org/example-repo/commit/abcdef1234567"},
            {"kind": "check",
             "url": "https://github.com/example-org/example-repo/actions/runs/34"},
        ))
        self.assertTrue(self.service.record_result(result).accepted)
        with closing(sqlite3.connect(self.database)) as connection:
            stored = connection.execute(
                "SELECT repository_references_json,repository_impact "
                "FROM task_execution_results "
                "WHERE result_id='result-001'"
            ).fetchone()
        self.assertEqual(json.loads(stored[0])[1]["kind"], "commit")
        self.assertEqual(stored[1], 1)

        invalid = replace(self._result(claim, result_id="result-invalid"),
                          repository_references=(
                              {"kind": "commit",
                               "url": "https://example.com/commit/abcdef1"},
                          ))
        refused = self.service.record_result(invalid)
        self.assertEqual(refused.refusal, WorkflowRefusal.INVALID_ARGUMENT)

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
            connection.execute(
                "ALTER TABLE task_review_cards DROP COLUMN source_revision"
            )
            connection.execute("PRAGMA user_version = 7")
            connection.commit()

        migrate_database(self.database)

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
            # v42 added this; a database older than that has not
            # got it yet.
            connection.execute(
                "ALTER TABLE task_execution_results DROP COLUMN "
                "reader_instruction_sequence"
            )
            connection.execute(
                "ALTER TABLE task_execution_results DROP COLUMN "
                "repository_references_json"
            )
            connection.execute(
                "ALTER TABLE task_execution_results DROP COLUMN "
                "repository_impact"
            )
            connection.execute(
                "DROP INDEX task_execution_workflows_priority_ready"
            )
            connection.execute(
                "ALTER TABLE task_execution_workflows DROP COLUMN "
                "queue_priority"
            )
            connection.execute(
                "ALTER TABLE task_execution_workflows DROP COLUMN "
                "last_failure_exit_code"
            )
            connection.execute(
                "ALTER TABLE task_execution_workflows DROP COLUMN "
                "last_failure_run_id"
            )
            # ADR 0036 added this at v23; a database at an older version
            # has not got it yet.
            connection.execute(
                "ALTER TABLE task_review_cards DROP COLUMN consumer_digest"
            )
            connection.execute(
                "ALTER TABLE execution_review_cards DROP COLUMN "
                "consumer_digest"
            )
            # v35 added these; a database at an older version has not got
            # them yet.
            connection.execute(
                "ALTER TABLE execution_review_cards DROP COLUMN "
                "superseded_delivery_ref"
            )
            connection.execute(
                "ALTER TABLE execution_review_cards DROP COLUMN "
                "superseded_transport"
            )
            connection.execute(
                "ALTER TABLE task_review_cards DROP COLUMN source_revision"
            )
            # v39 added this; a database at an older version has not got it.
            connection.execute(
                "ALTER TABLE execution_review_cards DROP COLUMN "
                "work_revision_id"
            )
            # v54 added the informational-delivery marker and split the one
            # active-card index in two; an older database has one index and
            # no such column.
            connection.execute(
                "DROP INDEX execution_review_cards_one_active_summary")
            connection.execute(
                "DROP INDEX execution_review_cards_one_active")
            connection.execute(
                "ALTER TABLE execution_review_cards DROP COLUMN summary_only"
            )
            connection.execute(
                "CREATE UNIQUE INDEX execution_review_cards_one_active "
                "ON execution_review_cards(task_id) "
                "WHERE status IN ('pending','delivering','delivered')"
            )
            connection.execute("PRAGMA user_version = 11")
            connection.commit()

        migrate_database(self.database)

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

    def test_a_retired_revision_is_available_but_not_current(self):
        """`available` says it resolves, not that it carries the budget.

        A revision kept in the store for replay resolves exactly and runs
        perfectly, so a workflow pinned to it reports healthy while being
        held to a timeout and turn limit the operator has replaced. That is
        how a deployment ran the bulk of its queue on two thirds of the
        budget it had configured, with nothing naming the cause.
        """
        retired = _profile(phases=("plan", "execute"))
        current = parse_profile({
            **{
                "schema": "foxhound.agent-profile",
                "schema_version": 1,
                "profile_id": retired.profile_id,
                "display_name": "Synthetic Specialist",
                "runtime": "hermes",
                "prompt_template": f"Use {WORKER_COMMAND_TOKEN} context.",
                "toolsets": ["terminal", "file"],
                "heartbeat_seconds": 60,
                "kill_grace_seconds": 30,
                "allowed_phases": ["plan", "execute"],
            },
            # The raise an operator makes, and the whole reason the
            # distinction matters. The lease grows with it because the
            # profile guard requires it to exceed timeout + kill grace.
            "max_turns": 120,
            "timeout_seconds": 3_300,
            "claim_lease_seconds": 3_600,
        })
        self.assertNotEqual(retired.revision, current.revision)

        # Schedule and pin the workflow to the retired revision.
        pinning = TaskExecutionService(
            self.database,
            clock=self.clock,
            profile_registry=AgentProfileRegistry(
                (general_profile(), retired)
            ),
        )
        scheduled = pinning.schedule(1, expected_task_version=1)
        selected = pinning.select_agent(
            1,
            expected_version=scheduled.version,
            profile_id=retired.profile_id,
            profile_revision=retired.revision,
        )
        self.assertTrue(selected.accepted)

        # The operator then installs a larger revision of the same profile.
        # The retired one stays resolvable, as replay requires.
        after = TaskExecutionService(
            self.database,
            clock=self.clock,
            profile_registry=AgentProfileRegistry(
                (general_profile(), current),
                historical_profiles=(retired,),
            ),
        )
        health = {
            row.agent_profile_revision: row for row in after.profile_health()
        }
        pinned = health[retired.revision]
        self.assertTrue(pinned.available)
        self.assertFalse(pinned.current)

        superseded = after.superseded_profile_revisions()
        self.assertEqual(
            [row.agent_profile_revision for row in superseded],
            [retired.revision],
        )
        self.assertEqual(superseded[0].workflows, 1)
        self.assertNotIn("Synthetic task", repr(superseded))

    def test_current_revision_is_not_reported_as_superseded(self):
        """The report must be empty when nothing is behind, or it is noise."""
        profile = _profile(phases=("plan", "execute"))
        service = TaskExecutionService(
            self.database,
            clock=self.clock,
            profile_registry=AgentProfileRegistry(
                (general_profile(), profile)
            ),
        )
        scheduled = service.schedule(1, expected_task_version=1)
        self.assertTrue(service.select_agent(
            1,
            expected_version=scheduled.version,
            profile_id=profile.profile_id,
            profile_revision=profile.revision,
        ).accepted)
        health = {
            row.agent_profile_revision: row for row in service.profile_health()
        }
        self.assertTrue(health[profile.revision].current)
        self.assertEqual(service.superseded_profile_revisions(), ())

    def test_a_dropped_profile_is_not_reported_as_merely_superseded(self):
        """Two different faults, and conflating them hides the worse one.

        A profile the registry no longer offers at all cannot be moved to a
        newer revision, because there is none. `available` already reports
        it; listing it as superseded would suggest a remedy that does not
        exist.
        """
        dropped = _profile(phases=("plan", "execute"))
        pinning = TaskExecutionService(
            self.database,
            clock=self.clock,
            profile_registry=AgentProfileRegistry(
                (general_profile(), dropped)
            ),
        )
        scheduled = pinning.schedule(1, expected_task_version=1)
        self.assertTrue(pinning.select_agent(
            1,
            expected_version=scheduled.version,
            profile_id=dropped.profile_id,
            profile_revision=dropped.revision,
        ).accepted)

        after = TaskExecutionService(
            self.database,
            clock=self.clock,
            profile_registry=AgentProfileRegistry((general_profile(),)),
        )
        health = {
            row.agent_profile_revision: row for row in after.profile_health()
        }
        self.assertFalse(health[dropped.revision].available)
        self.assertFalse(health[dropped.revision].current)
        self.assertEqual(after.superseded_profile_revisions(), ())

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

        # A pin this machine cannot resolve defers THAT workflow instead of
        # raising out of the claim: the runner keeps working, and the row
        # carries the same failure reason the post-claim path records.
        without_profile = TaskExecutionService(
            self.database, clock=self.clock, token_factory=lambda: TOKEN
        )
        self.assertIsNone(without_profile.claim_next())
        self.assertEqual(without_profile.last_claim_deferred, (1,))
        deferred = without_profile.get(1)
        self.assertEqual(deferred.status, WorkflowStatus.QUEUED)
        self.assertEqual(deferred.last_failure_reason, "startup_failed")
        self.assertIsNotNone(deferred.next_attempt_at)

        self._clear_retry_backoff(1)
        claim = service.claim_next()
        recorded = service.record_result(self._result(claim))
        approved = service.review_action(
            1, expected_version=recorded.version, action="approve"
        )
        self.assertEqual(approved.phase, WorkflowPhase.EXECUTE)
        # The specialist allows `plan` only, so `execute` is ineligible. That
        # is a property of this workflow too, not of the runner.
        self.assertIsNone(service.claim_next())
        self.assertEqual(service.last_claim_deferred, (1,))
        self.assertEqual(service.get(1).status, WorkflowStatus.QUEUED)

    def test_an_unresolvable_pin_does_not_block_the_queue_behind_it(self):
        """One poisoned row must not stop every other ready workflow.

        This is the outage this scan exists to prevent: resolving the head of
        the queue raised, so a single workflow pinned to a revision the
        catalog had dropped stopped every healthy workflow behind it for as
        long as it stayed at the head.
        """
        specialist = _profile(phases=("plan", "execute"))
        registry = AgentProfileRegistry((general_profile(), specialist))
        self._add_task(2, "Second synthetic task")

        pinning = TaskExecutionService(
            self.database,
            clock=self.clock,
            token_factory=lambda: TOKEN,
            profile_registry=registry,
        )
        scheduled = pinning.schedule(1, expected_task_version=1)
        selected = pinning.select_agent(
            1,
            expected_version=scheduled.version,
            profile_id=specialist.profile_id,
            profile_revision=specialist.revision,
        )
        pinning.start_action(
            1, expected_version=selected.version, action="start"
        )
        second = pinning.schedule(2, expected_task_version=1)
        pinning.start_action(
            2, expected_version=second.version, action="start"
        )

        # Task 1 is at the head and its pin is gone from this registry.
        without_profile = TaskExecutionService(
            self.database, clock=self.clock, token_factory=lambda: TOKEN
        )
        claim = without_profile.claim_next()
        self.assertIsNotNone(claim)
        self.assertEqual(claim.task_id, 2)
        self.assertEqual(without_profile.last_claim_deferred, (1,))

    def test_queue_priority_is_bounded_fenced_and_consumed_by_claim(self):
        self._add_task(2, "Second synthetic task")
        self._add_task(3, "Third synthetic task")
        first = self._schedule_and_start()
        second = self.service.schedule(2, expected_task_version=1)
        second = self.service.start_action(
            2, expected_version=second.version, action="start"
        )
        third = self.service.schedule(3, expected_task_version=1)
        third = self.service.start_action(
            3, expected_version=third.version, action="start"
        )

        raised = self.service.set_priority(
            3, expected_version=third.version, action="raise"
        )
        self.assertEqual(raised.disposition, WorkflowDisposition.APPLIED)
        self.assertEqual(raised.priority, WorkflowPriority.RAISED)
        self.assertEqual(
            self.service.set_priority(
                3, expected_version=third.version, action="lower"
            ).refusal,
            WorkflowRefusal.STALE_WORKFLOW,
        )
        self.assertEqual(
            self.service.set_priority(
                2, expected_version=second.version, action="unexpected"
            ).refusal,
            WorkflowRefusal.INVALID_ACTION,
        )

        claim = self.service.claim_next()
        self.assertEqual(claim.task_id, 3)
        self.assertEqual(self.service.get(3).priority, WorkflowPriority.NORMAL)
        self.assertEqual(self.service.get(1).priority, WorkflowPriority.NORMAL)
        self.assertEqual(self.service.get(2).priority, WorkflowPriority.NORMAL)
        self.assertEqual(first.status, WorkflowStatus.QUEUED)

    def test_queue_priority_refuses_non_ready_workflow_states(self):
        scheduled = self.service.schedule(1, expected_task_version=1)
        self.assertEqual(
            self.service.set_priority(
                1, expected_version=scheduled.version, action="raise"
            ).refusal,
            WorkflowRefusal.INVALID_STATE,
        )
        snoozed = self.service.start_action(
            1, expected_version=scheduled.version, action="snooze"
        )
        self.assertEqual(
            self.service.set_priority(
                1, expected_version=snoozed.version, action="raise"
            ).refusal,
            WorkflowRefusal.INVALID_STATE,
        )
        self.clock.advance(days=2)
        resumed = self.service.start_action(
            1, expected_version=snoozed.version, action="start"
        )
        claimed = self.service.claim_next()
        self.assertEqual(claimed.task_id, 1)
        self.assertEqual(
            self.service.set_priority(
                1, expected_version=claimed.workflow_version, action="raise"
            ).refusal,
            WorkflowRefusal.INVALID_STATE,
        )
        self.assertEqual(resumed.status, WorkflowStatus.QUEUED)

    def test_concurrent_claims_preserve_raised_priority_without_duplication(self):
        self._add_task(2, "Second synthetic task")
        self._add_task(3, "Third synthetic task")
        self._schedule_and_start()
        for task_id in (2, 3):
            scheduled = self.service.schedule(task_id, expected_task_version=1)
            self.service.start_action(
                task_id, expected_version=scheduled.version, action="start"
            )
        ready = self.service.get(3)
        self.assertIsNotNone(ready)
        self.assertTrue(self.service.set_priority(
            3, expected_version=ready.version, action="raise"
        ).accepted)

        barrier = threading.Barrier(3)
        claimed: list[int] = []
        claimed_lock = threading.Lock()

        def claim_once() -> None:
            worker = TaskExecutionService(
                self.database,
                clock=self.clock,
                token_factory=lambda: TOKEN,
                profile_registry=AgentProfileRegistry((
                    general_profile(),
                    _profile(
                        "repository-agent",
                        phases=("plan", "execute", "external_action"),
                    ),
                )),
            )
            barrier.wait()
            claim = worker.claim_next()
            if claim is not None:
                with claimed_lock:
                    claimed.append(claim.task_id)

        threads = [threading.Thread(target=claim_once) for _ in range(2)]
        for thread in threads:
            thread.start()
        barrier.wait()
        for thread in threads:
            thread.join()

        self.assertEqual(len(claimed), 2)
        self.assertEqual(len(set(claimed)), 2)
        self.assertIn(3, claimed)

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

    def test_source_kind_route_selects_its_declared_profile(self):
        general = self.service._profile_registry.get("general")
        repository = self.service._profile_registry.get("repository-agent")
        service = TaskExecutionService(
            self.database, clock=self.clock,
            profile_registry=AgentProfileRegistry([general, repository]),
            profile_routes={"issue": repository.profile_id},
        )
        self.assertEqual(
            service._profile_for("issue").profile_id, repository.profile_id
        )
        self.assertEqual(
            service._profile_for("meeting").profile_id, general.profile_id
        )
        self._bind_origin(1, "issue")
        scheduled = service.schedule(1, expected_task_version=1)
        self.assertEqual(scheduled.agent_profile_id, repository.profile_id)
        # Routing applies when the workflow is created. Reopening the same
        # service with a different route must not rebind the durable choice.
        changed = TaskExecutionService(
            self.database,
            clock=self.clock,
            profile_registry=AgentProfileRegistry([general, repository]),
            profile_routes={"issue": general.profile_id},
        )
        self.assertEqual(changed.get(1).agent_profile_id, repository.profile_id)

    def test_a_route_to_an_uninstalled_profile_is_refused_at_startup(self):
        """A missing catalog is a configuration fault, not a scheduling one.

        The machine that renders the scheduler's command line is not the one
        that installs the private catalog, so the two can disagree. Saying so
        once at startup beats one failed pass per timer tick.
        """
        general = self.service._profile_registry.get("general")
        with self.assertRaisesRegex(ValueError, "routed agent profile"):
            TaskExecutionService(
                self.database, clock=self.clock,
                profile_registry=AgentProfileRegistry([general]),
                profile_routes={"issue": "not-installed"},
            )

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

    def test_a_declared_kind_starts_at_execute_without_a_plan_event(self):
        """Skipping a phase is explicit and avoids creating plan evidence."""
        self._bind_origin(1, "issue")
        service = TaskExecutionService(
            self.database,
            clock=self.clock,
            planning_grants=["issue"],
            execution_grants=["issue"],
            skip_planning_for=["issue"],
            profile_registry=self.service._profile_registry,
        )

        scheduled = service.schedule(1, expected_task_version=1)

        self.assertEqual(
            (scheduled.status, scheduled.phase),
            (WorkflowStatus.QUEUED, WorkflowPhase.EXECUTE),
        )
        with closing(sqlite3.connect(self.database)) as connection:
            events = connection.execute(
                "SELECT phase FROM task_execution_events WHERE task_id=?",
                (1,),
            ).fetchall()
            results = connection.execute(
                "SELECT COUNT(*) FROM task_execution_results WHERE task_id=?",
                (1,),
            ).fetchone()[0]
        self.assertEqual(events, [("execute",)])
        self.assertEqual(results, 0)

    def test_skip_planning_requires_execution_authority(self):
        with self.assertRaisesRegex(ValueError, "execution grants: issue"):
            TaskExecutionService(
                self.database,
                clock=self.clock,
                skip_planning_for=["issue"],
                profile_registry=self.service._profile_registry,
            )

    def test_skip_planning_requires_planning_authority(self):
        """The plan phase carries the start gate, so skipping it needs the
        grant that already removed that gate."""
        with self.assertRaisesRegex(ValueError, "planning grants: issue"):
            TaskExecutionService(
                self.database,
                clock=self.clock,
                execution_grants=["issue"],
                skip_planning_for=["issue"],
                profile_registry=self.service._profile_registry,
            )

    def test_a_skipped_workflow_does_not_consume_plan_queue_capacity(self):
        self._bind_origin(1, "issue")
        service = TaskExecutionService(
            self.database,
            clock=self.clock,
            planning_grants=["issue"],
            execution_grants=["issue"],
            skip_planning_for=["issue"],
            plan_ready_cap=0,
            profile_registry=self.service._profile_registry,
        )

        result = service.schedule_new()

        self.assertEqual((result.scheduled, result.capped), (1, 0))
        self.assertEqual(service.get(1).phase, WorkflowPhase.EXECUTE)

    def test_planning_without_execution_keeps_the_plan_for_reader_review(self):
        self._bind_origin(1, "issue")
        service = TaskExecutionService(
            self.database,
            clock=self.clock,
            planning_grants=["issue"],
            profile_registry=self.service._profile_registry,
        )
        scheduled = service.schedule(1, expected_task_version=1)
        claim = service.claim_next()
        self.assertIsNotNone(claim)

        recorded = service.record_result(self._result(claim))

        self.assertEqual(
            (scheduled.phase, recorded.status, recorded.phase),
            (
                WorkflowPhase.PLAN,
                WorkflowStatus.AWAITING_REVIEW,
                WorkflowPhase.PLAN,
            ),
        )

    def test_a_skip_declaration_does_not_rewrite_an_existing_plan(self):
        self._bind_origin(1, "issue")
        original = TaskExecutionService(
            self.database,
            clock=self.clock,
            planning_grants=["issue"],
            profile_registry=self.service._profile_registry,
        ).schedule(1, expected_task_version=1)
        self.assertEqual(original.phase, WorkflowPhase.PLAN)

        replay = TaskExecutionService(
            self.database,
            clock=self.clock,
            planning_grants=["issue"],
            execution_grants=["issue"],
            skip_planning_for=["issue"],
            profile_registry=self.service._profile_registry,
        ).schedule(1, expected_task_version=1)

        self.assertEqual(replay.disposition, WorkflowDisposition.UNCHANGED)
        self.assertEqual(replay.phase, WorkflowPhase.PLAN)

    def test_a_new_planning_grant_promotes_an_existing_start_gate(self):
        """A previously delivered Start card must not survive the grant."""
        self._bind_origin(1, "review_request")
        gated = self.service.schedule(1, expected_task_version=1)
        self.assertEqual(gated.status, WorkflowStatus.AWAITING_START)
        cards = ExecutionCardService(
            self.database,
            clock=self.clock,
            token_factory=lambda: TOKEN,
            profile_registry=self.service._profile_registry,
        )
        cards.schedule()
        claim = cards.claim_next()
        self.assertIsNotNone(claim)
        delivered = cards.complete_delivery(
            claim.card.id,
            expected_version=claim.card.version,
            claim_token=claim.token,
            transport="synthetic",
            delivery_ref="message-1",
        )
        self.assertEqual(delivered.card_status, ExecutionCardStatus.DELIVERED)
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
        stale = cards.act(
            claim.card.id,
            expected_version=delivered.card_version,
            action="start",
        )
        self.assertEqual(stale.refusal, ExecutionCardRefusal.STALE_VERSION)
        with closing(sqlite3.connect(self.database)) as connection:
            status = connection.execute(
                "SELECT status FROM execution_review_cards WHERE id=?",
                (claim.card.id,),
            ).fetchone()[0]
        self.assertEqual(status, "cancelled")

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

    def test_schedule_new_prioritizes_reviews_then_communication_then_issues(
        self,
    ):
        """Planning reserve follows the three source-priority tiers."""
        for task_id, kind in (
            (2, "issue"),
            (3, "email"),
            (4, "meeting"),
            (5, "review_request"),
        ):
            self._add_task(task_id, f"Synthetic task {task_id}")
            self._bind_origin(task_id, kind)

        service = TaskExecutionService(
            self.database,
            clock=self.clock,
            planning_grants=["email", "issue", "meeting", "review_request"],
            plan_ready_cap=3,
            profile_registry=self.service._profile_registry,
        )

        result = service.schedule_new(limit=10)

        self.assertEqual((result.scheduled, result.remaining), (4, 1))
        self.assertEqual(service.get(3).status, WorkflowStatus.QUEUED)
        self.assertEqual(service.get(4).status, WorkflowStatus.QUEUED)
        self.assertEqual(service.get(5).status, WorkflowStatus.QUEUED)
        self.assertIsNone(service.get(2))

    def test_claim_prioritizes_reviews_then_communication_over_a_raised_issue(
        self,
    ):
        """Source tiers are stronger than an issue's explicit queue raise."""
        self._add_task(2, "Synthetic issue task")
        self._add_task(3, "Synthetic email task")
        self._add_task(4, "Synthetic meeting task")
        self._add_task(5, "Synthetic review task")
        self._bind_origin(2, "issue")
        self._bind_origin(3, "email")
        self._bind_origin(4, "meeting")
        self._bind_origin(5, "review_request")
        service = TaskExecutionService(
            self.database,
            clock=self.clock,
            token_factory=lambda: TOKEN,
            execution_slot_cap=4,
            profile_registry=self.service._profile_registry,
        )
        started = {}
        for task_id in (2, 3, 4, 5):
            workflow = service.schedule(task_id, expected_task_version=1)
            started[task_id] = service.start_action(
                task_id, expected_version=workflow.version, action="start"
            )
        service.set_priority(
            2, expected_version=started[2].version, action="raise"
        )

        claims = [service.claim_next() for _ in range(4)]

        self.assertEqual([claim.task_id for claim in claims], [5, 3, 4, 2])

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

    def _seed_issue_and_meeting_tasks(self) -> None:
        """34 open tasks: 10 issue-kind, 24 meeting-kind, alongside task 1."""
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

    def test_plan_ready_and_awaiting_reader_caps_are_overridable(self):
        self._seed_issue_and_meeting_tasks()
        narrow = TaskExecutionService(
            self.database,
            clock=self.clock,
            planning_grants=["issue"],
            profile_registry=self.service._profile_registry,
            plan_ready_cap=3,
            awaiting_reader_cap=4,
        )
        result = narrow.schedule_new(limit=100)
        self.assertEqual((result.scheduled, result.remaining), (7, 28))
        with closing(sqlite3.connect(self.database)) as connection:
            counts = dict(connection.execute(
                "SELECT status,COUNT(*) FROM task_execution_workflows "
                "GROUP BY status"
            ).fetchall())
        self.assertEqual(counts, {"awaiting_start": 4, "queued": 3})

    def test_plan_ready_and_awaiting_reader_caps_support_unbounded(self):
        self._seed_issue_and_meeting_tasks()
        wide = TaskExecutionService(
            self.database,
            clock=self.clock,
            planning_grants=["issue"],
            profile_registry=self.service._profile_registry,
            plan_ready_cap=UNBOUNDED_CAP,
            awaiting_reader_cap=UNBOUNDED_CAP,
        )
        result = wide.schedule_new(limit=100)
        self.assertEqual((result.scheduled, result.remaining), (35, 0))

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

    def test_execution_slot_cap_override_lowers_concurrent_claims(self):
        narrow = TaskExecutionService(
            self.database,
            clock=self.clock,
            token_factory=lambda: TOKEN,
            profile_registry=self.service._profile_registry,
            execution_slot_cap=1,
        )
        self._add_task(2, "Synthetic task 2")
        for task_id in (1, 2):
            scheduled = narrow.schedule(task_id, expected_task_version=1)
            narrow.start_action(
                task_id, expected_version=scheduled.version, action="start"
            )
        self.assertIsNotNone(narrow.claim_next())
        self.assertIsNone(narrow.claim_next())

    def test_execution_slot_cap_unbounded_allows_more_than_default(self):
        wide = TaskExecutionService(
            self.database,
            clock=self.clock,
            token_factory=lambda: TOKEN,
            profile_registry=self.service._profile_registry,
            execution_slot_cap=UNBOUNDED_CAP,
        )
        for task_id in (2, 3):
            self._add_task(task_id, f"Synthetic task {task_id}")
        for task_id in (1, 2, 3):
            scheduled = wide.schedule(task_id, expected_task_version=1)
            wide.start_action(
                task_id, expected_version=scheduled.version, action="start"
            )
        claimed = {wide.claim_next().task_id for _ in range(3)}
        self.assertEqual(claimed, {1, 2, 3})

    def test_start_gate_snooze_cancel_and_stale_taps_are_fenced(self):
        scheduled = self.service.schedule(1, expected_task_version=1)
        snoozed = self.service.start_action(
            1, expected_version=scheduled.version, action="snooze"
        )
        self.assertEqual(snoozed.status, WorkflowStatus.SNOOZED)
        self.assertIsNotNone(snoozed.wake_at)
        # A scheduler retry must leave a future snooze completely intact.
        # This is distinct from refusing an early reader tap: historical
        # early cards were created when a scheduler rewrote the workflow and
        # then presented a fresh Start gate.
        rescheduled = self.service.schedule(1, expected_task_version=1)
        self.assertEqual(
            (rescheduled.disposition, rescheduled.status,
             rescheduled.version, rescheduled.wake_at),
            (WorkflowDisposition.UNCHANGED, WorkflowStatus.SNOOZED,
             snoozed.version, snoozed.wake_at),
        )
        self.assertEqual(self.service.schedule_new().scheduled, 0)
        self.assertIsNone(self.service.claim_next())
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

    def test_current_run_identity_is_fenced_and_cleared_with_the_claim(self):
        self._schedule_and_start()
        claim = self._claim()
        wrong = self.service.attach_run_id(
            1,
            expected_version=claim.workflow_version,
            claim_token=OTHER_TOKEN,
            run_id="a" * 32,
        )
        self.assertEqual(wrong.refusal, WorkflowRefusal.CLAIM_MISMATCH)
        self.assertIsNone(self.service.get(1).current_run_id)
        attached = self.service.attach_run_id(
            1,
            expected_version=claim.workflow_version,
            claim_token=claim.token,
            run_id="a" * 32,
        )
        self.assertEqual(attached.disposition, WorkflowDisposition.APPLIED)
        self.assertEqual(self.service.get(1).current_run_id, "a" * 32)
        released = self.service.release(
            1,
            expected_version=claim.workflow_version,
            claim_token=claim.token,
        )
        self.assertEqual(released.status, WorkflowStatus.QUEUED)
        self.assertIsNone(self.service.get(1).current_run_id)

    def test_recording_a_result_clears_the_current_run_identity(self):
        self._schedule_and_start()
        claim = self._claim()
        self.assertTrue(self.service.attach_run_id(
            1,
            expected_version=claim.workflow_version,
            claim_token=claim.token,
            run_id="b" * 32,
        ).accepted)
        recorded = self.service.record_result(self._result(claim))
        self.assertTrue(recorded.accepted)
        self.assertIsNone(self.service.get(1).current_run_id)

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

    def _grant_service(
        self, *kinds: str, acting: tuple[str, ...] = ()
    ) -> TaskExecutionService:
        """The same service this suite builds, with gates granted."""
        return TaskExecutionService(
            self.database,
            clock=self.clock,
            token_factory=lambda: TOKEN,
            max_attempts=3,
            profile_registry=AgentProfileRegistry((
                general_profile(),
                _profile(
                    "repository-agent",
                    phases=("plan", "execute", "external_action"),
                ),
            )),
            execution_grants=list(kinds),
            action_grants=list(acting),
        )

    def _events(self, task_id: int) -> list[str]:
        with closing(sqlite3.connect(self.database)) as connection:
            return [
                row[0] for row in connection.execute(
                    "SELECT kind FROM task_execution_events WHERE task_id=? "
                    "ORDER BY sequence",
                    (task_id,),
                )
            ]

    def test_a_granted_kind_runs_its_plan_without_a_card(self):
        self._bind_origin(1, "issue")
        service = self._grant_service("issue")
        scheduled = service.schedule(1, expected_task_version=1)
        service.start_action(
            1, expected_version=scheduled.version, action="start")
        claim = service.claim_next()
        recorded = service.record_result(self._result(claim))

        self.assertEqual(recorded.status, WorkflowStatus.QUEUED)
        self.assertEqual(recorded.phase, WorkflowPhase.EXECUTE)
        workflow = service.get(1)
        self.assertEqual(workflow.status, WorkflowStatus.QUEUED)
        self.assertEqual(workflow.phase, WorkflowPhase.EXECUTE)

    def test_an_ungranted_kind_still_waits_for_a_reader(self):
        self._bind_origin(1, "issue")
        service = self._grant_service("review_request")
        scheduled = service.schedule(1, expected_task_version=1)
        service.start_action(
            1, expected_version=scheduled.version, action="start")
        claim = service.claim_next()
        recorded = service.record_result(self._result(claim))

        self.assertEqual(recorded.status, WorkflowStatus.AWAITING_REVIEW)
        self.assertEqual(recorded.phase, WorkflowPhase.PLAN)

    def test_a_task_with_no_origin_is_never_granted(self):
        """A task carrying no accepted candidate has no kind to match."""
        service = self._grant_service("issue")
        scheduled = service.schedule(1, expected_task_version=1)
        service.start_action(
            1, expected_version=scheduled.version, action="start")
        claim = service.claim_next()
        recorded = service.record_result(self._result(claim))

        self.assertEqual(recorded.status, WorkflowStatus.AWAITING_REVIEW)

    def test_a_grant_is_not_recorded_as_a_reader_approval(self):
        """The ledger must not claim someone approved what nobody saw."""
        self._bind_origin(1, "issue")
        service = self._grant_service("issue")
        scheduled = service.schedule(1, expected_task_version=1)
        service.start_action(
            1, expected_version=scheduled.version, action="start")
        claim = service.claim_next()
        service.record_result(self._result(claim))

        events = self._events(1)
        self.assertIn("phase_granted", events)
        self.assertNotIn("phase_approved", events)
        self.assertEqual(events[-2], "result_recorded")
        self.assertEqual(events[-1], "phase_granted")

    def test_new_execution_grant_reconciles_an_existing_plan_gate(self):
        """A policy edit advances an old plan card without human approval."""
        self._bind_origin(1, "issue")
        self._schedule_and_start()
        recorded = self.service.record_result(self._result(self._claim()))
        self.assertEqual(recorded.status, WorkflowStatus.AWAITING_REVIEW)

        reconciled = self._grant_service("issue").schedule_new()

        workflow = self.service.get(1)
        self.assertEqual(reconciled.scheduled, 1)
        self.assertEqual(workflow.status, WorkflowStatus.QUEUED)
        self.assertEqual(workflow.phase, WorkflowPhase.EXECUTE)
        self.assertEqual(workflow.version, recorded.version + 1)
        self.assertEqual(self._events(1)[-1], "phase_granted")
        self.assertNotIn("phase_approved", self._events(1))

    def test_reconciliation_retires_a_delivered_review_card_as_stale(self):
        """The card service owns retirement once a version fence advances."""
        self._bind_origin(1, "issue")
        self._schedule_and_start()
        self.service.record_result(self._result(self._claim()))
        cards = ExecutionCardService(self.database, clock=self.clock)
        self.assertEqual(cards.schedule().created, 1)

        self._grant_service("issue").schedule_new()
        retired = cards.schedule()

        self.assertEqual(retired.created, 0)
        self.assertEqual(retired.cancelled, 1)

    def test_reconciliation_is_idempotent(self):
        self._bind_origin(1, "issue")
        self._schedule_and_start()
        self.service.record_result(self._result(self._claim()))
        granted = self._grant_service("issue")

        first = granted.schedule_new()
        workflow = granted.get(1)
        second = granted.schedule_new()

        self.assertEqual(first.scheduled, 1)
        self.assertEqual(second.scheduled, 0)
        self.assertEqual(granted.get(1).version, workflow.version)
        self.assertEqual(self._events(1).count("phase_granted"), 1)

    def test_reconciliation_skips_completed_results_and_stale_workflows(self):
        self._bind_origin(1, "issue")
        self._schedule_and_start()
        recorded = self.service.record_result(self._result(
            self._claim(), outcome=ExecutionOutcome.COMPLETED
        ))
        completed = self._grant_service("issue").schedule_new()
        self.assertEqual(completed.scheduled, 0)
        self.assertEqual(self.service.get(1).version, recorded.version)

        with closing(sqlite3.connect(self.database)) as connection:
            connection.execute("UPDATE tasks SET version=2 WHERE id=1")
            connection.commit()
        stale = self._grant_service("issue").schedule_new()
        self.assertEqual(stale.scheduled, 0)
        self.assertEqual(self.service.get(1).status, WorkflowStatus.CANCELLED)

    def test_a_resurfaced_issue_task_gets_a_fresh_workflow(self):
        """End to end: the reopen is worthless if no work is ever scheduled.

        Reopening the task and re-scheduling it live in different modules, and
        each looked correct alone. Before the scheduler gate was relaxed a
        re-surfaced `issue` task reopened and then sat open forever, because
        its completed workflow still satisfied the join.
        """
        self._bind_origin(1, "issue")
        self._schedule_and_start()
        self.service.record_result(self._result(
            self._claim(), outcome=ExecutionOutcome.COMPLETED
        ))
        self.assertEqual(self._grant_service("issue").schedule_new().scheduled, 0)

        # The reader closes it and the source moves. A version advance alone
        # is NOT the trigger -- that also happens to a task nobody reopened,
        # and treating it as one broke stale reconciliation.
        with closing(sqlite3.connect(self.database)) as connection:
            connection.execute(
                "UPDATE task_execution_workflows SET status='completed',"
                "completed_at=? WHERE task_id=1",
                (self._now(),),
            )
            connection.execute(
                "UPDATE tasks SET status='open',version=version+1 WHERE id=1"
            )
            connection.commit()

        self.assertEqual(
            self._grant_service("issue").schedule_new().scheduled, 0,
            "a version advance without a reopen must not re-schedule",
        )

        # What the ledger actually writes when ADR 0039 re-surfaces a task.
        with closing(sqlite3.connect(self.database)) as connection:
            connection.execute(
                "INSERT INTO task_events(task_id,kind,task_version,"
                "candidate_id,source_revision,from_status,to_status,"
                "occurred_at) VALUES(1,'status_changed',"
                "(SELECT version FROM tasks WHERE id=1),NULL,NULL,"
                "'done','open',?)",
                (self._now(),),
            )
            connection.commit()

        resurfaced = self._grant_service("issue").schedule_new()

        self.assertEqual(resurfaced.scheduled, 1)
        self.assertEqual(
            self.service.get(1).status, WorkflowStatus.AWAITING_START
        )

    def test_granting_execution_does_not_grant_a_completed_result(self):
        """Only the plan-approval gate is granted; an ending still lands."""
        self._bind_origin(1, "issue")
        service = self._grant_service("issue")
        scheduled = service.schedule(1, expected_task_version=1)
        service.start_action(
            1, expected_version=scheduled.version, action="start")
        claim = service.claim_next()
        recorded = service.record_result(
            self._result(claim, outcome=ExecutionOutcome.COMPLETED)
        )

        self.assertEqual(recorded.status, WorkflowStatus.AWAITING_REVIEW)
        self.assertEqual(recorded.phase, WorkflowPhase.PLAN)
        self.assertNotIn("phase_granted", self._events(1))

    def test_an_unknown_granted_kind_is_refused_at_construction(self):
        with self.assertRaises(ValueError):
            self._grant_service("not-a-source-kind")

    def test_declaring_nothing_grants_nothing(self):
        self._bind_origin(1, "issue")
        scheduled = self.service.schedule(1, expected_task_version=1)
        self.service.start_action(
            1, expected_version=scheduled.version, action="start")
        claim = self._claim()
        recorded = self.service.record_result(self._result(claim))

        self.assertEqual(recorded.status, WorkflowStatus.AWAITING_REVIEW)

    def _reach_execute_phase(self, service):
        """Plan and approve, leaving the workflow queued to execute."""
        scheduled = service.schedule(1, expected_task_version=1)
        service.start_action(
            1, expected_version=scheduled.version, action="start")
        plan_claim = service.claim_next()
        recorded = service.record_result(self._result(plan_claim))
        if recorded.status is WorkflowStatus.AWAITING_REVIEW:
            recorded = service.review_action(
                1, expected_version=recorded.version, action="approve"
            )
        self.assertEqual(recorded.phase, WorkflowPhase.EXECUTE)
        return service.claim_next()

    def test_a_granted_kind_acts_without_a_second_card(self):
        self._bind_origin(1, "issue")
        service = self._grant_service("issue", acting=("issue",))
        claim = self._reach_execute_phase(service)
        recorded = service.record_result(self._result(
            claim,
            result_id="result-002",
            outcome=ExecutionOutcome.AWAITING_EXTERNAL,
        ))

        self.assertEqual(recorded.status, WorkflowStatus.QUEUED)
        self.assertEqual(recorded.phase, WorkflowPhase.EXTERNAL_ACTION)

    def test_granting_execution_does_not_grant_action(self):
        """The dangerous knob is not reached by turning the safe one."""
        self._bind_origin(1, "issue")
        service = self._grant_service("issue")
        claim = self._reach_execute_phase(service)
        recorded = service.record_result(self._result(
            claim,
            result_id="result-002",
            outcome=ExecutionOutcome.AWAITING_EXTERNAL,
        ))

        self.assertEqual(recorded.status, WorkflowStatus.AWAITING_REVIEW)
        self.assertEqual(recorded.phase, WorkflowPhase.EXECUTE)

    def test_granting_action_does_not_grant_execution(self):
        """And not the other way round either."""
        self._bind_origin(1, "issue")
        service = self._grant_service(acting=("issue",))
        scheduled = service.schedule(1, expected_task_version=1)
        service.start_action(
            1, expected_version=scheduled.version, action="start")
        claim = service.claim_next()
        recorded = service.record_result(self._result(claim))

        self.assertEqual(recorded.status, WorkflowStatus.AWAITING_REVIEW)
        self.assertEqual(recorded.phase, WorkflowPhase.PLAN)

    def test_an_ungranted_kind_still_waits_before_acting(self):
        self._bind_origin(1, "issue")
        service = self._grant_service("issue", acting=("review_request",))
        claim = self._reach_execute_phase(service)
        recorded = service.record_result(self._result(
            claim,
            result_id="result-002",
            outcome=ExecutionOutcome.AWAITING_EXTERNAL,
        ))

        self.assertEqual(recorded.status, WorkflowStatus.AWAITING_REVIEW)

    def test_a_granted_action_is_not_recorded_as_a_reader_approval(self):
        self._bind_origin(1, "issue")
        service = self._grant_service("issue", acting=("issue",))
        claim = self._reach_execute_phase(service)
        service.record_result(self._result(
            claim,
            result_id="result-002",
            outcome=ExecutionOutcome.AWAITING_EXTERNAL,
        ))

        events = self._events(1)
        self.assertNotIn("phase_approved", events)
        self.assertEqual(events.count("phase_granted"), 2)

    def test_new_action_grant_reconciles_an_existing_external_gate(self):
        self._bind_origin(1, "issue")
        service = self._grant_service("issue")
        claim = self._reach_execute_phase(service)
        recorded = service.record_result(self._result(
            claim,
            result_id="result-002",
            outcome=ExecutionOutcome.AWAITING_EXTERNAL,
        ))
        self.assertEqual(recorded.status, WorkflowStatus.AWAITING_REVIEW)
        approvals_before = self._events(1).count("phase_approved")

        reconciled = self._grant_service(
            "issue", acting=("issue",)
        ).schedule_new()

        workflow = service.get(1)
        self.assertEqual(reconciled.scheduled, 1)
        self.assertEqual(workflow.status, WorkflowStatus.QUEUED)
        self.assertEqual(workflow.phase, WorkflowPhase.EXTERNAL_ACTION)
        self.assertEqual(workflow.version, recorded.version + 1)
        self.assertEqual(self._events(1).count("phase_approved"), approvals_before)
        self.assertEqual(self._events(1)[-1], "phase_granted")

    def test_a_completed_action_still_reaches_the_reader(self):
        """Neither knob hides the end of the work."""
        self._bind_origin(1, "issue")
        service = self._grant_service("issue", acting=("issue",))
        claim = self._reach_execute_phase(service)
        service.record_result(self._result(
            claim,
            result_id="result-002",
            outcome=ExecutionOutcome.AWAITING_EXTERNAL,
        ))
        action_claim = service.claim_next()
        recorded = service.record_result(self._result(
            action_claim,
            result_id="result-003",
            outcome=ExecutionOutcome.COMPLETED,
        ))

        self.assertEqual(recorded.status, WorkflowStatus.AWAITING_REVIEW)
        self.assertEqual(recorded.phase, WorkflowPhase.EXTERNAL_ACTION)

    def test_an_unknown_action_kind_is_refused_at_construction(self):
        with self.assertRaises(ValueError):
            self._grant_service(acting=("not-a-source-kind",))

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

    def _revise_and_reclaim(self, version):
        self.service.review_action(1, expected_version=version, action="revise")
        return self._claim()

    def test_unchanged_answer_is_refused_even_when_questions_change(self):
        """A changing question list cannot make stale work a new answer."""
        self._schedule_and_start()
        first_claim = self._claim()
        first = self.service.record_result(self._result(first_claim))
        second_claim = self._revise_and_reclaim(first.version)

        repeated = replace(
            self._result(second_claim, result_id="result-002"),
            questions=("A different synthetic question?",),
        )
        refused = self.service.record_result(repeated)

        self.assertEqual(refused.disposition, WorkflowDisposition.REFUSED)
        self.assertEqual(refused.refusal, WorkflowRefusal.RESULT_UNCHANGED)
        self.assertEqual(self.service.result_count(), 1)
        self.assertEqual(self.service.get(1).status, WorkflowStatus.RUNNING)
        self.assertEqual(self.service.get(1).version,
                         second_claim.workflow_version)
        self.assertEqual(self._events(1)[-1], "result_unchanged")

        changed = replace(repeated, summary="Revised synthetic summary")
        recorded = self.service.record_result(changed)
        self.assertEqual(recorded.disposition, WorkflowDisposition.APPLIED)
        self.assertEqual(self.service.result_count(), 2)

    def test_an_answer_repeated_from_two_passes_ago_is_refused(self):
        """Comparing only the newest result let an X, Y, X sequence through."""
        self._schedule_and_start()
        claim = self._claim()
        first = self.service.record_result(self._result(claim))

        claim = self._revise_and_reclaim(first.version)
        second = self.service.record_result(replace(
            self._result(claim, result_id="result-002"),
            summary="A different synthetic summary",
        ))
        self.assertEqual(second.disposition, WorkflowDisposition.APPLIED)

        claim = self._revise_and_reclaim(second.version)
        refused = self.service.record_result(
            self._result(claim, result_id="result-003"))

        self.assertEqual(refused.refusal, WorkflowRefusal.RESULT_UNCHANGED)
        self.assertEqual(self.service.result_count(), 2)

    def test_attaching_a_missing_deliverable_is_a_changed_answer(self):
        """The guard must not teach an agent to pad its prose."""
        self._schedule_and_start()
        claim = self._claim()
        first = self.service.record_result(self._result(claim))
        claim = self._revise_and_reclaim(first.version)

        corrected = replace(
            self._result(claim, result_id="result-002"),
            deliverables=("Synthetic deliverable", "The one it forgot"),
        )
        recorded = self.service.record_result(corrected)

        self.assertEqual(recorded.disposition, WorkflowDisposition.APPLIED)
        self.assertEqual(self.service.result_count(), 2)

    def test_the_same_prose_under_a_new_outcome_is_a_changed_answer(self):
        """Proposing something and declaring it done are different answers."""
        self._schedule_and_start()
        claim = self._claim()
        first = self.service.record_result(self._result(claim))
        claim = self._revise_and_reclaim(first.version)

        terminal = self._result(
            claim, result_id="result-002",
            outcome=ExecutionOutcome.INELIGIBLE,
        )
        recorded = self.service.record_result(terminal)

        self.assertEqual(recorded.disposition, WorkflowDisposition.APPLIED)

    def test_a_rescheduled_task_may_open_with_its_previous_answer(self):
        """A new cycle must never be refused for matching the closed one.

        Nothing recovers from this: the identical result is all the agent has,
        every retry is refused the same way, and the claim expires into a
        parked workflow.
        """
        self._schedule_and_start()
        claim = self._claim()
        first = self.service.record_result(self._result(claim))
        cancelled = self.service.review_action(
            1, expected_version=first.version, action="cancel")
        self.assertEqual(cancelled.status, WorkflowStatus.CANCELLED)

        rescheduled = self.service.schedule(1, expected_task_version=1)
        self.service.start_action(
            1, expected_version=rescheduled.version, action="start")
        claim = self._claim()

        repeated = self._result(claim, result_id="result-002")
        recorded = self.service.record_result(repeated)

        self.assertEqual(recorded.disposition, WorkflowDisposition.APPLIED)
        self.assertEqual(self.service.result_count(), 2)

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

    def test_context_exhaustion_parks_immediately_and_is_countable(self):
        self._schedule_and_start()
        claim = self._claim()

        parked = self.service.fail(
            1,
            expected_version=claim.workflow_version,
            claim_token=claim.token,
            reason="context_exhausted",
        )

        self.assertEqual(parked.status, WorkflowStatus.PARKED)
        self.assertIsNone(parked.next_attempt_at)
        health = self.service.readiness()
        self.assertEqual((health.parked, health.context_exhausted), (1, 1))
        self.assertIsNone(self.service.claim_next())

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
        self.assertEqual(
            sum(value for name, value in health.__dict__.items()
                if name != "context_exhausted"),
            1,
        )
        self.assertNotIn("Synthetic", repr(health))
        self.assertNotIn(TOKEN, repr(health))


if __name__ == "__main__":
    unittest.main()
