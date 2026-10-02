#!/usr/bin/env python3
"""Synthetic tests for research-before-planning gating and planning context."""

from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock

from foxhound import migrate_database
from foxhound.agent_profiles import general_profile
from foxhound.deployment_config import (
    DEPLOYMENT_SCHEMA_VERSION,
    DeploymentConfigError,
    _parse_workflow,
)
from foxhound.execution_worker import (
    INSTRUCTIONS_NAME,
    ExecutionWorker,
)
from foxhound.knowledge_client import KnowledgeClientConfig
from foxhound.task_execution import (
    TaskExecutionService,
    WorkflowPhase,
    WorkflowStatus,
)
from foxhound.task_research import (
    DRAFT_SCHEMA,
    INPUT_SCHEMA,
    ResearchStore,
)


NOW = datetime(2030, 1, 5, 12, 0, tzinfo=timezone.utc)
READER = "Person A"
OTHER = "Person B"
RUN_ID = "a" * 32
WORKER_COMMAND = "foxhound-task-worker"
CLAIM_TOKEN = "synthetic-claim-token-000000000000000000000000"


def _snapshot(task_id: int = 1, version: int = 1, text: str = "Synthetic task 1"):
    return {
        "schema_version": INPUT_SCHEMA,
        "task_id": task_id,
        "task_version": version,
        "text": text,
        "structured": {"action": "prepare", "object": "brief", "confidence": 0.9},
        "due": "2030-01-10",
        "owner": {"person_id": "person-a", "reliability": "resolved"},
        "participants": [],
        "working_group": {"id": "group-alpha", "evidence": "explicit"},
        "external_identifiers": [{"kind": "issue", "value": "example-12"}],
        "origin": {"kind": "meeting", "source_digest": "1" * 64},
        "structured_schema_revisions": {"task": 12, "identity": 1},
    }


def _draft():
    empty = []
    return {
        "schema_version": DRAFT_SCHEMA,
        "research_status": "sufficient",
        "objective": {"text": "Produce the synthetic brief.", "status": "supported", "source_refs": ["src-001"]},
        "requested_action": {"text": "Draft the brief.", "status": "supported", "source_refs": ["src-001"]},
        "current_state": [],
        "expected_deliverables": [],
        "timeline": empty,
        "decisions": empty,
        "dependencies": [],
        "constraints": empty,
        "stakeholders": empty,
        "related_entities": empty,
        "findings": [{"text": "Synthetic finding.", "status": "supported", "source_refs": ["src-001"]}],
        "conflicts": empty,
        "open_questions": [],
        "scheduling_recommendations": [],
    }


def _sources():
    return [{
        "source_id": "src-001",
        "locator": {
            "namespace": "kb",
            "resource": "Projects/Project-Alpha.md",
            "fragment": "plan",
        },
        "content_digest": "2" * 64,
        "title": "Project Alpha plan",
    }]


class DeploymentConfigResearchGateTests(unittest.TestCase):
    """Deployment config parses/validates/renders research_before_planning."""

    def _document(self, **extra: object) -> dict[str, object]:
        return {
            "default_agent_profile": "general",
            "plan_without_asking": ["issue", "meeting"],
            "execution_slot_cap": 1,
            "plan_ready_cap": 1,
            "awaiting_reader_cap": 1,
            "execute_without_asking": [],
            "act_without_asking": [],
            "reader_aliases": [],
            "skip_planning_for": [],
            "agent_profile_routes": [],
        } | extra

    def test_parses_valid_research_before_planning(self):
        workflow = _parse_workflow(
            self._document(research_before_planning=["meeting", "issue"]),
            version=DEPLOYMENT_SCHEMA_VERSION,
        )
        self.assertEqual(workflow.research_before_planning, ("meeting", "issue"))

    def test_rejects_unknown_kinds_or_duplicates(self):
        with self.assertRaises(DeploymentConfigError):
            _parse_workflow(
                self._document(research_before_planning=["meeting", "meeting"]),
                version=DEPLOYMENT_SCHEMA_VERSION,
            )
        # Unknown kinds are refused when the runtime config is validated,
        # by the same check that guards the other per-kind declarations.
        from foxhound.source_policy import source_kind_grants
        with self.assertRaisesRegex(ValueError, "unknown source kinds"):
            source_kind_grants(
                ("unknown_kind_xyz",),
                label="research-before-planning declarations",
            )

    def test_parses_valid_research_wait_seconds(self):
        workflow = _parse_workflow(
            self._document(research_wait_seconds=1800),
            version=DEPLOYMENT_SCHEMA_VERSION,
        )
        self.assertEqual(workflow.research_wait_seconds, 1800)

    def test_rejects_invalid_research_wait_seconds(self):
        for invalid in [599, 86401, "3600", True, -1]:
            with self.assertRaises(DeploymentConfigError):
                _parse_workflow(
                    self._document(research_wait_seconds=invalid),
                    version=DEPLOYMENT_SCHEMA_VERSION,
                )

    def test_renders_schedule_argv(self):
        workflow = _parse_workflow(
            self._document(
                research_before_planning=["meeting"],
                research_wait_seconds=1800,
            ),
            version=DEPLOYMENT_SCHEMA_VERSION,
        )
        argv = workflow.schedule_argv(
            Path("/srv/example/db.sqlite3"),
            None,
            task_work_root=Path("/srv/example/work"),
            task_kb_root=Path("/srv/example/kb"),
        )
        self.assertIn("--research-before-planning", argv)
        index = argv.index("--research-before-planning")
        self.assertEqual(argv[index + 1], "meeting")
        self.assertIn("--research-wait-seconds", argv)
        index_wait = argv.index("--research-wait-seconds")
        self.assertEqual(argv[index_wait + 1], "1800")
        self.assertIn("--task-work-root", argv)
        index_work = argv.index("--task-work-root")
        self.assertEqual(argv[index_work + 1], "/srv/example/work")
        self.assertIn("--task-kb-root", argv)
        index_kb = argv.index("--task-kb-root")
        self.assertEqual(argv[index_kb + 1], "/srv/example/kb")

    def test_absent_setting_leaves_rendered_argv_identical(self):
        workflow = _parse_workflow(
            self._document(),
            version=DEPLOYMENT_SCHEMA_VERSION,
        )
        argv_no_roots = workflow.schedule_argv(Path("/srv/example/db.sqlite3"), None)
        argv_with_roots = workflow.schedule_argv(
            Path("/srv/example/db.sqlite3"),
            None,
            task_work_root=Path("/srv/example/work"),
            task_kb_root=Path("/srv/example/kb"),
        )
        self.assertEqual(argv_no_roots, argv_with_roots)
        self.assertNotIn("--research-before-planning", argv_no_roots)
        self.assertNotIn("--research-wait-seconds", argv_no_roots)
        self.assertNotIn("--task-work-root", argv_no_roots)
        self.assertNotIn("--task-kb-root", argv_no_roots)

    def test_runner_argv_with_kinds_and_wait(self):
        from foxhound.deployment_config import ExecutionRunnerDeploymentConfig
        runner = ExecutionRunnerDeploymentConfig(
            enabled=True,
            run_root=Path("/srv/example/runs"),
            gw_endpoint="http://127.0.0.1:8787",
            gw_alias="example-operator",
            gw_token_file=Path("/srv/example/token"),
            agent_command="hermes",
            worker_command="foxhound-task-worker",
            runner_slot="primary",
            task_work_root=Path("/srv/example/work"),
            task_kb_root=Path("/srv/example/kb"),
        )
        workflow = _parse_workflow(
            self._document(
                research_before_planning=["meeting"],
                research_wait_seconds=1800,
            ),
            version=DEPLOYMENT_SCHEMA_VERSION,
        )
        argv = runner.argv(Path("/srv/example/db.sqlite3"), None, workflow)
        self.assertIn("--research-before-planning", argv)
        idx_kind = argv.index("--research-before-planning")
        self.assertEqual(argv[idx_kind + 1], "meeting")
        self.assertIn("--research-wait-seconds", argv)
        idx_wait = argv.index("--research-wait-seconds")
        self.assertEqual(argv[idx_wait + 1], "1800")
        self.assertIn("--task-work-root", argv)
        self.assertEqual(argv[argv.index("--task-work-root") + 1], "/srv/example/work")
        self.assertIn("--task-kb-root", argv)
        self.assertEqual(argv[argv.index("--task-kb-root") + 1], "/srv/example/kb")

    def test_runner_argv_absent_setting_identical(self):
        from foxhound.deployment_config import ExecutionRunnerDeploymentConfig
        runner = ExecutionRunnerDeploymentConfig(
            enabled=True,
            run_root=Path("/srv/example/runs"),
            gw_endpoint="http://127.0.0.1:8787",
            gw_alias="example-operator",
            gw_token_file=Path("/srv/example/token"),
            agent_command="hermes",
            worker_command="foxhound-task-worker",
            runner_slot="primary",
        )
        workflow = _parse_workflow(
            self._document(),
            version=DEPLOYMENT_SCHEMA_VERSION,
        )
        argv = runner.argv(Path("/srv/example/db.sqlite3"), None, workflow)
        self.assertNotIn("--research-before-planning", argv)
        self.assertNotIn("--research-wait-seconds", argv)


class ResearchGatePlanningTests(unittest.TestCase):
    """Lifecycle and admission tests for tasks gated by research_before_planning."""

    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.root.chmod(0o700)
        self.database = self.root / "foxhound.sqlite3"
        self.cas = self.root / "research-cas"
        self.task_work_root = self.root / "tasks"
        self.task_kb_root = self.root / "kb"
        self.cas.mkdir(mode=0o700)
        self.task_work_root.mkdir(mode=0o700)
        self.task_kb_root.mkdir(mode=0o700)
        migrate_database(self.database)
        self.store = ResearchStore(self.database, self.cas, clock=lambda: NOW)

    def _now(self) -> str:
        return NOW.isoformat(timespec="seconds")

    def _task(
        self,
        task_id: int,
        owner: str = READER,
        origin_kind: str = "meeting",
        **owner_columns: object,
    ) -> None:
        columns = {
            "owner_kind": "person",
            "owner_ref_version": 1,
            "owner_provisional": 0,
        } | owner_columns
        with closing(sqlite3.connect(self.database)) as connection:
            connection.execute(
                "INSERT INTO tasks(id,status,text,owner,due,version,"
                "created_at,updated_at,closed_at,owner_kind,"
                "owner_ref_version,owner_provisional) "
                "VALUES(?,'open',?,?,NULL,1,?,?,NULL,?,?,?)",
                (
                    task_id, f"Synthetic task {task_id}", owner,
                    self._now(), self._now(), columns["owner_kind"],
                    columns["owner_ref_version"],
                    columns["owner_provisional"],
                ),
            )
            connection.execute(
                "INSERT INTO task_events(task_id,kind,task_version,"
                "candidate_id,source_revision,from_status,to_status,"
                "occurred_at) VALUES(?,'created',1,NULL,NULL,NULL,'open',?)",
                (task_id, self._now()),
            )
            connection.execute(
                "INSERT INTO candidate_inbox(candidate_id,source_system,"
                "source_kind,source_record_id,source_item_id,"
                "source_revision,payload_json,created_at,"
                "first_imported_at,updated_at) "
                "VALUES(?,'gw',?,'record-synthetic',?,?,'{}',?,?,?)",
                (
                    f"origin-{task_id}", origin_kind, str(task_id), "b" * 64,
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

    def _service(
        self,
        *,
        research_before_planning: object = ("meeting",),
        planning_grants: object = ("meeting", "issue"),
        reader_aliases: object = [READER],
        ask_when_owned_by_others: object = (),
    ) -> TaskExecutionService:
        return TaskExecutionService(
            self.database,
            planning_grants=planning_grants,
            reader_aliases=reader_aliases,
            ask_when_owned_by_others=ask_when_owned_by_others,
            research_before_planning=research_before_planning,
            research_task_roots=(self.task_work_root, self.task_kb_root),
            token_factory=lambda: CLAIM_TOKEN,
            clock=lambda: NOW,
        )

    def _publish_research(self, task_id: int = 1, text: str = "Synthetic task 1") -> Path:
        snap = _snapshot(task_id=task_id, version=1, text=text)
        task_folder = self.task_work_root / f"T{task_id}-folder"
        task_folder.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.store.request(
            snap,
            task_work_root=self.task_work_root,
            task_folder=task_folder,
        )
        research_claim = self.store.claim("worker-synthetic")
        assert research_claim is not None
        self.store.publish(
            job_id=research_claim.job.job_id,
            token=research_claim.token,
            draft=_draft(),
            sources=_sources(),
            provenance={
                "profile_id": "researcher", "profile_revision": "3" * 64,
                "model": "synthetic-thinking-model", "provider": "synthetic",
                "runtime": "manual-test",
                "reasoning_requested": "high", "reasoning_effective": "high",
            },
            coverage={
                "searched_namespaces": ["kb"], "queries": 1,
                "documents_retrieved": 1, "unavailable_source_ids": [],
                "knowledge_revisions": {"kb": "4" * 64},
            },
        )
        return task_folder

    def test_listed_kind_unclaimable_until_receipt_then_claimable(self):
        self._task(1, READER, origin_kind="meeting")
        service = self._service(research_before_planning=["meeting"])
        service.schedule_new(limit=10)

        # Claim attempt before research receipt: unclaimable because research is requested/held
        claim = service.claim_next()
        self.assertIsNone(claim)

        # Publish completed research receipt
        self._publish_research(task_id=1, text="Synthetic task 1")

        # Now claimable
        claim = service.claim_next()
        self.assertIsNotNone(claim)
        assert claim is not None
        self.assertEqual(claim.task_id, 1)
        self.assertEqual(claim.phase, WorkflowPhase.PLAN)

    def test_unlisted_kind_is_claimable_immediately(self):
        self._task(1, READER, origin_kind="issue")
        service = self._service(research_before_planning=["meeting"])
        service.schedule_new(limit=10)

        claim = service.claim_next()
        self.assertIsNotNone(claim)
        assert claim is not None
        self.assertEqual(claim.task_id, 1)

    def test_other_owned_task_waiting_at_start_does_not_request_research(self):
        self._task(1, OTHER, origin_kind="meeting")
        service = self._service(
            planning_grants=["issue"],
            research_before_planning=["meeting"],
            ask_when_owned_by_others=["meeting"],
        )
        service.schedule_new(limit=10)

        workflow = service.get(1)
        self.assertIsNotNone(workflow)
        assert workflow is not None
        self.assertEqual(workflow.status, WorkflowStatus.AWAITING_START)

        with closing(sqlite3.connect(self.database)) as connection:
            count = connection.execute(
                "SELECT COUNT(*) FROM task_research_jobs WHERE task_id=1"
            ).fetchone()[0]
        self.assertEqual(count, 0)

    def test_plan_context_includes_research_after_publication_and_not_before(self):
        self._task(1, READER, origin_kind="issue")
        service = self._service(research_before_planning=[])
        service.schedule_new(limit=10)
        claim = service.claim_next()
        self.assertIsNotNone(claim)
        assert claim is not None

        run_dir = self.root / f"run-{RUN_ID}"
        run_dir.mkdir(mode=0o700)
        state_path = run_dir / "run-state.json"
        task_folder = self.task_work_root / "T1-folder"
        task_folder.mkdir(parents=True, exist_ok=True, mode=0o700)
        task_kb_file = task_folder / "Task.md"
        task_kb_file.write_text("# Synthetic task\n", encoding="utf-8")

        run_state = {
            "schema": "foxhound.execution-run-state",
            "schema_version": 7,
            "run_id": RUN_ID,
            "database_path": str(self.database),
            "task_id": 1,
            "task_version": 1,
            "workflow_version": claim.workflow_version,
            "phase": WorkflowPhase.PLAN.value,
            "claim_token": CLAIM_TOKEN,
            "lease_seconds": claim.lease_seconds,
            "agent_profile_id": claim.agent_profile_id,
            "agent_profile_revision": claim.agent_profile_revision,
            "knowledge_root": str(self.task_kb_root),
            "worker_command": WORKER_COMMAND,
            "task_work_directory": str(task_folder),
            "task_kb_file": str(task_kb_file),
            "task_run_directory": str(run_dir),
            "execution_grants": [],
            "action_grants": [],
            "deployment_roots": {},
            "pass_budget_seconds": 2700,
            "pass_deadline": "2030-01-05T13:00:00+00:00",
        }
        state_path.write_text(json.dumps(run_state), encoding="utf-8")
        state_path.chmod(0o600)

        instructions_path = run_dir / INSTRUCTIONS_NAME
        instructions_path.write_text(
            json.dumps(general_profile().document()), encoding="utf-8"
        )
        instructions_path.chmod(0o600)

        worker = ExecutionWorker(
            state_path,
            KnowledgeClientConfig(
                endpoint="http://127.0.0.1:8000",
                alias="primary",
                token="01234567890123456789012345678901",
            ),
        )
        with (
            mock.patch(
                "foxhound.execution_worker._local_today",
                return_value="2030-01-05",
            ),
            mock.patch(
                "foxhound.execution_worker.GwKnowledgeClient.execution_context",
                return_value=mock.MagicMock(
                    revision="1" * 64,
                    display_name="Synthetic Operator",
                    operator_context="",
                    self_aliases=[],
                    institution_domains=[],
                ),
            ),
            mock.patch(
                "foxhound.execution_worker.GwKnowledgeClient.working_groups",
                return_value={},
            ),
        ):
            # Before publication: "research" is not in context
            ctx_before = worker.context()
            self.assertNotIn("research", ctx_before)

            # Publish research
            self._publish_research(task_id=1, text="Synthetic task 1")

            # After publication: "research" is in context
            ctx_after = worker.context()
            self.assertIn("research", ctx_after)
            self.assertIn("# Task Research", ctx_after["research"])
            self.assertLessEqual(len(ctx_after["research"]), 20000)

    def test_execution_runner_built_service_gating_behavior(self):
        from foxhound.execution_runner import ExecutionRunnerConfig
        token_file = self.root / "gateway.token"
        token_file.write_text("synthetic-token", encoding="utf-8")
        token_file.chmod(0o600)
        config = ExecutionRunnerConfig(
            database_path=self.database,
            run_root=self.root / "runs",
            gw_endpoint="http://127.0.0.1:8787",
            gw_alias="example-operator",
            gw_token_file=token_file,
            agent_command="hermes",
            worker_command="foxhound-task-worker",
            runner_slot="primary",
            task_work_root=self.task_work_root,
            task_kb_root=self.task_kb_root,
            planning_grants=("meeting",),
            research_before_planning=("meeting",),
            research_wait_seconds=1800,
        )
        research_task_roots = (
            (config.task_work_root, config.task_kb_root)
            if config.task_work_root is not None and config.task_kb_root is not None
            else None
        )
        service = TaskExecutionService(
            config.database_path,
            profile_registry=config.profile_registry,
            default_profile_id=config.default_agent_profile,
            planning_grants=config.planning_grants,
            execution_grants=config.execution_grants,
            action_grants=config.action_grants,
            profile_routes=config.profile_routes,
            execution_slot_cap=config.execution_slot_cap,
            plan_ready_cap=config.plan_ready_cap,
            awaiting_reader_cap=config.awaiting_reader_cap,
            reader_aliases=[READER],
            research_before_planning=config.research_before_planning,
            research_wait_seconds=config.research_wait_seconds,
            research_task_roots=research_task_roots,
            clock=lambda: NOW,
        )
        self._task(1, READER, origin_kind="meeting")
        service.schedule_new(limit=10)

        # Before research receipt: not claimed
        self.assertIsNone(service.claim_next())

        # Publish research receipt
        self._publish_research(task_id=1, text="Synthetic task 1")

        # After receipt: claimed
        claim = service.claim_next()
        self.assertIsNotNone(claim)
        assert claim is not None
        self.assertEqual(claim.task_id, 1)

    def test_schedule_main_with_gate_set(self):
        from foxhound.execution_schedule import main
        self._task(1, READER, origin_kind="meeting")
        exit_code = main([
            "--database", str(self.database),
            "--plan-without-asking", "meeting",
            "--reader-alias", READER,
            "--research-before-planning", "meeting",
            "--research-wait-seconds", "1800",
            "--task-work-root", str(self.task_work_root),
            "--task-kb-root", str(self.task_kb_root),
        ])
        self.assertEqual(exit_code, 0)
        # Without roots, schedule main exits 78 (config unavailable)
        exit_code_no_roots = main([
            "--database", str(self.database),
            "--plan-without-asking", "meeting",
            "--research-before-planning", "meeting",
        ])
        self.assertEqual(exit_code_no_roots, 78)

    def test_validate_runtime_rejects_research_kinds_without_roots(self):
        from foxhound.deployment_config import (
            DeploymentConfig,
            CardServiceConfig,
            ExecutionRunnerDeploymentConfig,
            _validate_runtime,
        )
        token_file = self.root / "gateway.token"
        token_file.write_text("synthetic-token", encoding="utf-8")
        token_file.chmod(0o600)
        card_service = CardServiceConfig(
            enabled=False,
        )
        workflow = _parse_workflow(
            {
                "default_agent_profile": "general",
                "plan_without_asking": ["meeting"],
                "execution_slot_cap": 1,
                "plan_ready_cap": 1,
                "awaiting_reader_cap": 1,
                "execute_without_asking": [],
                "act_without_asking": [],
                "reader_aliases": [],
                "skip_planning_for": [],
                "agent_profile_routes": [],
                "research_before_planning": ["meeting"],
            },
            version=DEPLOYMENT_SCHEMA_VERSION,
        )
        # Runner without task_work_root and task_kb_root
        runner_no_roots = ExecutionRunnerDeploymentConfig(
            enabled=True,
            run_root=self.root / "runs",
            gw_endpoint="http://127.0.0.1:8787",
            gw_alias="example-operator",
            gw_token_file=token_file,
            agent_command="hermes",
            worker_command="foxhound-task-worker",
            runner_slot="primary",
        )
        dep_config = DeploymentConfig(
            database=self.database,
            agent_profile_directory=None,
            card_service=card_service,
            workflow=workflow,
            execution_runners=(runner_no_roots,),
        )
        with self.assertRaises(DeploymentConfigError) as ctx:
            _validate_runtime(dep_config)
        self.assertIn("research-before-planning requires an enabled runner with both task archive roots configured", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
