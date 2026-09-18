#!/usr/bin/env python3
"""Synthetic tests for supervised Foxhound execution runs."""

from __future__ import annotations

from foxhound import migrate_database

import json
import sqlite3
import stat
import subprocess
import tempfile
import unittest
from contextlib import closing, redirect_stderr, redirect_stdout
from io import StringIO
from pathlib import Path
from unittest import mock

from foxhound.agent_profiles import (
    AgentProfileRegistry,
    WORKER_COMMAND_TOKEN,
    general_profile,
    load_registry,
    parse_profile,
)
from foxhound.candidate_inbox import CandidateInbox
from foxhound.execution_runner import (
    ExecutionRunnerConfig,
    ExecutionRunnerError,
    ExecutionRunResult,
    _exclusive_lock,
    _runner_lock_path,
    agent_prompt,
    hermes_argv,
    main,
    profile_argv,
    run_once,
)
from foxhound.execution_worker import INSTRUCTIONS_NAME, load_run_state
from foxhound.worker_resolution import WorkerMismatch
from foxhound.task_execution import (
    ExecutionOutcome,
    ExecutionResultEnvelope,
    TaskExecutionService,
    WorkflowPhase,
    WorkflowStatus,
)
from foxhound.task_ledger import TaskLedger, TaskLedgerError


RESULT_ID = "c" * 32


class MutableMonotonic:
    def __init__(self) -> None:
        self.value = 0.0

    def __call__(self) -> float:
        return self.value

    def sleep(self, seconds: float) -> None:
        self.value += seconds


class FakeProcess:
    def __init__(self, callback=None, exit_code=None) -> None:
        self.pid = 4242
        self.callback = callback
        self.exit_code = exit_code
        self.triggered = False
        self.terminated = False

    def poll(self):
        if self.terminated:
            return -15
        if self.callback is not None and not self.triggered:
            self.triggered = True
            self.callback()
            return None
        return self.exit_code


class ExecutionRunnerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.root.chmod(0o700)
        self.database = self.root / "foxhound.sqlite3"
        migrate_database(self.database)
        with closing(sqlite3.connect(self.database)) as connection:
            now = "2030-01-02T03:04:05+00:00"
            connection.execute(
                "INSERT INTO tasks(id,status,text,owner,due,version,created_at,"
                "updated_at,closed_at) VALUES(1,'open','Synthetic task',"
                "'Person A',NULL,1,?,?,NULL)",
                (now, now),
            )
            connection.commit()
        self.run_root = self.root / "runs"
        self.run_root.mkdir(mode=0o700)
        self.token_file = self.root / "knowledge.token"
        self.token_file.write_text(
            "synthetic-knowledge-token-with-sufficient-length\n",
            encoding="utf-8",
        )
        self.token_file.chmod(0o600)
        self.service = TaskExecutionService(self.database)

    def _ready(self) -> None:
        scheduled = self.service.schedule(1, expected_task_version=1)
        started = self.service.start_action(
            1, expected_version=scheduled.version, action="start"
        )
        self.assertEqual(started.status, WorkflowStatus.QUEUED)

    def _config(self, **changes) -> ExecutionRunnerConfig:
        values = {
            "database_path": self.database,
            "run_root": self.run_root,
            "gw_endpoint": "http://127.0.0.1:8787",
            "gw_alias": "primary",
            "gw_token_file": self.token_file,
            "agent_command": "synthetic-agent run",
            "poll_seconds": 1,
        }
        values.update(changes)
        return ExecutionRunnerConfig(**values)

    def _bind_origin(self, kind: str) -> None:
        with closing(sqlite3.connect(self.database)) as connection:
            connection.execute(
                "INSERT INTO candidate_inbox(candidate_id,source_system,"
                "source_kind,source_record_id,source_item_id,source_revision,"
                "payload_json,created_at,first_imported_at,updated_at) "
                "VALUES('candidate-1','gw',?,'record-1','item-1',?,'{}',"
                "'2030-01-02T03:04:05+00:00','2030-01-02T03:04:05+00:00',"
                "'2030-01-02T03:04:05+00:00')",
                (kind, "a" * 64),
            )
            connection.execute(
                "INSERT INTO task_candidate_bindings(candidate_id,"
                "source_revision,task_id,relation,decided_at) "
                "VALUES('candidate-1',?,1,'accepted',?)",
                ("a" * 64, "2030-01-02T03:04:05+00:00"),
            )
            connection.commit()

    @staticmethod
    def _terminator(process, _grace, **_kwargs) -> bool:
        process.terminated = True
        return False

    def test_a_failed_run_leaves_something_that_explains_it(self):
        """Two runs of one task each produced a complete result file, each
        failed to record it, and neither could be explained: the agent's
        output went to /dev/null, so the refusal it was given, the command
        it tried and the budget it spent were all gone. The transcript is
        owner-only and sits beside the result, which already holds the same
        private content.
        """
        self._ready()
        launched = {}

        def popen(argv, **kwargs):
            launched["kwargs"] = kwargs
            handle = kwargs["stdout"]
            handle.write(b"synthetic agent output\n")
            handle.flush()
            launched["path"] = Path(handle.name)
            # Exits without recording: the case that used to be unexplainable.
            return FakeProcess(exit_code=1)

        run_once(
            self._config(),
            base_environment={"PATH": "/usr/bin"},
            popen=popen,
            run_id_factory=lambda: "b" * 32,
            terminate=self._terminator,
        )
        path = launched["path"]
        self.assertEqual(path.name, "agent-output.log")
        self.assertEqual(path.read_bytes(), b"synthetic agent output\n")
        self.assertEqual(path.stat().st_mode & 0o777, 0o600)
        # Beside the run's own state, never outside the run directory.
        self.assertTrue((path.parent / "run-state.json").exists())

    def test_failed_run_is_copied_to_configured_task_locations(self):
        self._ready()
        work_root = self.root / "Project Alpha" / "Tasks"
        kb_root = self.root / "Project Alpha KB" / "Tasks"

        def popen(_argv, **kwargs):
            handle = kwargs["stdout"]
            handle.write(b"synthetic failed run\n")
            handle.flush()
            return FakeProcess(exit_code=1)

        result = run_once(
            self._config(task_work_root=work_root, task_kb_root=kb_root),
            popen=popen,
            run_id_factory=lambda: "d" * 32,
            terminate=self._terminator,
        )

        self.assertEqual(result.outcome, "process_exit")
        task_directory = work_root / "T1-synthetic-task"
        evidence = task_directory / "runs" / ("plan-" + "d" * 32)
        self.assertEqual(
            (evidence / "agent-output.log").read_text(encoding="utf-8"),
            "synthetic failed run\n",
        )
        self.assertFalse((evidence / "run-state.json").exists())
        self.assertFalse((evidence / INSTRUCTIONS_NAME).exists())
        self.assertTrue((task_directory / "README.md").is_file())
        self.assertTrue((kb_root / "T1-synthetic-task.md").is_file())

    def test_runner_records_and_scrubs_capability_without_shell_or_output(self):
        self._ready()
        launched = {}

        def popen(argv, **kwargs):
            launched.update(argv=argv, kwargs=kwargs)
            state_path = Path(kwargs["env"]["FOXHOUND_EXECUTION_STATE"])
            state_document = json.loads(state_path.read_text(encoding="utf-8"))
            self.assertEqual(state_document["execution_grants"], [])
            self.assertEqual(state_document["action_grants"], [])

            def record():
                state = load_run_state(state_path)
                launched["state"] = state
                result = TaskExecutionService(state.database_path).record_result(
                    ExecutionResultEnvelope(
                        result_id=RESULT_ID,
                        task_id=state.task_id,
                        task_version=state.task_version,
                        workflow_version=state.workflow_version,
                        phase=state.phase,
                        claim_token=state.claim_token,
                        outcome=ExecutionOutcome.AWAITING_PLAN,
                        summary="Synthetic result",
                        work_markdown="Synthetic plan",
                    )
                )
                self.assertTrue(result.accepted)

            return FakeProcess(callback=record)

        result = run_once(
            self._config(),
            base_environment={"PATH": "/usr/bin"},
            popen=popen,
            run_id_factory=lambda: "a" * 32,
            terminate=self._terminator,
        )

        self.assertEqual((result.outcome, result.exit_code), ("recorded", 0))
        self.assertFalse(launched["kwargs"]["shell"])
        # Kept, not discarded: a supervised run that fails must leave
        # something behind that explains it. Owner-only, beside the result.
        self.assertIsNot(launched["kwargs"]["stdout"], subprocess.DEVNULL)
        self.assertIs(launched["kwargs"]["stderr"], subprocess.STDOUT)
        self.assertIs(launched["kwargs"]["stdin"], subprocess.DEVNULL)
        turn_index = launched["argv"].index("--max-turns")
        self.assertEqual(
            launched["argv"][turn_index + 1],
            str(general_profile().max_turns),
        )
        self.assertEqual(
            launched["state"].lease_seconds,
            general_profile().claim_lease_seconds,
        )
        self.assertEqual(
            launched["state"].agent_profile_revision,
            general_profile().revision,
        )
        state_path = Path(
            launched["kwargs"]["env"]["FOXHOUND_EXECUTION_STATE"]
        )
        state_receipt = state_path.read_text(encoding="utf-8")
        self.assertNotIn("claim_token", state_receipt)
        self.assertNotIn("Synthetic task", state_receipt)
        rendered_launch = json.dumps({
            "argv": launched["argv"],
            "environment": launched["kwargs"]["env"],
        })
        self.assertNotIn("claim_token", rendered_launch)
        self.assertNotIn("Synthetic task", rendered_launch)
        self.assertEqual(
            self.service.get(1).status, WorkflowStatus.AWAITING_REVIEW
        )

    def test_a_granted_advance_is_a_recorded_run_not_a_release(self):
        """A grant queues the next phase instead of raising a card, so the
        run ends `queued` -- the one status a release also leaves behind.
        Read as a release it counted as no progress, and every successful
        run on a deployment that configures a grant exited 70. The new
        result is what tells them apart.
        """
        self._bind_origin("issue")
        self._ready()
        launched = {}

        def popen(_argv, **kwargs):
            state_path = Path(kwargs["env"]["FOXHOUND_EXECUTION_STATE"])
            document = json.loads(state_path.read_text(encoding="utf-8"))
            launched["grants"] = document["execution_grants"]

            def record():
                state = load_run_state(state_path)
                # As the worker does: the grants the runner handed it.
                service = TaskExecutionService(
                    state.database_path,
                    execution_grants=tuple(state.execution_grants),
                    action_grants=tuple(state.action_grants),
                )
                recorded = service.record_result(
                    ExecutionResultEnvelope(
                        result_id=RESULT_ID,
                        task_id=state.task_id,
                        task_version=state.task_version,
                        workflow_version=state.workflow_version,
                        phase=state.phase,
                        claim_token=state.claim_token,
                        outcome=ExecutionOutcome.AWAITING_PLAN,
                        summary="Synthetic result",
                        work_markdown="Synthetic plan",
                    )
                )
                self.assertEqual(recorded.status, WorkflowStatus.QUEUED)

            return FakeProcess(callback=record)

        result = run_once(
            self._config(execution_grants=("issue",)),
            base_environment={"PATH": "/usr/bin"},
            popen=popen,
            run_id_factory=lambda: "e" * 32,
            terminate=self._terminator,
        )

        self.assertEqual(launched["grants"], ["issue"])
        self.assertEqual((result.outcome, result.exit_code), ("recorded", 0))
        self.assertTrue(result.ok)
        workflow = self.service.get(1)
        self.assertEqual(workflow.status, WorkflowStatus.QUEUED)
        self.assertEqual(workflow.phase, WorkflowPhase.EXECUTE)

    def test_a_released_claim_is_still_no_progress(self):
        """The other `queued` ending, kept beside the one above: an agent
        that releases without recording has produced nothing, and must not
        be reported as a run that did.
        """
        self._ready()

        def popen(_argv, **kwargs):
            state_path = Path(kwargs["env"]["FOXHOUND_EXECUTION_STATE"])

            def release():
                state = load_run_state(state_path)
                TaskExecutionService(state.database_path).release(
                    state.task_id,
                    expected_version=state.workflow_version,
                    claim_token=state.claim_token,
                )

            return FakeProcess(callback=release)

        result = run_once(
            self._config(),
            base_environment={"PATH": "/usr/bin"},
            popen=popen,
            run_id_factory=lambda: "f" * 32,
            terminate=self._terminator,
        )

        self.assertEqual(
            (result.outcome, result.exit_code), ("released", 70)
        )
        self.assertFalse(result.ok)

    def test_process_exit_and_start_failure_enter_durable_backoff(self):
        self._ready()
        result = run_once(
            self._config(),
            popen=lambda *_args, **_kwargs: FakeProcess(exit_code=3),
            run_id_factory=lambda: "b" * 32,
            terminate=self._terminator,
        )
        self.assertEqual((result.outcome, result.exit_code), ("process_exit", 3))
        state = self.service.get(1)
        self.assertEqual(state.status, WorkflowStatus.QUEUED)
        self.assertEqual(state.last_failure_reason, "process_exit")
        self.assertEqual(state.last_failure_exit_code, 3)
        self.assertEqual(state.last_failure_run_id, "b" * 32)
        self.assertEqual(state.failure_count, 1)

        self.service.retry(1, expected_version=state.version)

        def failed_start(*_args, **_kwargs):
            raise OSError("synthetic launch failure")

        result = run_once(
            self._config(),
            popen=failed_start,
            run_id_factory=lambda: "c" * 32,
            terminate=self._terminator,
        )
        self.assertEqual(result.outcome, "startup_failed")
        self.assertEqual(
            self.service.get(1).last_failure_reason, "startup_failed"
        )

    def test_selected_profile_controls_exact_prompt_tools_turns_and_timing(self):
        specialist = parse_profile({
            "schema": "foxhound.agent-profile",
            "schema_version": 1,
            "profile_id": "specialist",
            "display_name": "Synthetic Specialist",
            "runtime": "hermes",
            "prompt_template": (
                f"First call {WORKER_COMMAND_TOKEN} context. Synthetic role."
            ),
            "toolsets": ["terminal", "file"],
            "max_turns": 50,
            "timeout_seconds": 1_800,
            "claim_lease_seconds": 2_700,
            "heartbeat_seconds": 60,
            "kill_grace_seconds": 30,
            "allowed_phases": ["plan", "execute"],
        })
        registry = AgentProfileRegistry((general_profile(), specialist))
        service = TaskExecutionService(
            self.database,
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
        launched = {}

        def popen(argv, **kwargs):
            launched.update(argv=argv, kwargs=kwargs)
            state = load_run_state(
                Path(kwargs["env"]["FOXHOUND_EXECUTION_STATE"])
            )
            launched["state"] = state
            instructions = Path(kwargs["cwd"]) / INSTRUCTIONS_NAME
            launched["instructions"] = json.loads(
                instructions.read_text(encoding="utf-8")
            )
            launched["instructions_mode"] = stat.S_IMODE(
                instructions.lstat().st_mode
            )
            launched["instructions_path"] = instructions

            def release():
                TaskExecutionService(state.database_path).release(
                    state.task_id,
                    expected_version=state.workflow_version,
                    claim_token=state.claim_token,
                )

            return FakeProcess(callback=release)

        result = run_once(
            self._config(profile_registry=registry),
            popen=popen,
            run_id_factory=lambda: "2" * 32,
            terminate=self._terminator,
        )

        self.assertEqual(result.outcome, "released")
        self.assertEqual(
            tuple(launched["argv"]),
            profile_argv("synthetic-agent run", specialist),
        )
        # The instructions belong to this run, not to its arguments: another
        # user reading the process table learns the profile's limits, never
        # its role.
        self.assertNotIn("Synthetic role", json.dumps(launched["argv"]))
        self.assertEqual(launched["instructions"], specialist.document())
        self.assertEqual(launched["instructions_mode"], 0o600)
        self.assertFalse(launched["instructions_path"].exists())
        self.assertEqual(
            launched["state"].worker_command, "foxhound-task-worker"
        )
        self.assertIn("50", launched["argv"])
        self.assertIn("terminal,file", launched["argv"])
        self.assertEqual(launched["state"].lease_seconds, 2_700)
        self.assertEqual(
            launched["state"].agent_profile_revision,
            specialist.revision,
        )

    def test_configured_default_profile_is_bound_before_the_first_claim(self):
        specialist = parse_profile({
            **general_profile().document(),
            "profile_id": "specialist",
            "display_name": "Synthetic Specialist",
        })
        registry = AgentProfileRegistry((general_profile(), specialist))
        with closing(sqlite3.connect(self.database)) as connection:
            connection.execute(
                "INSERT INTO candidate_inbox(candidate_id,source_system,"
                "source_kind,source_record_id,source_item_id,"
                "source_revision,payload_json,created_at,"
                "first_imported_at,updated_at) "
                "VALUES('candidate-1','gw','legacy','record-1','item-1',"
                "?,'{}','2030-01-02T03:04:05+00:00',"
                "'2030-01-02T03:04:05+00:00','2030-01-02T03:04:05+00:00')",
                ("a" * 64,),
            )
            connection.execute(
                "INSERT INTO task_candidate_bindings(candidate_id,"
                "source_revision,task_id,relation,decided_at) "
                "VALUES('candidate-1',?,1,'accepted',?)",
                ("a" * 64, "2030-01-02T03:04:05+00:00"),
            )
            connection.commit()
        launched = {}

        def popen(_argv, **kwargs):
            state = load_run_state(
                Path(kwargs["env"]["FOXHOUND_EXECUTION_STATE"])
            )
            launched["state"] = state

            def release():
                TaskExecutionService(state.database_path).release(
                    state.task_id,
                    expected_version=state.workflow_version,
                    claim_token=state.claim_token,
                )

            return FakeProcess(callback=release)

        result = run_once(
            self._config(
                profile_registry=registry,
                default_agent_profile=specialist.profile_id,
                planning_grants=("legacy",),
            ),
            popen=popen,
            run_id_factory=lambda: "3" * 32,
            terminate=self._terminator,
        )

        self.assertEqual(result.outcome, "released")
        self.assertEqual(
            launched["state"].agent_profile_id, specialist.profile_id,
        )

    def test_default_profile_must_be_installed_and_plan_eligible(self):
        execute_only = parse_profile({
            **general_profile().document(),
            "profile_id": "execute-only",
            "display_name": "Synthetic Executor",
            "allowed_phases": ["execute"],
        })
        registry = AgentProfileRegistry((general_profile(), execute_only))
        for profile_id in ("missing", execute_only.profile_id):
            with self.subTest(profile_id=profile_id):
                with self.assertRaisesRegex(
                    ValueError, "default agent profile"
                ):
                    self._config(
                        profile_registry=registry,
                        default_agent_profile=profile_id,
                    )

    def test_runner_honors_a_pinned_historical_general_policy(self):
        registry = load_registry()
        current = general_profile()
        historical = next(
            profile
            for _, revision in registry.revisions()
            if revision != current.revision
            and (profile := registry.resolve("general", revision)).max_turns
            == 12
        )
        old_service = TaskExecutionService(
            self.database,
            profile_registry=AgentProfileRegistry((historical,)),
        )
        scheduled = old_service.schedule(1, expected_task_version=1)
        old_service.start_action(
            1, expected_version=scheduled.version, action="start"
        )
        launched = {}

        def popen(argv, **kwargs):
            launched["argv"] = argv
            state = load_run_state(
                Path(kwargs["env"]["FOXHOUND_EXECUTION_STATE"])
            )
            launched["state"] = state

            def release():
                TaskExecutionService(state.database_path).release(
                    state.task_id,
                    expected_version=state.workflow_version,
                    claim_token=state.claim_token,
                )

            return FakeProcess(callback=release)

        result = run_once(
            self._config(),
            popen=popen,
            run_id_factory=lambda: "9" * 32,
            terminate=self._terminator,
        )

        self.assertEqual(result.outcome, "released")
        turn_index = launched["argv"].index("--max-turns")
        self.assertEqual(launched["argv"][turn_index + 1], "12")
        self.assertEqual(launched["state"].lease_seconds, 900)
        self.assertEqual(
            launched["state"].agent_profile_revision,
            historical.revision,
        )

    def test_runner_defers_a_selected_revision_missing_from_its_registry(self):
        """The run survives a pin this machine cannot resolve.

        Raising here stopped the runner itself, so one workflow pinned to a
        revision the catalog no longer carried also stopped every healthy
        workflow behind it. The pin is a property of that workflow: it is
        deferred, reported, and the run ends cleanly idle.
        """
        specialist = parse_profile({
            **general_profile().document(),
            "profile_id": "specialist",
            "display_name": "Synthetic Specialist",
        })
        registry = AgentProfileRegistry((general_profile(), specialist))
        service = TaskExecutionService(
            self.database, profile_registry=registry
        )
        scheduled = service.schedule(1, expected_task_version=1)
        selected = service.select_agent(
            1,
            expected_version=scheduled.version,
            profile_id=specialist.profile_id,
            profile_revision=specialist.revision,
        )
        service.start_action(1, expected_version=selected.version, action="start")

        result = run_once(self._config())

        self.assertEqual(result.outcome, "idle")
        self.assertTrue(result.ok)
        self.assertEqual(result.deferred, (1,))
        deferred = service.get(1)
        self.assertEqual(deferred.last_failure_reason, "startup_failed")
        self.assertIsNotNone(deferred.next_attempt_at)

    def test_plan_only_runner_does_not_claim_execute_work(self):
        self._ready()
        claim = self.service.claim_next()
        self.assertIsNotNone(claim)
        recorded = self.service.record_result(ExecutionResultEnvelope(
            result_id=RESULT_ID,
            task_id=claim.task_id,
            task_version=claim.task_version,
            workflow_version=claim.workflow_version,
            phase=claim.phase,
            claim_token=claim.token,
            outcome=ExecutionOutcome.AWAITING_PLAN,
            summary="Synthetic result",
            work_markdown="Synthetic plan",
        ))
        queued = self.service.review_action(
            1, expected_version=recorded.version, action="approve"
        )
        self.assertEqual(queued.phase, WorkflowPhase.EXECUTE)

        launched = False

        def popen(*_args, **_kwargs):
            nonlocal launched
            launched = True
            return FakeProcess(exit_code=0)

        before = self.service.get(1)
        result = run_once(
            self._config(allowed_phases=(WorkflowPhase.PLAN,)),
            popen=popen,
        )
        self.assertEqual((result.outcome, result.exit_code), ("idle", 0))
        self.assertFalse(launched)
        self.assertEqual(self.service.get(1), before)

    def test_timeout_terminates_and_records_failure(self):
        self._ready()
        monotonic = MutableMonotonic()
        process = FakeProcess()
        result = run_once(
            self._config(),
            popen=lambda *_args, **_kwargs: process,
            clock=monotonic,
            sleep=monotonic.sleep,
            run_id_factory=lambda: "d" * 32,
            terminate=self._terminator,
        )
        self.assertEqual((result.outcome, result.exit_code), ("timeout", 124))
        self.assertEqual(monotonic.value, general_profile().timeout_seconds)
        self.assertTrue(process.terminated)
        self.assertEqual(self.service.get(1).last_failure_reason, "timeout")

    def test_recorded_result_wins_a_race_with_process_failure(self):
        self._ready()
        original_fail = TaskExecutionService.fail

        def record_then_fail(
            service,
            task_id,
            *,
            expected_version,
            claim_token,
            reason,
            **diagnostics,
        ):
            accepted = TaskExecutionService(self.database).record_result(
                ExecutionResultEnvelope(
                    result_id=RESULT_ID,
                    task_id=task_id,
                    task_version=1,
                    workflow_version=expected_version,
                    phase="plan",
                    claim_token=claim_token,
                    outcome=ExecutionOutcome.AWAITING_PLAN,
                    summary="Synthetic result",
                    work_markdown="Synthetic plan",
                )
            )
            self.assertTrue(accepted.accepted)
            return original_fail(
                service,
                task_id,
                expected_version=expected_version,
                claim_token=claim_token,
                reason=reason,
                **diagnostics,
            )

        with mock.patch.object(
            TaskExecutionService, "fail", new=record_then_fail
        ):
            result = run_once(
                self._config(),
                popen=lambda *_args, **_kwargs: FakeProcess(exit_code=2),
                run_id_factory=lambda: "1" * 32,
                terminate=self._terminator,
            )

        self.assertEqual((result.outcome, result.exit_code), ("recorded", 0))
        self.assertEqual(
            self.service.get(1).status, WorkflowStatus.AWAITING_REVIEW
        )

    def test_release_and_task_invalidation_stop_the_child(self):
        self._ready()
        captured = {}

        def release_popen(_argv, **kwargs):
            state = load_run_state(kwargs["env"]["FOXHOUND_EXECUTION_STATE"])

            def release():
                TaskExecutionService(state.database_path).release(
                    state.task_id,
                    expected_version=state.workflow_version,
                    claim_token=state.claim_token,
                )

            captured["released"] = FakeProcess(callback=release)
            return captured["released"]

        result = run_once(
            self._config(),
            popen=release_popen,
            run_id_factory=lambda: "e" * 32,
            terminate=self._terminator,
        )
        self.assertEqual(result.outcome, "released")
        self.assertTrue(captured["released"].terminated)

        self.service.retry(1, expected_version=self.service.get(1).version)
        monotonic = MutableMonotonic()

        def close_popen(_argv, **_kwargs):
            def close_task():
                TaskLedger(self.database).transition(
                    1, expected_version=1, action="done"
                )

            return FakeProcess(callback=close_task)

        result = run_once(
            self._config(),
            popen=close_popen,
            clock=monotonic,
            sleep=monotonic.sleep,
            run_id_factory=lambda: "f" * 32,
            terminate=self._terminator,
        )
        self.assertEqual(result.outcome, "claim_lost")

    def test_lock_private_paths_and_timing_fail_closed_before_claim(self):
        self._ready()
        with _exclusive_lock(_runner_lock_path(self.run_root, "default")) as acquired:
            self.assertTrue(acquired)
            result = run_once(self._config())
        self.assertEqual(result.outcome, "already_running")
        self.assertEqual(self.service.get(1).status, WorkflowStatus.QUEUED)

        self.run_root.chmod(0o755)
        with self.assertRaises(ExecutionRunnerError):
            run_once(self._config())
        self.run_root.chmod(0o700)
        with self.assertRaises(ValueError):
            self._config(poll_seconds=0)
        for phases in (
            (),
            (WorkflowPhase.PLAN, WorkflowPhase.PLAN),
            ("plan",),
        ):
            with self.subTest(phases=phases):
                with self.assertRaises(ValueError):
                    self._config(allowed_phases=phases)

        self.database.chmod(0o644)
        with self.assertRaises(ExecutionRunnerError):
            run_once(self._config())

        rendered = repr(self._config())
        self.assertNotIn(str(self.database), rendered)
        self.assertNotIn(str(self.run_root), rendered)
        self.assertNotIn("primary", rendered)

    def test_public_agent_prompt_and_argv_have_no_task_or_capability(self):
        bootstrap = agent_prompt()
        self.assertIn("absolute path in the same starting directory", bootstrap)
        argv = hermes_argv("hermes", max_turns=12, toolsets="terminal")
        rendered = json.dumps(argv)
        instructions = general_profile().render_prompt("foxhound-task-worker")
        self.assertIn("foxhound-task-worker context", bootstrap)
        self.assertIn("--ignore-rules", argv)
        self.assertNotIn("--safe-mode", argv)
        # The bootstrap says how to ask for the instructions. It is not a
        # short copy of them: process arguments are readable outside the run.
        self.assertLess(len(bootstrap), len(instructions) // 2)
        self.assertNotIn(bootstrap, rendered.replace(json.dumps(bootstrap), ""))
        for sentence in (
            "result-summary.txt",
            "result-work.md",
            "draft --outcome OUTCOME",
            "Do not hand-author or experimentally probe the envelope schema",
        ):
            with self.subTest(sentence=sentence[:32]):
                self.assertIn(sentence, instructions)
                self.assertNotIn(sentence, bootstrap)
        prompt = instructions
        self.assertNotIn("Synthetic task", prompt + rendered)
        self.assertNotIn("claim_token", prompt + rendered)
        self.assertNotIn(str(self.database), prompt + rendered)

    def test_public_coder_example_builds_exact_hermes_argv(self):
        path = (
            Path(__file__).parents[1]
            / "examples"
            / "agent-profiles"
            / "example-coder.json"
        )
        profile = parse_profile(json.loads(path.read_text(encoding="utf-8")))
        argv = profile_argv(
            "synthetic-hermes --local",
            profile,
            worker_command="synthetic-worker",
        )

        self.assertEqual(
            argv,
            (
                "synthetic-hermes",
                "--local",
                "chat",
                "--quiet",
                "--query",
                agent_prompt("synthetic-worker"),
                "--max-turns",
                "50",
                "--source",
                "tool",
                "--ignore-rules",
                "--toolsets",
                "terminal,file,web,vision",
            ),
        )
        self.assertNotIn(
            profile.render_prompt("synthetic-worker"), json.dumps(argv)
        )
        for phase in WorkflowPhase:
            with self.subTest(phase=phase):
                self.assertIn(phase.value, profile.allowed_phases)

    def test_cli_internal_failure_is_content_free(self):
        private_value = "synthetic-private-runtime-value"
        output = StringIO()
        errors = StringIO()
        arguments = [
            "--database", private_value,
            "--run-root", private_value,
            "--gw-endpoint", "http://127.0.0.1:8787",
            "--gw-alias", "primary",
            "--gw-token-file", private_value,
        ]
        with redirect_stdout(output), redirect_stderr(errors):
            with mock.patch(
                "foxhound.execution_runner.verify_worker"
            ), mock.patch(
                "foxhound.execution_runner.run_once",
                side_effect=OSError(private_value),
            ):
                code = main(arguments)
        self.assertEqual(code, 70)
        self.assertNotIn(private_value, output.getvalue() + errors.getvalue())

    def test_cli_refuses_to_claim_when_the_worker_cannot_read_run_state(self):
        # The failure this prevents: the runner writes run state at its own
        # schema, the worker an agent reaches refuses a schema it does not
        # know, and every claimed run dies at the agent's first tool call
        # having produced nothing. One refusal costs a single poll; the
        # alternative costs every workflow in the queue.
        output = StringIO()
        errors = StringIO()
        arguments = [
            "--database", str(self.database),
            "--run-root", str(self.run_root),
            "--gw-endpoint", "http://127.0.0.1:8787",
            "--gw-alias", "primary",
            "--gw-token-file", str(self.token_file),
        ]
        with redirect_stdout(output), redirect_stderr(errors), mock.patch(
            "foxhound.execution_runner.verify_worker",
            side_effect=WorkerMismatch(
                "execution worker speaks run-state schema 4, runner writes 5"
            ),
        ), mock.patch("foxhound.execution_runner.run_once") as run:
            code = main(arguments)
        self.assertEqual(code, 78)
        run.assert_not_called()
        self.assertIn("refusing to claim", errors.getvalue())

    def test_cli_passes_an_explicit_phase_allowlist(self):
        output = StringIO()
        profiles = self.root / "profiles"
        profiles.mkdir(mode=0o700)
        specialist = {
            **general_profile().document(),
            "profile_id": "specialist",
            "display_name": "Synthetic Specialist",
        }
        manifest = profiles / "specialist.json"
        manifest.write_text(json.dumps(specialist), encoding="utf-8")
        manifest.chmod(0o600)
        arguments = [
            "--database", str(self.database),
            "--run-root", str(self.run_root),
            "--gw-endpoint", "http://127.0.0.1:8787",
            "--gw-alias", "primary",
            "--gw-token-file", str(self.token_file),
            "--agent-profile-directory", str(profiles),
            "--default-agent-profile", "specialist",
            "--allowed-phase", "plan",
        ]
        with redirect_stdout(output), mock.patch(
            "foxhound.execution_runner.verify_worker"
        ), mock.patch(
            "foxhound.execution_runner.run_once",
            return_value=ExecutionRunResult("idle", 0),
        ) as run:
            code = main(arguments)
        self.assertEqual(code, 0)
        self.assertEqual(
            run.call_args.args[0].allowed_phases,
            (WorkflowPhase.PLAN,),
        )
        self.assertEqual(
            run.call_args.args[0].default_agent_profile,
            "specialist",
        )
        self.assertIsNotNone(
            run.call_args.args[0].profile_registry.get("specialist")
        )


if __name__ == "__main__":
    unittest.main()
