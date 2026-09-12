#!/usr/bin/env python3
"""Synthetic tests for supervised Foxhound execution runs."""

from __future__ import annotations

import json
import sqlite3
import subprocess
import tempfile
import unittest
from contextlib import closing, redirect_stderr, redirect_stdout
from io import StringIO
from pathlib import Path
from unittest import mock

from foxhound.candidate_inbox import CandidateInbox
from foxhound.execution_runner import (
    ExecutionRunnerConfig,
    ExecutionRunnerError,
    ExecutionRunResult,
    _exclusive_lock,
    agent_prompt,
    hermes_argv,
    main,
    run_once,
)
from foxhound.execution_worker import load_run_state
from foxhound.task_execution import (
    ExecutionOutcome,
    ExecutionResultEnvelope,
    TaskExecutionService,
    WorkflowPhase,
    WorkflowStatus,
)
from foxhound.task_ledger import TaskLedger


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
        CandidateInbox(self.database).initialize()
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
            "agent_argv": ("synthetic-agent", "run"),
            "timeout_seconds": 10,
            "lease_seconds": 30,
            "heartbeat_seconds": 5,
            "kill_grace_seconds": 1,
            "poll_seconds": 1,
        }
        values.update(changes)
        return ExecutionRunnerConfig(**values)

    @staticmethod
    def _terminator(process, _grace, **_kwargs) -> bool:
        process.terminated = True
        return False

    def test_runner_records_and_scrubs_capability_without_shell_or_output(self):
        self._ready()
        launched = {}

        def popen(argv, **kwargs):
            launched.update(argv=argv, kwargs=kwargs)
            state_path = Path(kwargs["env"]["FOXHOUND_EXECUTION_STATE"])

            def record():
                state = load_run_state(state_path)
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
        self.assertIs(launched["kwargs"]["stdout"], subprocess.DEVNULL)
        self.assertIs(launched["kwargs"]["stderr"], subprocess.DEVNULL)
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

    def test_plan_only_runner_does_not_claim_execute_work(self):
        self._ready()
        claim = self.service.claim_next(lease_seconds=30)
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
            self._config(timeout_seconds=2),
            popen=lambda *_args, **_kwargs: process,
            clock=monotonic,
            sleep=monotonic.sleep,
            run_id_factory=lambda: "d" * 32,
            terminate=self._terminator,
        )
        self.assertEqual((result.outcome, result.exit_code), ("timeout", 124))
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
            self._config(heartbeat_seconds=1, lease_seconds=5,
                         kill_grace_seconds=0.5),
            popen=close_popen,
            clock=monotonic,
            sleep=monotonic.sleep,
            run_id_factory=lambda: "f" * 32,
            terminate=self._terminator,
        )
        self.assertEqual(result.outcome, "claim_lost")

    def test_lock_private_paths_and_timing_fail_closed_before_claim(self):
        self._ready()
        with _exclusive_lock(self.run_root / ".runner.lock") as acquired:
            self.assertTrue(acquired)
            result = run_once(self._config())
        self.assertEqual(result.outcome, "already_running")
        self.assertEqual(self.service.get(1).status, WorkflowStatus.QUEUED)

        self.run_root.chmod(0o755)
        with self.assertRaises(ExecutionRunnerError):
            run_once(self._config())
        self.run_root.chmod(0o700)
        with self.assertRaises(ValueError):
            self._config(heartbeat_seconds=10, lease_seconds=20)
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
        prompt = agent_prompt()
        argv = hermes_argv("hermes", max_turns=12, toolsets="terminal")
        rendered = json.dumps(argv)
        self.assertIn("foxhound-task-worker context", prompt)
        self.assertIn(
            "`summary` and `work_markdown` must each be one JSON string",
            prompt,
        )
        self.assertIn(
            "`questions`, `external_actions`, and `deliverables` must each "
            "be a JSON array of strings",
            prompt,
        )
        example = prompt.split("Shape example: ", 1)[1].splitlines()[0]
        draft = json.loads(example)
        self.assertIsInstance(draft["summary"], str)
        self.assertIsInstance(draft["work_markdown"], str)
        for field in ("questions", "external_actions", "deliverables"):
            self.assertIsInstance(draft[field], list)
        self.assertNotIn("Synthetic task", prompt + rendered)
        self.assertNotIn("claim_token", prompt + rendered)
        self.assertNotIn(str(self.database), prompt + rendered)

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
                "foxhound.execution_runner.run_once",
                side_effect=OSError(private_value),
            ):
                code = main(arguments)
        self.assertEqual(code, 70)
        self.assertNotIn(private_value, output.getvalue() + errors.getvalue())

    def test_cli_passes_an_explicit_phase_allowlist(self):
        output = StringIO()
        arguments = [
            "--database", str(self.database),
            "--run-root", str(self.run_root),
            "--gw-endpoint", "http://127.0.0.1:8787",
            "--gw-alias", "primary",
            "--gw-token-file", str(self.token_file),
            "--allowed-phase", "plan",
        ]
        with redirect_stdout(output), mock.patch(
            "foxhound.execution_runner.run_once",
            return_value=ExecutionRunResult("idle", 0),
        ) as run:
            code = main(arguments)
        self.assertEqual(code, 0)
        self.assertEqual(
            run.call_args.args[0].allowed_phases,
            (WorkflowPhase.PLAN,),
        )


if __name__ == "__main__":
    unittest.main()
