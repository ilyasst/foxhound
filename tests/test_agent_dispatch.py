from __future__ import annotations

import json
import os
import stat
import subprocess
import sys
import tempfile
import time
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from foxhound.agent_dispatch import (
    DispatchError,
    cancel,
    log,
    start,
    status,
    wait,
)


class AgentDispatchTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.state_root = self.root / "state"
        self.work = self.root / "work"
        self.work.mkdir()
        subprocess.run(["git", "init", "-q", str(self.work)], check=True)
        self.prompt = self.root / "prompt.txt"
        self.prompt.write_text("SYNTHETIC PRIVATE PROMPT\n", encoding="utf-8")
        self.credentials = self.root / ".env"
        self.credentials.write_text("SYNTHETIC_KEY=synthetic-secret\n", encoding="utf-8")
        self.runtime = self.root / "runtime"
        self.runtime.write_text(
            "#!" + sys.executable + "\n"
            "import json, os, sys, time\n"
            "print(json.dumps({'argv': sys.argv[1:], 'home': os.environ['HERMES_HOME'], "
            "'credential': bool(os.environ.get('SYNTHETIC_KEY'))}), flush=True)\n"
            "print(open(os.path.join(os.environ['HERMES_HOME'], 'config.yaml')).read(), flush=True)\n"
            "time.sleep(float(os.environ.get('SYNTHETIC_RUNTIME_SLEEP', '0')))\n"
            "raise SystemExit(int(os.environ.get('SYNTHETIC_RUNTIME_EXIT', '0')))\n",
            encoding="utf-8",
        )
        self.runtime.chmod(0o700)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def _start(self, *, work: Path | None = None, concurrency: int = 2):
        return start(
            prompt=self.prompt,
            working_directory=work or self.work,
            state_root=self.state_root,
            runtime_command=self.runtime,
            credential_file=self.credentials,
            credential_name="SYNTHETIC_KEY",
            model="synthetic-model",
            provider="synthetic-provider",
            reasoning="low",
            toolsets="terminal,file",
            max_turns=8,
            timeout_seconds=30,
            max_concurrency=concurrency,
        )

    def _wait_running(self, job_id: str) -> None:
        for _ in range(200):
            if status(self.state_root, job_id)["state"] == "running":
                return
            time.sleep(0.01)
        self.fail("job did not enter running state")

    def test_start_wait_and_log_keep_prompt_out_of_argv(self) -> None:
        result = self._start()
        completed = wait(self.state_root, str(result["job_id"]), 10)
        self.assertEqual(completed["state"], "completed")
        transcript = log(self.state_root, str(result["job_id"]), 128 * 1024).decode()
        self.assertNotIn("SYNTHETIC PRIVATE PROMPT", transcript)
        self.assertIn("reasoning_effort: low", transcript)
        self.assertIn('"credential": true', transcript)
        state_file = self.state_root / "jobs" / str(result["job_id"]) / "state.json"
        prompt_file = state_file.with_name("prompt.txt")
        self.assertEqual(stat.S_IMODE(prompt_file.stat().st_mode), 0o600)
        self.assertEqual(stat.S_IMODE(state_file.parent.stat().st_mode), 0o700)

    def test_nonzero_runtime_is_failed(self) -> None:
        with patch.dict(os.environ, {"SYNTHETIC_RUNTIME_EXIT": "9"}):
            result = self._start()
            completed = wait(self.state_root, str(result["job_id"]), 10)
        self.assertEqual(completed["state"], "failed")
        self.assertEqual(completed["exit_code"], 9)

    def test_same_working_directory_is_locked(self) -> None:
        with patch.dict(os.environ, {"SYNTHETIC_RUNTIME_SLEEP": "30"}):
            first = self._start()
            with self.assertRaisesRegex(DispatchError, "working_directory_busy"):
                self._start()
            job_id = str(first["job_id"])
            self._wait_running(job_id)
            cancel(self.state_root, job_id)
            self.assertEqual(wait(self.state_root, job_id, 10)["state"], "cancelled")

    def test_global_concurrency_is_bounded(self) -> None:
        other = self.root / "other"
        other.mkdir()
        subprocess.run(["git", "init", "-q", str(other)], check=True)
        with patch.dict(os.environ, {"SYNTHETIC_RUNTIME_SLEEP": "30"}):
            first = self._start(concurrency=1)
            with self.assertRaisesRegex(DispatchError, "concurrency_limit"):
                self._start(work=other, concurrency=1)
            job_id = str(first["job_id"])
            self._wait_running(job_id)
            cancel(self.state_root, job_id)
            self.assertEqual(wait(self.state_root, job_id, 10)["state"], "cancelled")

    def test_cancel_is_terminal_and_targets_recorded_group(self) -> None:
        with patch.dict(os.environ, {"SYNTHETIC_RUNTIME_SLEEP": "30"}):
            result = self._start()
            job_id = str(result["job_id"])
            self._wait_running(job_id)
            cancelled = cancel(self.state_root, job_id)
        self.assertEqual(cancelled["state"], "cancelling")
        self.assertEqual(wait(self.state_root, job_id, 10)["state"], "cancelled")

    def test_status_never_returns_prompt_text_or_process_identity(self) -> None:
        result = self._start()
        completed = wait(self.state_root, str(result["job_id"]), 10)
        encoded = json.dumps(completed)
        self.assertNotIn("SYNTHETIC PRIVATE PROMPT", encoded)
        self.assertNotIn("supervisor_pid", encoded)
        self.assertNotIn("synthetic-secret", encoded)

    def test_missing_credential_fails_inside_private_job(self) -> None:
        self.credentials.write_text("OTHER_KEY=value\n", encoding="utf-8")
        result = self._start()
        completed = wait(self.state_root, str(result["job_id"]), 10)
        self.assertEqual(completed["state"], "failed")
        self.assertEqual(completed["exit_code"], 70)

    def test_prompt_symlink_is_refused(self) -> None:
        link = self.root / "prompt-link"
        link.symlink_to(self.prompt)
        original = self.prompt
        self.prompt = link
        try:
            with self.assertRaisesRegex(DispatchError, "invalid_prompt"):
                self._start()
        finally:
            self.prompt = original

    def test_non_repository_working_directory_is_refused(self) -> None:
        outside = self.root / "outside"
        outside.mkdir()
        with self.assertRaisesRegex(DispatchError, "invalid_working_directory"):
            self._start(work=outside)

    def test_recent_heartbeat_survives_an_invisible_process_namespace(self) -> None:
        job_id = "1" * 32
        directory = self.state_root / "jobs" / job_id
        directory.mkdir(mode=0o700, parents=True)
        document = {
            "schema": "foxhound.agent-dispatch-job.v1",
            "job_id": job_id,
            "state": "running",
            "created_at": datetime.now(timezone.utc).isoformat(),
            "heartbeat_at": datetime.now(timezone.utc).isoformat(),
            "supervisor_pid": 999_999_999,
            "supervisor_start": "123",
        }
        (directory / "state.json").write_text(json.dumps(document), encoding="utf-8")
        with patch("foxhound.agent_dispatch._process_start", return_value=None):
            self.assertEqual(status(self.state_root, job_id)["state"], "running")

    def test_stale_heartbeat_marks_an_invisible_process_orphaned(self) -> None:
        job_id = "2" * 32
        directory = self.state_root / "jobs" / job_id
        directory.mkdir(mode=0o700, parents=True)
        old = datetime.now(timezone.utc) - timedelta(minutes=5)
        document = {
            "schema": "foxhound.agent-dispatch-job.v1",
            "job_id": job_id,
            "state": "running",
            "created_at": old.isoformat(),
            "heartbeat_at": old.isoformat(),
            "supervisor_pid": 999_999_999,
            "supervisor_start": "123",
        }
        (directory / "state.json").write_text(json.dumps(document), encoding="utf-8")
        with patch("foxhound.agent_dispatch._process_start", return_value=None):
            self.assertEqual(status(self.state_root, job_id)["state"], "orphaned")


if __name__ == "__main__":
    unittest.main()
