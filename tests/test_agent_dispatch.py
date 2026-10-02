from __future__ import annotations

import json
import os
import signal
import sqlite3
import stat
import subprocess
import sys
import tempfile
import time
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

from foxhound.agent_dispatch import (
    DispatchError,
    _probe_edited,
    _probe_tool_calls,
    _run,
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
        subprocess.run(["git", "-c", "core.hooksPath=/dev/null", "init", "-q", str(self.work)], check=True)
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
        subprocess.run(["git", "-c", "core.hooksPath=/dev/null", "init", "-q", str(other)], check=True)
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

    def test_first_edit_within_validation(self) -> None:
        with self.assertRaisesRegex(DispatchError, "invalid_limits"):
            start(
                prompt=self.prompt, working_directory=self.work, state_root=self.state_root,
                runtime_command=self.runtime, credential_file=self.credentials, credential_name="SYNTHETIC_KEY",
                model="gemini-3.8-flash", provider="gemini", reasoning="low", toolsets="terminal",
                max_turns=10, timeout_seconds=10, max_concurrency=2, first_edit_within=4,
            )
        with self.assertRaisesRegex(DispatchError, "invalid_limits"):
            start(
                prompt=self.prompt, working_directory=self.work, state_root=self.state_root,
                runtime_command=self.runtime, credential_file=self.credentials, credential_name="SYNTHETIC_KEY",
                model="gemini-3.8-flash", provider="gemini", reasoning="low", toolsets="terminal",
                max_turns=10, timeout_seconds=10, max_concurrency=2, first_edit_within=501,
            )
        # None and valid ints in 5..500 should pass validation
        res = start(
            prompt=self.prompt, working_directory=self.work, state_root=self.state_root,
            runtime_command=self.runtime, credential_file=self.credentials, credential_name="SYNTHETIC_KEY",
            model="gemini-3.8-flash", provider="gemini", reasoning="low", toolsets="terminal",
            max_turns=10, timeout_seconds=10, max_concurrency=2, first_edit_within=None,
        )
        self.assertIsNone(res.get("first_edit_within"))
        wait(self.state_root, str(res["job_id"]), 10)

    def test_status_shows_counters(self) -> None:
        result = self._start()
        stat_doc = status(self.state_root, str(result["job_id"]))
        self.assertIsInstance(stat_doc, dict)
        assert isinstance(stat_doc, dict)
        self.assertIn("tool_calls", stat_doc)
        self.assertIn("edited", stat_doc)
        self.assertIn("first_edit_within", stat_doc)
        self.assertEqual(stat_doc["first_edit_within"], 40)
        wait(self.state_root, str(result["job_id"]), 10)

    def test_stalled_lane_stopped_when_probes_report_no_edit_and_tool_calls_over_limit(self) -> None:
        result = self._start()
        job_id = str(result["job_id"])
        self._wait_running(job_id)
        job_dir = self.state_root / "jobs" / job_id
        # Wait for the running supervisor to finish
        wait(self.state_root, job_id, 10)
        # Remove transcript.log so a subsequent _run invocation can run
        transcript_file = job_dir / "transcript.log"
        if transcript_file.exists():
            transcript_file.unlink()

        # Populate state.db with messages containing 10 tool calls
        db_path = job_dir / "runtime-home" / "state.db"
        with sqlite3.connect(db_path) as conn:
            conn.execute("CREATE TABLE messages (id INTEGER PRIMARY KEY, tool_calls TEXT)")
            conn.execute(
                "INSERT INTO messages (tool_calls) VALUES (?)",
                (json.dumps([{"id": f"call_{i}"} for i in range(10)]),)
            )
        # Verify probes directly
        self.assertEqual(_probe_tool_calls(job_dir / "runtime-home"), 10)
        self.assertFalse(_probe_edited(self.work))
        # Now test _run loop logic directly using a mock Popen
        with patch("subprocess.Popen") as mock_popen, \
             patch("foxhound.agent_dispatch.HEARTBEAT_SECONDS", 0.0), \
             patch("time.sleep", return_value=None), \
             patch("os.killpg") as mock_killpg:
            fake_proc = MagicMock()
            fake_proc.poll.side_effect = [None, None, 0]
            fake_proc.pid = 12345
            fake_proc.wait.return_value = 0
            mock_popen.return_value = fake_proc

            # Update document to have first_edit_within=5 and supervisor info matching current process
            state_path = job_dir / "state.json"
            doc = json.loads(state_path.read_text(encoding="utf-8"))
            doc["state"] = "starting"
            doc["supervisor_pid"] = os.getpid()
            from foxhound.agent_dispatch import _process_start
            doc["supervisor_start"] = _process_start(os.getpid())
            doc["first_edit_within"] = 5
            state_path.write_text(json.dumps(doc), encoding="utf-8")

            exit_code = _run(self.state_root, job_id, self.runtime, self.credentials, "SYNTHETIC_KEY")
            self.assertEqual(exit_code, 125)
            final_doc = json.loads(state_path.read_text(encoding="utf-8"))
            self.assertEqual(final_doc["state"], "stalled_no_edit")
            self.assertEqual(final_doc["exit_code"], 125)
            self.assertEqual(final_doc["tool_calls"], 10)
            self.assertFalse(final_doc["edited"])
            mock_killpg.assert_called_with(12345, signal.SIGTERM)

    def test_lane_that_edits_is_not_stopped(self) -> None:
        result = self._start()
        job_id = str(result["job_id"])
        self._wait_running(job_id)
        job_dir = self.state_root / "jobs" / job_id
        wait(self.state_root, job_id, 10)
        transcript_file = job_dir / "transcript.log"
        if transcript_file.exists():
            transcript_file.unlink()

        (self.work / "file.txt").write_text("hello", encoding="utf-8")
        with patch("foxhound.agent_dispatch._probe_edited", return_value=True):
            self.assertTrue(_probe_edited(self.work))

            with patch("subprocess.Popen") as mock_popen, \
                 patch("foxhound.agent_dispatch.HEARTBEAT_SECONDS", 0.0), \
                 patch("time.sleep", return_value=None):
                fake_proc = MagicMock()
                fake_proc.poll.side_effect = [None, 0]
                fake_proc.pid = 12345
                fake_proc.wait.return_value = 0
                mock_popen.return_value = fake_proc

                state_path = job_dir / "state.json"
                doc = json.loads(state_path.read_text(encoding="utf-8"))
                doc["state"] = "starting"
                doc["supervisor_pid"] = os.getpid()
                from foxhound.agent_dispatch import _process_start
                doc["supervisor_start"] = _process_start(os.getpid())
                doc["first_edit_within"] = 5
                state_path.write_text(json.dumps(doc), encoding="utf-8")

                exit_code = _run(self.state_root, job_id, self.runtime, self.credentials, "SYNTHETIC_KEY")
                self.assertEqual(exit_code, 0)
                final_doc = json.loads(state_path.read_text(encoding="utf-8"))
                self.assertEqual(final_doc["state"], "completed")
                self.assertTrue(final_doc["edited"])

    def test_disabled_deadline_never_stops(self) -> None:
        result = self._start()
        job_id = str(result["job_id"])
        self._wait_running(job_id)
        job_dir = self.state_root / "jobs" / job_id
        wait(self.state_root, job_id, 10)
        transcript_file = job_dir / "transcript.log"
        if transcript_file.exists():
            transcript_file.unlink()

        db_path = job_dir / "runtime-home" / "state.db"
        with sqlite3.connect(db_path) as conn:
            conn.execute("CREATE TABLE messages (id INTEGER PRIMARY KEY, tool_calls TEXT)")
            conn.execute(
                "INSERT INTO messages (tool_calls) VALUES (?)",
                (json.dumps([{"id": f"call_{i}"} for i in range(50)]),)
            )

        with patch("subprocess.Popen") as mock_popen, \
             patch("foxhound.agent_dispatch.HEARTBEAT_SECONDS", 0.0), \
             patch("time.sleep", return_value=None):
            fake_proc = MagicMock()
            fake_proc.poll.side_effect = [None, None, 0]
            fake_proc.pid = 12345
            fake_proc.wait.return_value = 0
            mock_popen.return_value = fake_proc

            state_path = job_dir / "state.json"
            doc = json.loads(state_path.read_text(encoding="utf-8"))
            doc["state"] = "starting"
            doc["supervisor_pid"] = os.getpid()
            from foxhound.agent_dispatch import _process_start
            doc["supervisor_start"] = _process_start(os.getpid())
            doc["first_edit_within"] = None
            state_path.write_text(json.dumps(doc), encoding="utf-8")

            exit_code = _run(self.state_root, job_id, self.runtime, self.credentials, "SYNTHETIC_KEY")
            self.assertEqual(exit_code, 0)
            final_doc = json.loads(state_path.read_text(encoding="utf-8"))
            self.assertEqual(final_doc["state"], "completed")
            self.assertFalse(final_doc["edited"])
            self.assertEqual(final_doc["tool_calls"], 50)

    def test_bootstrap_query_includes_first_edit_deadline_when_set(self) -> None:
        result = self._start()
        job_id = str(result["job_id"])
        completed = wait(self.state_root, job_id, 10)
        self.assertEqual(completed["state"], "completed")
        transcript = log(self.state_root, job_id, 128 * 1024).decode()
        first_line = json.loads(transcript.splitlines()[0])
        argv = first_line["argv"]
        query_idx = argv.index("--query")
        query_text = argv[query_idx + 1]
        expected_warning = (
            "This run is stopped if it makes 40 tool calls without changing a file in the working tree: "
            "make a first concrete edit early, and keep notes in a file in the tree if you are still investigating."
        )
        self.assertIn(expected_warning, query_text)

    def test_bootstrap_query_omits_first_edit_deadline_when_disabled(self) -> None:
        result = start(
            prompt=self.prompt,
            working_directory=self.work,
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
            max_concurrency=2,
            first_edit_within=None,
        )
        job_id = str(result["job_id"])
        completed = wait(self.state_root, job_id, 10)
        self.assertEqual(completed["state"], "completed")
        transcript = log(self.state_root, job_id, 128 * 1024).decode()
        first_line = json.loads(transcript.splitlines()[0])
        argv = first_line["argv"]
        query_idx = argv.index("--query")
        query_text = argv[query_idx + 1]
        self.assertNotIn("This run is stopped if it makes", query_text)


if __name__ == "__main__":
    unittest.main()


class WorktreeBaselineTests(unittest.TestCase):
    def test_edit_is_measured_against_the_starting_state(self):
        import subprocess as sp
        from foxhound.agent_dispatch import _worktree_status
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            sp.run(["git", "-c", "core.hooksPath=/dev/null", "init", "-q", str(repo)], check=True)
            (repo / "earlier_lane.py").write_text("x = 1\n")
            baseline = _worktree_status(repo)
            self.assertFalse(_probe_edited(repo, baseline))
            (repo / "agent-guard-rejection.log").write_text("noise\n")
            self.assertFalse(_probe_edited(repo, baseline))
            (repo / "this_lane.py").write_text("y = 2\n")
            self.assertTrue(_probe_edited(repo, baseline))
