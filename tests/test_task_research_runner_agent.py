"""Synthetic tests for the one-shot Researcher runner in agent mode."""

from __future__ import annotations

import io
import json
import os
import sqlite3
import subprocess
import tempfile
import unittest
from contextlib import closing, redirect_stderr, redirect_stdout
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock

from foxhound import migrate_database
from foxhound.task_research import INPUT_SCHEMA, ResearchStore
from foxhound.task_research_runner import main, run_once


NOW = datetime(2032, 5, 6, 7, 8, 9, tzinfo=timezone.utc)


def _setup_test_env(temp_dir: Path, *, task_text: str = "Perform synthetic task.") -> dict[str, Path]:
    database = temp_dir / "foxhound.sqlite3"
    cas_root = temp_dir / "cas"
    task_work_root = temp_dir / "tasks"
    scratch_root = temp_dir / "scratch"

    cas_root.mkdir(mode=0o700)
    task_work_root.mkdir(mode=0o700)
    scratch_root.mkdir(mode=0o700)

    migrate_database(database)
    stamp = NOW.isoformat(timespec="seconds")
    with closing(sqlite3.connect(database)) as connection:
        connection.execute(
            "INSERT INTO tasks(id,status,text,version,created_at,updated_at) "
            "VALUES(1,'open',?,1,?,?)",
            (task_text, stamp, stamp),
        )
        connection.commit()

    return {
        "database": database,
        "cas_root": cas_root,
        "task_work_root": task_work_root,
        "scratch_root": scratch_root,
    }


def _queue_job(paths: dict[str, Path], *, task_id: int = 1, task_version: int = 1, text: str = "Perform synthetic task.") -> Path:
    task_folder = paths["task_work_root"] / f"T{task_id}-folder"
    task_folder.mkdir(mode=0o700)

    snapshot = {
        "schema_version": INPUT_SCHEMA,
        "task_id": task_id,
        "task_version": task_version,
        "text": text,
        "structured": {
            "action": "perform",
            "object": "synthetic task",
            "confidence": 0.9,
        },
        "due": None,
        "owner": None,
        "participants": [],
        "working_group": None,
        "external_identifiers": [],
        "origin": {"kind": "synthetic", "source_digest": "0" * 64},
        "structured_schema_revisions": {"task": 1},
    }
    store = ResearchStore(paths["database"], paths["cas_root"], clock=lambda: NOW)
    store.request(snapshot, task_work_root=paths["task_work_root"], task_folder=task_folder)
    return task_folder


def _sample_research_json(kb_file: Path) -> dict[str, object]:
    return {
        "ownership": {
            "verdict": "reader",
            "evidence": [str(kb_file)],
        },
        "requested_deliverable": {
            "text": "Produce research summary",
            "evidence": ["https://example.com/spec"],
        },
        "constraints": [
            {"text": "Strict deadline", "binding": True, "evidence": ["https://example.com/deadline"]}
        ],
        "entities": [
            {
                "as_written": "Project A",
                "status": "resolved",
                "meaning": "Alpha Project",
                "evidence": [str(kb_file)],
            }
        ],
        "facts": [
            {
                "text": "Repo is active",
                "status": "confirmed",
                "evidence": ["https://example.com/repo"],
            },
            {
                "text": "Inferred requirement",
                "status": "inferred",
                "evidence": [],
            },
            {
                "text": "Conflicting dates",
                "status": "conflicting",
                "evidence": [str(kb_file)],
            },
        ],
        "open_questions": [],
        "recommendation": {"text": "Proceed", "evidence": []},
    }


class TaskResearchRunnerAgentTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.root.chmod(0o700)
        self.paths = _setup_test_env(self.root)

        self.kb_dir = self.root / "sync_kb"
        self.kb_dir.mkdir(mode=0o700)
        self.kb_file = self.kb_dir / "notes.txt"
        self.kb_file.write_text("sample content")

    def test_agent_mode_publishes_completed_receipt_and_checks_cwd_env(self) -> None:
        task_folder = _queue_job(self.paths)
        valid_research = _sample_research_json(self.kb_file)
        recorded_calls = []

        def fake_runner(argv, cwd=None, env=None, timeout=None, **kwargs):
            recorded_calls.append({"argv": argv, "cwd": cwd, "env": env, "timeout": timeout})
            assert cwd is not None
            research_path = Path(cwd) / "research.json"
            research_path.write_text(json.dumps(valid_research), encoding="utf-8")
            return subprocess.CompletedProcess(argv, 0, stdout="", stderr="")

        result = run_once(
            database=self.paths["database"],
            cas_root=self.paths["cas_root"],
            task_work_root=self.paths["task_work_root"],
            scratch_root=self.paths["scratch_root"],
            model="synthetic-agent-model",
            endpoint="http://127.0.0.1:8800",
            synthesizer="agent",
            hermes_command="/usr/bin/synthetic-hermes",
            knowledge_roots=(("kb", str(self.kb_dir)),),
            agent_runner=fake_runner,
            clock=lambda: NOW,
        )

        self.assertTrue(result.claimed)
        self.assertTrue(result.completed)
        self.assertEqual(result.state, "completed")

        # Fake runner received cwd inside scratch_root and env TERMINAL_CWD equal to that dir
        self.assertEqual(len(recorded_calls), 1)
        runner_call = recorded_calls[0]
        cwd = Path(runner_call["cwd"])
        scratch_root_resolved = self.paths["scratch_root"].resolve()
        self.assertTrue(cwd.resolve().is_relative_to(scratch_root_resolved))
        self.assertEqual(runner_call["env"]["TERMINAL_CWD"], str(cwd.resolve()))

        # Published files exist
        self.assertTrue((task_folder / ".task-research.json").is_file())
        self.assertTrue((task_folder / "Research.md").is_file())

        with closing(sqlite3.connect(self.paths["database"])) as connection:
            receipt = connection.execute(
                "SELECT r.json_digest, r.markdown_digest FROM task_research_receipts r "
                "JOIN task_research_jobs j ON j.job_id=r.job_id WHERE j.task_id=1"
            ).fetchone()
            job = connection.execute("SELECT state FROM task_research_jobs WHERE task_id=1").fetchone()
        self.assertIsNotNone(receipt)
        self.assertEqual(job[0], "completed")

    def test_agent_timeout_leaves_job_requeued_without_consuming_attempt(self) -> None:
        _queue_job(self.paths)

        def timeout_runner(argv, cwd=None, env=None, timeout=None, **kwargs):
            raise subprocess.TimeoutExpired(argv, timeout or 3600)

        result = run_once(
            database=self.paths["database"],
            cas_root=self.paths["cas_root"],
            task_work_root=self.paths["task_work_root"],
            scratch_root=self.paths["scratch_root"],
            model="synthetic-agent-model",
            endpoint="http://127.0.0.1:8800",
            synthesizer="agent",
            hermes_command="/usr/bin/synthetic-hermes",
            agent_runner=timeout_runner,
            clock=lambda: NOW,
        )

        self.assertTrue(result.claimed)
        self.assertFalse(result.completed)
        self.assertEqual(result.failure_code, "model_timeout")
        self.assertEqual(result.state, "queued")

        with closing(sqlite3.connect(self.paths["database"])) as connection:
            row = connection.execute(
                "SELECT state, attempts, failure_code FROM task_research_jobs WHERE task_id=1"
            ).fetchone()
        self.assertEqual(row[0], "queued")
        self.assertEqual(row[1], 0)
        self.assertEqual(row[2], "model_timeout")

    def test_missing_hermes_command_raises_value_error(self) -> None:
        _queue_job(self.paths)
        with self.assertRaises(ValueError):
            run_once(
                database=self.paths["database"],
                cas_root=self.paths["cas_root"],
                task_work_root=self.paths["task_work_root"],
                scratch_root=self.paths["scratch_root"],
                model="synthetic-agent-model",
                endpoint="http://127.0.0.1:8800",
                synthesizer="agent",
                hermes_command=None,
                clock=lambda: NOW,
            )

    def test_cli_rejects_knowledge_root_without_equals_and_relative_paths(self) -> None:
        base_argv = [
            "--database", str(self.paths["database"]),
            "--cas-root", str(self.paths["cas_root"]),
            "--task-work-root", str(self.paths["task_work_root"]),
            "--scratch-root", str(self.paths["scratch_root"]),
            "--model", "synthetic-model",
            "--endpoint", "http://127.0.0.1:8800",
            "--synthesizer", "agent",
            "--hermes-command", "/usr/bin/synthetic-hermes",
        ]

        # Missing '='
        err_buf = io.StringIO()
        with redirect_stderr(err_buf):
            with self.assertRaises(SystemExit) as cm:
                main(base_argv + ["--knowledge-root", "invalid_entry"])
            self.assertEqual(cm.exception.code, 2)
        self.assertIn("must be NAME=PATH", err_buf.getvalue())

        # Relative path
        err_buf = io.StringIO()
        with redirect_stderr(err_buf):
            with self.assertRaises(SystemExit) as cm:
                main(base_argv + ["--knowledge-root", "name=relative/path"])
            self.assertEqual(cm.exception.code, 2)
        self.assertIn("PATH must be absolute", err_buf.getvalue())

    def test_cli_read_only_command_parsing_and_validation(self) -> None:
        base_argv = [
            "--database", str(self.paths["database"]),
            "--cas-root", str(self.paths["cas_root"]),
            "--task-work-root", str(self.paths["task_work_root"]),
            "--scratch-root", str(self.paths["scratch_root"]),
            "--model", "synthetic-model",
            "--endpoint", "http://127.0.0.1:8800",
            "--synthesizer", "agent",
            "--hermes-command", "/usr/bin/synthetic-hermes",
        ]

        # Invalid JSON
        err_buf = io.StringIO()
        with redirect_stderr(err_buf):
            with self.assertRaises(SystemExit) as cm:
                main(base_argv + ["--read-only-command", "not json"])
            self.assertEqual(cm.exception.code, 2)
        self.assertIn("invalid JSON", err_buf.getvalue())

        # Reserved name
        err_buf = io.StringIO()
        with redirect_stderr(err_buf):
            with self.assertRaises(SystemExit) as cm:
                main(base_argv + ["--read-only-command", json.dumps({"name": "meeting", "command": "/srv/example/bin/meeting", "description": "desc"})])
            self.assertEqual(cm.exception.code, 2)
        self.assertIn("is reserved", err_buf.getvalue())

        # Relative path
        err_buf = io.StringIO()
        with redirect_stderr(err_buf):
            with self.assertRaises(SystemExit) as cm:
                main(base_argv + ["--read-only-command", json.dumps({"name": "calendar", "command": "relative/path", "description": "desc"})])
            self.assertEqual(cm.exception.code, 2)
        self.assertIn("command must be an absolute path", err_buf.getvalue())

        # Duplicate name
        err_buf = io.StringIO()
        with redirect_stderr(err_buf):
            with self.assertRaises(SystemExit) as cm:
                main(base_argv + [
                    "--read-only-command", json.dumps({"name": "cal", "command": "/srv/example/bin/cal", "description": "desc"}),
                    "--read-only-command", json.dumps({"name": "cal", "command": "/srv/example/bin/cal2", "description": "desc2"}),
                ])
            self.assertEqual(cm.exception.code, 2)
        self.assertIn("duplicate command name", err_buf.getvalue())


if __name__ == "__main__":
    unittest.main()
