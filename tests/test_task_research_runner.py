"""Synthetic tests for the one-shot Researcher runner."""

from __future__ import annotations

import hashlib
import io
import json
import os
import sqlite3
import tempfile
import unittest
from contextlib import closing, redirect_stdout
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock

from foxhound import migrate_database
from foxhound.knowledge_client import KnowledgeDocument, KnowledgeLayer, KnowledgeSearchResult
from foxhound.task_research import INPUT_SCHEMA, ResearchStore
from foxhound.task_research_runner import main, run_once
from foxhound.task_research_synthesis import SynthesisError


NOW = datetime(2032, 5, 6, 7, 8, 9, tzinfo=timezone.utc)


class _Knowledge:
    def __init__(self, documents: tuple[KnowledgeDocument, ...] | None = None) -> None:
        self.documents = documents if documents is not None else (
            KnowledgeDocument(
                id="kb:Synthetic-Doc.md",
                path="Synthetic-Doc.md",
                kb_path="Synthetic-Doc.md",
                section="prerequisite",
                excerpt="Synthetic excerpt content.",
                date="2032-05-01",
            ),
        )

    def search(self, _query: str, **_options: object) -> KnowledgeSearchResult:
        return KnowledgeSearchResult((
            KnowledgeLayer("kb", len(self.documents), False, self.documents),
            KnowledgeLayer("secondary", 0, False, ()),
            KnowledgeLayer("emails", 0, False, ()),
        ))


class _Opener:
    def __init__(self, draft: dict[str, object]) -> None:
        self.draft = draft

    def open(self, _request: object, _timeout: float | None = None, **_kwargs: object):
        envelope = {
            "choices": [{"message": {"content": json.dumps(self.draft)}}],
            "usage": {"prompt_tokens": 40, "completion_tokens": 20},
            "reasoning_effective": "high",
        }
        payload = json.dumps(envelope).encode("utf-8")

        class _Response:
            def __enter__(self):
                return self

            def __exit__(self, *_args: object) -> bool:
                return False

            def read(self, amount: int = -1) -> bytes:
                return payload if amount < 0 else payload[:amount]

        return _Response()


def _claim(text: str, *, unknown: bool = False) -> dict[str, object]:
    return {
        "text": text,
        "status": "unknown" if unknown else "supported",
        "source_refs": [] if unknown else ["src-001"],
    }


def _draft() -> dict[str, object]:
    return {
        "schema_version": "foxhound.task-research-draft.v1",
        "research_status": "sufficient",
        "objective": _claim("Synthetic objective for test run."),
        "requested_action": _claim("Execute synthetic action."),
        "current_state": [_claim("Current state claim.")],
        "expected_deliverables": [_claim("Deliverable brief.")],
        "timeline": [],
        "decisions": [],
        "dependencies": [],
        "constraints": [],
        "stakeholders": [],
        "related_entities": [],
        "findings": [_claim("Synthetic finding.")],
        "conflicts": [],
        "open_questions": [],
        "scheduling_recommendations": [],
    }


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


class TaskResearchRunnerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.root.chmod(0o700)
        self.paths = _setup_test_env(self.root)

    def test_empty_queue_exits_successfully_with_idle_status(self) -> None:
        result = run_once(
            database=self.paths["database"],
            cas_root=self.paths["cas_root"],
            task_work_root=self.paths["task_work_root"],
            scratch_root=self.paths["scratch_root"],
            model="synthetic-model",
            endpoint="http://127.0.0.1:8800",
            knowledge_override=_Knowledge(),
            opener=_Opener(_draft()),
            clock=lambda: NOW,
        )
        self.assertFalse(result.claimed)
        self.assertFalse(result.completed)
        self.assertEqual(result.state, "idle")

    def test_successful_synthesis_and_publish(self) -> None:
        task_folder = _queue_job(self.paths)
        result = run_once(
            database=self.paths["database"],
            cas_root=self.paths["cas_root"],
            task_work_root=self.paths["task_work_root"],
            scratch_root=self.paths["scratch_root"],
            model="synthetic-model",
            endpoint="http://127.0.0.1:8800",
            knowledge_override=_Knowledge(),
            opener=_Opener(_draft()),
            clock=lambda: NOW,
        )
        self.assertTrue(result.claimed)
        self.assertTrue(result.completed)
        self.assertEqual(result.state, "completed")

        # Exact receipt-backed files appear in synthetic task folder
        json_file = task_folder / ".task-research.json"
        md_file = task_folder / "Research.md"
        self.assertTrue(json_file.is_file())
        self.assertTrue(md_file.is_file())

        with closing(sqlite3.connect(self.paths["database"])) as connection:
            receipt = connection.execute(
                "SELECT r.json_digest, r.markdown_digest FROM task_research_receipts r "
                "JOIN task_research_jobs j ON j.job_id=r.job_id WHERE j.task_id=1"
            ).fetchone()
            job = connection.execute("SELECT state FROM task_research_jobs WHERE task_id=1").fetchone()
        self.assertIsNotNone(receipt)
        self.assertEqual(job[0], "completed")

        # Transient scratch directories removed
        scratch_items = [d for d in self.paths["scratch_root"].iterdir() if d.name != "metrics"]
        self.assertEqual(scratch_items, [])

    def test_interrupted_publication_is_repaired_without_repeating_model_work(self) -> None:
        task_folder = _queue_job(self.paths)
        original_install = ResearchStore._install
        interrupted = False

        def interrupt_once(store: ResearchStore, path: Path, payload: bytes) -> None:
            nonlocal interrupted
            if not interrupted and path.parent == task_folder:
                interrupted = True
                raise OSError("synthetic interruption")
            original_install(path, payload)

        with mock.patch.object(ResearchStore, "_install", interrupt_once):
            result = run_once(
                database=self.paths["database"],
                cas_root=self.paths["cas_root"],
                task_work_root=self.paths["task_work_root"],
                scratch_root=self.paths["scratch_root"],
                model="synthetic-model",
                endpoint="http://127.0.0.1:8800",
                knowledge_override=_Knowledge(),
                opener=_Opener(_draft()),
                clock=lambda: NOW,
            )

        self.assertTrue(interrupted)
        self.assertTrue(result.claimed)
        self.assertTrue(result.completed)
        self.assertEqual(result.state, "completed")
        self.assertIsNone(result.failure_code)
        self.assertTrue((task_folder / ".task-research.json").is_file())
        self.assertTrue((task_folder / "Research.md").is_file())

    def test_second_runner_cannot_claim_same_job(self) -> None:
        _queue_job(self.paths)
        # First runner claims and runs
        res1 = run_once(
            database=self.paths["database"],
            cas_root=self.paths["cas_root"],
            task_work_root=self.paths["task_work_root"],
            scratch_root=self.paths["scratch_root"],
            model="synthetic-model",
            endpoint="http://127.0.0.1:8800",
            knowledge_override=_Knowledge(),
            opener=_Opener(_draft()),
            clock=lambda: NOW,
        )
        self.assertTrue(res1.claimed)
        self.assertTrue(res1.completed)

        # Second runner tries to claim
        res2 = run_once(
            database=self.paths["database"],
            cas_root=self.paths["cas_root"],
            task_work_root=self.paths["task_work_root"],
            scratch_root=self.paths["scratch_root"],
            model="synthetic-model",
            endpoint="http://127.0.0.1:8800",
            knowledge_override=_Knowledge(),
            opener=_Opener(_draft()),
            clock=lambda: NOW,
        )
        self.assertFalse(res2.claimed)
        self.assertFalse(res2.completed)
        self.assertEqual(res2.state, "idle")

    def test_runner_claim_is_scoped_to_configured_task_root(self) -> None:
        _queue_job(self.paths)
        other_root = self.root / "other-tasks"
        other_root.mkdir(mode=0o700)

        result = run_once(
            database=self.paths["database"],
            cas_root=self.paths["cas_root"],
            task_work_root=other_root,
            scratch_root=self.paths["scratch_root"],
            model="synthetic-model",
            endpoint="http://127.0.0.1:8800",
            knowledge_override=_Knowledge(),
            opener=_Opener(_draft()),
            clock=lambda: NOW,
        )

        self.assertFalse(result.claimed)
        with closing(sqlite3.connect(self.paths["database"])) as connection:
            state = connection.execute(
                "SELECT state FROM task_research_jobs WHERE task_id=1"
            ).fetchone()[0]
        self.assertEqual(state, "queued")

    def test_fixed_failure_records_retry_then_park(self) -> None:
        _queue_job(self.paths)
        empty_knowledge = _Knowledge(documents=())

        # Attempt 1: synthesis fails with retrieval_empty
        res1 = run_once(
            database=self.paths["database"],
            cas_root=self.paths["cas_root"],
            task_work_root=self.paths["task_work_root"],
            scratch_root=self.paths["scratch_root"],
            model="synthetic-model",
            endpoint="http://127.0.0.1:8800",
            knowledge_override=empty_knowledge,
            opener=_Opener(_draft()),
            clock=lambda: NOW,
        )
        self.assertTrue(res1.claimed)
        self.assertFalse(res1.completed)
        self.assertEqual(res1.failure_code, "retrieval_empty")
        self.assertEqual(res1.state, "queued")

        with closing(sqlite3.connect(self.paths["database"])) as connection:
            job = connection.execute(
                "SELECT state, attempts, failure_code FROM task_research_jobs WHERE task_id=1"
            ).fetchone()
        self.assertEqual(job[0], "queued")
        self.assertEqual(job[1], 1)
        self.assertEqual(job[2], "retrieval_empty")

        # Attempt 2
        res2 = run_once(
            database=self.paths["database"],
            cas_root=self.paths["cas_root"],
            task_work_root=self.paths["task_work_root"],
            scratch_root=self.paths["scratch_root"],
            model="synthetic-model",
            endpoint="http://127.0.0.1:8800",
            knowledge_override=empty_knowledge,
            opener=_Opener(_draft()),
            clock=lambda: NOW,
        )
        self.assertEqual(res2.state, "queued")

        # Attempt 3: hits max_attempts (3), transitions to parked
        res3 = run_once(
            database=self.paths["database"],
            cas_root=self.paths["cas_root"],
            task_work_root=self.paths["task_work_root"],
            scratch_root=self.paths["scratch_root"],
            model="synthetic-model",
            endpoint="http://127.0.0.1:8800",
            knowledge_override=empty_knowledge,
            opener=_Opener(_draft()),
            clock=lambda: NOW,
        )
        self.assertTrue(res3.claimed)
        self.assertFalse(res3.completed)
        self.assertEqual(res3.state, "parked")

        with closing(sqlite3.connect(self.paths["database"])) as connection:
            job = connection.execute(
                "SELECT state, attempts, failure_code FROM task_research_jobs WHERE task_id=1"
            ).fetchone()
        self.assertEqual(job[0], "parked")
        self.assertEqual(job[1], 3)
        self.assertEqual(job[2], "retrieval_empty")

        # Failed scratch preserved for diagnostics
        failed_dirs = [d for d in self.paths["scratch_root"].iterdir() if d.name != "metrics"]
        self.assertTrue(all(d.name.startswith("failed-") for d in failed_dirs))
        self.assertEqual(len(failed_dirs), 1)

    def test_stale_task_version_fails_closed(self) -> None:
        _queue_job(self.paths)
        # Advance task version in the tasks table before runner synthesizes/publishes
        with closing(sqlite3.connect(self.paths["database"])) as connection:
            connection.execute("UPDATE tasks SET version=2 WHERE id=1")
            connection.commit()

        result = run_once(
            database=self.paths["database"],
            cas_root=self.paths["cas_root"],
            task_work_root=self.paths["task_work_root"],
            scratch_root=self.paths["scratch_root"],
            model="synthetic-model",
            endpoint="http://127.0.0.1:8800",
            knowledge_override=_Knowledge(),
            opener=_Opener(_draft()),
            clock=lambda: NOW,
        )
        self.assertFalse(result.claimed)
        self.assertFalse(result.completed)

        with closing(sqlite3.connect(self.paths["database"])) as connection:
            job = connection.execute(
                "SELECT state, attempts, failure_code FROM task_research_jobs WHERE task_id=1"
            ).fetchone()
        self.assertEqual(job[0], "canceled")
        self.assertEqual(job[2], "superseded")

    def test_unsafe_scratch_path_rejection(self) -> None:
        _queue_job(self.paths)
        # Group-writable scratch root
        unsafe_scratch = self.root / "unsafe_scratch"
        unsafe_scratch.mkdir(mode=0o777)

        buf = io.StringIO()
        with redirect_stdout(buf):
            code = main([
                "--database", str(self.paths["database"]),
                "--cas-root", str(self.paths["cas_root"]),
                "--task-work-root", str(self.paths["task_work_root"]),
                "--scratch-root", str(unsafe_scratch),
                "--model", "synthetic-model",
                "--endpoint", "http://127.0.0.1:8800",
            ])
        self.assertEqual(code, 78)
        output = json.loads(buf.getvalue())
        self.assertFalse(output["accepted"])
        self.assertEqual(output["error_code"], "configuration_unavailable")

    def test_cli_output_contains_no_private_fields(self) -> None:
        private_text = "Highly sensitive private task body 12345"
        with closing(sqlite3.connect(self.paths["database"])) as connection:
            connection.execute(
                "UPDATE tasks SET text=? WHERE id=1", (private_text,)
            )
            connection.commit()
        _queue_job(self.paths, text=private_text)
        buf = io.StringIO()
        with redirect_stdout(buf):
            with mock.patch("foxhound.task_research_runner.synthesize") as mock_synth, \
                    mock.patch(
                        "foxhound.task_research_runner.load_knowledge_config",
                        return_value=object(),
                    ), mock.patch(
                        "foxhound.task_research_runner.GwKnowledgeClient",
                        return_value=_Knowledge(),
                    ):
                from foxhound.task_research_synthesis import SynthesisResult
                mock_synth.return_value = SynthesisResult(
                    draft=_draft(),
                    sources=({
                        "source_id": "src-001",
                        "locator": {
                            "namespace": "kb",
                            "resource": "Synthetic-Doc.md",
                            "fragment": None,
                        },
                        "content_digest": hashlib.sha256(
                            b"Synthetic excerpt content."
                        ).hexdigest(),
                        "title": "Synthetic document",
                    },),
                    coverage={"searched_namespaces": ["kb"], "queries": 1, "documents_retrieved": 1, "unavailable_source_ids": [], "knowledge_revisions": {}},
                    provenance={"profile_id": "researcher", "profile_revision": "0" * 64, "model": "m", "provider": "local", "runtime": "test", "reasoning_requested": "high", "reasoning_effective": "high"},
                    metrics={"searches": 1, "documents": 0, "latency_ms": 10, "prompt_tokens": 10, "completion_tokens": 10},
                )
                code = main([
                    "--database", str(self.paths["database"]),
                    "--cas-root", str(self.paths["cas_root"]),
                    "--task-work-root", str(self.paths["task_work_root"]),
                    "--scratch-root", str(self.paths["scratch_root"]),
                    "--model", "synthetic-model",
                    "--endpoint", "http://127.0.0.1:8800",
                    "--gw-endpoint", "http://127.0.0.1:8789",
                    "--gw-alias", "synthetic",
                    "--gw-token-file", str(self.root / "synthetic-token"),
                ])

        raw_output = buf.getvalue()
        self.assertEqual(code, 0)
        output = json.loads(raw_output)
        self.assertTrue(output["accepted"])
        self.assertTrue(output["claimed"])
        self.assertTrue(output["completed"], output)

        # Ensure no private text or token is in output
        self.assertNotIn("sensitive", raw_output)
        self.assertNotIn("12345", raw_output)
        self.assertNotIn("claim_token", raw_output)
        self.assertNotIn("token", raw_output)
        self.assertNotIn("draft", raw_output)
        self.assertNotIn("evidence", raw_output)


if __name__ == "__main__":
    unittest.main()


class OwnerPrivateRootTests(unittest.TestCase):
    """Only the root must be private; ancestors must not be world-writable."""

    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.base = Path(temporary.name).resolve()

    def _chain(self, *modes):
        path = self.base
        for index, mode in enumerate(modes):
            path = path / f"level-{index}"
            path.mkdir()
            path.chmod(mode)
        return path

    def test_ordinary_home_and_synced_ancestors_are_accepted(self):
        from foxhound.task_research_runner import _validate_owner_private_dir
        root = self._chain(0o701, 0o775, 0o700)
        self.assertEqual(_validate_owner_private_dir(root), root)

    def test_world_writable_ancestor_is_refused(self):
        from foxhound.task_research import ResearchError
        from foxhound.task_research_runner import _validate_owner_private_dir
        root = self._chain(0o777, 0o700)
        with self.assertRaisesRegex(ResearchError, "writable by others"):
            _validate_owner_private_dir(root)

    def test_root_must_still_be_private(self):
        from foxhound.task_research import ResearchError
        from foxhound.task_research_runner import _validate_owner_private_dir
        root = self._chain(0o755, 0o750)
        with self.assertRaisesRegex(ResearchError, "not owner-private"):
            _validate_owner_private_dir(root)
