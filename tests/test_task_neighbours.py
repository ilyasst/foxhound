#!/usr/bin/env python3
"""Tests for the task neighbours feature in execution_worker context."""

from __future__ import annotations

import hashlib
import json
import sqlite3
import tempfile
import threading
import unittest
from contextlib import closing, contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Iterator
from unittest import mock

from foxhound import migrate_database
from foxhound.agent_profiles import general_profile
from foxhound.execution_worker import (
    ExecutionWorker,
    MAX_NEIGHBOURS,
    WORK_CONTEXT_SCHEMA_VERSION,
    _gather_task_neighbours,
)
from foxhound.knowledge_client import KnowledgeClientConfig
from foxhound.task_execution import TaskExecutionService

CLAIM_TOKEN = "synthetic-claim-token-000000000000000000000000"
RUN_ID = "a" * 32
WORKER_COMMAND = "foxhound-task-worker"


@contextmanager
def knowledge_server() -> Iterator[str]:
    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):  # noqa: N802
            length = int(self.headers["Content-Length"])
            request = json.loads(self.rfile.read(length))
            if self.path == "/v1/execution-context":
                variables = {
                    "display_name": "Person A",
                    "operator_context": "Synthetic operator context.\n",
                    "self_aliases": ["Person A"],
                    "institution_domains": ["example.edu"],
                }
                canonical = json.dumps(
                    variables, ensure_ascii=True, separators=(",", ":"),
                    sort_keys=True,
                ).encode("utf-8")
                response = {
                    "schema": "gw.execution-context",
                    "schema_version": 1,
                    "ok": True,
                    "alias": request["alias"],
                    "revision": hashlib.sha256(canonical).hexdigest(),
                    "variables": variables,
                }
            elif self.path == "/v1/source-snapshot":
                response = {
                    "schema": "foxhound.source-snapshot",
                    "schema_version": 1,
                    "ok": True,
                    "system": request["system"],
                    "kind": request["kind"],
                    "record_id": request["record_id"],
                    "item_id": request["item_id"],
                    "expected_revision": request["expected_revision"],
                    "status": "current",
                    "snapshot": {
                        "revision": request["expected_revision"],
                        "observed_at": "2030-01-02T03:04:05+00:00",
                        "lifecycle": "active",
                        "actionability": "actionable",
                    },
                }
            elif self.path == "/v1/working-groups":
                response = {
                    "schema": "gw.working-groups",
                    "schema_version": 1,
                    "ok": True,
                    "matched_group": None,
                    "groups": [],
                }
            else:
                response = {
                    "schema": "gw.search",
                    "schema_version": 1,
                    "ok": True,
                    "query": request.get("query", ""),
                    "parameters": {},
                    "layers": [],
                }
            payload = json.dumps(response, separators=(",", ":")).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, format, *_args):  # noqa: A002
            return

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


NOW = "2030-01-02T03:04:05+00:00"


def _insert_task(
    conn: sqlite3.Connection,
    task_id: int,
    text: str = "Task",
    owner: str = "Person A",
    now: str = NOW,
) -> None:
    conn.execute(
        "INSERT INTO tasks(id,status,text,owner,version,"
        "created_at,updated_at) VALUES(?, 'open', ?, ?, 1, ?, ?)",
        (task_id, text, owner, now, now),
    )


def _bind_task_to_source(
    conn: sqlite3.Connection,
    task_id: int,
    candidate_id: str,
    source_system: str = "synthetic",
    source_kind: str = "meeting",
    source_record_id: str = "rec-001",
    source_item_id: str = "item-001",
    source_revision: str = "rev-001",
    now: str = NOW,
) -> None:
    """Create a candidate and bind it to a task as 'accepted'."""
    conn.execute(
        "INSERT OR IGNORE INTO candidate_inbox("
        "candidate_id,source_system,source_kind,source_record_id,"
        "source_item_id,source_revision,payload_json,"
        "created_at,first_imported_at,updated_at)"
        " VALUES(?,?,?,?,?,?,?,?,?,?)",
        (
            candidate_id, source_system, source_kind,
            source_record_id, source_item_id, source_revision,
            "{}", now, now, now,
        ),
    )
    conn.execute(
        "INSERT OR IGNORE INTO candidate_revision_history("
        "candidate_id,source_revision,payload_json,"
        "created_at,imported_at)"
        " VALUES(?,?,?,?,?)",
        (candidate_id, source_revision, "{}", now, now),
    )
    conn.execute(
        "INSERT INTO task_candidate_bindings("
        "candidate_id,source_revision,task_id,relation,decided_at)"
        " VALUES(?,?,?,?,?)",
        (candidate_id, source_revision, task_id, "accepted", now),
    )


def _insert_task_relation(
    conn: sqlite3.Connection,
    subject_id: int,
    object_id: int,
    now: str = NOW,
) -> None:
    conn.execute(
        "INSERT OR IGNORE INTO task_relations("
        "subject_id,object_id,kind,basis,asserted_by,created_at)"
        " VALUES(?,?,?,?,?,?)",
        (subject_id, object_id, "duplicate_of", "same topic", "machine", now),
    )


def _insert_duplicate_proposal(
    conn: sqlite3.Connection,
    task_a: int,
    task_b: int,
    state: str = "confirmed",
    now: str = NOW,
) -> None:
    left, right = sorted((task_a, task_b))
    # Settled proposals must have a non-NULL settled_at (schema CHECK constraint)
    settled = now if state in ("confirmed", "rejected", "superseded") else None
    conn.execute(
        "INSERT INTO task_duplicate_proposals("
        "left_task_id,right_task_id,left_task_version,right_task_version,"
        "basis,detector,state,created_at,updated_at,settled_at)"
        " VALUES(?,?,?,?,?,?,?,?,?,?)",
        (left, right, 1, 1, "same topic", "manual", state, now, now, settled),
    )


def _build_run_dir(
    tmpdir: Path,
    database: Path,
    claim_token: str,
    task_id: int = 1,
    task_version: int = 1,
    workflow_version: int = 1,
    phase: str = "plan",
    knowledge_root: Path | None = None,
) -> tuple[Path, dict]:
    """Create a minimal run-state directory and file."""
    run_dir = tmpdir / f"run-{RUN_ID}"
    run_dir.mkdir(mode=0o700, exist_ok=True)

    if knowledge_root is None:
        knowledge_root = tmpdir / "knowledge"
        knowledge_root.mkdir(exist_ok=True)

    profile = general_profile()
    (run_dir / "agent-instructions.json").write_text(
        json.dumps(profile.document()), encoding="utf-8",
    )

    document = {
        "schema": "foxhound.execution-run-state",
        "schema_version": 6,
        "run_id": RUN_ID,
        "database_path": str(database),
        "task_id": task_id,
        "task_version": task_version,
        "workflow_version": workflow_version,
        "phase": phase,
        "claim_token": claim_token,
        "lease_seconds": 3600,
        "agent_profile_id": profile.profile_id,
        "agent_profile_revision": profile.revision,
        "knowledge_root": str(knowledge_root),
        "worker_command": WORKER_COMMAND,
        "execution_grants": [],
        "action_grants": [],
        "deployment_roots": {},
        "task_work_directory": None,
        "task_kb_file": None,
        "task_run_directory": None,
    }
    state_path = run_dir / "run-state.json"
    state_path.write_text(json.dumps(document), encoding="utf-8")
    state_path.chmod(0o600)

    return run_dir, {"state_path": state_path, "knowledge_root": knowledge_root}


class TestGatherTaskNeighbours(unittest.TestCase):
    """Unit tests for _gather_task_neighbours directly."""

    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.root.chmod(0o700)
        self.database = self.root / "foxhound.sqlite3"
        migrate_database(self.database)

    def _setup_tasks(self, count: int = 3) -> list[int]:
        """Create tasks and return their IDs."""
        with closing(sqlite3.connect(self.database)) as conn:
            for i in range(1, count + 1):
                _insert_task(conn, i, f"Task {i}", f"Person {'A' if i % 2 else 'B'}")
            conn.commit()
        return list(range(1, count + 1))

    def test_empty_when_no_tasks(self) -> None:
        """No neighbours when there's only one task with no relations."""
        self._setup_tasks(1)
        result, truncated = _gather_task_neighbours(self.database, 1)
        self.assertEqual(result["items"], [])
        self.assertEqual(result["total_count"], 0)
        self.assertFalse(result["truncated"])

    def test_same_source_records_sibling_tasks(self) -> None:
        """Tasks sharing a source_record_id are neighbours."""
        self._setup_tasks(3)
        with closing(sqlite3.connect(self.database)) as conn:
            _bind_task_to_source(conn, 1, "cand-1", source_record_id="rec-same",
                                  source_item_id="item-1")
            _bind_task_to_source(conn, 2, "cand-2", source_record_id="rec-same",
                                  source_item_id="item-2")
            _bind_task_to_source(conn, 3, "cand-3", source_record_id="rec-diff",
                                  source_item_id="item-3")
            conn.commit()

        result, truncated = _gather_task_neighbours(self.database, 1)
        neighbour_ids = {n["id"] for n in result["items"]}
        self.assertIn(2, neighbour_ids)
        self.assertNotIn(3, neighbour_ids)
        self.assertEqual(result["total_count"], 1)
        self.assertFalse(result["truncated"])
        for n in result["items"]:
            if n["id"] == 2:
                self.assertEqual(n["selection"], "same_source")

    def test_task_relation_records_neighbours(self) -> None:
        """Live task_relations create assessed_similar neighbours."""
        self._setup_tasks(3)
        with closing(sqlite3.connect(self.database)) as conn:
            _insert_task_relation(conn, 1, 2)
            conn.commit()

        result, truncated = _gather_task_neighbours(self.database, 1)
        neighbour_ids = {n["id"] for n in result["items"]}
        self.assertIn(2, neighbour_ids)
        self.assertNotIn(3, neighbour_ids)
        self.assertEqual(result["total_count"], 1)
        for n in result["items"]:
            if n["id"] == 2:
                self.assertEqual(n["selection"], "assessed_similar")

    def test_withdrawn_relation_excluded(self) -> None:
        """Withdrawn relations do not appear as neighbours."""
        self._setup_tasks(2)
        with closing(sqlite3.connect(self.database)) as conn:
            _insert_task_relation(conn, 1, 2)
            conn.execute(
                "UPDATE task_relations SET withdrawn_at=?, withdrawn_by=? "
                "WHERE subject_id=1",
                (NOW, "machine"),
            )
            conn.commit()

        result, _ = _gather_task_neighbours(self.database, 1)
        self.assertEqual(result["items"], [])

    def test_duplicate_proposal_records_neighbours(self) -> None:
        """Confirmed/proposed duplicates create assessed_similar neighbours."""
        self._setup_tasks(3)
        with closing(sqlite3.connect(self.database)) as conn:
            _insert_duplicate_proposal(conn, 1, 2, state="confirmed")
            conn.commit()

        result, truncated = _gather_task_neighbours(self.database, 1)
        neighbour_ids = {n["id"] for n in result["items"]}
        self.assertIn(2, neighbour_ids)
        self.assertNotIn(3, neighbour_ids)
        self.assertEqual(result["total_count"], 1)

    def test_rejected_proposal_excluded(self) -> None:
        """Rejected duplicate proposals do not appear."""
        self._setup_tasks(2)
        with closing(sqlite3.connect(self.database)) as conn:
            _insert_duplicate_proposal(conn, 1, 2, state="rejected")
            conn.commit()

        result, _ = _gather_task_neighbours(self.database, 1)
        self.assertEqual(result["items"], [])

    def test_own_task_excluded(self) -> None:
        """A task is never its own neighbour."""
        self._setup_tasks(1)
        with closing(sqlite3.connect(self.database)) as conn:
            _bind_task_to_source(conn, 1, "cand-1", source_item_id="item-1")
            conn.commit()

        result, _ = _gather_task_neighbours(self.database, 1)
        self.assertEqual(result["items"], [])

    def test_limit_enforced(self) -> None:
        """Only MAX_NEIGHBOURS items are returned when more exist."""
        num_relations = MAX_NEIGHBOURS + 2
        self._setup_tasks(MAX_NEIGHBOURS + 3)
        with closing(sqlite3.connect(self.database)) as conn:
            for tid in range(2, num_relations + 2):
                _insert_task_relation(conn, 1, tid)
            conn.commit()

        result, truncated = _gather_task_neighbours(self.database, 1)
        self.assertLessEqual(len(result["items"]), MAX_NEIGHBOURS)
        self.assertTrue(result["truncated"])
        self.assertEqual(result["total_count"], num_relations)

    def test_custom_limit(self) -> None:
        """A custom limit overrides MAX_NEIGHBOURS."""
        self._setup_tasks(5)
        with closing(sqlite3.connect(self.database)) as conn:
            for tid in range(2, 6):
                _insert_task_relation(conn, 1, tid)
            conn.commit()

        result, truncated = _gather_task_neighbours(self.database, 1, limit=2)
        self.assertEqual(len(result["items"]), 2)
        self.assertTrue(result["truncated"])
        self.assertEqual(result["total_count"], 4)

    def test_deduplication_across_rules(self) -> None:
        """A task appearing in both rules is returned only once."""
        self._setup_tasks(2)
        with closing(sqlite3.connect(self.database)) as conn:
            _bind_task_to_source(conn, 1, "cand-1", source_record_id="rec-x",
                                  source_item_id="item-1")
            _bind_task_to_source(conn, 2, "cand-2", source_record_id="rec-x",
                                  source_item_id="item-2")
            _insert_task_relation(conn, 1, 2)
            conn.commit()

        result, _ = _gather_task_neighbours(self.database, 1)
        self.assertEqual(len(result["items"]), 1)
        self.assertEqual(result["total_count"], 1)

    def test_neighbour_has_required_fields(self) -> None:
        """Each neighbour record has id, text, status, owner, selection."""
        self._setup_tasks(2)
        with closing(sqlite3.connect(self.database)) as conn:
            _insert_task_relation(conn, 1, 2)
            conn.commit()

        result, _ = _gather_task_neighbours(self.database, 1)
        for n in result["items"]:
            self.assertIn("id", n)
            self.assertIn("text", n)
            self.assertIn("status", n)
            self.assertIn("owner", n)
            self.assertIn("selection", n)
            self.assertIn(n["selection"], ("same_source", "assessed_similar"))


class TestContextIncludesNeighbours(unittest.TestCase):
    """Integration tests: neighbours appear in the full worker context."""

    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.root.chmod(0o700)
        self.database = self.root / "foxhound.sqlite3"
        migrate_database(self.database)
        # Seed task 1
        with closing(sqlite3.connect(self.database)) as conn:
            conn.execute(
                "INSERT INTO tasks(id,status,text,owner,version,"
                "created_at,updated_at) VALUES(1, 'open', 'Main task', "
                "'Person A', 1, ?, ?)",
                (NOW, NOW),
            )
            conn.commit()
        # Create execution claim
        service = TaskExecutionService(
            self.database, token_factory=lambda: CLAIM_TOKEN
        )
        service.schedule(1, expected_task_version=1)
        service.start_action(1, expected_version=1, action="start")
        self.claim = service.claim_next()
        assert self.claim is not None
        # Build run directory
        run_dir, self.run_info = _build_run_dir(
            self.root, self.database, CLAIM_TOKEN,
            workflow_version=self.claim.workflow_version,
        )
        self.addCleanup(self.run_info["state_path"].unlink)

    def _worker(self, endpoint: str) -> ExecutionWorker:
        return ExecutionWorker(
            self.run_info["state_path"],
            KnowledgeClientConfig(
                endpoint=endpoint,
                alias="synthetic-alias",
                token="synthetic-knowledge-token-with-sufficient-length",
            ),
        )

    def test_context_has_neighbours_section(self) -> None:
        """Context always contains the neighbours section."""
        with knowledge_server() as endpoint:
            with mock.patch(
                "foxhound.execution_worker._local_today",
                return_value="2030-01-02",
            ), mock.patch(
                "foxhound.execution_worker.TaskLedger.source_snapshot_request",
                return_value=None,
            ):
                worker = self._worker(endpoint)
                context = worker.context()

        self.assertIn("neighbours", context)
        neighbours = context["neighbours"]
        self.assertIn("items", neighbours)
        self.assertIn("total_count", neighbours)
        self.assertIn("truncated", neighbours)
        self.assertEqual(neighbours["items"], [])
        self.assertEqual(neighbours["total_count"], 0)
        self.assertFalse(neighbours["truncated"])

    def test_context_schema_version_bumped(self) -> None:
        """Schema version reflects the neighbours addition."""
        with knowledge_server() as endpoint:
            with mock.patch(
                "foxhound.execution_worker._local_today",
                return_value="2030-01-02",
            ), mock.patch(
                "foxhound.execution_worker.TaskLedger.source_snapshot_request",
                return_value=None,
            ):
                worker = self._worker(endpoint)
                context = worker.context()

        self.assertEqual(context["schema_version"], WORK_CONTEXT_SCHEMA_VERSION)
        self.assertEqual(WORK_CONTEXT_SCHEMA_VERSION, 9)

    def test_context_populates_same_source_neighbours(self) -> None:
        """Neighbours from same source appear in context."""
        with closing(sqlite3.connect(self.database)) as conn:
            conn.execute(
                "INSERT INTO tasks(id,status,text,owner,version,"
                "created_at,updated_at) VALUES(2, 'open', 'Sibling task', "
                "'Person A', 1, ?, ?)",
                (NOW, NOW),
            )
            _bind_task_to_source(conn, 1, "cand-1", source_record_id="meeting-A",
                                  source_item_id="item-1")
            _bind_task_to_source(conn, 2, "cand-2", source_record_id="meeting-A",
                                  source_item_id="item-2")
            conn.commit()

        with knowledge_server() as endpoint:
            with mock.patch(
                "foxhound.execution_worker._local_today",
                return_value="2030-01-02",
            ), mock.patch(
                "foxhound.execution_worker.TaskLedger.source_snapshot_request",
                return_value=None,
            ):
                worker = self._worker(endpoint)
                context = worker.context()

        neighbours = context["neighbours"]
        self.assertEqual(len(neighbours["items"]), 1)
        self.assertEqual(neighbours["items"][0]["id"], 2)
        self.assertEqual(neighbours["items"][0]["text"], "Sibling task")
        self.assertEqual(neighbours["items"][0]["selection"], "same_source")

    def test_context_no_sensitive_data_leak(self) -> None:
        """Neighbours do not expose tokens or database paths."""
        with closing(sqlite3.connect(self.database)) as conn:
            conn.execute(
                "INSERT INTO tasks(id,status,text,owner,version,"
                "created_at,updated_at) VALUES(2, 'open', 'Neighbour', "
                "'Person A', 1, ?, ?)",
                (NOW, NOW),
            )
            _insert_task_relation(conn, 1, 2)
            conn.commit()

        with knowledge_server() as endpoint:
            with mock.patch(
                "foxhound.execution_worker._local_today",
                return_value="2030-01-02",
            ), mock.patch(
                "foxhound.execution_worker.TaskLedger.source_snapshot_request",
                return_value=None,
            ):
                worker = self._worker(endpoint)
                context = worker.context()

        rendered = json.dumps(context)
        self.assertNotIn(CLAIM_TOKEN, rendered)
        self.assertNotIn(str(self.database), rendered)


if __name__ == "__main__":
    unittest.main()
