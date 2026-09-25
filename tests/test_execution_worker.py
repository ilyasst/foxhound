#!/usr/bin/env python3
"""Synthetic tests for the narrow execution-agent worker boundary."""

from __future__ import annotations

from foxhound import execution_worker
from foxhound import migrate_database

import hashlib
import json
import os
import sqlite3
import tempfile
import types
import threading
import unittest
from contextlib import closing, contextmanager, redirect_stderr, redirect_stdout
from dataclasses import replace
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from io import StringIO
from pathlib import Path
from types import SimpleNamespace
from typing import Iterator
from unittest import mock

from foxhound.agent_profiles import general_profile
from foxhound.candidate_inbox import CandidateInbox
from foxhound.contracts import SourceSnapshotContractError
from foxhound.execution_worker import (
    INSTRUCTIONS_NAME,
    ExecutionWorker,
    ExecutionWorkerClaimError,
    ExecutionWorkerConfigError,
    ExecutionWorkerDraftError,
    load_result_draft,
    load_run_state,
    main,
    _local_calendar,
    _append_repository_receipt,
    _publication_is_the_deliverable,
    _repository_result,
    _repository_receipts,
    _read_handoff,
    _worker_operations,
)
from foxhound.knowledge_client import KnowledgeClientConfig, KnowledgeClientError
from foxhound.workflow_policy import parse_workflow_policy
from foxhound.execution_worker import _workflow_policy
from foxhound.task_execution import (
    ExecutionResultEnvelope,
    TaskExecutionService,
    WorkflowPhase,
    WorkflowStatus,
)
from foxhound.task_archive import prepare_task_archive


TOKEN = "synthetic-knowledge-token-with-sufficient-length"
CLAIM_TOKEN = "synthetic-claim-token-000000000000000000000000"
RUN_ID = "a" * 32
RESULT_ID = "b" * 32
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
                    "schema": "foxhound.source-snapshot", "schema_version": 1,
                    "ok": True, "system": request["system"],
                    "kind": request["kind"], "record_id": request["record_id"],
                    "item_id": request["item_id"],
                    "expected_revision": request["expected_revision"],
                    "status": "current", "snapshot": {
                        "revision": request["expected_revision"],
                        "observed_at": "2030-01-02T03:04:05+00:00",
                        "lifecycle": "active", "actionability": "actionable",
                    },
                }
            elif self.path == "/v1/working-groups":
                response = {
                    "schema": "gw.working-groups",
                    "schema_version": 1,
                    "ok": True,
                    "alias": request["alias"],
                    "total_groups": 1,
                    "working_groups": [
                        {
                            "cluster_id": 0,
                            "name": "Person A, Person B · Composite, Testing",
                            "dominant_people": ["Person A", "Person B"],
                            "keywords": ["composite", "testing"],
                            "member_count": 2,
                            "members": ["Person A", "Person B"],
                            "recent_artifacts": [],
                        }
                    ],
                    "matched_group": {
                        "cluster_id": 0,
                        "name": "Person A, Person B · Composite, Testing",
                        "dominant_people": ["Person A", "Person B"],
                        "keywords": ["composite", "testing"],
                        "member_count": 2,
                        "members": ["Person A", "Person B"],
                        "recent_artifacts": [],
                    },
                }
            else:
                response = {
                    "schema": "gw.search",
                    "schema_version": 1,
                    "ok": True,
                    "query": request["query"],
                    "parameters": {
                        "layers": request["layers"],
                        "context_lines": request["context_lines"],
                        "max_matches_per_document":
                            request["max_matches_per_document"],
                        "max_results_per_layer":
                            request["max_results_per_layer"],
                        "ranking": "hybrid_rrf",
                    },
                    "layers": [{
                        "name": "kb",
                        "total_results": 1,
                        "returned_results": 1,
                        "truncated": False,
                        "documents": [{
                            "id": "kb:Projects/alpha.md",
                            "path": "Projects/alpha.md",
                            "kb_path": "Projects/alpha.md",
                            "excerpt": "Synthetic evidence.",
                            "section": "Summary",
                            "ranking": {"method": "rrf", "score": 0.25},
                        }],
                    }],
                }
            payload = json.dumps(response, separators=(",", ":")).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, _format, *_args):
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


def _policy(freshness="before_effect", kinds=("issue",)):
    """One synthetic policy, fencing the named kinds at the named strictness."""
    return parse_workflow_policy({
        "policy_id": "synthetic-fence", "grants": {
            "plan": [], "execute": [], "external_action": [],
        }, "freshness": freshness, "freshness_kinds": list(kinds),
        "effects": [], "final_decision": True,
    })


class ExecutionWorkerTests(unittest.TestCase):
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
        service = TaskExecutionService(
            self.database, token_factory=lambda: CLAIM_TOKEN
        )
        scheduled = service.schedule(1, expected_task_version=1)
        service.start_action(
            1, expected_version=scheduled.version, action="start"
        )
        self.claim = service.claim_next()
        self.assertIsNotNone(self.claim)
        self.run_directory = self.root / f"run-{RUN_ID}"
        self.knowledge_root = self.root / "knowledge"
        self.knowledge_root.mkdir()
        self.run_directory.mkdir(mode=0o700)
        self.state_path = self.run_directory / "run-state.json"
        self._write_state()
        self._write_instructions()

    def _write_state(
        self,
        *,
        schema_version: int = 3,
        execution_grants: tuple[str, ...] = (),
        action_grants: tuple[str, ...] = (),
        deployment_roots: dict[str, str] | None = None,
    ) -> None:
        document = {
            "schema": "foxhound.execution-run-state",
            "schema_version": schema_version,
            "run_id": RUN_ID,
            "database_path": str(self.database),
            "task_id": 1,
            "task_version": 1,
            "workflow_version": self.claim.workflow_version,
            "phase": self.claim.phase.value,
            "claim_token": CLAIM_TOKEN,
            "lease_seconds": self.claim.lease_seconds,
            "agent_profile_id": self.claim.agent_profile_id,
            "agent_profile_revision": self.claim.agent_profile_revision,
            "knowledge_root": str(self.knowledge_root),
            "worker_command": WORKER_COMMAND,
        }
        if schema_version >= 4:
            document.update({
                "task_work_directory": None,
                "task_kb_file": None,
                "task_run_directory": None,
            })
        if schema_version >= 5:
            document.update({
                "execution_grants": list(execution_grants),
                "action_grants": list(action_grants),
            })
        if schema_version >= 6:
            document["deployment_roots"] = deployment_roots or {}
        self.state_path.write_text(json.dumps(document), encoding="utf-8")
        self.state_path.chmod(0o600)

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

    def _write_instructions(self, document: object | None = None) -> Path:
        path = self.run_directory / INSTRUCTIONS_NAME
        if document is None:
            document = general_profile().document()
        path.write_text(json.dumps(document), encoding="utf-8")
        path.chmod(0o600)
        return path

    def _worker(self, endpoint: str, **changes) -> ExecutionWorker:
        return ExecutionWorker(self.state_path, KnowledgeClientConfig(
            endpoint=endpoint, alias="primary", token=TOKEN
        ), **changes)

    def _bind_fresh_origin(
        self, task_id: int, *, kind: str, item_id: str,
    ) -> None:
        """Bind one synthetic forge origin for an effect-fence test."""
        now = "2030-01-02T03:04:05+00:00"
        candidate_id = f"fresh-{task_id}"
        with closing(sqlite3.connect(self.database)) as connection:
            if task_id != 1:
                connection.execute(
                    "INSERT INTO tasks(id,status,text,owner,due,version,"
                    "created_at,updated_at,closed_at) VALUES(?,'open',"
                    "'Synthetic task',NULL,NULL,1,?,?,NULL)",
                    (task_id, now, now),
                )
            connection.execute(
                "INSERT INTO candidate_inbox(candidate_id,source_system,"
                "source_kind,source_record_id,source_item_id,source_revision,"
                "payload_json,created_at,first_imported_at,updated_at) "
                "VALUES(?,'gw',?,'github.com/example-org/example-repo',?,?,'{}',?,?,?)",
                (candidate_id, kind, item_id, "b" * 64, now, now, now),
            )
            connection.execute(
                "INSERT INTO task_candidate_bindings(candidate_id,"
                "source_revision,task_id,relation,decided_at) "
                "VALUES(?,?,?,'accepted',?)",
                (candidate_id, "b" * 64, task_id, now),
            )
            connection.commit()

    def _effect_worker(
        self, endpoint: str, *, task_id: int, kind: str, item_id: str,
    ) -> tuple[ExecutionWorker, mock._patch]:
        self._bind_fresh_origin(task_id, kind=kind, item_id=item_id)
        worker = self._worker(endpoint, policy=_policy(kinds=(kind,)))
        state = replace(
            load_run_state(self.state_path), task_id=task_id,
            phase=WorkflowPhase.EXTERNAL_ACTION,
        )
        return worker, mock.patch.object(
            worker, "_active", return_value=(state, SimpleNamespace()),
        )

    def _write_effect_body(self, name: str) -> str:
        path = self.run_directory / name
        path.write_text("Synthetic approved effect body.\n", encoding="utf-8")
        path.chmod(0o600)
        return path.name

    def _write_draft(self, **changes) -> Path:
        document = {
            "schema": "foxhound.execution-result-draft",
            "schema_version": 1,
            "result_id": RESULT_ID,
            "outcome": "awaiting_plan",
            "summary": "Synthetic result summary",
            "work_markdown": "# Synthetic work\n\nNo private evidence.",
            "questions": ["Should Example A proceed?"],
            "external_actions": ["Prepare a synthetic draft."],
            "deliverables": ["Synthetic deliverable"],
            "repository_references": [],
            "repository_impact": True,
        }
        document.update(changes)
        path = self.run_directory / f"result-{RESULT_ID}.json"
        path.write_text(json.dumps(document), encoding="utf-8")
        path.chmod(0o600)
        return path

    def _write_result_input(self, name: str, value: object) -> Path:
        path = self.run_directory / name
        content = value if isinstance(value, str) else json.dumps(value)
        path.write_text(content, encoding="utf-8")
        path.chmod(0o600)
        return path

    def _write_result_inputs(self) -> None:
        self._write_result_input(
            "result-summary.txt", "Synthetic result summary\n"
        )
        self._write_result_input(
            "result-work.md", "# Synthetic work\n\nNo private evidence.\n"
        )
        self._write_result_input(
            "result-deliverables.json", ["Synthetic deliverable"]
        )

    def _enable_archive(self):
        paths = prepare_task_archive(
            working_root=self.root / "Project Alpha" / "Tasks",
            kb_root=self.root / "Project Alpha KB" / "Tasks",
            task_id=1,
            task_text="Synthetic task",
            run_id=RUN_ID,
            phase="plan",
            agent_display_name="General",
        )
        document = json.loads(self.state_path.read_text(encoding="utf-8"))
        document.update({
            "schema_version": 4,
            "task_work_directory": str(paths.working_directory),
            "task_kb_file": str(paths.task_file),
            "task_run_directory": str(paths.run_directory),
        })
        self.state_path.write_text(json.dumps(document), encoding="utf-8")
        self.state_path.chmod(0o600)
        return paths

    def test_context_and_search_are_bounded_and_hide_the_capability(self):
        with (
            mock.patch(
                "foxhound.execution_worker._local_today",
                return_value="2030-01-02",
            ),
            mock.patch(
                "foxhound.execution_worker.shutil.which",
                return_value="/usr/bin/synthetic-client",
            ),
            knowledge_server() as endpoint,
        ):
            worker = self._worker(endpoint)
            context = worker.context()
            result = worker.search(
                "synthetic query", max_results_per_layer=2
            )

        rendered = json.dumps({"context": context, "search": result})
        self.assertNotIn(CLAIM_TOKEN, rendered)
        self.assertNotIn(str(self.database), rendered)
        self.assertEqual(context["task"]["text"], "Synthetic task")
        self.assertEqual(
            context["task"]["working_group"],
            {
                "name": "Person A, Person B · Composite, Testing",
                "dominant_people": ["Person A", "Person B"],
                "keywords": ["composite", "testing"],
            },
        )
        self.assertEqual(context["schema_version"], 9)
        self.assertEqual(context["runtime"]["today"], "2030-01-02")
        self.assertEqual(context["runtime"]["today_weekday"], "Wednesday")
        self.assertEqual(
            context["runtime"]["next_week"],
            {
                "start": "2030-01-07",
                "start_weekday": "Monday",
                "end": "2030-01-13",
                "end_weekday": "Sunday",
            },
        )
        self.assertEqual(
            context["runtime"]["toolsets"], ["terminal", "file", "web"]
        )
        self.assertEqual(
            context["capabilities"],
            {
                "knowledge_layers": ["kb", "secondary", "emails"],
                "local_research_clients": {
                    "outlook": [
                        "folders", "inbox", "search", "read", "thread",
                        "attachment",
                    ],
                    "moodle": [
                        "renew", "whoami", "courses", "assignments",
                        "submissions", "assessment",
                    ],
                    "qmd": [
                        "query", "search", "get", "multi-get", "ls",
                        "status",
                    ],
                },
                "deployment_roots": {},
                "worker_operations": [
                    "context", "search", "draft", "record", "release",
                    "act.worktree", "thread",
                ],
                "external_effects_allowed": False,
            },
        )
        self.assertEqual(context["workflow"]["phase"], "plan")
        self.assertEqual(
            context["workflow"]["agent_profile_id"],
            self.claim.agent_profile_id,
        )
        self.assertEqual(
            context["workflow"]["agent_profile_revision"],
            self.claim.agent_profile_revision,
        )
        self.assertEqual(context["workflow"]["attempt_count"], 1)
        self.assertIsNone(context["workflow"]["handoff"])
        self.assertEqual(context["operator"]["display_name"], "Person A")
        self.assertEqual(result["layers"][0]["documents"][0]["excerpt"],
                         "Synthetic evidence.")
        self.assertNotIn(CLAIM_TOKEN, repr(load_run_state(self.state_path)))

    def test_handoff_evidence_reading_and_truncation(self):
        paths = self._enable_archive()
        worker = self._worker("http://127.0.0.1:9")
        self.assertIsNone(_read_handoff(None, "execute"))
        self.assertIsNone(_read_handoff(str(paths.working_directory), "execute"))

        handoff_file = paths.working_directory / "handoff-plan.md"
        handoff_file.write_text("Small handoff note.", encoding="utf-8")
        self.assertEqual(
            _read_handoff(str(paths.working_directory), "plan"),
            "Small handoff note.",
        )

        with knowledge_server() as endpoint:
            worker = self._worker(endpoint)
            context = worker.context()
        self.assertEqual(context["workflow"]["handoff"], "Small handoff note.")
        self.assertEqual(context["workflow"]["attempt_count"], 1)

        # Truncation over 16384 bytes
        large_content = "A" * 20000
        handoff_file.write_text(large_content, encoding="utf-8")
        truncated = _read_handoff(str(paths.working_directory), "plan")
        self.assertIsNotNone(truncated)
        self.assertTrue(truncated.endswith("[TRUNCATED: handoff file exceeded 16384 bytes]"))
        self.assertEqual(
            len(truncated.encode("utf-8")),
            16384 + len("\n\n[TRUNCATED: handoff file exceeded 16384 bytes]".encode("utf-8")),
        )

    def test_worker_capabilities_follow_the_phase_gate(self):
        for phase in WorkflowPhase:
            with self.subTest(phase=phase):
                self.assertIn("act.worktree", _worker_operations(phase))
        external = _worker_operations(WorkflowPhase.EXTERNAL_ACTION)
        self.assertIn("act.pull-request", external)
        self.assertIn("act.comment", external)
        self.assertIn("act.issue", external)
        self.assertIn("act.review", external)
        self.assertIn("act.mail", external)

    def test_a_working_tree_does_not_unlock_any_external_effect(self):
        """The gate is the effect, not the edit.

        A working tree is now available while planning. Nothing that reaches
        outside the run directory may follow it there, or the phase boundary
        has moved rather than the convenience.
        """
        for phase in (WorkflowPhase.PLAN, WorkflowPhase.EXECUTE):
            with self.subTest(phase=phase):
                operations = _worker_operations(phase)
                self.assertIn("act.worktree", operations)
                self.assertNotIn("act.pull-request", operations)
                self.assertNotIn("act.comment", operations)
                self.assertNotIn("act.issue", operations)
                self.assertNotIn("act.review", operations)

    def test_planning_prepares_a_working_tree_of_its_own(self):
        """A planning run that must write gets a tree inside its run directory.

        Refusing this is what sent a planning run looking for somewhere else
        writable, and the only such place on a host is a shared checkout.
        """
        self._bind_origin("issue")
        prepared = self.run_directory / "repo-record-1-item-1"

        with (
            mock.patch(
                "foxhound.execution_worker.forge_action.prepare_worktree",
                return_value=(prepared, "foxhound/issue-item-1", "main"),
            ) as prepare,
            knowledge_server() as endpoint,
        ):
            worker = self._worker(endpoint)
            self.assertEqual(
                worker.context()["workflow"]["phase"], "plan"
            )
            result = worker.act_worktree()

        self.assertEqual(result["repository"], "record-1")
        self.assertEqual(result["issue"], "item-1")
        self.assertEqual(result["path"], str(prepared))
        self.assertEqual(result["branch"], "foxhound/issue-item-1")
        self.assertEqual(result["base"], "main")
        self.assertEqual(
            prepare.call_args.kwargs["parent"], self.run_directory
        )

    def test_a_working_tree_still_requires_an_origin_to_name(self):
        with knowledge_server() as endpoint:
            with self.assertRaisesRegex(
                ExecutionWorkerClaimError, "no origin"
            ):
                self._worker(endpoint).act_worktree()

    def test_context_omits_local_clients_missing_from_the_runner(self):
        with (
            mock.patch(
                "foxhound.execution_worker.shutil.which", return_value=None
            ),
            knowledge_server() as endpoint,
        ):
            context = self._worker(endpoint).context()

        self.assertEqual(context["capabilities"]["local_research_clients"], {})

    def test_calendar_context_computes_the_subsequent_week_across_years(self):
        with mock.patch(
            "foxhound.execution_worker._local_today",
            return_value="2030-12-30",
        ):
            calendar = _local_calendar()

        self.assertEqual(calendar["today_weekday"], "Monday")
        self.assertEqual(
            calendar["next_week"],
            {
                "start": "2031-01-06",
                "start_weekday": "Monday",
                "end": "2031-01-12",
                "end_weekday": "Sunday",
            },
        )

    def test_instructions_come_from_the_pinned_revision_or_not_at_all(self):
        """The launch arguments say how to ask for instructions, not what
        they are. What comes back must be the revision the claim already
        recorded, so a bundle that was substituted, edited, or left behind by
        another profile cannot quietly become this run's policy."""
        with knowledge_server() as endpoint:
            context = self._worker(endpoint).context()

            self.assertEqual(
                context["agent"]["revision"],
                self.claim.agent_profile_revision,
            )
            self.assertEqual(
                context["agent"]["profile_id"], self.claim.agent_profile_id
            )
            self.assertEqual(
                context["agent"]["instructions"],
                general_profile().render_prompt(WORKER_COMMAND),
            )
            self.assertNotIn(
                "{{FOXHOUND_WORKER_COMMAND}}",
                context["agent"]["instructions"],
            )

            edited = general_profile().document()
            edited["max_turns"] = 12
            foreign = general_profile().document()
            foreign["profile_id"] = "other-agent"
            for case, document in (
                ("edited", edited),
                ("foreign", foreign),
                ("unparseable", {"schema": "foxhound.agent-profile"}),
            ):
                with self.subTest(case=case):
                    self._write_instructions(document)
                    with self.assertRaises(ExecutionWorkerConfigError):
                        self._worker(endpoint).context()

            (self.run_directory / INSTRUCTIONS_NAME).unlink()
            with self.assertRaises(ExecutionWorkerConfigError):
                self._worker(endpoint).context()

            self._write_instructions()
            (self.run_directory / INSTRUCTIONS_NAME).chmod(0o644)
            with self.assertRaises(ExecutionWorkerConfigError):
                self._worker(endpoint).context()

    def test_a_refused_result_says_why_it_was_refused(self):
        """An agent told only "refused" cannot tell a fixable state from a
        hopeless one, so it does the safe thing and gives up. One did:
        three attempts, three bare refusals, and a complete and correct
        result released instead of recorded. The reason is an enum token
        naming a state, never task content.
        """
        draft = self._write_draft()
        # Record once so the claim is spent, then try again on a claim that
        # is no longer running.
        with knowledge_server() as endpoint:
            self._worker(endpoint).record(draft.name)

        second_id = "d" * 32
        second = self.run_directory / f"result-{second_id}.json"
        second.write_text(json.dumps({
            "schema": "foxhound.execution-result-draft",
            "schema_version": 1,
            "result_id": second_id,
            "outcome": "awaiting_plan",
            "summary": "Synthetic second summary",
            "work_markdown": "# Synthetic work\n\nNo private evidence.",
            "questions": [],
            "external_actions": [],
            "deliverables": [],
            "repository_references": [],
        }), encoding="utf-8")
        second.chmod(0o600)
        with knowledge_server() as endpoint:
            with self.assertRaises(ExecutionWorkerDraftError) as caught:
                self._worker(endpoint).record(second.name)

        self.assertIsNotNone(caught.exception.reason)
        # A state, not content: nothing from the task may appear here.
        self.assertNotIn("Synthetic", caught.exception.reason)
        self.assertNotIn(" ", caught.exception.reason)

    def test_the_context_says_where_the_knowledge_base_is(self):
        """Search returns a fragment; the directory is how the agent reads
        the discussion that fragment came from. Across three supervised
        runs the agent made no knowledge query at all and planned from
        repository history alone, which says what the code is and never
        why it is that way.
        """
        with knowledge_server() as endpoint:
            context = self._worker(endpoint).context()
        self.assertEqual(
            context["knowledge"]["root"], str(self.knowledge_root))

    def test_a_machine_without_a_knowledge_base_is_ordinary(self):
        # Not every host keeps one. The agent must be able to tell that
        # apart from one it was simply not told about.
        document = json.loads(self.state_path.read_text(encoding="utf-8"))
        document["knowledge_root"] = None
        self.state_path.write_text(json.dumps(document), encoding="utf-8")
        with knowledge_server() as endpoint:
            context = self._worker(endpoint).context()
        self.assertIsNone(context["knowledge"]["root"])

    def test_the_context_names_the_thing_the_task_is_about(self):
        """The agent is told to take repository identity from here, and told
        to infer nothing from the task text. Omitting it did not make the
        agent careful, it made it blind: asked to scaffold an application
        for an issue that already had a repository, it planned a greenfield
        project and asked the reader where to put it.
        """
        with closing(sqlite3.connect(self.database)) as connection:
            connection.execute(
                "INSERT INTO candidate_inbox(candidate_id,source_system,"
                "source_kind,source_record_id,source_item_id,source_revision,"
                "payload_json,created_at,first_imported_at,updated_at) "
                "VALUES('cand-1','gw','issue','forge.example/acme/widget',"
                "'42',?,'{}','2030-01-01T00:00:00Z','2030-01-01T00:00:00Z',"
                "'2030-01-01T00:00:00Z')", ("b" * 64,))
            connection.execute(
                "INSERT INTO task_candidate_bindings(candidate_id,"
                "source_revision,task_id,relation,decided_at) "
                "VALUES('cand-1',?,1,'accepted','2030-01-01T00:00:00Z')",
                ("b" * 64,))
            connection.commit()

        with knowledge_server() as endpoint:
            context = self._worker(endpoint).context()

        self.assertEqual(
            context["task"]["origin"],
            {"system": "gw", "kind": "issue",
             "record_id": "forge.example/acme/widget", "item_id": "42"},
        )

    def test_freshness_is_opt_in_and_refuses_noncurrent_effects(self):
        with closing(sqlite3.connect(self.database)) as connection:
            connection.execute(
                "INSERT INTO candidate_inbox(candidate_id,source_system,"
                "source_kind,source_record_id,source_item_id,source_revision,"
                "payload_json,created_at,first_imported_at,updated_at) "
                "VALUES('fresh-1','gw','issue','forge.example/acme/widget',"
                "'42',?,'{}','2030-01-01T00:00:00Z','2030-01-01T00:00:00Z',"
                "'2030-01-01T00:00:00Z')", ("b" * 64,))
            connection.execute(
                "INSERT INTO task_candidate_bindings(candidate_id,source_revision,"
                "task_id,relation,decided_at) VALUES('fresh-1',?,1,'accepted',?)",
                ("b" * 64, "2030-01-01T00:00:00Z"))
            connection.commit()
        with knowledge_server() as endpoint:
            disabled = self._worker(endpoint)
            disabled._fresh_active("effect")
            enabled = self._worker(endpoint, policy=_policy())
            with mock.patch(
                "foxhound.execution_worker.GwKnowledgeClient.refresh_source",
                return_value=type("Result", (), {"usable": False})(),
            ):
                with self.assertRaises(ExecutionWorkerClaimError):
                    enabled._fresh_active("effect")

    def test_an_unconfigured_worker_checks_no_freshness(self):
        """The default is the behaviour before any of this existed."""
        with knowledge_server() as endpoint:
            worker = self._worker(endpoint)
            with mock.patch(
                "foxhound.execution_worker.GwKnowledgeClient.refresh_source",
                side_effect=AssertionError("must not be called"),
            ):
                worker._fresh_active("phase")
                worker._fresh_active("effect")

    def test_a_phase_only_policy_does_not_fence_an_effect(self):
        with knowledge_server() as endpoint:
            worker = self._worker(
                endpoint, policy=_policy(freshness="before_phase"))
            with mock.patch(
                "foxhound.execution_worker.GwKnowledgeClient.refresh_source",
                side_effect=AssertionError("must not be called"),
            ), mock.patch(
                "foxhound.execution_worker.TaskLedger.source_snapshot_request",
                return_value=SimpleNamespace(
                    locator=SimpleNamespace(kind="issue")),
            ):
                worker._fresh_active("effect")

    def test_the_policy_in_force_is_recoverable_from_the_environment(self):
        """An absent policy is the compatibility one; a broken one refuses."""
        self.assertIsNone(_workflow_policy(""))
        self.assertIsNone(_workflow_policy("   "))
        parsed = _workflow_policy(json.dumps({
            "policy_id": "synthetic-fence", "grants": {
                "plan": [], "execute": [], "external_action": [],
            }, "freshness": "before_effect", "freshness_kinds": ["issue"],
            "effects": [], "final_decision": True,
        }))
        self.assertEqual(parsed.revision, _policy().revision)
        for broken in ("{", "null", json.dumps({"policy_id": "x"})):
            with self.assertRaises(ExecutionWorkerConfigError):
                _workflow_policy(broken)

    def test_freshness_refusals_are_controlled(self):
        """A malformed request or unavailable source never escapes as a crash."""
        with knowledge_server() as endpoint:
            worker = self._worker(endpoint, policy=_policy())
            with self.assertRaises(ExecutionWorkerConfigError):
                worker._fresh_active("unknown")
            with mock.patch(
                "foxhound.execution_worker.TaskLedger.source_snapshot_request",
                side_effect=SourceSnapshotContractError("synthetic"),
            ):
                with self.assertRaises(ExecutionWorkerClaimError):
                    worker._fresh_active("effect")
            with mock.patch(
                "foxhound.execution_worker.TaskLedger.source_snapshot_request",
                return_value=SimpleNamespace(
                    locator=SimpleNamespace(kind="issue"),
                ),
            ), mock.patch(
                "foxhound.execution_worker.GwKnowledgeClient.refresh_source",
                side_effect=KnowledgeClientError("synthetic"),
            ):
                with self.assertRaises(ExecutionWorkerClaimError):
                    worker._fresh_active("effect")

    def test_noncurrent_snapshots_refuse_every_forge_effect_before_writing(self):
        """A status that is not current must not reach any forge adapter."""
        actions = (
            (
                "review", "review_request", "42/revision", "post_review",
                lambda worker: worker.act_review(
                    body_file=self._write_effect_body("review.md")),
            ),
            (
                "comment", "issue", "42", "post_issue_comment",
                lambda worker: worker.act_comment(
                    body_file=self._write_effect_body("comment.md")),
            ),
            (
                "pull_request", "issue", "43", "open_pull_request",
                lambda worker: worker.act_pull_request(
                    head="foxhound/issue-43", title="Synthetic proposal",
                    body_file=None,
                ),
            ),
            (
                "issue", "review_request", "44/revision", "open_issue",
                lambda worker: worker.act_issue(
                    title="Synthetic finding",
                    body_file=self._write_effect_body("issue.md")),
            ),
        )
        for index, (name, kind, item_id, forge_call, action) in enumerate(
            actions, start=1,
        ):
            with self.subTest(effect=name):
                with knowledge_server() as endpoint:
                    worker, active = self._effect_worker(
                        endpoint, task_id=index, kind=kind, item_id=item_id,
                    )
                    for status in (
                        "changed", "withdrawn", "unavailable", "unsupported",
                    ):
                        with self.subTest(status=status), active, mock.patch.object(
                            worker, "_renew"
                        ), mock.patch.object(
                            execution_worker.GwKnowledgeClient, "refresh_source",
                            return_value=SimpleNamespace(
                                status=status, usable=False,
                            ),
                        ), mock.patch.object(
                            execution_worker.forge_action, forge_call,
                        ) as forge:
                            with self.assertRaises(ExecutionWorkerClaimError):
                                action(worker)
                            forge.assert_not_called()

    def test_transport_failure_refuses_every_forge_effect_before_writing(self):
        actions = (
            (
                "review", "review_request", "42/revision", "post_review",
                lambda worker: worker.act_review(
                    body_file=self._write_effect_body("review.md")),
            ),
            (
                "comment", "issue", "42", "post_issue_comment",
                lambda worker: worker.act_comment(
                    body_file=self._write_effect_body("comment.md")),
            ),
            (
                "pull_request", "issue", "43", "open_pull_request",
                lambda worker: worker.act_pull_request(
                    head="foxhound/issue-43", title="Synthetic proposal",
                    body_file=None,
                ),
            ),
        )
        for index, (name, kind, item_id, forge_call, action) in enumerate(
            actions, start=1,
        ):
            with self.subTest(effect=name), knowledge_server() as endpoint:
                worker, active = self._effect_worker(
                    endpoint, task_id=index, kind=kind, item_id=item_id,
                )
                with active, mock.patch.object(worker, "_renew"), mock.patch.object(
                    execution_worker.GwKnowledgeClient, "refresh_source",
                    side_effect=KnowledgeClientError("synthetic"),
                ), mock.patch.object(
                    execution_worker.forge_action, forge_call,
                ) as forge:
                    with self.assertRaises(ExecutionWorkerClaimError):
                        action(worker)
                    forge.assert_not_called()

    def test_current_snapshot_allows_every_forge_effect(self):
        actions = (
            (
                "review", "review_request", "42/revision", "post_review",
                lambda worker: worker.act_review(
                    body_file=self._write_effect_body("review.md")),
                SimpleNamespace(
                    repository="github.com/example-org/example-repo", number=42,
                    url="https://github.com/example-org/example-repo/pull/42",
                ),
            ),
            (
                "comment", "issue", "42", "post_issue_comment",
                lambda worker: worker.act_comment(
                    body_file=self._write_effect_body("comment.md")),
                SimpleNamespace(
                    repository="github.com/example-org/example-repo", number=42,
                    url="https://github.com/example-org/example-repo/issues/42",
                ),
            ),
            (
                "pull_request", "issue", "43", "open_pull_request",
                lambda worker: worker.act_pull_request(
                    head="foxhound/issue-43", title="Synthetic proposal",
                    body_file=None,
                ),
                SimpleNamespace(
                    repository="github.com/example-org/example-repo", issue="43",
                    number=43, url="https://github.com/example-org/example-repo/pull/43",
                    head="foxhound/issue-43", base="main",
                ),
            ),
        )
        for index, (name, kind, item_id, forge_call, action, receipt) in enumerate(
            actions, start=1,
        ):
            with self.subTest(effect=name), knowledge_server() as endpoint:
                worker, active = self._effect_worker(
                    endpoint, task_id=index, kind=kind, item_id=item_id,
                )
                with active, mock.patch.object(worker, "_renew"), mock.patch.object(
                    execution_worker.GwKnowledgeClient, "refresh_source",
                    return_value=SimpleNamespace(status="current", usable=True),
                ) as refresh, mock.patch.object(
                    execution_worker.forge_action, forge_call,
                    return_value=receipt,
                ) as forge:
                    action(worker)
                    refresh.assert_called_once()
                    forge.assert_called_once()

    def test_before_effect_rechecks_after_phase_entry(self):
        self._bind_fresh_origin(1, kind="issue", item_id="42")
        body = self._write_effect_body("comment.md")
        with knowledge_server() as endpoint:
            worker = self._worker(endpoint, policy=_policy())
            effect_state = replace(
                load_run_state(self.state_path), phase=WorkflowPhase.EXTERNAL_ACTION,
            )
            with mock.patch.object(
                execution_worker.GwKnowledgeClient, "refresh_source",
                return_value=SimpleNamespace(status="current", usable=True),
            ) as refresh, mock.patch.object(
                execution_worker.forge_action, "post_issue_comment",
                return_value=SimpleNamespace(
                    repository="github.com/example-org/example-repo", number=42,
                    url="https://github.com/example-org/example-repo/issues/42",
                ),
            ), mock.patch.object(worker, "_renew"), mock.patch.object(
                worker, "_active", wraps=worker._active,
            ):
                worker.context()
                with mock.patch.object(
                    worker, "_active", return_value=(effect_state, SimpleNamespace()),
                ):
                    worker.act_comment(body_file=body)
        self.assertEqual(refresh.call_count, 2)

    def test_a_task_about_nothing_addressable_says_so(self):
        # An ordinary state, not an error: a task may come from a meeting,
        # or predate binding. The agent must be able to tell that apart from
        # a repository it simply was not told about.
        with knowledge_server() as endpoint:
            context = self._worker(endpoint).context()
        self.assertIsNone(context["task"]["origin"])

    def test_record_injects_identity_and_scrubs_the_private_draft(self):
        draft = self._write_draft()
        with knowledge_server() as endpoint:
            receipt = self._worker(endpoint).record(str(draft))

        state = TaskExecutionService(self.database).get(1)
        self.assertEqual(state.status, WorkflowStatus.AWAITING_REVIEW)
        self.assertEqual(state.last_result_id, RESULT_ID)
        self.assertEqual(receipt["status"], "awaiting_review")
        scrubbed = draft.read_text(encoding="utf-8")
        self.assertNotIn(CLAIM_TOKEN, scrubbed)
        self.assertNotIn("Synthetic result summary", scrubbed)
        self.assertNotIn("work_markdown", scrubbed)

    def test_record_applies_execution_grants_carried_by_run_state(self):
        self._bind_origin("issue")
        self._write_state(schema_version=5, execution_grants=("issue",))
        draft = self._write_draft()

        with knowledge_server() as endpoint:
            receipt = self._worker(endpoint).record(str(draft))

        workflow = TaskExecutionService(self.database).get(1)
        self.assertEqual(receipt["status"], "queued")
        self.assertEqual(workflow.status, WorkflowStatus.QUEUED)
        self.assertEqual(workflow.phase, WorkflowPhase.EXECUTE)
        with closing(sqlite3.connect(self.database)) as connection:
            events = [
                row[0] for row in connection.execute(
                    "SELECT kind FROM task_execution_events WHERE task_id=1 "
                    "ORDER BY sequence"
                )
            ]
        self.assertEqual(events[-1], "phase_granted")

    def test_schema_four_state_defaults_automation_grants_to_empty(self):
        self._write_state(schema_version=4)

        state = load_run_state(self.state_path)

        self.assertEqual(state.execution_grants, frozenset())
        self.assertEqual(state.action_grants, frozenset())

    def test_schema_five_state_rejects_unknown_automation_grants(self):
        self._write_state(schema_version=5, execution_grants=("unknown",))

        with self.assertRaises(ExecutionWorkerConfigError):
            load_run_state(self.state_path)

    def test_schema_six_state_carries_named_deployment_roots(self):
        shared = self.root / "shared"
        shared.mkdir()
        self._write_state(
            schema_version=6,
            deployment_roots={"shared_state": str(shared)},
        )

        state = load_run_state(self.state_path)

        self.assertEqual(state.deployment_roots, {"shared_state": str(shared)})

    def test_context_includes_configured_deployment_roots(self):
        drive = self.root / "drive"
        drive.mkdir()
        self._write_state(
            schema_version=6,
            deployment_roots={"sync_drive": str(drive)},
        )

        with (
            mock.patch(
                "foxhound.execution_worker._local_today",
                return_value="2030-01-02",
            ),
            mock.patch(
                "foxhound.execution_worker.shutil.which",
                return_value="/usr/bin/synthetic-client",
            ),
            knowledge_server() as endpoint,
        ):
            worker = self._worker(endpoint)
            context = worker.context()

        self.assertIn("deployment_roots", context["capabilities"])
        self.assertEqual(
            context["capabilities"]["deployment_roots"],
            {"sync_drive": str(drive)},
        )

    def test_result_path_is_confined_to_the_immediate_run_directory(self):
        draft = self._write_draft()
        path, document = load_result_draft(self.run_directory, str(draft))
        self.assertEqual(path, draft)
        self.assertEqual(document["result_id"], RESULT_ID)

        alias_id = "c" * 32
        alias = self.run_directory / f"result-{alias_id}.json"
        alias.symlink_to(draft)
        cases = (
            str(self.root / draft.name),
            f"nested/{draft.name}",
            f"../{self.run_directory.name}/{draft.name}",
            str(alias),
        )
        for supplied in cases:
            with self.subTest(supplied=Path(supplied).name):
                with self.assertRaises(ExecutionWorkerDraftError):
                    load_result_draft(self.run_directory, supplied)

    def test_draft_builds_private_schema_and_record_accepts_it(self):
        self._write_result_inputs()
        self._write_result_input(
            "result-questions.json", ["Should Example A proceed?"]
        )
        self._write_result_input(
            "result-external-actions.json", ["Prepare a synthetic draft."]
        )
        self._write_result_input(
            "result-deliverables.json", ["Synthetic deliverable"]
        )
        with knowledge_server() as endpoint:
            worker = self._worker(endpoint)
            ready = worker.draft(outcome="awaiting_plan")

            self.assertEqual(ready, {
                "schema": "foxhound.execution-result-draft-ready",
                "schema_version": 1,
                "draft": f"result-{RUN_ID}.json",
            })
            draft = self.run_directory / ready["draft"]
            self.assertEqual(draft.stat().st_mode & 0o777, 0o600)
            document = json.loads(draft.read_text(encoding="utf-8"))
            self.assertEqual(document, {
                "schema": "foxhound.execution-result-draft",
                "schema_version": 1,
                "result_id": RUN_ID,
                "outcome": "awaiting_plan",
                "summary": "Synthetic result summary",
                "work_markdown": "# Synthetic work\n\nNo private evidence.",
            "questions": ["Should Example A proceed?"],
            "external_actions": ["Prepare a synthetic draft."],
            "deliverables": ["Synthetic deliverable"],
            "repository_references": [],
            "repository_impact": True,
        })
            self.assertNotIn(CLAIM_TOKEN, draft.read_text(encoding="utf-8"))
            with self.assertRaises(ExecutionWorkerDraftError):
                worker.draft(outcome="awaiting_plan")
            receipt = worker.record(draft.name)

        self.assertEqual(receipt["status"], "awaiting_review")
        for name in (
            "result-summary.txt",
            "result-work.md",
            "result-questions.json",
            "result-external-actions.json",
            "result-deliverables.json",
            "result-repository-impact.json",
        ):
            self.assertFalse((self.run_directory / name).exists())

    def test_record_preserves_result_and_records_review_locations(self):
        paths = self._enable_archive()
        self._write_result_inputs()
        artifact = self.run_directory / "verification.txt"
        artifact.write_text("Synthetic verification.\n", encoding="utf-8")
        artifact.chmod(0o600)
        self._write_result_input("result-artifacts.json", ["verification.txt"])

        with knowledge_server() as endpoint:
            worker = self._worker(endpoint)
            context = worker.context()
            ready = worker.draft(outcome="awaiting_plan")
            worker.record(ready["draft"])

        self.assertNotIn("review", context)
        self.assertTrue((paths.run_directory / "verification.txt").is_file())
        self.assertTrue((paths.run_directory / "result-work.md").is_file())
        archived_draft = json.loads(
            (paths.run_directory / f"result-{RUN_ID}.json").read_text(
                encoding="utf-8"
            )
        )
        self.assertEqual(archived_draft["schema"], "foxhound.execution-result-draft")
        self.assertFalse((paths.run_directory / INSTRUCTIONS_NAME).exists())
        with closing(sqlite3.connect(self.database)) as connection:
            stored = connection.execute(
                "SELECT task_work_directory,task_kb_file "
                "FROM task_execution_results WHERE result_id=?",
                (RUN_ID,),
            ).fetchone()
            artifact_record = connection.execute(
                "SELECT relative_path,name,size_bytes,content_digest,run_directory "
                "FROM execution_result_artifacts WHERE result_id=?",
                (RUN_ID,),
            ).fetchone()
        self.assertEqual(stored, (
            str(paths.working_directory), str(paths.task_file)
        ))
        self.assertEqual(artifact_record, (
            "verification.txt", "verification.txt", 24,
            hashlib.sha256(b"Synthetic verification.\n").hexdigest(),
            str(paths.run_directory),
        ))

    def test_draft_rejects_invalid_inputs_before_writing(self):
        self._write_result_inputs()
        with knowledge_server() as endpoint:
            worker = self._worker(endpoint)
            with self.assertRaises(ExecutionWorkerDraftError):
                worker.draft(outcome="awaiting_external")
            self.assertEqual(
                list(self.run_directory.glob("result-????????????????????????????????.json")), []
            )

            questions = self._write_result_input(
                "result-questions.json", [{"text": "not a string"}]
            )
            with self.assertRaises(ExecutionWorkerDraftError):
                worker.draft(outcome="awaiting_plan")
            questions.unlink()

            summary = self.run_directory / "result-summary.txt"
            summary.chmod(0o644)
            with self.assertRaises(ExecutionWorkerDraftError):
                worker.draft(outcome="awaiting_plan")
            summary.chmod(0o600)
            summary.unlink()
            summary.symlink_to(self.run_directory / "result-work.md")
            with self.assertRaises(ExecutionWorkerDraftError):
                worker.draft(outcome="awaiting_plan")

        self.assertEqual(
            list(self.run_directory.glob("result-????????????????????????????????.json")), []
        )

    def test_draft_defaults_missing_collection_files_to_empty_arrays(self):
        self._write_result_inputs()
        (self.run_directory / "result-deliverables.json").unlink()
        with knowledge_server() as endpoint:
            ready = self._worker(endpoint).draft(outcome="awaiting_plan")
        document = json.loads(
            (self.run_directory / ready["draft"]).read_text(encoding="utf-8")
        )
        self.assertEqual(document["questions"], [])
        self.assertEqual(document["external_actions"], [])
        self.assertEqual(document["deliverables"], [])

    def test_draft_uses_this_claims_run_inputs_over_stale_task_inputs(self):
        """A durable task folder cannot make a later run replay old work."""
        paths = self._enable_archive()
        stale = {
            "result-summary.txt": "Earlier synthetic summary.\n",
            "result-work.md": "# Earlier synthetic work\n",
        }
        for name, text in stale.items():
            path = paths.working_directory / name
            path.write_text(text, encoding="utf-8")
            path.chmod(0o600)
            # The task folder outlives runs.  Make these files explicitly
            # older than this claim's private run-state anchor.
            anchor = self.state_path.stat().st_mtime_ns
            os.utime(path, ns=(anchor - 1_000_000, anchor - 1_000_000))

        self._write_result_inputs()
        with knowledge_server() as endpoint:
            ready = self._worker(endpoint).draft(outcome="awaiting_plan")

        document = json.loads(
            (self.run_directory / ready["draft"]).read_text(encoding="utf-8")
        )
        self.assertEqual(document["summary"], "Synthetic result summary")
        self.assertEqual(document["work_markdown"],
                         "# Synthetic work\n\nNo private evidence.")

    def test_draft_still_accepts_task_inputs_written_during_this_claim(self):
        paths = self._enable_archive()
        for name, text in (
            ("result-summary.txt", "Task-folder synthetic summary.\n"),
            ("result-work.md", "# Task-folder synthetic work\n"),
        ):
            path = paths.working_directory / name
            path.write_text(text, encoding="utf-8")
            path.chmod(0o600)

        with knowledge_server() as endpoint:
            ready = self._worker(endpoint).draft(outcome="awaiting_plan")

        document = json.loads(
            (self.run_directory / ready["draft"]).read_text(encoding="utf-8")
        )
        self.assertEqual(document["summary"], "Task-folder synthetic summary.")
        self.assertEqual(document["work_markdown"],
                         "# Task-folder synthetic work")

    def _age_task_input(self, path: Path) -> None:
        """Make a task-folder file predate this claim's run-state anchor."""
        anchor = self.state_path.stat().st_mtime_ns
        os.utime(path, ns=(anchor - 1_000_000, anchor - 1_000_000))

    def test_stale_task_input_is_not_read_when_this_run_wrote_none(self):
        """The fence must not be undone by the not-found fallback.

        Skipping a stale candidate and then returning that same path as the
        reported location handed it straight back to the readers, so a name
        this run never authored still replayed the previous claim.
        """
        paths = self._enable_archive()
        for name, text in (
            ("result-summary.txt", "Earlier synthetic summary.\n"),
            ("result-work.md", "# Earlier synthetic work\n"),
            ("result-questions.json", '["Stale synthetic question?"]\n'),
        ):
            path = paths.working_directory / name
            path.write_text(text, encoding="utf-8")
            path.chmod(0o600)
            self._age_task_input(path)

        # This claim authors a summary and a work body, but no questions.
        self._write_result_input(
            "result-summary.txt", "Synthetic result summary\n"
        )
        self._write_result_input(
            "result-work.md", "# Synthetic work\n\nNo private evidence.\n"
        )
        with knowledge_server() as endpoint:
            ready = self._worker(endpoint).draft(outcome="awaiting_plan")

        document = json.loads(
            (self.run_directory / ready["draft"]).read_text(encoding="utf-8")
        )
        self.assertEqual(document["questions"], [])

    def test_draft_refuses_when_only_stale_task_inputs_exist(self):
        """A claim that authored nothing is empty, not the previous claim."""
        paths = self._enable_archive()
        for name, text in (
            ("result-summary.txt", "Earlier synthetic summary.\n"),
            ("result-work.md", "# Earlier synthetic work\n"),
        ):
            path = paths.working_directory / name
            path.write_text(text, encoding="utf-8")
            path.chmod(0o600)
            self._age_task_input(path)

        with knowledge_server() as endpoint:
            with self.assertRaises(ExecutionWorkerDraftError):
                self._worker(endpoint).draft(outcome="awaiting_plan")

    def test_release_sees_result_inputs_authored_in_the_task_folder(self):
        """Releasing must not silently drop work authored where it belongs.

        The guard looked only at the private run directory, so an agent that
        used the intended durable location was released as having produced
        nothing at all.
        """
        paths = self._enable_archive()
        for name, text in (
            ("result-summary.txt", "Task-folder synthetic summary.\n"),
            ("result-work.md", "# Task-folder synthetic work\n"),
        ):
            path = paths.working_directory / name
            path.write_text(text, encoding="utf-8")
            path.chmod(0o600)

        with knowledge_server() as endpoint:
            receipt = self._worker(endpoint).release()

        self.assertEqual(receipt["schema"], "foxhound.execution-result-receipt")
        self.assertEqual(receipt["disposition"], "applied")

    def test_release_ignores_stale_task_inputs_from_an_earlier_claim(self):
        """A durable leftover must not block every future release."""
        paths = self._enable_archive()
        path = paths.working_directory / "result-summary.txt"
        path.write_text("Earlier synthetic summary.\n", encoding="utf-8")
        path.chmod(0o600)
        self._age_task_input(path)

        with knowledge_server() as endpoint:
            receipt = self._worker(endpoint).release()

        self.assertEqual(
            receipt["schema"], "foxhound.execution-release-receipt"
        )

    def test_repository_receipt_is_private_and_deduplicated(self):
        receipt = {
            "kind": "issue-comment",
            "repository": "github.com/example-org/example-repo",
            "url": "https://github.com/example-org/example-repo/issues/42#issuecomment-1",
        }
        _append_repository_receipt(self.run_directory, receipt)
        _append_repository_receipt(self.run_directory, receipt)

        self.assertEqual(_repository_receipts(self.run_directory), (receipt,))
        path = self.run_directory / "repository-action-receipts.json"
        self.assertEqual(path.stat().st_mode & 0o777, 0o600)

    def test_github_execution_must_wait_for_repository_follow_through(self):
        state = SimpleNamespace(
            database_path=self.database,
            task_id=1,
            phase=WorkflowPhase.EXECUTE,
        )
        with mock.patch(
            "foxhound.execution_worker._repository_origin",
            return_value=object(),
        ):
            with self.assertRaisesRegex(
                ExecutionWorkerDraftError,
                "write JSON false to result-repository-impact.json",
            ):
                _repository_result(
                    state,
                    {
                        "outcome": "completed",
                        "deliverables": ["private note"],
                    },
                    self.run_directory,
                )

    def test_repository_execution_requires_an_action_for_its_exact_origin(self):
        state = SimpleNamespace(
            database_path=self.database,
            task_id=1,
            phase=WorkflowPhase.EXECUTE,
        )
        origin = SimpleNamespace(
            kind="issue",
            record_id="github.com/example-org/example-repo",
            item_id="42",
        )
        draft = {
            "outcome": "awaiting_external",
            "deliverables": ["Prepared status update draft"],
            "external_actions": [{
                "action": "Post the prepared update",
                "target": "https://github.com/example-org/example-repo/issues/42",
            }],
        }
        with mock.patch(
            "foxhound.execution_worker._repository_origin", return_value=origin,
        ):
            self.assertEqual(
                _repository_result(state, draft, self.run_directory),
                {**draft, "repository_impact": True,
                 "repository_references": []},
            )
            with self.assertRaisesRegex(
                ExecutionWorkerDraftError, "targeting its origin",
            ):
                _repository_result(state, {
                    **draft,
                    "external_actions": [{
                        "action": "Post the prepared update",
                        "target": "https://github.com/example-org/example-repo/issues/41",
                    }],
                }, self.run_directory)

    def test_repository_work_that_changed_the_repository_cannot_be_ineligible(self):
        """`ineligible` claims the prerequisites were absent; a change disproves it.

        Without this the outcome ends repository work without publishing it and
        without advancing a phase, so the workflow reaches review and an
        ordinary `done` closes it as finished.
        """
        origin = SimpleNamespace(
            kind="issue",
            record_id="github.com/example-org/example-repo",
            item_id="42",
        )
        draft = {
            "outcome": "ineligible",
            "deliverables": [
                "Authored the change on branch issue-42-example-slug"
            ],
            "external_actions": [],
            "repository_impact": True,
        }
        for phase in (WorkflowPhase.PLAN, WorkflowPhase.EXECUTE):
            with self.subTest(phase=phase):
                state = SimpleNamespace(
                    database_path=self.database, task_id=1, phase=phase,
                )
                with mock.patch(
                    "foxhound.execution_worker._repository_origin",
                    return_value=origin,
                ):
                    with self.assertRaisesRegex(
                        ExecutionWorkerDraftError, "cannot be ineligible",
                    ):
                        _repository_result(state, draft, self.run_directory)

    def test_planning_run_with_repository_impact_cannot_record_completed(self):
        origin = SimpleNamespace(
            kind="issue",
            record_id="github.com/example-org/example-repo",
            item_id="42",
        )
        draft = {
            "outcome": "completed",
            "deliverables": [
                "Authored the change on branch issue-42-example-slug"
            ],
            "external_actions": [],
            "repository_impact": True,
        }
        state = SimpleNamespace(
            database_path=self.database, task_id=1, phase=WorkflowPhase.PLAN,
        )
        with mock.patch(
            "foxhound.execution_worker._repository_origin", return_value=origin,
        ):
            with self.assertRaisesRegex(
                ExecutionWorkerDraftError, "must record awaiting_plan",
            ):
                _repository_result(state, draft, self.run_directory)

    def test_planning_run_without_repository_impact_can_record_completed(self):
        origin = SimpleNamespace(
            kind="issue",
            record_id="github.com/example-org/example-repo",
            item_id="42",
        )
        draft = {
            "outcome": "completed",
            "deliverables": ["Analysis result"],
            "external_actions": [],
            "repository_impact": False,
        }
        state = SimpleNamespace(
            database_path=self.database, task_id=1, phase=WorkflowPhase.PLAN,
        )
        with mock.patch(
            "foxhound.execution_worker._repository_origin", return_value=origin,
        ):
            result = _repository_result(state, draft, self.run_directory)
            self.assertEqual(result["outcome"], "completed")

    def test_planning_run_with_repository_impact_and_references_can_record_completed(self):
        origin = SimpleNamespace(
            kind="issue",
            record_id="github.com/example-org/example-repo",
            item_id="42",
        )
        draft = {
            "outcome": "completed",
            "deliverables": ["Change already exists upstream"],
            "external_actions": [],
            "repository_impact": True,
            "repository_references": [
                {
                    "kind": "pull-request",
                    "url": "https://github.com/example-org/example-repo/pull/42",
                }
            ],
        }
        state = SimpleNamespace(
            database_path=self.database, task_id=1, phase=WorkflowPhase.PLAN,
        )
        with mock.patch(
            "foxhound.execution_worker._repository_origin", return_value=origin,
        ):
            result = _repository_result(state, draft, self.run_directory)
            self.assertEqual(result["outcome"], "completed")

    def test_execution_run_with_repository_impact_and_references_can_record_completed(self):
        origin = SimpleNamespace(
            kind="issue",
            record_id="github.com/example-org/example-repo",
            item_id="42",
        )
        draft = {
            "outcome": "completed",
            "deliverables": ["Work already completed upstream"],
            "external_actions": [],
            "repository_impact": True,
            "repository_references": [
                {
                    "kind": "pull-request",
                    "url": "https://github.com/example-org/example-repo/pull/42",
                }
            ],
        }
        state = SimpleNamespace(
            database_path=self.database, task_id=1, phase=WorkflowPhase.EXECUTE,
        )
        with mock.patch(
            "foxhound.execution_worker._repository_origin", return_value=origin,
        ):
            result = _repository_result(state, draft, self.run_directory)
            self.assertEqual(result["outcome"], "completed")

    def test_ineligible_is_accepted_when_the_repository_was_not_changed(self):
        """A genuinely blocked run keeps the outcome by reporting its effect."""
        origin = SimpleNamespace(
            kind="issue",
            record_id="github.com/example-org/example-repo",
            item_id="42",
        )
        draft = {
            "outcome": "ineligible",
            "deliverables": [
                "Dependency issue is unmerged; nothing smaller is valid"
            ],
            "external_actions": [],
            "repository_impact": False,
        }
        for phase in (WorkflowPhase.PLAN, WorkflowPhase.EXECUTE):
            with self.subTest(phase=phase):
                state = SimpleNamespace(
                    database_path=self.database, task_id=1, phase=phase,
                )
                with mock.patch(
                    "foxhound.execution_worker._repository_origin",
                    return_value=origin,
                ):
                    result = _repository_result(
                        state, draft, self.run_directory
                    )
                self.assertEqual(result["outcome"], "ineligible")

    def test_external_action_ineligible_is_unaffected_by_the_guard(self):
        """The publishing phase keeps its existing outcomes."""
        origin = SimpleNamespace(
            kind="issue",
            record_id="github.com/example-org/example-repo",
            item_id="42",
        )
        state = SimpleNamespace(
            database_path=self.database,
            task_id=1,
            phase=WorkflowPhase.EXTERNAL_ACTION,
        )
        with mock.patch(
            "foxhound.execution_worker._repository_origin", return_value=origin,
        ):
            result = _repository_result(state, {
                "outcome": "ineligible",
                "deliverables": ["The approved action can no longer apply"],
                "external_actions": [],
                "repository_impact": True,
            }, self.run_directory)
        self.assertEqual(result["outcome"], "ineligible")

    def test_analysis_only_repository_result_does_not_require_a_forge_update(self):
        state = SimpleNamespace(
            database_path=self.database,
            task_id=1,
            phase=WorkflowPhase.EXECUTE,
        )
        origin = SimpleNamespace(
            kind="issue",
            record_id="github.com/example-org/example-repo",
            item_id="42",
        )
        with mock.patch(
            "foxhound.execution_worker._repository_origin", return_value=origin,
        ):
            result = _repository_result(state, {
                "outcome": "completed",
                "deliverables": ["Analysis with a bounded recommendation"],
                "external_actions": [],
                "repository_impact": False,
            }, self.run_directory)
        self.assertEqual(result["repository_references"], [])

    def test_draft_allows_an_analysis_only_repository_result_to_complete(self):
        """An issue can ask a question, and the answer belongs on the card."""
        self._write_result_inputs()
        self._write_result_input("result-repository-impact.json", False)
        state = replace(
            load_run_state(self.state_path), phase=WorkflowPhase.EXECUTE,
        )
        origin = SimpleNamespace(
            kind="issue",
            record_id="github.com/example-org/example-repo",
            item_id="42",
        )
        with knowledge_server() as endpoint:
            worker = self._worker(endpoint)
            with (
                mock.patch.object(
                    worker, "_active", return_value=(
                        state,
                        SimpleNamespace(
                            delivered_reader_instruction_sequence=(
                                lambda *_args, **_kwargs: None
                            ),
                        ),
                    ),
                ),
                mock.patch(
                    "foxhound.execution_worker._repository_origin",
                    return_value=origin,
                ),
                mock.patch.object(worker, "_renew"),
            ):
                ready = worker.draft(outcome="completed")

        document = json.loads(
            (self.run_directory / ready["draft"]).read_text(encoding="utf-8")
        )
        self.assertEqual(document["outcome"], "completed")
        self.assertFalse(document["repository_impact"])

    def test_review_cannot_complete_without_publishing_the_review(self):
        """A review that changed no files still owes the review itself.

        `repository_impact: false` is truthful for a review and used to carry
        it past the follow-through guard, so the run recorded `completed`
        with no action and no receipt and the findings never left the run
        directory.
        """
        origin = SimpleNamespace(
            kind="review_request",
            record_id="github.com/example-org/example-repo",
            item_id="42",
        )
        draft = {
            "outcome": "completed",
            "deliverables": ["Review with two findings"],
            "external_actions": [],
            "repository_impact": False,
        }
        for phase in (WorkflowPhase.PLAN, WorkflowPhase.EXECUTE):
            with self.subTest(phase=phase):
                state = SimpleNamespace(
                    database_path=self.database, task_id=1, phase=phase,
                )
                with mock.patch(
                    "foxhound.execution_worker._repository_origin",
                    return_value=origin,
                ):
                    with self.assertRaisesRegex(
                        ExecutionWorkerDraftError,
                        "requires published follow-through",
                    ):
                        _repository_result(state, draft, self.run_directory)

    def test_review_completes_by_naming_follow_through_that_exists(self):
        """A re-surfaced review stops instead of repeating published work."""
        origin = SimpleNamespace(
            kind="review_request",
            record_id="github.com/example-org/example-repo",
            item_id="42",
        )
        state = SimpleNamespace(
            database_path=self.database, task_id=1,
            phase=WorkflowPhase.EXECUTE,
        )
        with mock.patch(
            "foxhound.execution_worker._repository_origin", return_value=origin,
        ):
            result = _repository_result(state, {
                "outcome": "completed",
                "deliverables": ["The review is already posted"],
                "external_actions": [],
                "repository_impact": False,
                "repository_references": [{
                    "kind": "review",
                    "url": (
                        "https://github.com/example-org/example-repo"
                        "/pull/42#issuecomment-1"
                    ),
                }],
            }, self.run_directory)
        self.assertEqual(result["outcome"], "completed")

    def test_review_awaiting_external_must_target_its_origin(self):
        """The action list is what the approval card shows the reader."""
        origin = SimpleNamespace(
            kind="review_request",
            record_id="github.com/example-org/example-repo",
            item_id="42",
        )
        state = SimpleNamespace(
            database_path=self.database, task_id=1,
            phase=WorkflowPhase.EXECUTE,
        )
        with mock.patch(
            "foxhound.execution_worker._repository_origin", return_value=origin,
        ):
            with self.assertRaisesRegex(
                ExecutionWorkerDraftError,
                r"targeting its origin: result-external-actions\.json must contain an action object with 'target': 'https://github\.com/example-org/example-repo/pull/42'",
            ):
                _repository_result(state, {
                    "outcome": "awaiting_external",
                    "deliverables": ["Review prepared"],
                    "external_actions": [{"action": "Post it"}],
                    "repository_impact": False,
                }, self.run_directory)

    def test_publication_is_the_deliverable_is_derived_from_receipts(self):
        """Classified by what an origin owes, not by a remembered list."""
        self.assertTrue(_publication_is_the_deliverable("review_request"))
        self.assertFalse(_publication_is_the_deliverable("issue"))
        self.assertFalse(_publication_is_the_deliverable("meeting"))
        self.assertFalse(_publication_is_the_deliverable(""))

    def test_draft_preserves_a_structured_origin_follow_through_action(self):
        self._write_result_inputs()
        action = {
            "action": "Post the prepared update",
            "target": "https://github.com/example-org/example-repo/issues/42",
        }
        self._write_result_input("result-external-actions.json", [action])
        state = replace(
            load_run_state(self.state_path), phase=WorkflowPhase.EXECUTE,
        )
        origin = SimpleNamespace(
            kind="issue",
            record_id="github.com/example-org/example-repo",
            item_id="42",
        )
        with knowledge_server() as endpoint:
            worker = self._worker(endpoint)
            with (
                mock.patch.object(
                    worker, "_active", return_value=(
                        state,
                        SimpleNamespace(
                            delivered_reader_instruction_sequence=(
                                lambda *_args, **_kwargs: None
                            ),
                        ),
                    ),
                ),
                mock.patch(
                    "foxhound.execution_worker._repository_origin",
                    return_value=origin,
                ),
                mock.patch.object(worker, "_renew"),
            ):
                ready = worker.draft(outcome="awaiting_external")

        document = json.loads(
            (self.run_directory / ready["draft"]).read_text(encoding="utf-8")
        )
        self.assertEqual(document["external_actions"], [action])

    def test_github_external_completion_needs_worker_receipt(self):
        state = SimpleNamespace(
            database_path=self.database,
            task_id=1,
            phase=WorkflowPhase.EXTERNAL_ACTION,
        )
        with mock.patch(
            "foxhound.execution_worker._repository_origin",
            return_value=object(),
        ):
            with self.assertRaisesRegex(
                ExecutionWorkerDraftError, "requires a worker action receipt"
            ):
                _repository_result(
                    state,
                    {"outcome": "completed", "deliverables": []},
                    self.run_directory,
                )

    def test_github_issue_completion_needs_pull_request_and_comment(self):
        state = SimpleNamespace(
            database_path=self.database,
            task_id=1,
            phase=WorkflowPhase.EXTERNAL_ACTION,
        )
        origin = SimpleNamespace(kind="issue")
        _append_repository_receipt(self.run_directory, {
            "kind": "issue-comment",
            "repository": "github.com/example-org/example-repo",
            "url": "https://github.com/example-org/example-repo/issues/42#issuecomment-1",
        })
        with mock.patch(
            "foxhound.execution_worker._repository_origin", return_value=origin,
        ):
            with self.assertRaisesRegex(
                ExecutionWorkerDraftError, "requires pull-request receipt",
            ):
                _repository_result(
                    state, {"outcome": "completed", "deliverables": []},
                    self.run_directory,
                )

        _append_repository_receipt(self.run_directory, {
            "kind": "pull-request",
            "repository": "github.com/example-org/example-repo",
            "url": "https://github.com/example-org/example-repo/pull/43",
        })
        with mock.patch(
            "foxhound.execution_worker._repository_origin", return_value=origin,
        ):
            result = _repository_result(
                state, {"outcome": "completed", "deliverables": []},
                self.run_directory,
            )
        self.assertEqual(len(result["deliverables"]), 2)

    def test_github_issue_completion_accepts_referenced_pull_request_with_comment_receipt(self):
        state = SimpleNamespace(
            database_path=self.database,
            task_id=1,
            phase=WorkflowPhase.EXTERNAL_ACTION,
        )
        origin = SimpleNamespace(
            kind="issue",
            record_id="github.com/example-org/example-repo",
            item_id="42",
        )
        _append_repository_receipt(self.run_directory, {
            "kind": "issue-comment",
            "repository": "github.com/example-org/example-repo",
            "url": "https://github.com/example-org/example-repo/issues/42#issuecomment-1",
        })
        with mock.patch(
            "foxhound.execution_worker._repository_origin", return_value=origin,
        ):
            result = _repository_result(
                state,
                {
                    "outcome": "completed",
                    "deliverables": [],
                    "repository_references": [{
                        "kind": "pull-request",
                        "url": "https://github.com/example-org/example-repo/pull/43",
                    }],
                },
                self.run_directory,
            )
        self.assertEqual(result["outcome"], "completed")
        self.assertEqual(len(result["deliverables"]), 2)
        urls = [r["url"] for r in result["repository_references"]]
        self.assertIn("https://github.com/example-org/example-repo/pull/43", urls)

    def test_github_issue_completion_accepts_both_referenced_pull_request_and_issue(self):
        state = SimpleNamespace(
            database_path=self.database,
            task_id=1,
            phase=WorkflowPhase.EXTERNAL_ACTION,
        )
        origin = SimpleNamespace(
            kind="issue",
            record_id="github.com/example-org/example-repo",
            item_id="42",
        )
        with mock.patch(
            "foxhound.execution_worker._repository_origin", return_value=origin,
        ):
            result = _repository_result(
                state,
                {
                    "outcome": "completed",
                    "deliverables": ["PR merged externally"],
                    "repository_references": [
                        {
                            "kind": "pull-request",
                            "url": "https://github.com/example-org/example-repo/pull/43",
                        },
                        {
                            "kind": "issue",
                            "url": "https://github.com/example-org/example-repo/issues/42",
                        },
                    ],
                },
                self.run_directory,
            )
        self.assertEqual(result["outcome"], "completed")
        self.assertEqual(len(result["deliverables"]), 3)

    def test_github_review_completion_accepts_referenced_review(self):
        state = SimpleNamespace(
            database_path=self.database,
            task_id=1,
            phase=WorkflowPhase.EXTERNAL_ACTION,
        )
        origin = SimpleNamespace(
            kind="review_request",
            record_id="github.com/example-org/example-repo",
            item_id="42",
        )
        with mock.patch(
            "foxhound.execution_worker._repository_origin", return_value=origin,
        ):
            result = _repository_result(
                state,
                {
                    "outcome": "completed",
                    "deliverables": ["Review already posted externally"],
                    "repository_references": [{
                        "kind": "review",
                        "url": "https://github.com/example-org/example-repo/pull/42#pullrequestreview-1",
                    }],
                },
                self.run_directory,
            )
        self.assertEqual(result["outcome"], "completed")

    def test_github_review_completion_needs_review_receipt(self):
        state = SimpleNamespace(
            database_path=self.database,
            task_id=1,
            phase=WorkflowPhase.EXTERNAL_ACTION,
        )
        origin = SimpleNamespace(kind="review_request")
        _append_repository_receipt(self.run_directory, {
            "kind": "pull-request",
            "repository": "github.com/example-org/example-repo",
            "url": "https://github.com/example-org/example-repo/pull/42",
        })
        with mock.patch(
            "foxhound.execution_worker._repository_origin", return_value=origin,
        ):
            with self.assertRaisesRegex(
                ExecutionWorkerDraftError, "requires review receipt",
            ):
                _repository_result(
                    state, {"outcome": "completed", "deliverables": []},
                    self.run_directory,
                )

        _append_repository_receipt(self.run_directory, {
            "kind": "review",
            "repository": "github.com/example-org/example-repo",
            "url": "https://github.com/example-org/example-repo/pull/43#pullrequestreview-1",
        })
        with mock.patch(
            "foxhound.execution_worker._repository_origin", return_value=origin,
        ):
            result = _repository_result(
                state, {"outcome": "completed", "deliverables": []},
                self.run_directory,
            )
        self.assertEqual(len(result["deliverables"]), 2)

    def test_github_review_completion_accepts_issue_comment_receipt(self):
        state = SimpleNamespace(
            database_path=self.database,
            task_id=1,
            phase=WorkflowPhase.EXTERNAL_ACTION,
        )
        origin = SimpleNamespace(kind="review_request")
        _append_repository_receipt(self.run_directory, {
            "kind": "issue-comment",
            "repository": "github.com/example-org/example-repo",
            "url": "https://github.com/example-org/example-repo/issues/42#issuecomment-1",
        })
        with mock.patch(
            "foxhound.execution_worker._repository_origin", return_value=origin,
        ):
            result = _repository_result(
                state, {"outcome": "completed", "deliverables": []},
                self.run_directory,
            )
        self.assertEqual(len(result["deliverables"]), 1)

    def test_draft_cli_errors_are_content_free(self):
        private_value = "synthetic-private-outcome-value"
        self._write_result_inputs()
        output = StringIO()
        errors = StringIO()
        with knowledge_server() as endpoint:
            with redirect_stdout(output), redirect_stderr(errors):
                with mock.patch(
                    "foxhound.execution_worker.load_worker_from_environment",
                    return_value=self._worker(endpoint),
                ):
                    code = main(["draft", "--outcome", private_value])
        self.assertEqual(code, 65)
        self.assertEqual(output.getvalue(), "")
        self.assertNotIn(private_value, errors.getvalue())

    def test_invalid_or_permissive_drafts_write_nothing(self):
        draft = self._write_draft(claim_token=CLAIM_TOKEN)
        with knowledge_server() as endpoint:
            worker = self._worker(endpoint)
            with self.assertRaises(ExecutionWorkerDraftError):
                worker.record(draft.name)
        self.assertEqual(
            TaskExecutionService(self.database).get(1).status,
            WorkflowStatus.RUNNING,
        )

        draft = self._write_draft()
        draft.chmod(0o644)
        with knowledge_server() as endpoint:
            with self.assertRaises(ExecutionWorkerDraftError):
                self._worker(endpoint).record(draft.name)

    def test_state_symlinks_and_permissive_modes_are_refused(self):
        self.state_path.chmod(0o644)
        with self.assertRaises(ExecutionWorkerConfigError):
            load_run_state(self.state_path)
        self.state_path.chmod(0o600)
        alias = self.run_directory / "alias.json"
        alias.symlink_to(self.state_path)
        with self.assertRaises(ExecutionWorkerConfigError):
            load_run_state(alias)
        document = json.loads(self.state_path.read_text(encoding="utf-8"))
        document["agent_profile_revision"] = "Z" * 64
        self.state_path.write_text(json.dumps(document), encoding="utf-8")
        self.state_path.chmod(0o600)
        with self.assertRaises(ExecutionWorkerConfigError):
            load_run_state(self.state_path)

    def test_release_is_fenced_and_content_free(self):
        with knowledge_server() as endpoint:
            receipt = self._worker(endpoint).release()
        self.assertEqual(receipt["status"], "queued")
        self.assertNotIn(CLAIM_TOKEN, json.dumps(receipt))

    def test_plan_release_records_valid_result_inputs(self):
        self._write_result_inputs()
        service = TaskExecutionService(self.database)

        with knowledge_server() as endpoint:
            receipt = self._worker(endpoint).release()

        after = service.get(1)
        self.assertEqual(
            receipt["schema"], "foxhound.execution-result-receipt"
        )
        self.assertEqual(receipt["status"], "awaiting_review")
        self.assertEqual(after.status, WorkflowStatus.AWAITING_REVIEW)
        self.assertEqual(after.last_result_id, RUN_ID)

    def test_plan_release_leaves_invalid_result_inputs_for_correction(self):
        self._write_result_inputs()
        self._write_result_input("result-questions.json", [{"invalid": True}])
        service = TaskExecutionService(self.database)
        before = service.get(1)

        with knowledge_server() as endpoint:
            with self.assertRaisesRegex(
                ExecutionWorkerDraftError,
                "execution result questions is invalid",
            ):
                self._worker(endpoint).release()

        after = service.get(1)
        self.assertEqual(after.status, WorkflowStatus.RUNNING)
        self.assertEqual(after.version, before.version)

    def test_execute_release_refuses_to_discard_result_inputs(self):
        service = TaskExecutionService(
            self.database, token_factory=lambda: CLAIM_TOKEN
        )
        recorded = service.record_result(ExecutionResultEnvelope(
            result_id=RESULT_ID,
            task_id=1,
            task_version=1,
            workflow_version=self.claim.workflow_version,
            phase="plan",
            claim_token=CLAIM_TOKEN,
            outcome="awaiting_plan",
            summary="Synthetic result",
            work_markdown="Synthetic plan",
            deliverables=("Synthetic plan deliverable",),
        ))
        service.review_action(
            1, expected_version=recorded.version, action="approve"
        )
        self.claim = service.claim_next()
        self.assertEqual(self.claim.phase, WorkflowPhase.EXECUTE)
        self.state_path.unlink()
        self._write_state()
        self._write_result_inputs()

        with knowledge_server() as endpoint:
            with self.assertRaisesRegex(
                ExecutionWorkerDraftError,
                "result inputs must be drafted or removed",
            ):
                self._worker(endpoint).release()

        after = service.get(1)
        self.assertEqual(after.status, WorkflowStatus.RUNNING)
        self.assertEqual(after.version, self.claim.workflow_version)

    def test_cli_failure_does_not_echo_private_configuration(self):
        private_value = "synthetic-private-config-value"
        output = StringIO()
        errors = StringIO()
        with redirect_stdout(output), redirect_stderr(errors):
            with mock.patch.dict(os.environ, {
                "FOXHOUND_EXECUTION_STATE": private_value,
                "FOXHOUND_GW_ENDPOINT": "invalid",
                "FOXHOUND_GW_ALIAS": "primary",
                "FOXHOUND_GW_TOKEN_FILE": private_value,
            }, clear=True):
                code = main(["context"])
        self.assertEqual(code, 78)
        self.assertNotIn(private_value, output.getvalue() + errors.getvalue())

        output = StringIO()
        errors = StringIO()
        with redirect_stdout(output), redirect_stderr(errors):
            with mock.patch(
                "foxhound.execution_worker.load_worker_from_environment",
                side_effect=OSError(private_value),
            ):
                code = main(["context"])
        self.assertEqual(code, 70)
        self.assertNotIn(private_value, output.getvalue() + errors.getvalue())

    @mock.patch("subprocess.run")
    def test_act_mail_validates_arguments_and_fencing(self, run_mock):
        run_mock.return_value = mock.Mock(stdout="sent", stderr="")
        with knowledge_server() as endpoint:
            # Plan phase -> not allowed
            worker = self._worker(endpoint)
            with self.assertRaisesRegex(ExecutionWorkerClaimError, "external_action phase"):
                worker.act_mail(to="user@example.com", subject="S", body_file="b", attachments=None)

            # External action phase -> allowed
            state = replace(
                load_run_state(self.state_path),
                phase=WorkflowPhase.EXTERNAL_ACTION,
            )
            with mock.patch.object(worker, "_active", return_value=(state, SimpleNamespace())), mock.patch.object(worker, "_renew"):
                # Invalid recipient
                with self.assertRaisesRegex(ExecutionWorkerClaimError, "recipient address is invalid"):
                    worker.act_mail(to="invalid", subject="S", body_file="b", attachments=None)

                # Missing body
                with self.assertRaisesRegex(ExecutionWorkerDraftError, "message body: not found at"):
                    worker.act_mail(to="user@example.com", subject="S", body_file="missing.md", attachments=None)

                # Success path
                body_path = worker._state_path.parent / "body.md"
                body_path.write_text("mail content")
                body_path.chmod(0o600)
                result = worker.act_mail(to="user@example.com", subject="Success", body_file="body.md", attachments=None)
                self.assertEqual(result["kind"], "outbound-mail")
                self.assertEqual(result["to"], "user@example.com")
                run_mock.assert_called_once()
                args = run_mock.call_args[0][0]
                self.assertEqual(args[:6], ["outlook", "send", "--to", "user@example.com", "--subject", "Success"])

                # Attachment bounds validation
                run_mock.reset_mock()
                with self.assertRaisesRegex(ExecutionWorkerClaimError, "attachment must be a result artifact in the task folder"):
                    worker.act_mail(to="user@example.com", subject="S", body_file="body.md", attachments="../../secret.txt")

                att_path = worker._state_path.parent / "att.txt"
                att_path.write_text("data")
                att_path.chmod(0o600)
                worker.act_mail(to="user@example.com", subject="S", body_file="body.md", attachments="att.txt")
                args = run_mock.call_args[0][0]
                self.assertIn("--attachment", args)
                self.assertIn(str(att_path.resolve()), args)

    def test_act_comment_finds_body_file_in_task_work_directory(self):
        with knowledge_server() as endpoint:
            worker, active = self._effect_worker(
                endpoint, task_id=1, kind="issue", item_id="42",
            )
            task_dir = Path(self.temporary.name) / "task-sync"
            task_dir.mkdir(mode=0o700)
            body_file = task_dir / "status_update.txt"
            body_file.write_text("Work completed successfully.\n", encoding="utf-8")
            body_file.chmod(0o600)
            with active, mock.patch.object(worker, "_renew"), mock.patch.object(
                execution_worker.GwKnowledgeClient, "refresh_source",
                return_value=SimpleNamespace(status="current", usable=True),
            ), mock.patch.object(
                execution_worker.forge_action, "post_issue_comment",
                return_value=SimpleNamespace(
                    repository="github.com/example-org/example-repo",
                    number=42,
                    url="https://github.com/example-org/example-repo/issues/42",
                ),
            ) as post_comment:
                state = replace(
                    load_run_state(self.state_path),
                    task_id=1,
                    phase=WorkflowPhase.EXTERNAL_ACTION,
                    task_work_directory=str(task_dir),
                )
                with mock.patch.object(worker, "_active", return_value=(state, SimpleNamespace())):
                    result = worker.act_comment(body_file="status_update.txt")
                self.assertEqual(result["kind"], "issue-comment")
                post_comment.assert_called_once()
                self.assertEqual(post_comment.call_args.kwargs["body"], "Work completed successfully.\n")

    def test_act_comment_cli_missing_body_file_exits_with_code_65(self):
        output = StringIO()
        errors = StringIO()
        token_file = self.run_directory / "token.txt"
        token_file.write_text(TOKEN, encoding="utf-8")
        token_file.chmod(0o600)
        with knowledge_server() as endpoint:
            state = replace(
                load_run_state(self.state_path),
                phase=WorkflowPhase.EXTERNAL_ACTION,
            )
            self._bind_fresh_origin(1, kind="issue", item_id="42")
            with redirect_stdout(output), redirect_stderr(errors):
                with mock.patch.dict(os.environ, {
                    "FOXHOUND_EXECUTION_STATE": str(self.state_path),
                    "FOXHOUND_GW_ENDPOINT": endpoint,
                    "FOXHOUND_GW_ALIAS": "primary",
                    "FOXHOUND_GW_TOKEN_FILE": str(token_file),
                }, clear=True), mock.patch(
                    "foxhound.execution_worker.load_run_state",
                    return_value=state,
                ):
                    code = main(["act", "comment", "--body-file", "missing-body.txt"])
            self.assertEqual(code, 65)
            err = errors.getvalue()
            self.assertIn("issue comment body: not found at", err)
            self.assertNotIn("configuration unavailable", err)


class ResultLocationTests(unittest.TestCase):
    """A result is authored where the reader will look for it."""

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        root = Path(self.temporary.name)
        self.task = root / "task-folder"
        self.task_run = root / "task-run"
        self.run = root / "run-folder"
        for directory in (self.task, self.task_run, self.run):
            directory.mkdir(mode=0o700)
        self.addCleanup(self.temporary.cleanup)

    @staticmethod
    def _state(task_folder, task_run_folder=None):
        return types.SimpleNamespace(
            task_work_directory=task_folder,
            task_run_directory=task_run_folder,
        )

    def _write(self, directory, name, text="Synthetic result."):
        path = directory / name
        path.write_text(text, encoding="utf-8")
        path.chmod(0o600)
        return path

    def test_the_task_folder_is_searched_before_the_run_directory(self):
        order = execution_worker._result_search_path(
            self._state(str(self.task)), self.run)
        self.assertEqual(order, (self.task, self.run))

    def test_the_task_run_directory_is_searched_before_task_folder(self):
        order = execution_worker._result_search_path(
            self._state(str(self.task), str(self.task_run)), self.run)
        self.assertEqual(order, (self.task_run, self.task, self.run))

    def test_a_result_in_the_task_run_directory_is_found(self):
        expected = self._write(self.task_run, "result-summary.txt")
        order = execution_worker._result_search_path(
            self._state(str(self.task), str(self.task_run)), self.run)

        self.assertEqual(
            execution_worker._locate_result(
                order, "result-summary.txt",
                task_folder_not_before=expected.stat().st_mtime_ns - 1,
            ),
            expected,
        )

    def test_a_result_in_the_task_folder_is_found(self):
        expected = self._write(self.task, "result-summary.txt")
        order = execution_worker._result_search_path(
            self._state(str(self.task)), self.run)

        self.assertEqual(
            execution_worker._locate_result(
                order, "result-summary.txt",
                task_folder_not_before=expected.stat().st_mtime_ns - 1,
            ),
            expected,
        )

    def test_a_stale_task_result_is_not_returned_as_the_path_to_read(self):
        """Fencing a candidate and then reporting it is not fencing it.

        Nothing else stands between this path and the readers: whatever comes
        back here is opened and recorded.
        """
        stale = self._write(self.task, "result-summary.txt")
        order = execution_worker._result_search_path(
            self._state(str(self.task)), self.run)

        located = execution_worker._locate_result(
            order, "result-summary.txt",
            task_folder_not_before=stale.stat().st_mtime_ns + 1,
        )

        self.assertNotEqual(located, stale)
        self.assertEqual(located, self.run / "result-summary.txt")

    def test_an_unreadable_anchor_fences_the_shared_folder(self):
        """No anchor means no way to prove freshness, so nothing shared wins."""
        self._write(self.task, "result-summary.txt")
        order = execution_worker._result_search_path(
            self._state(str(self.task)), self.run)

        self.assertEqual(
            execution_worker._locate_result(order, "result-summary.txt"),
            self.run / "result-summary.txt",
        )

    def test_a_result_in_the_run_directory_still_records(self):
        """Nothing that already records may stop recording."""
        expected = self._write(self.run, "result-summary.txt")
        order = execution_worker._result_search_path(
            self._state(str(self.task)), self.run)

        self.assertEqual(
            execution_worker._locate_result(order, "result-summary.txt"),
            expected,
        )

    def test_a_workflow_without_a_task_folder_uses_the_run_directory(self):
        order = execution_worker._result_search_path(self._state(None), self.run)
        self.assertEqual(order, (self.run,))

    def test_a_relative_task_folder_is_refused_rather_than_guessed(self):
        order = execution_worker._result_search_path(
            self._state("relative/path"), self.run)
        self.assertEqual(order, (self.run,))

    def test_a_missing_result_names_the_path_it_looked_at(self):
        """"invalid" sent an agent reading worker source; "not found" does not."""
        missing = self.task / "result-summary.txt"
        reason = execution_worker._result_read_failure(
            missing, Exception("unavailable"))

        self.assertIn("not found", reason)
        self.assertIn(str(missing), reason)

    def test_a_world_readable_result_says_so_and_says_how_to_fix_it(self):
        exposed = self._write(self.task, "result-summary.txt")
        exposed.chmod(0o644)

        reason = execution_worker._result_read_failure(
            exposed, Exception("not private"))

        self.assertIn("readable by others", reason)
        self.assertIn("600", reason)

    def test_the_reported_path_prefers_where_the_reader_was_aiming(self):
        """With nothing written anywhere, name the task folder, not scratch."""
        order = execution_worker._result_search_path(
            self._state(str(self.task)), self.run)

        self.assertEqual(
            execution_worker._locate_result(order, "result-summary.txt"),
            self.task / "result-summary.txt",
        )


class BodyFileLocationTests(unittest.TestCase):
    """An action body file can be authored in the run folder or the task folders."""

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        root = Path(self.temporary.name)
        self.task_work = root / "task-work"
        self.task_run = root / "task-run"
        self.run = root / "run-scratch"
        for directory in (self.task_work, self.task_run, self.run):
            directory.mkdir(mode=0o700)
        self.addCleanup(self.temporary.cleanup)

    @staticmethod
    def _state(task_work_directory=None, task_run_directory=None):
        return types.SimpleNamespace(
            task_work_directory=task_work_directory,
            task_run_directory=task_run_directory,
        )

    def _write(self, directory, name, text="Synthetic body text.\n"):
        path = directory / name
        path.write_text(text, encoding="utf-8")
        path.chmod(0o600)
        return path

    def test_search_path_includes_run_scratch_and_task_directories(self):
        state = self._state(str(self.task_work), str(self.task_run))
        order = execution_worker._body_search_path(state, self.run)
        self.assertIn(self.run, order)
        self.assertIn(self.task_run, order)
        self.assertIn(self.task_work, order)

    def test_locates_body_file_in_run_scratch_directory(self):
        expected = self._write(self.run, "comment.md")
        state = self._state(str(self.task_work), str(self.task_run))
        order = execution_worker._body_search_path(state, self.run)
        self.assertEqual(execution_worker._locate_body_file(order, "comment.md"), expected)

    def test_locates_body_file_in_task_run_directory(self):
        expected = self._write(self.task_run, "comment.md")
        state = self._state(str(self.task_work), str(self.task_run))
        order = execution_worker._body_search_path(state, self.run)
        self.assertEqual(execution_worker._locate_body_file(order, "comment.md"), expected)

    def test_locates_body_file_in_task_work_directory(self):
        expected = self._write(self.task_work, "comment.md")
        state = self._state(str(self.task_work), str(self.task_run))
        order = execution_worker._body_search_path(state, self.run)
        self.assertEqual(execution_worker._locate_body_file(order, "comment.md"), expected)

    def test_locates_body_file_with_absolute_path(self):
        other = Path(self.temporary.name) / "other"
        other.mkdir(mode=0o700)
        expected = self._write(other, "comment.md")
        state = self._state(str(self.task_work), str(self.task_run))
        order = execution_worker._body_search_path(state, self.run)
        self.assertEqual(execution_worker._locate_body_file(order, str(expected)), expected)

    def test_missing_body_file_returns_primary_expected_path(self):
        state = self._state(str(self.task_work), str(self.task_run))
        order = execution_worker._body_search_path(state, self.run)
        located = execution_worker._locate_body_file(order, "missing.md")
        self.assertEqual(located, self.run / "missing.md")

    def test_read_body_text_reads_and_validates_content(self):
        path = self._write(self.run, "comment.md", "Approved action body.\n")
        content = execution_worker._read_body_text(path, label="issue comment body")
        self.assertEqual(content, "Approved action body.\n")

    def test_read_body_text_refuses_empty_when_required(self):
        path = self._write(self.run, "empty.md", "   \n")
        with self.assertRaisesRegex(ExecutionWorkerDraftError, "issue comment body is empty"):
            execution_worker._read_body_text(path, label="issue comment body")

    def test_read_body_text_reports_not_found_with_informative_draft_error(self):
        missing = self.run / "nonexistent.md"
        with self.assertRaises(ExecutionWorkerDraftError) as cm:
            execution_worker._read_body_text(missing, label="issue comment body")
        self.assertIn("issue comment body: not found at", str(cm.exception))
        self.assertIn("nonexistent.md", str(cm.exception))


class ThreadReadTests(unittest.TestCase):
    """Tests for the read_thread operation on the execution worker."""

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
        service = TaskExecutionService(
            self.database, token_factory=lambda: CLAIM_TOKEN
        )
        scheduled = service.schedule(1, expected_task_version=1)
        service.start_action(
            1, expected_version=scheduled.version, action="start"
        )
        self.claim = service.claim_next()
        self.assertIsNotNone(self.claim)
        self.run_directory = self.root / f"run-{RUN_ID}"
        self.knowledge_root = self.root / "knowledge"
        self.knowledge_root.mkdir()
        self.run_directory.mkdir(mode=0o700)
        self.state_path = self.run_directory / "run-state.json"
        self._write_state()
        self._write_instructions()

    def _write_state(self, *, schema_version: int = 5, **kw) -> None:
        document = {
            "schema": "foxhound.execution-run-state",
            "schema_version": schema_version,
            "run_id": RUN_ID,
            "database_path": str(self.database),
            "task_id": 1,
            "task_version": 1,
            "workflow_version": self.claim.workflow_version,
            "phase": self.claim.phase.value,
            "claim_token": CLAIM_TOKEN,
            "lease_seconds": self.claim.lease_seconds,
            "agent_profile_id": self.claim.agent_profile_id,
            "agent_profile_revision": self.claim.agent_profile_revision,
            "knowledge_root": str(self.knowledge_root),
            "worker_command": WORKER_COMMAND,
            "task_work_directory": None,
            "task_kb_file": None,
            "task_run_directory": None,
        }
        if schema_version >= 5:
            document.update({
                "execution_grants": [],
                "action_grants": [],
            })
        document.update(kw)
        self.state_path.write_text(json.dumps(document), encoding="utf-8")
        self.state_path.chmod(0o600)

    def _write_instructions(self) -> None:
        path = self.run_directory / INSTRUCTIONS_NAME
        path.write_text(json.dumps(general_profile().document()), encoding="utf-8")
        path.chmod(0o600)

    def _bind_origin(self, kind: str, item_id: str = "42") -> None:
        with closing(sqlite3.connect(self.database)) as connection:
            connection.execute(
                "INSERT INTO candidate_inbox(candidate_id,source_system,"
                "source_kind,source_record_id,source_item_id,source_revision,"
                "payload_json,created_at,first_imported_at,updated_at) "
                "VALUES('candidate-1','gw',?,"
                "'github.com/example-org/example-repo',?,?,'{}',"
                "'2030-01-02T03:04:05+00:00','2030-01-02T03:04:05+00:00',"
                "'2030-01-02T03:04:05+00:00')",
                (kind, item_id, "a" * 64),
            )
            connection.execute(
                "INSERT INTO task_candidate_bindings(candidate_id,"
                "source_revision,task_id,relation,decided_at) "
                "VALUES('candidate-1',?,1,'accepted',?)",
                ("a" * 64, "2030-01-02T03:04:05+00:00"),
            )
            connection.commit()

    def _worker(self, endpoint: str) -> ExecutionWorker:
        return ExecutionWorker(self.state_path, KnowledgeClientConfig(
            endpoint=endpoint, alias="primary", token=TOKEN
        ))

    def test_thread_operation_is_available_in_plan(self):
        from foxhound.execution_worker import _worker_operations
        ops = _worker_operations(WorkflowPhase.PLAN)
        self.assertIn("thread", ops)

    def test_thread_operation_is_available_in_execute(self):
        from foxhound.execution_worker import _worker_operations
        ops = _worker_operations(WorkflowPhase.EXECUTE)
        self.assertIn("thread", ops)

    def test_thread_operation_is_not_available_in_external_action(self):
        from foxhound.execution_worker import _worker_operations
        ops = _worker_operations(WorkflowPhase.EXTERNAL_ACTION)
        self.assertNotIn("thread", ops)

    def test_no_origin_refuses_thread_read(self):
        with (\
            mock.patch(
                "foxhound.execution_worker._local_today",
                return_value="2030-01-02",
            ),
            knowledge_server() as endpoint,
        ):
            worker = self._worker(endpoint)
            with self.assertRaises(ExecutionWorkerClaimError) as caught:
                worker.read_thread()
        self.assertIn("no origin", str(caught.exception))

    def test_no_prior_thread_returns_empty(self):
        # A task with no prior comments returns an empty thread
        self._bind_origin("issue")
        issue_data = {
            "number": 42,
            "url": "https://github.com/example-org/example-repo/issues/42",
            "state": "open",
            "comments": {"nodes": [], "pageInfo": {"hasNextPage": False}},
        }

        def fake_run(*args, **kw):
            if args[:2] == ("gh", "api"):
                return (0, json.dumps({
                    "data": {"repository": {"issue": issue_data}}
                }), "")
            return (1, "", "unexpected")

        with (\
            mock.patch("foxhound.execution_worker._local_today",
                       return_value="2030-01-02"),
            mock.patch("foxhound.forge_thread._run", fake_run),
            knowledge_server() as endpoint,
        ):
            worker = self._worker(endpoint)
            result = worker.read_thread()

        self.assertEqual(result["kind"], "issue")
        self.assertEqual(result["number"], 42)
        self.assertEqual(result["comments"], [])
        self.assertFalse(result["truncated"])

    def test_thread_with_review_comments(self):
        # A task with review comments returns them
        self._bind_origin("issue")
        issue_data = {
            "number": 42,
            "url": "https://github.com/example-org/example-repo/issues/42",
            "state": "open",
            "comments": {
                "nodes": [
                    {
                        "author": {"login": "reviewer-a"},
                        "body": "The implementation adds a module the issue asked not to add.",
                        "createdAt": "2030-01-02T10:00:00Z",
                    },
                ],
                "pageInfo": {"hasNextPage": False},
            },
        }

        def fake_run(*args, **kw):
            if args[:2] == ("gh", "api"):
                return (0, json.dumps({
                    "data": {"repository": {"issue": issue_data}}
                }), "")
            return (1, "", "unexpected")

        with (\
            mock.patch("foxhound.execution_worker._local_today",
                       return_value="2030-01-02"),
            mock.patch("foxhound.forge_thread._run", fake_run),
            knowledge_server() as endpoint,
        ):
            worker = self._worker(endpoint)
            result = worker.read_thread()

        self.assertEqual(result["kind"], "issue")
        self.assertEqual(len(result["comments"]), 1)
        self.assertEqual(result["comments"][0]["author"], "reviewer-a")
        self.assertEqual(result["comment_count"], 1)

    def test_pr_thread_with_reviews(self):
        # A review_request task reads PR reviews
        self._bind_origin("review_request", item_id="7/2030-01-02")
        pr_data = {
            "number": 7,
            "url": "https://github.com/example-org/example-repo/pull/7",
            "state": "open",
            "reviews": {
                "nodes": [
                    {
                        "author": {"login": "reviewer-a"},
                        "state": "COMMENTED",
                        "body": "Please rebase on main.",
                        "createdAt": "2030-01-02T10:00:00Z",
                    },
                ],
            },
            "comments": {
                "nodes": [
                    {
                        "author": {"login": "author-x"},
                        "body": "I will address the feedback.",
                        "createdAt": "2030-01-02T11:00:00Z",
                    },
                ],
            },
        }

        def fake_run(*args, **kw):
            if args[:2] == ("gh", "api"):
                return (0, json.dumps({
                    "data": {"repository": {"pullRequest": pr_data}}
                }), "")
            return (1, "", "unexpected")

        with (\
            mock.patch("foxhound.execution_worker._local_today",
                       return_value="2030-01-02"),
            mock.patch("foxhound.forge_thread._run", fake_run),
            knowledge_server() as endpoint,
        ):
            worker = self._worker(endpoint)
            result = worker.read_thread()

        self.assertEqual(result["kind"], "pull-request")
        self.assertEqual(result["number"], 7)
        self.assertEqual(len(result["reviews"]), 1)
        self.assertEqual(result["reviews"][0]["author"], "reviewer-a")
        self.assertEqual(result["review_count"], 1)


if __name__ == "__main__":
    unittest.main()
