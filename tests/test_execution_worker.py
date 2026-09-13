#!/usr/bin/env python3
"""Synthetic tests for the narrow execution-agent worker boundary."""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import tempfile
import threading
import unittest
from contextlib import closing, contextmanager, redirect_stderr, redirect_stdout
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from io import StringIO
from pathlib import Path
from typing import Iterator
from unittest import mock

from foxhound.candidate_inbox import CandidateInbox
from foxhound.execution_worker import (
    ExecutionWorker,
    ExecutionWorkerConfigError,
    ExecutionWorkerDraftError,
    load_run_state,
    main,
)
from foxhound.knowledge_client import KnowledgeClientConfig
from foxhound.task_execution import TaskExecutionService, WorkflowStatus


TOKEN = "synthetic-knowledge-token-with-sufficient-length"
CLAIM_TOKEN = "synthetic-claim-token-000000000000000000000000"
RUN_ID = "a" * 32
RESULT_ID = "b" * 32


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


class ExecutionWorkerTests(unittest.TestCase):
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
        self.run_directory.mkdir(mode=0o700)
        self.state_path = self.run_directory / "run-state.json"
        self._write_state()

    def _write_state(self) -> None:
        self.state_path.write_text(json.dumps({
            "schema": "foxhound.execution-run-state",
            "schema_version": 2,
            "run_id": RUN_ID,
            "database_path": str(self.database),
            "task_id": 1,
            "task_version": 1,
            "workflow_version": self.claim.workflow_version,
            "phase": "plan",
            "claim_token": CLAIM_TOKEN,
            "lease_seconds": self.claim.lease_seconds,
            "agent_profile_id": self.claim.agent_profile_id,
            "agent_profile_revision": self.claim.agent_profile_revision,
        }), encoding="utf-8")
        self.state_path.chmod(0o600)

    def _worker(self, endpoint: str) -> ExecutionWorker:
        return ExecutionWorker(self.state_path, KnowledgeClientConfig(
            endpoint=endpoint, alias="primary", token=TOKEN
        ))

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

    def test_context_and_search_are_bounded_and_hide_the_capability(self):
        with knowledge_server() as endpoint:
            worker = self._worker(endpoint)
            context = worker.context()
            result = worker.search("synthetic query", max_results_per_layer=2)

        rendered = json.dumps({"context": context, "search": result})
        self.assertNotIn(CLAIM_TOKEN, rendered)
        self.assertNotIn(str(self.database), rendered)
        self.assertEqual(context["task"]["text"], "Synthetic task")
        self.assertEqual(context["schema_version"], 2)
        self.assertEqual(context["workflow"]["phase"], "plan")
        self.assertEqual(
            context["workflow"]["agent_profile_id"],
            self.claim.agent_profile_id,
        )
        self.assertEqual(
            context["workflow"]["agent_profile_revision"],
            self.claim.agent_profile_revision,
        )
        self.assertEqual(context["operator"]["display_name"], "Person A")
        self.assertEqual(result["layers"][0]["documents"][0]["excerpt"],
                         "Synthetic evidence.")
        self.assertNotIn(CLAIM_TOKEN, repr(load_run_state(self.state_path)))

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
            receipt = self._worker(endpoint).record(draft.name)

        state = TaskExecutionService(self.database).get(1)
        self.assertEqual(state.status, WorkflowStatus.AWAITING_REVIEW)
        self.assertEqual(state.last_result_id, RESULT_ID)
        self.assertEqual(receipt["status"], "awaiting_review")
        scrubbed = draft.read_text(encoding="utf-8")
        self.assertNotIn(CLAIM_TOKEN, scrubbed)
        self.assertNotIn("Synthetic result summary", scrubbed)
        self.assertNotIn("work_markdown", scrubbed)

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
        ):
            self.assertFalse((self.run_directory / name).exists())

    def test_draft_rejects_invalid_inputs_before_writing(self):
        self._write_result_inputs()
        with knowledge_server() as endpoint:
            worker = self._worker(endpoint)
            with self.assertRaises(ExecutionWorkerDraftError):
                worker.draft(outcome="awaiting_external")
            self.assertEqual(list(self.run_directory.glob("result-*.json")), [])

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

        self.assertEqual(list(self.run_directory.glob("result-*.json")), [])

    def test_draft_defaults_missing_collection_files_to_empty_arrays(self):
        self._write_result_inputs()
        with knowledge_server() as endpoint:
            ready = self._worker(endpoint).draft(outcome="awaiting_plan")
        document = json.loads(
            (self.run_directory / ready["draft"]).read_text(encoding="utf-8")
        )
        for name in ("questions", "external_actions", "deliverables"):
            self.assertEqual(document[name], [])

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


if __name__ == "__main__":
    unittest.main()
