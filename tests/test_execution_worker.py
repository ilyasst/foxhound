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

from foxhound.agent_profiles import general_profile
from foxhound.candidate_inbox import CandidateInbox
from foxhound.execution_worker import (
    INSTRUCTIONS_NAME,
    ExecutionWorker,
    ExecutionWorkerConfigError,
    ExecutionWorkerDraftError,
    load_result_draft,
    load_run_state,
    main,
    _local_calendar,
    _worker_operations,
)
from foxhound.knowledge_client import KnowledgeClientConfig
from foxhound.task_execution import (
    ExecutionResultEnvelope,
    TaskExecutionService,
    WorkflowPhase,
    WorkflowStatus,
)


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
        self.knowledge_root = self.root / "knowledge"
        self.knowledge_root.mkdir()
        self.run_directory.mkdir(mode=0o700)
        self.state_path = self.run_directory / "run-state.json"
        self._write_state()
        self._write_instructions()

    def _write_state(self) -> None:
        self.state_path.write_text(json.dumps({
            "schema": "foxhound.execution-run-state",
            "schema_version": 3,
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
        }), encoding="utf-8")
        self.state_path.chmod(0o600)

    def _write_instructions(self, document: object | None = None) -> Path:
        path = self.run_directory / INSTRUCTIONS_NAME
        if document is None:
            document = general_profile().document()
        path.write_text(json.dumps(document), encoding="utf-8")
        path.chmod(0o600)
        return path

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
        with mock.patch(
            "foxhound.execution_worker._local_today",
            return_value="2030-01-02",
        ):
            with knowledge_server() as endpoint:
                worker = self._worker(endpoint)
                context = worker.context()
                result = worker.search(
                    "synthetic query", max_results_per_layer=2
                )

        rendered = json.dumps({"context": context, "search": result})
        self.assertNotIn(CLAIM_TOKEN, rendered)
        self.assertNotIn(str(self.database), rendered)
        self.assertEqual(context["task"]["text"], "Synthetic task")
        self.assertEqual(context["schema_version"], 4)
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
                "worker_operations": [
                    "context", "search", "draft", "record", "release"
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
        self.assertEqual(context["operator"]["display_name"], "Person A")
        self.assertEqual(result["layers"][0]["documents"][0]["excerpt"],
                         "Synthetic evidence.")
        self.assertNotIn(CLAIM_TOKEN, repr(load_run_state(self.state_path)))

    def test_worker_capabilities_follow_the_phase_gate(self):
        self.assertNotIn(
            "act.worktree", _worker_operations(WorkflowPhase.PLAN)
        )
        self.assertIn(
            "act.worktree", _worker_operations(WorkflowPhase.EXECUTE)
        )
        external = _worker_operations(WorkflowPhase.EXTERNAL_ACTION)
        self.assertIn("act.worktree", external)
        self.assertIn("act.pull-request", external)

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


if __name__ == "__main__":
    unittest.main()
