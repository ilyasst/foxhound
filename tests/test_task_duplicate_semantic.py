#!/usr/bin/env python3
"""Synthetic tests for the local-only duplicate evaluator."""

from __future__ import annotations

import io
import json
import os
import sqlite3
import tempfile
import http.server
import threading
import urllib.request
import unittest
from contextlib import closing, redirect_stdout
from pathlib import Path

from foxhound import migrate_database
from foxhound import task_duplicate_proposals as proposals
from foxhound import task_duplicate_quality as quality
from foxhound import task_duplicate_semantic as semantic
from foxhound.candidate_inbox import SCHEMA_VERSION


NOW = "2030-03-01T12:00:00+00:00"


class _Response:
    def __init__(self, document: object) -> None:
        self._body = json.dumps(document).encode("utf-8")

    def __enter__(self):
        return self

    def __exit__(self, *_args) -> None:
        return None

    def read(self, _maximum: int) -> bytes:
        return self._body


class _Opener:
    def __init__(self, document: object) -> None:
        self.document = document
        self.requests = []

    def open(self, request, *, timeout: float):
        self.requests.append((request, timeout))
        return _Response(self.document)


class LocalSemanticEvaluationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.database = Path(self.directory.name) / "foxhound.sqlite3"
        migrate_database(self.database)
        self.connection = sqlite3.connect(self.database)
        self.connection.row_factory = sqlite3.Row
        self.addCleanup(self.connection.close)
        self._task(1, "Prepare the synthetic launch checklist")
        self._task(2, "Draft the synthetic launch checklist")
        self.connection.commit()

    def _task(self, task_id: int, text: str) -> None:
        revision = f"{task_id:064x}"
        self.connection.execute(
            "INSERT INTO tasks(id,status,text,version,created_at,updated_at,"
            "owner_ref_version,owner_kind,owner_speaker_id,"
            "owner_canonical_speaker_id,owner_speaker_registry_id,"
            "owner_pinned,owner_provisional) VALUES(?, 'open', ?, 1, ?, ?,"
            "1, 'person', 'SPK_1', 'SPK_1', 'registry', 0, 0)",
            (task_id, text, NOW, NOW),
        )
        self.connection.execute(
            "INSERT INTO candidate_inbox(candidate_id,source_system,"
            "source_kind,source_record_id,source_item_id,source_revision,"
            "payload_json,created_at,first_imported_at,updated_at) VALUES(?,"
            "'synthetic', 'note', ?, ?, ?, '{}', ?, ?, ?)",
            (f"candidate-{task_id}", f"record-{task_id}", str(task_id),
             revision, NOW, NOW, NOW),
        )
        self.connection.execute(
            "INSERT INTO task_candidate_bindings(candidate_id,source_revision,"
            "task_id,relation,decided_at) VALUES(?,?,?,'accepted',?)",
            (f"candidate-{task_id}", revision, task_id, NOW),
        )

    def _opener(self, verdict: str = "redundant") -> _Opener:
        """A gateway reply in the default dialect."""
        return _Opener({"choices": [{"message": {"content": json.dumps({
            "judgements": [{"task_id": 2, "verdict": verdict}],
        })}}], "usage": {"prompt_tokens": 12, "completion_tokens": 3}})

    def _runner_opener(self, verdict: str = "redundant") -> _Opener:
        """The same answer in the single-machine runner dialect."""
        return _Opener({"message": {"content": json.dumps({
            "judgements": [{"task_id": 2, "verdict": verdict}],
        })}, "prompt_eval_count": 12, "eval_count": 3})

    def test_dry_run_uses_loopback_model_without_writing_an_assessment(self) -> None:
        opener = self._opener()

        result = semantic.scan_database(
            self.database, model="synthetic-local", now=NOW, opener=opener,
        )

        self.assertEqual((result.pairs_retrieved, result.model_requests,
                          result.redundant, result.prompt_tokens), (1, 1, 1, 12))
        self.assertEqual(len(opener.requests), 1)
        self.assertEqual(opener.requests[0][0].host, "127.0.0.1:8800")
        self.assertEqual(
            opener.requests[0][0].selector, "/v1/chat/completions")
        with closing(sqlite3.connect(self.database)) as connection:
            rows = connection.execute(
                "SELECT count(*) FROM task_duplicate_assessments"
            ).fetchone()[0]
        self.assertEqual(rows, 0)

    def test_record_requires_every_existing_proposal_to_be_settled(self) -> None:
        proposals.propose(
            self.connection, task_id_a=1, task_id_b=2, basis="synthetic basis",
            detector="term-overlap", now=NOW,
        )
        self.connection.commit()
        opener = self._opener()

        with self.assertRaises(semantic.EvaluationReadinessError):
            semantic.scan_database(
                self.database, model="synthetic-local", now=NOW,
                record=True, opener=opener,
            )
        self.assertEqual(opener.requests, [])

    def test_recorded_assessment_is_scored_against_a_settled_reader_label(self) -> None:
        proposal = proposals.propose(
            self.connection, task_id_a=1, task_id_b=2,
            basis="shared task terms across note and note: checklist, launch",
            detector="term-overlap", now=NOW,
        )
        self.assertTrue(proposals.settle(
            self.connection, proposal_id=proposal.proposal_id,
            decision=proposals.Decision.CONFIRMED, actor="reader", now=NOW,
        ))
        self.connection.commit()

        result = semantic.scan_database(
            self.database, model="synthetic-local", now=NOW, record=True,
            opener=self._opener(),
        )
        self.assertEqual((result.assessments_recorded, result.assessments_unchanged),
                         (1, 0))
        lines = {line["detector"]: line for line in quality.report(self.database)}
        line = lines[semantic.DETECTOR]
        self.assertEqual((line["confirmed"], line["label_count"],
                          line["redundant"], line["cost_usd"]), (1, 1, 1, 0))
        self.assertEqual(line["not_redundant_with_term_overlap"], 0)

    def test_explicit_option_creates_a_reader_gated_local_detector_proposal(self) -> None:
        result = semantic.scan_database(
            self.database, model="synthetic-local", now=NOW, record=True,
            propose_redundant=True, opener=self._opener(),
        )
        self.assertEqual((result.assessments_recorded, result.proposals_recorded),
                         (1, 1))
        proposal = self.connection.execute(
            "SELECT detector,state FROM task_duplicate_proposals"
        ).fetchone()
        self.assertEqual(tuple(proposal), (semantic.DETECTOR, "proposed"))

    def test_proposal_option_requires_recording(self) -> None:
        stream = io.StringIO()
        with redirect_stdout(stream):
            code = semantic.main([
                "--database", str(self.database), "--model", "synthetic-local",
                "--propose-redundant",
            ])
        self.assertEqual(code, 2)
        self.assertEqual(json.loads(stream.getvalue()), {"accepted": False})

    def test_version_forty_two_migrates_the_immutable_assessment_ledger(self) -> None:
        self.connection.close()
        with closing(sqlite3.connect(self.database)) as connection:
            connection.execute("DROP TRIGGER task_duplicate_assessments_no_update")
            connection.execute("DROP TRIGGER task_duplicate_assessments_no_delete")
            connection.execute("DROP INDEX task_duplicate_assessments_pair")
            connection.execute("DROP TABLE task_duplicate_assessments")
            connection.execute("PRAGMA user_version = 42")
            connection.commit()
        migrate_database(self.database)
        with closing(sqlite3.connect(self.database)) as connection:
            version = connection.execute("PRAGMA user_version").fetchone()[0]
            table = connection.execute(
                "SELECT type FROM sqlite_master WHERE name='task_duplicate_assessments'"
            ).fetchone()[0]
        self.assertEqual((version, table), (SCHEMA_VERSION, "table"))

    def test_invalid_model_reply_leaves_recording_untouched(self) -> None:
        self.connection.commit()
        opener = _Opener({"message": {"content": "{}"}})
        with self.assertRaises(semantic.LocalModelError):
            semantic.scan_database(
                self.database, model="synthetic-local", now=NOW,
                opener=opener,
            )
        with closing(sqlite3.connect(self.database)) as connection:
            self.assertEqual(connection.execute(
                "SELECT count(*) FROM task_duplicate_assessments"
            ).fetchone()[0], 0)

    def test_command_refuses_public_or_hosted_endpoints_without_echoing_input(self) -> None:
        stream = io.StringIO()
        with redirect_stdout(stream):
            code = semantic.main([
                "--database", str(self.database), "--model", "synthetic-local",
                "--endpoint", "https://example.com/model",
            ])
        self.assertEqual(code, 2)
        self.assertEqual(json.loads(stream.getvalue()), {"accepted": False})


if __name__ == "__main__":
    unittest.main()


class LoopbackStaysLoopbackTests(unittest.TestCase):
    """A validated loopback endpoint must stay loopback for the exchange."""

    def _server(self, handler_cls):
        server = http.server.HTTPServer(("127.0.0.1", 0), handler_cls)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(server.shutdown)
        self.addCleanup(server.server_close)
        return server

    def test_a_redirect_away_from_the_endpoint_is_refused(self):
        reached = []

        class Elsewhere(http.server.BaseHTTPRequestHandler):
            def do_POST(self):  # noqa: N802
                reached.append(self.path)
                self.send_response(200)
                self.end_headers()

            def log_message(self, *arguments):
                return

        target = self._server(Elsewhere)
        elsewhere = f"http://127.0.0.1:{target.server_port}/api/chat"

        class Redirector(http.server.BaseHTTPRequestHandler):
            def do_POST(self):  # noqa: N802
                self.send_response(302)
                self.send_header("Location", elsewhere)
                self.end_headers()

            def log_message(self, *arguments):
                return

        redirector = self._server(Redirector)

        with self.assertRaises(semantic.LocalModelError):
            semantic._classify(
                "synthetic task", ((2, "synthetic other"),),
                model="synthetic-local",
                endpoint=f"http://127.0.0.1:{redirector.server_port}",
            )
        # The point of the test: the second server never heard from us.
        self.assertEqual(reached, [])

    def test_a_proxy_in_the_environment_is_ignored(self):
        """Passing an empty ProxyHandler suppresses the default one.

        It is never registered as a handler, so asserting on the handler list
        proves nothing; what matters is that a proxy variable pointing at a
        dead port does not stop a loopback request from arriving.
        """
        seen = []

        class Chat(http.server.BaseHTTPRequestHandler):
            def do_POST(self):  # noqa: N802
                seen.append(self.path)
                body = json.dumps({"choices": [{"message": {"content": json.dumps(
                    {"judgements": [{"task_id": 2, "verdict": "interconnected"}]}
                )}}]}).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *arguments):
                return

        server = self._server(Chat)
        # A proxy that would fail loudly if it were consulted.
        for name in ("http_proxy", "HTTP_PROXY", "all_proxy", "ALL_PROXY"):
            self.addCleanup(os.environ.pop, name, None)
            os.environ[name] = "http://127.0.0.1:9"

        semantic._classify(
            "synthetic task", ((2, "synthetic other"),),
            model="synthetic-local",
            endpoint=f"http://127.0.0.1:{server.server_port}",
        )

        self.assertEqual(seen, ["/v1/chat/completions"])


class DialectTests(unittest.TestCase):
    """Two gateways, two shapes, one scanner."""

    def test_the_runner_dialect_still_works(self):
        reply = {"message": {"content": "X"},
                 "prompt_eval_count": 12, "eval_count": 3}
        runner = semantic.DIALECTS["runner"]
        self.assertEqual(runner.path, "/api/chat")
        self.assertEqual(runner.content(reply), "X")
        self.assertEqual(runner.usage(reply), (12, 3))

    def test_the_gateway_dialect_reads_the_first_choice(self):
        reply = {"choices": [{"message": {"content": "X"}}],
                 "usage": {"prompt_tokens": 30, "completion_tokens": 7}}
        gateway = semantic.DIALECTS["openai"]
        self.assertEqual(gateway.path, "/v1/chat/completions")
        self.assertEqual(gateway.content(reply), "X")
        self.assertEqual(gateway.usage(reply), (30, 7))

    def test_a_reply_with_no_usable_choice_is_refused(self):
        gateway = semantic.DIALECTS["openai"]
        for reply in ({}, {"choices": None}, {"choices": []},
                      {"choices": [{}]}, {"choices": ["not an object"]},
                      "not an object"):
            with self.subTest(reply=reply):
                with self.assertRaises(semantic.LocalModelError):
                    gateway.content(reply)

    def test_absent_usage_counts_as_nothing_rather_than_failing(self):
        """A gateway that omits usage must not fail the scan."""
        gateway = semantic.DIALECTS["openai"]
        self.assertEqual(gateway.usage({"choices": []}), (0, 0))
        self.assertEqual(gateway.usage({"usage": "not an object"}), (0, 0))

    def test_an_unknown_dialect_is_refused(self):
        with self.assertRaises(semantic.LocalModelError):
            semantic._dialect("smoke-signals")
        with self.assertRaises(semantic.LocalModelError):
            semantic._dialect(None)

    def test_the_gateway_request_asks_for_json_at_zero_temperature(self):
        body = semantic.DIALECTS["openai"].body("light", "SYS", "USER")
        self.assertEqual(body["model"], "light")
        self.assertEqual(body["temperature"], 0)
        self.assertEqual(body["response_format"], {"type": "json_object"})
        self.assertEqual([m["role"] for m in body["messages"]],
                         ["system", "user"])
