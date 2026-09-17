from __future__ import annotations

import hashlib
import json
import os
import tempfile
import threading
import time
import unittest
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Iterator

from foxhound import (
    GwKnowledgeClient,
    KnowledgeClientConfig,
    KnowledgeConfigError,
    KnowledgeRequestError,
    KnowledgeResponseError,
    KnowledgeTransportError,
    OwnerUpcomingMeeting,
)
from foxhound.contracts.source_snapshot import source_snapshot_request


TOKEN = "synthetic-knowledge-token-with-sufficient-length"


def search_response(request: dict) -> dict:
    layers = []
    for name in request["layers"]:
        documents = []
        if name == "kb":
            documents = [{
                "id": "kb:Projects/alpha.md",
                "path": "Projects/alpha.md",
                "kb_path": "Projects/alpha.md",
                "excerpt": "A synthetic result.",
                "section": "Summary",
                "ranking": {"method": "rrf", "score": 0.125},
            }]
        layers.append({
            "name": name,
            "total_results": len(documents),
            "returned_results": len(documents),
            "truncated": False,
            "documents": documents,
        })
    return {
        "schema": "gw.search",
        "schema_version": 1,
        "ok": True,
        "query": request["query"],
        "parameters": {
            "layers": request["layers"],
            "context_lines": request["context_lines"],
            "max_matches_per_document": request["max_matches_per_document"],
            "max_results_per_layer": request["max_results_per_layer"],
            "ranking": "hybrid_rrf",
        },
        "layers": layers,
    }


def owner_response(request: dict) -> dict:
    return {
        "schema": "gw.task-owner-equivalence",
        "schema_version": 1,
        "ok": True,
        "alias": request["alias"],
        "candidate_id": request["candidate_id"],
        "source_revision": request["source_revision"],
        "legacy_task_id": request["legacy_task_id"],
        "legacy_digest": request["legacy_digest"],
        "status": "equivalent",
        "basis": "speaker_merge",
        "effective_owner": "Person B (SPK_002)",
    }


def execution_context_response(request: dict) -> dict:
    variables = {
        "display_name": "Person A",
        "operator_context": "Person A works with Example Org.\n",
        "self_aliases": ["Person A", "A. Person"],
        "institution_domains": ["example.edu"],
    }
    canonical = json.dumps(
        variables, ensure_ascii=True, separators=(",", ":"), sort_keys=True
    ).encode("utf-8")
    return {
        "schema": "gw.execution-context",
        "schema_version": 1,
        "ok": True,
        "alias": request["alias"],
        "revision": hashlib.sha256(canonical).hexdigest(),
        "variables": variables,
    }


def owner_meeting_response(_request: dict) -> dict:
    return {
        "schema": "gw.owner-upcoming-meeting",
        "schema_version": 1,
        "ok": True,
        "match": True,
        "checked_at": "2030-04-05T12:00:00+00:00",
        "evidence_revision": "a" * 64,
    }


def source_snapshot_response(request: dict) -> dict:
    return {
        "schema": "foxhound.source-snapshot", "schema_version": 1,
        "ok": True, "system": request["system"], "kind": request["kind"],
        "record_id": request["record_id"], "item_id": request["item_id"],
        "expected_revision": request["expected_revision"], "status": "current",
        "snapshot": {"revision": request["expected_revision"],
                     "observed_at": "2030-04-05T12:00:00+00:00",
                     "lifecycle": "active", "actionability": "actionable"},
    }


@contextmanager
def server(
    *,
    transform=None,
    raw: bytes | None = None,
    status: int = 200,
    content_type: str = "application/json",
    delay: float = 0,
    redirect: bool = False,
) -> Iterator[tuple[str, list[dict]]]:
    requests = []

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):  # noqa: N802
            length = int(self.headers.get("Content-Length", "0"))
            body = self.rfile.read(length)
            request = json.loads(body)
            requests.append({
                "path": self.path,
                "authorization": self.headers.get("Authorization"),
                "document": request,
            })
            if delay:
                time.sleep(delay)
            if redirect:
                self.send_response(302)
                self.send_header("Location", self.path)
                self.end_headers()
                return
            if self.path == "/v1/task-owner-equivalence":
                response = owner_response(request)
            elif self.path == "/v1/execution-context":
                response = execution_context_response(request)
            elif self.path == "/v1/owner-upcoming-meeting":
                response = owner_meeting_response(request)
            elif self.path == "/v1/source-snapshot":
                response = source_snapshot_response(request)
            else:
                response = search_response(request)
            if transform is not None:
                response = transform(response)
            payload = raw if raw is not None else (
                json.dumps(response, separators=(",", ":")) + "\n"
            ).encode("utf-8")
            try:
                self.send_response(status)
                self.send_header("Content-Type", content_type)
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
            except BrokenPipeError:
                pass

        def log_message(self, _format, *_args):
            return

    httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{httpd.server_address[1]}", requests
    finally:
        httpd.shutdown()
        httpd.server_close()
        thread.join(timeout=2)


def client(endpoint: str, **changes) -> GwKnowledgeClient:
    values = {
        "endpoint": endpoint,
        "alias": "primary",
        "token": TOKEN,
    }
    values.update(changes)
    return GwKnowledgeClient(KnowledgeClientConfig(**values))


class KnowledgeClientTests(unittest.TestCase):
    def test_source_snapshot_is_a_fixed_bounded_route(self):
        request = source_snapshot_request(
            system="gw", kind="issue", record_id="forge.example/acme/widget",
            item_id="7", expected_revision="b" * 64,
        )
        with server() as (endpoint, requests):
            result = client(endpoint).refresh_source(request)
        self.assertTrue(result.usable)
        self.assertEqual(requests[0]["path"], "/v1/source-snapshot")
        self.assertEqual(requests[0]["document"]["expected_revision"], "b" * 64)

    def test_owner_meeting_condition_is_exact_and_content_free(self):
        reference = {
            "kind": "person",
            "speaker_id": "SPK_002",
            "canonical_speaker_id": "SPK_002",
            "speaker_registry_id": "registry-1",
            "pinned": False,
            "provisional": False,
        }
        with server() as (endpoint, requests):
            result = client(endpoint).owner_upcoming_meeting(
                owner="Person B", owner_ref=reference
            )
        self.assertEqual(
            result,
            OwnerUpcomingMeeting(
                True,
                "2030-04-05T12:00:00+00:00",
                "a" * 64,
            ),
        )
        self.assertEqual(requests[0], {
            "path": "/v1/owner-upcoming-meeting",
            "authorization": f"Bearer {TOKEN}",
            "document": {
                "schema": "gw.owner-upcoming-meeting-request",
                "schema_version": 1,
                "alias": "primary",
                "owner": "Person B",
                "owner_ref": reference,
            },
        })

    def test_owner_meeting_condition_rejects_ambiguous_identity_and_response(self):
        with server() as (endpoint, _requests):
            with self.assertRaises(KnowledgeRequestError):
                client(endpoint).owner_upcoming_meeting(
                    owner="Person B",
                    owner_ref={
                        "kind": "person",
                        "speaker_id": "SPK_002",
                        "canonical_speaker_id": None,
                        "speaker_registry_id": "registry-1",
                        "pinned": False,
                        "provisional": False,
                    },
                )

        def add_content(document):
            document["event_title"] = "Synthetic private meeting"
            return document

        reference = {
            "kind": "external",
            "speaker_id": None,
            "canonical_speaker_id": None,
            "speaker_registry_id": None,
            "pinned": True,
            "provisional": False,
        }
        with server(transform=add_content) as (endpoint, _requests):
            with self.assertRaises(KnowledgeResponseError):
                client(endpoint).owner_upcoming_meeting(
                    owner="Person B", owner_ref=reference
                )

    def test_execution_context_is_allowlisted_and_digest_bound(self):
        with server() as (endpoint, requests):
            result = client(endpoint).execution_context()

        self.assertEqual(result.alias, "primary")
        self.assertEqual(result.display_name, "Person A")
        self.assertEqual(
            result.operator_context, "Person A works with Example Org.\n"
        )
        self.assertEqual(result.self_aliases, ("Person A", "A. Person"))
        self.assertEqual(result.institution_domains, ("example.edu",))
        self.assertNotIn(result.operator_context, repr(result))
        self.assertNotIn(result.display_name, repr(result))
        self.assertEqual(requests, [{
            "path": "/v1/execution-context",
            "authorization": f"Bearer {TOKEN}",
            "document": {
                "schema": "gw.execution-context-request",
                "schema_version": 1,
                "alias": "primary",
            },
        }])

    def test_execution_context_shape_identity_and_revision_fail_closed(self):
        def extra_variable(document):
            document["variables"]["source_settings"] = {"enabled": True}
            return document

        def wrong_alias(document):
            document["alias"] = "secondary"
            return document

        def wrong_revision(document):
            document["revision"] = "0" * 64
            return document

        def excessive_aliases(document):
            document["variables"]["self_aliases"] = [
                f"Person {index}" for index in range(33)
            ]
            return document

        def malformed_domain(document):
            document["variables"]["institution_domains"] = ["not a domain"]
            return document

        def duplicate_alias(document):
            document["variables"]["self_aliases"] = ["Person A", "Person A"]
            return document

        def oversized_context(document):
            document["variables"]["operator_context"] = "x" * 32_769
            return document

        for mutation in (
            extra_variable,
            wrong_alias,
            wrong_revision,
            excessive_aliases,
            malformed_domain,
            duplicate_alias,
            oversized_context,
        ):
            with self.subTest(mutation=mutation.__name__):
                with server(transform=mutation) as (endpoint, _requests):
                    with self.assertRaises(KnowledgeResponseError):
                        client(endpoint).execution_context()

    def test_search_is_bounded_authenticated_and_strictly_parsed(self):
        with server() as (endpoint, requests):
            result = client(endpoint).search(
                "synthetic query",
                layers=("emails", "kb"),
                context_lines=2,
                max_matches_per_document=3,
                max_results_per_layer=4,
            )

        self.assertEqual(result.document_count, 1)
        self.assertEqual([layer.name for layer in result.layers], ["kb", "emails"])
        document = result.layers[0].documents[0]
        self.assertEqual(document.path, "Projects/alpha.md")
        self.assertEqual(document.excerpt, "A synthetic result.")
        self.assertEqual(document.ranking_score, 0.125)
        self.assertEqual(len(requests), 1)
        self.assertEqual(requests[0]["path"], "/v1/search")
        self.assertEqual(requests[0]["authorization"], f"Bearer {TOKEN}")
        self.assertEqual(requests[0]["document"], {
            "alias": "primary",
            "query": "synthetic query",
            "layers": ["kb", "emails"],
            "context_lines": 2,
            "max_matches_per_document": 3,
            "max_results_per_layer": 4,
        })

    def test_search_accepts_bounded_aggregate_exclusion_counts(self):
        def add_exclusions(document):
            document["excluded"] = {
                "hits": 3,
                "directories": 2,
                "declined": 1,
            }
            return document

        with server(transform=add_exclusions) as (endpoint, _requests):
            result = client(endpoint).search(
                "synthetic query", layers=("emails",)
            )

        self.assertEqual([layer.name for layer in result.layers], ["emails"])

    def test_search_exclusion_metadata_remains_strict_and_content_free(self):
        invalid = (
            {"hits": -1, "directories": 0, "declined": 0},
            {"hits": 0, "directories": False, "declined": 0},
            {"hits": 0, "directories": 0, "declined": 0,
             "records": ["private"]},
        )
        for excluded in invalid:
            with self.subTest(excluded=excluded):
                def add_exclusions(document):
                    document["excluded"] = excluded
                    return document

                with server(transform=add_exclusions) as (endpoint, _requests):
                    with self.assertRaises(KnowledgeResponseError) as raised:
                        client(endpoint).search("synthetic query")
                self.assertNotIn("private", str(raised.exception))

    def test_owner_equivalence_is_identity_bound_and_strictly_parsed(self):
        request = {
            "candidate_id": "tc_" + "a" * 64,
            "source_revision": "b" * 64,
            "legacy_task_id": 101,
            "legacy_digest": "c" * 64,
        }
        with server() as (endpoint, requests):
            result = client(endpoint).resolve_task_owner(**request)

        self.assertTrue(result.equivalent)
        self.assertEqual(result.effective_owner, "Person B (SPK_002)")
        self.assertEqual(len(requests), 1)
        self.assertEqual(
            requests[0]["path"], "/v1/task-owner-equivalence"
        )
        self.assertEqual(requests[0]["authorization"], f"Bearer {TOKEN}")
        self.assertEqual(requests[0]["document"], {
            "schema": "gw.task-owner-equivalence-request",
            "schema_version": 1,
            "alias": "primary",
            **request,
        })

        def wrong_revision(document):
            document["source_revision"] = "d" * 64
            return document

        with server(transform=wrong_revision) as (endpoint, _requests):
            with self.assertRaises(KnowledgeResponseError):
                client(endpoint).resolve_task_owner(**request)

        def unresolved(document):
            document["status"] = "unresolved"
            document["basis"] = None
            document["effective_owner"] = None
            return document

        with server(transform=unresolved) as (endpoint, _requests):
            result = client(endpoint).resolve_task_owner(**request)
        self.assertFalse(result.equivalent)
        self.assertIsNone(result.effective_owner)

        with server() as (endpoint, requests):
            with self.assertRaises(KnowledgeRequestError):
                client(endpoint).resolve_task_owner(
                    **{**request, "candidate_id": "not-an-id"}
                )
        self.assertEqual(requests, [])

    def test_configuration_refuses_unsafe_endpoints_and_tokens(self):
        refused = (
            "http://192.0.2.1",
            "http://localhost",
            "http://127.0.0.1/path",
            "https://person@example.com",
            "https://example.com?mode=write",
            "file:///srv/example",
        )
        for endpoint in refused:
            with self.subTest(endpoint=endpoint):
                with self.assertRaises(KnowledgeConfigError):
                    KnowledgeClientConfig(
                        endpoint=endpoint, alias="primary", token=TOKEN
                    )
        with self.assertRaises(KnowledgeConfigError):
            KnowledgeClientConfig(
                endpoint="https://example.com", alias="primary", token="short"
            )
        representation = repr(KnowledgeClientConfig(
            endpoint="https://example.com", alias="primary", token=TOKEN
        ))
        self.assertNotIn(TOKEN, representation)

    def test_invalid_request_never_leaves_the_process_or_echoes_content(self):
        private_query = "-synthetic-private-query"
        with server() as (endpoint, requests):
            with self.assertRaises(KnowledgeRequestError) as raised:
                client(endpoint).search(private_query)
        self.assertEqual(requests, [])
        self.assertNotIn(private_query, str(raised.exception))

    def test_redirect_http_error_and_timeout_are_closed_transport_failures(self):
        with server(redirect=True) as (endpoint, requests):
            with self.assertRaises(KnowledgeTransportError):
                client(endpoint).search("synthetic query")
        self.assertEqual(len(requests), 1)

        with server(status=401) as (endpoint, _requests):
            with self.assertRaises(KnowledgeTransportError) as raised:
                client(endpoint).search("synthetic query")
            self.assertIsNone(raised.exception.__cause__)

        with server(delay=0.1) as (endpoint, _requests):
            with self.assertRaises(KnowledgeTransportError):
                client(endpoint, timeout_seconds=0.02).search("synthetic query")

    def test_size_media_json_and_duplicate_field_failures_are_closed(self):
        with server(raw=b"{}" + b" " * 1_100) as (endpoint, _requests):
            with self.assertRaises(KnowledgeResponseError):
                client(endpoint, max_response_bytes=1_024).search("synthetic query")
        with server(content_type="text/plain") as (endpoint, _requests):
            with self.assertRaises(KnowledgeResponseError):
                client(endpoint).search("synthetic query")
        with server(raw=b"not-json") as (endpoint, _requests):
            with self.assertRaises(KnowledgeResponseError) as raised:
                client(endpoint).search("synthetic query")
            self.assertIsNone(raised.exception.__cause__)
        duplicate = (
            b'{"schema":"gw.search","schema":"gw.search",'
            b'"schema_version":1,"ok":true,"query":"synthetic query",'
            b'"parameters":{},"layers":[]}'
        )
        with server(raw=duplicate) as (endpoint, _requests):
            with self.assertRaises(KnowledgeResponseError):
                client(endpoint).search("synthetic query")

    def test_response_identity_paths_counts_and_fields_fail_closed(self):
        mutations = []

        def add_private_field(document):
            document["private_state"] = "not accepted"
            return document
        mutations.append(add_private_field)

        def boolean_version(document):
            document["schema_version"] = True
            return document
        mutations.append(boolean_version)

        def absolute_path(document):
            document["layers"][0]["documents"][0]["path"] = "/private/path"
            return document
        mutations.append(absolute_path)

        def wrong_count(document):
            document["layers"][0]["returned_results"] = 2
            return document
        mutations.append(wrong_count)

        def changed_query(document):
            document["query"] = "different query"
            return document
        mutations.append(changed_query)

        for mutation in mutations:
            with self.subTest(mutation=mutation.__name__):
                with server(transform=mutation) as (endpoint, _requests):
                    with self.assertRaises(KnowledgeResponseError):
                        client(endpoint).search("synthetic query")

    def test_client_has_no_database_or_filesystem_mutation_path(self):
        with tempfile.TemporaryDirectory() as temporary:
            sentinel = Path(temporary) / "sentinel.txt"
            sentinel.write_text("unchanged", encoding="utf-8")
            before = tuple(os.scandir(temporary))
            with server() as (endpoint, _requests):
                instance = client(endpoint)
                self.assertEqual(
                    {
                        name for name in dir(instance)
                        if not name.startswith("_")
                        and callable(getattr(instance, name))
                    },
                    {
                        "execution_context", "owner_upcoming_meeting",
                        "refresh_source", "resolve_task_owner", "search",
                    },
                )
                instance.search("synthetic query")
                instance.execution_context()
                instance.owner_upcoming_meeting(
                    owner="Person B",
                    owner_ref={
                        "kind": "external",
                        "speaker_id": None,
                        "canonical_speaker_id": None,
                        "speaker_registry_id": None,
                        "pinned": True,
                        "provisional": False,
                    },
                )
            after = tuple(os.scandir(temporary))
            self.assertEqual(
                [entry.name for entry in before],
                [entry.name for entry in after],
            )
            self.assertEqual(sentinel.read_text(encoding="utf-8"), "unchanged")


if __name__ == "__main__":
    unittest.main()
