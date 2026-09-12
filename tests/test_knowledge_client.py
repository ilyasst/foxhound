from __future__ import annotations

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
)


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
                self.send_header("Location", "/v1/search")
                self.end_headers()
                return
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
                    {"search"},
                )
                instance.search("synthetic query")
            after = tuple(os.scandir(temporary))
            self.assertEqual(
                [entry.name for entry in before],
                [entry.name for entry in after],
            )
            self.assertEqual(sentinel.read_text(encoding="utf-8"), "unchanged")


if __name__ == "__main__":
    unittest.main()
