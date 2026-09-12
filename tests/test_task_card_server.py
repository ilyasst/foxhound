from __future__ import annotations

import http.client
import io
import json
import logging
import sqlite3
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.parse
import urllib.request
from contextlib import closing, contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path

from foxhound import CandidateInbox
from foxhound.task_card_server import (
    CLAIM_SCHEMA,
    ERROR_SCHEMA,
    HEALTH_SCHEMA,
    OPERATION_SCHEMA,
    REQUEST_SCHEMA,
    SCHEDULE_SCHEMA,
    TaskCardApplication,
    TaskCardServerConfigError,
    TaskCardServerLimits,
    is_canonical_loopback,
    load_token,
    make_server,
)
from foxhound.task_cards import TaskCardService


NOW = datetime(2030, 3, 1, 12, 0, tzinfo=timezone.utc)
TOKEN = "s" * 43
CLAIM_TOKEN = "c" * 43


class Clock:
    def __init__(self):
        self.value = NOW

    def __call__(self):
        return self.value

    def advance(self, delta: timedelta):
        self.value += delta


def request_document(**fields):
    return {
        "schema": REQUEST_SCHEMA,
        "schema_version": 1,
        **fields,
    }


@contextmanager
def running_server(app):
    server = make_server("127.0.0.1", 0, app)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def request(
    endpoint: str,
    path: str,
    document=None,
    *,
    token: str | None = TOKEN,
    method: str = "POST",
    raw: bytes | None = None,
    content_type: str = "application/json",
):
    body = raw if raw is not None else json.dumps(document).encode("utf-8")
    headers = {"Content-Type": content_type}
    if token is not None:
        headers["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(
        endpoint + path, data=body, headers=headers, method=method
    )
    try:
        with urllib.request.urlopen(req, timeout=2) as response:
            return response.status, dict(response.headers), json.load(response)
    except urllib.error.HTTPError as exc:
        with exc:
            return exc.code, dict(exc.headers), json.load(exc)


class TaskCardServerTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.database = Path(self.temporary.name) / "foxhound.sqlite3"
        self.clock = Clock()
        CandidateInbox(self.database, clock=self.clock).initialize()
        with closing(sqlite3.connect(self.database)) as connection, connection:
            for index in range(1, 5):
                connection.execute(
                    "INSERT INTO tasks(status,text,owner,due,version,created_at,"
                    "updated_at,closed_at) VALUES('open',?,?,?,?,?,?,NULL)",
                    (
                        f"Synthetic task {index} <private>",
                        f"Person {index}",
                        None,
                        1,
                        f"2030-01-{index:02d}T12:00:00+00:00",
                        NOW.isoformat(timespec="seconds"),
                    ),
                )
        self.cards = TaskCardService(
            self.database,
            clock=self.clock,
            token_factory=lambda: CLAIM_TOKEN,
        )
        self.app = TaskCardApplication(self.cards, TOKEN)

    def test_configuration_requires_private_token_and_canonical_loopback(self):
        token_path = Path(self.temporary.name) / "token"
        token_path.write_text(TOKEN + "\n", encoding="utf-8")
        token_path.chmod(0o600)
        self.assertEqual(load_token(token_path), TOKEN)

        token_path.chmod(0o640)
        with self.assertRaisesRegex(TaskCardServerConfigError, "0600"):
            load_token(token_path)
        token_path.chmod(0o600)
        link = Path(self.temporary.name) / "token-link"
        link.symlink_to(token_path)
        with self.assertRaisesRegex(TaskCardServerConfigError, "unavailable"):
            load_token(link)

        self.assertTrue(is_canonical_loopback("127.0.0.1"))
        self.assertTrue(is_canonical_loopback("127.0.0.2"))
        for host in ("localhost", "0.0.0.0", "192.0.2.1", "::1"):
            self.assertFalse(is_canonical_loopback(host))
            with self.assertRaisesRegex(TaskCardServerConfigError, "loopback"):
                make_server(host, 0, self.app)
        with self.assertRaises(TaskCardServerConfigError):
            TaskCardApplication(
                self.cards,
                TOKEN,
                limits=TaskCardServerLimits(max_body_bytes=1),
            )

    def test_health_unknown_routes_methods_and_authentication_are_closed(self):
        with running_server(self.app) as endpoint:
            status, headers, body = request(
                endpoint, "/healthz", None, token=None, method="GET", raw=b""
            )
            self.assertEqual((status, body["schema"], body["ok"]),
                             (200, HEALTH_SCHEMA, True))
            self.assertEqual(headers["Cache-Control"], "no-store")

            status, _, body = request(
                endpoint,
                "/v1/task-cards/schedule",
                request_document(limit=1),
                token=None,
            )
            self.assertEqual((status, body["schema"]), (401, ERROR_SCHEMA))
            status, _, body = request(
                endpoint,
                "/v1/task-cards/schedule",
                request_document(limit=1),
                token="w" * 43,
            )
            self.assertEqual((status, body["error"]["code"]),
                             (401, "unauthorized"))
            status, _, body = request(
                endpoint, "/v1/unknown?private=value",
                request_document(limit=1),
            )
            self.assertEqual((status, body["error"]["code"]),
                             (404, "not_found"))
            status, headers, body = request(
                endpoint, "/v1/task-cards/action",
                request_document(card_id=1, card_version=1, action="done"),
                method="PUT",
            )
            self.assertEqual((status, headers["Allow"]), (405, "GET, POST"))
            self.assertEqual(body["error"]["code"], "method_not_allowed")

    def test_strict_request_parsing_and_limits_are_content_free(self):
        private = "Synthetic private request value"
        with running_server(self.app) as endpoint:
            cases = (
                (b'{"schema":"x","schema":"y"}', 400, "invalid_json"),
                (json.dumps(request_document(limit=1, extra=private)).encode(),
                 400, "invalid_request"),
                (json.dumps(request_document(limit=True)).encode(),
                 400, "invalid_request"),
                (json.dumps(request_document(limit=1)).encode(),
                 415, "unsupported_media_type"),
                (b"{" + b"x" * (17 * 1024), 413, "request_too_large"),
            )
            for index, (raw, expected, code) in enumerate(cases):
                content_type = "text/plain" if index == 3 else "application/json"
                status, _, body = request(
                    endpoint,
                    "/v1/task-cards/schedule",
                    raw=raw,
                    content_type=content_type,
                )
                self.assertEqual((status, body["error"]["code"]),
                                 (expected, code))
                self.assertNotIn(private, json.dumps(body))

            # Duplicate authentication headers are refused before body parsing.
            parsed = urllib.parse.urlsplit(endpoint)
            connection = http.client.HTTPConnection(parsed.hostname, parsed.port)
            body = json.dumps(request_document(limit=1))
            connection.putrequest("POST", "/v1/task-cards/schedule")
            connection.putheader("Content-Type", "application/json")
            connection.putheader("Content-Length", str(len(body.encode())))
            connection.putheader("Authorization", f"Bearer {TOKEN}")
            connection.putheader("Authorization", f"Bearer {TOKEN}")
            connection.endheaders(body.encode())
            response = connection.getresponse()
            self.assertEqual(response.status, 401)
            response.read()
            connection.close()

    def test_routes_drive_delivery_retry_snooze_and_atomic_completion(self):
        with running_server(self.app) as endpoint:
            status, _, scheduled = request(
                endpoint,
                "/v1/task-cards/schedule",
                request_document(limit=2),
            )
            self.assertEqual((status, scheduled["schema"], scheduled["created"]),
                             (200, SCHEDULE_SCHEMA, 2))
            _, _, claimed = request(
                endpoint,
                "/v1/task-cards/claim",
                request_document(lease_seconds=60),
            )
            self.assertEqual((claimed["schema"], claimed["status"]),
                             (CLAIM_SCHEMA, "claimed"))
            claim = claimed["claim"]
            self.assertIn("&lt;private&gt;", claim["body"])
            self.assertNotIn("<private>", claim["body"])

            _, _, failed = request(
                endpoint,
                "/v1/task-cards/delivery-failed",
                request_document(
                    card_id=claim["card_id"],
                    card_version=claim["card_version"],
                    claim_token=claim["claim_token"],
                ),
            )
            self.assertEqual((failed["schema"], failed["disposition"]),
                             (OPERATION_SCHEMA, "applied"))
            _, _, claimed = request(
                endpoint,
                "/v1/task-cards/claim",
                request_document(lease_seconds=60),
            )
            claim = claimed["claim"]
            delivery = request_document(
                card_id=claim["card_id"],
                card_version=claim["card_version"],
                claim_token=claim["claim_token"],
                transport="synthetic",
                delivery_ref="message-alpha",
            )
            _, _, delivered = request(
                endpoint, "/v1/task-cards/delivered", delivery
            )
            self.assertEqual(delivered["card_status"], "delivered")
            _, _, replayed = request(
                endpoint, "/v1/task-cards/delivered", delivery
            )
            self.assertEqual(replayed["disposition"], "unchanged")

            action = request_document(
                card_id=claim["card_id"],
                card_version=claim["card_version"],
                action="snooze",
            )
            _, _, snoozed = request(
                endpoint, "/v1/task-cards/action", action
            )
            self.assertEqual((snoozed["card_status"], snoozed["task_status"]),
                             ("snoozed", "open"))
            _, _, stale = request(
                endpoint, "/v1/task-cards/action", action
            )
            self.assertEqual((stale["ok"], stale["refusal"]),
                             (False, "stale_version"))

            self.clock.advance(timedelta(days=3))
            _, _, claimed = request(
                endpoint,
                "/v1/task-cards/claim",
                request_document(lease_seconds=60),
            )
            claim = claimed["claim"]
            delivery.update(
                card_id=claim["card_id"],
                card_version=claim["card_version"],
                claim_token=claim["claim_token"],
                delivery_ref="message-beta",
            )
            request(endpoint, "/v1/task-cards/delivered", delivery)
            _, _, done = request(
                endpoint,
                "/v1/task-cards/action",
                request_document(
                    card_id=claim["card_id"],
                    card_version=claim["card_version"],
                    action="done",
                ),
            )
            self.assertEqual((done["card_status"], done["task_status"]),
                             ("resolved", "done"))

    def test_access_logs_exclude_content_tokens_and_identifiers(self):
        stream = io.StringIO()
        handler = logging.StreamHandler(stream)
        logger = logging.getLogger("foxhound.task_card_server")
        old_level = logger.level
        logger.setLevel(logging.INFO)
        logger.addHandler(handler)
        try:
            with running_server(self.app) as endpoint:
                request(
                    endpoint,
                    "/v1/task-cards/schedule",
                    request_document(limit=1),
                )
                request(
                    endpoint,
                    "/v1/task-cards/claim",
                    request_document(lease_seconds=60),
                )
                request(
                    endpoint,
                    "/unknown?Synthetic-private-query",
                    request_document(limit=1),
                )
        finally:
            logger.removeHandler(handler)
            logger.setLevel(old_level)
        output = stream.getvalue()
        self.assertIn("method=POST", output)
        self.assertIn("route=/v1/task-cards/claim", output)
        self.assertIn("route=unknown", output)
        for forbidden in (
            "Synthetic task", TOKEN, CLAIM_TOKEN, "private-query",
            "card_id", "delivery_ref",
        ):
            self.assertNotIn(forbidden, output)

    def test_http_server_serializes_concurrent_application_requests(self):
        lock = threading.Lock()
        active = 0
        maximum = 0

        class SlowApplication(TaskCardApplication):
            def dispatch(self, operation, payload):
                nonlocal active, maximum
                with lock:
                    active += 1
                    maximum = max(maximum, active)
                try:
                    time.sleep(0.05)
                    return super().dispatch(operation, payload)
                finally:
                    with lock:
                        active -= 1

        app = SlowApplication(self.cards, TOKEN)
        with running_server(app) as endpoint:
            start = threading.Event()

            def send():
                start.wait()
                request(
                    endpoint,
                    "/v1/task-cards/schedule",
                    request_document(limit=1),
                )

            threads = [threading.Thread(target=send) for _ in range(2)]
            for thread in threads:
                thread.start()
            start.set()
            for thread in threads:
                thread.join(timeout=2)
        self.assertEqual(maximum, 1)


if __name__ == "__main__":
    unittest.main()
