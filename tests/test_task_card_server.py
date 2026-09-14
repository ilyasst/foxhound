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
from foxhound.agent_profiles import (
    AgentProfileRegistry,
    general_profile,
    parse_profile,
)
from foxhound.execution_cards import (
    ExecutionCardService,
    parse_execution_agent_callback,
    parse_execution_review_callback,
)
from foxhound.task_card_server import (
    CLAIM_SCHEMA,
    ERROR_SCHEMA,
    EXECUTION_AGENT_OPTIONS_SCHEMA,
    EXECUTION_AGENT_SELECTION_SCHEMA,
    EXECUTION_CLAIM_SCHEMA,
    EXECUTION_OPERATION_SCHEMA,
    EXECUTION_SCHEDULE_SCHEMA,
    EXECUTION_STATS_SCHEMA,
    HEALTH_SCHEMA,
    OPERATION_SCHEMA,
    REQUEST_SCHEMA,
    SCHEDULE_SCHEMA,
    STATS_SCHEMA,
    TaskCardApplication,
    TaskCardServerConfigError,
    TaskCardServerLimits,
    TaskCardServerRequestError,
    is_canonical_loopback,
    load_token,
    make_server,
)
from foxhound.task_cards import TaskCardService
from foxhound.task_execution import (
    ExecutionOutcome,
    ExecutionResultEnvelope,
    TaskExecutionService,
    WorkflowStatus,
)


NOW = datetime(2030, 3, 1, 12, 0, tzinfo=timezone.utc)
TOKEN = "s" * 43
CLAIM_TOKEN = "c" * 43
EXECUTION_DELIVERY_TOKEN = "e" * 43
WORKFLOW_TOKEN = "w" * 43


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
        self.execution = TaskExecutionService(
            self.database,
            clock=self.clock,
            token_factory=lambda: WORKFLOW_TOKEN,
        )
        self.execution_cards = ExecutionCardService(
            self.database,
            clock=self.clock,
            token_factory=lambda: EXECUTION_DELIVERY_TOKEN,
        )
        self.app = TaskCardApplication(
            self.cards,
            TOKEN,
            execution_cards=self.execution_cards,
        )

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

    def test_authenticated_stats_are_exact_and_do_not_write(self):
        before = (self.cards.count(), self.cards.event_count())
        with running_server(self.app) as endpoint:
            status, _, body = request(
                endpoint,
                "/v1/task-cards/stats",
                request_document(),
            )
            self.assertEqual(status, 200)
            self.assertEqual(body, {
                "schema": STATS_SCHEMA,
                "schema_version": 1,
                "ok": True,
                "pending": 0,
                "delivering": 0,
                "delivered": 0,
                "snoozed": 0,
                "active": 0,
            })
            status, _, body = request(
                endpoint,
                "/v1/task-cards/stats",
                request_document(extra=True),
            )
            self.assertEqual((status, body["error"]["code"]),
                             (400, "invalid_request"))
            status, _, _ = request(
                endpoint,
                "/v1/task-cards/stats",
                request_document(),
                token=None,
            )
            self.assertEqual(status, 401)
        self.assertEqual((self.cards.count(), self.cards.event_count()), before)

    def test_execution_stats_and_missing_adapter_are_content_free(self):
        before = (
            self.execution_cards.count(),
            self.execution_cards.event_count(),
        )
        with running_server(self.app) as endpoint:
            status, _, body = request(
                endpoint,
                "/v1/execution-cards/stats",
                request_document(),
            )
            self.assertEqual(status, 200)
            self.assertEqual(body, {
                "schema": EXECUTION_STATS_SCHEMA,
                "schema_version": 1,
                "ok": True,
                "pending": 0,
                "delivering": 0,
                "delivered": 0,
                "active": 0,
            })
            status, _, body = request(
                endpoint,
                "/v1/execution-cards/stats",
                request_document(),
                token=None,
            )
            self.assertEqual((status, body["error"]["code"]),
                             (401, "unauthorized"))
        self.assertEqual(
            (self.execution_cards.count(), self.execution_cards.event_count()),
            before,
        )

        unavailable = TaskCardApplication(self.cards, TOKEN)
        with running_server(unavailable) as endpoint:
            status, _, body = request(
                endpoint,
                "/v1/execution-cards/stats",
                request_document(),
            )
        self.assertEqual(
            (status, body["error"]["code"]),
            (503, "service_unavailable"),
        )

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

    def test_execution_routes_drive_start_and_plan_review(self):
        workflow = self.execution.schedule(1, expected_task_version=1)
        self.assertEqual(workflow.status, WorkflowStatus.AWAITING_START)
        with running_server(self.app) as endpoint:
            status, _, scheduled = request(
                endpoint,
                "/v1/execution-cards/schedule",
                request_document(limit=2),
            )
            self.assertEqual(
                (status, scheduled["schema"], scheduled["created"]),
                (200, EXECUTION_SCHEDULE_SCHEMA, 1),
            )
            _, _, claimed = request(
                endpoint,
                "/v1/execution-cards/claim",
                request_document(lease_seconds=60),
            )
            self.assertEqual(
                (claimed["schema"], claimed["claim"]["kind"]),
                (EXECUTION_CLAIM_SCHEMA, "start"),
            )
            claim = claimed["claim"]
            self.assertIn("&lt;private&gt;", claim["body"])

            _, _, failed = request(
                endpoint,
                "/v1/execution-cards/delivery-failed",
                request_document(
                    card_id=claim["card_id"],
                    card_version=claim["card_version"],
                    claim_token=claim["claim_token"],
                ),
            )
            self.assertEqual(
                (failed["schema"], failed["card_status"]),
                (EXECUTION_OPERATION_SCHEMA, "pending"),
            )
            _, _, claimed = request(
                endpoint,
                "/v1/execution-cards/claim",
                request_document(lease_seconds=60),
            )
            claim = claimed["claim"]
            delivery = request_document(
                card_id=claim["card_id"],
                card_version=claim["card_version"],
                claim_token=claim["claim_token"],
                transport="synthetic",
                delivery_ref="execution-message-alpha",
            )
            _, _, delivered = request(
                endpoint, "/v1/execution-cards/delivered", delivery
            )
            self.assertEqual(delivered["card_status"], "delivered")
            _, _, agent_options = request(
                endpoint,
                "/v1/execution-cards/agent-options",
                request_document(
                    card_id=claim["card_id"],
                    card_version=claim["card_version"],
                ),
            )
            self.assertEqual(
                (agent_options["schema"], agent_options["ok"]),
                (EXECUTION_AGENT_OPTIONS_SCHEMA, True),
            )
            agent_callback = agent_options["presentation"]["reply_markup"][
                "inline_keyboard"
            ][0][0]["callback_data"]
            agent_card_id, agent_card_version, selection_token = (
                parse_execution_agent_callback(agent_callback)
            )
            _, _, agent_selection = request(
                endpoint,
                "/v1/execution-cards/agent-selection",
                request_document(
                    card_id=agent_card_id,
                    card_version=agent_card_version,
                    selection_token=selection_token,
                ),
            )
            self.assertEqual(
                (
                    agent_selection["schema"],
                    agent_selection["disposition"],
                    agent_selection["agent_display_name"],
                ),
                (EXECUTION_AGENT_SELECTION_SCHEMA, "unchanged", "General"),
            )
            action = request_document(
                card_id=claim["card_id"],
                card_version=claim["card_version"],
                action="start",
            )
            _, _, started = request(
                endpoint, "/v1/execution-cards/action", action
            )
            self.assertEqual(
                (started["workflow_status"], started["workflow_phase"]),
                ("queued", "plan"),
            )
            _, _, stale = request(
                endpoint, "/v1/execution-cards/action", action
            )
            self.assertEqual(
                (stale["ok"], stale["refusal"]),
                (False, "stale_version"),
            )

            execution_claim = self.execution.claim_next()
            recorded = self.execution.record_result(ExecutionResultEnvelope(
                result_id="synthetic-plan-result",
                task_id=1,
                task_version=1,
                workflow_version=execution_claim.workflow_version,
                phase="plan",
                claim_token=execution_claim.token,
                outcome=ExecutionOutcome.AWAITING_PLAN,
                summary="Synthetic plan summary",
                work_markdown="Synthetic plan body",
            ))
            self.assertTrue(recorded.accepted)
            _, _, scheduled = request(
                endpoint,
                "/v1/execution-cards/schedule",
                request_document(limit=2),
            )
            self.assertEqual(scheduled["created"], 1)
            _, _, claimed = request(
                endpoint,
                "/v1/execution-cards/claim",
                request_document(lease_seconds=60),
            )
            claim = claimed["claim"]
            self.assertEqual(claim["kind"], "plan_review")
            delivery.update(
                card_id=claim["card_id"],
                card_version=claim["card_version"],
                claim_token=claim["claim_token"],
                delivery_ref="execution-message-beta",
            )
            request(endpoint, "/v1/execution-cards/delivered", delivery)
            _, _, approved = request(
                endpoint,
                "/v1/execution-cards/action",
                request_document(
                    card_id=claim["card_id"],
                    card_version=claim["card_version"],
                    action="approve",
                ),
            )
            self.assertEqual(
                (approved["workflow_status"], approved["workflow_phase"]),
                ("queued", "execute"),
            )

    def test_execution_routes_reject_invalid_request_shapes(self):
        with running_server(self.app) as endpoint:
            status, _, body = request(
                endpoint,
                "/v1/execution-cards/schedule",
                request_document(limit=1, extra="Synthetic private value"),
            )
            self.assertEqual(
                (status, body["error"]["code"]),
                (400, "invalid_request"),
            )
            status, _, body = request(
                endpoint,
                "/v1/execution-cards/schedule",
                raw=b'{"schema":"x","schema":"y"}',
            )
            self.assertEqual(
                (status, body["error"]["code"]),
                (400, "invalid_json"),
            )
            status, _, body = request(
                endpoint,
                "/v1/execution-cards/schedule",
                raw=b"{" + b"x" * (17 * 1024),
            )
            self.assertEqual(
                (status, body["error"]["code"]),
                (413, "request_too_large"),
            )
            status, headers, body = request(
                endpoint,
                "/v1/execution-cards/action",
                request_document(card_id=1, card_version=1, action="cancel"),
                method="PUT",
            )
            self.assertEqual(
                (status, headers["Allow"], body["error"]["code"]),
                (405, "GET, POST", "method_not_allowed"),
            )
            status, _, body = request(
                endpoint,
                "/v1/execution-cards/action",
                request_document(
                    card_id=1,
                    card_version=1,
                    action="invented",
                ),
            )
            self.assertEqual(
                (status, body["error"]["code"]),
                (400, "invalid_request"),
            )

    def test_agent_adapter_contract_returns_refreshed_start_card(self):
        profile_document = general_profile().document()
        profile_document.update({
            "profile_id": "specialist",
            "display_name": "Synthetic Specialist",
            "max_turns": 50,
        })
        specialist = parse_profile(profile_document)
        registry = AgentProfileRegistry((general_profile(), specialist))
        execution = TaskExecutionService(
            self.database,
            clock=self.clock,
            profile_registry=registry,
        )
        cards = ExecutionCardService(
            self.database,
            clock=self.clock,
            token_factory=lambda: EXECUTION_DELIVERY_TOKEN,
            profile_registry=registry,
        )
        app = TaskCardApplication(self.cards, TOKEN, execution_cards=cards)
        workflow = execution.schedule(1, expected_task_version=1)
        cards.schedule()
        claim = cards.claim_next()
        cards.complete_delivery(
            claim.card.id,
            expected_version=claim.card.version,
            claim_token=claim.token,
            transport="synthetic",
            delivery_ref="synthetic-agent-message",
        )

        options = app.dispatch("execution_agent_options", request_document(
            card_id=claim.card.id,
            card_version=claim.card.version,
        ))
        self.assertEqual(options["schema"], EXECUTION_AGENT_OPTIONS_SCHEMA)
        specialist_callback = next(
            row[0]["callback_data"]
            for row in options["presentation"]["reply_markup"][
                "inline_keyboard"
            ]
            if row[0]["text"] == "Synthetic Specialist"
        )
        card_id, card_version, selection_token = (
            parse_execution_agent_callback(specialist_callback)
        )
        selected = app.dispatch("execution_agent_selection", request_document(
            card_id=card_id,
            card_version=card_version,
            selection_token=selection_token,
        ))

        self.assertEqual(
            (
                selected["schema"],
                selected["disposition"],
                selected["card_version"],
                selected["agent_profile_id"],
            ),
            (
                EXECUTION_AGENT_SELECTION_SCHEMA,
                "applied",
                claim.card.version + 1,
                "specialist",
            ),
        )
        self.assertIn(
            "<b>Start this task?</b>",
            selected["presentation"]["body"],
        )
        self.assertNotIn(
            "Synthetic Specialist",
            selected["presentation"]["body"],
        )
        callbacks = [
            button["callback_data"]
            for row in selected["presentation"]["reply_markup"][
                "inline_keyboard"
            ]
            for button in row
        ]
        self.assertTrue(all(
            parse_execution_review_callback(value)[1]
            == selected["card_version"]
            for value in callbacks
        ))
        self.assertEqual(
            (execution.get(1).status, execution.get(1).version),
            (WorkflowStatus.AWAITING_START, workflow.version + 1),
        )
        stale = app.dispatch("execution_agent_selection", request_document(
            card_id=card_id,
            card_version=card_version,
            selection_token=selection_token,
        ))
        unknown = app.dispatch("execution_agent_selection", request_document(
            card_id=card_id,
            card_version=selected["card_version"],
            selection_token="0" * 20,
        ))
        self.assertEqual(stale["refusal"], "stale_version")
        self.assertEqual(unknown["refusal"], "invalid_argument")
        with self.assertRaises(TaskCardServerRequestError):
            app.dispatch("execution_agent_selection", request_document(
                card_id=card_id,
                card_version=selected["card_version"],
                selection_token="a" * 21,
            ))

    def test_execution_input_route_is_strict_and_version_bound(self):
        workflow = self.execution.schedule(1, expected_task_version=1)
        self.execution.start_action(
            1, expected_version=workflow.version, action="start"
        )
        run = self.execution.claim_next()
        self.execution.record_result(ExecutionResultEnvelope(
            result_id="synthetic-input-plan",
            task_id=1,
            task_version=1,
            workflow_version=run.workflow_version,
            phase="plan",
            claim_token=run.token,
            outcome="awaiting_plan",
            summary="Synthetic plan.",
            work_markdown="Synthetic work.",
        ))
        self.execution_cards.schedule()
        claim = self.execution_cards.claim_next()
        self.execution_cards.complete_delivery(
            claim.card.id,
            expected_version=claim.card.version,
            claim_token=claim.token,
            transport="synthetic",
            delivery_ref="synthetic-input-message",
        )

        result = self.app.dispatch("execution_input", request_document(
            card_id=claim.card.id,
            card_version=claim.card.version,
            input_kind="discussion",
            value="Check the synthetic constraint.",
        ))

        self.assertEqual(
            (result["schema"], result["workflow_status"]),
            (EXECUTION_OPERATION_SCHEMA, "queued"),
        )
        stale = self.app.dispatch("execution_input", request_document(
            card_id=claim.card.id,
            card_version=claim.card.version,
            input_kind="discussion",
            value="A second synthetic request.",
        ))
        self.assertEqual((stale["ok"], stale["refusal"]),
                         (False, "stale_version"))
        for kind, value in (
            ("invented", "Synthetic value"),
            ("reassignment", "Person A\nPerson B"),
            ("discussion", "\x00"),
        ):
            with self.subTest(kind=kind):
                with self.assertRaises(TaskCardServerRequestError):
                    self.app.dispatch("execution_input", request_document(
                        card_id=claim.card.id,
                        card_version=claim.card.version,
                        input_kind=kind,
                        value=value,
                    ))

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
                self.execution.schedule(1, expected_task_version=1)
                request(
                    endpoint,
                    "/v1/execution-cards/schedule",
                    request_document(limit=1),
                )
                request(
                    endpoint,
                    "/v1/execution-cards/claim",
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
        self.assertIn("route=/v1/execution-cards/claim", output)
        self.assertIn("route=unknown", output)
        for forbidden in (
            "Synthetic task", TOKEN, CLAIM_TOKEN, "private-query",
            EXECUTION_DELIVERY_TOKEN, WORKFLOW_TOKEN, "card_id",
            "delivery_ref",
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
