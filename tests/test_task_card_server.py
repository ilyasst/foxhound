from __future__ import annotations

import hashlib
import hmac
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
from unittest import mock

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
from foxhound.knowledge_client import OwnerUpcomingMeeting
from foxhound.task_card_server import (
    CLAIM_SCHEMA,
    DRIP_ROLE,
    ERROR_SCHEMA,
    EXECUTION_AGENT_OPTIONS_SCHEMA,
    EXECUTION_AGENT_SELECTION_SCHEMA,
    EXECUTION_BRIEF_SCHEMA,
    EXECUTION_CLAIM_SCHEMA,
    EXECUTION_OPERATION_SCHEMA,
    EXECUTION_SCHEDULE_SCHEMA,
    EXECUTION_STATS_SCHEMA,
    HEALTH_SCHEMA,
    OPERATION_SCHEMA,
    QUEUE_VIEW_ROLE,
    REQUEST_SCHEMA,
    SCHEDULE_SCHEMA,
    STATS_SCHEMA,
    TASK_CARD_CONSUMER_ROLES,
    ConsumerIdentity,
    TaskCardApplication,
    TaskCardConsumerIdentityError,
    TaskCardServerConfigError,
    TaskCardServerLimits,
    TaskCardServerRequestError,
    is_canonical_loopback,
    load_role_tokens,
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

    def test_execution_brief_route_is_a_versioned_read(self):
        self.execution.schedule(1, expected_task_version=1)
        self.execution_cards.schedule()
        claim = self.execution_cards.claim_next()
        before = self.execution_cards.stats()

        response = self.app.dispatch(
            "execution_brief",
            request_document(
                card_id=claim.card.id,
                card_version=claim.card.version,
            ),
        )

        self.assertEqual(response["schema"], EXECUTION_BRIEF_SCHEMA)
        self.assertTrue(response["ok"])
        self.assertIn("Synthetic task 1", response["text"])
        self.assertEqual(self.execution_cards.stats(), before)

        stale = self.app.dispatch(
            "execution_brief",
            request_document(
                card_id=claim.card.id,
                card_version=claim.card.version + 1,
            ),
        )
        self.assertFalse(stale["ok"])
        self.assertIsNone(stale["text"])
        self.assertEqual(stale["refusal"], "stale_version")

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
        # The refreshed card names the agent that was just chosen: a
        # selection with no visible effect reads as a tap that failed.
        self.assertIn(
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

    def test_execution_action_route_forwards_owner_hold_exactly(self):
        with closing(sqlite3.connect(self.database)) as connection:
            connection.execute(
                "UPDATE tasks SET owner='Person B',owner_ref_version=1,"
                "owner_kind='external',owner_pinned=1,owner_provisional=0 "
                "WHERE id=1"
            )
            connection.commit()
        cards = ExecutionCardService(
            self.database,
            clock=self.clock,
            token_factory=lambda: EXECUTION_DELIVERY_TOKEN,
            owner_condition=lambda _owner, _ref: OwnerUpcomingMeeting(
                False, NOW.isoformat(timespec="seconds"), "b" * 64
            ),
            reader_aliases=("Person A",),
        )
        app = TaskCardApplication(
            self.cards, TOKEN, execution_cards=cards
        )
        self.execution.schedule(1, expected_task_version=1)
        cards.schedule()
        claim = cards.claim_next()
        cards.complete_delivery(
            claim.card.id,
            expected_version=claim.card.version,
            claim_token=claim.token,
            transport="synthetic",
            delivery_ref="synthetic-owner-hold",
        )

        result = app.dispatch("execution_action", request_document(
            card_id=claim.card.id,
            card_version=claim.card.version,
            action="until_meeting",
        ))

        self.assertEqual(
            (result["ok"], result["workflow_status"]),
            (True, "snoozed"),
        )

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
            def dispatch(self, operation, payload, *, authorization=None):
                nonlocal active, maximum
                with lock:
                    active += 1
                    maximum = max(maximum, active)
                try:
                    time.sleep(0.05)
                    return super().dispatch(
                        operation, payload, authorization=authorization
                    )
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


QUEUE_VIEW_TOKEN = "q" * 43


class TaskCardTokenRoleTests(unittest.TestCase):
    """ADR 0036 decision 1: token-to-role configuration and fail-closed
    consumer identity resolution (issue #192)."""

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.database = Path(self.temporary.name) / "foxhound.sqlite3"
        self.cards = TaskCardService(self.database)
        self.cards.initialize()

    def test_single_configured_token_defaults_to_drip_role_unchanged(self):
        """ADR 0036, invariant 3: a lone configured token with no explicit
        role reproduces today's behavior exactly -- no configuration
        change, and the deployed chat gateway must not be able to tell
        this landed."""
        app = TaskCardApplication(self.cards, TOKEN)
        self.assertEqual(app.tokens, {DRIP_ROLE: TOKEN})
        self.assertTrue(app.authorized(f"Bearer {TOKEN}"))
        identity = app.resolve_consumer(f"Bearer {TOKEN}")
        self.assertIsInstance(identity, ConsumerIdentity)
        self.assertIn(identity.role, TASK_CARD_CONSUMER_ROLES)
        self.assertEqual(identity.role, DRIP_ROLE)

        with running_server(app) as endpoint:
            # The exact response bytes for /healthz and stats are the same
            # shape ADR 0011 already documents: no route gains a role or
            # consumer field, and no new top-level key appears.
            status, _, body = request(
                endpoint, "/healthz", None, token=None, method="GET", raw=b""
            )
            self.assertEqual(status, 200)
            self.assertEqual(
                set(body), {"schema", "schema_version", "ok"}
            )

            status, _, body = request(
                endpoint, "/v1/task-cards/stats", request_document()
            )
            self.assertEqual(status, 200)
            self.assertEqual(
                set(body),
                {
                    "schema", "schema_version", "ok", "pending",
                    "delivering", "delivered", "snoozed", "active",
                },
            )
            self.assertEqual(body["schema"], STATS_SCHEMA)

    def test_second_token_configured_as_queue_view(self):
        app = TaskCardApplication(
            self.cards, {DRIP_ROLE: TOKEN, QUEUE_VIEW_ROLE: QUEUE_VIEW_TOKEN}
        )
        self.assertTrue(app.authorized(f"Bearer {TOKEN}"))
        self.assertTrue(app.authorized(f"Bearer {QUEUE_VIEW_TOKEN}"))

        drip_identity = app.resolve_consumer(f"Bearer {TOKEN}")
        queue_identity = app.resolve_consumer(f"Bearer {QUEUE_VIEW_TOKEN}")
        self.assertEqual(drip_identity.role, DRIP_ROLE)
        self.assertEqual(queue_identity.role, QUEUE_VIEW_ROLE)
        # Consumer identity is the digest of the accepting token, never the
        # token itself, and the two tokens resolve to different digests.
        self.assertNotEqual(drip_identity.digest, queue_identity.digest)
        self.assertNotIn(TOKEN, drip_identity.digest)
        self.assertNotIn(QUEUE_VIEW_TOKEN, queue_identity.digest)

        # No route gains role-scoped behavior in this issue: both tokens
        # can still call the existing routes exactly as one shared token
        # could before.
        with running_server(app) as endpoint:
            for token in (TOKEN, QUEUE_VIEW_TOKEN):
                status, _, body = request(
                    endpoint,
                    "/v1/task-cards/stats",
                    request_document(),
                    token=token,
                )
                self.assertEqual(status, 200)
                self.assertTrue(body["ok"])

    def test_consumer_identity_is_never_accepted_from_the_request(self):
        """ADR 0036 invariant 1: no route reads or trusts a client-supplied
        consumer field; identity comes only from the authenticating
        token."""
        app = TaskCardApplication(
            self.cards, {DRIP_ROLE: TOKEN, QUEUE_VIEW_ROLE: QUEUE_VIEW_TOKEN}
        )
        with running_server(app) as endpoint:
            status, _, body = request(
                endpoint,
                "/v1/task-cards/stats",
                {
                    "schema": REQUEST_SCHEMA,
                    "schema_version": 1,
                    "consumer": QUEUE_VIEW_ROLE,
                },
                token=TOKEN,
            )
            # An unrecognized "consumer" field is rejected exactly like any
            # other unexpected field -- no route defines a request
            # parameter for it at all.
            self.assertEqual(status, 400)
            self.assertEqual(body["error"]["code"], "invalid_request")

    def test_unresolvable_role_fails_closed_and_never_defaults(self):
        """ADR 0036 decision 1, invariant 2: a token accepted as *some*
        known bearer value but whose role cannot be resolved must refuse
        with a fixed error, never default to either role. Construction
        already refuses to build a service in this state (every configured
        role is validated up front against the closed set); this test
        breaks that invariant directly on the built object to prove the
        runtime resolution path itself -- not just construction-time
        validation -- fails closed rather than guessing."""
        stray_token = "z" * 43
        app = TaskCardApplication(self.cards, TOKEN)
        app.tokens = {**app.tokens, "not_a_real_role": stray_token}

        # The token still authenticates -- it matches a configured value --
        # but its role cannot be resolved, so identity resolution must
        # refuse rather than default it to `drip` or `queue_view`.
        self.assertTrue(app.authorized(f"Bearer {stray_token}"))
        with self.assertRaises(TaskCardConsumerIdentityError):
            app.resolve_consumer(f"Bearer {stray_token}")

        # The legitimate drip token is unaffected by the stray entry.
        identity = app.resolve_consumer(f"Bearer {TOKEN}")
        self.assertEqual(identity.role, DRIP_ROLE)

    def test_construction_refuses_to_start_with_an_unresolvable_role(self):
        """This service's chosen fail-closed behavior (documented on issue
        #192): a configured token whose role is outside the closed set
        never lets the server start at all, rather than starting and
        refusing requests one at a time."""
        with self.assertRaises(TaskCardServerConfigError):
            TaskCardApplication(self.cards, {"not_a_role": TOKEN})

    def test_service_config_rejects_ambiguous_or_unknown_roles(self):
        with self.assertRaises(TaskCardServerConfigError):
            TaskCardApplication(self.cards, {"not_a_role": TOKEN})
        with self.assertRaises(TaskCardServerConfigError):
            TaskCardApplication(
                self.cards, {DRIP_ROLE: TOKEN, QUEUE_VIEW_ROLE: TOKEN}
            )
        with self.assertRaises(TaskCardServerConfigError):
            TaskCardApplication(self.cards, {})
        with self.assertRaises(TaskCardServerConfigError):
            TaskCardApplication(self.cards, "too-short")

    def test_comparison_checks_every_token_without_early_return(self):
        """A wrong-role or unmatched token must not be distinguishable by
        timing from a matched one: every configured token is compared, in
        both orders, regardless of where (or whether) a match occurs."""
        app = TaskCardApplication(
            self.cards, {DRIP_ROLE: TOKEN, QUEUE_VIEW_ROLE: QUEUE_VIEW_TOKEN}
        )
        calls = []
        real_compare = hmac.compare_digest

        def counting_compare(a, b):
            calls.append((a, b))
            return real_compare(a, b)

        with mock.patch(
            "foxhound.task_card_server.hmac.compare_digest",
            side_effect=counting_compare,
        ):
            calls.clear()
            app.authorized(f"Bearer {TOKEN}")
            self.assertEqual(len(calls), len(app.tokens))

            calls.clear()
            app.authorized(f"Bearer {QUEUE_VIEW_TOKEN}")
            self.assertEqual(len(calls), len(app.tokens))

            calls.clear()
            app.authorized("Bearer " + "n" * 43)
            self.assertEqual(len(calls), len(app.tokens))

    def test_access_log_stays_content_free_with_multiple_tokens(self):
        app = TaskCardApplication(
            self.cards, {DRIP_ROLE: TOKEN, QUEUE_VIEW_ROLE: QUEUE_VIEW_TOKEN}
        )
        with running_server(app) as endpoint:
            with self.assertLogs(
                "foxhound.task_card_server", level="INFO"
            ) as captured:
                request(
                    endpoint,
                    "/v1/task-cards/stats",
                    request_document(),
                    token=QUEUE_VIEW_TOKEN,
                )
        [line] = captured.output
        self.assertIn("method=POST", line)
        self.assertIn("route=/v1/task-cards/stats", line)
        self.assertIn("status=200", line)
        self.assertNotIn(QUEUE_VIEW_TOKEN, line)
        self.assertNotIn(QUEUE_VIEW_ROLE, line)
        self.assertNotIn("127.0.0.1", line)

    def test_load_role_tokens_syntax_and_backward_compatibility(self):
        root = Path(self.temporary.name)
        drip_path = root / "drip.token"
        drip_path.write_text(TOKEN + "\n", encoding="utf-8")
        drip_path.chmod(0o600)
        queue_path = root / "queue.token"
        queue_path.write_text(QUEUE_VIEW_TOKEN + "\n", encoding="utf-8")
        queue_path.chmod(0o600)

        # A single bare path is the legacy, unchanged invocation shape.
        self.assertEqual(
            load_role_tokens([str(drip_path)]), {DRIP_ROLE: TOKEN}
        )
        # A single spec may still name its role explicitly.
        self.assertEqual(
            load_role_tokens([f"drip={drip_path}"]), {DRIP_ROLE: TOKEN}
        )
        # Two roles, mirroring the repeated ROLE=PATH syntax.
        self.assertEqual(
            load_role_tokens(
                [f"drip={drip_path}", f"queue_view={queue_path}"]
            ),
            {DRIP_ROLE: TOKEN, QUEUE_VIEW_ROLE: QUEUE_VIEW_TOKEN},
        )

        for bad_specs in (
            [str(drip_path), str(queue_path)],  # ambiguous once plural
            [f"unknown_role={drip_path}"],
            [f"drip={drip_path}", f"drip={queue_path}"],
            [f"drip={drip_path}", f"queue_view={drip_path}"],
            [],
        ):
            with self.assertRaises(TaskCardServerConfigError):
                load_role_tokens(bad_specs)

    def test_every_configured_token_file_keeps_existing_discipline(self):
        """Duplicate, malformed, symlinked, or wrong-permission token files
        are rejected for every configured token, not just the first."""
        root = Path(self.temporary.name)
        drip_path = root / "drip.token"
        drip_path.write_text(TOKEN + "\n", encoding="utf-8")
        drip_path.chmod(0o600)

        # A bad *second* file is still rejected even though the first one
        # is perfectly valid.
        wrong_mode_path = root / "queue-wrong-mode.token"
        wrong_mode_path.write_text(QUEUE_VIEW_TOKEN + "\n", encoding="utf-8")
        wrong_mode_path.chmod(0o644)
        with self.assertRaisesRegex(TaskCardServerConfigError, "0600"):
            load_role_tokens(
                [f"drip={drip_path}", f"queue_view={wrong_mode_path}"]
            )

        symlinked_path = root / "queue-symlink.token"
        symlinked_path.symlink_to(drip_path)
        with self.assertRaisesRegex(TaskCardServerConfigError, "unavailable"):
            load_role_tokens(
                [f"drip={drip_path}", f"queue_view={symlinked_path}"]
            )

        missing_path = root / "queue-missing.token"
        with self.assertRaisesRegex(TaskCardServerConfigError, "unavailable"):
            load_role_tokens(
                [f"drip={drip_path}", f"queue_view={missing_path}"]
            )


class TaskCardClaimConsumerIdentityTests(unittest.TestCase):
    """ADR 0036 decision 2 (issue #193): `claim_next` records the resolved
    consumer identity in the same transaction as the claim, and this is the
    first real HTTP-reachable caller of `resolve_consumer` (added, but
    uncalled, by issue #192)."""

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.database = Path(self.temporary.name) / "foxhound.sqlite3"
        self.clock = Clock()
        CandidateInbox(self.database, clock=self.clock).initialize()
        with closing(sqlite3.connect(self.database)) as connection, connection:
            for index in range(1, 3):
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
            self.database, clock=self.clock, token_factory=lambda: CLAIM_TOKEN
        )
        self.cards.schedule()

    def _consumer_digest(self, card_id: int):
        with closing(sqlite3.connect(self.database)) as connection:
            row = connection.execute(
                "SELECT consumer_digest FROM task_review_cards WHERE id=?",
                (card_id,),
            ).fetchone()
        return row[0]

    def test_single_gateway_claim_is_unchanged_but_leaves_a_digest(self):
        """The acceptance test that matters most: with one configured
        token defaulting to role `drip`, the existing chat gateway must
        claim exactly as it does today -- same status code, same response
        keys, same claim fields -- it simply now leaves a digest behind
        server-side."""
        app = TaskCardApplication(self.cards, TOKEN)
        with running_server(app) as endpoint:
            status, _, claimed = request(
                endpoint,
                "/v1/task-cards/claim",
                request_document(lease_seconds=60),
            )
            self.assertEqual(status, 200)
            self.assertEqual(
                set(claimed),
                {"schema", "schema_version", "ok", "status", "claim"},
            )
            self.assertEqual(claimed["status"], "claimed")
            self.assertEqual(
                set(claimed["claim"]),
                {
                    "card_id", "card_version", "claim_token", "expires_at",
                    "delivery_key", "body", "reply_markup",
                },
            )
            card_id = claimed["claim"]["card_id"]
        self.assertEqual(
            self._consumer_digest(card_id),
            hashlib.sha256(TOKEN.encode("utf-8")).hexdigest(),
        )

    def test_claim_records_distinct_digest_per_configured_token(self):
        """Claiming under token A sets the column to digest(A); claiming
        under token B sets it to digest(B) -- independent identities, both
        HTTP-reachable through the one `claim` route."""
        app = TaskCardApplication(
            self.cards, {DRIP_ROLE: TOKEN, QUEUE_VIEW_ROLE: QUEUE_VIEW_TOKEN}
        )
        with running_server(app) as endpoint:
            _, _, first = request(
                endpoint,
                "/v1/task-cards/claim",
                request_document(lease_seconds=60),
                token=TOKEN,
            )
            _, _, second = request(
                endpoint,
                "/v1/task-cards/claim",
                request_document(lease_seconds=60),
                token=QUEUE_VIEW_TOKEN,
            )
        first_digest = self._consumer_digest(first["claim"]["card_id"])
        second_digest = self._consumer_digest(second["claim"]["card_id"])
        self.assertEqual(
            first_digest, hashlib.sha256(TOKEN.encode("utf-8")).hexdigest()
        )
        self.assertEqual(
            second_digest,
            hashlib.sha256(QUEUE_VIEW_TOKEN.encode("utf-8")).hexdigest(),
        )
        self.assertNotEqual(first_digest, second_digest)

    def test_unresolvable_role_fails_closed_over_http_and_claims_nothing(self):
        """The fail-closed branch issue #192 added but never called from
        any route (its acknowledged weak point): an authenticated token
        whose role cannot be resolved must refuse the claim outright, never
        claim with a null identity."""
        stray_token = "z" * 43
        app = TaskCardApplication(self.cards, TOKEN)
        app.tokens = {**app.tokens, "not_a_real_role": stray_token}
        before = self.cards.stats()
        with running_server(app) as endpoint:
            status, _, body = request(
                endpoint,
                "/v1/task-cards/claim",
                request_document(lease_seconds=60),
                token=stray_token,
            )
        self.assertEqual(status, 403)
        self.assertEqual(body["schema"], ERROR_SCHEMA)
        self.assertEqual(body["error"]["code"], "consumer_unresolved")
        # Nothing was claimed: the pool is exactly as it was before the
        # refused attempt, and the legitimate drip token can still claim.
        self.assertEqual(self.cards.stats(), before)
        with running_server(app) as endpoint:
            status, _, claimed = request(
                endpoint,
                "/v1/task-cards/claim",
                request_document(lease_seconds=60),
                token=TOKEN,
            )
        self.assertEqual(status, 200)
        self.assertEqual(claimed["status"], "claimed")


if __name__ == "__main__":
    unittest.main()
