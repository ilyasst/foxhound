from __future__ import annotations

from foxhound import migrate_database

import contextlib
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

from foxhound import CandidateInbox, task_duplicate_proposals
from foxhound.agent_profiles import (
    AgentProfileRegistry,
    general_profile,
    parse_profile,
)
from foxhound.execution_cards import (
    ExecutionCardStats,
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
    EXECUTION_DELIVERABLES_SCHEMA,
    EXECUTION_ARTIFACTS_SCHEMA,
    EXECUTION_VIEW_SCHEMA,
    EXECUTION_CLAIM_SCHEMA,
    EXECUTION_DETAIL_SCHEMA,
    EXECUTION_OPERATION_SCHEMA,
    EXECUTION_PRIORITY_SCHEMA,
    EXECUTION_QUEUE_SCHEMA,
    EXECUTION_SCHEDULE_SCHEMA,
    EXECUTION_STATS_SCHEMA,
    HEALTH_SCHEMA,
    OPERATION_SCHEMA,
    QUEUE_SCHEMA,
    QUEUE_SCHEMA_VERSION,
    QUEUE_VIEW_ROLE,
    REQUEST_SCHEMA,
    SCHEDULE_SCHEMA,
    STATS_SCHEMA,
    STATS_SCHEMA_VERSION,
    VIEW_SCHEMA,
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
    main,
)
from review_card_fixture import raise_review_cards
from foxhound.task_cards import TASK_CARD_READS, TaskCardService
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


def _read_control(document: dict) -> str:
    """The read action the rendered keyboard offers, whichever row it is on."""
    rows = document["presentation"]["reply_markup"]["inline_keyboard"]
    return [
        button["callback_data"].rsplit("|", 1)[1]
        for row in rows for button in row
        if button["callback_data"].rsplit("|", 1)[1] in TASK_CARD_READS
    ][0]


class TaskCardServerTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.database = Path(self.temporary.name) / "foxhound.sqlite3"
        self.artifact_root = Path(self.temporary.name) / "task-work"
        self.artifact_root.mkdir(mode=0o700)
        self.clock = Clock()
        migrate_database(self.database)
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
            artifact_root=self.artifact_root,
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

    def test_execution_deliverables_route_is_a_delivered_card_read(self):
        workflow = self.execution.schedule(1, expected_task_version=1)
        self.execution.start_action(
            1, expected_version=workflow.version, action="start"
        )
        run = self.execution.claim_next()
        self.execution.record_result(ExecutionResultEnvelope(
            result_id="synthetic-deliverables-plan",
            task_id=1,
            task_version=1,
            workflow_version=run.workflow_version,
            phase="plan",
            claim_token=run.token,
            outcome="awaiting_plan",
            summary="Synthetic plan.",
            work_markdown="Synthetic work.",
            deliverables=("## Synthetic deliverable\n\nReview this draft.",),
        ))
        self.execution_cards.schedule()
        claim = self.execution_cards.claim_next()
        delivered = self.execution_cards.complete_delivery(
            claim.card.id,
            expected_version=claim.card.version,
            claim_token=claim.token,
            transport="synthetic",
            delivery_ref="synthetic-deliverables-message",
        )
        before = self.execution_cards.stats()

        response = self.app.dispatch(
            "execution_deliverables",
            request_document(
                card_id=claim.card.id,
                card_version=delivered.card_version,
            ),
        )

        self.assertEqual(response["schema"], EXECUTION_DELIVERABLES_SCHEMA)
        self.assertTrue(response["ok"])
        self.assertIn("# Deliverables", response["text"])
        self.assertIn("Synthetic deliverable", response["text"])
        self.assertEqual(self.execution_cards.stats(), before)

        stale = self.app.dispatch(
            "execution_deliverables",
            request_document(
                card_id=claim.card.id,
                card_version=delivered.card_version + 1,
            ),
        )
        self.assertFalse(stale["ok"])
        self.assertIsNone(stale["text"])
        self.assertEqual(stale["refusal"], "stale_version")

    def test_a_deployment_without_an_artifact_root_says_so_distinctly(self):
        """"Not enabled here" must not arrive as "this card is in a bad state".

        Both used to be `invalid_state`, so a caller could not tell a
        deployment that serves no files from a card that produced none, and
        a reader was left waiting for an attachment that was never coming.
        """
        unconfigured = ExecutionCardService(
            self.database, clock=self.clock,
            token_factory=lambda: EXECUTION_DELIVERY_TOKEN,
        )
        self.assertFalse(unconfigured.serves_artifacts)
        self.assertTrue(self.execution_cards.serves_artifacts)
        app = TaskCardApplication(
            self.cards, TOKEN, execution_cards=unconfigured,
        )

        refused = app.dispatch("execution_artifacts", request_document(
            card_id=1, card_version=1,
        ))

        self.assertFalse(refused["ok"])
        self.assertEqual(refused["refusal"], "artifacts_unavailable")
        self.assertIsNone(refused["artifacts"])
        # The same request against a configured deployment that simply has
        # no files is an answer, not a refusal. That is the distinction.
        self.assertNotEqual(refused["refusal"], "invalid_state")

    def test_execution_artifacts_are_listed_and_downloaded_by_record(self):
        workflow = self.execution.schedule(1, expected_task_version=1)
        self.execution.start_action(
            1, expected_version=workflow.version, action="start"
        )
        run = self.execution.claim_next()
        run_directory = self.artifact_root / "task-1" / "runs" / "plan-example"
        run_directory.mkdir(parents=True, mode=0o700)
        content = b"Synthetic attachment.\n"
        (run_directory / "example.txt").write_bytes(content)
        self.execution.record_result(ExecutionResultEnvelope(
            result_id="synthetic-artifact-plan",
            task_id=1,
            task_version=1,
            workflow_version=run.workflow_version,
            phase="plan",
            claim_token=run.token,
            outcome="awaiting_plan",
            summary="Synthetic plan.",
            work_markdown="Synthetic work.",
            artifacts=({
                "relative_path": "example.txt",
                "name": "example.txt",
                "size_bytes": len(content),
                "content_digest": hashlib.sha256(content).hexdigest(),
                "run_directory": str(run_directory),
            },),
        ))
        self.execution_cards.schedule()
        claim = self.execution_cards.claim_next()
        delivered = self.execution_cards.complete_delivery(
            claim.card.id, expected_version=claim.card.version,
            claim_token=claim.token, transport="synthetic",
            delivery_ref="synthetic-artifact-message",
        )

        listed = self.app.dispatch("execution_artifacts", request_document(
            card_id=claim.card.id, card_version=delivered.card_version,
        ))
        self.assertEqual(listed["schema"], EXECUTION_ARTIFACTS_SCHEMA)
        self.assertEqual(listed["artifacts"], [{
            "ordinal": 0, "name": "example.txt", "size_bytes": len(content),
        }])
        downloaded = self.app.dispatch("execution_artifact", request_document(
            card_id=claim.card.id, card_version=delivered.card_version,
            ordinal=0,
        ))
        self.assertEqual(downloaded["artifact"].content, content)
        with running_server(self.app) as endpoint:
            raw_request = urllib.request.Request(
                endpoint + "/v1/execution-cards/artifact",
                data=json.dumps(request_document(
                    card_id=claim.card.id,
                    card_version=delivered.card_version,
                    ordinal=0,
                )).encode("utf-8"),
                headers={
                    "Authorization": f"Bearer {TOKEN}",
                    "Content-Type": "application/json",
                },
                method="POST",
            )
            with urllib.request.urlopen(raw_request, timeout=2) as response:
                self.assertEqual(response.headers["Content-Type"],
                                 "application/octet-stream")
                self.assertEqual(response.read(), content)
        (run_directory / "example.txt").write_bytes(b"Changed.")
        refused = self.execution_cards.artifact(
            claim.card.id, expected_version=delivered.card_version, ordinal=0,
        )
        self.assertFalse(refused.accepted)
        self.assertEqual(refused.refusal.value, "invalid_state")

    def _queue_card(self):
        self.execution.schedule(1, expected_task_version=1)
        self.execution_cards.schedule()
        card = self.execution_cards.due(limit=1)[0]
        return card

    def test_execution_retraction_routes_acknowledge_a_stale_delivery(self):
        self.execution.schedule(1, expected_task_version=1)
        self.execution_cards.schedule()
        delivery = self.execution_cards.claim_next()
        self.execution_cards.complete_delivery(
            delivery.card.id,
            expected_version=delivery.card.version,
            claim_token=delivery.token,
            transport="synthetic",
            delivery_ref="synthetic-stale-message",
        )
        with closing(sqlite3.connect(self.database)) as connection, connection:
            connection.execute(
                "UPDATE task_execution_workflows SET status='queued',"
                "version=version+1,claim_token_digest=NULL,claimed_at=NULL,"
                "claim_heartbeat_at=NULL,claim_expires_at=NULL,"
                "current_run_id=NULL WHERE task_id=1"
            )
        self.execution_cards.schedule()

        claimed = self.app.dispatch(
            "execution_retraction_claim",
            request_document(lease_seconds=60),
            authorization=f"Bearer {TOKEN}",
        )
        self.assertEqual(claimed["status"], "claimed")
        self.assertEqual(claimed["claim"]["delivery_ref"],
                         "synthetic-stale-message")
        completed = self.app.dispatch(
            "execution_retracted",
            request_document(
                card_id=claimed["claim"]["card_id"],
                claim_token=claimed["claim"]["claim_token"],
            ),
            authorization=f"Bearer {TOKEN}",
        )
        self.assertEqual(completed["status"], "applied")

    def test_execution_queue_resolve_claims_and_resolves_atomically(self):
        card = self._queue_card()
        queue = "q" * 43
        app = TaskCardApplication(
            self.cards, {DRIP_ROLE: TOKEN, QUEUE_VIEW_ROLE: queue},
            execution_cards=self.execution_cards,
            execution_tokens={DRIP_ROLE: TOKEN, QUEUE_VIEW_ROLE: queue},
        )
        result = app.dispatch(
            "execution_resolve",
            request_document(card_id=card.id, card_version=card.version, action="start"),
            authorization=f"Bearer {queue}",
        )
        self.assertTrue(result["ok"])
        self.assertEqual(result["status"], "resolved")
        with closing(sqlite3.connect(self.database)) as db:
            row = db.execute("SELECT status,consumer_digest,transport FROM execution_review_cards WHERE id=?", (card.id,)).fetchone()
            self.assertEqual(row[0], "resolved")
            self.assertIsNone(row[1])
            self.assertEqual(row[2], "queue_view")

    def test_execution_priority_is_queue_scoped_and_version_fenced(self):
        queue = "q" * 43
        self.execution.schedule(1, expected_task_version=1)
        ready = self.execution.start_action(1, expected_version=1, action="start")
        app = TaskCardApplication(
            self.cards,
            {DRIP_ROLE: TOKEN, QUEUE_VIEW_ROLE: queue},
            execution_cards=self.execution_cards,
            execution_workflows=self.execution,
            execution_tokens={DRIP_ROLE: TOKEN, QUEUE_VIEW_ROLE: queue},
        )
        with running_server(app) as endpoint:
            status, _, response = request(
                endpoint,
                "/v1/execution-workflows/priority",
                request_document(
                    task_id=1, workflow_version=ready.version, action="raise"
                ),
                token=queue,
            )
        self.assertEqual(status, 200)
        self.assertEqual(response["schema"], EXECUTION_PRIORITY_SCHEMA)
        self.assertTrue(response["ok"])
        self.assertEqual(response["priority"], "raised")
        self.assertEqual(response["workflow_version"], ready.version + 1)
        self.assertEqual(self.execution.claim_next().task_id, 1)

        with self.assertRaises(TaskCardServerRequestError):
            app.dispatch(
                "execution_priority",
                request_document(
                    task_id=1, workflow_version=ready.version, action="raise"
                ),
                authorization=f"Bearer {TOKEN}",
            )
        with self.assertRaises(TaskCardServerRequestError):
            app.dispatch(
                "execution_priority",
                request_document(
                    task_id=1, workflow_version=ready.version, action="raise",
                    extra="rejected",
                ),
                authorization=f"Bearer {queue}",
            )

    def test_execution_queue_resolve_is_strict_and_queue_view_only(self):
        card = self._queue_card()
        queue = "q" * 43
        app = TaskCardApplication(self.cards, {DRIP_ROLE: TOKEN, QUEUE_VIEW_ROLE: queue}, execution_cards=self.execution_cards, execution_tokens={DRIP_ROLE: TOKEN, QUEUE_VIEW_ROLE: queue})
        with self.assertRaises(TaskCardServerRequestError):
            app.dispatch("execution_resolve", request_document(card_id=card.id, card_version=card.version, action="start", extra="x"), authorization=f"Bearer {queue}")
        with self.assertRaises(TaskCardServerRequestError):
            app.dispatch("execution_resolve", request_document(card_id=card.id, card_version=card.version, action="start"), authorization=f"Bearer {TOKEN}")
        stale = app.dispatch("execution_resolve", request_document(card_id=card.id, card_version=card.version + 1, action="start"), authorization=f"Bearer {queue}")
        self.assertFalse(stale["ok"])
        self.assertEqual(stale["refusal"], "stale_version")

    def test_execution_queue_resolve_bounded_discussion_and_invalid_input_rollback(self):
        card = self._queue_card()
        digest = hashlib.sha256(("q" * 43).encode()).hexdigest()
        bad = self.execution_cards.resolve_queue_view(card.id, expected_version=card.version, action="discussion", input_kind="discussion", value="\x00", consumer_digest=digest)
        self.assertEqual(bad.refusal.value, "invalid_argument")
        good = self.execution_cards.resolve_queue_view(card.id, expected_version=card.version, action="discussion", input_kind="discussion", value="Synthetic note", consumer_digest=digest)
        self.assertTrue(good.accepted)
        self.assertEqual(self.execution_cards.event_count(), 4)

    def test_execution_queue_resolve_at_ceiling_and_old_replay(self):
        queue = "q" * 43
        app = TaskCardApplication(self.cards, {DRIP_ROLE: TOKEN, QUEUE_VIEW_ROLE: queue}, execution_cards=self.execution_cards, execution_tokens={DRIP_ROLE: TOKEN, QUEUE_VIEW_ROLE: queue})
        first = self._queue_card()
        self.execution.schedule(2, expected_task_version=1)
        self.execution.schedule(3, expected_task_version=1)
        self.execution_cards.schedule()
        third = self.execution_cards.due(limit=3)[2]
        claim1 = self.execution_cards.claim_next(consumer_digest=hashlib.sha256(queue.encode()).hexdigest(), consumer_role=QUEUE_VIEW_ROLE)
        claim2 = self.execution_cards.claim_next(consumer_digest=hashlib.sha256(queue.encode()).hexdigest(), consumer_role=QUEUE_VIEW_ROLE)
        self.assertIsNotNone(claim1)
        self.assertIsNotNone(claim2)
        result = app.dispatch("execution_resolve", request_document(card_id=third.id, card_version=third.version, action="start"), authorization=f"Bearer {queue}")
        self.assertEqual(result["status"], "at_ceiling")

    def test_execution_queue_resolve_rolls_back_after_internal_delivery(self):
        card = self._queue_card()
        digest = hashlib.sha256(("q" * 43).encode()).hexdigest()
        before_events = self.execution_cards.event_count()
        original_event = self.execution_cards._event
        calls = 0

        def fail_after_delivery(*args, **kwargs):
            nonlocal calls
            calls += 1
            if calls == 2:
                raise RuntimeError("synthetic injected failure")
            return original_event(*args, **kwargs)

        with mock.patch.object(self.execution_cards, "_event", fail_after_delivery):
            with self.assertRaisesRegex(RuntimeError, "synthetic injected failure"):
                self.execution_cards.resolve_queue_view(
                    card.id, expected_version=card.version, action="start",
                    consumer_digest=digest,
                )
        with closing(sqlite3.connect(self.database)) as db:
            state = db.execute(
                "SELECT status,version,consumer_digest FROM execution_review_cards WHERE id=?",
                (card.id,),
            ).fetchone()
            self.assertEqual(state, ("pending", 1, None))
            self.assertEqual(
                db.execute("SELECT version,status FROM task_execution_workflows WHERE task_id=?", (card.task_id,)).fetchone(),
                (1, "awaiting_start"),
            )
            self.assertEqual(db.execute("SELECT count(*) FROM execution_reader_inputs").fetchone()[0], 0)
        self.assertEqual(self.execution_cards.event_count(), before_events)

    def test_execution_queue_resolve_rejects_agent_selection_input(self):
        card = self._queue_card()
        queue = "q" * 43
        app = TaskCardApplication(
            self.cards, {DRIP_ROLE: TOKEN, QUEUE_VIEW_ROLE: queue},
            execution_cards=self.execution_cards,
            execution_tokens={DRIP_ROLE: TOKEN, QUEUE_VIEW_ROLE: queue},
        )
        with self.assertRaisesRegex(TaskCardServerRequestError, "agent selection"):
            app.dispatch(
                "execution_resolve",
                request_document(
                    card_id=card.id, card_version=card.version,
                    action="start", selection_token="a" * 20,
                ),
                authorization=f"Bearer {queue}",
            )

    def test_execution_queue_is_scoped_bounded_and_non_mutating(self):
        self.execution.schedule(1, expected_task_version=1)
        self.execution_cards.schedule()
        before = (self.execution_cards.count(), self.execution_cards.event_count())
        queue_token = "q" * 43
        app = TaskCardApplication(
            self.cards, {DRIP_ROLE: TOKEN, QUEUE_VIEW_ROLE: queue_token},
            execution_cards=self.execution_cards,
            execution_tokens={DRIP_ROLE: TOKEN, QUEUE_VIEW_ROLE: queue_token},
        )
        response = app.dispatch(
            "execution_queue", request_document(limit=10),
            authorization=f"Bearer {queue_token}",
        )
        self.assertEqual(
            (response["schema"], response["schema_version"], response["ok"]),
            (EXECUTION_QUEUE_SCHEMA, 1, True),
        )
        self.assertEqual(len(response["cards"]), 1)
        card = response["cards"][0]
        self.assertEqual(
            set(card), {"id", "version", "kind", "phase", "task", "owner",
                        "summary", "work_digest", "questions", "external_actions",
                        "deliverables"},
        )
        for forbidden in ("task_id", "agent_profile_id", "agent_profile_revision",
                          "task_work_directory", "task_kb_file", "origin_sources",
                          "claim_token", "delivery_key", "status"):
            self.assertNotIn(forbidden, card)
        self.assertEqual((self.execution_cards.count(), self.execution_cards.event_count()), before)
        with self.assertRaises(TaskCardServerRequestError):
            app.dispatch("execution_queue", request_document(limit=0),
                         authorization=f"Bearer {queue_token}")
        with self.assertRaises(TaskCardServerRequestError):
            app.dispatch("execution_queue", request_document(limit=1),
                         authorization=f"Bearer {TOKEN}")

        claim = self.execution_cards.claim_next(
            consumer_digest=hashlib.sha256(TOKEN.encode()).hexdigest(),
            consumer_role=DRIP_ROLE,
        )
        self.assertIsNotNone(claim)
        held = app.dispatch(
            "execution_queue", request_document(limit=10),
            authorization=f"Bearer {queue_token}",
        )
        self.assertEqual(held["cards"], [])

    def test_execution_detail_is_bounded_versioned_and_queue_scoped(self):
        card = self._queue_card()
        queue = "q" * 43
        app = TaskCardApplication(
            self.cards, {DRIP_ROLE: TOKEN, QUEUE_VIEW_ROLE: queue},
            execution_cards=self.execution_cards,
            execution_tokens={DRIP_ROLE: TOKEN, QUEUE_VIEW_ROLE: queue},
        )
        before = (self.execution_cards.count(), self.execution_cards.event_count())
        detail = app.dispatch(
            "execution_detail",
            request_document(card_id=card.id, card_version=card.version),
            authorization=f"Bearer {queue}",
        )
        self.assertEqual(detail["schema"], EXECUTION_DETAIL_SCHEMA)
        self.assertEqual(detail["schema_version"], 2)
        self.assertTrue(detail["ok"])
        self.assertEqual(detail["status"], "awaiting_start")
        self.assertEqual(detail["phase"], "plan")
        self.assertEqual(detail["deliverables"], [])
        self.assertIsNone(detail["failure_reason"])
        self.assertIsNone(detail["failure_exit_code"])
        self.assertIsNone(detail["failure_run_id"])
        self.assertNotIn("work_markdown", detail)
        self.assertNotIn("task_work_directory", detail)
        self.assertEqual((self.execution_cards.count(), self.execution_cards.event_count()), before)
        with self.assertRaises(TaskCardServerRequestError):
            app.dispatch(
                "execution_detail",
                request_document(card_id=card.id, card_version=card.version, extra="x"),
                authorization=f"Bearer {queue}",
            )
        with self.assertRaises(TaskCardServerRequestError):
            app.dispatch(
                "execution_detail",
                request_document(card_id=card.id, card_version=card.version),
                authorization=f"Bearer {TOKEN}",
            )
        claimed = self.execution_cards.claim_next(
            consumer_digest=hashlib.sha256(TOKEN.encode()).hexdigest(),
            consumer_role=DRIP_ROLE,
        )
        self.assertIsNotNone(claimed)
        held = app.dispatch(
            "execution_detail",
            request_document(card_id=card.id, card_version=card.version),
            authorization=f"Bearer {queue}",
        )
        self.assertFalse(held["ok"])
        self.assertIsNone(held["summary"])

    def test_execution_detail_stale_refusal_is_content_free_and_versioned(self):
        card = self._queue_card()
        queue = "q" * 43
        app = TaskCardApplication(
            self.cards, {DRIP_ROLE: TOKEN, QUEUE_VIEW_ROLE: queue},
            execution_cards=self.execution_cards,
            execution_tokens={DRIP_ROLE: TOKEN, QUEUE_VIEW_ROLE: queue},
        )
        response = app.dispatch(
            "execution_detail",
            request_document(card_id=card.id, card_version=card.version + 1),
            authorization=f"Bearer {queue}",
        )
        self.assertEqual(
            (response["schema"], response["schema_version"], response["ok"]),
            (EXECUTION_DETAIL_SCHEMA, 2, False),
        )
        self.assertEqual(response["refusal"], "stale_version")
        self.assertIsNone(response["summary"])
        self.assertIsNone(response["work_digest"])
        self.assertEqual(response["deliverables"], [])
        self.assertIsNone(response["failure_reason"])
        self.assertIsNone(response["failure_exit_code"])
        self.assertIsNone(response["failure_run_id"])

    def test_claim_at_ceiling_is_distinct_and_content_free(self):
        raise_review_cards(self.database, self.clock())
        app = TaskCardApplication(
            self.cards, {DRIP_ROLE: TOKEN, QUEUE_VIEW_ROLE: "q" * 43}
        )
        with running_server(app) as endpoint:
            for _ in range(2):
                status, _, body = request(
                    endpoint, "/v1/task-cards/claim",
                    request_document(lease_seconds=60),
                    token="q" * 43,
                )
                self.assertEqual(status, 200)
                self.assertEqual(body["status"], "claimed")
            status, _, body = request(
                endpoint, "/v1/task-cards/claim",
                request_document(lease_seconds=60), token="q" * 43,
            )
        self.assertEqual(status, 200)
        self.assertEqual(body["status"], "at_ceiling")
        self.assertEqual(body["claim"], None)
        self.assertEqual((body["held_count"], body["ceiling"]), (2, 2))
        self.assertNotIn("card_id", body)
        self.assertNotIn("task_id", body)

    def test_view_route_opens_a_comparison_without_answering_it(self):
        """A read: the card stays exactly as answerable as it was."""
        with closing(sqlite3.connect(self.database)) as connection, connection:
            connection.row_factory = sqlite3.Row
            # A proposal compares two tasks the same person owns.
            connection.execute(
                "UPDATE tasks SET owner='Person A',owner_ref_version=1,"
                "owner_kind='person',owner_speaker_id='SPK_1',"
                "owner_canonical_speaker_id='SPK_1',"
                "owner_speaker_registry_id='registry-A',owner_pinned=0,"
                "owner_provisional=0 WHERE id IN (1,2)"
            )
            task_duplicate_proposals.propose(
                connection,
                task_id_a=1,
                task_id_b=2,
                basis="Same synthetic deliverable and confirmed owner.",
                detector="synthetic-detector",
                now=NOW.isoformat(timespec="seconds"),
            )
        self.cards.schedule_duplicate_proposals()
        digest = hashlib.sha256(TOKEN.encode()).hexdigest()
        claim = self.cards.claim_next(consumer_digest=digest)
        self.assertTrue(self.cards.complete_delivery(
            claim.card.id,
            expected_version=claim.card.version,
            claim_token=claim.token,
            transport="synthetic",
            delivery_ref="view-message",
        ).accepted)
        before = (self.cards.count(), self.cards.event_count())

        response = self.app.dispatch(
            "view",
            request_document(
                card_id=claim.card.id,
                card_version=claim.card.version,
                view="duplicate_expand",
            ),
        )

        self.assertEqual(response["schema"], VIEW_SCHEMA)
        self.assertTrue(response["ok"])
        self.assertTrue(response["expanded"])
        self.assertIn("Same task?", response["presentation"]["body"])
        self.assertIn("&lt;private&gt;", response["presentation"]["body"])
        self.assertIn(
            "inline_keyboard", response["presentation"]["reply_markup"])
        self.assertEqual(
            (self.cards.count(), self.cards.event_count()), before)

        collapsed = self.app.dispatch(
            "view",
            request_document(
                card_id=claim.card.id,
                card_version=claim.card.version,
                view="duplicate_collapse",
            ),
        )
        self.assertFalse(collapsed["expanded"])
        # Which control the reader is offered is what the two views differ
        # by here: this pair carries no evidence for the expansion to add.
        self.assertEqual(
            [_read_control(document) for document in (response, collapsed)],
            ["duplicate_collapse", "duplicate_expand"],
        )

        stale = self.app.dispatch(
            "view",
            request_document(
                card_id=claim.card.id,
                card_version=claim.card.version + 1,
                view="duplicate_expand",
            ),
        )
        self.assertFalse(stale["ok"])
        self.assertIsNone(stale["presentation"])

        with self.assertRaises(TaskCardServerRequestError):
            self.app.dispatch(
                "view",
                request_document(
                    card_id=claim.card.id,
                    card_version=claim.card.version,
                    view="duplicate_confirm",
                ),
            )

    def test_execution_view_route_restores_a_card_without_writing(self):
        self.execution.schedule(1, expected_task_version=1)
        self.execution_cards.schedule()
        claim = self.execution_cards.claim_next()
        self.execution_cards.complete_delivery(
            claim.card.id,
            expected_version=claim.card.version,
            claim_token=claim.token,
            transport="synthetic",
            delivery_ref="view-message",
        )
        before = self.execution_cards.stats()

        response = self.app.dispatch(
            "execution_view",
            request_document(
                card_id=claim.card.id,
                card_version=claim.card.version,
            ),
        )

        self.assertEqual(response["schema"], EXECUTION_VIEW_SCHEMA)
        self.assertTrue(response["ok"])
        self.assertIn("Synthetic task 1", response["presentation"]["body"])
        self.assertIn(
            "inline_keyboard", response["presentation"]["reply_markup"]
        )
        self.assertEqual(response["kind"], "start")
        self.assertEqual(self.execution_cards.stats(), before)

        stale = self.app.dispatch(
            "execution_view",
            request_document(
                card_id=claim.card.id,
                card_version=claim.card.version + 1,
            ),
        )
        self.assertFalse(stale["ok"])
        # Absent, not partial: a caller that cannot restore the card must
        # not be handed something that looks like it could be shown.
        self.assertIsNone(stale["presentation"])
        self.assertIsNone(stale["kind"])
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

    def test_duplicate_actions_are_accepted_through_http_boundary(self):
        for action, expected_state, expected_relations in (
            ("duplicate_confirm", "confirmed", 1),
            ("duplicate_reject", "rejected", 0),
        ):
            with self.subTest(action=action), tempfile.TemporaryDirectory() as root:
                database = Path(root) / "foxhound.sqlite3"
                migrate_database(database)
                with closing(sqlite3.connect(database)) as connection, connection:
                    connection.row_factory = sqlite3.Row
                    for text in (
                        "Prepare the synthetic rollout checklist",
                        "Draft the synthetic rollout checklist",
                    ):
                        connection.execute(
                            "INSERT INTO tasks(status,text,owner,version,created_at,"
                            "updated_at,owner_ref_version,owner_kind,"
                            "owner_speaker_id,owner_canonical_speaker_id,"
                            "owner_speaker_registry_id,owner_pinned,"
                            "owner_provisional) VALUES('open',?,'Person A',1,"
                            "?,?,1,'person','SPK_1','SPK_1','registry-A',0,0)",
                            (text, NOW.isoformat(), NOW.isoformat()),
                        )
                    proposal = task_duplicate_proposals.propose(
                        connection,
                        task_id_a=1,
                        task_id_b=2,
                        basis="Same synthetic deliverable and confirmed owner.",
                        detector="synthetic-detector",
                        now=NOW.isoformat(),
                    )

                cards = TaskCardService(
                    database,
                    clock=self.clock,
                    token_factory=lambda: CLAIM_TOKEN,
                )
                cards.schedule_duplicate_proposals()
                claim = cards.claim_next(
                    consumer_digest=hashlib.sha256(TOKEN.encode()).hexdigest()
                )
                self.assertIsNotNone(claim)
                self.assertTrue(cards.complete_delivery(
                    claim.card.id,
                    expected_version=claim.card.version,
                    claim_token=claim.token,
                    transport="synthetic",
                    delivery_ref="message-alpha",
                ).accepted)

                with running_server(TaskCardApplication(cards, TOKEN)) as endpoint:
                    status, _, body = request(
                        endpoint,
                        "/v1/task-cards/action",
                        request_document(
                            card_id=claim.card.id,
                            card_version=claim.card.version,
                            action=action,
                        ),
                    )

                self.assertEqual(status, 200)
                self.assertEqual(body["disposition"], "applied")
                self.assertEqual(body["card_status"], "cancelled")
                with closing(sqlite3.connect(database)) as connection:
                    state = connection.execute(
                        "SELECT state FROM task_duplicate_proposals WHERE id=?",
                        (proposal.proposal_id,),
                    ).fetchone()[0]
                    relation_count = connection.execute(
                        "SELECT count(*) FROM task_relations"
                    ).fetchone()[0]
                self.assertEqual(state, expected_state)
                self.assertEqual(relation_count, expected_relations)

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
                endpoint, "/v2/task-cards/stats", request_document()
            )
            self.assertEqual(status, 200)
            self.assertEqual(body["schema_version"], STATS_SCHEMA_VERSION)
            self.assertEqual(body["elsewhere"], 0)
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

    def test_v1_stats_keeps_its_key_set_when_steer_work_exists(self):
        """A versioned response may not grow a field on some deployments.

        The client validates this key set exactly.  Adding steer counts
        only when a steer card happens to exist makes the response valid
        on quiet deployments and refused on busy ones -- and a refused
        response stops the sweep, so no execution card reaches the reader
        at all.  Observed: a steer card entered `delivering`, and
        sixty-one seconds later delivery stopped entirely.
        """
        steer = ExecutionCardStats(
            pending=0, delivering=0, delivered=0, active=0,
            steer_pending=2, steer_delivering=1, steer_delivered=3,
        )
        with mock.patch.object(
            ExecutionCardService, "stats", return_value=steer
        ):
            with running_server(self.app) as endpoint:
                status, _, body = request(
                    endpoint,
                    "/v1/execution-cards/stats",
                    request_document(),
                )

        self.assertEqual(status, 200)
        self.assertEqual(set(body), {
            "schema", "schema_version", "ok",
            "pending", "delivering", "delivered", "active",
        })

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
        raise_review_cards(self.database, self.clock(), limit=2)
        with running_server(self.app) as endpoint:
            status, _, scheduled = request(
                endpoint,
                "/v1/task-cards/schedule",
                request_document(limit=2),
            )
            # The route still runs a scheduling pass -- it retracts stale
            # cards and asks the done-check and duplicate questions -- but it
            # no longer manufactures a review card for every open task, so a
            # healthy call against an already-carded queue creates nothing.
            self.assertEqual((status, scheduled["schema"], scheduled["created"]),
                             (200, SCHEDULE_SCHEMA, 0))
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

    def test_execution_comment_and_go_route_advances_a_plan_with_its_note(self):
        workflow = self.execution.schedule(1, expected_task_version=1)
        self.execution.start_action(
            1, expected_version=workflow.version, action="start"
        )
        run = self.execution.claim_next()
        self.execution.record_result(ExecutionResultEnvelope(
            result_id="synthetic-comment-go-plan",
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
            delivery_ref="synthetic-comment-go-message",
        )

        result = self.app.dispatch(
            "execution_comment_and_go",
            request_document(
                card_id=claim.card.id,
                card_version=claim.card.version,
                value="Use the synthetic constraint.",
            ),
        )

        self.assertEqual(
            (result["ok"], result["card_status"], result["workflow_status"],
             result["workflow_phase"]),
            (True, "resolved", "queued", "execute"),
        )
        stale = self.app.dispatch(
            "execution_comment_and_go",
            request_document(
                card_id=claim.card.id,
                card_version=claim.card.version,
                value="A second synthetic instruction.",
            ),
        )
        self.assertEqual((stale["ok"], stale["refusal"]),
                         (False, "stale_version"))
        with self.assertRaises(TaskCardServerRequestError):
            self.app.dispatch(
                "execution_comment_and_go",
                request_document(
                    card_id=claim.card.id,
                    card_version=claim.card.version,
                    value=" ",
                ),
            )

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

    def test_database_contention_is_a_bounded_retryable_response(self):
        cases = (
            (
                "/v1/task-cards/schedule",
                request_document(limit=1),
                self.cards,
            ),
            (
                "/v1/execution-cards/schedule",
                request_document(limit=1),
                self.execution_cards,
            ),
        )
        with running_server(self.app) as endpoint:
            for path, document, service in cases:
                with self.subTest(path=path), mock.patch.object(
                    service,
                    "schedule",
                    side_effect=sqlite3.OperationalError("database is locked"),
                ):
                    status, headers, body = request(endpoint, path, document)

                self.assertEqual(status, 503)
                self.assertEqual(headers["Retry-After"], "1")
                self.assertEqual(body, {
                    "schema": ERROR_SCHEMA,
                    "schema_version": 1,
                    "ok": False,
                    "error": {
                        "code": "temporarily_unavailable",
                        "message": "card service is temporarily busy",
                    },
                })

    def test_other_database_errors_remain_opaque_and_non_retryable(self):
        with running_server(self.app) as endpoint, mock.patch.object(
            self.cards,
            "schedule",
            side_effect=sqlite3.OperationalError("synthetic database error"),
        ):
            status, headers, body = request(
                endpoint,
                "/v1/task-cards/schedule",
                request_document(limit=1),
            )

        self.assertEqual(status, 500)
        self.assertNotIn("Retry-After", headers)
        self.assertEqual(body["error"], {
            "code": "internal_error",
            "message": "internal service error",
        })


QUEUE_VIEW_TOKEN = "q" * 43


class TaskCardQueueProjectionTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.database = Path(self.temporary.name) / "foxhound.sqlite3"
        self.clock = Clock()
        migrate_database(self.database)
        with closing(sqlite3.connect(self.database)) as connection, connection:
            for index in range(1, 4):
                connection.execute(
                    "INSERT INTO tasks(status,text,owner,due,version,created_at,"
                    "updated_at,closed_at) VALUES('open',?,?,?,?,?,?,NULL)",
                    (
                        f"Synthetic queue task {index}", f"Person {index}",
                        None, 1, f"2030-01-{index:02d}T12:00:00+00:00",
                        NOW.isoformat(timespec="seconds"),
                    ),
                )
        self.cards = TaskCardService(
            self.database, clock=self.clock, token_factory=lambda: CLAIM_TOKEN
        )
        raise_review_cards(self.database, self.clock(), limit=3)
        self.app = TaskCardApplication(
            self.cards, {DRIP_ROLE: TOKEN, QUEUE_VIEW_ROLE: QUEUE_VIEW_TOKEN}
        )

    def test_queue_view_reads_due_cards_without_claiming_and_drip_is_refused(self):
        before = (self.cards.count(), self.cards.event_count())
        with running_server(self.app) as endpoint:
            status, _, body = request(
                endpoint, "/v1/task-cards/queue",
                request_document(limit=3), token=QUEUE_VIEW_TOKEN,
            )
            self.assertEqual(status, 200)
            self.assertEqual(
                (body["schema"], body["schema_version"], body["ok"]),
                (QUEUE_SCHEMA, QUEUE_SCHEMA_VERSION, True),
            )
            self.assertEqual(len(body["cards"]), 3)
            self.assertEqual(body["cards"][0]["text"], "Synthetic queue task 1")
            self.assertEqual(body["cards"][0]["status"], "pending")
            status, _, refused = request(
                endpoint, "/v1/task-cards/queue",
                request_document(limit=3), token=TOKEN,
            )
            self.assertEqual((status, refused["error"]["code"]),
                             (403, "role_forbidden"))
        self.assertEqual((self.cards.count(), self.cards.event_count()), before)
        # A read did not consume a lease: the drip consumer can still claim.
        claim = self.cards.claim_next(
            lease_seconds=60,
            consumer_digest=hashlib.sha256(TOKEN.encode()).hexdigest(),
            consumer_role=DRIP_ROLE,
        )
        self.assertIsNotNone(claim)

    def test_queue_projection_excludes_cards_held_by_any_consumer(self):
        claim = self.cards.claim_next(
            lease_seconds=60,
            consumer_digest=hashlib.sha256(TOKEN.encode()).hexdigest(),
            consumer_role=DRIP_ROLE,
        )
        self.assertIsNotNone(claim)
        held_id = claim.card.id
        with running_server(self.app) as endpoint:
            status, _, body = request(
                endpoint, "/v1/task-cards/queue",
                request_document(limit=3), token=QUEUE_VIEW_TOKEN,
            )
        self.assertEqual(status, 200)
        self.assertNotIn(held_id, {card["id"] for card in body["cards"]})

    def _resolve_request(self, card, action="done"):
        return self.app.dispatch(
            "resolve",
            {"schema": REQUEST_SCHEMA, "schema_version": 1,
             "card_id": card.id, "card_version": card.version,
             "action": action},
            authorization=f"Bearer {QUEUE_VIEW_TOKEN}",
        )

    def test_resolve_claims_delivers_and_acts_in_one_operation(self):
        card = self.cards.due(limit=1)[0]
        result = self._resolve_request(card)
        self.assertEqual((result["status"], result["ok"]), ("resolved", True))
        self.assertNotIn("claim_token", result)
        self.assertNotIn("transport", result)
        self.assertEqual(result["resolution"]["card_status"], "resolved")

    def test_resolve_stale_version_does_not_act(self):
        card = self.cards.due(limit=1)[0]
        self.assertEqual(self._resolve_request(card, "snooze")["status"], "resolved")
        stale = self._resolve_request(card, "done")
        self.assertEqual(stale["status"], "refused")
        self.assertEqual(stale["resolution"]["refusal"], "stale_version")

    def test_resolve_at_ceiling_is_distinct(self):
        cards = self.cards.due(limit=3)
        for card in cards[:2]:
            claim = self.cards.claim_next(
                consumer_digest=hashlib.sha256(QUEUE_VIEW_TOKEN.encode()).hexdigest(),
                consumer_role=QUEUE_VIEW_ROLE,
            )
            self.assertIsNotNone(claim)
        result = self._resolve_request(cards[2])
        self.assertEqual(result["status"], "at_ceiling")
        self.assertEqual((result["held_count"], result["ceiling"]), (2, 2))

    def test_resolve_failure_after_delivery_leaves_delivered_card(self):
        card = self.cards.due(limit=1)[0]
        original = self.cards.act
        self.cards.act = lambda *args, **kwargs: (_ for _ in ()).throw(
            RuntimeError("synthetic action failure")
        )
        try:
            with self.assertRaises(RuntimeError):
                self._resolve_request(card)
        finally:
            self.cards.act = original
        with closing(sqlite3.connect(self.database)) as connection:
            row = connection.execute(
                "SELECT status,consumer_digest,transport FROM task_review_cards WHERE id=?",
                (card.id,),
            ).fetchone()
        self.assertEqual(row[0], "delivered")
        self.assertEqual(row[1], hashlib.sha256(QUEUE_VIEW_TOKEN.encode()).hexdigest())
        self.assertEqual(row[2], "resolved")

    def test_resolve_accepts_duplicate_actions_over_http(self):
        """Acceptance criterion: /v1/task-cards/resolve forwards both
        duplicate_confirm and duplicate_reject to the domain layer and
        returns an operation document instead of invalid_request."""
        from foxhound import task_duplicate_proposals
        for action, expected_state, expected_relations in (
            ("duplicate_confirm", "confirmed", 1),
            ("duplicate_reject", "rejected", 0),
        ):
            with self.subTest(action=action), tempfile.TemporaryDirectory() as root:
                database = Path(root) / "foxhound.sqlite3"
                migrate_database(database)
                with closing(sqlite3.connect(database)) as connection, connection:
                    connection.row_factory = sqlite3.Row
                    for text in (
                        "Prepare the synthetic rollout checklist",
                        "Draft the synthetic rollout checklist",
                    ):
                        connection.execute(
                            "INSERT INTO tasks(status,text,owner,version,created_at,"
                            "updated_at,owner_ref_version,owner_kind,"
                            "owner_speaker_id,owner_canonical_speaker_id,"
                            "owner_speaker_registry_id,owner_pinned,"
                            "owner_provisional) VALUES('open',?,'Person A',1,"
                            "?,?,1,'person','SPK_1','SPK_1','registry-A',0,0)",
                            (text, NOW.isoformat(), NOW.isoformat()),
                        )
                    proposal = task_duplicate_proposals.propose(
                        connection,
                        task_id_a=1,
                        task_id_b=2,
                        basis="Same synthetic deliverable and confirmed owner.",
                        detector="synthetic-detector",
                        now=NOW.isoformat(),
                    )

                cards = TaskCardService(
                    database,
                    clock=self.clock,
                    token_factory=lambda: CLAIM_TOKEN,
                )
                app = TaskCardApplication(
                    cards, {DRIP_ROLE: TOKEN, QUEUE_VIEW_ROLE: QUEUE_VIEW_TOKEN}
                )

                # Schedule and claim the duplicate-review card
                cards.schedule_duplicate_proposals()
                due = cards.due(limit=3)
                duplicate_card = None
                for card in due:
                    if card.duplicate is not None:
                        duplicate_card = card
                        break
                self.assertIsNotNone(duplicate_card)

                # Test via the resolve endpoint (the route that was broken)
                result = app.dispatch(
                    "resolve",
                    {"schema": REQUEST_SCHEMA, "schema_version": 1,
                     "card_id": duplicate_card.id,
                     "card_version": duplicate_card.version,
                     "action": action},
                    authorization=f"Bearer {QUEUE_VIEW_TOKEN}",
                )

                # Must return an operation document, not invalid_request
                self.assertNotIn("error", result)
                self.assertEqual(result["status"], "resolved")
                self.assertTrue(result["ok"])
                self.assertEqual(result["resolution"]["card_status"], "cancelled")

                # Verify the duplicate proposal was actually processed
                with closing(sqlite3.connect(database)) as connection:
                    state = connection.execute(
                        "SELECT state FROM task_duplicate_proposals WHERE id=?",
                        (proposal.proposal_id,),
                    ).fetchone()[0]
                    relation_count = connection.execute(
                        "SELECT count(*) FROM task_relations"
                    ).fetchone()[0]
                self.assertEqual(state, expected_state)
                self.assertEqual(relation_count, expected_relations)

    def test_resolve_refuses_unknown_action(self):
        """Unknown action strings are still refused with invalid_request."""
        card = self.cards.due(limit=1)[0]
        with running_server(self.app) as endpoint:
            status, _, body = request(
                endpoint,
                "/v1/task-cards/resolve",
                request_document(
                    card_id=card.id,
                    card_version=card.version,
                    action="unknown_action",
                ),
                token=QUEUE_VIEW_TOKEN,
            )
        self.assertEqual(status, 400)
        self.assertEqual(body["error"]["code"], "invalid_request")


class TaskCardTokenRoleTests(unittest.TestCase):
    """ADR 0036 decision 1: token-to-role configuration and fail-closed
    consumer identity resolution (issue #192)."""

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.database = Path(self.temporary.name) / "foxhound.sqlite3"
        self.cards = TaskCardService(self.database)
        migrate_database(self.database)

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
            # The response remains content-free and does not expose the
            # resolved role or consumer identity.
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

    def test_task_role_map_does_not_authorize_execution_without_policy(self):
        app = TaskCardApplication(self.cards, {DRIP_ROLE: TOKEN})
        self.assertTrue(app.authorized(f"Bearer {TOKEN}"))
        self.assertFalse(app.authorized_execution(f"Bearer {TOKEN}"))
        self.assertIsNone(app.resolve_execution_consumer(f"Bearer {TOKEN}"))

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

    def test_cli_accepts_independent_execution_role_map(self):
        root = Path(self.temporary.name)
        task_path = root / "task.token"
        task_path.write_text(TOKEN + "\n", encoding="utf-8")
        task_path.chmod(0o600)
        execution_path = root / "execution-queue.token"
        execution_path.write_text(QUEUE_VIEW_TOKEN + "\n", encoding="utf-8")
        execution_path.chmod(0o600)

        with mock.patch("foxhound.task_card_server.serve") as serve:
            self.assertEqual(
                main([
                    "--database", str(self.database),
                    "--token-file", str(task_path),
                    "--execution-token-file",
                    f"queue_view={execution_path}",
                    "--bind", "127.0.0.1",
                    "--port", "8790",
                ]),
                0,
            )
        app = serve.call_args.args[2]
        self.assertEqual(app.tokens, {DRIP_ROLE: TOKEN})
        self.assertEqual(
            app.execution_tokens,
            {QUEUE_VIEW_ROLE: QUEUE_VIEW_TOKEN},
        )

    def test_startup_reports_when_result_artifacts_are_unavailable(self):
        """Configuration, said once, where an operator is already looking.

        A deployment with no task work root refuses every artifact request
        for its whole life. One refusal at a time looks like a card with no
        files; said at start-up it reads as the decision it is.
        """
        root = Path(self.temporary.name)
        task_path = root / "task.token"
        task_path.write_text(TOKEN + "\n", encoding="utf-8")
        task_path.chmod(0o600)
        work_root = root / "task-work-cli"
        work_root.mkdir(mode=0o700)

        def run(*extra):
            stream = io.StringIO()
            with mock.patch("foxhound.task_card_server.serve"), \
                    contextlib.redirect_stdout(stream):
                self.assertEqual(
                    main([
                        "--database", str(self.database),
                        "--token-file", str(task_path),
                        "--bind", "127.0.0.1", "--port", "8790", *extra,
                    ]),
                    0,
                )
            return stream.getvalue()

        without = run()
        with_root = run("--task-work-root", str(work_root))

        self.assertIn("result artifacts unavailable", without)
        self.assertIn("deployment configuration version 8", without)
        self.assertNotIn("result artifacts unavailable", with_root)
        # The revision is still reported either way: this line is additional,
        # not a replacement.
        self.assertIn("revision", without)
        self.assertIn("revision", with_root)

    def test_cli_role_mapped_task_policy_does_not_borrow_for_execution(self):
        root = Path(self.temporary.name)
        task_path = root / "task.token"
        task_path.write_text(TOKEN + "\n", encoding="utf-8")
        task_path.chmod(0o600)
        queue_path = root / "task-queue.token"
        queue_path.write_text(QUEUE_VIEW_TOKEN + "\n", encoding="utf-8")
        queue_path.chmod(0o600)

        with mock.patch("foxhound.task_card_server.serve") as serve:
            self.assertEqual(
                main([
                    "--database", str(self.database),
                    "--token-file", f"drip={task_path}",
                    "--token-file", f"queue_view={queue_path}",
                    "--bind", "127.0.0.1",
                    "--port", "8790",
                ]),
                0,
            )
        app = serve.call_args.args[2]
        self.assertEqual(app.execution_tokens, {})


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
        migrate_database(self.database)
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
        raise_review_cards(self.database, self.clock())

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
        before = self.cards.stats(
            consumer_digest=hashlib.sha256(TOKEN.encode()).hexdigest()
        )
        with running_server(app) as endpoint:
            status, _, body = request(
                endpoint,
                "/v2/task-cards/stats",
                request_document(),
                token=stray_token,
            )
            self.assertEqual(status, 403)
            self.assertEqual(body["error"]["code"], "consumer_unresolved")
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
        self.assertEqual(
            self.cards.stats(
                consumer_digest=hashlib.sha256(TOKEN.encode()).hexdigest()
            ),
            before,
        )
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
