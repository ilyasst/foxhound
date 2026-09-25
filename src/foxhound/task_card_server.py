"""Authenticated loopback boundary for task and execution review cards."""

from __future__ import annotations

import argparse
from contextlib import closing
from dataclasses import asdict
import hashlib
import hmac
import ipaddress
import json
import logging
import os
import socket
import sqlite3
import stat
import sys
import time
from dataclasses import dataclass
from datetime import timedelta
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any, Mapping

from .agent_profiles import AgentProfileError, load_registry
from .execution_cards import (
    AGENT_SELECTION_TOKEN_CHARS,
    ExecutionCardOperationResult,
    ExecutionCardPresentation,
    ExecutionCardArtifacts,
    ExecutionCardArtifact,
    ExecutionCardDeliverables,
    ExecutionCardDetail,
    ExecutionCardScheduleResult,
    ExecutionCardService,
    ClaimAtCeiling as ExecutionClaimAtCeiling,
    ExecutionAgentSelectorResult,
    render_execution_agent_selector,
    render_execution_review_card,
    execution_board_status,
    EXECUTION_BOARD_STATUSES,
)
from .execution_worker import (
    ExecutionWorkerConfigError,
    load_knowledge_config,
)
from .knowledge_client import GwKnowledgeClient, KnowledgeClientError
from .release_revision import describe
from .task_cards import (
    ClaimAtCeiling,
    CardOperationResult,
    CardPresentation,
    DUPLICATE_EXPAND,
    ScheduleResult,
    TASK_CARD_ACTIONS,
    TASK_CARD_READS,
    TaskCardService,
    CardStatus,
    render_duplicate_view,
    render_task_review_card,
)
from .task_ledger import TaskLedgerError
from .task_execution import (
    BOARD_OWNER_MAX,
    BOARD_SUMMARY_MAX,
    BOARD_TEXT_MAX,
    WORK_BODY_PROJECTION_MAX,
    WORKFLOW_BOARD_STATUSES,
    TaskExecutionService,
    WorkflowBoard,
    WorkflowBoardDetail,
    WorkflowOperationResult,
)


log = logging.getLogger("foxhound.task_card_server")

SERVICE_VERSION = 1
RETRY_AFTER_SECONDS = 1
REQUEST_SCHEMA = "foxhound.task-card-service.request"
ERROR_SCHEMA = "foxhound.task-card-service.error"
HEALTH_SCHEMA = "foxhound.task-card-service.health"
SCHEDULE_SCHEMA = "foxhound.task-card-service.schedule"
CLAIM_SCHEMA = "foxhound.task-card-service.claim"
CLAIM_SCHEMA_VERSION = 2
OPERATION_SCHEMA = "foxhound.task-card-service.operation"
STATS_SCHEMA = "foxhound.task-card-service.stats"
STATS_SCHEMA_VERSION = 2
QUEUE_SCHEMA = "foxhound.task-card-service.queue"
QUEUE_SCHEMA_VERSION = 1
BOARD_SCHEMA = "foxhound.task-card-service.board"
BOARD_SCHEMA_VERSION = 1
RESOLVE_SCHEMA = "foxhound.task-card-service.resolve"
VIEW_SCHEMA = "foxhound.task-card-service.view"
RESOLVE_SCHEMA_VERSION = 1
EXECUTION_SCHEDULE_SCHEMA = "foxhound.execution-card-service.schedule"
EXECUTION_CLAIM_SCHEMA = "foxhound.execution-card-service.claim"
EXECUTION_CLAIM_SCHEMA_VERSION = 2
EXECUTION_OPERATION_SCHEMA = "foxhound.execution-card-service.operation"
EXECUTION_STATS_SCHEMA = "foxhound.execution-card-service.stats"
EXECUTION_BRIEF_SCHEMA = "foxhound.execution-card-service.brief"
EXECUTION_DELIVERABLES_SCHEMA = "foxhound.execution-card-service.deliverables"
EXECUTION_ARTIFACTS_SCHEMA = "foxhound.execution-card-service.artifacts"
EXECUTION_VIEW_SCHEMA = "foxhound.execution-card-service.view"
EXECUTION_DETAIL_SCHEMA = "foxhound.execution-card-service.detail"
EXECUTION_DETAIL_SCHEMA_VERSION = 3
EXECUTION_QUEUE_SCHEMA = "foxhound.execution-card-service.queue"
EXECUTION_QUEUE_SCHEMA_VERSION = 1
EXECUTION_BOARD_SCHEMA = "foxhound.execution-card-service.board"
EXECUTION_BOARD_SCHEMA_VERSION = 2
EXECUTION_RESOLVE_SCHEMA = "foxhound.execution-card-service.resolve"
EXECUTION_RESOLVE_SCHEMA_VERSION = 1
EXECUTION_AGENT_OPTIONS_SCHEMA = (
    "foxhound.execution-card-service.agent-options"
)
EXECUTION_AGENT_SELECTION_SCHEMA = (
    "foxhound.execution-card-service.agent-selection"
)
EXECUTION_PRIORITY_SCHEMA = "foxhound.execution-workflow-service.priority"
EXECUTION_PRIORITY_SCHEMA_VERSION = 1
WORKFLOW_BOARD_SCHEMA = "foxhound.execution-workflow-service.board"
WORKFLOW_DETAIL_SCHEMA = "foxhound.execution-workflow-service.detail"
# Version 1 of this document rode SERVICE_VERSION, which pins every route that
# has never changed shape. Version 2 adds the recorded run body, so it carries
# its own constant: a consumer has to be able to tell these two apart.
WORKFLOW_DETAIL_SCHEMA_VERSION = 2

# ADR 0036 decision 1: every accepted bearer token is configured with
# exactly one role from this closed set. A single legacy token with no
# explicit role is `DRIP_ROLE`, reproducing today's behavior -- the existing
# chat gateway's drip delivery -- with no configuration change (invariant 3).
# `QUEUE_VIEW_ROLE` is the console's read/resolve pattern. Both roles are
# resolved, or refused, before a consumer-scoped operation runs.
DRIP_ROLE = "drip"
QUEUE_VIEW_ROLE = "queue_view"
TASK_CARD_CONSUMER_ROLES = frozenset({DRIP_ROLE, QUEUE_VIEW_ROLE})
EXECUTION_CARD_CONSUMER_ROLES = TASK_CARD_CONSUMER_ROLES

BOUNDED_SOURCE_KINDS = frozenset({
    "issue",
    "review_request",
    "meeting",
    "email",
    "teams",
    "calendar",
    "mention",
    "alert",
    "legacy",
})


def _bounded_source_kind(origin_kind: object) -> str | None:
    if isinstance(origin_kind, str) and origin_kind in BOUNDED_SOURCE_KINDS:
        return origin_kind
    return None

ROUTES = {
    "/v1/task-cards/stats": "stats",
    "/v2/task-cards/stats": "stats_scoped",
    "/v1/task-cards/queue": "queue",
    "/v1/task-cards/board": "board",
    "/v1/task-cards/resolve": "resolve",
    "/v1/task-cards/schedule": "schedule",
    "/v1/task-cards/claim": "claim",
    "/v1/task-cards/delivered": "delivered",
    "/v1/task-cards/delivery-failed": "delivery_failed",
    "/v1/task-cards/action": "action",
    "/v1/task-cards/view": "view",
    "/v1/execution-cards/stats": "execution_stats",
    "/v2/execution-cards/stats": "execution_stats_scoped",
    "/v1/execution-cards/schedule": "execution_schedule",
    "/v1/execution-cards/claim": "execution_claim",
    "/v1/execution-cards/retraction-claim": "execution_retraction_claim",
    "/v1/execution-cards/retracted": "execution_retracted",
    "/v1/execution-cards/retraction-failed": "execution_retraction_failed",
    "/v1/execution-cards/delivered": "execution_delivered",
    "/v1/execution-cards/delivery-failed": "execution_delivery_failed",
    "/v1/execution-cards/release": "execution_release",
    "/v1/execution-cards/action": "execution_action",
    "/v1/execution-cards/input": "execution_input",
    "/v1/execution-cards/comment-and-go": "execution_comment_and_go",
    "/v1/execution-cards/agent-options": "execution_agent_options",
    "/v1/execution-cards/agent-selection": "execution_agent_selection",
    "/v1/execution-cards/brief": "execution_brief",
    "/v1/execution-cards/deliverables": "execution_deliverables",
    "/v1/execution-cards/artifacts": "execution_artifacts",
    "/v1/execution-cards/artifact": "execution_artifact",
    "/v1/execution-cards/view": "execution_view",
    "/v1/execution-cards/detail": "execution_detail",
    "/v1/execution-cards/queue": "execution_queue",
    "/v1/execution-cards/board": "execution_board",
    "/v1/execution-cards/resolve": "execution_resolve",
    "/v1/execution-workflows/board": "workflow_board",
    "/v1/execution-workflows/detail": "workflow_detail",
    "/v1/execution-workflows/priority": "execution_priority",
}


class TaskCardServerConfigError(ValueError):
    pass


class TaskCardServerRequestError(ValueError):
    def __init__(
        self,
        code: str,
        message: str,
        status: int = HTTPStatus.BAD_REQUEST,
    ) -> None:
        self.code = code
        self.message = message
        self.status = int(status)
        super().__init__(message)


class TaskCardServerResponseTooLarge(RuntimeError):
    pass


class TaskCardConsumerIdentityError(RuntimeError):
    """Fail-closed: an authenticated token's role could not be resolved.

    ADR 0036 decision 1, invariant 2 requires that a token accepted by
    ``authorized()`` whose role cannot be resolved never default to any
    role. This service's chosen fail-closed behavior (documented on issue
    #192) is that construction already refuses to build an application with
    any token configured outside the closed role set -- see
    ``_normalize_role_tokens`` -- so this state cannot arise through normal
    configuration and the server never starts with it. This exception is
    the runtime backstop for that same guarantee: if a token's resolved
    role is ever found outside ``TASK_CARD_CONSUMER_ROLES`` regardless,
    identity resolution refuses with this fixed error rather than guessing
    a role or resolving no identity at all.
    """


@dataclass(frozen=True)
class TaskCardServerLimits:
    max_body_bytes: int = 16 * 1024
    #: Sized against the largest reply this service offers to build: a board at
    #: its maximum `limit` of 100 rows, each bounded by the constants above,
    #: measures ~80 KiB at worst. 64 KiB was below that, so the board route
    #: refused the bound it advertised once a queue grew into it. Consumers
    #: guard their own reads at 256 KiB, which this stays under.
    max_response_bytes: int = 192 * 1024
    max_artifact_bytes: int = 2 * 1024 * 1024
    request_timeout_seconds: float = 5.0

    def validate(self) -> None:
        if any(
            isinstance(value, bool) or not isinstance(value, int) or value < 1024
            for value in (
                self.max_body_bytes, self.max_response_bytes,
                self.max_artifact_bytes,
            )
        ):
            raise TaskCardServerConfigError(
                "task card byte limits must be integers of at least 1024"
            )
        if (
            isinstance(self.request_timeout_seconds, bool)
            or not isinstance(self.request_timeout_seconds, (int, float))
            or not 0 < self.request_timeout_seconds <= 30
        ):
            raise TaskCardServerConfigError(
                "task card request timeout must be between 0 and 30 seconds"
            )


@dataclass(frozen=True)
class ConsumerIdentity:
    """A request's consumer identity, resolved only from the token that
    authenticated it (ADR 0036 decision 1, invariant 1). ``digest`` reuses
    the same digest approach as ``claim_token_digest`` -- a hex SHA-256 of
    the credential -- so a consumer identity never carries the token
    itself, only a stable, non-reversible reference to it.
    """

    role: str
    digest: str


class TaskCardApplication:
    """Strict request validation around one task-card aggregate."""

    def __init__(
        self,
        cards: TaskCardService,
        token: str | Mapping[str, str],
        *,
        execution_cards: ExecutionCardService | None = None,
        execution_workflows: TaskExecutionService | None = None,
        execution_tokens: str | Mapping[str, str] | None = None,
        limits: TaskCardServerLimits | None = None,
    ) -> None:
        if not isinstance(cards, TaskCardService):
            raise TaskCardServerConfigError("task card service is invalid")
        if execution_cards is not None and not isinstance(
            execution_cards, ExecutionCardService
        ):
            raise TaskCardServerConfigError(
                "execution card service is invalid"
            )
        if execution_workflows is not None and not isinstance(
            execution_workflows, TaskExecutionService
        ):
            raise TaskCardServerConfigError(
                "execution workflow service is invalid"
            )
        self.cards = cards
        self.execution_cards = execution_cards
        self.execution_workflows = execution_workflows
        # `tokens` maps role -> bearer token. A bare string is today's
        # single shared token, normalized to role `drip` -- this is what
        # keeps an existing single-token deployment behaving exactly as it
        # does today with no configuration change (ADR 0036, invariant 3).
        self.tokens = _normalize_role_tokens(token)
        # Execution authorization is a separate aggregate policy. A bare
        # legacy token remains drip-compatible; deployments with a role map
        # must opt in to the execution map explicitly.
        if execution_tokens is None and isinstance(token, str):
            self.execution_tokens = _normalize_role_tokens(token)
        elif execution_tokens is None:
            # Role maps are task-aggregate policy. Never borrow one of their
            # credentials for execution cards; omission means no execution
            # consumer is configured.
            self.execution_tokens = {}
        else:
            self.execution_tokens = _normalize_role_tokens(execution_tokens)
        self.limits = limits or TaskCardServerLimits()
        self.limits.validate()

    def authorized(self, header: str | None) -> bool:
        return self._match_role(header) is not None

    def authorized_execution(self, header: str | None) -> bool:
        return self.resolve_execution_consumer(header) is not None

    def resolve_consumer(self, header: str | None) -> ConsumerIdentity | None:
        """Return the consumer identity of whichever configured token
        authenticated ``header``, or ``None`` if none did.

        Raises ``TaskCardConsumerIdentityError`` if the accepting token's
        role cannot be resolved to a member of ``TASK_CARD_CONSUMER_ROLES``
        -- a fail-closed condition that never defaults to any role (ADR
        0036 decision 1, invariant 2). Construction already refuses to
        build an application with a token configured outside that closed
        set (see ``_normalize_role_tokens``), so this branch is a runtime
        backstop rather than a state reachable through ordinary
        configuration.
        """
        role = self._match_role(header)
        if role is None:
            return None
        if role not in TASK_CARD_CONSUMER_ROLES:
            raise TaskCardConsumerIdentityError(
                "task card consumer role is unresolved"
            )
        prefix = "Bearer "
        token = header[len(prefix):]  # type: ignore[index]
        return ConsumerIdentity(role=role, digest=_consumer_digest(token))

    def resolve_execution_consumer(self, header: str | None) -> ConsumerIdentity | None:
        prefix = "Bearer "
        if not header or not header.startswith(prefix):
            return None
        candidate = header[len(prefix):]
        matched: str | None = None
        for role, value in self.execution_tokens.items():
            if hmac.compare_digest(candidate, value):
                matched = role
        if matched is None:
            return None
        if matched not in EXECUTION_CARD_CONSUMER_ROLES:
            raise TaskCardConsumerIdentityError("execution card consumer role is unresolved")
        return ConsumerIdentity(role=matched, digest=_consumer_digest(candidate))

    def _match_role(self, header: str | None) -> str | None:
        """Return the role of whichever configured token matches
        ``header``, or ``None`` if it matches none of them.

        Every configured token is compared -- the loop never returns
        early -- so which one matched (if any) cannot be inferred from
        comparison timing.
        """
        prefix = "Bearer "
        if not header or not header.startswith(prefix):
            return None
        candidate = header[len(prefix):]
        matched: str | None = None
        for role, value in self.tokens.items():
            if hmac.compare_digest(candidate, value):
                matched = role
        return matched

    def dispatch(
        self,
        operation: str,
        payload: object,
        *,
        authorization: str | None = None,
    ) -> dict[str, Any]:
        if operation == "stats":
            _request(payload, required=set())
            stats = self.cards.stats_global()
            return {
                "schema": STATS_SCHEMA,
                "schema_version": SERVICE_VERSION,
                "ok": True,
                "pending": stats.pending,
                "delivering": stats.delivering,
                "delivered": stats.delivered,
                "snoozed": stats.snoozed,
                "active": stats.active,
            }
        if operation == "stats_scoped":
            _request(payload, required=set())
            try:
                identity = self.resolve_consumer(authorization)
            except TaskCardConsumerIdentityError as exc:
                raise TaskCardServerRequestError(
                    "consumer_unresolved",
                    "task card consumer role is unresolved",
                    HTTPStatus.FORBIDDEN,
                ) from exc
            if identity is None:
                raise TaskCardServerRequestError(
                    "consumer_unresolved",
                    "task card consumer role is unresolved",
                    HTTPStatus.FORBIDDEN,
                )
            stats = self.cards.stats(consumer_digest=identity.digest)
            return {
                "schema": STATS_SCHEMA,
                "schema_version": STATS_SCHEMA_VERSION,
                "ok": True,
                "pending": stats.pending,
                "delivering": stats.delivering,
                "delivered": stats.delivered,
                "snoozed": stats.snoozed,
                "elsewhere": stats.elsewhere,
                "active": stats.active,
            }
        if operation == "queue":
            request = _request(payload, required={"limit"})
            try:
                identity = self.resolve_consumer(authorization)
            except TaskCardConsumerIdentityError as exc:
                raise TaskCardServerRequestError(
                    "consumer_unresolved",
                    "task card consumer role is unresolved",
                    HTTPStatus.FORBIDDEN,
                ) from exc
            if identity is None:
                raise TaskCardServerRequestError(
                    "consumer_unresolved",
                    "task card consumer role is unresolved",
                    HTTPStatus.FORBIDDEN,
                )
            if identity.role != QUEUE_VIEW_ROLE:
                raise TaskCardServerRequestError(
                    "role_forbidden",
                    "task card queue requires the queue_view role",
                    HTTPStatus.FORBIDDEN,
                )
            cards = self.cards.due(
                limit=_integer(request["limit"], minimum=1, maximum=1_000)
            )
            return {
                "schema": QUEUE_SCHEMA,
                "schema_version": QUEUE_SCHEMA_VERSION,
                "ok": True,
                "cards": [_queue_card_document(card) for card in cards],
            }
        if operation == "board":
            request = _request(payload, required={"limit"})
            try:
                identity = self.resolve_consumer(authorization)
            except TaskCardConsumerIdentityError as exc:
                raise TaskCardServerRequestError(
                    "consumer_unresolved",
                    "task card consumer role is unresolved",
                    HTTPStatus.FORBIDDEN,
                ) from exc
            if identity is None or identity.role != QUEUE_VIEW_ROLE:
                raise TaskCardServerRequestError(
                    "role_forbidden",
                    "task card board requires the queue_view role",
                    HTTPStatus.FORBIDDEN,
                )
            board = self.cards.board(
                limit=_integer(request["limit"], minimum=1, maximum=100)
            )
            stats = self.cards.stats(consumer_digest=identity.digest)
            return {
                "schema": BOARD_SCHEMA,
                "schema_version": BOARD_SCHEMA_VERSION,
                "ok": True,
                "columns": [
                    {"status": "review", "total": board.review_total},
                    {"status": "snoozed", "total": board.snoozed_total},
                ],
                "held_elsewhere": stats.elsewhere,
                "cards": [_task_board_card_document(card) for card in board.cards],
            }
        if operation == "schedule":
            request = _request(payload, required={"limit"})
            limit = _integer(request["limit"], minimum=1, maximum=1_000)
            return _schedule_document(self.cards.schedule(limit=limit))
        if operation == "resolve":
            request = _request(
                payload, required={"card_id", "card_version", "action"}
            )
            try:
                identity = self.resolve_consumer(authorization)
            except TaskCardConsumerIdentityError as exc:
                raise TaskCardServerRequestError(
                    "consumer_unresolved",
                    "task card consumer role is unresolved",
                    HTTPStatus.FORBIDDEN,
                ) from exc
            if identity is None or identity.role != QUEUE_VIEW_ROLE:
                raise TaskCardServerRequestError(
                    "role_forbidden",
                    "task card resolve requires the queue_view role",
                    HTTPStatus.FORBIDDEN,
                )
            action = request["action"]
            if not isinstance(action, str) or action not in TASK_CARD_ACTIONS:
                raise TaskCardServerRequestError(
                    "invalid_request", "task card action is invalid"
                )
            result = self.cards.resolve(
                _integer(request["card_id"], minimum=1),
                expected_version=_integer(request["card_version"], minimum=1),
                action=action, consumer_digest=identity.digest,
                consumer_role=identity.role,
            )
            if isinstance(result, ClaimAtCeiling):
                return {
                    "schema": RESOLVE_SCHEMA,
                    "schema_version": RESOLVE_SCHEMA_VERSION,
                    "ok": True, "status": "at_ceiling",
                    "held_count": result.held_count, "ceiling": result.ceiling,
                    "resolution": None,
                }
            if result is None:
                return {
                    "schema": RESOLVE_SCHEMA,
                    "schema_version": RESOLVE_SCHEMA_VERSION,
                    "ok": True, "status": "empty", "resolution": None,
                }
            return {
                "schema": RESOLVE_SCHEMA,
                "schema_version": RESOLVE_SCHEMA_VERSION,
                "ok": result.accepted,
                "status": "resolved" if result.accepted else "refused",
                "resolution": _operation_document(result),
            }
        if operation == "claim":
            request = _strict_request(
                payload,
                required={"lease_seconds"},
                optional={"claim_version"},
            )
            lease = _integer(request["lease_seconds"], minimum=5, maximum=300)
            claim_version = _integer(
                request.get("claim_version", CLAIM_SCHEMA_VERSION),
                minimum=1,
                maximum=CLAIM_SCHEMA_VERSION,
            )
            # ADR 0036 decision 2: a claim is a consumer-scoped operation
            # (invariant 2) -- the resolved identity is bound to the card
            # inside `claim_next` itself, in the same transaction as the
            # claim. A token whose role cannot be resolved must fail closed
            # here, before any card is touched, rather than claim under a
            # null identity.
            try:
                identity = self.resolve_consumer(authorization)
            except TaskCardConsumerIdentityError as exc:
                raise TaskCardServerRequestError(
                    "consumer_unresolved",
                    "task card consumer role is unresolved",
                    HTTPStatus.FORBIDDEN,
                ) from exc
            if identity is None:
                # Unreachable in practice: `do_POST` already requires
                # `authorized()` to accept the same header before dispatch
                # is ever called. Treated the same as an unresolved role
                # rather than trusted to proceed, so a future caller of
                # `dispatch` that skips that gate still fails closed.
                raise TaskCardServerRequestError(
                    "consumer_unresolved",
                    "task card consumer role is unresolved",
                    HTTPStatus.FORBIDDEN,
                )
            claim = self.cards.claim_next(
                lease_seconds=lease, consumer_digest=identity.digest,
                consumer_role=identity.role,
            )
            if isinstance(claim, ClaimAtCeiling):
                return {
                    "schema": CLAIM_SCHEMA,
                    "schema_version": claim_version,
                    "ok": True,
                    "status": "at_ceiling",
                    "held_count": claim.held_count,
                    "ceiling": claim.ceiling,
                    "claim": None,
                }
            if claim is None:
                return {
                    "schema": CLAIM_SCHEMA,
                    "schema_version": claim_version,
                    "ok": True,
                    "status": "empty",
                    "claim": None,
                }
            body, reply_markup = render_task_review_card(claim.card)
            claim_payload = {
                "card_id": claim.card.id,
                "card_version": claim.card.version,
                "claim_token": claim.token,
                "expires_at": claim.expires_at,
                "delivery_key": (
                    f"foxhound-task-card-{claim.card.id}-"
                    f"v{claim.card.version}"
                ),
                "body": body,
                "reply_markup": reply_markup,
            }
            if claim_version == 2:
                claim_payload["source_kind"] = _bounded_source_kind(
                    claim.card.origin_kind
                )
            return {
                "schema": CLAIM_SCHEMA,
                "schema_version": claim_version,
                "ok": True,
                "status": "claimed",
                "claim": claim_payload,
            }
        if operation == "delivered":
            request = _request(
                payload,
                required={
                    "card_id", "card_version", "claim_token", "transport",
                    "delivery_ref",
                },
            )
            return _operation_document(self.cards.complete_delivery(
                _integer(request["card_id"], minimum=1),
                expected_version=_integer(request["card_version"], minimum=1),
                claim_token=_secret(request["claim_token"]),
                transport=_opaque(request["transport"], maximum=64),
                delivery_ref=_opaque(request["delivery_ref"], maximum=200),
            ))
        if operation == "delivery_failed":
            request = _request(
                payload,
                required={"card_id", "card_version", "claim_token"},
            )
            return _operation_document(self.cards.fail_delivery(
                _integer(request["card_id"], minimum=1),
                expected_version=_integer(request["card_version"], minimum=1),
                claim_token=_secret(request["claim_token"]),
            ))
        if operation == "action":
            request = _request(
                payload,
                required={"card_id", "card_version", "action"},
            )
            action = request["action"]
            if (
                not isinstance(action, str)
                or action not in TASK_CARD_ACTIONS
            ):
                raise TaskCardServerRequestError(
                    "invalid_request", "task card action is invalid"
                )
            return _operation_document(self.cards.act(
                _integer(request["card_id"], minimum=1),
                expected_version=_integer(request["card_version"], minimum=1),
                action=action,
            ))
        if operation == "view":
            request = _strict_request(
                payload,
                required={"card_id", "card_version", "view"},
                optional=set(),
            )
            view = request["view"]
            if not isinstance(view, str) or view not in TASK_CARD_READS:
                raise TaskCardServerRequestError(
                    "invalid_request", "task card view is invalid"
                )
            return _view_document(self.cards.view(
                _integer(request["card_id"], minimum=1),
                expected_version=_integer(request["card_version"], minimum=1),
                expanded=view == DUPLICATE_EXPAND,
            ))
        if operation == "execution_stats":
            _request(payload, required=set())
            stats = self._execution_cards().stats()
            response = {
                "schema": EXECUTION_STATS_SCHEMA,
                "schema_version": SERVICE_VERSION,
                "ok": True,
                "pending": stats.pending,
                "delivering": stats.delivering,
                "delivered": stats.delivered,
                "active": stats.active,
            }
            # Steer counts are deliberately absent from v1.  This response
            # is a versioned contract and its client validates the key set
            # exactly, so a field that appears only when a steer card
            # happens to exist is not a compatible addition -- it is a
            # response the client refuses, on exactly the deployments that
            # have steer work and nowhere else.  Observed: a steer card
            # entered `delivering`, and sixty-one seconds later the drip
            # sweep began refusing every response and no execution card
            # reached the reader at all.  Steer counts belong to v2, which
            # is where they are.
            return response
        if operation == "execution_stats_scoped":
            _request(payload, required=set())
            identity = self.resolve_execution_consumer(authorization)
            if identity is None:
                raise TaskCardServerRequestError("consumer_unresolved", "execution card consumer role is unresolved", HTTPStatus.FORBIDDEN)
            stats = self._execution_cards().stats_scoped(consumer_digest=identity.digest)
            response = {"schema": EXECUTION_STATS_SCHEMA, "schema_version": 2,
                    "ok": True, "pending": stats.pending,
                    "delivering": stats.delivering, "delivered": stats.delivered,
                    "elsewhere": stats.elsewhere, "active": stats.active}
            if stats.steer_pending or stats.steer_delivering or stats.steer_delivered:
                response.update(steer_pending=stats.steer_pending,
                                steer_delivering=stats.steer_delivering,
                                steer_delivered=stats.steer_delivered)
            return response
        if operation == "execution_schedule":
            request = _request(payload, required={"limit"})
            limit = _integer(request["limit"], minimum=1, maximum=1_000)
            return _execution_schedule_document(
                self._execution_cards().schedule(limit=limit)
            )
        if operation == "execution_queue":
            request = _request(payload, required={"limit"})
            try:
                identity = self.resolve_execution_consumer(authorization)
            except TaskCardConsumerIdentityError as exc:
                raise TaskCardServerRequestError(
                    "consumer_unresolved",
                    "execution card consumer role is unresolved",
                    HTTPStatus.FORBIDDEN,
                ) from exc
            if identity is None or identity.role != QUEUE_VIEW_ROLE:
                raise TaskCardServerRequestError(
                    "role_forbidden",
                    "execution card queue requires the queue_view role",
                    HTTPStatus.FORBIDDEN,
                )
            cards = self._execution_cards().due(
                limit=_integer(request["limit"], minimum=1, maximum=1000)
            )
            return {
                "schema": EXECUTION_QUEUE_SCHEMA,
                "schema_version": EXECUTION_QUEUE_SCHEMA_VERSION,
                "ok": True,
                "cards": [_execution_queue_card_document(card) for card in cards],
            }
        if operation == "execution_board":
            request = _request(payload, required={"limit"})
            try:
                identity = self.resolve_execution_consumer(authorization)
            except TaskCardConsumerIdentityError as exc:
                raise TaskCardServerRequestError(
                    "consumer_unresolved",
                    "execution card consumer role is unresolved",
                    HTTPStatus.FORBIDDEN,
                ) from exc
            if identity is None or identity.role != QUEUE_VIEW_ROLE:
                raise TaskCardServerRequestError(
                    "role_forbidden",
                    "execution card board requires the queue_view role",
                    HTTPStatus.FORBIDDEN,
                )
            board = self._execution_cards().board(
                limit=_integer(request["limit"], minimum=1, maximum=100)
            )
            stats = self._execution_cards().stats_scoped(
                consumer_digest=identity.digest
            )
            return {
                "schema": EXECUTION_BOARD_SCHEMA,
                "schema_version": EXECUTION_BOARD_SCHEMA_VERSION,
                "ok": True,
                "columns": [
                    {"status": status, "total": board.totals[status]}
                    for status in EXECUTION_BOARD_STATUSES
                ],
                "held_elsewhere": stats.elsewhere,
                "cards": [
                    _execution_board_card_document(card) for card in board.cards
                ],
            }
        if operation == "execution_resolve":
            request = _strict_request(
                payload, required={"card_id", "card_version", "action"},
                optional={"input_kind", "value", "selection_token"},
            )
            identity = self.resolve_execution_consumer(authorization)
            if identity is None:
                raise TaskCardServerRequestError(
                    "consumer_unresolved", "execution card consumer role is unresolved",
                    HTTPStatus.FORBIDDEN,
                )
            if identity.role != QUEUE_VIEW_ROLE:
                raise TaskCardServerRequestError(
                    "role_forbidden", "execution card resolve requires the queue_view role",
                    HTTPStatus.FORBIDDEN,
                )
            action = request["action"]
            if not isinstance(action, str):
                raise TaskCardServerRequestError("invalid_request", "execution card action is invalid")
            input_kind = request.get("input_kind")
            value = request.get("value")
            if input_kind is not None:
                if input_kind not in {"discussion", "reassignment"}:
                    raise TaskCardServerRequestError("invalid_request", "execution card input kind is invalid")
                value = _reader_input(value, kind=input_kind)
            selection = request.get("selection_token")
            if selection is not None:
                raise TaskCardServerRequestError(
                    "invalid_request",
                    "agent selection is not supported by queue resolve",
                )
            result = self._execution_cards().resolve_queue_view(
                _integer(request["card_id"], minimum=1),
                expected_version=_integer(request["card_version"], minimum=1),
                action=action, input_kind=input_kind, value=value,
                selection_token=selection, consumer_digest=identity.digest,
            )
            if isinstance(result, ExecutionClaimAtCeiling):
                return {
                    "schema": EXECUTION_RESOLVE_SCHEMA,
                    "schema_version": EXECUTION_RESOLVE_SCHEMA_VERSION,
                    "ok": True, "status": "at_ceiling",
                    "held_count": result.held_count, "ceiling": result.ceiling,
                    "resolution": None,
                }
            return _execution_resolve_document(result)
        if operation == "workflow_board":
            request = _strict_request(payload, required={"limit"}, optional=set())
            identity = self.resolve_execution_consumer(authorization)
            if identity is None or identity.role != QUEUE_VIEW_ROLE:
                raise TaskCardServerRequestError(
                    "role_forbidden", "workflow board requires the queue_view role",
                    HTTPStatus.FORBIDDEN,
                )
            return _workflow_board_document(
                self._execution_workflows().board(
                    limit=_integer(request["limit"], minimum=1, maximum=100)
                )
            )
        if operation == "workflow_detail":
            request = _strict_request(
                payload, required={"task_id", "workflow_version"}, optional=set()
            )
            identity = self.resolve_execution_consumer(authorization)
            if identity is None or identity.role != QUEUE_VIEW_ROLE:
                raise TaskCardServerRequestError(
                    "role_forbidden", "workflow detail requires the queue_view role",
                    HTTPStatus.FORBIDDEN,
                )
            return _workflow_detail_document(
                self._execution_workflows().board_detail(
                    _integer(request["task_id"], minimum=1),
                    expected_version=_integer(request["workflow_version"], minimum=1),
                )
            )
        if operation == "execution_priority":
            request = _strict_request(
                payload,
                required={"task_id", "workflow_version", "action"},
                optional=set(),
            )
            identity = self.resolve_execution_consumer(authorization)
            if identity is None or identity.role != QUEUE_VIEW_ROLE:
                raise TaskCardServerRequestError(
                    "role_forbidden",
                    "execution workflow priority requires the queue_view role",
                    HTTPStatus.FORBIDDEN,
                )
            action = request["action"]
            if not isinstance(action, str) or action not in {
                "raise", "lower", "clear"
            }:
                raise TaskCardServerRequestError(
                    "invalid_request",
                    "execution workflow priority action is invalid",
                )
            return _execution_priority_document(
                self._execution_workflows().set_priority(
                    _integer(request["task_id"], minimum=1),
                    expected_version=_integer(
                        request["workflow_version"], minimum=1
                    ),
                    action=action,
                )
            )
        if operation == "execution_claim":
            request = _strict_request(
                payload,
                required={"lease_seconds"},
                optional={"claim_version"},
            )
            lease = _integer(request["lease_seconds"], minimum=5, maximum=300)
            claim_version = _integer(
                request.get("claim_version", EXECUTION_CLAIM_SCHEMA_VERSION),
                minimum=1,
                maximum=EXECUTION_CLAIM_SCHEMA_VERSION,
            )
            identity = self.resolve_execution_consumer(authorization)
            if identity is None:
                raise TaskCardServerRequestError("consumer_unresolved", "execution card consumer role is unresolved", HTTPStatus.FORBIDDEN)
            claim = self._execution_cards().claim_next(
                lease_seconds=lease, consumer_digest=identity.digest,
                consumer_role=identity.role)
            if isinstance(claim, ExecutionClaimAtCeiling):
                return {"schema": EXECUTION_CLAIM_SCHEMA, "schema_version": claim_version,
                        "ok": True, "status": "at_ceiling",
                        "held_count": claim.held_count, "ceiling": claim.ceiling,
                        "claim": None}
            if claim is None:
                return {
                    "schema": EXECUTION_CLAIM_SCHEMA,
                    "schema_version": claim_version,
                    "ok": True,
                    "status": "empty",
                    "claim": None,
                }
            body, reply_markup = render_execution_review_card(claim.card)
            claim_payload = {
                "card_id": claim.card.id,
                "card_version": claim.card.version,
                "kind": claim.card.kind.value,
                "phase": claim.card.phase.value,
                "claim_token": claim.token,
                "expires_at": claim.expires_at,
                "delivery_key": (
                    f"foxhound-execution-card-{claim.card.id}-"
                    f"v{claim.card.version}"
                ),
                "superseded_delivery_ref": claim.superseded_delivery_ref,
                "superseded_transport": claim.superseded_transport,
                "body": body,
                "reply_markup": reply_markup,
            }
            if claim.card.result_id:
                try:
                    with closing(self._execution_cards()._connect()) as db:
                        row = db.execute(
                            "SELECT 1 FROM execution_result_artifacts "
                            "WHERE result_id=? AND name='voice_summary.wav'",
                            (claim.card.result_id,),
                        ).fetchone()
                        if row is not None:
                            claim_payload["voice_artifact_name"] = "voice_summary.wav"
                except Exception:
                    pass
            if claim_version == 2:
                claim_payload["source_kind"] = _bounded_source_kind(
                    claim.card.origin_kind
                )
            return {
                "schema": EXECUTION_CLAIM_SCHEMA,
                "schema_version": claim_version,
                "ok": True,
                "status": "claimed",
                "claim": claim_payload,
            }
        if operation == "execution_retraction_claim":
            request = _request(payload, required={"lease_seconds"})
            identity = self.resolve_execution_consumer(authorization)
            if identity is None:
                raise TaskCardServerRequestError("consumer_unresolved", "execution card consumer role is unresolved", HTTPStatus.FORBIDDEN)
            claim = self._execution_cards().claim_retraction(
                lease_seconds=_integer(request["lease_seconds"], minimum=5, maximum=300)
            )
            return {"schema": EXECUTION_CLAIM_SCHEMA, "schema_version": SERVICE_VERSION,
                    "ok": True, "status": "empty" if claim is None else "claimed",
                    "claim": None if claim is None else {
                        "card_id": claim.card_id, "transport": claim.transport,
                        "delivery_ref": claim.delivery_ref, "claim_token": claim.token,
                        "expires_at": claim.expires_at,
                    }}
        if operation in {"execution_retracted", "execution_retraction_failed"}:
            request = _request(payload, required={"card_id", "claim_token"})
            from .execution_cards import ExecutionCardRetractionClaim
            claim = ExecutionCardRetractionClaim(
                _integer(request["card_id"], minimum=1), "", "",
                _secret(request["claim_token"]), "",
            )
            completed = (self._execution_cards().complete_retraction(claim)
                         if operation == "execution_retracted"
                         else self._execution_cards().fail_retraction(claim))
            return {"schema": EXECUTION_OPERATION_SCHEMA,
                    "schema_version": SERVICE_VERSION, "ok": True,
                    "status": "applied" if completed else "refused"}
        if operation == "execution_delivered":
            request = _request(
                payload,
                required={
                    "card_id", "card_version", "claim_token", "transport",
                    "delivery_ref",
                },
            )
            return _execution_operation_document(
                self._execution_cards().complete_delivery(
                    _integer(request["card_id"], minimum=1),
                    expected_version=_integer(
                        request["card_version"], minimum=1
                    ),
                    claim_token=_secret(request["claim_token"]),
                    transport=_opaque(request["transport"], maximum=64),
                    delivery_ref=_opaque(
                        request["delivery_ref"], maximum=200
                    ),
                )
            )
        if operation == "execution_delivery_failed":
            request = _request(
                payload,
                required={"card_id", "card_version", "claim_token"},
            )
            return _execution_operation_document(
                self._execution_cards().fail_delivery(
                    _integer(request["card_id"], minimum=1),
                    expected_version=_integer(
                        request["card_version"], minimum=1
                    ),
                    claim_token=_secret(request["claim_token"]),
                )
            )
        if operation == "execution_release":
            request = _request(
                payload,
                required={"card_id", "card_version", "claim_token", "reason"},
            )
            identity = self.resolve_execution_consumer(authorization)
            if identity is None:
                raise TaskCardServerRequestError(
                    "consumer_unresolved",
                    "execution card consumer role is unresolved",
                    HTTPStatus.FORBIDDEN,
                )
            reason = request["reason"]
            if not isinstance(reason, str) or reason not in {
                "surface_full",
                "client_rejected",
            }:
                raise TaskCardServerRequestError(
                    "invalid_request",
                    "execution card release reason is invalid",
                )
            return _execution_operation_document(
                self._execution_cards().release_delivery(
                    _integer(request["card_id"], minimum=1),
                    expected_version=_integer(
                        request["card_version"], minimum=1
                    ),
                    claim_token=_secret(request["claim_token"]),
                    consumer_digest=identity.digest,
                    reason=reason,
                )
            )
        if operation == "execution_action":
            request = _request(
                payload,
                required={"card_id", "card_version", "action"},
            )
            action = request["action"]
            if not isinstance(action, str) or action not in {
                "start", "snooze", "cancel", "approve", "revise",
                "done", "drop", "until_meeting", "snooze_1d", "snooze_7d",
                "snooze_14d", "snooze_30d",
            }:
                raise TaskCardServerRequestError(
                    "invalid_request", "execution card action is invalid"
                )
            return _execution_operation_document(self._execution_cards().act(
                _integer(request["card_id"], minimum=1),
                expected_version=_integer(request["card_version"], minimum=1),
                action=action,
            ))
        if operation == "execution_input":
            request = _request(
                payload,
                required={"card_id", "card_version", "input_kind", "value"},
            )
            kind = request["input_kind"]
            if kind not in {"discussion", "reassignment"}:
                raise TaskCardServerRequestError(
                    "invalid_request", "execution card input kind is invalid"
                )
            value = _reader_input(request["value"], kind=kind)
            return _execution_operation_document(
                self._execution_cards().submit_input(
                    _integer(request["card_id"], minimum=1),
                    expected_version=_integer(
                        request["card_version"], minimum=1
                    ),
                    kind=kind,
                    value=value,
                )
            )
        if operation == "execution_comment_and_go":
            request = _request(
                payload, required={"card_id", "card_version", "value"}
            )
            return _execution_operation_document(
                self._execution_cards().comment_and_go(
                    _integer(request["card_id"], minimum=1),
                    expected_version=_integer(
                        request["card_version"], minimum=1
                    ),
                    value=_reader_input(request["value"], kind="discussion"),
                )
            )
        if operation == "execution_view":
            request = _request(payload, required={"card_id", "card_version"})
            return _execution_view_document(
                self._execution_cards().view(
                    _integer(request["card_id"], minimum=1),
                    expected_version=_integer(
                        request["card_version"], minimum=1),
                )
            )
        if operation == "execution_detail":
            request = _strict_request(
                payload, required={"card_id", "card_version"}, optional=set()
            )
            identity = self.resolve_execution_consumer(authorization)
            if identity is None:
                raise TaskCardServerRequestError(
                    "consumer_unresolved",
                    "execution card consumer role is unresolved",
                    HTTPStatus.FORBIDDEN,
                )
            if identity.role != QUEUE_VIEW_ROLE:
                raise TaskCardServerRequestError(
                    "role_forbidden",
                    "execution card detail requires the queue_view role",
                    HTTPStatus.FORBIDDEN,
                )
            return _execution_detail_document(
                self._execution_cards().detail(
                    _integer(request["card_id"], minimum=1),
                    expected_version=_integer(request["card_version"], minimum=1),
                )
            )
        if operation == "execution_brief":
            request = _request(payload, required={"card_id", "card_version"})
            result = self._execution_cards().brief(
                _integer(request["card_id"], minimum=1),
                expected_version=_integer(
                    request["card_version"], minimum=1),
            )
            return {
                "schema": EXECUTION_BRIEF_SCHEMA,
                "schema_version": SERVICE_VERSION,
                "ok": result.accepted,
                "card_id": result.card_id,
                "card_version": result.card_version,
                "text": result.text if result.accepted else None,
                "refusal": (
                    None if result.refusal is None else result.refusal.value
                ),
            }
        if operation == "execution_deliverables":
            request = _request(payload, required={"card_id", "card_version"})
            return _execution_deliverables_document(
                self._execution_cards().deliverables(
                    _integer(request["card_id"], minimum=1),
                    expected_version=_integer(
                        request["card_version"], minimum=1),
                )
            )
        if operation == "execution_artifacts":
            request = _request(payload, required={"card_id", "card_version"})
            return _execution_artifacts_document(
                self._execution_cards().artifacts(
                    _integer(request["card_id"], minimum=1),
                    expected_version=_integer(
                        request["card_version"], minimum=1),
                )
            )
        if operation == "execution_artifact":
            request = _request(
                payload, required={"card_id", "card_version", "ordinal"}
            )
            result = self._execution_cards().artifact(
                _integer(request["card_id"], minimum=1),
                expected_version=_integer(request["card_version"], minimum=1),
                ordinal=_integer(request["ordinal"], minimum=0),
            )
            if not result.accepted:
                return _execution_artifacts_document(result)
            artifact = result.artifacts[0]
            if len(artifact.content) > self.limits.max_artifact_bytes:
                raise TaskCardServerResponseTooLarge
            return {
                "schema": EXECUTION_ARTIFACTS_SCHEMA,
                "schema_version": SERVICE_VERSION,
                "ok": True,
                "disposition": result.disposition.value,
                "card_id": result.card_id,
                "card_version": result.card_version,
                "artifact": artifact,
                "refusal": None,
            }
        if operation == "execution_agent_options":
            request = _request(
                payload, required={"card_id", "card_version"}
            )
            return _execution_agent_options_document(
                self._execution_cards().agent_options(
                    _integer(request["card_id"], minimum=1),
                    expected_version=_integer(
                        request["card_version"], minimum=1
                    ),
                )
            )
        if operation == "execution_agent_selection":
            request = _request(
                payload,
                required={"card_id", "card_version", "selection_token"},
            )
            return _execution_agent_selection_document(
                self._execution_cards().select_agent(
                    _integer(request["card_id"], minimum=1),
                    expected_version=_integer(
                        request["card_version"], minimum=1
                    ),
                    selection_token=_opaque(
                        request["selection_token"],
                        maximum=AGENT_SELECTION_TOKEN_CHARS,
                    ),
                )
            )
        raise TaskCardServerRequestError(
            "not_found", "route not found", HTTPStatus.NOT_FOUND
        )

    def _execution_cards(self) -> ExecutionCardService:
        if self.execution_cards is None:
            raise TaskCardServerRequestError(
                "service_unavailable",
                "execution card service is unavailable",
                HTTPStatus.SERVICE_UNAVAILABLE,
            )
        return self.execution_cards

    def _execution_workflows(self) -> TaskExecutionService:
        if self.execution_workflows is None:
            raise TaskCardServerRequestError(
                "service_unavailable",
                "execution workflow service is unavailable",
                HTTPStatus.SERVICE_UNAVAILABLE,
            )
        return self.execution_workflows


class _TaskCardHTTPServer(HTTPServer):
    allow_reuse_address = True

    def __init__(self, address: tuple[str, int], app: TaskCardApplication):
        self.app = app
        super().__init__(address, TaskCardRequestHandler)

    def get_request(self):
        connection, address = super().get_request()
        connection.settimeout(self.app.limits.request_timeout_seconds)
        return connection, address


class TaskCardRequestHandler(BaseHTTPRequestHandler):
    server_version = "foxhound-task-cards/1"
    sys_version = ""

    @property
    def app(self) -> TaskCardApplication:
        return self.server.app  # type: ignore[attr-defined,no-any-return]

    def do_GET(self) -> None:  # noqa: N802
        started = time.monotonic()
        if self.path != "/healthz":
            self._error(HTTPStatus.NOT_FOUND, "not_found", "route not found")
            self._audit(HTTPStatus.NOT_FOUND, started)
            return
        self._json(HTTPStatus.OK, {
            "schema": HEALTH_SCHEMA,
            "schema_version": SERVICE_VERSION,
            "ok": True,
        })
        self._audit(HTTPStatus.OK, started)

    def do_POST(self) -> None:  # noqa: N802
        started = time.monotonic()
        operation = ROUTES.get(self.path)
        if operation is None:
            self._error(HTTPStatus.NOT_FOUND, "not_found", "route not found")
            self._audit(HTTPStatus.NOT_FOUND, started)
            return
        auth_headers = self.headers.get_all("Authorization") or []
        authenticated = (
            self.app.authorized_execution(auth_headers[0])
            if operation.startswith("execution_") and len(auth_headers) == 1
            else self.app.authorized(auth_headers[0]) if len(auth_headers) == 1
            else False
        )
        if not authenticated:
            self._error(
                HTTPStatus.UNAUTHORIZED,
                "unauthorized",
                "authentication required",
            )
            self._audit(HTTPStatus.UNAUTHORIZED, started)
            return
        try:
            result = self.app.dispatch(
                operation,
                self._read_json_body(),
                authorization=auth_headers[0],
            )
            artifact = result.pop("artifact", None)
            if operation == "execution_artifact" and isinstance(
                artifact, ExecutionCardArtifact
            ):
                self._binary(HTTPStatus.OK, artifact.content)
            else:
                self._json(HTTPStatus.OK, result)
            self._audit(HTTPStatus.OK, started)
        except TaskCardServerRequestError as exc:
            self._error(exc.status, exc.code, exc.message)
            self._audit(exc.status, started)
        except TaskCardServerResponseTooLarge:
            self._error(
                HTTPStatus.BAD_GATEWAY,
                "response_too_large",
                "task card response exceeds the service limit",
            )
            self._audit(HTTPStatus.BAD_GATEWAY, started)
        except socket.timeout:
            self._error(
                HTTPStatus.REQUEST_TIMEOUT,
                "request_timeout",
                "request body timed out",
            )
            self._audit(HTTPStatus.REQUEST_TIMEOUT, started)
        except (BrokenPipeError, ConnectionResetError):
            self._audit(499, started)
        except Exception as exc:  # pragma: no cover - defensive boundary
            if _is_retryable_database_contention(exc):
                self._error(
                    HTTPStatus.SERVICE_UNAVAILABLE,
                    "temporarily_unavailable",
                    "card service is temporarily busy",
                    extra_headers={"Retry-After": str(RETRY_AFTER_SECONDS)},
                )
                self._audit(HTTPStatus.SERVICE_UNAVAILABLE, started)
                return
            log.error("status=500 exception=%s", type(exc).__name__)
            self._error(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                "internal_error",
                "internal service error",
            )
            self._audit(HTTPStatus.INTERNAL_SERVER_ERROR, started)

    def do_PUT(self) -> None:  # noqa: N802
        self._method_not_allowed()

    def do_PATCH(self) -> None:  # noqa: N802
        self._method_not_allowed()

    def do_DELETE(self) -> None:  # noqa: N802
        self._method_not_allowed()

    def _method_not_allowed(self) -> None:
        started = time.monotonic()
        self._error(
            HTTPStatus.METHOD_NOT_ALLOWED,
            "method_not_allowed",
            "method not allowed",
            extra_headers={"Allow": "GET, POST"},
        )
        self._audit(HTTPStatus.METHOD_NOT_ALLOWED, started)

    def _read_json_body(self) -> object:
        if self.headers.get("Transfer-Encoding"):
            raise TaskCardServerRequestError(
                "unsupported_transfer_encoding",
                "transfer encoding is not supported",
            )
        media_type = self.headers.get("Content-Type", "").split(";", 1)[0]
        if media_type.strip().lower() != "application/json":
            raise TaskCardServerRequestError(
                "unsupported_media_type",
                "content type must be application/json",
                HTTPStatus.UNSUPPORTED_MEDIA_TYPE,
            )
        lengths = self.headers.get_all("Content-Length") or []
        if len(lengths) != 1:
            raise TaskCardServerRequestError(
                "length_required",
                "one content length is required",
                HTTPStatus.LENGTH_REQUIRED,
            )
        try:
            length = int(lengths[0])
        except ValueError as exc:
            raise TaskCardServerRequestError(
                "invalid_request", "content length is invalid"
            ) from exc
        if length < 0 or length > self.app.limits.max_body_bytes:
            raise TaskCardServerRequestError(
                "request_too_large",
                "request body exceeds the service limit",
                HTTPStatus.REQUEST_ENTITY_TOO_LARGE,
            )
        body = self.rfile.read(length)
        if len(body) != length:
            raise TaskCardServerRequestError(
                "invalid_request", "request body is incomplete"
            )
        try:
            return json.loads(
                body.decode("utf-8"), object_pairs_hook=_strict_json_object
            )
        except (UnicodeDecodeError, ValueError) as exc:
            raise TaskCardServerRequestError(
                "invalid_json", "request body is not valid JSON"
            ) from exc

    def _json(
        self,
        status: int,
        document: dict[str, Any],
        *,
        extra_headers: dict[str, str] | None = None,
    ) -> None:
        body = json.dumps(
            document, ensure_ascii=False, separators=(",", ":")
        ).encode("utf-8")
        if len(body) > self.app.limits.max_response_bytes:
            if document.get("schema") == ERROR_SCHEMA:
                self.close_connection = True
                return
            raise TaskCardServerResponseTooLarge
        self.send_response(int(status))
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        if extra_headers:
            for name, value in extra_headers.items():
                self.send_header(name, value)
        self.end_headers()
        self.wfile.write(body)

    def _binary(self, status: int, body: bytes) -> None:
        if len(body) > self.app.limits.max_artifact_bytes:
            raise TaskCardServerResponseTooLarge
        self.send_response(int(status))
        self.send_header("Content-Type", "application/octet-stream")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.end_headers()
        self.wfile.write(body)

    def _error(
        self,
        status: int,
        code: str,
        message: str,
        *,
        extra_headers: dict[str, str] | None = None,
    ) -> None:
        self._json(status, {
            "schema": ERROR_SCHEMA,
            "schema_version": SERVICE_VERSION,
            "ok": False,
            "error": {"code": code, "message": message},
        }, extra_headers=extra_headers)

    def _audit(self, status: int, started: float) -> None:
        route = self.path.split("?", 1)[0]
        if route not in {"/healthz", *ROUTES}:
            route = "unknown"
        log.info(
            "method=%s route=%s status=%s duration_ms=%s",
            self.command,
            route,
            int(status),
            round((time.monotonic() - started) * 1000),
        )

    def log_message(self, _format: str, *_args: Any) -> None:
        return


def _is_retryable_database_contention(exc: BaseException) -> bool:
    """True only for SQLite's bounded lock-contention outcomes.

    The service never exposes exception text to the client.  Walking a short
    exception chain recognizes a service-layer wrapper while leaving every
    unrelated database failure as the ordinary non-retryable internal error.
    """
    current: BaseException | None = exc
    seen: set[int] = set()
    for _ in range(4):
        if current is None or id(current) in seen:
            return False
        seen.add(id(current))
        if isinstance(current, sqlite3.OperationalError):
            message = str(current).lower()
            return (
                "database is locked" in message
                or "database is busy" in message
            )
        current = current.__cause__ or current.__context__
    return False


def _request(payload: object, *, required: set[str]) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise TaskCardServerRequestError(
            "invalid_request", "request body must be a JSON object"
        )
    common = {"schema", "schema_version"}
    if set(payload) != common | required:
        raise TaskCardServerRequestError(
            "invalid_request", "request fields are invalid"
        )
    version = payload["schema_version"]
    if (
        payload["schema"] != REQUEST_SCHEMA
        or isinstance(version, bool)
        or version != SERVICE_VERSION
    ):
        raise TaskCardServerRequestError(
            "invalid_request", "request contract is unsupported"
        )
    return payload


def _strict_request(
    payload: object, *, required: set[str], optional: set[str]
) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise TaskCardServerRequestError(
            "invalid_request", "request body must be a JSON object"
        )
    if not isinstance(payload, dict) or not required.issubset(payload):
        raise TaskCardServerRequestError("invalid_request", "request fields are invalid")
    if set(payload) - ({"schema", "schema_version"} | required | optional):
        raise TaskCardServerRequestError(
            "invalid_request", "request contains an unknown field"
        )
    if payload.get("schema") != REQUEST_SCHEMA or payload.get("schema_version") != SERVICE_VERSION:
        raise TaskCardServerRequestError("invalid_request", "request contract is unsupported")
    return payload


def _integer(value: object, *, minimum: int, maximum: int | None = None) -> int:
    upper = 9_223_372_036_854_775_807 if maximum is None else maximum
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or not minimum <= value <= upper
    ):
        raise TaskCardServerRequestError(
            "invalid_request", "integer request field is outside its limit"
        )
    return value


def _secret(value: object) -> str:
    if not _valid_secret(value):
        raise TaskCardServerRequestError(
            "invalid_request", "claim capability is invalid"
        )
    return value


def _opaque(value: object, *, maximum: int) -> str:
    if (
        not isinstance(value, str)
        or not 1 <= len(value) <= maximum
        or value != value.strip()
        or any(ord(char) < 32 or ord(char) == 127 for char in value)
    ):
        raise TaskCardServerRequestError(
            "invalid_request", "opaque request field is invalid"
        )
    return value


def _reader_input(value: object, *, kind: str) -> str:
    maximum = 200 if kind == "reassignment" else 12 * 1024
    if (
        not isinstance(value, str)
        or value != value.strip()
        or not value
        or len(value.encode("utf-8")) > maximum
        or "\x00" in value
        or (kind == "reassignment" and "\n" in value)
    ):
        raise TaskCardServerRequestError(
            "invalid_request", "execution card input value is invalid"
        )
    return value


def _valid_secret(value: object) -> bool:
    return (
        isinstance(value, str)
        and 32 <= len(value) <= 4_096
        and not any(char.isspace() for char in value)
    )


def _normalize_role_tokens(token: str | Mapping[str, str]) -> dict[str, str]:
    """Validate and normalize the ``token`` constructor argument.

    Accepts either today's single bearer-token string (normalized to role
    ``drip``) or a role-to-token mapping over the closed
    ``TASK_CARD_CONSUMER_ROLES`` set. Every token is checked with the same
    length/whitespace discipline ``load_token`` already applies, and no two
    roles may share the same token -- an ambiguous token would make role
    resolution meaningless.

    A role outside the closed set is rejected here, at construction: this
    is this service's chosen fail-closed behavior for ADR 0036 decision 1,
    invariant 2 (the server refuses to start rather than run with an
    unresolvable role -- see ``TaskCardConsumerIdentityError`` for the
    accompanying runtime backstop).
    """
    if isinstance(token, str):
        candidate: dict[str, str] = {DRIP_ROLE: token}
    elif isinstance(token, Mapping):
        candidate = dict(token)
    else:
        raise TaskCardServerConfigError(
            "task card token must be a bearer token string or a "
            "role-to-token mapping"
        )
    if not candidate:
        raise TaskCardServerConfigError("at least one bearer token is required")
    seen: set[str] = set()
    for role, value in candidate.items():
        if role not in TASK_CARD_CONSUMER_ROLES:
            raise TaskCardServerConfigError(
                "token role must be one of "
                + ", ".join(sorted(TASK_CARD_CONSUMER_ROLES))
            )
        if not _valid_secret(value):
            raise TaskCardServerConfigError("task card bearer token is invalid")
        if value in seen:
            raise TaskCardServerConfigError(
                "bearer token is configured for more than one role"
            )
        seen.add(value)
    return candidate


def _consumer_digest(token: str) -> str:
    """The same digest approach already used for ``claim_token_digest`` in
    ``task_cards.py``: a hex SHA-256 of the credential, never the
    credential itself.
    """
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _strict_json_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON field")
        result[key] = value
    return result


def _schedule_document(result: ScheduleResult) -> dict[str, Any]:
    return {
        "schema": SCHEDULE_SCHEMA,
        "schema_version": SERVICE_VERSION,
        "ok": result.disposition != "refused",
        "disposition": result.disposition.value,
        "created": result.created,
        "cancelled": result.cancelled,
        "refusal": None if result.refusal is None else result.refusal.value,
    }


def _queue_card_document(card: Any) -> dict[str, Any]:
    """Expose the read-only ``due()`` projection without rendering or lease data."""
    document = asdict(card)
    # The service's enum is a str enum, but normalize it explicitly so this
    # contract remains JSON-shaped if the internal enum implementation changes.
    document["status"] = card.status.value
    return document


def _task_board_card_document(card: Any) -> dict[str, Any]:
    """Small, explicit task-board face; never serialize private provenance."""
    board_status = (
        "snoozed" if card.status is CardStatus.SNOOZED else "review"
    )
    return {
        "id": card.id,
        "version": card.version,
        "handle": f"task-{card.task_id}",
        "board_status": board_status,
        "delivery_status": card.status.value,
        "task": _queue_projection_text(card.text, 500),
        "owner": _queue_projection_text(card.owner, 200),
        "source": _queue_projection_text(card.origin_kind, 80),
        "state_since": _queue_projection_text(card.due_at, 64),
    }


def _queue_projection_text(value: object, maximum: int) -> str:
    if not isinstance(value, str):
        return ""
    if len(value) <= maximum:
        return value
    return value[: maximum - 1].rstrip() + "…"


def _queue_projection_records(records: object) -> list[dict[str, str]]:
    if not isinstance(records, (tuple, list)):
        return []
    result: list[dict[str, str]] = []
    for record in records[:32]:
        if not hasattr(record, "text"):
            continue
        result.append({
            "text": _queue_projection_text(record.text, 3_000),
            "requires": _queue_projection_text(record.requires, 200),
            "channel": _queue_projection_text(record.channel, 200),
            "label": _queue_projection_text(record.label, 200),
            "recipient": _queue_projection_text(record.recipient, 200),
            "subject": _queue_projection_text(record.subject, 500),
        })
    return result


def _execution_queue_card_document(card: Any) -> dict[str, Any]:
    """Dedicated ADR 0041 allowlist; never serialize the card dataclass."""
    return {
        "id": card.id,
        "version": card.version,
        "kind": card.kind.value,
        "phase": card.phase.value,
        "task": _queue_projection_text(card.task_text, 2_000),
        "owner": _queue_projection_text(card.owner, 200),
        "summary": _queue_projection_text(card.summary, 3_000),
        "work_digest": _queue_projection_text(card.work_digest, 3_000),
        "questions": [
            _queue_projection_text(question, 1_000)
            for question in card.questions[:32]
            if isinstance(question, str)
        ],
        "external_actions": _queue_projection_records(card.external_actions),
        "deliverables": _queue_projection_records(card.deliverables),
    }


def _execution_board_card_document(card: Any) -> dict[str, Any]:
    """Small, explicit execution-board face; no profile ids or raw sources.

    A board row is a headline. It used to carry a task at 500 characters and
    a summary at 1000, which measured ~900 bytes a row in practice: the route
    accepts `limit` up to 100, so it could serialize about 69 of them before
    the reply exceeded `max_response_bytes` and every caller got a 502. The
    detail routes carry the full text, and the column totals carry the true
    counts, so the row does not need to.
    """
    return {
        "id": card.id,
        "version": card.version,
        "task_id": card.task_id,
        "workflow_version": card.workflow_version,
        "handle": f"execution-{card.id}",
        "board_status": execution_board_status(card),
        "delivery_status": card.status.value,
        "kind": card.kind.value,
        "phase": card.phase.value,
        "task": _queue_projection_text(card.task_text, BOARD_TEXT_MAX),
        "owner": _queue_projection_text(card.owner, BOARD_OWNER_MAX),
        "agent": _queue_projection_text(card.agent_display_name, 200),
        "source": _queue_projection_text(card.origin_kind, 80),
        "summary": _queue_projection_text(card.summary, BOARD_SUMMARY_MAX),
        "state_since": _queue_projection_text(card.created_at, 64),
    }


def _operation_document(result: CardOperationResult) -> dict[str, Any]:
    return {
        "schema": OPERATION_SCHEMA,
        "schema_version": SERVICE_VERSION,
        "ok": result.accepted,
        "disposition": result.disposition.value,
        "card_id": result.card_id,
        "card_version": result.version,
        "card_status": None if result.status is None else result.status.value,
        "task_version": result.task_version,
        "task_status": (
            None if result.task_status is None else result.task_status.value
        ),
        "wake_at": result.wake_at,
        "refusal": None if result.refusal is None else result.refusal.value,
    }


def _view_document(result: CardPresentation) -> dict[str, Any]:
    """The card as this surface would render it now, or a refusal.

    `presentation` is absent rather than partial on a refusal, so a caller
    that cannot show the card is never handed something that looks showable.
    """
    document: dict[str, Any] = {
        "schema": VIEW_SCHEMA,
        "schema_version": SERVICE_VERSION,
        "ok": result.accepted,
        "disposition": result.disposition.value,
        "card_id": result.card_id,
        "card_version": result.card_version,
        "expanded": result.expanded,
        "presentation": None,
        "refusal": None if result.refusal is None else result.refusal.value,
    }
    if result.accepted and result.card is not None:
        body, reply_markup = render_duplicate_view(
            result.card, expanded=result.expanded)
        document["presentation"] = {"body": body, "reply_markup": reply_markup}
    return document


def _execution_schedule_document(
    result: ExecutionCardScheduleResult,
) -> dict[str, Any]:
    return {
        "schema": EXECUTION_SCHEDULE_SCHEMA,
        "schema_version": SERVICE_VERSION,
        "ok": result.disposition != "refused",
        "disposition": result.disposition.value,
        "created": result.created,
        "cancelled": result.cancelled,
        "refusal": None if result.refusal is None else result.refusal.value,
    }


def _execution_operation_document(
    result: ExecutionCardOperationResult,
) -> dict[str, Any]:
    return {
        "schema": EXECUTION_OPERATION_SCHEMA,
        "schema_version": SERVICE_VERSION,
        "ok": result.accepted,
        "disposition": result.disposition.value,
        "card_id": result.card_id,
        "card_version": result.card_version,
        "card_status": (
            None if result.card_status is None else result.card_status.value
        ),
        "workflow_version": result.workflow_version,
        "workflow_status": (
            None
            if result.workflow_status is None
            else result.workflow_status.value
        ),
        "workflow_phase": (
            None
            if result.workflow_phase is None
            else result.workflow_phase.value
        ),
        "wake_at": result.wake_at,
        "refusal": None if result.refusal is None else result.refusal.value,
    }


def _execution_resolve_document(result: ExecutionCardOperationResult) -> dict[str, Any]:
    return {
        "schema": EXECUTION_RESOLVE_SCHEMA,
        "schema_version": EXECUTION_RESOLVE_SCHEMA_VERSION,
        "ok": result.accepted,
        "status": "resolved" if result.accepted else "refused",
        "disposition": result.disposition.value,
        "card_id": result.card_id,
        "card_version": result.card_version,
        "refusal": None if result.refusal is None else result.refusal.value,
    }


def _execution_view_document(
    result: ExecutionCardPresentation,
) -> dict[str, Any]:
    """The same presentation the delivery rendered, or a refusal.

    `presentation` is absent rather than partial on a refusal: a caller that
    cannot restore the card must not be handed something that looks like it
    could be shown.
    """
    document: dict[str, Any] = {
        "schema": EXECUTION_VIEW_SCHEMA,
        "schema_version": SERVICE_VERSION,
        "ok": result.accepted,
        "disposition": result.disposition.value,
        "card_id": result.card_id,
        "card_version": result.card_version,
        # The kind travels with the presentation. A caller restoring a card
        # has only its id and version -- the claim that told it the kind is
        # long gone -- and the checks worth keeping on a restored keyboard
        # are the kind-dependent ones: no approval on work that has already
        # run, no owner hold outside a start gate.
        "kind": None if result.card is None else result.card.kind.value,
        "presentation": None,
        "refusal": None if result.refusal is None else result.refusal.value,
    }
    if result.accepted and result.card is not None:
        body, reply_markup = render_execution_review_card(result.card)
        document["presentation"] = {
            "body": body,
            "reply_markup": reply_markup,
        }
    return document


def _execution_deliverables_document(
    result: ExecutionCardDeliverables,
) -> dict[str, Any]:
    """Serialize one non-mutating, version-fenced deliverables read."""
    return {
        "schema": EXECUTION_DELIVERABLES_SCHEMA,
        "schema_version": SERVICE_VERSION,
        "ok": result.accepted,
        "disposition": result.disposition.value,
        "card_id": result.card_id,
        "card_version": result.card_version,
        "text": result.text if result.accepted else None,
        "refusal": None if result.refusal is None else result.refusal.value,
    }


def _execution_artifacts_document(
    result: ExecutionCardArtifacts,
) -> dict[str, Any]:
    """Serialize file metadata without exposing archive paths or digests."""
    return {
        "schema": EXECUTION_ARTIFACTS_SCHEMA,
        "schema_version": SERVICE_VERSION,
        "ok": result.accepted,
        "disposition": result.disposition.value,
        "card_id": result.card_id,
        "card_version": result.card_version,
        "artifacts": [
            {
                "ordinal": artifact.ordinal,
                "name": artifact.name,
                "size_bytes": artifact.size_bytes,
            }
            for artifact in result.artifacts
        ] if result.accepted else None,
        "refusal": None if result.refusal is None else result.refusal.value,
    }


def _execution_detail_document(result: ExecutionCardDetail) -> dict[str, Any]:
    """Serialize only the bounded current-run/detail allowlist."""
    document: dict[str, Any] = {
        "schema": EXECUTION_DETAIL_SCHEMA,
        "schema_version": EXECUTION_DETAIL_SCHEMA_VERSION,
        "ok": result.accepted,
        "disposition": result.disposition.value,
        "card_id": result.card_id,
        "card_version": result.card_version,
        "workflow_version": result.workflow_version,
        "status": None if result.status is None else result.status.value,
        "phase": None if result.phase is None else result.phase.value,
        "updated_at": result.updated_at,
        "due_at": result.due_at,
        "completed_at": result.completed_at,
        "outcome": None if result.outcome is None else result.outcome.value,
        "summary": _queue_projection_text(result.summary, 1_200),
        "work_digest": _queue_projection_text(result.work_digest, 800),
        "work_markdown": _queue_projection_text(
            result.work_markdown, WORK_BODY_PROJECTION_MAX),
        "deliverables": _queue_projection_records(result.deliverables),
        "failure_reason": result.failure_reason,
        "failure_exit_code": result.failure_exit_code,
        "failure_run_id": result.failure_run_id,
        "refusal": None if result.refusal is None else result.refusal.value,
    }
    if not result.accepted:
        for key in ("workflow_version", "status", "phase", "updated_at",
                    "due_at", "completed_at", "outcome", "summary",
                    "work_digest", "work_markdown", "deliverables",
                    "failure_reason", "failure_exit_code", "failure_run_id"):
            document[key] = None if key != "deliverables" else []
    return document


def _workflow_board_document(result: WorkflowBoard) -> dict[str, Any]:
    return {
        "schema": WORKFLOW_BOARD_SCHEMA,
        "schema_version": SERVICE_VERSION,
        "ok": True,
        "columns": [
            {"status": status, "total": result.totals[status]}
            for status in WORKFLOW_BOARD_STATUSES
        ],
        "workflows": [
            {
                "task_id": entry.task_id,
                "workflow_version": entry.workflow_version,
                "board_status": entry.board_status,
                "phase": entry.phase.value,
                "task": entry.task,
                "owner": entry.owner,
                "agent": entry.agent,
                "state_since": entry.state_since,
            }
            for entry in result.entries
        ],
    }


def _workflow_detail_document(result: WorkflowBoardDetail) -> dict[str, Any]:
    document = {
        "schema": WORKFLOW_DETAIL_SCHEMA,
        "schema_version": WORKFLOW_DETAIL_SCHEMA_VERSION,
        "ok": result.accepted,
        "task_id": result.task_id,
        "workflow_version": result.workflow_version,
        "status": None if result.status is None else result.status.value,
        "phase": None if result.phase is None else result.phase.value,
        "updated_at": result.updated_at,
        "summary": result.summary if result.accepted else "",
        "work_digest": result.work_digest if result.accepted else "",
        "work_markdown": result.work_markdown if result.accepted else "",
        "deliverables": list(result.deliverables) if result.accepted else [],
        "refusal": None if result.refusal is None else result.refusal.value,
    }
    return document


def _execution_priority_document(
    result: WorkflowOperationResult,
) -> dict[str, Any]:
    """Serialize a content-free queue-priority acknowledgement."""
    return {
        "schema": EXECUTION_PRIORITY_SCHEMA,
        "schema_version": EXECUTION_PRIORITY_SCHEMA_VERSION,
        "ok": result.accepted,
        "disposition": result.disposition.value,
        "task_id": result.task_id,
        "workflow_version": result.version,
        "priority": (
            None if result.priority is None else result.priority.value
        ),
        "refusal": (
            None if result.refusal is None else result.refusal.value
        ),
    }


def _execution_agent_options_document(
    result: ExecutionAgentSelectorResult,
) -> dict[str, Any]:
    document: dict[str, Any] = {
        "schema": EXECUTION_AGENT_OPTIONS_SCHEMA,
        "schema_version": SERVICE_VERSION,
        "ok": result.accepted,
        "disposition": result.disposition.value,
        "card_id": result.card_id,
        "card_version": result.card_version,
        "presentation": None,
        "refusal": None if result.refusal is None else result.refusal.value,
    }
    if result.accepted:
        body, reply_markup = render_execution_agent_selector(result)
        document["presentation"] = {
            "body": body,
            "reply_markup": reply_markup,
        }
    return document


def _execution_agent_selection_document(
    result: ExecutionAgentSelectorResult,
) -> dict[str, Any]:
    document: dict[str, Any] = {
        "schema": EXECUTION_AGENT_SELECTION_SCHEMA,
        "schema_version": SERVICE_VERSION,
        "ok": result.accepted,
        "disposition": result.disposition.value,
        "card_id": result.card_id,
        "card_version": result.card_version,
        "agent_profile_id": None,
        "agent_display_name": None,
        "presentation": None,
        "refusal": None if result.refusal is None else result.refusal.value,
    }
    if result.accepted and result.card is not None:
        body, reply_markup = render_execution_review_card(result.card)
        document.update(
            agent_profile_id=result.card.agent_profile_id,
            agent_display_name=result.card.agent_display_name,
            presentation={"body": body, "reply_markup": reply_markup},
        )
    return document


def load_token(path: str | os.PathLike[str]) -> str:
    token_path = Path(path).expanduser()
    if token_path.is_symlink():
        raise TaskCardServerConfigError("task card token file is unavailable")
    flags = os.O_RDONLY
    flags |= getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(token_path, flags)
    except OSError as exc:
        raise TaskCardServerConfigError("task card token file is unavailable") from exc
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode):
            raise TaskCardServerConfigError(
                "task card token file must be a regular file"
            )
        if os.name == "posix" and (
            info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) != 0o600
        ):
            raise TaskCardServerConfigError(
                "task card token file must be owner-only mode 0600"
            )
        with os.fdopen(descriptor, "r", encoding="utf-8") as handle:
            descriptor = -1
            token = handle.read().strip()
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    if not _valid_secret(token):
        raise TaskCardServerConfigError("task card bearer token is invalid")
    return token


def load_role_tokens(
    specs: list[str], *, option_name: str = "--token-file"
) -> dict[str, str]:
    """Load one or more token-file specs into a role-to-token map.

    A single spec with no ``=`` is today's invocation shape: the file it
    names is loaded exactly as ``load_token`` always has, with no role
    written anywhere, and normalized to role ``drip`` by
    ``TaskCardApplication`` itself. This is what keeps an existing
    single-token deployment working unchanged with no configuration edit
    (ADR 0036, invariant 3).

    Once more than one ``--token-file`` is configured, each one must name
    its role explicitly as ``ROLE=PATH``, because there is no longer a
    single implicit default to fall back on.
    """
    if not specs:
        raise TaskCardServerConfigError(
            f"at least one {option_name} is required"
        )
    if len(specs) == 1 and "=" not in specs[0]:
        return {DRIP_ROLE: load_token(specs[0])}
    tokens: dict[str, str] = {}
    for spec in specs:
        if "=" not in spec:
            raise TaskCardServerConfigError(
                f"{option_name} must have the form ROLE=PATH once more than "
                "one is configured"
            )
        role, path = spec.split("=", 1)
        if role not in TASK_CARD_CONSUMER_ROLES:
            raise TaskCardServerConfigError(
                "token role must be one of "
                + ", ".join(sorted(TASK_CARD_CONSUMER_ROLES))
            )
        if role in tokens:
            raise TaskCardServerConfigError("token role is duplicated")
        if not path:
            raise TaskCardServerConfigError("token file path must not be empty")
        tokens[role] = load_token(path)
    if len(set(tokens.values())) != len(tokens):
        raise TaskCardServerConfigError(
            "bearer token is configured for more than one role"
        )
    return tokens


def is_canonical_loopback(host: object) -> bool:
    if not isinstance(host, str):
        return False
    try:
        address = ipaddress.ip_address(host)
    except ValueError:
        return False
    return address.version == 4 and address.is_loopback and address.compressed == host


def make_server(
    host: str, port: int, app: TaskCardApplication
) -> _TaskCardHTTPServer:
    if not is_canonical_loopback(host):
        raise TaskCardServerConfigError(
            "task card service requires a canonical IPv4 loopback bind"
        )
    if (
        isinstance(port, bool)
        or not isinstance(port, int)
        or not 0 <= port <= 65_535
    ):
        raise TaskCardServerConfigError("task card service port is invalid")
    return _TaskCardHTTPServer((host, port), app)


def serve(host: str, port: int, app: TaskCardApplication) -> None:
    if port == 0:
        raise TaskCardServerConfigError("task card production port must be nonzero")
    server = make_server(host, port, app)
    log.info("task card service started")
    try:
        server.serve_forever(poll_interval=0.25)
    finally:
        server.server_close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Serve Foxhound review cards on an authenticated loopback API"
    )
    parser.add_argument("--database", required=True)
    parser.add_argument(
        "--token-file",
        action="append",
        required=True,
        metavar="PATH|ROLE=PATH",
        help=(
            "bearer token file (repeatable); a single bare PATH defaults to "
            "role 'drip' with no other change, matching today's behavior; "
            "once more than one is given, each must name its role "
            "explicitly as ROLE=PATH with ROLE in {drip, queue_view}"
        ),
    )
    parser.add_argument(
        "--execution-token-file",
        action="append",
        default=None,
        metavar="PATH|ROLE=PATH",
        help=(
            "independent execution-card bearer token file (repeatable); "
            "a single bare PATH defaults to role 'drip', while multiple "
            "files must use ROLE=PATH with ROLE in {drip, queue_view}; "
            "omitting this option keeps execution authorization disabled "
            "for role-mapped task tokens"
        ),
    )
    parser.add_argument("--bind", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8790)
    parser.add_argument("--request-timeout", type=float, default=5.0)
    parser.add_argument(
        "--steer-plan-threshold-seconds", type=int, default=20 * 60,
        help="running-plan age before its Steer card is eligible",
    )
    parser.add_argument(
        "--steer-execute-threshold-seconds", type=int, default=20 * 60,
        help="running-execute age before its Steer card is eligible",
    )
    parser.add_argument("--agent-profile-directory", type=Path)
    parser.add_argument(
        "--task-work-root", type=Path,
        help="canonical task archive root used for verified result files",
    )
    parser.add_argument("--gw-endpoint")
    parser.add_argument("--gw-alias")
    parser.add_argument("--gw-token-file", type=Path)
    arguments = parser.parse_args(argv)
    try:
        cards = TaskCardService(arguments.database)
        cards.count()
        registry = load_registry(arguments.agent_profile_directory)
        gw_values = (
            arguments.gw_endpoint,
            arguments.gw_alias,
            arguments.gw_token_file,
        )
        if any(value is not None for value in gw_values) and not all(gw_values):
            raise TaskCardServerConfigError(
                "GW owner meeting configuration must be complete"
            )
        owner_condition = None
        reader_aliases: tuple[str, ...] = ()
        if all(gw_values):
            knowledge = GwKnowledgeClient(load_knowledge_config(*gw_values))
            context = knowledge.execution_context()
            owner_condition = knowledge.owner_upcoming_meeting
            reader_aliases = (context.display_name, *context.self_aliases)
        execution_cards = ExecutionCardService(
            arguments.database,
            profile_registry=registry,
            owner_condition=owner_condition,
            reader_aliases=reader_aliases,
            artifact_root=arguments.task_work_root,
            steer_plan_threshold=timedelta(
                seconds=arguments.steer_plan_threshold_seconds),
            steer_execute_threshold=timedelta(
                seconds=arguments.steer_execute_threshold_seconds),
        )
        execution_cards.count()
        execution_workflows = TaskExecutionService(
            arguments.database,
            profile_registry=registry,
        )
        execution_workflows.readiness()
        app = TaskCardApplication(
            cards,
            load_role_tokens(arguments.token_file),
            execution_cards=execution_cards,
            execution_workflows=execution_workflows,
            execution_tokens=(
                load_role_tokens(
                    arguments.execution_token_file,
                    option_name="--execution-token-file",
                )
                if arguments.execution_token_file is not None
                else (
                    load_token(arguments.token_file[0])
                    if len(arguments.token_file) == 1
                    and "=" not in arguments.token_file[0]
                    else None
                )
            ),
            limits=TaskCardServerLimits(
                request_timeout_seconds=arguments.request_timeout
            ),
        )
        # Reported before the first request, so a process that has outlived
        # its deploy is visible without being interrogated.
        print(f"foxhound task-card-server: revision {describe(__file__)}",
              flush=True)
        # Same reasoning, for a capability rather than a revision. A
        # deployment with no artifact root refuses every artifact request,
        # for its whole life, and the refusal alone looks like a card that
        # happens to have produced no files. Said once, at the only moment
        # it can be acted on, it reads as the configuration decision it is.
        if not execution_cards.serves_artifacts:
            print(
                "foxhound task-card-server: result artifacts unavailable "
                "(no task work root configured; requires deployment "
                "configuration version 8)",
                flush=True,
            )
        serve(arguments.bind, arguments.port, app)
    except (
        AgentProfileError,
        ExecutionWorkerConfigError,
        KnowledgeClientError,
        OSError,
        TaskLedgerError,
        TaskCardServerConfigError,
        ValueError,
    ) as exc:
        print(f"foxhound task-card-server: {exc}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        return 0
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
