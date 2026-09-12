"""Transport-neutral reader cards for Foxhound execution gates."""

from __future__ import annotations

import hashlib
import html
import json
import os
import re
import secrets
import sqlite3
import urllib.parse
from contextlib import closing
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import StrEnum
from pathlib import Path
from typing import Callable, Mapping, Sequence

from .candidate_inbox import CandidateInbox, InboxError, SCHEMA_VERSION
from .task_execution import (
    ExecutionOutcome,
    REVIEW_SNOOZE_INTERVALS,
    TaskExecutionService,
    WorkflowDisposition,
    WorkflowOperationResult,
    WorkflowPhase,
    WorkflowRefusal,
    WorkflowStatus,
    _apply_review_action,
    _apply_start_action,
)
from .task_ledger import (
    TaskLedgerError,
    TaskStatus,
    TransitionDisposition,
    _apply_task_transition,
)


CALLBACK_PREFIX = "fhe"
CALLBACK_DATA_LIMIT = 64
MAX_CARD_BODY_BYTES = 24 * 1024
MAX_RENDER_SOURCE_LINE_CHARS = 500
MAX_TRUNCATED_CARD_BODY_BYTES = 3_500
ACTIVE_STATUSES = ("pending", "delivering", "delivered")
REVIEW_DIRECT_ACTIONS = {
    "approve", "revise", "cancel", "done", "drop",
    *REVIEW_SNOOZE_INTERVALS,
}
READER_INPUT_KINDS = {"discussion", "reassignment"}
MAX_DISCUSSION_CHARS = 16_000
MAX_OWNER_CHARS = 200


class ExecutionCardKind(StrEnum):
    START = "start"
    PLAN_REVIEW = "plan_review"
    EXTERNAL_REVIEW = "external_review"
    RESULT_REVIEW = "result_review"


class ExecutionCardStatus(StrEnum):
    PENDING = "pending"
    DELIVERING = "delivering"
    DELIVERED = "delivered"
    RESOLVED = "resolved"
    CANCELLED = "cancelled"


class ExecutionCardDisposition(StrEnum):
    APPLIED = "applied"
    UNCHANGED = "unchanged"
    REFUSED = "refused"


class ExecutionCardRefusal(StrEnum):
    INVALID_ARGUMENT = "invalid_argument"
    INVALID_ACTION = "invalid_action"
    NOT_FOUND = "not_found"
    STALE_VERSION = "stale_version"
    INVALID_STATE = "invalid_state"
    CLAIM_MISMATCH = "claim_mismatch"


@dataclass(frozen=True)
class ExecutionCardScheduleResult:
    disposition: ExecutionCardDisposition
    created: int = 0
    cancelled: int = 0
    refusal: ExecutionCardRefusal | None = None


@dataclass(frozen=True)
class ExecutionCardStats:
    pending: int
    delivering: int
    delivered: int
    active: int


@dataclass(frozen=True)
class ExecutionReviewCard:
    id: int
    task_id: int
    task_version: int
    workflow_version: int
    kind: ExecutionCardKind
    phase: WorkflowPhase
    result_id: str | None
    status: ExecutionCardStatus
    version: int
    created_at: str
    task_text: str = field(repr=False)
    owner: str | None = field(repr=False)
    due: str | None = field(repr=False)
    summary: str = field(default="", repr=False)
    work_markdown: str = field(default="", repr=False)
    questions: tuple[str, ...] = field(default=(), repr=False)
    external_actions: tuple[str, ...] = field(default=(), repr=False)
    deliverables: tuple[str, ...] = field(default=(), repr=False)
    outcome: ExecutionOutcome | None = None


@dataclass(frozen=True)
class ExecutionCardDeliveryClaim:
    card: ExecutionReviewCard
    token: str = field(repr=False)
    expires_at: str


@dataclass(frozen=True)
class ExecutionCardOperationResult:
    disposition: ExecutionCardDisposition
    card_id: int
    card_version: int | None = None
    card_status: ExecutionCardStatus | None = None
    workflow_version: int | None = None
    workflow_status: WorkflowStatus | None = None
    workflow_phase: WorkflowPhase | None = None
    wake_at: str | None = None
    refusal: ExecutionCardRefusal | None = None

    @property
    def accepted(self) -> bool:
        return self.disposition is not ExecutionCardDisposition.REFUSED


class ExecutionCardService:
    """Durable delivery and atomic reader actions for execution gates."""

    def __init__(
        self,
        database_path: str | os.PathLike[str],
        *,
        clock: Callable[[], datetime] | None = None,
        token_factory: Callable[[], str] | None = None,
    ) -> None:
        self.database_path = Path(database_path)
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._token_factory = token_factory or (
            lambda: secrets.token_urlsafe(32)
        )

    def initialize(self) -> None:
        CandidateInbox(self.database_path, clock=self._clock).initialize()

    def schedule(self, *, limit: int = 100) -> ExecutionCardScheduleResult:
        if not _valid_limit(limit):
            return ExecutionCardScheduleResult(
                ExecutionCardDisposition.REFUSED,
                refusal=ExecutionCardRefusal.INVALID_ARGUMENT,
            )
        now = self._now()
        with closing(self._connect()) as connection:
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute("BEGIN IMMEDIATE")
            try:
                cancelled = self._cancel_stale(connection, now)
                rows = connection.execute(
                    "SELECT w.task_id,w.task_version,w.status,w.phase,"
                    "w.version,w.last_result_id,r.outcome "
                    "FROM task_execution_workflows AS w "
                    "JOIN tasks AS t ON t.id=w.task_id "
                    "LEFT JOIN task_execution_results AS r "
                    "ON r.result_id=w.last_result_id AND r.task_id=w.task_id "
                    "WHERE t.status='open' AND t.version=w.task_version "
                    "AND NOT EXISTS("
                    " SELECT 1 FROM execution_review_cards AS active "
                    " WHERE active.task_id=w.task_id AND active.status IN "
                    " ('pending','delivering','delivered')"
                    ") AND ("
                    " w.status='awaiting_start' OR "
                    " (w.status='snoozed' AND w.due_at<=?) OR "
                    " (w.status='awaiting_review' AND w.phase='plan' "
                    "  AND r.outcome='awaiting_plan') OR "
                    " (w.status='awaiting_review' AND w.phase='execute' "
                    "  AND r.outcome='awaiting_external') OR "
                    " (w.status IN ('awaiting_review','completed') "
                    "  AND r.outcome IN ('completed','declined','ineligible'))"
                    ") ORDER BY w.updated_at,w.task_id LIMIT ?",
                    (now, limit),
                ).fetchall()
                for row in rows:
                    kind = _kind_for_workflow(row)
                    result_id = (
                        None
                        if kind is ExecutionCardKind.START
                        else row["last_result_id"]
                    )
                    cursor = connection.execute(
                        "INSERT INTO execution_review_cards("
                        "task_id,task_version,workflow_version,kind,phase,"
                        "result_id,status,version,created_at,updated_at) "
                        "VALUES(?,?,?,?,?,?,'pending',1,?,?)",
                        (
                            int(row["task_id"]),
                            int(row["task_version"]),
                            int(row["version"]),
                            kind,
                            row["phase"],
                            result_id,
                            now,
                            now,
                        ),
                    )
                    self._event(
                        connection,
                        card_id=int(cursor.lastrowid),
                        task_id=int(row["task_id"]),
                        kind="scheduled",
                        card_version=1,
                        workflow_version=int(row["version"]),
                        action=None,
                        now=now,
                    )
                connection.commit()
                created = len(rows)
                return ExecutionCardScheduleResult(
                    ExecutionCardDisposition.APPLIED
                    if created or cancelled
                    else ExecutionCardDisposition.UNCHANGED,
                    created=created,
                    cancelled=cancelled,
                )
            except Exception:
                connection.rollback()
                raise

    def claim_next(
        self, *, lease_seconds: int = 60
    ) -> ExecutionCardDeliveryClaim | None:
        if not _valid_lease(lease_seconds):
            raise TaskLedgerError("execution card delivery lease is invalid")
        stamp = self._clock_value()
        now = stamp.isoformat(timespec="seconds")
        expires = (stamp + timedelta(seconds=lease_seconds)).isoformat(
            timespec="seconds"
        )
        token = self._token_factory()
        if not _valid_secret(token):
            raise TaskLedgerError(
                "execution card token factory returned invalid state"
            )
        digest = _token_digest(token)
        with closing(self._connect()) as connection:
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute("BEGIN IMMEDIATE")
            try:
                self._cancel_stale(connection, now)
                expired = connection.execute(
                    "SELECT id,task_id,workflow_version,version "
                    "FROM execution_review_cards WHERE status='delivering' "
                    "AND claim_expires_at<=? ORDER BY id",
                    (now,),
                ).fetchall()
                for row in expired:
                    version = int(row["version"]) + 1
                    connection.execute(
                        "UPDATE execution_review_cards SET status='pending',"
                        "version=?,claim_token_digest=NULL,"
                        "claim_expires_at=NULL,updated_at=? "
                        "WHERE id=? AND version=? AND status='delivering'",
                        (version, now, int(row["id"]), int(row["version"])),
                    )
                    self._event(
                        connection,
                        card_id=int(row["id"]),
                        task_id=int(row["task_id"]),
                        kind="delivery_expired",
                        card_version=version,
                        workflow_version=int(row["workflow_version"]),
                        action=None,
                        now=now,
                    )
                row = connection.execute(
                    self._card_select()
                    + " WHERE c.status='pending' "
                    "ORDER BY c.created_at,c.id LIMIT 1"
                ).fetchone()
                if row is None:
                    connection.commit()
                    return None
                if not _current_card(row):
                    raise TaskLedgerError("execution card state is invalid")
                version = int(row["version"]) + 1
                updated = connection.execute(
                    "UPDATE execution_review_cards SET status='delivering',"
                    "version=?,claim_token_digest=?,claim_expires_at=?,"
                    "transport=NULL,delivery_ref=NULL,delivered_at=NULL,"
                    "updated_at=? WHERE id=? AND version=? AND status='pending'",
                    (
                        version,
                        digest,
                        expires,
                        now,
                        int(row["id"]),
                        int(row["version"]),
                    ),
                )
                if updated.rowcount != 1:
                    connection.rollback()
                    return None
                self._event(
                    connection,
                    card_id=int(row["id"]),
                    task_id=int(row["task_id"]),
                    kind="delivery_claimed",
                    card_version=version,
                    workflow_version=int(row["workflow_version"]),
                    action=None,
                    now=now,
                )
                connection.commit()
                values = dict(row)
                values.update(status=ExecutionCardStatus.DELIVERING, version=version)
                return ExecutionCardDeliveryClaim(
                    _card(values), token, expires
                )
            except Exception:
                connection.rollback()
                raise

    def complete_delivery(
        self,
        card_id: int,
        *,
        expected_version: int,
        claim_token: str,
        transport: str,
        delivery_ref: str,
    ) -> ExecutionCardOperationResult:
        if not _valid_identity(card_id, expected_version):
            return _refused(card_id, ExecutionCardRefusal.INVALID_ARGUMENT)
        if (
            not _valid_secret(claim_token)
            or not _valid_opaque(transport, 64)
            or not _valid_opaque(delivery_ref, 200)
        ):
            return _refused(card_id, ExecutionCardRefusal.INVALID_ARGUMENT)
        now = self._now()
        digest = _token_digest(claim_token)
        with closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = connection.execute(
                    "SELECT * FROM execution_review_cards WHERE id=?",
                    (card_id,),
                ).fetchone()
                refusal = _card_guard(row, expected_version)
                if refusal is not None:
                    connection.rollback()
                    return _refused_row(card_id, row, refusal)
                if row["status"] == ExecutionCardStatus.DELIVERED:
                    connection.rollback()
                    if (
                        row["transport"] == transport
                        and row["delivery_ref"] == delivery_ref
                    ):
                        return _operation(row, ExecutionCardDisposition.UNCHANGED)
                    return _refused_row(
                        card_id, row, ExecutionCardRefusal.INVALID_STATE
                    )
                if row["status"] != ExecutionCardStatus.DELIVERING:
                    connection.rollback()
                    return _refused_row(
                        card_id, row, ExecutionCardRefusal.INVALID_STATE
                    )
                if row["claim_token_digest"] != digest:
                    connection.rollback()
                    return _refused_row(
                        card_id, row, ExecutionCardRefusal.CLAIM_MISMATCH
                    )
                connection.execute(
                    "UPDATE execution_review_cards SET status='delivered',"
                    "claim_token_digest=NULL,claim_expires_at=NULL,transport=?,"
                    "delivery_ref=?,delivered_at=?,updated_at=? "
                    "WHERE id=? AND version=?",
                    (
                        transport,
                        delivery_ref,
                        now,
                        now,
                        card_id,
                        expected_version,
                    ),
                )
                self._event(
                    connection,
                    card_id=card_id,
                    task_id=int(row["task_id"]),
                    kind="delivered",
                    card_version=expected_version,
                    workflow_version=int(row["workflow_version"]),
                    action=None,
                    now=now,
                )
                connection.commit()
                values = dict(row)
                values.update(
                    status=ExecutionCardStatus.DELIVERED,
                    claim_token_digest=None,
                    claim_expires_at=None,
                    transport=transport,
                    delivery_ref=delivery_ref,
                    delivered_at=now,
                )
                return _operation(values, ExecutionCardDisposition.APPLIED)
            except Exception:
                connection.rollback()
                raise

    def fail_delivery(
        self, card_id: int, *, expected_version: int, claim_token: str
    ) -> ExecutionCardOperationResult:
        if (
            not _valid_identity(card_id, expected_version)
            or not _valid_secret(claim_token)
        ):
            return _refused(card_id, ExecutionCardRefusal.INVALID_ARGUMENT)
        now = self._now()
        digest = _token_digest(claim_token)
        with closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = connection.execute(
                    "SELECT * FROM execution_review_cards WHERE id=?",
                    (card_id,),
                ).fetchone()
                refusal = _card_guard(row, expected_version)
                if refusal is not None:
                    connection.rollback()
                    return _refused_row(card_id, row, refusal)
                if row["status"] != ExecutionCardStatus.DELIVERING:
                    connection.rollback()
                    return _refused_row(
                        card_id, row, ExecutionCardRefusal.INVALID_STATE
                    )
                if row["claim_token_digest"] != digest:
                    connection.rollback()
                    return _refused_row(
                        card_id, row, ExecutionCardRefusal.CLAIM_MISMATCH
                    )
                version = expected_version + 1
                connection.execute(
                    "UPDATE execution_review_cards SET status='pending',"
                    "version=?,claim_token_digest=NULL,claim_expires_at=NULL,"
                    "updated_at=? WHERE id=? AND version=?",
                    (version, now, card_id, expected_version),
                )
                self._event(
                    connection,
                    card_id=card_id,
                    task_id=int(row["task_id"]),
                    kind="delivery_failed",
                    card_version=version,
                    workflow_version=int(row["workflow_version"]),
                    action=None,
                    now=now,
                )
                connection.commit()
                values = dict(row)
                values.update(
                    status=ExecutionCardStatus.PENDING,
                    version=version,
                    claim_token_digest=None,
                    claim_expires_at=None,
                )
                return _operation(values, ExecutionCardDisposition.APPLIED)
            except Exception:
                connection.rollback()
                raise

    def retry_delivery(
        self, card_id: int, *, expected_version: int
    ) -> ExecutionCardOperationResult:
        """Requeue a current card whose acknowledged presentation failed.

        This is a local operator repair, not part of the transport API. It
        changes only delivery metadata and versions the card so callbacks on
        the superseded presentation become stale.
        """
        if not _valid_identity(card_id, expected_version):
            return _refused(card_id, ExecutionCardRefusal.INVALID_ARGUMENT)
        now = self._now()
        with closing(self._connect()) as connection:
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = connection.execute(
                    self._card_select() + " WHERE c.id=?", (card_id,)
                ).fetchone()
                refusal = _card_guard(row, expected_version)
                if (
                    refusal is None
                    and row["status"] != ExecutionCardStatus.DELIVERED
                ):
                    refusal = ExecutionCardRefusal.INVALID_STATE
                if refusal is None and not _current_card(row):
                    refusal = ExecutionCardRefusal.STALE_VERSION
                if refusal is not None:
                    connection.rollback()
                    return _refused_row(card_id, row, refusal)
                version = expected_version + 1
                updated = connection.execute(
                    "UPDATE execution_review_cards SET status='pending',"
                    "version=?,claim_token_digest=NULL,claim_expires_at=NULL,"
                    "transport=NULL,delivery_ref=NULL,delivered_at=NULL,"
                    "updated_at=? WHERE id=? AND version=? "
                    "AND status='delivered'",
                    (version, now, card_id, expected_version),
                )
                if updated.rowcount != 1:
                    raise TaskLedgerError("execution card state changed")
                self._event(
                    connection,
                    card_id=card_id,
                    task_id=int(row["task_id"]),
                    kind="delivery_failed",
                    card_version=version,
                    workflow_version=int(row["workflow_version"]),
                    action=None,
                    now=now,
                )
                connection.commit()
                values = dict(row)
                values.update(
                    status=ExecutionCardStatus.PENDING,
                    version=version,
                    claim_token_digest=None,
                    claim_expires_at=None,
                    transport=None,
                    delivery_ref=None,
                    delivered_at=None,
                )
                return _operation(values, ExecutionCardDisposition.APPLIED)
            except Exception:
                connection.rollback()
                raise

    def act(
        self, card_id: int, *, expected_version: int, action: str
    ) -> ExecutionCardOperationResult:
        if not _valid_identity(card_id, expected_version):
            return _refused(card_id, ExecutionCardRefusal.INVALID_ARGUMENT)
        if action not in {"start", "snooze", *REVIEW_DIRECT_ACTIONS}:
            return _refused(card_id, ExecutionCardRefusal.INVALID_ACTION)
        stamp = self._clock_value()
        now = stamp.isoformat(timespec="seconds")
        with closing(self._connect()) as connection:
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = connection.execute(
                    self._card_select() + " WHERE c.id=?", (card_id,)
                ).fetchone()
                refusal = _card_guard(row, expected_version)
                if refusal is None and row["status"] != ExecutionCardStatus.DELIVERED:
                    refusal = ExecutionCardRefusal.INVALID_STATE
                if refusal is None and not _current_card(row):
                    refusal = ExecutionCardRefusal.STALE_VERSION
                if refusal is None and action not in _direct_actions_for_kind(
                    ExecutionCardKind(row["kind"])
                ):
                    refusal = ExecutionCardRefusal.INVALID_ACTION
                if (
                    refusal is None
                    and action in {"start", "approve", "done"}
                    and not _card_fits(_card(row))
                ):
                    refusal = ExecutionCardRefusal.INVALID_STATE
                if refusal is not None:
                    connection.rollback()
                    return _refused_row(card_id, row, refusal)

                card_kind = ExecutionCardKind(row["kind"])
                if card_kind is ExecutionCardKind.START:
                    workflow = _apply_start_action(
                        connection,
                        int(row["task_id"]),
                        expected_version=int(row["workflow_version"]),
                        action=action,
                        stamp=stamp,
                    )
                elif action in {"done", "drop"}:
                    workflow = _apply_review_lifecycle_action(
                        connection,
                        row,
                        action=action,
                        now=now,
                    )
                else:
                    workflow = _apply_review_action(
                        connection,
                        int(row["task_id"]),
                        expected_version=int(row["workflow_version"]),
                        action=action,
                        stamp=stamp,
                    )
                if workflow.disposition is WorkflowDisposition.REFUSED:
                    connection.rollback()
                    return _refused_row(
                        card_id,
                        row,
                        _workflow_refusal(workflow.refusal),
                    )
                version = expected_version + 1
                updated = connection.execute(
                    "UPDATE execution_review_cards SET status='resolved',"
                    "version=?,claim_token_digest=NULL,claim_expires_at=NULL,"
                    "resolution=?,resolved_at=?,updated_at=? "
                    "WHERE id=? AND version=? AND status='delivered'",
                    (
                        version,
                        _stored_action(action),
                        now,
                        now,
                        card_id,
                        expected_version,
                    ),
                )
                if updated.rowcount != 1:
                    raise TaskLedgerError("execution card state changed")
                self._event(
                    connection,
                    card_id=card_id,
                    task_id=int(row["task_id"]),
                    kind="resolved",
                    card_version=version,
                    workflow_version=int(workflow.version),
                    action=_stored_action(action),
                    now=now,
                )
                connection.commit()
                return ExecutionCardOperationResult(
                    ExecutionCardDisposition.APPLIED,
                    card_id,
                    card_version=version,
                    card_status=ExecutionCardStatus.RESOLVED,
                    workflow_version=workflow.version,
                    workflow_status=workflow.status,
                    workflow_phase=workflow.phase,
                    wake_at=workflow.wake_at,
                )
            except Exception:
                connection.rollback()
                raise

    def submit_input(
        self,
        card_id: int,
        *,
        expected_version: int,
        kind: str,
        value: str,
    ) -> ExecutionCardOperationResult:
        """Apply one version-bound discussion or reassignment response."""
        if (
            not _valid_identity(card_id, expected_version)
            or kind not in READER_INPUT_KINDS
            or not _valid_reader_input(kind, value)
        ):
            return _refused(card_id, ExecutionCardRefusal.INVALID_ARGUMENT)
        stamp = self._clock_value()
        now = stamp.isoformat(timespec="seconds")
        with closing(self._connect()) as connection:
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = connection.execute(
                    self._card_select() + " WHERE c.id=?", (card_id,)
                ).fetchone()
                refusal = _card_guard(row, expected_version)
                if (
                    refusal is None
                    and row["status"] != ExecutionCardStatus.DELIVERED
                ):
                    refusal = ExecutionCardRefusal.INVALID_STATE
                if refusal is None and not _current_card(row):
                    refusal = ExecutionCardRefusal.STALE_VERSION
                if (
                    refusal is None
                    and ExecutionCardKind(row["kind"])
                    is ExecutionCardKind.START
                ):
                    refusal = ExecutionCardRefusal.INVALID_ACTION
                if (
                    refusal is None
                    and kind == "reassignment"
                    and row["owner"] == value
                ):
                    refusal = ExecutionCardRefusal.INVALID_STATE
                if refusal is not None:
                    connection.rollback()
                    return _refused_row(card_id, row, refusal)

                target_workflow_version = int(row["workflow_version"]) + 1
                connection.execute(
                    "INSERT INTO execution_reader_inputs("
                    "card_id,task_id,card_version,task_version,"
                    "workflow_version,target_workflow_version,kind,value,"
                    "prior_value,occurred_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
                    (
                        card_id,
                        int(row["task_id"]),
                        expected_version,
                        int(row["task_version"]),
                        int(row["workflow_version"]),
                        target_workflow_version,
                        kind,
                        value,
                        row["owner"] if kind == "reassignment" else None,
                        now,
                    ),
                )
                if kind == "discussion":
                    task_version = int(row["task_version"])
                    status = WorkflowStatus.QUEUED
                    phase = WorkflowPhase.PLAN
                    event_kind = "discussion_requested"
                    resolution = "discuss"
                    workflow_update = connection.execute(
                        "UPDATE task_execution_workflows SET "
                        "status='queued',phase='plan',version=?,due_at=NULL,"
                        "claim_token_digest=NULL,claimed_at=NULL,"
                        "claim_heartbeat_at=NULL,claim_expires_at=NULL,"
                        "failure_count=0,last_failure_reason=NULL,"
                        "last_failure_at=NULL,next_attempt_at=NULL,"
                        "parked_at=NULL,updated_at=?,completed_at=NULL "
                        "WHERE task_id=? AND version=?",
                        (
                            target_workflow_version,
                            now,
                            int(row["task_id"]),
                            int(row["workflow_version"]),
                        ),
                    )
                else:
                    task_version = int(row["task_version"]) + 1
                    status = WorkflowStatus.AWAITING_START
                    phase = WorkflowPhase.PLAN
                    event_kind = "reassigned"
                    resolution = "reassign"
                    task_update = connection.execute(
                        "UPDATE tasks SET owner=?,version=?,updated_at=? "
                        "WHERE id=? AND version=? AND status='open'",
                        (
                            value,
                            task_version,
                            now,
                            int(row["task_id"]),
                            int(row["task_version"]),
                        ),
                    )
                    if task_update.rowcount != 1:
                        raise TaskLedgerError("task ownership state changed")
                    connection.execute(
                        "INSERT INTO task_owner_events("
                        "task_id,task_version,card_id,from_owner,to_owner,"
                        "occurred_at) VALUES(?,?,?,?,?,?)",
                        (
                            int(row["task_id"]),
                            task_version,
                            card_id,
                            row["owner"],
                            value,
                            now,
                        ),
                    )
                    workflow_update = connection.execute(
                        "UPDATE task_execution_workflows SET task_version=?,"
                        "status='awaiting_start',phase='plan',version=?,"
                        "due_at=NULL,claim_token_digest=NULL,claimed_at=NULL,"
                        "claim_heartbeat_at=NULL,claim_expires_at=NULL,"
                        "failure_count=0,last_failure_reason=NULL,"
                        "last_failure_at=NULL,next_attempt_at=NULL,"
                        "parked_at=NULL,last_result_id=NULL,updated_at=?,"
                        "completed_at=NULL WHERE task_id=? AND version=?",
                        (
                            task_version,
                            target_workflow_version,
                            now,
                            int(row["task_id"]),
                            int(row["workflow_version"]),
                        ),
                    )

                if workflow_update.rowcount != 1:
                    raise TaskLedgerError("execution workflow state changed")

                TaskExecutionService._event(
                    connection,
                    int(row["task_id"]),
                    event_kind,
                    target_workflow_version,
                    task_version,
                    phase,
                    status,
                    now,
                )
                version = expected_version + 1
                updated = connection.execute(
                    "UPDATE execution_review_cards SET status='resolved',"
                    "version=?,resolution=?,resolved_at=?,updated_at=? "
                    "WHERE id=? AND version=? AND status='delivered'",
                    (
                        version,
                        resolution,
                        now,
                        now,
                        card_id,
                        expected_version,
                    ),
                )
                if updated.rowcount != 1:
                    raise TaskLedgerError("execution card state changed")
                self._event(
                    connection,
                    card_id=card_id,
                    task_id=int(row["task_id"]),
                    kind="resolved",
                    card_version=version,
                    workflow_version=target_workflow_version,
                    action=resolution,
                    now=now,
                )
                connection.commit()
                return ExecutionCardOperationResult(
                    ExecutionCardDisposition.APPLIED,
                    card_id,
                    card_version=version,
                    card_status=ExecutionCardStatus.RESOLVED,
                    workflow_version=target_workflow_version,
                    workflow_status=status,
                    workflow_phase=phase,
                )
            except Exception:
                connection.rollback()
                raise

    def stats(self) -> ExecutionCardStats:
        with closing(self._connect()) as connection:
            rows = connection.execute(
                "SELECT status,COUNT(*) AS total FROM execution_review_cards "
                "WHERE status IN ('pending','delivering','delivered') "
                "GROUP BY status"
            ).fetchall()
        counts = {row["status"]: int(row["total"]) for row in rows}
        return ExecutionCardStats(
            pending=counts.get("pending", 0),
            delivering=counts.get("delivering", 0),
            delivered=counts.get("delivered", 0),
            active=sum(counts.values()),
        )

    def count(self) -> int:
        with closing(self._connect()) as connection:
            return int(
                connection.execute(
                    "SELECT COUNT(*) FROM execution_review_cards"
                ).fetchone()[0]
            )

    def event_count(self) -> int:
        with closing(self._connect()) as connection:
            return int(
                connection.execute(
                    "SELECT COUNT(*) FROM execution_review_card_events"
                ).fetchone()[0]
            )

    def _cancel_stale(
        self, connection: sqlite3.Connection, now: str
    ) -> int:
        rows = connection.execute(
            self._card_select()
            + " WHERE c.status IN ('pending','delivering','delivered') "
            "ORDER BY c.id"
        ).fetchall()
        cancelled = 0
        for row in rows:
            if _current_card(row):
                continue
            version = int(row["version"]) + 1
            connection.execute(
                "UPDATE execution_review_cards SET status='cancelled',"
                "version=?,claim_token_digest=NULL,claim_expires_at=NULL,"
                "resolved_at=?,updated_at=? WHERE id=? AND version=?",
                (version, now, now, int(row["id"]), int(row["version"])),
            )
            self._event(
                connection,
                card_id=int(row["id"]),
                task_id=int(row["task_id"]),
                kind="cancelled",
                card_version=version,
                workflow_version=int(row["workflow_version"]),
                action=None,
                now=now,
            )
            cancelled += 1
        return cancelled

    @staticmethod
    def _card_select() -> str:
        return (
            "SELECT c.*,t.text AS task_text,t.owner,t.due,"
            "t.status AS task_status_current,t.version AS task_version_current,"
            "w.status AS workflow_status_current,"
            "w.phase AS workflow_phase_current,"
            "w.version AS workflow_version_current,"
            "w.task_version AS workflow_task_version_current,"
            "w.last_result_id AS workflow_result_id,"
            "r.task_id AS result_task_id,r.workflow_version AS result_version,"
            "r.task_version AS result_task_version,r.phase AS result_phase,"
            "r.outcome AS result_outcome,r.summary,r.work_markdown,"
            "r.questions_json,r.external_actions_json,r.deliverables_json "
            "FROM execution_review_cards AS c "
            "JOIN tasks AS t ON t.id=c.task_id "
            "JOIN task_execution_workflows AS w ON w.task_id=c.task_id "
            "LEFT JOIN task_execution_results AS r ON r.result_id=c.result_id"
        )

    @staticmethod
    def _event(
        connection: sqlite3.Connection,
        *,
        card_id: int,
        task_id: int,
        kind: str,
        card_version: int,
        workflow_version: int,
        action: str | None,
        now: str,
    ) -> None:
        connection.execute(
            "INSERT INTO execution_review_card_events("
            "card_id,task_id,kind,card_version,workflow_version,action,"
            "occurred_at) VALUES(?,?,?,?,?,?,?)",
            (
                card_id,
                task_id,
                kind,
                card_version,
                workflow_version,
                action,
                now,
            ),
        )

    def _connect(self) -> sqlite3.Connection:
        if not self.database_path.is_file() or self.database_path.is_symlink():
            raise TaskLedgerError("execution card database is not initialized")
        connection = sqlite3.connect(self.database_path, timeout=5)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout = 5000")
        version = int(connection.execute("PRAGMA user_version").fetchone()[0])
        if version != SCHEMA_VERSION:
            connection.close()
            raise TaskLedgerError(
                "execution card database schema is not supported"
            )
        try:
            CandidateInbox._require_schema(connection)
        except InboxError as exc:
            connection.close()
            raise TaskLedgerError(
                "execution card database schema is incomplete"
            ) from exc
        return connection

    def _clock_value(self) -> datetime:
        value = self._clock()
        if value.tzinfo is None or value.utcoffset() is None:
            raise TaskLedgerError(
                "execution card clock must include a timezone"
            )
        return value.astimezone(timezone.utc)

    def _now(self) -> str:
        return self._clock_value().isoformat(timespec="seconds")


def render_execution_review_card(
    card: ExecutionReviewCard,
) -> tuple[str, dict[str, list[list[dict[str, str]]]]]:
    if not isinstance(card, ExecutionReviewCard):
        raise TaskLedgerError("execution review card is invalid")
    plain = "\n".join(_card_lines(card))
    complete = "\n".join(_html_card_lines(card))
    approvable = len(complete.encode("utf-8")) <= MAX_CARD_BODY_BYTES
    body = (
        complete
        if approvable
        else _escape_bounded(
            plain,
            MAX_TRUNCATED_CARD_BODY_BYTES,
            suffix=(
                "\n\nContent is too long to approve in this card. "
                "Approval is disabled."
            ),
        )
    )
    keyboard = {
        "inline_keyboard": [
            [
                {
                    "text": label,
                    "callback_data": _callback(
                        card.id, card.version, action
                    ),
                }
                for label, action in row
            ]
            for row in _button_rows(card.kind, approvable=approvable)
        ]
    }
    return body, keyboard


def parse_execution_review_callback(
    value: object,
) -> tuple[int, int, str] | None:
    if not isinstance(value, str) or not 1 <= len(value) <= CALLBACK_DATA_LIMIT:
        return None
    parts = value.split("|")
    if len(parts) != 4 or parts[0] != CALLBACK_PREFIX:
        return None
    try:
        card_id = int(parts[1])
        version = int(parts[2])
    except ValueError:
        return None
    if not _valid_identity(card_id, version):
        return None
    if parts[1] != str(card_id) or parts[2] != str(version):
        return None
    if parts[3] not in {
        "start", "snooze", "cancel", "approve", "revise", "discuss",
        "done", "reassign", "drop", *REVIEW_SNOOZE_INTERVALS,
    }:
        return None
    return card_id, version, parts[3]


def _kind_for_workflow(row: Mapping[str, object]) -> ExecutionCardKind:
    if (
        row["status"] in {WorkflowStatus.AWAITING_START, WorkflowStatus.SNOOZED}
        and row["last_result_id"] is None
    ):
        return ExecutionCardKind.START
    if (
        row["status"] in {WorkflowStatus.AWAITING_REVIEW, WorkflowStatus.SNOOZED}
        and row["phase"] == WorkflowPhase.PLAN
        and row["outcome"] == ExecutionOutcome.AWAITING_PLAN
    ):
        return ExecutionCardKind.PLAN_REVIEW
    if (
        row["status"] in {WorkflowStatus.AWAITING_REVIEW, WorkflowStatus.SNOOZED}
        and row["phase"] == WorkflowPhase.EXECUTE
        and row["outcome"] == ExecutionOutcome.AWAITING_EXTERNAL
    ):
        return ExecutionCardKind.EXTERNAL_REVIEW
    if (
        row["status"] in {
            WorkflowStatus.AWAITING_REVIEW,
            WorkflowStatus.COMPLETED,
            WorkflowStatus.SNOOZED,
        }
        and row["outcome"] in {
            ExecutionOutcome.COMPLETED,
            ExecutionOutcome.DECLINED,
            ExecutionOutcome.INELIGIBLE,
        }
    ):
        return ExecutionCardKind.RESULT_REVIEW
    raise TaskLedgerError("execution workflow cannot be rendered as a card")


def _current_card(row: Mapping[str, object]) -> bool:
    try:
        kind = ExecutionCardKind(row["kind"])
        if (
            row["task_status_current"] != TaskStatus.OPEN
            or int(row["task_version_current"]) != int(row["task_version"])
            or int(row["workflow_task_version_current"])
            != int(row["task_version"])
            or int(row["workflow_version_current"])
            != int(row["workflow_version"])
            or row["workflow_phase_current"] != row["phase"]
        ):
            return False
        if kind is ExecutionCardKind.START:
            return (
                row["result_id"] is None
                and row["workflow_status_current"]
                in {WorkflowStatus.AWAITING_START, WorkflowStatus.SNOOZED}
            )
        expected = {
            ExecutionCardKind.PLAN_REVIEW: {ExecutionOutcome.AWAITING_PLAN},
            ExecutionCardKind.EXTERNAL_REVIEW: {
                ExecutionOutcome.AWAITING_EXTERNAL
            },
            ExecutionCardKind.RESULT_REVIEW: {
                ExecutionOutcome.COMPLETED,
                ExecutionOutcome.DECLINED,
                ExecutionOutcome.INELIGIBLE,
            },
        }[kind]
        statuses = {
            WorkflowStatus.AWAITING_REVIEW,
            WorkflowStatus.SNOOZED,
        }
        if kind is ExecutionCardKind.RESULT_REVIEW:
            statuses.add(WorkflowStatus.COMPLETED)
        return (
            row["workflow_status_current"] in statuses
            and row["workflow_result_id"] == row["result_id"]
            and row["result_task_id"] == row["task_id"]
            and row["result_task_version"] == row["task_version"]
            and int(row["result_version"]) < int(row["workflow_version"])
            and row["result_phase"] == row["phase"]
            and row["result_outcome"] in expected
        )
    except (KeyError, TypeError, ValueError):
        return False


def _card(row: Mapping[str, object]) -> ExecutionReviewCard:
    try:
        return ExecutionReviewCard(
            id=int(row["id"]),
            task_id=int(row["task_id"]),
            task_version=int(row["task_version"]),
            workflow_version=int(row["workflow_version"]),
            kind=ExecutionCardKind(row["kind"]),
            phase=WorkflowPhase(row["phase"]),
            result_id=row["result_id"],
            status=ExecutionCardStatus(row["status"]),
            version=int(row["version"]),
            created_at=str(row["created_at"]),
            task_text=str(row["task_text"]),
            owner=row["owner"],
            due=row["due"],
            summary="" if row["summary"] is None else str(row["summary"]),
            work_markdown=(
                ""
                if row["work_markdown"] is None
                else str(row["work_markdown"])
            ),
            questions=_stored_collection(row["questions_json"]),
            external_actions=_stored_collection(row["external_actions_json"]),
            deliverables=_stored_collection(row["deliverables_json"]),
            outcome=(
                None
                if row["result_outcome"] is None
                else ExecutionOutcome(row["result_outcome"])
            ),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise TaskLedgerError("execution review card state is invalid") from exc


def _stored_collection(value: object) -> tuple[str, ...]:
    if value is None:
        return ()
    try:
        parsed = json.loads(value)
    except (TypeError, ValueError):
        raise TaskLedgerError("execution review card result is invalid") from None
    if not isinstance(parsed, list) or any(
        not isinstance(item, str) for item in parsed
    ):
        raise TaskLedgerError("execution review card result is invalid")
    return tuple(parsed)


def _card_lines(card: ExecutionReviewCard) -> list[str]:
    details = [f"Task: {card.task_text}"]
    if card.owner:
        details.append(f"Owner: {card.owner}")
    if card.due:
        details.append(f"Due: {card.due}")
    if card.kind is ExecutionCardKind.START:
        return [
            "Foxhound execution request",
            "",
            *details,
            "",
            "Start the planning phase? No task work or external action has run.",
        ]
    if card.kind is ExecutionCardKind.EXTERNAL_REVIEW:
        lines = [
            "Foxhound external-action approval",
            "",
            *details,
            "",
            "Requested external actions:",
            *_listed(card.external_actions, empty="None supplied."),
            "",
            "Approve only if these exact external effects are intended.",
            "",
            f"Summary: {card.summary}",
        ]
        if card.questions:
            lines.extend(("", "Questions:", *_listed(card.questions)))
        return lines
    if card.kind is ExecutionCardKind.RESULT_REVIEW:
        lines = [
            "Foxhound result review",
            "",
            *details,
            "",
            f"Outcome: {card.outcome}",
            f"Summary: {card.summary}",
        ]
        if card.questions:
            lines.extend(("", "Questions:", *_listed(card.questions)))
        if card.deliverables:
            lines.extend(("", "Deliverables:", *_listed(card.deliverables)))
        if card.work_markdown:
            lines.extend(("", "Work:", card.work_markdown))
        return lines
    lines = [
        "Foxhound plan review",
        "",
        *details,
        "",
        f"Summary: {card.summary}",
    ]
    if card.questions:
        lines.extend(("", "Questions:", *_listed(card.questions)))
    if card.external_actions:
        lines.extend((
            "",
            "Potential external actions (not yet authorized):",
            *_listed(card.external_actions),
        ))
    if card.deliverables:
        lines.extend(("", "Deliverables:", *_listed(card.deliverables)))
    if card.work_markdown:
        lines.extend(("", "Plan:", card.work_markdown))
    return lines


def _listed(values: Sequence[str], *, empty: str = "None.") -> list[str]:
    return [f"- {value}" for value in values] if values else [empty]


def _html_card_lines(card: ExecutionReviewCard) -> list[str]:
    details = _labelled_html_lines("Task", card.task_text)
    if card.owner:
        details.extend(_labelled_html_lines("Owner", card.owner))
    if card.due:
        details.extend(_labelled_html_lines("Due", card.due))
    if card.kind is ExecutionCardKind.START:
        return [
            "<b>Foxhound execution request</b>",
            "",
            *details,
            "",
            "Start the planning phase? No task work or external action has run.",
        ]
    if card.kind is ExecutionCardKind.EXTERNAL_REVIEW:
        lines = [
            "<b>Foxhound external-action approval</b>",
            "",
            *details,
            "",
            "<b>Requested external actions:</b>",
            *_html_listed(card.external_actions, empty="None supplied."),
            "",
            "Approve only if these exact external effects are intended.",
            "",
            *_labelled_html_lines("Summary", card.summary),
        ]
        if card.questions:
            lines.extend((
                "",
                "<b>Questions:</b>",
                *_html_listed(card.questions),
            ))
        return lines
    if card.kind is ExecutionCardKind.RESULT_REVIEW:
        lines = [
            "<b>Foxhound result review</b>",
            "",
            *details,
            "",
            *_labelled_html_lines("Outcome", str(card.outcome)),
            *_labelled_html_lines("Summary", card.summary),
        ]
        if card.questions:
            lines.extend(("", "<b>Questions:</b>", *_html_listed(card.questions)))
        if card.deliverables:
            lines.extend(("", "<b>Deliverables:</b>", *_html_listed(card.deliverables)))
        if card.work_markdown:
            lines.extend(("", "<b>Work:</b>", *_markdown_lines(card.work_markdown)))
        return lines
    lines = [
        "<b>Foxhound plan review</b>",
        "",
        *details,
        "",
        *_labelled_html_lines("Summary", card.summary),
    ]
    if card.questions:
        lines.extend((
            "",
            "<b>Questions:</b>",
            *_html_listed(card.questions),
        ))
    if card.external_actions:
        lines.extend((
            "",
            "<b>Potential external actions (not yet authorized):</b>",
            *_html_listed(card.external_actions),
        ))
    if card.deliverables:
        lines.extend((
            "",
            "<b>Deliverables:</b>",
            *_html_listed(card.deliverables),
        ))
    if card.work_markdown:
        lines.extend(("", "<b>Plan:</b>", *_markdown_lines(card.work_markdown)))
    return lines


def _html_listed(
    values: Sequence[str], *, empty: str = "None."
) -> list[str]:
    if not values:
        return [empty]
    lines: list[str] = []
    for value in values:
        segments = _escaped_source_lines(value)
        lines.append(f"• {segments[0]}")
        lines.extend(f"  {segment}" for segment in segments[1:])
    return lines


def _labelled_html_lines(label: str, value: str) -> list[str]:
    segments = _escaped_source_lines(value)
    return [f"<b>{label}:</b> {segments[0]}", *segments[1:]]


def _escaped_source_lines(value: str) -> list[str]:
    return [
        _escape(segment)
        for line in value.split("\n")
        for segment in _source_line_segments(line)
    ]


def _source_line_segments(value: str) -> list[str]:
    if not value:
        return [""]
    return [
        value[offset:offset + MAX_RENDER_SOURCE_LINE_CHARS]
        for offset in range(0, len(value), MAX_RENDER_SOURCE_LINE_CHARS)
    ]


def _markdown_lines(value: str) -> list[str]:
    lines: list[str] = []
    for raw in value.split("\n"):
        for index, line in enumerate(_source_line_segments(raw.rstrip())):
            heading = re.match(r"^(#{1,6})\s+(.*)$", line) if not index else None
            bullet = re.match(r"^\s*[-*+]\s+(.*)$", line) if not index else None
            ordered = (
                re.match(r"^\s*(\d+[.)])\s+(.*)$", line)
                if not index
                else None
            )
            if heading:
                lines.append(f"<b>{_markdown_inline(heading.group(2))}</b>")
            elif bullet:
                lines.append(f"• {_markdown_inline(bullet.group(1))}")
            elif ordered:
                lines.append(
                    f"{_escape(ordered.group(1))} "
                    f"{_markdown_inline(ordered.group(2))}"
                )
            else:
                lines.append(_markdown_inline(line))
    return lines


def _markdown_inline(value: str) -> str:
    fragments: list[str] = []

    def stash(fragment: str) -> str:
        fragments.append(fragment)
        return f"\x00{len(fragments) - 1}\x00"

    value = re.sub(
        r"`([^`\n]+)`",
        lambda match: stash(f"<code>{_escape(match.group(1))}</code>"),
        value,
    )

    def link(match: re.Match[str]) -> str:
        target = match.group(2)
        if not _safe_link(target):
            return stash(_escape(match.group(0)))
        return stash(
            f'<a href="{html.escape(target, quote=True)}">'
            f"{_escape(match.group(1))}</a>"
        )

    value = re.sub(r"\[([^\]\n]+)\]\(([^)\s]+)\)", link, value)
    value = _escape(value)
    value = re.sub(
        r"\*\*([^*\n]+)\*\*|__([^_\n]+)__",
        lambda match: f"<b>{match.group(1) or match.group(2)}</b>",
        value,
    )
    value = re.sub(
        r"(?<!\*)\*(?!\s)([^*\n]+?)\*(?!\*)",
        lambda match: f"<i>{match.group(1)}</i>",
        value,
    )
    return re.sub(
        r"\x00(\d+)\x00",
        lambda match: fragments[int(match.group(1))],
        value,
    )


def _safe_link(value: str) -> bool:
    if (
        not value
        or len(value) > 2_048
        or any(character.isspace() or ord(character) < 32 for character in value)
    ):
        return False
    try:
        parsed = urllib.parse.urlsplit(value)
        port = parsed.port
    except ValueError:
        return False
    return (
        parsed.scheme in {"http", "https"}
        and parsed.hostname is not None
        and parsed.username is None
        and parsed.password is None
        and (port is None or 1 <= port <= 65_535)
    )


def _escape(value: str) -> str:
    return html.escape(value, quote=False)


def _button_rows(
    kind: ExecutionCardKind, *, approvable: bool
) -> tuple[tuple[tuple[str, str], ...], ...]:
    if kind is ExecutionCardKind.START:
        rows = (
            (("▶️ Start planning", "start"),),
            (("🕒 Snooze 24h", "snooze"),),
            (("⛔ Cancel workflow", "cancel"),),
        )
        return rows if approvable else rows[1:]
    if kind is ExecutionCardKind.EXTERNAL_REVIEW:
        rows = (
            (("✅ Authorize action", "approve"), ("⛔ Not now", "revise")),
            (("🕒 Snooze", "snooze"),),
            (("💬 Discuss", "discuss"), ("✅ Mark as done", "done")),
            (("👥 Reassign", "reassign"), ("🗑 Drop task", "drop")),
        )
    elif kind is ExecutionCardKind.RESULT_REVIEW:
        rows = (
            (("✅ Mark as done", "done"),),
            (("💬 Discuss", "discuss"), ("🕒 Snooze", "snooze")),
            (("👥 Reassign", "reassign"), ("🗑 Drop task", "drop")),
        )
    else:
        rows = (
            (("🔎 Investigate further", "revise"), ("💬 Discuss", "discuss")),
            (("▶️ Execute plan", "approve"), ("🕒 Snooze", "snooze")),
            (("✅ Mark as done", "done"),),
            (("👥 Reassign", "reassign"), ("🗑 Drop task", "drop")),
        )
    if approvable:
        return rows
    return tuple(
        tuple(button for button in row if button[1] not in {"approve", "done"})
        for row in rows
        if any(button[1] not in {"approve", "done"} for button in row)
    )


def _direct_actions_for_kind(kind: ExecutionCardKind) -> set[str]:
    if kind is ExecutionCardKind.START:
        return {"start", "snooze", "cancel"}
    if kind is ExecutionCardKind.RESULT_REVIEW:
        return {"done", "drop", *REVIEW_SNOOZE_INTERVALS}
    return {
        "approve", "revise", "cancel", "done", "drop",
        *REVIEW_SNOOZE_INTERVALS,
    }


def _stored_action(action: str) -> str:
    return "snooze" if action in REVIEW_SNOOZE_INTERVALS else action


def _valid_reader_input(kind: str, value: object) -> bool:
    maximum = MAX_DISCUSSION_CHARS if kind == "discussion" else MAX_OWNER_CHARS
    return (
        isinstance(value, str)
        and value == value.strip()
        and 1 <= len(value) <= maximum
        and not any(ord(character) == 0 for character in value)
        and (kind != "reassignment" or "\n" not in value)
    )


def _apply_review_lifecycle_action(
    connection: sqlite3.Connection,
    row: Mapping[str, object],
    *,
    action: str,
    now: str,
) -> WorkflowOperationResult:
    transition = _apply_task_transition(
        connection,
        task_id=int(row["task_id"]),
        expected_version=int(row["task_version"]),
        action=action,
        now=now,
    )
    if transition.disposition is TransitionDisposition.REFUSED:
        return WorkflowOperationResult(
            WorkflowDisposition.REFUSED,
            int(row["task_id"]),
            refusal=WorkflowRefusal.STALE_TASK,
        )
    workflow_status = (
        WorkflowStatus.COMPLETED if action == "done" else WorkflowStatus.CANCELLED
    )
    workflow_version = int(row["workflow_version"]) + 1
    phase = WorkflowPhase(row["phase"])
    updated = connection.execute(
        "UPDATE task_execution_workflows SET task_version=?,status=?,"
        "version=?,due_at=NULL,claim_token_digest=NULL,claimed_at=NULL,"
        "claim_heartbeat_at=NULL,claim_expires_at=NULL,failure_count=0,"
        "last_failure_reason=NULL,last_failure_at=NULL,next_attempt_at=NULL,"
        "parked_at=NULL,updated_at=?,completed_at=? "
        "WHERE task_id=? AND version=?",
        (
            int(transition.version),
            workflow_status,
            workflow_version,
            now,
            now,
            int(row["task_id"]),
            int(row["workflow_version"]),
        ),
    )
    if updated.rowcount != 1:
        return WorkflowOperationResult(
            WorkflowDisposition.REFUSED,
            int(row["task_id"]),
            refusal=WorkflowRefusal.STALE_WORKFLOW,
        )
    TaskExecutionService._event(
        connection,
        int(row["task_id"]),
        "task_completed" if action == "done" else "task_dropped",
        workflow_version,
        int(transition.version),
        phase,
        workflow_status,
        now,
    )
    return WorkflowOperationResult(
        WorkflowDisposition.APPLIED,
        int(row["task_id"]),
        workflow_version,
        workflow_status,
        phase,
    )


def _callback(card_id: int, version: int, action: str) -> str:
    value = f"{CALLBACK_PREFIX}|{card_id}|{version}|{action}"
    if len(value.encode("utf-8")) > CALLBACK_DATA_LIMIT:
        raise TaskLedgerError("execution review callback is too large")
    return value


def _card_fits(card: ExecutionReviewCard) -> bool:
    return len("\n".join(_html_card_lines(card)).encode("utf-8")) <= (
        MAX_CARD_BODY_BYTES
    )


def _escape_bounded(value: str, maximum: int, *, suffix: str) -> str:
    encoded = html.escape(value, quote=False).encode("utf-8")
    if len(encoded) <= maximum:
        return encoded.decode("utf-8")
    suffix_bytes = ("\n…" + suffix).encode("utf-8")
    limit = maximum - len(suffix_bytes)
    fragments: list[str] = []
    size = 0
    for character in value:
        escaped = html.escape(character, quote=False)
        width = len(escaped.encode("utf-8"))
        if size + width > limit:
            break
        fragments.append(escaped)
        size += width
    return "".join(fragments) + suffix_bytes.decode("utf-8")


def _card_guard(
    row: Mapping[str, object] | None, expected_version: int
) -> ExecutionCardRefusal | None:
    if row is None:
        return ExecutionCardRefusal.NOT_FOUND
    if int(row["version"]) != expected_version:
        return ExecutionCardRefusal.STALE_VERSION
    return None


def _workflow_refusal(
    refusal: WorkflowRefusal | None,
) -> ExecutionCardRefusal:
    if refusal in {WorkflowRefusal.STALE_TASK, WorkflowRefusal.STALE_WORKFLOW}:
        return ExecutionCardRefusal.STALE_VERSION
    return ExecutionCardRefusal.INVALID_STATE


def _operation(
    row: Mapping[str, object], disposition: ExecutionCardDisposition
) -> ExecutionCardOperationResult:
    return ExecutionCardOperationResult(
        disposition,
        int(row["id"]),
        card_version=int(row["version"]),
        card_status=ExecutionCardStatus(row["status"]),
        workflow_version=int(row["workflow_version"]),
    )


def _refused(
    card_id: object, refusal: ExecutionCardRefusal
) -> ExecutionCardOperationResult:
    return ExecutionCardOperationResult(
        ExecutionCardDisposition.REFUSED,
        card_id if isinstance(card_id, int) and not isinstance(card_id, bool) else 0,
        refusal=refusal,
    )


def _refused_row(
    card_id: int,
    row: Mapping[str, object] | None,
    refusal: ExecutionCardRefusal,
) -> ExecutionCardOperationResult:
    if row is None:
        return _refused(card_id, refusal)
    return ExecutionCardOperationResult(
        ExecutionCardDisposition.REFUSED,
        card_id,
        card_version=int(row["version"]),
        card_status=ExecutionCardStatus(row["status"]),
        workflow_version=int(row["workflow_version"]),
        refusal=refusal,
    )


def _valid_identity(card_id: object, version: object) -> bool:
    return all(
        isinstance(value, int) and not isinstance(value, bool) and value > 0
        for value in (card_id, version)
    )


def _valid_limit(value: object) -> bool:
    return (
        isinstance(value, int)
        and not isinstance(value, bool)
        and 1 <= value <= 1_000
    )


def _valid_lease(value: object) -> bool:
    return (
        isinstance(value, int)
        and not isinstance(value, bool)
        and 5 <= value <= 300
    )


def _valid_secret(value: object) -> bool:
    return (
        isinstance(value, str)
        and 32 <= len(value) <= 512
        and not any(character.isspace() for character in value)
    )


def _valid_opaque(value: object, maximum: int) -> bool:
    return (
        isinstance(value, str)
        and 1 <= len(value) <= maximum
        and value == value.strip()
        and not any(ord(character) < 32 or ord(character) == 127 for character in value)
    )


def _token_digest(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()
