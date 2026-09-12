"""Transport-neutral reader cards for Foxhound execution gates."""

from __future__ import annotations

import hashlib
import html
import json
import os
import secrets
import sqlite3
from contextlib import closing
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import StrEnum
from pathlib import Path
from typing import Callable, Mapping, Sequence

from .candidate_inbox import CandidateInbox, InboxError, SCHEMA_VERSION
from .task_execution import (
    ExecutionOutcome,
    WorkflowDisposition,
    WorkflowPhase,
    WorkflowRefusal,
    WorkflowStatus,
    _apply_review_action,
    _apply_start_action,
)
from .task_ledger import TaskLedgerError, TaskStatus


CALLBACK_PREFIX = "fhe"
CALLBACK_DATA_LIMIT = 64
MAX_CARD_BODY_BYTES = 3_500
ACTIVE_STATUSES = ("pending", "delivering", "delivered")


class ExecutionCardKind(StrEnum):
    START = "start"
    PLAN_REVIEW = "plan_review"
    EXTERNAL_REVIEW = "external_review"


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
                    "  AND r.outcome='awaiting_external')"
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

    def act(
        self, card_id: int, *, expected_version: int, action: str
    ) -> ExecutionCardOperationResult:
        if not _valid_identity(card_id, expected_version):
            return _refused(card_id, ExecutionCardRefusal.INVALID_ARGUMENT)
        if action not in {"start", "snooze", "cancel", "approve", "revise"}:
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
                if refusal is None and action not in _actions_for_kind(
                    ExecutionCardKind(row["kind"])
                ):
                    refusal = ExecutionCardRefusal.INVALID_ACTION
                if (
                    refusal is None
                    and action in {"start", "approve"}
                    and not _card_fits(_card(row))
                ):
                    refusal = ExecutionCardRefusal.INVALID_STATE
                if refusal is not None:
                    connection.rollback()
                    return _refused_row(card_id, row, refusal)

                if row["kind"] == ExecutionCardKind.START:
                    workflow = _apply_start_action(
                        connection,
                        int(row["task_id"]),
                        expected_version=int(row["workflow_version"]),
                        action=action,
                        stamp=stamp,
                    )
                else:
                    workflow = _apply_review_action(
                        connection,
                        int(row["task_id"]),
                        expected_version=int(row["workflow_version"]),
                        action=action,
                        now=now,
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
                    (version, action, now, now, card_id, expected_version),
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
                    action=action,
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
    approvable = _card_fits(card)
    body = _escape_bounded(
        plain,
        MAX_CARD_BODY_BYTES,
        suffix=(
            "\n\nContent is too long to approve in this card. "
            "Approval is disabled."
        ),
    )
    keyboard = {
        "inline_keyboard": [[
            {
                "text": label,
                "callback_data": _callback(card.id, card.version, action),
            }
            for label, action in _buttons(card.kind, approvable=approvable)
        ]]
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
    if parts[3] not in {"start", "snooze", "cancel", "approve", "revise"}:
        return None
    return card_id, version, parts[3]


def _kind_for_workflow(row: Mapping[str, object]) -> ExecutionCardKind:
    if row["status"] in {WorkflowStatus.AWAITING_START, WorkflowStatus.SNOOZED}:
        return ExecutionCardKind.START
    if (
        row["status"] == WorkflowStatus.AWAITING_REVIEW
        and row["phase"] == WorkflowPhase.PLAN
        and row["outcome"] == ExecutionOutcome.AWAITING_PLAN
    ):
        return ExecutionCardKind.PLAN_REVIEW
    if (
        row["status"] == WorkflowStatus.AWAITING_REVIEW
        and row["phase"] == WorkflowPhase.EXECUTE
        and row["outcome"] == ExecutionOutcome.AWAITING_EXTERNAL
    ):
        return ExecutionCardKind.EXTERNAL_REVIEW
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
        expected = (
            ExecutionOutcome.AWAITING_PLAN
            if kind is ExecutionCardKind.PLAN_REVIEW
            else ExecutionOutcome.AWAITING_EXTERNAL
        )
        return (
            row["workflow_status_current"] == WorkflowStatus.AWAITING_REVIEW
            and row["workflow_result_id"] == row["result_id"]
            and row["result_task_id"] == row["task_id"]
            and row["result_task_version"] == row["task_version"]
            and int(row["result_version"]) + 1
            == int(row["workflow_version"])
            and row["result_phase"] == row["phase"]
            and row["result_outcome"] == expected
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


def _buttons(
    kind: ExecutionCardKind, *, approvable: bool
) -> tuple[tuple[str, str], ...]:
    if kind is ExecutionCardKind.START:
        safe = (
            ("Snooze 1 day", "snooze"),
            ("Cancel", "cancel"),
        )
        return (("Start planning", "start"), *safe) if approvable else safe
    if kind is ExecutionCardKind.EXTERNAL_REVIEW:
        safe = (
            ("Revise", "revise"),
            ("Cancel", "cancel"),
        )
        return (("Approve action", "approve"), *safe) if approvable else safe
    safe = (
        ("Revise", "revise"),
        ("Cancel", "cancel"),
    )
    return (("Approve plan", "approve"), *safe) if approvable else safe


def _actions_for_kind(kind: ExecutionCardKind) -> set[str]:
    if kind is ExecutionCardKind.START:
        return {"start", "snooze", "cancel"}
    return {"approve", "revise", "cancel"}


def _callback(card_id: int, version: int, action: str) -> str:
    value = f"{CALLBACK_PREFIX}|{card_id}|{version}|{action}"
    if len(value.encode("utf-8")) > CALLBACK_DATA_LIMIT:
        raise TaskLedgerError("execution review callback is too large")
    return value


def _card_fits(card: ExecutionReviewCard) -> bool:
    return len(
        html.escape("\n".join(_card_lines(card)), quote=False).encode("utf-8")
    ) <= MAX_CARD_BODY_BYTES


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
