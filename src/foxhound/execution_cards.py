"""Transport-neutral reader cards for Foxhound execution gates."""

from __future__ import annotations

import hashlib
import html
import json
import os
import re
import secrets
import sqlite3
import unicodedata
import urllib.parse
from contextlib import closing
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import StrEnum
from pathlib import Path
from typing import Callable, Mapping, Sequence

from .agent_profiles import (
    AgentProfile,
    AgentProfileError,
    AgentProfileRegistry,
    load_registry,
)
from .candidate_inbox import CandidateInbox, InboxError, SCHEMA_VERSION
from .card_provenance import (
    ADDRESSABLE_ORIGINS,
    CardSourceEvidence,
    origin_lines as shared_origin_lines,
    stored_origin_sources,
)
from .knowledge_client import KnowledgeClientError, OwnerUpcomingMeeting
from .task_execution import (
    ExecutionOutcome,
    REVIEW_SNOOZE_INTERVALS,
    TaskExecutionService,
    WorkflowDisposition,
    WorkflowOperationResult,
    WorkflowPhase,
    WorkflowRefusal,
    WorkflowStatus,
    _apply_agent_selection,
    _apply_review_action,
    _apply_start_action,
)
from .task_ledger import (
    TaskLedgerError,
    TaskStatus,
    TransitionDisposition,
    _apply_task_transition,
)
from .task_archive import review_links
from .task_owner import canonical_owner_display


CALLBACK_PREFIX = "fhe"
AGENT_CALLBACK_PREFIX = "fha"
CALLBACK_DATA_LIMIT = 64
AGENT_SELECTION_TOKEN_CHARS = 20
MAX_REVISION_NOTE_CHARS = 400
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
OWNER_HOLD_INTERVAL = timedelta(days=21)
MAX_OWNER_HOLD_CHECKS = 100
OWNER_HOLD_ACTION = "until_meeting"
_OWNER_HOLD_SELECT = (
    "SELECT h.*,t.status AS task_status_current,"
    "t.version AS task_version_current,t.owner AS owner_current,"
    "t.owner_ref_version AS owner_ref_version_current,"
    "t.owner_kind AS owner_kind_current,"
    "t.owner_speaker_id AS owner_speaker_id_current,"
    "t.owner_canonical_speaker_id AS owner_canonical_speaker_id_current,"
    "t.owner_speaker_registry_id AS owner_speaker_registry_id_current,"
    "t.owner_pinned AS owner_pinned_current,"
    "t.owner_provisional AS owner_provisional_current,"
    "w.status AS workflow_status_current,"
    "w.version AS workflow_version_current,"
    "w.task_version AS workflow_task_version_current,"
    "w.due_at AS workflow_due_at "
    "FROM task_execution_owner_holds AS h "
    "JOIN tasks AS t ON t.id=h.task_id "
    "JOIN task_execution_workflows AS w ON w.task_id=h.task_id"
)


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
    workflow_status: WorkflowStatus
    agent_profile_id: str
    agent_profile_revision: str = field(repr=False)
    agent_display_name: str = field(repr=False)
    task_text: str = field(repr=False)
    owner: str | None = field(repr=False)
    due: str | None = field(repr=False)
    first_raised: str | None = field(repr=False)
    last_mentioned: str | None = field(repr=False)
    owner_hold_eligible: bool = field(default=False, repr=False)
    summary: str = field(default="", repr=False)
    work_markdown: str = field(default="", repr=False)
    questions: tuple[str, ...] = field(default=(), repr=False)
    external_actions: tuple[CardRecord, ...] = field(default=(), repr=False)
    deliverables: tuple[CardRecord, ...] = field(default=(), repr=False)
    revisions: int = 0
    revision_note: str = field(default="", repr=False)
    origin_kind: str = field(default="", repr=False)
    origin_record: str = field(default="", repr=False)
    origin_item: str = field(default="", repr=False)
    prior_task_id: int | None = None
    #: How a parked workflow got there. A reader told only that nothing
    #: happened cannot tell a task nobody reached from one that was
    #: abandoned.
    failure_count: int = 0
    failure_reason: str = field(default="", repr=False)
    task_work_directory: str = field(default="", repr=False)
    task_kb_file: str = field(default="", repr=False)
    origin_sources: tuple["CardSourceEvidence", ...] = field(
        default=(), repr=False
    )
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


@dataclass(frozen=True)
class ExecutionAgentOption:
    display_name: str = field(repr=False)
    selection_token: str = field(repr=False)
    selected: bool = False


@dataclass(frozen=True)
class ExecutionAgentSelectorResult:
    disposition: ExecutionCardDisposition
    card_id: int
    card_version: int | None = None
    card: ExecutionReviewCard | None = field(default=None, repr=False)
    options: tuple[ExecutionAgentOption, ...] = ()
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
        profile_registry: AgentProfileRegistry | None = None,
        owner_condition: Callable[
            [str, Mapping[str, object]], OwnerUpcomingMeeting
        ] | None = None,
        reader_aliases: Sequence[str] = (),
    ) -> None:
        self.database_path = Path(database_path)
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._token_factory = token_factory or (
            lambda: secrets.token_urlsafe(32)
        )
        registry = profile_registry or load_registry()
        if not isinstance(registry, AgentProfileRegistry):
            raise ValueError("agent profile registry is invalid")
        self._profile_registry = registry
        if owner_condition is not None and not callable(owner_condition):
            raise ValueError("owner meeting condition is invalid")
        aliases = tuple(reader_aliases)
        if any(
            not isinstance(alias, str)
            or not alias
            or alias != alias.strip()
            or len(alias) > MAX_OWNER_CHARS
            for alias in aliases
        ):
            raise ValueError("reader aliases are invalid")
        self._owner_condition = owner_condition
        self._reader_aliases = frozenset(
            _normalized_owner(alias) for alias in aliases
        )

    def initialize(self) -> None:
        CandidateInbox(self.database_path, clock=self._clock).initialize()

    def schedule(self, *, limit: int = 100) -> ExecutionCardScheduleResult:
        if not _valid_limit(limit):
            return ExecutionCardScheduleResult(
                ExecutionCardDisposition.REFUSED,
                refusal=ExecutionCardRefusal.INVALID_ARGUMENT,
            )
        stamp = self._clock_value()
        self._poll_owner_holds(stamp, limit=min(limit, MAX_OWNER_HOLD_CHECKS))
        now = stamp.isoformat(timespec="seconds")
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
                    # `parked` is included deliberately. It means the
                    # agent gave up after repeated failure, and that is
                    # exactly when a reader needs telling — it used to be
                    # terminal and silent, so a task sat open forever with
                    # its workflow quietly abandoned and no card anywhere.
                    # No "is anything else busy" condition. It used to
                    # suppress every gate whenever ANY workflow anywhere was
                    # queued, running or awaiting review — one machine had
                    # 143 tasks invisible behind seven in flight, with a
                    # free card surface and nothing to show on it. How many
                    # cards a reader sees at once is the drip's business,
                    # and it already bounds that; a gate is how work gets
                    # queued in the first place, so suppressing it while
                    # work runs is how a queue empties and never refills.
                    # A parked workflow is offered as a gate, and the
                    # schema requires a gate to be in `plan`. One parked
                    # later than that is deliberately left out rather than
                    # written as a card the constraint would refuse — it
                    # needs a card of its own, which does not exist yet.
                    " (w.status='awaiting_start' OR "
                    "  (w.status='parked' AND w.phase='plan') OR "
                    "  (w.status='snoozed' AND w.due_at<=? "
                    "   AND w.last_result_id IS NULL)) OR "
                    " (w.status='snoozed' AND w.due_at<=? "
                    "  AND w.last_result_id IS NOT NULL) OR "
                    " (w.status='awaiting_review' AND w.phase='plan' "
                    "  AND r.outcome='awaiting_plan') OR "
                    " (w.status='awaiting_review' AND w.phase='execute' "
                    "  AND r.outcome='awaiting_external') OR "
                    " (w.status IN ('awaiting_review','completed') "
                    "  AND r.outcome IN ('completed','declined','ineligible'))"
                    ") ORDER BY CASE WHEN w.status='awaiting_start' OR "
                    "(w.status='snoozed' AND w.last_result_id IS NULL) "
                    "THEN 1 ELSE 0 END,w.updated_at,w.task_id LIMIT ?",
                    (now, now, limit),
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
                self._render_card(row)
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
                    self._render_card(values), token, expires
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
        if action not in {
            "start", "snooze", OWNER_HOLD_ACTION, *REVIEW_DIRECT_ACTIONS
        }:
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
                    and not _card_fits(self._render_card(row))
                ):
                    refusal = ExecutionCardRefusal.INVALID_STATE
                if refusal is not None:
                    connection.rollback()
                    return _refused_row(card_id, row, refusal)

                card_kind = ExecutionCardKind(row["kind"])
                if (
                    card_kind is ExecutionCardKind.START
                    and action in {"done", "drop"}
                ):
                    workflow = _apply_review_lifecycle_action(
                        connection,
                        row,
                        action=action,
                        now=now,
                    )
                elif (
                    card_kind is ExecutionCardKind.START
                    and action == OWNER_HOLD_ACTION
                ):
                    card = self._render_card(row)
                    if not card.owner_hold_eligible:
                        connection.rollback()
                        return _refused_row(
                            card_id, row, ExecutionCardRefusal.INVALID_ACTION
                        )
                    workflow = _apply_owner_hold(
                        connection, row, stamp=stamp
                    )
                elif card_kind is ExecutionCardKind.START:
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

    def agent_options(
        self, card_id: int, *, expected_version: int
    ) -> ExecutionAgentSelectorResult:
        """Return bounded installed choices for one current Start card."""
        if not _valid_identity(card_id, expected_version):
            return _agent_refused(
                card_id, ExecutionCardRefusal.INVALID_ARGUMENT
            )
        with closing(self._connect()) as connection:
            row = connection.execute(
                self._card_select() + " WHERE c.id=?", (card_id,)
            ).fetchone()
            refusal = _agent_card_refusal(row, expected_version)
            if refusal is not None:
                return _agent_refused_row(card_id, row, refusal)
            card = self._render_card(row)
            options = _agent_options(self._profile_registry, card)
            return ExecutionAgentSelectorResult(
                ExecutionCardDisposition.UNCHANGED,
                card_id,
                card_version=expected_version,
                card=card,
                options=options,
            )

    def select_agent(
        self,
        card_id: int,
        *,
        expected_version: int,
        selection_token: str,
    ) -> ExecutionAgentSelectorResult:
        """Select an eligible exact profile and refresh the Start card."""
        if (
            not _valid_identity(card_id, expected_version)
            or not _valid_agent_selection_token(selection_token)
        ):
            return _agent_refused(
                card_id, ExecutionCardRefusal.INVALID_ARGUMENT
            )
        matches = [
            profile
            for profile in _eligible_profiles(self._profile_registry)
            if _agent_selection_token(profile) == selection_token
        ]
        if len(matches) != 1:
            return _agent_refused(
                card_id, ExecutionCardRefusal.INVALID_ARGUMENT
            )
        profile = matches[0]
        stamp = self._clock_value()
        now = stamp.isoformat(timespec="seconds")
        with closing(self._connect()) as connection:
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = connection.execute(
                    self._card_select() + " WHERE c.id=?", (card_id,)
                ).fetchone()
                refusal = _agent_card_refusal(row, expected_version)
                if refusal is not None:
                    connection.rollback()
                    return _agent_refused_row(card_id, row, refusal)
                current = self._render_card(row)
                workflow = _apply_agent_selection(
                    connection,
                    int(row["task_id"]),
                    expected_version=int(row["workflow_version"]),
                    profile=profile,
                    stamp=stamp,
                )
                if workflow.disposition is WorkflowDisposition.REFUSED:
                    connection.rollback()
                    return _agent_refused_row(
                        card_id,
                        row,
                        _workflow_refusal(workflow.refusal),
                    )
                if workflow.disposition is WorkflowDisposition.UNCHANGED:
                    connection.rollback()
                    return ExecutionAgentSelectorResult(
                        ExecutionCardDisposition.UNCHANGED,
                        card_id,
                        card_version=expected_version,
                        card=current,
                    )
                card_version = expected_version + 1
                updated = connection.execute(
                    "UPDATE execution_review_cards SET version=?,"
                    "workflow_version=?,updated_at=? WHERE id=? AND version=? "
                    "AND status='delivered'",
                    (
                        card_version,
                        workflow.version,
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
                    kind="refreshed",
                    card_version=card_version,
                    workflow_version=int(workflow.version),
                    action="agent",
                    now=now,
                )
                values = dict(row)
                values.update(
                    version=card_version,
                    workflow_version=workflow.version,
                    workflow_version_current=workflow.version,
                    workflow_agent_profile_id=profile.profile_id,
                    workflow_agent_profile_revision=profile.revision,
                )
                card = self._render_card(values)
                connection.commit()
                return ExecutionAgentSelectorResult(
                    ExecutionCardDisposition.APPLIED,
                    card_id,
                    card_version=card_version,
                    card=card,
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
                # A gate used to refuse both, on the reasoning that there
                # was no work yet to talk about. But the note a reader wants
                # to leave is most useful BEFORE the first pass, not after
                # reading a plan that ignored it, and a task is most often
                # noticed as someone else's at the moment it is offered.
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
                    status = (
                        WorkflowStatus.AWAITING_START
                        if ExecutionCardKind(row["kind"])
                        is ExecutionCardKind.START
                        else WorkflowStatus.QUEUED
                    )
                    phase = WorkflowPhase.PLAN
                    event_kind = "discussion_requested"
                    resolution = "discuss"
                    workflow_update = connection.execute(
                        "UPDATE task_execution_workflows SET "
                        "status=?,phase='plan',version=?,due_at=NULL,"
                        "claim_token_digest=NULL,claimed_at=NULL,"
                        "claim_heartbeat_at=NULL,claim_expires_at=NULL,"
                        "failure_count=0,last_failure_reason=NULL,"
                        "last_failure_at=NULL,next_attempt_at=NULL,"
                        "parked_at=NULL,updated_at=?,completed_at=NULL "
                        "WHERE task_id=? AND version=?",
                        (
                            status,
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
                        "UPDATE tasks SET owner=?,version=?,updated_at=?,"
                        "owner_ref_version=1,owner_kind='external',"
                        "owner_speaker_id=NULL,"
                        "owner_canonical_speaker_id=NULL,"
                        "owner_speaker_registry_id=NULL,owner_pinned=1,"
                        "owner_provisional=0 "
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
        """What is actually occupying the reader's surface.

        A card counts only while it can still be answered. Once the work
        behind it has moved on, its buttons are refused, so it is not
        holding a place in any sense the reader would recognise — but it
        used to be counted anyway. With room for one card, a single
        superseded card meant no card could ever be scheduled again, and
        the sweep that cancels it lives behind the scheduling call the
        drip had already returned from. The surface simply went quiet and
        stayed quiet.
        """
        with closing(self._connect()) as connection:
            rows = [
                row
                for row in connection.execute(
                    self._card_select()
                    + " WHERE c.status IN ('pending','delivering','delivered')"
                ).fetchall()
                if _current_card(row)
            ]
        counts: dict[str, int] = {}
        for row in rows:
            status = str(row["status"])
            counts[status] = counts.get(status, 0) + 1
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

    def _poll_owner_holds(self, stamp: datetime, *, limit: int) -> None:
        """Refresh bounded durable holds without keeping a write lock."""
        now = stamp.isoformat(timespec="seconds")
        with closing(self._connect()) as connection:
            rows = connection.execute(
                _OWNER_HOLD_SELECT + " "
                "WHERE h.status='active' ORDER BY h.id LIMIT ?",
                (limit,),
            ).fetchall()
        for row in rows:
            if not _current_owner_hold(row):
                self._finish_owner_hold(row, now=now, reason="stale")
                continue
            if row["backstop_at"] <= now:
                self._finish_owner_hold(row, now=now, reason="backstop")
                continue
            if self._owner_condition is None:
                continue
            try:
                result = self._owner_condition(
                    str(row["owner_display"]), _owner_ref(row)
                )
            except KnowledgeClientError:
                continue
            if not isinstance(result, OwnerUpcomingMeeting):
                continue
            self._record_owner_condition(row, result=result, now=now)

    def _record_owner_condition(
        self,
        row: Mapping[str, object],
        *,
        result: OwnerUpcomingMeeting,
        now: str,
    ) -> None:
        with closing(self._connect()) as connection:
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute("BEGIN IMMEDIATE")
            try:
                current = connection.execute(
                    _OWNER_HOLD_SELECT + " WHERE h.id=?",
                    (int(row["id"]),),
                ).fetchone()
                if (
                    current is None
                    or current["status"] != "active"
                    or not _current_owner_hold(current)
                ):
                    connection.rollback()
                    return
                row = current
                connection.execute(
                    "UPDATE task_execution_owner_holds SET "
                    "last_checked_at=?,last_evidence_revision=?,"
                    "last_match=? WHERE id=? AND status='active'",
                    (
                        result.checked_at,
                        result.evidence_revision,
                        int(result.match),
                        int(row["id"]),
                    ),
                )
                _owner_hold_event(
                    connection,
                    row,
                    kind="condition_checked",
                    matched=result.match,
                    evidence_revision=result.evidence_revision,
                    now=now,
                )
                connection.commit()
            except Exception:
                connection.rollback()
                raise
        if result.match:
            self._finish_owner_hold(row, now=now, reason="meeting")

    def _finish_owner_hold(
        self, row: Mapping[str, object], *, now: str, reason: str
    ) -> None:
        with closing(self._connect()) as connection:
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute("BEGIN IMMEDIATE")
            try:
                current = connection.execute(
                    _OWNER_HOLD_SELECT + " WHERE h.id=?",
                    (int(row["id"]),),
                ).fetchone()
                if current is None or current["status"] != "active":
                    connection.rollback()
                    return
                if not _current_owner_hold(current):
                    reason = "stale"
                row = current
                if reason == "stale":
                    connection.execute(
                        "UPDATE task_execution_owner_holds SET "
                        "status='cancelled',release_reason='stale',"
                        "released_at=? WHERE id=? AND status='active'",
                        (now, int(row["id"])),
                    )
                    _owner_hold_event(
                        connection, row, kind="cancelled", now=now
                    )
                    connection.commit()
                    return
                workflow_version = int(row["workflow_version"]) + 1
                updated = connection.execute(
                    "UPDATE task_execution_workflows SET "
                    "status='awaiting_start',phase='plan',version=?,"
                    "due_at=NULL,claim_token_digest=NULL,claimed_at=NULL,"
                    "claim_heartbeat_at=NULL,claim_expires_at=NULL,"
                    "failure_count=0,last_failure_reason=NULL,"
                    "last_failure_at=NULL,next_attempt_at=NULL,"
                    "parked_at=NULL,updated_at=?,completed_at=NULL "
                    "WHERE task_id=? AND task_version=? AND version=? "
                    "AND status='snoozed' AND due_at=?",
                    (
                        workflow_version,
                        now,
                        int(row["task_id"]),
                        int(row["task_version"]),
                        int(row["workflow_version"]),
                        row["backstop_at"],
                    ),
                )
                if updated.rowcount != 1:
                    connection.execute(
                        "UPDATE task_execution_owner_holds SET "
                        "status='cancelled',release_reason='stale',"
                        "released_at=? WHERE id=? AND status='active'",
                        (now, int(row["id"])),
                    )
                    _owner_hold_event(
                        connection, row, kind="cancelled", now=now
                    )
                    connection.commit()
                    return
                connection.execute(
                    "UPDATE task_execution_owner_holds SET status='released',"
                    "release_reason=?,released_at=? "
                    "WHERE id=? AND status='active'",
                    (reason, now, int(row["id"])),
                )
                _owner_hold_event(
                    connection, row, kind="released", now=now
                )
                TaskExecutionService._event(
                    connection,
                    int(row["task_id"]),
                    "scheduled",
                    workflow_version,
                    int(row["task_version"]),
                    WorkflowPhase.PLAN,
                    WorkflowStatus.AWAITING_START,
                    now,
                )
                connection.commit()
            except Exception:
                connection.rollback()
                raise

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
            "SELECT c.*,t.text AS task_text,t.owner,t.owner_ref_version,"
            "t.owner_kind,t.owner_speaker_id,t.owner_canonical_speaker_id,"
            "t.owner_speaker_registry_id,t.owner_pinned,t.owner_provisional,"
            "t.due,"
            "t.status AS task_status_current,t.version AS task_version_current,"
            "w.status AS workflow_status_current,"
            "w.failure_count AS workflow_failure_count,"
            "w.last_failure_reason AS workflow_failure_reason,"
            "w.phase AS workflow_phase_current,"
            "w.version AS workflow_version_current,"
            "w.task_version AS workflow_task_version_current,"
            "w.agent_profile_id AS workflow_agent_profile_id,"
            "w.agent_profile_revision AS workflow_agent_profile_revision,"
            "w.last_result_id AS workflow_result_id,"
            "r.task_id AS result_task_id,r.workflow_version AS result_version,"
            "r.task_version AS result_task_version,r.phase AS result_phase,"
            "r.outcome AS result_outcome,r.summary,r.work_markdown,"
            "r.questions_json,r.external_actions_json,r.deliverables_json,"
            "r.task_work_directory,r.task_kb_file,"
            "(SELECT min(h.created_at) "
            " FROM task_candidate_bindings AS b "
            " JOIN candidate_revision_history AS h "
            " ON h.candidate_id=b.candidate_id "
            " WHERE b.task_id=c.task_id) AS first_raised,"
            "(SELECT max(h.created_at) "
            " FROM task_candidate_bindings AS b "
            " JOIN candidate_revision_history AS h "
            " ON h.candidate_id=b.candidate_id "
            " WHERE b.task_id=c.task_id) AS last_mentioned,"
            # How many plans this task has already produced, and the last
            # thing the reader said. Counted rather than stored: a pass is a
            # result, so the ledger already knows, and a second column would
            # be a second thing to keep true.
            "(SELECT count(*)-1 FROM task_execution_results AS prior "
            " WHERE prior.task_id=c.task_id AND prior.phase=r.phase)"
            " AS revision_count,"
            "(SELECT i.value FROM execution_reader_inputs AS i "
            " WHERE i.task_id=c.task_id AND i.kind='discussion' "
            " ORDER BY i.sequence DESC LIMIT 1) AS revision_note,"
            # Where the task came from. A reader asked to authorise work on
            # an issue cannot answer without being told which issue.
            "(SELECT o.source_kind FROM task_candidate_bindings AS b "
            " JOIN candidate_inbox AS o ON o.candidate_id=b.candidate_id "
            " WHERE b.task_id=c.task_id AND b.relation='accepted') "
            " AS origin_kind,"
            "(SELECT o.source_record_id FROM task_candidate_bindings AS b "
            " JOIN candidate_inbox AS o ON o.candidate_id=b.candidate_id "
            " WHERE b.task_id=c.task_id AND b.relation='accepted') "
            " AS origin_record,"
            "(SELECT o.source_item_id FROM task_candidate_bindings AS b "
            " JOIN candidate_inbox AS o ON o.candidate_id=b.candidate_id "
            " WHERE b.task_id=c.task_id AND b.relation='accepted') "
            " AS origin_item,"
            # The task that reviewed an earlier state of the same thing.
            # An identity like `7/<state>` makes each state its own task, so
            # without this an agent starts from nothing every time a pull
            # request moves, and a reader cannot tell a second pass from a
            # duplicate.
            "(SELECT b2.task_id FROM task_candidate_bindings AS b2 "
            " JOIN candidate_inbox AS o2 "
            " ON o2.candidate_id=b2.candidate_id "
            " JOIN task_candidate_bindings AS b1 "
            " ON b1.task_id=c.task_id AND b1.relation='accepted' "
            " JOIN candidate_inbox AS o1 "
            " ON o1.candidate_id=b1.candidate_id "
            " WHERE b2.relation='accepted' AND b2.task_id<>c.task_id "
            " AND o2.source_kind=o1.source_kind "
            " AND o2.source_record_id=o1.source_record_id "
            " AND " + _ITEM_STEM.format(column="o2.source_item_id") + "="
            + _ITEM_STEM.format(column="o1.source_item_id") +
            " ORDER BY b2.task_id DESC LIMIT 1) AS prior_task_id,"
            "(SELECT h.payload_json FROM task_candidate_bindings AS b "
            " JOIN candidate_revision_history AS h "
            " ON h.candidate_id=b.candidate_id "
            " AND h.source_revision=b.source_revision "
            " WHERE b.task_id=c.task_id AND b.relation='accepted') "
            " AS origin_payload "
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

    def _render_card(
        self, row: Mapping[str, object]
    ) -> ExecutionReviewCard:
        return _card(
            row,
            self._profile_registry,
            reader_aliases=self._reader_aliases,
            condition_available=self._owner_condition is not None,
        )

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
            for row in _button_rows(card, approvable=approvable)
        ]
    }
    return body, keyboard


def render_execution_agent_selector(
    result: ExecutionAgentSelectorResult,
) -> tuple[str, dict[str, list[list[dict[str, str]]]]]:
    """Render one bounded Telegram-compatible agent choice view."""
    if not isinstance(result, ExecutionAgentSelectorResult):
        raise TaskLedgerError("execution agent selector is invalid")
    if not result.accepted or result.card is None or not result.options:
        raise TaskLedgerError("execution agent selector is unavailable")
    body, _ = render_execution_review_card(result.card)
    suffix = "\n\n<b>Choose the agent for planning:</b>"
    if len((body + suffix).encode("utf-8")) > MAX_CARD_BODY_BYTES:
        body = _escape_bounded(
            "\n".join(_card_lines(result.card)),
            MAX_TRUNCATED_CARD_BODY_BYTES,
            suffix="\n\nChoose the agent for planning:",
        )
        suffix = ""
    keyboard = {
        "inline_keyboard": [
            [{
                "text": (
                    f"✓ {option.display_name}"
                    if option.selected
                    else option.display_name
                ),
                "callback_data": _agent_callback(
                    result.card.id,
                    result.card.version,
                    option.selection_token,
                ),
            }]
            for option in result.options
        ]
    }
    return body + suffix, keyboard


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
        "done", "reassign", "drop", "agent", OWNER_HOLD_ACTION,
        *REVIEW_SNOOZE_INTERVALS,
    }:
        return None
    return card_id, version, parts[3]


def parse_execution_agent_callback(
    value: object,
) -> tuple[int, int, str] | None:
    if not isinstance(value, str) or not 1 <= len(value) <= CALLBACK_DATA_LIMIT:
        return None
    parts = value.split("|")
    if len(parts) != 4 or parts[0] != AGENT_CALLBACK_PREFIX:
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
    if not _valid_agent_selection_token(parts[3]):
        return None
    return card_id, version, parts[3]


def _kind_for_workflow(row: Mapping[str, object]) -> ExecutionCardKind:
    if (
        row["status"] in {
            WorkflowStatus.AWAITING_START,
            WorkflowStatus.SNOOZED,
            # A workflow that gave up is asked about like one that has not
            # begun: the question is whether to run it, and the card says
            # it already tried.
            WorkflowStatus.PARKED,
        }
        and (
            row["last_result_id"] is None
            # A parked workflow may carry a result from an earlier phase
            # that succeeded. It is still a "run this again?" question, and
            # requiring no result here left it matching the eligibility
            # query and no card kind at all — which raised, and took the
            # whole sweep down with it, including cards that were fine.
            or row["status"] == WorkflowStatus.PARKED
        )
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
                in {
                    WorkflowStatus.AWAITING_START,
                    WorkflowStatus.SNOOZED,
                    WorkflowStatus.PARKED,
                }
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


def _card(
    row: Mapping[str, object],
    registry: AgentProfileRegistry,
    *,
    reader_aliases: frozenset[str] = frozenset(),
    condition_available: bool = False,
) -> ExecutionReviewCard:
    try:
        kind = ExecutionCardKind(row["kind"])
        profile_id = str(row["workflow_agent_profile_id"])
        profile_revision = str(row["workflow_agent_profile_revision"])
        try:
            profile = registry.resolve(profile_id, profile_revision)
            profile_name = profile.display_name
        except AgentProfileError:
            if kind is ExecutionCardKind.START:
                raise
            profile_name = profile_id
        owner_display = canonical_owner_display(row["owner"], row["owner_kind"])
        return ExecutionReviewCard(
            id=int(row["id"]),
            task_id=int(row["task_id"]),
            task_version=int(row["task_version"]),
            workflow_version=int(row["workflow_version"]),
            kind=kind,
            phase=WorkflowPhase(row["phase"]),
            result_id=row["result_id"],
            status=ExecutionCardStatus(row["status"]),
            version=int(row["version"]),
            created_at=str(row["created_at"]),
            workflow_status=WorkflowStatus(row["workflow_status_current"]),
            failure_count=int(row["workflow_failure_count"] or 0),
            failure_reason=str(row["workflow_failure_reason"] or ""),
            agent_profile_id=profile_id,
            agent_profile_revision=profile_revision,
            agent_display_name=profile_name,
            task_text=str(row["task_text"]),
            owner=owner_display,
            due=row["due"],
            first_raised=row["first_raised"],
            last_mentioned=row["last_mentioned"],
            owner_hold_eligible=_owner_hold_eligible(
                row,
                owner_display,
                reader_aliases=reader_aliases,
                condition_available=condition_available,
            ),
            summary="" if row["summary"] is None else str(row["summary"]),
            work_markdown=(
                ""
                if row["work_markdown"] is None
                else str(row["work_markdown"])
            ),
            questions=_stored_lines(row["questions_json"]),
            external_actions=_stored_collection(row["external_actions_json"]),
            deliverables=_stored_collection(row["deliverables_json"]),
            outcome=(
                None
                if row["result_outcome"] is None
                else ExecutionOutcome(row["result_outcome"])
            ),
            revisions=max(0, int(row["revision_count"] or 0)),
            revision_note=str(row["revision_note"] or ""),
            origin_kind=str(row["origin_kind"] or ""),
            origin_record=str(row["origin_record"] or ""),
            origin_item=str(row["origin_item"] or ""),
            prior_task_id=(
                None if row["prior_task_id"] is None
                else int(row["prior_task_id"])
            ),
            task_work_directory=str(row["task_work_directory"] or ""),
            task_kb_file=str(row["task_kb_file"] or ""),
            origin_sources=stored_origin_sources(row["origin_payload"]),
        )
    except (AgentProfileError, KeyError, TypeError, ValueError) as exc:
        raise TaskLedgerError("execution review card state is invalid") from exc


def _normalized_owner(value: str) -> str:
    decomposed = unicodedata.normalize("NFKD", value).casefold()
    return " ".join(
        "".join(
            character if character.isalnum() else " "
            for character in decomposed
            if not unicodedata.combining(character)
        ).split()
    )


def _owner_hold_eligible(
    row: Mapping[str, object],
    owner_display: str | None,
    *,
    reader_aliases: frozenset[str],
    condition_available: bool,
) -> bool:
    if (
        not condition_available
        or not reader_aliases
        or owner_display in {None, "(unassigned)"}
        or row["owner_ref_version"] != 1
        or row["owner_kind"] not in {"person", "external"}
        or row["owner_provisional"] != 0
        or _normalized_owner(owner_display) in reader_aliases
    ):
        return False
    scoped = (
        row["owner_speaker_id"],
        row["owner_canonical_speaker_id"],
        row["owner_speaker_registry_id"],
    )
    return all(value is None for value in scoped) or all(
        isinstance(value, str) and bool(value) for value in scoped
    )


def _owner_ref(row: Mapping[str, object]) -> dict[str, object]:
    return {
        "kind": row["owner_kind"],
        "speaker_id": row["owner_speaker_id"],
        "canonical_speaker_id": row["owner_canonical_speaker_id"],
        "speaker_registry_id": row["owner_speaker_registry_id"],
        "pinned": bool(row["owner_pinned"]),
        "provisional": bool(row["owner_provisional"]),
    }


def _current_owner_hold(row: Mapping[str, object]) -> bool:
    return (
        row["task_status_current"] == TaskStatus.OPEN
        and row["task_version_current"] == row["task_version"]
        and row["workflow_task_version_current"] == row["task_version"]
        and row["workflow_version_current"] == row["workflow_version"]
        and row["workflow_status_current"] == WorkflowStatus.SNOOZED
        and row["workflow_due_at"] == row["backstop_at"]
        and canonical_owner_display(
            row["owner_current"], row["owner_kind_current"]
        ) == row["owner_display"]
        and row["owner_ref_version_current"] == row["owner_ref_version"]
        and row["owner_kind_current"] == row["owner_kind"]
        and row["owner_speaker_id_current"] == row["owner_speaker_id"]
        and row["owner_canonical_speaker_id_current"]
        == row["owner_canonical_speaker_id"]
        and row["owner_speaker_registry_id_current"]
        == row["owner_speaker_registry_id"]
        and row["owner_pinned_current"] == row["owner_pinned"]
        and row["owner_provisional_current"] == row["owner_provisional"]
    )


def _apply_owner_hold(
    connection: sqlite3.Connection,
    row: Mapping[str, object],
    *,
    stamp: datetime,
) -> WorkflowOperationResult:
    now = stamp.isoformat(timespec="seconds")
    wake = (stamp + OWNER_HOLD_INTERVAL).isoformat(timespec="seconds")
    task_id = int(row["task_id"])
    workflow_version = int(row["workflow_version"]) + 1
    updated = connection.execute(
        "UPDATE task_execution_workflows SET status='snoozed',phase='plan',"
        "version=?,due_at=?,claim_token_digest=NULL,claimed_at=NULL,"
        "claim_heartbeat_at=NULL,claim_expires_at=NULL,failure_count=0,"
        "last_failure_reason=NULL,last_failure_at=NULL,next_attempt_at=NULL,"
        "parked_at=NULL,updated_at=?,completed_at=NULL "
        "WHERE task_id=? AND version=? AND task_version=? "
        "AND status IN ('awaiting_start','snoozed','parked')",
        (
            workflow_version,
            wake,
            now,
            task_id,
            int(row["workflow_version"]),
            int(row["task_version"]),
        ),
    )
    if updated.rowcount != 1:
        return WorkflowOperationResult(
            WorkflowDisposition.REFUSED,
            task_id,
            refusal=WorkflowRefusal.STALE_WORKFLOW,
        )
    cursor = connection.execute(
        "INSERT INTO task_execution_owner_holds("
        "task_id,task_version,workflow_version,status,owner_display,"
        "owner_ref_version,owner_kind,owner_speaker_id,"
        "owner_canonical_speaker_id,owner_speaker_registry_id,owner_pinned,"
        "owner_provisional,backstop_at,created_at) "
        "VALUES(?,?,?,'active',?,?,?,?,?,?,?,?,?,?)",
        (
            task_id,
            int(row["task_version"]),
            workflow_version,
            canonical_owner_display(row["owner"], row["owner_kind"]),
            int(row["owner_ref_version"]),
            row["owner_kind"],
            row["owner_speaker_id"],
            row["owner_canonical_speaker_id"],
            row["owner_speaker_registry_id"],
            int(row["owner_pinned"]),
            int(row["owner_provisional"]),
            wake,
            now,
        ),
    )
    hold_row = {"id": int(cursor.lastrowid), "task_id": task_id}
    _owner_hold_event(connection, hold_row, kind="created", now=now)
    TaskExecutionService._event(
        connection,
        task_id,
        "snoozed",
        workflow_version,
        int(row["task_version"]),
        WorkflowPhase.PLAN,
        WorkflowStatus.SNOOZED,
        now,
    )
    return WorkflowOperationResult(
        WorkflowDisposition.APPLIED,
        task_id,
        workflow_version,
        WorkflowStatus.SNOOZED,
        WorkflowPhase.PLAN,
        wake_at=wake,
        agent_profile_id=row["workflow_agent_profile_id"],
        agent_profile_revision=row["workflow_agent_profile_revision"],
    )


def _owner_hold_event(
    connection: sqlite3.Connection,
    row: Mapping[str, object],
    *,
    kind: str,
    now: str,
    matched: bool | None = None,
    evidence_revision: str | None = None,
) -> None:
    connection.execute(
        "INSERT INTO task_execution_owner_hold_events("
        "hold_id,task_id,kind,matched,evidence_revision,occurred_at) "
        "VALUES(?,?,?,?,?,?)",
        (
            int(row["id"]),
            int(row["task_id"]),
            kind,
            None if matched is None else int(matched),
            evidence_revision,
            now,
        ),
    )


@dataclass(frozen=True)
class CardRecord:
    """One thing an agent recorded, as much of it as a card can show.

    A plain line an agent wrote arrives with `text` set and nothing else,
    which is what every result written before records existed looks like.
    A structured one fills the fields a reader needs in order to approve
    it without opening anything: what the action needs before it can run,
    or who a draft is addressed to and what it says.
    """

    text: str
    requires: str = ""
    channel: str = ""
    label: str = ""
    recipient: str = ""
    subject: str = ""

    @property
    def structured(self) -> bool:
        return bool(self.requires or self.channel or self.label
                    or self.recipient or self.subject)


def _stored_lines(value: object) -> tuple[str, ...]:
    """Questions are prose, so a record is flattened back to its sentence."""
    return tuple(record.text for record in _stored_collection(value))


def _stored_collection(value: object) -> tuple[CardRecord, ...]:
    if value is None:
        return ()
    try:
        parsed = json.loads(value)
    except (TypeError, ValueError):
        raise TaskLedgerError("execution review card result is invalid") from None
    if not isinstance(parsed, list):
        raise TaskLedgerError("execution review card result is invalid")
    records: list[CardRecord] = []
    for item in parsed:
        if isinstance(item, str):
            records.append(CardRecord(item))
            continue
        if not isinstance(item, dict):
            raise TaskLedgerError(
                "execution review card result is invalid")
        text = item.get("action") or item.get("body") or ""
        fields = {
            name: item.get(name) or ""
            for name in ("requires", "channel", "label", "recipient",
                         "subject")
        }
        if not isinstance(text, str) or not text or any(
            not isinstance(field_value, str)
            for field_value in fields.values()
        ):
            raise TaskLedgerError(
                "execution review card result is invalid")
        records.append(CardRecord(text, **fields))
    return tuple(records)


#: The part of an identifier before its state, if it has one. A review
#: task is `7/<state>`; an issue is just `42`. Written once because it has
#: to mean the same thing on both sides of a comparison.
_ITEM_STEM = (
    "(CASE WHEN instr({column},'/')>0 "
    "THEN substr({column},1,instr({column},'/')-1) ELSE {column} END)"
)

def _continues_lines(
    card: ExecutionReviewCard, *, html: bool
) -> list[str]:
    """Name the task that looked at an earlier state of the same thing.

    Some sources are identified by state as well as by thing, because
    looking at a pull request as it stood on Monday and as it stands on
    Thursday are two jobs. Without naming the first, an agent starts from
    nothing each time, and a reader cannot tell a second pass from a
    duplicate card.

    There are two heading builders, so this lives in one place rather than
    being written twice and drifting.
    """
    if card.prior_task_id is None:
        return []
    shown = f"Continues T{card.prior_task_id}"
    return [f"↩ <b>{shown}</b>" if html else f"↩ {shown}"]


def _origin_lines(card: ExecutionReviewCard, *, html: bool) -> list[str]:
    """Say where the work came from, and make it reachable.

    A gate asks the reader to authorise work on something. Naming the task
    is not the same as naming the thing: two issues can share a title, and
    an issue number is what the reader will search for afterwards.
    """
    return shared_origin_lines(
        kind=card.origin_kind,
        record=card.origin_record,
        item=card.origin_item,
        sources=card.origin_sources,
        html_output=html,
    )


def _card_date(value: str | None) -> str:
    return "" if not value else str(value)[:10]


def _start_card_lines(
    card: ExecutionReviewCard, *, html: bool
) -> list[str]:
    """Render the established pre-work decision card."""
    handle = f"T{card.task_id}"
    if html:
        head = f"🚦 <b>Start this task?</b>  <code>{handle}</code>"
        task = f"<b>{_escape(card.task_text)}</b>"
    else:
        head = f"🚦 Start this task?  {handle}"
        task = card.task_text
    lines = [head, "", task]
    if card.owner:
        lines.append(
            f"👤 <b>Owner:</b> {_escape(card.owner)}"
            if html else f"👤 Owner: {card.owner}"
        )
    first = _card_date(card.first_raised)
    if first:
        lines.append(
            f"📌 <b>First raised:</b> {_escape(first)}"
            if html else f"📌 First raised: {first}"
        )
    last = _card_date(card.last_mentioned)
    if last and last != first:
        lines.append(
            f"🕑 <b>Last mentioned:</b> {_escape(last)}"
            if html else f"🕑 Last mentioned: {last}"
        )
    lines.extend(_origin_lines(card, html=html))
    lines.extend(_continues_lines(card, html=html))
    # Which agent would run this, before it runs. A reader who cannot see
    # it cannot tell that a pull request is about to be reviewed by a
    # compatibility profile, which is how one review was lost.
    agent = (_escape(card.agent_display_name) if html
             else card.agent_display_name)
    lines.append(f"🤖 <b>Agent:</b> {agent}" if html else f"🤖 Agent: {agent}")
    if card.workflow_status is WorkflowStatus.PARKED:
        # A reader who is never told has no way to distinguish a task
        # nobody has reached from one the agent abandoned.
        stopped = (
            f"⚠️ Stopped after {card.failure_count} failed attempt"
            f"{'s' if card.failure_count != 1 else ''}"
            + (f" ({card.failure_reason})" if card.failure_reason else "")
        )
        lines.append(f"<b>{_escape(stopped)}</b>" if html else stopped)
        explanation = (
            "Continue tries again. The runs so far left nothing recorded."
        )
        return lines + ["", explanation]
    explanation = "No agent has looked at this yet. "
    explanation += (
        "<b>Continue</b> starts the investigation."
        if html else "Continue starts the investigation."
    )
    return [*lines, "", explanation]


def _heading_lines(card: ExecutionReviewCard, *, html: bool) -> list[str]:
    """Identify the task before describing it.

    A card the reader cannot name is a card they cannot refer to, ask about,
    or find again. The identifier goes first for the same reason it does in
    GW: on a phone the first line is often the whole notification.
    """
    handle = f"T{card.task_id}"
    title = "Task workflow"
    if html:
        head = f"🤖 <b>{title}</b>  <code>{handle}</code>"
        body = f"<b>{_escape(card.task_text)}</b>"
    else:
        head = f"🤖 {title}  {handle}"
        body = card.task_text
    lines = [head, body, ""]
    phase = _phase_name(card.phase)
    if card.kind is ExecutionCardKind.START:
        # The phase names what WOULD run. Naming it here would read as
        # though it already had, which is the one thing a start gate must
        # not imply.
        phase = "not started"
    elif card.outcome is ExecutionOutcome.COMPLETED:
        # Marked rather than renamed: the same card carrying its last
        # update, recognisable at a glance as finished.
        done = "✅ done — close it with Mark as done"
        phase = f"{phase} {done}" if not html else f"{phase} <b>{done}</b>"
    lines.append(f"<b>Phase:</b> {phase}" if html else f"Phase: {phase}")
    if card.kind is not ExecutionCardKind.START:
        agent = (
            _escape(card.agent_display_name)
            if html
            else card.agent_display_name
        )
        lines.append(f"<b>Agent:</b> {agent}" if html else f"Agent: {agent}")
    if card.revisions:
        revised = f"Revision {card.revisions}"
        lines.append(f"<b>{revised}</b>" if html else revised)
    lines.extend(_origin_lines(card, html=html))
    lines.extend(_continues_lines(card, html=html))
    return lines


def _asked_for_lines(card: ExecutionReviewCard, *, html: bool) -> list[str]:
    """The reader's own words, quoted back.

    Without them a third pass reads exactly like a first, and an hour later
    the reader cannot recall what they asked this one to fix.
    """
    if not (card.revisions and card.revision_note):
        return []
    shown = card.revision_note
    if len(shown) > MAX_REVISION_NOTE_CHARS:
        shown = shown[:MAX_REVISION_NOTE_CHARS].rstrip() + "…"
    if html:
        return ["", "<b>You asked for:</b>",
                "<blockquote>" + _escape(shown) + "</blockquote>"]
    return ["", "You asked for:", shown]


def _review_lines(card: ExecutionReviewCard, *, html: bool) -> list[str]:
    """Put the durable evidence and forge references before the long work."""
    lines: list[str] = []
    if card.task_work_directory or card.task_kb_file:
        lines.extend(("", "<b>Review files:</b>" if html else "Review files:"))
        if card.task_work_directory:
            shown = _escape(card.task_work_directory)
            lines.append(
                f"• Working folder: <code>{shown}</code>"
                if html else f"- Working folder: {card.task_work_directory}"
            )
        if card.task_kb_file:
            shown = _escape(card.task_kb_file)
            lines.append(
                f"• KB task file: <code>{shown}</code>"
                if html else f"- KB task file: {card.task_kb_file}"
            )
    links = review_links(
        "\n".join((
            card.summary,
            card.work_markdown,
            *(question for question in card.questions),
            *(record.text for record in card.external_actions),
            *(record.text for record in card.deliverables),
        )),
        origin_kind=card.origin_kind,
        origin_record=card.origin_record,
        origin_item=card.origin_item,
    )
    if links:
        lines.extend(("", "<b>Review links:</b>" if html else "Review links:"))
        lines.extend(
            f"• {_markdown_inline(link)}" if html else f"- {link}"
            for link in links
        )
    return lines


def _card_lines(card: ExecutionReviewCard) -> list[str]:
    if card.kind is ExecutionCardKind.START:
        return _start_card_lines(card, html=False)
    details = []
    if card.owner:
        details.append(f"Owner: {card.owner}")
    if card.due:
        details.append(f"Due: {card.due}")
    if card.kind is ExecutionCardKind.EXTERNAL_REVIEW:
        lines = [
            *_heading_lines(card, html=False),
            *details,
            "",
            "External action awaiting your approval:",
            *_listed(card.external_actions, empty="None supplied."),
            "",
            "Approve only if these exact external effects are intended.",
            "",
            f"Summary: {card.summary}",
            *_review_lines(card, html=False),
            *_asked_for_lines(card, html=False),
        ]
        if card.questions:
            lines.extend(("", "Needs your input:",
                      *[f"- {q}" for q in card.questions]))
        if card.deliverables:
            # The effect being approved is often "send this". A reader
            # cannot judge that from a summary of it.
            lines.extend(("", "Deliverables:", *_drafted(card.deliverables)))
        return lines
    if card.kind is ExecutionCardKind.RESULT_REVIEW:
        lines = [
            *_heading_lines(card, html=False),
            *details,
            "",
            f"Outcome: {card.outcome}",
            f"Summary: {card.summary}",
            *_review_lines(card, html=False),
            *_asked_for_lines(card, html=False),
        ]
        if card.questions:
            lines.extend(("", "Needs your input:",
                      *[f"- {q}" for q in card.questions]))
        if card.deliverables:
            lines.extend(("", "Deliverables:", *_drafted(card.deliverables)))
        if card.work_markdown:
            lines.extend(("", "Work:", card.work_markdown))
        return lines
    lines = [
        *_heading_lines(card, html=False),
        *details,
        "",
        f"Summary: {card.summary}",
        *_review_lines(card, html=False),
        *_asked_for_lines(card, html=False),
    ]
    if card.questions:
        lines.extend(("", "Needs your input:",
                      *[f"- {q}" for q in card.questions]))
    if card.external_actions:
        lines.extend((
            "",
            "Potential external actions (not yet authorized):",
            *_listed(card.external_actions),
        ))
    if card.deliverables:
        lines.extend(("", "Deliverables:", *_drafted(card.deliverables)))
    if card.work_markdown:
        lines.extend(("", "Plan:", card.work_markdown))
    return lines


#: What a phase is called to a reader. The enum names the machinery; these
#: name the thing the reader is being asked about.
PHASE_NAMES = {
    WorkflowPhase.PLAN: "plan refinement",
    WorkflowPhase.EXECUTE: "plan execution",
    WorkflowPhase.EXTERNAL_ACTION: "external action",
}


def _phase_name(phase: WorkflowPhase) -> str:
    return PHASE_NAMES.get(phase, phase.value)


def _record_detail_lines(record: CardRecord) -> list[str]:
    """The sub-lines under one record: what it needs, and where it goes."""
    details = []
    if record.requires:
        details.append("Needs: " + record.requires)
    if record.channel and record.channel.casefold() not in record.text.casefold():
        details.append("Channel: " + record.channel)
    return details


def _listed(values: Sequence[CardRecord], *, empty: str = "None.") -> list[str]:
    if not values:
        return [empty]
    lines: list[str] = []
    for record in values:
        lines.append(f"- {record.text}")
        lines.extend(f"  {detail}"
                     for detail in _record_detail_lines(record))
    return lines


def _drafted(values: Sequence[CardRecord]) -> list[str]:
    """A prepared draft, shown in full rather than named.

    A deliverable a reader cannot read is a deliverable they cannot approve,
    so the body goes on the card. Anything without one is just a line.
    """
    lines: list[str] = []
    for record in values:
        if not record.structured:
            lines.append(f"- {record.text}")
            continue
        heading = record.label or "Prepared draft"
        lines.extend(("", heading))
        if record.recipient:
            lines.append("To: " + record.recipient)
        if record.subject:
            lines.append("Subject: " + record.subject)
        lines.append(record.text)
    return lines


def _html_card_lines(card: ExecutionReviewCard) -> list[str]:
    if card.kind is ExecutionCardKind.START:
        return _start_card_lines(card, html=True)
    details = []
    if card.owner:
        details.extend(_labelled_html_lines("Owner", card.owner))
    if card.due:
        details.extend(_labelled_html_lines("Due", card.due))
    if card.kind is ExecutionCardKind.EXTERNAL_REVIEW:
        lines = [
            *_heading_lines(card, html=True),
            *details,
            "",
            "<b>External action awaiting your approval:</b>",
            *_html_listed(card.external_actions, empty="None supplied."),
            "",
            "Approve only if these exact external effects are intended.",
            "",
            *_labelled_html_lines("Summary", card.summary),
            *_review_lines(card, html=True),
            *_asked_for_lines(card, html=True),
        ]
        if card.questions:
            lines.extend((
                "",
                "<b>Needs your input:</b>",
                *_html_question_lines(card.questions),
            ))
        if card.deliverables:
            # The effect being approved is often "send this". A reader
            # cannot judge that from a summary of it.
            lines.extend((
                "",
                "<b>Deliverables:</b>",
                *_html_drafted(card.deliverables),
            ))
        return lines
    if card.kind is ExecutionCardKind.RESULT_REVIEW:
        lines = [
            *_heading_lines(card, html=True),
            *details,
            "",
            *_labelled_html_lines("Outcome", str(card.outcome)),
            *_labelled_html_lines("Summary", card.summary),
            *_review_lines(card, html=True),
            *_asked_for_lines(card, html=True),
        ]
        if card.questions:
            lines.extend(("", "<b>Needs your input:</b>",
                          *_html_question_lines(card.questions)))
        if card.deliverables:
            lines.extend(("", "<b>Deliverables:</b>",
                          *_html_drafted(card.deliverables)))
        if card.work_markdown:
            lines.extend(("", "<b>Work:</b>", *_markdown_lines(card.work_markdown)))
        return lines
    lines = [
        *_heading_lines(card, html=True),
        *details,
        "",
        *_labelled_html_lines("Summary", card.summary),
        *_review_lines(card, html=True),
        *_asked_for_lines(card, html=True),
    ]
    if card.questions:
        lines.extend((
            "",
            "<b>Needs your input:</b>",
            *_html_question_lines(card.questions),
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
            *_html_drafted(card.deliverables),
        ))
    if card.work_markdown:
        lines.extend(("", "<b>Plan:</b>", *_markdown_lines(card.work_markdown)))
    return lines


def _html_listed(
    values: Sequence[CardRecord], *, empty: str = "None."
) -> list[str]:
    if not values:
        return [empty]
    lines: list[str] = []
    for record in values:
        segments = _escaped_source_lines(record.text)
        lines.append(f"• {segments[0]}")
        lines.extend(f"  {segment}" for segment in segments[1:])
        lines.extend(
            f"  {_escape(detail)}"
            for detail in _record_detail_lines(record)
        )
    return lines


def _html_question_lines(values: Sequence[str]) -> list[str]:
    lines: list[str] = []
    for value in values:
        segments = _escaped_source_lines(value)
        lines.append(f"• {segments[0]}")
        lines.extend(f"  {segment}" for segment in segments[1:])
    return lines


def _html_drafted(values: Sequence[CardRecord]) -> list[str]:
    """A prepared draft in full, so it can be judged without opening a file.

    The body goes in a <pre> block: it is someone else's text, often an
    email, and re-flowing it would misrepresent what would actually be
    sent.
    """
    lines: list[str] = []
    for record in values:
        if not record.structured:
            segments = _escaped_source_lines(record.text)
            lines.append(f"• {segments[0]}")
            lines.extend(f"  {segment}" for segment in segments[1:])
            continue
        heading = _escape(record.label or "Prepared draft")
        lines.extend(("", f"<b>{heading}</b>"))
        if record.recipient:
            lines.append("To: " + _escape(record.recipient))
        if record.subject:
            lines.append("Subject: " + _escape(record.subject))
        lines.append("<pre>" + _escape(record.text) + "</pre>")
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


_MAX_MARKDOWN_TABLE_COLUMNS = 12
_MAX_MARKDOWN_TABLE_ROWS = 200


def _markdown_table_cells(line: str) -> list[str] | None:
    """Split a table row while preserving pipes escaped or inside code."""
    text = line.strip()
    if "|" not in text:
        return None
    cells: list[str] = []
    cell: list[str] = []
    code_ticks = 0
    index = 0
    while index < len(text):
        char = text[index]
        if char == "\\" and index + 1 < len(text):
            following = text[index + 1]
            if following in {"\\", "|"}:
                cell.append(following)
                index += 2
                continue
        if char == "`":
            end = index + 1
            while end < len(text) and text[end] == "`":
                end += 1
            ticks = end - index
            if code_ticks == 0:
                code_ticks = ticks
            elif code_ticks == ticks:
                code_ticks = 0
            cell.append(text[index:end])
            index = end
            continue
        if char == "|" and code_ticks == 0:
            cells.append("".join(cell).strip())
            cell = []
        else:
            cell.append(char)
        index += 1
    cells.append("".join(cell).strip())
    if text.startswith("|"):
        cells.pop(0)
    backslashes = 0
    for char in reversed(text[:-1]):
        if char != "\\":
            break
        backslashes += 1
    if text.endswith("|") and backslashes % 2 == 0:
        cells.pop()
    return cells if len(cells) >= 2 else None


def _markdown_table_alignment(cell: str) -> str | None:
    marker = cell.strip()
    if not re.fullmatch(r":?-{3,}:?", marker):
        return None
    if marker.startswith(":") and marker.endswith(":"):
        return "center"
    if marker.endswith(":"):
        return "right"
    return "left"


def _markdown_table_cell_text(cell: str) -> str:
    """Flatten inline Markdown before measuring a monospace table cell."""
    text = re.sub(r"\[([^\]]+)\]\(([^)\s]+)\)", r"\1", cell)
    text = re.sub(r"`([^`]*)`", r"\1", text)
    text = re.sub(
        r"\*\*([^*]+)\*\*|__([^_]+)__",
        lambda match: match.group(1) or match.group(2),
        text,
    )
    text = re.sub(r"(?<!\*)\*(?!\s)([^*\n]+?)\*(?!\*)", r"\1", text)
    return text.strip()


def _pad_markdown_table_cell(text: str, width: int, alignment: str) -> str:
    room = width - len(text)
    if alignment == "right":
        return " " * room + text
    if alignment == "center":
        left = room // 2
        return " " * left + text + " " * (room - left)
    return text + " " * room


def _markdown_table(
    source_lines: list[str], start: int
) -> tuple[str, int] | None:
    """Render one bounded Markdown table and return its first unused line."""
    if start + 1 >= len(source_lines):
        return None
    header = _markdown_table_cells(source_lines[start])
    delimiter = _markdown_table_cells(source_lines[start + 1])
    if header is None or delimiter is None or len(header) != len(delimiter):
        return None
    if not 2 <= len(header) <= _MAX_MARKDOWN_TABLE_COLUMNS:
        return None
    if any(
        len(cell) > MAX_RENDER_SOURCE_LINE_CHARS
        for cell in [*header, *delimiter]
    ):
        return None
    alignments = [_markdown_table_alignment(cell) for cell in delimiter]
    if any(alignment is None for alignment in alignments):
        return None

    rows: list[list[str]] = []
    end = start + 2
    while end < len(source_lines):
        row = _markdown_table_cells(source_lines[end])
        if row is None or len(row) > len(header):
            break
        if len(rows) >= _MAX_MARKDOWN_TABLE_ROWS or any(
            len(cell) > MAX_RENDER_SOURCE_LINE_CHARS for cell in row
        ):
            return None
        rows.append(row + [""] * (len(header) - len(row)))
        end += 1

    rendered_rows = [list(map(_markdown_table_cell_text, header))]
    rendered_rows.extend(
        [list(map(_markdown_table_cell_text, row)) for row in rows]
    )
    widths = [
        max(3, *(len(row[column]) for row in rendered_rows))
        for column in range(len(header))
    ]
    resolved_alignments = [
        alignment for alignment in alignments if alignment is not None
    ]

    def render(row: list[str]) -> str:
        return " │ ".join(
            _pad_markdown_table_cell(
                value, widths[column], resolved_alignments[column]
            )
            for column, value in enumerate(row)
        ).rstrip()

    separator = "─┼─".join("─" * width for width in widths)
    display = [render(rendered_rows[0]), separator]
    display.extend(render(row) for row in rendered_rows[1:])
    escaped = _escape("\n".join(display))
    return f"<pre>{escaped}</pre>", end


def _markdown_lines(value: str) -> list[str]:
    lines: list[str] = []
    source_lines = value.split("\n")
    cursor = 0
    while cursor < len(source_lines):
        table = _markdown_table(source_lines, cursor)
        if table is not None:
            rendered, cursor = table
            lines.append(rendered)
            continue
        raw = source_lines[cursor]
        cursor += 1
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
    card: ExecutionReviewCard, *, approvable: bool
) -> tuple[tuple[tuple[str, str], ...], ...]:
    kind = card.kind
    if kind is ExecutionCardKind.START:
        rows: tuple[tuple[tuple[str, str], ...], ...] = (
            (("✅ Done", "done"), ("▶️ Continue", "start")),
            (("🗑 Drop", "drop"), ("✏️ Update", "discuss")),
            (("🕓 Snooze", "snooze"), ("👥 Reassign", "reassign")),
        )
        if card.owner_hold_eligible and card.owner:
            rows += ((
                (_owner_hold_button_label(card.owner), OWNER_HOLD_ACTION),
            ),)
        if card.workflow_status is WorkflowStatus.AWAITING_START:
            # Only before it starts: once a workflow is running, changing
            # the agent underneath it would rebind work already in flight.
            rows += ((("🤖 Agent", "agent"),),)
        return rows if approvable else rows[1:]
    stop_row = (("👥 Reassign", "reassign"), ("🗑 Drop task", "drop"))
    if kind is ExecutionCardKind.EXTERNAL_REVIEW:
        rows = (
            (("✅ Authorize action", "approve"), ("⛔ Not now", "revise")),
            (("🕒 Snooze", "snooze"),),
            (("💬 Discuss", "discuss"), ("✅ Mark as done", "done")),
            stop_row,
        )
    elif kind is ExecutionCardKind.RESULT_REVIEW:
        rows = (
            (("✅ Mark as done", "done"),),
            (("💬 Discuss", "discuss"), ("🕒 Snooze", "snooze")),
            stop_row,
        )
    else:
        rows = (
            (("🔎 Investigate further", "revise"), ("💬 Discuss", "discuss")),
            (("▶️ Execute plan", "approve"), ("🕒 Snooze", "snooze")),
            (("✅ Mark as done", "done"),),
            stop_row,
        )
    if approvable:
        return rows
    return tuple(
        tuple(button for button in row if button[1] not in {"approve", "done"})
        for row in rows
        if any(button[1] not in {"approve", "done"} for button in row)
    )


def _owner_hold_button_label(owner: str) -> str:
    prefix = "🗓 Until next meeting with "
    maximum = 64
    if len((prefix + owner).encode("utf-8")) <= maximum:
        return prefix + owner
    suffix = "…"
    budget = maximum - len((prefix + suffix).encode("utf-8"))
    encoded = owner.encode("utf-8")[:budget]
    while encoded:
        try:
            shortened = encoded.decode("utf-8")
            break
        except UnicodeDecodeError:
            encoded = encoded[:-1]
    else:
        shortened = ""
    return prefix + shortened.rstrip() + suffix


def _direct_actions_for_kind(kind: ExecutionCardKind) -> set[str]:
    if kind is ExecutionCardKind.START:
        # The intervals the review cards already accept. A gate is the card
        # most likely to be deferred, because it is asked before any work
        # has been done, and "tomorrow" is rarely the right answer for a
        # task waiting on someone else, a release, or a month end.
        return {
            "start", "snooze", "cancel", "done", "drop",
            OWNER_HOLD_ACTION,
            *REVIEW_SNOOZE_INTERVALS,
        }
    if kind is ExecutionCardKind.RESULT_REVIEW:
        return {"done", "drop", *REVIEW_SNOOZE_INTERVALS}
    return {
        "approve", "revise", "cancel", "done", "drop",
        *REVIEW_SNOOZE_INTERVALS,
    }


def _stored_action(action: str) -> str:
    return (
        "snooze"
        if action in {*REVIEW_SNOOZE_INTERVALS, OWNER_HOLD_ACTION}
        else action
    )


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


def _agent_callback(card_id: int, version: int, token: str) -> str:
    if not _valid_agent_selection_token(token):
        raise TaskLedgerError("execution agent callback is invalid")
    value = f"{AGENT_CALLBACK_PREFIX}|{card_id}|{version}|{token}"
    if len(value.encode("utf-8")) > CALLBACK_DATA_LIMIT:
        raise TaskLedgerError("execution agent callback is too large")
    return value


def _agent_selection_token(profile: AgentProfile) -> str:
    source = f"{profile.profile_id}\0{profile.revision}".encode("utf-8")
    return hashlib.sha256(source).hexdigest()[:AGENT_SELECTION_TOKEN_CHARS]


def _valid_agent_selection_token(value: object) -> bool:
    return bool(
        isinstance(value, str)
        and re.fullmatch(
            rf"[0-9a-f]{{{AGENT_SELECTION_TOKEN_CHARS}}}", value
        )
    )


def _eligible_profiles(
    registry: AgentProfileRegistry,
) -> tuple[AgentProfile, ...]:
    return tuple(
        profile
        for profile in registry.list()
        if WorkflowPhase.PLAN.value in profile.allowed_phases
    )


def _agent_options(
    registry: AgentProfileRegistry, card: ExecutionReviewCard
) -> tuple[ExecutionAgentOption, ...]:
    options = tuple(
        ExecutionAgentOption(
            profile.display_name,
            _agent_selection_token(profile),
            selected=(
                profile.profile_id == card.agent_profile_id
                and profile.revision == card.agent_profile_revision
            ),
        )
        for profile in _eligible_profiles(registry)
    )
    tokens = [option.selection_token for option in options]
    if not options or len(tokens) != len(set(tokens)):
        raise TaskLedgerError("execution agent choices are unavailable")
    selected = sum(option.selected for option in options)
    if selected > 1:
        raise TaskLedgerError("selected execution agent is unavailable")
    if selected == 0:
        try:
            historical = registry.resolve(
                card.agent_profile_id, card.agent_profile_revision
            )
        except AgentProfileError as exc:
            raise TaskLedgerError(
                "selected execution agent is unavailable"
            ) from exc
        current = registry.get(card.agent_profile_id)
        if current is None or historical.revision == current.revision:
            raise TaskLedgerError("selected execution agent is unavailable")
    return options


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


def _agent_card_refusal(
    row: Mapping[str, object] | None, expected_version: int
) -> ExecutionCardRefusal | None:
    refusal = _card_guard(row, expected_version)
    if refusal is not None:
        return refusal
    if row["status"] != ExecutionCardStatus.DELIVERED:
        return ExecutionCardRefusal.INVALID_STATE
    if not _current_card(row):
        return ExecutionCardRefusal.STALE_VERSION
    if ExecutionCardKind(row["kind"]) is not ExecutionCardKind.START:
        return ExecutionCardRefusal.INVALID_ACTION
    if row["workflow_status_current"] != WorkflowStatus.AWAITING_START:
        return ExecutionCardRefusal.INVALID_STATE
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


def _agent_refused(
    card_id: object, refusal: ExecutionCardRefusal
) -> ExecutionAgentSelectorResult:
    return ExecutionAgentSelectorResult(
        ExecutionCardDisposition.REFUSED,
        card_id if isinstance(card_id, int) and not isinstance(card_id, bool) else 0,
        refusal=refusal,
    )


def _agent_refused_row(
    card_id: int,
    row: Mapping[str, object] | None,
    refusal: ExecutionCardRefusal,
) -> ExecutionAgentSelectorResult:
    if row is None:
        return _agent_refused(card_id, refusal)
    return ExecutionAgentSelectorResult(
        ExecutionCardDisposition.REFUSED,
        card_id,
        card_version=int(row["version"]),
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
