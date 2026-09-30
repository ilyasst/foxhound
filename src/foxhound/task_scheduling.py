"""Bounded local scheduling conditions and reversible change sets.

This module is the only mutation boundary expected by future Researcher
integration.  It accepts a *trusted, already validated* recommendation object;
models and retrieved documents never receive database or queue tools.
"""

from __future__ import annotations

import hashlib
import html
import json
import re
import secrets
import sqlite3
from contextlib import closing
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import StrEnum
from pathlib import Path
from typing import Callable, Sequence

from .candidate_inbox import CandidateInbox, SCHEMA_VERSION
from .task_ledger import TaskLedgerError


MAX_ACTIVE_CONDITIONS = 3
MAX_PREREQUISITE_EDGES = 2
MAX_DEPENDENCY_DEPTH = 8
MAX_RATIONALE_CHARS = 2_000
MAX_PREREQUISITE_TEXT_CHARS = 4_000
MAX_SOURCE_REFS = 16
MAX_NOT_BEFORE_DAYS = 365
CALLBACK_PREFIX = "fhs"
CALLBACK_DATA_LIMIT = 64
_DIGEST_RE = re.compile(r"^[0-9a-f]{64}$")
_SOURCE_REF_RE = re.compile(r"^src-[0-9]{3}$")


class SchedulingKind(StrEnum):
    AFTER_TASK_COMPLETED = "after_task_completed"
    NOT_BEFORE = "not_before"
    RAISE_PRIORITY = "raise_priority"
    CREATE_PREREQUISITE = "create_prerequisite"


class SchedulingDisposition(StrEnum):
    APPLIED = "applied"
    UNCHANGED = "unchanged"
    REFUSED = "refused"


class SchedulingRefusal(StrEnum):
    INVALID_ARGUMENT = "invalid_argument"
    INVALID_RECOMMENDATION = "invalid_recommendation"
    NOT_FOUND = "not_found"
    STALE_TASK = "stale_task"
    STALE_WORKFLOW = "stale_workflow"
    INVALID_STATE = "invalid_state"
    LIMIT_EXCEEDED = "limit_exceeded"
    CYCLE = "cycle"
    DEPTH_EXCEEDED = "depth_exceeded"
    MANUAL_PRIORITY = "manual_priority"
    STALE_CARD = "stale_card"
    UNDO_CONFLICT = "undo_conflict"
    CLAIM_MISMATCH = "claim_mismatch"


@dataclass(frozen=True)
class ValidatedSchedulingRecommendation:
    """Trusted boundary object produced by deterministic policy validation."""

    kind: SchedulingKind
    target_task_id: int
    target_task_version: int
    expected_workflow_version: int
    rationale: str
    source_refs: Sequence[str]
    research_receipt_id: str
    research_document_digest: str
    related_task_id: int | None = None
    not_before: str | None = None
    prerequisite_text: str | None = None


@dataclass(frozen=True)
class SchedulingApplyResult:
    disposition: SchedulingDisposition
    refusal: SchedulingRefusal | None = None
    change_set_id: int | None = None
    card_id: int | None = None
    created_task_id: int | None = None
    resulting_workflow_version: int | None = None

    @property
    def accepted(self) -> bool:
        return self.disposition is not SchedulingDisposition.REFUSED


@dataclass(frozen=True)
class SchedulingCardActionResult:
    disposition: SchedulingDisposition
    refusal: SchedulingRefusal | None = None
    card_id: int | None = None
    card_version: int | None = None

    @property
    def accepted(self) -> bool:
        return self.disposition is not SchedulingDisposition.REFUSED


@dataclass(frozen=True)
class SchedulingReviewCard:
    card_id: int
    change_set_id: int
    task_id: int
    version: int
    kind: SchedulingKind
    rationale: str
    related_task_id: int | None
    not_before: str | None
    prerequisite_text: str | None


@dataclass(frozen=True)
class SchedulingDeliveryClaim:
    card: SchedulingReviewCard
    token: str = field(repr=False)
    expires_at: str


class TaskSchedulingService:
    """Apply bounded scheduling recommendations and their exact inverses."""

    def __init__(
        self,
        database_path: Path | str,
        *,
        clock: Callable[[], datetime] | None = None,
        token_factory: Callable[[], str] | None = None,
        provenance_validator: Callable[
            [sqlite3.Connection, ValidatedSchedulingRecommendation], bool
        ] | None = None,
    ) -> None:
        self.database_path = Path(database_path)
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._token_factory = token_factory or (lambda: secrets.token_urlsafe(32))
        self._provenance_validator = provenance_validator

    def apply(
        self, recommendation: ValidatedSchedulingRecommendation,
    ) -> SchedulingApplyResult:
        document = _recommendation_document(recommendation)
        if document is None:
            return _refused(SchedulingRefusal.INVALID_RECOMMENDATION)
        payload = json.dumps(
            document, ensure_ascii=True, separators=(",", ":"), sort_keys=True
        )
        digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()
        now = self._now()
        now_value = _timestamp(now)
        if recommendation.kind is SchedulingKind.NOT_BEFORE:
            requested = _timestamp(recommendation.not_before)
            if requested <= now_value or requested > now_value + timedelta(days=MAX_NOT_BEFORE_DAYS):
                return _refused(SchedulingRefusal.INVALID_RECOMMENDATION)
            normalized = requested.astimezone(timezone.utc).isoformat(timespec="seconds")
            document["not_before"] = normalized
            payload = json.dumps(
                document, ensure_ascii=True, separators=(",", ":"), sort_keys=True
            )
            digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()
        with closing(self._connect()) as connection:
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute("BEGIN IMMEDIATE")
            try:
                if (
                    self._provenance_validator is None
                    or not self._provenance_validator(connection, recommendation)
                ):
                    connection.rollback()
                    return _refused(SchedulingRefusal.INVALID_RECOMMENDATION)
                existing = connection.execute(
                    "SELECT id,resulting_workflow_version FROM "
                    "task_scheduling_change_sets WHERE research_receipt_id=? "
                    "AND recommendation_digest=? ORDER BY id LIMIT 1",
                    (recommendation.research_receipt_id, digest),
                ).fetchone()
                if existing is not None:
                    card = connection.execute(
                        "SELECT id FROM task_scheduling_review_cards "
                        "WHERE change_set_id=?",
                        (int(existing["id"]),),
                    ).fetchone()
                    connection.commit()
                    return SchedulingApplyResult(
                        SchedulingDisposition.UNCHANGED,
                        change_set_id=int(existing["id"]),
                        card_id=None if card is None else int(card["id"]),
                        resulting_workflow_version=int(
                            existing["resulting_workflow_version"]
                        ),
                    )
                target = connection.execute(
                    "SELECT t.status AS task_status,t.version AS task_version,w.* "
                    "FROM tasks AS t LEFT JOIN task_execution_workflows AS w "
                    "ON w.task_id=t.id WHERE t.id=?",
                    (recommendation.target_task_id,),
                ).fetchone()
                refusal = _target_refusal(target, recommendation)
                if refusal is not None:
                    connection.rollback()
                    return _refused(refusal)

                active = int(connection.execute(
                    "SELECT count(*) FROM task_scheduling_conditions "
                    "WHERE task_id=? AND state IN ('active','needs_review')",
                    (recommendation.target_task_id,),
                ).fetchone()[0])
                adds_condition = recommendation.kind in {
                    SchedulingKind.AFTER_TASK_COMPLETED,
                    SchedulingKind.NOT_BEFORE,
                    SchedulingKind.CREATE_PREREQUISITE,
                }
                if adds_condition and active >= MAX_ACTIVE_CONDITIONS:
                    connection.rollback()
                    return _refused(SchedulingRefusal.LIMIT_EXCEEDED)

                if recommendation.kind is SchedulingKind.NOT_BEFORE:
                    existing = connection.execute(
                        "SELECT 1 FROM task_scheduling_conditions WHERE task_id=? "
                        "AND kind='not_before' AND state IN ('active','needs_review')",
                        (recommendation.target_task_id,),
                    ).fetchone()
                    if existing is not None:
                        connection.rollback()
                        return _refused(SchedulingRefusal.LIMIT_EXCEEDED)

                related = recommendation.related_task_id
                if recommendation.kind is SchedulingKind.AFTER_TASK_COMPLETED:
                    refusal = _dependency_refusal(
                        connection, recommendation.target_task_id, related
                    )
                    if refusal is not None:
                        connection.rollback()
                        return _refused(refusal)

                prior_priority: str | None = None
                result_version = recommendation.expected_workflow_version
                created_task_id: int | None = None
                if recommendation.kind is SchedulingKind.RAISE_PRIORITY:
                    # Any non-normal current preference is reader/manual state.
                    # Automated research may never overwrite it.
                    if (
                        target["queue_priority"] != "normal"
                        or target["queue_priority_source"] is not None
                    ):
                        connection.rollback()
                        return _refused(SchedulingRefusal.MANUAL_PRIORITY)
                    prior_priority = "normal"
                    result_version += 1
                    changed = connection.execute(
                        "UPDATE task_execution_workflows SET queue_priority='raised',"
                        "queue_priority_source='automation',"
                        "version=?,updated_at=? WHERE task_id=? AND version=? "
                        "AND status='queued' AND queue_priority='normal' "
                        "AND (next_attempt_at IS NULL OR next_attempt_at<=?)",
                        (
                            result_version, now, recommendation.target_task_id,
                            recommendation.expected_workflow_version, now,
                        ),
                    )
                    if changed.rowcount != 1:
                        connection.rollback()
                        return _refused(SchedulingRefusal.INVALID_STATE)
                    _execution_priority_event(
                        connection, recommendation.target_task_id,
                        result_version, "priority_raised", now,
                    )

                if recommendation.kind is SchedulingKind.CREATE_PREREQUISITE:
                    created_task_id = _create_prerequisite(
                        connection, recommendation.prerequisite_text or "", now,
                        target,
                    )
                    refusal = _dependency_refusal(
                        connection, recommendation.target_task_id, created_task_id
                    )
                    if refusal is not None:
                        connection.rollback()
                        return _refused(refusal)
                    related = created_task_id

                change = connection.execute(
                    "INSERT INTO task_scheduling_change_sets("
                    "target_task_id,target_task_version,expected_workflow_version,"
                    "kind,state,research_receipt_id,research_document_digest,"
                    "recommendation_digest,recommendation_json,prior_priority,"
                    "prior_priority_source,resulting_workflow_version,created_task_id,"
                    "created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        recommendation.target_task_id,
                        recommendation.target_task_version,
                        recommendation.expected_workflow_version,
                        recommendation.kind.value, "active",
                        recommendation.research_receipt_id,
                        recommendation.research_document_digest,
                        digest, payload, prior_priority,
                        target["queue_priority_source"], result_version,
                        created_task_id, now, now,
                    ),
                )
                change_set_id = int(change.lastrowid)

                if adds_condition:
                    condition_kind = (
                        "not_before" if recommendation.kind is SchedulingKind.NOT_BEFORE
                        else "after_task_completed"
                    )
                    condition = connection.execute(
                        "INSERT INTO task_scheduling_conditions("
                        "task_id,task_version,kind,depends_on_task_id,not_before,"
                        "state,change_set_id,created_at,updated_at) "
                        "VALUES(?,?,?,?,?,'active',?,?,?)",
                        (
                            recommendation.target_task_id,
                            recommendation.target_task_version,
                            condition_kind,
                            related,
                            document["not_before"],
                            change_set_id,
                            now,
                            now,
                        ),
                    )
                    _condition_event(
                        connection, int(condition.lastrowid),
                        recommendation.target_task_id, "created", "active", now,
                    )

                connection.execute(
                    "INSERT INTO task_scheduling_change_events("
                    "change_set_id,task_id,kind,occurred_at) VALUES(?,?,?,?)",
                    (change_set_id, recommendation.target_task_id, "applied", now),
                )
                card = connection.execute(
                    "INSERT INTO task_scheduling_review_cards("
                    "change_set_id,task_id,status,version,resolution,created_at,"
                    "updated_at,resolved_at) VALUES(?,?,'pending',1,NULL,?,?,NULL)",
                    (change_set_id, recommendation.target_task_id, now, now),
                )
                card_id = int(card.lastrowid)
                _card_event(
                    connection, card_id, change_set_id,
                    recommendation.target_task_id, "scheduled", 1, None, now,
                )
                connection.commit()
                return SchedulingApplyResult(
                    SchedulingDisposition.APPLIED,
                    change_set_id=change_set_id,
                    card_id=card_id,
                    created_task_id=created_task_id,
                    resulting_workflow_version=result_version,
                )
            except Exception:
                connection.rollback()
                raise

    def claim_next(
        self, *, consumer_digest: str, lease_seconds: int = 60,
    ) -> SchedulingDeliveryClaim | None:
        if (
            isinstance(lease_seconds, bool)
            or not isinstance(lease_seconds, int)
            or not 5 <= lease_seconds <= 300
        ):
            raise TaskLedgerError("scheduling card delivery lease is invalid")
        if not _valid_digest(consumer_digest):
            raise TaskLedgerError("scheduling card consumer digest is invalid")
        stamp = self._clock()
        if stamp.tzinfo is None or stamp.utcoffset() is None:
            raise TaskLedgerError("task scheduling clock must include a timezone")
        now = stamp.isoformat(timespec="seconds")
        expires = (stamp + timedelta(seconds=lease_seconds)).isoformat(
            timespec="seconds"
        )
        token = self._token_factory()
        if not _valid_secret(token):
            raise TaskLedgerError("scheduling card token factory returned invalid state")
        digest = _token_digest(token)
        with closing(self._connect()) as connection:
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute("BEGIN IMMEDIATE")
            try:
                expired = connection.execute(
                    "SELECT id,change_set_id,task_id,version FROM "
                    "task_scheduling_review_cards WHERE status='delivering' "
                    "AND claim_expires_at<=? ORDER BY id",
                    (now,),
                ).fetchall()
                for row in expired:
                    version = int(row["version"]) + 1
                    changed = connection.execute(
                        "UPDATE task_scheduling_review_cards SET status='pending',"
                        "version=?,claim_token_digest=NULL,claim_expires_at=NULL,"
                        "consumer_digest=NULL,updated_at=? WHERE id=? AND version=? "
                        "AND status='delivering'",
                        (version, now, int(row["id"]), int(row["version"])),
                    )
                    if changed.rowcount == 1:
                        _card_event(
                            connection, int(row["id"]), int(row["change_set_id"]),
                            int(row["task_id"]), "delivery_expired", version, None, now,
                        )
                row = connection.execute(
                    "SELECT c.*,s.kind,s.recommendation_json FROM "
                    "task_scheduling_review_cards c JOIN task_scheduling_change_sets s "
                    "ON s.id=c.change_set_id WHERE c.status='pending' "
                    "ORDER BY c.created_at,c.id LIMIT 1"
                ).fetchone()
                if row is None:
                    connection.commit()
                    return None
                version = int(row["version"]) + 1
                changed = connection.execute(
                    "UPDATE task_scheduling_review_cards SET status='delivering',"
                    "version=?,claim_token_digest=?,claim_expires_at=?,"
                    "consumer_digest=?,updated_at=? WHERE id=? AND version=? "
                    "AND status='pending'",
                    (version, digest, expires, consumer_digest, now,
                     int(row["id"]), int(row["version"])),
                )
                if changed.rowcount != 1:
                    connection.rollback()
                    return None
                _card_event(
                    connection, int(row["id"]), int(row["change_set_id"]),
                    int(row["task_id"]), "delivery_claimed", version, None, now,
                )
                connection.commit()
                return SchedulingDeliveryClaim(
                    card=_review_card(row, version=version), token=token,
                    expires_at=expires,
                )
            except Exception:
                connection.rollback()
                raise

    def complete_delivery(
        self, card_id: int, *, expected_version: int, claim_token: str,
        transport: str, delivery_ref: str,
    ) -> SchedulingCardActionResult:
        if (
            not _positive_id(card_id) or not _positive_id(expected_version)
            or not _valid_secret(claim_token) or not _valid_opaque(transport, 64)
            or not _valid_opaque(delivery_ref, 200)
        ):
            return _card_refused(card_id, SchedulingRefusal.INVALID_ARGUMENT)
        digest = _token_digest(claim_token)
        now = self._now()
        with closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = connection.execute(
                    "SELECT * FROM task_scheduling_review_cards WHERE id=?",
                    (card_id,),
                ).fetchone()
                if row is None:
                    connection.rollback()
                    return _card_refused(card_id, SchedulingRefusal.NOT_FOUND)
                if int(row["version"]) != expected_version:
                    connection.rollback()
                    return _card_refused(card_id, SchedulingRefusal.STALE_CARD)
                if row["status"] == "delivered":
                    connection.rollback()
                    if (
                        row["transport"] == transport
                        and row["delivery_ref"] == delivery_ref
                    ):
                        return SchedulingCardActionResult(
                            SchedulingDisposition.UNCHANGED, card_id=card_id,
                            card_version=expected_version,
                        )
                    return _card_refused(
                        card_id, SchedulingRefusal.INVALID_STATE
                    )
                refusal = _delivery_guard(row, expected_version, digest, now)
                if refusal is not None:
                    connection.rollback()
                    return _card_refused(card_id, refusal)
                changed = connection.execute(
                    "UPDATE task_scheduling_review_cards SET status='delivered',"
                    "claim_token_digest=NULL,claim_expires_at=NULL,transport=?,"
                    "delivery_ref=?,delivered_at=?,updated_at=? WHERE id=? "
                    "AND version=? AND status='delivering'",
                    (transport, delivery_ref, now, now, card_id, expected_version),
                )
                if changed.rowcount != 1:
                    connection.rollback()
                    return _card_refused(card_id, SchedulingRefusal.STALE_CARD)
                _card_event(
                    connection, card_id, int(row["change_set_id"]),
                    int(row["task_id"]), "delivered", expected_version, None, now,
                )
                connection.commit()
                return SchedulingCardActionResult(
                    SchedulingDisposition.APPLIED, card_id=card_id,
                    card_version=expected_version,
                )
            except Exception:
                connection.rollback()
                raise

    def fail_delivery(
        self, card_id: int, *, expected_version: int, claim_token: str,
    ) -> SchedulingCardActionResult:
        if (
            not _positive_id(card_id) or not _positive_id(expected_version)
            or not _valid_secret(claim_token)
        ):
            return _card_refused(card_id, SchedulingRefusal.INVALID_ARGUMENT)
        digest = _token_digest(claim_token)
        now = self._now()
        with closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = connection.execute(
                    "SELECT * FROM task_scheduling_review_cards WHERE id=?",
                    (card_id,),
                ).fetchone()
                refusal = _delivery_guard(row, expected_version, digest, now)
                if refusal is not None:
                    connection.rollback()
                    return _card_refused(card_id, refusal)
                version = expected_version + 1
                changed = connection.execute(
                    "UPDATE task_scheduling_review_cards SET status='pending',"
                    "version=?,claim_token_digest=NULL,claim_expires_at=NULL,"
                    "consumer_digest=NULL,updated_at=? WHERE id=? AND version=? "
                    "AND status='delivering'",
                    (version, now, card_id, expected_version),
                )
                if changed.rowcount != 1:
                    connection.rollback()
                    return _card_refused(card_id, SchedulingRefusal.STALE_CARD)
                _card_event(
                    connection, card_id, int(row["change_set_id"]),
                    int(row["task_id"]), "delivery_failed", version, None, now,
                )
                connection.commit()
                return SchedulingCardActionResult(
                    SchedulingDisposition.APPLIED, card_id=card_id,
                    card_version=version,
                )
            except Exception:
                connection.rollback()
                raise

    def act(
        self, card_id: int, *, expected_version: int, action: str,
    ) -> SchedulingCardActionResult:
        if (
            isinstance(card_id, bool) or not isinstance(card_id, int) or card_id < 1
            or isinstance(expected_version, bool)
            or not isinstance(expected_version, int) or expected_version < 1
            or action not in {"undo", "keep"}
        ):
            return _card_refused(card_id, SchedulingRefusal.INVALID_RECOMMENDATION)
        now = self._now()
        with closing(self._connect()) as connection:
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = connection.execute(
                    "SELECT c.*,s.target_task_version,s.expected_workflow_version,"
                    "s.kind,s.state AS change_state,s.prior_priority,"
                    "s.prior_priority_source,"
                    "s.resulting_workflow_version,s.created_task_id "
                    "FROM task_scheduling_review_cards AS c "
                    "JOIN task_scheduling_change_sets AS s ON s.id=c.change_set_id "
                    "WHERE c.id=?",
                    (card_id,),
                ).fetchone()
                if row is None:
                    connection.rollback()
                    return _card_refused(card_id, SchedulingRefusal.NOT_FOUND)
                if row["status"] != "delivered":
                    connection.rollback()
                    return _card_refused(card_id, SchedulingRefusal.INVALID_STATE)
                if int(row["version"]) != expected_version:
                    connection.rollback()
                    return _card_refused(card_id, SchedulingRefusal.STALE_CARD)
                if action == "keep":
                    version = expected_version + 1
                    _resolve_card(connection, row, version, "keep", now)
                    connection.commit()
                    return SchedulingCardActionResult(
                        SchedulingDisposition.APPLIED, card_id=card_id,
                        card_version=version,
                    )

                conflict = _undo_conflict(connection, row)
                if conflict:
                    connection.execute(
                        "UPDATE task_scheduling_change_sets SET state='undo_conflict',"
                        "updated_at=? WHERE id=? AND state='active'",
                        (now, int(row["change_set_id"])),
                    )
                    connection.execute(
                        "INSERT INTO task_scheduling_change_events("
                        "change_set_id,task_id,kind,occurred_at) VALUES(?,?,?,?)",
                        (int(row["change_set_id"]), int(row["task_id"]),
                         "undo_conflict", now),
                    )
                    _card_event(
                        connection, card_id, int(row["change_set_id"]),
                        int(row["task_id"]), "undo_conflict", expected_version,
                        "undo", now,
                    )
                    connection.commit()
                    return _card_refused(card_id, SchedulingRefusal.UNDO_CONFLICT)

                connection.execute(
                    "UPDATE task_scheduling_conditions SET state='canceled',"
                    "updated_at=? WHERE change_set_id=? AND state IN "
                    "('active','satisfied','needs_review')",
                    (now, int(row["change_set_id"])),
                )
                for condition in connection.execute(
                    "SELECT id,task_id FROM task_scheduling_conditions "
                    "WHERE change_set_id=? AND state='canceled'",
                    (int(row["change_set_id"]),),
                ):
                    _condition_event(
                        connection, int(condition["id"]), int(condition["task_id"]),
                        "canceled", "canceled", now,
                    )
                if row["kind"] == SchedulingKind.RAISE_PRIORITY:
                    next_version = int(row["resulting_workflow_version"]) + 1
                    connection.execute(
                        "UPDATE task_execution_workflows SET queue_priority=?,"
                        "queue_priority_source=?,version=?,updated_at=? "
                        "WHERE task_id=? AND version=?",
                        (
                            row["prior_priority"], row["prior_priority_source"],
                            next_version, now,
                            int(row["task_id"]),
                            int(row["resulting_workflow_version"]),
                        ),
                    )
                    _execution_priority_event(
                        connection, int(row["task_id"]), next_version,
                        "priority_cleared", now,
                    )
                if row["created_task_id"] is not None:
                    created = int(row["created_task_id"])
                    created_workflow = connection.execute(
                        "SELECT * FROM task_execution_workflows WHERE task_id=?",
                        (created,),
                    ).fetchone()
                    if created_workflow is not None:
                        _cancel_prerequisite_cards(
                            connection, created,
                            int(created_workflow["version"]), now,
                        )
                        canceled_version = int(created_workflow["version"]) + 1
                        connection.execute(
                            "UPDATE task_execution_workflows SET status='cancelled',"
                            "version=?,updated_at=?,completed_at=? WHERE task_id=? "
                            "AND version=?",
                            (
                                canceled_version, now, now, created,
                                int(created_workflow["version"]),
                            ),
                        )
                        connection.execute(
                            "INSERT INTO task_execution_events(task_id,kind,"
                            "workflow_version,task_version,phase,status,occurred_at,"
                            "agent_profile_id,agent_profile_revision) "
                            "VALUES(?,'cancelled',?,1,?,'cancelled',?,?,?)",
                            (
                                created, canceled_version,
                                created_workflow["phase"], now,
                                created_workflow["agent_profile_id"],
                                created_workflow["agent_profile_revision"],
                            ),
                        )
                    connection.execute(
                        "UPDATE tasks SET status='dropped',version=2,updated_at=?,"
                        "closed_at=? WHERE id=? AND status='open' AND version=1",
                        (now, now, created),
                    )
                    connection.execute(
                        "INSERT INTO task_events(task_id,kind,task_version,"
                        "candidate_id,source_revision,from_status,to_status,"
                        "occurred_at) VALUES(?,'status_changed',2,NULL,NULL,"
                        "'open','dropped',?)",
                        (created, now),
                    )
                connection.execute(
                    "UPDATE task_scheduling_change_sets SET state='undone',"
                    "updated_at=? WHERE id=? AND state='active'",
                    (now, int(row["change_set_id"])),
                )
                connection.execute(
                    "INSERT INTO task_scheduling_change_events("
                    "change_set_id,task_id,kind,occurred_at) VALUES(?,?,?,?)",
                    (int(row["change_set_id"]), int(row["task_id"]), "undone", now),
                )
                version = expected_version + 1
                _resolve_card(connection, row, version, "undo", now)
                connection.commit()
                return SchedulingCardActionResult(
                    SchedulingDisposition.APPLIED,
                    card_id=card_id,
                    card_version=version,
                )
            except Exception:
                connection.rollback()
                raise

    def _connect(self) -> sqlite3.Connection:
        if not self.database_path.is_file() or self.database_path.is_symlink():
            raise TaskLedgerError("task scheduling database is not initialized")
        connection = sqlite3.connect(self.database_path, timeout=5)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout = 5000")
        if int(connection.execute("PRAGMA user_version").fetchone()[0]) != SCHEMA_VERSION:
            connection.close()
            raise TaskLedgerError("task scheduling schema is not supported")
        CandidateInbox._require_schema(connection)
        return connection

    def _now(self) -> str:
        value = self._clock()
        if value.tzinfo is None or value.utcoffset() is None:
            raise TaskLedgerError("task scheduling clock must include a timezone")
        return value.isoformat(timespec="seconds")


def evaluate_task_conditions(
    connection: sqlite3.Connection, task_id: int, task_version: int, now: str,
) -> bool:
    """Refresh conditions and return claim eligibility; errors fail closed."""
    moment = _timestamp(now)
    rows = connection.execute(
        "SELECT c.*,d.status AS dependency_status FROM task_scheduling_conditions c "
        "LEFT JOIN tasks d ON d.id=c.depends_on_task_id "
        "WHERE c.task_id=? AND c.state IN ('active','needs_review') ORDER BY c.id",
        (task_id,),
    ).fetchall()
    eligible = True
    for row in rows:
        state = row["state"]
        if int(row["task_version"]) != task_version:
            if state != "needs_review":
                _set_condition_state(connection, row, "needs_review", now)
            eligible = False
            continue
        if row["kind"] == "not_before":
            if moment >= _timestamp(row["not_before"]):
                if state != "satisfied":
                    _set_condition_state(connection, row, "satisfied", now)
            else:
                eligible = False
        elif row["kind"] == "after_task_completed":
            if row["dependency_status"] == "done":
                if state != "satisfied":
                    _set_condition_state(connection, row, "satisfied", now)
            elif row["dependency_status"] == "open":
                eligible = False
            else:
                if state != "needs_review":
                    _set_condition_state(connection, row, "needs_review", now)
                eligible = False
        else:  # Defensive fail-closed guard beyond the SQL CHECK.
            raise TaskLedgerError("task scheduling condition is invalid")
    return eligible


def render_scheduling_review_card(
    card: SchedulingReviewCard,
) -> tuple[str, dict[str, list[list[dict[str, str]]]]]:
    if not isinstance(card, SchedulingReviewCard):
        raise TaskLedgerError("scheduling review card is invalid")
    target = f"T{card.task_id}"
    if card.kind is SchedulingKind.AFTER_TASK_COMPLETED:
        change = f"Keep {target} behind T{card.related_task_id}."
    elif card.kind is SchedulingKind.NOT_BEFORE:
        change = f"Park {target} until {card.not_before}."
    elif card.kind is SchedulingKind.RAISE_PRIORITY:
        change = f"Move {target} to the raised-priority queue."
    else:
        prerequisite = (card.prerequisite_text or "")[:1000]
        change = (
            f"Create a prerequisite ahead of {target}: "
            f"{html.escape(prerequisite, quote=False)}"
        )
    rationale = html.escape(card.rationale[:2000], quote=False)
    body = (
        "🧭 <b>Queue updated</b>\n\n"
        f"{change}\n\n"
        f"<b>Why:</b> {rationale}\n\n"
        "The change is already active. Use Undo to reverse it."
    )

    def callback(action: str) -> str:
        value = f"{CALLBACK_PREFIX}|{card.card_id}|{card.version}|{action}"
        if len(value.encode("utf-8")) > CALLBACK_DATA_LIMIT:
            raise TaskLedgerError("scheduling callback exceeds transport limit")
        return value

    return body, {"inline_keyboard": [[
        {"text": "Keep", "callback_data": callback("keep")},
        {"text": "Undo", "callback_data": callback("undo")},
    ]]}


def parse_scheduling_review_callback(
    value: object,
) -> tuple[int, int, str] | None:
    if not isinstance(value, str) or not 1 <= len(value) <= CALLBACK_DATA_LIMIT:
        return None
    parts = value.split("|")
    if len(parts) != 4 or parts[0] != CALLBACK_PREFIX:
        return None
    try:
        card_id, version = int(parts[1]), int(parts[2])
    except ValueError:
        return None
    action = parts[3]
    if not _positive_id(card_id) or not _positive_id(version):
        return None
    if action not in {"keep", "undo"}:
        return None
    return card_id, version, action


def _recommendation_document(
    value: object,
) -> dict[str, object] | None:
    if not isinstance(value, ValidatedSchedulingRecommendation):
        return None
    if (
        isinstance(value.target_task_id, bool)
        or not isinstance(value.target_task_id, int)
        or value.target_task_id < 1
        or isinstance(value.target_task_version, bool)
        or not isinstance(value.target_task_version, int)
        or value.target_task_version < 1
        or isinstance(value.expected_workflow_version, bool)
        or not isinstance(value.expected_workflow_version, int)
        or value.expected_workflow_version < 1
        or not isinstance(value.rationale, str)
        or not 1 <= len(value.rationale.strip()) <= MAX_RATIONALE_CHARS
        or isinstance(value.source_refs, (str, bytes))
        or not isinstance(value.source_refs, Sequence)
        or not 1 <= len(value.source_refs) <= MAX_SOURCE_REFS
        or any(not isinstance(ref, str) or not ref or len(ref) > 128
               for ref in value.source_refs)
        or len(set(value.source_refs)) != len(value.source_refs)
        or any(not _SOURCE_REF_RE.fullmatch(ref) for ref in value.source_refs)
        or not isinstance(value.research_receipt_id, str)
        or not 1 <= len(value.research_receipt_id) <= 128
        or not isinstance(value.research_document_digest, str)
        or not _DIGEST_RE.fullmatch(value.research_document_digest)
    ):
        return None
    if value.kind is SchedulingKind.AFTER_TASK_COMPLETED:
        if not _positive_id(value.related_task_id) or value.not_before is not None or value.prerequisite_text is not None:
            return None
    elif value.kind is SchedulingKind.NOT_BEFORE:
        if value.related_task_id is not None or value.prerequisite_text is not None:
            return None
        try:
            _timestamp(value.not_before)
        except (TaskLedgerError, TypeError):
            return None
    elif value.kind is SchedulingKind.RAISE_PRIORITY:
        if any(item is not None for item in (value.related_task_id, value.not_before, value.prerequisite_text)):
            return None
    elif value.kind is SchedulingKind.CREATE_PREREQUISITE:
        if (
            value.related_task_id is not None or value.not_before is not None
            or not isinstance(value.prerequisite_text, str)
            or not 1 <= len(value.prerequisite_text.strip()) <= MAX_PREREQUISITE_TEXT_CHARS
        ):
            return None
    else:
        return None
    return {
        "kind": value.kind.value,
        "target_task_id": value.target_task_id,
        "target_task_version": value.target_task_version,
        "expected_workflow_version": value.expected_workflow_version,
        "rationale": value.rationale.strip(),
        "source_refs": list(value.source_refs),
        "research_receipt_id": value.research_receipt_id,
        "research_document_digest": value.research_document_digest,
        "related_task_id": value.related_task_id,
        "not_before": value.not_before,
        "prerequisite_text": value.prerequisite_text,
    }


def _target_refusal(row: sqlite3.Row | None, value: ValidatedSchedulingRecommendation) -> SchedulingRefusal | None:
    if row is None or row["task_id"] is None:
        return SchedulingRefusal.NOT_FOUND
    if row["task_status"] != "open":
        return SchedulingRefusal.INVALID_STATE
    if int(row["task_version"]) != value.target_task_version:
        return SchedulingRefusal.STALE_TASK
    if int(row["version"]) != value.expected_workflow_version:
        return SchedulingRefusal.STALE_WORKFLOW
    if row["status"] in {"running", "completed", "cancelled", "awaiting_review"}:
        return SchedulingRefusal.INVALID_STATE
    return None


def _dependency_refusal(connection: sqlite3.Connection, target: int, dependency: int | None) -> SchedulingRefusal | None:
    if not _positive_id(dependency) or dependency == target:
        return SchedulingRefusal.INVALID_RECOMMENDATION
    found = connection.execute("SELECT status FROM tasks WHERE id=?", (dependency,)).fetchone()
    if found is None or found["status"] != "open":
        return SchedulingRefusal.INVALID_STATE
    edges = int(connection.execute(
        "SELECT count(*) FROM task_scheduling_conditions WHERE task_id=? "
        "AND kind='after_task_completed' AND state IN ('active','needs_review')",
        (target,),
    ).fetchone()[0])
    if edges >= MAX_PREREQUISITE_EDGES:
        return SchedulingRefusal.LIMIT_EXCEEDED
    graph: dict[int, set[int]] = {}
    for row in connection.execute(
        "SELECT task_id,depends_on_task_id FROM task_scheduling_conditions "
        "WHERE kind='after_task_completed' AND state IN ('active','needs_review')"
    ):
        graph.setdefault(int(row["task_id"]), set()).add(int(row["depends_on_task_id"]))
    graph.setdefault(target, set()).add(int(dependency))

    def visit(node: int, path: frozenset[int]) -> int:
        if node in path:
            raise ValueError("cycle")
        children = graph.get(node, set())
        if not children:
            return 0
        return 1 + max(visit(child, path | {node}) for child in children)

    try:
        depth = max((visit(node, frozenset()) for node in graph), default=0)
    except ValueError:
        return SchedulingRefusal.CYCLE
    if depth > MAX_DEPENDENCY_DEPTH:
        return SchedulingRefusal.DEPTH_EXCEEDED
    return None


def _create_prerequisite(
    connection: sqlite3.Connection, text: str, now: str, template: sqlite3.Row,
) -> int:
    cursor = connection.execute(
        "INSERT INTO tasks(status,text,owner,due,version,created_at,updated_at,closed_at) "
        "VALUES('open',?,NULL,NULL,1,?,?,NULL)",
        (text.strip(), now, now),
    )
    task_id = int(cursor.lastrowid)
    connection.execute(
        "INSERT INTO task_events(task_id,kind,task_version,candidate_id,"
        "source_revision,from_status,to_status,occurred_at) "
        "VALUES(?,'created',1,NULL,NULL,NULL,'open',?)",
        (task_id, now),
    )
    # A prerequisite is a real, claimable workflow (and therefore has the
    # same start-card/read authority path as any other task), not merely a
    # dangling task row hidden behind the dependency edge.
    connection.execute(
        "INSERT INTO task_execution_workflows(task_id,task_version,status,phase,"
        "version,due_at,failure_count,created_at,updated_at,agent_profile_id,"
        "agent_profile_revision,queue_priority,queue_priority_source) "
        "VALUES(?,1,'queued','plan',1,NULL,0,?,?,?,?, 'normal',NULL)",
        (task_id, now, now, template["agent_profile_id"],
         template["agent_profile_revision"]),
    )
    return task_id


def _undo_conflict(connection: sqlite3.Connection, row: sqlite3.Row) -> bool:
    task = connection.execute(
        "SELECT status,version FROM tasks WHERE id=?", (int(row["task_id"]),)
    ).fetchone()
    workflow = connection.execute(
        "SELECT status,version,queue_priority,queue_priority_source "
        "FROM task_execution_workflows WHERE task_id=?",
        (int(row["task_id"]),),
    ).fetchone()
    if (
        row["change_state"] != "active" or task is None or workflow is None
        or task["status"] != "open"
        or int(task["version"]) != int(row["target_task_version"])
        or int(workflow["version"]) != int(row["resulting_workflow_version"])
        or workflow["status"] in {"running", "completed", "cancelled", "awaiting_review"}
    ):
        return True
    if row["kind"] == SchedulingKind.RAISE_PRIORITY and (
        workflow["queue_priority"] != "raised"
        or workflow["queue_priority_source"] != "automation"
    ):
        return True
    if row["created_task_id"] is not None:
        created = connection.execute(
            "SELECT status,version FROM tasks WHERE id=?",
            (int(row["created_task_id"]),),
        ).fetchone()
        created_workflow = connection.execute(
            "SELECT status,version,failure_count,last_result_id,claim_token_digest "
            "FROM task_execution_workflows WHERE task_id=?",
            (int(row["created_task_id"]),),
        ).fetchone()
        pristine_workflow = created_workflow is None or (
            created_workflow["status"] in {"awaiting_start", "queued"}
            and int(created_workflow["version"]) == 1
            and int(created_workflow["failure_count"]) == 0
            and created_workflow["last_result_id"] is None
            and created_workflow["claim_token_digest"] is None
        )
        if (
            created is None or created["status"] != "open"
            or int(created["version"]) != 1 or not pristine_workflow
        ):
            return True
    return False


def _execution_priority_event(connection: sqlite3.Connection, task_id: int, version: int, kind: str, now: str) -> None:
    connection.execute(
        "INSERT INTO task_execution_events(task_id,kind,workflow_version,"
        "task_version,phase,status,occurred_at,agent_profile_id,"
        "agent_profile_revision) SELECT task_id,?, ?,task_version,phase,status,?,"
        "agent_profile_id,agent_profile_revision FROM task_execution_workflows "
        "WHERE task_id=?",
        (kind, version, now, task_id),
    )


def _cancel_prerequisite_cards(
    connection: sqlite3.Connection, task_id: int, workflow_version: int, now: str,
) -> None:
    rows = connection.execute(
        "SELECT id,version,transport,delivery_ref FROM execution_review_cards "
        "WHERE task_id=? AND status IN ('pending','delivering','delivered')",
        (task_id,),
    ).fetchall()
    for row in rows:
        version = int(row["version"]) + 1
        connection.execute(
            "UPDATE execution_review_cards SET status='cancelled',version=?,"
            "claim_token_digest=NULL,claim_expires_at=NULL,"
            "superseded_delivery_ref=delivery_ref,superseded_transport=transport,"
            "transport=NULL,delivery_ref=NULL,resolved_at=?,updated_at=? "
            "WHERE id=? AND version=?",
            (version, now, now, int(row["id"]), int(row["version"])),
        )
        if row["transport"] is not None and row["delivery_ref"] is not None:
            connection.execute(
                "INSERT OR IGNORE INTO execution_card_retractions("
                "card_id,transport,delivery_ref,state,created_at,updated_at) "
                "VALUES(?,?,?,'pending',?,?)",
                (int(row["id"]), row["transport"], row["delivery_ref"], now, now),
            )
        connection.execute(
            "INSERT INTO execution_review_card_events(card_id,task_id,kind,"
            "card_version,workflow_version,action,occurred_at) "
            "VALUES(?,?,'cancelled',?,?,NULL,?)",
            (int(row["id"]), task_id, version, workflow_version, now),
        )


def _set_condition_state(connection: sqlite3.Connection, row: sqlite3.Row, state: str, now: str) -> None:
    connection.execute(
        "UPDATE task_scheduling_conditions SET state=?,updated_at=? WHERE id=?",
        (state, now, int(row["id"])),
    )
    _condition_event(connection, int(row["id"]), int(row["task_id"]), state, state, now)


def _condition_event(connection: sqlite3.Connection, condition_id: int, task_id: int, kind: str, state: str, now: str) -> None:
    connection.execute(
        "INSERT INTO task_scheduling_condition_events(condition_id,task_id,"
        "kind,state,occurred_at) VALUES(?,?,?,?,?)",
        (condition_id, task_id, kind, state, now),
    )


def _card_event(connection: sqlite3.Connection, card_id: int, change_set_id: int, task_id: int, kind: str, version: int, action: str | None, now: str) -> None:
    connection.execute(
        "INSERT INTO task_scheduling_review_card_events(card_id,change_set_id,"
        "task_id,kind,card_version,action,occurred_at) VALUES(?,?,?,?,?,?,?)",
        (card_id, change_set_id, task_id, kind, version, action, now),
    )


def _resolve_card(connection: sqlite3.Connection, row: sqlite3.Row, version: int, action: str, now: str) -> None:
    connection.execute(
        "UPDATE task_scheduling_review_cards SET status='resolved',version=?,"
        "resolution=?,updated_at=?,resolved_at=? WHERE id=? AND version=? "
        "AND status='delivered'",
        (version, action, now, now, int(row["id"]), int(row["version"])),
    )
    _card_event(
        connection, int(row["id"]), int(row["change_set_id"]),
        int(row["task_id"]), "resolved", version, action, now,
    )


def _timestamp(value: object) -> datetime:
    if not isinstance(value, str) or not value or len(value) > 64:
        raise TaskLedgerError("task scheduling timestamp is invalid")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise TaskLedgerError("task scheduling timestamp is invalid") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise TaskLedgerError("task scheduling timestamp is invalid")
    return parsed


def _positive_id(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value > 0


def _valid_digest(value: object) -> bool:
    return isinstance(value, str) and _DIGEST_RE.fullmatch(value) is not None


def _valid_secret(value: object) -> bool:
    return isinstance(value, str) and 32 <= len(value) <= 512 and "\x00" not in value


def _valid_opaque(value: object, maximum: int) -> bool:
    return (
        isinstance(value, str) and 1 <= len(value) <= maximum
        and "\x00" not in value and "\n" not in value and "\r" not in value
    )


def _token_digest(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _delivery_guard(
    row: sqlite3.Row | None, expected_version: int, digest: str, now: str,
) -> SchedulingRefusal | None:
    if row is None:
        return SchedulingRefusal.NOT_FOUND
    if int(row["version"]) != expected_version:
        return SchedulingRefusal.STALE_CARD
    if row["status"] != "delivering":
        return SchedulingRefusal.INVALID_STATE
    stored = row["claim_token_digest"]
    if not isinstance(stored, str) or not secrets.compare_digest(stored, digest):
        return SchedulingRefusal.CLAIM_MISMATCH
    if _timestamp(row["claim_expires_at"]) <= _timestamp(now):
        return SchedulingRefusal.INVALID_STATE
    return None


def _review_card(row: sqlite3.Row, *, version: int) -> SchedulingReviewCard:
    try:
        document = json.loads(row["recommendation_json"])
        kind = SchedulingKind(row["kind"])
        rationale = document["rationale"]
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
        raise TaskLedgerError("scheduling card payload is invalid") from exc
    if not isinstance(rationale, str):
        raise TaskLedgerError("scheduling card payload is invalid")
    return SchedulingReviewCard(
        card_id=int(row["id"]), change_set_id=int(row["change_set_id"]),
        task_id=int(row["task_id"]), version=version, kind=kind,
        rationale=rationale, related_task_id=document.get("related_task_id"),
        not_before=document.get("not_before"),
        prerequisite_text=document.get("prerequisite_text"),
    )


def _refused(refusal: SchedulingRefusal) -> SchedulingApplyResult:
    return SchedulingApplyResult(SchedulingDisposition.REFUSED, refusal=refusal)


def _card_refused(card_id: object, refusal: SchedulingRefusal) -> SchedulingCardActionResult:
    return SchedulingCardActionResult(
        SchedulingDisposition.REFUSED,
        refusal=refusal,
        card_id=card_id if _positive_id(card_id) else None,
    )
