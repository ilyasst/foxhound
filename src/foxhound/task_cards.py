"""Foxhound-owned, transport-neutral task review cards."""

from __future__ import annotations

import hashlib
import html
import os
import secrets
import sqlite3
from contextlib import closing
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import StrEnum
from pathlib import Path
from typing import Callable

from .card_provenance import (
    CardSourceEvidence,
    origin_lines,
    origin_url,
    quotable,
    stored_origin_sources,
)

from . import task_completion as completion
from .candidate_inbox import CandidateInbox, InboxError, SCHEMA_VERSION
from .task_ledger import (
    TaskLedgerError,
    TaskStatus,
    TransitionRefusal,
    _apply_task_transition,
)
from .task_owner import canonical_owner_display


#: What each answer says about the evidence that prompted the question.
#: `drop` is neither an acceptance nor a refusal of the detection: the reader
#: discarded the task itself, and never reached the question.
_ANSWERS = {
    "done": completion.Outcome.ACCEPTED,
    "keep_open": completion.Outcome.REJECTED,
    "drop": completion.Outcome.SUPERSEDED,
}

SNOOZE_INTERVAL = timedelta(days=3)
OPEN_REVIEW_INTERVAL = timedelta(days=7)
CALLBACK_PREFIX = "fhc"
CALLBACK_DATA_LIMIT = 64


class CardStatus(StrEnum):
    PENDING = "pending"
    DELIVERING = "delivering"
    DELIVERED = "delivered"
    SNOOZED = "snoozed"
    RESOLVED = "resolved"
    CANCELLED = "cancelled"


class CardDisposition(StrEnum):
    APPLIED = "applied"
    UNCHANGED = "unchanged"
    REFUSED = "refused"


class CardRefusal(StrEnum):
    INVALID_ARGUMENT = "invalid_argument"
    INVALID_ACTION = "invalid_action"
    NOT_FOUND = "not_found"
    STALE_VERSION = "stale_version"
    INVALID_STATE = "invalid_state"
    CLAIM_MISMATCH = "claim_mismatch"


@dataclass(frozen=True)
class ScheduleResult:
    """Content-free aggregate result for one explicit scheduler pass."""

    disposition: CardDisposition
    created: int = 0
    cancelled: int = 0
    #: Cards now carrying a completion question, whether they were created for
    #: it or already waiting. Counted apart from `created` because asking is
    #: not scheduling: a question can reach a reader without a new card.
    asked: int = 0
    refusal: CardRefusal | None = None


@dataclass(frozen=True)
class CardStats:
    """Aggregate-only queue state from one database snapshot."""

    pending: int
    delivering: int
    delivered: int
    snoozed: int
    active: int


@dataclass(frozen=True)
class CardCompletionQuestion:
    """The one detection a card is asking about; private card content."""

    id: int
    source_kind: str = field(repr=False)
    source_record_id: str = field(repr=False)
    source_item_id: str = field(repr=False)
    observed_at: str = field(repr=False)
    quotation: str = field(repr=False)
    reason: str = field(repr=False)
    confidence: str = field(repr=False)


@dataclass(frozen=True)
class TaskReviewCard:
    """Private card projection; callers must not log or persist its content."""

    id: int
    task_id: int
    task_version: int
    status: CardStatus
    version: int
    due_at: str
    text: str = field(repr=False)
    owner: str | None = field(repr=False)
    due: str | None = field(repr=False)
    first_raised: str | None = field(default=None, repr=False)
    last_mentioned: str | None = field(default=None, repr=False)
    origin_kind: str = field(default="", repr=False)
    origin_record: str = field(default="", repr=False)
    origin_item: str = field(default="", repr=False)
    origin_sources: tuple[CardSourceEvidence, ...] = field(
        default=(), repr=False
    )
    #: Set only when this card is a done-check. An ordinary review card asks
    #: whether a task is finished and shows nothing about why it is asking;
    #: this one has an answer to that.
    completion: CardCompletionQuestion | None = field(
        default=None, repr=False
    )


@dataclass(frozen=True)
class DeliveryClaim:
    card: TaskReviewCard
    token: str = field(repr=False)
    expires_at: str


@dataclass(frozen=True)
class CardOperationResult:
    """Content-free result for delivery and reader operations."""

    disposition: CardDisposition
    card_id: int
    version: int | None = None
    status: CardStatus | None = None
    task_version: int | None = None
    task_status: TaskStatus | None = None
    wake_at: str | None = None
    refusal: CardRefusal | None = None

    @property
    def accepted(self) -> bool:
        return self.disposition is not CardDisposition.REFUSED


class TaskCardService:
    """Durable scheduling and actions for Foxhound task review cards."""

    def __init__(
        self,
        database_path: str | os.PathLike[str],
        *,
        clock: Callable[[], datetime] | None = None,
        token_factory: Callable[[], str] | None = None,
    ) -> None:
        self.database_path = Path(database_path)
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._token_factory = token_factory or (lambda: secrets.token_urlsafe(32))

    def initialize(self) -> None:
        CandidateInbox(self.database_path, clock=self._clock).initialize()

    def schedule(self, *, limit: int = 100) -> ScheduleResult:
        if not _valid_limit(limit):
            return ScheduleResult(
                CardDisposition.REFUSED,
                refusal=CardRefusal.INVALID_ARGUMENT,
            )
        now = self._now()
        with closing(self._connect()) as connection:
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute("BEGIN IMMEDIATE")
            try:
                cancelled = self._cancel_stale(connection, now)
                rows = connection.execute(
                    "SELECT t.id,t.version FROM tasks AS t "
                    "WHERE t.status='open' "
                    "AND NOT EXISTS("
                    " SELECT 1 FROM task_candidate_bindings AS b JOIN "
                    " task_candidate_lifecycle AS l ON l.candidate_id=b.candidate_id "
                    " WHERE b.task_id=t.id AND b.relation='accepted' "
                    " AND l.state='withdrawn' "
                    " AND l.resolution='preserved_open'"
                    ") "
                    "AND NOT EXISTS("
                    " SELECT 1 FROM task_review_cards AS active "
                    " WHERE active.task_id=t.id AND active.status IN "
                    " ('pending','delivering','delivered','snoozed')"
                    ") "
                    "AND COALESCE(("
                    " SELECT prior.review_after FROM task_review_cards AS prior "
                    " WHERE prior.task_id=t.id ORDER BY prior.id DESC LIMIT 1"
                    "),'')<=? "
                    "ORDER BY t.created_at,t.id LIMIT ?",
                    (now, limit),
                ).fetchall()
                for row in rows:
                    cursor = connection.execute(
                        "INSERT INTO task_review_cards("
                        "task_id,task_version,status,version,due_at,created_at,"
                        "updated_at) VALUES(?,?,'pending',1,?,?,?)",
                        (int(row["id"]), int(row["version"]), now, now, now),
                    )
                    self._event(
                        connection,
                        card_id=int(cursor.lastrowid),
                        task_id=int(row["id"]),
                        kind="scheduled",
                        card_version=1,
                        task_version=int(row["version"]),
                        now=now,
                    )
                created = len(rows)
                asked, raised = self._ask_completion_questions(
                    connection, now, limit=limit
                )
                created += raised
                connection.commit()
                return ScheduleResult(
                    CardDisposition.APPLIED
                    if created or cancelled or asked
                    else CardDisposition.UNCHANGED,
                    created=created,
                    cancelled=cancelled,
                    asked=asked,
                )
            except Exception:
                connection.rollback()
                raise

    def due(self, *, limit: int = 20) -> tuple[TaskReviewCard, ...]:
        if not _valid_limit(limit):
            raise TaskLedgerError("task card due limit is invalid")
        now = self._now()
        with closing(self._connect()) as connection:
            rows = connection.execute(
                self._card_select()
                + " WHERE c.status IN ('pending','snoozed') AND c.due_at<=? "
                "AND t.status='open' AND t.version=c.task_version "
                "ORDER BY c.due_at,c.id LIMIT ?",
                (now, limit),
            ).fetchall()
        return tuple(_card(row) for row in rows)

    def claim_next(
        self, *, lease_seconds: int = 60, consumer_digest: str
    ) -> DeliveryClaim | None:
        if (isinstance(lease_seconds, bool)
                or not isinstance(lease_seconds, int)
                or not 5 <= lease_seconds <= 300):
            raise TaskLedgerError("task card delivery lease is invalid")
        if not _valid_digest(consumer_digest):
            raise TaskLedgerError("task card consumer digest is invalid")
        now_dt = self._clock_value()
        now = now_dt.isoformat(timespec="seconds")
        expires_at = (now_dt + timedelta(seconds=lease_seconds)).isoformat(
            timespec="seconds"
        )
        token = self._token_factory()
        if not _valid_secret(token):
            raise TaskLedgerError("task card token factory returned invalid state")
        digest = _token_digest(token)
        with closing(self._connect()) as connection:
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute("BEGIN IMMEDIATE")
            try:
                expired = connection.execute(
                    "SELECT id,task_id,task_version,version "
                    "FROM task_review_cards WHERE status='delivering' "
                    "AND claim_expires_at<=? ORDER BY id",
                    (now,),
                ).fetchall()
                for row in expired:
                    next_version = int(row["version"]) + 1
                    connection.execute(
                        "UPDATE task_review_cards SET status='pending',"
                        "version=?,claim_token_digest=NULL,claim_expires_at=NULL,"
                        "consumer_digest=NULL,"
                        "updated_at=? WHERE id=? AND status='delivering' "
                        "AND version=?",
                        (next_version, now, int(row["id"]), int(row["version"])),
                    )
                    self._event(
                        connection,
                        card_id=int(row["id"]),
                        task_id=int(row["task_id"]),
                        kind="delivery_expired",
                        card_version=next_version,
                        task_version=int(row["task_version"]),
                        now=now,
                    )
                row = connection.execute(
                    self._card_select()
                    + " WHERE c.status IN ('pending','snoozed') "
                    "AND c.due_at<=? AND t.status='open' "
                    "AND t.version=c.task_version "
                    "ORDER BY c.due_at,c.id LIMIT 1",
                    (now,),
                ).fetchone()
                if row is None:
                    connection.commit()
                    return None
                next_version = int(row["version"]) + 1
                updated = connection.execute(
                    "UPDATE task_review_cards SET status='delivering',"
                    "version=?,claim_token_digest=?,claim_expires_at=?,"
                    "consumer_digest=?,"
                    "transport=NULL,delivery_ref=NULL,delivered_at=NULL,"
                    "updated_at=? WHERE id=? AND version=? "
                    "AND status IN ('pending','snoozed')",
                    (
                        next_version,
                        digest,
                        expires_at,
                        consumer_digest,
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
                    card_version=next_version,
                    task_version=int(row["task_version"]),
                    now=now,
                )
                connection.commit()
                values = dict(row)
                values["status"] = CardStatus.DELIVERING
                values["version"] = next_version
                return DeliveryClaim(
                    card=_card(values), token=token, expires_at=expires_at
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
    ) -> CardOperationResult:
        if not _valid_identity(card_id, expected_version):
            return _refused(card_id, CardRefusal.INVALID_ARGUMENT)
        if (not _valid_secret(claim_token)
                or not _valid_opaque(transport, 64)
                or not _valid_opaque(delivery_ref, 200)):
            return _refused(card_id, CardRefusal.INVALID_ARGUMENT)
        digest = _token_digest(claim_token)
        now = self._now()
        with closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = connection.execute(
                    "SELECT * FROM task_review_cards WHERE id=?", (card_id,)
                ).fetchone()
                refusal = _card_guard(row, expected_version)
                if refusal is not None:
                    connection.rollback()
                    return _refused_row(card_id, row, refusal)
                if row["claim_token_digest"] != digest:
                    connection.rollback()
                    return _refused_row(card_id, row, CardRefusal.CLAIM_MISMATCH)
                if row["status"] == CardStatus.DELIVERED:
                    connection.rollback()
                    if (row["transport"] == transport
                            and row["delivery_ref"] == delivery_ref):
                        return _operation(row, CardDisposition.UNCHANGED)
                    return _refused_row(card_id, row, CardRefusal.INVALID_STATE)
                if row["status"] != CardStatus.DELIVERING:
                    connection.rollback()
                    return _refused_row(card_id, row, CardRefusal.INVALID_STATE)
                connection.execute(
                    "UPDATE task_review_cards SET status='delivered',"
                    "claim_expires_at=NULL,transport=?,delivery_ref=?,"
                    "delivered_at=?,updated_at=? WHERE id=? AND version=?",
                    (transport, delivery_ref, now, now, card_id, expected_version),
                )
                self._event(
                    connection,
                    card_id=card_id,
                    task_id=int(row["task_id"]),
                    kind="delivered",
                    card_version=expected_version,
                    task_version=int(row["task_version"]),
                    now=now,
                )
                connection.commit()
                values = dict(row)
                values.update(status=CardStatus.DELIVERED, transport=transport,
                              delivery_ref=delivery_ref, delivered_at=now)
                return _operation(values, CardDisposition.APPLIED)
            except Exception:
                connection.rollback()
                raise

    def fail_delivery(
        self, card_id: int, *, expected_version: int, claim_token: str
    ) -> CardOperationResult:
        if (not _valid_identity(card_id, expected_version)
                or not _valid_secret(claim_token)):
            return _refused(card_id, CardRefusal.INVALID_ARGUMENT)
        now = self._now()
        digest = _token_digest(claim_token)
        with closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = connection.execute(
                    "SELECT * FROM task_review_cards WHERE id=?", (card_id,)
                ).fetchone()
                refusal = _card_guard(row, expected_version)
                if refusal is not None:
                    connection.rollback()
                    return _refused_row(card_id, row, refusal)
                if row["claim_token_digest"] != digest:
                    connection.rollback()
                    return _refused_row(card_id, row, CardRefusal.CLAIM_MISMATCH)
                if row["status"] != CardStatus.DELIVERING:
                    connection.rollback()
                    return _refused_row(card_id, row, CardRefusal.INVALID_STATE)
                next_version = expected_version + 1
                connection.execute(
                    "UPDATE task_review_cards SET status='pending',version=?,"
                    "claim_token_digest=NULL,claim_expires_at=NULL,"
                    "consumer_digest=NULL,updated_at=? "
                    "WHERE id=? AND version=?",
                    (next_version, now, card_id, expected_version),
                )
                self._event(
                    connection,
                    card_id=card_id,
                    task_id=int(row["task_id"]),
                    kind="delivery_failed",
                    card_version=next_version,
                    task_version=int(row["task_version"]),
                    now=now,
                )
                connection.commit()
                values = dict(row)
                values.update(status=CardStatus.PENDING, version=next_version)
                return _operation(values, CardDisposition.APPLIED)
            except Exception:
                connection.rollback()
                raise

    def act(
        self, card_id: int, *, expected_version: int, action: str
    ) -> CardOperationResult:
        if not _valid_identity(card_id, expected_version):
            return _refused(card_id, CardRefusal.INVALID_ARGUMENT)
        if action not in {"done", "keep_open", "drop", "snooze"}:
            return _refused(card_id, CardRefusal.INVALID_ACTION)
        now_dt = self._clock_value()
        now = now_dt.isoformat(timespec="seconds")
        with closing(self._connect()) as connection:
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = connection.execute(
                    "SELECT c.*,t.status AS task_status_current,"
                    "t.version AS task_version_current "
                    "FROM task_review_cards AS c JOIN tasks AS t "
                    "ON t.id=c.task_id WHERE c.id=?",
                    (card_id,),
                ).fetchone()
                refusal = _card_guard(row, expected_version)
                if refusal is not None:
                    connection.rollback()
                    return _refused_row(card_id, row, refusal)
                if row["status"] != CardStatus.DELIVERED:
                    connection.rollback()
                    return _refused_row(card_id, row, CardRefusal.INVALID_STATE)
                if (row["task_status_current"] != TaskStatus.OPEN
                        or int(row["task_version_current"])
                        != int(row["task_version"])):
                    connection.rollback()
                    return _refused_row(card_id, row, CardRefusal.STALE_VERSION)

                task_version = int(row["task_version"])
                task_status = TaskStatus.OPEN
                if action in {"done", "drop"}:
                    transition = _apply_task_transition(
                        connection,
                        task_id=int(row["task_id"]),
                        expected_version=task_version,
                        action=action,
                        now=now,
                    )
                    if not transition.accepted:
                        connection.rollback()
                        card_refusal = (
                            CardRefusal.STALE_VERSION
                            if transition.refusal is TransitionRefusal.STALE_VERSION
                            else CardRefusal.INVALID_STATE
                        )
                        return _refused_row(card_id, row, card_refusal)
                    task_version = int(transition.version)
                    task_status = transition.status

                next_version = expected_version + 1
                if action == "snooze":
                    wake = (now_dt + SNOOZE_INTERVAL).isoformat(timespec="seconds")
                    connection.execute(
                        "UPDATE task_review_cards SET status='snoozed',"
                        "version=?,due_at=?,claim_token_digest=NULL,"
                        "claim_expires_at=NULL,consumer_digest=NULL,"
                        "transport=NULL,delivery_ref=NULL,"
                        "delivered_at=NULL,updated_at=? WHERE id=? AND version=?",
                        (next_version, wake, now, card_id, expected_version),
                    )
                    kind = "snoozed"
                    status = CardStatus.SNOOZED
                else:
                    wake = (
                        (now_dt + OPEN_REVIEW_INTERVAL).isoformat(timespec="seconds")
                        if action == "keep_open" else None
                    )
                    connection.execute(
                        "UPDATE task_review_cards SET status='resolved',"
                        "version=?,resolution=?,review_after=?,resolved_at=?,"
                        "consumer_digest=NULL,"
                        "updated_at=? WHERE id=? AND version=?",
                        (
                            next_version,
                            action,
                            wake,
                            now,
                            now,
                            card_id,
                            expected_version,
                        ),
                    )
                    kind = "resolved"
                    status = CardStatus.RESOLVED
                # The answer to the question, if this card was carrying one.
                # `keep_open` on a done-check is Reopen: the suggestion is
                # refused, and refusing it durably is what stops the next pass
                # asking again from the same evidence -- the exact failure the
                # removed engine had, where reopening cleared the record of
                # why the task had been closed and the following run closed it
                # again on the same sentence.
                if action != "snooze":
                    completion.settle_for_card(
                        connection,
                        card_id=card_id,
                        outcome=_ANSWERS[action],
                        now=now,
                    )
                self._event(
                    connection,
                    card_id=card_id,
                    task_id=int(row["task_id"]),
                    kind=kind,
                    card_version=next_version,
                    task_version=task_version,
                    action=action,
                    now=now,
                )
                connection.commit()
                return CardOperationResult(
                    CardDisposition.APPLIED,
                    card_id=card_id,
                    version=next_version,
                    status=status,
                    task_version=task_version,
                    task_status=task_status,
                    wake_at=wake,
                )
            except Exception:
                connection.rollback()
                raise

    def completion_counts(self) -> tuple[completion.DetectorCounts, ...]:
        """Per-detector accept/reject totals, so a bad matcher is visible."""
        with closing(self._connect()) as connection:
            return completion.counts(connection)

    def count(self) -> int:
        with closing(self._connect()) as connection:
            return int(connection.execute(
                "SELECT count(*) FROM task_review_cards"
            ).fetchone()[0])

    def stats(self) -> CardStats:
        with closing(self._connect()) as connection:
            row = connection.execute(
                "SELECT "
                "SUM(CASE WHEN status='pending' THEN 1 ELSE 0 END) AS pending,"
                "SUM(CASE WHEN status='delivering' THEN 1 ELSE 0 END) AS delivering,"
                "SUM(CASE WHEN status='delivered' THEN 1 ELSE 0 END) AS delivered,"
                "SUM(CASE WHEN status='snoozed' THEN 1 ELSE 0 END) AS snoozed,"
                "SUM(CASE WHEN status IN "
                "('pending','delivering','delivered','snoozed') "
                "THEN 1 ELSE 0 END) AS active "
                "FROM task_review_cards"
            ).fetchone()
        return CardStats(*(
            int(row[name] or 0)
            for name in ("pending", "delivering", "delivered", "snoozed", "active")
        ))

    def event_count(self) -> int:
        with closing(self._connect()) as connection:
            return int(connection.execute(
                "SELECT count(*) FROM task_review_card_events"
            ).fetchone()[0])

    def _ask_completion_questions(
        self, connection: sqlite3.Connection, now: str, *, limit: int
    ) -> tuple[int, int]:
        """Put unanswered completion questions in front of a reader.

        A done-check is not a new card kind. It is the task's own review card,
        carrying the evidence that says the work is finished — so a task with
        a card already waiting gets its question attached to that card, and
        only a task with no card at all gets one made for it.

        This is the one path that ignores `review_after`. The weekly rhythm
        exists so an untouched task is not asked about repeatedly; evidence
        that the task is finished is exactly the event that rhythm should not
        delay, and a reader who has just been told why we think it is done is
        not being asked the same question again.

        A card already claimed, delivered or snoozed is left alone. Binding to
        it would change nothing a reader has been shown, and a question that
        looks asked but was never displayed is worse than one still waiting.
        """
        rows = connection.execute(
            "SELECT e.id AS evidence_id,e.task_id AS task_id,"
            "(SELECT c.id FROM task_review_cards AS c "
            " WHERE c.task_id=e.task_id AND c.status IN "
            " ('pending','delivering','delivered','snoozed')) AS active_id,"
            "(SELECT c.status FROM task_review_cards AS c "
            " WHERE c.task_id=e.task_id AND c.status IN "
            " ('pending','delivering','delivered','snoozed')) AS active_status,"
            "t.version AS task_version "
            "FROM task_completion_evidence AS e "
            "JOIN tasks AS t ON t.id=e.task_id "
            "WHERE e.state='proposed' AND e.card_id IS NULL "
            "AND t.status='open' "
            "AND NOT EXISTS("
            " SELECT 1 FROM task_candidate_bindings AS b JOIN "
            " task_candidate_lifecycle AS l ON l.candidate_id=b.candidate_id "
            " WHERE b.task_id=t.id AND b.relation='accepted' "
            " AND l.state='withdrawn' AND l.resolution='preserved_open'"
            ") "
            "ORDER BY e.id LIMIT ?",
            (limit,),
        ).fetchall()

        asked = raised = 0
        seen: set[int] = set()
        for row in rows:
            task_id = int(row["task_id"])
            # One question per task per pass. The next one waits for this
            # one's answer, which is what stops a detector from turning a
            # single task into a queue of cards.
            if task_id in seen:
                continue
            status = row["active_status"]
            if status is not None and status != CardStatus.PENDING:
                continue
            if status == CardStatus.PENDING:
                card_id = int(row["active_id"])
                # Due now: the card was waiting on its own schedule, and it
                # has just acquired something to say.
                connection.execute(
                    "UPDATE task_review_cards SET due_at=?,updated_at=? "
                    "WHERE id=? AND status='pending'",
                    (now, now, card_id),
                )
            else:
                cursor = connection.execute(
                    "INSERT INTO task_review_cards("
                    "task_id,task_version,status,version,due_at,created_at,"
                    "updated_at) VALUES(?,?,'pending',1,?,?,?)",
                    (task_id, int(row["task_version"]), now, now, now),
                )
                card_id = int(cursor.lastrowid)
                self._event(
                    connection,
                    card_id=card_id,
                    task_id=task_id,
                    kind="scheduled",
                    card_version=1,
                    task_version=int(row["task_version"]),
                    now=now,
                )
                raised += 1
            if not completion.bind(
                connection,
                evidence_id=int(row["evidence_id"]),
                card_id=card_id,
            ):
                continue
            seen.add(task_id)
            asked += 1
        return asked, raised

    def _cancel_stale(self, connection: sqlite3.Connection, now: str) -> int:
        rows = connection.execute(
            "SELECT c.id,c.task_id,c.task_version,c.version "
            "FROM task_review_cards AS c JOIN tasks AS t ON t.id=c.task_id "
            "WHERE c.status IN ('pending','delivering','delivered','snoozed') "
            "AND (t.status!='open' OR t.version!=c.task_version OR EXISTS("
            " SELECT 1 FROM task_candidate_bindings AS b JOIN "
            " task_candidate_lifecycle AS l ON l.candidate_id=b.candidate_id "
            " WHERE b.task_id=t.id AND b.relation='accepted' "
            " AND l.state='withdrawn' AND l.resolution='preserved_open'"
            ")) ORDER BY c.id"
        ).fetchall()
        for row in rows:
            next_version = int(row["version"]) + 1
            connection.execute(
                "UPDATE task_review_cards SET status='cancelled',version=?,"
                "claim_token_digest=NULL,claim_expires_at=NULL,"
                "consumer_digest=NULL,resolved_at=?,"
                "updated_at=? WHERE id=? AND version=?",
                (next_version, now, now, int(row["id"]), int(row["version"])),
            )
            # The question goes back in the queue rather than down with the
            # card: bound to a card nobody will see, it could never be asked
            # again, because the identity index refuses a second row for the
            # same evidence.
            completion.release(connection, int(row["id"]))
            self._event(
                connection,
                card_id=int(row["id"]),
                task_id=int(row["task_id"]),
                kind="cancelled",
                card_version=next_version,
                task_version=int(row["task_version"]),
                now=now,
            )
        return len(rows)

    @staticmethod
    def _event(
        connection: sqlite3.Connection,
        *,
        card_id: int,
        task_id: int,
        kind: str,
        card_version: int,
        task_version: int,
        now: str,
        action: str | None = None,
    ) -> None:
        connection.execute(
            "INSERT INTO task_review_card_events("
            "card_id,task_id,kind,card_version,task_version,action,occurred_at) "
            "VALUES(?,?,?,?,?,?,?)",
            (card_id, task_id, kind, card_version, task_version, action, now),
        )

    @staticmethod
    def _card_select() -> str:
        return (
            "SELECT c.id,c.task_id,c.task_version,c.status,c.version,c.due_at,"
            "t.text,t.owner,t.owner_kind,t.due,"
            "(SELECT min(h.created_at) FROM task_candidate_bindings AS b "
            " JOIN candidate_revision_history AS h "
            " ON h.candidate_id=b.candidate_id WHERE b.task_id=c.task_id) "
            " AS first_raised,"
            "(SELECT max(h.created_at) FROM task_candidate_bindings AS b "
            " JOIN candidate_revision_history AS h "
            " ON h.candidate_id=b.candidate_id WHERE b.task_id=c.task_id) "
            " AS last_mentioned,"
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
            "(SELECT h.payload_json FROM task_candidate_bindings AS b "
            " JOIN candidate_revision_history AS h "
            " ON h.candidate_id=b.candidate_id "
            " AND h.source_revision=b.source_revision "
            " WHERE b.task_id=c.task_id AND b.relation='accepted') "
            " AS origin_payload,"
            # A card carries at most one unanswered question -- scheduling
            # binds one at a time -- so this join never multiplies rows.
            "e.id AS completion_id,e.source_kind AS completion_kind,"
            "e.source_record_id AS completion_record,"
            "e.source_item_id AS completion_item,"
            "e.observed_at AS completion_observed,"
            "e.quotation AS completion_quotation,"
            "e.reason AS completion_reason,"
            "e.confidence AS completion_confidence "
            "FROM task_review_cards AS c "
            "JOIN tasks AS t ON t.id=c.task_id "
            "LEFT JOIN task_completion_evidence AS e "
            "ON e.card_id=c.id AND e.state='proposed'"
        )

    def _connect(self) -> sqlite3.Connection:
        if not self.database_path.is_file() or self.database_path.is_symlink():
            raise TaskLedgerError("task card database is not initialized")
        connection = sqlite3.connect(self.database_path, timeout=5)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout = 5000")
        version = int(connection.execute("PRAGMA user_version").fetchone()[0])
        if version != SCHEMA_VERSION:
            connection.close()
            raise TaskLedgerError("task card database schema is not supported")
        try:
            CandidateInbox._require_schema(connection)
        except InboxError as exc:
            connection.close()
            raise TaskLedgerError("task card database schema is incomplete") from exc
        return connection

    def _clock_value(self) -> datetime:
        value = self._clock()
        if value.tzinfo is None or value.utcoffset() is None:
            raise TaskLedgerError("task card clock must include a timezone")
        return value.astimezone(timezone.utc)

    def _now(self) -> str:
        return self._clock_value().isoformat(timespec="seconds")


def render_task_review_card(card: TaskReviewCard) -> tuple[str, dict]:
    """Render one claimed card without performing I/O."""
    if card.status is not CardStatus.DELIVERING:
        raise ValueError("task review card is not claimed for delivery")
    text = html.escape(card.text, quote=False)
    if card.completion is not None:
        return _render_done_check(card, text)
    lines = [f"☑️ <b>Task done?</b>  <code>T{card.task_id}</code>", "", f"<b>{text}</b>"]
    if card.owner:
        lines.extend(("", f"👤 <b>Owner:</b> {html.escape(card.owner, quote=False)}"))
    if card.due:
        lines.append(f"📅 <b>Due:</b> {html.escape(card.due, quote=False)}")
    first = "" if not card.first_raised else str(card.first_raised)[:10]
    if first:
        lines.append(f"📌 <b>First raised:</b> {html.escape(first, quote=False)}")
    last = "" if not card.last_mentioned else str(card.last_mentioned)[:10]
    if last and last != first:
        lines.append(f"🕑 <b>Last mentioned:</b> {html.escape(last, quote=False)}")
    # Conditional because the provenance block is now allowed to be empty:
    # an unconditional blank separator would end the card on a stray line.
    origin = origin_lines(
        kind=card.origin_kind,
        record=card.origin_record,
        item=card.origin_item,
        sources=card.origin_sources,
        html_output=True,
    )
    if origin:
        lines.extend(("", *origin))

    def callback(action: str) -> str:
        value = f"{CALLBACK_PREFIX}|{card.id}|{card.version}|{action}"
        if len(value.encode("utf-8")) > CALLBACK_DATA_LIMIT:
            raise ValueError("task review callback exceeds transport limit")
        return value

    keyboard = {"inline_keyboard": [
        [
            {"text": "✅ Done", "callback_data": callback("done")},
            {"text": "⏳ Still open", "callback_data": callback("keep_open")},
            {"text": "🗑 Drop", "callback_data": callback("drop")},
        ],
        [{"text": "💤 Snooze", "callback_data": callback("snooze")}],
    ]}
    return "\n".join(lines), keyboard


def _render_done_check(card: TaskReviewCard, text: str) -> tuple[str, dict]:
    """The same card, asking a question it can justify.

    An ordinary review card asks whether a task is done and shows the reader
    nothing about why it is asking now. This one is only ever raised because
    something said the work is finished, so it shows that something: the
    source, its date, the sentence itself, and why that sentence was read as
    closing this task rather than a similar one. A reader can check all four
    without leaving the chat, which is the whole difference between a
    suggestion and a nag.

    Two controls, and no third. Snooze and Drop belong to the open question
    "is this still live?"; this card asks "is this finished?", and the only
    honest answers to it are yes and no.
    """
    question = card.completion
    quotation = quotable(question.quotation)
    lines = [
        f"✅ <b>Looks done</b>  <code>T{card.task_id}</code>",
        "",
        f"<b>{text}</b>",
    ]
    if card.owner:
        lines.extend(("", f"👤 <b>Owner:</b> {_escape(card.owner)}"))
    origin = _source_line(question)
    when = str(question.observed_at)[:10]
    lines.extend((
        "",
        f"🔎 <b>Why we think so</b> — {origin}"
        + (f", {_escape(when)}" if when else ""),
        f"<blockquote>{_escape(quotation)}</blockquote>",
        _escape(question.reason),
    ))
    # The task's own justifying extract, so the reader can see both halves of
    # the match: what the task asked for, and what is said to have answered
    # it. Without it "why this task" is a claim they have to take on trust.
    #
    # Only when there is an extract. The missing-evidence warning that block
    # otherwise carries is written for the card that asks "is this still
    # live?"; here it would sit under a heading promising the task's own words
    # and deliver a second "From:" naming an unknown source, which reads as a
    # doubt about the quotation immediately above it.
    if card.origin_sources:
        lines.extend((
            "",
            "📌 <b>What this task asked for</b>",
            *origin_lines(
                kind=card.origin_kind,
                record=card.origin_record,
                item=card.origin_item,
                sources=card.origin_sources,
                html_output=True,
            ),
        ))

    def callback(action: str) -> str:
        value = f"{CALLBACK_PREFIX}|{card.id}|{card.version}|{action}"
        if len(value.encode("utf-8")) > CALLBACK_DATA_LIMIT:
            raise ValueError("task review callback exceeds transport limit")
        return value

    keyboard = {"inline_keyboard": [[
        {"text": "✅ Mark as done", "callback_data": callback("done")},
        {"text": "↩️ Reopen", "callback_data": callback("keep_open")},
    ]]}
    return "\n".join(lines), keyboard


def _source_line(question: CardCompletionQuestion) -> str:
    """Name the completing source the way the reader would name it."""
    url = origin_url(
        kind=question.source_kind,
        record=question.source_record_id,
        item=question.source_item_id,
    )
    if url is not None:
        name = question.source_record_id.rsplit("/", 1)[-1]
        number = question.source_item_id.split("/", 1)[0]
        return f'<a href="{_escape(url)}">{_escape(name)} #{_escape(number)}</a>'
    kind = question.source_kind.replace("_", " ").title() or "Source"
    return _escape(kind)


def _escape(value: str) -> str:
    return html.escape(value, quote=False)


def parse_task_review_callback(value: object) -> tuple[int, int, str] | None:
    if not isinstance(value, str) or len(value.encode("utf-8")) > CALLBACK_DATA_LIMIT:
        return None
    parts = value.split("|")
    if len(parts) != 4 or parts[0] != CALLBACK_PREFIX:
        return None
    try:
        card_id = int(parts[1])
        version = int(parts[2])
    except ValueError:
        return None
    action = parts[3]
    if (not _valid_identity(card_id, version)
            or action not in {"done", "keep_open", "drop", "snooze"}):
        return None
    return card_id, version, action


def _card(row) -> TaskReviewCard:
    return TaskReviewCard(
        id=int(row["id"]),
        task_id=int(row["task_id"]),
        task_version=int(row["task_version"]),
        status=CardStatus(row["status"]),
        version=int(row["version"]),
        due_at=row["due_at"],
        text=row["text"],
        owner=canonical_owner_display(row["owner"], row["owner_kind"]),
        due=row["due"],
        first_raised=row["first_raised"],
        last_mentioned=row["last_mentioned"],
        origin_kind=str(row["origin_kind"] or ""),
        origin_record=str(row["origin_record"] or ""),
        origin_item=str(row["origin_item"] or ""),
        origin_sources=stored_origin_sources(row["origin_payload"]),
        completion=_question(row),
    )


def _question(row) -> CardCompletionQuestion | None:
    if row["completion_id"] is None:
        return None
    return CardCompletionQuestion(
        id=int(row["completion_id"]),
        source_kind=str(row["completion_kind"] or ""),
        source_record_id=str(row["completion_record"] or ""),
        source_item_id=str(row["completion_item"] or ""),
        observed_at=str(row["completion_observed"] or ""),
        quotation=str(row["completion_quotation"] or ""),
        reason=str(row["completion_reason"] or ""),
        confidence=str(row["completion_confidence"] or ""),
    )


def _valid_limit(value: object) -> bool:
    return (not isinstance(value, bool) and isinstance(value, int)
            and 1 <= value <= 1_000)


def _valid_identity(card_id: object, version: object) -> bool:
    return (
        not isinstance(card_id, bool)
        and isinstance(card_id, int)
        and card_id >= 1
        and not isinstance(version, bool)
        and isinstance(version, int)
        and version >= 1
    )


def _valid_secret(value: object) -> bool:
    return (
        isinstance(value, str)
        and 32 <= len(value) <= 512
        and not any(char.isspace() for char in value)
    )


def _valid_opaque(value: object, maximum: int) -> bool:
    return (
        isinstance(value, str)
        and 1 <= len(value) <= maximum
        and value == value.strip()
        and not any(ord(char) < 32 or ord(char) == 127 for char in value)
    )


def _token_digest(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _valid_digest(value: object) -> bool:
    """A resolved consumer identity, shaped exactly like ``_token_digest``'s
    output: a lowercase hex sha256 digest, never the token itself (ADR 0036
    decision 1, invariant 1) and never empty or null-in-disguise.
    """
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(char in "0123456789abcdef" for char in value)
    )


def _card_guard(row, expected_version: int) -> CardRefusal | None:
    if row is None:
        return CardRefusal.NOT_FOUND
    if int(row["version"]) != expected_version:
        return CardRefusal.STALE_VERSION
    return None


def _refused(card_id: object, refusal: CardRefusal) -> CardOperationResult:
    return CardOperationResult(
        CardDisposition.REFUSED,
        card_id=card_id if isinstance(card_id, int) and not isinstance(card_id, bool) else 0,
        refusal=refusal,
    )


def _refused_row(card_id: int, row, refusal: CardRefusal) -> CardOperationResult:
    if row is None:
        return _refused(card_id, refusal)
    task_status = None
    if "task_status_current" in row.keys():
        task_status = TaskStatus(row["task_status_current"])
    return CardOperationResult(
        CardDisposition.REFUSED,
        card_id=card_id,
        version=int(row["version"]),
        status=CardStatus(row["status"]),
        task_version=(int(row["task_version_current"])
                      if "task_version_current" in row.keys()
                      else int(row["task_version"])),
        task_status=task_status,
        refusal=refusal,
    )


def _operation(row, disposition: CardDisposition) -> CardOperationResult:
    return CardOperationResult(
        disposition,
        card_id=int(row["id"]),
        version=int(row["version"]),
        status=CardStatus(row["status"]),
        task_version=int(row["task_version"]),
    )
