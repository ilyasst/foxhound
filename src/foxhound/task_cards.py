"""Foxhound-owned, transport-neutral task review cards."""

from __future__ import annotations

import hashlib
import html
import os
import secrets
import re
import sqlite3
from contextlib import closing
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import StrEnum
from pathlib import Path
from typing import Callable, Sequence

from .card_provenance import (
    CardSourceEvidence,
    origin_lines,
    origin_url,
    quotable,
    stored_origin_sources,
)

from . import task_completion as completion
from . import task_duplicate_proposals as duplicates
from . import task_relations
from . import fused_task_titles
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

TASK_CARD_ACTIONS = frozenset({
    "done",
    "keep_open",
    "drop",
    "snooze",
    "duplicate_confirm",
    "duplicate_reject",
    "show_full_cards",
})

#: Wire tokens for the controls that read a card instead of answering it.
#: Kept apart from `TASK_CARD_ACTIONS` because the separation is the
#: guarantee: a read reaches a route that writes nothing, and no list a
#: client validates against can let it arrive at `act` by accident.
DUPLICATE_EXPAND = "duplicate_expand"
DUPLICATE_COLLAPSE = "duplicate_collapse"

TASK_CARD_READS = frozenset({DUPLICATE_EXPAND, DUPLICATE_COLLAPSE})

#: What the expansion may spend on source extracts, per side, in bytes.
#:
#: The compact card is bounded by what it drops. The expansion is not bounded
#: by anything, and the transport's own ceiling is a hard one -- a body over
#: it is not shortened, it is refused, and the reader loses the card rather
#: than the evidence. So the expansion stops adding extracts and says how
#: many it left out. Whole extracts are dropped rather than the body being
#: cut to length: a body cut mid-tag renders as nothing at all.
EXPANDED_EVIDENCE_BUDGET_BYTES = 1_400

#: Workflow statuses that mean execution is finished with a task. Everything
#: else counts as execution still holding it.
#:
#: Stated as the terminal set rather than the live one on purpose. A status
#: added to the workflow later is then held by default, and the cost of that
#: default is a card we did not raise. Enumerating the live statuses instead
#: would let a new one fall through to this surface, where `done` and `drop`
#: close the task and cancel the workflow underneath it.
FINISHED_WORKFLOW_STATUSES = ("completed", "cancelled")

_FINISHED_WORKFLOW_SQL = ",".join(
    f"'{status}'" for status in FINISHED_WORKFLOW_STATUSES
)

#: A task whose workflow has not finished. Execution owns the question, asks
#: it with the evidence attached, and offers controls that do not close the
#: task as a side effect.
def _execution_holds(task_id: str) -> str:
    """SQL predicate: an unfinished workflow holds the named task.

    Parameterised by the task expression because the duplicate selection
    joins `tasks` twice. Both sides of a proposal have to be testable, and a
    predicate that could only say `t.id` is what let that selection keep
    asking about a task the workflow already held.
    """
    return (
        "EXISTS(SELECT 1 FROM task_execution_workflows AS workflow "
        f" WHERE workflow.task_id={task_id} "
        f" AND workflow.status NOT IN ({_FINISHED_WORKFLOW_SQL}))"
    )


_EXECUTION_HOLDS = _execution_holds("t.id")


def _preserved_open_withdrawal(task_id: str) -> str:
    """SQL predicate: the task outlived an accepted withdrawn candidate."""
    return (
        "EXISTS(SELECT 1 FROM task_candidate_bindings AS b JOIN "
        " task_candidate_lifecycle AS l ON l.candidate_id=b.candidate_id "
        f" WHERE b.task_id={task_id} AND b.relation='accepted' "
        " AND l.state='withdrawn' AND l.resolution='preserved_open')"
    )

#: A workflow that exists but is not in flight. A snooze defers work with no
#: deadline, so treating it as a hold kept a duplicate question unaskable for
#: as long as the snooze lasted -- while it still counted as unsettled, which
#: holds the gate that requires every proposal to be settled.
DORMANT_WORKFLOW_STATUSES = ("snoozed",)

_UNHELD_WORKFLOW_SQL = ",".join(
    f"'{status}'"
    for status in FINISHED_WORKFLOW_STATUSES + DORMANT_WORKFLOW_STATUSES
)


def _duplicate_execution_holds(task_id: str) -> str:
    """SQL predicate: a workflow holds this task against a duplicate question.

    Deliberately narrower than `_execution_holds`, and only for this question.
    The hold exists because confirming a duplicate closes one of the two
    tasks, which must not happen under work in flight. A snoozed workflow is
    not in flight, and closing its task is already safe: the scheduler cancels
    any unfinished workflow whose task stops being open, so a confirmed
    duplicate retires the snooze rather than stranding it.

    The ordinary task-card path keeps the wider predicate. Its reasons for
    waiting on a snoozed workflow are its own, and nothing measured here says
    they are wrong.
    """
    return (
        "EXISTS(SELECT 1 FROM task_execution_workflows AS workflow "
        f" WHERE workflow.task_id={task_id} "
        f" AND workflow.status NOT IN ({_UNHELD_WORKFLOW_SQL}))"
    )


#: The hold that governs one card, according to the question it asks.
_CARD_EXECUTION_HOLDS = (
    "CASE WHEN EXISTS(SELECT 1 FROM task_duplicate_proposals AS proposal "
    " WHERE proposal.card_id=c.id AND proposal.state='proposed') THEN "
    + _duplicate_execution_holds("t.id")
    + " ELSE " + _EXECUTION_HOLDS + " END"
)


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


# ADR 0036 invariant 5 fixes these ceilings in code rather than accepting
# them from callers. ADR 0036 invariant 10 requires revalidating the current
# gateway configuration before ever changing the drip ceiling.
TASK_CARD_CLAIM_CEILINGS = {
    "queue_view": 2,
    "drip": 20,
}

# A board is a glanceable, bounded projection rather than an export.  The
# ceiling is fixed here so a reader cannot turn a board request into an
# unbounded read of private card content.
BOARD_CARD_LIMIT = 100


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


#: A `delivering` row whose lease has run out. `claim_next` returns these to
#: `pending` before it serves anyone, so nothing is on screen for them and
#: nothing is being delivered. Shared so the reaper and the counts that
#: promise a revival cannot drift apart. Takes the current time as its one
#: bound parameter.
EXPIRED_DELIVERING = "(status='delivering' AND claim_expires_at<=?)"


@dataclass(frozen=True)
class CardStats:
    """Consumer-scoped, aggregate-only queue state from one snapshot."""

    pending: int
    delivering: int
    delivered: int
    snoozed: int
    elsewhere: int
    active: int


@dataclass(frozen=True)
class TaskBoard:
    """Current reader-visible intake cards and their true column totals."""

    cards: tuple["TaskReviewCard", ...]
    review_total: int
    snoozed_total: int


@dataclass(frozen=True)
class TaskCardRequeueResult:
    """Content-free result of re-presenting unanswered task-review cards."""

    requeued: int = 0


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
class CardDuplicateProposal:
    """Private side-by-side task comparison carried by one review card."""

    id: int
    other_task_id: int
    other_status: str
    other_text: str = field(repr=False)
    other_origin_kind: str = field(repr=False)
    other_origin_record: str = field(repr=False)
    other_origin_item: str = field(repr=False)
    other_origin_sources: tuple[CardSourceEvidence, ...] = field(
        default=(), repr=False
    )
    other_owner: str | None = field(default=None, repr=False)
    other_due: str | None = field(default=None, repr=False)
    other_raised: str | None = field(default=None, repr=False)
    other_closed_at: str | None = field(default=None, repr=False)
    basis: str = field(default="", repr=False)


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
    participants: tuple[str, ...] = field(default=(), repr=False)
    confidence: float | None = field(default=None, repr=False)
    first_raised: str | None = field(default=None, repr=False)
    task_created: str | None = field(default=None, repr=False)
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
    duplicate: CardDuplicateProposal | None = field(default=None, repr=False)
    #: The source revision displayed by this card. It is the stale-action
    #: fence, not reader content.
    source_revision: str | None = field(default=None, repr=False)
    #: A previous card was delivered for this task, but this one carries a
    #: later source revision. No semantic diff is inferred from digests.
    source_changed: bool = field(default=False, repr=False)


@dataclass(frozen=True)
class DeliveryClaim:
    card: TaskReviewCard
    token: str = field(repr=False)
    expires_at: str


@dataclass(frozen=True)
class ClaimAtCeiling:
    """Content-free result when a consumer has filled its claim capacity."""

    held_count: int
    ceiling: int

    @property
    def at_ceiling(self) -> bool:
        return True


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


@dataclass(frozen=True)
class CardPresentation:
    """One delivered card rendered again, or the refusal instead.

    `card` is absent rather than partial on a refusal: a caller that cannot
    show the card must not be handed something that looks like it could be.
    """

    disposition: CardDisposition
    card_id: int
    card_version: int | None = None
    card: TaskReviewCard | None = field(default=None, repr=False)
    expanded: bool = False
    refusal: CardRefusal | None = None

    @property
    def accepted(self) -> bool:
        return self.disposition is not CardDisposition.REFUSED


@dataclass(frozen=True)
class FullCardsResult:
    """The two standard full cards delivered for a duplicate pair."""

    disposition: CardDisposition
    card_id: int
    card_version: int | None = None
    cards: tuple[TaskReviewCard, ...] = ()
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

    def schedule(self, *, limit: int = 100) -> ScheduleResult:
        """Raise the cards that have something to say, and nothing else.

        This used to also raise a periodic "☑️ Task done?" card for every
        open task on a seven-day rhythm. That card asked a question the
        ledger could not answer and the reader had no new information to
        answer either: it started no work, and answering "still open" only
        moved the same question a week out.

        Worse, it was loudest exactly when it was least useful. A task with
        no execution workflow is not held by `_execution_holds`, so whenever
        admission stalled -- a saturated `plan_ready_cap`, see the sibling
        scheduler -- every task the system had refused to start became
        card-eligible and stayed that way. One deployment answered 69 of
        them in a day against a baseline of 5, each one asking whether work
        was finished that had never been allowed to begin. A failure in
        admission was being reported to the reader as a question about
        completion.

        What remains are the two cards that carry information the reader
        does not already have: a done-check, raised only when evidence
        suggests the task is finished, and a duplicate check, raised only
        when intake has proposed one. Both are answers to something, not
        a rhythm.
        """
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
                created = 0
                asked, raised = self._ask_completion_questions(
                    connection, now, limit=limit
                )
                duplicate_asked, duplicate_raised = self._ask_duplicate_proposals(
                    connection, now, limit=limit
                )
                created += raised
                created += duplicate_raised
                asked += duplicate_asked
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

    def schedule_duplicate_proposals(self, *, limit: int = 100) -> ScheduleResult:
        """Bind proposed duplicates to reader cards without scheduling tasks.

        Native intake has already made the narrow decision to propose a
        duplicate.  Its timer needs to make that question deliverable, but it
        must not incidentally create the ordinary task-review cards owned by
        the broader scheduler above.
        """
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
                superseded = self._resolve_unaskable_proposals(
                    connection, now)
                asked, raised = self._ask_duplicate_proposals(
                    connection, now, limit=limit
                )
                connection.commit()
                return ScheduleResult(
                    CardDisposition.APPLIED
                    if cancelled or asked or superseded
                    else CardDisposition.UNCHANGED,
                    created=raised,
                    cancelled=cancelled,
                    asked=asked,
                )
            except Exception:
                connection.rollback()
                raise

    def view(
        self, card_id: int, *, expected_version: int, expanded: bool
    ) -> CardPresentation:
        """One delivered duplicate card, rendered again at another detail.

        A read. Nothing is written and no version moves, so the answers on
        the keyboard it returns still address the card the reader was looking
        at -- a restored keyboard whose taps all answer "stale" would be the
        same dead end reached more slowly.

        Delivered and current are both required. A card that was never
        delivered has no presentation to reopen, and one already answered or
        superseded must not be handed back looking answerable.

        Only a duplicate card has a second detail to show. Every other card
        already shows everything it holds, so asking to expand one is a
        caller error rather than an empty result.
        """
        if not _valid_identity(card_id, expected_version) or not isinstance(
                expanded, bool):
            return _view_refused(
                card_id if isinstance(card_id, int)
                and not isinstance(card_id, bool) else 0,
                None, CardRefusal.INVALID_ARGUMENT,
            )
        with closing(self._connect()) as connection:
            row = connection.execute(
                self._card_select() + " WHERE c.id=?", (card_id,)
            ).fetchone()
            refusal = _card_guard(row, expected_version)
            if refusal is None and row["status"] != CardStatus.DELIVERED:
                refusal = CardRefusal.INVALID_STATE
            if refusal is None and not _card_is_current(connection, row):
                refusal = CardRefusal.STALE_VERSION
            if refusal is not None:
                return _view_refused(card_id, row, refusal)
            card = _card(row)
        if card.duplicate is None:
            return _view_refused(card_id, row, CardRefusal.INVALID_STATE)
        return CardPresentation(
            CardDisposition.UNCHANGED,
            card_id,
            card_version=expected_version,
            card=card,
            expanded=expanded,
        )

    def show_full_cards(
        self, card_id: int, *, expected_version: int
    ) -> FullCardsResult:
        """Deliver the standard full cards for both members of a duplicate pair."""
        if not _valid_identity(card_id, expected_version):
            return FullCardsResult(
                CardDisposition.REFUSED,
                card_id if isinstance(card_id, int) and not isinstance(card_id, bool) else 0,
                refusal=CardRefusal.INVALID_ARGUMENT,
            )
        now_dt = self._clock_value()
        now = now_dt.isoformat(timespec="seconds")
        with closing(self._connect()) as connection:
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = connection.execute(
                    self._card_select() + " WHERE c.id=?", (card_id,)
                ).fetchone()
                refusal = _card_guard(row, expected_version)
                if refusal is not None:
                    connection.rollback()
                    return FullCardsResult(
                        CardDisposition.REFUSED, card_id,
                        card_version=None if row is None else int(row["version"]),
                        refusal=refusal,
                    )
                if row["status"] != CardStatus.DELIVERED:
                    connection.rollback()
                    return FullCardsResult(
                        CardDisposition.REFUSED, card_id,
                        card_version=int(row["version"]),
                        refusal=CardRefusal.INVALID_STATE,
                    )
                if not _card_is_current(connection, row):
                    connection.rollback()
                    return FullCardsResult(
                        CardDisposition.REFUSED, card_id,
                        card_version=int(row["version"]),
                        refusal=CardRefusal.STALE_VERSION,
                    )
                card = _card(row)
                if card.duplicate is None:
                    connection.rollback()
                    return FullCardsResult(
                        CardDisposition.REFUSED, card_id,
                        card_version=int(row["version"]),
                        refusal=CardRefusal.INVALID_STATE,
                    )

                task_id_a = card.task_id
                task_id_b = card.duplicate.other_task_id

                # Both tasks must be retrievable
                task_a = connection.execute(
                    "SELECT id, status, version FROM tasks WHERE id=?",
                    (task_id_a,),
                ).fetchone()
                task_b = connection.execute(
                    "SELECT id, status, version FROM tasks WHERE id=?",
                    (task_id_b,),
                ).fetchone()
                if task_a is None or task_b is None:
                    connection.rollback()
                    return FullCardsResult(
                        CardDisposition.REFUSED, card_id,
                        card_version=int(row["version"]),
                        refusal=CardRefusal.NOT_FOUND,
                    )

                # Cancel the comparison card
                next_version = expected_version + 1
                connection.execute(
                    "UPDATE task_review_cards SET status='cancelled',version=?,"
                    "claim_token_digest=NULL,claim_expires_at=NULL,consumer_digest=NULL,"
                    "resolved_at=?,updated_at=? WHERE id=? AND version=?",
                    (next_version, now, now, card_id, expected_version),
                )
                self._event(
                    connection, card_id=card_id, task_id=task_id_a,
                    kind="cancelled", card_version=next_version,
                    task_version=int(row["task_version"]), now=now,
                )

                # Deliver standard cards for both tasks
                delivered_cards: list[TaskReviewCard] = []
                for task in (task_a, task_b):
                    tid = int(task["id"])
                    tver = int(task["version"])
                    rev = connection.execute(
                        "SELECT b.source_revision FROM task_candidate_bindings AS b "
                        "WHERE b.task_id=? AND b.relation='accepted'",
                        (tid,),
                    ).fetchone()
                    source_rev = rev[0] if rev else None

                    transport = row["transport"] or "synthetic"
                    delivery_ref = f"{row['delivery_ref'] or 'card'}-t{tid}"
                    consumer = row["consumer_digest"]

                    cursor = connection.execute(
                        "INSERT INTO task_review_cards("
                        "task_id,task_version,source_revision,status,version,"
                        "due_at,transport,delivery_ref,delivered_at,consumer_digest,"
                        "created_at,updated_at) VALUES(?,?,?,'delivered',1,?,?,?,?,?,?,?)",
                        (
                            tid, tver, source_rev, now, transport,
                            delivery_ref, now, consumer, now, now,
                        ),
                    )
                    new_card_id = int(cursor.lastrowid)
                    self._event(
                        connection, card_id=new_card_id, task_id=tid,
                        kind="scheduled", card_version=1,
                        task_version=tver, now=now,
                    )
                    self._event(
                        connection, card_id=new_card_id, task_id=tid,
                        kind="delivered", card_version=1,
                        task_version=tver, now=now,
                    )
                    new_row = connection.execute(
                        self._card_select() + " WHERE c.id=?",
                        (new_card_id,),
                    ).fetchone()
                    delivered_cards.append(_card(new_row))

                connection.commit()
                return FullCardsResult(
                    CardDisposition.APPLIED, card_id=card_id,
                    card_version=next_version,
                    cards=tuple(delivered_cards),
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
                "AND NOT EXISTS(SELECT 1 FROM task_relations AS relation "
                " WHERE relation.subject_id=t.id AND relation.kind='duplicate_of' "
                " AND relation.withdrawn_at IS NULL) "
                    "AND COALESCE(c.source_revision,'')=COALESCE("
                    + _bound_source_revision("t.id") + ",'') "
                "ORDER BY c.due_at,c.id LIMIT ?",
                (now, limit),
            ).fetchall()
        return tuple(_card(row) for row in rows)

    def board(self, *, limit: int = BOARD_CARD_LIMIT) -> TaskBoard:
        """Return current intake work without claiming or changing a card."""
        if (isinstance(limit, bool) or not isinstance(limit, int)
                or not 1 <= limit <= BOARD_CARD_LIMIT):
            raise TaskLedgerError("task board limit is invalid")
        where = (
            " WHERE c.status IN ('pending','snoozed') "
            "AND t.status='open' AND t.version=c.task_version "
            "AND NOT EXISTS(SELECT 1 FROM task_relations AS relation "
            " WHERE relation.subject_id=t.id AND relation.kind='duplicate_of' "
            " AND relation.withdrawn_at IS NULL) "
            "AND COALESCE(c.source_revision,'')=COALESCE("
            + _bound_source_revision("t.id") + ",'') "
        )
        with closing(self._connect()) as connection:
            totals = {
                str(row["status"]): int(row["total"])
                for row in connection.execute(
                    "SELECT c.status,COUNT(*) AS total FROM task_review_cards AS c "
                    "JOIN tasks AS t ON t.id=c.task_id"
                    + where + "GROUP BY c.status"
                ).fetchall()
            }
            rows = connection.execute(
                self._card_select() + where
                + "ORDER BY CASE c.status WHEN 'pending' THEN 0 ELSE 1 END,"
                "c.due_at,c.id LIMIT ?",
                (limit,),
            ).fetchall()
        return TaskBoard(
            cards=tuple(_card(row) for row in rows),
            review_total=totals.get(CardStatus.PENDING.value, 0),
            snoozed_total=totals.get(CardStatus.SNOOZED.value, 0),
        )

    def claim_next(
        self, *, lease_seconds: int = 60, consumer_digest: str,
        consumer_role: str = "drip",
    ) -> DeliveryClaim | ClaimAtCeiling | None:
        if (isinstance(lease_seconds, bool)
                or not isinstance(lease_seconds, int)
                or not 5 <= lease_seconds <= 300):
            raise TaskLedgerError("task card delivery lease is invalid")
        if not _valid_digest(consumer_digest):
            raise TaskLedgerError("task card consumer digest is invalid")
        try:
            ceiling = TASK_CARD_CLAIM_CEILINGS[consumer_role]
        except (KeyError, TypeError) as exc:
            raise TaskLedgerError("task card consumer role is invalid") from exc
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
                    f"FROM task_review_cards WHERE {EXPIRED_DELIVERING} "
                    "ORDER BY id",
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
                held_count = connection.execute(
                    "SELECT count(*) FROM task_review_cards "
                    "WHERE status IN ('delivering','delivered') "
                    "AND consumer_digest=?",
                    (consumer_digest,),
                ).fetchone()[0]
                if held_count >= ceiling:
                    connection.commit()
                    return ClaimAtCeiling(
                        held_count=int(held_count), ceiling=ceiling
                    )
                row = connection.execute(
                    self._card_select()
                    + " WHERE c.status IN ('pending','snoozed') "
                    "AND c.due_at<=? AND t.status='open' "
                    "AND NOT EXISTS(SELECT 1 FROM task_relations AS relation "
                    " WHERE relation.subject_id=t.id AND relation.kind='duplicate_of' "
                    " AND relation.withdrawn_at IS NULL) "
                    "AND t.version=c.task_version "
                    "AND COALESCE(c.source_revision,'')=COALESCE("
                    + _bound_source_revision("t.id") + ",'') "
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

    def resolve(
        self, card_id: int, *, expected_version: int, action: str,
        consumer_digest: str, consumer_role: str,
    ) -> CardOperationResult | ClaimAtCeiling | None:
        """Claim one exact card, record internal delivery, then apply action.

        These are intentionally three committed operations, not one database
        transaction. Delivery is committed before ``act`` is attempted, so a
        failure after delivery can leave a delivered card for operator repair,
        but can never apply an action without its delivery record.
        """
        if (not _valid_identity(card_id, expected_version)
                or action not in TASK_CARD_ACTIONS
                or not _valid_digest(consumer_digest)):
            return _refused(card_id, CardRefusal.INVALID_ARGUMENT)
        try:
            ceiling = TASK_CARD_CLAIM_CEILINGS[consumer_role]
        except (KeyError, TypeError) as exc:
            raise TaskLedgerError("task card consumer role is invalid") from exc
        now_dt = self._clock_value()
        now = now_dt.isoformat(timespec="seconds")
        token = self._token_factory()
        if not _valid_secret(token):
            raise TaskLedgerError("task card token factory returned invalid state")
        token_digest = _token_digest(token)
        with closing(self._connect()) as connection:
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute("BEGIN IMMEDIATE")
            try:
                held = connection.execute(
                    "SELECT count(*) FROM task_review_cards WHERE "
                    "status IN ('delivering','delivered') AND consumer_digest=?",
                    (consumer_digest,),
                ).fetchone()[0]
                if held >= ceiling:
                    connection.commit()
                    return ClaimAtCeiling(int(held), ceiling)
                row = connection.execute(
                    self._card_select() + " WHERE c.id=?", (card_id,)
                ).fetchone()
                if row is None:
                    connection.rollback()
                    return None
                if int(row["version"]) != expected_version:
                    connection.rollback()
                    return _refused_row(card_id, row, CardRefusal.STALE_VERSION)
                if row["status"] not in (CardStatus.PENDING, CardStatus.SNOOZED):
                    connection.rollback()
                    return _refused_row(card_id, row, CardRefusal.STALE_VERSION)
                if (row["task_status_current"] != TaskStatus.OPEN
                        or int(row["task_version_current"])
                        != int(row["task_version"])
                        or (row["source_revision"] or None)
                        != (row["source_revision_current"] or None)):
                    connection.rollback()
                    return _refused_row(card_id, row, CardRefusal.STALE_VERSION)
                next_version = expected_version + 1
                expires = (now_dt + timedelta(seconds=60)).isoformat(
                    timespec="seconds"
                )
                updated = connection.execute(
                    "UPDATE task_review_cards SET status='delivering',"
                    "version=?,claim_token_digest=?,claim_expires_at=?,"
                    "consumer_digest=?,transport=NULL,delivery_ref=NULL,"
                    "delivered_at=NULL,updated_at=? WHERE id=? AND version=? "
                    "AND status IN ('pending','snoozed')",
                    (next_version, token_digest, expires, consumer_digest,
                     now, card_id, expected_version),
                )
                if updated.rowcount != 1:
                    connection.rollback()
                    return _refused_row(card_id, row, CardRefusal.STALE_VERSION)
                self._event(connection, card_id=card_id,
                            task_id=int(row["task_id"]),
                            kind="delivery_claimed", card_version=next_version,
                            task_version=int(row["task_version"]), now=now)
                connection.commit()
            except Exception:
                connection.rollback()
                raise
        delivered = self.complete_delivery(
            card_id, expected_version=next_version, claim_token=token,
            transport="resolved", delivery_ref=f"resolve-{card_id}-v{next_version}",
        )
        if not delivered.accepted:
            return delivered
        return self.act(card_id, expected_version=next_version, action=action)

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
                current_revision = connection.execute(
                    "SELECT b.source_revision FROM task_candidate_bindings AS b "
                    "WHERE b.task_id=? AND b.relation='accepted'",
                    (int(row["task_id"]),),
                ).fetchone()
                if (row["source_revision"] or None) != (
                    None if current_revision is None else current_revision[0]
                ):
                    connection.rollback()
                    return _refused_row(card_id, row, CardRefusal.STALE_VERSION)
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

    def retry_delivery(
        self, card_id: int, *, expected_version: int
    ) -> CardOperationResult:
        """Requeue a current delivered card for local operator recovery.

        This is a local operator repair, not part of the transport API. It
        clears the presentation metadata and consumer affinity, then versions
        the card so callbacks from the failed presentation become stale.
        """
        if not _valid_identity(card_id, expected_version):
            return _refused(card_id, CardRefusal.INVALID_ARGUMENT)
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
                    and row["status"] != CardStatus.DELIVERED
                ):
                    refusal = CardRefusal.INVALID_STATE
                if (
                    refusal is None
                    and (
                        row["task_status_current"] != TaskStatus.OPEN
                        or int(row["task_version_current"])
                        != int(row["task_version"])
                        or (row["source_revision"] or None)
                        != (row["source_revision_current"] or None)
                    )
                ):
                    refusal = CardRefusal.STALE_VERSION
                if refusal is not None:
                    connection.rollback()
                    return _refused_row(card_id, row, refusal)
                version = expected_version + 1
                updated = connection.execute(
                    "UPDATE task_review_cards SET status='pending',"
                    "version=?,claim_token_digest=NULL,claim_expires_at=NULL,"
                    "consumer_digest=NULL,transport=NULL,delivery_ref=NULL,"
                    "delivered_at=NULL,updated_at=? WHERE id=? AND version=? "
                    "AND status='delivered'",
                    (version, now, card_id, expected_version),
                )
                if updated.rowcount != 1:
                    raise TaskLedgerError("task card state changed")
                self._event(
                    connection,
                    card_id=card_id,
                    task_id=int(row["task_id"]),
                    kind="delivery_failed",
                    card_version=version,
                    task_version=int(row["task_version"]),
                    now=now,
                )
                connection.commit()
                values = dict(row)
                values.update(
                    status=CardStatus.PENDING,
                    version=version,
                    claim_token_digest=None,
                    claim_expires_at=None,
                    consumer_digest=None,
                    transport=None,
                    delivery_ref=None,
                    delivered_at=None,
                )
                return _operation(values, CardDisposition.APPLIED)
            except Exception:
                connection.rollback()
                raise

    def requeue_unanswered(
        self, *, limit: int = 100
    ) -> TaskCardRequeueResult:
        """Re-present current task-review cards unanswered for one hour.

        A delivered row otherwise occupies the reader surface forever when a
        presentation disappears.  Requeueing clears only transport and claim
        state, so the replacement receives a new version while the task
        itself remains unchanged.
        """
        if not _valid_limit(limit):
            return TaskCardRequeueResult()
        stamp = self._clock_value()
        now = stamp.isoformat(timespec="seconds")
        due = (stamp - timedelta(hours=1)).isoformat(timespec="seconds")
        with closing(self._connect()) as connection:
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute("BEGIN IMMEDIATE")
            try:
                self._cancel_stale(connection, now)
                rows = connection.execute(
                    self._card_select()
                    + " WHERE c.status='delivered' AND c.delivered_at<=? "
                    "ORDER BY c.delivered_at,c.id LIMIT ?",
                    (due, limit),
                ).fetchall()
                requeued = 0
                for row in rows:
                    if (
                        row["task_status_current"] != TaskStatus.OPEN
                        or int(row["task_version_current"])
                        != int(row["task_version"])
                        or (row["source_revision"] or None)
                        != (row["source_revision_current"] or None)
                    ):
                        continue
                    version = int(row["version"]) + 1
                    updated = connection.execute(
                        "UPDATE task_review_cards SET status='pending',"
                        "version=?,claim_token_digest=NULL,claim_expires_at=NULL,"
                        "consumer_digest=NULL,transport=NULL,delivery_ref=NULL,"
                        "delivered_at=NULL,updated_at=? WHERE id=? AND version=? "
                        "AND status='delivered'",
                        (version, now, int(row["id"]), int(row["version"])),
                    )
                    if updated.rowcount != 1:
                        raise TaskLedgerError("task card state changed")
                    self._event(
                        connection,
                        card_id=int(row["id"]),
                        task_id=int(row["task_id"]),
                        kind="delivery_failed",
                        card_version=version,
                        task_version=int(row["task_version"]),
                        now=now,
                    )
                    requeued += 1
                connection.commit()
                return TaskCardRequeueResult(requeued=requeued)
            except Exception:
                connection.rollback()
                raise

    def act(
        self, card_id: int, *, expected_version: int, action: str
    ) -> CardOperationResult:
        if not _valid_identity(card_id, expected_version):
            return _refused(card_id, CardRefusal.INVALID_ARGUMENT)
        if action not in TASK_CARD_ACTIONS:
            return _refused(card_id, CardRefusal.INVALID_ACTION)
        now_dt = self._clock_value()
        now = now_dt.isoformat(timespec="seconds")
        with closing(self._connect()) as connection:
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = connection.execute(
                    "SELECT c.*,t.status AS task_status_current,"
                    "t.version AS task_version_current,"
                    + _bound_source_revision("t.id") + " AS source_revision_current "
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
                        != int(row["task_version"])
                        or (row["source_revision"] or None)
                        != (row["source_revision_current"] or None)):
                    connection.rollback()
                    return _refused_row(card_id, row, CardRefusal.STALE_VERSION)

                if action in {"duplicate_confirm", "duplicate_reject"}:
                    result = self._act_duplicate(
                        connection, row=row, action=action, now=now
                    )
                    if result is None:
                        connection.rollback()
                        return _refused_row(card_id, row, CardRefusal.STALE_VERSION)
                    connection.commit()
                    return result

                if action == "show_full_cards":
                    connection.rollback()
                    full_res = self.show_full_cards(
                        card_id, expected_version=expected_version
                    )
                    if not full_res.accepted:
                        return _refused_row(
                            card_id, row,
                            full_res.refusal or CardRefusal.INVALID_STATE,
                        )
                    return CardOperationResult(
                        CardDisposition.APPLIED,
                        card_id=card_id,
                        version=expected_version + 1,
                        status=CardStatus.CANCELLED,
                        task_version=int(row["task_version"]),
                        task_status=TaskStatus.OPEN,
                    )

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

    def reverse_duplicate(self, relation_id: int) -> bool:
        """Withdraw one reader-confirmed duplicate relation and re-offer it."""
        if not isinstance(relation_id, int) or isinstance(relation_id, bool):
            return False
        now = self._now()
        with closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                relation = task_relations.get(connection, relation_id)
                if (not relation.live or relation.kind != "duplicate_of"
                        or relation.asserted_by != "reader"):
                    connection.rollback()
                    return False
                proposal = connection.execute(
                    "SELECT id FROM task_duplicate_proposals "
                    "WHERE left_task_id=? AND right_task_id=? AND state='confirmed'",
                    (relation.object_id, relation.subject_id),
                ).fetchone()
                if proposal is None:
                    connection.rollback()
                    return False
                task_relations.withdraw(
                    connection, relation_id, withdrawn_by="reader"
                )
                self._reopen_task(
                    connection, task_id=relation.subject_id, now=now,
                )
                if not duplicates.reopen_confirmed(
                    connection, proposal_id=int(proposal["id"]),
                    actor="reader", now=now,
                ):
                    connection.rollback()
                    return False
                fused_task_titles.refresh_after_withdrawal(
                    connection, task_id=relation.object_id, now=now,
                )
                connection.commit()
                return True
            except (task_relations.TaskRelationError, sqlite3.Error):
                connection.rollback()
                return False

    def count(self) -> int:
        with closing(self._connect()) as connection:
            return int(connection.execute(
                "SELECT count(*) FROM task_review_cards"
            ).fetchone()[0])

    def stats(self, *, consumer_digest: str) -> CardStats:
        """Report the queue as it IS, counting a dead lease as pending.

        A `delivering` row whose lease has expired is not on anyone's
        screen: the send either never happened or was never acknowledged,
        and `claim_next` will return it to `pending` the moment it is asked
        for another card. Reporting it as `delivering` claims a place on a
        surface that is actually empty.

        That is not cosmetic. A consumer sizes its next batch by subtracting
        `delivering` and `delivered` from the depth it wants on screen, and
        stops before claiming when the answer is zero. The reaper that
        revives these rows lives inside `claim_next`, so once enough dead
        leases accumulate to fill that depth, the consumer stops claiming,
        the reaper stops running, and neither side ever recovers. The queue
        deadlocks with every card waiting and nothing being delivered.

        `EXPIRED_DELIVERING` is deliberately the same comparison the reaper
        uses. If the two ever disagree, this count promises a revival that
        does not happen.
        """
        if not _valid_digest(consumer_digest):
            raise TaskLedgerError("task card consumer digest is invalid")
        now = self._now()
        with closing(self._connect()) as connection:
            row = connection.execute(
                "SELECT "
                f"SUM(CASE WHEN status='pending' OR {EXPIRED_DELIVERING} "
                "THEN 1 ELSE 0 END) AS pending,"
                "SUM(CASE WHEN status='delivering' AND consumer_digest=? "
                f"AND NOT {EXPIRED_DELIVERING} "
                "THEN 1 ELSE 0 END) AS delivering,"
                "SUM(CASE WHEN status='delivered' AND consumer_digest=? "
                "THEN 1 ELSE 0 END) AS delivered,"
                "SUM(CASE WHEN status='snoozed' THEN 1 ELSE 0 END) AS snoozed,"
                "SUM(CASE WHEN consumer_digest IS NOT NULL "
                "AND consumer_digest<>? AND (status='delivered' OR "
                f"(status='delivering' AND NOT {EXPIRED_DELIVERING})) "
                "THEN 1 ELSE 0 END) AS elsewhere,"
                "SUM(CASE WHEN status IN "
                "('pending','delivering','delivered','snoozed') "
                "THEN 1 ELSE 0 END) AS active "
                "FROM task_review_cards",
                (now, consumer_digest, now, consumer_digest,
                 consumer_digest, now),
            ).fetchone()
        return CardStats(*(
            int(row[name] or 0)
            for name in (
                "pending", "delivering", "delivered", "snoozed",
                "elsewhere", "active",
            )
        ))

    def stats_global(self) -> CardStats:
        """Return the legacy unscoped queue snapshot for v1 clients.

        A dead lease counts as pending here for the same reason it does in
        `stats`: a v1 consumer sizes its batch the same way.
        """
        now = self._now()
        with closing(self._connect()) as connection:
            row = connection.execute(
                "SELECT "
                f"SUM(CASE WHEN status='pending' OR {EXPIRED_DELIVERING} "
                "THEN 1 ELSE 0 END) AS pending,"
                "SUM(CASE WHEN status='delivering' "
                f"AND NOT {EXPIRED_DELIVERING} "
                "THEN 1 ELSE 0 END) AS delivering,"
                "SUM(CASE WHEN status='delivered' THEN 1 ELSE 0 END) AS delivered,"
                "SUM(CASE WHEN status='snoozed' THEN 1 ELSE 0 END) AS snoozed,"
                "SUM(CASE WHEN status IN "
                "('pending','delivering','delivered','snoozed') "
                "THEN 1 ELSE 0 END) AS active "
                "FROM task_review_cards",
                (now, now),
            ).fetchone()
        return CardStats(
            pending=int(row["pending"] or 0),
            delivering=int(row["delivering"] or 0),
            delivered=int(row["delivered"] or 0),
            snoozed=int(row["snoozed"] or 0),
            elsewhere=0,
            active=int(row["active"] or 0),
        )

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
            "t.version AS task_version,"
            + _bound_source_revision("t.id") + " AS source_revision "
            "FROM task_completion_evidence AS e "
            "JOIN tasks AS t ON t.id=e.task_id "
            "WHERE e.state='proposed' AND e.card_id IS NULL "
            "AND t.status='open' "
            # The answer to a done-check closes the task, which cancels the
            # workflow underneath it. While execution holds the task, that
            # question is not this surface's to ask.
            "AND NOT " + _EXECUTION_HOLDS + " "
            "AND NOT " + _preserved_open_withdrawal("t.id") + " "
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
                    "task_id,task_version,source_revision,status,version,"
                    "due_at,created_at,updated_at) VALUES(?,?,?,'pending',1,?,?,?)",
                    (task_id, int(row["task_version"]),
                     row["source_revision"], now, now, now),
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

    def _resolve_unaskable_proposals(
        self, connection: sqlite3.Connection, now: str
    ) -> int:
        """Stop counting a question nobody can ever be asked.

        `_ask_duplicate_proposals` fences on an exact task-version match, and
        the recorded versions are immutable by design -- the settle-only
        trigger refuses to change them, because a proposal is a fixed record
        of two tasks as they were. So once either task advances, that
        proposal can never be carded, and it is not a delay: it is permanent.

        Leaving it `proposed` is worse than losing the question. The
        recorded-assessment gate refuses while any proposal is unsettled, so a
        single unaskable pair closes it for good.

        Superseding says the question expired. It does not answer it, and it
        is deliberately not a reader decision: pair uniqueness ignores
        superseded rows, so the detector may raise the pair again at current
        versions with a current basis. Re-asking is then a new question rather
        than an old one edited to look current.
        """
        current_left = "(SELECT version FROM tasks WHERE id=left_task_id)"
        current_right = "(SELECT version FROM tasks WHERE id=right_task_id)"
        open_left = "(SELECT status FROM tasks WHERE id=left_task_id)='open'"
        open_right = "(SELECT status FROM tasks WHERE id=right_task_id)='open'"
        relation_left = (
            "(SELECT count(1) FROM task_relations WHERE subject_id=left_task_id "
            "AND kind='duplicate_of' AND withdrawn_at IS NULL)>0"
        )
        relation_right = (
            "(SELECT count(1) FROM task_relations WHERE subject_id=right_task_id "
            "AND kind='duplicate_of' AND withdrawn_at IS NULL)>0"
        )
        carded_task = (
            f"CASE WHEN {open_left} THEN left_task_id ELSE right_task_id END"
        )
        updated = connection.execute(
            "UPDATE task_duplicate_proposals SET state='superseded',"
            "settled_at=?,updated_at=? "
            "WHERE state='proposed' AND card_id IS NULL "
            # Either the comparison has moved, or there is no open task left
            # to consolidate into, an active duplicate_of relation makes
            # delivery permanently impossible, or the chosen open task was
            # deliberately preserved after its accepted source disappeared.
            f"AND (left_task_version<>{current_left} "
            f"OR right_task_version<>{current_right} "
            f"OR (NOT {open_left} AND NOT {open_right}) "
            f"OR {relation_left} OR {relation_right} OR "
            + _preserved_open_withdrawal(carded_task)
            + ")",
            (now, now),
        )
        return int(updated.rowcount)

    def _ask_duplicate_proposals(
        self, connection: sqlite3.Connection, now: str, *, limit: int
    ) -> tuple[int, int]:
        """Bind one unread duplicate proposal to its open task card."""
        rows = connection.execute(
            "SELECT d.id AS proposal_id,"
            "CASE WHEN left_task.status='open' THEN left_task.id "
            "ELSE right_task.id END AS task_id,"
            "CASE WHEN left_task.status='open' THEN d.left_task_version "
            "ELSE d.right_task_version END AS task_version,"
            + _bound_source_revision(
                "CASE WHEN left_task.status='open' THEN left_task.id "
                "ELSE right_task.id END"
            ) + " AS source_revision,"
            "(SELECT c.id FROM task_review_cards AS c WHERE c.task_id="
            "(CASE WHEN left_task.status='open' THEN left_task.id "
            "ELSE right_task.id END) "
            " AND c.status IN ('pending','delivering','delivered','snoozed')) "
            " AS active_id,"
            "(SELECT c.status FROM task_review_cards AS c WHERE c.task_id="
            "(CASE WHEN left_task.status='open' THEN left_task.id "
            "ELSE right_task.id END) "
            " AND c.status IN ('pending','delivering','delivered','snoozed')) "
            " AS active_status "
            "FROM task_duplicate_proposals AS d "
            "JOIN tasks AS left_task ON left_task.id=d.left_task_id "
            "JOIN tasks AS right_task ON right_task.id=d.right_task_id "
            "WHERE d.state='proposed' AND d.card_id IS NULL "
            # Either side, not just the carded one: confirming a duplicate
            # closes one of the two, so a workflow holding EITHER makes the
            # question unsafe to ask.
            #
            # Without this the selection fought `_cancel_stale`, which
            # retracts on the same hold and releases the proposal's card
            # binding as it goes. The retraction restored exactly the
            # condition this selection looks for -- a proposed pair with no
            # card -- so every pass cancelled a card and raised another, and
            # the reader was sent every one of them.
            "AND NOT " + _duplicate_execution_holds("left_task.id") + " "
            "AND NOT " + _duplicate_execution_holds("right_task.id") + " "
            # `_cancel_stale` retracts every card for a task preserved after
            # its accepted source was withdrawn. Selecting that task again
            # would release and re-bind the proposal on every scheduler pass.
            "AND NOT " + _preserved_open_withdrawal(
                "CASE WHEN left_task.status='open' THEN left_task.id "
                "ELSE right_task.id END"
            ) + " "
            "AND left_task.version=d.left_task_version "
            "AND right_task.version=d.right_task_version "
            "AND ((left_task.status='open' AND right_task.status "
            "IN ('open','done','dropped')) OR (right_task.status='open' "
            "AND left_task.status IN ('done','dropped'))) "
            "ORDER BY d.id LIMIT ?",
            (limit,),
        ).fetchall()
        asked = raised = 0
        # A task can carry several proposals at once -- three copies of one
        # commitment guarantee it -- but only one active card, and a card shows
        # exactly one comparison.  The candidate rows were read in a single
        # query, so their view of what is already active does not see cards
        # raised earlier in this same pass; without tracking that here, the
        # second proposal for a task inserts a second active card and violates
        # `task_review_cards_one_active`.  The remaining proposals keep their
        # place and are asked once this card is settled.
        handled: set[int] = set()
        for row in rows:
            task_id = int(row["task_id"])
            if task_id in handled:
                continue
            status = row["active_status"]
            if status is not None and status != CardStatus.PENDING:
                continue
            if status == CardStatus.PENDING:
                card_id = int(row["active_id"])
                if connection.execute(
                    "SELECT 1 FROM task_duplicate_proposals "
                    "WHERE card_id=? AND state='proposed'", (card_id,)
                ).fetchone() is not None:
                    # That card already asks a different comparison.
                    handled.add(task_id)
                    continue
                connection.execute(
                    "UPDATE task_review_cards SET due_at=?,updated_at=? "
                    "WHERE id=? AND status='pending'", (now, now, card_id)
                )
            else:
                cursor = connection.execute(
                    "INSERT INTO task_review_cards("
                    "task_id,task_version,source_revision,status,version,"
                    "due_at,created_at,updated_at) VALUES(?,?,?,'pending',1,?,?,?)",
                    (task_id, int(row["task_version"]),
                     row["source_revision"], now, now, now),
                )
                card_id = int(cursor.lastrowid)
                self._event(
                    connection, card_id=card_id, task_id=task_id,
                    kind="scheduled", card_version=1,
                    task_version=int(row["task_version"]), now=now,
                )
                raised += 1
            handled.add(task_id)
            if duplicates.bind(
                connection, proposal_id=int(row["proposal_id"]),
                card_id=card_id, now=now,
            ):
                asked += 1
        return asked, raised

    def _act_duplicate(self, connection: sqlite3.Connection, *, row,
                       action: str, now: str) -> CardOperationResult | None:
        proposal = connection.execute(
            "SELECT * FROM task_duplicate_proposals WHERE card_id=? "
            "AND state='proposed'", (int(row["id"]),)
        ).fetchone()
        if proposal is None:
            return None
        tasks = connection.execute(
            "SELECT id,status,version FROM tasks WHERE id IN (?,?) ORDER BY id",
            (int(proposal["left_task_id"]), int(proposal["right_task_id"])),
        ).fetchall()
        if (len(tasks) != 2 or not duplicates.reviewable_status_pair(tasks)
                or int(tasks[0]["version"]) != int(proposal["left_task_version"])
                or int(tasks[1]["version"]) != int(proposal["right_task_version"])):
            return None
        active_execution = connection.execute(
            "SELECT 1 FROM task_execution_workflows WHERE task_id IN (?,?) "
            "AND status IN ('running','awaiting_review') LIMIT 1",
            (int(proposal["left_task_id"]), int(proposal["right_task_id"])),
        ).fetchone()
        if active_execution is not None:
            return None
        proposal_id = int(proposal["id"])
        if action == "duplicate_confirm":
            if tasks[0]["status"] == "open" and tasks[1]["status"] == "open":
                subject_id = int(proposal["right_task_id"])
                object_id = int(proposal["left_task_id"])
            else:
                open_task = next(item for item in tasks if item["status"] == "open")
                closed_task = next(item for item in tasks if item["status"] != "open")
                subject_id = int(open_task["id"])
                object_id = int(closed_task["id"])
            try:
                task_relations.assert_relation(
                    connection,
                    subject_id=subject_id,
                    object_id=object_id,
                    kind="duplicate_of", basis=str(proposal["basis"])[:500],
                    asserted_by="reader", actor="reader",
                )
            except task_relations.TaskRelationError:
                return None
            # This is durable local bookkeeping only. The remote call happens
            # after this reader action commits.
            fused_task_titles.enqueue(connection, task_id=object_id, now=now)
            subject_version = (
                int(proposal["right_task_version"])
                if subject_id == int(proposal["right_task_id"])
                else int(proposal["left_task_version"])
            )
            transition = _apply_task_transition(
                connection,
                task_id=subject_id,
                expected_version=subject_version,
                action="drop",
                now=now,
            )
            if not transition.accepted:
                return None
            decision = duplicates.Decision.CONFIRMED
            self._cancel_duplicate_task_cards(
                connection, task_id=subject_id, now=now,
                except_card_id=int(row["id"]),
            )
        else:
            decision = duplicates.Decision.REJECTED
        if not duplicates.settle(
            connection, proposal_id=proposal_id, decision=decision,
            actor="reader", now=now,
        ):
            return None
        next_version = int(row["version"]) + 1
        connection.execute(
            "UPDATE task_review_cards SET status='cancelled',version=?,"
            "claim_token_digest=NULL,claim_expires_at=NULL,consumer_digest=NULL,"
            "resolved_at=?,updated_at=? WHERE id=? AND version=?",
            (next_version, now, now, int(row["id"]), int(row["version"])),
        )
        self._event(
            connection, card_id=int(row["id"]), task_id=int(row["task_id"]),
            kind="cancelled", card_version=next_version,
            task_version=int(row["task_version"]), now=now,
        )
        subject_status = (
            TaskStatus.DROPPED if action == "duplicate_confirm" else TaskStatus.OPEN
        )
        return CardOperationResult(
            CardDisposition.APPLIED, card_id=int(row["id"]),
            version=next_version, status=CardStatus.CANCELLED,
            task_version=int(row["task_version"]), task_status=subject_status,
        )

    @staticmethod
    def _reopen_task(
        connection: sqlite3.Connection, *, task_id: int, now: str,
    ) -> None:
        """Reopen a task that was dropped by a confirmed duplicate.

        Idempotent: if the task is already open it writes nothing.
        """
        connection.execute(
            "UPDATE tasks SET status='open',version=version+1,"
            "updated_at=?,closed_at=NULL "
            "WHERE id=? AND status='dropped'",
            (now, task_id),
        )

    def _cancel_duplicate_task_cards(
        self, connection: sqlite3.Connection, *, task_id: int, now: str,
        except_card_id: int | None = None,
    ) -> None:
        rows = connection.execute(
            "SELECT id,task_version,version FROM task_review_cards "
            "WHERE task_id=? AND status IN ('pending','delivering','delivered','snoozed')",
            (task_id,),
        ).fetchall()
        for stale in rows:
            if int(stale["id"]) == except_card_id:
                continue
            next_version = int(stale["version"]) + 1
            connection.execute(
                "UPDATE task_review_cards SET status='cancelled',version=?,"
                "claim_token_digest=NULL,claim_expires_at=NULL,consumer_digest=NULL,"
                "resolved_at=?,updated_at=? WHERE id=? AND version=?",
                (next_version, now, now, int(stale["id"]), int(stale["version"])),
            )
            completion.release(connection, int(stale["id"]))
            duplicates.release_for_card(connection, int(stale["id"]), now=now)
            self._event(
                connection, card_id=int(stale["id"]), task_id=task_id,
                kind="cancelled", card_version=next_version,
                task_version=int(stale["task_version"]), now=now,
            )

    def _cancel_stale(self, connection: sqlite3.Connection, now: str) -> int:
        rows = connection.execute(
            "SELECT c.id,c.task_id,c.task_version,c.version,c.source_revision "
            "FROM task_review_cards AS c JOIN tasks AS t ON t.id=c.task_id "
            "WHERE c.status IN ('pending','delivering','delivered','snoozed') "
            "AND (t.status!='open' OR t.version!=c.task_version "
            "OR " + _CARD_EXECUTION_HOLDS + " "
            "OR COALESCE(c.source_revision,'')!=COALESCE("
            + _bound_source_revision("t.id") + ",'') OR EXISTS("
            " SELECT 1 FROM task_relations AS relation "
            " WHERE relation.subject_id=t.id AND relation.kind='duplicate_of' "
            " AND relation.withdrawn_at IS NULL) OR "
            + _preserved_open_withdrawal("t.id")
            + ") ORDER BY c.id"
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
            duplicates.release_for_card(connection, int(row["id"]), now=now)
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
            "c.source_revision,c.transport,c.delivery_ref,c.consumer_digest,"
            "COALESCE((SELECT job.title FROM task_fused_title_jobs AS job "
            "WHERE job.task_id=c.task_id AND job.state='ready'),t.text) AS text,"
            "t.owner,t.owner_kind,t.due,t.confidence,t.created_at AS task_created,"
            + _owner_display("t") + " AS owner_display,"
            "(SELECT group_concat(CASE participant.kind "
            " WHEN 'unresolved' THEN '(unresolved speaker)' "
            " WHEN 'external' THEN '(external participant)' "
            " WHEN 'group' THEN '(group participant)' "
            " ELSE COALESCE((SELECT entry.display_name "
            " FROM speaker_registry_entries AS entry "
            " WHERE entry.speaker_registry_id=participant.speaker_registry_id "
            " AND entry.speaker_id=COALESCE(participant.canonical_speaker_id,"
            " participant.speaker_id)),'(unresolved speaker)') END,char(30)) "
            " FROM task_participants AS participant "
            " WHERE participant.task_id=t.id ORDER BY participant.position) "
            " AS participants_display,"
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
            "(SELECT group_concat(payload_json, char(30)) FROM ("
            " SELECT h.payload_json AS payload_json FROM task_candidate_bindings AS b "
            " JOIN candidate_revision_history AS h ON h.candidate_id=b.candidate_id "
            " AND h.source_revision=b.source_revision "
            " WHERE b.relation='accepted' AND (b.task_id=c.task_id OR b.task_id IN ("
            "  SELECT relation.subject_id FROM task_relations AS relation "
            "  WHERE relation.object_id=c.task_id AND relation.kind='duplicate_of' "
            "  AND relation.withdrawn_at IS NULL)) ORDER BY h.created_at)) "
            " AS origin_payloads,"
            "EXISTS(SELECT 1 FROM task_review_cards AS seen "
            " WHERE seen.task_id=c.task_id "
            " AND seen.source_revision<>c.source_revision "
            " AND EXISTS(SELECT 1 FROM task_review_card_events AS event "
            "            WHERE event.card_id=seen.id AND event.kind='delivered')) "
            " AS source_changed,"
            # A card carries at most one unanswered question -- scheduling
            # binds one at a time -- so this join never multiplies rows.
            "e.id AS completion_id,e.source_kind AS completion_kind,"
            "e.source_record_id AS completion_record,"
            "e.source_item_id AS completion_item,"
            "e.observed_at AS completion_observed,"
            "e.quotation AS completion_quotation,"
            "e.reason AS completion_reason,"
            "e.confidence AS completion_confidence,"
            "d.id AS duplicate_id,"
            "CASE WHEN d.left_task_id=c.task_id THEN d.right_task_id "
            "ELSE d.left_task_id END AS duplicate_other_task_id,"
            "other.status AS duplicate_other_status,"
            "other.text AS duplicate_other_text,"
            + _owner_display("other") + " AS duplicate_other_owner,"
            "other.owner_kind AS duplicate_other_owner_kind,"
            "other.due AS duplicate_other_due,"
            "COALESCE((SELECT min(h.created_at) "
            " FROM task_candidate_bindings AS b "
            " JOIN candidate_revision_history AS h "
            " ON h.candidate_id=b.candidate_id WHERE b.task_id=other.id),"
            " other.created_at) AS duplicate_other_created,"
            "other.closed_at AS duplicate_other_closed,"
            "d.basis AS duplicate_basis,"
            "(SELECT o.source_kind FROM task_candidate_bindings AS b "
            " JOIN candidate_inbox AS o ON o.candidate_id=b.candidate_id "
            " WHERE b.task_id=(CASE WHEN d.left_task_id=c.task_id "
            " THEN d.right_task_id ELSE d.left_task_id END) "
            " AND b.relation='accepted') "
            " AS duplicate_other_kind,"
            "(SELECT o.source_record_id FROM task_candidate_bindings AS b "
            " JOIN candidate_inbox AS o ON o.candidate_id=b.candidate_id "
            " WHERE b.task_id=(CASE WHEN d.left_task_id=c.task_id "
            " THEN d.right_task_id ELSE d.left_task_id END) "
            " AND b.relation='accepted') "
            " AS duplicate_other_record,"
            "(SELECT o.source_item_id FROM task_candidate_bindings AS b "
            " JOIN candidate_inbox AS o ON o.candidate_id=b.candidate_id "
            " WHERE b.task_id=(CASE WHEN d.left_task_id=c.task_id "
            " THEN d.right_task_id ELSE d.left_task_id END) "
            " AND b.relation='accepted') "
            " AS duplicate_other_item,"
            "(SELECT h.payload_json FROM task_candidate_bindings AS b "
            " JOIN candidate_revision_history AS h ON h.candidate_id=b.candidate_id "
            " AND h.source_revision=b.source_revision "
            " WHERE b.task_id=(CASE WHEN d.left_task_id=c.task_id "
            " THEN d.right_task_id ELSE d.left_task_id END) "
            " AND b.relation='accepted') "
            " AS duplicate_other_payload,"
            "t.status AS task_status_current,t.version AS task_version_current,"
            + _bound_source_revision("t.id") + " AS source_revision_current "
            "FROM task_review_cards AS c "
            "JOIN tasks AS t ON t.id=c.task_id "
            "LEFT JOIN task_completion_evidence AS e "
            "ON e.card_id=c.id AND e.state='proposed' "
            "LEFT JOIN task_duplicate_proposals AS d "
            "ON d.card_id=c.id AND d.state='proposed' "
            "LEFT JOIN tasks AS other ON other.id="
            "(CASE WHEN d.left_task_id=c.task_id THEN d.right_task_id "
            "ELSE d.left_task_id END)"
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
    """Render one claimed card without performing I/O.

    The plain "☑️ Task done?" branch below is no longer reachable for any
    card raised after `schedule()` stopped creating them. It stays because
    a deployment upgrading mid-flight can still be holding one that was
    raised before, and a reader who has it on screen must be able to answer
    it rather than watch it fail to render. Once no such row remains
    anywhere -- `SELECT COUNT(*) FROM task_review_cards WHERE status IN
    ('pending','delivering','delivered','snoozed') AND duplicate/completion
    are both absent` -- this branch and its buttons can go.
    """
    if card.status not in (CardStatus.DELIVERING, CardStatus.DELIVERED):
        raise ValueError("task review card is not claimed for delivery")
    text = html.escape(card.text, quote=False)
    if card.duplicate is not None:
        return _render_duplicate_check(card, text)
    if card.completion is not None:
        return _render_done_check(card, text)
    lines = [f"☑️ <b>Task done?</b>  <code>T{card.task_id}</code>", "", f"<b>{text}</b>"]
    if card.source_changed:
        lines.extend((
            "",
            "🔄 <b>Source updated since you last saw this task</b>",
        ))
    if card.owner:
        lines.extend(("", f"👤 <b>Owner:</b> {html.escape(card.owner, quote=False)}"))
    if card.participants:
        lines.append(
            "👥 <b>Participants:</b> "
            + html.escape(", ".join(card.participants), quote=False)
        )
    if card.due:
        lines.append(f"📅 <b>Due:</b> {html.escape(card.due, quote=False)}")
    if card.confidence is not None:
        lines.append(f"📊 <b>Extraction confidence:</b> {card.confidence:.0%}")
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


def render_duplicate_view(
    card: TaskReviewCard, *, expanded: bool
) -> tuple[str, dict]:
    """Render a delivered duplicate card again, at the asked-for detail.

    Separate from `render_task_review_card` on its status guard alone: that
    one renders a card claimed for delivery, this one renders a card the
    reader already has on screen and is looking at again.
    """
    if card.status is not CardStatus.DELIVERED:
        raise ValueError("task review card is not delivered")
    if card.duplicate is None:
        raise ValueError("task review card carries no comparison to open")
    return _render_duplicate_check(
        card, html.escape(card.text, quote=False), expanded=expanded,
    )


def _render_duplicate_check(
    card: TaskReviewCard, text: str, *, expanded: bool = False
) -> tuple[str, dict]:
    """Ask the reader to adjudicate one suspected duplicate.

    The question is only answerable from facts the reader can compare, so the
    card carries them for both sides: who owns it, when it is due, when it was
    raised, and where it came from.  Two tasks that read alike but were raised
    months apart and fall due in different terms are a recurrence, not a copy,
    and nothing in the two sentences alone shows that.

    The detector's own reason is shown as well.  A reader who can see that the
    pair was matched on one re-read mail thread, rather than on a handful of
    shared words, knows how much to trust the suggestion before answering.

    `expanded` renders the same comparison without those compressions: every
    source extract each side carries, and the detector's basis in its own
    words.  It is what the reader opens when the compact card is not enough
    to decide on, and there is nowhere else to look -- the counterpart is
    usually already closed, and a closed task has no card of its own.
    """
    duplicate = card.duplicate
    recent_closed = duplicate.other_status in {"done", "dropped"}
    lines = [
        f"🔀 <b>Same task?</b>  <code>T{card.task_id}</code> and "
        f"<code>T{duplicate.other_task_id}</code>",
        "",
        f"<b>Task T{card.task_id}</b>", text,
        *_comparison_facts(
            owner=card.owner, due=card.due,
            raised=card.first_raised or card.task_created,
            last=card.last_mentioned, status="open", closed_at=None,
        ),
        *_comparison_origin(
            kind=card.origin_kind, record=card.origin_record,
            item=card.origin_item, sources=card.origin_sources,
            expanded=expanded,
        ),
        "",
        f"<b>Task T{duplicate.other_task_id}</b>",
        _escape(duplicate.other_text),
        *_comparison_facts(
            owner=duplicate.other_owner, due=duplicate.other_due,
            raised=duplicate.other_raised, last=None,
            status=duplicate.other_status, closed_at=duplicate.other_closed_at,
        ),
        *_comparison_origin(
            kind=duplicate.other_origin_kind,
            record=duplicate.other_origin_record,
            item=duplicate.other_origin_item,
            sources=duplicate.other_origin_sources,
            expanded=expanded,
        ),
    ]
    reason = (
        _escape(duplicate.basis) if expanded
        else _duplicate_reason(duplicate.basis)
    )
    if reason:
        lines.extend(("", f"🔎 <b>Matched on:</b> {reason}"))
    lines.extend((
        "",
        (
            "Confirming records this open task as the same recently closed task "
            "and preserves both sources."
            if recent_closed
            else "Confirming keeps the lower-numbered task and preserves both sources."
        ),
    ))

    def callback(action: str) -> str:
        value = f"{CALLBACK_PREFIX}|{card.id}|{card.version}|{action}"
        if len(value.encode("utf-8")) > CALLBACK_DATA_LIMIT:
            raise ValueError("task review callback exceeds transport limit")
        return value

    keyboard = {"inline_keyboard": [
        [
            {
                "text": "✅ Already completed" if recent_closed else "✅ Same task",
                "callback_data": callback("duplicate_confirm"),
            },
            {"text": "↔️ Keep separate", "callback_data": callback("duplicate_reject")},
            {
                "text": "↩️ Less" if expanded else "🔍 Show both in full",
                "callback_data": callback(
                    DUPLICATE_COLLAPSE if expanded else DUPLICATE_EXPAND
                ),
            },
        ],
        [
            {
                "text": "📋 Show full cards",
                "callback_data": callback("show_full_cards"),
            },
        ],
    ]}
    return "\n".join(lines), keyboard


def _comparison_origin(
    *, kind: str, record: str, item: str,
    sources: Sequence[CardSourceEvidence], expanded: bool,
) -> list[str]:
    """One side's provenance, at the detail the surface asked for.

    Compact drops the handoff extract and keeps the rest. Expanded keeps
    everything that fits and says what it could not fit, because a reader who
    opened the comparison for the evidence must not be shown a shortened list
    that looks complete.
    """
    if not expanded:
        return origin_lines(
            kind=kind, record=record, item=item,
            sources=_quotable_sources(sources), html_output=True,
        )
    kept, dropped = _affordable_sources(sources)
    lines = origin_lines(
        kind=kind, record=record, item=item, sources=kept, html_output=True,
    )
    if dropped:
        lines.append(
            f"⋯ <i>{dropped} further extract"
            f"{'' if dropped == 1 else 's'} not shown here.</i>"
        )
    return lines


def _affordable_sources(
    sources: Sequence[CardSourceEvidence],
) -> tuple[tuple[CardSourceEvidence, ...], int]:
    """As many whole extracts as the body can afford, and the count left out.

    At least one is always kept. A side whose single extract is longer than
    the whole budget is exactly the side worth reading, and answering it with
    provenance alone would be the compact card again.
    """
    kept: list[CardSourceEvidence] = []
    spent = 0
    for source in sources:
        cost = len(source.name.encode("utf-8")) + len(
            quotable(source.extract).encode("utf-8"))
        if kept and spent + cost > EXPANDED_EVIDENCE_BUDGET_BYTES:
            break
        kept.append(source)
        spent += cost
    return tuple(kept), len(sources) - len(kept)


def _quotable_sources(
    sources: Sequence[CardSourceEvidence],
) -> tuple[CardSourceEvidence, ...]:
    """Evidence a reader can read, without the machine handoff.

    The handoff extract is the task record itself rendered as JSON: it repeats
    the sentence printed directly above it and adds the owner and due date that
    the facts line already carries. On a card showing two tasks at once it
    crowds out the one thing worth reading, which is the source's own words.
    Dropped only here; a single-task card has the room.
    """
    kept = tuple(source for source in sources if source.role != "handoff")
    return kept or tuple(sources)


def _comparison_facts(*, owner: str | None, due: str | None,
                      raised: str | None, last: str | None,
                      status: str, closed_at: str | None) -> tuple[str, ...]:
    """One compact line of the facts that separate a copy from a recurrence."""
    parts: list[str] = []
    if owner:
        parts.append(f"👤 {_escape(owner)}")
    if due:
        parts.append(f"📅 due {_escape(str(due)[:10])}")
    if raised:
        parts.append(f"📌 raised {_escape(str(raised)[:10])}")
    if last and str(last)[:10] != str(raised or "")[:10]:
        parts.append(f"🕑 last seen {_escape(str(last)[:10])}")
    if status in {"done", "dropped"}:
        closed = f" {str(closed_at)[:10]}" if closed_at else ""
        parts.append(f"🔒 {_escape(status)}{_escape(closed)}")
    return (" · ".join(parts),) if parts else ()


def _duplicate_reason(basis: str) -> str:
    """The detector's evidence, in the reader's words rather than its own."""
    if not basis:
        return ""
    if "later reading" in basis:
        return "the same source was read again later and carded twice"
    match = re.search(r"shared task terms[^:]*:\s*(.+)$", basis)
    if match:
        terms = ", ".join(
            term.strip() for term in match.group(1).split(",")[:8] if term.strip()
        )
        return f"shared wording — {_escape(terms)}" if terms else ""
    return _escape(basis[:180])


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
            or action not in TASK_CARD_ACTIONS):
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
        owner=canonical_owner_display(row["owner_display"], row["owner_kind"]),
        due=row["due"],
        participants=tuple(filter(None, str(row["participants_display"] or "").split(chr(30)))),
        confidence=(None if row["confidence"] is None else float(row["confidence"])),
        first_raised=row["first_raised"],
        task_created=_optional_text(row["task_created"]),
        last_mentioned=row["last_mentioned"],
        origin_kind=str(row["origin_kind"] or ""),
        origin_record=str(row["origin_record"] or ""),
        origin_item=str(row["origin_item"] or ""),
        origin_sources=_stored_origin_sources(row["origin_payloads"]),
        completion=_question(row),
        duplicate=_duplicate(row),
        source_revision=row["source_revision"],
        source_changed=bool(row["source_changed"]),
    )


def _owner_display(task: str) -> str:
    """SQL scalar for the name to show beside one task.

    Parameterised by the task expression because a comparison card carries
    two of them. Both sides have to read the same way: a card asking whether
    two tasks are the same is answered partly on who owns each, and an owner
    that resolves on one side and not on the other is not a comparison.

    The registry's current name wins, because a label cached on the task is
    whatever it was called when it was raised. Where the registry has no
    entry the cached label is shown rather than withheld: it is the only
    name anyone has, `canonical_owner_display` already strips a raw speaker
    token out of it, and an owner the reader cannot read is the one fact
    this line exists to carry.

    An owner that is genuinely unassigned is named by
    `canonical_owner_display`, from the kind, on the way out. Saying it here
    as well would be the same rule written twice, in two vocabularies, free
    to drift.
    """
    return (
        "COALESCE((SELECT entry.display_name FROM speaker_registry_entries "
        " AS entry WHERE entry.speaker_registry_id="
        f"{task}.owner_speaker_registry_id AND entry.speaker_id=COALESCE("
        f"{task}.owner_canonical_speaker_id,{task}.owner_speaker_id)),"
        f"{task}.owner)"
    )


def _bound_source_revision(task_id: str) -> str:
    """SQL scalar for the accepted candidate revision of one task.

    A task can be created without a producer candidate, so callers compare the
    result with ``COALESCE`` rather than treating NULL as a stale revision.
    The accepted-binding invariant makes this scalar unambiguous.
    """
    return (
        "(SELECT b.source_revision FROM task_candidate_bindings AS b "
        f"WHERE b.task_id={task_id} AND b.relation='accepted')"
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


def _stored_origin_sources(value: object) -> tuple[CardSourceEvidence, ...]:
    if not isinstance(value, str) or not value:
        return ()
    return tuple(
        source for payload in value.split("\x1e")
        for source in stored_origin_sources(payload)
    )


def _duplicate(row) -> CardDuplicateProposal | None:
    if row["duplicate_id"] is None:
        return None
    return CardDuplicateProposal(
        id=int(row["duplicate_id"]),
        other_task_id=int(row["duplicate_other_task_id"]),
        other_status=str(row["duplicate_other_status"] or ""),
        other_text=str(row["duplicate_other_text"] or ""),
        other_origin_kind=str(row["duplicate_other_kind"] or ""),
        other_origin_record=str(row["duplicate_other_record"] or ""),
        other_origin_item=str(row["duplicate_other_item"] or ""),
        other_origin_sources=stored_origin_sources(row["duplicate_other_payload"]),
        other_owner=canonical_owner_display(
            _optional_text(row["duplicate_other_owner"]),
            row["duplicate_other_owner_kind"],
        ),
        other_due=_optional_text(row["duplicate_other_due"]),
        other_raised=_optional_text(row["duplicate_other_created"]),
        other_closed_at=_optional_text(row["duplicate_other_closed"]),
        basis=str(row["duplicate_basis"] or ""),
    )


def _optional_text(value: object) -> str | None:
    text = "" if value is None else str(value).strip()
    return text or None


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


def _card_is_current(connection: sqlite3.Connection, row) -> bool:
    """The card still describes the task as it stands now.

    The same fence `act` applies, asked without writing: a read that restored
    a card the reader could no longer answer would hand back a keyboard whose
    every tap refuses.
    """
    task = connection.execute(
        "SELECT t.status,t.version," + _bound_source_revision("t.id")
        + " AS source_revision FROM tasks AS t WHERE t.id=?",
        (int(row["task_id"]),),
    ).fetchone()
    return (
        task is not None
        and task["status"] == TaskStatus.OPEN
        and int(task["version"]) == int(row["task_version"])
        and (task["source_revision"] or None) == (row["source_revision"] or None)
    )


def _view_refused(card_id: int, row, refusal: CardRefusal) -> CardPresentation:
    return CardPresentation(
        CardDisposition.REFUSED,
        card_id=card_id,
        card_version=None if row is None else int(row["version"]),
        refusal=refusal,
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
