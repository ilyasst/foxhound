"""Reasons to believe an open task is finished, and what the reader said.

Nothing here closes anything. The engine this replaces did two things at once
and only one of them was wrong: measured over a legacy corpus of roughly 1,300
tasks, the silent high-confidence auto-close fired 21 times while the path that
asked first accounted for 524 closures — and about a third of the answers came
back "still open". An inference that wrong that often cannot be allowed to
close a task by itself; an inference that useful should not be thrown away. So
the judgement is kept and the automation is not: a detection becomes a question
on the task's own card, and a reader answers it.

What a row is
-------------
One quoted piece of evidence, the source and date it came from, and the stated
reason it was read as closing THIS task rather than a similar one. The reader
cannot check a match they were never shown, so all three are required and none
of them may be empty.

Identity is the evidence, not the judgement
-------------------------------------------
Two detections that quote the same sentence from the same source are one
question, however differently they word their reasoning, because the reader is
being asked to look at that sentence. That single choice buys two properties
the issue asks for separately:

  * re-detection is idempotent — one question per piece of evidence, not one
    per run;
  * a rejection is durable — a settled row still occupies the pair, so the
    same sentence can never raise the question twice.

The second is the failure this module exists to avoid. In the removed engine,
reopening a task cleared the fields that recorded why it had been closed, and
the next pass then re-closed it from the evidence the reader had just refused.
Here the refusal is the durable part.

Closing goes through the ledger
-------------------------------
Accepting a detection does not write a status here. The card applies the
ordinary task transition, exactly as it does when a reader closes a task
unprompted, and this module records only that the suggestion was accepted.
"""
from __future__ import annotations

import hashlib
import re
import sqlite3
from dataclasses import dataclass
from enum import StrEnum

MAX_QUOTATION = 1200
MAX_REASON = 500
MAX_DETECTOR = 64
MAX_SOURCE_KIND = 64
MAX_SOURCE_RECORD = 200
MAX_SOURCE_ITEM = 200
MAX_OBSERVED_AT = 40

#: How confident the detector is. Recorded and shown, never acted on: no value
#: here shortens the path to a closed task, because every value ends at the
#: same question.
CONFIDENCES = ("high", "medium", "low")

#: How many unanswered questions one task may carry. A card asks about one at
#: a time, so a detector that proposes without limit would queue questions
#: faster than anyone can answer them. The bound turns that into a visible
#: refusal instead of a backlog.
MAX_OPEN_QUESTIONS_PER_TASK = 5

_COLLAPSE_RE = re.compile(r"\s+")


class TaskCompletionError(ValueError):
    """A completion detection cannot be recorded safely."""


class ProposalDisposition(StrEnum):
    RECORDED = "recorded"
    UNCHANGED = "unchanged"
    REFUSED = "refused"


class ProposalRefusal(StrEnum):
    UNKNOWN_TASK = "unknown_task"
    TASK_NOT_OPEN = "task_not_open"
    TOO_MANY_OPEN_QUESTIONS = "too_many_open_questions"


class Outcome(StrEnum):
    """What became of one question. `SUPERSEDED` is not an answer: the task
    left the state the question was about — dropped, or closed by another
    route — before anyone got to it."""

    ACCEPTED = "accepted"
    REJECTED = "rejected"
    SUPERSEDED = "superseded"


@dataclass(frozen=True)
class CompletionEvidence:
    """Private card content; callers must not log or persist it."""

    id: int
    task_id: int
    evidence_digest: str
    source_kind: str
    source_record_id: str
    source_item_id: str
    observed_at: str
    quotation: str
    reason: str
    detector: str
    confidence: str
    state: str
    card_id: int | None
    created_at: str
    settled_at: str | None

    @property
    def open(self) -> bool:
        return self.state == "proposed"


@dataclass(frozen=True)
class ProposalResult:
    """Content-free result for one detection."""

    disposition: ProposalDisposition
    evidence_id: int | None = None
    refusal: ProposalRefusal | None = None

    @property
    def accepted(self) -> bool:
        return self.disposition is not ProposalDisposition.REFUSED


@dataclass(frozen=True)
class DetectorCounts:
    """Aggregate-only quality signal for one detector."""

    detector: str
    proposed: int
    accepted: int
    rejected: int
    superseded: int

    @property
    def answered(self) -> int:
        return self.accepted + self.rejected


def fold(value: str) -> str:
    """The sameness test for a quotation.

    Case and run-length of whitespace are how the same sentence differs
    between two renderings of one source — a transcript rewrapped, a summary
    re-cased. Neither makes it a different piece of evidence, and treating
    them as different is what would re-ask a question the reader answered.
    """
    return _COLLAPSE_RE.sub(" ", value).strip().casefold()


def evidence_digest(
    *,
    source_kind: str,
    source_record_id: str,
    source_item_id: str,
    quotation: str,
) -> str:
    """Identity of the quoted evidence. Excludes the reason and the detector:
    a second detector quoting the same sentence is the same question."""
    parts = (
        source_kind.strip().casefold(),
        source_record_id.strip(),
        source_item_id.strip(),
        fold(quotation),
    )
    joined = "\x1f".join(parts).encode("utf-8")
    return hashlib.sha256(joined).hexdigest()


def propose(
    connection: sqlite3.Connection,
    *,
    task_id: int,
    source_kind: str,
    source_record_id: str,
    observed_at: str,
    quotation: str,
    reason: str,
    detector: str,
    confidence: str,
    source_item_id: str = "",
    now: str,
) -> ProposalResult:
    """Record one reason to ask whether a task is done.

    Refuses rather than raises for the three states a detector cannot know in
    advance: the task was archived, closed, or is already carrying its share
    of unanswered questions. Invalid input raises, because that is a caller
    defect and not a state.
    """
    task_id = _identifier(task_id, "task id")
    source_kind = _bounded(source_kind, "source kind", MAX_SOURCE_KIND)
    source_record_id = _bounded(
        source_record_id, "source record id", MAX_SOURCE_RECORD)
    source_item_id = _optional(
        source_item_id, "source item id", MAX_SOURCE_ITEM)
    observed_at = _bounded(observed_at, "observed at", MAX_OBSERVED_AT)
    quotation = _quotation(quotation)
    reason = _bounded(reason, "reason", MAX_REASON)
    detector = _bounded(detector, "detector", MAX_DETECTOR)
    now = _bounded(now, "timestamp", MAX_OBSERVED_AT)
    if confidence not in CONFIDENCES:
        raise TaskCompletionError("confidence is invalid")

    row = connection.execute(
        "SELECT status FROM tasks WHERE id=?", (task_id,)
    ).fetchone()
    if row is None:
        return ProposalResult(
            ProposalDisposition.REFUSED,
            refusal=ProposalRefusal.UNKNOWN_TASK,
        )
    if row["status"] != "open":
        return ProposalResult(
            ProposalDisposition.REFUSED,
            refusal=ProposalRefusal.TASK_NOT_OPEN,
        )

    digest = evidence_digest(
        source_kind=source_kind,
        source_record_id=source_record_id,
        source_item_id=source_item_id,
        quotation=quotation,
    )
    # Asked before, in any state. A proposed row is the same question still
    # waiting; a settled one is the same question already answered. Neither is
    # asked again, and the caller is told nothing changed either way.
    existing = connection.execute(
        "SELECT id FROM task_completion_evidence "
        "WHERE task_id=? AND evidence_digest=?",
        (task_id, digest),
    ).fetchone()
    if existing is not None:
        return ProposalResult(
            ProposalDisposition.UNCHANGED, evidence_id=int(existing["id"])
        )

    open_questions = int(connection.execute(
        "SELECT count(*) FROM task_completion_evidence "
        "WHERE task_id=? AND state='proposed'",
        (task_id,),
    ).fetchone()[0])
    if open_questions >= MAX_OPEN_QUESTIONS_PER_TASK:
        return ProposalResult(
            ProposalDisposition.REFUSED,
            refusal=ProposalRefusal.TOO_MANY_OPEN_QUESTIONS,
        )

    cursor = connection.execute(
        "INSERT INTO task_completion_evidence("
        "task_id,evidence_digest,source_kind,source_record_id,source_item_id,"
        "observed_at,quotation,reason,detector,confidence,state,created_at) "
        "VALUES(?,?,?,?,?,?,?,?,?,?,'proposed',?)",
        (
            task_id,
            digest,
            source_kind,
            source_record_id,
            source_item_id,
            observed_at,
            quotation,
            reason,
            detector,
            confidence,
            now,
        ),
    )
    return ProposalResult(
        ProposalDisposition.RECORDED, evidence_id=int(cursor.lastrowid)
    )


def next_unasked(
    connection: sqlite3.Connection, task_id: int
) -> CompletionEvidence | None:
    """The oldest unanswered question for a task that no card is carrying.

    Oldest first, so a queue of detections drains in the order it arrived
    rather than by whichever the detector happened to rank highest — the
    ranking is the detector's opinion, and the point of this module is that
    its opinion does not decide anything on its own.
    """
    row = connection.execute(
        _SELECT + " WHERE task_id=? AND state='proposed' AND card_id IS NULL "
        "ORDER BY id LIMIT 1",
        (_identifier(task_id, "task id"),),
    ).fetchone()
    return None if row is None else _row(row)


def for_card(
    connection: sqlite3.Connection, card_id: int
) -> CompletionEvidence | None:
    """The question one card is carrying, if it is carrying one."""
    row = connection.execute(
        _SELECT + " WHERE card_id=? AND state='proposed'",
        (_identifier(card_id, "card id"),),
    ).fetchone()
    return None if row is None else _row(row)


def bind(
    connection: sqlite3.Connection, *, evidence_id: int, card_id: int
) -> bool:
    """Attach an unanswered question to the card that will ask it."""
    updated = connection.execute(
        "UPDATE task_completion_evidence SET card_id=? "
        "WHERE id=? AND state='proposed' AND card_id IS NULL",
        (
            _identifier(card_id, "card id"),
            _identifier(evidence_id, "evidence id"),
        ),
    )
    return updated.rowcount == 1


def release(connection: sqlite3.Connection, card_id: int) -> int:
    """Detach questions from a card that will never ask them.

    A cancelled card leaves its question unanswered, and a question bound to a
    card nobody will see is one that can never be asked again — the identity
    index would refuse a second row for the same evidence. So cancelling
    returns the question to the queue rather than burying it.
    """
    updated = connection.execute(
        "UPDATE task_completion_evidence SET card_id=NULL "
        "WHERE card_id=? AND state='proposed'",
        (_identifier(card_id, "card id"),),
    )
    return updated.rowcount


def settle(
    connection: sqlite3.Connection,
    *,
    evidence_id: int,
    outcome: Outcome,
    now: str,
) -> bool:
    """Record the answer. Once, and never again: the row is what the accept
    ratio is measured from, and one that could be rewritten would measure the
    last pass instead of the detector."""
    if outcome not in tuple(Outcome):
        raise TaskCompletionError("outcome is invalid")
    updated = connection.execute(
        "UPDATE task_completion_evidence SET state=?,settled_at=? "
        "WHERE id=? AND state='proposed'",
        (
            str(outcome),
            _bounded(now, "timestamp", MAX_OBSERVED_AT),
            _identifier(evidence_id, "evidence id"),
        ),
    )
    return updated.rowcount == 1


def settle_for_card(
    connection: sqlite3.Connection,
    *,
    card_id: int,
    outcome: Outcome,
    now: str,
) -> int:
    """Answer whatever question a card was carrying."""
    evidence = for_card(connection, card_id)
    if evidence is None:
        return 0
    return int(settle(
        connection, evidence_id=evidence.id, outcome=outcome, now=now
    ))


def counts(connection: sqlite3.Connection) -> tuple[DetectorCounts, ...]:
    """Per-detector totals, so a bad matcher is visible as a ratio.

    Aggregate only: no task, no source, no quotation. A detector whose
    rejected count dwarfs its accepted one is answering the wrong question,
    and that is readable here without opening a single card.
    """
    rows = connection.execute(
        "SELECT detector,"
        "SUM(state='proposed') AS proposed,"
        "SUM(state='accepted') AS accepted,"
        "SUM(state='rejected') AS rejected,"
        "SUM(state='superseded') AS superseded "
        "FROM task_completion_evidence GROUP BY detector ORDER BY detector"
    ).fetchall()
    return tuple(
        DetectorCounts(
            detector=row["detector"],
            proposed=int(row["proposed"] or 0),
            accepted=int(row["accepted"] or 0),
            rejected=int(row["rejected"] or 0),
            superseded=int(row["superseded"] or 0),
        )
        for row in rows
    )


_SELECT = (
    "SELECT id,task_id,evidence_digest,source_kind,source_record_id,"
    "source_item_id,observed_at,quotation,reason,detector,confidence,state,"
    "card_id,created_at,settled_at FROM task_completion_evidence"
)


def _row(row) -> CompletionEvidence:
    return CompletionEvidence(
        id=int(row["id"]),
        task_id=int(row["task_id"]),
        evidence_digest=row["evidence_digest"],
        source_kind=row["source_kind"],
        source_record_id=row["source_record_id"],
        source_item_id=row["source_item_id"],
        observed_at=row["observed_at"],
        quotation=row["quotation"],
        reason=row["reason"],
        detector=row["detector"],
        confidence=row["confidence"],
        state=row["state"],
        card_id=None if row["card_id"] is None else int(row["card_id"]),
        created_at=row["created_at"],
        settled_at=row["settled_at"],
    )


def _identifier(value, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise TaskCompletionError(f"{field} is invalid")
    return value


def _bounded(value, field: str, maximum: int) -> str:
    if not isinstance(value, str):
        raise TaskCompletionError(f"{field} must be text")
    value = value.strip()
    if not 1 <= len(value) <= maximum:
        raise TaskCompletionError(f"{field} has invalid length")
    if any(ord(char) < 32 or ord(char) == 127 for char in value):
        raise TaskCompletionError(f"{field} contains control characters")
    return value


def _optional(value, field: str, maximum: int) -> str:
    if not isinstance(value, str):
        raise TaskCompletionError(f"{field} must be text")
    return "" if not value.strip() else _bounded(value, field, maximum)


def _quotation(value) -> str:
    """Verbatim, so newlines survive: a quotation from a transcript is several
    turns, and flattening them makes two speakers read as one."""
    if not isinstance(value, str):
        raise TaskCompletionError("quotation must be text")
    value = value.strip()
    if not 1 <= len(value) <= MAX_QUOTATION:
        raise TaskCompletionError("quotation has invalid length")
    if any(ord(char) < 32 and char not in "\n\t" or ord(char) == 127
           for char in value):
        raise TaskCompletionError("quotation contains control characters")
    return value
