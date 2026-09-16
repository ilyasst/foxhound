"""Private, reader-gated proposals to consolidate duplicate tasks.

Source candidates are intentionally independent: an email and a meeting are
not the same source item merely because both describe one commitment.  This
module records the later, fallible judgement that their durable tasks may be
the same.  It never changes a task, asserts a task relation, or schedules a
card; those reader actions belong to the following slices.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import StrEnum


MAX_BASIS = 1_200
MAX_DETECTOR = 64
MAX_ACTOR = 200
MAX_OPEN_PROPOSALS_PER_TASK = 5
RECENTLY_CLOSED_DAYS = 30


class DuplicateProposalError(ValueError):
    """The caller supplied an invalid duplicate-proposal argument."""


class ProposalDisposition(StrEnum):
    RECORDED = "recorded"
    UNCHANGED = "unchanged"
    REFUSED = "refused"


class ProposalRefusal(StrEnum):
    SAME_TASK = "same_task"
    UNKNOWN_TASK = "unknown_task"
    TASK_NOT_OPEN = "task_not_open"
    INCOMPATIBLE_OWNER = "incompatible_owner"
    TOO_MANY_OPEN_PROPOSALS = "too_many_open_proposals"


class Decision(StrEnum):
    CONFIRMED = "confirmed"
    REJECTED = "rejected"


@dataclass(frozen=True)
class DuplicateProposal:
    """Private proposal content; do not include it in operational output."""

    id: int
    left_task_id: int
    right_task_id: int
    left_task_version: int
    right_task_version: int
    basis: str
    detector: str
    state: str
    created_at: str
    updated_at: str
    settled_at: str | None
    card_id: int | None

    @property
    def open(self) -> bool:
        return self.state == "proposed"


@dataclass(frozen=True)
class ProposalResult:
    """Content-free result for one detector attempt."""

    disposition: ProposalDisposition
    proposal_id: int | None = None
    refusal: ProposalRefusal | None = None

    @property
    def accepted(self) -> bool:
        return self.disposition is not ProposalDisposition.REFUSED


@dataclass(frozen=True)
class ProposalCounts:
    """Content-free, event-based quality counts for one detector."""

    detector: str
    proposed: int
    confirmed: int
    rejected: int
    reopened: int


def propose(
    connection: sqlite3.Connection,
    *,
    task_id_a: int,
    task_id_b: int,
    basis: str,
    detector: str,
    now: str,
    allow_unconfirmed_owner: bool = False,
) -> ProposalResult:
    """Record one unordered candidate pair for later reader review.

    State a detector cannot know in advance is a content-free refusal.  Bad
    argument shape is a caller bug and raises instead.  An existing row in any
    state is unchanged: a reader who rejected a pair must not see it again
    merely because a later scan phrases its basis differently.
    """
    task_id_a = _identifier(task_id_a, "first task id")
    task_id_b = _identifier(task_id_b, "second task id")
    basis = _bounded(basis, "proposal basis", MAX_BASIS)
    detector = _bounded(detector, "detector", MAX_DETECTOR)
    now = _bounded(now, "timestamp", 40)
    if task_id_a == task_id_b:
        return ProposalResult(
            ProposalDisposition.REFUSED,
            refusal=ProposalRefusal.SAME_TASK,
        )
    left_task_id, right_task_id = sorted((task_id_a, task_id_b))
    rows = connection.execute(
        "SELECT id,status,closed_at,version,owner_ref_version,owner_kind,"
        "owner_speaker_id,owner_canonical_speaker_id,"
        "owner_speaker_registry_id,owner_provisional "
        "FROM tasks WHERE id IN (?,?) ORDER BY id",
        (left_task_id, right_task_id),
    ).fetchall()
    if len(rows) != 2:
        return ProposalResult(
            ProposalDisposition.REFUSED,
            refusal=ProposalRefusal.UNKNOWN_TASK,
        )
    if not _eligible_for_proposal(rows, now=now):
        return ProposalResult(
            ProposalDisposition.REFUSED,
            refusal=ProposalRefusal.TASK_NOT_OPEN,
        )
    if not allow_unconfirmed_owner and not _same_confirmed_owner(rows[0], rows[1]):
        return ProposalResult(
            ProposalDisposition.REFUSED,
            refusal=ProposalRefusal.INCOMPATIBLE_OWNER,
        )
    existing = connection.execute(
        "SELECT id FROM task_duplicate_proposals "
        "WHERE left_task_id=? AND right_task_id=?",
        (left_task_id, right_task_id),
    ).fetchone()
    if existing is not None:
        return ProposalResult(
            ProposalDisposition.UNCHANGED, proposal_id=int(existing["id"])
        )
    for task_id in (left_task_id, right_task_id):
        open_count = int(connection.execute(
            "SELECT count(*) FROM task_duplicate_proposals "
            "WHERE state='proposed' AND (left_task_id=? OR right_task_id=?)",
            (task_id, task_id),
        ).fetchone()[0])
        if open_count >= MAX_OPEN_PROPOSALS_PER_TASK:
            return ProposalResult(
                ProposalDisposition.REFUSED,
                refusal=ProposalRefusal.TOO_MANY_OPEN_PROPOSALS,
            )
    cursor = connection.execute(
        "INSERT INTO task_duplicate_proposals("
        "left_task_id,right_task_id,left_task_version,right_task_version,"
        "basis,detector,state,created_at,updated_at) VALUES(?,?,?,?,?,?,'"
        "proposed',?,?)",
        (left_task_id, right_task_id, int(rows[0]["version"]),
         int(rows[1]["version"]), basis, detector, now, now),
    )
    proposal_id = int(cursor.lastrowid)
    connection.execute(
        "INSERT INTO task_duplicate_proposal_events("
        "proposal_id,kind,actor,occurred_at) VALUES(?, 'proposed', ?, ?)",
        (proposal_id, detector, now),
    )
    return ProposalResult(ProposalDisposition.RECORDED, proposal_id=proposal_id)


def get(connection: sqlite3.Connection, proposal_id: int) -> DuplicateProposal:
    """Return one private proposal or raise when it does not exist."""
    proposal_id = _identifier(proposal_id, "proposal id")
    row = connection.execute(
        "SELECT * FROM task_duplicate_proposals WHERE id=?", (proposal_id,)
    ).fetchone()
    if row is None:
        raise DuplicateProposalError("proposal does not exist")
    return _proposal(row)


def settle(
    connection: sqlite3.Connection,
    *,
    proposal_id: int,
    decision: Decision,
    actor: str,
    now: str,
) -> bool:
    """Record one reader answer.  Repeated or stale answers write nothing."""
    proposal_id = _identifier(proposal_id, "proposal id")
    actor = _bounded(actor, "actor", MAX_ACTOR)
    now = _bounded(now, "timestamp", 40)
    if decision not in {Decision.CONFIRMED, Decision.REJECTED}:
        raise DuplicateProposalError("proposal decision is invalid")
    cursor = connection.execute(
        "UPDATE task_duplicate_proposals SET state=?,updated_at=?,settled_at=? "
        "WHERE id=? AND state='proposed'",
        (decision.value, now, now, proposal_id),
    )
    if cursor.rowcount != 1:
        return False
    connection.execute(
        "INSERT INTO task_duplicate_proposal_events("
        "proposal_id,kind,actor,occurred_at) VALUES(?,?,?,?)",
        (proposal_id, decision.value, actor, now),
    )
    return True


def reopen(
    connection: sqlite3.Connection,
    *,
    proposal_id: int,
    actor: str,
    now: str,
) -> bool:
    """Explicitly reconsider one rejected pair; detectors cannot do this."""
    proposal_id = _identifier(proposal_id, "proposal id")
    actor = _bounded(actor, "actor", MAX_ACTOR)
    now = _bounded(now, "timestamp", 40)
    cursor = connection.execute(
        "UPDATE task_duplicate_proposals SET state='proposed',updated_at=?,"
        "settled_at=NULL WHERE id=? AND state='rejected'",
        (now, proposal_id),
    )
    if cursor.rowcount != 1:
        return False
    connection.execute(
        "INSERT INTO task_duplicate_proposal_events("
        "proposal_id,kind,actor,occurred_at) VALUES(?, 'reopened', ?, ?)",
        (proposal_id, actor, now),
    )
    return True


def reopen_confirmed(
    connection: sqlite3.Connection,
    *,
    proposal_id: int,
    actor: str,
    now: str,
) -> bool:
    """Reconsider a confirmed pair after its reader relation is withdrawn."""
    proposal_id = _identifier(proposal_id, "proposal id")
    actor = _bounded(actor, "actor", MAX_ACTOR)
    now = _bounded(now, "timestamp", 40)
    cursor = connection.execute(
        "UPDATE task_duplicate_proposals SET state='proposed',card_id=NULL,"
        "updated_at=?,settled_at=NULL WHERE id=? AND state='confirmed'",
        (now, proposal_id),
    )
    if cursor.rowcount != 1:
        return False
    connection.execute(
        "INSERT INTO task_duplicate_proposal_events("
        "proposal_id,kind,actor,occurred_at) VALUES(?, 'reopened', ?, ?)",
        (proposal_id, actor, now),
    )
    return True


def bind(connection: sqlite3.Connection, *, proposal_id: int, card_id: int,
         now: str) -> bool:
    """Attach an unanswered proposal to the one card that will show it."""
    proposal_id = _identifier(proposal_id, "proposal id")
    card_id = _identifier(card_id, "card id")
    now = _bounded(now, "timestamp", 40)
    cursor = connection.execute(
        "UPDATE task_duplicate_proposals SET card_id=?,updated_at=? "
        "WHERE id=? AND state='proposed' AND card_id IS NULL",
        (card_id, now, proposal_id),
    )
    return cursor.rowcount == 1


def release_for_card(connection: sqlite3.Connection, card_id: int, *, now: str) -> int:
    """Make unanswered proposals on a stale card eligible for a fresh card."""
    card_id = _identifier(card_id, "card id")
    now = _bounded(now, "timestamp", 40)
    cursor = connection.execute(
        "UPDATE task_duplicate_proposals SET card_id=NULL,updated_at=? "
        "WHERE card_id=? AND state='proposed'",
        (now, card_id),
    )
    return int(cursor.rowcount)


def next_open(connection: sqlite3.Connection) -> DuplicateProposal | None:
    """Return the oldest unanswered proposal, without claiming it."""
    row = connection.execute(
        "SELECT * FROM task_duplicate_proposals WHERE state='proposed' "
        "ORDER BY id LIMIT 1"
    ).fetchone()
    return None if row is None else _proposal(row)


def counts(connection: sqlite3.Connection) -> tuple[ProposalCounts, ...]:
    """Return detector quality counts without task or source content."""
    rows = connection.execute(
        "SELECT p.detector, "
        "SUM(e.kind='proposed') AS proposed, "
        "SUM(e.kind='confirmed') AS confirmed, "
        "SUM(e.kind='rejected') AS rejected, "
        "SUM(e.kind='reopened') AS reopened "
        "FROM task_duplicate_proposals AS p "
        "JOIN task_duplicate_proposal_events AS e ON e.proposal_id=p.id "
        "GROUP BY p.detector ORDER BY p.detector"
    ).fetchall()
    return tuple(ProposalCounts(
        detector=row["detector"],
        proposed=int(row["proposed"] or 0),
        confirmed=int(row["confirmed"] or 0),
        rejected=int(row["rejected"] or 0),
        reopened=int(row["reopened"] or 0),
    ) for row in rows)


def _same_confirmed_owner(left: sqlite3.Row, right: sqlite3.Row) -> bool:
    """Require one fully scoped, non-provisional person on both tasks."""
    fields = (
        "owner_ref_version", "owner_kind", "owner_speaker_id",
        "owner_canonical_speaker_id", "owner_speaker_registry_id",
        "owner_provisional",
    )
    if any(left[field] != right[field] for field in fields):
        return False
    return (
        int(left["owner_ref_version"]) == 1
        and left["owner_kind"] == "person"
        and bool(left["owner_speaker_id"])
        and bool(left["owner_canonical_speaker_id"])
        and bool(left["owner_speaker_registry_id"])
        and not bool(left["owner_provisional"])
    )


def reviewable_status_pair(rows: tuple[sqlite3.Row, ...] | list[sqlite3.Row]) -> bool:
    """Return whether a persisted proposal can still reach a reader.

    A proposal may compare two open tasks, or one open task with a task that
    was recently closed when the detector recorded the proposal.  The latter
    stays reviewable after the lookback expires: the reader is deciding a
    durable recorded comparison, not asking the detector to widen its window.
    """
    if len(rows) != 2:
        return False
    statuses = [str(row["status"]) for row in rows]
    return statuses.count("open") == 2 or (
        statuses.count("open") == 1
        and any(status in {"done", "dropped"} for status in statuses)
    )


def _eligible_for_proposal(rows: tuple[sqlite3.Row, ...] | list[sqlite3.Row], *,
                           now: str) -> bool:
    if not reviewable_status_pair(rows):
        return False
    if all(str(row["status"]) == "open" for row in rows):
        return True
    closed = next(row for row in rows if str(row["status"]) != "open")
    return _closed_within_lookback(closed["closed_at"], now)


def _closed_within_lookback(closed_at: object, now: str) -> bool:
    if not isinstance(closed_at, str):
        return False
    try:
        closed = datetime.fromisoformat(closed_at).astimezone(timezone.utc)
        observed = datetime.fromisoformat(now).astimezone(timezone.utc)
    except ValueError:
        return False
    return observed - timedelta(days=RECENTLY_CLOSED_DAYS) <= closed <= observed


def _identifier(value: int, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise DuplicateProposalError(f"{field} is invalid")
    return value


def _bounded(value: str, field: str, maximum: int) -> str:
    if not isinstance(value, str):
        raise DuplicateProposalError(f"{field} must be text")
    value = value.strip()
    if not 1 <= len(value) <= maximum:
        raise DuplicateProposalError(f"{field} has invalid length")
    if any(ord(char) < 32 or ord(char) == 127 for char in value):
        raise DuplicateProposalError(f"{field} contains control characters")
    return value


def _proposal(row: sqlite3.Row) -> DuplicateProposal:
    return DuplicateProposal(
        id=int(row["id"]),
        left_task_id=int(row["left_task_id"]),
        right_task_id=int(row["right_task_id"]),
        left_task_version=int(row["left_task_version"]),
        right_task_version=int(row["right_task_version"]),
        basis=row["basis"],
        detector=row["detector"],
        state=row["state"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        settled_at=row["settled_at"],
        card_id=(None if row["card_id"] is None else int(row["card_id"])),
    )
