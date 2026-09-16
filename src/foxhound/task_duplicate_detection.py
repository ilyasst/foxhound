"""Recall-first discovery of cross-source duplicate tasks.

Native intake invokes this scan after adding tasks, and an operator can run it
explicitly to reconcile an existing queue.  Candidate intake still creates a
task for each source item; this module later asks whether two task descriptions
with one confirmed owner may name one commitment.  The scan records a
*proposal*, never a relation or task change.
"""

from __future__ import annotations

import argparse
import json
import re
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable

from . import task_duplicate_proposals as proposals
from .candidate_inbox import CandidateInbox, InboxError


# Words that express a task shape but not its subject.  One remaining shared
# term is enough for recall: the reader, rather than this lexical filter,
# decides semantic sameness on the later card.
_STOP_WORDS = frozenset({
    "a", "an", "and", "as", "at", "by", "for", "from", "in", "is",
    "of", "on", "or", "the", "to", "with", "action", "draft", "follow",
    "make", "next", "prepare", "review", "send", "task", "this", "that",
    "will", "work", "write",
})


@dataclass(frozen=True)
class DuplicateCandidate:
    """Private detector input; it must not be included in command output."""

    task_id: int
    task_text: str
    task_version: int
    task_status: str
    task_closed_at: str | None
    source_kind: str
    source_created_at: str
    owner_ref_version: int
    owner_kind: str | None
    owner_speaker_id: str | None
    owner_canonical_speaker_id: str | None
    owner_speaker_registry_id: str | None
    owner_provisional: bool


@dataclass(frozen=True)
class DetectionRun:
    """Aggregate-only result safe for an operator command or metric."""

    pairs_considered: int = 0
    pairs_signalled: int = 0
    proposals_recorded: int = 0
    proposals_unchanged: int = 0
    proposals_refused: int = 0


def scan(connection: sqlite3.Connection, *, now: str,
         focus_task_ids: Iterable[int] | None = None) -> DetectionRun:
    """Scan current tasks and record reviewable, cross-source proposals.

    The scan is idempotent: the proposal ledger's unordered-pair identity
    returns an unchanged result for a proposal already awaiting or carrying a
    reader decision.  No automatic decision is made from the lexical signal.
    """
    now = _timestamp(now)
    candidates = tuple(_candidates(connection))
    focus = None if focus_task_ids is None else frozenset(focus_task_ids)
    considered = signalled = recorded = unchanged = refused = 0
    strongest: dict[int, tuple[float, int, DuplicateCandidate, DuplicateCandidate,
                               tuple[str, ...]]] = {}
    for index, left in enumerate(candidates):
        for right in candidates[index + 1:]:
            if focus is not None and left.task_id not in focus and right.task_id not in focus:
                continue
            if left.source_kind == right.source_kind:
                continue
            if not _eligible_status_pair(left, right, now=now):
                continue
            considered += 1
            shared = _shared_terms(left.task_text, right.task_text)
            shorter = min(len(_terms(left.task_text)), len(_terms(right.task_text)))
            coverage = 0 if shorter == 0 else len(shared) / shorter
            if len(shared) < 2 or coverage < 0.6:
                continue
            signalled += 1
            targets = (left, right) if focus is None else tuple(
                candidate for candidate in (left, right)
                if candidate.task_id in focus
            )
            for candidate in targets:
                score = (coverage, len(shared), left, right, shared)
                prior = strongest.get(candidate.task_id)
                if prior is None or score[:2] > prior[:2]:
                    strongest[candidate.task_id] = score
    pairs = {(item[2].task_id, item[3].task_id, item[4])
             for item in strongest.values()}
    for left_id, right_id, shared in pairs:
        left = next(candidate for candidate in candidates if candidate.task_id == left_id)
        right = next(candidate for candidate in candidates if candidate.task_id == right_id)
        result = proposals.propose(
                connection,
                task_id_a=left.task_id, task_id_b=right.task_id,
                basis=_basis(left, right, shared),
                detector="cross-source-overlap-review-v1", now=now,
                allow_unconfirmed_owner=True,
        )
        if result.disposition is proposals.ProposalDisposition.RECORDED:
            recorded += 1
        elif result.disposition is proposals.ProposalDisposition.UNCHANGED:
            unchanged += 1
        else:
            refused += 1
    return DetectionRun(considered, signalled, recorded, unchanged, refused)


def scan_database(database_path: str | Path, *, now: str | None = None) -> DetectionRun:
    """Run the explicit scan against one initialized private inbox database."""
    inbox = CandidateInbox(database_path)
    if not inbox.database_path.is_file() or inbox.database_path.is_symlink():
        raise InboxError("candidate inbox is not initialized")
    connection = sqlite3.connect(inbox.database_path, timeout=5)
    connection.row_factory = sqlite3.Row
    try:
        inbox._require_current_schema(connection)
        connection.execute("BEGIN IMMEDIATE")
        try:
            result = scan(
                connection,
                now=now or datetime.now(timezone.utc).isoformat(timespec="seconds"),
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        return result
    finally:
        connection.close()


def main(argv: list[str] | None = None) -> int:
    """Run the scanner without printing task, source, or detector content."""
    parser = argparse.ArgumentParser(
        description="Propose cross-source duplicate tasks for reader review."
    )
    parser.add_argument("--database", required=True, type=Path)
    arguments = parser.parse_args(argv)
    try:
        result = scan_database(arguments.database)
    except (InboxError, sqlite3.Error, ValueError):
        # The database path and exception can be operational data.  Keep the
        # command's failure shape useful without reflecting either one.
        print(json.dumps({"accepted": False}, separators=(",", ":")))
        return 2
    print(json.dumps({"accepted": True, **result.__dict__}, separators=(",", ":")))
    return 0


def _candidates(connection: sqlite3.Connection) -> Iterable[DuplicateCandidate]:
    # A task has one accepted binding.  The current candidate row carries the
    # source kind even after its revision has advanced, which is enough for
    # the cross-source gate; neither evidence nor source identifiers leave the
    # database through this scan.
    rows = connection.execute(
        "SELECT t.id,t.text,t.version,t.status,t.closed_at,c.source_kind,c.created_at,"
        "t.owner_ref_version,t.owner_kind,t.owner_speaker_id,"
        "t.owner_canonical_speaker_id,t.owner_speaker_registry_id,"
        "t.owner_provisional "
        "FROM tasks AS t "
        "JOIN task_candidate_bindings AS b ON b.task_id=t.id "
        "JOIN candidate_inbox AS c ON c.candidate_id=b.candidate_id "
        "WHERE (t.status='open' OR (t.status IN ('done','dropped') "
        "AND t.closed_at IS NOT NULL)) AND b.relation='accepted' "
        "ORDER BY t.id"
    ).fetchall()
    for row in rows:
        yield DuplicateCandidate(
            task_id=int(row["id"]),
            task_text=row["text"],
            task_version=int(row["version"]),
            task_status=str(row["status"]),
            task_closed_at=row["closed_at"],
            source_kind=row["source_kind"],
            source_created_at=row["created_at"],
            owner_ref_version=int(row["owner_ref_version"]),
            owner_kind=row["owner_kind"],
            owner_speaker_id=row["owner_speaker_id"],
            owner_canonical_speaker_id=row["owner_canonical_speaker_id"],
            owner_speaker_registry_id=row["owner_speaker_registry_id"],
            owner_provisional=bool(row["owner_provisional"]),
        )


def _same_confirmed_owner(left: DuplicateCandidate,
                          right: DuplicateCandidate) -> bool:
    fields = (
        "owner_ref_version", "owner_kind", "owner_speaker_id",
        "owner_canonical_speaker_id", "owner_speaker_registry_id",
        "owner_provisional",
    )
    if any(getattr(left, field) != getattr(right, field) for field in fields):
        return False
    return (
        left.owner_ref_version == 1
        and left.owner_kind == "person"
        and bool(left.owner_speaker_id)
        and bool(left.owner_canonical_speaker_id)
        and bool(left.owner_speaker_registry_id)
        and not left.owner_provisional
    )


def _eligible_status_pair(left: DuplicateCandidate, right: DuplicateCandidate,
                          *, now: str) -> bool:
    statuses = (left.task_status, right.task_status)
    if statuses == ("open", "open"):
        return True
    if statuses.count("open") != 1:
        return False
    closed_at = (
        left.task_closed_at if left.task_status != "open"
        else right.task_closed_at
    )
    if not isinstance(closed_at, str):
        return False
    try:
        closed = datetime.fromisoformat(closed_at).astimezone(timezone.utc)
        observed = datetime.fromisoformat(now).astimezone(timezone.utc)
    except ValueError:
        return False
    return (
        observed - timedelta(days=proposals.RECENTLY_CLOSED_DAYS)
        <= closed <= observed
    )


def _shared_terms(left: str, right: str) -> tuple[str, ...]:
    left_terms = _terms(left)
    right_terms = _terms(right)
    return tuple(sorted(left_terms & right_terms))


def _terms(value: str) -> set[str]:
    return {
        term for term in re.findall(r"[a-z0-9]{3,}", value.casefold())
        if term not in _STOP_WORDS
    }


def _basis(left: DuplicateCandidate, right: DuplicateCandidate,
           shared: tuple[str, ...]) -> str:
    # Private detector evidence: the later card gives the reader the full
    # source extracts.  Bound it here so a long task name cannot make storing
    # a proposal fail after an otherwise successful scan.
    terms = ", ".join(shared[:12])
    text = (
        "same confirmed owner; distinct source kinds "
        f"{left.source_kind} and {right.source_kind}; shared task terms: {terms}"
    )
    return text[:proposals.MAX_BASIS]


def _timestamp(value: str) -> str:
    if not isinstance(value, str) or not 1 <= len(value.strip()) <= 40:
        raise ValueError("timestamp is invalid")
    return value.strip()


if __name__ == "__main__":  # pragma: no cover - module entry point
    raise SystemExit(main())
