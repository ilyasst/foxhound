"""Recall-first discovery of duplicate tasks.

Native intake invokes this scan after adding tasks, and an operator can run it
explicitly to reconcile an existing queue.  Candidate intake still creates a
task for each source item; this module later asks whether two task descriptions
may name one commitment.  The scan records a *proposal*, never a relation or
task change.

Duplication is mostly not cross-source.  One mail thread re-read as it grows
mints a fresh task each time, and one ledger can hold three copies of one
commitment, so an earlier same-source-kind gate hid the common cases.  Recall
comes from three independent channels, and precision from two vetoes that a
lexical score cannot express:

* a forge review names the issue it closes, which is an exact join;
* one source record re-read at a later time is a re-carding of one commitment;
* weighted term overlap, with rare terms counting for more than common ones.

The vetoes: two tasks naming *different* identifiers of one class are different
commitments however alike the prose, and two tasks lifted from a single reading
of one document are its separate action items, never copies of each other.
"""

from __future__ import annotations

import argparse
import json
import math
import re
import sqlite3
import unicodedata
from dataclasses import dataclass, field
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


DETECTOR = "duplicate-review-v2"

# Source kinds minted from a forge.  Two of these are paired by the exact
# `Closes #N` join below, so comparing their prose adds noise and no recall:
# forge titles share a house grammar ("Add ...", "Expose ...", "Review: ...")
# that scores high between unrelated work.  A forge task is still compared with
# mail, meeting and ledger tasks, because one commitment can be tracked both as
# an issue and as something said in a meeting.
FORGE_KINDS = frozenset({"issue", "review_request"})

# A record id shared by more tasks than this is a FEED (a repository, a ledger),
# not one source item.
MAX_RECORD_FANOUT = 12

# Items lifted from a single reading of one document land within seconds of
# each other; a thread re-read later re-cards hours or days apart.
SAME_READING_SECONDS = 120

# Share of the lighter task's weighted terms the heavier one must repeat.
MIN_WEIGHTED_COVERAGE = 0.45
MIN_SHARED_TERMS = 2

# Identifier classes whose disagreement is decisive.  Each pattern must expose
# the bare identifier as group 1 so that "AAA 111" and "AAA111" compare equal.
_IDENTIFIER_PATTERNS = (
    ("order", re.compile(r"\b(\d{6}-\d{3})\b")),
    ("article", re.compile(r"\b([A-Z]{3,}_\d{4,})\b")),
    ("course", re.compile(r"\b[A-Z]{2,4}[\s-]?(\d{3})\b")),
    ("grant", re.compile(r"\b([A-Z]{2}\d{5})\b")),
)

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
    source_record_id: str
    owner_ref_version: int
    owner_kind: str | None
    owner_speaker_id: str | None
    owner_canonical_speaker_id: str | None
    owner_speaker_registry_id: str | None
    owner_provisional: bool
    terms: frozenset[str] = frozenset()
    identifiers: tuple[tuple[str, frozenset[str]], ...] = ()


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
    """Scan current tasks and record reviewable duplicate proposals.

    The scan is idempotent: the proposal ledger's unordered-pair identity
    returns an unchanged result for a proposal already awaiting or carrying a
    reader decision.  No automatic decision is made from any channel here; the
    reader settles every pair on a later card.
    """
    now = _timestamp(now)
    candidates = tuple(_candidates(connection))
    by_id = {candidate.task_id: candidate for candidate in candidates}
    focus = None if focus_task_ids is None else frozenset(focus_task_ids)
    weights = _weights(candidates)

    considered = signalled = 0
    pairs: dict[tuple[int, int], str] = {}

    def offer(left: DuplicateCandidate, right: DuplicateCandidate,
              reason: str) -> None:
        key = (left.task_id, right.task_id)
        if key[0] > key[1]:
            key = (key[1], key[0])
        pairs.setdefault(key, reason)

    for index, left in enumerate(candidates):
        for right in candidates[index + 1:]:
            if focus is not None and left.task_id not in focus \
                    and right.task_id not in focus:
                continue
            if not _comparable(left, right, now=now):
                continue
            considered += 1
            shared = left.terms & right.terms
            coverage = _weighted_coverage(left, right, weights)
            if len(shared) < MIN_SHARED_TERMS or coverage < MIN_WEIGHTED_COVERAGE:
                continue
            signalled += 1
            offer(left, right, _overlap_basis(left, right, shared))

    for left, right in _reread_pairs(candidates, focus=focus, now=now):
        signalled += 1
        offer(left, right, _reread_basis(left, right))

    recorded = unchanged = refused = 0
    for (left_id, right_id), basis in sorted(pairs.items()):
        result = proposals.propose(
            connection,
            task_id_a=left_id, task_id_b=right_id,
            basis=basis, detector=DETECTOR, now=now,
            allow_unconfirmed_owner=True,
        )
        if result.disposition is proposals.ProposalDisposition.RECORDED:
            recorded += 1
        elif result.disposition is proposals.ProposalDisposition.UNCHANGED:
            unchanged += 1
        else:
            refused += 1
    return DetectionRun(considered, signalled, recorded, unchanged, refused)


def _comparable(left: DuplicateCandidate, right: DuplicateCandidate,
                *, now: str) -> bool:
    """Gates that apply to every channel."""
    if not _eligible_status_pair(left, right, now=now):
        return False
    if _identifier_conflict(left, right):
        return False
    if _one_reading(left, right):
        return False
    if left.source_kind in FORGE_KINDS and right.source_kind in FORGE_KINDS:
        # ...unless one item was carded twice, which shows up as identical text
        # under one kind.  An issue and the review that closes it are one work
        # item, but a merge is not the question the reader wants put to them:
        # the forge already records that pairing, and asking would bury the
        # duplicates that nothing else records.
        return (left.source_kind == right.source_kind
                and _fold(left.task_text) == _fold(right.task_text))
    return True


def _identifier_conflict(left: DuplicateCandidate,
                         right: DuplicateCandidate) -> bool:
    """True when both name an identifier of one class and share none.

    Two purchase orders, two course codes, two articles: different
    commitments, whatever the surrounding words claim.
    """
    right_by_class = dict(right.identifiers)
    for name, values in left.identifiers:
        other = right_by_class.get(name)
        if other and not (values & other):
            return True
    return False


def _one_reading(left: DuplicateCandidate, right: DuplicateCandidate) -> bool:
    """True when both were lifted from a single reading of one document.

    Sharing a source record means one of two opposite things.  A meeting
    protocol is read once and yields several distinct action items, seconds
    apart.  A mail thread is re-read as it grows and re-cards one commitment,
    hours or days apart.  The gap, not the record, tells them apart.
    """
    if not _own_record(left) or left.source_record_id != right.source_record_id:
        return False
    return _within(left.source_created_at, right.source_created_at,
                   SAME_READING_SECONDS)


def _reread_pairs(candidates: tuple[DuplicateCandidate, ...], *,
                  focus: frozenset[int] | None, now: str):
    """One source record carded again later: the strongest duplicate signal."""
    grouped: dict[str, list[DuplicateCandidate]] = {}
    for candidate in candidates:
        if _own_record(candidate):
            grouped.setdefault(candidate.source_record_id, []).append(candidate)
    for group in grouped.values():
        if len(group) > MAX_RECORD_FANOUT:
            continue
        group = sorted(group, key=lambda item: item.task_id)
        for index, left in enumerate(group):
            for right in group[index + 1:]:
                if focus is not None and left.task_id not in focus \
                        and right.task_id not in focus:
                    continue
                if not _comparable(left, right, now=now):
                    continue
                yield left, right


def _own_record(candidate: DuplicateCandidate) -> bool:
    """False for feed identifiers, which name a stream rather than one item."""
    record = candidate.source_record_id
    return bool(record) and candidate.source_kind not in FORGE_KINDS


def _within(left: str, right: str, seconds: int) -> bool:
    try:
        first = datetime.fromisoformat(str(left).replace("Z", "+00:00"))
        second = datetime.fromisoformat(str(right).replace("Z", "+00:00"))
    except ValueError:
        return left == right
    return abs((first - second).total_seconds()) <= seconds


def _weights(candidates: tuple[DuplicateCandidate, ...]) -> dict[str, float]:
    """Inverse document frequency: a rare term is evidence, a common one is not."""
    total = len(candidates)
    frequency: dict[str, int] = {}
    for candidate in candidates:
        for term in candidate.terms:
            frequency[term] = frequency.get(term, 0) + 1
    # Smoothed: an unsmoothed log(total/count) is exactly zero for a term
    # present in every task, which erases the only shared evidence when the
    # eligible set is small.  Smoothing keeps such a term near-zero without
    # letting the whole comparison collapse.
    return {term: math.log((total + 1) / count)
            for term, count in frequency.items()}


def _weighted_coverage(left: DuplicateCandidate, right: DuplicateCandidate,
                       weights: dict[str, float]) -> float:
    shared = left.terms & right.terms
    if not shared:
        return 0.0
    mass = sum(weights.get(term, 0.0) for term in shared)
    floor = min(sum(weights.get(term, 0.0) for term in left.terms),
                sum(weights.get(term, 0.0) for term in right.terms))
    return 0.0 if floor <= 0 else mass / floor


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
        "c.source_record_id,"
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
        text = row["text"]
        yield DuplicateCandidate(
            task_id=int(row["id"]),
            task_text=text,
            task_version=int(row["version"]),
            task_status=str(row["status"]),
            task_closed_at=row["closed_at"],
            source_kind=row["source_kind"],
            source_created_at=row["created_at"],
            source_record_id=str(row["source_record_id"] or ""),
            owner_ref_version=int(row["owner_ref_version"]),
            owner_kind=row["owner_kind"],
            owner_speaker_id=row["owner_speaker_id"],
            owner_canonical_speaker_id=row["owner_canonical_speaker_id"],
            owner_speaker_registry_id=row["owner_speaker_registry_id"],
            owner_provisional=bool(row["owner_provisional"]),
            terms=frozenset(_terms(text)),
            identifiers=_identifiers(text),
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
        term for term in re.findall(r"[a-z0-9]{3,}", _fold(value))
        if term not in _STOP_WORDS
    }


def _fold(value: str) -> str:
    """Case- and accent-insensitive form, so 'resume' matches 'résumé'."""
    folded = unicodedata.normalize("NFKD", value.casefold())
    return "".join(ch for ch in folded if not unicodedata.combining(ch))


def _identifiers(value: str) -> tuple[tuple[str, frozenset[str]], ...]:
    found = []
    for name, pattern in _IDENTIFIER_PATTERNS:
        hits = {match.group(1).casefold() for match in pattern.finditer(value)}
        if hits:
            found.append((name, frozenset(hits)))
    return tuple(found)


def _overlap_basis(left: DuplicateCandidate, right: DuplicateCandidate,
                   shared: frozenset[str]) -> str:
    # Private detector evidence: the later card gives the reader the full
    # source extracts.  Bound it here so a long task name cannot make storing
    # a proposal fail after an otherwise successful scan.
    terms = ", ".join(sorted(shared)[:12])
    return _bounded(
        f"shared task terms across {left.source_kind} and {right.source_kind}: "
        f"{terms}"
    )


def _reread_basis(left: DuplicateCandidate, right: DuplicateCandidate) -> str:
    return _bounded(
        f"one {left.source_kind} source record carded again at a later reading"
    )


def _bounded(text: str) -> str:
    return text[:proposals.MAX_BASIS]


def _timestamp(value: str) -> str:
    if not isinstance(value, str) or not 1 <= len(value.strip()) <= 40:
        raise ValueError("timestamp is invalid")
    return value.strip()


if __name__ == "__main__":  # pragma: no cover - module entry point
    raise SystemExit(main())
