"""Research-before-planning gate for selected source kinds (ADR 0063).

A deployment may name source kinds whose plan phase waits for a completed
Researcher receipt.  This module owns the two deterministic halves of that
policy, both evaluated inside a transaction the execution service already
holds:

* **request** -- a selected, claimable plan workflow without research for its
  exact task version gets one durable research job.  Nothing here runs a
  model, opens a network connection, or reads task content into a log.
* **gate** -- the plan stays unclaimable while that job is in flight, is
  released by a matching completed receipt, and is released *without*
  research when the research parked, was cancelled, could not be requested,
  or has waited longer than the bounded deadline.  A missing or broken
  Researcher therefore delays planning by at most the deadline; it never
  strands a task.

The plan/execute agent learns which of those happened from
`research_context`, which the worker publishes in its work context.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import StrEnum
from pathlib import Path

from .source_policy import source_kind_grants
from .task_archive import TaskArchiveError, ensure_task_directory
from .task_owner import UNRESOLVED_DISPLAY, canonical_owner_display, normalized_owner, reader_owned
from .task_research import (
    version_document,
    INPUT_SCHEMA,
    ResearchError,
    enqueue_in_transaction,
    version_projection,
)


#: Long enough for the one-shot runner's three attempts at its default lease
#: plus timer slack; short enough that a host whose Researcher is down still
#: plans the same day.
DEFAULT_RESEARCH_WAIT_SECONDS = 3 * 60 * 60
MIN_RESEARCH_WAIT_SECONDS = 10 * 60
MAX_RESEARCH_WAIT_SECONDS = 24 * 60 * 60
#: Upper bound on research requests one claim pass may create.
MAX_REQUESTS_PER_PASS = 20
_ACTIVE_STATES = frozenset({"queued", "running", "publishing"})


class ResearchGateDecision(StrEnum):
    #: A completed receipt exists for this exact task version.
    READY = "ready"
    #: Research is queued or running and the deadline has not passed.
    PENDING = "pending"
    #: Planning proceeds without research; the reason says why.
    BYPASSED = "bypassed"


@dataclass(frozen=True)
class ResearchGateResult:
    decision: ResearchGateDecision
    #: Content-free token: requested, deferred, in_flight, receipt, parked,
    #: canceled, timed_out, request_refused.
    reason: str

    @property
    def claimable(self) -> bool:
        return self.decision is not ResearchGateDecision.PENDING


@dataclass(frozen=True)
class ResearchGatePolicy:
    """One machine's validated research-before-planning declaration."""

    source_kinds: frozenset[str]
    wait_seconds: int | None
    task_work_root: Path | None
    task_kb_root: Path | None

    @classmethod
    def build(
        cls,
        source_kinds: object,
        *,
        wait_seconds: object = None,
        task_work_root: Path | None = None,
        task_kb_root: Path | None = None,
    ) -> "ResearchGatePolicy":
        kinds = source_kind_grants(
            source_kinds, label="research-before-planning declarations"
        )
        if wait_seconds is not None and (
            isinstance(wait_seconds, bool)
            or not isinstance(wait_seconds, int)
            or not MIN_RESEARCH_WAIT_SECONDS <= wait_seconds
            <= MAX_RESEARCH_WAIT_SECONDS
        ):
            raise ValueError("research wait is invalid")
        if kinds and (task_work_root is None or task_kb_root is None):
            # The research job is bound to the task folder the plan will
            # use; without both archive roots there is no such folder.
            raise ValueError("research before planning requires task archive roots")
        for root in (task_work_root, task_kb_root):
            if root is not None and (
                not isinstance(root, Path) or not root.is_absolute()
            ):
                raise ValueError("task archive root is invalid")
        return cls(kinds, wait_seconds, task_work_root, task_kb_root)

    @property
    def enabled(self) -> bool:
        return bool(self.source_kinds)

    def selects(self, origin_kind: object) -> bool:
        return isinstance(origin_kind, str) and origin_kind in self.source_kinds


def research_snapshot(
    connection: sqlite3.Connection, task_id: int,
) -> dict[str, object] | None:
    """Build the canonical research input for the task's current version.

    Only ledger facts are used, so the same version always yields the same
    digest and a retried request converges on the existing job.
    """
    task = connection.execute(
        "SELECT id,status,text,owner,due,version,action,object,confidence,"
        "working_group FROM tasks WHERE id=?",
        (task_id,),
    ).fetchone()
    if task is None or task["status"] != "open":
        return None
    origin = connection.execute(
        "SELECT i.source_system,i.source_kind,i.source_record_id,i.source_item_id "
        "FROM task_candidate_bindings AS b "
        "JOIN candidate_inbox AS i ON i.candidate_id=b.candidate_id "
        "WHERE b.task_id=? AND b.relation='accepted'",
        (task_id,),
    ).fetchone()
    return {
        "schema_version": INPUT_SCHEMA,
        "task_id": int(task["id"]),
        "task_version": int(task["version"]),
        "text": task["text"],
        "structured": {
            "action": task["action"],
            "object": task["object"],
            "confidence": task["confidence"],
        },
        "due": task["due"],
        "owner": None if task["owner"] is None else {"name": task["owner"]},
        "participants": [],
        "working_group": (
            None if task["working_group"] is None
            else {"name": task["working_group"]}
        ),
        "external_identifiers": [],
        "origin": None if origin is None else {
            "system": str(origin["source_system"]),
            "kind": str(origin["source_kind"]),
            "record_id": str(origin["source_record_id"]),
            "item_id": str(origin["source_item_id"]),
        },
        "structured_schema_revisions": {"task_research_gate": 1},
    }


class RequestOutcome(StrEnum):
    REQUESTED = "requested"
    #: Transient: another version's research is mid-publication.
    DEFERRED = "deferred"
    #: Permanent for this input: the store or the archive refused it.
    REFUSED = "refused"


def request_research(
    connection: sqlite3.Connection,
    policy: ResearchGatePolicy,
    task_id: int,
    task_version: int,
    now: datetime,
) -> RequestOutcome:
    """Durably queue research for one task version.

    Idempotent: an existing job for the same canonical input is returned
    rather than duplicated, and a job for an older version is cancelled by
    the research store's own one-active-job rule.  A refusal rolls back to
    a savepoint, so it never leaves a half-written request in the caller's
    transaction.
    """
    if policy.task_work_root is None or policy.task_kb_root is None:
        return RequestOutcome.REFUSED
    snapshot = research_snapshot(connection, task_id)
    if snapshot is None or snapshot["task_version"] != task_version:
        return RequestOutcome.REFUSED
    publishing = connection.execute(
        "SELECT 1 FROM task_research_jobs WHERE task_id=? AND state='publishing'",
        (task_id,),
    ).fetchone()
    if publishing is not None:
        return RequestOutcome.DEFERRED
    connection.execute("SAVEPOINT research_gate_request")
    try:
        folder = ensure_task_directory(
            working_root=policy.task_work_root,
            kb_root=policy.task_kb_root,
            task_id=task_id,
            task_text=str(snapshot["text"]),
        )
        enqueue_in_transaction(
            connection,
            snapshot,
            task_work_root=policy.task_work_root,
            task_folder=folder,
            now=_research_timestamp(now),
        )
    except (ResearchError, TaskArchiveError, OSError, ValueError, sqlite3.IntegrityError):
        connection.execute("ROLLBACK TO research_gate_request")
        connection.execute("RELEASE research_gate_request")
        return RequestOutcome.REFUSED
    connection.execute("RELEASE research_gate_request")
    return RequestOutcome.REQUESTED


def evaluate_research_gate(
    connection: sqlite3.Connection,
    policy: ResearchGatePolicy,
    task_id: int,
    task_version: int,
    now: datetime,
) -> ResearchGateResult:
    """Decide whether a selected plan may be claimed now.

    Requests research when none exists for this version, so every path that
    makes a selected plan claimable -- a reader's Start, a planning grant, a
    parked retry -- converges here without each having to know about it.
    """
    rows = connection.execute(
        "SELECT j.state,j.requested_at,"
        "EXISTS(SELECT 1 FROM task_research_receipts AS r "
        "WHERE r.job_id=j.job_id) AS has_receipt "
        "FROM task_research_jobs AS j WHERE j.task_id=? AND j.task_version=? "
        "ORDER BY j.generation DESC",
        (task_id, task_version),
    ).fetchall()
    if any(row["state"] == "completed" and row["has_receipt"] for row in rows):
        return ResearchGateResult(ResearchGateDecision.READY, "receipt")
    if not rows:
        outcome = request_research(connection, policy, task_id, task_version, now)
        if outcome is RequestOutcome.REQUESTED:
            return ResearchGateResult(ResearchGateDecision.PENDING, "requested")
        if outcome is RequestOutcome.DEFERRED:
            return ResearchGateResult(ResearchGateDecision.PENDING, "deferred")
        # A snapshot the store refuses (oversized text, an unusable task
        # folder) will be refused again on every pass.  Waiting for it would
        # be a silent stall, which is the one outcome this gate must not have.
        return ResearchGateResult(
            ResearchGateDecision.BYPASSED, "request_refused"
        )
    latest = rows[0]["state"]
    if latest in _ACTIVE_STATES:
        if policy.wait_seconds is not None:
            first_requested = min(_parse(row["requested_at"]) for row in rows)
            if now - first_requested >= timedelta(seconds=policy.wait_seconds):
                return ResearchGateResult(
                    ResearchGateDecision.BYPASSED, "timed_out"
                )
        return ResearchGateResult(ResearchGateDecision.PENDING, "in_flight")
    if latest == "parked":
        return ResearchGateResult(ResearchGateDecision.BYPASSED, "parked")
    return ResearchGateResult(ResearchGateDecision.BYPASSED, "canceled")


def request_pending_research(
    connection: sqlite3.Connection,
    policy: ResearchGatePolicy,
    now: datetime,
    *,
    limit: int = MAX_REQUESTS_PER_PASS,
) -> int:
    """Queue research for selected claimable plans that have none yet.

    Run on every claim pass before the slot check, so research proceeds
    while execution slots are busy rather than only once one frees.
    """
    if not policy.enabled:
        return 0
    kinds = sorted(policy.source_kinds)
    marks = ",".join("?" for _ in kinds)
    rows = connection.execute(
        "SELECT w.task_id,w.task_version FROM task_execution_workflows AS w "
        "JOIN tasks AS t ON t.id=w.task_id "
        "JOIN task_candidate_bindings AS b "
        "ON b.task_id=t.id AND b.relation='accepted' "
        "JOIN candidate_inbox AS o ON o.candidate_id=b.candidate_id "
        "WHERE w.phase='plan' AND w.status IN ('queued','parked') "
        "AND t.status='open' AND t.version=w.task_version "
        f"AND o.source_kind IN ({marks}) "
        "AND NOT EXISTS(SELECT 1 FROM task_research_jobs AS j "
        "WHERE j.task_id=w.task_id AND j.task_version=w.task_version) "
        "ORDER BY w.task_id LIMIT ?",
        (*kinds, limit),
    ).fetchall()
    requested = 0
    for row in rows:
        if request_research(
            connection, policy, int(row["task_id"]), int(row["task_version"]),
            now,
        ) is RequestOutcome.REQUESTED:
            requested += 1
    return requested


def research_context(
    connection: sqlite3.Connection, task_id: int, task_version: int,
) -> dict[str, object]:
    """What a plan/execute agent is told about pre-planning research.

    `available` carries the evidence-only projection (at most 16 KiB) of the
    newest completed receipt for this exact task version.  Otherwise the
    agent is told research is unavailable and why, so a plan made without it
    says so instead of reading as though nothing was found.
    """
    projection = version_projection(connection, task_id, task_version)
    if projection is not None:
        return {"status": "available", "reason": None, "projection": projection}
    row = connection.execute(
        "SELECT state FROM task_research_jobs WHERE task_id=? AND task_version=? "
        "ORDER BY generation DESC LIMIT 1",
        (task_id, task_version),
    ).fetchone()
    if row is None:
        reason = "not_requested"
    elif row[0] in _ACTIVE_STATES:
        reason = "not_finished"
    elif row[0] == "completed":
        # A receipt whose files no longer verify is not evidence.
        reason = "unverifiable"
    else:
        reason = str(row[0])
    return {"status": "unavailable", "reason": reason, "projection": None}


def _research_timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


@dataclass(frozen=True)
class OwnershipProposal:
    """The Researcher's cited disagreement with a task's recorded owner."""

    receipt_job_id: str
    proposed_owner: str
    proposed_kind: str  # "reader" | "other"
    reasoning: str | None
    source_title: str | None


def _owner_claim(document: dict[str, object]) -> tuple[str, str | None, list[str]] | None:
    report = document.get("report")
    if not isinstance(report, dict):
        return None
    for claim in report.get("stakeholders") or ():
        if not isinstance(claim, dict):
            continue
        text = claim.get("text")
        if not isinstance(text, str) or not text.startswith("Owner: "):
            continue
        verdict, _, reasoning = text[len("Owner: "):].partition(" — ")
        refs = [str(r) for r in claim.get("source_refs") or () if isinstance(r, str)]
        return verdict.strip(), (reasoning.strip() or None), refs
    return None


def ownership_disagreement(
    connection: sqlite3.Connection,
    task_row: object,
    task_id: int,
    task_version: int,
    reader_aliases: frozenset[str],
) -> OwnershipProposal | None:
    """A proposal when the published research names a different owner.

    Only a verdict that cites at least one source counts; ``undetermined``
    never does. A task whose owner is unresolved belongs to the reader, so
    a cited named owner is a proposal too. A pinned owner is never reopened.
    """
    if not reader_aliases:
        return None
    try:
        if int(task_row["owner_pinned"] or 0):
            return None
    except (KeyError, IndexError, TypeError, ValueError):
        pass
    found = version_document(connection, task_id, task_version)
    if found is None:
        return None
    job_id, document = found
    claim = _owner_claim(document)
    if claim is None:
        return None
    verdict, reasoning, refs = claim
    if not refs:
        return None
    current_display = canonical_owner_display(
        task_row["owner"], task_row["owner_kind"]
    )
    current_is_reader = (
        reader_owned(task_row, reader_aliases)
        or current_display in (None, UNRESOLVED_DISPLAY)
    )
    if verdict == "reader":
        if current_is_reader:
            return None
        proposed, kind = "reader", "reader"
    elif verdict.startswith("other:"):
        name = verdict[len("other:"):].strip()
        if not name or normalized_owner(name) in reader_aliases:
            return None
        if not current_is_reader and current_display and (
            normalized_owner(current_display) == normalized_owner(name)
        ):
            return None
        proposed, kind = name[:200], "other"
    else:
        return None
    title = None
    sources = document.get("sources")
    if isinstance(sources, list):
        for source in sources:
            if isinstance(source, dict) and source.get("source_id") == refs[0]:
                title = str(source.get("title") or "")[:500] or None
                break
    return OwnershipProposal(
        receipt_job_id=job_id, proposed_owner=proposed, proposed_kind=kind,
        reasoning=(reasoning or None) and reasoning[:2000], source_title=title,
    )


def record_ownership_review(
    connection: sqlite3.Connection, task_id: int, task_version: int,
    proposal: OwnershipProposal, now: str,
) -> None:
    connection.execute(
        "INSERT OR IGNORE INTO ownership_reviews(task_id,task_version,"
        "receipt_job_id,proposed_owner,proposed_kind,reasoning,source_title,"
        "status,created_at) VALUES(?,?,?,?,?,?,?,'pending',?)",
        (task_id, task_version, proposal.receipt_job_id, proposal.proposed_owner,
         proposal.proposed_kind, proposal.reasoning, proposal.source_title, now),
    )


def ownership_review_status(
    connection: sqlite3.Connection, task_id: int, task_version: int,
) -> str | None:
    row = connection.execute(
        "SELECT status FROM ownership_reviews WHERE task_id=? AND task_version=?",
        (task_id, task_version),
    ).fetchone()
    return None if row is None else str(row[0])


def record_ownership_decision(
    connection: sqlite3.Connection, task_id: int, task_version: int,
    decision: str, decided_owner: str | None, now: str,
) -> bool:
    """Close a pending proposal; False when none is pending."""
    if decision not in {"confirmed", "kept", "reassigned"}:
        raise ValueError("ownership decision is invalid")
    cursor = connection.execute(
        "UPDATE ownership_reviews SET status=?,decided_owner=?,decided_at=? "
        "WHERE task_id=? AND task_version=? AND status='pending'",
        (decision, decided_owner, now, task_id, task_version),
    )
    return cursor.rowcount == 1

