"""Read-only diagnostic: why each proposed duplicate is not being carded.

This is a pure explanation tool. It does not change any state, settle,
supersede, or retire anything. An operator can ask "why is this proposal
not in front of a reader?" and get an answer without writing SQL or
reading the selection query.

Every condition is reported for each proposal. A proposal can fail
multiple conditions at once, so the result lists all that apply.
Conditions are classified as permanent or transient so the operator
knows whether the situation will resolve on its own or requires action.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Sequence

from .candidate_inbox import CandidateInbox, InboxError
from .task_bootstrap import TaskBootstrapConfigError, _private_database
from .task_ledger import TaskLedgerError

#: Workflow statuses that mean execution is finished with a task.
#: Mirrors task_cards.FINISHED_WORKFLOW_STATUSES.
_FINISHED_WORKFLOW_STATUSES = ("completed", "cancelled")

#: Workflow statuses that do not block a duplicate question.
#: Mirrors task_cards.DORMANT_WORKFLOW_STATUSES.
_DORMANT_WORKFLOW_STATUSES = ("snoozed",)

_UNHELD_WORKFLOW_STATUSES = (
    _FINISHED_WORKFLOW_STATUSES + _DORMANT_WORKFLOW_STATUSES
)


@dataclass(frozen=True)
class DisqualificationReason:
    """One condition that prevents a proposal from being carded."""

    code: str
    permanent: bool
    description: str


@dataclass(frozen=True)
class ProposalDiagnostic:
    """Content-free diagnostic for one proposed duplicate."""

    proposal_id: int
    left_task_id: int
    right_task_id: int
    left_task_version: int
    right_task_version: int
    left_task_status: str
    right_task_status: str
    detector: str
    state: str
    card_id: int | None
    reasons: tuple[DisqualificationReason, ...]


@dataclass(frozen=True)
class QueueDiagnostic:
    """Aggregate state of the duplicate proposal queue."""

    total_proposed: int
    #: Proposals bound to a card (active or pending delivery).
    carded: int
    #: Proposals not yet bound to any card.
    uncarded: int
    #: Uncarded proposals that are permanently unaskable.
    permanently_blocked: int
    #: Uncarded proposals blocked only by transient conditions.
    transiently_blocked: int
    proposals: tuple[ProposalDiagnostic, ...]


# ---------------------------------------------------------------------------
# Condition checks
# ---------------------------------------------------------------------------

#: A proposal whose state is not 'proposed' has already been settled or
#: superseded. This is permanent: it will never be carded in this state.
_REASON_SETTLED = DisqualificationReason(
    code="settled",
    permanent=True,
    description="Proposal is already settled (confirmed/rejected/superseded)",
)

#: A proposal already bound to a card cannot be carded again. If that card
#: is still active the proposal is being delivered through it; if the card
#: was cancelled, `release_for_card` should clear it. This is transient
#: because the card can be cancelled or settled.
_REASON_CARD_BOUND = DisqualificationReason(
    code="card_bound",
    permanent=False,
    description="Proposal is bound to another card",
)

#: Workflow hold: an unfinished, non-dormant workflow on either task blocks
#: the question because confirming a duplicate closes a task. Transient:
#: the workflow finishes and the hold is released.
_REASON_LEFT_WORKFLOW_HOLD = DisqualificationReason(
    code="left_workflow_hold",
    permanent=False,
    description="Active workflow holds the left task",
)
_REASON_RIGHT_WORKFLOW_HOLD = DisqualificationReason(
    code="right_workflow_hold",
    permanent=False,
    description="Active workflow holds the right task",
)

#: Version mismatch: the recorded task version no longer matches the
#: current task version. Permanent by design — the settle-only trigger
#: refuses to change recorded versions, so this proposal can never match
#: the selection predicate again.
_REASON_LEFT_VERSION_STALE = DisqualificationReason(
    code="left_version_stale",
    permanent=True,
    description="Left task version has advanced past the recorded version",
)
_REASON_RIGHT_VERSION_STALE = DisqualificationReason(
    code="right_version_stale",
    permanent=True,
    description="Right task version has advanced past the recorded version",
)

#: Status condition: the selection requires at least one open task, and
#: the other to be open/done/dropped. If both are closed (neither open),
#: or if a task is in an unexpected status, the proposal is unaskable.
#: Both-closed is permanent; a task that went from open to closed is
#: permanent for this proposal.
_REASON_BOTH_TASKS_CLOSED = DisqualificationReason(
    code="both_tasks_closed",
    permanent=True,
    description="Both tasks are closed — no open task remains to consolidate into",
)
_REASON_TASK_STATUS_INVALID = DisqualificationReason(
    code="task_status_invalid",
    permanent=True,
    description="Task status pair does not match reviewable pattern",
)

#: Active card conflict: the chosen task already has an active card
#: (pending/delivering/delivered/snoozed) that is not PENDING, or its
#: pending card already asks a different comparison.
_REASON_CARD_CONFLICT = DisqualificationReason(
    code="card_conflict",
    permanent=False,
    description="Task already has an active card that cannot carry this proposal",
)


# ---------------------------------------------------------------------------
# SQL fragments — mirrors of task_cards selection predicates
# ---------------------------------------------------------------------------

def _unheld_workflow_sql() -> str:
    return ",".join(f"'{s}'" for s in _UNHELD_WORKFLOW_STATUSES)


def _workflow_hold_predicate(task_id_expr: str) -> str:
    """SQL: an active, non-dormant workflow holds this task.
    Mirrors _duplicate_execution_holds from task_cards."""
    return (
        "EXISTS(SELECT 1 FROM task_execution_workflows AS w "
        f" WHERE w.task_id={task_id_expr} "
        f" AND w.status NOT IN ({_unheld_workflow_sql()}))"
    )


def _reviewable_status_check(left_status: str, right_status: str) -> bool:
    """Mirrors the status condition in _ask_duplicate_proposals.
    Returns True if the status pair allows the question to be asked."""
    return (
        (left_status == "open" and right_status in ("open", "done", "dropped"))
        or (right_status == "open" and left_status in ("done", "dropped"))
    )


# ---------------------------------------------------------------------------
# Core diagnostic logic
# ---------------------------------------------------------------------------

def diagnose_duplicate_queue(
    *, database_path: Path
) -> QueueDiagnostic:
    """Report why each proposed duplicate is or is not being carded.

    Read-only: never writes to the database.
    """
    inbox = CandidateInbox(database_path)
    if not inbox.database_path.is_file() or inbox.database_path.is_symlink():
        raise InboxError("candidate inbox is not initialized")

    connection = sqlite3.connect(inbox.database_path, timeout=5)
    connection.row_factory = sqlite3.Row
    try:
        inbox._require_current_schema(connection)
        return _diagnose(connection)
    finally:
        connection.close()


def _diagnose(connection: sqlite3.Connection) -> QueueDiagnostic:
    """Core diagnostic logic operating on an open connection."""
    # Fetch all proposals — proposed, settled, superseded — so the operator
    # sees the full picture of what is in the queue and why each sits where it does.
    proposals = connection.execute(
        "SELECT d.id AS proposal_id, "
        "d.left_task_id, d.right_task_id, "
        "d.left_task_version, d.right_task_version, "
        "d.detector, d.state, d.card_id "
        "FROM task_duplicate_proposals AS d "
        "ORDER BY d.id"
    ).fetchall()

    if not proposals:
        return QueueDiagnostic(
            total_proposed=0,
            carded=0,
            uncarded=0,
            permanently_blocked=0,
            transiently_blocked=0,
            proposals=(),
        )

    # Collect all task ids to fetch in one query
    task_ids = set()
    for p in proposals:
        task_ids.add(int(p["left_task_id"]))
        task_ids.add(int(p["right_task_id"]))

    # Fetch current task state
    tasks = {}
    if task_ids:
        placeholders = ",".join("?" * len(task_ids))
        for row in connection.execute(
            f"SELECT id, status, version FROM tasks WHERE id IN ({placeholders})",
            sorted(task_ids),
        ):
            tasks[int(row["id"])] = {
                "status": str(row["status"]),
                "version": int(row["version"]),
            }

    # Fetch active workflow states for each task
    workflow_holds: dict[int, bool] = {}
    task_ph = ",".join("?" * len(task_ids)) if task_ids else "0"
    if task_ids:
        for row in connection.execute(
            f"SELECT task_id FROM task_execution_workflows "
            f"WHERE task_id IN ({task_ph}) "
            f"AND status NOT IN ({_unheld_workflow_sql()})",
            sorted(task_ids),
        ):
            workflow_holds[int(row["task_id"])] = True

    # Fetch active card state for each task
    active_cards: dict[int, dict[str, object]] = {}
    if task_ids:
        for row in connection.execute(
            f"SELECT task_id, id, status FROM task_review_cards "
            f"WHERE task_id IN ({task_ph}) "
            f"AND status IN ('pending','delivering','delivered','snoozed') "
            f"ORDER BY task_id, id",
            sorted(task_ids),
        ):
            tid = int(row["task_id"])
            if tid not in active_cards:
                active_cards[tid] = {
                    "card_id": int(row["id"]),
                    "status": str(row["status"]),
                }

    # For each pending card, check if it already has a proposed duplicate bound
    cards_with_proposals = set()
    for card_info in active_cards.values():
        cid = card_info["card_id"]
        existing = connection.execute(
            "SELECT 1 FROM task_duplicate_proposals "
            "WHERE card_id=? AND state='proposed'",
            (cid,),
        ).fetchone()
        if existing is not None:
            cards_with_proposals.add(cid)

    diagnostics: list[ProposalDiagnostic] = []
    total_proposed = 0
    carded = 0
    uncarded = 0
    permanently_blocked = 0
    transiently_blocked = 0

    for p in proposals:
        pid = int(p["proposal_id"])
        left_id = int(p["left_task_id"])
        right_id = int(p["right_task_id"])
        left_rec_version = int(p["left_task_version"])
        right_rec_version = int(p["right_task_version"])
        detector = str(p["detector"])
        state = str(p["state"])
        card_id = int(p["card_id"]) if p["card_id"] is not None else None

        left_task = tasks.get(left_id, {})
        right_task = tasks.get(right_id, {})
        left_status = left_task.get("status", "unknown")
        right_status = right_task.get("status", "unknown")
        left_cur_version = left_task.get("version")
        right_cur_version = right_task.get("version")

        reasons: list[DisqualificationReason] = []

        # 1. Proposal state check
        if state != "proposed":
            reasons.append(_REASON_SETTLED)
            reasons_tuple = tuple(reasons)
            # Build diagnostic and continue — settled proposals need no
            # further checks.
            diag = ProposalDiagnostic(
                proposal_id=pid,
                left_task_id=left_id,
                right_task_id=right_id,
                left_task_version=left_rec_version,
                right_task_version=right_rec_version,
                left_task_status=left_status,
                right_task_status=right_status,
                detector=detector,
                state=state,
                card_id=card_id,
                reasons=reasons_tuple,
            )
            diagnostics.append(diag)
            continue

        # 2. Already carded — happy path, no blocking reasons.
        if card_id is not None:
            # Count it
            total_proposed += 1
            carded += 1
            reasons_tuple = tuple(reasons)
            diag = ProposalDiagnostic(
                proposal_id=pid,
                left_task_id=left_id,
                right_task_id=right_id,
                left_task_version=left_rec_version,
                right_task_version=right_rec_version,
                left_task_status=left_status,
                right_task_status=right_status,
                detector=detector,
                state=state,
                card_id=card_id,
                reasons=reasons_tuple,
            )
            diagnostics.append(diag)
            continue

        # Remaining checks apply only to proposed, uncarded proposals.

        # 3. Workflow holds (transient)
        if left_id in workflow_holds:
            reasons.append(_REASON_LEFT_WORKFLOW_HOLD)
        if right_id in workflow_holds:
            reasons.append(_REASON_RIGHT_WORKFLOW_HOLD)

        # 4. Version staleness (permanent)
        if left_cur_version is not None and left_rec_version != left_cur_version:
            reasons.append(_REASON_LEFT_VERSION_STALE)
        if right_cur_version is not None and right_rec_version != right_cur_version:
            reasons.append(_REASON_RIGHT_VERSION_STALE)

        # 5. Status condition
        if not _reviewable_status_check(left_status, right_status):
            # Distinguish: both closed vs. other invalid pair
            if left_status != "open" and right_status != "open":
                reasons.append(_REASON_BOTH_TASKS_CLOSED)
            else:
                reasons.append(_REASON_TASK_STATUS_INVALID)

        # 6. Card conflict (only when no permanent reason already exists)
        if not any(r.permanent for r in reasons):
            # Determine which task would be carded (the open one)
            if left_status == "open":
                card_target = left_id
            elif right_status == "open":
                card_target = right_id
            else:
                card_target = None

            if card_target is not None and card_target in active_cards:
                card_info = active_cards[card_target]
                card_status = card_info["status"]
                existing_cid = card_info["card_id"]

                if card_status != "pending":
                    # Active card is delivering/delivered/snoozed — cannot bind
                    reasons.append(_REASON_CARD_CONFLICT)
                elif existing_cid in cards_with_proposals:
                    # Pending card already asks a different comparison
                    reasons.append(_REASON_CARD_CONFLICT)

        reasons_tuple = tuple(reasons)
        diag = ProposalDiagnostic(
            proposal_id=pid,
            left_task_id=left_id,
            right_task_id=right_id,
            left_task_version=left_rec_version,
            right_task_version=right_rec_version,
            left_task_status=left_status,
            right_task_status=right_status,
            detector=detector,
            state=state,
            card_id=card_id,
            reasons=reasons_tuple,
        )
        diagnostics.append(diag)

        # Counting
        if state == "proposed":
            total_proposed += 1
            if card_id is not None:
                carded += 1
            else:
                uncarded += 1
                has_permanent = any(r.permanent for r in reasons_tuple)
                if has_permanent:
                    permanently_blocked += 1
                elif reasons_tuple:
                    transiently_blocked += 1
                # If no reasons and not carded, it's eligible but not yet
                # picked up (within limit, or no run happened yet)

    return QueueDiagnostic(
        total_proposed=total_proposed,
        carded=carded,
        uncarded=uncarded,
        permanently_blocked=permanently_blocked,
        transiently_blocked=transiently_blocked,
        proposals=tuple(diagnostics),
    )


# ---------------------------------------------------------------------------
# Serialisation helpers
# ---------------------------------------------------------------------------

def _diagnostic_to_json(diag: QueueDiagnostic) -> dict:
    """Content-free JSON serialisation of the diagnostic."""
    return {
        "summary": {
            "proposed": diag.total_proposed,
            "carded": diag.carded,
            "uncarded": diag.uncarded,
            "permanently_blocked": diag.permanently_blocked,
            "transiently_blocked": diag.transiently_blocked,
        },
        "proposals": [
            {
                "proposal_id": p.proposal_id,
                "left_task_id": p.left_task_id,
                "right_task_id": p.right_task_id,
                "left_task_version": p.left_task_version,
                "right_task_version": p.right_task_version,
                "left_task_status": p.left_task_status,
                "right_task_status": p.right_task_status,
                "detector": p.detector,
                "state": p.state,
                "card_id": p.card_id,
                "reasons": [
                    {
                        "code": r.code,
                        "permanent": r.permanent,
                        "description": r.description,
                    }
                    for r in p.reasons
                ],
            }
            for p in diag.proposals
        ],
    }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="foxhound-task-duplicate-queue-diagnostic",
        description="Diagnose why duplicate proposals are not being carded",
    )
    parser.add_argument("--database", required=True, type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    arguments = _parser().parse_args(argv)
    try:
        result = diagnose_duplicate_queue(database_path=arguments.database)
    except (TaskBootstrapConfigError, InboxError, TaskLedgerError,
            OSError, ValueError) as exc:
        print(json.dumps({
            "accepted": False,
            "error": str(exc).split("\n")[0],
        }, separators=(",", ":")))
        return 2
    output = _diagnostic_to_json(result)
    print(json.dumps(output, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
