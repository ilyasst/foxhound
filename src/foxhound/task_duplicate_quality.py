"""Report how a duplicate detector is doing, from the reader's own answers.

Every card the reader settles is a label.  Comparing detectors by their
confirm rate turns a tuning argument into a measurement, and it keeps working
as the queue changes, which a hand-built fixture cannot.

The report is content-free: detector names and counts, never task text.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path
from typing import Sequence

from . import task_duplicate_assessments as assessments
from . import task_duplicate_proposals as proposals
from .candidate_inbox import CandidateInbox, InboxError


def report(database_path: str | Path) -> tuple[dict[str, object], ...]:
    """Per-detector proposal outcomes, newest schema assumed."""
    inbox = CandidateInbox(database_path)
    if not inbox.database_path.is_file() or inbox.database_path.is_symlink():
        raise InboxError("candidate inbox is not initialized")
    connection = sqlite3.connect(inbox.database_path, timeout=5)
    connection.row_factory = sqlite3.Row
    try:
        inbox._require_current_schema(connection)
        proposal_rows = proposals.counts(connection)
        assessment_rows = assessments.counts(connection)
    finally:
        connection.close()
    rows = [_proposal_line(row) for row in proposal_rows]
    rows.extend(_assessment_line(row) for row in assessment_rows)
    return tuple(sorted(rows, key=lambda row: str(row["detector"])))


def route_report(database_path: str | Path) -> tuple[dict[str, object], ...]:
    """Report candidacy routes without task text or proposal bases."""
    inbox = CandidateInbox(database_path)
    if not inbox.database_path.is_file() or inbox.database_path.is_symlink():
        raise InboxError("candidate inbox is not initialized")
    connection = sqlite3.connect(inbox.database_path, timeout=5)
    connection.row_factory = sqlite3.Row
    try:
        inbox._require_current_schema(connection)
        return _route_lines(connection)
    finally:
        connection.close()


def _route_lines(connection: sqlite3.Connection) -> tuple[dict[str, object], ...]:
    """Content-free route outcomes, including structure-only recall."""
    rows = connection.execute(
        "SELECT route, count(*) AS proposed,"
        "sum(state='confirmed') AS confirmed,sum(state='rejected') AS rejected "
        "FROM task_duplicate_proposal_routes AS route "
        "JOIN task_duplicate_proposals AS proposal ON proposal.id=route.proposal_id "
        "GROUP BY route.route ORDER BY route.route"
    ).fetchall()
    lines = [_route_line(str(row["route"]), row) for row in rows]
    only = connection.execute(
        "SELECT count(*) AS proposed,"
        "sum(state='confirmed') AS confirmed,sum(state='rejected') AS rejected "
        "FROM task_duplicate_proposals AS proposal "
        "WHERE EXISTS(SELECT 1 FROM task_duplicate_proposal_routes AS route "
        " WHERE route.proposal_id=proposal.id AND route.route IN ('object','participant')) "
        "AND NOT EXISTS(SELECT 1 FROM task_duplicate_proposal_routes AS route "
        " WHERE route.proposal_id=proposal.id AND route.route IN ('words','reread'))"
    ).fetchone()
    lines.append(_route_line("structure_only", only))
    return tuple(lines)


def _route_line(route: str, row: sqlite3.Row) -> dict[str, object]:
    proposed = int(row["proposed"] or 0)
    confirmed = int(row["confirmed"] or 0)
    rejected = int(row["rejected"] or 0)
    labels = confirmed + rejected
    return {
        "route": route,
        "proposed": proposed,
        "confirmed": confirmed,
        "rejected": rejected,
        "awaiting": max(proposed - labels, 0),
        "label_count": labels,
        "confirm_rate": None if labels == 0 else round(confirmed / labels, 3),
    }


def _proposal_line(counts: proposals.ProposalCounts) -> dict[str, object]:
    settled = counts.confirmed + counts.rejected
    return {
        "detector": counts.detector,
        "proposed": counts.proposed,
        "confirmed": counts.confirmed,
        "rejected": counts.rejected,
        "reopened": counts.reopened,
        "awaiting": max(counts.proposed - settled, 0),
        "label_count": settled,
        # None until the reader has answered something: a rate over zero
        # answers would read as a score rather than as an absence of evidence.
        "confirm_rate": None if settled == 0 else round(counts.confirmed / settled, 3),
    }


def _assessment_line(counts: assessments.AssessmentCounts) -> dict[str, object]:
    """Project local-model results onto reader labels without task content."""
    settled = counts.confirmed + counts.rejected
    return {
        "detector": counts.detector,
        # A semantic ``redundant`` verdict is the directly comparable proposal.
        "proposed": counts.redundant,
        "confirmed": counts.confirmed,
        "rejected": counts.rejected,
        "reopened": 0,
        "awaiting": counts.awaiting,
        # This is all settled reader evidence available to the semantic
        # evaluator, including the labels for its non-redundant verdicts.
        "label_count": counts.labeled,
        "confirm_rate": None if settled == 0 else round(counts.confirmed / settled, 3),
        "assessed": counts.assessed,
        "redundant": counts.redundant,
        "intersecting": counts.intersecting,
        "interconnected": counts.interconnected,
        "latency_ms": counts.latency_ms,
        "prompt_tokens": counts.prompt_tokens,
        "completion_tokens": counts.completion_tokens,
        "cost_usd": 0,
        # These disagreement counts use the incumbent's explicit lexical
        # basis, never task text or a stored explanation.
        "redundant_without_term_overlap": counts.redundant_without_overlap,
        "not_redundant_with_term_overlap": counts.not_redundant_with_overlap,
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="foxhound-task-duplicate-quality",
        description="Report duplicate-detector outcomes from reader decisions",
    )
    parser.add_argument("--database", required=True, type=Path)
    arguments = parser.parse_args(argv)
    try:
        lines = report(arguments.database)
        routes = route_report(arguments.database)
    except (InboxError, sqlite3.Error, ValueError):
        # The path and the exception can both be operational data.
        print(json.dumps({"accepted": False}, separators=(",", ":")))
        return 2
    print(json.dumps({"accepted": True, "detectors": list(lines), "routes": list(routes)},
                     sort_keys=True))
    return 0


if __name__ == "__main__":  # pragma: no cover - module entry point
    raise SystemExit(main())
