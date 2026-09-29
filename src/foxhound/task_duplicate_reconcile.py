"""Reconcile unverified Stage-1 candidates with the current route policy."""

from __future__ import annotations

import argparse
import json
import sqlite3
from contextlib import closing
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

from .candidate_inbox import CandidateInbox, InboxError
from .task_duplicate_stage1 import INDEPENDENT_ROUTES, ROUTE_WEIGHTS


DEFAULT_LIMIT = 1_000
MAX_LIMIT = 10_000


@dataclass(frozen=True)
class ReconcileResult:
    applied: bool
    examined: int
    score_changes: int
    supporting_only_removed: int
    claimed_skipped: int


def _scored_candidates_sql() -> str:
    independent = ",".join(f"'{route}'" for route in sorted(INDEPENDENT_ROUTES))
    factors = "*".join(
        "(1.0-MAX(CASE WHEN route.route='{}' AND route.score IS NOT NULL "
        "THEN {}*route.score ELSE 0.0 END))".format(route, weight)
        for route, weight in sorted(ROUTE_WEIGHTS.items())
    )
    return (
        "WITH scored AS ("
        "SELECT candidate.id,candidate.rank_score,"
        "(claim.candidate_id IS NOT NULL) AS claimed,"
        f"SUM(CASE WHEN route.route IN ({independent}) THEN 1 ELSE 0 END) "
        "AS independent_routes,"
        f"1.0-({factors}) AS current_score "
        "FROM task_duplicate_candidates AS candidate "
        "JOIN tasks AS left_task ON left_task.id=candidate.left_task_id "
        "JOIN tasks AS right_task ON right_task.id=candidate.right_task_id "
        "LEFT JOIN task_duplicate_verifications AS verification "
        "ON verification.candidate_id=candidate.id "
        "LEFT JOIN task_duplicate_verification_claims AS claim "
        "ON claim.candidate_id=candidate.id "
        "LEFT JOIN task_duplicate_candidate_routes AS route "
        "ON route.candidate_id=candidate.id "
        "WHERE candidate.state='queued' AND verification.candidate_id IS NULL "
        "AND candidate.left_task_version=left_task.version "
        "AND candidate.right_task_version=right_task.version "
        "GROUP BY candidate.id,candidate.rank_score,claim.candidate_id"
        "), changes AS ("
        "SELECT * FROM scored WHERE independent_routes=0 "
        "OR abs(rank_score-current_score)>1e-12"
        ") "
    )


def reconcile(
    connection: sqlite3.Connection, *, apply: bool, limit: int, now: str,
) -> ReconcileResult:
    """Re-score or remove a bounded set of current, unverified candidates."""
    if isinstance(limit, bool) or not 1 <= limit <= MAX_LIMIT:
        raise ValueError("reconciliation limit is invalid")

    scored_sql = _scored_candidates_sql()
    claimed_skipped = int(connection.execute(
        scored_sql + "SELECT count(*) FROM changes WHERE claimed",
    ).fetchone()[0])
    rows = connection.execute(
        scored_sql + "SELECT id,independent_routes,current_score FROM changes "
        "WHERE NOT claimed ORDER BY id LIMIT ?",
        (limit,),
    ).fetchall()

    score_changes = supporting_only_removed = 0
    for row in rows:
        candidate_id = int(row["id"])
        if int(row["independent_routes"]) == 0:
            supporting_only_removed += 1
            if apply:
                connection.execute(
                    "DELETE FROM task_duplicate_candidate_routes WHERE candidate_id=?",
                    (candidate_id,),
                )
                connection.execute(
                    "DELETE FROM task_duplicate_candidates WHERE id=?",
                    (candidate_id,),
                )
            continue

        score_changes += 1
        if apply:
            connection.execute(
                "UPDATE task_duplicate_candidates SET rank_score=?,updated_at=? "
                "WHERE id=?",
                (float(row["current_score"]), now, candidate_id),
            )

    return ReconcileResult(
        applied=apply,
        examined=len(rows) + claimed_skipped,
        score_changes=score_changes,
        supporting_only_removed=supporting_only_removed,
        claimed_skipped=claimed_skipped,
    )


def run_database(
    database_path: str | Path, *, apply: bool = False,
    limit: int = DEFAULT_LIMIT,
) -> ReconcileResult:
    inbox = CandidateInbox(database_path)
    if not inbox.database_path.is_file() or inbox.database_path.is_symlink():
        raise InboxError("candidate inbox is not initialized")
    with closing(sqlite3.connect(inbox.database_path, timeout=5)) as connection:
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        inbox._require_current_schema(connection)
        if apply:
            connection.execute("BEGIN IMMEDIATE")
        try:
            result = reconcile(
                connection, apply=apply, limit=limit,
                now=datetime.now(timezone.utc).isoformat(timespec="seconds"),
            )
            if apply:
                connection.commit()
            return result
        except Exception:
            if apply:
                connection.rollback()
            raise


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="foxhound-task-duplicate-reconcile",
        description="Reconcile unverified duplicate candidates with current route policy",
    )
    parser.add_argument("--database", required=True, type=Path)
    parser.add_argument("--limit", default=DEFAULT_LIMIT, type=int)
    parser.add_argument("--apply", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    arguments = _parser().parse_args(argv)
    try:
        result = run_database(
            arguments.database, apply=arguments.apply, limit=arguments.limit,
        )
    except (InboxError, sqlite3.Error, OSError, ValueError):
        print(json.dumps({"accepted": False}, separators=(",", ":")))
        return 2
    print(json.dumps({"accepted": True, **asdict(result)}, sort_keys=True))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
