"""Backfill tool to close open tasks whose forge candidate was withdrawn with reader_conflict."""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Sequence

from .candidate_inbox import CandidateInbox
from .task_bootstrap import TaskBootstrapConfigError, _private_database
from .task_ledger import TaskLedger, TaskLedgerError


def _candidate_stub(source_kind: str, item_id: str) -> object:
    return SimpleNamespace(source=SimpleNamespace(kind=source_kind, item_id=item_id))


def close_forge_withdrawn_task(
    connection: sqlite3.Connection,
    *,
    task_id: int,
    candidate_id: str,
    source_kind: str,
    item_id: str,
    now: str,
) -> int | None:
    """Close an open task withdrawn with reader_conflict if not running.

    Does not open or commit a transaction on connection. Returns the new task
    version on success, or None if skipped/refused.
    """
    curr_task = connection.execute(
        "SELECT * FROM tasks WHERE id = ?", (task_id,)
    ).fetchone()
    if curr_task is None or curr_task["status"] != "open":
        return None

    wf = connection.execute(
        "SELECT status FROM task_execution_workflows WHERE task_id = ?",
        (task_id,),
    ).fetchone()
    if wf is not None and wf["status"] == "running":
        return None

    stub = _candidate_stub(source_kind, item_id)
    new_version = TaskLedger._close_for_forge_source(
        connection,
        candidate=stub,  # type: ignore[arg-type]
        task=curr_task,
        now=now,
    )
    if new_version is None:
        return None

    connection.execute(
        "UPDATE task_candidate_lifecycle "
        "SET resolution = 'closed_by_source', task_version = ?, decided_at = ? "
        "WHERE candidate_id = ?",
        (new_version, now, candidate_id),
    )
    connection.execute(
        "UPDATE work_items SET state = 'closed' WHERE task_id = ?",
        (task_id,),
    )
    return int(new_version)


def run_forge_close_backfill(
    *,
    database_path: Path,
    task_ids: Sequence[int] | None = None,
    limit: int | None = None,
    apply: bool = False,
    now: str | None = None,
) -> dict[str, object]:
    database = _private_database(database_path)
    inbox = CandidateInbox(database)
    timestamp = now or datetime.now(timezone.utc).isoformat(timespec="seconds")

    connection = sqlite3.connect(database, timeout=5)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    try:
        inbox._require_current_schema(connection)

        query = [
            "SELECT t.id, t.version, t.status, b.candidate_id, c.source_kind, c.source_item_id ",
            "FROM tasks AS t ",
            "JOIN task_candidate_bindings AS b ON b.task_id = t.id AND b.relation = 'accepted' ",
            "JOIN candidate_inbox AS c ON c.candidate_id = b.candidate_id ",
            "JOIN task_candidate_lifecycle AS l ON l.candidate_id = b.candidate_id ",
            "WHERE t.status = 'open' ",
            "AND c.source_kind IN ('issue', 'review_request') ",
            "AND l.state = 'withdrawn' ",
            "AND l.resolution = 'reader_conflict' ",
        ]
        params: list[object] = []

        if task_ids:
            unique_task_ids = sorted(set(task_ids))
            placeholders = ",".join("?" for _ in unique_task_ids)
            query.append(f"AND t.id IN ({placeholders}) ")
            params.extend(unique_task_ids)

        query.append("ORDER BY t.id")
        if limit is not None and limit >= 0:
            query.append("LIMIT ?")
            params.append(limit)

        sql = "".join(query)
        rows = connection.execute(sql, params).fetchall()

        eligible: list[int] = [int(r["id"]) for r in rows]
        skipped: list[dict[str, object]] = []
        closed: list[int] = []

        if not apply:
            return {
                "dry_run": True,
                "eligible": eligible,
                "skipped": skipped,
            }

        # Apply mode
        for row in rows:
            task_id = int(row["id"])
            candidate_id = str(row["candidate_id"])
            source_kind = str(row["source_kind"])

            connection.execute("BEGIN IMMEDIATE")
            try:
                # Check why it might be skipped if close_forge_withdrawn_task returns None
                curr_task = connection.execute(
                    "SELECT status FROM tasks WHERE id = ?", (task_id,)
                ).fetchone()
                if curr_task is None or curr_task["status"] != "open":
                    connection.rollback()
                    skipped.append({"task_id": task_id, "reason": "refused"})
                    continue

                wf = connection.execute(
                    "SELECT status FROM task_execution_workflows WHERE task_id = ?",
                    (task_id,),
                ).fetchone()
                if wf is not None and wf["status"] == "running":
                    connection.rollback()
                    skipped.append({"task_id": task_id, "reason": "running"})
                    continue

                version = close_forge_withdrawn_task(
                    connection,
                    task_id=task_id,
                    candidate_id=candidate_id,
                    source_kind=source_kind,
                    item_id=str(row["source_item_id"]),
                    now=timestamp,
                )
                if version is None:
                    connection.rollback()
                    wf_after = connection.execute(
                        "SELECT status FROM task_execution_workflows WHERE task_id = ?",
                        (task_id,),
                    ).fetchone()
                    reason = "running" if (wf_after is not None and wf_after["status"] == "running") else "refused"
                    skipped.append({"task_id": task_id, "reason": reason})
                    continue

                connection.commit()
                closed.append(task_id)
            except Exception:
                connection.rollback()
                raise

        return {
            "dry_run": False,
            "eligible": eligible,
            "closed": closed,
            "skipped": skipped,
        }
    finally:
        connection.close()


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="foxhound-task-forge-close-backfill",
        description="Backfill forge withdrawal closure for reader_conflict tasks",
    )
    parser.add_argument("--database", required=True, type=Path)
    parser.add_argument(
        "--task-id",
        dest="task_ids",
        action="append",
        type=int,
        help="Include specific task ID (can be repeated)",
    )
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="write changes (the default is a dry-run)",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        result = run_forge_close_backfill(
            database_path=args.database,
            task_ids=args.task_ids,
            limit=args.limit,
            apply=args.apply,
        )
    except (TaskBootstrapConfigError, TaskLedgerError, OSError, ValueError) as exc:
        print(f"foxhound task forge close backfill: operation failed: {exc}", file=sys.stderr)
        return 70
    except Exception as exc:
        print(f"foxhound task forge close backfill: operation failed: {exc}", file=sys.stderr)
        return 70

    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
