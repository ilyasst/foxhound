"""Bulk drop open tasks with atomic workflow and review card cleanup."""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path
from typing import Sequence

from .candidate_inbox import CandidateInbox
from .execution_cards import ExecutionCardService, _apply_review_lifecycle_action
from .task_bootstrap import TaskBootstrapConfigError, _private_database
from .task_cards import TaskCardService
from .task_ledger import (
    TaskLedger,
    TaskLedgerError,
    TransitionDisposition,
    TransitionRefusal,
    _apply_task_transition,
)


def run_bulk_drop(
    *,
    database_path: Path,
    reason: str,
    task_ids: Sequence[int] | None = None,
    exclude_task_ids: Sequence[int] | None = None,
    limit: int | None = None,
    apply: bool = False,
    now: str | None = None,
) -> dict[str, object]:
    database = _private_database(database_path)
    inbox = CandidateInbox(database)
    timestamp = now or datetime.now(timezone.utc).isoformat(timespec="seconds")

    connection = sqlite3.connect(database, timeout=5)
    connection.row_factory = sqlite3.Row
    # Every other writer enables this; a drop must not be the one path that
    # can leave a dangling reference behind.
    connection.execute("PRAGMA foreign_keys = ON")
    try:
        inbox._require_current_schema(connection)

        # Select open tasks according to filters
        query = ["SELECT id, version FROM tasks WHERE status='open'"]
        params: list[object] = []

        if task_ids:
            unique_task_ids = sorted(set(task_ids))
            placeholders = ",".join("?" for _ in unique_task_ids)
            query.append(f"AND id IN ({placeholders})")
            params.extend(unique_task_ids)

        if exclude_task_ids:
            unique_exclude_ids = sorted(set(exclude_task_ids))
            placeholders = ",".join("?" for _ in unique_exclude_ids)
            query.append(f"AND id NOT IN ({placeholders})")
            params.extend(unique_exclude_ids)

        query.append("ORDER BY id")
        if limit is not None and limit >= 0:
            query.append("LIMIT ?")
            params.append(limit)

        sql = " ".join(query)
        rows = connection.execute(sql, params).fetchall()

        seen = len(rows)
        dropped: list[int] = []
        skipped: list[dict[str, object]] = []

        if not apply:
            # Dry run: report what would happen without writes
            for row in rows:
                dropped.append(int(row["id"]))
            return {
                "dry_run": True,
                "reason": reason,
                "seen": seen,
                "dropped": dropped,
                "skipped": skipped,
            }

        # Apply: process each task in its own BEGIN IMMEDIATE transaction
        for row in rows:
            task_id = int(row["id"])
            expected_version = int(row["version"])

            connection.execute("BEGIN IMMEDIATE")
            try:
                # Check for workflow row
                wf_row = connection.execute(
                    "SELECT task_id, task_version, status, phase, version AS workflow_version "
                    "FROM task_execution_workflows WHERE task_id=?",
                    (task_id,),
                ).fetchone()

                if wf_row is not None:
                    # Drop via execution review lifecycle action
                    # Note: wf_row["task_version"] might differ from current tasks.version if out of sync
                    # but if we construct a mapping with current task_version or check conflict:
                    # Let's see: row has current task version.
                    wf_mapping = {
                        "task_id": task_id,
                        "task_version": expected_version,
                        "workflow_version": int(wf_row["workflow_version"]),
                        "phase": wf_row["phase"],
                    }
                    wf_result = _apply_review_lifecycle_action(
                        connection,
                        wf_mapping,
                        action="drop",
                        now=timestamp,
                    )
                    if not wf_result.accepted:
                        connection.rollback()
                        skipped.append({
                            "task_id": task_id,
                            "reason": f"workflow refused: {wf_result.refusal.value if wf_result.refusal else 'unknown'}",
                        })
                        continue
                else:
                    # No workflow row, apply ledger drop transition directly
                    transition = _apply_task_transition(
                        connection,
                        task_id=task_id,
                        expected_version=expected_version,
                        action="drop",
                        now=timestamp,
                    )
                    if not transition.accepted:
                        connection.rollback()
                        skipped.append({
                            "task_id": task_id,
                            "reason": f"ledger refused: {transition.refusal.value if transition.refusal else 'unknown'}",
                        })
                        continue

                connection.commit()
                dropped.append(task_id)
            except Exception:
                connection.rollback()
                raise

        # After the loop, call existing stale-cancel entry points once
        exec_cards_service = ExecutionCardService(database)
        task_cards_service = TaskCardService(database)

        connection.execute("BEGIN IMMEDIATE")
        try:
            exec_cards_service._cancel_stale(connection, timestamp)
            task_cards_service._cancel_stale(connection, timestamp)
            connection.commit()
        except Exception:
            connection.rollback()
            raise

        return {
            "dry_run": False,
            "reason": reason,
            "seen": seen,
            "dropped": dropped,
            "skipped": skipped,
        }
    finally:
        connection.close()


def _parse_id_list(values: Sequence[str] | None) -> list[int]:
    if not values:
        return []
    result: list[int] = []
    for item in values:
        for piece in str(item).split(","):
            piece = piece.strip()
            if piece:
                result.append(int(piece))
    return result


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="foxhound-task-bulk-drop",
        description="Bulk drop open tasks with atomic workflow and review card cleanup",
    )
    parser.add_argument("--database", required=True, type=Path)
    parser.add_argument("--reason", required=True, type=str)
    parser.add_argument(
        "--task-id",
        dest="task_ids",
        action="append",
        type=int,
        help="Include specific task ID (can be repeated)",
    )
    parser.add_argument(
        "--exclude-task-id",
        dest="exclude_task_ids",
        action="append",
        type=int,
        help="Exclude specific task ID (can be repeated)",
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
        result = run_bulk_drop(
            database_path=args.database,
            reason=args.reason,
            task_ids=args.task_ids,
            exclude_task_ids=args.exclude_task_ids,
            limit=args.limit,
            apply=args.apply,
        )
    except (TaskBootstrapConfigError, TaskLedgerError, OSError, ValueError) as exc:
        print(f"foxhound task bulk drop: operation failed: {exc}", file=sys.stderr)
        return 70
    except Exception as exc:
        print(f"foxhound task bulk drop: operation failed: {exc}", file=sys.stderr)
        return 70

    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
