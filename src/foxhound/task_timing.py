"""Task timing storage for research-backed effort and deadline estimates."""

from __future__ import annotations

import sqlite3
from typing import Any, Mapping


def record_research_timing(
    connection: sqlite3.Connection,
    task_id: int,
    job_id: str,
    document: Mapping[str, Any],
    now: str,
) -> bool:
    """UPSERT task timing from research report if effort or deadline present."""
    report = document.get("report")
    if not isinstance(report, dict):
        return False

    effort_block = report.get("effort")
    deadline_block = report.get("deadline")

    new_effort = effort_block.get("size") if isinstance(effort_block, dict) else None
    new_due = deadline_block.get("date") if isinstance(deadline_block, dict) else None

    if effort_block is None and deadline_block is None:
        return False

    cursor = connection.execute(
        "SELECT effort, researched_due FROM task_timing WHERE task_id = ?",
        (task_id,),
    )
    row = cursor.fetchone()

    if row is not None:
        prev_effort, prev_due = row[0], row[1]
        effort_to_set = new_effort if effort_block is not None else prev_effort
        due_to_set = new_due if deadline_block is not None else prev_due
        if effort_to_set == prev_effort and due_to_set == prev_due:
            return False
        connection.execute(
            """
            UPDATE task_timing
            SET effort = ?, researched_due = ?, source_job_id = ?, updated_at = ?
            WHERE task_id = ?
            """,
            (effort_to_set, due_to_set, job_id, now, task_id),
        )
        return True
    else:
        effort_to_set = new_effort if effort_block is not None else None
        due_to_set = new_due if deadline_block is not None else None
        connection.execute(
            """
            INSERT INTO task_timing (task_id, effort, researched_due, source_job_id, updated_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (task_id, effort_to_set, due_to_set, job_id, now),
        )
        return True


def read_timing(
    connection: sqlite3.Connection,
    task_ids: list[int],
) -> dict[int, tuple[str | None, str | None]]:
    """Read timing (effort, researched_due) for task_ids."""
    if not task_ids:
        return {}
    placeholders = ",".join("?" for _ in task_ids)
    cursor = connection.execute(
        f"SELECT task_id, effort, researched_due FROM task_timing WHERE task_id IN ({placeholders})",
        tuple(task_ids),
    )
    result: dict[int, tuple[str | None, str | None]] = {tid: (None, None) for tid in task_ids}
    for row in cursor.fetchall():
        result[row[0]] = (row[1], row[2])
    return result


def with_research_timing(connection: sqlite3.Connection, tasks: Mapping[int, Any]) -> dict[int, Any]:
    """Fold stored research timing into `task_deadlines.TaskTiming` inputs.

    The earlier of the task's own due date and a cited research deadline
    wins, and a researched effort replaces an unknown one. A task without a
    timing row is returned unchanged; any read failure returns the input.
    """
    from dataclasses import replace
    from datetime import date

    try:
        timing = read_timing(connection, list(tasks))
    except sqlite3.Error:
        return dict(tasks)
    result = dict(tasks)
    for task_id, (effort, researched_due) in timing.items():
        current = result.get(task_id)
        if current is None or (effort is None and researched_due is None):
            continue
        due = current.due
        if researched_due:
            try:
                cited = date.fromisoformat(researched_due)
            except ValueError:
                cited = None
            if cited is not None and (due is None or cited < due):
                due = cited
        result[task_id] = replace(
            current, due=due, effort=current.effort or effort)
    return result
