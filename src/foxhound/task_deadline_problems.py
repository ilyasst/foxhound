"""Task deadline problems detection: cycles, infeasible chains, and foreign blockers."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date
import sqlite3

from .task_deadlines import TaskTiming, effective_deadlines
from .task_owner import confidently_other_owned, normalized_aliases, reader_owned


@dataclass(frozen=True)
class DeadlineProblems:
    cycles: tuple[tuple[int, ...], ...]
    infeasible: tuple[tuple[int, date], ...]
    foreign_blockers: tuple[tuple[int, int], ...]


def find_problems(
    connection: sqlite3.Connection,
    today: date,
    reader_aliases: Iterable[str],
) -> DeadlineProblems:
    """Find cycles, infeasible chains, and foreign blockers in task dependencies.

    - cycles: task-id cycles from active after_task_completed dependencies.
    - infeasible: tasks whose effective deadline is already past while a predecessor is still open.
    - foreign_blockers: (blocking_predecessor_id, blocked_task_id) pairs where an open
      predecessor confidently owned by someone other than the reader blocks a task the reader owns.

    Returns a frozen DeadlineProblems dataclass with deterministic sorted tuples.
    """
    aliases = normalized_aliases(reader_aliases)

    # 1. Read active after_task_completed dependencies
    dep_rows = connection.execute(
        "SELECT depends_on_task_id, task_id "
        "FROM task_scheduling_conditions "
        "WHERE kind='after_task_completed' AND state='active' AND depends_on_task_id IS NOT NULL"
    ).fetchall()

    dependencies: list[tuple[int, int]] = []
    dep_task_ids: set[int] = set()
    for row in dep_rows:
        pred_id = int(row[0] if isinstance(row, (tuple, list)) else row["depends_on_task_id"])
        dep_id = int(row[1] if isinstance(row, (tuple, list)) else row["task_id"])
        dependencies.append((pred_id, dep_id))
        dep_task_ids.add(pred_id)
        dep_task_ids.add(dep_id)

    if not dep_task_ids:
        return DeadlineProblems(cycles=(), infeasible=(), foreign_blockers=())

    # 2. Read task rows for involved tasks
    placeholders = ",".join("?" for _ in dep_task_ids)
    task_rows = connection.execute(
        f"SELECT id, status, due, owner, owner_kind, owner_ref_version, owner_provisional "
        f"FROM tasks WHERE id IN ({placeholders})",
        tuple(dep_task_ids),
    ).fetchall()

    tasks_by_id: dict[int, dict] = {}
    for r in task_rows:
        if isinstance(r, sqlite3.Row):
            r_dict = dict(r)
        elif isinstance(r, dict):
            r_dict = r
        else:
            r_dict = {
                "id": r[0],
                "status": r[1],
                "due": r[2],
                "owner": r[3],
                "owner_kind": r[4],
                "owner_ref_version": r[5],
                "owner_provisional": r[6],
            }
        tasks_by_id[int(r_dict["id"])] = r_dict

    tasks_map: dict[int, TaskTiming] = {}
    for tid in dep_task_ids:
        tr = tasks_by_id.get(tid)
        if tr is not None:
            due_raw = tr.get("due")
            due_val: date | None = None
            if due_raw:
                try:
                    due_val = date.fromisoformat(due_raw)
                except (ValueError, TypeError):
                    due_val = None
            is_open = (tr.get("status") == "open")
            tasks_map[tid] = TaskTiming(due=due_val, effort=None, open=is_open)
        else:
            tasks_map[tid] = TaskTiming(due=None, effort=None, open=True)

    deadline_result = effective_deadlines(tasks_map, dependencies, today=today)

    # Cycles: list of lists -> tuple of tuples
    cycles_list = [tuple(int(x) for x in c) for c in deadline_result.cycles]
    cycles_tuple: tuple[tuple[int, ...], ...] = tuple(sorted(cycles_list))

    # Infeasible: tasks whose effective deadline is already past while a predecessor is still open
    predecessors: dict[int, set[int]] = {tid: set() for tid in dep_task_ids}
    for pred_id, dep_id in dependencies:
        predecessors[dep_id].add(pred_id)

    infeasible_list: list[tuple[int, date]] = []
    for tid, dl in deadline_result.infeasible:
        # Check if any predecessor of tid is still open
        preds = predecessors.get(int(tid), set())
        has_open_pred = any(
            tasks_map.get(p_id) is not None and tasks_map[p_id].open
            for p_id in preds
        )
        if has_open_pred:
            infeasible_list.append((int(tid), dl))
    infeasible_tuple: tuple[tuple[int, date], ...] = tuple(
        sorted(infeasible_list, key=lambda item: (item[0], item[1]))
    )

    # Foreign blockers: (blocking_predecessor_id, blocked_task_id)
    foreign_blockers_list: list[tuple[int, int]] = []
    for pred_id, dep_id in dependencies:
        pred_task = tasks_by_id.get(pred_id)
        dep_task = tasks_by_id.get(dep_id)
        if pred_task is None or dep_task is None:
            continue
        # Predecessor must be open
        if pred_task.get("status") != "open":
            continue
        # Predecessor must be confidently owned by someone other than the reader
        if not confidently_other_owned(pred_task, aliases):
            continue
        # Blocked task must be owned by the reader
        if not reader_owned(dep_task, aliases):
            continue
        foreign_blockers_list.append((pred_id, dep_id))

    foreign_blockers_tuple: tuple[tuple[int, int], ...] = tuple(
        sorted(set(foreign_blockers_list))
    )

    return DeadlineProblems(
        cycles=cycles_tuple,
        infeasible=infeasible_tuple,
        foreign_blockers=foreign_blockers_tuple,
    )
