"""Effective task deadline computation and banding."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any, Mapping, Sequence


@dataclass(frozen=True)
class TaskTiming:
    due: date | None = None
    effort: str | None = None  # "hour" | "day" | "week" | None
    open: bool = True


@dataclass(frozen=True)
class DeadlineResult:
    deadlines: dict[Any, date | None]
    cycles: list[list[Any]]
    infeasible: list[tuple[Any, date]]


_EFFORT_DAYS: dict[str | None, int] = {
    "hour": 0,
    "day": 1,
    "week": 7,
    None: 0,
}


def _effort_to_days(effort: str | None) -> int:
    return _EFFORT_DAYS.get(effort, 0)


def band(deadline: date | None, today: date) -> int:
    """Return the deadline band for queue ordering:

    0 = overdue or within 3 days (deadline <= today + 3 days)
    1 = within 14 days (today + 3 days < deadline <= today + 14 days)
    2 = later or none (deadline > today + 14 days, or deadline is None)
    """
    if deadline is None:
        return 2
    delta = (deadline - today).days
    if delta <= 3:
        return 0
    if delta <= 14:
        return 1
    return 2


def effective_deadlines(
    tasks: Mapping[Any, TaskTiming],
    dependencies: Sequence[tuple[Any, Any]],
    today: date | None = None,
) -> DeadlineResult:
    """Compute effective deadlines for tasks.

    Input:
      - tasks: mapping of task_id -> TaskTiming
      - dependencies: sequence of (predecessor_id, dependant_id) where dependant waits for predecessor.
      - today: reference date for detecting infeasible deadlines (effective deadline < today).

    A task's effective deadline is the earliest of its own due and, for every OPEN dependant,
    that dependant's effective deadline minus the dependant's effort.
    Tasks in cycles keep their own due only, and cycles are reported deterministically.
    """
    # Sorted list of all known task IDs
    all_task_ids = sorted(
        set(tasks.keys()) | {p for p, _ in dependencies} | {d for _, d in dependencies},
        key=lambda x: str(x),
    )

    # Filtered graph over known tasks
    # Forward: predecessor -> list of dependants
    # Backward: dependant -> list of predecessors
    dependants_of: dict[Any, set[Any]] = {t: set() for t in all_task_ids}
    predecessors_of: dict[Any, set[Any]] = {t: set() for t in all_task_ids}

    for pred, dep in dependencies:
        if pred in dependants_of and dep in dependants_of:
            dependants_of[pred].add(dep)
            predecessors_of[dep].add(pred)

    # Find strongly connected components (SCCs) using Tarjan's algorithm (deterministic via sorted iterations)
    # to identify cycles cleanly.
    index = 0
    indices: dict[Any, int] = {}
    lowlinks: dict[Any, int] = {}
    on_stack: dict[Any, bool] = {t: False for t in all_task_ids}
    stack: list[Any] = []
    sccs: list[list[Any]] = []

    for node in all_task_ids:
        if node not in indices:
            # Iterative or recursive Tarjan. Using call stack to avoid recursion depth issues.
            # Explicit call stack for Tarjan DFS:
            call_stack: list[tuple[Any, int, list[Any]]] = [
                (node, 0, sorted(dependants_of[node], key=lambda x: str(x)))
            ]
            indices[node] = lowlinks[node] = index
            index += 1
            stack.append(node)
            on_stack[node] = True

            while call_stack:
                cur, edge_idx, neighbors = call_stack[-1]
                if edge_idx < len(neighbors):
                    nxt = neighbors[edge_idx]
                    # Update edge index for current frame
                    call_stack[-1] = (cur, edge_idx + 1, neighbors)
                    if nxt not in indices:
                        indices[nxt] = lowlinks[nxt] = index
                        index += 1
                        stack.append(nxt)
                        on_stack[nxt] = True
                        call_stack.append(
                            (nxt, 0, sorted(dependants_of[nxt], key=lambda x: str(x)))
                        )
                    elif on_stack[nxt]:
                        lowlinks[cur] = min(lowlinks[cur], indices[nxt])
                else:
                    # Finished exploring cur
                    call_stack.pop()
                    if call_stack:
                        parent = call_stack[-1][0]
                        lowlinks[parent] = min(lowlinks[parent], lowlinks[cur])

                    if lowlinks[cur] == indices[cur]:
                        scc: list[Any] = []
                        while True:
                            w = stack.pop()
                            on_stack[w] = False
                            scc.append(w)
                            if w == cur:
                                break
                        sccs.append(scc)

    # Identify cyclic nodes (SCC of size > 1, or self-loop)
    cyclic_nodes: set[Any] = set()
    cycles_found: list[list[Any]] = []

    for scc in sccs:
        if len(scc) > 1:
            cyclic_nodes.update(scc)
            # Find an elementary cycle in scc deterministically
            # Sort SCC to pick smallest starting node
            scc_sorted = sorted(scc, key=lambda x: str(x))
            start = scc_sorted[0]
            # Simple BFS/DFS within SCC to extract a cycle from start back to start
            path = [start]
            visited = {start}
            found_cycle = False

            def find_cycle_path(curr: Any) -> list[Any] | None:
                for neighbor in sorted(dependants_of[curr], key=lambda x: str(x)):
                    if neighbor == start and len(path) > 1:
                        return path + [start]
                    if neighbor in scc and neighbor not in visited:
                        visited.add(neighbor)
                        path.append(neighbor)
                        res = find_cycle_path(neighbor)
                        if res is not None:
                            return res
                        path.pop()
                        visited.remove(neighbor)
                return None

            c_path = find_cycle_path(start)
            if c_path:
                cycles_found.append(c_path[:-1])
            else:
                cycles_found.append(scc_sorted)
        elif len(scc) == 1:
            node = scc[0]
            if node in dependants_of[node]:
                cyclic_nodes.add(node)
                cycles_found.append([node])

    # Sort reported cycles deterministically
    cycles_found.sort(key=lambda c: [str(x) for x in c])

    # Deadlines mapping initialisation: own due date
    deadlines: dict[Any, date | None] = {}
    for t in all_task_ids:
        timing = tasks.get(t)
        deadlines[t] = timing.due if timing else None

    # Propagation graph:
    # A dependant pulls its predecessor's deadline earlier.
    # So info flows from dependant -> predecessor.
    # Only OPEN dependants pull predecessors!
    # Also ignore edges involving cyclic nodes for propagation.
    rev_adj: dict[Any, list[Any]] = {t: [] for t in all_task_ids}
    in_degrees: dict[Any, int] = {t: 0 for t in all_task_ids}

    for pred in all_task_ids:
        if pred in cyclic_nodes:
            continue
        for dep in sorted(dependants_of[pred], key=lambda x: str(x)):
            if dep in cyclic_nodes:
                continue
            dep_timing = tasks.get(dep)
            # Only consider if dependant is open
            if dep_timing is not None and not dep_timing.open:
                continue
            # Directed propagation edge: dep -> pred
            rev_adj[dep].append(pred)
            in_degrees[pred] += 1

    # Topological sort for backwards propagation (from dependants with no open dependants to predecessors)
    # Using Kahn's algorithm with deterministic tie-breaking (sorted queue)
    import heapq

    # Queue of nodes with in_degrees == 0 (no open dependants pulling on them)
    # We sort by str(x)
    class PrioritizedItem:
        __slots__ = ("key", "val")

        def __init__(self, val: Any):
            self.val = val
            self.key = str(val)

        def __lt__(self, other: PrioritizedItem) -> bool:
            return self.key < other.key

    queue: list[PrioritizedItem] = [
        PrioritizedItem(t)
        for t in all_task_ids
        if in_degrees[t] == 0 and t not in cyclic_nodes
    ]
    heapq.heapify(queue)

    topo_order: list[Any] = []
    while queue:
        item = heapq.heappop(queue)
        u = item.val
        topo_order.append(u)
        for pred in rev_adj[u]:
            in_degrees[pred] -= 1
            if in_degrees[pred] == 0:
                heapq.heappush(queue, PrioritizedItem(pred))

    # Propagate effective deadlines in topological order
    for dep in topo_order:
        dep_effective = deadlines[dep]
        if dep_effective is None:
            continue
        dep_timing = tasks.get(dep)
        effort_days = _effort_to_days(dep_timing.effort if dep_timing else None)
        candidate = dep_effective - timedelta(days=effort_days)

        for pred in rev_adj[dep]:
            curr_pred_dl = deadlines[pred]
            if curr_pred_dl is None or candidate < curr_pred_dl:
                deadlines[pred] = candidate

    # Find infeasible deadlines if today is provided
    infeasible: list[tuple[Any, date]] = []
    if today is not None:
        for t in all_task_ids:
            dl = deadlines[t]
            if dl is not None and dl < today:
                infeasible.append((t, dl))
        infeasible.sort(key=lambda x: str(x[0]))

    return DeadlineResult(
        deadlines=deadlines,
        cycles=cycles_found,
        infeasible=infeasible,
    )
