"""Bounded neighbouring-task resolution for worker and researcher context."""

from __future__ import annotations

import sqlite3
from contextlib import closing
from pathlib import Path
from typing import Any

#: Hard cap on neighbours returned in one context call. The section must
#: fit inside the context budget, so a task that shares a source with dozens
#: of siblings or carries many assessed relations does not turn the context
#: into a queue dump.
MAX_NEIGHBOURS = 10


def gather_task_neighbours(
    database_path: str | Path,
    task_id: int,
    *,
    limit: int | None = None,
) -> tuple[dict[str, Any], bool]:
    """Return bounded neighbour data for the context section.

    Each neighbour carries its task id, text, status, owner, and the selection
    rule (``same_source`` or ``assessed_similar``). The second return value
    is True when the limit was hit and more neighbours exist beyond what is
    shown.

    Selection rules:
    - same_source: the neighbour's accepted candidate shares the same
      source_record_id. This catches tasks from the same meeting, the same
      email thread, or the same issue tracker.
    - assessed_similar: a live task relation (``task_relations``) or a
      confirmed duplicate proposal links the two tasks.

    The limit applies to the merged result, not per rule. When the limit is
    reached the caller knows more exist and can report truncation.
    """
    if limit is None:
        limit = MAX_NEIGHBOURS
    with closing(sqlite3.connect(database_path)) as conn:
        conn.row_factory = sqlite3.Row
        neighbours: dict[int, dict[str, Any]] = {}
        total_count = 0

        # Rule 1: same source_record_id (same meeting / same thread)
        same_source_rows = conn.execute(
            "SELECT DISTINCT t.id, t.text, t.status, t.owner "
            "FROM tasks AS t "
            "JOIN task_candidate_bindings AS b1 ON b1.task_id=t.id "
            "JOIN candidate_inbox AS i1 ON i1.candidate_id=b1.candidate_id "
            "  AND i1.source_revision=b1.source_revision "
            "JOIN task_candidate_bindings AS b2 ON b2.task_id=? "
            "JOIN candidate_inbox AS i2 ON i2.candidate_id=b2.candidate_id "
            "  AND i2.source_revision=b2.source_revision "
            "WHERE t.id != ? "
            "  AND i1.source_record_id = i2.source_record_id "
            "  AND b1.relation='accepted' "
            "  AND b2.relation='accepted' "
            "ORDER BY t.id",
            (task_id, task_id),
        ).fetchall()
        for row in same_source_rows:
            tid = int(row["id"])
            if tid not in neighbours:
                total_count += 1
                neighbours[tid] = {
                    "id": tid,
                    "text": row["text"],
                    "status": row["status"],
                    "owner": row["owner"],
                    "selection": "same_source",
                }

        # Rule 2: assessed similar via live task_relations
        relation_rows = conn.execute(
            "SELECT DISTINCT t.id, t.text, t.status, t.owner "
            "FROM tasks AS t "
            "JOIN task_relations AS r ON r.subject_id=t.id OR r.object_id=t.id "
            "WHERE t.id != ? "
            "  AND (r.subject_id=? OR r.object_id=?) "
            "  AND r.withdrawn_at IS NULL "
            "ORDER BY t.id",
            (task_id, task_id, task_id),
        ).fetchall()
        for row in relation_rows:
            tid = int(row["id"])
            if tid not in neighbours:
                total_count += 1
                neighbours[tid] = {
                    "id": tid,
                    "text": row["text"],
                    "status": row["status"],
                    "owner": row["owner"],
                    "selection": "assessed_similar",
                }

        # Rule 3: assessed similar via confirmed duplicate proposals
        proposal_rows = conn.execute(
            "SELECT DISTINCT t.id, t.text, t.status, t.owner "
            "FROM tasks AS t "
            "JOIN task_duplicate_proposals AS dp "
            "  ON (dp.left_task_id=t.id OR dp.right_task_id=t.id) "
            "WHERE t.id != ? "
            "  AND ((dp.left_task_id=? AND dp.right_task_id != ?) "
            "       OR (dp.right_task_id=? AND dp.left_task_id != ?)) "
            "  AND dp.state IN ('confirmed', 'proposed') "
            "ORDER BY t.id",
            (task_id, task_id, task_id, task_id, task_id),
        ).fetchall()
        for row in proposal_rows:
            tid = int(row["id"])
            if tid not in neighbours:
                total_count += 1
                neighbours[tid] = {
                    "id": tid,
                    "text": row["text"],
                    "status": row["status"],
                    "owner": row["owner"],
                    "selection": "assessed_similar",
                }

    # Most useful first, then cut: a task already assessed as similar is
    # the likeliest duplicate or earlier half of this one, and an open
    # sibling matters more than one already finished or dropped. Filling the
    # bound in rule order let a busy meeting's closed siblings crowd both out.
    ordered = sorted(
        neighbours.values(),
        key=lambda item: (
            item["selection"] != "assessed_similar",
            item["status"] != "open",
            item["id"],
        ),
    )
    shown = ordered[:limit]
    truncated = total_count > len(shown)
    result = {
        "items": shown,
        "total_count": total_count,
        "truncated": truncated,
    }
    return result, truncated
