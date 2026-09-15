"""Read-only audit of legacy repository completion results.

The execution worker now refuses an empty deliverable for GitHub work.  This
module identifies the older completed results that predate that invariant so
an operator can review each repository artifact without rewriting append-only
history or mass-posting comments.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

from .task_bootstrap import TaskBootstrapConfigError, _private_database


@dataclass(frozen=True)
class EmptyRepositoryCompletion:
    """One completed GitHub result without a recorded deliverable."""

    result_id: str
    task_id: int
    task_status: str
    origin_kind: str
    origin_record_id: str
    origin_item_id: str
    phase: str
    created_at: str

    def document(self) -> dict[str, object]:
        return {
            "result_id": self.result_id,
            "task_id": self.task_id,
            "task_status": self.task_status,
            "origin": {
                "kind": self.origin_kind,
                "record_id": self.origin_record_id,
                "item_id": self.origin_item_id,
            },
            "phase": self.phase,
            "created_at": self.created_at,
        }


def find_empty_repository_completions(
    *, database_path: Path,
) -> tuple[EmptyRepositoryCompletion, ...]:
    """Return completed GitHub results lacking a visible deliverable.

    Only completed execute/external-action results are included. Empty plan
    result collections may be ordinary historical review state and are not a
    public-delivery claim.
    """
    database = _private_database(database_path)
    try:
        with closing(sqlite3.connect(database)) as connection:
            connection.row_factory = sqlite3.Row
            rows = connection.execute(
                "SELECT r.result_id,r.task_id,t.status AS task_status,"
                "c.source_kind,c.source_record_id,c.source_item_id,"
                "r.phase,r.created_at "
                "FROM task_execution_results AS r "
                "JOIN tasks AS t ON t.id=r.task_id "
                "JOIN task_candidate_bindings AS b "
                "ON b.task_id=r.task_id AND b.relation='accepted' "
                "JOIN candidate_inbox AS c ON c.candidate_id=b.candidate_id "
                "WHERE r.deliverables_json='[]' "
                "AND r.outcome='completed' "
                "AND r.phase IN ('execute','external_action') "
                "AND c.source_kind IN ('issue','review_request') "
                "AND c.source_record_id GLOB 'github.com/*/*' "
                "ORDER BY r.created_at,r.result_id"
            ).fetchall()
    except sqlite3.Error as exc:
        raise ExecutionAuditError("execution audit is unavailable") from exc
    return tuple(
        EmptyRepositoryCompletion(
            result_id=str(row["result_id"]),
            task_id=int(row["task_id"]),
            task_status=str(row["task_status"]),
            origin_kind=str(row["source_kind"]),
            origin_record_id=str(row["source_record_id"]),
            origin_item_id=str(row["source_item_id"]),
            phase=str(row["phase"]),
            created_at=str(row["created_at"]),
        )
        for row in rows
    )


class ExecutionAuditError(RuntimeError):
    """The private execution history could not be read safely."""


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="foxhound-execution-audit",
        description="List legacy completed repository results without deliverables",
    )
    parser.add_argument("--database", required=True, type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        findings = find_empty_repository_completions(
            database_path=args.database,
        )
    except (ExecutionAuditError, TaskBootstrapConfigError, OSError, ValueError):
        print("foxhound execution audit: audit unavailable", file=sys.stderr)
        return 70
    print(json.dumps({
        "ok": True,
        "count": len(findings),
        "findings": [finding.document() for finding in findings],
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
