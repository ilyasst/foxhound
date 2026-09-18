"""Content-free visibility into the pre-structure task backlog."""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path
from typing import Sequence

from .candidate_inbox import CandidateInbox, InboxError


def report(database_path: str | Path) -> dict[str, int]:
    """Count tasks with and without all new required structured fields."""
    inbox = CandidateInbox(database_path)
    if not inbox.database_path.is_file() or inbox.database_path.is_symlink():
        raise InboxError("candidate inbox is not initialized")
    connection = sqlite3.connect(inbox.database_path, timeout=5)
    connection.row_factory = sqlite3.Row
    try:
        inbox._require_current_schema(connection)
        row = connection.execute(
            "SELECT count(*) AS total,"
            "sum(CASE WHEN object IS NOT NULL AND action IS NOT NULL "
            "AND confidence IS NOT NULL THEN 1 ELSE 0 END) AS structured "
            "FROM tasks"
        ).fetchone()
    finally:
        connection.close()
    total = int(row[0])
    structured = int(row[1] or 0)
    return {"structured_tasks": structured, "unstructured_tasks": total - structured}


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="foxhound-task-structure-status",
        description="Report structured-task backlog counts",
    )
    parser.add_argument("--database", required=True, type=Path)
    arguments = parser.parse_args(argv)
    try:
        result = report(arguments.database)
    except (InboxError, sqlite3.Error, ValueError):
        print(json.dumps({"accepted": False}, separators=(",", ":")))
        return 2
    print(json.dumps({"accepted": True, **result}, sort_keys=True))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
