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
        rows = proposals.counts(connection)
    finally:
        connection.close()
    return tuple(_line(row) for row in rows)


def _line(counts: proposals.ProposalCounts) -> dict[str, object]:
    settled = counts.confirmed + counts.rejected
    return {
        "detector": counts.detector,
        "proposed": counts.proposed,
        "confirmed": counts.confirmed,
        "rejected": counts.rejected,
        "reopened": counts.reopened,
        "awaiting": max(counts.proposed - settled, 0),
        # None until the reader has answered something: a rate over zero
        # answers would read as a score rather than as an absence of evidence.
        "confirm_rate": None if settled == 0 else round(counts.confirmed / settled, 3),
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
    except (InboxError, sqlite3.Error, ValueError):
        # The path and the exception can both be operational data.
        print(json.dumps({"accepted": False}, separators=(",", ":")))
        return 2
    print(json.dumps({"accepted": True, "detectors": list(lines)},
                     sort_keys=True))
    return 0


if __name__ == "__main__":  # pragma: no cover - module entry point
    raise SystemExit(main())
