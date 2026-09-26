"""Create the review cards that workflows waiting at a gate are owed.

Separate from delivery on purpose. Scheduling used to happen only as a side
effect of a chat surface topping itself up, so a card existed only when that
surface had room for one: ten delivered and unanswered against a surface of
ten meant no scheduling call for two days, 37 workflows waiting at a gate with
no card, and a console reader — who has no per-surface limit — able to answer
none of them. How many cards fit on a surface is the deliverer's business;
whether a workflow waiting for a decision has one is this service's.

Idempotent by construction rather than by a lock: the scheduling query skips
any workflow that already has a live card, so this and a delivering side can
both ask at once without creating anything twice.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from .execution_cards import ExecutionCardService
from .task_bootstrap import TaskBootstrapConfigError, _private_database
from .task_ledger import TaskLedgerError


def run_schedule(*, database_path: Path, limit: int = 100) -> tuple[int, int]:
    """(created, cancelled) for one pass."""
    database = _private_database(database_path)
    result = ExecutionCardService(database).schedule(limit=limit)
    return result.created, result.cancelled


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="foxhound-execution-card-schedule",
        description="Schedule Foxhound execution review cards for waiting gates",
    )
    parser.add_argument("--database", required=True, type=Path)
    parser.add_argument("--limit", default=100, type=int)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        created, cancelled = run_schedule(
            database_path=args.database, limit=args.limit
        )
    except (TaskBootstrapConfigError, TaskLedgerError, OSError, ValueError):
        print(
            "foxhound execution card schedule: operation failed", file=sys.stderr
        )
        return 70
    # Counts only: a card's content never reaches a log.
    print(json.dumps(
        {"ok": True, "created": created, "cancelled": cancelled}, sort_keys=True
    ))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
