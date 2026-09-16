"""One bounded pass that makes duplicate proposals deliverable to a reader."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from .task_bootstrap import TaskBootstrapConfigError, _private_database
from .task_cards import CardDisposition, ScheduleResult, TaskCardService
from .task_ledger import TaskLedgerError


def schedule_duplicate_cards(
    *, database_path: Path, limit: int = 100
) -> ScheduleResult:
    database = _private_database(database_path)
    return TaskCardService(database).schedule_duplicate_proposals(limit=limit)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="foxhound-task-duplicate-card-schedule",
        description="Schedule bounded duplicate review cards",
    )
    parser.add_argument("--database", required=True, type=Path)
    parser.add_argument("--limit", default=100, type=int)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    arguments = _parser().parse_args(argv)
    try:
        result = schedule_duplicate_cards(
            database_path=arguments.database, limit=arguments.limit,
        )
    except (TaskBootstrapConfigError, TaskLedgerError, OSError, ValueError):
        print("foxhound duplicate card schedule: operation failed", file=sys.stderr)
        return 70
    print(json.dumps({
        "ok": result.disposition is not CardDisposition.REFUSED,
        "disposition": result.disposition.value,
        "created": result.created,
        "cancelled": result.cancelled,
        "asked": result.asked,
        "refusal": None if result.refusal is None else result.refusal.value,
    }, sort_keys=True))
    return 0 if result.disposition is not CardDisposition.REFUSED else 1


if __name__ == "__main__":
    raise SystemExit(main())
