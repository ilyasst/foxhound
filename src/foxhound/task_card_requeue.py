"""One-shot hourly re-presentation of unanswered task review cards."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from .task_bootstrap import TaskBootstrapConfigError, _private_database
from .task_cards import TaskCardService
from .task_ledger import TaskLedgerError


def run_requeue(*, database_path: Path, limit: int = 100) -> int:
    database = _private_database(database_path)
    return TaskCardService(database).requeue_unanswered(limit=limit).requeued


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="foxhound-task-card-requeue",
        description="Re-present unanswered Foxhound task review cards",
    )
    parser.add_argument("--database", required=True, type=Path)
    parser.add_argument("--limit", default=100, type=int)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        requeued = run_requeue(database_path=args.database, limit=args.limit)
    except (TaskBootstrapConfigError, TaskLedgerError, OSError, ValueError):
        print("foxhound task-card requeue: operation failed", file=sys.stderr)
        return 70
    print(json.dumps({"ok": True, "requeued": requeued}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
