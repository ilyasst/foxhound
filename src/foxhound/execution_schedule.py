"""Explicit one-shot scheduling of new Foxhound execution workflows."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from .task_bootstrap import TaskBootstrapConfigError, _private_database
from .task_execution import ExecutionScheduleResult, TaskExecutionService
from .task_ledger import TaskLedgerError


def run_schedule(
    *, database_path: Path, limit: int = 100
) -> ExecutionScheduleResult:
    database = _private_database(database_path)
    return TaskExecutionService(database).schedule_new(limit=limit)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="foxhound-execution-schedule",
        description="Schedule new open tasks behind the execution Start gate",
    )
    parser.add_argument("--database", required=True, type=Path)
    parser.add_argument("--limit", default=100, type=int)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        result = run_schedule(database_path=args.database, limit=args.limit)
    except (TaskBootstrapConfigError, ValueError):
        print(
            "foxhound execution schedule: configuration unavailable",
            file=sys.stderr,
        )
        return 78
    except (TaskLedgerError, OSError):
        print("foxhound execution schedule: scheduling failed", file=sys.stderr)
        return 70
    except Exception:
        print("foxhound execution schedule: scheduling failed", file=sys.stderr)
        return 70
    print(json.dumps({
        "ok": True,
        "remaining": result.remaining,
        "scheduled": result.scheduled,
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
