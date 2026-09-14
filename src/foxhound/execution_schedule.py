"""Explicit one-shot scheduling of new Foxhound execution workflows."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from .agent_profiles import AgentProfileError, load_registry
from .task_bootstrap import TaskBootstrapConfigError, _private_database
from .task_execution import ExecutionScheduleResult, TaskExecutionService
from .task_ledger import TaskLedgerError


def run_schedule(
    *,
    database_path: Path,
    limit: int = 100,
    agent_profile_directory: Path | None = None,
    default_agent_profile: str = "general",
) -> ExecutionScheduleResult:
    database = _private_database(database_path)
    registry = load_registry(agent_profile_directory)
    return TaskExecutionService(
        database,
        profile_registry=registry,
        default_profile_id=default_agent_profile,
    ).schedule_new(limit=limit)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="foxhound-execution-schedule",
        description="Schedule new open tasks behind the execution Start gate",
    )
    parser.add_argument("--database", required=True, type=Path)
    parser.add_argument("--limit", default=100, type=int)
    parser.add_argument("--agent-profile-directory", type=Path)
    parser.add_argument("--default-agent-profile", default="general")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        result = run_schedule(
            database_path=args.database,
            limit=args.limit,
            agent_profile_directory=args.agent_profile_directory,
            default_agent_profile=args.default_agent_profile,
        )
    except (AgentProfileError, TaskBootstrapConfigError, ValueError):
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
