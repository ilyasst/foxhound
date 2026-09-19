"""Explicit one-shot scheduling of new Foxhound execution workflows."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Mapping, Sequence

from .agent_profiles import AgentProfileError, load_registry
from .task_bootstrap import TaskBootstrapConfigError, _private_database
from .task_execution import (
    ExecutionScheduleResult,
    TaskExecutionService,
    WorkflowPhase,
)
from .task_ledger import TaskLedgerError


def run_schedule(
    *,
    database_path: Path,
    limit: int = 100,
    agent_profile_directory: Path | None = None,
    default_agent_profile: str = "general",
    profile_routes: Mapping[str, str] | None = None,
    plan_without_asking: Sequence[str] | None = None,
    skip_planning_for: Sequence[str] | None = None,
    plan_ready_cap: int | None = None,
    awaiting_reader_cap: int | None = None,
    reader_aliases: Sequence[str] | None = None,
) -> ExecutionScheduleResult:
    database = _private_database(database_path)
    registry = load_registry(agent_profile_directory)
    return TaskExecutionService(
        database,
        profile_registry=registry,
        default_profile_id=default_agent_profile,
        profile_routes=profile_routes,
        planning_grants=plan_without_asking,
        skip_planning_for=skip_planning_for,
        plan_ready_cap=plan_ready_cap,
        awaiting_reader_cap=awaiting_reader_cap,
        reader_aliases=reader_aliases,
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
    parser.add_argument(
        "--profile-route", action="append", metavar="SOURCE_KIND=PROFILE",
        help="route one source kind to an installed profile; repeat as needed",
    )
    parser.add_argument(
        "--plan-without-asking",
        action="append",
        metavar="SOURCE_KIND",
        help=(
            "let this machine plan tasks from SOURCE_KIND without asking "
            "first; repeat for each kind. Omitted means every task is "
            "asked about, which is the default and the cautious answer."
        ),
    )
    parser.add_argument(
        "--skip-planning-for",
        action="append",
        metavar="SOURCE_KIND",
        help=(
            "begin new tasks from SOURCE_KIND at execute; repeat for each "
            "kind. Each kind also needs --execute-without-asking in the "
            "deployment configuration. Omitted retains the plan phase."
        ),
    )
    parser.add_argument(
        "--reader-alias",
        action="append",
        metavar="NAME",
        help=(
            "a name this machine's reader is known by; repeat for each. A "
            "task whose confirmed owner matches one is planned without "
            "asking, whatever its source. Omitted means ownership never "
            "admits a task, which is the cautious answer and the default."
        ),
    )
    parser.add_argument(
        "--plan-ready-cap",
        type=int,
        default=None,
        help=(
            "how many workflows may sit ready to execute at once; -1 means "
            "no cap. Omitted keeps this machine's compiled-in default."
        ),
    )
    parser.add_argument(
        "--awaiting-reader-cap",
        type=int,
        default=None,
        help=(
            "how many workflows may wait on an operator decision at once; "
            "-1 means no cap. Omitted keeps this machine's compiled-in "
            "default."
        ),
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        result = run_schedule(
            database_path=args.database,
            limit=args.limit,
            agent_profile_directory=args.agent_profile_directory,
            default_agent_profile=args.default_agent_profile,
            profile_routes=_profile_routes(args.profile_route),
            plan_without_asking=args.plan_without_asking,
            skip_planning_for=args.skip_planning_for,
            plan_ready_cap=args.plan_ready_cap,
            awaiting_reader_cap=args.awaiting_reader_cap,
            reader_aliases=args.reader_alias,
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
    if result.capped:
        # A saturated cap is not a failure -- the exit status stays 0 so a
        # supervisor does not mark a healthy timer failed every pass -- but it
        # must not look like an idle one either. Admission has stopped, and
        # the only prior evidence was a count that also means "nothing to do".
        # Count only; naming a task here would put ledger content in a log.
        print(
            "foxhound execution schedule: "
            f"{result.capped} eligible task(s) held by a capacity cap",
            file=sys.stderr,
        )
    print(json.dumps({
        "ok": True,
        "capped": result.capped,
        "remaining": result.remaining,
        "scheduled": result.scheduled,
    }, sort_keys=True))
    return 0


def _profile_routes(values: Sequence[str] | None) -> dict[str, str]:
    result: dict[str, str] = {}
    for value in values or ():
        kind, separator, profile_id = value.partition("=")
        if not separator or not kind or not profile_id or kind in result:
            raise ValueError("agent profile routes are invalid")
        result[kind] = profile_id
    return result


if __name__ == "__main__":
    raise SystemExit(main())
