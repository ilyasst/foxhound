"""Explicit one-shot scheduling of new Foxhound execution workflows."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

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
    default_agent_profile: str = "sigint",
    plan_without_asking: Sequence[str] | None = None,
    plan_ready_cap: int | None = None,
    awaiting_reader_cap: int | None = None,
    reader_aliases: Sequence[str] | None = None,
) -> ExecutionScheduleResult:
    database = _private_database(database_path)
    registry = load_registry(agent_profile_directory)
    # The production scheduler must never silently run repository work on the
    # compatibility profile when SigInt is missing or disabled.
    if agent_profile_directory is not None and default_agent_profile == "sigint":
        sigint = registry.get("sigint")
        if sigint is None or any(
            phase.value not in sigint.allowed_phases for phase in WorkflowPhase
        ):
            raise AgentProfileError(
                "the installed SigInt profile is unavailable"
            )
    # A library/test invocation without the private catalog can only use the
    # built-in compatibility profile. Production invocations pass the
    # catalog directory and are rejected above when SigInt is unavailable.
    selected_profile = default_agent_profile
    if agent_profile_directory is None and selected_profile == "sigint":
        selected_profile = "general"
    return TaskExecutionService(
        database,
        profile_registry=registry,
        default_profile_id=selected_profile,
        planning_grants=plan_without_asking,
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
    parser.add_argument("--default-agent-profile", default="sigint")
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
            plan_without_asking=args.plan_without_asking,
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


if __name__ == "__main__":
    raise SystemExit(main())
