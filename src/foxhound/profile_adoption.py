"""Explicit one-shot adoption of an agent profile's installed revision.

A workflow keeps the profile revision it was scheduled under for life, and a
revision fixes the timeout and turn limit (ADR 0024). Raising a budget
therefore changes nothing for work already queued: it keeps the budget it was
scheduled under, and no card, log or readiness report used to say so.

This is the explicit trigger that moves it. An operator raises a budget, sees
the superseded count in `foxhound-delivery-health`, and runs this. The ledger
then records that an operator did it, which is the property an automatic
scheduler pass would not have — see ADR 0024's amendment for why that
distinction was kept.

Dry run by default. The count an operator acts on and the act itself should
not be the same keystroke.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from .agent_profiles import AgentProfileError, load_registry
from .task_bootstrap import TaskBootstrapConfigError, _private_database
from .task_execution import ProfileAdoptionResult, TaskExecutionService
from .task_ledger import TaskLedgerError


def run_adoption(
    *,
    database_path: Path,
    limit: int = 100,
    apply: bool = False,
    agent_profile_directory: Path | None = None,
    default_agent_profile: str = "general",
) -> ProfileAdoptionResult:
    """Adopt installed revisions for eligible workflows, bounded by *limit*."""
    registry = load_registry(agent_profile_directory)
    service = TaskExecutionService(
        _private_database(database_path),
        profile_registry=registry,
        default_profile_id=default_agent_profile,
    )
    return service.adopt_installed_revisions(limit=limit, dry_run=not apply)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Rebind workflows pinned to a retired profile revision onto the "
            "installed revision of the same profile. Never changes which "
            "profile a workflow names, and never touches a running one."
        ),
    )
    parser.add_argument("--database", type=Path, required=True)
    parser.add_argument(
        "--limit", type=int, default=100,
        help=(
            "Maximum workflows to rebind in one pass. Bounded because "
            "rebinding retires a card for every workflow it touches."
        ),
    )
    parser.add_argument(
        "--apply", action="store_true",
        help="Write the change. Without it the pass reports and rolls back.",
    )
    parser.add_argument("--agent-profile-directory", type=Path, default=None)
    parser.add_argument("--default-agent-profile", default="general")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        result = run_adoption(
            database_path=args.database,
            limit=args.limit,
            apply=args.apply,
            agent_profile_directory=args.agent_profile_directory,
            default_agent_profile=args.default_agent_profile,
        )
    except (AgentProfileError, TaskBootstrapConfigError, ValueError):
        print(
            "foxhound profile adoption: configuration unavailable",
            file=sys.stderr,
        )
        return 78
    except (TaskLedgerError, OSError):
        print("foxhound profile adoption: adoption failed", file=sys.stderr)
        return 70
    except Exception:
        print("foxhound profile adoption: adoption failed", file=sys.stderr)
        return 70

    if result.narrowed:
        # Adoption is allowed to reduce a budget -- the installed revision is
        # by definition the one the operator chose -- but a workflow losing
        # time because its profile was narrowed is a decision, not a detail.
        # Count only; naming a task here would put ledger content in a log.
        print(
            "foxhound profile adoption: "
            f"{result.narrowed} workflow(s) moved to a smaller budget",
            file=sys.stderr,
        )
    print(json.dumps({
        "ok": True,
        "adopted": result.adopted,
        "dry_run": result.dry_run,
        "examined": result.examined,
        "narrowed": result.narrowed,
        "remaining": result.remaining,
        "skipped_running": result.skipped_running,
        "unknown_budget": result.unknown_budget,
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
