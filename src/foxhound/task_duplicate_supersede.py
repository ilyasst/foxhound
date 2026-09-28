"""Cancel unanswered duplicate proposals without discarding reader labels."""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path
from typing import Sequence

from .task_bootstrap import TaskBootstrapConfigError, _private_database
from .task_cards import DuplicateSupersedeResult, TaskCardService
from .task_duplicate_proposals import DuplicateProposalError
from .task_ledger import TaskLedgerError


def supersede_duplicate_proposals(
    *,
    database_path: Path,
    detector: str | None = None,
    apply: bool = False,
) -> DuplicateSupersedeResult:
    """Run one content-free dry-run or cancellation transaction."""
    database = _private_database(database_path)
    return TaskCardService(database).supersede_duplicate_proposals(
        detector=detector,
        apply=apply,
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="foxhound-task-duplicate-supersede",
        description="Supersede unanswered duplicate proposals",
    )
    parser.add_argument("--database", required=True, type=Path)
    parser.add_argument("--detector")
    parser.add_argument(
        "--apply",
        action="store_true",
        help="write changes (the default is a dry-run)",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    arguments = _parser().parse_args(argv)
    try:
        result = supersede_duplicate_proposals(
            database_path=arguments.database,
            detector=arguments.detector,
            apply=arguments.apply,
        )
    except (
        DuplicateProposalError,
        TaskBootstrapConfigError,
        TaskLedgerError,
        OSError,
        sqlite3.Error,
        ValueError,
    ):
        print(
            json.dumps({"accepted": False}, separators=(",", ":")),
            file=sys.stdout,
        )
        return 2
    print(json.dumps({
        "accepted": True,
        "applied": result.applied,
        "cards_cancelled": result.cards_cancelled,
        "cards_matched": result.cards_matched,
        "matched": result.matched,
        "superseded": result.superseded,
    }, sort_keys=True))
    return 0


if __name__ == "__main__":  # pragma: no cover - module entry point
    raise SystemExit(main())
