"""Recover an unacknowledged execution review card delivery."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from .execution_cards import ExecutionCardService
from .task_bootstrap import TaskBootstrapConfigError, _private_database
from .task_ledger import TaskLedgerError


def run_recover(
    *, database_path: Path, card_id: int, expected_version: int
) -> dict[str, bool | str]:
    database = _private_database(database_path)
    result = ExecutionCardService(database).recover_delivery(
        card_id, expected_version=expected_version
    )
    return {
        "ok": result.accepted,
        "refusal": result.refusal.value if result.refusal else None,
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="foxhound-execution-card-recover",
        description="Recover an unacknowledged Foxhound execution card delivery",
    )
    parser.add_argument("--database", required=True, type=Path)
    parser.add_argument("--card-id", required=True, type=int)
    parser.add_argument("--version", required=True, type=int)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        outcome = run_recover(
            database_path=args.database,
            card_id=args.card_id,
            expected_version=args.version,
        )
    except (TaskBootstrapConfigError, TaskLedgerError, OSError, ValueError):
        print("foxhound execution card recover: operation failed", file=sys.stderr)
        return 70
    print(json.dumps(outcome, sort_keys=True))
    if not outcome.get("ok"):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
