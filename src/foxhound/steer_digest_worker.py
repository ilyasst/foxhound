"""One-shot best-effort digest pass for current Steer cards."""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

from . import steer_digest
from .execution_cards import ExecutionCardService
from .failure_digest_worker import _read_tail, _transcript_path
from .task_bootstrap import TaskBootstrapConfigError, _private_database
from .task_ledger import TaskLedgerError


@dataclass(frozen=True)
class SteerDigestResult:
    considered: int = 0
    recorded: int = 0
    missing: int = 0
    undigested: int = 0


def run_pass(*, database_path: Path, run_root: Path | Sequence[Path],
             limit: int = 20,
             digester=steer_digest.digest) -> SteerDigestResult:
    service = ExecutionCardService(_private_database(database_path))
    pending = service.steer_cards_awaiting_digest(limit=limit)
    recorded = missing = undigested = 0
    for item in pending:
        found = _transcript_path(run_root, item.run_id)
        transcript = "" if found is None else _read_tail(found)
        if not transcript.strip():
            missing += 1
            continue
        answer = digester(transcript)
        if not answer:
            undigested += 1
        elif service.record_steer_digest(item, answer):
            recorded += 1
        else:
            undigested += 1
    return SteerDigestResult(len(pending), recorded, missing, undigested)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="foxhound-steer-digest")
    parser.add_argument("--database", required=True, type=Path)
    parser.add_argument("--run-root", required=True, type=Path)
    parser.add_argument("--limit", default=20, type=int)
    args = parser.parse_args(argv)
    try:
        result = run_pass(database_path=args.database, run_root=args.run_root,
                          limit=args.limit)
    except (TaskBootstrapConfigError, TaskLedgerError, OSError, ValueError):
        print("foxhound steer digest: unavailable", file=sys.stderr)
        return 70
    print(json.dumps(result.__dict__, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
