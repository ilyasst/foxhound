"""One-shot pass that explains failed runs from the transcripts they left.

Held apart from `failure_digest`, which knows how to summarise text and
nothing else. This module knows where a run's transcript lives, which
attempts still need one, and how to record the answer.

Every failure mode here is a skipped row, never a raised error: an absent
run directory, an unreadable transcript, an empty one, a gateway that is
busy or absent. A pass that summarises nothing exits 0 and says how many
it skipped. The one thing this must never do is turn a failed run into a
second failure.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

from . import failure_digest
from .task_archive import TRANSCRIPT_NAME
from .task_bootstrap import TaskBootstrapConfigError, _private_database
from .task_execution import TaskExecutionService
from .task_ledger import TaskLedgerError


#: Read at most this much of a transcript from disk before handing the
#: tail to the summariser. An agent that printed a great deal is exactly
#: the case where reading the whole file would be wasteful, and the
#: summariser only ever looks at the end.
MAX_TRANSCRIPT_BYTES = 256 * 1024


@dataclass(frozen=True)
class FailureDigestResult:
    """Content-free aggregate outcome for one pass."""

    considered: int = 0
    recorded: int = 0
    #: No transcript could be read for the attempt. Normal: run roots are
    #: pruned, and a run that died before opening one leaves nothing.
    missing: int = 0
    #: A transcript was read and the model gave nothing back. Also normal;
    #: the gateway is remote.
    undigested: int = 0


def _read_tail(path: Path) -> str:
    """The end of a transcript, or "" if it cannot be read.

    Reads only the tail from disk rather than the whole file: the failure
    is at the end, and a transcript has no upper bound.
    """
    try:
        size = path.stat().st_size
        with open(path, "rb") as handle:
            if size > MAX_TRANSCRIPT_BYTES:
                handle.seek(size - MAX_TRANSCRIPT_BYTES)
            raw = handle.read(MAX_TRANSCRIPT_BYTES)
    except OSError:
        return ""
    return raw.decode("utf-8", errors="replace")


def _transcript_path(run_root: Path, run_id: str) -> Path:
    """Where the runner put this run's output.

    The layout is the runner's: one directory per run, named for the run
    id, with the transcript inside it under a shared constant so the two
    sides cannot drift.
    """
    return run_root / f"run-{run_id}" / TRANSCRIPT_NAME


def run_pass(
    *,
    database_path: Path,
    run_root: Path,
    limit: int = 20,
    digester=failure_digest.digest,
) -> FailureDigestResult:
    database = _private_database(database_path)
    service = TaskExecutionService(database)
    pending = service.failures_awaiting_digest(limit=limit)
    recorded = missing = undigested = 0
    for item in pending:
        transcript = _read_tail(_transcript_path(run_root, item.run_id))
        if not transcript.strip():
            missing += 1
            continue
        summary = digester(transcript)
        if not summary:
            undigested += 1
            continue
        if service.record_failure_digest(
            item.task_id,
            workflow_version=item.workflow_version,
            phase=item.phase,
            run_id=item.run_id,
            digest=summary,
        ):
            recorded += 1
        else:
            # Another pass got there first, or the reply did not fit the
            # column. Neither is this pass's problem to report as an error.
            undigested += 1
    return FailureDigestResult(
        considered=len(pending),
        recorded=recorded,
        missing=missing,
        undigested=undigested,
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="foxhound-failure-digest",
        description="Explain failed Foxhound runs from their transcripts",
    )
    parser.add_argument("--database", required=True, type=Path)
    parser.add_argument(
        "--run-root",
        required=True,
        type=Path,
        help="the runner's run root, where run-<id> directories live",
    )
    parser.add_argument("--limit", default=20, type=int)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        result = run_pass(
            database_path=args.database,
            run_root=args.run_root,
            limit=args.limit,
        )
    except (TaskBootstrapConfigError, ValueError):
        print("foxhound failure digest: configuration unavailable", file=sys.stderr)
        return 78
    except (TaskLedgerError, OSError):
        print("foxhound failure digest: pass failed", file=sys.stderr)
        return 70
    # Counts only. What an agent said about a task is never log content.
    print(json.dumps({
        "ok": True,
        "considered": result.considered,
        "recorded": result.recorded,
        "missing": result.missing,
        "undigested": result.undigested,
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
