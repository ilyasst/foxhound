"""Explicit producer-independent activation and candidate intake."""

from __future__ import annotations

import argparse
import json
import stat
import sys
from pathlib import Path
from typing import Sequence

from .task_ledger import (
    HistoricalRefusalResult,
    NativeIntakeActivationResult,
    NativeIntakeResult,
    TaskLedger,
    TaskLedgerError,
)


class NativeIntakeConfigError(RuntimeError):
    """The native-intake state path is unsafe or unavailable."""


def activate(
    *,
    database_path: Path,
    producer: str,
    stream_id: str,
    expected_cursor: int,
) -> NativeIntakeActivationResult:
    database = _private_database(database_path)
    return TaskLedger(database).activate_native_intake(
        producer=producer,
        stream_id=stream_id,
        expected_cursor=expected_cursor,
    )


def run(
    *,
    database_path: Path,
    producer: str,
    stream_id: str,
    limit: int,
) -> NativeIntakeResult:
    database = _private_database(database_path)
    return TaskLedger(database).accept_native_candidates(
        producer=producer,
        stream_id=stream_id,
        limit=limit,
    )


def refuse_divergent(
    *,
    database_path: Path,
    producer: str,
    stream_id: str,
    expected_count: int,
    reason_code: str,
) -> HistoricalRefusalResult:
    database = _private_database(database_path)
    return TaskLedger(database).refuse_divergent_history(
        producer=producer,
        stream_id=stream_id,
        expected_count=expected_count,
        reason_code=reason_code,
    )


def _private_database(path: Path) -> Path:
    if not isinstance(path, Path) or not path.is_absolute():
        raise NativeIntakeConfigError("native intake database path is invalid")
    try:
        if path.resolve(strict=True) != path:
            raise NativeIntakeConfigError(
                "native intake database path is invalid"
            )
        parent = path.parent.stat()
        info = path.lstat()
    except OSError:
        raise NativeIntakeConfigError(
            "native intake database is unavailable"
        ) from None
    if (
        not stat.S_ISDIR(parent.st_mode)
        or parent.st_mode & 0o077
        or not stat.S_ISREG(info.st_mode)
        or info.st_mode & 0o077
    ):
        raise NativeIntakeConfigError("native intake database is not private")
    return path


def _activation_document(
    result: NativeIntakeActivationResult,
) -> dict[str, object]:
    return {
        "ok": result.accepted,
        "disposition": result.disposition.value,
        "activation_cursor": result.activation_cursor,
        "refusal": None if result.refusal is None else result.refusal.value,
    }


def _run_document(result: NativeIntakeResult) -> dict[str, object]:
    return {
        "ok": result.accepted,
        "disposition": result.disposition.value,
        "previous_cursor": result.previous_cursor,
        "current_cursor": result.current_cursor,
        "remaining": result.remaining,
        # Every disposition a candidate can reach, so a pass that moved the
        # cursor without folding anything in says why. `candidates_withdrawn`
        # was already counted and never reported, and a revision the reader's
        # decision overtook is the case most worth seeing: the producer
        # changed a task and the change was deliberately not applied.
        "counts": {
            "tasks_created": result.tasks_created,
            "tasks_revised": result.tasks_revised,
            "candidates_unchanged": result.candidates_unchanged,
            "candidates_withdrawn": result.candidates_withdrawn,
            "candidates_after_close": result.candidates_after_close,
        },
        "refusal": None if result.refusal is None else result.refusal.value,
    }


def _historical_refusal_document(
    result: HistoricalRefusalResult,
) -> dict[str, object]:
    return {
        "ok": result.accepted,
        "disposition": result.disposition.value,
        "candidates_matched": result.candidates_matched,
        "refusals_recorded": result.refusals_recorded,
        "refusals_unchanged": result.refusals_unchanged,
        "refusal": None if result.refusal is None else result.refusal.value,
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="foxhound-native-intake",
        description="Activate or run ordered Foxhound candidate intake",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    for name in ("refuse-divergent", "activate", "run"):
        command = subparsers.add_parser(name)
        command.add_argument("--database", required=True, type=Path)
        command.add_argument("--producer", default="gw")
        command.add_argument("--stream-id", required=True)
        if name == "refuse-divergent":
            command.add_argument("--expected-count", required=True, type=int)
            command.add_argument(
                "--reason",
                required=True,
                choices=("preserved_legacy_owner",),
            )
        elif name == "activate":
            command.add_argument("--expected-cursor", required=True, type=int)
        else:
            command.add_argument("--limit", default=100, type=int)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    arguments = _parser().parse_args(argv)
    try:
        if arguments.command == "refuse-divergent":
            result = refuse_divergent(
                database_path=arguments.database,
                producer=arguments.producer,
                stream_id=arguments.stream_id,
                expected_count=arguments.expected_count,
                reason_code=arguments.reason,
            )
            document = _historical_refusal_document(result)
        elif arguments.command == "activate":
            result = activate(
                database_path=arguments.database,
                producer=arguments.producer,
                stream_id=arguments.stream_id,
                expected_cursor=arguments.expected_cursor,
            )
            document = _activation_document(result)
        else:
            result = run(
                database_path=arguments.database,
                producer=arguments.producer,
                stream_id=arguments.stream_id,
                limit=arguments.limit,
            )
            document = _run_document(result)
    except NativeIntakeConfigError:
        print(
            "foxhound native intake: configuration unavailable",
            file=sys.stderr,
        )
        return 78
    except (TaskLedgerError, OSError):
        print("foxhound native intake: operation failed", file=sys.stderr)
        return 70
    except Exception:
        print("foxhound native intake: operation failed", file=sys.stderr)
        return 70
    print(json.dumps(document, sort_keys=True))
    return 0 if result.accepted else 1


if __name__ == "__main__":
    raise SystemExit(main())
