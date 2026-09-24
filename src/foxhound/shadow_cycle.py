"""One ordered, overlap-safe Foxhound shadow import cycle."""

from __future__ import annotations

import argparse
import fcntl
import json
import os
import sqlite3
import stat
import sys
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Iterator, Sequence

from . import candidate_feed_import, task_shadow_feed_import
from .candidate_inbox import (
    CandidateInbox,
    InboxError,
    ShadowComparisonReport,
    ShadowImportCycleReceipt,
)


_LOCK_NAME = ".foxhound-shadow-cycle.lock"


class ShadowCycleError(RuntimeError):
    """A complete shadow cycle cannot be run safely."""


class ShadowCycleAbsentOutboxError(ShadowCycleError):
    """An outbox directory has not been created yet."""


class ShadowCycleUnreadableOutboxError(ShadowCycleError):
    """An outbox directory exists but cannot be read."""


@dataclass(frozen=True)
class ShadowCycleResult:
    receipt_sequence: int
    candidate_disposition: str
    candidate_previous_cursor: int
    candidate_current_cursor: int
    candidate_pages_applied: int
    candidates_inserted: int
    candidates_updated: int
    observation_disposition: str
    observation_previous_cursor: int
    observation_current_cursor: int
    observation_pages_applied: int
    observations_inserted: int
    comparison: ShadowComparisonReport


def run_cycle(
    *,
    candidate_outbox_dir: Path,
    observation_outbox_dir: Path,
    database_path: Path,
    stream_id: str,
    clock: Callable[[], datetime] | None = None,
) -> ShadowCycleResult:
    """Import candidates, then observations, and receipt only full success."""
    candidate_outbox, observation_outbox, database = _validate_locations(
        candidate_outbox_dir,
        observation_outbox_dir,
        database_path,
    )
    now = clock or (lambda: datetime.now(timezone.utc))
    lock_path = database.parent / _LOCK_NAME

    with _cycle_lock(lock_path):
        started_at = _timestamp(now())
        try:
            candidates = candidate_feed_import.import_outbox(
                outbox_dir=candidate_outbox,
                database_path=database,
                stream_id=stream_id,
            )
            observations = task_shadow_feed_import.import_outbox(
                outbox_dir=observation_outbox,
                database_path=database,
                stream_id=stream_id,
            )
            completed_at = _timestamp(now())
            receipt = ShadowImportCycleReceipt(
                stream_id=stream_id,
                started_at=started_at,
                completed_at=completed_at,
                candidate_previous_cursor=candidates.previous_cursor,
                candidate_current_cursor=candidates.current_cursor,
                candidates_inserted=candidates.candidates_inserted,
                candidates_updated=candidates.candidates_updated,
                observation_previous_cursor=observations.previous_cursor,
                observation_current_cursor=observations.current_cursor,
                observations_inserted=observations.observations_inserted,
                comparison=observations.comparison,
            )
            receipt_sequence = CandidateInbox(
                database
            ).append_shadow_import_cycle(receipt)
        except (
            candidate_feed_import.CandidateFeedImportError,
            task_shadow_feed_import.TaskShadowFeedImportError,
            InboxError,
            OSError,
            sqlite3.Error,
        ):
            raise ShadowCycleError("Foxhound shadow cycle failed") from None

    return ShadowCycleResult(
        receipt_sequence=receipt_sequence,
        candidate_disposition=str(candidates.disposition),
        candidate_previous_cursor=candidates.previous_cursor,
        candidate_current_cursor=candidates.current_cursor,
        candidate_pages_applied=candidates.pages_applied,
        candidates_inserted=candidates.candidates_inserted,
        candidates_updated=candidates.candidates_updated,
        observation_disposition=str(observations.disposition),
        observation_previous_cursor=observations.previous_cursor,
        observation_current_cursor=observations.current_cursor,
        observation_pages_applied=observations.pages_applied,
        observations_inserted=observations.observations_inserted,
        comparison=observations.comparison,
    )


def _check_outbox_accessible(path: Path, label: str) -> None:
    """Raise a distinct error for absent vs unreadable outbox directories."""
    if not path.exists():
        raise ShadowCycleAbsentOutboxError(
            f"{label} has not been created yet"
        )
    try:
        path.iterdir()
    except PermissionError:
        raise ShadowCycleUnreadableOutboxError(
            f"{label} cannot be read"
        )
    except OSError:
        raise ShadowCycleUnreadableOutboxError(
            f"{label} is unreadable"
        )


def _validate_locations(
    candidate_outbox_dir: Path,
    observation_outbox_dir: Path,
    database_path: Path,
) -> tuple[Path, Path, Path]:
    _check_outbox_accessible(candidate_outbox_dir, "Candidate outbox")
    _check_outbox_accessible(observation_outbox_dir, "Observation outbox")
    try:
        candidate_outbox = candidate_feed_import._require_private_directory(
            Path(candidate_outbox_dir), "candidate outbox"
        )
        observation_outbox = candidate_feed_import._require_private_directory(
            Path(observation_outbox_dir), "observation outbox"
        )
        database = candidate_feed_import._require_private_database(
            Path(database_path)
        )
    except (candidate_feed_import.CandidateFeedImportError, OSError):
        raise ShadowCycleError("Foxhound shadow cycle locations are unsafe") from None
    if candidate_outbox == observation_outbox:
        raise ShadowCycleError("Foxhound shadow cycle outboxes must be distinct")
    if (database == candidate_outbox or database in candidate_outbox.parents
            or database == observation_outbox
            or database in observation_outbox.parents
            or candidate_outbox in database.parents
            or observation_outbox in database.parents):
        raise ShadowCycleError(
            "Foxhound state must be outside producer outboxes"
        )
    return candidate_outbox, observation_outbox, database


@contextmanager
def _cycle_lock(path: Path) -> Iterator[None]:
    flags = os.O_CREAT | os.O_RDWR
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(path, flags, 0o600)
    except OSError:
        raise ShadowCycleError("Foxhound shadow cycle lock is unsafe") from None
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode):
            raise ShadowCycleError("Foxhound shadow cycle lock is unsafe")
        os.fchmod(descriptor, 0o600)
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise ShadowCycleError(
                "another Foxhound shadow cycle is already running"
            ) from None
        except OSError:
            raise ShadowCycleError(
                "Foxhound shadow cycle lock cannot be acquired"
            ) from None
        yield
    finally:
        os.close(descriptor)


def _timestamp(value: datetime) -> str:
    if (not isinstance(value, datetime) or value.tzinfo is None
            or value.utcoffset() is None):
        raise ShadowCycleError("Foxhound shadow cycle clock is invalid")
    return value.isoformat(timespec="seconds")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run one ordered Foxhound shadow import cycle."
    )
    parser.add_argument("--candidate-outbox", type=Path, required=True)
    parser.add_argument("--observation-outbox", type=Path, required=True)
    parser.add_argument("--database", type=Path, required=True)
    parser.add_argument("--stream-id", required=True)
    return parser


def _result_document(result: ShadowCycleResult) -> dict[str, object]:
    return {
        "candidate": {
            "current_cursor": result.candidate_current_cursor,
            "disposition": result.candidate_disposition,
            "inserted": result.candidates_inserted,
            "pages_applied": result.candidate_pages_applied,
            "previous_cursor": result.candidate_previous_cursor,
            "updated": result.candidates_updated,
        },
        "comparison": {
            "agreed": result.comparison.agreed,
            "divergent": result.comparison.divergent,
            "refused": result.comparison.refused,
            "total": result.comparison.total,
            "unmapped": result.comparison.unmapped,
        },
        "observation": {
            "current_cursor": result.observation_current_cursor,
            "disposition": result.observation_disposition,
            "inserted": result.observations_inserted,
            "pages_applied": result.observation_pages_applied,
            "previous_cursor": result.observation_previous_cursor,
        },
        "receipt_sequence": result.receipt_sequence,
    }


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        result = run_cycle(
            candidate_outbox_dir=args.candidate_outbox,
            observation_outbox_dir=args.observation_outbox,
            database_path=args.database,
            stream_id=args.stream_id,
        )
    except ShadowCycleAbsentOutboxError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    except ShadowCycleUnreadableOutboxError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    except ShadowCycleError:
        print("Foxhound shadow cycle failed", file=sys.stderr)
        return 1
    print(json.dumps(_result_document(result), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
