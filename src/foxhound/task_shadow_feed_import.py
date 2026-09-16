"""Manual, read-only import of a producer shadow-observation outbox."""

from __future__ import annotations

import argparse
import fcntl
import json
import os
import re
import sqlite3
import stat
import sys
from contextlib import contextmanager
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any, Iterator

from .candidate_feed_import import (
    CandidateFeedImportError,
    _read_page,
    _require_private_database,
    _require_private_directory,
)
from .candidate_inbox import (
    CandidateInbox,
    InboxError,
    ShadowComparisonReport,
    ShadowFeedImportDisposition,
)
from .contracts import ShadowFeedContractError, parse_task_shadow_feed


_LOCK_NAME = ".task-shadow-feed.lock"
_TEMP_PREFIX = ".task-shadow-feed-tmp-"
_PAGE_RE = re.compile(r"^page-([0-9]{20})-([0-9]{20})\.json$")
_STREAM_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,199}$")


class TaskShadowFeedImportError(RuntimeError):
    """The observation outbox cannot be imported safely."""


class TaskShadowImportDisposition(StrEnum):
    IMPORTED = "imported"
    UNCHANGED = "unchanged"


@dataclass(frozen=True)
class TaskShadowImportResult:
    """Content-free aggregate status for one manual observation import."""

    disposition: TaskShadowImportDisposition
    previous_cursor: int
    current_cursor: int
    pages_seen: int
    pages_applied: int
    pages_replayed: int
    observations_inserted: int
    observations_unchanged: int
    comparison: ShadowComparisonReport


@dataclass(frozen=True)
class _Snapshot:
    pages: tuple[dict[str, Any], ...]
    cursor: int


def import_outbox(
    *,
    outbox_dir: Path,
    database_path: Path,
    stream_id: str,
) -> TaskShadowImportResult:
    """Validate and import one complete observation ledger in page order."""
    expected_stream = _validate_stream_id(stream_id)
    try:
        outbox = _require_private_directory(Path(outbox_dir), "outbox")
        database = _require_private_database(Path(database_path))
    except (CandidateFeedImportError, OSError) as exc:
        raise TaskShadowFeedImportError(
            "observation import location is unsafe"
        ) from exc
    if outbox == database or outbox in database.parents:
        raise TaskShadowFeedImportError(
            "Foxhound state must be outside the producer outbox"
        )

    with _producer_snapshot_lock(outbox):
        snapshot = _read_snapshot(outbox, expected_stream)
        inbox = CandidateInbox(database)
        try:
            previous_cursor = inbox.shadow_feed_cursor("gw", expected_stream)
            if previous_cursor > snapshot.cursor:
                raise TaskShadowFeedImportError(
                    "Foxhound cursor is ahead of the producer ledger"
                )

            applied = 0
            replayed = 0
            inserted = 0
            unchanged = 0
            for document in snapshot.pages:
                result = inbox.import_shadow_feed(document)
                if not result.accepted:
                    raise TaskShadowFeedImportError(
                        "observation page was refused by Foxhound"
                    )
                if result.disposition is ShadowFeedImportDisposition.APPLIED:
                    applied += 1
                elif result.disposition is ShadowFeedImportDisposition.REPLAYED:
                    replayed += 1
                inserted += result.inserted
                unchanged += result.unchanged

            current_cursor = inbox.shadow_feed_cursor("gw", expected_stream)
            comparison = inbox.shadow_report()
        except TaskShadowFeedImportError:
            raise
        except (InboxError, OSError, sqlite3.Error) as exc:
            raise TaskShadowFeedImportError(
                "Foxhound could not apply the observation ledger"
            ) from exc

    disposition = (
        TaskShadowImportDisposition.IMPORTED
        if applied else TaskShadowImportDisposition.UNCHANGED
    )
    return TaskShadowImportResult(
        disposition=disposition,
        previous_cursor=previous_cursor,
        current_cursor=current_cursor,
        pages_seen=len(snapshot.pages),
        pages_applied=applied,
        pages_replayed=replayed,
        observations_inserted=inserted,
        observations_unchanged=unchanged,
        comparison=comparison,
    )


def _validate_stream_id(value: object) -> str:
    if not isinstance(value, str) or not _STREAM_ID_RE.fullmatch(value):
        raise TaskShadowFeedImportError("stream ID is invalid")
    return value


@contextmanager
def _producer_snapshot_lock(outbox: Path) -> Iterator[None]:
    lock_path = outbox / _LOCK_NAME
    flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(lock_path, flags)
    except OSError as exc:
        raise TaskShadowFeedImportError(
            "producer snapshot lock is unavailable"
        ) from exc
    try:
        info = os.fstat(descriptor)
        if (not stat.S_ISREG(info.st_mode)
                or stat.S_IMODE(info.st_mode) != 0o600):
            raise TaskShadowFeedImportError(
                "producer snapshot lock is unsafe"
            )
        try:
            fcntl.flock(descriptor, fcntl.LOCK_SH | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise TaskShadowFeedImportError(
                "producer outbox is currently being updated"
            ) from exc
        except OSError as exc:
            raise TaskShadowFeedImportError(
                "producer snapshot lock cannot be acquired"
            ) from exc
        yield
    finally:
        os.close(descriptor)


def _read_snapshot(outbox: Path, expected_stream: str) -> _Snapshot:
    paths: list[tuple[int, int, Path]] = []
    try:
        entries = tuple(outbox.iterdir())
    except OSError as exc:
        raise TaskShadowFeedImportError(
            "producer outbox cannot be listed"
        ) from exc
    for entry in entries:
        if entry.name == _LOCK_NAME:
            continue
        if entry.name.startswith(_TEMP_PREFIX):
            raise TaskShadowFeedImportError(
                "producer outbox contains an incomplete page"
            )
        match = _PAGE_RE.fullmatch(entry.name)
        if match is None:
            raise TaskShadowFeedImportError(
                "producer outbox contains an unrecognized entry"
            )
        paths.append((int(match.group(1)), int(match.group(2)), entry))

    paths.sort(key=lambda item: (item[0], item[1]))
    pages: list[dict[str, Any]] = []
    cursor = 0
    revisions: set[tuple[str, str]] = set()
    for filename_start, filename_end, path in paths:
        try:
            document = _read_page(path)
        except CandidateFeedImportError as exc:
            raise TaskShadowFeedImportError(
                "producer observation page cannot be read"
            ) from exc
        try:
            feed = parse_task_shadow_feed(document)
        except ShadowFeedContractError as exc:
            raise TaskShadowFeedImportError(
                "producer outbox contains an invalid observation page"
            ) from exc
        if feed.producer != "gw" or feed.stream_id != expected_stream:
            raise TaskShadowFeedImportError(
                "producer outbox stream does not match the requested stream"
            )
        if feed.from_cursor != cursor:
            raise TaskShadowFeedImportError(
                "producer outbox page history is not contiguous"
            )
        if filename_start != cursor + 1 or filename_end != feed.to_cursor:
            raise TaskShadowFeedImportError(
                "producer outbox filename does not match its cursor range"
            )
        for item in feed.items:
            candidate = item.observation.candidate
            revision = (candidate.candidate_id, candidate.source.revision)
            if revision in revisions:
                raise TaskShadowFeedImportError(
                    "producer outbox repeats an observation revision"
                )
            revisions.add(revision)
        pages.append(document)
        cursor = feed.to_cursor
    return _Snapshot(tuple(pages), cursor)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Import a private offline shadow-observation outbox."
    )
    parser.add_argument("--outbox", type=Path, required=True)
    parser.add_argument("--database", type=Path, required=True)
    parser.add_argument("--stream-id", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        result = import_outbox(
            outbox_dir=args.outbox,
            database_path=args.database,
            stream_id=args.stream_id,
        )
    except TaskShadowFeedImportError:
        print("task shadow feed import failed", file=sys.stderr)
        return 1
    print(json.dumps({
        "comparison": {
            "agreed": result.comparison.agreed,
            "divergent": result.comparison.divergent,
            "refused": result.comparison.refused,
            "total": result.comparison.total,
            "unmapped": result.comparison.unmapped,
        },
        "current_cursor": result.current_cursor,
        "disposition": result.disposition,
        "observations_inserted": result.observations_inserted,
        "observations_unchanged": result.observations_unchanged,
        "pages_applied": result.pages_applied,
        "pages_replayed": result.pages_replayed,
        "pages_seen": result.pages_seen,
        "previous_cursor": result.previous_cursor,
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
