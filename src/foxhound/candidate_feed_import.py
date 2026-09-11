"""Manual, read-only import of a producer candidate-feed outbox.

The adapter takes a stable snapshot while sharing the producer's advisory
lock, validates the complete immutable page chain, and then delegates each
page to :class:`CandidateInbox`.  It never acknowledges, removes, rewrites, or
otherwise mutates producer-owned files.
"""

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

from .candidate_inbox import (
    CandidateInbox,
    FeedImportDisposition,
    InboxError,
)
from .contracts import FeedContractError, parse_candidate_feed


_LOCK_NAME = ".candidate-feed.lock"
_TEMP_PREFIX = ".candidate-feed-tmp-"
_PAGE_RE = re.compile(r"^page-([0-9]{20})-([0-9]{20})\.json$")
_STREAM_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,199}$")
_MAX_PAGE_BYTES = 4 * 1024 * 1024


class CandidateFeedImportError(RuntimeError):
    """The outbox cannot be imported without violating the boundary."""


class ShadowImportDisposition(StrEnum):
    IMPORTED = "imported"
    UNCHANGED = "unchanged"


@dataclass(frozen=True)
class ShadowImportResult:
    """Content-free aggregate status for one manual shadow import."""

    disposition: ShadowImportDisposition
    previous_cursor: int
    current_cursor: int
    pages_seen: int
    pages_applied: int
    pages_replayed: int
    candidates_inserted: int
    candidates_updated: int
    candidates_unchanged: int


@dataclass(frozen=True)
class _Snapshot:
    pages: tuple[dict[str, Any], ...]
    cursor: int


def import_outbox(
    *,
    outbox_dir: Path,
    database_path: Path,
    stream_id: str,
) -> ShadowImportResult:
    """Validate and import one complete producer ledger in page order."""
    expected_stream = _validate_stream_id(stream_id)
    outbox = _require_private_directory(Path(outbox_dir), "outbox")
    database = _require_private_database(Path(database_path))
    if outbox == database or outbox in database.parents:
        raise CandidateFeedImportError(
            "candidate inbox database must be outside the producer outbox"
        )

    with _producer_snapshot_lock(outbox):
        snapshot = _read_snapshot(outbox, expected_stream)
        inbox = CandidateInbox(database)
        try:
            inbox.initialize()
            previous_cursor = inbox.feed_cursor("gw", expected_stream)
            if previous_cursor > snapshot.cursor:
                raise CandidateFeedImportError(
                    "candidate inbox cursor is ahead of the producer ledger"
                )

            applied = 0
            replayed = 0
            inserted = 0
            updated = 0
            unchanged = 0
            for document in snapshot.pages:
                result = inbox.import_feed(document)
                if not result.accepted:
                    raise CandidateFeedImportError(
                        "candidate feed page was refused by the inbox"
                    )
                if result.disposition is FeedImportDisposition.APPLIED:
                    applied += 1
                elif result.disposition is FeedImportDisposition.REPLAYED:
                    replayed += 1
                inserted += result.inserted
                updated += result.updated
                unchanged += result.unchanged

            current_cursor = inbox.feed_cursor("gw", expected_stream)
        except CandidateFeedImportError:
            raise
        except (InboxError, OSError, sqlite3.Error) as exc:
            raise CandidateFeedImportError(
                "candidate inbox could not apply the producer ledger"
            ) from exc

    disposition = (
        ShadowImportDisposition.IMPORTED
        if applied else ShadowImportDisposition.UNCHANGED
    )
    return ShadowImportResult(
        disposition=disposition,
        previous_cursor=previous_cursor,
        current_cursor=current_cursor,
        pages_seen=len(snapshot.pages),
        pages_applied=applied,
        pages_replayed=replayed,
        candidates_inserted=inserted,
        candidates_updated=updated,
        candidates_unchanged=unchanged,
    )


def _validate_stream_id(value: object) -> str:
    if not isinstance(value, str) or not _STREAM_ID_RE.fullmatch(value):
        raise CandidateFeedImportError("stream ID is invalid")
    return value


def _require_private_directory(path: Path, label: str) -> Path:
    if not path.is_absolute():
        raise CandidateFeedImportError(f"{label} directory must be absolute")
    try:
        info = path.lstat()
    except OSError as exc:
        raise CandidateFeedImportError(
            f"{label} directory must already exist"
        ) from exc
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
        raise CandidateFeedImportError(f"{label} path must be a real directory")
    if stat.S_IMODE(info.st_mode) & 0o077:
        raise CandidateFeedImportError(
            f"{label} directory must exclude group and other access"
        )
    try:
        resolved = path.resolve(strict=True)
    except OSError as exc:
        raise CandidateFeedImportError(
            f"{label} directory cannot be resolved"
        ) from exc
    if Path(os.path.abspath(path)) != resolved:
        raise CandidateFeedImportError(
            f"{label} directory path must not traverse symbolic links"
        )
    for parent in (resolved, *resolved.parents):
        if _is_git_marker(parent / ".git"):
            raise CandidateFeedImportError(
                f"{label} directory must be outside a Git worktree"
            )
    return resolved


def _require_private_database(path: Path) -> Path:
    if not path.is_absolute():
        raise CandidateFeedImportError("candidate inbox database must be absolute")
    parent = _require_private_directory(path.parent, "database parent")
    database = parent / path.name
    if Path(os.path.abspath(path)) != database:
        raise CandidateFeedImportError(
            "candidate inbox database path must not traverse symbolic links"
        )
    try:
        info = database.lstat()
    except FileNotFoundError:
        return database
    except OSError as exc:
        raise CandidateFeedImportError(
            "candidate inbox database cannot be inspected"
        ) from exc
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
        raise CandidateFeedImportError(
            "candidate inbox database must be a regular file"
        )
    if stat.S_IMODE(info.st_mode) & 0o077:
        raise CandidateFeedImportError(
            "candidate inbox database must exclude group and other access"
        )
    return database


def _is_git_marker(path: Path) -> bool:
    try:
        if path.is_dir():
            return (path / "HEAD").is_file()
        if path.is_file():
            return path.read_text(
                encoding="utf-8", errors="replace"
            ).startswith("gitdir: ")
    except OSError:
        return True
    return False


@contextmanager
def _producer_snapshot_lock(outbox: Path) -> Iterator[None]:
    lock_path = outbox / _LOCK_NAME
    flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(lock_path, flags)
    except OSError as exc:
        raise CandidateFeedImportError(
            "producer snapshot lock is unavailable"
        ) from exc
    try:
        info = os.fstat(descriptor)
        if (not stat.S_ISREG(info.st_mode)
                or stat.S_IMODE(info.st_mode) != 0o600):
            raise CandidateFeedImportError("producer snapshot lock is unsafe")
        try:
            fcntl.flock(descriptor, fcntl.LOCK_SH | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise CandidateFeedImportError(
                "producer outbox is currently being updated"
            ) from exc
        except OSError as exc:
            raise CandidateFeedImportError(
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
        raise CandidateFeedImportError("producer outbox cannot be listed") from exc
    for entry in entries:
        if entry.name == _LOCK_NAME:
            continue
        if entry.name.startswith(_TEMP_PREFIX):
            raise CandidateFeedImportError(
                "producer outbox contains an incomplete page"
            )
        match = _PAGE_RE.fullmatch(entry.name)
        if match is None:
            raise CandidateFeedImportError(
                "producer outbox contains an unrecognized entry"
            )
        paths.append((int(match.group(1)), int(match.group(2)), entry))

    paths.sort(key=lambda item: (item[0], item[1]))
    pages: list[dict[str, Any]] = []
    cursor = 0
    revisions: set[tuple[str, str]] = set()
    creation_times: dict[str, str] = {}
    for filename_start, filename_end, path in paths:
        document = _read_page(path)
        try:
            feed = parse_candidate_feed(document)
        except FeedContractError as exc:
            raise CandidateFeedImportError(
                "producer outbox contains an invalid feed page"
            ) from exc
        if feed.producer != "gw" or feed.stream_id != expected_stream:
            raise CandidateFeedImportError(
                "producer outbox stream does not match the requested stream"
            )
        if feed.from_cursor != cursor:
            raise CandidateFeedImportError(
                "producer outbox page history is not contiguous"
            )
        if (filename_start != cursor + 1
                or filename_end != feed.to_cursor):
            raise CandidateFeedImportError(
                "producer outbox filename does not match its cursor range"
            )
        for item in feed.items:
            candidate = item.candidate
            revision = (candidate.candidate_id, candidate.source.revision)
            if revision in revisions:
                raise CandidateFeedImportError(
                    "producer outbox repeats a candidate revision"
                )
            revisions.add(revision)
            created_at = creation_times.get(candidate.candidate_id)
            if created_at is not None and created_at != candidate.created_at:
                raise CandidateFeedImportError(
                    "producer outbox changes a candidate creation time"
                )
            creation_times[candidate.candidate_id] = candidate.created_at
        pages.append(document)
        cursor = feed.to_cursor
    return _Snapshot(tuple(pages), cursor)


def _read_page(path: Path) -> dict[str, Any]:
    flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise CandidateFeedImportError(
            "producer feed page cannot be opened"
        ) from exc
    try:
        info = os.fstat(descriptor)
        if (not stat.S_ISREG(info.st_mode)
                or stat.S_IMODE(info.st_mode) != 0o600):
            raise CandidateFeedImportError("producer feed page is unsafe")
        if info.st_size > _MAX_PAGE_BYTES:
            raise CandidateFeedImportError("producer feed page is too large")
        with os.fdopen(descriptor, "rb", closefd=False) as handle:
            raw = handle.read(_MAX_PAGE_BYTES + 1)
    except CandidateFeedImportError:
        raise
    except OSError as exc:
        raise CandidateFeedImportError(
            "producer feed page cannot be read"
        ) from exc
    finally:
        os.close(descriptor)
    if len(raw) > _MAX_PAGE_BYTES:
        raise CandidateFeedImportError("producer feed page is too large")
    try:
        document = json.loads(raw)
    except (UnicodeDecodeError, ValueError) as exc:
        raise CandidateFeedImportError(
            "producer feed page cannot be decoded"
        ) from exc
    if not isinstance(document, dict) or raw != _canonical_bytes(document):
        raise CandidateFeedImportError(
            "producer feed page is not canonical JSON"
        )
    return document


def _canonical_bytes(document: object) -> bytes:
    return (
        json.dumps(
            document,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Import a private offline candidate-feed outbox."
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
    except CandidateFeedImportError:
        print("candidate feed import failed", file=sys.stderr)
        return 1
    print(json.dumps({
        "candidates_inserted": result.candidates_inserted,
        "candidates_unchanged": result.candidates_unchanged,
        "candidates_updated": result.candidates_updated,
        "current_cursor": result.current_cursor,
        "disposition": result.disposition,
        "pages_applied": result.pages_applied,
        "pages_replayed": result.pages_replayed,
        "pages_seen": result.pages_seen,
        "previous_cursor": result.previous_cursor,
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
