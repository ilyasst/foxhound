"""Manual offline export of correlated, append-only lifecycle outcomes.

The exporter reads Foxhound state without mutation and appends canonical
pages to a private outbox. It exports no task content and has no transport,
scheduler, GW client, card operation, or acknowledgement path.
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
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import StrEnum
from pathlib import Path
from typing import Any, Callable, Iterator, Sequence

from .candidate_inbox import CandidateInbox, InboxError, SCHEMA_VERSION
from .contracts import (
    LifecycleOutcomeContractError,
    LifecycleOutcomeFeedContractError,
    TaskLifecycleOutcome,
    TaskLifecycleOutcomeFeed,
    TaskLifecycleOutcomeFeedItem,
    parse_task_lifecycle_outcome_feed,
    task_lifecycle_outcome_document,
    task_lifecycle_outcome_feed_document,
)


MAX_PAGE_ITEMS = 500
MAX_PAGE_BYTES = 4 * 1024 * 1024
MAX_CURSOR = 9_223_372_036_854_775_807
_STREAM_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,199}$")
_PAGE_RE = re.compile(r"^page-([0-9]{20})-([0-9]{20})\.json$")
_TEMP_PREFIX = ".task-lifecycle-outcome-feed-tmp-"
_LOCK_NAME = ".task-lifecycle-outcome-feed.lock"


class TaskLifecycleOutcomeExportError(RuntimeError):
    """The lifecycle outcome ledger cannot be exported safely."""


class ExportDisposition(StrEnum):
    EXPORTED = "exported"
    UNCHANGED = "unchanged"


@dataclass(frozen=True)
class ExportResult:
    """Content-free export status suitable for operator output."""

    disposition: ExportDisposition
    previous_cursor: int
    current_cursor: int
    outcomes_seen: int
    outcomes_exported: int
    pages: tuple[Path, ...]


def export_outcomes(
    database_path: Path,
    *,
    outbox_dir: Path,
    stream_id: str,
    max_page_items: int = MAX_PAGE_ITEMS,
    clock: Callable[[], datetime] | None = None,
) -> ExportResult:
    """Append the unexported suffix of correlated lifecycle outcomes."""
    stream_id = _validate_stream_id(stream_id)
    if (isinstance(max_page_items, bool)
            or not isinstance(max_page_items, int)
            or not 1 <= max_page_items <= MAX_PAGE_ITEMS):
        raise TaskLifecycleOutcomeExportError(
            "max_page_items must be an integer from 1 through 500"
        )
    database = _require_private_database(Path(database_path))
    outbox = _require_private_directory(Path(outbox_dir))
    if database == outbox or outbox in database.parents:
        raise TaskLifecycleOutcomeExportError(
            "outbox and Foxhound state must be distinct"
        )
    outcomes = _read_outcomes(database)
    now = clock or (lambda: datetime.now(timezone.utc))

    with _outbox_lock(outbox):
        _remove_abandoned_temps(outbox)
        cursor = _read_exported_prefix(outbox, stream_id, outcomes)
        remaining = outcomes[cursor:]
        if not remaining:
            return ExportResult(
                ExportDisposition.UNCHANGED,
                cursor,
                cursor,
                len(outcomes),
                0,
                (),
            )
        emitted_at = _aware_timestamp(now())
        pages = []
        current = cursor
        for offset in range(0, len(remaining), max_page_items):
            chunk = remaining[offset:offset + max_page_items]
            if current > MAX_CURSOR - len(chunk):
                raise TaskLifecycleOutcomeExportError(
                    "lifecycle outcome feed cursor is exhausted"
                )
            page = TaskLifecycleOutcomeFeed(
                stream_id=stream_id,
                from_cursor=current,
                to_cursor=current + len(chunk),
                items=tuple(
                    TaskLifecycleOutcomeFeedItem(current + index, outcome)
                    for index, outcome in enumerate(chunk, start=1)
                ),
                emitted_at=emitted_at,
            )
            document = task_lifecycle_outcome_feed_document(page)
            filename = _page_filename(current + 1, page.to_cursor)
            _publish_page(outbox, filename, _canonical_bytes(document))
            pages.append(outbox / filename)
            current = page.to_cursor
        return ExportResult(
            ExportDisposition.EXPORTED,
            cursor,
            current,
            len(outcomes),
            len(remaining),
            tuple(pages),
        )


def _read_outcomes(database: Path) -> tuple[TaskLifecycleOutcome, ...]:
    try:
        connection = sqlite3.connect(
            database.resolve(strict=True).as_uri() + "?mode=ro", uri=True
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA query_only=ON")
        connection.execute("BEGIN")
        version = int(connection.execute("PRAGMA user_version").fetchone()[0])
        if version != SCHEMA_VERSION:
            raise TaskLifecycleOutcomeExportError(
                "Foxhound database schema is unsupported"
            )
        CandidateInbox._require_schema(connection)
        rows = connection.execute(
            "SELECT e.sequence,e.task_id,e.task_version,e.from_status,"
            "e.to_status,e.occurred_at,c.producer,c.legacy_task_id "
            "FROM task_events AS e "
            "JOIN task_bootstrap_correlations AS c ON c.task_id=e.task_id "
            "WHERE e.kind='status_changed' ORDER BY e.sequence"
        ).fetchall()
    except TaskLifecycleOutcomeExportError:
        raise
    except (InboxError, OSError, sqlite3.Error, TypeError, ValueError) as exc:
        raise TaskLifecycleOutcomeExportError(
            "Foxhound lifecycle state cannot be read"
        ) from exc
    finally:
        if "connection" in locals():
            connection.close()

    outcomes = []
    previous_event_sequence = 0
    per_task_version: dict[int, int] = {}
    for row in rows:
        document = {
            "schema": "foxhound.task-lifecycle-outcome",
            "schema_version": 1,
            "event_sequence": row["sequence"],
            "task_id": row["task_id"],
            "task_version": row["task_version"],
            "correlation": {
                "system": row["producer"],
                "task_id": row["legacy_task_id"],
            },
            "from_status": row["from_status"],
            "to_status": row["to_status"],
            "occurred_at": row["occurred_at"],
        }
        try:
            outcome = _parse_outcome(document)
        except LifecycleOutcomeContractError as exc:
            raise TaskLifecycleOutcomeExportError(
                "Foxhound lifecycle state is invalid"
            ) from exc
        if outcome.correlation.system != "gw":
            raise TaskLifecycleOutcomeExportError(
                "Foxhound lifecycle correlation is unsupported"
            )
        if outcome.event_sequence <= previous_event_sequence:
            raise TaskLifecycleOutcomeExportError(
                "Foxhound lifecycle event order is invalid"
            )
        prior_version = per_task_version.get(outcome.task_id)
        if (prior_version is not None
                and outcome.task_version <= prior_version):
            raise TaskLifecycleOutcomeExportError(
                "Foxhound task transition versions are not increasing"
            )
        previous_event_sequence = outcome.event_sequence
        per_task_version[outcome.task_id] = outcome.task_version
        outcomes.append(outcome)
    return tuple(outcomes)


def _parse_outcome(document: object) -> TaskLifecycleOutcome:
    # Local import avoids broadening the public module surface further.
    from .contracts import parse_task_lifecycle_outcome

    return parse_task_lifecycle_outcome(document)


def _read_exported_prefix(
    outbox: Path,
    stream_id: str,
    outcomes: Sequence[TaskLifecycleOutcome],
) -> int:
    paths = []
    try:
        entries = tuple(outbox.iterdir())
    except OSError as exc:
        raise TaskLifecycleOutcomeExportError(
            "outbox cannot be listed"
        ) from exc
    for entry in entries:
        if entry.name == _LOCK_NAME or entry.name.startswith(_TEMP_PREFIX):
            continue
        match = _PAGE_RE.fullmatch(entry.name)
        if match is None:
            raise TaskLifecycleOutcomeExportError(
                "outbox contains an unrecognized entry"
            )
        info = _safe_regular_file(entry, "lifecycle outcome page")
        if stat.S_IMODE(info.st_mode) != 0o600:
            raise TaskLifecycleOutcomeExportError(
                "lifecycle outcome page permissions are unsafe"
            )
        if info.st_size > MAX_PAGE_BYTES:
            raise TaskLifecycleOutcomeExportError(
                "lifecycle outcome page is too large"
            )
        paths.append((int(match.group(1)), int(match.group(2)), entry))
    paths.sort(key=lambda item: (item[0], item[1]))
    cursor = 0
    for first, last, path in paths:
        document, raw = _read_page(path)
        try:
            feed = parse_task_lifecycle_outcome_feed(document)
        except LifecycleOutcomeFeedContractError as exc:
            raise TaskLifecycleOutcomeExportError(
                "outbox contains an invalid lifecycle outcome page"
            ) from exc
        if raw != _canonical_bytes(document):
            raise TaskLifecycleOutcomeExportError(
                "lifecycle outcome page is not canonical"
            )
        if feed.stream_id != stream_id or feed.from_cursor != cursor:
            raise TaskLifecycleOutcomeExportError(
                "lifecycle outcome page history is not contiguous"
            )
        if first != cursor + 1 or last != feed.to_cursor:
            raise TaskLifecycleOutcomeExportError(
                "lifecycle outcome page filename does not match its "
                "cursor range"
            )
        if feed.to_cursor > len(outcomes):
            raise TaskLifecycleOutcomeExportError(
                "exported lifecycle history is ahead of Foxhound state"
            )
        for item in feed.items:
            expected = outcomes[item.sequence - 1]
            if (task_lifecycle_outcome_document(item.outcome)
                    != task_lifecycle_outcome_document(expected)):
                raise TaskLifecycleOutcomeExportError(
                    "exported lifecycle history differs from Foxhound state"
                )
        cursor = feed.to_cursor
    return cursor


def _read_page(path: Path) -> tuple[dict[str, Any], bytes]:
    flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(path, flags)
        with os.fdopen(descriptor, "rb") as handle:
            raw = handle.read(MAX_PAGE_BYTES + 1)
    except OSError as exc:
        raise TaskLifecycleOutcomeExportError(
            "lifecycle outcome page cannot be read"
        ) from exc
    if len(raw) > MAX_PAGE_BYTES:
        raise TaskLifecycleOutcomeExportError(
            "lifecycle outcome page is too large"
        )
    try:
        document = json.loads(raw, object_pairs_hook=_strict_object)
    except (UnicodeDecodeError, ValueError, TypeError) as exc:
        raise TaskLifecycleOutcomeExportError(
            "lifecycle outcome page cannot be decoded"
        ) from exc
    if not isinstance(document, dict):
        raise TaskLifecycleOutcomeExportError(
            "lifecycle outcome page must be an object"
        )
    return document, raw


def _strict_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate field")
        result[key] = value
    return result


def _require_private_database(path: Path) -> Path:
    if not path.is_absolute():
        raise TaskLifecycleOutcomeExportError("database path must be absolute")
    parent = _require_private_directory(path.parent)
    database = parent / path.name
    if Path(os.path.abspath(path)) != database:
        raise TaskLifecycleOutcomeExportError(
            "database path must not traverse symbolic links"
        )
    try:
        info = database.lstat()
    except OSError as exc:
        raise TaskLifecycleOutcomeExportError(
            "database must already exist"
        ) from exc
    if (stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode)
            or stat.S_IMODE(info.st_mode) & 0o077):
        raise TaskLifecycleOutcomeExportError("database file is unsafe")
    return database


def _require_private_directory(path: Path) -> Path:
    if not path.is_absolute():
        raise TaskLifecycleOutcomeExportError(
            "directory path must be absolute"
        )
    try:
        info = path.lstat()
        resolved = path.resolve(strict=True)
    except OSError as exc:
        raise TaskLifecycleOutcomeExportError(
            "private directory must already exist"
        ) from exc
    if (stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode)
            or stat.S_IMODE(info.st_mode) & 0o077):
        raise TaskLifecycleOutcomeExportError("private directory is unsafe")
    if Path(os.path.abspath(path)) != resolved:
        raise TaskLifecycleOutcomeExportError(
            "directory path must not traverse symbolic links"
        )
    for parent in (resolved, *resolved.parents):
        if _is_git_marker(parent / ".git"):
            raise TaskLifecycleOutcomeExportError(
                "private directory must be outside a Git worktree"
            )
    return resolved


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


def _safe_regular_file(path: Path, label: str) -> os.stat_result:
    try:
        info = path.lstat()
    except OSError as exc:
        raise TaskLifecycleOutcomeExportError(
            f"{label} cannot be inspected"
        ) from exc
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
        raise TaskLifecycleOutcomeExportError(
            f"{label} must be a regular file"
        )
    return info


@contextmanager
def _outbox_lock(outbox: Path) -> Iterator[None]:
    lock_path = outbox / _LOCK_NAME
    flags = os.O_CREAT | os.O_RDWR
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(lock_path, flags, 0o600)
    except OSError as exc:
        raise TaskLifecycleOutcomeExportError(
            "lifecycle outcome feed lock cannot be opened"
        ) from exc
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode):
            raise TaskLifecycleOutcomeExportError(
                "lifecycle outcome feed lock must be a regular file"
            )
        os.fchmod(descriptor, 0o600)
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except (BlockingIOError, OSError) as exc:
            raise TaskLifecycleOutcomeExportError(
                "lifecycle outcome feed outbox is already in use"
            ) from exc
        yield
    finally:
        os.close(descriptor)


def _remove_abandoned_temps(outbox: Path) -> None:
    for entry in outbox.iterdir():
        if not entry.name.startswith(_TEMP_PREFIX):
            continue
        _safe_regular_file(entry, "lifecycle outcome temporary entry")
        try:
            entry.unlink()
        except OSError as exc:
            raise TaskLifecycleOutcomeExportError(
                "lifecycle outcome temporary file cannot be removed"
            ) from exc


def _validate_stream_id(value: object) -> str:
    if not isinstance(value, str) or not _STREAM_ID_RE.fullmatch(value):
        raise TaskLifecycleOutcomeExportError("stream ID is invalid")
    return value


def _aware_timestamp(value: datetime) -> str:
    if (not isinstance(value, datetime) or value.tzinfo is None
            or value.utcoffset() is None):
        raise TaskLifecycleOutcomeExportError(
            "export clock must include a timezone"
        )
    return value.isoformat(timespec="seconds")


def _canonical_bytes(document: object) -> bytes:
    return (json.dumps(
        document, ensure_ascii=True, separators=(",", ":"), sort_keys=True
    ) + "\n").encode("utf-8")


def _page_filename(first: int, last: int) -> str:
    return f"page-{first:020d}-{last:020d}.json"


def _publish_page(outbox: Path, filename: str, payload: bytes) -> None:
    temporary = outbox / f"{_TEMP_PREFIX}{uuid.uuid4().hex}"
    target = outbox / filename
    flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = None
    try:
        descriptor = os.open(temporary, flags, 0o600)
        with os.fdopen(descriptor, "wb") as handle:
            descriptor = None
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.link(temporary, target, follow_symlinks=False)
        except FileExistsError as exc:
            raise TaskLifecycleOutcomeExportError(
                "lifecycle outcome page cursor already exists"
            ) from exc
        temporary.unlink()
        directory = os.open(outbox, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    except TaskLifecycleOutcomeExportError:
        raise
    except OSError as exc:
        raise TaskLifecycleOutcomeExportError(
            "lifecycle outcome page cannot be published"
        ) from exc
    finally:
        if descriptor is not None:
            os.close(descriptor)
        try:
            temporary.unlink(missing_ok=True)
        except OSError:
            pass


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Export correlated lifecycle outcomes to a private outbox."
    )
    parser.add_argument("--database", type=Path, required=True)
    parser.add_argument("--outbox", type=Path, required=True)
    parser.add_argument("--stream-id", required=True)
    parser.add_argument("--max-page-items", type=int, default=MAX_PAGE_ITEMS)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        result = export_outcomes(
            args.database,
            outbox_dir=args.outbox,
            stream_id=args.stream_id,
            max_page_items=args.max_page_items,
        )
    except TaskLifecycleOutcomeExportError:
        print("task lifecycle outcome export failed", file=sys.stderr)
        return 1
    print(json.dumps({
        "current_cursor": result.current_cursor,
        "disposition": result.disposition,
        "outcomes_exported": result.outcomes_exported,
        "outcomes_seen": result.outcomes_seen,
        "pages": len(result.pages),
        "previous_cursor": result.previous_cursor,
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
