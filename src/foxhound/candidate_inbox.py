"""Private, offline persistence for validated task candidates.

The inbox is deliberately transport-agnostic. Callers select the database path
and deliver complete candidate documents; this module performs no discovery,
networking, scheduling, logging, or task creation.

Source revisions are content digests, not sequence numbers. The inbox detects a
contradictory replay of one revision, but it does not guess which of two
different revisions is newer. A future ordered producer feed owns that rule.
"""

from __future__ import annotations

import json
import os
import sqlite3
import stat
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import StrEnum
from pathlib import Path
from typing import Callable

from .contracts import ContractError, TaskCandidate, parse_task_candidate


SCHEMA_VERSION = 1

_SCHEMA_COLUMNS = (
    "candidate_id",
    "source_system",
    "source_kind",
    "source_record_id",
    "source_item_id",
    "source_revision",
    "payload_json",
    "created_at",
    "first_imported_at",
    "updated_at",
)

_SCHEMA_V1 = """
CREATE TABLE candidate_inbox (
    candidate_id       TEXT PRIMARY KEY,
    source_system      TEXT NOT NULL,
    source_kind        TEXT NOT NULL,
    source_record_id   TEXT NOT NULL,
    source_item_id     TEXT NOT NULL,
    source_revision    TEXT NOT NULL,
    payload_json       TEXT NOT NULL,
    created_at         TEXT NOT NULL,
    first_imported_at  TEXT NOT NULL,
    updated_at         TEXT NOT NULL,
    UNIQUE(source_system, source_kind, source_record_id, source_item_id)
);
"""


class InboxError(RuntimeError):
    """The inbox cannot safely initialize or read its state."""


class ImportDisposition(StrEnum):
    INSERTED = "inserted"
    UNCHANGED = "unchanged"
    UPDATED = "updated"
    REFUSED = "refused"


class ImportRefusal(StrEnum):
    INVALID_CONTRACT = "invalid_contract"
    REVISION_CONFLICT = "revision_conflict"
    CREATED_AT_CONFLICT = "created_at_conflict"


@dataclass(frozen=True)
class ImportResult:
    """Content-free result safe for aggregate reporting."""

    disposition: ImportDisposition
    refusal: ImportRefusal | None = None

    @property
    def accepted(self) -> bool:
        return self.disposition is not ImportDisposition.REFUSED


class CandidateInbox:
    """A versioned SQLite inbox at one explicitly selected path."""

    def __init__(self, database_path: str | os.PathLike[str], *,
                 clock: Callable[[], datetime] | None = None) -> None:
        self.database_path = Path(database_path)
        self._clock = clock or (lambda: datetime.now(timezone.utc))

    def initialize(self) -> None:
        """Create or verify the inbox schema without touching parent paths."""
        self._prepare_database_file()
        with closing(self._connect()) as connection:
            version = self._schema_version(connection)
            if version > SCHEMA_VERSION:
                raise InboxError("candidate inbox schema is newer than this application")
            if version == 0:
                connection.execute("BEGIN IMMEDIATE")
                try:
                    connection.execute(_SCHEMA_V1)
                    connection.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
                    connection.commit()
                except Exception:
                    connection.rollback()
                    raise
            self._require_schema(connection)

    def import_document(self, document: object) -> ImportResult:
        """Validate and transactionally insert, replay, or update a candidate."""
        try:
            candidate = parse_task_candidate(document)
        except ContractError:
            return ImportResult(
                ImportDisposition.REFUSED,
                ImportRefusal.INVALID_CONTRACT,
            )

        payload = _canonical_payload(candidate)
        imported_at = self._now()
        with closing(self._connect()) as connection:
            self._require_current_schema(connection)
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = connection.execute(
                    "SELECT source_revision,payload_json,created_at "
                    "FROM candidate_inbox WHERE candidate_id=?",
                    (candidate.candidate_id,),
                ).fetchone()
                if row is None:
                    connection.execute(
                        "INSERT INTO candidate_inbox("
                        "candidate_id,source_system,source_kind,source_record_id,"
                        "source_item_id,source_revision,payload_json,created_at,"
                        "first_imported_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
                        (
                            candidate.candidate_id,
                            candidate.source.system,
                            candidate.source.kind,
                            candidate.source.record_id,
                            candidate.source.item_id,
                            candidate.source.revision,
                            payload,
                            candidate.created_at,
                            imported_at,
                            imported_at,
                        ),
                    )
                    connection.commit()
                    return ImportResult(ImportDisposition.INSERTED)

                if row["created_at"] != candidate.created_at:
                    connection.rollback()
                    return ImportResult(
                        ImportDisposition.REFUSED,
                        ImportRefusal.CREATED_AT_CONFLICT,
                    )
                if row["source_revision"] == candidate.source.revision:
                    connection.rollback()
                    if row["payload_json"] == payload:
                        return ImportResult(ImportDisposition.UNCHANGED)
                    return ImportResult(
                        ImportDisposition.REFUSED,
                        ImportRefusal.REVISION_CONFLICT,
                    )

                connection.execute(
                    "UPDATE candidate_inbox SET source_revision=?,payload_json=?,"
                    "updated_at=? WHERE candidate_id=?",
                    (candidate.source.revision, payload, imported_at,
                     candidate.candidate_id),
                )
                connection.commit()
                return ImportResult(ImportDisposition.UPDATED)
            except Exception:
                connection.rollback()
                raise

    def get(self, candidate_id: str) -> TaskCandidate | None:
        """Return one validated stored candidate for internal application use."""
        with closing(self._connect()) as connection:
            self._require_current_schema(connection)
            row = connection.execute(
                "SELECT payload_json FROM candidate_inbox WHERE candidate_id=?",
                (candidate_id,),
            ).fetchone()
        if row is None:
            return None
        try:
            document = json.loads(row["payload_json"])
            return parse_task_candidate(document)
        except (json.JSONDecodeError, ContractError) as exc:
            raise InboxError("candidate inbox contains an invalid stored payload") from exc

    def count(self) -> int:
        """Return a content-free candidate count."""
        with closing(self._connect()) as connection:
            self._require_current_schema(connection)
            row = connection.execute(
                "SELECT COUNT(*) AS total FROM candidate_inbox"
            ).fetchone()
            return int(row["total"])

    def _prepare_database_file(self) -> None:
        parent = self.database_path.parent
        if not parent.is_dir():
            raise InboxError("candidate inbox parent directory does not exist")
        if self.database_path.is_symlink():
            raise InboxError("candidate inbox database must not be a symbolic link")
        if self.database_path.exists():
            mode = self.database_path.stat().st_mode
            if not stat.S_ISREG(mode):
                raise InboxError("candidate inbox database must be a regular file")
            os.chmod(self.database_path, 0o600)
            return
        try:
            descriptor = os.open(
                self.database_path,
                os.O_CREAT | os.O_EXCL | os.O_WRONLY,
                0o600,
            )
        except FileExistsError as exc:
            raise InboxError("candidate inbox database path changed during creation") from exc
        else:
            os.close(descriptor)

    def _connect(self) -> sqlite3.Connection:
        if not self.database_path.is_file() or self.database_path.is_symlink():
            raise InboxError("candidate inbox is not initialized")
        connection = sqlite3.connect(self.database_path, timeout=5)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout = 5000")
        return connection

    @staticmethod
    def _schema_version(connection: sqlite3.Connection) -> int:
        return int(connection.execute("PRAGMA user_version").fetchone()[0])

    def _require_current_schema(self, connection: sqlite3.Connection) -> None:
        version = self._schema_version(connection)
        if version != SCHEMA_VERSION:
            raise InboxError("candidate inbox schema is not initialized or supported")
        self._require_schema(connection)

    @staticmethod
    def _require_schema(connection: sqlite3.Connection) -> None:
        row = connection.execute(
            "SELECT type FROM sqlite_master WHERE name='candidate_inbox'"
        ).fetchone()
        if row is None or row["type"] != "table":
            raise InboxError("candidate inbox schema is incomplete")
        columns = tuple(
            item["name"]
            for item in connection.execute("PRAGMA table_info(candidate_inbox)")
        )
        if columns != _SCHEMA_COLUMNS:
            raise InboxError("candidate inbox schema is incomplete")

    def _now(self) -> str:
        value = self._clock()
        if value.tzinfo is None or value.utcoffset() is None:
            raise InboxError("candidate inbox clock must include a timezone")
        return value.isoformat(timespec="seconds")


def _canonical_payload(candidate: TaskCandidate) -> str:
    document = {
        "schema": candidate.schema,
        "schema_version": candidate.schema_version,
        "candidate_id": candidate.candidate_id,
        "source": {
            "system": candidate.source.system,
            "kind": candidate.source.kind,
            "record_id": candidate.source.record_id,
            "item_id": candidate.source.item_id,
            "revision": candidate.source.revision,
        },
        "task": {
            "text": candidate.task.text,
            "project": candidate.task.project,
            "owner": candidate.task.owner,
            "due": candidate.task.due,
        },
        "evidence": {
            "document_id": candidate.evidence.document_id,
            "locator": candidate.evidence.locator,
        },
        "created_at": candidate.created_at,
    }
    return json.dumps(
        document,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )
