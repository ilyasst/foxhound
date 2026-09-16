"""Explicit inspection and migration of one Foxhound database."""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from contextlib import closing
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Sequence

from .candidate_inbox import CandidateInbox, InboxError, SCHEMA_VERSION


class DatabaseState(StrEnum):
    """Content-free schema state suitable for deployment decisions."""

    MISSING = "missing"
    NEW = "new"
    UPGRADE_REQUIRED = "upgrade_required"
    CURRENT = "current"
    NEWER = "newer"
    INCOMPLETE = "incomplete"


@dataclass(frozen=True)
class DatabaseInspection:
    state: DatabaseState
    schema_version: int | None

    @property
    def compatible(self) -> bool:
        return self.state is DatabaseState.CURRENT

    def document(self) -> dict[str, object]:
        return {
            "compatible": self.compatible,
            "expected_schema_version": SCHEMA_VERSION,
            "schema_version": self.schema_version,
            "state": self.state.value,
        }


def inspect_database(database_path: Path) -> DatabaseInspection:
    """Inspect database schema state without creating or changing anything."""
    inbox = CandidateInbox(database_path)
    if not inbox.database_path.exists():
        return DatabaseInspection(DatabaseState.MISSING, None)
    if not inbox.database_path.is_file() or inbox.database_path.is_symlink():
        return DatabaseInspection(DatabaseState.INCOMPLETE, None)
    try:
        with closing(sqlite3.connect(
            inbox.database_path.resolve(strict=True).as_uri() + "?mode=ro",
            uri=True,
        )) as connection:
            connection.row_factory = sqlite3.Row
            version = CandidateInbox._schema_version(connection)
            if version == 0:
                return DatabaseInspection(DatabaseState.NEW, version)
            if version > SCHEMA_VERSION:
                return DatabaseInspection(DatabaseState.NEWER, version)
            if version < SCHEMA_VERSION:
                return DatabaseInspection(DatabaseState.UPGRADE_REQUIRED, version)
            try:
                CandidateInbox._require_schema(connection)
            except InboxError:
                return DatabaseInspection(DatabaseState.INCOMPLETE, version)
            return DatabaseInspection(DatabaseState.CURRENT, version)
    except (OSError, sqlite3.Error, ValueError) as exc:
        raise InboxError("candidate inbox cannot be inspected") from exc


def migrate_database(database_path: Path) -> DatabaseInspection:
    """Create or upgrade one database, then verify its final schema."""
    CandidateInbox(database_path)._migrate()
    inspection = inspect_database(database_path)
    if not inspection.compatible:
        raise InboxError("candidate inbox schema is not initialized or supported")
    return inspection


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="foxhound-database",
        description="Inspect or migrate one Foxhound database",
    )
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("inspect", "migrate"):
        command = commands.add_parser(name)
        command.add_argument("--database", required=True, type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    arguments = _parser().parse_args(argv)
    try:
        inspection = (
            inspect_database(arguments.database)
            if arguments.command == "inspect"
            else migrate_database(arguments.database)
        )
    except (InboxError, OSError, sqlite3.Error, ValueError):
        print("foxhound database: operation failed", file=sys.stderr)
        return 70
    print(json.dumps({"ok": inspection.compatible, **inspection.document()},
                     sort_keys=True))
    return 0 if inspection.compatible else 1


if __name__ == "__main__":
    raise SystemExit(main())
