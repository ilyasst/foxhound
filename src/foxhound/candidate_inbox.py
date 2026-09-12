"""Private, offline persistence for candidates and shadow observations.

The inbox is deliberately transport-agnostic. Callers select the database path
and deliver complete contract documents; this module performs no discovery,
networking, scheduling, logging, or task creation.

Source revisions are content digests, not sequence numbers. The inbox detects a
contradictory replay of one revision. Ordered producer feeds use a separate,
monotonic cursor to decide delivery order without interpreting revisions.
Candidate revision history lets passive observations bind to the exact payload
that the producer handled without rolling the current candidate backward.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
import stat
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import StrEnum
from pathlib import Path
from typing import Callable

from .contracts import (
    CandidateFeed,
    ContractError,
    FeedContractError,
    ShadowFeedContractError,
    TaskShadowFeed,
    TaskCandidate,
    TaskShadowObservation,
    candidate_comparable_digest,
    parse_candidate_feed,
    parse_task_candidate,
    parse_task_shadow_feed,
    task_candidate_document,
    task_shadow_feed_document,
    task_shadow_observation_document,
)


SCHEMA_VERSION = 7
_STREAM_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,199}$")
_MAX_SQLITE_INTEGER = 9_223_372_036_854_775_807

_SCHEMA_COLUMNS = {
    "candidate_inbox": (
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
    ),
    "candidate_feed_cursors": (
        "producer",
        "stream_id",
        "cursor",
        "updated_at",
    ),
    "candidate_feed_receipts": (
        "producer",
        "stream_id",
        "from_cursor",
        "to_cursor",
        "page_digest",
        "imported_at",
    ),
    "candidate_revision_history": (
        "candidate_id",
        "source_revision",
        "payload_json",
        "created_at",
        "imported_at",
    ),
    "task_shadow_observations": (
        "candidate_id",
        "source_revision",
        "disposition",
        "legacy_task_id",
        "comparable_digest",
        "reason_code",
        "comparison",
        "payload_json",
        "observed_at",
        "first_imported_at",
    ),
    "task_shadow_feed_cursors": (
        "producer",
        "stream_id",
        "cursor",
        "updated_at",
    ),
    "task_shadow_feed_receipts": (
        "producer",
        "stream_id",
        "from_cursor",
        "to_cursor",
        "page_digest",
        "imported_at",
    ),
    "tasks": (
        "id",
        "status",
        "text",
        "owner",
        "due",
        "version",
        "created_at",
        "updated_at",
        "closed_at",
    ),
    "task_candidate_bindings": (
        "candidate_id",
        "source_revision",
        "task_id",
        "relation",
        "decided_at",
    ),
    "task_bootstrap_correlations": (
        "producer",
        "legacy_task_id",
        "task_id",
        "created_at",
    ),
    "task_events": (
        "sequence",
        "task_id",
        "kind",
        "task_version",
        "candidate_id",
        "source_revision",
        "from_status",
        "to_status",
        "occurred_at",
    ),
    "shadow_import_cycles": (
        "sequence",
        "stream_id",
        "started_at",
        "completed_at",
        "candidate_previous_cursor",
        "candidate_current_cursor",
        "candidates_inserted",
        "candidates_updated",
        "observation_previous_cursor",
        "observation_current_cursor",
        "observations_inserted",
        "comparison_total",
        "comparison_agreed",
        "comparison_divergent",
        "comparison_refused",
        "comparison_unmapped",
    ),
    "task_owner_equivalences": (
        "candidate_id",
        "source_revision",
        "legacy_task_id",
        "legacy_digest",
        "effective_owner",
        "basis",
        "resolved_at",
    ),
    "task_review_cards": (
        "id",
        "task_id",
        "task_version",
        "status",
        "version",
        "due_at",
        "claim_token_digest",
        "claim_expires_at",
        "transport",
        "delivery_ref",
        "delivered_at",
        "resolution",
        "review_after",
        "created_at",
        "updated_at",
        "resolved_at",
    ),
    "task_review_card_events": (
        "sequence",
        "card_id",
        "task_id",
        "kind",
        "card_version",
        "task_version",
        "action",
        "occurred_at",
    ),
}

_SCHEMA_OBJECTS = {
    "task_candidate_bindings_one_accepted": "index",
    "task_events_no_update": "trigger",
    "task_events_no_delete": "trigger",
    "shadow_import_cycles_no_update": "trigger",
    "shadow_import_cycles_no_delete": "trigger",
    "task_owner_equivalences_no_update": "trigger",
    "task_owner_equivalences_no_delete": "trigger",
    "task_review_cards_one_active": "index",
    "task_review_card_events_no_update": "trigger",
    "task_review_card_events_no_delete": "trigger",
}

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

_SCHEMA_V2 = (
    """
CREATE TABLE candidate_feed_cursors (
    producer   TEXT NOT NULL,
    stream_id  TEXT NOT NULL,
    cursor     INTEGER NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY(producer, stream_id)
);
""",
    """
CREATE TABLE candidate_feed_receipts (
    producer    TEXT NOT NULL,
    stream_id   TEXT NOT NULL,
    from_cursor INTEGER NOT NULL,
    to_cursor   INTEGER NOT NULL,
    page_digest TEXT NOT NULL,
    imported_at TEXT NOT NULL,
    PRIMARY KEY(producer, stream_id, from_cursor, to_cursor)
);
""",
)

_SCHEMA_V3 = (
    """
CREATE TABLE candidate_revision_history (
    candidate_id    TEXT NOT NULL,
    source_revision TEXT NOT NULL,
    payload_json    TEXT NOT NULL,
    created_at      TEXT NOT NULL,
    imported_at     TEXT NOT NULL,
    PRIMARY KEY(candidate_id, source_revision)
);
""",
    """
INSERT INTO candidate_revision_history(
    candidate_id,source_revision,payload_json,created_at,imported_at
)
SELECT candidate_id,source_revision,payload_json,created_at,first_imported_at
FROM candidate_inbox;
""",
    """
CREATE TABLE task_shadow_observations (
    candidate_id       TEXT NOT NULL,
    source_revision    TEXT NOT NULL,
    disposition        TEXT NOT NULL,
    legacy_task_id     INTEGER,
    comparable_digest  TEXT,
    reason_code        TEXT,
    comparison         TEXT NOT NULL,
    payload_json       TEXT NOT NULL,
    observed_at        TEXT NOT NULL,
    first_imported_at  TEXT NOT NULL,
    PRIMARY KEY(candidate_id, source_revision),
    FOREIGN KEY(candidate_id, source_revision)
        REFERENCES candidate_revision_history(candidate_id, source_revision)
);
""",
    """
CREATE TABLE task_shadow_feed_cursors (
    producer   TEXT NOT NULL,
    stream_id  TEXT NOT NULL,
    cursor     INTEGER NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY(producer, stream_id)
);
""",
    """
CREATE TABLE task_shadow_feed_receipts (
    producer    TEXT NOT NULL,
    stream_id   TEXT NOT NULL,
    from_cursor INTEGER NOT NULL,
    to_cursor   INTEGER NOT NULL,
    page_digest TEXT NOT NULL,
    imported_at TEXT NOT NULL,
    PRIMARY KEY(producer, stream_id, from_cursor, to_cursor)
);
""",
)

_SCHEMA_V4 = (
    """
CREATE TABLE tasks (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    status     TEXT NOT NULL CHECK(status IN ('open','done','dropped')),
    text       TEXT NOT NULL,
    owner      TEXT,
    due        TEXT,
    version    INTEGER NOT NULL CHECK(version >= 1),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    closed_at  TEXT
);
""",
    """
CREATE TABLE task_candidate_bindings (
    candidate_id    TEXT PRIMARY KEY,
    source_revision TEXT NOT NULL,
    task_id         INTEGER NOT NULL,
    relation        TEXT NOT NULL CHECK(relation IN ('accepted','folded')),
    decided_at      TEXT NOT NULL,
    FOREIGN KEY(candidate_id, source_revision)
        REFERENCES candidate_revision_history(candidate_id, source_revision),
    FOREIGN KEY(task_id) REFERENCES tasks(id)
);
""",
    """
CREATE UNIQUE INDEX task_candidate_bindings_one_accepted
    ON task_candidate_bindings(task_id)
    WHERE relation='accepted';
""",
    """
CREATE TABLE task_bootstrap_correlations (
    producer       TEXT NOT NULL,
    legacy_task_id INTEGER NOT NULL CHECK(legacy_task_id > 0),
    task_id        INTEGER NOT NULL UNIQUE,
    created_at     TEXT NOT NULL,
    PRIMARY KEY(producer, legacy_task_id),
    FOREIGN KEY(task_id) REFERENCES tasks(id)
);
""",
    """
CREATE TABLE task_events (
    sequence        INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id         INTEGER NOT NULL,
    kind            TEXT NOT NULL CHECK(
                        kind IN ('created','candidate_folded','status_changed')
                    ),
    task_version    INTEGER NOT NULL CHECK(task_version >= 1),
    candidate_id    TEXT,
    source_revision TEXT,
    from_status     TEXT,
    to_status       TEXT,
    occurred_at     TEXT NOT NULL,
    FOREIGN KEY(task_id) REFERENCES tasks(id)
);
""",
    """
CREATE TRIGGER task_events_no_update
BEFORE UPDATE ON task_events
BEGIN
    SELECT RAISE(ABORT, 'task events are append-only');
END;
""",
    """
CREATE TRIGGER task_events_no_delete
BEFORE DELETE ON task_events
BEGIN
    SELECT RAISE(ABORT, 'task events are append-only');
END;
""",
)

_SCHEMA_V5 = (
    """
CREATE TABLE shadow_import_cycles (
    sequence                    INTEGER PRIMARY KEY AUTOINCREMENT,
    stream_id                   TEXT NOT NULL,
    started_at                  TEXT NOT NULL,
    completed_at                TEXT NOT NULL,
    candidate_previous_cursor   INTEGER NOT NULL
                                CHECK(candidate_previous_cursor >= 0),
    candidate_current_cursor    INTEGER NOT NULL
                                CHECK(candidate_current_cursor >= 0),
    candidates_inserted         INTEGER NOT NULL
                                CHECK(candidates_inserted >= 0),
    candidates_updated          INTEGER NOT NULL
                                CHECK(candidates_updated >= 0),
    observation_previous_cursor INTEGER NOT NULL
                                CHECK(observation_previous_cursor >= 0),
    observation_current_cursor  INTEGER NOT NULL
                                CHECK(observation_current_cursor >= 0),
    observations_inserted       INTEGER NOT NULL
                                CHECK(observations_inserted >= 0),
    comparison_total            INTEGER NOT NULL CHECK(comparison_total >= 0),
    comparison_agreed           INTEGER NOT NULL CHECK(comparison_agreed >= 0),
    comparison_divergent        INTEGER NOT NULL
                                CHECK(comparison_divergent >= 0),
    comparison_refused          INTEGER NOT NULL
                                CHECK(comparison_refused >= 0),
    comparison_unmapped         INTEGER NOT NULL
                                CHECK(comparison_unmapped >= 0),
    CHECK(candidate_current_cursor >= candidate_previous_cursor),
    CHECK(observation_current_cursor >= observation_previous_cursor),
    CHECK(comparison_total = comparison_agreed + comparison_divergent
          + comparison_refused + comparison_unmapped)
);
""",
    """
CREATE TRIGGER shadow_import_cycles_no_update
BEFORE UPDATE ON shadow_import_cycles
BEGIN
    SELECT RAISE(ABORT, 'shadow import cycle receipts are append-only');
END;
""",
    """
CREATE TRIGGER shadow_import_cycles_no_delete
BEFORE DELETE ON shadow_import_cycles
BEGIN
    SELECT RAISE(ABORT, 'shadow import cycle receipts are append-only');
END;
""",
)

_SCHEMA_V6 = (
    """
CREATE TABLE task_owner_equivalences (
    candidate_id    TEXT NOT NULL,
    source_revision TEXT NOT NULL,
    legacy_task_id  INTEGER NOT NULL CHECK(legacy_task_id > 0),
    legacy_digest   TEXT NOT NULL,
    effective_owner TEXT NOT NULL,
    basis           TEXT NOT NULL CHECK(basis = 'speaker_merge'),
    resolved_at     TEXT NOT NULL,
    PRIMARY KEY(candidate_id, source_revision),
    FOREIGN KEY(candidate_id, source_revision)
        REFERENCES task_shadow_observations(candidate_id, source_revision)
);
""",
    """
CREATE TRIGGER task_owner_equivalences_no_update
BEFORE UPDATE ON task_owner_equivalences
BEGIN
    SELECT RAISE(ABORT, 'task owner equivalences are append-only');
END;
""",
    """
CREATE TRIGGER task_owner_equivalences_no_delete
BEFORE DELETE ON task_owner_equivalences
BEGIN
    SELECT RAISE(ABORT, 'task owner equivalences are append-only');
END;
""",
)

_SCHEMA_V7 = (
    """
CREATE TABLE task_review_cards (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id            INTEGER NOT NULL,
    task_version       INTEGER NOT NULL CHECK(task_version >= 1),
    status             TEXT NOT NULL CHECK(status IN (
                           'pending','delivering','delivered','snoozed',
                           'resolved','cancelled'
                       )),
    version            INTEGER NOT NULL CHECK(version >= 1),
    due_at             TEXT NOT NULL,
    claim_token_digest TEXT,
    claim_expires_at   TEXT,
    transport          TEXT,
    delivery_ref       TEXT,
    delivered_at       TEXT,
    resolution         TEXT CHECK(resolution IS NULL OR resolution IN (
                           'done','keep_open','drop'
                       )),
    review_after       TEXT,
    created_at         TEXT NOT NULL,
    updated_at         TEXT NOT NULL,
    resolved_at        TEXT,
    FOREIGN KEY(task_id) REFERENCES tasks(id)
);
""",
    """
CREATE UNIQUE INDEX task_review_cards_one_active
    ON task_review_cards(task_id)
    WHERE status IN ('pending','delivering','delivered','snoozed');
""",
    """
CREATE TABLE task_review_card_events (
    sequence     INTEGER PRIMARY KEY AUTOINCREMENT,
    card_id      INTEGER NOT NULL,
    task_id      INTEGER NOT NULL,
    kind         TEXT NOT NULL CHECK(kind IN (
                     'scheduled','delivery_claimed','delivered',
                     'delivery_failed','delivery_expired','snoozed',
                     'resolved','cancelled'
                 )),
    card_version INTEGER NOT NULL CHECK(card_version >= 1),
    task_version INTEGER NOT NULL CHECK(task_version >= 1),
    action       TEXT CHECK(action IS NULL OR action IN (
                     'done','keep_open','drop','snooze'
                 )),
    occurred_at  TEXT NOT NULL,
    FOREIGN KEY(card_id) REFERENCES task_review_cards(id),
    FOREIGN KEY(task_id) REFERENCES tasks(id)
);
""",
    """
CREATE TRIGGER task_review_card_events_no_update
BEFORE UPDATE ON task_review_card_events
BEGIN
    SELECT RAISE(ABORT, 'task review card events are append-only');
END;
""",
    """
CREATE TRIGGER task_review_card_events_no_delete
BEFORE DELETE ON task_review_card_events
BEGIN
    SELECT RAISE(ABORT, 'task review card events are append-only');
END;
""",
)


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


class FeedImportDisposition(StrEnum):
    APPLIED = "applied"
    REPLAYED = "replayed"
    EMPTY = "empty"
    REFUSED = "refused"


class FeedImportRefusal(StrEnum):
    INVALID_CONTRACT = "invalid_contract"
    CURSOR_GAP = "cursor_gap"
    CURSOR_OVERLAP = "cursor_overlap"
    CURSOR_REUSE = "cursor_reuse"
    CANDIDATE_CONFLICT = "candidate_conflict"


class ShadowFeedImportDisposition(StrEnum):
    APPLIED = "applied"
    REPLAYED = "replayed"
    EMPTY = "empty"
    REFUSED = "refused"


class ShadowFeedImportRefusal(StrEnum):
    INVALID_CONTRACT = "invalid_contract"
    CURSOR_GAP = "cursor_gap"
    CURSOR_OVERLAP = "cursor_overlap"
    CURSOR_REUSE = "cursor_reuse"
    CANDIDATE_MISSING = "candidate_missing"
    CANDIDATE_CONFLICT = "candidate_conflict"
    OBSERVATION_CONFLICT = "observation_conflict"


@dataclass(frozen=True)
class ImportResult:
    """Content-free result safe for aggregate reporting."""

    disposition: ImportDisposition
    refusal: ImportRefusal | None = None

    @property
    def accepted(self) -> bool:
        return self.disposition is not ImportDisposition.REFUSED


@dataclass(frozen=True)
class FeedImportResult:
    """Content-free aggregate result for one atomic feed-page import."""

    disposition: FeedImportDisposition
    inserted: int = 0
    updated: int = 0
    unchanged: int = 0
    refusal: FeedImportRefusal | None = None

    @property
    def accepted(self) -> bool:
        return self.disposition is not FeedImportDisposition.REFUSED


@dataclass(frozen=True)
class ShadowFeedImportResult:
    """Content-free aggregate result for one atomic observation page."""

    disposition: ShadowFeedImportDisposition
    inserted: int = 0
    unchanged: int = 0
    refusal: ShadowFeedImportRefusal | None = None

    @property
    def accepted(self) -> bool:
        return self.disposition is not ShadowFeedImportDisposition.REFUSED


@dataclass(frozen=True)
class ShadowComparisonReport:
    """Content-free aggregate status for all persisted observations."""

    total: int
    agreed: int
    divergent: int
    refused: int
    unmapped: int


@dataclass(frozen=True)
class ShadowImportCycleReceipt:
    """Content-free durable record of one completed import cycle."""

    stream_id: str
    started_at: str
    completed_at: str
    candidate_previous_cursor: int
    candidate_current_cursor: int
    candidates_inserted: int
    candidates_updated: int
    observation_previous_cursor: int
    observation_current_cursor: int
    observations_inserted: int
    comparison: ShadowComparisonReport


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
                raise InboxError(
                    "candidate inbox schema is newer than this application"
                )
            if version == 0:
                connection.execute("BEGIN IMMEDIATE")
                try:
                    connection.execute(_SCHEMA_V1)
                    connection.execute("PRAGMA user_version = 1")
                    connection.commit()
                except Exception:
                    connection.rollback()
                    raise
                version = 1
            if version == 1:
                self._require_tables(connection, ("candidate_inbox",))
                connection.execute("BEGIN IMMEDIATE")
                try:
                    for statement in _SCHEMA_V2:
                        connection.execute(statement)
                    connection.execute("PRAGMA user_version = 2")
                    connection.commit()
                except Exception:
                    connection.rollback()
                    raise
                version = 2
            if version == 2:
                self._require_tables(
                    connection,
                    (
                        "candidate_inbox",
                        "candidate_feed_cursors",
                        "candidate_feed_receipts",
                    ),
                )
                connection.execute("BEGIN IMMEDIATE")
                try:
                    for statement in _SCHEMA_V3:
                        connection.execute(statement)
                    connection.execute("PRAGMA user_version = 3")
                    connection.commit()
                except Exception:
                    connection.rollback()
                    raise
                version = 3
            if version == 3:
                self._require_tables(
                    connection,
                    (
                        "candidate_inbox",
                        "candidate_feed_cursors",
                        "candidate_feed_receipts",
                        "candidate_revision_history",
                        "task_shadow_observations",
                        "task_shadow_feed_cursors",
                        "task_shadow_feed_receipts",
                    ),
                )
                connection.execute("PRAGMA foreign_keys = ON")
                connection.execute("BEGIN IMMEDIATE")
                try:
                    for statement in _SCHEMA_V4:
                        connection.execute(statement)
                    connection.execute("PRAGMA user_version = 4")
                    connection.commit()
                except Exception:
                    connection.rollback()
                    raise
                version = 4
            if version == 4:
                self._require_tables(
                    connection,
                    (
                        "candidate_inbox",
                        "candidate_feed_cursors",
                        "candidate_feed_receipts",
                        "candidate_revision_history",
                        "task_shadow_observations",
                        "task_shadow_feed_cursors",
                        "task_shadow_feed_receipts",
                        "tasks",
                        "task_candidate_bindings",
                        "task_bootstrap_correlations",
                        "task_events",
                    ),
                )
                connection.execute("BEGIN IMMEDIATE")
                try:
                    for statement in _SCHEMA_V5:
                        connection.execute(statement)
                    connection.execute("PRAGMA user_version = 5")
                    connection.commit()
                except Exception:
                    connection.rollback()
                    raise
                version = 5
            if version == 5:
                self._require_tables(
                    connection,
                    (
                        "candidate_inbox",
                        "candidate_feed_cursors",
                        "candidate_feed_receipts",
                        "candidate_revision_history",
                        "task_shadow_observations",
                        "task_shadow_feed_cursors",
                        "task_shadow_feed_receipts",
                        "tasks",
                        "task_candidate_bindings",
                        "task_bootstrap_correlations",
                        "task_events",
                        "shadow_import_cycles",
                    ),
                )
                connection.execute("PRAGMA foreign_keys = ON")
                connection.execute("BEGIN IMMEDIATE")
                try:
                    for statement in _SCHEMA_V6:
                        connection.execute(statement)
                    connection.execute("PRAGMA user_version = 6")
                    connection.commit()
                except Exception:
                    connection.rollback()
                    raise
                version = 6
            if version == 6:
                self._require_tables(
                    connection,
                    (
                        "candidate_inbox",
                        "candidate_feed_cursors",
                        "candidate_feed_receipts",
                        "candidate_revision_history",
                        "task_shadow_observations",
                        "task_shadow_feed_cursors",
                        "task_shadow_feed_receipts",
                        "tasks",
                        "task_candidate_bindings",
                        "task_bootstrap_correlations",
                        "task_events",
                        "shadow_import_cycles",
                        "task_owner_equivalences",
                    ),
                )
                connection.execute("PRAGMA foreign_keys = ON")
                connection.execute("BEGIN IMMEDIATE")
                try:
                    for statement in _SCHEMA_V7:
                        connection.execute(statement)
                    connection.execute("PRAGMA user_version = 7")
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
                result = self._apply_candidate(
                    connection, candidate, payload, imported_at)
                if result.accepted:
                    connection.commit()
                else:
                    connection.rollback()
                return result
            except Exception:
                connection.rollback()
                raise

    def import_feed(self, document: object) -> FeedImportResult:
        """Validate and atomically import one ordered candidate-feed page."""
        try:
            feed = parse_candidate_feed(document)
        except FeedContractError:
            return FeedImportResult(
                FeedImportDisposition.REFUSED,
                refusal=FeedImportRefusal.INVALID_CONTRACT,
            )

        page_digest = _feed_digest(feed)
        imported_at = self._now()
        with closing(self._connect()) as connection:
            self._require_current_schema(connection)
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = connection.execute(
                    "SELECT cursor FROM candidate_feed_cursors "
                    "WHERE producer=? AND stream_id=?",
                    (feed.producer, feed.stream_id),
                ).fetchone()
                current_cursor = 0 if row is None else int(row["cursor"])

                if feed.from_cursor < current_cursor:
                    receipt = connection.execute(
                        "SELECT page_digest FROM candidate_feed_receipts "
                        "WHERE producer=? AND stream_id=? AND from_cursor=? "
                        "AND to_cursor=?",
                        (feed.producer, feed.stream_id, feed.from_cursor,
                         feed.to_cursor),
                    ).fetchone()
                    connection.rollback()
                    if receipt is None:
                        return FeedImportResult(
                            FeedImportDisposition.REFUSED,
                            refusal=FeedImportRefusal.CURSOR_OVERLAP,
                        )
                    if receipt["page_digest"] != page_digest:
                        return FeedImportResult(
                            FeedImportDisposition.REFUSED,
                            refusal=FeedImportRefusal.CURSOR_REUSE,
                        )
                    return FeedImportResult(FeedImportDisposition.REPLAYED)

                if feed.from_cursor > current_cursor:
                    connection.rollback()
                    return FeedImportResult(
                        FeedImportDisposition.REFUSED,
                        refusal=FeedImportRefusal.CURSOR_GAP,
                    )

                if feed.from_cursor == feed.to_cursor:
                    connection.rollback()
                    return FeedImportResult(FeedImportDisposition.EMPTY)

                counts = {
                    ImportDisposition.INSERTED: 0,
                    ImportDisposition.UPDATED: 0,
                    ImportDisposition.UNCHANGED: 0,
                }
                for item in feed.items:
                    payload = _canonical_payload(item.candidate)
                    result = self._apply_candidate(
                        connection, item.candidate, payload, imported_at)
                    if not result.accepted:
                        connection.rollback()
                        return FeedImportResult(
                            FeedImportDisposition.REFUSED,
                            refusal=FeedImportRefusal.CANDIDATE_CONFLICT,
                        )
                    counts[result.disposition] += 1

                connection.execute(
                    "INSERT INTO candidate_feed_receipts("
                    "producer,stream_id,from_cursor,to_cursor,page_digest,"
                    "imported_at) VALUES(?,?,?,?,?,?)",
                    (feed.producer, feed.stream_id, feed.from_cursor,
                     feed.to_cursor, page_digest, imported_at),
                )
                connection.execute(
                    "INSERT INTO candidate_feed_cursors("
                    "producer,stream_id,cursor,updated_at) VALUES(?,?,?,?) "
                    "ON CONFLICT(producer,stream_id) DO UPDATE SET "
                    "cursor=excluded.cursor,updated_at=excluded.updated_at",
                    (feed.producer, feed.stream_id, feed.to_cursor,
                     imported_at),
                )
                connection.commit()
                return FeedImportResult(
                    FeedImportDisposition.APPLIED,
                    inserted=counts[ImportDisposition.INSERTED],
                    updated=counts[ImportDisposition.UPDATED],
                    unchanged=counts[ImportDisposition.UNCHANGED],
                )
            except Exception:
                connection.rollback()
                raise

    def import_shadow_feed(self, document: object) -> ShadowFeedImportResult:
        """Validate and atomically import one ordered observation page."""
        try:
            feed = parse_task_shadow_feed(document)
        except ShadowFeedContractError:
            return ShadowFeedImportResult(
                ShadowFeedImportDisposition.REFUSED,
                refusal=ShadowFeedImportRefusal.INVALID_CONTRACT,
            )

        page_digest = _shadow_feed_digest(feed)
        imported_at = self._now()
        with closing(self._connect()) as connection:
            self._require_current_schema(connection)
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = connection.execute(
                    "SELECT cursor FROM task_shadow_feed_cursors "
                    "WHERE producer=? AND stream_id=?",
                    (feed.producer, feed.stream_id),
                ).fetchone()
                current_cursor = 0 if row is None else int(row["cursor"])

                if feed.from_cursor < current_cursor:
                    receipt = connection.execute(
                        "SELECT page_digest FROM task_shadow_feed_receipts "
                        "WHERE producer=? AND stream_id=? AND from_cursor=? "
                        "AND to_cursor=?",
                        (feed.producer, feed.stream_id, feed.from_cursor,
                         feed.to_cursor),
                    ).fetchone()
                    connection.rollback()
                    if receipt is None:
                        return ShadowFeedImportResult(
                            ShadowFeedImportDisposition.REFUSED,
                            refusal=ShadowFeedImportRefusal.CURSOR_OVERLAP,
                        )
                    if receipt["page_digest"] != page_digest:
                        return ShadowFeedImportResult(
                            ShadowFeedImportDisposition.REFUSED,
                            refusal=ShadowFeedImportRefusal.CURSOR_REUSE,
                        )
                    return ShadowFeedImportResult(
                        ShadowFeedImportDisposition.REPLAYED
                    )

                if feed.from_cursor > current_cursor:
                    connection.rollback()
                    return ShadowFeedImportResult(
                        ShadowFeedImportDisposition.REFUSED,
                        refusal=ShadowFeedImportRefusal.CURSOR_GAP,
                    )

                if feed.from_cursor == feed.to_cursor:
                    connection.rollback()
                    return ShadowFeedImportResult(
                        ShadowFeedImportDisposition.EMPTY
                    )

                inserted = 0
                unchanged = 0
                for item in feed.items:
                    was_inserted, refusal = self._apply_shadow_observation(
                        connection, item.observation, imported_at
                    )
                    if refusal is not None:
                        connection.rollback()
                        return ShadowFeedImportResult(
                            ShadowFeedImportDisposition.REFUSED,
                            refusal=refusal,
                        )
                    if was_inserted:
                        inserted += 1
                    else:
                        unchanged += 1

                connection.execute(
                    "INSERT INTO task_shadow_feed_receipts("
                    "producer,stream_id,from_cursor,to_cursor,page_digest,"
                    "imported_at) VALUES(?,?,?,?,?,?)",
                    (feed.producer, feed.stream_id, feed.from_cursor,
                     feed.to_cursor, page_digest, imported_at),
                )
                connection.execute(
                    "INSERT INTO task_shadow_feed_cursors("
                    "producer,stream_id,cursor,updated_at) VALUES(?,?,?,?) "
                    "ON CONFLICT(producer,stream_id) DO UPDATE SET "
                    "cursor=excluded.cursor,updated_at=excluded.updated_at",
                    (feed.producer, feed.stream_id, feed.to_cursor,
                     imported_at),
                )
                connection.commit()
                return ShadowFeedImportResult(
                    ShadowFeedImportDisposition.APPLIED,
                    inserted=inserted,
                    unchanged=unchanged,
                )
            except Exception:
                connection.rollback()
                raise

    def shadow_feed_cursor(self, producer: str, stream_id: str) -> int:
        """Return one content-free observation cursor, or zero initially."""
        with closing(self._connect()) as connection:
            self._require_current_schema(connection)
            row = connection.execute(
                "SELECT cursor FROM task_shadow_feed_cursors "
                "WHERE producer=? AND stream_id=?",
                (producer, stream_id),
            ).fetchone()
            return 0 if row is None else int(row["cursor"])

    def shadow_report(self) -> ShadowComparisonReport:
        """Return aggregate comparison counts without candidate content."""
        with closing(self._connect()) as connection:
            self._require_current_schema(connection)
            rows = connection.execute(
                "SELECT comparison,COUNT(*) AS total "
                "FROM task_shadow_observations GROUP BY comparison"
            ).fetchall()
        counts = {row["comparison"]: int(row["total"]) for row in rows}
        if set(counts) - {"agreed", "divergent", "refused", "unmapped"}:
            raise InboxError("task shadow observations contain invalid state")
        return ShadowComparisonReport(
            total=sum(counts.values()),
            agreed=counts.get("agreed", 0),
            divergent=counts.get("divergent", 0),
            refused=counts.get("refused", 0),
            unmapped=counts.get("unmapped", 0),
        )

    def feed_cursor(self, producer: str, stream_id: str) -> int:
        """Return one content-free feed cursor, or zero before first import."""
        with closing(self._connect()) as connection:
            self._require_current_schema(connection)
            row = connection.execute(
                "SELECT cursor FROM candidate_feed_cursors "
                "WHERE producer=? AND stream_id=?",
                (producer, stream_id),
            ).fetchone()
            return 0 if row is None else int(row["cursor"])

    def append_shadow_import_cycle(
        self, receipt: ShadowImportCycleReceipt
    ) -> int:
        """Append one validated success receipt and return its sequence."""
        _validate_shadow_cycle_receipt(receipt)
        with closing(self._connect()) as connection:
            self._require_current_schema(connection)
            connection.execute("BEGIN IMMEDIATE")
            try:
                report = receipt.comparison
                cursor = connection.execute(
                    "INSERT INTO shadow_import_cycles("
                    "stream_id,started_at,completed_at,"
                    "candidate_previous_cursor,candidate_current_cursor,"
                    "candidates_inserted,candidates_updated,"
                    "observation_previous_cursor,observation_current_cursor,"
                    "observations_inserted,comparison_total,"
                    "comparison_agreed,comparison_divergent,"
                    "comparison_refused,comparison_unmapped) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        receipt.stream_id,
                        receipt.started_at,
                        receipt.completed_at,
                        receipt.candidate_previous_cursor,
                        receipt.candidate_current_cursor,
                        receipt.candidates_inserted,
                        receipt.candidates_updated,
                        receipt.observation_previous_cursor,
                        receipt.observation_current_cursor,
                        receipt.observations_inserted,
                        report.total,
                        report.agreed,
                        report.divergent,
                        report.refused,
                        report.unmapped,
                    ),
                )
                sequence = int(cursor.lastrowid)
                connection.commit()
                return sequence
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
            raise InboxError(
                "candidate inbox contains an invalid stored payload"
            ) from exc

    def count(self) -> int:
        """Return a content-free candidate count."""
        with closing(self._connect()) as connection:
            self._require_current_schema(connection)
            row = connection.execute(
                "SELECT COUNT(*) AS total FROM candidate_inbox"
            ).fetchone()
            return int(row["total"])

    @staticmethod
    def _apply_shadow_observation(
        connection: sqlite3.Connection,
        observation: TaskShadowObservation,
        imported_at: str,
    ) -> tuple[bool, ShadowFeedImportRefusal | None]:
        candidate = observation.candidate
        candidate_payload = _canonical_payload(candidate)
        revision = connection.execute(
            "SELECT payload_json FROM candidate_revision_history "
            "WHERE candidate_id=? AND source_revision=?",
            (candidate.candidate_id, candidate.source.revision),
        ).fetchone()
        if revision is None:
            return False, ShadowFeedImportRefusal.CANDIDATE_MISSING
        if revision["payload_json"] != candidate_payload:
            return False, ShadowFeedImportRefusal.CANDIDATE_CONFLICT

        payload = _canonical_shadow_observation(observation)
        existing = connection.execute(
            "SELECT payload_json FROM task_shadow_observations "
            "WHERE candidate_id=? AND source_revision=?",
            (candidate.candidate_id, candidate.source.revision),
        ).fetchone()
        if existing is not None:
            if existing["payload_json"] == payload:
                return False, None
            return False, ShadowFeedImportRefusal.OBSERVATION_CONFLICT

        legacy = observation.legacy_task
        if observation.disposition in {"minted", "folded"}:
            comparison = (
                "agreed"
                if legacy.comparable_digest
                == candidate_comparable_digest(candidate)
                else "divergent"
            )
        else:
            comparison = observation.disposition
        connection.execute(
            "INSERT INTO task_shadow_observations("
            "candidate_id,source_revision,disposition,legacy_task_id,"
            "comparable_digest,reason_code,comparison,payload_json,"
            "observed_at,first_imported_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
            (
                candidate.candidate_id,
                candidate.source.revision,
                observation.disposition,
                None if legacy is None else legacy.task_id,
                None if legacy is None else legacy.comparable_digest,
                observation.reason_code,
                comparison,
                payload,
                observation.observed_at,
                imported_at,
            ),
        )
        return True, None

    @staticmethod
    def _apply_candidate(connection: sqlite3.Connection,
                         candidate: TaskCandidate, payload: str,
                         imported_at: str) -> ImportResult:
        row = connection.execute(
            "SELECT source_revision,payload_json,created_at "
            "FROM candidate_inbox WHERE candidate_id=?",
            (candidate.candidate_id,),
        ).fetchone()
        history = connection.execute(
            "SELECT payload_json,created_at FROM candidate_revision_history "
            "WHERE candidate_id=? AND source_revision=?",
            (candidate.candidate_id, candidate.source.revision),
        ).fetchone()
        if row is None:
            if history is not None:
                raise InboxError("candidate revision history is inconsistent")
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
            connection.execute(
                "INSERT INTO candidate_revision_history("
                "candidate_id,source_revision,payload_json,created_at,"
                "imported_at) VALUES(?,?,?,?,?)",
                (
                    candidate.candidate_id,
                    candidate.source.revision,
                    payload,
                    candidate.created_at,
                    imported_at,
                ),
            )
            return ImportResult(ImportDisposition.INSERTED)

        if row["created_at"] != candidate.created_at:
            return ImportResult(
                ImportDisposition.REFUSED,
                ImportRefusal.CREATED_AT_CONFLICT,
            )
        if history is not None:
            if history["created_at"] != candidate.created_at:
                return ImportResult(
                    ImportDisposition.REFUSED,
                    ImportRefusal.CREATED_AT_CONFLICT,
                )
            if history["payload_json"] != payload:
                return ImportResult(
                    ImportDisposition.REFUSED,
                    ImportRefusal.REVISION_CONFLICT,
                )
            if (row["source_revision"] == candidate.source.revision
                    and row["payload_json"] != payload):
                raise InboxError("candidate inbox and history are inconsistent")
            return ImportResult(ImportDisposition.UNCHANGED)

        if row["source_revision"] == candidate.source.revision:
            raise InboxError("candidate revision history is incomplete")

        connection.execute(
            "INSERT INTO candidate_revision_history("
            "candidate_id,source_revision,payload_json,created_at,imported_at) "
            "VALUES(?,?,?,?,?)",
            (
                candidate.candidate_id,
                candidate.source.revision,
                payload,
                candidate.created_at,
                imported_at,
            ),
        )
        connection.execute(
            "UPDATE candidate_inbox SET source_revision=?,payload_json=?,"
            "updated_at=? WHERE candidate_id=?",
            (candidate.source.revision, payload, imported_at,
             candidate.candidate_id),
        )
        return ImportResult(ImportDisposition.UPDATED)

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
            raise InboxError(
                "candidate inbox database path changed during creation"
            ) from exc
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
        CandidateInbox._require_tables(connection, tuple(_SCHEMA_COLUMNS))
        for name, expected_type in _SCHEMA_OBJECTS.items():
            row = connection.execute(
                "SELECT type FROM sqlite_master WHERE name=?", (name,)
            ).fetchone()
            if row is None or row["type"] != expected_type:
                raise InboxError("candidate inbox schema is incomplete")

    @staticmethod
    def _require_tables(connection: sqlite3.Connection,
                        tables: tuple[str, ...]) -> None:
        for table in tables:
            expected_columns = _SCHEMA_COLUMNS[table]
            row = connection.execute(
                "SELECT type FROM sqlite_master WHERE name=?", (table,)
            ).fetchone()
            if row is None or row["type"] != "table":
                raise InboxError("candidate inbox schema is incomplete")
            columns = tuple(
                item["name"]
                for item in connection.execute(f"PRAGMA table_info({table})")
            )
            if columns != expected_columns:
                raise InboxError("candidate inbox schema is incomplete")

    def _now(self) -> str:
        value = self._clock()
        if value.tzinfo is None or value.utcoffset() is None:
            raise InboxError("candidate inbox clock must include a timezone")
        return value.isoformat(timespec="seconds")


def _validate_shadow_cycle_receipt(receipt: object) -> None:
    if not isinstance(receipt, ShadowImportCycleReceipt):
        raise InboxError("shadow import cycle receipt is invalid")
    stream_id = receipt.stream_id
    if not isinstance(stream_id, str) or not _STREAM_ID_RE.fullmatch(stream_id):
        raise InboxError("shadow import cycle stream ID is invalid")
    started = _receipt_timestamp(receipt.started_at)
    completed = _receipt_timestamp(receipt.completed_at)
    if completed < started:
        raise InboxError("shadow import cycle timestamps are invalid")

    counts = (
        receipt.candidate_previous_cursor,
        receipt.candidate_current_cursor,
        receipt.candidates_inserted,
        receipt.candidates_updated,
        receipt.observation_previous_cursor,
        receipt.observation_current_cursor,
        receipt.observations_inserted,
        receipt.comparison.total,
        receipt.comparison.agreed,
        receipt.comparison.divergent,
        receipt.comparison.refused,
        receipt.comparison.unmapped,
    )
    if any(isinstance(value, bool) or not isinstance(value, int)
           or not 0 <= value <= _MAX_SQLITE_INTEGER
           for value in counts):
        raise InboxError("shadow import cycle counts are invalid")
    if (receipt.candidate_current_cursor
            < receipt.candidate_previous_cursor
            or receipt.observation_current_cursor
            < receipt.observation_previous_cursor):
        raise InboxError("shadow import cycle cursors are invalid")
    report = receipt.comparison
    if report.total != (
        report.agreed + report.divergent + report.refused + report.unmapped
    ):
        raise InboxError("shadow import cycle comparison is invalid")


def _receipt_timestamp(value: object) -> datetime:
    if not isinstance(value, str) or not value or len(value) > 64:
        raise InboxError("shadow import cycle timestamp is invalid")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise InboxError("shadow import cycle timestamp is invalid") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise InboxError("shadow import cycle timestamp is invalid")
    return parsed


def _canonical_payload(candidate: TaskCandidate) -> str:
    return json.dumps(
        task_candidate_document(candidate),
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )


def _feed_digest(feed: CandidateFeed) -> str:
    document = {
        "schema": feed.schema,
        "schema_version": feed.schema_version,
        "producer": feed.producer,
        "stream_id": feed.stream_id,
        "from_cursor": feed.from_cursor,
        "to_cursor": feed.to_cursor,
        "items": [
            {
                "sequence": item.sequence,
                "candidate": task_candidate_document(item.candidate),
            }
            for item in feed.items
        ],
        "emitted_at": feed.emitted_at,
    }
    payload = json.dumps(
        document,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _canonical_shadow_observation(
    observation: TaskShadowObservation,
) -> str:
    return json.dumps(
        task_shadow_observation_document(observation),
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )


def _shadow_feed_digest(feed: TaskShadowFeed) -> str:
    payload = json.dumps(
        task_shadow_feed_document(feed),
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()
