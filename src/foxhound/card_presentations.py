"""Execution cards above surfaces: one presentation per (card, surface).

A card is a decision, and only a decision; each surface that shows it -- the
chat, the console, any later one -- keeps its own record here: whether it is
being delivered, shown, being brought up to date with the card's outcome, or
done, with its own message handle, lease and version. A surface's delivery
therefore never moves the card, and a decision taken on one surface reaches
every other surface that showed it. See ilyasst/foxhound#901.

This module holds the v72 table, the one-time backfill from the card-wide
delivery columns, and the read helpers. The service switches to it in the
items that follow #903.
"""

from __future__ import annotations

from dataclasses import dataclass, fields
from typing import Optional, Tuple
import sqlite3

_SCHEMA_V72 = (
    """
CREATE TABLE IF NOT EXISTS execution_card_presentations (
    card_id             INTEGER NOT NULL REFERENCES execution_review_cards(id),
    surface             TEXT    NOT NULL CHECK(length(surface) BETWEEN 1 AND 128),
    state               TEXT    NOT NULL CHECK(state IN ('pending','delivering','shown','updating','closed','failed')),
    version             INTEGER NOT NULL CHECK(version >= 1),
    claim_token_digest  TEXT,
    claim_expires_at    TEXT,
    transport           TEXT,
    message_ref         TEXT,
    shown_at            TEXT,
    last_presented_at   TEXT,
    outcome             TEXT CHECK(outcome IS NULL OR outcome IN ('resolved','cancelled','superseded')),
    outcome_resolution  TEXT,
    outcome_reported_at TEXT,
    created_at          TEXT    NOT NULL,
    updated_at          TEXT    NOT NULL,
    PRIMARY KEY (card_id, surface)
);
""",
    """
CREATE INDEX IF NOT EXISTS execution_card_presentations_surface_state
    ON execution_card_presentations(surface, state);
"""
)

@dataclass(frozen=True)
class CardPresentation:
    card_id: int
    surface: str
    state: str
    version: int
    claim_token_digest: Optional[str]
    claim_expires_at: Optional[str]
    transport: Optional[str]
    message_ref: Optional[str]
    shown_at: Optional[str]
    last_presented_at: Optional[str]
    outcome: Optional[str]
    outcome_resolution: Optional[str]
    outcome_reported_at: Optional[str]
    created_at: str
    updated_at: str

def _presentations(cursor: sqlite3.Cursor) -> Tuple[CardPresentation, ...]:
    # Built from the cursor's own column names, so a caller's row factory is
    # neither required nor changed.
    names = [column[0] for column in cursor.description]
    wanted = {field.name for field in fields(CardPresentation)}
    return tuple(
        CardPresentation(**{name: value for name, value in zip(names, row) if name in wanted})
        for row in cursor.fetchall()
    )


def presentations_for(connection: sqlite3.Connection, card_id: int) -> Tuple[CardPresentation, ...]:
    """Every surface's record of one card, ordered by surface."""
    return _presentations(connection.execute(
        "SELECT * FROM execution_card_presentations WHERE card_id = ? ORDER BY surface", (card_id,)
    ))


def presentations_on(connection: sqlite3.Connection, surface: str, *, states: Optional[Tuple[str, ...]] = None) -> Tuple[CardPresentation, ...]:
    """One surface's records, optionally in the given states, ordered by card."""
    query = "SELECT * FROM execution_card_presentations WHERE surface = ?"
    params: list[object] = [surface]
    if states is not None:
        placeholders = ",".join("?" for _ in states)
        query += f" AND state IN ({placeholders})"
        params.extend(states)
    query += " ORDER BY card_id"
    return _presentations(connection.execute(query, params))


def backfill_presentations(connection: sqlite3.Connection, now: str) -> None:
    """Add the card's decision columns and move delivery state into presentations.

    Runs inside the v71 -> v72 migration transaction. Card rows are left as
    they are (v1 code still reads them); only the two decision columns are
    added. Delivered and delivering cards get a presentation for the consumer
    that holds them, and every retraction still waiting becomes a presentation
    to be brought up to date, so the chat messages it names are corrected by
    the outcome pass instead of being forgotten.
    """
    columns = {row[1] for row in connection.execute("PRAGMA table_info(execution_review_cards)")}
    if "decision_version" not in columns:
        connection.execute("ALTER TABLE execution_review_cards ADD COLUMN decision_version INTEGER NOT NULL DEFAULT 1")
    if "resolved_by_surface" not in columns:
        connection.execute("ALTER TABLE execution_review_cards ADD COLUMN resolved_by_surface TEXT")

    # Every card with status IN ('delivering','delivered') and a non-null consumer_digest
    connection.execute(
        """
        INSERT INTO execution_card_presentations (
            card_id, surface, state, version, claim_token_digest, claim_expires_at,
            transport, message_ref, shown_at, last_presented_at, created_at, updated_at
        )
        SELECT
            id,
            consumer_digest,
            CASE WHEN status = 'delivering' THEN 'delivering' ELSE 'shown' END,
            1,
            claim_token_digest,
            claim_expires_at,
            transport,
            delivery_ref,
            delivered_at,
            delivered_at,
            updated_at,
            updated_at
        FROM execution_review_cards
        WHERE status IN ('delivering', 'delivered') AND consumer_digest IS NOT NULL
        """
    )
    
    # Retractions logic (state IN ('pending','delivering')):
    # Insert or update a presentation with state='updating', outcome='cancelled'|'superseded', transport, message_ref, surface
    connection.execute(
        """
        INSERT INTO execution_card_presentations (
            card_id, surface, state, version, claim_token_digest, claim_expires_at,
            transport, message_ref, shown_at, last_presented_at, outcome, created_at, updated_at
        )
        SELECT
            c.id,
            COALESCE(c.consumer_digest, 'transport:' || r.transport),
            'updating',
            1,
            NULL,
            NULL,
            r.transport,
            r.delivery_ref,
            NULL,
            NULL,
            CASE WHEN c.status = 'cancelled' THEN 'cancelled' ELSE 'superseded' END,
            c.updated_at,
            c.updated_at
        FROM execution_card_retractions r
        JOIN execution_review_cards c ON c.id = r.card_id
        WHERE r.state IN ('pending', 'delivering')
        ON CONFLICT(card_id, surface) DO UPDATE SET
            state = 'updating',
            outcome = excluded.outcome,
            transport = excluded.transport,
            message_ref = excluded.message_ref,
            updated_at = excluded.updated_at
        """
    )
