"""Synthetic tests for supported duplicate-proposal cancellation."""

from __future__ import annotations

import contextlib
import io
import json
import sqlite3
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from foxhound import migrate_database
from foxhound import task_duplicate_proposals as proposals
from foxhound.candidate_inbox import SCHEMA_VERSION
from foxhound.task_cards import TaskCardService
from foxhound.task_duplicate_supersede import main


NOW = datetime(2030, 3, 1, 12, 0, tzinfo=timezone.utc)
LATER = "2030-03-02T12:00:00+00:00"
TOKEN = "a" * 43
CONSUMER = "b" * 64


class DuplicateSupersedeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.database = Path(self.directory.name) / "foxhound.sqlite3"
        migrate_database(self.database)
        self.connection = sqlite3.connect(self.database)
        self.connection.row_factory = sqlite3.Row
        self.addCleanup(self.connection.close)
        for task_id in range(1, 7):
            self.connection.execute(
                "INSERT INTO tasks(id,status,text,version,created_at,updated_at,"
                "owner_ref_version,owner_kind,owner_speaker_id,"
                "owner_canonical_speaker_id,owner_speaker_registry_id,"
                "owner_pinned,owner_provisional) VALUES(?, 'open', ?, 1, ?, ?,"
                "1, 'person', 'SPK_1', 'SPK_1', 'registry-A', 0, 0)",
                (task_id, f"Synthetic task {task_id}", NOW.isoformat(),
                 NOW.isoformat()),
            )
        self.open = self._propose(1, 2, "detector-a")
        self.confirmed = self._propose(3, 4, "detector-a")
        self.rejected = self._propose(5, 6, "detector-b")
        proposals.settle(
            self.connection,
            proposal_id=self.confirmed,
            decision=proposals.Decision.CONFIRMED,
            actor="reader",
            now=LATER,
        )
        proposals.settle(
            self.connection,
            proposal_id=self.rejected,
            decision=proposals.Decision.REJECTED,
            actor="reader",
            now=LATER,
        )
        self.connection.commit()
        self.cards = TaskCardService(
            self.database, clock=lambda: NOW, token_factory=lambda: TOKEN
        )

    def _propose(self, left: int, right: int, detector: str) -> int:
        result = proposals.propose(
            self.connection,
            task_id_a=left,
            task_id_b=right,
            basis="Synthetic shared deliverable.",
            detector=detector,
            now=NOW.isoformat(),
        )
        self.assertIsNotNone(result.proposal_id)
        return int(result.proposal_id)

    def _states(self) -> dict[int, str]:
        return {
            int(row["id"]): str(row["state"])
            for row in self.connection.execute(
                "SELECT id,state FROM task_duplicate_proposals"
            )
        }

    def test_dry_run_is_default_and_writes_nothing(self) -> None:
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            code = main(["--database", str(self.database)])
        self.assertEqual(code, 0)
        self.assertEqual(json.loads(output.getvalue()), {
            "accepted": True,
            "applied": False,
            "cards_cancelled": 0,
            "cards_matched": 0,
            "matched": 1,
            "superseded": 0,
        })
        self.assertEqual(
            self._states(),
            {self.open: "proposed", self.confirmed: "confirmed",
             self.rejected: "rejected"},
        )

    def test_apply_preserves_answers_withdraws_card_and_is_idempotent(self) -> None:
        self.cards.schedule_duplicate_proposals()
        claim = self.cards.claim_next(consumer_digest=CONSUMER)
        self.assertIsNotNone(claim)
        self.assertTrue(self.cards.complete_delivery(
            claim.card.id,
            expected_version=claim.card.version,
            claim_token=claim.token,
            transport="synthetic",
            delivery_ref="message-1",
        ).accepted)

        result = self.cards.supersede_duplicate_proposals(apply=True)
        self.assertEqual(
            (result.matched, result.superseded, result.cards_cancelled),
            (1, 1, 1),
        )
        self.assertEqual(result.cards_matched, 1)
        self.assertEqual(
            self._states(),
            {self.open: "superseded", self.confirmed: "confirmed",
             self.rejected: "rejected"},
        )
        event = self.connection.execute(
            "SELECT kind,actor,reason FROM task_duplicate_proposal_events "
            "WHERE proposal_id=? ORDER BY sequence DESC LIMIT 1",
            (self.open,),
        ).fetchone()
        self.assertEqual(tuple(event), (
            "superseded", "operator", "duplicate proposal queue reset"
        ))
        status = self.connection.execute(
            "SELECT status FROM task_review_cards WHERE id=?", (claim.card.id,)
        ).fetchone()[0]
        self.assertEqual(status, "cancelled")

        again = self.cards.supersede_duplicate_proposals(apply=True)
        self.assertEqual((again.matched, again.superseded), (0, 0))
        events = self.connection.execute(
            "SELECT count(*) FROM task_duplicate_proposal_events "
            "WHERE proposal_id=? AND kind='superseded'", (self.open,)
        ).fetchone()[0]
        self.assertEqual(events, 1)

        replacement = self._propose(1, 2, "detector-a")
        self.assertNotEqual(replacement, self.open)

    def test_detector_limits_scope(self) -> None:
        second = self._propose(1, 3, "detector-b")
        self.connection.commit()
        result = self.cards.supersede_duplicate_proposals(
            detector="detector-b", apply=True
        )
        self.assertEqual((result.matched, result.superseded), (1, 1))
        self.assertEqual(self._states()[self.open], "proposed")
        self.assertEqual(self._states()[second], "superseded")

    def test_version_sixty_event_ledger_migrates_without_losing_history(self) -> None:
        self._rebuild_version_sixty_ledger()
        before = self.connection.execute(
            "SELECT count(*) FROM task_duplicate_proposal_events"
        ).fetchone()[0]

        migrate_database(self.database)

        version = self.connection.execute("PRAGMA user_version").fetchone()[0]
        after = self.connection.execute(
            "SELECT count(*) FROM task_duplicate_proposal_events"
        ).fetchone()[0]
        columns = {
            row[1] for row in self.connection.execute(
                "PRAGMA table_info(task_duplicate_proposal_events)"
            )
        }
        self.assertEqual((version, before, after), (SCHEMA_VERSION, before, before))
        self.assertIn("reason", columns)

    def test_migration_records_proposals_superseded_before_events_existed(self) -> None:
        """Version 60's scheduler expired proposals with a bare state update.

        Those rows are `superseded` with no event saying so, and after 61 the
        ledger is append-only, so the migration is the only chance to record
        them. Each gets exactly one event; nothing else changes.
        """
        self.connection.execute(
            "UPDATE task_duplicate_proposals SET state='superseded',"
            "settled_at='2030-01-02T00:00:00+00:00' WHERE id=?",
            (self.open,),
        )
        self.connection.commit()
        self._rebuild_version_sixty_ledger()

        migrate_database(self.database)

        rows = self.connection.execute(
            "SELECT proposal_id,actor,reason,occurred_at "
            "FROM task_duplicate_proposal_events WHERE kind='superseded'"
        ).fetchall()
        self.assertEqual(len(rows), 1)
        proposal_id, actor, reason, occurred_at = rows[0]
        self.assertEqual(proposal_id, self.open)
        self.assertEqual(actor, "schema-migration-61")
        self.assertTrue(reason)
        self.assertEqual(occurred_at, "2030-01-02T00:00:00+00:00")
        self.assertEqual(
            self._states(),
            {self.open: "superseded", self.confirmed: "confirmed",
             self.rejected: "rejected"},
        )

        # Running the migration again adds nothing.
        migrate_database(self.database)
        again = self.connection.execute(
            "SELECT count(*) FROM task_duplicate_proposal_events "
            "WHERE kind='superseded'"
        ).fetchone()[0]
        self.assertEqual(again, 1)

    def _rebuild_version_sixty_ledger(self) -> None:
        """Put the event ledger back in its pre-61 shape, at version 60."""
        self.connection.execute(
            "DROP TRIGGER task_duplicate_proposal_events_no_update"
        )
        self.connection.execute(
            "DROP TRIGGER task_duplicate_proposal_events_no_delete"
        )
        self.connection.execute(
            "ALTER TABLE task_duplicate_proposal_events "
            "RENAME TO task_duplicate_proposal_events_v61"
        )
        self.connection.execute("""
            CREATE TABLE task_duplicate_proposal_events (
                sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                proposal_id INTEGER NOT NULL,
                kind TEXT NOT NULL CHECK(kind IN (
                    'proposed','confirmed','rejected','reopened'
                )),
                actor TEXT NOT NULL CHECK(length(actor) BETWEEN 1 AND 200),
                occurred_at TEXT NOT NULL,
                FOREIGN KEY(proposal_id) REFERENCES task_duplicate_proposals(id)
            )
        """)
        self.connection.execute(
            "INSERT INTO task_duplicate_proposal_events("
            "sequence,proposal_id,kind,actor,occurred_at) "
            "SELECT sequence,proposal_id,kind,actor,occurred_at "
            "FROM task_duplicate_proposal_events_v61"
        )
        self.connection.execute("DROP TABLE task_duplicate_proposal_events_v61")
        self.connection.execute("""
            CREATE TRIGGER task_duplicate_proposal_events_no_update
            BEFORE UPDATE ON task_duplicate_proposal_events
            BEGIN
                SELECT RAISE(ABORT, 'task duplicate proposal events are append-only');
            END
        """)
        self.connection.execute("""
            CREATE TRIGGER task_duplicate_proposal_events_no_delete
            BEFORE DELETE ON task_duplicate_proposal_events
            BEGIN
                SELECT RAISE(ABORT, 'task duplicate proposal events are append-only');
            END
        """)
        self.connection.execute("PRAGMA user_version = 60")
        self.connection.commit()


if __name__ == "__main__":
    unittest.main()
