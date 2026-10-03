"""Tests for execution_card_presentations and schema v72 migration."""

from __future__ import annotations

import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path

from foxhound import CandidateInbox, migrate_database
from foxhound.candidate_inbox import SCHEMA_VERSION
from foxhound.card_presentations import presentations_for, presentations_on


class CardPresentationsTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "test.db"
        migrate_database(self.db_path)
        self.inbox = CandidateInbox(self.db_path)

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_schema_v72_created_and_version_updated(self):
        with closing(sqlite3.connect(self.db_path)) as conn:
            v = conn.execute("PRAGMA user_version").fetchone()[0]
            self.assertEqual(v, SCHEMA_VERSION)
            tables = {
                r[0]
                for r in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }
            self.assertIn("execution_card_presentations", tables)

            # Check new columns in execution_review_cards
            cols = {
                r[1]
                for r in conn.execute(
                    "PRAGMA table_info(execution_review_cards)"
                ).fetchall()
            }
            self.assertIn("decision_version", cols)
            self.assertIn("resolved_by_surface", cols)

    def test_v71_to_v72_migration(self):
        mig_dir = tempfile.TemporaryDirectory()
        try:
            mig_db = Path(mig_dir.name) / "migrate.db"
            migrate_database(mig_db)

            # Downgrade to 71 and insert test data
            with closing(sqlite3.connect(mig_db)) as conn:
                conn.execute("PRAGMA foreign_keys = OFF")
                conn.execute("DROP TABLE execution_card_presentations")
                # Drop columns created by v72
                conn.execute("ALTER TABLE execution_review_cards DROP COLUMN decision_version")
                conn.execute("ALTER TABLE execution_review_cards DROP COLUMN resolved_by_surface")
                
                # Mock a task
                for i in range(1, 6):
                    conn.execute(
                        "INSERT INTO tasks (id, status, text, owner, due, version, created_at, updated_at) "
                        "VALUES (?, 'open', 'T', 'O', NULL, 1, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')",
                        (i,)
                    )
    
                # Insert one card in each state
                def insert_card(i, status, **kwargs):
                    conn.execute(
                        f"""
                        INSERT INTO execution_review_cards (
                            id, task_id, task_version, workflow_version, kind, phase,
                            status, version, created_at, updated_at,
                            claim_token_digest, claim_expires_at, consumer_digest,
                            transport, delivery_ref, delivered_at, resolution, resolved_at, summary_only
                        ) VALUES (
                            ?, ?, 1, 1, 'start', 'plan',
                            ?, 1, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z',
                            ?, ?, ?, ?, ?, ?, ?, ?, 0
                        )
                        """,
                        (
                            i, i, status,
                            kwargs.get('claim_token_digest'), kwargs.get('claim_expires_at'),
                            kwargs.get('consumer_digest'), kwargs.get('transport'),
                            kwargs.get('delivery_ref'), kwargs.get('delivered_at'),
                            kwargs.get('resolution'), kwargs.get('resolved_at')
                        )
                    )

                digest64 = "a" * 64
                
                # Pending (1)
                insert_card(1, 'pending')
                # Delivering (2)
                insert_card(2, 'delivering', consumer_digest=digest64, claim_token_digest=digest64, claim_expires_at='2026-01-02T00:00:00Z')
                # Delivered (3)
                insert_card(3, 'delivered', consumer_digest=digest64, transport='t', delivery_ref='ref', delivered_at='2026-01-01T12:00:00Z')
                # Resolved (4)
                insert_card(4, 'resolved', consumer_digest=digest64, transport='t', delivery_ref='ref', delivered_at='2026-01-01T12:00:00Z', resolution='approve', resolved_at='2026-01-01T13:00:00Z')
                # Cancelled (5)
                insert_card(5, 'cancelled', resolved_at='2026-01-01T13:00:00Z')

                # Retractions
                conn.execute(
                    "INSERT INTO execution_card_retractions (card_id, state, transport, delivery_ref, created_at, updated_at) "
                    "VALUES (3, 'pending', 't', 'ref', '2026-01-01T12:00:00Z', '2026-01-01T12:00:00Z')"
                )
                conn.execute(
                    "INSERT INTO execution_card_retractions (card_id, state, transport, delivery_ref, created_at, updated_at) "
                    "VALUES (4, 'completed', 't', 'ref', '2026-01-01T12:00:00Z', '2026-01-01T12:00:00Z')"
                )

                conn.execute("PRAGMA user_version = 71")
                conn.commit()

            # Run migration 71 -> 72
            migrate_database(mig_db)

            with closing(sqlite3.connect(mig_db)) as conn:
                v = conn.execute("PRAGMA user_version").fetchone()[0]
                self.assertEqual(v, SCHEMA_VERSION)
                
                # Check presentations
                p2 = presentations_for(conn, 2)
                self.assertEqual(len(p2), 1)
                self.assertEqual(p2[0].state, 'delivering')
                self.assertEqual(p2[0].surface, digest64)

                p3 = presentations_for(conn, 3)
                self.assertEqual(len(p3), 1)
                self.assertEqual(p3[0].state, 'updating') # Due to pending retraction

                p4 = presentations_for(conn, 4)
                self.assertEqual(len(p4), 0)

        finally:
            mig_dir.cleanup()
