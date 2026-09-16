"""Synthetic coverage for the independent delivery-health signal."""

from __future__ import annotations

import contextlib
from contextlib import closing
from datetime import datetime, timedelta, timezone
import io
from pathlib import Path
import sqlite3
import tempfile
import unittest

from foxhound.database_lifecycle import migrate_database
from foxhound.delivery_health import (
    DeliveryHealthPolicy,
    collect_delivery_health,
    main,
)


NOW = datetime(2040, 1, 2, 12, 0, tzinfo=timezone.utc)


class DeliveryHealthTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.database = Path(self.temporary.name) / "foxhound.sqlite3"
        migrate_database(self.database)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def _insert_task_card(self, *, due: datetime) -> int:
        with closing(sqlite3.connect(self.database)) as connection, connection:
            cursor = connection.execute(
                "INSERT INTO task_review_cards("
                "task_id,task_version,status,version,due_at,created_at,updated_at"
                ") VALUES(1,1,'pending',1,?,?,?)",
                (self._time(due), self._time(due), self._time(due)),
            )
        return int(cursor.lastrowid)

    def _task_event(self, card_id: int, *, kind: str, at: datetime) -> None:
        with closing(sqlite3.connect(self.database)) as connection, connection:
            connection.execute(
                "INSERT INTO task_review_card_events("
                "card_id,task_id,kind,card_version,task_version,action,occurred_at"
                ") VALUES(?,1,?,1,1,NULL,?)",
                (card_id, kind, self._time(at)),
            )

    def _execution_event(self, *, kind: str, at: datetime) -> None:
        with closing(sqlite3.connect(self.database)) as connection, connection:
            card = connection.execute(
                "INSERT INTO execution_review_cards("
                "task_id,task_version,workflow_version,kind,phase,status,version,"
                "created_at,updated_at"
                ") VALUES(2,1,1,'start','plan','pending',1,?,?)",
                (self._time(at), self._time(at)),
            )
            connection.execute(
                "INSERT INTO execution_review_card_events("
                "card_id,task_id,kind,card_version,workflow_version,action,occurred_at"
                ") VALUES(?,2,?,1,1,NULL,?)",
                (int(card.lastrowid), kind, self._time(at)),
            )

    @staticmethod
    def _time(value: datetime) -> str:
        return value.isoformat(timespec="seconds")

    def test_empty_database_is_healthy(self) -> None:
        health = collect_delivery_health(self.database, clock=lambda: NOW)

        self.assertTrue(health.ok)
        self.assertEqual(health.alerts, ())
        self.assertEqual(health.task_cards.active, 0)
        self.assertEqual(health.execution_cards.active, 0)
        self.assertIsNone(health.last_successful_delivery_age_seconds)
        self.assertEqual(health.workflows.ready, 0)

    def test_old_pending_delivery_is_unhealthy_without_exposing_the_card(self) -> None:
        self._insert_task_card(due=NOW - timedelta(minutes=20))

        health = collect_delivery_health(self.database, clock=lambda: NOW)

        self.assertFalse(health.ok)
        self.assertEqual(
            health.alerts, ("pending_age_exceeded", "delivery_stale")
        )
        self.assertEqual(health.task_cards.oldest_pending_age_seconds, 1_200)
        self.assertNotIn("card_id", health.document())
        self.assertNotIn("task_id", health.document())

    def test_recent_delivery_failures_are_unhealthy(self) -> None:
        card_id = self._insert_task_card(due=NOW - timedelta(seconds=5))
        self._task_event(card_id, kind="delivery_failed", at=NOW - timedelta(seconds=1))
        self._task_event(card_id, kind="delivery_failed", at=NOW)

        health = collect_delivery_health(
            self.database,
            policy=DeliveryHealthPolicy(max_recent_failures=2),
            clock=lambda: NOW,
        )

        self.assertEqual(health.recent_failures, 2)
        self.assertIn("recent_delivery_failures_exceeded", health.alerts)

    def test_execution_card_events_contribute_to_delivery_health(self) -> None:
        self._execution_event(kind="delivery_failed", at=NOW)

        health = collect_delivery_health(
            self.database,
            policy=DeliveryHealthPolicy(max_recent_failures=1),
            clock=lambda: NOW,
        )

        self.assertEqual(health.execution_cards.pending, 1)
        self.assertEqual(health.recent_failures, 1)
        self.assertIn("recent_delivery_failures_exceeded", health.alerts)

    def test_recent_success_keeps_fresh_pending_delivery_healthy(self) -> None:
        card_id = self._insert_task_card(due=NOW - timedelta(seconds=20))
        self._task_event(card_id, kind="delivered", at=NOW - timedelta(seconds=10))

        health = collect_delivery_health(self.database, clock=lambda: NOW)

        self.assertTrue(health.ok)
        self.assertEqual(health.last_successful_delivery_age_seconds, 10)

    def test_unavailable_database_has_content_free_cli_output(self) -> None:
        output = io.StringIO()
        missing = self.database.parent / "missing.sqlite3"
        with contextlib.redirect_stderr(output):
            self.assertEqual(main(["--database", str(missing)]), 78)

        self.assertEqual(output.getvalue(), "foxhound delivery health: unavailable\n")
        self.assertNotIn(str(missing), output.getvalue())


if __name__ == "__main__":
    unittest.main()
