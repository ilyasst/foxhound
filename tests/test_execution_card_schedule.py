"""The scheduling consumer: cards exist without a delivery surface asking."""

from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock

from foxhound import migrate_database
from foxhound.execution_card_schedule import main, run_schedule
from foxhound.execution_cards import ExecutionCardService
from foxhound.task_execution import TaskExecutionService

NOW = datetime(2030, 3, 1, 12, 0, tzinfo=timezone.utc)


class ExecutionCardScheduleConsumerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.database = Path(self.temporary.name) / "foxhound.sqlite3"
        migrate_database(self.database)
        with closing(sqlite3.connect(self.database)) as connection:
            for task_id in (1, 2, 3):
                connection.execute(
                    "INSERT INTO tasks(id,status,text,owner,due,version,"
                    "created_at,updated_at,closed_at) "
                    "VALUES(?,'open',?,?,NULL,1,?,?,NULL)",
                    (
                        task_id,
                        f"Synthetic task {task_id}",
                        "Person A",
                        NOW.isoformat(timespec="seconds"),
                        NOW.isoformat(timespec="seconds"),
                    ),
                )
            connection.commit()
        self.execution = TaskExecutionService(self.database, clock=lambda: NOW)
        for task_id in (1, 2, 3):
            self.execution.schedule(task_id, expected_task_version=1)

    def _live_cards(self) -> int:
        with closing(sqlite3.connect(self.database)) as connection:
            return connection.execute(
                "SELECT COUNT(*) FROM execution_review_cards "
                "WHERE status IN ('pending','delivering','delivered')"
            ).fetchone()[0]

    def test_waiting_gates_get_cards_with_nothing_delivering(self) -> None:
        """No surface, no drip, no claim — and the cards exist.

        This is the whole point: a card used to appear only when a chat
        surface had room for one, so a console reader with no per-surface
        limit could be blocked by a bound belonging to somewhere else.
        """
        created, cancelled = run_schedule(database_path=self.database)
        self.assertEqual((created, cancelled), (3, 0))
        self.assertEqual(self._live_cards(), 3)

    def test_a_second_pass_creates_nothing(self) -> None:
        """Idempotent by construction: the query skips a workflow that has one.

        So this consumer and a delivering side can both ask at once without
        carding the same gate twice.
        """
        run_schedule(database_path=self.database)
        created, _ = run_schedule(database_path=self.database)
        self.assertEqual(created, 0)
        self.assertEqual(self._live_cards(), 3)

    def test_the_limit_bounds_one_pass(self) -> None:
        created, _ = run_schedule(database_path=self.database, limit=2)
        self.assertEqual(created, 2)
        self.assertEqual(self._live_cards(), 2)

    def test_cli_reports_counts_only(self) -> None:
        with mock.patch("sys.stdout") as stdout:
            self.assertEqual(
                main(["--database", str(self.database), "--limit", "100"]), 0
            )
        written = "".join(
            call.args[0] for call in stdout.write.call_args_list
        ).strip()
        self.assertEqual(
            json.loads(written), {"ok": True, "created": 3, "cancelled": 0}
        )
        # A card's text never reaches a log.
        self.assertNotIn("Synthetic task", written)

    def test_cli_failure_is_content_free(self) -> None:
        missing = Path(self.temporary.name) / "absent" / "foxhound.sqlite3"
        with mock.patch("sys.stderr") as stderr:
            self.assertEqual(main(["--database", str(missing)]), 70)
        said = "".join(call.args[0] for call in stderr.write.call_args_list)
        self.assertIn("operation failed", said)
        self.assertNotIn(str(missing), said)


if __name__ == "__main__":
    unittest.main()
