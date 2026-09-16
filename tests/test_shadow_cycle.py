from __future__ import annotations

import fcntl
import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock

from foxhound import CandidateInbox, migrate_database
from foxhound.shadow_cycle import ShadowCycleError, run_cycle
from foxhound.task_shadow_feed_import import TaskShadowFeedImportError


FIXTURES = Path(__file__).parent / "fixtures" / "contracts"
SOURCE_ROOT = Path(__file__).parents[1] / "src"
NOW = datetime(2030, 3, 1, 12, 0, tzinfo=timezone.utc)


def fixture(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def canonical_bytes(document: object) -> bytes:
    return (
        json.dumps(
            document,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")


def private_dir(parent: Path, name: str) -> Path:
    path = parent / name
    path.mkdir(mode=0o700)
    path.chmod(0o700)
    return path


class ShadowCycleTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.candidates = private_dir(self.root, "candidate-outbox")
        self.observations = private_dir(self.root, "observation-outbox")
        self.state = private_dir(self.root, "foxhound-state")
        self.database = self.state / "foxhound.sqlite3"
        self._initialize_outbox(self.candidates, ".candidate-feed.lock")
        self._initialize_outbox(
            self.observations, ".task-shadow-feed.lock"
        )

    @staticmethod
    def _initialize_outbox(path: Path, lock_name: str) -> None:
        lock = path / lock_name
        lock.write_bytes(b"")
        lock.chmod(0o600)

    @staticmethod
    def _write_page(outbox: Path, document: dict) -> None:
        start = document["from_cursor"] + 1
        end = document["to_cursor"]
        page = outbox / f"page-{start:020d}-{end:020d}.json"
        page.write_bytes(canonical_bytes(document))
        page.chmod(0o600)

    def _populate(self) -> None:
        migrate_database(self.database)
        self._write_page(
            self.candidates, fixture("candidate-feed-page-v1.json")
        )
        self._write_page(
            self.observations,
            fixture("task-shadow-observation-feed-page-v1.json"),
        )

    def _run(self):
        return run_cycle(
            candidate_outbox_dir=self.candidates,
            observation_outbox_dir=self.observations,
            database_path=self.database,
            stream_id="primary",
            clock=lambda: NOW,
        )

    def _receipt_count(self) -> int:
        with closing(sqlite3.connect(self.database)) as connection:
            return int(connection.execute(
                "SELECT COUNT(*) FROM shadow_import_cycles"
            ).fetchone()[0])

    def test_imports_in_order_and_appends_immutable_success_receipts(self):
        self._populate()

        first = self._run()

        self.assertEqual(first.candidate_current_cursor, 2)
        self.assertEqual(first.observation_current_cursor, 2)
        self.assertEqual(first.candidates_inserted, 2)
        self.assertEqual(first.observations_inserted, 2)
        self.assertEqual(first.receipt_sequence, 1)
        self.assertEqual(self._receipt_count(), 1)
        with closing(sqlite3.connect(self.database)) as connection:
            receipt = connection.execute(
                "SELECT stream_id,candidate_previous_cursor,"
                "candidate_current_cursor,observation_previous_cursor,"
                "observation_current_cursor,candidates_inserted,"
                "observations_inserted,comparison_total "
                "FROM shadow_import_cycles WHERE sequence=1"
            ).fetchone()
        self.assertEqual(
            receipt,
            ("primary", 0, 2, 0, 2, 2, 2, 2),
        )

        replay = self._run()
        self.assertEqual(replay.candidate_disposition, "unchanged")
        self.assertEqual(replay.observation_disposition, "unchanged")
        self.assertEqual(replay.receipt_sequence, 2)
        self.assertEqual(self._receipt_count(), 2)

        with closing(sqlite3.connect(self.database)) as connection:
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute(
                    "UPDATE shadow_import_cycles SET stream_id='changed' "
                    "WHERE sequence=1"
                )
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute(
                    "DELETE FROM shadow_import_cycles WHERE sequence=1"
                )

    def test_partial_failure_has_no_false_receipt_and_retry_recovers(self):
        self._populate()
        with mock.patch(
            "foxhound.shadow_cycle.task_shadow_feed_import.import_outbox",
            side_effect=TaskShadowFeedImportError("synthetic failure"),
        ):
            with self.assertRaisesRegex(ShadowCycleError, "cycle failed"):
                self._run()

        inbox = CandidateInbox(self.database)
        self.assertEqual(inbox.feed_cursor("gw", "primary"), 2)
        self.assertEqual(inbox.shadow_feed_cursor("gw", "primary"), 0)
        self.assertEqual(self._receipt_count(), 0)

        recovered = self._run()
        self.assertEqual(recovered.candidate_disposition, "unchanged")
        self.assertEqual(recovered.observation_disposition, "imported")
        self.assertEqual(recovered.receipt_sequence, 1)
        self.assertEqual(self._receipt_count(), 1)

    def test_overlap_is_refused_without_touching_database(self):
        lock = self.state / ".foxhound-shadow-cycle.lock"
        lock.write_bytes(b"")
        lock.chmod(0o600)
        with lock.open("r+b") as handle:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            with self.assertRaisesRegex(ShadowCycleError, "already running"):
                self._run()
        self.assertFalse(self.database.exists())

    def test_unsafe_or_aliased_locations_are_refused(self):
        with self.assertRaisesRegex(ShadowCycleError, "distinct"):
            run_cycle(
                candidate_outbox_dir=self.candidates,
                observation_outbox_dir=self.candidates,
                database_path=self.database,
                stream_id="primary",
            )
        self.assertFalse(self.database.exists())

        nested_database = self.candidates / "foxhound.sqlite3"
        with self.assertRaisesRegex(ShadowCycleError, "outside"):
            run_cycle(
                candidate_outbox_dir=self.candidates,
                observation_outbox_dir=self.observations,
                database_path=nested_database,
                stream_id="primary",
            )
        self.assertFalse(nested_database.exists())

        alias = self.root / "candidate-alias"
        alias.symlink_to(self.candidates, target_is_directory=True)
        with self.assertRaisesRegex(ShadowCycleError, "unsafe"):
            run_cycle(
                candidate_outbox_dir=alias,
                observation_outbox_dir=self.observations,
                database_path=self.database,
                stream_id="primary",
            )

    def test_empty_ledgers_and_cli_output_are_aggregate_only(self):
        migrate_database(self.database)
        environment = dict(os.environ)
        environment["PYTHONPATH"] = str(SOURCE_ROOT)
        process = subprocess.run(
            [
                sys.executable,
                "-m",
                "foxhound.shadow_cycle",
                "--candidate-outbox",
                str(self.candidates),
                "--observation-outbox",
                str(self.observations),
                "--database",
                str(self.database),
                "--stream-id",
                "primary",
            ],
            env=environment,
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertEqual(process.returncode, 0, process.stderr)
        output = json.loads(process.stdout)
        self.assertEqual(output["candidate"]["current_cursor"], 0)
        self.assertEqual(output["observation"]["current_cursor"], 0)
        self.assertEqual(output["receipt_sequence"], 1)
        self.assertNotIn(str(self.root), process.stdout)
        self.assertNotIn("primary", process.stdout)

        refused = subprocess.run(
            [
                sys.executable,
                "-m",
                "foxhound.shadow_cycle",
                "--candidate-outbox",
                str(self.candidates),
                "--observation-outbox",
                str(self.candidates),
                "--database",
                str(self.database),
                "--stream-id",
                "private-stream-name",
            ],
            env=environment,
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertEqual(refused.returncode, 1)
        self.assertEqual(refused.stderr.strip(), "Foxhound shadow cycle failed")
        self.assertNotIn(str(self.root), refused.stderr)
        self.assertNotIn("private-stream-name", refused.stderr)


if __name__ == "__main__":
    unittest.main()
