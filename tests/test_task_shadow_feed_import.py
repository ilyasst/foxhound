from __future__ import annotations

from foxhound import migrate_database

import copy
import fcntl
import hashlib
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from foxhound import CandidateInbox, ShadowComparisonReport
from foxhound.task_shadow_feed_import import (
    TaskShadowFeedImportError,
    TaskShadowImportDisposition,
    import_outbox,
)


FIXTURES = Path(__file__).parent / "fixtures" / "contracts"
SOURCE_ROOT = Path(__file__).parents[1] / "src"


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


def split_pages() -> tuple[dict, dict]:
    document = fixture("task-shadow-observation-feed-page-v1.json")
    first = copy.deepcopy(document)
    first["to_cursor"] = 1
    first["items"] = first["items"][:1]
    second = copy.deepcopy(document)
    second["from_cursor"] = 1
    second["items"] = second["items"][1:]
    second["items"][0]["sequence"] = 2
    return first, second


class TaskShadowFeedImportTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.outbox = private_dir(self.root, "producer-outbox")
        self.state = private_dir(self.root, "foxhound-state")
        self.database = self.state / "candidate-inbox.sqlite3"
        self.lock = self.outbox / ".task-shadow-feed.lock"
        self.lock.write_bytes(b"")
        self.lock.chmod(0o600)

    def seed_candidates(self, *, both: bool = True) -> CandidateInbox:
        inbox = CandidateInbox(self.database)
        migrate_database(inbox.database_path)
        if both:
            result = inbox.import_feed(
                fixture("candidate-feed-page-v1.json")
            )
        else:
            result = inbox.import_document(
                fixture("meeting-candidate-v1.json")
            )
        self.assertTrue(result.accepted)
        return inbox

    def write_page(self, document: dict, *, canonical: bool = True) -> Path:
        start = document["from_cursor"] + 1
        end = document["to_cursor"]
        path = self.outbox / f"page-{start:020d}-{end:020d}.json"
        if canonical:
            path.write_bytes(canonical_bytes(document))
        else:
            path.write_text(json.dumps(document, indent=2), encoding="utf-8")
        path.chmod(0o600)
        return path

    def import_once(self):
        return import_outbox(
            outbox_dir=self.outbox,
            database_path=self.database,
            stream_id="primary",
        )

    def test_imports_complete_ledger_without_changing_outbox(self):
        self.seed_candidates()
        first, second = split_pages()
        self.write_page(first)
        self.write_page(second)
        before = self._outbox_state()

        result = self.import_once()

        self.assertEqual(
            result.disposition, TaskShadowImportDisposition.IMPORTED
        )
        self.assertEqual((result.previous_cursor, result.current_cursor), (0, 2))
        self.assertEqual((result.pages_seen, result.pages_applied), (2, 2))
        self.assertEqual(
            (result.pages_replayed, result.observations_inserted), (0, 2)
        )
        self.assertEqual(
            result.comparison,
            ShadowComparisonReport(
                total=2, agreed=1, divergent=0, refused=0, unmapped=1
            ),
        )
        self.assertEqual(self._outbox_state(), before)

    def test_retry_and_restart_are_idempotent(self):
        self.seed_candidates()
        first, second = split_pages()
        self.write_page(first)
        self.write_page(second)
        self.import_once()

        result = self.import_once()

        self.assertEqual(
            result.disposition, TaskShadowImportDisposition.UNCHANGED
        )
        self.assertEqual((result.pages_applied, result.pages_replayed), (0, 2))
        reopened = CandidateInbox(self.database)
        self.assertEqual(reopened.shadow_report(), result.comparison)

    def test_invalid_chain_is_refused_before_database_creation(self):
        _, second = split_pages()
        self.write_page(second)

        with self.assertRaisesRegex(TaskShadowFeedImportError, "contiguous"):
            self.import_once()

        self.assertFalse(self.database.exists())

    def test_noncanonical_and_incomplete_pages_are_refused(self):
        first, _ = split_pages()
        self.write_page(first, canonical=False)
        with self.assertRaisesRegex(TaskShadowFeedImportError, "cannot be read"):
            self.import_once()
        self.assertFalse(self.database.exists())

        for entry in self.outbox.glob("page-*.json"):
            entry.unlink()
        temporary = self.outbox / ".task-shadow-feed-tmp-example"
        temporary.write_text("synthetic", encoding="utf-8")
        temporary.chmod(0o600)
        with self.assertRaisesRegex(TaskShadowFeedImportError, "incomplete"):
            self.import_once()
        self.assertFalse(self.database.exists())

    def test_wrong_stream_filename_and_unknown_entry_are_refused(self):
        first, _ = split_pages()
        first["stream_id"] = "another-stream"
        path = self.write_page(first)
        with self.assertRaisesRegex(TaskShadowFeedImportError, "stream"):
            self.import_once()

        path.unlink()
        first["stream_id"] = "primary"
        wrong = self.outbox / "page-00000000000000000002-00000000000000000002.json"
        wrong.write_bytes(canonical_bytes(first))
        wrong.chmod(0o600)
        with self.assertRaisesRegex(TaskShadowFeedImportError, "filename"):
            self.import_once()

        wrong.unlink()
        unknown = self.outbox / "unexpected.txt"
        unknown.write_text("synthetic", encoding="utf-8")
        unknown.chmod(0o600)
        with self.assertRaisesRegex(TaskShadowFeedImportError, "unrecognized"):
            self.import_once()
        self.assertFalse(self.database.exists())

    def test_repeated_observation_revision_is_refused_before_import(self):
        first, _ = split_pages()
        self.write_page(first)
        repeated = copy.deepcopy(first)
        repeated["from_cursor"] = 1
        repeated["to_cursor"] = 2
        repeated["items"][0]["sequence"] = 2
        self.write_page(repeated)

        with self.assertRaisesRegex(TaskShadowFeedImportError, "repeats"):
            self.import_once()

        self.assertFalse(self.database.exists())

    def test_lock_contention_does_not_touch_database(self):
        first, _ = split_pages()
        self.write_page(first)
        with self.lock.open("rb") as handle:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            with self.assertRaisesRegex(TaskShadowFeedImportError, "updated"):
                self.import_once()
        self.assertFalse(self.database.exists())

    def test_unsafe_locations_and_page_permissions_are_refused(self):
        first, _ = split_pages()
        page = self.write_page(first)
        page.chmod(0o640)
        with self.assertRaisesRegex(TaskShadowFeedImportError, "cannot be read"):
            self.import_once()
        page.chmod(0o600)

        self.outbox.chmod(0o750)
        with self.assertRaisesRegex(TaskShadowFeedImportError, "unsafe"):
            self.import_once()
        self.outbox.chmod(0o700)

        self.state.chmod(0o750)
        with self.assertRaisesRegex(TaskShadowFeedImportError, "unsafe"):
            self.import_once()
        self.state.chmod(0o700)

        with self.assertRaisesRegex(TaskShadowFeedImportError, "unsafe"):
            import_outbox(
                outbox_dir=Path("relative-outbox"),
                database_path=self.database,
                stream_id="primary",
            )
        with self.assertRaisesRegex(TaskShadowFeedImportError, "outside"):
            import_outbox(
                outbox_dir=self.outbox,
                database_path=self.outbox / "inbox.sqlite3",
                stream_id="primary",
            )

        git_tree = private_dir(self.root, "synthetic-worktree")
        git_marker = private_dir(git_tree, ".git")
        (git_marker / "HEAD").write_text(
            "ref: refs/heads/example\n", encoding="utf-8"
        )
        git_outbox = private_dir(git_tree, "outbox")
        git_lock = git_outbox / ".task-shadow-feed.lock"
        git_lock.write_bytes(b"")
        git_lock.chmod(0o600)
        with self.assertRaisesRegex(TaskShadowFeedImportError, "unsafe"):
            import_outbox(
                outbox_dir=git_outbox,
                database_path=self.database,
                stream_id="primary",
            )

    def test_symbolic_link_page_is_refused(self):
        first, _ = split_pages()
        outside = self.root / "outside-page.json"
        outside.write_bytes(canonical_bytes(first))
        outside.chmod(0o600)
        page = self.outbox / "page-00000000000000000001-00000000000000000001.json"
        page.symlink_to(outside)

        with self.assertRaisesRegex(TaskShadowFeedImportError, "cannot be read"):
            self.import_once()

        self.assertFalse(self.database.exists())

    def test_later_refusal_preserves_the_committed_prefix(self):
        self.seed_candidates(both=False)
        first, second = split_pages()
        self.write_page(first)
        self.write_page(second)

        with self.assertRaisesRegex(TaskShadowFeedImportError, "refused"):
            self.import_once()

        reopened = CandidateInbox(self.database)
        self.assertEqual(reopened.shadow_feed_cursor("gw", "primary"), 1)
        self.assertEqual(
            reopened.shadow_report(),
            ShadowComparisonReport(
                total=1, agreed=1, divergent=0, refused=0, unmapped=0
            ),
        )

    def test_cli_outputs_aggregate_status_and_generic_failure(self):
        self.seed_candidates()
        first, _ = split_pages()
        self.write_page(first)
        environment = os.environ.copy()
        environment["PYTHONPATH"] = str(SOURCE_ROOT)
        environment["PYTHONDONTWRITEBYTECODE"] = "1"
        command = [
            sys.executable,
            "-m",
            "foxhound.task_shadow_feed_import",
            "--outbox",
            str(self.outbox),
            "--database",
            str(self.database),
            "--stream-id",
            "primary",
        ]

        process = subprocess.run(
            command, check=False, capture_output=True, text=True,
            env=environment,
        )

        self.assertEqual(process.returncode, 0)
        status = json.loads(process.stdout)
        self.assertEqual(status["current_cursor"], 1)
        self.assertEqual(status["comparison"]["agreed"], 1)
        self.assertNotIn("candidate", process.stdout)

        page = next(self.outbox.glob("page-*.json"))
        page.chmod(0o640)
        failed = subprocess.run(
            command, check=False, capture_output=True, text=True,
            env=environment,
        )
        self.assertEqual(failed.returncode, 1)
        self.assertEqual(failed.stderr.strip(), "task shadow feed import failed")
        self.assertEqual(failed.stdout, "")

    def _outbox_state(self) -> tuple[tuple[str, int, str], ...]:
        state = []
        for path in sorted(self.outbox.iterdir()):
            payload = path.read_bytes()
            state.append((
                path.name,
                path.stat().st_mode & 0o777,
                hashlib.sha256(payload).hexdigest(),
            ))
        return tuple(state)


if __name__ == "__main__":
    unittest.main()
