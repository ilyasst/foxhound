from __future__ import annotations

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

from foxhound import CandidateInbox
from foxhound.candidate_feed_import import (
    CandidateFeedImportError,
    ShadowImportDisposition,
    import_outbox,
)
from foxhound.contracts import task_candidate_document


FIXTURES = Path(__file__).parent / "fixtures" / "contracts"
SOURCE_ROOT = Path(__file__).parents[1] / "src"


def fixture(name: str = "candidate-feed-page-v1.json") -> dict:
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
    document = fixture()
    first = copy.deepcopy(document)
    first["to_cursor"] = 1
    first["items"] = first["items"][:1]
    second = copy.deepcopy(document)
    second["from_cursor"] = 1
    second["items"] = second["items"][1:]
    second["items"][0]["sequence"] = 2
    return first, second


class CandidateFeedShadowImportTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.outbox = private_dir(self.root, "producer-outbox")
        self.state = private_dir(self.root, "foxhound-state")
        self.database = self.state / "candidate-inbox.sqlite3"
        self.lock = self.outbox / ".candidate-feed.lock"
        self.lock.write_bytes(b"")
        self.lock.chmod(0o600)

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
        first, second = split_pages()
        self.write_page(first)
        self.write_page(second)
        before = self._outbox_state()

        result = self.import_once()

        self.assertEqual(result.disposition, ShadowImportDisposition.IMPORTED)
        self.assertEqual((result.previous_cursor, result.current_cursor), (0, 2))
        self.assertEqual((result.pages_seen, result.pages_applied), (2, 2))
        self.assertEqual((result.pages_replayed, result.candidates_inserted),
                         (0, 2))
        self.assertEqual(self._outbox_state(), before)
        inbox = CandidateInbox(self.database)
        self.assertEqual(inbox.count(), 2)
        self.assertEqual(inbox.feed_cursor("gw", "primary"), 2)

    def test_retry_and_restart_replay_without_logical_changes(self):
        first, second = split_pages()
        self.write_page(first)
        self.write_page(second)
        self.import_once()
        inbox = CandidateInbox(self.database)
        before = (inbox.count(), inbox.feed_cursor("gw", "primary"))

        result = self.import_once()

        self.assertEqual(result.disposition, ShadowImportDisposition.UNCHANGED)
        self.assertEqual((result.pages_applied, result.pages_replayed), (0, 2))
        reopened = CandidateInbox(self.database)
        self.assertEqual(
            (reopened.count(), reopened.feed_cursor("gw", "primary")), before
        )

    def test_new_revision_updates_the_same_candidate(self):
        first, _ = split_pages()
        first_path = self.write_page(first)
        self.import_once()
        revised = copy.deepcopy(first)
        revised["from_cursor"] = 1
        revised["to_cursor"] = 2
        revised["items"][0]["sequence"] = 2
        revised["items"][0]["candidate"]["source"]["revision"] = "f" * 64
        revised["items"][0]["candidate"]["task"]["text"] = (
            "Prepare the revised Project Alpha summary"
        )
        self.write_page(revised)

        result = self.import_once()

        self.assertEqual((result.pages_replayed, result.pages_applied), (1, 1))
        self.assertEqual((result.candidates_updated, result.current_cursor), (1, 2))
        self.assertTrue(first_path.is_file())
        inbox = CandidateInbox(self.database)
        self.assertEqual(inbox.count(), 1)

    def test_invalid_complete_ledger_does_not_create_database(self):
        _, second = split_pages()
        self.write_page(second)

        with self.assertRaisesRegex(
                CandidateFeedImportError, "not contiguous"):
            self.import_once()

        self.assertFalse(self.database.exists())

    def test_noncanonical_page_is_refused_before_database_creation(self):
        first, second = split_pages()
        self.write_page(first)
        self.write_page(second, canonical=False)

        with self.assertRaisesRegex(CandidateFeedImportError, "not canonical"):
            self.import_once()

        self.assertFalse(self.database.exists())

    def test_wrong_stream_and_filename_are_refused(self):
        first, _ = split_pages()
        first["stream_id"] = "another-stream"
        path = self.write_page(first)
        with self.assertRaisesRegex(CandidateFeedImportError, "stream"):
            self.import_once()
        self.assertFalse(self.database.exists())

        path.unlink()
        first["stream_id"] = "primary"
        wrong = self.outbox / "page-00000000000000000002-00000000000000000002.json"
        wrong.write_bytes(canonical_bytes(first))
        wrong.chmod(0o600)
        with self.assertRaisesRegex(CandidateFeedImportError, "filename"):
            self.import_once()
        self.assertFalse(self.database.exists())

    def test_unsafe_page_and_unrecognized_entries_are_refused(self):
        first, _ = split_pages()
        page = self.write_page(first)
        page.chmod(0o640)
        with self.assertRaisesRegex(CandidateFeedImportError, "unsafe"):
            self.import_once()
        self.assertFalse(self.database.exists())

        page.chmod(0o600)
        unknown = self.outbox / "unexpected.txt"
        unknown.write_text("synthetic", encoding="utf-8")
        unknown.chmod(0o600)
        with self.assertRaisesRegex(CandidateFeedImportError, "unrecognized"):
            self.import_once()
        self.assertFalse(self.database.exists())

        unknown.unlink()
        temporary = self.outbox / ".candidate-feed-tmp-example"
        temporary.write_text("synthetic", encoding="utf-8")
        temporary.chmod(0o600)
        with self.assertRaisesRegex(CandidateFeedImportError, "incomplete"):
            self.import_once()
        self.assertFalse(self.database.exists())

    def test_symbolic_link_page_is_refused(self):
        first, _ = split_pages()
        outside = self.root / "outside-page.json"
        outside.write_bytes(canonical_bytes(first))
        outside.chmod(0o600)
        page = self.outbox / "page-00000000000000000001-00000000000000000001.json"
        page.symlink_to(outside)

        with self.assertRaisesRegex(CandidateFeedImportError, "cannot be opened"):
            self.import_once()

        self.assertFalse(self.database.exists())

    def test_exporter_lock_contention_does_not_create_database(self):
        first, _ = split_pages()
        self.write_page(first)
        with self.lock.open("rb") as lock_handle:
            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            with self.assertRaisesRegex(CandidateFeedImportError, "updated"):
                self.import_once()

        self.assertFalse(self.database.exists())

    def test_unsafe_locations_are_refused(self):
        first, _ = split_pages()
        self.write_page(first)
        with self.assertRaisesRegex(CandidateFeedImportError, "absolute"):
            import_outbox(
                outbox_dir=Path("relative-outbox"),
                database_path=self.database,
                stream_id="primary",
            )
        with self.assertRaisesRegex(CandidateFeedImportError, "absolute"):
            import_outbox(
                outbox_dir=self.outbox,
                database_path=Path("relative.sqlite3"),
                stream_id="primary",
            )

        self.outbox.chmod(0o750)
        with self.assertRaisesRegex(CandidateFeedImportError, "group"):
            self.import_once()
        self.outbox.chmod(0o700)

        self.state.chmod(0o750)
        with self.assertRaisesRegex(CandidateFeedImportError, "group"):
            self.import_once()
        self.state.chmod(0o700)

        with self.assertRaisesRegex(CandidateFeedImportError, "outside"):
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
        git_lock = git_outbox / ".candidate-feed.lock"
        git_lock.write_bytes(b"")
        git_lock.chmod(0o600)
        with self.assertRaisesRegex(CandidateFeedImportError, "Git worktree"):
            import_outbox(
                outbox_dir=git_outbox,
                database_path=self.database,
                stream_id="primary",
            )

    def test_repeated_revision_is_refused_before_database_creation(self):
        first, _ = split_pages()
        self.write_page(first)
        repeated = copy.deepcopy(first)
        repeated["from_cursor"] = 1
        repeated["to_cursor"] = 2
        repeated["items"][0]["sequence"] = 2
        self.write_page(repeated)

        with self.assertRaisesRegex(CandidateFeedImportError, "repeats"):
            self.import_once()

        self.assertFalse(self.database.exists())

    def test_database_refusal_preserves_only_the_committed_prefix(self):
        meeting = fixture("meeting-candidate-v1.json")
        inbox = CandidateInbox(self.database)
        inbox.initialize()
        self.assertTrue(inbox.import_document(meeting).accepted)

        base = fixture()
        email = copy.deepcopy(base["items"][1]["candidate"])
        page_one = copy.deepcopy(base)
        page_one["to_cursor"] = 1
        page_one["items"] = [{"sequence": 1, "candidate": email}]
        self.write_page(page_one)

        conflicting = copy.deepcopy(meeting)
        conflicting["task"]["text"] = "Contradictory synthetic action"
        page_two = copy.deepcopy(base)
        page_two["from_cursor"] = 1
        page_two["to_cursor"] = 2
        page_two["items"] = [{"sequence": 2, "candidate": conflicting}]
        self.write_page(page_two)

        later = copy.deepcopy(meeting)
        later["source"]["revision"] = "e" * 64
        later["task"]["text"] = "Prepare a later Project Alpha summary"
        page_three = copy.deepcopy(base)
        page_three["from_cursor"] = 2
        page_three["to_cursor"] = 3
        page_three["items"] = [{"sequence": 3, "candidate": later}]
        self.write_page(page_three)

        with self.assertRaisesRegex(CandidateFeedImportError, "refused"):
            self.import_once()

        reopened = CandidateInbox(self.database)
        self.assertEqual(reopened.feed_cursor("gw", "primary"), 1)
        self.assertEqual(reopened.count(), 2)
        stored = reopened.get(meeting["candidate_id"])
        self.assertEqual(task_candidate_document(stored), meeting)

    def test_cli_outputs_only_aggregate_status_and_generic_failure(self):
        first, _ = split_pages()
        self.write_page(first)
        environment = os.environ.copy()
        environment["PYTHONPATH"] = str(SOURCE_ROOT)
        environment["PYTHONDONTWRITEBYTECODE"] = "1"
        command = [
            sys.executable,
            "-m",
            "foxhound.candidate_feed_import",
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
        self.assertNotIn(str(self.outbox), process.stdout)
        candidate_id = first["items"][0]["candidate"]["candidate_id"]
        self.assertNotIn(candidate_id, process.stdout)

        unknown = self.outbox / "unexpected.txt"
        unknown.write_text("synthetic", encoding="utf-8")
        unknown.chmod(0o600)
        failed = subprocess.run(
            command, check=False, capture_output=True, text=True,
            env=environment,
        )
        self.assertEqual(failed.returncode, 1)
        self.assertEqual(failed.stdout, "")
        self.assertEqual(failed.stderr, "candidate feed import failed\n")
        self.assertNotIn(str(self.outbox), failed.stderr)

    def _outbox_state(self) -> tuple[tuple[str, int, str], ...]:
        state = []
        for path in sorted(self.outbox.iterdir()):
            if path.is_file():
                state.append((
                    path.name,
                    path.stat().st_mode & 0o777,
                    hashlib.sha256(path.read_bytes()).hexdigest(),
                ))
        return tuple(state)


if __name__ == "__main__":
    unittest.main()
