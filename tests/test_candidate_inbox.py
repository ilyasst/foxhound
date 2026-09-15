from __future__ import annotations

import copy
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

from foxhound import (
    CandidateInbox,
    ImportDisposition,
    ImportRefusal,
    InboxError,
)
from foxhound.candidate_inbox import SCHEMA_VERSION


FIXTURES = Path(__file__).parent / "fixtures" / "contracts"
NOW = datetime(2030, 3, 1, 12, 0, tzinfo=timezone.utc)


def fixture(name: str = "meeting-candidate-v1.json") -> dict:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


class CandidateInboxTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.database = Path(self.temporary.name) / "candidate-inbox.sqlite3"
        self.inbox = CandidateInbox(self.database, clock=lambda: NOW)
        self.inbox.initialize()

    def test_initializes_private_versioned_database(self):
        self.assertEqual(os.stat(self.database).st_mode & 0o777, 0o600)
        with closing(sqlite3.connect(self.database)) as connection, connection:
            version = connection.execute("PRAGMA user_version").fetchone()[0]
        self.assertEqual(version, SCHEMA_VERSION)
        self.assertEqual(self.inbox.count(), 0)

    def test_version_seventeen_migration_does_not_invent_owner_identity(self):
        with closing(sqlite3.connect(self.database)) as connection:
            for column in (
                "owner_provisional",
                "owner_pinned",
                "owner_speaker_registry_id",
                "owner_canonical_speaker_id",
                "owner_speaker_id",
                "owner_kind",
                "owner_ref_version",
            ):
                connection.execute(f"ALTER TABLE tasks DROP COLUMN {column}")
            now = NOW.isoformat(timespec="seconds")
            connection.execute(
                "INSERT INTO tasks(status,text,owner,due,version,created_at,"
                "updated_at,closed_at) VALUES('open','Synthetic task',"
                "'Person A',NULL,1,?,?,NULL)",
                (now, now),
            )
            # v21 added this; a database at an older version has
            # not got it yet.
            connection.execute(
                "ALTER TABLE task_execution_results DROP COLUMN work_digest"
            )
            connection.execute("PRAGMA user_version = 17")
            connection.commit()

        CandidateInbox(self.database, clock=lambda: NOW).initialize()

        with closing(sqlite3.connect(self.database)) as connection:
            version = connection.execute("PRAGMA user_version").fetchone()[0]
            columns = {
                row[1] for row in connection.execute("PRAGMA table_info(tasks)")
            }
            owner = connection.execute(
                "SELECT owner,owner_ref_version,owner_kind,owner_pinned,"
                "owner_provisional FROM tasks"
            ).fetchone()
        self.assertEqual(version, SCHEMA_VERSION)
        self.assertEqual(owner, ("Person A", 0, None, 0, 1))
        self.assertTrue({
            "owner_ref_version", "owner_kind", "owner_pinned",
            "owner_provisional",
        } <= columns)

    def test_first_import_inserts_candidate(self):
        result = self.inbox.import_document(fixture())
        self.assertEqual(result.disposition, ImportDisposition.INSERTED)
        self.assertTrue(result.accepted)
        self.assertEqual(self.inbox.count(), 1)

    def test_exact_replay_is_unchanged_and_does_not_rewrite_row(self):
        document = fixture()
        self.inbox.import_document(document)
        before = self._stored_row(document["candidate_id"])

        later = datetime(2030, 3, 2, 12, 0, tzinfo=timezone.utc)
        replay = CandidateInbox(self.database, clock=lambda: later)
        result = replay.import_document(copy.deepcopy(document))
        after = self._stored_row(document["candidate_id"])

        self.assertEqual(result.disposition, ImportDisposition.UNCHANGED)
        self.assertEqual(before, after)

    def test_new_revision_updates_same_candidate(self):
        document = fixture()
        self.inbox.import_document(document)
        revised = copy.deepcopy(document)
        revised["source"]["revision"] = "f" * 64
        revised["task"]["text"] = "Prepare the revised Project Alpha summary"

        result = self.inbox.import_document(revised)
        stored = self.inbox.get(document["candidate_id"])

        self.assertEqual(result.disposition, ImportDisposition.UPDATED)
        self.assertEqual(self.inbox.count(), 1)
        self.assertEqual(stored.source.revision, "f" * 64)
        self.assertEqual(stored.task.text, revised["task"]["text"])

    def test_projectless_version_updates_same_candidate_identity(self):
        version_1 = fixture("meeting-candidate-v1.json")
        version_2 = fixture("meeting-candidate-v2.json")
        self.inbox.import_document(version_1)

        result = self.inbox.import_document(version_2)
        stored = self.inbox.get(version_1["candidate_id"])

        self.assertEqual(result.disposition, ImportDisposition.UPDATED)
        self.assertEqual(self.inbox.count(), 1)
        self.assertEqual(stored.schema_version, 2)
        self.assertIsNone(stored.task.project)

    def test_same_revision_with_different_content_is_refused_without_write(self):
        document = fixture()
        self.inbox.import_document(document)
        conflicting = copy.deepcopy(document)
        conflicting["task"]["text"] = "Contradictory synthetic action"
        before = self._stored_row(document["candidate_id"])

        result = self.inbox.import_document(conflicting)

        self.assertEqual(result.disposition, ImportDisposition.REFUSED)
        self.assertEqual(result.refusal, ImportRefusal.REVISION_CONFLICT)
        self.assertFalse(result.accepted)
        self.assertEqual(before, self._stored_row(document["candidate_id"]))

    def test_changed_creation_time_is_refused_without_write(self):
        document = fixture()
        self.inbox.import_document(document)
        conflicting = copy.deepcopy(document)
        conflicting["source"]["revision"] = "e" * 64
        conflicting["created_at"] = "2030-01-02T12:00:00Z"
        before = self._stored_row(document["candidate_id"])

        result = self.inbox.import_document(conflicting)

        self.assertEqual(result.disposition, ImportDisposition.REFUSED)
        self.assertEqual(result.refusal, ImportRefusal.CREATED_AT_CONFLICT)
        self.assertEqual(before, self._stored_row(document["candidate_id"]))

    def test_invalid_contract_is_refused_without_creating_row(self):
        document = fixture()
        document["source"]["host"] = "host-a"

        result = self.inbox.import_document(document)

        self.assertEqual(result.disposition, ImportDisposition.REFUSED)
        self.assertEqual(result.refusal, ImportRefusal.INVALID_CONTRACT)
        self.assertEqual(self.inbox.count(), 0)

    def test_source_identity_change_cannot_overwrite_existing_candidate(self):
        document = fixture()
        self.inbox.import_document(document)
        changed = copy.deepcopy(document)
        changed["source"]["item_id"] = "action-02"

        result = self.inbox.import_document(changed)

        self.assertEqual(result.disposition, ImportDisposition.REFUSED)
        self.assertEqual(result.refusal, ImportRefusal.INVALID_CONTRACT)
        self.assertEqual(self.inbox.count(), 1)

    def test_state_survives_reopening_in_a_separate_process(self):
        document = fixture("email-candidate-v1.json")
        self.inbox.import_document(document)

        script = (
            "from foxhound import CandidateInbox; import sys; "
            "inbox=CandidateInbox(sys.argv[1]); inbox.initialize(); "
            "candidate=inbox.get(sys.argv[2]); "
            "print(inbox.count(), candidate.source.kind)"
        )
        result = subprocess.run(
            [sys.executable, "-c", script, str(self.database),
             document["candidate_id"]],
            check=True,
            capture_output=True,
            text=True,
        )
        self.assertEqual(result.stdout.strip(), "1 email")

    def test_newer_database_schema_is_refused(self):
        with closing(sqlite3.connect(self.database)) as connection, connection:
            connection.execute(f"PRAGMA user_version = {SCHEMA_VERSION + 1}")
        with self.assertRaisesRegex(InboxError, "newer"):
            self.inbox.initialize()

    def test_missing_parent_directory_is_not_created(self):
        parent = Path(self.temporary.name) / "missing"
        inbox = CandidateInbox(parent / "inbox.sqlite3", clock=lambda: NOW)
        with self.assertRaisesRegex(InboxError, "parent"):
            inbox.initialize()
        self.assertFalse(parent.exists())

    def test_symbolic_link_database_is_refused(self):
        target = Path(self.temporary.name) / "target.sqlite3"
        target.touch()
        link = Path(self.temporary.name) / "linked.sqlite3"
        link.symlink_to(target)
        inbox = CandidateInbox(link, clock=lambda: NOW)
        with self.assertRaisesRegex(InboxError, "symbolic link"):
            inbox.initialize()

    def test_incomplete_versioned_schema_is_refused(self):
        with closing(sqlite3.connect(self.database)) as connection, connection:
            connection.execute("DROP TABLE candidate_inbox")
            connection.execute("CREATE TABLE candidate_inbox(candidate_id TEXT)")
        with self.assertRaisesRegex(InboxError, "incomplete"):
            self.inbox.initialize()

    def _stored_row(self, candidate_id: str) -> tuple:
        with closing(sqlite3.connect(self.database)) as connection, connection:
            return connection.execute(
                "SELECT source_revision,payload_json,created_at,"
                "first_imported_at,updated_at FROM candidate_inbox "
                "WHERE candidate_id=?",
                (candidate_id,),
            ).fetchone()


if __name__ == "__main__":
    unittest.main()
