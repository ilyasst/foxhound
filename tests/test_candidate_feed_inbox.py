from __future__ import annotations

from foxhound import migrate_database

import copy
import json
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
    FeedImportDisposition,
    FeedImportRefusal,
    InboxError,
)
from foxhound.contracts.task_candidate import (
    SOURCE_HISTORY_SCHEMA_VERSION,
)
from foxhound.candidate_inbox import (
    _CUMULATIVE_SCHEMA_VERSIONS,
    _is_cumulative_contract_upgrade,
    ImportRefusal,
    SCHEMA_VERSION,
)


FIXTURES = Path(__file__).parent / "fixtures" / "contracts"
NOW = datetime(2030, 3, 1, 12, 0, tzinfo=timezone.utc)


def fixture(name: str = "candidate-feed-page-v1.json") -> dict:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def _drop_native_intake_schema(connection: sqlite3.Connection) -> None:
    connection.execute("DROP TRIGGER task_owner_events_no_update")
    connection.execute("DROP TRIGGER task_owner_events_no_delete")
    connection.execute("DROP TRIGGER execution_reader_inputs_no_update")
    connection.execute("DROP TRIGGER execution_reader_inputs_no_delete")
    connection.execute("DROP TABLE task_owner_events")
    connection.execute("DROP TABLE execution_reader_inputs")
    connection.execute("DROP TRIGGER native_candidate_intake_events_no_update")
    connection.execute("DROP TRIGGER native_candidate_intake_events_no_delete")
    connection.execute("DROP TRIGGER candidate_feed_items_no_update")
    connection.execute("DROP TRIGGER candidate_feed_items_no_delete")
    connection.execute("DROP TABLE native_candidate_intake_events")
    connection.execute("DROP TABLE native_candidate_intakes")
    connection.execute("DROP TABLE candidate_feed_items")


class CandidateFeedInboxTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.database = Path(self.temporary.name) / "candidate-inbox.sqlite3"
        self.inbox = CandidateInbox(self.database, clock=lambda: NOW)
        migrate_database(self.database)

    def test_page_applies_candidates_receipt_and_cursor(self):
        result = self.inbox.import_feed(fixture())

        self.assertEqual(result.disposition, FeedImportDisposition.APPLIED)
        self.assertTrue(result.accepted)
        self.assertEqual((result.inserted, result.updated, result.unchanged),
                         (2, 0, 0))
        self.assertEqual(self.inbox.count(), 2)
        self.assertEqual(self.inbox.feed_cursor("gw", "primary"), 2)
        self.assertEqual(self._receipt_count(), 1)

    def test_exact_page_replay_is_accepted_without_writes(self):
        document = fixture()
        self.inbox.import_feed(document)
        before = self._database_state()

        result = self.inbox.import_feed(copy.deepcopy(document))

        self.assertEqual(result.disposition, FeedImportDisposition.REPLAYED)
        self.assertTrue(result.accepted)
        self.assertEqual(self._database_state(), before)

    def test_altered_replay_is_refused_as_cursor_reuse(self):
        document = fixture()
        self.inbox.import_feed(document)
        altered = copy.deepcopy(document)
        altered["emitted_at"] = "2030-03-01T12:01:00Z"
        before = self._database_state()

        result = self.inbox.import_feed(altered)

        self.assertEqual(result.disposition, FeedImportDisposition.REFUSED)
        self.assertEqual(result.refusal, FeedImportRefusal.CURSOR_REUSE)
        self.assertEqual(self._database_state(), before)

    def test_cursor_gap_is_refused_without_writes(self):
        document = fixture()
        document["from_cursor"] = 2
        document["to_cursor"] = 4
        document["items"][0]["sequence"] = 3
        document["items"][1]["sequence"] = 4

        result = self.inbox.import_feed(document)

        self.assertEqual(result.refusal, FeedImportRefusal.CURSOR_GAP)
        self.assertEqual(self._feed_state(), (0, 0, 0))

    def test_unreceipted_overlap_is_refused_without_writes(self):
        self.inbox.import_feed(fixture())
        overlap = fixture()
        overlap["from_cursor"] = 1
        overlap["to_cursor"] = 3
        overlap["items"][0]["sequence"] = 2
        overlap["items"][1]["sequence"] = 3
        before = self._feed_state()

        result = self.inbox.import_feed(overlap)

        self.assertEqual(result.refusal, FeedImportRefusal.CURSOR_OVERLAP)
        self.assertEqual(self._feed_state(), before)

    def test_later_page_can_update_an_existing_candidate(self):
        first = fixture()
        first["to_cursor"] = 1
        first["items"] = first["items"][:1]
        self.inbox.import_feed(first)

        second = fixture()
        second["from_cursor"] = 1
        second["to_cursor"] = 2
        second["items"] = second["items"][:1]
        second["items"][0]["sequence"] = 2
        second["items"][0]["candidate"]["source"]["revision"] = "f" * 64
        second["items"][0]["candidate"]["task"]["text"] = (
            "Prepare the revised Project Alpha summary"
        )

        result = self.inbox.import_feed(second)

        self.assertEqual(result.disposition, FeedImportDisposition.APPLIED)
        self.assertEqual((result.inserted, result.updated, result.unchanged),
                         (0, 1, 0))
        self.assertEqual(self.inbox.count(), 1)
        self.assertEqual(self.inbox.feed_cursor("gw", "primary"), 2)

    def test_candidate_conflict_rolls_back_entire_page(self):
        meeting = fixture("meeting-candidate-v1.json")
        self.inbox.import_document(meeting)
        page = fixture()
        page["items"].reverse()
        page["items"][0]["sequence"] = 1
        page["items"][1]["sequence"] = 2
        page["items"][1]["candidate"]["task"]["text"] = (
            "Contradictory synthetic action"
        )

        result = self.inbox.import_feed(page)

        self.assertEqual(result.refusal, FeedImportRefusal.CANDIDATE_CONFLICT)
        self.assertEqual(self.inbox.count(), 1)
        self.assertEqual(self.inbox.feed_cursor("gw", "primary"), 0)
        self.assertEqual(self._receipt_count(), 0)
        email_id = page["items"][0]["candidate"]["candidate_id"]
        self.assertIsNone(self.inbox.get(email_id))

    def test_same_version_additive_change_is_not_an_upgrade(self):
        """Decided 2026-09-27: the generation counter stays strict.

        A candidate arriving at the version already stored, with fields added
        but the change counter unmoved, is NOT excused as a contract upgrade.
        gw is responsible for advancing the counter, and this refusal is the
        only signal the inbox has that a producer stopped doing so -- excusing
        it would have swallowed ilyasst/gw#1196 instead of surfacing it.
        """
        stored = fixture()["items"][0]["candidate"]
        stored["schema_version"] = max(_CUMULATIVE_SCHEMA_VERSIONS)
        incoming = copy.deepcopy(stored)
        incoming["task"]["priority"] = "synthetic-added-field"

        self.assertFalse(
            _is_cumulative_contract_upgrade(
                json.dumps(stored, sort_keys=True),
                json.dumps(incoming, sort_keys=True),
            )
        )

    def test_upgrade_path_is_reachable_for_every_non_newest_version(self):
        """Guard the dead branch this function becomes at the newest version.

        `_is_cumulative_contract_upgrade` only fires when the arriving version
        is strictly higher than the stored one, so it can never fire for
        whichever cumulative version is currently the newest -- and that is the
        version every candidate converges on. The rule is deliberate (see the
        test above), but the dead branch is easy to forget.

        If this fails because a newer cumulative version was added, that is the
        moment to re-read that decision: the version that used to be newest is
        now upgradable, so confirm the additive `shared_shape` normalization
        covers the fields the new version adds, then update the expectation.
        """
        newest = max(_CUMULATIVE_SCHEMA_VERSIONS)
        self.assertEqual(
            newest,
            SOURCE_HISTORY_SCHEMA_VERSION,
            "a newer cumulative schema version was added -- re-read the "
            "docstring before updating this expectation",
        )
        # Every other cumulative version can still reach the upgrade path.
        for older in sorted(_CUMULATIVE_SCHEMA_VERSIONS - {newest}):
            with self.subTest(stored=older):
                stored = fixture()["items"][0]["candidate"]
                stored["schema_version"] = older
                stored["source"].pop("history", None)
                incoming = copy.deepcopy(stored)
                incoming["schema_version"] = newest
                self.assertTrue(
                    _is_cumulative_contract_upgrade(
                        json.dumps(stored, sort_keys=True),
                        json.dumps(incoming, sort_keys=True),
                    ),
                    f"version {older} can no longer upgrade to {newest}",
                )

    def test_candidate_conflict_names_the_offending_record(self):
        """A refusal has to say which candidate, and why.

        One refusal rolls the whole page back and halts every page behind it,
        so an aggregate `candidate_conflict` left an operator replaying
        `_apply_candidate` by hand against a copy of the database to learn
        that a single record had conflicted.
        """
        meeting = fixture("meeting-candidate-v1.json")
        self.inbox.import_document(meeting)
        page = fixture()
        page["items"].reverse()
        page["items"][0]["sequence"] = 1
        page["items"][1]["sequence"] = 2
        page["items"][1]["candidate"]["task"]["text"] = (
            "Contradictory synthetic action"
        )
        offending = page["items"][1]["candidate"]["candidate_id"]

        result = self.inbox.import_feed(page)

        self.assertEqual(result.refusal, FeedImportRefusal.CANDIDATE_CONFLICT)
        self.assertEqual(result.candidate_id, offending)
        self.assertEqual(
            result.candidate_refusal, ImportRefusal.REVISION_CONFLICT
        )
        # Still atomic: naming the record must not apply any of the page.
        self.assertEqual(self.inbox.feed_cursor("gw", "primary"), 0)

    def test_invalid_page_is_refused_without_writes(self):
        document = fixture()
        document["private_hint"] = "not-accepted"

        result = self.inbox.import_feed(document)

        self.assertEqual(result.refusal, FeedImportRefusal.INVALID_CONTRACT)
        self.assertEqual(self._feed_state(), (0, 0, 0))

    def test_empty_page_at_current_cursor_is_accepted_without_receipt(self):
        document = fixture()
        document["to_cursor"] = 0
        document["items"] = []

        result = self.inbox.import_feed(document)

        self.assertEqual(result.disposition, FeedImportDisposition.EMPTY)
        self.assertEqual(self._feed_state(), (0, 0, 0))

    def test_stream_cursors_advance_independently(self):
        primary = fixture()
        secondary = fixture()
        secondary["stream_id"] = "secondary"

        self.inbox.import_feed(primary)
        result = self.inbox.import_feed(secondary)

        self.assertEqual(result.disposition, FeedImportDisposition.APPLIED)
        self.assertEqual(result.unchanged, 2)
        self.assertEqual(self.inbox.feed_cursor("gw", "primary"), 2)
        self.assertEqual(self.inbox.feed_cursor("gw", "secondary"), 2)
        self.assertEqual(self.inbox.count(), 2)

    def test_receipt_and_cursor_survive_a_separate_process(self):
        document = fixture()
        self.inbox.import_feed(document)
        feed_path = FIXTURES / "candidate-feed-page-v1.json"
        script = (
            "from foxhound import CandidateInbox; import json,sys; "
            "inbox=CandidateInbox(sys.argv[1]); "
            "document=json.load(open(sys.argv[2], encoding='utf-8')); "
            "result=inbox.import_feed(document); "
            "print(result.disposition, inbox.feed_cursor('gw','primary'), "
            "inbox.count())"
        )

        result = subprocess.run(
            [sys.executable, "-c", script, str(self.database), str(feed_path)],
            check=True,
            capture_output=True,
            text=True,
        )

        self.assertEqual(result.stdout.strip(), "replayed 2 2")

    def test_version_one_database_migrates_without_losing_candidates(self):
        meeting = fixture("meeting-candidate-v1.json")
        self.inbox.import_document(meeting)
        with closing(sqlite3.connect(self.database)) as connection, connection:
            _drop_native_intake_schema(connection)
            connection.execute(
                "DROP TRIGGER execution_review_card_events_no_update"
            )
            connection.execute(
                "DROP TRIGGER execution_review_card_events_no_delete"
            )
            connection.execute("DROP INDEX execution_review_cards_one_active")
            connection.execute("DROP TABLE execution_review_card_events")
            connection.execute("DROP TABLE execution_review_cards")
            connection.execute("DROP TRIGGER task_execution_events_no_update")
            connection.execute("DROP TRIGGER task_execution_events_no_delete")
            connection.execute("DROP TRIGGER task_execution_results_no_update")
            connection.execute("DROP TRIGGER task_execution_results_no_delete")
            connection.execute("DROP INDEX task_execution_workflows_ready")
            connection.execute("DROP TABLE task_execution_events")
            connection.execute("DROP TABLE task_execution_results")
            connection.execute("DROP TABLE task_execution_workflows")
            connection.execute("DROP TRIGGER task_review_card_events_no_update")
            connection.execute("DROP TRIGGER task_review_card_events_no_delete")
            connection.execute("DROP TABLE task_review_card_events")
            connection.execute("DROP TABLE task_review_cards")
            connection.execute("DROP TRIGGER task_owner_equivalences_no_update")
            connection.execute("DROP TRIGGER task_owner_equivalences_no_delete")
            connection.execute("DROP TABLE task_owner_equivalences")
            connection.execute("DROP TRIGGER shadow_import_cycles_no_update")
            connection.execute("DROP TRIGGER shadow_import_cycles_no_delete")
            connection.execute("DROP TABLE shadow_import_cycles")
            connection.execute("DROP TABLE task_events")
            connection.execute("DROP TABLE task_bootstrap_correlations")
            connection.execute("DROP TABLE task_candidate_bindings")
            connection.execute("DROP TABLE tasks")
            connection.execute("DROP TABLE task_shadow_feed_receipts")
            connection.execute("DROP TABLE task_shadow_feed_cursors")
            connection.execute("DROP TABLE task_shadow_observations")
            connection.execute("DROP TABLE candidate_revision_history")
            connection.execute("DROP TABLE candidate_feed_cursors")
            connection.execute("DROP TABLE candidate_feed_receipts")
            connection.execute("PRAGMA user_version = 1")

        migrate_database(self.database)

        self.assertEqual(self.inbox.count(), 1)
        self.assertIsNotNone(self.inbox.get(meeting["candidate_id"]))
        self.assertEqual(self.inbox.feed_cursor("gw", "primary"), 0)
        with closing(sqlite3.connect(self.database)) as connection, connection:
            version = connection.execute("PRAGMA user_version").fetchone()[0]
        self.assertEqual(version, SCHEMA_VERSION)

    def test_incomplete_version_one_database_is_not_migrated(self):
        with closing(sqlite3.connect(self.database)) as connection, connection:
            _drop_native_intake_schema(connection)
            connection.execute(
                "DROP TRIGGER execution_review_card_events_no_update"
            )
            connection.execute(
                "DROP TRIGGER execution_review_card_events_no_delete"
            )
            connection.execute("DROP INDEX execution_review_cards_one_active")
            connection.execute("DROP TABLE execution_review_card_events")
            connection.execute("DROP TABLE execution_review_cards")
            connection.execute("DROP TRIGGER task_execution_events_no_update")
            connection.execute("DROP TRIGGER task_execution_events_no_delete")
            connection.execute("DROP TRIGGER task_execution_results_no_update")
            connection.execute("DROP TRIGGER task_execution_results_no_delete")
            connection.execute("DROP INDEX task_execution_workflows_ready")
            connection.execute("DROP TABLE task_execution_events")
            connection.execute("DROP TABLE task_execution_results")
            connection.execute("DROP TABLE task_execution_workflows")
            connection.execute("DROP TRIGGER task_review_card_events_no_update")
            connection.execute("DROP TRIGGER task_review_card_events_no_delete")
            connection.execute("DROP TABLE task_review_card_events")
            connection.execute("DROP TABLE task_review_cards")
            connection.execute("DROP TRIGGER task_owner_equivalences_no_update")
            connection.execute("DROP TRIGGER task_owner_equivalences_no_delete")
            connection.execute("DROP TABLE task_owner_equivalences")
            connection.execute("DROP TRIGGER shadow_import_cycles_no_update")
            connection.execute("DROP TRIGGER shadow_import_cycles_no_delete")
            connection.execute("DROP TABLE shadow_import_cycles")
            connection.execute("DROP TABLE task_events")
            connection.execute("DROP TABLE task_bootstrap_correlations")
            connection.execute("DROP TABLE task_candidate_bindings")
            connection.execute("DROP TABLE tasks")
            connection.execute("DROP TABLE task_shadow_feed_receipts")
            connection.execute("DROP TABLE task_shadow_feed_cursors")
            connection.execute("DROP TABLE task_shadow_observations")
            connection.execute("DROP TABLE candidate_revision_history")
            connection.execute("DROP TABLE candidate_feed_cursors")
            connection.execute("DROP TABLE candidate_feed_receipts")
            connection.execute("DROP TABLE candidate_inbox")
            connection.execute("CREATE TABLE candidate_inbox(candidate_id TEXT)")
            connection.execute("PRAGMA user_version = 1")

        with self.assertRaisesRegex(InboxError, "incomplete"):
            migrate_database(self.database)

        with closing(sqlite3.connect(self.database)) as connection, connection:
            version = connection.execute("PRAGMA user_version").fetchone()[0]
            feed_tables = connection.execute(
                "SELECT COUNT(*) FROM sqlite_master WHERE name LIKE "
                "'candidate_feed_%'"
            ).fetchone()[0]
        self.assertEqual(version, 1)
        self.assertEqual(feed_tables, 0)

    def _receipt_count(self) -> int:
        with closing(sqlite3.connect(self.database)) as connection, connection:
            return int(connection.execute(
                "SELECT COUNT(*) FROM candidate_feed_receipts"
            ).fetchone()[0])

    def _feed_state(self) -> tuple[int, int, int]:
        return (
            self.inbox.count(),
            self.inbox.feed_cursor("gw", "primary"),
            self._receipt_count(),
        )

    def _database_state(self) -> tuple[tuple, tuple, tuple]:
        with closing(sqlite3.connect(self.database)) as connection, connection:
            candidates = tuple(connection.execute(
                "SELECT * FROM candidate_inbox ORDER BY candidate_id"
            ))
            cursors = tuple(connection.execute(
                "SELECT * FROM candidate_feed_cursors ORDER BY producer,stream_id"
            ))
            receipts = tuple(connection.execute(
                "SELECT * FROM candidate_feed_receipts "
                "ORDER BY producer,stream_id,from_cursor,to_cursor"
            ))
        return candidates, cursors, receipts


if __name__ == "__main__":
    unittest.main()
