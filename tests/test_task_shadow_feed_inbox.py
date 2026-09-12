from __future__ import annotations

import copy
import json
import sqlite3
import tempfile
import unittest
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path

from foxhound import (
    CandidateInbox,
    ShadowComparisonReport,
    ShadowFeedImportDisposition,
    ShadowFeedImportRefusal,
)
from foxhound.contracts import comparable_task_digest
from foxhound.candidate_inbox import SCHEMA_VERSION


FIXTURES = Path(__file__).parent / "fixtures" / "contracts"
NOW = datetime(2030, 3, 1, 12, 0, tzinfo=timezone.utc)


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


def fixture(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


class TaskShadowFeedInboxTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.database = Path(self.temporary.name) / "candidate-inbox.sqlite3"
        self.inbox = CandidateInbox(self.database, clock=lambda: NOW)
        self.inbox.initialize()

    def import_candidates(self):
        return self.inbox.import_feed(
            fixture("candidate-feed-page-v1.json")
        )

    def observation_feed(self) -> dict:
        return fixture("task-shadow-observation-feed-page-v1.json")

    def test_page_binds_exact_candidates_and_persists_aggregate_report(self):
        self.import_candidates()

        result = self.inbox.import_shadow_feed(self.observation_feed())

        self.assertEqual(result.disposition, ShadowFeedImportDisposition.APPLIED)
        self.assertEqual((result.inserted, result.unchanged), (2, 0))
        self.assertEqual(self.inbox.shadow_feed_cursor("gw", "primary"), 2)
        self.assertEqual(
            self.inbox.shadow_report(),
            ShadowComparisonReport(
                total=2, agreed=1, divergent=0, refused=0, unmapped=1
            ),
        )

        reopened = CandidateInbox(self.database)
        self.assertEqual(reopened.shadow_report(), self.inbox.shadow_report())

    def test_mapped_digest_difference_is_reported_as_divergent(self):
        self.import_candidates()
        document = self.observation_feed()
        document["to_cursor"] = 1
        document["items"] = document["items"][:1]
        document["items"][0]["observation"]["legacy_task"][
            "comparable_digest"
        ] = "0" * 64

        result = self.inbox.import_shadow_feed(document)

        self.assertTrue(result.accepted)
        report = self.inbox.shadow_report()
        self.assertEqual((report.total, report.agreed, report.divergent), (1, 0, 1))

    def test_projectless_candidate_uses_version_two_comparison_shape(self):
        candidate = fixture("meeting-candidate-v2.json")
        self.inbox.import_document(candidate)
        document = self.observation_feed()
        document["to_cursor"] = 1
        document["items"] = document["items"][:1]
        observation = document["items"][0]["observation"]
        observation["candidate"] = candidate
        observation["legacy_task"]["comparable_digest"] = comparable_task_digest(
            text=candidate["task"]["text"],
            project=None,
            owner=candidate["task"]["owner"],
        )

        result = self.inbox.import_shadow_feed(document)

        self.assertTrue(result.accepted)
        self.assertEqual(self.inbox.shadow_report().agreed, 1)

    def test_refused_and_unmapped_outcomes_remain_distinct(self):
        self.import_candidates()
        document = self.observation_feed()
        document["items"][0]["observation"]["disposition"] = "refused"
        document["items"][0]["observation"]["legacy_task"] = None
        document["items"][0]["observation"]["reason_code"] = (
            "unaddressable_projection"
        )

        result = self.inbox.import_shadow_feed(document)

        self.assertTrue(result.accepted)
        self.assertEqual(
            self.inbox.shadow_report(),
            ShadowComparisonReport(
                total=2, agreed=0, divergent=0, refused=1, unmapped=1
            ),
        )

    def test_exact_page_replay_is_idempotent(self):
        self.import_candidates()
        document = self.observation_feed()
        self.inbox.import_shadow_feed(document)
        before = self._shadow_state()

        result = self.inbox.import_shadow_feed(copy.deepcopy(document))

        self.assertEqual(
            result.disposition, ShadowFeedImportDisposition.REPLAYED
        )
        self.assertEqual(self._shadow_state(), before)

    def test_gap_overlap_and_cursor_reuse_are_refused_without_writes(self):
        self.import_candidates()
        gap = self.observation_feed()
        gap["from_cursor"] = 2
        gap["to_cursor"] = 4
        gap["items"][0]["sequence"] = 3
        gap["items"][1]["sequence"] = 4
        result = self.inbox.import_shadow_feed(gap)
        self.assertEqual(result.refusal, ShadowFeedImportRefusal.CURSOR_GAP)
        self.assertEqual(self._shadow_state(), (0, 0, 0))

        document = self.observation_feed()
        self.inbox.import_shadow_feed(document)
        before = self._shadow_state()

        overlap = self.observation_feed()
        overlap["from_cursor"] = 1
        overlap["to_cursor"] = 3
        overlap["items"][0]["sequence"] = 2
        overlap["items"][1]["sequence"] = 3
        result = self.inbox.import_shadow_feed(overlap)
        self.assertEqual(result.refusal, ShadowFeedImportRefusal.CURSOR_OVERLAP)
        self.assertEqual(self._shadow_state(), before)

        altered = self.observation_feed()
        altered["emitted_at"] = "2030-03-01T12:01:00Z"
        result = self.inbox.import_shadow_feed(altered)
        self.assertEqual(result.refusal, ShadowFeedImportRefusal.CURSOR_REUSE)
        self.assertEqual(self._shadow_state(), before)

    def test_missing_candidate_refuses_entire_page(self):
        document = self.observation_feed()

        result = self.inbox.import_shadow_feed(document)

        self.assertEqual(
            result.refusal, ShadowFeedImportRefusal.CANDIDATE_MISSING
        )
        self.assertEqual(self._shadow_state(), (0, 0, 0))

    def test_candidate_payload_conflict_refuses_entire_page(self):
        self.import_candidates()
        document = self.observation_feed()
        document["items"][1]["observation"]["candidate"]["task"][
            "text"
        ] = "A contradictory synthetic action"

        result = self.inbox.import_shadow_feed(document)

        self.assertEqual(
            result.refusal, ShadowFeedImportRefusal.CANDIDATE_CONFLICT
        )
        self.assertEqual(self._shadow_state(), (0, 0, 0))

    def test_contradictory_observation_refuses_new_page_atomically(self):
        self.import_candidates()
        initial = self.observation_feed()
        initial["to_cursor"] = 1
        initial["items"] = initial["items"][:1]
        self.inbox.import_shadow_feed(initial)
        before = self._shadow_state()

        conflicting = copy.deepcopy(initial)
        conflicting["from_cursor"] = 1
        conflicting["to_cursor"] = 2
        conflicting["items"][0]["sequence"] = 2
        conflicting["items"][0]["observation"]["disposition"] = "folded"
        result = self.inbox.import_shadow_feed(conflicting)

        self.assertEqual(
            result.refusal, ShadowFeedImportRefusal.OBSERVATION_CONFLICT
        )
        self.assertEqual(self._shadow_state(), before)

    def test_old_candidate_revision_remains_addressable_after_update(self):
        meeting = fixture("meeting-candidate-v1.json")
        self.inbox.import_document(meeting)
        revised = copy.deepcopy(meeting)
        revised["source"]["revision"] = "f" * 64
        revised["task"]["text"] = "Prepare a revised Project Alpha summary"
        self.inbox.import_document(revised)

        feed = self.observation_feed()
        feed["to_cursor"] = 1
        feed["items"] = feed["items"][:1]
        result = self.inbox.import_shadow_feed(feed)

        self.assertTrue(result.accepted)
        self.assertEqual(self.inbox.shadow_report().agreed, 1)
        self.assertEqual(
            self.inbox.get(meeting["candidate_id"]).source.revision,
            "f" * 64,
        )

        replay = self.inbox.import_document(meeting)
        self.assertTrue(replay.accepted)
        self.assertEqual(
            self.inbox.get(meeting["candidate_id"]).source.revision,
            "f" * 64,
        )

    def test_version_two_migration_preserves_current_candidate_revision(self):
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
            connection.execute("PRAGMA user_version = 2")

        self.inbox.initialize()
        feed = self.observation_feed()
        feed["to_cursor"] = 1
        feed["items"] = feed["items"][:1]

        self.assertTrue(self.inbox.import_shadow_feed(feed).accepted)
        self.assertEqual(self.inbox.shadow_report().agreed, 1)
        with closing(sqlite3.connect(self.database)) as connection, connection:
            version = connection.execute("PRAGMA user_version").fetchone()[0]
        self.assertEqual(version, SCHEMA_VERSION)

    def test_invalid_contract_and_empty_page_do_not_write_receipts(self):
        invalid = self.observation_feed()
        invalid["private_context"] = "not accepted"
        result = self.inbox.import_shadow_feed(invalid)
        self.assertEqual(
            result.refusal, ShadowFeedImportRefusal.INVALID_CONTRACT
        )

        empty = self.observation_feed()
        empty["to_cursor"] = 0
        empty["items"] = []
        result = self.inbox.import_shadow_feed(empty)
        self.assertEqual(result.disposition, ShadowFeedImportDisposition.EMPTY)
        self.assertEqual(self._shadow_state(), (0, 0, 0))

    def _shadow_state(self) -> tuple[int, int, int]:
        with closing(sqlite3.connect(self.database)) as connection, connection:
            observations = connection.execute(
                "SELECT COUNT(*) FROM task_shadow_observations"
            ).fetchone()[0]
            receipts = connection.execute(
                "SELECT COUNT(*) FROM task_shadow_feed_receipts"
            ).fetchone()[0]
        return (
            observations,
            self.inbox.shadow_feed_cursor("gw", "primary"),
            receipts,
        )


if __name__ == "__main__":
    unittest.main()
