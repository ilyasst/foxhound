#!/usr/bin/env python3
"""Synthetic tests for duplicate-task recall and its vetoes."""

from __future__ import annotations

from foxhound import migrate_database

import json
import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path

from foxhound import task_duplicate_detection as detection
from foxhound import task_duplicate_proposals as proposals
from foxhound.candidate_inbox import CandidateInbox


NOW = "2030-03-01T12:00:00+00:00"


class DetectionFixture(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.database = Path(self.directory.name) / "foxhound.sqlite3"
        migrate_database(self.database)
        self.connection = sqlite3.connect(self.database)
        self.connection.row_factory = sqlite3.Row
        self.addCleanup(self.connection.close)

    def _task(self, task_id: int, *, kind: str, text: str,
              owner: str = "A", status: str = "open",
              closed_at: str | None = None, record: str | None = None,
              item: str | None = None, payload: str = "{}",
              read_at: str | None = None) -> None:
        candidate_id = f"candidate-{task_id}"
        revision = f"{task_id:064x}"
        record = record if record is not None else f"record-{task_id}"
        item = item if item is not None else str(task_id)
        read_at = read_at or NOW
        self.connection.execute(
            "INSERT INTO tasks("
            "id,status,text,version,created_at,updated_at,closed_at,owner_ref_version,"
            "owner_kind,owner_speaker_id,owner_canonical_speaker_id,"
            "owner_speaker_registry_id,owner_pinned,owner_provisional) "
            "VALUES(?,?,?,1,?,?,?,1,'person','SPK_1','SPK_1',?,0,0)",
            (task_id, status, text, NOW, NOW, closed_at, f"registry-{owner}"),
        )
        self.connection.execute(
            "INSERT INTO candidate_inbox("
            "candidate_id,source_system,source_kind,source_record_id,"
            "source_item_id,source_revision,payload_json,created_at,"
            "first_imported_at,updated_at) VALUES(?,'gw',?,?,?,?,?,"
            "?,?,?)",
            (candidate_id, kind, record, item, revision, payload,
             read_at, read_at, read_at),
        )
        self.connection.execute(
            "INSERT INTO task_candidate_bindings("
            "candidate_id,source_revision,task_id,relation,decided_at) "
            "VALUES(?,?,?,'accepted',?)",
            (candidate_id, revision, task_id, NOW),
        )


class CrossSourceDetectionTests(DetectionFixture):
    def test_distinct_sources_with_a_shared_deliverable_become_a_proposal(self):
        self._task(1, kind="email", text="Prepare the synthetic rollout checklist")
        self._task(2, kind="meeting", text="Draft the rollout checklist")
        result = detection.scan(self.connection, now=NOW)
        self.assertEqual(
            (result.pairs_considered, result.pairs_signalled,
             result.proposals_recorded, result.proposals_unchanged,
             result.proposals_refused),
            (1, 1, 1, 0, 0),
        )
        proposal = proposals.next_open(self.connection)
        self.assertIsNotNone(proposal)
        self.assertEqual((proposal.left_task_id, proposal.right_task_id), (1, 2))
        self.assertIn("email and meeting", proposal.basis)

    def test_same_source_kind_pairs_are_signalled(self):
        """One ledger holding two copies of one commitment is the common case."""
        self._task(1, kind="email", text="Prepare the synthetic rollout checklist",
                   record="record-a")
        self._task(2, kind="email", text="Draft the synthetic rollout checklist",
                   record="record-b")
        result = detection.scan(self.connection, now=NOW)
        self.assertEqual(result.proposals_recorded, 1)

    def test_unrelated_text_does_not_create_a_proposal(self):
        self._task(1, kind="email", text="Prepare the synthetic checklist")
        self._task(2, kind="meeting", text="Review the unrelated budget")
        result = detection.scan(self.connection, now=NOW)
        self.assertEqual((result.pairs_considered, result.pairs_signalled), (1, 0))
        self.assertIsNone(proposals.next_open(self.connection))

    def test_cross_source_overlap_without_a_shared_owner_is_reviewable(self):
        self._task(1, kind="email", text="Prepare the synthetic rollout checklist")
        self._task(2, kind="meeting", text="Draft the synthetic rollout checklist",
                   owner="B")
        result = detection.scan(self.connection, now=NOW)
        self.assertEqual((result.pairs_signalled, result.proposals_recorded), (1, 1))

    def test_recently_closed_task_is_compared_with_a_new_open_task(self):
        self._task(
            1, kind="email", text="Prepare the synthetic rollout checklist",
            status="done", closed_at="2030-02-15T12:00:00+00:00",
        )
        self._task(2, kind="meeting", text="Draft the synthetic rollout checklist")
        result = detection.scan(self.connection, now=NOW)
        self.assertEqual((result.pairs_considered, result.proposals_recorded), (1, 1))

    def test_long_closed_task_is_not_compared(self):
        self._task(
            1, kind="email", text="Prepare the synthetic rollout checklist",
            status="done", closed_at="2030-01-01T12:00:00+00:00",
        )
        self._task(2, kind="meeting", text="Draft the synthetic rollout checklist")
        result = detection.scan(self.connection, now=NOW)
        self.assertEqual((result.pairs_considered, result.proposals_recorded), (0, 0))

    def test_repeat_scan_is_idempotent_and_preserves_reader_rejection(self):
        self._task(1, kind="email", text="Prepare the synthetic checklist")
        self._task(2, kind="meeting", text="Draft the synthetic checklist")
        first = detection.scan(self.connection, now=NOW)
        proposal = proposals.next_open(self.connection)
        self.assertTrue(proposals.settle(
            self.connection, proposal_id=proposal.id,
            decision=proposals.Decision.REJECTED, actor="reader", now=NOW,
        ))
        again = detection.scan(self.connection, now="2030-03-02T12:00:00+00:00")
        self.assertEqual((first.proposals_recorded, again.proposals_unchanged),
                         (1, 1))
        self.assertIsNone(proposals.next_open(self.connection))

    def test_database_scan_returns_only_content_free_counts(self):
        self.connection.close()
        with closing(sqlite3.connect(self.database)) as connection:
            connection.row_factory = sqlite3.Row
            self.connection = connection
            self._task(1, kind="email", text="Prepare the synthetic checklist")
            self._task(2, kind="meeting", text="Draft the synthetic checklist")
            connection.commit()
        result = detection.scan_database(self.database, now=NOW)
        self.assertEqual(result.proposals_recorded, 1)


if __name__ == "__main__":
    unittest.main()


class IdentifierVetoTests(DetectionFixture):
    """A different identifier of one class means a different commitment."""

    def test_differing_order_numbers_are_never_proposed(self):
        self._task(1, kind="email", record="record-a",
                   text="Approve purchase document 400111-000 for Example Org")
        self._task(2, kind="meeting", record="record-b",
                   text="Approve purchase document 400222-000 for Example Org")
        result = detection.scan(self.connection, now=NOW)
        self.assertEqual(result.proposals_recorded, 0)
        self.assertIsNone(proposals.next_open(self.connection))

    def test_matching_order_numbers_still_pair(self):
        self._task(1, kind="email", record="record-a",
                   text="Approve purchase document 400111-000 for Example Org")
        self._task(2, kind="meeting", record="record-b",
                   text="Approve purchase order 400111-000 for Example Org")
        result = detection.scan(self.connection, now=NOW)
        self.assertEqual(result.proposals_recorded, 1)

    def test_course_codes_compare_without_their_spacing(self):
        """'AAA 111' and 'AAA111' name one course; the veto must not fire."""
        self._task(1, kind="email", record="record-a",
                   text="Submit the signed synthetic lab form for AAA 111")
        self._task(2, kind="meeting", record="record-b",
                   text="Submit the signed synthetic lab form for AAA111")
        result = detection.scan(self.connection, now=NOW)
        self.assertEqual(result.proposals_recorded, 1)

    def test_differing_course_codes_are_vetoed(self):
        self._task(1, kind="email", record="record-a",
                   text="Instruct the students of the AAA 111 laboratory session")
        self._task(2, kind="meeting", record="record-b",
                   text="Instruct the students of the AAA 222 laboratory session")
        result = detection.scan(self.connection, now=NOW)
        self.assertEqual(result.proposals_recorded, 0)


class OneReadingTests(DetectionFixture):
    """Sharing a source record means two opposite things."""

    def test_items_from_a_single_reading_are_not_duplicates(self):
        """A protocol read once yields separate action items, seconds apart."""
        self._task(1, kind="meeting", record="record-x", item="1",
                   text="Prepare the synthetic rollout checklist",
                   read_at="2030-03-01T12:00:00+00:00")
        self._task(2, kind="meeting", record="record-x", item="2",
                   text="Draft the synthetic rollout checklist",
                   read_at="2030-03-01T12:00:30+00:00")
        result = detection.scan(self.connection, now=NOW)
        self.assertEqual(result.proposals_recorded, 0)

    def test_a_record_read_again_later_is_a_duplicate(self):
        """A thread re-read as it grows re-cards one commitment."""
        self._task(1, kind="email", record="record-x", item="1",
                   text="Reply with availability for the synthetic review",
                   read_at="2030-03-01T09:00:00+00:00")
        self._task(2, kind="email", record="record-x", item="2",
                   text="Confirm a slot for the synthetic review",
                   read_at="2030-03-01T12:00:00+00:00")
        result = detection.scan(self.connection, now=NOW)
        self.assertEqual(result.proposals_recorded, 1)
        proposal = proposals.next_open(self.connection)
        self.assertIn("later reading", proposal.basis)

    def test_a_wide_record_is_treated_as_a_feed(self):
        """A record shared by many tasks names a stream, not one item.

        The texts share nothing, so only the re-read channel could pair them;
        it must not, or every task on one ledger would pair with every other.
        """
        subjects = ("hose fittings", "lamp housings", "camera mounts",
                    "floor sealant", "cable trays", "door magnets",
                    "spare fuses", "label ribbon", "bench clamps",
                    "filter media", "vent grilles", "torque keys",
                    "resin pumps", "glass slides")
        for offset, subject in enumerate(subjects):
            self._task(offset + 1, kind="email", record="feed",
                       item=str(offset + 1), text=f"Order the {subject}",
                       read_at=f"2030-03-01T{offset + 1:02d}:00:00+00:00")
        self.assertGreater(len(subjects), detection.MAX_RECORD_FANOUT)
        result = detection.scan(self.connection, now=NOW)
        self.assertEqual(result.proposals_recorded, 0)


class ForgeTests(DetectionFixture):
    """Forge prose is noise, and an issue-to-review pairing is not a merge."""

    def _forge(self, task_id, *, kind, number, text, body=""):
        self._task(task_id, kind=kind, record="forge/example", item=str(number),
                   text=text, payload=json.dumps({"body": body}))

    def test_an_issue_and_the_review_that_closes_it_are_not_proposed(self):
        """The forge already records that pairing; a merge card would bury the
        duplicates that nothing else records."""
        self._forge(1, kind="issue", number=10, text="Add the synthetic widget")
        self._forge(2, kind="review_request", number=12,
                    text="Review: add the synthetic widget",
                    body="Closes #10.")
        result = detection.scan(self.connection, now=NOW)
        self.assertEqual(result.proposals_recorded, 0)

    def test_forge_prose_is_not_compared(self):
        """Forge titles share a house grammar that scores high when unrelated."""
        self._forge(1, kind="issue", number=10,
                    text="Expose a bounded synthetic projection for readers")
        self._forge(2, kind="issue", number=11,
                    text="Expose a bounded synthetic projection for writers")
        result = detection.scan(self.connection, now=NOW)
        self.assertEqual(result.proposals_recorded, 0)

    def test_one_item_carded_twice_is_still_a_duplicate(self):
        """Identical text under one kind is a double-carding, not a lookalike."""
        self._forge(1, kind="review_request", number=10,
                    text="Review: explain the synthetic hold")
        self._forge(2, kind="review_request", number=11,
                    text="Review: explain the synthetic hold")
        result = detection.scan(self.connection, now=NOW)
        self.assertEqual(result.proposals_recorded, 1)

    def test_identical_text_across_forge_kinds_is_not_a_duplicate(self):
        """An issue and its review are one work item, never a merge question."""
        self._forge(1, kind="issue", number=10,
                    text="Explain the synthetic hold")
        self._forge(2, kind="review_request", number=11,
                    text="Explain the synthetic hold")
        result = detection.scan(self.connection, now=NOW)
        self.assertEqual(result.proposals_recorded, 0)

    def test_a_forge_task_is_still_compared_with_a_meeting_task(self):
        """One commitment can be tracked as an issue and stated in a meeting."""
        self._forge(1, kind="issue", number=10,
                    text="Install the synthetic mesh relay in the annex")
        self._task(2, kind="meeting", record="record-m",
                   text="Install the synthetic mesh relay in the annex")
        result = detection.scan(self.connection, now=NOW)
        self.assertEqual(result.proposals_recorded, 1)


class AccentFoldingTests(DetectionFixture):
    def test_accented_and_unaccented_terms_match(self):
        self._task(1, kind="email", record="record-a",
                   text="Préparer le résumé du séminaire synthétique")
        self._task(2, kind="meeting", record="record-b",
                   text="Preparer le resume du seminaire synthetique")
        result = detection.scan(self.connection, now=NOW)
        self.assertEqual(result.proposals_recorded, 1)


class BasisTests(DetectionFixture):
    def test_basis_does_not_claim_an_owner_check_that_did_not_run(self):
        self._task(1, kind="email", record="record-a",
                   text="Prepare the synthetic rollout checklist", owner="A")
        self._task(2, kind="meeting", record="record-b",
                   text="Draft the synthetic rollout checklist", owner="B")
        detection.scan(self.connection, now=NOW)
        proposal = proposals.next_open(self.connection)
        self.assertNotIn("owner", proposal.basis)
        self.assertIn("shared task terms", proposal.basis)


class StructuredRouteTests(DetectionFixture):
    def _participant(self, task_id: int, *, kind: str, speaker: str | None) -> None:
        self.connection.execute(
            "INSERT INTO task_participants("
            "task_id,position,kind,speaker_id,canonical_speaker_id,"
            "speaker_registry_id) VALUES(?,0,?,?,?,?)",
            (task_id, kind, speaker, speaker if kind == "person" else None,
             "registry-synthetic" if speaker is not None else None),
        )

    def test_object_agreement_adds_a_pair_without_shared_words(self):
        self._task(1, kind="email", text="Arrange a synthetic venue")
        self._task(2, kind="meeting", text="Confirm the demonstration site")
        self.connection.execute("UPDATE tasks SET object=' The Sample Archive ' WHERE id=1")
        self.connection.execute("UPDATE tasks SET object='sample archive' WHERE id=2")
        result = detection.scan(self.connection, now=NOW)
        self.assertEqual(result.proposals_recorded, 1)
        proposal = proposals.next_open(self.connection)
        routes = self.connection.execute(
            "SELECT route FROM task_duplicate_proposal_routes WHERE proposal_id=?",
            (proposal.id,),
        ).fetchall()
        self.assertEqual({row["route"] for row in routes}, {"object"})

    def test_resolved_participant_agreement_adds_a_pair_without_words(self):
        self._task(1, kind="email", text="Arrange a synthetic venue")
        self._task(2, kind="meeting", text="Confirm the demonstration site")
        self._participant(1, kind="person", speaker="SPK_001")
        self._participant(2, kind="person", speaker="SPK_001")
        self.assertEqual(detection.scan(self.connection, now=NOW).proposals_recorded, 1)

    def test_unresolved_participants_do_not_match(self):
        self._task(1, kind="email", text="Arrange a synthetic venue")
        self._task(2, kind="meeting", text="Confirm the demonstration site")
        self._participant(1, kind="unresolved", speaker="SPK_999")
        self._participant(2, kind="unresolved", speaker="SPK_999")
        self.assertEqual(detection.scan(self.connection, now=NOW).proposals_recorded, 0)

    def test_lexical_pair_survives_structural_disagreement(self):
        self._task(1, kind="email", text="Prepare the synthetic rollout checklist")
        self._task(2, kind="meeting", text="Draft the synthetic rollout checklist")
        self.connection.execute("UPDATE tasks SET object='sample archive' WHERE id=1")
        self.connection.execute("UPDATE tasks SET object='different archive' WHERE id=2")
        self.assertEqual(detection.scan(self.connection, now=NOW).proposals_recorded, 1)
