#!/usr/bin/env python3
"""Asking whether a task is done, with the evidence that says it is."""

from __future__ import annotations

from foxhound import migrate_database

import sqlite3
import tempfile
import unittest
from contextlib import closing
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path

from foxhound import task_completion as completion
from foxhound.candidate_inbox import CandidateInbox
from foxhound.card_provenance import CardSourceEvidence
from foxhound.task_cards import (
    CardDisposition,
    CardStatus,
    TaskCardService,
    render_task_review_card,
)
from foxhound.task_ledger import TaskStatus


NOW = datetime(2030, 3, 1, 12, 0, tzinfo=timezone.utc)
TOKEN = "a" * 43
CONSUMER = "b" * 64

QUOTATION = "Person A: the synthetic item shipped on Tuesday, so that is closed."
REASON = "Person A names the same synthetic item this task asks Person A to prepare."


class Clock:
    def __init__(self) -> None:
        self.value = NOW

    def __call__(self) -> datetime:
        return self.value

    def advance(self, delta: timedelta) -> None:
        self.value += delta


class CompletionEvidenceTests(unittest.TestCase):
    """The record itself: what it refuses, and what it never asks twice."""

    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.database = Path(self.directory.name) / "foxhound.sqlite3"
        migrate_database(self.database)
        self.connection = sqlite3.connect(self.database)
        self.connection.row_factory = sqlite3.Row
        self.addCleanup(self.connection.close)
        for task_id in (1, 2):
            self.connection.execute(
                "INSERT INTO tasks(id,status,text,version,created_at,"
                "updated_at) VALUES(?,'open',?,1,'2030-01-01T00:00:00+00:00',"
                "'2030-01-01T00:00:00+00:00')",
                (task_id, f"synthetic task {task_id}"),
            )

    def _propose(self, **overrides):
        values = {
            "task_id": 1,
            "source_kind": "meeting",
            "source_record_id": "record-009",
            "observed_at": "2030-02-20",
            "quotation": QUOTATION,
            "reason": REASON,
            "detector": "synthetic-detector",
            "confidence": "medium",
            "now": "2030-03-01T12:00:00+00:00",
        }
        values.update(overrides)
        return completion.propose(self.connection, **values)

    def test_a_detection_records_what_the_reader_must_be_shown(self) -> None:
        result = self._propose()
        self.assertIs(result.disposition, completion.ProposalDisposition.RECORDED)
        evidence = completion.next_unasked(self.connection, 1)
        self.assertEqual(evidence.quotation, QUOTATION)
        self.assertEqual(evidence.reason, REASON)
        self.assertEqual(evidence.observed_at, "2030-02-20")
        self.assertTrue(evidence.open)

    def test_detecting_nothing_to_quote_is_a_caller_defect(self) -> None:
        with self.assertRaises(completion.TaskCompletionError):
            self._propose(quotation="   ")
        with self.assertRaises(completion.TaskCompletionError):
            self._propose(reason="")

    def test_a_detection_does_not_close_anything(self) -> None:
        self._propose()
        status = self.connection.execute(
            "SELECT status FROM tasks WHERE id=1").fetchone()[0]
        self.assertEqual(status, "open")

    def test_the_same_sentence_asks_once_however_it_is_reasoned_about(self) -> None:
        first = self._propose()
        again = self._propose(
            reason="A different detector worded this differently.",
            detector="other-detector",
            confidence="high",
        )
        self.assertIs(again.disposition, completion.ProposalDisposition.UNCHANGED)
        self.assertEqual(again.evidence_id, first.evidence_id)
        self.assertEqual(self._rows(), 1)

    def test_rewrapping_a_source_does_not_make_new_evidence(self) -> None:
        self._propose()
        rewrapped = self._propose(
            quotation="Person A:  The Synthetic Item shipped on Tuesday,\n"
                      "so that is closed."
        )
        self.assertIs(
            rewrapped.disposition, completion.ProposalDisposition.UNCHANGED)

    def test_a_refused_suggestion_is_never_raised_again(self) -> None:
        recorded = self._propose()
        self.assertTrue(completion.settle(
            self.connection,
            evidence_id=recorded.evidence_id,
            outcome=completion.Outcome.REJECTED,
            now="2030-03-02T12:00:00+00:00",
        ))
        again = self._propose()
        self.assertIs(again.disposition, completion.ProposalDisposition.UNCHANGED)
        self.assertEqual(self._rows(), 1)
        self.assertIsNone(completion.next_unasked(self.connection, 1))

    def test_a_settled_answer_cannot_be_rewritten(self) -> None:
        recorded = self._propose()
        completion.settle(
            self.connection,
            evidence_id=recorded.evidence_id,
            outcome=completion.Outcome.ACCEPTED,
            now="2030-03-02T12:00:00+00:00",
        )
        self.assertFalse(completion.settle(
            self.connection,
            evidence_id=recorded.evidence_id,
            outcome=completion.Outcome.REJECTED,
            now="2030-03-03T12:00:00+00:00",
        ))

    def test_evidence_is_append_only(self) -> None:
        self._propose()
        with self.assertRaises(sqlite3.IntegrityError):
            self.connection.execute("DELETE FROM task_completion_evidence")
        with self.assertRaises(sqlite3.IntegrityError):
            self.connection.execute(
                "UPDATE task_completion_evidence SET quotation='rewritten'")

    def test_a_closed_task_is_not_asked_about(self) -> None:
        self.connection.execute("UPDATE tasks SET status='done' WHERE id=1")
        result = self._propose()
        self.assertIs(result.disposition, completion.ProposalDisposition.REFUSED)
        self.assertIs(result.refusal, completion.ProposalRefusal.TASK_NOT_OPEN)

    def test_an_unknown_task_is_refused_rather_than_invented(self) -> None:
        result = self._propose(task_id=99)
        self.assertIs(result.refusal, completion.ProposalRefusal.UNKNOWN_TASK)

    def test_a_runaway_detector_is_refused_not_queued(self) -> None:
        for index in range(completion.MAX_OPEN_QUESTIONS_PER_TASK):
            self.assertIs(
                self._propose(quotation=f"{QUOTATION} ({index})").disposition,
                completion.ProposalDisposition.RECORDED,
            )
        overflow = self._propose(quotation=f"{QUOTATION} (one too many)")
        self.assertIs(
            overflow.refusal, completion.ProposalRefusal.TOO_MANY_OPEN_QUESTIONS)

    def test_quality_is_recoverable_without_reading_a_card(self) -> None:
        accepted = self._propose()
        rejected = self._propose(quotation=f"{QUOTATION} (second)")
        self._propose(task_id=2, detector="other-detector")
        completion.settle(
            self.connection, evidence_id=accepted.evidence_id,
            outcome=completion.Outcome.ACCEPTED, now="2030-03-02T12:00:00+00:00")
        completion.settle(
            self.connection, evidence_id=rejected.evidence_id,
            outcome=completion.Outcome.REJECTED, now="2030-03-02T12:00:00+00:00")
        by_detector = {counts.detector: counts
                       for counts in completion.counts(self.connection)}
        self.assertEqual(by_detector["synthetic-detector"].accepted, 1)
        self.assertEqual(by_detector["synthetic-detector"].rejected, 1)
        self.assertEqual(by_detector["synthetic-detector"].answered, 2)
        self.assertEqual(by_detector["other-detector"].proposed, 1)

    def _rows(self) -> int:
        return int(self.connection.execute(
            "SELECT count(*) FROM task_completion_evidence").fetchone()[0])


class DoneCheckCardTests(unittest.TestCase):
    """The question as a reader meets it, and what each answer does."""

    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.database = Path(self.directory.name) / "foxhound.sqlite3"
        self.clock = Clock()
        migrate_database(self.database)
        with closing(sqlite3.connect(self.database)) as connection:
            connection.execute(
                "INSERT INTO tasks(id,status,text,owner,version,created_at,"
                "updated_at) VALUES(1,'open','Prepare the synthetic item',"
                "'Person A',1,'2030-01-01T00:00:00+00:00',"
                "'2030-01-01T00:00:00+00:00')"
            )
            connection.commit()
        self.cards = TaskCardService(
            self.database, clock=self.clock, token_factory=lambda: TOKEN
        )

    def _propose(self, **overrides) -> int:
        values = {
            "task_id": 1,
            "source_kind": "meeting",
            "source_record_id": "record-009",
            "observed_at": "2030-02-20",
            "quotation": QUOTATION,
            "reason": REASON,
            "detector": "synthetic-detector",
            "confidence": "medium",
            "now": self.clock().isoformat(timespec="seconds"),
        }
        values.update(overrides)
        with closing(sqlite3.connect(self.database)) as connection:
            connection.row_factory = sqlite3.Row
            result = completion.propose(connection, **values)
            connection.commit()
        self.assertTrue(result.accepted)
        return result.evidence_id

    def _deliver(self):
        claim = self.cards.claim_next(consumer_digest=CONSUMER)
        self.assertIsNotNone(claim)
        self.assertTrue(self.cards.complete_delivery(
            claim.card.id,
            expected_version=claim.card.version,
            claim_token=claim.token,
            transport="synthetic",
            delivery_ref="synthetic-1",
        ).accepted)
        return claim

    def _state(self, evidence_id: int) -> str:
        with closing(sqlite3.connect(self.database)) as connection:
            return connection.execute(
                "SELECT state FROM task_completion_evidence WHERE id=?",
                (evidence_id,),
            ).fetchone()[0]

    def test_a_detection_produces_a_card_on_the_existing_task(self) -> None:
        self._propose()
        result = self.cards.schedule()
        self.assertIs(result.disposition, CardDisposition.APPLIED)
        self.assertEqual(result.asked, 1)
        self.assertEqual(self.cards.count(), 1)

    def test_the_card_shows_the_source_the_date_the_quote_and_the_reason(self) -> None:
        self._propose()
        self.cards.schedule()
        claim = self.cards.claim_next(consumer_digest=CONSUMER)
        text, keyboard = render_task_review_card(claim.card)
        self.assertIn("Meeting", text)
        self.assertIn("2030-02-20", text)
        self.assertIn("shipped on Tuesday", text)
        self.assertIn("this task asks Person A to prepare", text)
        labels = [button["text"]
                  for row in keyboard["inline_keyboard"] for button in row]
        self.assertEqual(len(labels), 2)
        self.assertIn("Mark as done", labels[0])
        self.assertIn("Reopen", labels[1])

    def test_an_ordinary_card_is_unchanged(self) -> None:
        self.cards.schedule()
        claim = self.cards.claim_next(consumer_digest=CONSUMER)
        text, keyboard = render_task_review_card(claim.card)
        self.assertIn("Task done?", text)
        self.assertEqual(len(keyboard["inline_keyboard"]), 2)

    def test_marking_it_done_closes_the_task_through_the_ledger(self) -> None:
        evidence_id = self._propose()
        self.cards.schedule()
        claim = self._deliver()
        result = self.cards.act(
            claim.card.id, expected_version=claim.card.version,
            action="done")
        self.assertTrue(result.accepted)
        self.assertIs(result.task_status, TaskStatus.DONE)
        self.assertEqual(self._state(evidence_id), "accepted")

    def test_reopening_leaves_the_task_open_and_refuses_the_evidence(self) -> None:
        evidence_id = self._propose()
        self.cards.schedule()
        claim = self._deliver()
        result = self.cards.act(
            claim.card.id, expected_version=claim.card.version,
            action="keep_open")
        self.assertTrue(result.accepted)
        self.assertIs(result.task_status, TaskStatus.OPEN)
        self.assertEqual(self._state(evidence_id), "rejected")

    def test_a_reopened_suggestion_never_comes_back(self) -> None:
        self._propose()
        self.cards.schedule()
        claim = self._deliver()
        self.cards.act(claim.card.id, expected_version=claim.card.version,
                       action="keep_open")
        # The same detector runs again over the same unchanged source.
        with closing(sqlite3.connect(self.database)) as connection:
            connection.row_factory = sqlite3.Row
            repeat = completion.propose(
                connection, task_id=1, source_kind="meeting",
                source_record_id="record-009", observed_at="2030-02-20",
                quotation=QUOTATION, reason=REASON,
                detector="synthetic-detector", confidence="medium",
                now=self.clock().isoformat(timespec="seconds"))
            connection.commit()
        self.assertIs(repeat.disposition, completion.ProposalDisposition.UNCHANGED)
        self.clock.advance(timedelta(days=30))
        follow_up = self.cards.schedule()
        self.assertEqual(follow_up.asked, 0)

    def test_detection_does_not_wait_for_the_weekly_rhythm(self) -> None:
        self.cards.schedule()
        claim = self._deliver()
        self.cards.act(claim.card.id, expected_version=claim.card.version,
                       action="keep_open")
        self.clock.advance(timedelta(days=1))
        self.assertEqual(self.cards.schedule().created, 0)
        self._propose()
        result = self.cards.schedule()
        self.assertEqual(result.asked, 1)
        self.assertEqual(self.cards.due()[0].completion.quotation, QUOTATION)

    def test_a_waiting_card_carries_the_question_rather_than_a_second_card(self) -> None:
        self.cards.schedule()
        self.assertEqual(self.cards.count(), 1)
        self._propose()
        result = self.cards.schedule()
        self.assertEqual(result.created, 0)
        self.assertEqual(result.asked, 1)
        self.assertEqual(self.cards.count(), 1)

    def test_a_card_in_flight_is_left_alone(self) -> None:
        self.cards.schedule()
        self._deliver()
        self._propose()
        result = self.cards.schedule()
        self.assertEqual(result.asked, 0)

    def test_a_cancelled_card_returns_the_question_to_the_queue(self) -> None:
        evidence_id = self._propose()
        self.cards.schedule()
        with closing(sqlite3.connect(self.database)) as connection:
            connection.execute(
                "UPDATE tasks SET version=2 WHERE id=1")
            connection.commit()
        # Cancelled, and re-offered on the fresh card in the same pass: the
        # question was never answered, so it is still owed.
        reissued = self.cards.schedule()
        self.assertEqual(reissued.cancelled, 1)
        self.assertEqual(self._state(evidence_id), "proposed")
        self.assertEqual(reissued.asked, 1)
        self.assertEqual(self.cards.due()[0].completion.id, evidence_id)

    def test_an_unanswered_detection_leaves_the_task_exactly_as_it_was(self) -> None:
        self._propose()
        self.cards.schedule()
        self._deliver()
        with closing(sqlite3.connect(self.database)) as connection:
            row = connection.execute(
                "SELECT status,version FROM tasks WHERE id=1").fetchone()
        self.assertEqual(tuple(row), ("open", 1))

    def test_dropping_the_task_answers_no_question(self) -> None:
        evidence_id = self._propose()
        self.cards.schedule()
        claim = self._deliver()
        self.cards.act(claim.card.id, expected_version=claim.card.version,
                       action="drop")
        # Neither accepted nor rejected: the reader discarded the task and
        # never reached the question, so the detector is not scored on it.
        self.assertEqual(self._state(evidence_id), "superseded")

    def test_the_card_shows_both_halves_of_the_match(self) -> None:
        self._propose()
        self.cards.schedule()
        claim = self.cards.claim_next(consumer_digest=CONSUMER)
        card = replace(
            claim.card,
            origin_kind="meeting",
            origin_record="record-001",
            origin_item="action-001",
            origin_sources=(CardSourceEvidence(
                name="meeting-001.md",
                role="transcript",
                extract="Person B: Person A will prepare the synthetic item.",
            ),),
        )
        text, _ = render_task_review_card(card)
        self.assertIn("What this task asked for", text)
        self.assertIn("will prepare the synthetic item", text)
        self.assertIn("shipped on Tuesday", text)

    def test_a_task_with_no_provenance_still_renders(self) -> None:
        self._propose()
        self.cards.schedule()
        claim = self.cards.claim_next(consumer_digest=CONSUMER)
        self.assertEqual(claim.card.origin_kind, "")
        text, _ = render_task_review_card(claim.card)
        self.assertIn("Looks done", text)
        # No second "From:" claiming the completing source is unknown.
        self.assertNotIn("Unknown source", text)
        self.assertNotIn("What this task asked for", text)


if __name__ == "__main__":
    unittest.main()
