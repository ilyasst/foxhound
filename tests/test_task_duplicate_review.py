#!/usr/bin/env python3
"""Synthetic reader-card tests for cross-source task consolidation."""

from __future__ import annotations

from foxhound import migrate_database

import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from foxhound import task_duplicate_proposals as proposals
from foxhound.candidate_inbox import CandidateInbox
from review_card_fixture import raise_review_cards
from foxhound.task_cards import CardDisposition, CardStatus, TaskCardService, render_task_review_card


NOW = datetime(2030, 3, 1, 12, 0, tzinfo=timezone.utc)
TOKEN = "a" * 43
CONSUMER = "b" * 64


class DuplicateReviewCardTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.database = Path(self.directory.name) / "foxhound.sqlite3"
        migrate_database(self.database)
        self.connection = sqlite3.connect(self.database)
        self.connection.row_factory = sqlite3.Row
        self.addCleanup(self.connection.close)
        self._task(1, "email", "Prepare the synthetic rollout checklist")
        self._task(2, "meeting", "Draft the synthetic rollout checklist")
        self.proposal = proposals.propose(
            self.connection, task_id_a=1, task_id_b=2,
            basis="Same synthetic deliverable and confirmed owner.",
            detector="synthetic-detector", now=NOW.isoformat(),
        )
        self.connection.commit()
        self.cards = TaskCardService(
            self.database, clock=lambda: NOW, token_factory=lambda: TOKEN
        )

    def _task(self, task_id: int, kind: str, text: str) -> None:
        candidate_id = f"candidate-{task_id}"
        revision = f"{task_id:064x}"
        self.connection.execute(
            "INSERT INTO tasks(id,status,text,version,created_at,updated_at,"
            "owner_ref_version,owner_kind,owner_speaker_id,"
            "owner_canonical_speaker_id,owner_speaker_registry_id,"
            "owner_pinned,owner_provisional) VALUES(?, 'open', ?, 1, ?, ?,"
            "1, 'person', 'SPK_1', 'SPK_1', 'registry-A', 0, 0)",
            (task_id, text, NOW.isoformat(), NOW.isoformat()),
        )
        self.connection.execute(
            "INSERT INTO candidate_inbox(candidate_id,source_system,source_kind,"
            "source_record_id,source_item_id,source_revision,payload_json,"
            "created_at,first_imported_at,updated_at) VALUES(?, 'gw', ?,"
            "'record', ?, ?, '{}', ?, ?, ?)",
            (candidate_id, kind, str(task_id), revision, NOW.isoformat(),
             NOW.isoformat(), NOW.isoformat()),
        )
        self.connection.execute(
            "INSERT INTO task_candidate_bindings(candidate_id,source_revision,"
            "task_id,relation,decided_at) VALUES(?,?,?,'accepted',?)",
            (candidate_id, revision, task_id, NOW.isoformat()),
        )

    def _deliver(self):
        self.cards.schedule()
        claim = self.cards.claim_next(consumer_digest=CONSUMER)
        self.assertIsNotNone(claim)
        self.assertTrue(self.cards.complete_delivery(
            claim.card.id, expected_version=claim.card.version,
            claim_token=claim.token, transport="synthetic",
            delivery_ref="message-1",
        ).accepted)
        return claim

    def test_proposal_becomes_one_side_by_side_reader_card(self) -> None:
        scheduled = self.cards.schedule()
        self.assertEqual((scheduled.created, scheduled.asked), (1, 1))
        claim = self.cards.claim_next(consumer_digest=CONSUMER)
        self.assertIsNotNone(claim)
        self.assertIsNotNone(claim.card.duplicate)
        text, keyboard = render_task_review_card(claim.card)
        self.assertIn("Same task?", text)
        self.assertIn("Task T1", text)
        self.assertIn("Task T2", text)
        self.assertEqual(len(keyboard["inline_keyboard"][0]), 2)

    def test_duplicate_only_scheduler_does_not_create_ordinary_task_cards(self) -> None:
        self._task(3, "note", "Review an unrelated synthetic topic")
        self.connection.commit()

        scheduled = self.cards.schedule_duplicate_proposals()

        self.assertEqual((scheduled.created, scheduled.asked), (1, 1))
        cards = self.connection.execute(
            "SELECT task_id FROM task_review_cards ORDER BY id"
        ).fetchall()
        self.assertEqual([int(row[0]) for row in cards], [1])

        proposal = self.connection.execute(
            "SELECT card_id FROM task_duplicate_proposals WHERE id=?",
            (self.proposal.proposal_id,),
        ).fetchone()
        self.assertIsNotNone(proposal[0])

    def _hold(self, task_id: int, status: str = "queued") -> None:
        """Put an unfinished execution workflow on one task."""
        self.connection.execute(
            "INSERT INTO task_execution_workflows("
            "task_id,task_version,status,phase,version,created_at,updated_at,"
            "completed_at,agent_profile_id,agent_profile_revision) "
            "VALUES(?,1,?,'plan',1,?,?,?,'synthetic',?)",
            (task_id, status, NOW.isoformat(), NOW.isoformat(),
             NOW.isoformat() if status in ("completed", "cancelled") else None,
             f"{7:064x}"),
        )
        self.connection.commit()

    def _live_cards(self) -> list[int]:
        return [int(row[0]) for row in self.connection.execute(
            "SELECT id FROM task_review_cards WHERE status IN "
            "('pending','delivering','delivered','snoozed') ORDER BY id"
        )]

    def test_a_held_pair_is_not_asked_even_when_the_other_side_is_free(self):
        """Confirming closes one side, so holding either makes it unsafe."""
        self._hold(2)

        scheduled = self.cards.schedule_duplicate_proposals()

        self.assertEqual(scheduled.asked, 0)
        self.assertEqual(self._live_cards(), [])

    def test_a_held_pair_does_not_loop_across_repeated_passes(self):
        """The loop, not the single card, is what reaches the reader.

        Asserting one fewer card in one pass would have passed while this
        was happening: `_cancel_stale` retracts on the hold and releases the
        proposal's card binding, restoring exactly what the selection looks
        for, so every pass cancelled a card and raised another. Only running
        several passes shows it.
        """
        self._hold(1)

        raised = 0
        for _ in range(5):
            raised += self.cards.schedule_duplicate_proposals().created
            raised += self.cards.schedule().created

        self.assertEqual(raised, 0)
        self.assertEqual(self._live_cards(), [])
        cancelled = self.connection.execute(
            "SELECT count(*) FROM task_review_cards WHERE status='cancelled'"
        ).fetchone()[0]
        self.assertEqual(cancelled, 0)

    def test_a_card_on_screen_is_retracted_once_a_workflow_takes_the_task(self):
        """A live card must go when execution picks the task up."""
        self.assertEqual(self.cards.schedule_duplicate_proposals().asked, 1)
        self.assertEqual(len(self._live_cards()), 1)

        self._hold(1)
        self.cards.schedule_duplicate_proposals()

        self.assertEqual(self._live_cards(), [])

    def test_a_finished_workflow_does_not_block_the_question(self):
        """The hold is about live work, not about ever having run."""
        self._hold(2, status="completed")

        scheduled = self.cards.schedule_duplicate_proposals()

        self.assertEqual(scheduled.asked, 1)
        self.assertEqual(len(self._live_cards()), 1)

    def test_confirm_records_relation_and_hides_the_noncanonical_task(self) -> None:
        claim = self._deliver()
        result = self.cards.act(
            claim.card.id, expected_version=claim.card.version,
            action="duplicate_confirm",
        )
        self.assertIs(result.disposition, CardDisposition.APPLIED)
        self.assertIs(result.status, CardStatus.CANCELLED)
        self.assertEqual(
            self.connection.execute(
                "SELECT state FROM task_duplicate_proposals WHERE id=?",
                (self.proposal.proposal_id,),
            ).fetchone()[0],
            "confirmed",
        )
        relation = self.connection.execute(
            "SELECT kind,asserted_by FROM task_relations"
        ).fetchone()
        self.assertEqual(tuple(relation), ("duplicate_of", "reader"))
        job = self.connection.execute(
            "SELECT state,title FROM task_fused_title_jobs WHERE task_id=1"
        ).fetchone()
        self.assertEqual(tuple(job), ("pending", None))
        # Confirming used to leave a fresh review card on the canonical task.
        # Nothing raises one now: the duplicate question was answered, and
        # that is the end of it.
        self.cards.schedule()
        self.assertEqual([card.task_id for card in self.cards.due()], [])

    def test_completed_title_replaces_only_the_canonical_card_display_text(self) -> None:
        claim = self._deliver()
        self.assertTrue(self.cards.act(
            claim.card.id, expected_version=claim.card.version,
            action="duplicate_confirm",
        ).accepted)
        self.connection.execute(
            "UPDATE task_fused_title_jobs SET state='ready',title=?,updated_at=? "
            "WHERE task_id=1",
            ("Synthetic rollout checklist", NOW.isoformat()),
        )
        self.connection.commit()
        # The fused title is a property of the card's display text, so this
        # still needs a card on the canonical task; nothing raises one now.
        raise_review_cards(self.database, NOW)
        due = self.cards.due()
        self.assertEqual([card.task_id for card in due], [1])
        self.assertEqual(due[0].text, "Synthetic rollout checklist")
        self.assertEqual(
            self.connection.execute("SELECT text FROM tasks WHERE id=1").fetchone()[0],
            "Prepare the synthetic rollout checklist",
        )

    def test_rejection_is_durable_and_leaves_both_tasks_open(self) -> None:
        claim = self._deliver()
        result = self.cards.act(
            claim.card.id, expected_version=claim.card.version,
            action="duplicate_reject",
        )
        self.assertTrue(result.accepted)
        self.assertEqual(
            self.connection.execute(
                "SELECT state FROM task_duplicate_proposals WHERE id=?",
                (self.proposal.proposal_id,),
            ).fetchone()[0],
            "rejected",
        )
        self.assertEqual(
            self.connection.execute("SELECT count(*) FROM task_relations").fetchone()[0],
            0,
        )
        self.assertEqual(
            [tuple(row) for row in self.connection.execute(
                "SELECT status FROM tasks ORDER BY id")],
            [("open",), ("open",)],
        )

    def test_recently_closed_comparison_uses_the_open_task_card(self) -> None:
        self.connection.execute(
            "UPDATE tasks SET status='done',closed_at=? WHERE id=1",
            ((NOW - timedelta(days=1)).isoformat(),),
        )
        self.connection.commit()

        self.assertEqual(self.cards.schedule().asked, 1)
        claim = self.cards.claim_next(consumer_digest=CONSUMER)
        self.assertIsNotNone(claim)
        self.assertEqual(claim.card.task_id, 2)
        self.assertEqual(claim.card.duplicate.other_task_id, 1)
        text, keyboard = render_task_review_card(claim.card)
        self.assertIn("recently closed task", text)
        self.assertEqual(keyboard["inline_keyboard"][0][0]["text"], "✅ Already completed")

        self.assertTrue(self.cards.complete_delivery(
            claim.card.id, expected_version=claim.card.version,
            claim_token=claim.token, transport="synthetic",
            delivery_ref="message-1",
        ).accepted)
        result = self.cards.act(
            claim.card.id, expected_version=claim.card.version,
            action="duplicate_confirm",
        )
        self.assertTrue(result.accepted)
        relation = self.connection.execute(
            "SELECT subject_id,object_id FROM task_relations"
        ).fetchone()
        self.assertEqual(tuple(relation), (2, 1))

    def test_stale_right_task_refuses_confirmation(self) -> None:
        claim = self._deliver()
        self.connection.execute("UPDATE tasks SET version=2 WHERE id=2")
        self.connection.commit()
        result = self.cards.act(
            claim.card.id, expected_version=claim.card.version,
            action="duplicate_confirm",
        )
        self.assertIs(result.disposition, CardDisposition.REFUSED)
        self.assertEqual(
            self.connection.execute("SELECT count(*) FROM task_relations").fetchone()[0], 0
        )

    def test_active_execution_refuses_confirmation(self) -> None:
        claim = self._deliver()
        self.connection.execute(
            "INSERT INTO task_execution_workflows("
            "task_id,task_version,status,phase,version,failure_count,"
            "created_at,updated_at) VALUES(1,1,'awaiting_review','plan',1,0,?,?)",
            (NOW.isoformat(), NOW.isoformat()),
        )
        self.connection.commit()
        result = self.cards.act(
            claim.card.id, expected_version=claim.card.version,
            action="duplicate_confirm",
        )
        self.assertIs(result.disposition, CardDisposition.REFUSED)
        self.assertEqual(
            self.connection.execute("SELECT count(*) FROM task_relations").fetchone()[0], 0
        )

    def test_reader_can_reverse_a_confirmation_without_losing_history(self) -> None:
        claim = self._deliver()
        self.assertTrue(self.cards.act(
            claim.card.id, expected_version=claim.card.version,
            action="duplicate_confirm",
        ).accepted)
        self.connection.execute(
            "UPDATE task_fused_title_jobs SET state='ready',title=?,updated_at=? "
            "WHERE task_id=1",
            ("Synthetic rollout checklist", NOW.isoformat()),
        )
        self.connection.commit()
        relation_id = self.connection.execute(
            "SELECT id FROM task_relations"
        ).fetchone()[0]
        self.assertTrue(self.cards.reverse_duplicate(relation_id))
        self.assertEqual(
            self.connection.execute(
                "SELECT withdrawn_at FROM task_relations WHERE id=?", (relation_id,)
            ).fetchone()[0] is not None,
            True,
        )
        self.assertEqual(
            self.connection.execute(
                "SELECT state FROM task_duplicate_proposals WHERE id=?",
                (self.proposal.proposal_id,),
            ).fetchone()[0],
            "proposed",
        )
        self.assertEqual(
            tuple(self.connection.execute(
                "SELECT state,title FROM task_fused_title_jobs WHERE task_id=1"
            ).fetchone()),
            ("idle", None),
        )


if __name__ == "__main__":
    unittest.main()


class DuplicateCardDetailTests(DuplicateReviewCardTests):
    """A merge question is only answerable from facts shown on the card."""

    def _enrich(self, task_id, *, owner, due, created, closed=None,
                status="open"):
        self.connection.execute(
            "UPDATE tasks SET owner=?,due=?,created_at=?,closed_at=?,status=? "
            "WHERE id=?",
            (owner, due, created, closed, status, task_id),
        )
        self.connection.commit()

    def _render(self):
        self.cards.schedule()
        claim = self.cards.claim_next(consumer_digest=CONSUMER)
        self.assertIsNotNone(claim)
        self.assertIsNotNone(claim.card.duplicate)
        return render_task_review_card(claim.card)[0]

    def test_both_sides_carry_owner_due_and_when_raised(self) -> None:
        self._enrich(1, owner="Person A", due="2030-04-01",
                     created="2030-01-05T09:00:00+00:00")
        self._enrich(2, owner="Person B", due="2030-09-01",
                     created="2030-02-09T09:00:00+00:00")
        text = self._render()
        for expected in ("Person A", "Person B", "2030-04-01", "2030-09-01",
                         "2030-01-05", "2030-02-09"):
            self.assertIn(expected, text)

    def test_a_closed_counterpart_shows_when_it_closed(self) -> None:
        self._enrich(2, owner="Person B", due=None,
                     created="2030-02-09T09:00:00+00:00",
                     closed="2030-02-20T09:00:00+00:00", status="done")
        text = self._render()
        self.assertIn("2030-02-20", text)
        self.assertIn("done", text)


class DuplicateBasisTests(unittest.TestCase):
    """The card explains its own evidence, so a reader can weigh it.

    `basis` is immutable once recorded, so each case builds its own proposal
    rather than editing one.
    """

    def _card_text(self, basis: str) -> str:
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        database = Path(directory.name) / "foxhound.sqlite3"
        migrate_database(database)
        connection = sqlite3.connect(database)
        connection.row_factory = sqlite3.Row
        self.addCleanup(connection.close)
        for task_id, kind, text in (
            (1, "email", "Prepare the synthetic rollout checklist"),
            (2, "meeting", "Draft the synthetic rollout checklist"),
        ):
            revision = f"{task_id:064x}"
            connection.execute(
                "INSERT INTO tasks(id,status,text,version,created_at,updated_at,"
                "owner_ref_version,owner_kind,owner_speaker_id,"
                "owner_canonical_speaker_id,owner_speaker_registry_id,"
                "owner_pinned,owner_provisional) VALUES(?, 'open', ?, 1, ?, ?,"
                "1, 'person', 'SPK_1', 'SPK_1', 'registry-A', 0, 0)",
                (task_id, text, NOW.isoformat(), NOW.isoformat()),
            )
            connection.execute(
                "INSERT INTO candidate_inbox(candidate_id,source_system,"
                "source_kind,source_record_id,source_item_id,source_revision,"
                "payload_json,created_at,first_imported_at,updated_at) "
                "VALUES(?, 'gw', ?, 'record', ?, ?, '{}', ?, ?, ?)",
                (f"candidate-{task_id}", kind, str(task_id), revision,
                 NOW.isoformat(), NOW.isoformat(), NOW.isoformat()),
            )
            connection.execute(
                "INSERT INTO task_candidate_bindings(candidate_id,"
                "source_revision,task_id,relation,decided_at) "
                "VALUES(?,?,?,'accepted',?)",
                (f"candidate-{task_id}", revision, task_id, NOW.isoformat()),
            )
        proposals.propose(
            connection, task_id_a=1, task_id_b=2, basis=basis,
            detector="synthetic-detector", now=NOW.isoformat(),
        )
        connection.commit()
        cards = TaskCardService(
            database, clock=lambda: NOW, token_factory=lambda: TOKEN
        )
        cards.schedule()
        claim = cards.claim_next(consumer_digest=CONSUMER)
        self.assertIsNotNone(claim)
        self.assertIsNotNone(claim.card.duplicate)
        return render_task_review_card(claim.card)[0]

    def test_shared_wording_is_shown_as_the_matched_terms(self) -> None:
        text = self._card_text(
            "shared task terms across email and meeting: alpha, beta"
        )
        self.assertIn("Matched on", text)
        self.assertIn("alpha", text)
        self.assertIn("beta", text)

    def test_a_re_read_source_is_explained_in_words(self) -> None:
        """'later reading' is detector shorthand; the card says what it means."""
        text = self._card_text(
            "one email source record carded again at a later reading"
        )
        self.assertIn("read again later", text)


class DuplicateSchedulingCollisionTests(DuplicateReviewCardTests):
    def test_a_task_in_two_proposals_gets_one_card_at_a_time(self) -> None:
        """Three copies of one commitment put a task in two proposals at once.

        Only one active card per task is allowed, so the second proposal must
        wait rather than insert a card that violates that guarantee.
        """
        self._task(3, "note", "Prepare the synthetic rollout checklist again")
        self.connection.commit()
        second = proposals.propose(
            self.connection, task_id_a=1, task_id_b=3,
            basis="Another synthetic overlap.", detector="synthetic-detector",
            now=NOW.isoformat(),
        )
        self.assertTrue(second.accepted)
        self.connection.commit()
        result = self.cards.schedule_duplicate_proposals(limit=50)
        self.assertIsNot(result.disposition, CardDisposition.REFUSED)
        active = self.connection.execute(
            "SELECT count(*) FROM task_review_cards WHERE task_id=1 "
            "AND status IN ('pending','delivering','delivered','snoozed')"
        ).fetchone()[0]
        self.assertEqual(active, 1)
        bound = self.connection.execute(
            "SELECT count(*) FROM task_duplicate_proposals "
            "WHERE card_id IS NOT NULL"
        ).fetchone()[0]
        self.assertEqual(bound, 1)

    def _state(self, proposal_id: int = 1) -> str:
        return self.connection.execute(
            "SELECT state FROM task_duplicate_proposals WHERE id=?",
            (proposal_id,),
        ).fetchone()[0]

    def test_a_proposal_whose_task_moved_is_superseded_not_left_pending(self):
        """The ask fences on an exact version; a moved task is permanent."""
        self.connection.execute("UPDATE tasks SET version=version+1 WHERE id=1")
        self.connection.commit()

        self.cards.schedule_duplicate_proposals()

        self.assertEqual(self._state(), "superseded")
        self.assertIsNotNone(self.connection.execute(
            "SELECT settled_at FROM task_duplicate_proposals WHERE id=1"
        ).fetchone()[0])

    def test_a_superseded_pair_can_be_raised_again_at_current_versions(self):
        """Expired is not answered: the question may come back, freshly."""
        self.connection.execute("UPDATE tasks SET version=version+1 WHERE id=1")
        self.connection.commit()
        self.cards.schedule_duplicate_proposals()
        self.assertEqual(self._state(), "superseded")

        result = proposals.propose(
            self.connection, task_id_a=1, task_id_b=2,
            basis="synthetic repeat", detector="synthetic-detector",
            now=NOW.isoformat(), allow_unconfirmed_owner=True,
        )

        self.assertIs(result.disposition, proposals.ProposalDisposition.RECORDED)
        states = self.connection.execute(
            "SELECT state,COUNT(*) FROM task_duplicate_proposals GROUP BY state"
        ).fetchall()
        self.assertEqual(dict(states), {"superseded": 1, "proposed": 1})

    def test_a_pair_with_no_open_task_left_is_superseded(self):
        self.connection.execute(
            "UPDATE tasks SET status='done' WHERE id IN (1,2)")
        self.connection.commit()

        self.cards.schedule_duplicate_proposals()

        self.assertEqual(self._state(), "superseded")

    def test_a_merely_busy_pair_keeps_its_place(self):
        """An execution hold is transient; superseding it would lose a live question."""
        self._workflow(1, "queued")

        self.cards.schedule_duplicate_proposals()

        self.assertEqual(self._state(), "proposed")

    def _workflow(self, task_id: int, status: str) -> None:
        """Insert one workflow, satisfying that status's own invariants."""
        when = NOW.isoformat()
        running = status == "running"
        claim = ("d" * 64, when, when, when) if running else (None,) * 4
        self.connection.execute(
            "INSERT INTO task_execution_workflows(task_id,task_version,status,"
            "phase,version,failure_count,created_at,updated_at,due_at,"
            "claim_token_digest,claimed_at,claim_heartbeat_at,"
            "claim_expires_at) VALUES(?,1,?,'plan',1,0,?,?,?,?,?,?,?)",
            (task_id, status, when, when,
             when if status == "snoozed" else None, *claim))
        self.connection.commit()

    def test_work_in_flight_still_withholds_the_question(self):
        """Confirming closes a task; that must not happen under live work."""
        for status in ("queued", "running", "awaiting_review"):
            with self.subTest(status=status):
                self.connection.execute("DELETE FROM task_execution_workflows")
                self._workflow(1, status)
                self.assertEqual(
                    self.cards.schedule_duplicate_proposals().created, 0)

    def test_a_snoozed_workflow_no_longer_withholds_the_question(self):
        """A snooze defers work with no deadline; it is not work in flight.

        Closing the task under it is safe -- the scheduler cancels any
        unfinished workflow whose task stops being open -- so waiting on a
        snooze only kept the question unaskable and the gate shut.
        """
        self._workflow(1, "snoozed")

        scheduled = self.cards.schedule_duplicate_proposals()

        self.assertEqual((scheduled.created, scheduled.asked), (1, 1))

    def test_a_snoozed_workflow_does_not_loop_the_question_across_passes(self):
        """Raising on the narrow hold and retracting on the wide one is a loop.

        The single-pass assertion above is satisfied on every pass of it, so
        it cannot see this: the retraction releases the proposal's card
        binding, restoring exactly the shape the selection looks for, and the
        reader is sent a new copy of the same comparison every pass. Only
        several passes show it.
        """
        self._workflow(1, "snoozed")

        created = cancelled = 0
        for _ in range(5):
            created += self.cards.schedule_duplicate_proposals().created
            created += self.cards.schedule().created
            cancelled = self.connection.execute(
                "SELECT count(*) FROM task_review_cards WHERE status='cancelled'"
            ).fetchone()[0]

        self.assertEqual((created, cancelled), (1, 0))
        self.assertEqual(len(self._live_cards()), 1)

    def test_live_work_still_retracts_a_duplicate_card_on_the_next_pass(self):
        """Narrowing the retraction must not stop it happening at all."""
        self.assertEqual(self.cards.schedule_duplicate_proposals().created, 1)
        self.assertEqual(len(self._live_cards()), 1)

        self._workflow(1, "running")
        self.cards.schedule_duplicate_proposals()

        self.assertEqual(self._live_cards(), [])

    def test_the_ordinary_card_path_still_waits_on_a_snooze(self):
        """Only the duplicate question was measured; do not widen the change."""
        self._task(3, "note", "Review the synthetic rollout checklist")
        self.connection.commit()
        self.assertEqual(raise_review_cards(self.database, NOW), 3)
        ordinary = self.connection.execute(
            "SELECT id FROM task_review_cards WHERE task_id=3 "
            "AND status IN ('pending','delivering','delivered','snoozed')"
        ).fetchall()
        self.assertEqual(len(ordinary), 1)

        self._workflow(3, "snoozed")
        self.cards.schedule()

        self.assertEqual(
            self.connection.execute(
                "SELECT status FROM task_review_cards WHERE id=?",
                (int(ordinary[0][0]),),
            ).fetchone()[0],
            "cancelled",
        )

    def test_the_recorded_versions_are_still_immutable(self):
        """Superseding is a state change; it must not license editing history."""
        with self.assertRaises(sqlite3.IntegrityError):
            self.connection.execute(
                "UPDATE task_duplicate_proposals SET left_task_version=99 "
                "WHERE id=1")
