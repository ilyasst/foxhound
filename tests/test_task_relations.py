#!/usr/bin/env python3
"""What one task has to do with another, recorded rather than inferred."""

from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from foxhound import task_relations as relations
from foxhound.candidate_inbox import CandidateInbox


class TaskRelationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        database = Path(self.directory.name) / "foxhound.sqlite3"
        CandidateInbox(database).initialize()
        self.connection = sqlite3.connect(database)
        self.connection.row_factory = sqlite3.Row
        self.addCleanup(self.connection.close)
        for task_id in range(1, 6):
            self.connection.execute(
                "INSERT INTO tasks(id,status,text,version,created_at,"
                "updated_at) VALUES(?,'open',?,1,'2030-01-01T00:00:00+00:00',"
                "'2030-01-01T00:00:00+00:00')",
                (task_id, f"synthetic task {task_id}"),
            )

    def _assert(self, subject: int, obj: int, **overrides):
        values = {
            "kind": "supersedes",
            "basis": "the same ask, raised again in a later meeting",
            "asserted_by": "reader",
        }
        values.update(overrides)
        return relations.assert_relation(
            self.connection, subject_id=subject, object_id=obj, **values)

    def test_a_relation_records_who_said_so_and_why(self) -> None:
        relation = self._assert(2, 1, actor="reader-a", note="continued by T2")
        self.assertEqual(relation.kind, "supersedes")
        self.assertEqual(relation.asserted_by, "reader")
        self.assertEqual(relation.actor, "reader-a")
        self.assertEqual(relation.note, "continued by T2")
        self.assertTrue(relation.live)
        self.assertTrue(relation.basis)

    def test_a_machine_and_a_reader_are_not_the_same_fact(self) -> None:
        inferred = self._assert(2, 1, asserted_by="machine",
                                actor="duplicate-detector")
        confirmed = self._assert(3, 1, asserted_by="reader")
        self.assertNotEqual(inferred.asserted_by, confirmed.asserted_by)

    def test_linking_is_not_closing(self) -> None:
        self._assert(2, 1)
        for task_id in (1, 2):
            status = self.connection.execute(
                "SELECT status FROM tasks WHERE id=?", (task_id,)).fetchone()
            self.assertEqual(status["status"], "open")

    def test_withdrawal_keeps_the_record_of_the_attempt(self) -> None:
        relation = self._assert(2, 1)
        withdrawn = relations.withdraw(
            self.connection, relation.id, withdrawn_by="reader")
        self.assertFalse(withdrawn.live)
        self.assertEqual(withdrawn.withdrawn_by, "reader")
        self.assertEqual(relations.for_task(self.connection, 1), ())
        self.assertEqual(
            len(relations.for_task(self.connection, 1,
                                   include_withdrawn=True)), 1)

    def test_a_withdrawn_pair_can_be_asserted_again(self) -> None:
        first = self._assert(2, 1)
        relations.withdraw(self.connection, first.id, withdrawn_by="reader")
        again = self._assert(2, 1, basis="on reflection, it is the same ask")
        self.assertTrue(again.live)
        self.assertNotEqual(again.id, first.id)

    def test_the_same_live_relation_is_refused_twice(self) -> None:
        self._assert(2, 1)
        with self.assertRaises(relations.TaskRelationError):
            self._assert(2, 1)

    def test_a_relation_is_append_only(self) -> None:
        relation = self._assert(2, 1)
        with self.assertRaises(sqlite3.IntegrityError):
            self.connection.execute(
                "DELETE FROM task_relations WHERE id=?", (relation.id,))
        with self.assertRaises(sqlite3.IntegrityError):
            self.connection.execute(
                "UPDATE task_relations SET basis='rewritten' WHERE id=?",
                (relation.id,))

    def test_a_cycle_is_refused(self) -> None:
        self._assert(2, 1)
        self._assert(3, 2)
        with self.assertRaises(relations.TaskRelationError):
            self._assert(1, 3)

    def test_the_refusals_that_keep_it_reviewable(self) -> None:
        for kwargs, why in (
            ({"subject": 1, "obj": 1}, "a task related to itself"),
            ({"subject": 2, "obj": 1, "kind": "resembles"}, "unknown kind"),
            ({"subject": 2, "obj": 1, "asserted_by": "nobody"},
             "unknown asserter"),
            ({"subject": 2, "obj": 1, "basis": ""}, "an empty basis"),
            ({"subject": 2, "obj": 99}, "a task that does not exist"),
            ({"subject": 2, "obj": 1, "basis": "x" * 501},
             "an unbounded basis"),
            ({"subject": 2, "obj": 1, "note": "line\nbreak"},
             "a control character"),
        ):
            with self.subTest(why=why):
                with self.assertRaises(relations.TaskRelationError):
                    self._assert(kwargs.pop("subject"), kwargs.pop("obj"),
                                 **kwargs)

    def test_a_task_carries_a_bounded_share(self) -> None:
        for task_id in range(6, 6 + relations.MAX_LIVE_RELATIONS_PER_TASK + 1):
            self.connection.execute(
                "INSERT INTO tasks(id,status,text,version,created_at,"
                "updated_at) VALUES(?,'open','synthetic',1,"
                "'2030-01-01T00:00:00+00:00','2030-01-01T00:00:00+00:00')",
                (task_id,),
            )
        made = 0
        for task_id in range(6, 6 + relations.MAX_LIVE_RELATIONS_PER_TASK + 1):
            try:
                self._assert(task_id, 1, kind="duplicate_of")
                made += 1
            except relations.TaskRelationError:
                break
        self.assertEqual(made, relations.MAX_LIVE_RELATIONS_PER_TASK)

    def test_both_ends_see_the_relation(self) -> None:
        relation = self._assert(2, 1)
        for task_id in (1, 2):
            found = relations.for_task(self.connection, task_id)
            self.assertEqual([r.id for r in found], [relation.id])


if __name__ == "__main__":
    unittest.main()
