"""A task carries identifiers for what it is about, so an agent can act on it.

A task row holds text and nothing else. An issue-derived task therefore reads
as a sentence with no way back to the issue, and an agent asked to act on it
would have to guess the repository from prose. The origin closes that gap with
identifiers only.
"""
from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from foxhound.candidate_inbox import CandidateInbox
from foxhound.contracts.task_candidate import candidate_id_for
from foxhound.task_ledger import TaskLedger, TaskOrigin


def _issue_candidate(number: int, repo: str = "forge.example/acme/widget") -> dict:
    item_id = str(number)
    return {
        "schema": "foxhound.task-candidate",
        "schema_version": 2,
        "candidate_id": candidate_id_for(system="gw", kind="issue",
                                         record_id=repo, item_id=item_id),
        "source": {"system": "gw", "kind": "issue", "record_id": repo,
                   "item_id": item_id, "revision": "b" * 64},
        "task": {"text": f"Do the thing in {item_id}", "owner": None, "due": None},
        "evidence": {"document_id": repo, "locator": f"{repo}/issues/{item_id}"},
        "created_at": "2030-01-01T12:00:00Z",
    }


class TaskOriginRead(unittest.TestCase):
    def setUp(self) -> None:
        self._dir = tempfile.TemporaryDirectory()
        self.addCleanup(self._dir.cleanup)
        self.db = Path(self._dir.name) / "foxhound.sqlite3"
        ledger = TaskLedger(self.db)
        ledger.initialize()
        self.ledger = ledger

    def _bind(self, task_id: int, candidate: dict, relation: str = "accepted",
              kind: str = "issue") -> None:
        """Insert an inbox row and its binding directly.

        The intake path is exercised elsewhere; this test is about the read.
        """
        source = candidate["source"]
        with sqlite3.connect(self.db) as conn:
            conn.execute(
                "INSERT INTO candidate_inbox(candidate_id,source_system,"
                "source_kind,source_record_id,source_item_id,source_revision,"
                "payload_json,created_at,first_imported_at,updated_at) "
                "VALUES(?,?,?,?,?,?,?,?,?,?)",
                (candidate["candidate_id"], source["system"], kind,
                 source["record_id"], source["item_id"], source["revision"],
                 json.dumps(candidate), candidate["created_at"],
                 candidate["created_at"], candidate["created_at"]),
            )
            conn.execute(
                "INSERT INTO tasks(id,status,text,owner,due,version,"
                "created_at,updated_at) VALUES(?,?,?,?,?,?,?,?)",
                (task_id, "open", candidate["task"]["text"], None, None, 1,
                 candidate["created_at"], candidate["created_at"]),
            )
            conn.execute(
                "INSERT INTO task_candidate_bindings(candidate_id,"
                "source_revision,task_id,relation,decided_at) VALUES(?,?,?,?,?)",
                (candidate["candidate_id"], source["revision"], task_id,
                 relation, candidate["created_at"]),
            )

    def test_an_issue_task_names_its_repository_and_number(self) -> None:
        self._bind(1, _issue_candidate(42))
        self.assertEqual(
            self.ledger.origin(1),
            TaskOrigin(system="gw", kind="issue",
                       record_id="forge.example/acme/widget", item_id="42"),
        )

    def test_the_origin_is_identifiers_only(self) -> None:
        # Never the issue body, the title, or anything a reader would call
        # content — only enough to address the thing.
        self._bind(1, _issue_candidate(42))
        origin = self.ledger.origin(1)
        self.assertEqual(
            set(vars(origin)), {"system", "kind", "record_id", "item_id"})

    def test_a_task_bound_to_nothing_has_no_origin(self) -> None:
        # An ordinary state, not an error: a task may predate binding.
        with sqlite3.connect(self.db) as conn:
            conn.execute(
                "INSERT INTO tasks(id,status,text,owner,due,version,"
                "created_at,updated_at) VALUES(7,'open','Freestanding',"
                "NULL,NULL,1,'2030-01-01T00:00:00Z','2030-01-01T00:00:00Z')")
        self.assertIsNone(self.ledger.origin(7))

    def test_an_unknown_task_has_no_origin(self) -> None:
        self.assertIsNone(self.ledger.origin(999))

    def test_a_meeting_task_is_distinguishable_from_a_forge_one(self) -> None:
        # The agent must be able to tell an addressable forge issue from a
        # meeting action whose record is opaque.
        candidate = _issue_candidate(1, repo="record_" + "c" * 32)
        self._bind(2, candidate, kind="meeting")
        origin = self.ledger.origin(2)
        self.assertEqual(origin.kind, "meeting")
        self.assertNotIn("/", origin.record_id)


class WorkerContextGuidance(unittest.TestCase):
    def test_the_agent_is_told_to_act_only_on_the_named_target(self) -> None:
        from foxhound.execution_runner import agent_prompt

        prompt = agent_prompt()
        self.assertIn("task.origin", prompt)
        # The origin tells the agent where to START. It is not a restriction
        # on what may be read: real work spans repositories, and an agent that
        # cannot look at a second one cannot do the task.
        self.assertIn("the lead to start from", prompt)
        self.assertIn("not a limit on what you may read", prompt)


if __name__ == "__main__":
    unittest.main()
