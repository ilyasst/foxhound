"""Synthetic tests for evidence-grounded Stage 2 verification."""

from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from foxhound import migrate_database
from foxhound.knowledge_client import (
    KnowledgeDocument,
    KnowledgeLayer,
    KnowledgeResponseError,
    KnowledgeSearchResult,
)
from foxhound import task_duplicate_stage2 as stage2


NOW = "2030-03-01T12:00:00+00:00"


class FakeKnowledge:
    def __init__(self) -> None:
        self.calls: list[str] = []
        self.document = KnowledgeDocument(
            id="emails:synthetic-message-1",
            path="mail/example-thread/message-1",
            excerpt="Person A confirms the fictional rollout checklist is the same action.",
        )

    def search(self, query, **_options):
        self.calls.append(query)
        return KnowledgeSearchResult((KnowledgeLayer(
            name="emails", total_results=1, truncated=False,
            documents=(self.document,),
        ),))


class FakeAgent:
    def __init__(self, verdict: str = "same", *, usable: bool = True) -> None:
        self.verdict = verdict
        self.usable = usable
        self.calls: list[int] = []

    def verify(self, pair, knowledge, *, timeout):
        self.calls.append(pair.candidate_id)
        result = knowledge.search(pair.left.text)
        evidence = tuple(
            document for layer in result.layers for document in layer.documents
        )
        document = evidence[0]
        citation = {
            "document_id": document.id,
            "locator": document.path,
            "excerpt": "fictional rollout checklist is the same action",
        }
        if not self.usable:
            citation["document_id"] = "emails:invented"
        return stage2.AgentRun({
            "verdict": self.verdict,
            "confidence": 0.91,
            "citations": [citation],
        }, evidence, prompt_tokens=12, completion_tokens=7)


class FirstReplyInvalidAgent(FakeAgent):
    def verify(self, pair, knowledge, *, timeout):
        self.usable = bool(self.calls)
        return super().verify(pair, knowledge, timeout=timeout)


class StageTwoTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.database = Path(self.directory.name) / "foxhound.sqlite3"
        migrate_database(self.database)
        self.connection = sqlite3.connect(self.database)
        self.connection.row_factory = sqlite3.Row
        self.addCleanup(self.connection.close)

    def _task(self, task_id: int, text: str, *, kind: str) -> None:
        candidate_id = f"candidate-{task_id}"
        revision = f"{task_id:064x}"
        self.connection.execute(
            "INSERT INTO tasks(id,status,text,version,created_at,updated_at,"
            "owner_ref_version,owner_kind,owner_speaker_id,"
            "owner_canonical_speaker_id,owner_speaker_registry_id,"
            "owner_pinned,owner_provisional) VALUES(?, 'open', ?, 1, ?, ?,"
            "1,'person','SPK_1','SPK_1','registry-synthetic',0,0)",
            (task_id, text, NOW, NOW),
        )
        self.connection.execute(
            "INSERT INTO candidate_inbox(candidate_id,source_system,source_kind,"
            "source_record_id,source_item_id,source_revision,payload_json,"
            "created_at,first_imported_at,updated_at) VALUES(?,'gw',?,?,"
            "?,?, '{}',?,?,?)",
            (candidate_id, kind, f"record-{task_id}", str(task_id), revision,
             NOW, NOW, NOW),
        )
        self.connection.execute(
            "INSERT INTO task_candidate_bindings(candidate_id,source_revision,"
            "task_id,relation,decided_at) VALUES(?,?,?,'accepted',?)",
            (candidate_id, revision, task_id, NOW),
        )

    def _pair(self, left: int, right: int, *, rank_score: float = 0.9,
              routes: tuple[str, ...] = ("words",)) -> int:
        cursor = self.connection.execute(
            "INSERT INTO task_duplicate_candidates("
            "left_task_id,right_task_id,left_task_version,right_task_version,"
            "rank_score,state,created_at,updated_at) "
            "VALUES(?,?,1,1,?,'queued',?,?)",
            (left, right, rank_score, NOW, NOW),
        )
        candidate_id = int(cursor.lastrowid)
        self.connection.executemany(
            "INSERT INTO task_duplicate_candidate_routes(candidate_id,route,score) "
            "VALUES(?,?,?)",
            ((candidate_id, route, 1.0) for route in routes),
        )
        self.connection.commit()
        return candidate_id

    def _basic_pair(self) -> int:
        self._task(1, "Prepare the synthetic rollout checklist", kind="meeting")
        self._task(2, "Send the fictional rollout checklist", kind="email")
        return self._pair(1, 2)


    def test_agent_prompt_carries_owner_reliability_and_fired_routes(self) -> None:
        class Opener:
            def __init__(self):
                self.requests = []
            def open(self, request, timeout):
                self.requests.append(request)
                import io
                return io.BytesIO(b'{"choices": [{"message": {"content": "{\\"verdict\\": \\"same\\", \\"confidence\\": 1.0, \\"citations\\": []}"}}], "usage": {"prompt_tokens": 10, "completion_tokens": 10}}')
                
        agent = stage2.LocalVerificationAgent(model="synthetic", opener=Opener())
        
        pair = stage2.CandidatePair(
            candidate_id=1,
            left=stage2.TaskSnapshot(id=1, version=1, source_kind="email", text="A", owner="Person", owner_reliability="confirmed"),
            right=stage2.TaskSnapshot(id=2, version=1, source_kind="email", text="A", owner="Person", owner_reliability="provisional"),
            routes={"words": 0.9, "owner": 0.5}
        )
        
        class Knowledge:
            def search(self, *args, **kwargs):
                return KnowledgeSearchResult((KnowledgeLayer("kb", 1, False, (KnowledgeDocument("doc", "path", "excerpt"),)),))
                
        agent.verify(pair, Knowledge(), timeout=10.0)
        
        req = agent.opener.requests[0]
        payload = json.loads(req.data.decode("utf-8"))
        
        # Check system prompt
        self.assertIn("owners are unreliable hints", payload["messages"][0]["content"].lower())
        self.assertIn("same_working_group", payload["messages"][0]["content"])
        self.assertIn(
            "not evidence against duplication",
            payload["messages"][0]["content"].lower(),
        )
        
        # Check user prompt
        user_msg = json.loads(payload["messages"][1]["content"])
        self.assertEqual(user_msg["routes"], {"words": 0.9, "owner": 0.5})
        self.assertEqual(user_msg["tasks"][0]["owner_reliability"], "confirmed")
        self.assertEqual(user_msg["tasks"][1]["owner_reliability"], "provisional")
        self.assertEqual(user_msg["same_working_group"], "unknown")

    def test_stage2_same_working_group_values_and_key_privacy(self) -> None:
        key_alpha = "wg_" + "a" * 32
        key_beta = "wg_" + "b" * 32

        class Opener:
            def __init__(self):
                self.requests = []
            def open(self, req, timeout=0):
                self.requests.append(req)
                import io
                reply = json.dumps({"choices": [{"message": {"content": json.dumps({"verdict": "same", "confidence": 0.9, "citations": [{"document_id": "doc", "locator": "path", "excerpt": "excerpt"}]})}}], "usage": {"prompt_tokens": 10, "completion_tokens": 5}}).encode("utf-8")
                return io.BytesIO(reply)

        class Knowledge:
            def search(self, *args, **kwargs):
                return KnowledgeSearchResult((KnowledgeLayer("kb", 1, False, (KnowledgeDocument("doc", "path", "excerpt"),)),))

        # 1. Matching keys -> same_working_group: "true"
        pair_match = stage2.CandidatePair(
            candidate_id=1,
            left=stage2.TaskSnapshot(id=1, version=1, source_kind="email", text="A", owner="Person", working_group=key_alpha),
            right=stage2.TaskSnapshot(id=2, version=1, source_kind="email", text="B", owner="Person", working_group=key_alpha),
            same_working_group="true",
        )
        agent = stage2.LocalVerificationAgent(model="synthetic", opener=Opener())
        agent.verify(pair_match, Knowledge(), timeout=10.0)
        req_data = agent.opener.requests[0].data.decode("utf-8")
        payload = json.loads(req_data)
        user_msg = json.loads(payload["messages"][1]["content"])
        self.assertEqual(user_msg["same_working_group"], "true")

        # Must NEVER contain the key string or expose working_group on task cards
        self.assertNotIn(key_alpha, req_data)
        self.assertNotIn("working_group", user_msg["tasks"][0])
        self.assertNotIn("working_group", user_msg["tasks"][1])

        # 2. Distinct keys -> same_working_group: "false"
        pair_diff = stage2.CandidatePair(
            candidate_id=2,
            left=stage2.TaskSnapshot(id=1, version=1, source_kind="email", text="A", owner="Person", working_group=key_alpha),
            right=stage2.TaskSnapshot(id=2, version=1, source_kind="email", text="B", owner="Person", working_group=key_beta),
            same_working_group="false",
        )
        agent2 = stage2.LocalVerificationAgent(model="synthetic", opener=Opener())
        agent2.verify(pair_diff, Knowledge(), timeout=10.0)
        payload2 = json.loads(agent2.opener.requests[0].data.decode("utf-8"))
        user_msg2 = json.loads(payload2["messages"][1]["content"])
        self.assertEqual(user_msg2["same_working_group"], "false")

        # 3. Missing keys -> same_working_group: "unknown"
        pair_unknown = stage2.CandidatePair(
            candidate_id=3,
            left=stage2.TaskSnapshot(id=1, version=1, source_kind="email", text="A", owner="Person", working_group=None),
            right=stage2.TaskSnapshot(id=2, version=1, source_kind="email", text="B", owner="Person", working_group=key_alpha),
            same_working_group="unknown",
        )
        agent3 = stage2.LocalVerificationAgent(model="synthetic", opener=Opener())
        agent3.verify(pair_unknown, Knowledge(), timeout=10.0)
        payload3 = json.loads(agent3.opener.requests[0].data.decode("utf-8"))
        user_msg3 = json.loads(payload3["messages"][1]["content"])
        self.assertEqual(user_msg3["same_working_group"], "unknown")

    def test_pair_construction_determines_same_working_group(self) -> None:
        candidate_id = self._basic_pair()
        key_shared = "wg_" + "a" * 32
        with sqlite3.connect(self.database) as conn:
            conn.row_factory = sqlite3.Row
            conn.execute("UPDATE tasks SET working_group=? WHERE id IN (1, 2)", (key_shared,))
            conn.commit()
            pair = stage2._pair(conn, candidate_id)
            self.assertEqual(pair.same_working_group, "true")

            conn.execute("UPDATE tasks SET working_group=? WHERE id=2", ("wg_" + "b" * 32,))
            conn.commit()
            pair = stage2._pair(conn, candidate_id)
            self.assertEqual(pair.same_working_group, "false")

            conn.execute("UPDATE tasks SET working_group=NULL WHERE id=2")
            conn.commit()
            pair = stage2._pair(conn, candidate_id)
            self.assertEqual(pair.same_working_group, "unknown")

    def test_a_person_with_no_identity_is_not_a_confirmed_owner(self) -> None:
        """A bare name with no speaker or person id confirms nothing."""
        candidate_id = self._basic_pair()
        self.connection.execute(
            "UPDATE tasks SET owner_kind='person',owner_provisional=0,"
            "owner_speaker_id=NULL,owner_canonical_speaker_id=NULL,"
            "owner_person_id=NULL WHERE id=1"
        )
        self.connection.commit()
        pair = stage2._pair(self.connection, candidate_id)
        self.assertEqual(pair.left.owner_reliability, "unknown")

    def test_same_creates_one_proposal_with_private_citations(self) -> None:
        candidate_id = self._basic_pair()
        agent = FakeAgent("same")

        first = stage2.run_database(
            self.database, agent=agent, knowledge=FakeKnowledge(), now=NOW,
        )
        second = stage2.run_database(
            self.database, agent=agent, knowledge=FakeKnowledge(), now=NOW,
        )

        self.assertEqual((first.same, first.proposals_recorded), (1, 1))
        self.assertEqual((second.pairs_claimed, len(agent.calls)), (0, 1))
        with sqlite3.connect(self.database) as connection:
            proposal_count = connection.execute(
                "SELECT count(*) FROM task_duplicate_proposals"
            ).fetchone()[0]
            row = connection.execute(
                "SELECT verdict,citations_json,proposal_id "
                "FROM task_duplicate_verifications WHERE candidate_id=?",
                (candidate_id,),
            ).fetchone()
        self.assertEqual(proposal_count, 1)
        self.assertEqual(row[0], "same")
        self.assertEqual(
            json.loads(row[1])[0]["document_id"],
            "emails:synthetic-message-1",
        )
        self.assertIsNotNone(row[2])
        with sqlite3.connect(self.database) as connection:
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute(
                    "UPDATE task_duplicate_verifications SET confidence=0.1 "
                    "WHERE candidate_id=?", (candidate_id,)
                )
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute(
                    "DELETE FROM task_duplicate_verifications WHERE candidate_id=?",
                    (candidate_id,),
                )

    def test_related_and_different_are_recorded_without_proposals(self) -> None:
        for verdict in ("related", "different"):
            with self.subTest(verdict=verdict):
                directory = tempfile.TemporaryDirectory()
                self.addCleanup(directory.cleanup)
                database = Path(directory.name) / "foxhound.sqlite3"
                migrate_database(database)
                old_database, old_connection = self.database, self.connection
                self.database = database
                self.connection = sqlite3.connect(database)
                self.connection.row_factory = sqlite3.Row
                try:
                    self._basic_pair()
                    result = stage2.run_database(
                        database, agent=FakeAgent(verdict),
                        knowledge=FakeKnowledge(), now=NOW,
                    )
                    proposal_count = self.connection.execute(
                        "SELECT count(*) FROM task_duplicate_proposals"
                    ).fetchone()[0]
                    stored = self.connection.execute(
                        "SELECT verdict FROM task_duplicate_verifications"
                    ).fetchone()[0]
                finally:
                    self.connection.close()
                    self.database, self.connection = old_database, old_connection
                self.assertEqual(getattr(result, verdict), 1)
                self.assertEqual((stored, proposal_count), (verdict, 0))

    def test_unusable_reply_is_released_for_a_later_retry(self) -> None:
        candidate_id = self._basic_pair()

        failed = stage2.run_database(
            self.database, agent=FakeAgent(usable=False),
            knowledge=FakeKnowledge(), now=NOW,
        )
        retried = stage2.run_database(
            self.database, agent=FakeAgent(), knowledge=FakeKnowledge(), now=NOW,
        )

        self.assertEqual((failed.retries, failed.proposals_recorded), (1, 0))
        self.assertEqual((retried.same, retried.proposals_recorded), (1, 1))
        with sqlite3.connect(self.database) as connection:
            self.assertEqual(connection.execute(
                "SELECT count(*) FROM task_duplicate_verification_claims"
            ).fetchone()[0], 0)
            self.assertIsNotNone(connection.execute(
                "SELECT 1 FROM task_duplicate_verifications WHERE candidate_id=?",
                (candidate_id,),
            ).fetchone())

    def test_retries_say_why_without_carrying_content(self) -> None:
        """A retry names its cause as a fixed code.

        Counting retries alone hid a real outage: every pair failed in 250 ms
        because GW's search contract had grown a field this client refused,
        and the only visible outcome was `retries: 10`.
        """
        self._basic_pair()

        class FailingKnowledge(FakeKnowledge):
            def search(self, query, **_options):
                raise KnowledgeResponseError(
                    "GW knowledge document fields are invalid"
                )

        search_failed = stage2.run_database(
            self.database, agent=FakeAgent(), knowledge=FailingKnowledge(),
            now=NOW,
        )
        ungrounded = stage2.run_database(
            self.database, agent=FakeAgent(usable=False),
            knowledge=FakeKnowledge(), now=NOW,
        )

        self.assertEqual(search_failed.retry_reasons, {"knowledge_search": 1})
        self.assertEqual(ungrounded.retry_reasons, {"citation_not_grounded": 1})
        rendered = json.dumps(ungrounded.retry_reasons)
        self.assertNotIn("fictional", rendered)
        self.assertNotIn("emails:", rendered)

    def test_strongest_candidate_is_verified_first(self) -> None:
        """The budget goes to stage one's best candidates, not its oldest."""
        self._task(1, "Prepare the synthetic rollout checklist", kind="meeting")
        self._task(2, "Draft the synthetic rollout checklist", kind="email")
        self._task(3, "Order synthetic lab supplies", kind="email")
        weak = self._pair(1, 3, rank_score=0.3)
        strong = self._pair(1, 2, rank_score=0.97)
        agent = FakeAgent()

        stage2.run_database(
            self.database, agent=agent, knowledge=FakeKnowledge(), now=NOW,
            limit=1,
        )

        self.assertLess(weak, strong)
        self.assertEqual(agent.calls, [strong])

    def test_supporting_only_legacy_candidate_is_not_claimed(self) -> None:
        self._task(1, "Prepare the synthetic workshop notes", kind="meeting")
        self._task(2, "Order fictional laboratory supplies", kind="meeting")
        supporting_only = self._pair(1, 2, routes=("reread",))
        self._task(3, "Prepare the synthetic workshop summary", kind="email")
        independent = self._pair(1, 3, rank_score=0.4, routes=("words",))
        agent = FakeAgent("different")

        result = stage2.run_database(
            self.database, agent=agent, knowledge=FakeKnowledge(), now=NOW,
            limit=2,
        )

        self.assertEqual(result.pairs_claimed, 1)
        self.assertEqual(agent.calls, [independent])
        self.assertNotEqual(supporting_only, independent)

    def test_daily_budget_stops_the_pass(self) -> None:
        self._task(1, "Synthetic task one", kind="meeting")
        self._task(2, "Synthetic task two", kind="email")
        self._task(3, "Synthetic task three", kind="teams")
        self._pair(1, 2)
        self._pair(1, 3)

        result = stage2.run_database(
            self.database, agent=FakeAgent("related"),
            knowledge=FakeKnowledge(), now=NOW, limit=10, daily_budget=1,
        )

        self.assertEqual((result.pairs_claimed, result.related), (1, 1))
        self.assertTrue(result.budget_exhausted)
        with sqlite3.connect(self.database) as connection:
            self.assertEqual(connection.execute(
                "SELECT runs FROM task_duplicate_verification_days"
            ).fetchone()[0], 1)

    def test_failed_pair_does_not_block_the_next_pair(self) -> None:
        self._task(1, "Synthetic task one", kind="meeting")
        self._task(2, "Synthetic task two", kind="email")
        self._task(3, "Synthetic task three", kind="teams")
        self._pair(1, 2)
        self._pair(1, 3)
        agent = FirstReplyInvalidAgent("related", usable=False)

        result = stage2.run_database(
            self.database, agent=agent, knowledge=FakeKnowledge(), now=NOW,
            limit=2,
        )

        self.assertEqual((result.pairs_claimed, result.retries, result.related),
                         (2, 1, 1))
        self.assertEqual(len(agent.calls), 2)

    def test_agent_runs_without_a_database_write_transaction(self) -> None:
        self._basic_pair()
        database = self.database

        class LockCheckingAgent(FakeAgent):
            def verify(self, pair, knowledge, *, timeout):
                with sqlite3.connect(database, timeout=0.1) as connection:
                    connection.execute("BEGIN IMMEDIATE")
                    connection.rollback()
                return super().verify(pair, knowledge, timeout=timeout)

        result = stage2.run_database(
            self.database, agent=LockCheckingAgent("different"),
            knowledge=FakeKnowledge(), now=NOW,
        )

        self.assertEqual(result.different, 1)

    def test_revision_is_reverified_but_same_versions_are_not(self) -> None:
        self._basic_pair()
        agent = FakeAgent("related")
        first = stage2.run_database(
            self.database, agent=agent, knowledge=FakeKnowledge(), now=NOW,
        )
        unchanged = stage2.run_database(
            self.database, agent=agent, knowledge=FakeKnowledge(), now=NOW,
        )
        with sqlite3.connect(self.database) as connection, connection:
            connection.execute(
                "UPDATE tasks SET text=?,version=2,updated_at=? WHERE id=1",
                ("Prepare the revised synthetic checklist", NOW),
            )
            revised_cursor = connection.execute(
                "INSERT INTO task_duplicate_candidates("
                "left_task_id,right_task_id,left_task_version,right_task_version,"
                "rank_score,state,created_at,updated_at) "
                "VALUES(1,2,2,1,0.95,'queued',?,?)",
                (NOW, NOW),
            )
            connection.execute(
                "INSERT INTO task_duplicate_candidate_routes("
                "candidate_id,route,score) VALUES(?,'words',0.9)",
                (int(revised_cursor.lastrowid),),
            )
        revised = stage2.run_database(
            self.database, agent=agent, knowledge=FakeKnowledge(), now=NOW,
        )

        self.assertEqual((first.related, unchanged.pairs_claimed, revised.related),
                         (1, 0, 1))
        self.assertEqual(len(agent.calls), 2)


if __name__ == "__main__":
    unittest.main()
