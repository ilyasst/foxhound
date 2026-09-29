"""Synthetic tests for the bounded stage-one duplicate queue."""

from __future__ import annotations

import math
import sqlite3
import tempfile
import unittest
from pathlib import Path

from foxhound import migrate_database
from foxhound import task_duplicate_detection as lexical
from foxhound import task_duplicate_proposals as proposals
from foxhound import task_duplicate_stage1 as stage1


NOW = "2030-03-01T12:00:00+00:00"


class FakeEmbeddings:
    model_id = stage1.MODEL_ID

    def __init__(self, vectors: dict[str, tuple[float, ...]]) -> None:
        self.vectors = vectors
        self.calls: list[tuple[str, ...]] = []

    def encode(self, texts):
        self.calls.append(tuple(texts))
        return [self.vectors[text] for text in texts]


class FailingEmbeddings:
    model_id = stage1.MODEL_ID

    def encode(self, texts):
        raise stage1.EmbeddingUnavailable("synthetic backend failure")


def vector(degrees: float) -> tuple[float, float]:
    radians = math.radians(degrees)
    return (math.cos(radians), math.sin(radians))


class StageOneTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.database = Path(self.directory.name) / "foxhound.sqlite3"
        migrate_database(self.database)
        self.connection = sqlite3.connect(self.database)
        self.connection.row_factory = sqlite3.Row
        self.addCleanup(self.connection.close)

    def _task(self, task_id: int, text: str, *, kind: str = "email",
              speaker: str | None = "SPK_1",
              registry: str | None = "registry-synthetic",
              provisional: bool = False,
              working_group: str | None = None) -> None:
        candidate_id = f"candidate-{task_id}"
        revision = f"{task_id:064x}"
        self.connection.execute(
            "INSERT INTO tasks(id,status,text,version,created_at,updated_at,"
            "owner_ref_version,owner_kind,owner_speaker_id,"
            "owner_canonical_speaker_id,owner_speaker_registry_id,"
            "owner_pinned,owner_provisional,working_group) VALUES(?, 'open', ?, 1, ?, ?,"
            "1,'person',?,?,?,0,?,?)",
            (task_id, text, NOW, NOW, speaker, speaker, registry,
             int(provisional), working_group),
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

    def _label(self, left: int, right: int, decision: proposals.Decision) -> None:
        result = proposals.propose(
            self.connection,
            task_id_a=left,
            task_id_b=right,
            basis="Synthetic calibration pair.",
            detector="synthetic-calibration",
            now=NOW,
        )
        proposals.settle(
            self.connection,
            proposal_id=int(result.proposal_id),
            decision=decision,
            actor="reader",
            now=NOW,
        )

    def _calibrated_fixture(self) -> FakeEmbeddings:
        texts = {
            1: "Send the revised synthetic quote",
            2: "Envoyer le devis synthétique révisé",
            3: "Confirmed calibration alpha",
            4: "Confirmed calibration beta",
            5: "Rejected calibration alpha",
            6: "Rejected calibration beta",
        }
        for task_id, text in texts.items():
            self._task(task_id, text, kind="meeting" if task_id % 2 == 0 else "email")
        self._label(3, 4, proposals.Decision.CONFIRMED)
        self._label(5, 6, proposals.Decision.REJECTED)
        return FakeEmbeddings({
            texts[1]: vector(0),
            texts[2]: vector(10),
            texts[3]: vector(0),
            texts[4]: vector(20),
            texts[5]: vector(0),
            texts[6]: vector(40),
        })

    def test_multilingual_pair_above_calibrated_threshold_is_queued(self) -> None:
        backend = self._calibrated_fixture()
        stage1.enqueue(self.connection, 1, now=NOW)

        result = stage1.run(self.connection, now=NOW, backend=backend)

        pair = self.connection.execute(
            "SELECT id FROM task_duplicate_candidates "
            "WHERE left_task_id=1 AND right_task_id=2"
        ).fetchone()
        self.assertIsNotNone(pair)
        routes = self.connection.execute(
            "SELECT route,score FROM task_duplicate_candidate_routes "
            "WHERE candidate_id=?", (int(pair["id"]),)
        ).fetchall()
        self.assertIn("embedding", {row["route"] for row in routes})
        self.assertGreater(result.precision, 0)
        self.assertGreater(result.recall, 0)
        self.assertGreaterEqual(math.cos(math.radians(10)), result.threshold)
        self.assertEqual(
            self.connection.execute(
                "SELECT count(*) FROM task_duplicate_proposals "
                "WHERE state='proposed'"
            ).fetchone()[0],
            0,
        )

    def test_embedding_failure_keeps_retry_and_commits_other_signals(self) -> None:
        self._task(1, "Prepare the synthetic rollout checklist")
        self._task(2, "Draft the synthetic rollout checklist", kind="meeting")
        stage1.enqueue(self.connection, 1, now=NOW)

        result = stage1.run(
            self.connection, now=NOW, backend=FailingEmbeddings()
        )

        self.assertEqual((result.embedding_retries, result.tasks_completed), (1, 0))
        queued = self.connection.execute(
            "SELECT signals_done,embedding_done,embedding_attempts "
            "FROM task_duplicate_checks WHERE task_id=1"
        ).fetchone()
        self.assertEqual(tuple(queued), (1, 0, 1))
        routes = self.connection.execute(
            "SELECT route FROM task_duplicate_candidate_routes"
        ).fetchall()
        self.assertEqual({row["route"] for row in routes}, {"words", "owner"})

    def test_persistent_embedding_failure_does_not_starve_the_queue(self) -> None:
        """A failing embedding path must not freeze local signals.

        Ordering the queue by enqueue time alone kept the oldest tasks, each
        still owed an embedding, at the head of every pass, so the tasks
        behind them never got their words, reread or participant signals.
        Seen for real when a shared GPU ran out of memory mid-backfill.
        """
        for task_id in range(1, 6):
            self._task(task_id, f"Synthetic task number {task_id}")
            stage1.enqueue(self.connection, task_id, now=NOW)

        for _pass in range(3):
            stage1.run(
                self.connection, now=NOW, backend=FailingEmbeddings(),
                task_limit=2,
            )

        rows = self.connection.execute(
            "SELECT task_id,signals_done,embedding_done "
            "FROM task_duplicate_checks ORDER BY task_id"
        ).fetchall()
        self.assertEqual(
            [(row["task_id"], row["signals_done"]) for row in rows],
            [(task_id, 1) for task_id in range(1, 6)],
        )
        self.assertTrue(all(row["embedding_done"] == 0 for row in rows))

    def test_caproute_backend_prefixes_batches_and_orders(self) -> None:
        """E5 needs `query: `; batches stay bounded; order follows `index`."""
        import io
        import json as json_module

        requests: list[dict] = []

        class Opener:
            def open(self, request, timeout):
                body = json_module.loads(request.data.decode("utf-8"))
                requests.append({"url": request.full_url, **body})
                data = [
                    {"index": index, "embedding": [float(index + 1), 0.0]}
                    for index in range(len(body["input"]))
                ]
                data.reverse()  # the gateway may answer out of order
                return io.BytesIO(json_module.dumps({"data": data}).encode())

        backend = stage1.CaprouteEmbeddingBackend(opener=Opener())
        texts = [f"synthetic task {number}" for number in range(40)]

        vectors = backend.encode(texts)

        self.assertEqual(backend.model_id, "caproute:embedding-multilingual")
        self.assertEqual([len(request["input"]) for request in requests], [32, 8])
        self.assertTrue(all(
            request["url"] == stage1.DEFAULT_EMBEDDING_ENDPOINT + "/v1/embeddings"
            and request["model"] == "embedding-multilingual"
            for request in requests
        ))
        self.assertEqual(requests[0]["input"][0], "query: synthetic task 0")
        self.assertEqual(vectors[0], [1.0, 0.0])
        self.assertEqual(vectors[33], [2.0, 0.0])

    def test_caproute_failure_is_content_free(self) -> None:
        class Opener:
            def open(self, request, timeout):
                raise OSError("synthetic task text in a transport error")

        backend = stage1.CaprouteEmbeddingBackend(opener=Opener())
        with self.assertRaises(stage1.EmbeddingUnavailable) as caught:
            backend.encode(["synthetic task text"])
        self.assertNotIn("synthetic", str(caught.exception))

    def test_embedding_endpoint_must_be_loopback(self) -> None:
        with self.assertRaises(ValueError):
            stage1.CaprouteEmbeddingBackend(endpoint="http://192.0.2.10:8800")

    def test_embeddings_are_fetched_without_the_write_lock(self) -> None:
        """Intake shares the write lock and waits only seconds for it."""
        self._task(1, "Prepare the synthetic rollout checklist")
        self._task(2, "Draft the synthetic rollout checklist", kind="meeting")
        stage1.enqueue(self.connection, 1, now=NOW)
        self.connection.commit()
        database = self.database
        calls: list[int] = []

        class LockCheckingEmbeddings:
            model_id = "caproute:synthetic"

            def encode(self, texts):
                with sqlite3.connect(database, timeout=0.1) as connection:
                    connection.execute("BEGIN IMMEDIATE")
                    connection.rollback()
                calls.append(len(texts))
                return [[1.0, 0.0] for _ in texts]

        stage1.run_database(self.database, backend=LockCheckingEmbeddings())

        self.assertEqual(calls, [2])
        cached = self.connection.execute(
            "SELECT count(*) FROM task_duplicate_embeddings WHERE model_id=?",
            ("caproute:synthetic",),
        ).fetchone()[0]
        self.assertEqual(cached, 2)

    def test_top_k_and_global_pair_cap_are_honoured(self) -> None:
        backend = self._calibrated_fixture()
        for task_id in (1, 2, 3):
            stage1.enqueue(self.connection, task_id, now=NOW)

        result = stage1.run(
            self.connection,
            now=NOW,
            backend=backend,
            top_k=1,
            pair_limit=2,
        )

        pairs = self.connection.execute(
            "SELECT left_task_id,right_task_id FROM task_duplicate_candidates"
        ).fetchall()
        self.assertLessEqual(len(pairs), 2)
        counts: dict[int, int] = {}
        for left, right in pairs:
            counts[left] = counts.get(left, 0) + 1
            counts[right] = counts.get(right, 0) + 1
        self.assertTrue(all(count <= 1 for count in counts.values()))
        self.assertGreaterEqual(result.capped, 0)

    def test_vectors_are_cached_per_task_version(self) -> None:
        backend = self._calibrated_fixture()
        stage1.enqueue(self.connection, 1, now=NOW)
        stage1.run(self.connection, now=NOW, backend=backend)
        first_calls = len(backend.calls)
        stage1.enqueue(self.connection, 1, now=NOW)
        stage1.run(self.connection, now=NOW, backend=backend)
        self.assertEqual(len(backend.calls), first_calls)
        cached = self.connection.execute(
            "SELECT count(*) FROM task_duplicate_embeddings WHERE task_id=1 "
            "AND task_version=1"
        ).fetchone()[0]
        self.assertEqual(cached, 1)

    def test_person_identity_matches_across_source_registries(self) -> None:
        self._task(1, "Coordinate the synthetic sample", kind="meeting")
        self._task(2, "Coordinate the synthetic sample checklist", kind="email")
        person_id = "person_" + "c" * 32
        self.connection.executemany(
            "INSERT INTO task_participants(task_id,position,kind,speaker_id,"
            "canonical_speaker_id,speaker_registry_id,person_id) "
            "VALUES(?,0,'person',?,?,?,?)",
            [
                (1, "SPK_7", "SPK_7", "registry-one", person_id),
                (2, "SPK_91", "SPK_91", "registry-two", person_id),
            ],
        )
        stage1.enqueue(self.connection, 1, now=NOW)

        result = stage1.run(
            self.connection, now=NOW, backend=FailingEmbeddings()
        )

        routes = self.connection.execute(
            "SELECT route FROM task_duplicate_candidate_routes"
        ).fetchall()
        self.assertIn("participant", {row["route"] for row in routes})
        self.assertIn("words", {row["route"] for row in routes})
        self.assertEqual(result.embedding_retries, 1)


if __name__ == "__main__":
    unittest.main()


class OwnerSignalTests(StageOneTests):
    """Owners strengthen a pair; they never create one and never block one."""

    def _routes(self) -> dict[tuple[int, int], dict[str, float]]:
        found: dict[tuple[int, int], dict[str, float]] = {}
        for row in self.connection.execute(
            "SELECT c.left_task_id,c.right_task_id,r.route,r.score "
            "FROM task_duplicate_candidates AS c "
            "JOIN task_duplicate_candidate_routes AS r ON r.candidate_id=c.id"
        ):
            found.setdefault((row[0], row[1]), {})[row[2]] = row[3]
        return found

    def test_a_shared_owner_alone_queues_nothing(self) -> None:
        """One person owns dozens of unrelated tasks; that is not a lead."""
        self._task(1, "Book the synthetic venue for the spring workshop")
        self._task(2, "Review the fictional grant budget spreadsheet")
        stage1.enqueue(self.connection, 1, now=NOW)
        stage1.run(self.connection, now=NOW, backend=FailingEmbeddings())
        self.assertEqual(self._routes(), {})

    def test_a_provisional_owner_still_strengthens_the_pair(self) -> None:
        self._task(1, "Prepare the synthetic rollout checklist")
        self._task(2, "Draft the synthetic rollout checklist",
                   kind="meeting", provisional=True)
        stage1.enqueue(self.connection, 1, now=NOW)
        stage1.run(self.connection, now=NOW, backend=FailingEmbeddings())
        self.assertEqual(self._routes()[(1, 2)]["owner"], 0.5)

    def test_speaker_ids_from_different_registries_do_not_match(self) -> None:
        self._task(1, "Prepare the synthetic rollout checklist")
        self._task(2, "Draft the synthetic rollout checklist",
                   kind="meeting", registry="registry-other")
        stage1.enqueue(self.connection, 1, now=NOW)
        stage1.run(self.connection, now=NOW, backend=FailingEmbeddings())
        self.assertNotIn("owner", self._routes()[(1, 2)])

    def test_a_different_owner_scores_the_same_as_an_unknown_one(self) -> None:
        self._task(1, "Prepare the synthetic rollout checklist")
        self._task(2, "Draft the synthetic rollout checklist",
                   kind="meeting", speaker="SPK_2")
        self._task(3, "Prepare the synthetic rollout checklist", kind="teams",
                   speaker=None, registry=None)
        self._task(4, "Draft the synthetic rollout checklist",
                   kind="meeting", speaker=None, registry=None)
        stage1.enqueue(self.connection, 1, now=NOW)
        stage1.enqueue(self.connection, 3, now=NOW)
        stage1.run(self.connection, now=NOW, backend=FailingEmbeddings())
        scores = dict(self.connection.execute(
            "SELECT left_task_id||'-'||right_task_id, rank_score "
            "FROM task_duplicate_candidates"
        ).fetchall())
        self.assertEqual(scores["1-2"], scores["3-4"])

    def test_independent_signals_outrank_one_shared_participant(self) -> None:
        agreeing = stage1._combined_score(
            {"words": 0.6, "embedding": 0.96, "owner": 1.0}
        )
        participant_only = stage1._combined_score({"participant": 0.9})
        self.assertGreater(agreeing, participant_only)
        self.assertGreater(
            agreeing, stage1._combined_score({"words": 0.6, "embedding": 0.96})
        )


class WorkingGroupSignalTests(StageOneTests):
    """Working group strengthens a pair; it never creates one alone."""

    def _routes(self) -> dict[tuple[int, int], dict[str, float]]:
        found: dict[tuple[int, int], dict[str, float]] = {}
        for row in self.connection.execute(
            "SELECT c.left_task_id,c.right_task_id,r.route,r.score "
            "FROM task_duplicate_candidates AS c "
            "JOIN task_duplicate_candidate_routes AS r ON r.candidate_id=c.id"
        ):
            found.setdefault((row[0], row[1]), {})[row[2]] = row[3]
        return found

    def test_equal_keys_offer_working_group_route(self) -> None:
        key = "wg_" + "a" * 32
        self._task(1, "Prepare the synthetic rollout checklist alpha", working_group=key)
        self._task(2, "Draft the synthetic rollout checklist", kind="meeting", working_group=key)
        stage1.enqueue(self.connection, 1, now=NOW)
        stage1.run(self.connection, now=NOW, backend=FailingEmbeddings())
        routes = self._routes()[(1, 2)]
        self.assertIn("working_group", routes)
        self.assertGreater(routes["working_group"], 0.0)

    def test_missing_keys_never_match(self) -> None:
        key = "wg_" + "a" * 32
        self._task(1, "Prepare the synthetic rollout checklist", working_group=key)
        self._task(2, "Draft the synthetic rollout checklist", kind="meeting", working_group=None)
        stage1.enqueue(self.connection, 1, now=NOW)
        stage1.run(self.connection, now=NOW, backend=FailingEmbeddings())
        routes = self._routes()[(1, 2)]
        self.assertNotIn("working_group", routes)

    def test_distinct_keys_never_match(self) -> None:
        key1 = "wg_" + "a" * 32
        key2 = "wg_" + "b" * 32
        self._task(1, "Prepare the synthetic rollout checklist", working_group=key1)
        self._task(2, "Draft the synthetic rollout checklist", kind="meeting", working_group=key2)
        stage1.enqueue(self.connection, 1, now=NOW)
        stage1.run(self.connection, now=NOW, backend=FailingEmbeddings())
        routes = self._routes()[(1, 2)]
        self.assertNotIn("working_group", routes)

    def test_a_shared_working_group_alone_queues_nothing(self) -> None:
        key = "wg_" + "a" * 32
        self._task(1, "Book the synthetic venue for the spring workshop",
                   speaker=None, registry=None, working_group=key)
        self._task(2, "Review the fictional grant budget spreadsheet",
                   kind="meeting", speaker=None, registry=None, working_group=key)
        stage1.enqueue(self.connection, 1, now=NOW)
        stage1.run(self.connection, now=NOW, backend=FailingEmbeddings())
        self.assertEqual(self._routes(), {})

    def test_large_group_pair_does_not_outrank_independent_signals(self) -> None:
        # A large group has score 0.5 with weight 0.2
        large_group_score = stage1._combined_score(
            {"words": 0.5, "working_group": 0.5}
        )
        independent_signals = stage1._combined_score(
            {"words": 0.5, "owner": 1.0}
        )
        self.assertGreater(independent_signals, large_group_score)

    def test_group_size_weighting_favors_smaller_groups(self) -> None:
        small_score = stage1._working_group_score(
            lexical.DuplicateCandidate(1, "text", 1, "open", None, "email", "2030-01-01", "rec", 1, "person", None, None, None, False, working_group="wg_small"),
            lexical.DuplicateCandidate(2, "text", 1, "open", None, "email", "2030-01-01", "rec", 1, "person", None, None, None, False, working_group="wg_small"),
            group_sizes={"wg_small": 3},
        )
        large_score = stage1._working_group_score(
            lexical.DuplicateCandidate(3, "text", 1, "open", None, "email", "2030-01-01", "rec", 1, "person", None, None, None, False, working_group="wg_large"),
            lexical.DuplicateCandidate(4, "text", 1, "open", None, "email", "2030-01-01", "rec", 1, "person", None, None, None, False, working_group="wg_large"),
            group_sizes={"wg_large": 30},
        )
        self.assertEqual(small_score, 1.0)
        self.assertEqual(large_score, 0.5)
        self.assertGreater(small_score, large_score)

    def test_old_closed_tasks_do_not_dilute_an_active_small_group(self) -> None:
        key = "wg_" + "a" * 32
        self._task(1, "Prepare the synthetic rollout checklist", working_group=key)
        self._task(
            2,
            "Draft the synthetic rollout checklist",
            kind="meeting",
            working_group=key,
        )
        for task_id in range(3, 23):
            self._task(
                task_id,
                f"Archived synthetic action {task_id}",
                working_group=key,
            )
            self.connection.execute(
                "UPDATE tasks SET status='done',closed_at=? WHERE id=?",
                ("2020-01-01T00:00:00+00:00", task_id),
            )

        stage1.enqueue(self.connection, 1, now=NOW)
        stage1.run(self.connection, now=NOW, backend=FailingEmbeddings())

        routes = self._routes()[(1, 2)]
        self.assertEqual(routes["working_group"], 1.0)

    def test_changed_key_removes_stale_route_and_rank_boost(self) -> None:
        key = "wg_" + "a" * 32
        self._task(
            1,
            "Prepare the synthetic rollout checklist alpha",
            working_group=key,
        )
        self._task(
            2,
            "Draft the synthetic rollout checklist beta",
            kind="meeting",
            working_group=key,
        )
        stage1.enqueue(self.connection, 1, now=NOW)
        stage1.run(self.connection, now=NOW, backend=FailingEmbeddings())
        before = self.connection.execute(
            "SELECT rank_score FROM task_duplicate_candidates "
            "WHERE left_task_id=1 AND right_task_id=2"
        ).fetchone()[0]
        self.assertIn("working_group", self._routes()[(1, 2)])

        self.connection.execute(
            "UPDATE tasks SET working_group=? WHERE id=1",
            ("wg_" + "b" * 32,),
        )
        stage1.enqueue(self.connection, 1, now=NOW)
        stage1.run(self.connection, now=NOW, backend=FailingEmbeddings())

        after = self.connection.execute(
            "SELECT rank_score FROM task_duplicate_candidates "
            "WHERE left_task_id=1 AND right_task_id=2"
        ).fetchone()[0]
        self.assertNotIn("working_group", self._routes()[(1, 2)])
        self.assertLess(after, before)


class RereadSignalTests(StageOneTests):
    """A shared record supports an independent lead but is not one itself."""

    def _same_record(self, left: int, right: int) -> None:
        self.connection.execute(
            "UPDATE candidate_inbox SET source_record_id='record-shared' "
            "WHERE candidate_id IN (?,?)",
            (f"candidate-{left}", f"candidate-{right}"),
        )
        self.connection.execute(
            "UPDATE candidate_inbox SET created_at=? WHERE candidate_id=?",
            ("2030-03-01T13:00:00+00:00", f"candidate-{right}"),
        )

    def test_reread_alone_queues_nothing(self) -> None:
        self._task(1, "Book the synthetic workshop venue", kind="meeting")
        self._task(2, "Order fictional laboratory supplies", kind="meeting")
        self._same_record(1, 2)
        stage1.enqueue(self.connection, 1, now=NOW)

        stage1.run(self.connection, now=NOW, backend=FailingEmbeddings())

        self.assertEqual(
            self.connection.execute(
                "SELECT count(*) FROM task_duplicate_candidates"
            ).fetchone()[0],
            0,
        )

    def test_reread_strengthens_an_independent_route(self) -> None:
        self._task(1, "Prepare the synthetic rollout checklist", kind="meeting")
        self._task(2, "Revise the synthetic rollout checklist", kind="meeting")
        self._same_record(1, 2)
        stage1.enqueue(self.connection, 1, now=NOW)

        stage1.run(self.connection, now=NOW, backend=FailingEmbeddings())

        routes = {
            row[0] for row in self.connection.execute(
                "SELECT route FROM task_duplicate_candidate_routes"
            )
        }
        self.assertIn("words", routes)
        self.assertIn("reread", routes)


class ParticipantSignalTests(StageOneTests):
    """Participant agreement strengthens a pair; it never creates one alone."""

    def _participant(
        self,
        task_id: int,
        *,
        person_id: str | None = None,
        speaker_id: str | None = "SPK_1",
        registry: str | None = "registry-main",
    ) -> None:
        if speaker_id is None:
            registry = None
        self.connection.execute(
            "INSERT INTO task_participants(task_id,position,kind,speaker_id,"
            "canonical_speaker_id,speaker_registry_id,person_id) "
            "VALUES(?,0,'person',?,?,?,?)",
            (task_id, speaker_id, speaker_id, registry, person_id),
        )

    def test_participant_alone_queues_nothing(self) -> None:
        self._task(1, "Book the synthetic workshop venue", kind="meeting")
        self._task(2, "Order fictional laboratory supplies", kind="meeting")
        self._participant(1, person_id="person_" + "a" * 32)
        self._participant(2, person_id="person_" + "a" * 32)
        stage1.enqueue(self.connection, 1, now=NOW)

        stage1.run(self.connection, now=NOW, backend=FailingEmbeddings())

        self.assertEqual(
            self.connection.execute(
                "SELECT count(*) FROM task_duplicate_candidates"
            ).fetchone()[0],
            0,
        )

    def test_participant_strengthens_an_independent_route(self) -> None:
        self._task(1, "Review the synthetic rollout checklist alpha", kind="meeting")
        self._task(2, "Draft the synthetic rollout checklist beta", kind="meeting")
        stage1.enqueue(self.connection, 1, now=NOW)
        stage1.run(self.connection, now=NOW, backend=FailingEmbeddings())
        score_without = self.connection.execute(
            "SELECT rank_score FROM task_duplicate_candidates WHERE left_task_id=1 AND right_task_id=2"
        ).fetchone()[0]

        self._participant(1, person_id="person_" + "a" * 32)
        self._participant(2, person_id="person_" + "a" * 32)
        stage1.enqueue(self.connection, 1, now=NOW)
        stage1.run(self.connection, now=NOW, backend=FailingEmbeddings())

        routes = {
            row[0] for row in self.connection.execute(
                "SELECT route FROM task_duplicate_candidate_routes"
            )
        }
        self.assertIn("words", routes)
        self.assertIn("participant", routes)
        score_with = self.connection.execute(
            "SELECT rank_score FROM task_duplicate_candidates WHERE left_task_id=1 AND right_task_id=2"
        ).fetchone()[0]
        self.assertGreater(score_with, score_without)

    def test_different_or_missing_participant_is_neutral(self) -> None:
        # Task 1 & 2 have different participants; Task 3 & 4 have missing participants.
        # Both pairs have identical wording overlap and should yield the exact same score.
        self._task(1, "Review the synthetic weekly report checklist", kind="meeting")
        self._task(2, "Draft the synthetic weekly report checklist", kind="meeting")
        self._participant(1, person_id="person_" + "a" * 32)
        self._participant(2, person_id="person_" + "b" * 32)

        self._task(3, "Review the synthetic weekly report checklist", kind="teams")
        self._task(4, "Draft the synthetic weekly report checklist", kind="teams")

        stage1.enqueue(self.connection, 1, now=NOW)
        stage1.enqueue(self.connection, 3, now=NOW)

        stage1.run(self.connection, now=NOW, backend=FailingEmbeddings())

        scores = dict(self.connection.execute(
            "SELECT left_task_id||'-'||right_task_id, rank_score "
            "FROM task_duplicate_candidates"
        ).fetchall())
        self.assertEqual(scores["1-2"], scores["3-4"])

    def test_missing_or_different_owner_and_working_group_are_neutral(self) -> None:
        # Task 1 & 2 have different owners and working groups.
        # Task 3 & 4 have missing owners and working groups.
        # Neither pair should be penalized or vetoed; both should have the exact same score.
        self._task(
            1, "Review the synthetic deployment guide", kind="meeting",
            speaker="SPK_10", registry="reg-1",
            working_group="wg_" + "1" * 32,
        )
        self._task(
            2, "Draft the synthetic deployment guide", kind="meeting",
            speaker="SPK_20", registry="reg-1",
            working_group="wg_" + "2" * 32,
        )

        self._task(
            3, "Review the synthetic deployment guide", kind="teams",
            speaker=None, registry=None, working_group=None,
        )
        self._task(
            4, "Draft the synthetic deployment guide", kind="teams",
            speaker=None, registry=None, working_group=None,
        )

        stage1.enqueue(self.connection, 1, now=NOW)
        stage1.enqueue(self.connection, 3, now=NOW)

        stage1.run(self.connection, now=NOW, backend=FailingEmbeddings())

        scores = dict(self.connection.execute(
            "SELECT left_task_id||'-'||right_task_id, rank_score "
            "FROM task_duplicate_candidates"
        ).fetchall())
        self.assertIn("1-2", scores)
        self.assertIn("3-4", scores)
        self.assertEqual(scores["1-2"], scores["3-4"])
