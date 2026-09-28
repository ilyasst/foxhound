"""Synthetic tests for the bounded stage-one duplicate queue."""

from __future__ import annotations

import math
import sqlite3
import tempfile
import unittest
from pathlib import Path

from foxhound import migrate_database
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

    def _task(self, task_id: int, text: str, *, kind: str = "email") -> None:
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

    def _label(self, left: int, right: int, decision: proposals.Decision) -> None:
        result = proposals.propose(
            self.connection,
            task_id_a=left,
            task_id_b=right,
            basis="Synthetic calibration pair.",
            detector="synthetic-calibration",
            now=NOW,
            allow_unconfirmed_owner=True,
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
        self.assertEqual({row["route"] for row in routes}, {"words"})

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
        self._task(2, "Discuss an unrelated fictional topic", kind="email")
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
        self.assertEqual(result.embedding_retries, 1)


if __name__ == "__main__":
    unittest.main()
