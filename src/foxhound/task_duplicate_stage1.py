"""Bounded, recall-first duplicate candidacy outside the intake transaction."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import sqlite3
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Protocol, Sequence

from .candidate_inbox import CandidateInbox, InboxError
from . import task_duplicate_detection as lexical


MODEL_ID = "intfloat/multilingual-e5-base"
DEFAULT_TASK_LIMIT = 20
DEFAULT_TOP_K = 5
DEFAULT_PAIR_LIMIT = 100
CALIBRATION_PRECISION_FLOOR = 0.80


class EmbeddingUnavailable(RuntimeError):
    """The local embedding path cannot safely score this pass."""


class EmbeddingBackend(Protocol):
    model_id: str

    def encode(self, texts: Sequence[str]) -> Sequence[Sequence[float]]:
        """Return one vector per text."""


class SentenceTransformerBackend:
    """Lazy in-process E5 backend; model files must already be local."""

    model_id = MODEL_ID

    def __init__(self) -> None:
        self._model = None

    def encode(self, texts: Sequence[str]) -> Sequence[Sequence[float]]:
        try:
            from sentence_transformers import SentenceTransformer
        except (ImportError, OSError) as exc:  # pragma: no cover - deployment path
            raise EmbeddingUnavailable("local embedding runtime unavailable") from exc
        if self._model is None:
            try:
                # CPU, deliberately. The hosts that run this share one GPU
                # with speech recognition and local model serving; on a busy
                # card the default device fails with CUDA out-of-memory, and
                # a background candidacy pass must not compete with, or be
                # starved by, the workloads the operator is actually waiting
                # on. The E5 base model encodes a batch of task texts on CPU
                # in well under a second.
                self._model = SentenceTransformer(
                    self.model_id,
                    local_files_only=True,
                    device="cpu",
                )
            except (OSError, RuntimeError, ValueError) as exc:
                raise EmbeddingUnavailable("local embedding model unavailable") from exc
        try:
            return self._model.encode(
                [f"query: {text}" for text in texts],
                normalize_embeddings=True,
                convert_to_numpy=True,
            )
        except (OSError, RuntimeError, ValueError) as exc:
            raise EmbeddingUnavailable("local embedding inference failed") from exc


@dataclass(frozen=True)
class Calibration:
    threshold: float
    labels: int
    confirmed: int
    rejected: int
    true_positive: int
    false_positive: int
    false_negative: int
    true_negative: int

    @property
    def precision(self) -> float:
        denominator = self.true_positive + self.false_positive
        return 0.0 if denominator == 0 else self.true_positive / denominator

    @property
    def recall(self) -> float:
        denominator = self.true_positive + self.false_negative
        return 0.0 if denominator == 0 else self.true_positive / denominator


@dataclass(frozen=True)
class StageOneResult:
    tasks_selected: int = 0
    tasks_completed: int = 0
    embedding_retries: int = 0
    pairs_considered: int = 0
    pairs_queued: int = 0
    pairs_unchanged: int = 0
    capped: int = 0
    threshold: float | None = None
    label_count: int = 0
    precision: float | None = None
    recall: float | None = None


def comparable_digest(connection: sqlite3.Connection, task_id: int) -> str:
    """Digest only fields whose change requires duplicate re-evaluation."""
    row = connection.execute(
        "SELECT text,object,action,owner,owner_ref_version,owner_kind,"
        "owner_speaker_id,owner_canonical_speaker_id,owner_speaker_registry_id,"
        "owner_provisional,owner_person_id FROM tasks WHERE id=?",
        (task_id,),
    ).fetchone()
    if row is None:
        raise ValueError("task does not exist")
    participants = connection.execute(
        "SELECT kind,speaker_id,canonical_speaker_id,speaker_registry_id,person_id "
        "FROM task_participants WHERE task_id=? ORDER BY position",
        (task_id,),
    ).fetchall()
    material = {
        "task": [row[key] for key in row.keys()],
        "participants": [[item[key] for key in item.keys()] for item in participants],
    }
    encoded = json.dumps(
        material, ensure_ascii=True, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def enqueue(connection: sqlite3.Connection, task_id: int, *, now: str) -> None:
    """Record or refresh one task's durable stage-one obligation."""
    row = connection.execute(
        "SELECT version FROM tasks WHERE id=?", (task_id,)
    ).fetchone()
    if row is None:
        raise ValueError("task does not exist")
    digest = comparable_digest(connection, task_id)
    connection.execute(
        "INSERT INTO task_duplicate_checks("
        "task_id,task_version,content_digest,signals_done,embedding_done,"
        "embedding_attempts,enqueued_at,updated_at) VALUES(?,?,?,0,0,0,?,?) "
        "ON CONFLICT(task_id) DO UPDATE SET "
        "task_version=excluded.task_version,content_digest=excluded.content_digest,"
        "signals_done=0,embedding_done=0,embedding_attempts=0,"
        "enqueued_at=excluded.enqueued_at,updated_at=excluded.updated_at "
        "WHERE task_duplicate_checks.task_version<>excluded.task_version "
        "OR task_duplicate_checks.content_digest<>excluded.content_digest",
        (task_id, int(row["version"]), digest, now, now),
    )


def _normalised(vector: Sequence[float]) -> tuple[float, ...]:
    try:
        values = tuple(float(value) for value in vector)
    except (TypeError, ValueError) as exc:
        raise EmbeddingUnavailable("local embedding vector is invalid") from exc
    if not values or any(not math.isfinite(value) for value in values):
        raise EmbeddingUnavailable("local embedding vector is invalid")
    norm = math.sqrt(sum(value * value for value in values))
    if norm <= 0:
        raise EmbeddingUnavailable("local embedding vector is invalid")
    return tuple(value / norm for value in values)


def _similarity(left: Sequence[float], right: Sequence[float]) -> float:
    if len(left) != len(right):
        raise EmbeddingUnavailable("local embedding dimensions disagree")
    return max(0.0, min(1.0, sum(a * b for a, b in zip(left, right))))


def _embedding_text(candidate: lexical.DuplicateCandidate) -> str:
    return candidate.object or candidate.task_text


def _vectors(
    connection: sqlite3.Connection,
    candidates: Sequence[lexical.DuplicateCandidate],
    *,
    backend: EmbeddingBackend,
    now: str,
) -> dict[int, tuple[float, ...]]:
    vectors: dict[int, tuple[float, ...]] = {}
    missing: list[lexical.DuplicateCandidate] = []
    for candidate in candidates:
        digest = _candidate_digest(candidate)
        row = connection.execute(
            "SELECT vector_json FROM task_duplicate_embeddings WHERE task_id=? "
            "AND task_version=? AND content_digest=? AND model_id=?",
            (candidate.task_id, candidate.task_version, digest, backend.model_id),
        ).fetchone()
        if row is None:
            missing.append(candidate)
            continue
        try:
            vectors[candidate.task_id] = _normalised(json.loads(row["vector_json"]))
        except (json.JSONDecodeError, TypeError, EmbeddingUnavailable):
            missing.append(candidate)
    if missing:
        encoded = backend.encode([_embedding_text(item) for item in missing])
        if len(encoded) != len(missing):
            raise EmbeddingUnavailable("local embedding batch is incomplete")
        for candidate, raw_vector in zip(missing, encoded, strict=True):
            vector = _normalised(raw_vector)
            vectors[candidate.task_id] = vector
            connection.execute(
                "INSERT INTO task_duplicate_embeddings("
                "task_id,task_version,content_digest,model_id,vector_json,created_at) "
                "VALUES(?,?,?,?,?,?) ON CONFLICT(task_id,task_version) DO UPDATE SET "
                "content_digest=excluded.content_digest,model_id=excluded.model_id,"
                "vector_json=excluded.vector_json,created_at=excluded.created_at",
                (
                    candidate.task_id,
                    candidate.task_version,
                    _candidate_digest(candidate),
                    backend.model_id,
                    json.dumps(vector, separators=(",", ":")),
                    now,
                ),
            )
    return vectors


def _candidate_digest(candidate: lexical.DuplicateCandidate) -> str:
    material = json.dumps(
        [candidate.task_text, candidate.object],
        ensure_ascii=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(material).hexdigest()


def calibrate(
    connection: sqlite3.Connection,
    vectors: dict[int, tuple[float, ...]],
) -> Calibration:
    """Choose maximum recall at the labelled precision floor, then F1."""
    rows = connection.execute(
        "SELECT left_task_id,right_task_id,state FROM task_duplicate_proposals "
        "WHERE state IN ('confirmed','rejected') ORDER BY id"
    ).fetchall()
    labels = [
        (
            _similarity(vectors[int(row["left_task_id"])],
                        vectors[int(row["right_task_id"])]),
            row["state"] == "confirmed",
        )
        for row in rows
        if int(row["left_task_id"]) in vectors
        and int(row["right_task_id"]) in vectors
    ]
    if not any(label for _, label in labels) or not any(
        not label for _, label in labels
    ):
        raise EmbeddingUnavailable("embedding threshold lacks reader labels")
    candidates = sorted({score for score, _ in labels}, reverse=True)
    measured = [_measure_threshold(labels, threshold) for threshold in candidates]
    eligible = [item for item in measured if item.precision >= CALIBRATION_PRECISION_FLOOR]
    if eligible:
        return max(eligible, key=lambda item: (item.recall, item.precision, item.threshold))
    return max(
        measured,
        key=lambda item: (
            0.0 if item.precision + item.recall == 0 else
            2 * item.precision * item.recall / (item.precision + item.recall),
            item.threshold,
        ),
    )


def _measure_threshold(
    labels: Sequence[tuple[float, bool]], threshold: float
) -> Calibration:
    tp = fp = fn = tn = 0
    for score, confirmed in labels:
        predicted = score >= threshold
        if predicted and confirmed:
            tp += 1
        elif predicted:
            fp += 1
        elif confirmed:
            fn += 1
        else:
            tn += 1
    return Calibration(threshold, len(labels), tp + fn, fp + tn, tp, fp, fn, tn)


def run(
    connection: sqlite3.Connection,
    *,
    now: str,
    backend: EmbeddingBackend,
    task_limit: int = DEFAULT_TASK_LIMIT,
    top_k: int = DEFAULT_TOP_K,
    pair_limit: int = DEFAULT_PAIR_LIMIT,
) -> StageOneResult:
    if any(
        isinstance(value, bool) or not isinstance(value, int) or value <= 0
        for value in (task_limit, top_k, pair_limit)
    ):
        raise ValueError("stage-one limits must be positive integers")
    queue = connection.execute(
        "SELECT task_id,task_version,content_digest FROM task_duplicate_checks "
        "WHERE embedding_done=0 OR signals_done=0 "
        # Outstanding work first, oldest within it. Ordering by enqueue time
        # alone starved the queue: when embeddings keep failing, the oldest
        # tasks stay `embedding_done=0` and head the queue forever, so tasks
        # behind them never got even their local signals. Tasks still owed
        # signals come first; embedding retries follow, least-tried first,
        # so a persistent embedding outage rotates instead of stalling.
        "ORDER BY signals_done,embedding_attempts,enqueued_at,task_id LIMIT ?",
        (task_limit,),
    ).fetchall()
    if not queue:
        return StageOneResult()
    candidates = tuple(lexical._candidates(connection))
    by_id = {item.task_id: item for item in candidates}
    focus = {int(row["task_id"]) for row in queue if int(row["task_id"]) in by_id}
    weights = lexical._weights(candidates)
    offers: dict[tuple[int, int], dict[str, float | None]] = {}
    considered = 0

    def offer(left: lexical.DuplicateCandidate, right: lexical.DuplicateCandidate,
              route: str, score: float | None) -> None:
        key = tuple(sorted((left.task_id, right.task_id)))
        routes = offers.setdefault(key, {})
        previous = routes.get(route)
        if previous is None or (score is not None and score > previous):
            routes[route] = score

    record_counts: dict[str, int] = {}
    for item in candidates:
        if lexical._own_record(item):
            record_counts[item.source_record_id] = (
                record_counts.get(item.source_record_id, 0) + 1
            )
    for left in candidates:
        if left.task_id not in focus:
            continue
        for right in candidates:
            if right.task_id == left.task_id or not lexical._comparable(left, right, now=now):
                continue
            considered += 1
            shared = left.terms & right.terms
            coverage = lexical._weighted_coverage(left, right, weights)
            if len(shared) >= lexical.MIN_SHARED_TERMS and coverage >= lexical.MIN_WEIGHTED_COVERAGE:
                offer(left, right, "words", coverage)
            if (
                lexical._own_record(left)
                and left.source_record_id == right.source_record_id
                and record_counts.get(left.source_record_id, 0) <= lexical.MAX_RECORD_FANOUT
            ):
                offer(left, right, "reread", 1.0)
            if lexical._resolved_participants(left.participants) & lexical._resolved_participants(right.participants):
                offer(left, right, "participant", 0.9)

    calibration: Calibration | None = None
    embedding_failed = False
    try:
        vectors = _vectors(connection, candidates, backend=backend, now=now)
        calibration = calibrate(connection, vectors)
        for left in candidates:
            if left.task_id not in focus:
                continue
            for right in candidates:
                if right.task_id == left.task_id or not lexical._comparable(left, right, now=now):
                    continue
                score = _similarity(vectors[left.task_id], vectors[right.task_id])
                if score >= calibration.threshold:
                    offer(left, right, "embedding", score)
    except (EmbeddingUnavailable, OSError, RuntimeError, ValueError):
        embedding_failed = True

    offered = sorted(
        offers,
        key=lambda key: (
            -max((score or 0.0) for score in offers[key].values()), key
        ),
    )
    ranked: list[tuple[int, int]] = []
    degree: dict[int, int] = {}
    for key in offered:
        if len(ranked) >= pair_limit:
            break
        if any(degree.get(task_id, 0) >= top_k for task_id in key):
            continue
        ranked.append(key)
        for task_id in key:
            degree[task_id] = degree.get(task_id, 0) + 1
    capped = len(offered) - len(ranked)
    queued = unchanged = 0
    for key in ranked:
        left, right = by_id[key[0]], by_id[key[1]]
        rank_score = max((score or 0.0) for score in offers[key].values())
        existing = connection.execute(
            "SELECT id FROM task_duplicate_candidates WHERE "
            "left_task_id=? AND right_task_id=? AND left_task_version=? "
            "AND right_task_version=?",
            (left.task_id, right.task_id, left.task_version, right.task_version),
        ).fetchone()
        cursor = connection.execute(
            "INSERT INTO task_duplicate_candidates("
            "left_task_id,right_task_id,left_task_version,right_task_version,"
            "rank_score,state,created_at,updated_at) VALUES(?,?,?,?,?,'queued',?,?) "
            "ON CONFLICT(left_task_id,right_task_id,left_task_version,right_task_version) "
            "DO UPDATE SET rank_score=MAX(rank_score,excluded.rank_score),"
            "updated_at=excluded.updated_at RETURNING id",
            (left.task_id, right.task_id, left.task_version, right.task_version,
             rank_score, now, now),
        )
        candidate_id = int(cursor.fetchone()[0])
        for route, score in sorted(offers[key].items()):
            connection.execute(
                "INSERT INTO task_duplicate_candidate_routes(candidate_id,route,score) "
                "VALUES(?,?,?) ON CONFLICT(candidate_id,route) DO UPDATE SET "
                "score=MAX(score,excluded.score)",
                (candidate_id, route, score),
            )
        if existing is not None:
            unchanged += 1
        else:
            queued += 1

    for row in queue:
        task_id = int(row["task_id"])
        if task_id not in by_id:
            connection.execute("DELETE FROM task_duplicate_checks WHERE task_id=?", (task_id,))
        elif embedding_failed:
            connection.execute(
                "UPDATE task_duplicate_checks SET signals_done=1,"
                "embedding_attempts=embedding_attempts+1,updated_at=? WHERE task_id=?",
                (now, task_id),
            )
        else:
            connection.execute("DELETE FROM task_duplicate_checks WHERE task_id=?", (task_id,))
    return StageOneResult(
        tasks_selected=len(queue),
        tasks_completed=0 if embedding_failed else len(queue),
        embedding_retries=len(queue) if embedding_failed else 0,
        pairs_considered=considered,
        pairs_queued=queued,
        pairs_unchanged=unchanged,
        capped=capped,
        threshold=None if calibration is None else calibration.threshold,
        label_count=0 if calibration is None else calibration.labels,
        precision=None if calibration is None else round(calibration.precision, 3),
        recall=None if calibration is None else round(calibration.recall, 3),
    )


def run_database(
    database_path: str | Path,
    *,
    backend: EmbeddingBackend | None = None,
    task_limit: int = DEFAULT_TASK_LIMIT,
    top_k: int = DEFAULT_TOP_K,
    pair_limit: int = DEFAULT_PAIR_LIMIT,
) -> StageOneResult:
    inbox = CandidateInbox(database_path)
    if not inbox.database_path.is_file() or inbox.database_path.is_symlink():
        raise InboxError("candidate inbox is not initialized")
    with closing(sqlite3.connect(inbox.database_path, timeout=5)) as connection:
        connection.row_factory = sqlite3.Row
        inbox._require_current_schema(connection)
        connection.execute("BEGIN IMMEDIATE")
        try:
            result = run(
                connection,
                now=datetime.now(timezone.utc).isoformat(timespec="seconds"),
                backend=backend or SentenceTransformerBackend(),
                task_limit=task_limit,
                top_k=top_k,
                pair_limit=pair_limit,
            )
            connection.commit()
            return result
        except Exception:
            connection.rollback()
            raise


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="foxhound-task-duplicate-stage1",
        description="Run one bounded local duplicate-candidacy pass",
    )
    parser.add_argument("--database", required=True, type=Path)
    parser.add_argument("--limit", default=DEFAULT_TASK_LIMIT, type=int)
    parser.add_argument("--top-k", default=DEFAULT_TOP_K, type=int)
    parser.add_argument("--pair-limit", default=DEFAULT_PAIR_LIMIT, type=int)
    return parser


def main(argv: list[str] | None = None) -> int:
    arguments = _parser().parse_args(argv)
    try:
        result = run_database(
            arguments.database,
            task_limit=arguments.limit,
            top_k=arguments.top_k,
            pair_limit=arguments.pair_limit,
        )
    except (InboxError, sqlite3.Error, OSError, RuntimeError, ValueError):
        print(json.dumps({"accepted": False}, separators=(",", ":")))
        return 2
    print(json.dumps({"accepted": True, **result.__dict__}, sort_keys=True))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
