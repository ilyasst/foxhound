"""Receipt-gated projection from published research into bounded scheduling."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from contextlib import closing
from datetime import datetime
from pathlib import Path
from typing import Callable, Mapping

from .candidate_inbox import CandidateInbox, SCHEMA_VERSION
from .task_ledger import TaskLedgerError
from .task_research import PUBLISHED_SCHEMA
from .task_scheduling import (
    SchedulingApplyResult,
    SchedulingDisposition,
    SchedulingKind,
    SchedulingRefusal,
    TaskSchedulingService,
    ValidatedSchedulingRecommendation,
)


MIN_AUTOMATIC_CONFIDENCE = 0.8


class ResearchSchedulingError(RuntimeError):
    """Published research cannot safely cross the scheduling boundary."""


def _canonical_digest(document: Mapping[str, object]) -> str:
    payload = json.dumps(
        document, ensure_ascii=False, separators=(",", ":"), sort_keys=True
    )
    payload = (payload + "\n").encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _connect(database_path: Path) -> sqlite3.Connection:
    if not database_path.is_file() or database_path.is_symlink():
        raise ResearchSchedulingError("research scheduling database is unavailable")
    connection = sqlite3.connect(database_path, timeout=5)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    if int(connection.execute("PRAGMA user_version").fetchone()[0]) != SCHEMA_VERSION:
        connection.close()
        raise ResearchSchedulingError("research scheduling schema is not current")
    CandidateInbox._require_schema(connection)
    return connection


def apply_published_recommendations(
    database_path: Path | str,
    document: object,
    *,
    clock: Callable[[], datetime] | None = None,
    minimum_confidence: float = MIN_AUTOMATIC_CONFIDENCE,
) -> tuple[SchedulingApplyResult, ...]:
    """Apply sufficiently grounded recommendations from one immutable receipt.

    Low-confidence, ungrounded, or non-sufficient recommendations remain in the
    report but do not mutate the queue. Replays converge through the scheduling
    ledger's receipt-and-recommendation identity check.
    """
    if (
        not isinstance(minimum_confidence, (int, float))
        or isinstance(minimum_confidence, bool)
        or not 0 <= minimum_confidence <= 1
    ):
        raise ResearchSchedulingError("invalid automatic confidence threshold")
    if not isinstance(document, Mapping):
        raise ResearchSchedulingError("invalid published research")
    if document.get("schema_version") != PUBLISHED_SCHEMA \
            or document.get("authority") != "evidence_only":
        raise ResearchSchedulingError("invalid published research")
    identity = document.get("task_identity")
    provenance = document.get("provenance")
    sources = document.get("sources")
    recommendations = document.get("scheduling_recommendations")
    if not isinstance(identity, Mapping) or not isinstance(provenance, Mapping) \
            or not isinstance(sources, list) or not isinstance(recommendations, list):
        raise ResearchSchedulingError("invalid published research")
    task_id = identity.get("task_id")
    task_version = identity.get("task_version")
    generation = identity.get("generation")
    job_id = provenance.get("job_id")
    if any(isinstance(value, bool) or not isinstance(value, int) or value < 1
           for value in (task_id, task_version, generation)) \
            or not isinstance(job_id, str) or not job_id:
        raise ResearchSchedulingError("invalid published research identity")
    source_ids: set[str] = set()
    for source in sources:
        if not isinstance(source, Mapping):
            raise ResearchSchedulingError("invalid published research sources")
        source_id = source.get("source_id")
        if not isinstance(source_id, str) or not source_id or source_id in source_ids:
            raise ResearchSchedulingError("invalid published research sources")
        source_ids.add(source_id)
    digest = _canonical_digest(document)
    path = Path(database_path)

    with closing(_connect(path)) as connection:
        receipt = connection.execute(
            "SELECT j.task_id,j.task_version,j.generation,j.state,r.json_digest "
            "FROM task_research_jobs j JOIN task_research_receipts r "
            "ON r.job_id=j.job_id WHERE j.job_id=?",
            (job_id,),
        ).fetchone()
        if (
            receipt is None
            or receipt["state"] != "completed"
            or receipt["json_digest"] != digest
            or (receipt["task_id"], receipt["task_version"], receipt["generation"])
            != (task_id, task_version, generation)
        ):
            raise ResearchSchedulingError("published research receipt mismatch")

    if document.get("research_status") != "sufficient":
        return ()

    def validate_receipt(
        connection: sqlite3.Connection,
        recommendation: ValidatedSchedulingRecommendation,
    ) -> bool:
        row = connection.execute(
            "SELECT j.task_id,j.task_version,j.generation,j.state,r.json_digest "
            "FROM task_research_jobs j JOIN task_research_receipts r "
            "ON r.job_id=j.job_id WHERE j.job_id=?",
            (recommendation.research_receipt_id,),
        ).fetchone()
        return bool(
            row is not None
            and row["state"] == "completed"
            and row["json_digest"] == recommendation.research_document_digest
            and (row["task_id"], row["task_version"], row["generation"])
            == (task_id, task_version, generation)
            and set(recommendation.source_refs) <= source_ids
        )

    service = TaskSchedulingService(
        path, clock=clock, provenance_validator=validate_receipt
    )
    results: list[SchedulingApplyResult] = []
    for item in recommendations:
        if not isinstance(item, Mapping):
            raise ResearchSchedulingError("invalid scheduling recommendation")
        confidence = item.get("confidence")
        rationale = item.get("rationale")
        if not isinstance(confidence, (int, float)) or isinstance(confidence, bool) \
                or not isinstance(rationale, Mapping):
            raise ResearchSchedulingError("invalid scheduling recommendation")
        refs = rationale.get("source_refs")
        if confidence < minimum_confidence or rationale.get("status") != "supported" \
                or not isinstance(refs, list) or not refs:
            continue
        if any(not isinstance(ref, str) or ref not in source_ids for ref in refs):
            raise ResearchSchedulingError("invalid scheduling recommendation sources")
        try:
            kind = SchedulingKind(item.get("type"))
        except (TypeError, ValueError) as exc:
            raise ResearchSchedulingError("invalid scheduling recommendation type") from exc
        with closing(_connect(path)) as connection:
            # The workflow version is an application fence, not part of the
            # Researcher's semantic recommendation.  Match without that fence
            # so replay still converges after a priority change increments it.
            for existing in connection.execute(
                "SELECT s.id,s.kind,s.recommendation_json,"
                "s.resulting_workflow_version,c.id AS card_id "
                "FROM task_scheduling_change_sets s LEFT JOIN "
                "task_scheduling_review_cards c ON c.change_set_id=s.id "
                "WHERE s.research_receipt_id=? ORDER BY s.id",
                (job_id,),
            ):
                try:
                    prior = json.loads(existing["recommendation_json"])
                except (TypeError, ValueError):
                    raise ResearchSchedulingError("stored scheduling recommendation is invalid")
                if (
                    prior.get("kind") == kind.value
                    and prior.get("target_task_id") == task_id
                    and prior.get("target_task_version") == task_version
                    and prior.get("rationale") == str(rationale.get("text", "")).strip()
                    and prior.get("source_refs") == refs
                    and prior.get("research_document_digest") == digest
                    and prior.get("related_task_id") == item.get("related_task_id")
                    and prior.get("not_before") == item.get("not_before")
                    and prior.get("prerequisite_text") == item.get("prerequisite_text")
                ):
                    results.append(SchedulingApplyResult(
                        SchedulingDisposition.UNCHANGED,
                        change_set_id=int(existing["id"]),
                        card_id=None if existing["card_id"] is None else int(existing["card_id"]),
                        resulting_workflow_version=int(existing["resulting_workflow_version"]),
                    ))
                    break
            else:
                existing = None
            if existing is not None:
                continue
            workflow = connection.execute(
                "SELECT version FROM task_execution_workflows WHERE task_id=?",
                (task_id,),
            ).fetchone()
        if workflow is None:
            results.append(SchedulingApplyResult(
                SchedulingDisposition.REFUSED,
                refusal=SchedulingRefusal.NOT_FOUND,
            ))
            continue
        recommendation = ValidatedSchedulingRecommendation(
            kind=kind,
            target_task_id=task_id,
            target_task_version=task_version,
            expected_workflow_version=int(workflow["version"]),
            rationale=str(rationale.get("text", "")),
            source_refs=tuple(refs),
            research_receipt_id=job_id,
            research_document_digest=digest,
            related_task_id=item.get("related_task_id"),
            not_before=item.get("not_before"),
            prerequisite_text=item.get("prerequisite_text"),
        )
        try:
            results.append(service.apply(recommendation))
        except (TaskLedgerError, sqlite3.DatabaseError) as exc:
            raise ResearchSchedulingError("scheduling application failed") from exc
    return tuple(results)
