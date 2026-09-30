"""Synthetic Researcher-to-scheduling-to-Undo acceptance test."""

from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path

from foxhound import migrate_database
from foxhound.agent_profiles import general_profile
from foxhound.knowledge_client import KnowledgeDocument, KnowledgeLayer, KnowledgeSearchResult
from foxhound.task_research import INPUT_SCHEMA, ResearchStore
from foxhound.task_research_scheduling import apply_published_recommendations
from foxhound.task_research_synthesis import SynthesisConfig, synthesize
from foxhound.task_scheduling import SchedulingDisposition, TaskSchedulingService


NOW = datetime(2032, 3, 4, 5, 6, 7, tzinfo=timezone.utc)


class _Knowledge:
    def search(self, _query: str, **_options: object) -> KnowledgeSearchResult:
        return KnowledgeSearchResult((
            KnowledgeLayer("kb", 1, False, (KnowledgeDocument(
                id="kb:Projects/Synthetic-Launch.md",
                path="Projects/Synthetic-Launch.md",
                kb_path="Projects/Synthetic-Launch.md",
                section="prerequisite",
                excerpt="The synthetic source review must finish before drafting.",
                date="2032-03-01",
            ),)),
            KnowledgeLayer("secondary", 0, False, ()),
            KnowledgeLayer("emails", 0, False, ()),
        ))


class _Response:
    def __init__(self, payload: bytes) -> None:
        self.payload = payload

    def __enter__(self):
        return self

    def __exit__(self, *_args: object) -> bool:
        return False

    def read(self, amount: int) -> bytes:
        return self.payload[:amount]


class _Opener:
    def __init__(self, draft: dict[str, object]) -> None:
        self.draft = draft

    def open(self, _request: object, _timeout: float | None = None, **_kwargs: object) -> _Response:
        envelope = {
            "choices": [{"message": {"content": json.dumps(self.draft)}}],
            "usage": {"prompt_tokens": 31, "completion_tokens": 19},
            "reasoning_effective": "high",
        }
        return _Response(json.dumps(envelope).encode("utf-8"))


def _claim(text: str, *, unknown: bool = False) -> dict[str, object]:
    return {
        "text": text,
        "status": "unknown" if unknown else "supported",
        "source_refs": [] if unknown else ["src-001"],
    }


def _draft() -> dict[str, object]:
    return {
        "schema_version": "foxhound.task-research-draft.v1",
        "research_status": "sufficient",
        "objective": _claim("Produce the synthetic launch brief."),
        "requested_action": _claim("Draft the brief from approved sources."),
        "current_state": [_claim("The source review remains open.")],
        "expected_deliverables": [_claim("One reviewed synthetic brief.")],
        "timeline": [],
        "decisions": [],
        "dependencies": [_claim("Task 2 must finish first.")],
        "constraints": [],
        "stakeholders": [],
        "related_entities": [],
        "findings": [_claim("The prerequisite is explicit.")],
        "conflicts": [],
        "open_questions": [_claim("The reviewer is unknown.", unknown=True)],
        "scheduling_recommendations": [{
            "type": "after_task_completed",
            "related_task_id": 2,
            "confidence": 0.95,
            "rationale": _claim("Task 2 produces the required source review."),
        }],
    }


class TaskResearchPipelineTests(unittest.TestCase):
    def test_structured_task_research_scheduling_delivery_and_undo(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        database = root / "foxhound.sqlite3"
        cas = root / "research-cas"
        task_root = root / "Tasks"
        task_folder = task_root / "T1-synthetic-launch"
        cas.mkdir(mode=0o700)
        task_folder.mkdir(parents=True, mode=0o700)
        migrate_database(database)
        profile = general_profile()
        stamp = NOW.isoformat(timespec="seconds")
        with closing(sqlite3.connect(database)) as connection:
            for task_id, text in (
                (1, "Prepare the synthetic launch brief."),
                (2, "Finish the synthetic source review."),
            ):
                connection.execute(
                    "INSERT INTO tasks(id,status,text,version,created_at,updated_at) "
                    "VALUES(?,'open',?,1,?,?)",
                    (task_id, text, stamp, stamp),
                )
                connection.execute(
                    "INSERT INTO task_execution_workflows(task_id,task_version,status,"
                    "phase,version,failure_count,created_at,updated_at,agent_profile_id,"
                    "agent_profile_revision,queue_priority) "
                    "VALUES(?,1,?,'plan',1,0,?,?,?,?, 'normal')",
                    (
                        task_id,
                        "queued" if task_id == 1 else "awaiting_start",
                        stamp,
                        stamp,
                        profile.profile_id,
                        profile.revision,
                    ),
                )
            connection.commit()

        snapshot = {
            "schema_version": INPUT_SCHEMA,
            "task_id": 1,
            "task_version": 1,
            "text": "Prepare the synthetic launch brief.",
            "structured": {
                "action": "prepare",
                "object": "synthetic launch brief",
                "confidence": 0.94,
            },
            "due": None,
            "owner": None,
            "participants": [],
            "working_group": None,
            "external_identifiers": [],
            "origin": {"kind": "synthetic", "source_digest": "1" * 64},
            "structured_schema_revisions": {"task": 1},
        }
        store = ResearchStore(database, cas, clock=lambda: NOW)
        job = store.request(
            snapshot, task_work_root=task_root, task_folder=task_folder
        )
        claimed = store.claim("synthetic-researcher")
        self.assertIsNotNone(claimed)
        assert claimed is not None
        context = store.context(job.job_id, claimed.token)
        synthesis = synthesize(
            context,
            knowledge=_Knowledge(),
            config=SynthesisConfig(
                model="synthetic-thinking-model",
                endpoint="http://127.0.0.1:8800",
                profile_id="researcher",
                profile_revision="a" * 64,
                provider="synthetic",
                reasoning="high",
            ),
            opener=_Opener(_draft()),
        )
        published = store.publish(
            job_id=job.job_id,
            token=claimed.token,
            draft=synthesis.draft,
            sources=list(synthesis.sources),
            provenance=synthesis.provenance,
            coverage=synthesis.coverage,
        )
        self.assertTrue((task_folder / ".task-research.json").is_file())
        self.assertTrue((task_folder / "Research.md").is_file())

        applied = apply_published_recommendations(
            database, published, clock=lambda: NOW
        )
        self.assertEqual(1, len(applied))
        self.assertEqual(SchedulingDisposition.APPLIED, applied[0].disposition)
        replay = apply_published_recommendations(
            database, published, clock=lambda: NOW
        )
        self.assertEqual(SchedulingDisposition.UNCHANGED, replay[0].disposition)

        scheduling = TaskSchedulingService(
            database,
            clock=lambda: NOW,
            token_factory=lambda: "synthetic-delivery-token-000000000000000000000",
        )
        delivery = scheduling.claim_next(consumer_digest="b" * 64)
        self.assertIsNotNone(delivery)
        assert delivery is not None
        self.assertEqual(applied[0].card_id, delivery.card.card_id)
        acknowledged = scheduling.complete_delivery(
            delivery.card.card_id,
            expected_version=delivery.card.version,
            claim_token=delivery.token,
            transport="synthetic",
            delivery_ref="synthetic-card-1",
        )
        self.assertTrue(acknowledged.accepted)
        undone = scheduling.act(
            delivery.card.card_id,
            expected_version=delivery.card.version,
            action="undo",
        )
        self.assertEqual(SchedulingDisposition.APPLIED, undone.disposition)
        with closing(sqlite3.connect(database)) as connection:
            condition = connection.execute(
                "SELECT state FROM task_scheduling_conditions"
            ).fetchone()[0]
            workflow = connection.execute(
                "SELECT status,version,queue_priority,queue_priority_source "
                "FROM task_execution_workflows WHERE task_id=1"
            ).fetchone()
            card = connection.execute(
                "SELECT status,resolution FROM task_scheduling_review_cards"
            ).fetchone()
        self.assertEqual("canceled", condition)
        self.assertEqual(("queued", 1, "normal", None), workflow)
        self.assertEqual(("resolved", "undo"), card)
