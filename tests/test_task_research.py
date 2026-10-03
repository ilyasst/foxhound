"""Synthetic end-to-end tests for the manual task-research foundation."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import sqlite3
import stat
import tempfile
import unittest
from unittest import mock
from datetime import datetime, timedelta, timezone
from pathlib import Path

from foxhound import migrate_database
from foxhound.task_research import (
    DRAFT_SCHEMA,
    INPUT_SCHEMA,
    MAX_PROJECTION_BYTES,
    PUBLISHED_SCHEMA,
    ResearchError,
    ResearchStore,
    consumer_projection,
    task_input_digest,
)


NOW = datetime(2030, 1, 2, 12, 0, tzinfo=timezone.utc)


def snapshot(*, version: int = 1, text: str = "Prepare Project Alpha brief."):
    return {
        "schema_version": INPUT_SCHEMA,
        "task_id": 1,
        "task_version": version,
        "text": text,
        "structured": {"action": "prepare", "object": "brief", "confidence": 0.9},
        "due": "2030-01-10",
        "owner": {"person_id": "person-a", "reliability": "resolved"},
        "participants": [
            {"person_id": "person-b", "resolution": "resolved"},
            {"speaker_id": "speaker-local", "resolution": "unresolved"},
        ],
        "working_group": {"id": "group-alpha", "evidence": "explicit"},
        "external_identifiers": [{"kind": "issue", "value": "example-12"}],
        "origin": {"kind": "email", "source_digest": "1" * 64},
        "structured_schema_revisions": {"task": 12, "identity": 1},
    }


def sources():
    return [{
        "source_id": "src-001",
        "locator": {
            "namespace": "kb",
            "resource": "Projects/Project-Alpha.md",
            "fragment": "plan",
        },
        "content_digest": "2" * 64,
        "title": "Project Alpha plan",
    }]


def claim(text: str, *, status: str = "supported"):
    return {"text": text, "status": status,
            "source_refs": [] if status == "unknown" else ["src-001"]}


def draft(*, recommendation: str = "after_task_completed"):
    empty = []
    return {
        "schema_version": DRAFT_SCHEMA,
        "research_status": "sufficient",
        "objective": claim("Produce the synthetic brief."),
        "requested_action": claim("Draft the brief from approved material."),
        "current_state": [claim("The outline exists.")],
        "expected_deliverables": [claim("One reviewed brief.")],
        "timeline": empty,
        "decisions": empty,
        "dependencies": [claim("Finish the source review first.")],
        "constraints": empty,
        "stakeholders": empty,
        "related_entities": empty,
        "findings": [claim("The approved outline has three sections.")],
        "conflicts": empty,
        "open_questions": [claim("The review date is unknown.", status="unknown")],
        "scheduling_recommendations": [{
            "type": recommendation,
            "confidence": 0.8,
            "rationale": claim("The source review is a prerequisite."),
            **({"related_task_id": 2} if recommendation == "after_task_completed" else {}),
        }],
    }


class TaskResearchTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        root = Path(self.temporary.name)
        self.database = root / "foxhound.sqlite3"
        self.cas = root / "research-cas"
        self.folder = root / "Tasks" / "T1-project-alpha"
        self.cas.mkdir(mode=0o700)
        self.folder.mkdir(parents=True, mode=0o700)
        self.folder.chmod(0o700)
        migrate_database(self.database)
        with sqlite3.connect(self.database) as connection:
            connection.execute(
                "INSERT INTO tasks(id,status,text,version,created_at,updated_at) "
                "VALUES(1,'open',?,1,?,?)",
                ("Prepare Project Alpha brief.", NOW.isoformat(), NOW.isoformat()),
            )
        self.store = ResearchStore(self.database, self.cas, clock=lambda: NOW)

    def _request(self, document=None, *, refresh=False):
        return self.store.request(
            snapshot() if document is None else document,
            task_work_root=self.folder.parent,
            task_folder=self.folder,
            refresh=refresh,
        )

    def test_digest_is_canonical_and_research_relevant(self):
        first = snapshot()
        reordered = snapshot()
        reordered["participants"].reverse()
        self.assertEqual(task_input_digest(first), task_input_digest(reordered))
        changed = snapshot()
        changed["working_group"] = {"id": "group-beta", "evidence": "explicit"}
        self.assertNotEqual(task_input_digest(first), task_input_digest(changed))
        changed = snapshot()
        changed["structured"]["action"] = "review"
        self.assertNotEqual(task_input_digest(first), task_input_digest(changed))

    def test_manual_request_converges_and_stale_version_is_refused(self):
        first = self._request()
        second = self._request()
        self.assertEqual(first.job_id, second.job_id)
        self.assertEqual(first.state, "queued")
        with self.assertRaisesRegex(ResearchError, "task snapshot is stale or does not match ledger"):
            self._request(snapshot(version=2))

    def test_claim_persists_only_token_digest(self):
        job = self._request()
        research_claim = self.store.claim("worker-synthetic")
        self.assertEqual(research_claim.job.job_id, job.job_id)
        with sqlite3.connect(self.database) as connection:
            stored = connection.execute(
                "SELECT token_digest FROM task_research_claims WHERE job_id=?", (job.job_id,)
            ).fetchone()[0]
        self.assertNotEqual(stored, research_claim.token)
        self.assertEqual(stored, hashlib.sha256(research_claim.token.encode()).hexdigest())
        context = self.store.context(job.job_id, research_claim.token)
        self.assertEqual(context["draft_filename"], "draft-research.json")
        self.assertEqual(context["task_snapshot"], snapshot())
        self.assertFalse(context["scheduling_recommendations_are_applied"])

    def test_failures_retry_then_park_at_bounded_attempt_limit(self):
        self._request()
        for attempt in range(1, 4):
            research_claim = self.store.claim("worker-synthetic")
            state = self.store.fail(
                research_claim.job.job_id, research_claim.token, "synthetic_failure"
            )
            self.assertEqual(state, "parked" if attempt == 3 else "queued")
        self.assertIsNone(self.store.claim("worker-synthetic"))

    def test_claim_supersedes_stale_task_version_or_non_open_task(self):
        job1 = self._request()
        # Bump task version to 2
        with sqlite3.connect(self.database) as connection:
            connection.execute("UPDATE tasks SET version=2 WHERE id=1")

        # Claiming should see job1 is stale, cancel it as superseded, and return None
        claimed = self.store.claim("worker-synthetic")
        self.assertIsNone(claimed)

        with sqlite3.connect(self.database) as connection:
            row = connection.execute(
                "SELECT state, failure_code FROM task_research_jobs WHERE job_id=?", (job1.job_id,)
            ).fetchone()
            self.assertEqual(tuple(row), ("canceled", "superseded"))

            events = connection.execute(
                "SELECT kind, from_state, to_state FROM task_research_events WHERE job_id=? ORDER BY occurred_at",
                (job1.job_id,),
            ).fetchall()
            self.assertIn(("canceled", "queued", "canceled"), [tuple(e) for e in events])

        # A current-version job can still be requested and claimed normally
        job2 = self._request(snapshot(version=2))
        claimed2 = self.store.claim("worker-synthetic")
        self.assertIsNotNone(claimed2)
        assert claimed2 is not None
        self.assertEqual(claimed2.job.job_id, job2.job_id)

    def test_claim_supersedes_closed_task(self):
        job = self._request()
        # Close task
        with sqlite3.connect(self.database) as connection:
            connection.execute("UPDATE tasks SET status='done' WHERE id=1")

        claimed = self.store.claim("worker-synthetic")
        self.assertIsNone(claimed)

        with sqlite3.connect(self.database) as connection:
            row = connection.execute(
                "SELECT state, failure_code FROM task_research_jobs WHERE job_id=?", (job.job_id,)
            ).fetchone()
            self.assertEqual(tuple(row), ("canceled", "superseded"))

    def test_model_timeout_requeues_without_consuming_attempts(self):
        job = self._request()
        for _ in range(5):
            research_claim = self.store.claim("worker-synthetic")
            self.assertIsNotNone(research_claim)
            state = self.store.fail(
                research_claim.job.job_id, research_claim.token, "model_timeout"  # type: ignore[union-attr]
            )
            self.assertEqual(state, "queued")
            row = self.store._connect().execute(
                "SELECT attempts FROM task_research_jobs WHERE job_id=?", (job.job_id,)
            ).fetchone()
            self.assertEqual(row[0], 0)
        # Next claim should still succeed and not be parked
        claim_after_timeouts = self.store.claim("worker-synthetic")
        self.assertIsNotNone(claim_after_timeouts)
        row = self.store._connect().execute(
            "SELECT attempts FROM task_research_jobs WHERE job_id=?", (job.job_id,)
        ).fetchone()
        self.assertEqual(row[0], 1)

    def test_agent_cannot_author_owned_fields(self):
        bad = draft()
        bad["provenance"] = {"model": "agent-selected"}
        job = self._request()
        research_claim = self.store.claim("worker-synthetic")
        with self.assertRaisesRegex(ResearchError, "publisher-owned"):
            self._publish(job.job_id, research_claim.token, bad)
        bad_sources = sources()
        bad_sources[0]["locator"]["namespace"] = "calendar"
        with self.assertRaisesRegex(ResearchError, "namespace"):
            self._publish(job.job_id, research_claim.token, draft(), bad_sources)

    def _publish(self, job_id, token, document=None, source_document=None):
        return self.store.publish(
            job_id=job_id,
            token=token,
            draft=draft() if document is None else document,
            sources=sources() if source_document is None else source_document,
            provenance={
                "profile_id": "researcher", "profile_revision": "3" * 64,
                "model": "synthetic-thinking-model", "provider": "synthetic",
                "runtime": "manual-test",
                "reasoning_requested": "high", "reasoning_effective": "high",
            },
            coverage={
                "searched_namespaces": ["kb"], "queries": 1,
                "documents_retrieved": 1, "unavailable_source_ids": [],
                "knowledge_revisions": {"kb": "4" * 64},
            },
        )

    def test_publish_is_receipt_gated_deterministic_and_repairable(self):
        job = self._request()
        research_claim = self.store.claim("worker-synthetic")
        final = self._publish(job.job_id, research_claim.token)
        self.assertEqual(final["schema_version"], PUBLISHED_SCHEMA)
        self.assertEqual(final["authority"], "evidence_only")
        self.assertEqual(final["scheduling_recommendations"][0]["type"],
                         "after_task_completed")
        self.assertEqual(os.stat(self.folder / ".task-research.json").st_mode & 0o777, 0o600)
        self.assertEqual(os.stat(self.folder / "Research.md").st_mode & 0o777, 0o600)
        markdown = (self.folder / "Research.md").read_text()
        self.assertIn("# Task Research", markdown)
        self.assertIn("kb:Projects/Project-Alpha.md#plan", markdown)
        projection = self.store.projection(1, 1, task_input_digest(snapshot()))
        self.assertEqual(projection["authority"], "evidence_only")
        self.assertLessEqual(len(json.dumps(projection).encode()), MAX_PROJECTION_BYTES)

        (self.folder / "Research.md").write_text("tampered")
        self.assertIsNone(self.store.projection(1, 1, task_input_digest(snapshot())))
        self.assertTrue(self.store.repair(job.job_id))
        self.assertIsNotNone(self.store.projection(1, 1, task_input_digest(snapshot())))

    def test_interrupted_publication_is_completed_from_cas(self):
        job = self._request()
        research_claim = self.store.claim("worker-synthetic")
        original = self.store._install

        def interrupt(path, payload):
            if path.parent == self.folder:
                raise OSError("synthetic interruption")
            return original(path, payload)

        with mock.patch.object(self.store, "_install", side_effect=interrupt):
            with self.assertRaisesRegex(OSError, "synthetic interruption"):
                self._publish(job.job_id, research_claim.token)
        with sqlite3.connect(self.database) as connection:
            state = connection.execute(
                "SELECT state FROM task_research_jobs WHERE job_id=?", (job.job_id,)
            ).fetchone()[0]
            receipts = connection.execute(
                "SELECT COUNT(*) FROM task_research_receipts WHERE job_id=?", (job.job_id,)
            ).fetchone()[0]
        self.assertEqual((state, receipts), ("publishing", 0))
        self.assertTrue(self.store.repair(job.job_id))
        self.assertIsNotNone(self.store.projection(1, 1, task_input_digest(snapshot())))

    def test_recommendations_are_recorded_but_apply_nothing(self):
        job = self._request()
        research_claim = self.store.claim("worker-synthetic")
        self._publish(job.job_id, research_claim.token)
        with sqlite3.connect(self.database) as connection:
            tables = {row[0] for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'task_research_%'"
            )}
            self.assertEqual(tables, {
                "task_research_jobs", "task_research_claims",
                "task_research_receipts", "task_research_events",
            })
            self.assertEqual(connection.execute(
                "SELECT COUNT(*) FROM task_research_events WHERE kind='completed'"
            ).fetchone()[0], 1)

    def test_projection_trims_optional_claims_to_bound(self):
        item = claim("x" * 8000)
        document = {
            "task_identity": {"task_id": "1", "task_version": 1},
            "research_status": "sufficient",
            "report": {
                "objective": claim("objective"),
                "requested_action": claim("action"),
                "findings": [item, item, item],
                "dependencies": [item], "constraints": [item],
                "open_questions": [item],
            },
            "scheduling_recommendations": [],
        }
        projection = consumer_projection(document)
        self.assertLessEqual(len(json.dumps(projection, separators=(",", ":")).encode()),
                             MAX_PROJECTION_BYTES)

    def test_reserved_outputs_cannot_be_published_by_task_archive(self):
        from foxhound.task_archive import (
            TaskArchiveError,
            TaskArchivePaths,
            publish_deliverables,
        )

        # The reserved-name contract is exercised through its public manifest
        # validator rather than by reaching into a private constant.
        run = self.folder / "run"
        run.mkdir()
        (run / "Research.md").write_text("agent overwrite")
        (run / "result-artifacts.json").write_text(json.dumps(["Research.md"]))
        paths = TaskArchivePaths(
            working_directory=self.folder,
            task_file=self.folder / "task.md",
            run_directory=run,
        )
        with self.assertRaises(TaskArchiveError):
            publish_deliverables(paths, run)
        self.assertFalse((self.folder / "Research.md").exists())

    def test_integer_task_id_and_ledger_mismatch_rejection(self):
        bad_id = snapshot()
        bad_id["task_id"] = "not-an-int"
        with self.assertRaisesRegex(ResearchError, "invalid task id"):
            self._request(bad_id)

        mismatched_text = snapshot(text="Text that does not match ledger")
        with self.assertRaisesRegex(ResearchError, "task snapshot is stale or does not match ledger"):
            self._request(mismatched_text)

    def test_task_folder_and_cas_containment_and_symlink_rejection(self):
        outside_folder = self.temporary.name + "-outside"
        os.makedirs(outside_folder, exist_ok=True)
        with self.assertRaisesRegex(ResearchError, "task folder is outside its configured root"):
            self.store.request(
                snapshot(),
                task_work_root=self.folder.parent,
                task_folder=Path(outside_folder),
            )

        symlink_folder = self.folder.parent / "symlink-folder"
        symlink_folder.symlink_to(self.folder, target_is_directory=True)
        with self.assertRaisesRegex(ResearchError, "unsafe task folder"):
            self.store.request(
                snapshot(),
                task_work_root=self.folder.parent,
                task_folder=symlink_folder,
            )

    def test_source_locator_rejection_rules(self):
        bad_locators = [
            "/absolute/path/doc.md",
            "../traversal.md",
            r"back\slash.md",
            "C:drive.md",
            "https://example.com/doc.md",
            "s3://bucket/doc.md",
            "scheme:resource.md",
        ]
        job = self._request()
        research_claim = self.store.claim("worker-synthetic")
        for bad_resource in bad_locators:
            bad_sources = sources()
            bad_sources[0]["locator"]["resource"] = bad_resource
            with self.assertRaisesRegex(ResearchError, "unsafe source resource"):
                self._publish(job.job_id, research_claim.token, source_document=bad_sources)

    def test_web_namespace_accepts_only_http_urls(self):
        job = self._request()
        research_claim = self.store.claim("worker-synthetic")
        for bad_resource in ["file:///etc/passwd", "ftp://example.com/x", "notes/doc.md"]:
            bad_sources = sources()
            bad_sources[0]["locator"]["namespace"] = "web"
            bad_sources[0]["locator"]["resource"] = bad_resource
            with self.assertRaisesRegex(ResearchError, "unsafe source resource"):
                self._publish(job.job_id, research_claim.token, source_document=bad_sources)
        web_sources = sources()
        web_sources[0]["locator"]["namespace"] = "web"
        web_sources[0]["locator"]["resource"] = "https://example.com/program/rules"
        self._publish(job.job_id, research_claim.token, source_document=web_sources)

    def test_tool_namespace_receipt_validation(self):
        job = self._request()
        research_claim = self.store.claim("worker-synthetic")
        assert research_claim is not None
        # Valid tool resource
        tool_sources = sources()
        tool_sources[0]["locator"]["namespace"] = "tool"
        tool_sources[0]["locator"]["resource"] = "calendar-cmd:events --week"
        tool_sources[0]["locator"]["fragment"] = None
        self._publish(job.job_id, research_claim.token, source_document=tool_sources)

        # Invalid tool resources: too long, multiline, missing colon, empty text, bad name
        bad_tool_resources = [
            "calendar-cmd:" + "x" * 300,  # exceeds 300 chars
            "calendar-cmd:events\n--week",  # newline
            "calendar-cmd:events\r--week",  # carriage return
            "calendar-cmd:events\x00--week",  # null
            "calendar-cmd",  # missing colon
            "calendar-cmd:   ",  # empty text
            "CALENDAR:events",  # uppercase name
            "-calendar:events",  # leading dash
        ]
        for bad_resource in bad_tool_resources:
            bad_sources = sources()
            bad_sources[0]["locator"]["namespace"] = "tool"
            bad_sources[0]["locator"]["resource"] = bad_resource
            bad_sources[0]["locator"]["fragment"] = None
            with self.assertRaisesRegex(ResearchError, "unsafe source resource"):
                self._publish(job.job_id, research_claim.token, source_document=bad_sources)

    def test_scheduling_recommendations_canonical_rules(self):
        job = self._request()
        research_claim = self.store.claim("worker-synthetic")

        # More than 3 recommendations rejected
        too_many = draft()
        item = too_many["scheduling_recommendations"][0]
        too_many["scheduling_recommendations"] = [item, item, item, item]
        with self.assertRaisesRegex(ResearchError, "invalid scheduling recommendations"):
            self._publish(job.job_id, research_claim.token, document=too_many)

        # after_task_completed requires integer related_task_id
        bad_after = draft(recommendation="after_task_completed")
        bad_after["scheduling_recommendations"] = [{
            "type": "after_task_completed",
            "confidence": 0.9,
            "rationale": claim("Needs related task id."),
            "related_task_id": "not-an-int",
        }]
        with self.assertRaisesRegex(ResearchError, "invalid predecessor task id"):
            self._publish(job.job_id, research_claim.token, document=bad_after)

        # not_before requires UTC ISO timestamp
        bad_nb = draft(recommendation="not_before")
        bad_nb["scheduling_recommendations"] = [{
            "type": "not_before",
            "confidence": 0.9,
            "rationale": claim("Needs valid not_before."),
            "not_before": "invalid-time",
        }]
        with self.assertRaisesRegex(ResearchError, "invalid not before"):
            self._publish(job.job_id, research_claim.token, document=bad_nb)

        # create_prerequisite requires prerequisite_text
        bad_prereq = draft(recommendation="create_prerequisite")
        bad_prereq["scheduling_recommendations"] = [{
            "type": "create_prerequisite",
            "confidence": 0.9,
            "rationale": claim("Needs prerequisite text."),
        }]
        with self.assertRaisesRegex(ResearchError, "invalid prerequisite text"):
            self._publish(job.job_id, research_claim.token, document=bad_prereq)

        # raise_priority takes no extra target
        good_priority = draft(recommendation="raise_priority")
        good_priority["scheduling_recommendations"] = [{
            "type": "raise_priority",
            "confidence": 0.9,
            "rationale": claim("Implicit target task."),
        }]
        published = self._publish(job.job_id, research_claim.token, document=good_priority)
        self.assertEqual(published["scheduling_recommendations"][0]["type"], "raise_priority")
        self.assertNotIn("target_task_id", published["scheduling_recommendations"][0])

    def test_stale_task_version_or_expired_lease_at_publishing_boundary_fails(self):
        job = self._request()
        research_claim = self.store.claim("worker-synthetic")

        # Bump task version in tasks table to simulate stale work
        with sqlite3.connect(self.database) as connection:
            connection.execute("UPDATE tasks SET version=2 WHERE id=1")

        with self.assertRaisesRegex(ResearchError, "research job or task changed during publication"):
            self._publish(job.job_id, research_claim.token)

    def test_expired_lease_recovery(self):
        job = self._request()
        research_claim = self.store.claim("worker-synthetic", lease_seconds=10)
        # Advance time past lease expiration
        expired_time = NOW + timedelta(seconds=20)
        self.store._now = lambda: expired_time

        recovery = self.store.recover_expired()
        self.assertEqual(recovery, {"retried": 1, "parked": 0})

        with sqlite3.connect(self.database) as connection:
            job_state = connection.execute(
                "SELECT state,failure_code FROM task_research_jobs WHERE job_id=?", (job.job_id,)
            ).fetchone()
            self.assertEqual(tuple(job_state), ("queued", "lease_expired"))
            claims = connection.execute(
                "SELECT COUNT(*) FROM task_research_claims WHERE job_id=?", (job.job_id,)
            ).fetchone()[0]
            self.assertEqual(claims, 0)


    def test_claim_lease_validation_and_expired_rejection(self):
        job = self._request()
        # Reject bool and non-int lease_seconds explicitly
        with self.assertRaisesRegex(ResearchError, "invalid claim lease"):
            self.store.claim("worker-synthetic", lease_seconds=True)  # type: ignore
        with self.assertRaisesRegex(ResearchError, "invalid claim lease"):
            self.store.claim("worker-synthetic", lease_seconds=False)  # type: ignore
        with self.assertRaisesRegex(ResearchError, "invalid claim lease"):
            self.store.claim("worker-synthetic", lease_seconds="900")  # type: ignore
        with self.assertRaisesRegex(ResearchError, "invalid claim lease"):
            self.store.claim("worker-synthetic", lease_seconds=0)
        with self.assertRaisesRegex(ResearchError, "invalid claim lease"):
            self.store.claim("worker-synthetic", lease_seconds=14_401)

        # Successful claim with 10s lease
        research_claim = self.store.claim("worker-synthetic", lease_seconds=10)
        self.assertIsNotNone(research_claim)

        # Context succeeds before expiry
        ctx = self.store.context(job.job_id, research_claim.token)
        self.assertEqual(ctx["job_id"], job.job_id)

        # Advance past expiry
        self.store._now = lambda: NOW + timedelta(seconds=20)

        # Context and fail must reject expired leases
        with self.assertRaisesRegex(ResearchError, "research claim is unavailable"):
            self.store.context(job.job_id, research_claim.token)

        with self.assertRaisesRegex(ResearchError, "research claim is unavailable"):
            self.store.fail(job.job_id, research_claim.token, "rate_limit")

    def test_refresh_arriving_during_publishing_fails_closed(self):
        job = self._request()
        research_claim = self.store.claim("worker-synthetic")

        # Put the job in 'publishing' state
        with sqlite3.connect(self.database) as connection:
            connection.execute(
                "UPDATE task_research_jobs SET state='publishing' WHERE job_id=?",
                (job.job_id,),
            )

        # Calling request with refresh=True while job is publishing must fail-closed
        with self.assertRaisesRegex(ResearchError, "task research publication is currently in progress"):
            self._request(refresh=True)

        # Verify job was NOT canceled or modified
        with sqlite3.connect(self.database) as connection:
            row = connection.execute(
                "SELECT state FROM task_research_jobs WHERE job_id=?", (job.job_id,)
            ).fetchone()
            self.assertEqual(row[0], "publishing")

    def test_post_file_install_race_fencing(self):
        job = self._request()
        research_claim = self.store.claim("worker-synthetic")

        original_install = self.store._install
        calls = []

        def tracked_install(path, payload):
            original_install(path, payload)
            calls.append(path)
            # When the folder .task-research.json is installed, simulate an intervening change
            if path == self.folder / ".task-research.json":
                with sqlite3.connect(self.database) as connection:
                    # Cancel the job behind its back
                    connection.execute(
                        "UPDATE task_research_jobs SET state='canceled' WHERE job_id=?",
                        (job.job_id,),
                    )

        with mock.patch.object(self.store, "_install", side_effect=tracked_install):
            with self.assertRaisesRegex(ResearchError, "research job or task version changed before completion"):
                self._publish(job.job_id, research_claim.token)

        # Receipt must NOT have been written
        with sqlite3.connect(self.database) as connection:
            receipts = connection.execute(
                "SELECT COUNT(*) FROM task_research_receipts WHERE job_id=?", (job.job_id,)
            ).fetchone()[0]
            self.assertEqual(receipts, 0)
            events = connection.execute(
                "SELECT COUNT(*) FROM task_research_events WHERE job_id=? AND kind='completed'",
                (job.job_id,),
            ).fetchone()[0]
            self.assertEqual(events, 0)

    def test_repair_rejects_symlink_in_cas_path(self):
        job = self._request()
        research_claim = self.store.claim("worker-synthetic")
        final = self._publish(job.job_id, research_claim.token)
        json_digest = final["provenance"]["job_id"]  # Wait, let's get real digest
        with sqlite3.connect(self.database) as connection:
            row = connection.execute(
                "SELECT json_digest FROM task_research_receipts WHERE job_id=?", (job.job_id,)
            ).fetchone()
            json_digest = row[0]

        # Tamper task folder so repair is needed
        (self.folder / "Research.md").write_text("tampered")
        self.assertIsNone(self.store.projection(1, 1, task_input_digest(snapshot())))

        # Replace intermediate CAS component prefix directory with a symlink to outside
        outside_dir = tempfile.TemporaryDirectory()
        self.addCleanup(outside_dir.cleanup)
        prefix_dir = self.cas / json_digest[:2]

        # Move the prefix_dir contents to outside and replace prefix_dir with symlink
        outside_target = Path(outside_dir.name) / "evil"
        shutil.move(str(prefix_dir), str(outside_target))
        prefix_dir.symlink_to(outside_target, target_is_directory=True)

        with self.assertRaisesRegex(ResearchError, "unsafe CAS component"):
            self.store.repair(job.job_id)

    def test_claim_file_safety(self):
        job = self._request()
        claim = self.store.claim("worker-synthetic")
        claim_file = self.folder / "research.claim"

        # Parent directory must be private (0700)
        from foxhound.task_research import _write_claim, _read_claim
        _write_claim(claim_file, claim)

        # Check permissions: must be 0600 regular file
        st = claim_file.lstat()
        self.assertFalse(claim_file.is_symlink())
        self.assertEqual(stat.S_IMODE(st.st_mode), 0o600)

        # Successful read
        read_job_id, read_token = _read_claim(claim_file)
        self.assertEqual(read_job_id, job.job_id)
        self.assertEqual(read_token, claim.token)

        # Reject if symlink
        claim_file.unlink()
        target = self.folder / "actual_token"
        target.write_text(json.dumps({"job_id": job.job_id, "claim_token": claim.token}))
        target.chmod(0o600)
        claim_file.symlink_to(target)
        with self.assertRaisesRegex(ResearchError, "unsafe claim file"):
            _read_claim(claim_file)

        # Reject if permissions too loose (e.g. 0644)
        claim_file.unlink()
        claim_file.write_text(json.dumps({"job_id": job.job_id, "claim_token": claim.token}))
        claim_file.chmod(0o644)
        with self.assertRaisesRegex(ResearchError, "unsafe claim file"):
            _read_claim(claim_file)

        # Reject if parent directory is not private (e.g. 0777)
        claim_file.chmod(0o600)
        self.folder.chmod(0o777)
        with self.assertRaisesRegex(ResearchError, "claim directory is not private"):
            _read_claim(claim_file)
        with self.assertRaisesRegex(ResearchError, "claim directory is not private"):
            _write_claim(self.folder / "another.claim", claim)
        self.folder.chmod(0o700)

    def test_validate_draft_and_render_markdown_with_guide(self):
        from foxhound.task_research import render_markdown, validate_draft
        srcs = sources()
        doc = {
            "schema_version": DRAFT_SCHEMA,
            "research_status": "sufficient",
            "objective": {"text": "Synthetic objective", "status": "supported", "source_refs": ["src-001"]},
            "requested_action": {"text": "Synthetic action", "status": "supported", "source_refs": ["src-001"]},
            "current_state": [],
            "expected_deliverables": [],
            "timeline": [],
            "decisions": [],
            "dependencies": [],
            "constraints": [],
            "stakeholders": [],
            "related_entities": [],
            "findings": [{"text": "Synthetic finding", "status": "supported", "source_refs": ["src-001"]}],
            "conflicts": [],
            "open_questions": [],
            "scheduling_recommendations": [],
            "guide": {
                "text": "Follow Project Alpha process guide",
                "status": "supported",
                "source_refs": ["src-001"],
            },
        }
        validated = validate_draft(doc, srcs)
        self.assertIn("guide", validated)
        guide_val = validated["guide"]
        assert isinstance(guide_val, dict)
        self.assertEqual(guide_val["text"], "Follow Project Alpha process guide")

        published = {
            "task_identity": {"task_id": 42, "task_version": 1},
            "research_status": "sufficient",
            "report": validated,
            "scheduling_recommendations": [],
            "sources": srcs,
        }
        rendered = render_markdown(published)
        self.assertIn("## Guide", rendered)
        self.assertIn("Follow Project Alpha process guide (supported) [src-001]", rendered)

        # Check order: ## Guide right after Requested action
        req_idx = rendered.index("## Requested action")
        guide_idx = rendered.index("## Guide")
        find_idx = rendered.index("## Findings")
        self.assertTrue(req_idx < guide_idx < find_idx)

    def test_validate_draft_and_render_markdown_with_timing(self):
        from foxhound.task_research import consumer_projection, render_markdown, validate_draft
        srcs = sources()
        doc = {
            "schema_version": DRAFT_SCHEMA,
            "research_status": "sufficient",
            "objective": {"text": "Synthetic objective", "status": "supported", "source_refs": ["src-001"]},
            "requested_action": {"text": "Synthetic action", "status": "supported", "source_refs": ["src-001"]},
            "current_state": [],
            "expected_deliverables": [],
            "timeline": [],
            "decisions": [],
            "dependencies": [],
            "constraints": [],
            "stakeholders": [],
            "related_entities": [],
            "findings": [{"text": "Synthetic finding", "status": "supported", "source_refs": ["src-001"]}],
            "conflicts": [],
            "open_questions": [],
            "scheduling_recommendations": [],
            "deadline": {
                "text": "Target completion date changed to 2026-11-15",
                "status": "supported",
                "source_refs": ["src-001"],
                "date": "2026-11-15",
            },
            "effort": {
                "text": "Estimated implementation effort is one day",
                "status": "supported",
                "source_refs": ["src-001"],
                "size": "day",
            },
        }
        validated = validate_draft(doc, srcs)
        self.assertIn("deadline", validated)
        self.assertIn("effort", validated)
        dl_val = validated["deadline"]
        assert isinstance(dl_val, dict)
        self.assertEqual(dl_val["date"], "2026-11-15")
        eff_val = validated["effort"]
        assert isinstance(eff_val, dict)
        self.assertEqual(eff_val["size"], "day")

        published = {
            "task_identity": {"task_id": 42, "task_version": 1},
            "research_status": "sufficient",
            "report": validated,
            "scheduling_recommendations": [],
            "sources": srcs,
        }
        rendered = render_markdown(published)
        self.assertIn("## Timing", rendered)
        self.assertIn("- Deadline: 2026-11-15 — Target completion date changed to 2026-11-15 (supported) [src-001]", rendered)
        self.assertIn("- Effort: day — Estimated implementation effort is one day (supported) [src-001]", rendered)

        req_idx = rendered.index("## Requested action")
        timing_idx = rendered.index("## Timing")
        find_idx = rendered.index("## Findings")
        self.assertTrue(req_idx < timing_idx < find_idx)

        proj = consumer_projection(published)
        self.assertIn("deadline", proj)
        self.assertIn("effort", proj)

    def test_render_markdown_omits_open_questions(self):
        from foxhound.task_research import render_markdown
        doc = {
            "task_identity": {"task_id": 42, "task_version": 1},
            "research_status": "sufficient",
            "report": {
                "objective": {"text": "Synthetic objective", "status": "supported", "source_refs": []},
                "requested_action": {"text": "Synthetic action", "status": "supported", "source_refs": []},
                "findings": [{"text": "Synthetic finding", "status": "supported", "source_refs": []}],
                "open_questions": [{"text": "Synthetic open question?", "status": "supported", "source_refs": []}],
            },
            "scheduling_recommendations": [],
            "sources": [],
        }
        rendered = render_markdown(doc)
        self.assertIn("## Findings", rendered)
        self.assertNotIn("Open Questions", rendered)


if __name__ == "__main__":
    unittest.main()
