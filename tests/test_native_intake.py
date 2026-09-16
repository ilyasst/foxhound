#!/usr/bin/env python3
"""Synthetic tests for producer-independent ordered candidate intake."""

from __future__ import annotations

import copy
import hashlib
import json
import sqlite3
import tempfile
import unittest
from contextlib import closing, redirect_stderr, redirect_stdout
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path

from foxhound.candidate_inbox import CandidateInbox, SCHEMA_VERSION
from foxhound.contracts import candidate_id_for, comparable_task_digest
from foxhound.native_intake import main
from foxhound.task_cards import CardRefusal, TaskCardService
from foxhound.task_execution import TaskExecutionService, WorkflowStatus
from foxhound.task_ledger import (
    BootstrapDisposition,
    BootstrapRefusal,
    NativeIntakeDisposition,
    NativeIntakeRefusal,
    TaskLedger,
    TaskStatus,
)


NOW = datetime(2030, 3, 1, 12, 0, tzinfo=timezone.utc)


def candidate(
    index: int,
    *,
    text: str | None = None,
    owner: str | None = "Person A",
    due: str | None = None,
) -> dict:
    task_text = text or f"Prepare synthetic summary {index}"
    record_id = f"record-{index:03d}"
    item_id = f"action-{index:03d}"
    revision = hashlib.sha256(
        json.dumps([task_text, owner, due, index]).encode("utf-8")
    ).hexdigest()
    return {
        "schema": "foxhound.task-candidate",
        "schema_version": 2,
        "candidate_id": candidate_id_for(
            system="gw",
            kind="meeting",
            record_id=record_id,
            item_id=item_id,
        ),
        "source": {
            "system": "gw",
            "kind": "meeting",
            "record_id": record_id,
            "item_id": item_id,
            "revision": revision,
        },
        "task": {"text": task_text, "owner": owner, "due": due},
        "evidence": {
            "document_id": record_id,
            "locator": f"action-item-{index:03d}",
        },
        "created_at": "2030-01-01T12:00:00Z",
    }


def lifecycle_candidate(
    index: int,
    *,
    generation: int,
    state: str = "active",
    text: str | None = None,
) -> dict:
    item = candidate(index, text=text)
    item["schema_version"] = 3
    item["lifecycle"] = {
        "state": state,
        "generation": generation,
        "changed_at": f"2030-02-{generation:02d}T12:00:00Z",
    }
    item["source"]["revision"] = hashlib.sha256(
        json.dumps(
            [generation, state, item["task"]],
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    return item


def provenance_candidate(index: int) -> dict:
    """The same fictional task with a richer source-only revision."""
    item = candidate(index)
    item["schema_version"] = 4
    item["evidence"]["sources"] = [
        {
            "name": "20300102_example_handoff.json",
            "role": "handoff",
            "extract": "The handoff declares this synthetic action.",
        },
        {
            "name": "20300102_example_protocol.md",
            "role": "protocol",
            "extract": "Action item:\nPrepare the synthetic summary.",
        },
        {
            "name": "20300102_example_transcript.txt",
            "role": "transcript",
            "extract": "[Person A] I will prepare the synthetic summary.",
        },
    ]
    item["source"]["revision"] = hashlib.sha256(
        json.dumps(item["evidence"], sort_keys=True).encode("utf-8")
    ).hexdigest()
    return item


def owner_candidate(
    index: int,
    *,
    owner: str = "Person A",
    speaker_id: str = "SPK_101",
    canonical_speaker_id: str = "SPK_001",
) -> dict:
    item = candidate(index, owner=owner)
    item["schema_version"] = 5
    item["task"]["owner_ref"] = {
        "kind": "person",
        "speaker_id": speaker_id,
        "canonical_speaker_id": canonical_speaker_id,
        "speaker_registry_id": "registry-alpha",
        "pinned": False,
        "provisional": False,
    }
    item["source"]["revision"] = hashlib.sha256(
        json.dumps(item["task"], sort_keys=True).encode("utf-8")
    ).hexdigest()
    return item


def cross_source_owner_candidate(
    index: int,
    *,
    kind: str,
    text: str,
) -> dict:
    """A confirmed-owner candidate from one synthetic source kind."""
    item = owner_candidate(index)
    item["source"]["kind"] = kind
    item["candidate_id"] = candidate_id_for(
        system="gw",
        kind=kind,
        record_id=item["source"]["record_id"],
        item_id=item["source"]["item_id"],
    )
    item["task"]["text"] = text
    item["source"]["revision"] = hashlib.sha256(
        json.dumps(
            {"source": item["source"], "task": item["task"]},
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()
    return item


def owner_provenance_candidate(index: int) -> dict:
    item = provenance_candidate(index)
    item["schema_version"] = 6
    item["task"]["owner_ref"] = {
        "kind": "person",
        "speaker_id": "SPK_101",
        "canonical_speaker_id": "SPK_001",
        "speaker_registry_id": "registry-alpha",
        "pinned": False,
        "provisional": False,
    }
    item["source"]["revision"] = hashlib.sha256(
        json.dumps(
            {"task": item["task"], "evidence": item["evidence"]},
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()
    return item


def cumulative_candidate(index: int, kind: str) -> dict:
    """A fictional current-shape candidate for any accepted source kind."""
    item = owner_candidate(index)
    item["schema_version"] = 7
    item["source"]["kind"] = kind
    item["candidate_id"] = candidate_id_for(
        system="gw",
        kind=kind,
        record_id=item["source"]["record_id"],
        item_id=item["source"]["item_id"],
    )
    item["lifecycle"] = {
        "state": "active",
        "generation": 1,
        "changed_at": "2030-02-01T12:00:00Z",
    }
    item["source"]["revision"] = hashlib.sha256(
        json.dumps(item, sort_keys=True).encode("utf-8")
    ).hexdigest()
    return item


SOURCE_ROLE = {
    "meeting": "transcript",
    "email": "message",
    "teams": "message",
    "issue": "body",
    "legacy": "record",
    "review_request": "diff",
    "mention": "comment",
    "calendar": "description",
    "alert": "detail",
}


def legacy_candidate(index: int) -> dict:
    """A fictional open task offered only for a bounded cutover."""
    item = candidate(
        index,
        text=f"Prepare migrated synthetic summary {index}",
        owner="Person B",
        due="2030-03-20",
    )
    item["schema_version"] = 1
    item["source"].update({
        "kind": "legacy",
        "record_id": "example-task-ledger",
        "item_id": f"task-{index:03d}",
    })
    item["candidate_id"] = candidate_id_for(
        system="gw",
        kind="legacy",
        record_id=item["source"]["record_id"],
        item_id=item["source"]["item_id"],
    )
    item["task"]["project"] = "Project Alpha"
    item["evidence"] = {
        "document_id": "example-task-ledger",
        "locator": f"task-{index:03d}",
    }
    return item


def teams_candidate(index: int) -> dict:
    """A fictional action discovered in a bounded chat context."""
    item = candidate(index)
    item["source"]["kind"] = "teams"
    item["candidate_id"] = candidate_id_for(
        system="gw",
        kind="teams",
        record_id=item["source"]["record_id"],
        item_id=item["source"]["item_id"],
    )
    return item


def feed(from_cursor: int, *items: dict) -> dict:
    return {
        "schema": "foxhound.task-candidate-feed",
        "schema_version": 1,
        "producer": "gw",
        "stream_id": "primary",
        "from_cursor": from_cursor,
        "to_cursor": from_cursor + len(items),
        "items": [
            {"sequence": from_cursor + offset, "candidate": item}
            for offset, item in enumerate(items, start=1)
        ],
        "emitted_at": "2030-03-01T12:00:00Z",
    }


def observation(
    item: dict,
    *,
    disposition: str,
    task_id: int | None,
    legacy_owner: str | None = None,
) -> dict:
    legacy = None
    if task_id is not None:
        legacy = {
            "task_id": task_id,
            "comparable_digest": comparable_task_digest(
                text=item["task"]["text"],
                project=None,
                owner=(
                    item["task"]["owner"]
                    if legacy_owner is None
                    else legacy_owner
                ),
            ),
        }
    return {
        "schema": "foxhound.task-shadow-observation",
        "schema_version": 1,
        "candidate": copy.deepcopy(item),
        "disposition": disposition,
        "legacy_task": legacy,
        "reason_code": None if task_id is not None else "ambiguous_match",
        "observed_at": "2030-02-01T12:00:00Z",
    }


def shadow_feed(*items: dict) -> dict:
    return {
        "schema": "foxhound.task-shadow-observation-feed",
        "schema_version": 1,
        "producer": "gw",
        "stream_id": "primary",
        "from_cursor": 0,
        "to_cursor": len(items),
        "items": [
            {"sequence": offset, "observation": item}
            for offset, item in enumerate(items, start=1)
        ],
        "emitted_at": "2030-03-01T12:00:00Z",
    }


class NativeCandidateIntakeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.root.chmod(0o700)
        self.database = self.root / "foxhound.sqlite3"
        self.inbox = CandidateInbox(self.database, clock=lambda: NOW)
        self.inbox.initialize()
        self.database.chmod(0o600)
        self.ledger = TaskLedger(self.database, clock=lambda: NOW)

    def activate(self, cursor: int = 0):
        return self.ledger.activate_native_intake(
            producer="gw", stream_id="primary", expected_cursor=cursor
        )

    def intake(self, *, limit: int = 100):
        return self.ledger.accept_native_candidates(
            producer="gw", stream_id="primary", limit=limit
        )

    def test_activation_is_exact_idempotent_and_requires_reconciled_prefix(self):
        item = candidate(1)
        self.assertTrue(self.inbox.import_feed(feed(0, item)).accepted)

        refused = self.activate(1)
        self.assertEqual(refused.disposition, NativeIntakeDisposition.REFUSED)
        self.assertEqual(refused.refusal, NativeIntakeRefusal.UNRECONCILED_PREFIX)

        self.assertTrue(
            self.inbox.import_shadow_feed(
                shadow_feed(observation(item, disposition="refused", task_id=None))
            ).accepted
        )
        activated = self.activate(1)
        self.assertEqual(activated.disposition, NativeIntakeDisposition.APPLIED)
        self.assertEqual(
            self.activate(1).disposition, NativeIntakeDisposition.UNCHANGED
        )
        mismatch = self.activate(0)
        self.assertEqual(mismatch.refusal, NativeIntakeRefusal.CURSOR_MISMATCH)

    def test_activation_accepts_a_historically_bound_prefix(self):
        item = candidate(1)
        self.inbox.import_feed(feed(0, item))
        self.inbox.import_shadow_feed(
            shadow_feed(observation(item, disposition="minted", task_id=1001))
        )
        self.assertEqual(self.ledger.bootstrap_from_shadow().tasks_created, 1)

        activated = self.activate(1)

        self.assertEqual(activated.disposition, NativeIntakeDisposition.APPLIED)
        self.assertEqual(activated.activation_cursor, 1)

    def test_divergent_history_requires_exact_immutable_reconciliation(self):
        item = candidate(1, owner="Person A")
        self.inbox.import_feed(feed(0, item))
        self.inbox.import_shadow_feed(shadow_feed(observation(
            item,
            disposition="minted",
            task_id=1001,
            legacy_owner="Person B",
        )))
        self.assertEqual(
            self.activate(1).refusal,
            NativeIntakeRefusal.UNRECONCILED_PREFIX,
        )

        mismatch = self.ledger.refuse_divergent_history(
            producer="gw",
            stream_id="primary",
            expected_count=2,
            reason_code="preserved_legacy_owner",
        )
        self.assertEqual(
            mismatch.refusal,
            NativeIntakeRefusal.EXPECTED_COUNT_MISMATCH,
        )

        applied = self.ledger.refuse_divergent_history(
            producer="gw",
            stream_id="primary",
            expected_count=1,
            reason_code="preserved_legacy_owner",
        )
        self.assertEqual(
            (applied.disposition, applied.candidates_matched,
             applied.refusals_recorded, applied.refusals_unchanged),
            (NativeIntakeDisposition.APPLIED, 1, 1, 0),
        )
        unchanged = self.ledger.refuse_divergent_history(
            producer="gw",
            stream_id="primary",
            expected_count=1,
            reason_code="preserved_legacy_owner",
        )
        self.assertEqual(
            (unchanged.disposition, unchanged.refusals_recorded,
             unchanged.refusals_unchanged),
            (NativeIntakeDisposition.UNCHANGED, 0, 1),
        )

        with closing(sqlite3.connect(self.database)) as connection:
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute(
                    "UPDATE native_intake_historical_refusals "
                    "SET refused_at='2031-01-01T00:00:00+00:00'"
                )
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute(
                    "DELETE FROM native_intake_historical_refusals"
                )

        self.assertEqual(
            self.activate(1).disposition,
            NativeIntakeDisposition.APPLIED,
        )
        fenced = self.ledger.refuse_divergent_history(
            producer="gw",
            stream_id="primary",
            expected_count=1,
            reason_code="preserved_legacy_owner",
        )
        self.assertEqual(fenced.refusal, NativeIntakeRefusal.ALREADY_ACTIVATED)

    def test_schema_fifteen_migration_is_passive(self):
        item = candidate(1)
        self.assertTrue(self.inbox.import_document(item).accepted)
        with closing(sqlite3.connect(self.database)) as connection:
            for column in (
                "owner_provisional",
                "owner_pinned",
                "owner_speaker_registry_id",
                "owner_canonical_speaker_id",
                "owner_speaker_id",
                "owner_kind",
                "owner_ref_version",
            ):
                connection.execute(f"ALTER TABLE tasks DROP COLUMN {column}")
            connection.execute("DROP TABLE native_intake_historical_refusals")
            connection.execute(
                "ALTER TABLE task_execution_results DROP COLUMN task_kb_file"
            )
            connection.execute(
                "ALTER TABLE task_execution_results "
                "DROP COLUMN task_work_directory"
            )
            # v21 added this; a database at an older version has
            # not got it yet.
            connection.execute(
                "ALTER TABLE task_execution_results DROP COLUMN work_digest"
            )
            # ADR 0036 added this at v23; a database at an older version
            # has not got it yet.
            connection.execute(
                "ALTER TABLE task_review_cards DROP COLUMN consumer_digest"
            )
            # Source-revision snapshots arrive at v26. This fixture models
            # v15, before review cards carried that fence.
            connection.execute(
                "ALTER TABLE task_review_cards DROP COLUMN source_revision"
            )
            connection.execute("PRAGMA user_version = 15")

        CandidateInbox(self.database, clock=lambda: NOW).initialize()

        with closing(sqlite3.connect(self.database)) as connection:
            version = connection.execute("PRAGMA user_version").fetchone()[0]
            rows = connection.execute(
                "SELECT COUNT(*) FROM native_intake_historical_refusals"
            ).fetchone()[0]
        self.assertEqual((version, rows), (SCHEMA_VERSION, 0))
        self.assertEqual(self.inbox.get(item["candidate_id"]).task.text,
                         item["task"]["text"])

    def test_bounded_ordered_intake_creates_once_and_replays_without_writes(self):
        self.assertEqual(self.activate().disposition, NativeIntakeDisposition.APPLIED)
        first = candidate(1)
        second = candidate(2)
        self.assertTrue(self.inbox.import_feed(feed(0, first, second)).accepted)

        one = self.intake(limit=1)
        self.assertEqual(
            (one.tasks_created, one.previous_cursor, one.current_cursor, one.remaining),
            (1, 0, 1, 1),
        )
        two = self.intake(limit=1)
        self.assertEqual(
            (two.tasks_created, two.previous_cursor, two.current_cursor, two.remaining),
            (1, 1, 2, 0),
        )
        before = self._state()
        replay = self.intake()
        self.assertEqual(replay.disposition, NativeIntakeDisposition.UNCHANGED)
        self.assertEqual(self._state(), before)
        self.assertEqual((self.ledger.count(), self.ledger.binding_count()), (2, 2))

    def test_legacy_candidate_uses_ordinary_exactly_once_intake(self):
        self.activate()
        item = legacy_candidate(17)
        self.assertTrue(self.inbox.import_feed(feed(0, item)).accepted)

        applied = self.intake()

        self.assertEqual(applied.tasks_created, 1)
        task = self.ledger.get(1)
        self.assertEqual(
            (task.text, task.owner, task.due),
            (
                "Prepare migrated synthetic summary 17",
                "Person B",
                "2030-03-20",
            ),
        )
        origin = self.ledger.origin(1)
        self.assertEqual(
            (origin.kind, origin.record_id, origin.item_id),
            ("legacy", "example-task-ledger", "task-017"),
        )
        self.assertEqual(
            self.intake().disposition, NativeIntakeDisposition.UNCHANGED
        )
        self.assertEqual(self.ledger.count(), 1)

    def test_structured_owner_is_persisted_separately_from_display(self):
        self.activate()
        item = owner_candidate(1)
        self.assertTrue(self.inbox.import_feed(feed(0, item)).accepted)

        self.assertEqual(self.intake().tasks_created, 1)

        task = self.ledger.get(1)
        self.assertEqual(task.owner, "Person A")
        self.assertEqual(task.owner_ref_version, 1)
        self.assertEqual(task.owner_kind, "person")
        self.assertEqual(task.owner_speaker_id, "SPK_101")
        self.assertEqual(task.owner_canonical_speaker_id, "SPK_001")
        self.assertEqual(task.owner_speaker_registry_id, "registry-alpha")
        self.assertFalse(task.owner_pinned)
        self.assertFalse(task.owner_provisional)

    def test_new_task_scans_the_existing_open_queue_for_duplicates(self):
        self.activate()
        first = cross_source_owner_candidate(
            1, kind="meeting", text="Prepare the synthetic rollout checklist"
        )
        self.assertTrue(self.inbox.import_feed(feed(0, first)).accepted)
        self.assertEqual(self.intake().tasks_created, 1)

        second = cross_source_owner_candidate(
            2, kind="email", text="Draft the synthetic rollout checklist"
        )
        self.assertTrue(self.inbox.import_feed(feed(1, second)).accepted)
        self.assertEqual(self.intake().tasks_created, 1)

        with closing(sqlite3.connect(self.database)) as connection:
            proposal = connection.execute(
                "SELECT left_task_id,right_task_id,state "
                "FROM task_duplicate_proposals"
            ).fetchone()
        self.assertEqual(proposal, (1, 2, "proposed"))

    def test_new_task_scans_a_recently_closed_task_for_duplicates(self):
        self.activate()
        closed = cross_source_owner_candidate(
            1, kind="meeting", text="Prepare the synthetic rollout checklist"
        )
        self.assertTrue(self.inbox.import_feed(feed(0, closed)).accepted)
        self.assertEqual(self.intake().tasks_created, 1)
        self.assertTrue(
            self.ledger.transition(1, expected_version=1, action="done").accepted
        )

        new = cross_source_owner_candidate(
            2, kind="email", text="Draft the synthetic rollout checklist"
        )
        self.assertTrue(self.inbox.import_feed(feed(1, new)).accepted)
        self.assertEqual(self.intake().tasks_created, 1)

        with closing(sqlite3.connect(self.database)) as connection:
            proposal = connection.execute(
                "SELECT left_task_id,right_task_id,state "
                "FROM task_duplicate_proposals"
            ).fetchone()
        self.assertEqual(proposal, (1, 2, "proposed"))

    def test_task_revision_does_not_trigger_duplicate_scan(self):
        self.activate()
        first = cross_source_owner_candidate(
            1, kind="meeting", text="Prepare the rollout checklist"
        )
        second = cross_source_owner_candidate(
            2, kind="email", text="Review the agenda memorandum"
        )
        self.assertTrue(self.inbox.import_feed(feed(0, first)).accepted)
        self.assertEqual(self.intake().tasks_created, 1)
        self.assertTrue(self.inbox.import_feed(feed(1, second)).accepted)
        self.assertEqual(self.intake().tasks_created, 1)

        revised = cross_source_owner_candidate(
            2, kind="email", text="Draft the rollout checklist"
        )
        self.assertTrue(self.inbox.import_feed(feed(2, revised)).accepted)
        self.assertEqual(self.intake().tasks_revised, 1)

        with closing(sqlite3.connect(self.database)) as connection:
            proposal_count = connection.execute(
                "SELECT count(*) FROM task_duplicate_proposals"
            ).fetchone()[0]
        self.assertEqual(proposal_count, 0)

    def test_pinned_owner_survives_a_later_candidate_revision(self):
        self.activate()
        initial = owner_candidate(1)
        self.inbox.import_feed(feed(0, initial))
        self.intake()
        with closing(sqlite3.connect(self.database)) as connection, connection:
            connection.execute(
                "UPDATE tasks SET owner='Reader choice',owner_ref_version=1,"
                "owner_kind='external',owner_speaker_id=NULL,"
                "owner_canonical_speaker_id=NULL,"
                "owner_speaker_registry_id=NULL,owner_pinned=1,"
                "owner_provisional=0 WHERE id=1"
            )
        revised = owner_candidate(
            1,
            owner="Person B",
            speaker_id="SPK_202",
            canonical_speaker_id="SPK_002",
        )
        self.inbox.import_feed(feed(1, revised))

        result = self.intake()

        task = self.ledger.get(1)
        self.assertEqual(result.tasks_revised, 1)
        self.assertEqual(task.owner, "Reader choice")
        self.assertEqual(task.owner_kind, "external")
        self.assertTrue(task.owner_pinned)
        self.assertFalse(task.owner_provisional)
        self.assertEqual(task.version, 1)

    def test_teams_kind_survives_feed_inbox_and_native_intake(self):
        self.activate()
        item = teams_candidate(1)

        self.assertTrue(self.inbox.import_feed(feed(0, item)).accepted)
        self.assertEqual(
            self.inbox.get(item["candidate_id"]).source.kind, "teams"
        )
        self.assertEqual(self.intake().tasks_created, 1)

        origin = self.ledger.origin(1)
        self.assertEqual(
            (origin.kind, origin.record_id, origin.item_id),
            (
                "teams",
                item["source"]["record_id"],
                item["source"]["item_id"],
            ),
        )

    def test_exact_historically_bound_candidate_advances_as_unchanged(self):
        item = candidate(1)
        legacy_feed = feed(0, item)
        legacy_feed["stream_id"] = "legacy-shadow"
        self.assertTrue(self.inbox.import_feed(legacy_feed).accepted)
        self.inbox.import_shadow_feed(
            shadow_feed(observation(item, disposition="minted", task_id=1001))
        )
        self.assertEqual(self.ledger.bootstrap_from_shadow().tasks_created, 1)
        self.assertEqual(self.activate().disposition, NativeIntakeDisposition.APPLIED)
        self.assertTrue(self.inbox.import_feed(feed(0, item)).accepted)

        result = self.intake()

        self.assertEqual(result.disposition, NativeIntakeDisposition.APPLIED)
        self.assertEqual(
            (
                result.tasks_created,
                result.tasks_revised,
                result.candidates_unchanged,
                result.previous_cursor,
                result.current_cursor,
            ),
            (0, 0, 1, 0, 1),
        )
        self.assertEqual((self.ledger.count(), self.ledger.binding_count()), (1, 1))

    def test_historically_bound_nonaccepted_candidate_fails_closed(self):
        item = candidate(1)
        legacy_feed = feed(0, item)
        legacy_feed["stream_id"] = "legacy-shadow"
        self.assertTrue(self.inbox.import_feed(legacy_feed).accepted)
        self.inbox.import_shadow_feed(
            shadow_feed(observation(item, disposition="minted", task_id=1001))
        )
        self.assertEqual(self.ledger.bootstrap_from_shadow().tasks_created, 1)
        with closing(sqlite3.connect(self.database)) as connection, connection:
            connection.execute(
                "UPDATE task_candidate_bindings SET relation='folded'"
            )
        self.assertEqual(self.activate().disposition, NativeIntakeDisposition.APPLIED)
        self.assertTrue(self.inbox.import_feed(feed(0, item)).accepted)

        result = self.intake()

        self.assertEqual(result.refusal, NativeIntakeRefusal.STATE_CONFLICT)
        self.assertEqual(self._intake_cursor(), 0)

    def test_revision_updates_only_the_accepted_open_task_and_appends_event(self):
        self.activate()
        initial = candidate(1)
        self.inbox.import_feed(feed(0, initial))
        self.intake()
        revised = candidate(
            1,
            text="Prepare the revised synthetic summary",
            owner="Person B",
            due="2030-03-20",
        )
        self.inbox.import_feed(feed(1, revised))

        result = self.intake()

        self.assertEqual(result.tasks_revised, 1)
        task = self.ledger.get(1)
        self.assertEqual(
            (task.text, task.owner, task.due, task.version),
            (
                "Prepare the revised synthetic summary",
                "Person B",
                "2030-03-20",
                2,
            ),
        )
        with closing(sqlite3.connect(self.database)) as connection:
            binding = connection.execute(
                "SELECT source_revision FROM task_candidate_bindings"
            ).fetchone()[0]
            event = connection.execute(
                "SELECT kind,task_version,source_revision FROM task_events "
                "ORDER BY sequence DESC LIMIT 1"
            ).fetchone()
        self.assertEqual(binding, revised["source"]["revision"])
        self.assertEqual(event, ("candidate_revised", 2, binding))

    def test_provenance_only_revision_preserves_an_active_workflow(self):
        self.activate()
        initial = candidate(1)
        self.inbox.import_feed(feed(0, initial))
        self.intake()
        execution = TaskExecutionService(self.database, clock=lambda: NOW)
        scheduled = execution.schedule(1, expected_task_version=1)
        started = execution.start_action(
            1, expected_version=scheduled.version, action="start"
        )
        enriched = provenance_candidate(1)
        self.assertEqual(enriched["task"], initial["task"])
        self.inbox.import_feed(feed(1, enriched))

        result = self.intake()

        task = self.ledger.get(1)
        workflow = execution.get(1)
        self.assertEqual((result.tasks_revised, self.ledger.count()), (1, 1))
        self.assertEqual(task.version, 1)
        self.assertEqual(workflow.version, started.version)
        self.assertEqual(workflow.status, WorkflowStatus.QUEUED)
        self.assertEqual(
            self.inbox.get(enriched["candidate_id"]).evidence.sources[1].role,
            "protocol",
        )
        with closing(sqlite3.connect(self.database)) as connection:
            binding, event = connection.execute(
                "SELECT source_revision FROM task_candidate_bindings"
            ).fetchone()[0], connection.execute(
                "SELECT kind,task_version FROM task_events "
                "ORDER BY sequence DESC LIMIT 1"
            ).fetchone()
        self.assertEqual(binding, enriched["source"]["revision"])
        self.assertEqual(event, ("candidate_revised", 1))

    def test_cumulative_provenance_revision_never_versions_the_task(self):
        self.activate()
        cursor = 0
        for offset, (kind, role) in enumerate(SOURCE_ROLE.items(), start=10):
            with self.subTest(kind=kind):
                initial = cumulative_candidate(offset, kind)
                self.inbox.import_feed(feed(cursor, initial))
                cursor = self.intake().current_cursor
                enriched = copy.deepcopy(initial)
                enriched["evidence"]["sources"] = [{
                    "name": f"example-{kind}.txt",
                    "role": role,
                    "extract": f"Synthetic {kind} evidence for this action.",
                }]
                enriched["lifecycle"].update({
                    "generation": 2,
                    "changed_at": "2030-02-02T12:00:00Z",
                })
                enriched["source"]["revision"] = hashlib.sha256(
                    json.dumps(enriched["evidence"], sort_keys=True).encode("utf-8")
                ).hexdigest()
                self.inbox.import_feed(feed(cursor, enriched))

                result = self.intake()
                cursor = result.current_cursor

                task = self.ledger.get(offset - 9)
                self.assertEqual(result.tasks_revised, 1)
                self.assertEqual(task.version, 1)
                stored = self.inbox.get(enriched["candidate_id"])
                self.assertEqual(stored.evidence.sources[0].role, role)

    def test_withdrawal_before_binding_advances_without_creating_a_task(self):
        self.activate()
        withdrawn = lifecycle_candidate(1, generation=1, state="withdrawn")
        self.assertTrue(self.inbox.import_feed(feed(0, withdrawn)).accepted)

        result = self.intake()

        self.assertEqual(result.candidates_withdrawn, 1)
        self.assertEqual((result.current_cursor, self.ledger.count()), (1, 0))
        self.assertEqual(self.intake().disposition, NativeIntakeDisposition.UNCHANGED)

    def test_withdrawal_preserves_open_task_and_appends_auditable_event(self):
        self.activate()
        active = lifecycle_candidate(1, generation=1)
        self.inbox.import_feed(feed(0, active))
        self.intake()
        withdrawn = lifecycle_candidate(1, generation=2, state="withdrawn")
        self.inbox.import_feed(feed(1, withdrawn))

        result = self.intake()

        task = self.ledger.get(1)
        self.assertEqual(result.candidates_withdrawn, 1)
        self.assertEqual((task.status, task.version), (TaskStatus.OPEN, 2))
        with closing(sqlite3.connect(self.database)) as connection:
            lifecycle = connection.execute(
                "SELECT state,resolution,task_version FROM "
                "task_candidate_lifecycle"
            ).fetchone()
            event = connection.execute(
                "SELECT kind,task_version FROM task_events "
                "ORDER BY sequence DESC LIMIT 1"
            ).fetchone()
        self.assertEqual(lifecycle, ("withdrawn", "preserved_open", 2))
        self.assertEqual(event, ("candidate_withdrawn", 2))

    def test_reader_decision_wins_when_candidate_is_withdrawn(self):
        self.activate()
        active = lifecycle_candidate(1, generation=1)
        self.inbox.import_feed(feed(0, active))
        self.intake()
        self.assertTrue(
            self.ledger.transition(1, expected_version=1, action="done").accepted
        )
        withdrawn = lifecycle_candidate(1, generation=2, state="withdrawn")
        self.inbox.import_feed(feed(1, withdrawn))

        self.assertEqual(self.intake().candidates_withdrawn, 1)

        task = self.ledger.get(1)
        self.assertEqual((task.status, task.version), (TaskStatus.DONE, 2))
        with closing(sqlite3.connect(self.database)) as connection:
            lifecycle = connection.execute(
                "SELECT state,resolution FROM task_candidate_lifecycle"
            ).fetchone()
            event = connection.execute(
                "SELECT kind FROM task_events ORDER BY sequence DESC LIMIT 1"
            ).fetchone()[0]
        self.assertEqual(lifecycle, ("withdrawn", "reader_conflict"))
        self.assertEqual(event, "candidate_withdrawal_conflict")

    def test_a_revision_for_a_closed_task_is_recorded_not_applied(self):
        """The reader closing a task is the end of its life, not a conflict.

        A producer that still holds the task open keeps re-emitting it, so
        refusing here did not pause the stream, it stopped it: the cursor
        never advanced past the item, and every later candidate -- for open
        tasks too -- was blocked behind a decision the reader had already
        made correctly.
        """
        self.activate()
        self.inbox.import_feed(feed(0, candidate(1)))
        self.intake()
        self.assertTrue(
            self.ledger.transition(1, expected_version=1, action="done").accepted
        )
        closed = self.ledger.get(1)
        revised = candidate(
            1,
            text="Prepare the revised synthetic summary",
            owner="Person B",
            due="2030-03-20",
        )
        self.inbox.import_feed(feed(1, revised))

        result = self.intake()

        self.assertTrue(result.accepted, result.refusal)
        self.assertEqual(result.candidates_after_close, 1)
        # Counted apart from a pass where nothing happened: the producer
        # changed a task and the change was deliberately not applied.
        self.assertEqual(result.candidates_unchanged, 0)
        self.assertEqual(result.tasks_revised, 0)

        # The reader's decision stands, untouched and unversioned.
        after = self.ledger.get(1)
        self.assertEqual(
            (after.text, after.owner, after.due, after.status, after.version),
            (closed.text, closed.owner, closed.due,
             TaskStatus.DONE, closed.version),
        )
        with closing(sqlite3.connect(self.database)) as connection:
            binding = connection.execute(
                "SELECT source_revision FROM task_candidate_bindings"
            ).fetchone()[0]
            lifecycle = connection.execute(
                "SELECT state,resolution,task_version "
                "FROM task_candidate_lifecycle"
            ).fetchone()
            event = connection.execute(
                "SELECT kind,task_version,source_revision FROM task_events "
                "ORDER BY sequence DESC LIMIT 1"
            ).fetchone()
        # Advanced, so the producer is not asked about this revision again.
        self.assertEqual(binding, revised["source"]["revision"])
        self.assertEqual(
            lifecycle, ("active", "reader_conflict", closed.version))
        # Not `candidate_revised`: the ledger must not claim a revision was
        # folded into a task when it was not.
        self.assertEqual(
            event,
            ("candidate_revision_conflict", closed.version, binding),
        )

    def test_a_closed_task_does_not_block_the_candidates_behind_it(self):
        """The whole point. One settled task must not stop the stream."""
        self.activate()
        self.inbox.import_feed(feed(0, candidate(1), candidate(2)))
        self.intake()
        self.assertTrue(
            self.ledger.transition(1, expected_version=1, action="done").accepted
        )
        self.inbox.import_feed(feed(
            2,
            candidate(1, text="Revised after the reader closed it"),
            candidate(2, text="Revised while still open"),
        ))

        result = self.intake()

        self.assertTrue(result.accepted, result.refusal)
        self.assertEqual(
            (result.candidates_after_close, result.tasks_revised), (1, 1))
        self.assertEqual(self.ledger.get(1).status, TaskStatus.DONE)
        self.assertEqual(
            self.ledger.get(2).text, "Revised while still open")

    def test_a_revision_whose_task_is_gone_still_refuses(self):
        """A binding pointing at a task that does not exist is corruption,
        not a race, and must still stop the pass."""
        self.activate()
        self.inbox.import_feed(feed(0, candidate(1)))
        self.intake()
        with closing(sqlite3.connect(self.database)) as connection:
            connection.execute("PRAGMA writable_schema = ON")
            connection.execute("DELETE FROM tasks WHERE id=1")
            connection.commit()
        self.inbox.import_feed(feed(1, candidate(1, text="Revised")))

        result = self.intake()

        self.assertFalse(result.accepted)
        self.assertEqual(result.refusal, NativeIntakeRefusal.STATE_CONFLICT)

    def test_stale_generation_cannot_reactivate_withdrawn_candidate(self):
        active = lifecycle_candidate(1, generation=1)
        withdrawn = lifecycle_candidate(1, generation=2, state="withdrawn")
        self.assertTrue(self.inbox.import_document(active).accepted)
        self.assertTrue(self.inbox.import_document(withdrawn).accepted)

        stale = self.inbox.import_document(active)

        self.assertEqual(stale.disposition.value, "refused")
        self.assertEqual(stale.refusal.value, "stale_generation")
        self.assertEqual(
            self.inbox.get(active["candidate_id"]).lifecycle.state,
            "withdrawn",
        )

    def test_reactivation_is_explicit_and_keeps_stable_task_identity(self):
        self.activate()
        active = lifecycle_candidate(1, generation=1)
        self.inbox.import_feed(feed(0, active))
        self.intake()
        withdrawn = lifecycle_candidate(1, generation=2, state="withdrawn")
        self.inbox.import_feed(feed(1, withdrawn))
        self.intake()
        reactivated = lifecycle_candidate(
            1, generation=3, text="Prepare the restored synthetic summary"
        )
        self.inbox.import_feed(feed(2, reactivated))

        result = self.intake()

        self.assertEqual((result.tasks_revised, self.ledger.count()), (1, 1))
        self.assertEqual(self.ledger.get(1).text, reactivated["task"]["text"])
        with closing(sqlite3.connect(self.database)) as connection:
            event = connection.execute(
                "SELECT kind FROM task_events ORDER BY sequence DESC LIMIT 1"
            ).fetchone()[0]
        self.assertEqual(event, "candidate_reactivated")

    def test_withdrawal_cancels_stale_cards_without_dropping_task(self):
        self.activate()
        active = lifecycle_candidate(1, generation=1)
        self.inbox.import_feed(feed(0, active))
        self.intake()
        cards = TaskCardService(
            self.database, clock=lambda: NOW, token_factory=lambda: "a" * 43
        )
        self.assertEqual(cards.schedule().created, 1)
        withdrawn = lifecycle_candidate(1, generation=2, state="withdrawn")
        self.inbox.import_feed(feed(1, withdrawn))
        self.intake()

        converged = cards.schedule()

        self.assertEqual((converged.created, converged.cancelled), (0, 1))
        self.assertEqual(cards.due(), ())
        self.assertEqual(self.ledger.get(1).status, TaskStatus.OPEN)

    def test_withdrawal_after_newer_revision_preserves_latest_task_text(self):
        self.activate()
        first = lifecycle_candidate(1, generation=1)
        self.inbox.import_feed(feed(0, first))
        self.intake()
        revised = lifecycle_candidate(
            1, generation=2, text="Prepare the newer synthetic summary"
        )
        self.inbox.import_feed(feed(1, revised))
        self.intake()
        withdrawn = lifecycle_candidate(1, generation=3, state="withdrawn")
        self.inbox.import_feed(feed(2, withdrawn))

        self.assertEqual(self.intake().candidates_withdrawn, 1)

        self.assertEqual(self.ledger.get(1).text, revised["task"]["text"])

    def test_withdrawal_cancels_start_gated_execution_without_rescheduling(self):
        self.activate()
        active = lifecycle_candidate(1, generation=1)
        self.inbox.import_feed(feed(0, active))
        self.intake()
        execution = TaskExecutionService(self.database, clock=lambda: NOW)
        scheduled = execution.schedule(1, expected_task_version=1)
        self.assertEqual(scheduled.status, WorkflowStatus.AWAITING_START)
        withdrawn = lifecycle_candidate(1, generation=2, state="withdrawn")
        self.inbox.import_feed(feed(1, withdrawn))
        self.intake()

        self.assertEqual(execution.schedule_new().scheduled, 0)

        self.assertEqual(execution.get(1).status, WorkflowStatus.CANCELLED)

    def test_already_active_execution_is_preserved_as_withdrawal_conflict(self):
        self.activate()
        active = lifecycle_candidate(1, generation=1)
        self.inbox.import_feed(feed(0, active))
        self.intake()
        execution = TaskExecutionService(self.database, clock=lambda: NOW)
        scheduled = execution.schedule(1, expected_task_version=1)
        started = execution.start_action(
            1, expected_version=scheduled.version, action="start"
        )
        self.assertEqual(started.status, WorkflowStatus.QUEUED)
        withdrawn = lifecycle_candidate(1, generation=2, state="withdrawn")
        self.inbox.import_feed(feed(1, withdrawn))

        self.assertEqual(self.intake().candidates_withdrawn, 1)

        self.assertEqual(self.ledger.get(1).version, 1)
        self.assertEqual(execution.get(1).status, WorkflowStatus.QUEUED)
        with closing(sqlite3.connect(self.database)) as connection:
            resolution = connection.execute(
                "SELECT resolution FROM task_candidate_lifecycle"
            ).fetchone()[0]
        self.assertEqual(resolution, "reader_conflict")

    def test_revision_invalidates_an_existing_task_card(self):
        self.activate()
        initial = candidate(1)
        self.inbox.import_feed(feed(0, initial))
        self.intake()
        cards = TaskCardService(
            self.database, clock=lambda: NOW, token_factory=lambda: "a" * 43
        )
        self.assertEqual(cards.schedule().created, 1)
        claim = cards.claim_next(
            consumer_digest=hashlib.sha256(b"synthetic-consumer").hexdigest()
        )
        self.assertIsNotNone(claim)
        delivered = cards.complete_delivery(
            claim.card.id,
            expected_version=claim.card.version,
            claim_token=claim.token,
            transport="synthetic",
            delivery_ref="message-1",
        )
        revised = candidate(1, text="Prepare a newer synthetic summary")
        self.inbox.import_feed(feed(1, revised))
        self.assertEqual(self.intake().tasks_revised, 1)

        stale = cards.act(
            claim.card.id,
            expected_version=delivered.version,
            action="done",
        )

        self.assertEqual(stale.refusal, CardRefusal.STALE_VERSION)
        self.assertEqual(self.ledger.get(1).status.value, "open")

    def test_owner_identity_only_revision_invalidates_an_existing_card(self):
        self.activate()
        initial = owner_candidate(1)
        self.inbox.import_feed(feed(0, initial))
        self.intake()
        cards = TaskCardService(
            self.database, clock=lambda: NOW, token_factory=lambda: "a" * 43
        )
        self.assertEqual(cards.schedule().created, 1)
        claim = cards.claim_next(
            consumer_digest=hashlib.sha256(b"synthetic-consumer").hexdigest()
        )
        delivered = cards.complete_delivery(
            claim.card.id,
            expected_version=claim.card.version,
            claim_token=claim.token,
            transport="synthetic",
            delivery_ref="message-1",
        )
        revised = owner_candidate(
            1,
            owner="Person A",
            speaker_id="SPK_102",
            canonical_speaker_id="SPK_001",
        )
        self.inbox.import_feed(feed(1, revised))

        self.assertEqual(self.intake().tasks_revised, 1)
        self.assertEqual(self.ledger.get(1).version, 2)
        stale = cards.act(
            claim.card.id,
            expected_version=delivered.version,
            action="done",
        )
        self.assertEqual(stale.refusal, CardRefusal.STALE_VERSION)

    def test_owner_provenance_revision_retains_sources_and_updates_owner(self):
        self.activate()
        initial = provenance_candidate(1)
        self.inbox.import_feed(feed(0, initial))
        self.intake()
        revised = owner_provenance_candidate(1)
        self.inbox.import_feed(feed(1, revised))

        result = self.intake()

        self.assertEqual(result.tasks_revised, 1)
        task = self.ledger.get(1)
        self.assertEqual(task.owner_ref_version, 1)
        self.assertEqual(task.owner_kind, "person")
        with closing(sqlite3.connect(self.database)) as connection:
            payload = json.loads(
                connection.execute(
                    "SELECT payload_json FROM candidate_inbox WHERE candidate_id=?",
                    (revised["candidate_id"],),
                ).fetchone()[0]
            )
        self.assertEqual(payload["schema_version"], 6)
        self.assertEqual(len(payload["evidence"]["sources"]), 3)

    def test_conflict_rolls_back_complete_pass_and_keeps_cursor(self):
        self.activate()
        first = candidate(1)
        second = candidate(2)
        self.inbox.import_feed(feed(0, first, second))
        self.inbox.import_shadow_feed(
            shadow_feed(observation(second, disposition="refused", task_id=None))
        )

        result = self.intake()

        self.assertEqual(result.refusal, NativeIntakeRefusal.STATE_CONFLICT)
        self.assertEqual((self.ledger.count(), self.ledger.binding_count()), (0, 0))
        self.assertEqual(self._intake_cursor(), 0)

    def test_revision_with_a_post_boundary_producer_decision_fails_closed(self):
        self.activate()
        initial = candidate(1)
        self.inbox.import_feed(feed(0, initial))
        self.intake()
        revised = candidate(1, text="Prepare a disputed synthetic revision")
        self.inbox.import_feed(feed(1, revised))
        self.inbox.import_shadow_feed(
            shadow_feed(observation(revised, disposition="minted", task_id=1001))
        )

        result = self.intake()

        self.assertEqual(result.refusal, NativeIntakeRefusal.STATE_CONFLICT)
        self.assertEqual(self.ledger.get(1).version, 1)
        self.assertEqual(self._intake_cursor(), 1)

    def test_terminal_revision_advances_the_cursor_past_itself(self):
        """This used to fail closed, and that was the defect (#209).

        Failing closed on a revision for a task the reader has closed reads
        as caution, but the reader closing a task is the ordinary end of its
        life and the producer keeps re-emitting it, so the cursor could never
        get past the item. It did not pause the stream; it ended it. The task
        is still protected -- nothing is applied to it -- but the pass
        continues, which is what the cursor assertion here now checks.
        """
        self.activate()
        first = candidate(1)
        self.inbox.import_feed(feed(0, first))
        self.intake()
        self.ledger.transition(1, expected_version=1, action="done")
        terminal_revision = candidate(1, text="Revise a closed synthetic task")
        self.inbox.import_feed(feed(1, terminal_revision))

        result = self.intake()

        self.assertTrue(result.accepted, result.refusal)
        self.assertEqual(result.candidates_after_close, 1)
        self.assertEqual(self._intake_cursor(), 2)
        self.assertEqual(self.ledger.get(1).status, TaskStatus.DONE)

    def test_folded_revision_fails_closed(self):
        self.activate()
        first = candidate(1)
        self.inbox.import_feed(feed(0, first))
        self.intake()
        other = candidate(2)
        with closing(sqlite3.connect(self.database)) as connection, connection:
            connection.execute(
                "INSERT INTO candidate_revision_history("
                "candidate_id,source_revision,payload_json,created_at,imported_at) "
                "VALUES(?,?,?,?,?)",
                (
                    other["candidate_id"],
                    other["source"]["revision"],
                    json.dumps(other, sort_keys=True, separators=(",", ":")),
                    other["created_at"],
                    NOW.isoformat(),
                ),
            )
            connection.execute(
                "INSERT INTO candidate_inbox("
                "candidate_id,source_system,source_kind,source_record_id,"
                "source_item_id,source_revision,payload_json,created_at,"
                "first_imported_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
                (
                    other["candidate_id"], "gw", "meeting", "record-002",
                    "action-002", other["source"]["revision"],
                    json.dumps(other, sort_keys=True, separators=(",", ":")),
                    other["created_at"], NOW.isoformat(), NOW.isoformat(),
                ),
            )
            connection.execute(
                "INSERT INTO candidate_lifecycle("
                "candidate_id,source_revision,state,generation,changed_at,"
                "updated_at) VALUES(?,?,'active',0,NULL,?)",
                (
                    other["candidate_id"],
                    other["source"]["revision"],
                    NOW.isoformat(),
                ),
            )
            connection.execute(
                "INSERT INTO task_candidate_bindings("
                "candidate_id,source_revision,task_id,relation,decided_at) "
                "VALUES(?,?,1,'folded',?)",
                (other["candidate_id"], other["source"]["revision"], NOW.isoformat()),
            )
            connection.execute(
                "INSERT INTO task_candidate_lifecycle("
                "candidate_id,source_revision,task_version,state,resolution,"
                "changed_at,decided_at) "
                "VALUES(?,?,1,'active','current',NULL,?)",
                (
                    other["candidate_id"],
                    other["source"]["revision"],
                    NOW.isoformat(),
                ),
            )
        folded_revision = candidate(2, text="Revise a folded synthetic task")
        self.inbox.import_feed(feed(1, folded_revision))
        self.assertEqual(self.intake().refusal, NativeIntakeRefusal.STATE_CONFLICT)
        self.assertEqual(self._intake_cursor(), 1)

    def test_missing_provenance_and_invalid_arguments_fail_closed(self):
        self.activate()
        with closing(sqlite3.connect(self.database)) as connection, connection:
            connection.execute(
                "INSERT INTO candidate_feed_cursors("
                "producer,stream_id,cursor,updated_at) VALUES('gw','primary',1,?) "
                "ON CONFLICT(producer,stream_id) DO UPDATE SET cursor=1",
                (NOW.isoformat(),),
            )
        self.assertEqual(self.intake().refusal, NativeIntakeRefusal.STATE_CONFLICT)
        self.assertEqual(self._intake_cursor(), 0)
        invalid = self.ledger.accept_native_candidates(
            producer="gw", stream_id="primary", limit=True
        )
        self.assertEqual(invalid.refusal, NativeIntakeRefusal.INVALID_ARGUMENT)

    def test_provenance_and_intake_events_are_append_only(self):
        self.activate()
        self.inbox.import_feed(feed(0, candidate(1)))
        self.intake()
        with closing(sqlite3.connect(self.database)) as connection:
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute(
                    "UPDATE candidate_feed_items SET sequence=2 WHERE sequence=1"
                )
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute(
                    "DELETE FROM native_candidate_intake_events WHERE sequence=1"
                )
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute(
                    "UPDATE native_candidate_intakes SET activation_cursor=1"
                )
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute("DELETE FROM native_candidate_intakes")

    def test_activation_permanently_disables_legacy_bootstrap(self):
        self.activate()
        resolver_called = False

        def resolver(*_args, **_kwargs):
            nonlocal resolver_called
            resolver_called = True
            raise AssertionError("legacy resolver must not be called")

        result = self.ledger.bootstrap_from_shadow(owner_resolver=resolver)

        self.assertEqual(result.disposition, BootstrapDisposition.REFUSED)
        self.assertEqual(result.refusal, BootstrapRefusal.INVALID_STATE)
        self.assertFalse(resolver_called)

    def test_cli_is_content_free_and_unsafe_state_is_refused(self):
        stdout = StringIO()
        with redirect_stdout(stdout):
            self.assertEqual(main([
                "activate", "--database", str(self.database),
                "--stream-id", "primary", "--expected-cursor", "0",
            ]), 0)
        activated = json.loads(stdout.getvalue())
        self.assertEqual(activated["disposition"], "applied")

        private_text = "Prepare a private-looking synthetic task"
        self.inbox.import_feed(feed(0, candidate(1, text=private_text)))
        stdout = StringIO()
        with redirect_stdout(stdout):
            self.assertEqual(main([
                "run", "--database", str(self.database),
                "--stream-id", "primary", "--limit", "1",
            ]), 0)
        self.assertNotIn(private_text, stdout.getvalue())
        self.assertEqual(json.loads(stdout.getvalue())["counts"]["tasks_created"], 1)

        alias = self.root / "alias.sqlite3"
        alias.symlink_to(self.database)
        stderr = StringIO()
        with redirect_stderr(stderr):
            self.assertEqual(main([
                "run", "--database", str(alias), "--stream-id", "primary",
            ]), 78)
        self.assertEqual(
            stderr.getvalue().strip(),
            "foxhound native intake: configuration unavailable",
        )

    def test_reconciliation_cli_reports_only_aggregate_counts(self):
        private_text = "Prepare private-looking synthetic reconciliation"
        item = candidate(1, text=private_text, owner="Person A")
        self.inbox.import_feed(feed(0, item))
        self.inbox.import_shadow_feed(shadow_feed(observation(
            item,
            disposition="minted",
            task_id=1001,
            legacy_owner="Person B",
        )))

        stdout = StringIO()
        with redirect_stdout(stdout):
            self.assertEqual(main([
                "refuse-divergent", "--database", str(self.database),
                "--stream-id", "primary", "--expected-count", "1",
                "--reason", "preserved_legacy_owner",
            ]), 0)
        document = json.loads(stdout.getvalue())
        self.assertEqual(document["refusals_recorded"], 1)
        self.assertNotIn(private_text, stdout.getvalue())

    def _intake_cursor(self) -> int:
        with closing(sqlite3.connect(self.database)) as connection:
            return int(connection.execute(
                "SELECT cursor FROM native_candidate_intakes"
            ).fetchone()[0])

    def _state(self) -> tuple:
        with closing(sqlite3.connect(self.database)) as connection:
            return (
                tuple(connection.execute(
                    "SELECT id,status,text,owner,due,version FROM tasks ORDER BY id"
                )),
                tuple(connection.execute(
                    "SELECT candidate_id,source_revision,task_id,relation "
                    "FROM task_candidate_bindings ORDER BY candidate_id"
                )),
                tuple(connection.execute(
                    "SELECT kind,from_cursor,to_cursor,tasks_created,tasks_revised,"
                    "candidates_unchanged FROM native_candidate_intake_events "
                    "ORDER BY sequence"
                )),
                self._intake_cursor(),
            )


if __name__ == "__main__":
    unittest.main()
