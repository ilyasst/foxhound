"""Synthetic tests for Researcher's bounded task-origin evidence."""

from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path
from unittest import mock

from foxhound import migrate_database
from foxhound.forge_thread import ForgeThreadError, ThreadResult
from foxhound.task_research_sources import (
    ResearchSourceError,
    bound_research_sources,
)


def candidate(*, host: str = "github.com", kind: str = "issue") -> dict:
    item = "42" if kind == "issue" else "7/2030-01-01T00:00:00Z"
    return {
        "schema": "foxhound.task-candidate",
        "schema_version": 2,
        "candidate_id": "candidate-synthetic-001",
        "source": {
            "system": "gw", "kind": kind,
            "record_id": f"{host}/example-org/project-alpha",
            "item_id": item, "revision": "a" * 64,
        },
        "task": {"text": "Prepare the bounded detail view.", "owner": None, "due": None},
        "evidence": {
            "document_id": "synthetic-document",
            "locator": "synthetic-locator",
            "sources": [{
                "name": "issue-body", "role": "primary",
                "extract": "The detail view must remain bounded.",
            }],
        },
        "created_at": "2030-01-01T00:00:00Z",
    }


class BoundResearchSourceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.database = Path(self.temp.name) / "foxhound.sqlite3"
        migrate_database(self.database)

    def bind(self, document: dict) -> None:
        source = document["source"]
        with closing(sqlite3.connect(self.database)) as connection, connection:
            connection.execute(
                "INSERT INTO candidate_inbox(candidate_id,source_system,source_kind,"
                "source_record_id,source_item_id,source_revision,payload_json,created_at,"
                "first_imported_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
                (
                    document["candidate_id"], source["system"], source["kind"],
                    source["record_id"], source["item_id"], source["revision"],
                    json.dumps(document), document["created_at"],
                    document["created_at"], document["created_at"],
                ),
            )
            connection.execute(
                "INSERT INTO candidate_revision_history(candidate_id,source_revision,"
                "payload_json,created_at,imported_at) VALUES(?,?,?,?,?)",
                (
                    document["candidate_id"], source["revision"],
                    json.dumps(document), document["created_at"],
                    document["created_at"],
                ),
            )
            connection.execute(
                "INSERT INTO tasks(id,status,text,owner,due,version,created_at,updated_at) "
                "VALUES(1,'open',?,NULL,NULL,1,?,?)",
                (document["task"]["text"], document["created_at"], document["created_at"]),
            )
            connection.execute(
                "INSERT INTO task_candidate_bindings(candidate_id,source_revision,task_id,"
                "relation,decided_at) VALUES(?,?,1,'accepted',?)",
                (document["candidate_id"], source["revision"], document["created_at"]),
            )

    def test_exact_issue_origin_supplies_snapshot_and_live_thread(self) -> None:
        self.bind(candidate())
        live = ThreadResult(
            repository="github.com/example-org/project-alpha",
            kind="issue", number=42, comments=[], reviews=[], truncated=False,
            url="https://github.com/example-org/project-alpha/issues/42",
            title="Bounded detail view", body="Keep the board face compact.",
            state="OPEN",
        )
        with mock.patch(
            "foxhound.task_research_sources.forge_thread.read_issue_thread",
            return_value=live,
        ) as reader:
            result = bound_research_sources(
                self.database, task_id=1, task_version=1,
            )
        self.assertEqual(result.attempted_namespaces, ("repo",))
        self.assertEqual(len(result.documents), 2)
        self.assertTrue(all(layer == "repo" for layer, _ in result.documents))
        self.assertIn("bounded", result.documents[0][1].excerpt.lower())
        self.assertIn("Keep the board face compact", result.documents[1][1].excerpt)
        reader.assert_called_once_with(
            repository="github.com/example-org/project-alpha", number="42",
        )

    def test_live_read_failure_keeps_immutable_snapshot(self) -> None:
        self.bind(candidate())
        with mock.patch(
            "foxhound.task_research_sources.forge_thread.read_issue_thread",
            side_effect=ForgeThreadError("synthetic failure"),
        ):
            result = bound_research_sources(
                self.database, task_id=1, task_version=1,
            )
        self.assertEqual(len(result.documents), 1)
        self.assertEqual(result.unavailable_source_ids, ("repo:bound-origin-live",))

    def test_review_request_uses_only_its_bound_pull_request(self) -> None:
        self.bind(candidate(kind="review_request"))
        live = ThreadResult(
            repository="github.com/example-org/project-alpha",
            kind="pull-request", number=7, comments=[], reviews=[],
            truncated=False,
            url="https://github.com/example-org/project-alpha/pull/7",
            title="Synthetic review", body="Review the bounded change.",
            state="OPEN",
        )
        with mock.patch(
            "foxhound.task_research_sources.forge_thread.read_pull_request_thread",
            return_value=live,
        ) as reader:
            result = bound_research_sources(
                self.database, task_id=1, task_version=1,
            )
        self.assertEqual(len(result.documents), 2)
        self.assertIn("/pull/7/", result.documents[1][1].path)
        reader.assert_called_once_with(
            repository="github.com/example-org/project-alpha", number="7",
        )

    def test_stale_task_version_is_refused(self) -> None:
        self.bind(candidate())
        with self.assertRaisesRegex(ResearchSourceError, "stale"):
            bound_research_sources(self.database, task_id=1, task_version=2)

    def test_unsupported_host_is_refused_before_a_forge_read(self) -> None:
        self.bind(candidate(host="forge.example"))
        with mock.patch(
            "foxhound.task_research_sources.forge_thread.read_issue_thread",
        ) as reader:
            with self.assertRaisesRegex(ResearchSourceError, "invalid"):
                bound_research_sources(self.database, task_id=1, task_version=1)
        reader.assert_not_called()

    def test_non_forge_origin_adds_nothing(self) -> None:
        document = candidate(kind="meeting")
        document["source"]["item_id"] = "meeting-item"
        self.bind(document)
        result = bound_research_sources(self.database, task_id=1, task_version=1)
        self.assertEqual(result.documents, ())
        self.assertEqual(result.attempted_namespaces, ())


if __name__ == "__main__":
    unittest.main()
