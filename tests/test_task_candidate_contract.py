from __future__ import annotations

import copy
import json
import unittest
from pathlib import Path

from foxhound.contracts import (
    ContractError,
    candidate_id_for,
    parse_task_candidate,
    task_candidate_document,
)


FIXTURES = Path(__file__).parent / "fixtures" / "contracts"


def fixture(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


class TaskCandidateContractTests(unittest.TestCase):
    def test_accepts_synthetic_meeting_candidate(self):
        candidate = parse_task_candidate(fixture("meeting-candidate-v1.json"))
        self.assertEqual(candidate.source.kind, "meeting")
        self.assertEqual(candidate.task.project, "Project Alpha")
        self.assertEqual(candidate.task.due, "2030-01-15")

    def test_accepts_synthetic_email_candidate(self):
        candidate = parse_task_candidate(fixture("email-candidate-v1.json"))
        self.assertEqual(candidate.source.kind, "email")
        self.assertIsNone(candidate.task.owner)
        self.assertIsNone(candidate.task.due)

    def test_accepts_and_round_trips_synthetic_teams_candidate(self):
        document = fixture("teams-candidate-v2.json")

        candidate = parse_task_candidate(document)

        self.assertEqual(candidate.source.kind, "teams")
        self.assertEqual(task_candidate_document(candidate), document)

    def test_accepts_and_round_trips_projectless_version_2(self):
        document = fixture("meeting-candidate-v2.json")
        candidate = parse_task_candidate(document)

        self.assertEqual(candidate.schema_version, 2)
        self.assertIsNone(candidate.task.project)
        self.assertEqual(task_candidate_document(candidate), document)

    def test_version_3_distinguishes_active_from_withdrawn(self):
        for state in ("active", "withdrawn"):
            document = fixture("meeting-candidate-v2.json")
            document["schema_version"] = 3
            document["lifecycle"] = {
                "state": state,
                "generation": 7,
                "changed_at": "2030-02-01T12:00:00Z",
            }

            candidate = parse_task_candidate(document)

            self.assertEqual(candidate.lifecycle.state, state)
            self.assertEqual(candidate.lifecycle.generation, 7)
            self.assertEqual(task_candidate_document(candidate), document)

    def test_version_3_lifecycle_is_strict(self):
        document = fixture("meeting-candidate-v2.json")
        document["schema_version"] = 3
        document["lifecycle"] = {
            "state": "withdrawn",
            "generation": 1,
            "changed_at": "2030-02-01T12:00:00Z",
        }
        for field, value in (
            ("state", "unknown"),
            ("generation", 0),
            ("generation", True),
            ("changed_at", "2030-02-01T12:00:00"),
        ):
            changed = copy.deepcopy(document)
            changed["lifecycle"][field] = value
            with self.assertRaises(ContractError):
                parse_task_candidate(changed)

        older = fixture("meeting-candidate-v2.json")
        older["lifecycle"] = copy.deepcopy(document["lifecycle"])
        with self.assertRaisesRegex(ContractError, "additional fields"):
            parse_task_candidate(older)

    def test_accepts_and_round_trips_a_synthetic_legacy_candidate(self):
        document = fixture("meeting-candidate-v1.json")
        document["source"].update({
            "kind": "legacy",
            "record_id": "example-task-ledger",
            "item_id": "task-17",
        })
        document["candidate_id"] = candidate_id_for(
            system="gw",
            kind="legacy",
            record_id="example-task-ledger",
            item_id="task-17",
        )

        candidate = parse_task_candidate(document)

        self.assertEqual(candidate.source.kind, "legacy")
        self.assertEqual(candidate.task.project, "Project Alpha")
        self.assertEqual(task_candidate_document(candidate), document)

    def test_versions_have_distinct_strict_task_shapes(self):
        version_1 = fixture("meeting-candidate-v1.json")
        version_1["task"].pop("project")
        with self.assertRaisesRegex(ContractError, "missing required fields"):
            parse_task_candidate(version_1)

        version_2 = fixture("meeting-candidate-v2.json")
        version_2["task"]["project"] = "Project Alpha"
        with self.assertRaisesRegex(ContractError, "additional fields"):
            parse_task_candidate(version_2)

        empty_project = fixture("meeting-candidate-v2.json")
        empty_project["task"]["project"] = ""
        with self.assertRaisesRegex(ContractError, "additional fields"):
            parse_task_candidate(empty_project)

    def test_identity_is_stable_across_source_revisions(self):
        original = fixture("meeting-candidate-v1.json")
        revised = copy.deepcopy(original)
        revised["source"]["revision"] = "f" * 64
        revised["task"]["text"] = "Prepare the revised Project Alpha summary"

        first = parse_task_candidate(original)
        second = parse_task_candidate(revised)

        self.assertEqual(first.candidate_id, second.candidate_id)
        self.assertNotEqual(first.source.revision, second.source.revision)

    def test_identity_changes_for_a_different_source_action(self):
        first = candidate_id_for(
            system="gw", kind="meeting", record_id="record-001",
            item_id="action-01")
        second = candidate_id_for(
            system="gw", kind="meeting", record_id="record-001",
            item_id="action-02")
        self.assertNotEqual(first, second)

    def test_rejects_unknown_version_without_echoing_candidate_content(self):
        document = fixture("meeting-candidate-v1.json")
        document["schema_version"] = 999
        document["task"]["text"] = "private candidate text"

        with self.assertRaisesRegex(ContractError, "schema_version") as raised:
            parse_task_candidate(document)
        self.assertNotIn("private candidate text", str(raised.exception))

    def test_rejects_additional_secret_bearing_field(self):
        document = fixture("meeting-candidate-v1.json")
        document["token"] = "synthetic-secret-value"

        with self.assertRaisesRegex(ContractError, "additional fields") as raised:
            parse_task_candidate(document)
        self.assertNotIn("synthetic-secret-value", str(raised.exception))

    def test_rejects_nested_deployment_field(self):
        document = fixture("meeting-candidate-v1.json")
        document["source"]["host"] = "host-a"
        with self.assertRaisesRegex(ContractError, "additional fields"):
            parse_task_candidate(document)

    def test_rejects_path_shaped_identifier(self):
        document = fixture("meeting-candidate-v1.json")
        document["source"]["record_id"] = "/srv/example/record-001"
        with self.assertRaisesRegex(ContractError, "record_id"):
            parse_task_candidate(document)

    def test_rejects_naive_timestamp(self):
        document = fixture("meeting-candidate-v1.json")
        document["created_at"] = "2030-01-01T12:00:00"
        with self.assertRaisesRegex(ContractError, "timezone"):
            parse_task_candidate(document)

    def test_rejects_invalid_calendar_date(self):
        document = fixture("meeting-candidate-v1.json")
        document["task"]["due"] = "2030-02-30"
        with self.assertRaisesRegex(ContractError, "ISO-8601 date"):
            parse_task_candidate(document)

    def test_rejects_candidate_id_that_does_not_match_source(self):
        document = fixture("meeting-candidate-v1.json")
        document["candidate_id"] = "tc_" + "0" * 64
        with self.assertRaisesRegex(ContractError, "does not match"):
            parse_task_candidate(document)

    def test_schema_is_strict_at_every_object_boundary(self):
        schema_path = (
            Path(__file__).parents[1]
            / "src" / "foxhound" / "contracts" / "schemas"
            / "task-candidate-v1.schema.json"
        )
        schema = json.loads(schema_path.read_text(encoding="utf-8"))
        self.assertFalse(schema["additionalProperties"])
        self.assertFalse(schema["properties"]["source"]["additionalProperties"])
        self.assertFalse(schema["properties"]["task"]["additionalProperties"])
        self.assertFalse(schema["properties"]["evidence"]["additionalProperties"])

        version_2_path = schema_path.with_name("task-candidate-v2.schema.json")
        version_2 = json.loads(version_2_path.read_text(encoding="utf-8"))
        self.assertEqual(version_2["properties"]["schema_version"]["const"], 2)
        self.assertFalse(version_2["additionalProperties"])
        self.assertFalse(
            version_2["properties"]["task"]["additionalProperties"]
        )
        self.assertNotIn("project", version_2["properties"]["task"]["properties"])
        version_3 = json.loads(
            schema_path.with_name("task-candidate-v3.schema.json").read_text(
                encoding="utf-8"
            )
        )
        self.assertEqual(version_3["properties"]["schema_version"]["const"], 3)
        self.assertEqual(
            set(version_3["properties"]["lifecycle"]["properties"]["state"]["enum"]),
            {"active", "withdrawn"},
        )
        self.assertFalse(version_3["additionalProperties"])
        expected_kinds = {"meeting", "email", "teams", "issue", "legacy"}
        self.assertEqual(
            set(schema["properties"]["source"]["properties"]["kind"]["enum"]),
            expected_kinds,
        )
        self.assertEqual(
            set(version_2["properties"]["source"]["properties"]["kind"]["enum"]),
            expected_kinds,
        )
        self.assertEqual(
            set(version_3["properties"]["source"]["properties"]["kind"]["enum"]),
            expected_kinds,
        )


if __name__ == "__main__":
    unittest.main()
