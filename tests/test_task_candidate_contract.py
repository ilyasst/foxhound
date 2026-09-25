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

    def test_accepts_and_round_trips_bounded_source_provenance(self):
        document = fixture("meeting-candidate-v4.json")

        candidate = parse_task_candidate(document)

        self.assertEqual(candidate.schema_version, 4)
        self.assertEqual(
            [source.role for source in candidate.evidence.sources],
            ["handoff", "protocol", "transcript"],
        )
        self.assertEqual(task_candidate_document(candidate), document)

    def test_version_4_allows_a_projectless_meeting(self):
        document = fixture("meeting-candidate-v4.json")
        del document["task"]["project"]

        candidate = parse_task_candidate(document)

        self.assertIsNone(candidate.task.project)
        self.assertEqual(task_candidate_document(candidate), document)

    def test_version_4_provenance_is_strict_and_bounded(self):
        document = fixture("meeting-candidate-v4.json")
        changes = (
            lambda value: value["evidence"]["sources"].__setitem__(slice(None), []),
            lambda value: value["evidence"]["sources"][0].__setitem__(
                "name", "../private.txt"
            ),
            lambda value: value["evidence"]["sources"][0].__setitem__(
                "extract", "x" * 1_201
            ),
            lambda value: value["evidence"]["sources"][0].__setitem__(
                "token", "synthetic-secret"
            ),
        )
        for change in changes:
            broken = copy.deepcopy(document)
            change(broken)
            with self.assertRaises(ContractError):
                parse_task_candidate(broken)

        wrong_kind = copy.deepcopy(document)
        wrong_kind["source"]["kind"] = "email"
        wrong_kind["candidate_id"] = candidate_id_for(
            system="gw",
            kind="email",
            record_id=wrong_kind["source"]["record_id"],
            item_id=wrong_kind["source"]["item_id"],
        )
        with self.assertRaisesRegex(ContractError, "provenance"):
            parse_task_candidate(wrong_kind)

    def test_version_5_round_trips_a_scoped_owner_identity(self):
        document = fixture("meeting-candidate-v2.json")
        document["schema_version"] = 5
        document["task"]["owner_ref"] = {
            "kind": "person",
            "speaker_id": "SPK_101",
            "canonical_speaker_id": "SPK_001",
            "speaker_registry_id": "registry-alpha",
            "pinned": False,
            "provisional": False,
        }

        candidate = parse_task_candidate(document)

        self.assertEqual(candidate.task.owner_ref.kind, "person")
        self.assertEqual(
            candidate.task.owner_ref.canonical_speaker_id, "SPK_001"
        )
        self.assertEqual(task_candidate_document(candidate), document)

    def test_version_5_requires_complete_nonleaking_owner_identity(self):
        valid = fixture("meeting-candidate-v2.json")
        valid["schema_version"] = 5
        valid["task"]["owner_ref"] = {
            "kind": "person",
            "speaker_id": "SPK_101",
            "canonical_speaker_id": "SPK_001",
            "speaker_registry_id": "registry-alpha",
            "pinned": False,
            "provisional": False,
        }
        changes = (
            lambda value: value["task"]["owner_ref"].__setitem__(
                "speaker_registry_id", None
            ),
            lambda value: value["task"].__setitem__(
                "owner", "Person A (SPK_101)"
            ),
            lambda value: value["task"]["owner_ref"].__setitem__(
                "pinned", 1
            ),
            lambda value: value["task"]["owner_ref"].__setitem__(
                "token", "synthetic-secret"
            ),
        )
        for change in changes:
            broken = copy.deepcopy(valid)
            change(broken)
            with self.assertRaises(ContractError):
                parse_task_candidate(broken)

        producer_pin = copy.deepcopy(valid)
        producer_pin["task"]["owner_ref"]["pinned"] = True
        with self.assertRaisesRegex(ContractError, "producer pin"):
            parse_task_candidate(producer_pin)

        unresolved = copy.deepcopy(valid)
        unresolved["task"]["owner"] = "Unknown speaker"
        unresolved["task"]["owner_ref"].update({
            "kind": "unresolved",
            "canonical_speaker_id": None,
            "provisional": True,
        })
        with self.assertRaisesRegex(ContractError, "unresolved display"):
            parse_task_candidate(unresolved)

    def test_version_6_round_trips_provenance_and_owner_identity(self):
        document = fixture("meeting-candidate-v4.json")
        document["schema_version"] = 6
        document["task"]["owner"] = "Person A"
        document["task"]["owner_ref"] = {
            "kind": "person",
            "speaker_id": "SPK_101",
            "canonical_speaker_id": "SPK_001",
            "speaker_registry_id": "registry-alpha",
            "pinned": False,
            "provisional": False,
        }

        candidate = parse_task_candidate(document)

        self.assertEqual(candidate.schema_version, 6)
        self.assertEqual(candidate.task.owner_ref.kind, "person")
        self.assertEqual(len(candidate.evidence.sources), 3)
        self.assertEqual(task_candidate_document(candidate), document)

    def test_version_6_requires_meeting_provenance_and_unpinned_owner(self):
        valid = fixture("meeting-candidate-v4.json")
        valid["schema_version"] = 6
        valid["task"]["owner_ref"] = {
            "kind": "person",
            "speaker_id": None,
            "canonical_speaker_id": None,
            "speaker_registry_id": None,
            "pinned": False,
            "provisional": False,
        }
        parse_task_candidate(valid)

        no_sources = copy.deepcopy(valid)
        del no_sources["evidence"]["sources"]
        with self.assertRaisesRegex(ContractError, "missing required fields"):
            parse_task_candidate(no_sources)

        wrong_kind = copy.deepcopy(valid)
        wrong_kind["source"]["kind"] = "email"
        wrong_kind["candidate_id"] = candidate_id_for(
            system="gw",
            kind="email",
            record_id=wrong_kind["source"]["record_id"],
            item_id=wrong_kind["source"]["item_id"],
        )
        with self.assertRaisesRegex(ContractError, "provenance"):
            parse_task_candidate(wrong_kind)

        pinned = copy.deepcopy(valid)
        pinned["task"]["owner_ref"]["pinned"] = True
        with self.assertRaisesRegex(ContractError, "producer pin"):
            parse_task_candidate(pinned)

    def test_version_7_combines_owner_lifecycle_and_source_specific_provenance(self):
        roles = {
            "meeting": "transcript",
            "email": "message",
            "teams": "message",
            "issue": "body",
            "legacy": "record",
            "review_request": "diff",
            "mention": "comment",
            "calendar": "event",
            "alert": "signal",
        }
        for index, (kind, role) in enumerate(roles.items(), start=1):
            document = fixture("meeting-candidate-v2.json")
            document["schema_version"] = 7
            document["source"].update({
                "kind": kind,
                "record_id": f"record-{index:03d}",
                "item_id": f"item-{index:03d}",
            })
            document["candidate_id"] = candidate_id_for(
                system="gw",
                kind=kind,
                record_id=document["source"]["record_id"],
                item_id=document["source"]["item_id"],
            )
            document["task"]["owner_ref"] = {
                "kind": "person",
                "speaker_id": None,
                "canonical_speaker_id": None,
                "speaker_registry_id": None,
                "pinned": False,
                "provisional": False,
            }
            document["evidence"]["sources"] = [{
                "name": f"source-{index:03d}.txt",
                "role": role,
                "extract": "Person A requested the synthetic work.",
            }]
            document["lifecycle"] = {
                "state": "active",
                "generation": 1,
                "changed_at": "2030-02-01T12:00:00Z",
            }

            candidate = parse_task_candidate(document)

            self.assertEqual(candidate.source.kind, kind)
            self.assertEqual(candidate.evidence.sources[0].role, role)
            self.assertEqual(candidate.lifecycle.generation, 1)
            self.assertEqual(task_candidate_document(candidate), document)

    def test_version_7_provenance_is_optional_and_roles_are_kind_scoped(self):
        document = fixture("meeting-candidate-v2.json")
        document["schema_version"] = 7
        document["source"]["kind"] = "email"
        document["candidate_id"] = candidate_id_for(
            system="gw", kind="email",
            record_id=document["source"]["record_id"],
            item_id=document["source"]["item_id"],
        )
        document["task"]["owner_ref"] = {
            "kind": "person",
            "speaker_id": None,
            "canonical_speaker_id": None,
            "speaker_registry_id": None,
            "pinned": False,
            "provisional": False,
        }
        document["lifecycle"] = {
            "state": "active",
            "generation": 1,
            "changed_at": "2030-02-01T12:00:00Z",
        }
        self.assertEqual(task_candidate_document(
            parse_task_candidate(document)
        ), document)

        wrong_role = copy.deepcopy(document)
        wrong_role["evidence"]["sources"] = [{
            "name": "message.txt",
            "role": "transcript",
            "extract": "Synthetic source quotation.",
        }]
        with self.assertRaisesRegex(ContractError, "role"):
            parse_task_candidate(wrong_role)

        valid = copy.deepcopy(document)
        valid["evidence"]["sources"] = [{
            "name": "message.txt",
            "role": "message",
            "extract": "Synthetic source quotation.",
        }]
        changes = (
            lambda value: value["evidence"]["sources"].__setitem__(
                slice(None), []
            ),
            lambda value: value["evidence"]["sources"][0].__setitem__(
                "name", "../message.txt"
            ),
            lambda value: value["evidence"]["sources"][0].__setitem__(
                "extract", "x" * 1_201
            ),
            lambda value: value["evidence"]["sources"][0].__setitem__(
                "extract", "Synthetic\x00quotation"
            ),
            lambda value: value["evidence"]["sources"][0].__setitem__(
                "secret", "not-permitted"
            ),
        )
        for change in changes:
            broken = copy.deepcopy(valid)
            change(broken)
            with self.assertRaises(ContractError):
                parse_task_candidate(broken)

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
        version_4 = json.loads(
            schema_path.with_name("task-candidate-v4.schema.json").read_text(
                encoding="utf-8"
            )
        )
        self.assertEqual(version_4["properties"]["schema_version"]["const"], 4)
        self.assertEqual(
            version_4["properties"]["evidence"]["properties"]["sources"][
                "maxItems"
            ],
            3,
        )
        self.assertFalse(version_4["additionalProperties"])
        version_5 = json.loads(
            schema_path.with_name("task-candidate-v5.schema.json").read_text(
                encoding="utf-8"
            )
        )
        self.assertEqual(version_5["properties"]["schema_version"]["const"], 5)
        self.assertFalse(version_5["additionalProperties"])
        self.assertFalse(
            version_5["$defs"]["ownerRef"]["additionalProperties"]
        )
        version_6 = json.loads(
            schema_path.with_name("task-candidate-v6.schema.json").read_text(
                encoding="utf-8"
            )
        )
        self.assertEqual(version_6["properties"]["schema_version"]["const"], 6)
        self.assertEqual(
            version_6["properties"]["evidence"]["properties"]["sources"][
                "maxItems"
            ],
            3,
        )
        self.assertFalse(version_6["additionalProperties"])
        self.assertFalse(
            version_6["$defs"]["ownerRef"]["additionalProperties"]
        )
        version_7 = json.loads(
            schema_path.with_name("task-candidate-v7.schema.json").read_text(
                encoding="utf-8"
            )
        )
        self.assertEqual(version_7["properties"]["schema_version"]["const"], 7)
        self.assertIn("lifecycle", version_7["required"])
        self.assertIn(
            "sources", version_7["properties"]["evidence"]["properties"]
        )
        self.assertFalse(version_7["additionalProperties"])
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

    def test_version_8_carries_structured_fields_and_unresolved_participants(self):
        document = fixture("meeting-candidate-v2.json")
        document["schema_version"] = 8
        document["task"]["owner_ref"] = {
            "kind": "person", "speaker_id": "SPK_101",
            "canonical_speaker_id": "SPK_001",
            "speaker_registry_id": "registry-alpha", "pinned": False,
            "provisional": False,
        }
        document["task"].update({
            "object": "synthetic sample", "action": "review",
            "participants": [{
                "kind": "unresolved", "speaker_id": "SPK_404",
                "canonical_speaker_id": None,
                "speaker_registry_id": "registry-alpha",
            }],
            "confidence": 0.75,
        })
        document["lifecycle"] = {
            "state": "active", "generation": 1,
            "changed_at": "2030-01-01T00:00:00Z",
        }

        candidate = parse_task_candidate(document)

        self.assertEqual(candidate.task.object, "synthetic sample")
        self.assertEqual(candidate.task.participants[0].kind, "unresolved")
        self.assertEqual(task_candidate_document(candidate), document)

        invalid = copy.deepcopy(document)
        invalid["task"]["participants"][0]["canonical_speaker_id"] = "SPK_404"
        with self.assertRaisesRegex(ContractError, "unresolved"):
            parse_task_candidate(invalid)

        required_only = copy.deepcopy(document)
        del required_only["task"]["participants"]
        parsed = parse_task_candidate(required_only)
        self.assertEqual(parsed.task.participants, ())
        self.assertEqual(task_candidate_document(parsed), required_only)

    def test_version_9_carries_the_exact_producer_history_entry(self):
        document = fixture("meeting-candidate-v2.json")
        document["schema_version"] = 9
        document["source"]["history"] = {
            "source": "email",
            "stream_id": "primary",
            "item_id": "message-017",
            "position": 17,
            "revision": "f" * 64,
        }
        document["task"].update({
            "owner_ref": {
                "kind": "person", "speaker_id": "SPK_101",
                "canonical_speaker_id": "SPK_001",
                "speaker_registry_id": "registry-alpha", "pinned": False,
                "provisional": False,
            },
            "object": "synthetic sample", "action": "review",
            "confidence": 0.75,
        })
        document["lifecycle"] = {
            "state": "active", "generation": 1,
            "changed_at": "2030-01-01T00:00:00Z",
        }

        candidate = parse_task_candidate(document)

        self.assertEqual(candidate.source.history.position, 17)
        self.assertEqual(candidate.source.history.revision, "f" * 64)
        self.assertEqual(task_candidate_document(candidate), document)

        for field, value in (
            ("position", 0), ("position", True),
            ("revision", "f" * 63), ("extra", "unexpected"),
        ):
            invalid = copy.deepcopy(document)
            invalid["source"]["history"][field] = value
            with self.subTest(field=field, value=value):
                with self.assertRaises(ContractError):
                    parse_task_candidate(invalid)

        schema = json.loads(
            (Path(__file__).parents[1] / "src" / "foxhound" / "contracts" /
             "schemas" / "task-candidate-v9.schema.json").read_text(
                encoding="utf-8"
            )
        )
        self.assertEqual(schema["properties"]["schema_version"]["const"], 9)
        self.assertIn("history", schema["properties"]["source"]["required"])

    def test_version_9_does_not_require_structured_task_fields(self):
        document = fixture("meeting-candidate-v2.json")
        document["schema_version"] = 9
        document["source"]["history"] = {
            "source": "meeting",
            "stream_id": "primary",
            "item_id": document["candidate_id"],
            "position": 1,
            "revision": "e" * 64,
        }
        document["task"]["owner_ref"] = {
            "kind": "person", "speaker_id": None,
            "canonical_speaker_id": None, "speaker_registry_id": None,
            "pinned": False, "provisional": True,
        }
        document["lifecycle"] = {
            "state": "active", "generation": 1,
            "changed_at": "2030-01-01T00:00:00Z",
        }

        candidate = parse_task_candidate(document)

        self.assertIsNone(candidate.task.object)
        self.assertEqual(task_candidate_document(candidate), document)

        invalid = copy.deepcopy(document)
        invalid["task"]["object"] = "synthetic sample"
        with self.assertRaises(ContractError):
            parse_task_candidate(invalid)


if __name__ == "__main__":
    unittest.main()
