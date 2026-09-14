from __future__ import annotations

import copy
import hashlib
import json
import unittest
from pathlib import Path

from foxhound.contracts import (
    ShadowObservationError,
    candidate_comparable_digest,
    comparable_task_digest,
    parse_task_shadow_observation,
    parse_task_shadow_observation_json,
    task_shadow_observation_document,
)


FIXTURES = Path(__file__).parent / "fixtures" / "contracts"


def fixture(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


class TaskShadowObservationContractTests(unittest.TestCase):
    def test_accepts_mapped_synthetic_meeting_observation(self):
        observation = parse_task_shadow_observation(
            fixture("meeting-shadow-observation-v1.json")
        )
        self.assertEqual(observation.disposition, "minted")
        self.assertEqual(observation.legacy_task.task_id, 101)
        self.assertEqual(
            observation.legacy_task.comparable_digest,
            candidate_comparable_digest(observation.candidate),
        )

    def test_accepts_unmapped_synthetic_email_observation(self):
        observation = parse_task_shadow_observation(
            fixture("email-shadow-observation-v1.json")
        )
        self.assertEqual(observation.candidate.source.kind, "email")
        self.assertEqual(observation.disposition, "unmapped")
        self.assertIsNone(observation.legacy_task)

    def test_shadow_contract_preserves_teams_source_kind(self):
        document = fixture("meeting-shadow-observation-v1.json")
        document["candidate"] = fixture("teams-candidate-v2.json")
        document["legacy_task"]["comparable_digest"] = (
            candidate_comparable_digest(
                parse_task_shadow_observation({
                    **document,
                    "disposition": "unmapped",
                    "legacy_task": None,
                    "reason_code": "ambiguous_retrofit",
                }).candidate
            )
        )

        observation = parse_task_shadow_observation(document)

        self.assertEqual(observation.candidate.source.kind, "teams")
        self.assertEqual(
            task_shadow_observation_document(observation), document
        )

    def test_round_trip_preserves_the_canonical_document(self):
        document = fixture("meeting-shadow-observation-v1.json")
        observation = parse_task_shadow_observation(document)
        self.assertEqual(task_shadow_observation_document(observation), document)

    def test_comparable_digest_has_a_fixed_synthetic_vector(self):
        digest = comparable_task_digest(
            text="Prepare the Project Alpha summary",
            project="Project Alpha",
            owner="Person A",
        )
        self.assertEqual(
            digest,
            "733c8a5100c0e5dfc736b5c886a550b4ea0639a987596b8d24bfd77f4d91ccd3",
        )
        changed = comparable_task_digest(
            text="Prepare a revised Project Alpha summary",
            project="Project Alpha",
            owner="Person A",
        )
        self.assertNotEqual(changed, digest)

    def test_projectless_digest_has_a_distinct_fixed_shape(self):
        digest = comparable_task_digest(
            text="Prepare the synthetic summary",
            project=None,
            owner="Person A",
        )
        material = json.dumps(
            ["Prepare the synthetic summary", "Person A"],
            ensure_ascii=True,
            separators=(",", ":"),
        ).encode("utf-8")
        self.assertEqual(digest, hashlib.sha256(material).hexdigest())

        document = fixture("meeting-shadow-observation-v1.json")
        projectless = fixture("meeting-candidate-v2.json")
        document["candidate"] = projectless
        document["legacy_task"]["comparable_digest"] = digest
        observation = parse_task_shadow_observation(document)
        self.assertEqual(
            candidate_comparable_digest(observation.candidate), digest
        )

    def test_distinct_candidates_may_reference_one_legacy_task(self):
        meeting = fixture("meeting-shadow-observation-v1.json")
        email = fixture("email-shadow-observation-v1.json")
        email["disposition"] = "folded"
        email["legacy_task"] = copy.deepcopy(meeting["legacy_task"])
        email["reason_code"] = None

        first = parse_task_shadow_observation(meeting)
        second = parse_task_shadow_observation(email)

        self.assertNotEqual(
            first.candidate.candidate_id, second.candidate.candidate_id
        )
        self.assertEqual(
            first.legacy_task.task_id, second.legacy_task.task_id
        )

    def test_each_disposition_enforces_its_field_combination(self):
        mapped = fixture("meeting-shadow-observation-v1.json")
        for disposition in ("minted", "folded"):
            document = copy.deepcopy(mapped)
            document["disposition"] = disposition
            self.assertEqual(
                parse_task_shadow_observation(document).disposition,
                disposition,
            )

        refused = copy.deepcopy(mapped)
        refused["disposition"] = "refused"
        refused["legacy_task"] = None
        refused["reason_code"] = "identity_conflict"
        self.assertEqual(
            parse_task_shadow_observation(refused).disposition, "refused"
        )

        missing_task = copy.deepcopy(mapped)
        missing_task["legacy_task"] = None
        with self.assertRaisesRegex(ShadowObservationError, "legacy_task"):
            parse_task_shadow_observation(missing_task)

        reason_on_mapping = copy.deepcopy(mapped)
        reason_on_mapping["reason_code"] = "identity_conflict"
        with self.assertRaisesRegex(ShadowObservationError, "reason_code"):
            parse_task_shadow_observation(reason_on_mapping)

        task_on_refusal = copy.deepcopy(refused)
        task_on_refusal["legacy_task"] = copy.deepcopy(mapped["legacy_task"])
        with self.assertRaisesRegex(ShadowObservationError, "legacy_task"):
            parse_task_shadow_observation(task_on_refusal)

        wrong_reason = copy.deepcopy(refused)
        wrong_reason["reason_code"] = "legacy_identity_absent"
        with self.assertRaisesRegex(ShadowObservationError, "reason_code"):
            parse_task_shadow_observation(wrong_reason)

    def test_rejects_unknown_fields_versions_and_dispositions(self):
        for field, value in (
            ("schema_version", 2),
            ("disposition", "completed"),
        ):
            document = fixture("meeting-shadow-observation-v1.json")
            document[field] = value
            with self.assertRaises(ShadowObservationError):
                parse_task_shadow_observation(document)

        additional = fixture("meeting-shadow-observation-v1.json")
        additional["private_context"] = "synthetic secret"
        with self.assertRaisesRegex(
            ShadowObservationError, "additional fields"
        ) as raised:
            parse_task_shadow_observation(additional)
        self.assertNotIn("synthetic secret", str(raised.exception))

    def test_rejects_invalid_task_reference_digest_and_timestamp(self):
        for mutation, message in (
            (("task_id", True), "task_id"),
            (("task_id", 0), "task_id"),
            (("comparable_digest", "0" * 63), "comparable_digest"),
        ):
            document = fixture("meeting-shadow-observation-v1.json")
            document["legacy_task"][mutation[0]] = mutation[1]
            with self.assertRaisesRegex(ShadowObservationError, message):
                parse_task_shadow_observation(document)

        timestamp = fixture("meeting-shadow-observation-v1.json")
        timestamp["observed_at"] = "2030-01-01T12:01:00"
        with self.assertRaisesRegex(ShadowObservationError, "timezone"):
            parse_task_shadow_observation(timestamp)

    def test_json_parser_rejects_duplicate_fields_at_any_depth(self):
        text = json.dumps(fixture("meeting-shadow-observation-v1.json"))
        duplicate = text.replace(
            '"system": "gw"',
            '"system": "gw", "system": "gw"',
            1,
        )
        with self.assertRaisesRegex(ShadowObservationError, "JSON"):
            parse_task_shadow_observation_json(duplicate)

    def test_nested_candidate_errors_name_fields_without_echoing_values(self):
        document = fixture("meeting-shadow-observation-v1.json")
        document["candidate"]["source"]["record_id"] = "/private/value"
        with self.assertRaisesRegex(
            ShadowObservationError, "candidate.source.record_id"
        ) as raised:
            parse_task_shadow_observation(document)
        self.assertNotIn("/private/value", str(raised.exception))

    def test_schema_is_strict_at_each_new_object_boundary(self):
        schema_path = (
            Path(__file__).parents[1]
            / "src" / "foxhound" / "contracts" / "schemas"
            / "task-shadow-observation-v1.schema.json"
        )
        schema = json.loads(schema_path.read_text(encoding="utf-8"))
        self.assertFalse(schema["additionalProperties"])
        legacy = schema["properties"]["legacy_task"]["oneOf"][0]
        self.assertFalse(legacy["additionalProperties"])
        candidate_versions = schema["properties"]["candidate"]["oneOf"]
        self.assertEqual(
            [item["$ref"] for item in candidate_versions],
            [
                "task-candidate-v1.schema.json",
                "task-candidate-v2.schema.json",
                "task-candidate-v3.schema.json",
            ],
        )


if __name__ == "__main__":
    unittest.main()
