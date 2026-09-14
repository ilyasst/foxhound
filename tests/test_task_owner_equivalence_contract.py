from __future__ import annotations

import copy
import unittest

from foxhound.contracts import (
    OwnerEquivalenceContractError,
    PEOPLE_DIRECTORY_BASIS,
    TaskOwnerEquivalence,
    owner_equivalence_request,
    owner_equivalence_request_document,
    parse_owner_equivalence_response,
    task_owner_equivalence_document,
)


def request():
    return owner_equivalence_request(
        alias="primary",
        candidate_id="tc_" + "a" * 64,
        source_revision="b" * 64,
        legacy_task_id=101,
        legacy_digest="c" * 64,
    )


def response() -> dict:
    return {
        "schema": "gw.task-owner-equivalence",
        "schema_version": 1,
        "ok": True,
        "alias": "primary",
        "candidate_id": "tc_" + "a" * 64,
        "source_revision": "b" * 64,
        "legacy_task_id": 101,
        "legacy_digest": "c" * 64,
        "status": "equivalent",
        "basis": "speaker_merge",
        "effective_owner": "Person B (SPK_002)",
    }


class TaskOwnerEquivalenceContractTests(unittest.TestCase):
    def test_request_and_response_round_trip(self):
        expected_request = request()
        self.assertEqual(owner_equivalence_request_document(expected_request), {
            "schema": "gw.task-owner-equivalence-request",
            "schema_version": 1,
            "alias": "primary",
            "candidate_id": "tc_" + "a" * 64,
            "source_revision": "b" * 64,
            "legacy_task_id": 101,
            "legacy_digest": "c" * 64,
        })
        parsed = parse_owner_equivalence_response(response(), expected_request)
        self.assertTrue(parsed.equivalent)
        self.assertEqual(parsed.effective_owner, "Person B (SPK_002)")
        self.assertEqual(task_owner_equivalence_document(parsed), response())

        directory_response = response()
        directory_response["basis"] = PEOPLE_DIRECTORY_BASIS
        self.assertEqual(
            parse_owner_equivalence_response(
                directory_response, expected_request
            ).basis,
            PEOPLE_DIRECTORY_BASIS,
        )

        unresolved = TaskOwnerEquivalence(
            request=expected_request,
            status="unresolved",
            basis=None,
            effective_owner=None,
        )
        parsed_unresolved = parse_owner_equivalence_response(
            task_owner_equivalence_document(unresolved), expected_request
        )
        self.assertFalse(parsed_unresolved.equivalent)

    def test_request_rejects_invalid_identity_fields(self):
        changes = (
            {"alias": "bad alias"},
            {"candidate_id": "tc_short"},
            {"source_revision": "b" * 63},
            {"legacy_task_id": True},
            {"legacy_digest": "not-a-digest"},
        )
        base = {
            "alias": "primary",
            "candidate_id": "tc_" + "a" * 64,
            "source_revision": "b" * 64,
            "legacy_task_id": 101,
            "legacy_digest": "c" * 64,
        }
        for change in changes:
            with self.subTest(change=change):
                with self.assertRaises(OwnerEquivalenceContractError):
                    owner_equivalence_request(**{**base, **change})

    def test_response_rejects_wrong_identity_shape_and_equivalence(self):
        mutations = []
        for field, value in (
            ("schema", "unknown"),
            ("schema_version", True),
            ("ok", False),
            ("candidate_id", "tc_" + "d" * 64),
            ("source_revision", "d" * 64),
            ("legacy_task_id", 102),
            ("legacy_digest", "d" * 64),
            ("status", "accepted"),
            ("basis", "text_match"),
            ("effective_owner", None),
        ):
            def mutate(document, field=field, value=value):
                changed = copy.deepcopy(document)
                changed[field] = value
                return changed
            mutations.append(mutate)
        mutations.append(lambda document: {**document, "extra": "field"})
        for mutation in mutations:
            with self.subTest(mutation=mutation):
                with self.assertRaises(OwnerEquivalenceContractError):
                    parse_owner_equivalence_response(
                        mutation(response()), request()
                    )


if __name__ == "__main__":
    unittest.main()
