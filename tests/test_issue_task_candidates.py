"""A forge issue nominated for work, arriving as an ordinary task candidate.

Issues enter through the same contract meetings and emails use, so they
inherit the inbox's idempotency, the task ledger, the reader gate and the
execution workflow rather than needing a second path into the system.
"""
from __future__ import annotations

import copy
import json
import unittest
from pathlib import Path

from foxhound.contracts.task_candidate import (
    ContractError,
    candidate_id_for,
    parse_task_candidate,
)

FIXTURES = Path(__file__).parent / "fixtures" / "contracts"


def fixture() -> dict:
    return json.loads((FIXTURES / "issue-candidate-v2.json").read_text())


class IssueCandidateContract(unittest.TestCase):
    def test_an_issue_candidate_parses(self) -> None:
        candidate = parse_task_candidate(fixture())
        self.assertEqual(candidate.source.kind, "issue")
        self.assertEqual(candidate.source.record_id, "forge.example/acme/widget")
        self.assertEqual(candidate.source.item_id, "42")
        self.assertEqual(candidate.task.text, "Add a README")

    def test_identity_is_the_repository_and_issue_number(self) -> None:
        # The inbox's uniqueness constraint is
        # (system, kind, record_id, item_id), so this is what makes one issue
        # exactly one task.
        doc = fixture()
        self.assertEqual(
            doc["candidate_id"],
            candidate_id_for(system="gw", kind="issue",
                             record_id="forge.example/acme/widget",
                             item_id="42"),
        )

    def test_editing_an_issue_addresses_the_same_candidate(self) -> None:
        # The revision is deliberately not part of the identity: an issue that
        # is retitled or edited must update its task, never mint a second one.
        doc = fixture()
        edited = copy.deepcopy(doc)
        edited["source"]["revision"] = "a" * 64
        edited["task"]["text"] = "Add a README, with a usage section"
        self.assertEqual(
            parse_task_candidate(edited).candidate_id,
            parse_task_candidate(doc).candidate_id,
        )

    def test_a_different_issue_is_a_different_candidate(self) -> None:
        doc = copy.deepcopy(fixture())
        doc["source"]["item_id"] = "43"
        doc["candidate_id"] = candidate_id_for(
            system="gw", kind="issue",
            record_id="forge.example/acme/widget", item_id="43")
        self.assertNotEqual(parse_task_candidate(doc).candidate_id,
                            fixture()["candidate_id"])

    def test_the_same_number_in_another_repository_is_distinct(self) -> None:
        doc = copy.deepcopy(fixture())
        doc["source"]["record_id"] = "forge.example/acme/other"
        doc["candidate_id"] = candidate_id_for(
            system="gw", kind="issue",
            record_id="forge.example/acme/other", item_id="42")
        self.assertNotEqual(parse_task_candidate(doc).candidate_id,
                            fixture()["candidate_id"])


class IdentifierShape(unittest.TestCase):
    """`/` is now accepted because forge identifiers are path-shaped."""

    def test_a_slash_bearing_locator_is_accepted(self) -> None:
        doc = copy.deepcopy(fixture())
        doc["evidence"]["locator"] = "forge.example/acme/widget/issues/42"
        self.assertEqual(
            parse_task_candidate(doc).evidence.locator,
            "forge.example/acme/widget/issues/42",
        )

    def test_widening_did_not_admit_whitespace(self) -> None:
        doc = copy.deepcopy(fixture())
        doc["evidence"]["locator"] = "forge.example/acme/a widget"
        with self.assertRaises(ContractError):
            parse_task_candidate(doc)

    def test_an_identifier_may_not_begin_with_a_separator(self) -> None:
        doc = copy.deepcopy(fixture())
        doc["evidence"]["locator"] = "/acme/widget"
        with self.assertRaises(ContractError):
            parse_task_candidate(doc)

    def test_the_previous_kinds_still_parse(self) -> None:
        # Widening only ever accepts more; nothing valid before may break.
        for name in ("meeting-candidate-v1.json", "meeting-candidate-v2.json",
                     "email-candidate-v1.json"):
            with self.subTest(name=name):
                parse_task_candidate(json.loads((FIXTURES / name).read_text()))

    def test_an_unknown_kind_is_still_refused(self) -> None:
        doc = copy.deepcopy(fixture())
        doc["source"]["kind"] = "pull_request"
        doc["candidate_id"] = candidate_id_for(
            system="gw", kind="pull_request",
            record_id="forge.example/acme/widget", item_id="42")
        with self.assertRaises(ContractError):
            parse_task_candidate(doc)


if __name__ == "__main__":
    unittest.main()
