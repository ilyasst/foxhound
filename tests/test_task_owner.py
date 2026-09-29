from __future__ import annotations

import unittest

from foxhound.task_owner import canonical_owner_display


class TaskOwnerDisplayTests(unittest.TestCase):
    def test_structured_unresolved_owner_is_neutral(self):
        self.assertEqual(
            canonical_owner_display("private source label", "unresolved"),
            "(unassigned)",
        )

    def test_legacy_display_never_exposes_a_speaker_identifier(self):
        self.assertEqual(
            canonical_owner_display("Person A (SPK_101)", None),
            "Person A",
        )
        self.assertEqual(
            canonical_owner_display("SPK_101", None),
            "(unassigned)",
        )


    def test_cluster_speaker_identifier_is_stripped_like_a_legacy_one(self):
        cases = {
            "Person A (CLU_000101)": "Person A",
            "CLU_000101": "(unassigned)",
            "Person A (CLU_000101/SPK_7)": "Person A",
            "Person A (SPK_7/CLU_000101)": "Person A",
            "Person A, CLU_000101": "Person A",
            # Not the grammar: left alone, exactly like any other text.
            "Person A CLU_1708": "Person A CLU_1708",
        }
        for owner, expected in cases.items():
            with self.subTest(owner=owner):
                self.assertEqual(canonical_owner_display(owner, None), expected)

    def test_legacy_speaker_tokens_are_unchanged(self):
        cases = {
            "Person A (SPK_1/2)": "Person A",
            "Person A (SPK_1/SPK_22)": "Person A",
            "SPK_1/2": "(unassigned)",
            "Person A SPK_101": "Person A",
            "XSPK_101": "XSPK_101",
        }
        for owner, expected in cases.items():
            with self.subTest(owner=owner):
                self.assertEqual(canonical_owner_display(owner, None), expected)


if __name__ == "__main__":
    unittest.main()
