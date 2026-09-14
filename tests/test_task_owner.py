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


if __name__ == "__main__":
    unittest.main()
