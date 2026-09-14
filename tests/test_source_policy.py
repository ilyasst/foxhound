from __future__ import annotations

import unittest

from foxhound.source_policy import SOURCE_POLICIES, source_kinds_accepting
from foxhound.task_execution import WorkflowStatus, _initial_status


class SourcePolicyTests(unittest.TestCase):
    def test_teams_is_explicit_at_each_candidate_gate(self):
        policy = SOURCE_POLICIES["teams"]

        self.assertTrue(policy.accepts_candidates)
        self.assertTrue(policy.accepts_shadow_observations)
        self.assertTrue(policy.accepts_native_intake)

    def test_teams_does_not_pre_authorize_agent_planning(self):
        self.assertNotIn(
            "teams", source_kinds_accepting("pre_authorized_planning")
        )
        self.assertEqual(
            source_kinds_accepting("pre_authorized_planning"), {"issue"}
        )
        self.assertEqual(
            _initial_status("teams"), WorkflowStatus.AWAITING_START
        )

    def test_unknown_capability_fails_closed(self):
        with self.assertRaisesRegex(ValueError, "unknown source-policy"):
            source_kinds_accepting("unknown")


if __name__ == "__main__":
    unittest.main()
