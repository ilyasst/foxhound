"""Synthetic checks for revisioned workflow policy."""

import unittest

from foxhound.workflow_policy import (
    WorkflowPolicyError, parse_workflow_policy, policy_from_legacy,
)


class WorkflowPolicyTests(unittest.TestCase):
    def test_legacy_policy_preserves_independent_grants(self):
        policy = policy_from_legacy(plan=["issue"], execute=["issue"])
        self.assertTrue(policy.grants_stage("plan", "issue"))
        self.assertTrue(policy.grants_stage("execute", "issue"))
        self.assertFalse(policy.grants_stage("external_action", "issue"))
        self.assertEqual(len(policy.revision), 64)

    def test_one_policy_has_one_revision(self):
        """Grant order is not part of a policy's identity."""
        def policy(plan):
            return parse_workflow_policy({
                "policy_id": "repository-auto", "grants": {
                    "plan": plan, "execute": [], "external_action": [],
                }, "freshness": "none", "effects": [],
                "final_decision": True,
            })

        first = policy(["issue", "email"])
        reordered = policy(["email", "issue"])
        repeated = policy(["issue", "email", "issue"])
        changed = policy(["issue"])

        self.assertEqual(first.grants, reordered.grants)
        self.assertEqual(first.revision, reordered.revision)
        self.assertEqual(first.revision, repeated.revision)
        self.assertNotEqual(first.revision, changed.revision)

    def test_explicit_policy_is_closed_and_revisioned(self):
        policy = parse_workflow_policy({
            "policy_id": "repository-auto", "grants": {
                "plan": ["issue"], "execute": ["issue"],
                "external_action": [],
            }, "freshness": "before_effect", "effects": ["forge"],
            "final_decision": True,
        })
        self.assertEqual(policy.freshness, "before_effect")
        self.assertEqual(policy.effects, frozenset({"forge"}))
        with self.assertRaises(WorkflowPolicyError):
            parse_workflow_policy({"policy_id": "bad"})
        with self.assertRaises(WorkflowPolicyError):
            parse_workflow_policy({
                "policy_id": " policy ", "grants": {
                    "plan": [], "execute": [], "external_action": [],
                }, "freshness": "none", "effects": [],
                "final_decision": True,
            })
