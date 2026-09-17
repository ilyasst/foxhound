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
                }, "freshness": "none", "freshness_kinds": [], "effects": [],
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
            }, "freshness": "before_effect", "freshness_kinds": [], "effects": ["forge"],
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
                }, "freshness": "none", "freshness_kinds": [], "effects": [],
                "final_decision": True,
            })

    def test_freshness_applies_only_to_the_kinds_it_names(self):
        """A mode without kinds checks nothing; kinds decide where it lands."""
        policy = parse_workflow_policy({
            "policy_id": "repo-auto", "grants": {
                "plan": [], "execute": [], "external_action": [],
            }, "freshness": "before_effect", "freshness_kinds": ["issue"],
            "effects": [], "final_decision": True,
        })
        self.assertTrue(policy.checks_freshness("phase", "issue"))
        self.assertTrue(policy.checks_freshness("effect", "issue"))
        self.assertFalse(policy.checks_freshness("effect", "email"))

        phase_only = parse_workflow_policy({
            "policy_id": "repo-auto", "grants": {
                "plan": [], "execute": [], "external_action": [],
            }, "freshness": "before_phase", "freshness_kinds": ["issue"],
            "effects": [], "final_decision": True,
        })
        self.assertTrue(phase_only.checks_freshness("phase", "issue"))
        self.assertFalse(phase_only.checks_freshness("effect", "issue"))

    def test_the_compatibility_policy_checks_nothing(self):
        policy = policy_from_legacy(plan=["issue"], execute=["issue"])
        self.assertFalse(policy.checks_freshness("phase", "issue"))
        self.assertFalse(policy.checks_freshness("effect", "issue"))

    def test_naming_kinds_under_a_mode_that_checks_nothing_is_refused(self):
        with self.assertRaises(WorkflowPolicyError):
            parse_workflow_policy({
                "policy_id": "repo-auto", "grants": {
                    "plan": [], "execute": [], "external_action": [],
                }, "freshness": "none", "freshness_kinds": ["issue"],
                "effects": [], "final_decision": True,
            })

    def test_an_unknown_checkpoint_is_refused(self):
        policy = policy_from_legacy()
        with self.assertRaises(WorkflowPolicyError):
            policy.checks_freshness("whenever", "issue")
