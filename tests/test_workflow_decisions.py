import unittest

from foxhound.workflow_decisions import (
    DecisionRequest, DecisionResponse, FinalOutcome, WorkflowDecisionError,
)


class WorkflowDecisionTests(unittest.TestCase):
    def test_decision_is_version_fenced_and_transport_independent(self):
        request = DecisionRequest("send-draft", 1, 2, 3, "a" * 64, "send", frozenset({"approve", "revise", "discard"}))
        self.assertEqual(DecisionResponse(request.decision_id, 3, "approve").response, "approve")
        self.assertEqual(FinalOutcome(1, 2, "a" * 64, "completed", ("receipt-1",)).disposition, "completed")
        with self.assertRaises(WorkflowDecisionError):
            DecisionResponse("send-draft", 0, "approve")

    def test_identities_and_the_policy_revision_are_strict(self):
        """bool is an int, and a 64-character string is not a digest."""
        with self.assertRaises(WorkflowDecisionError):
            DecisionRequest(
                "send-draft", True, 2, 3, "a" * 64, "send",
                frozenset({"approve"}),
            )
        with self.assertRaises(WorkflowDecisionError):
            DecisionRequest(
                "send-draft", 1, 2, 3, "z" * 64, "send",
                frozenset({"approve"}),
            )
        with self.assertRaises(WorkflowDecisionError):
            FinalOutcome(1, 2, "Z" * 64, "completed")
        with self.assertRaises(WorkflowDecisionError):
            DecisionResponse("send-draft", True, "approve")
