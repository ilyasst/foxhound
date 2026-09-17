import unittest

from foxhound.workflow_decisions import (
    DecisionRequest, DecisionResponse, FinalOutcome, WorkflowDecisionError,
)
from foxhound.forge_action import (
    IssueCommentReceipt,
    PullRequestReceipt,
    ReviewReceipt,
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

    def test_final_outcomes_accept_every_forge_adapter_receipt(self):
        receipts = (
            PullRequestReceipt(
                "example.com/ExampleOrg/ProjectAlpha", "7", 8,
                "https://example.com/ExampleOrg/ProjectAlpha/pull/8",
                "foxhound/issue-7", "main",
            ),
            ReviewReceipt(
                "example.com/ExampleOrg/ProjectAlpha", 8,
                "https://example.com/ExampleOrg/ProjectAlpha/pull/8",
            ),
            IssueCommentReceipt(
                "example.com/ExampleOrg/ProjectAlpha", 7,
                "https://example.com/ExampleOrg/ProjectAlpha/issues/7",
            ),
        )
        outcome = FinalOutcome(
            1, 2, "a" * 64, "completed",
            tuple(receipt.url for receipt in receipts),
        )
        self.assertEqual(outcome.receipt_ids, tuple(receipt.url for receipt in receipts))
        with self.assertRaises(WorkflowDecisionError):
            FinalOutcome(1, 2, "a" * 64, "completed", ("http://example.com/7",))
