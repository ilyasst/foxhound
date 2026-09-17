import unittest

from foxhound.effect_intents import EffectIntent, EffectIntentError, EffectReceipt
from foxhound.forge_action import (
    IssueCommentReceipt,
    PullRequestReceipt,
    ReviewReceipt,
)


class EffectIntentTests(unittest.TestCase):
    def test_target_bound_intent_and_receipt_are_strict(self):
        intent = EffectIntent("forge-comment", 1, 2, "forge", "github.com/acme/widget/issues/7", "a" * 64, "b" * 64, True)
        self.assertTrue(intent.freshness_required)
        self.assertEqual(EffectReceipt("forge-comment", "completed", "receipt-1", False).state, "completed")
        with self.assertRaises(EffectIntentError):
            EffectIntent("bad", 0, 2, "forge", "x", "a" * 64, "b" * 64, True)

    def test_receipts_accept_every_forge_adapter_reference(self):
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
        for receipt in receipts:
            with self.subTest(receipt=type(receipt).__name__):
                self.assertEqual(
                    EffectReceipt("forge-comment", "completed", receipt.url, False)
                    .receipt_id,
                    receipt.url,
                )
        with self.assertRaises(EffectIntentError):
            EffectReceipt("forge-comment", "completed", "http://example.com/7", False)

    def test_a_boolean_is_not_a_row_identity(self):
        """bool subclasses int, so True must not pass as work item 1."""
        with self.assertRaises(EffectIntentError):
            EffectIntent(
                "forge-comment", True, 2, "forge", "x", "a" * 64, "b" * 64,
                True,
            )
        with self.assertRaises(EffectIntentError):
            EffectIntent(
                "forge-comment", 1, True, "forge", "x", "a" * 64, "b" * 64,
                True,
            )
