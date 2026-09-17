import unittest

from foxhound.effect_intents import EffectIntent, EffectIntentError, EffectReceipt


class EffectIntentTests(unittest.TestCase):
    def test_target_bound_intent_and_receipt_are_strict(self):
        intent = EffectIntent("forge-comment", 1, 2, "forge", "github.com/acme/widget/issues/7", "a" * 64, "b" * 64, True)
        self.assertTrue(intent.freshness_required)
        self.assertEqual(EffectReceipt("forge-comment", "completed", "receipt-1", False).state, "completed")
        with self.assertRaises(EffectIntentError):
            EffectIntent("bad", 0, 2, "forge", "x", "a" * 64, "b" * 64, True)
