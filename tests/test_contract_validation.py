import unittest

from foxhound.contracts.validation import (
    is_external_receipt_reference,
    is_positive_row_id,
)


class ContractValidationTests(unittest.TestCase):
    def test_receipts_accept_adapter_urls_and_opaque_ids(self) -> None:
        self.assertTrue(is_external_receipt_reference("receipt-1"))
        self.assertTrue(is_external_receipt_reference(
            "https://example.com/acme/widget/pull/42"))
        self.assertFalse(is_external_receipt_reference("http://example.com/42"))

    def test_a_boolean_is_not_a_database_identity(self) -> None:
        self.assertTrue(is_positive_row_id(1))
        self.assertFalse(is_positive_row_id(True))
