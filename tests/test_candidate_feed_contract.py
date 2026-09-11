from __future__ import annotations

import copy
import json
import unittest
from pathlib import Path

from foxhound.contracts import FeedContractError, parse_candidate_feed


FIXTURES = Path(__file__).parent / "fixtures" / "contracts"


def fixture() -> dict:
    return json.loads(
        (FIXTURES / "candidate-feed-page-v1.json").read_text(
            encoding="utf-8")
    )


class CandidateFeedContractTests(unittest.TestCase):
    def test_valid_contiguous_page_is_accepted(self):
        feed = parse_candidate_feed(fixture())

        self.assertEqual(feed.producer, "gw")
        self.assertEqual(feed.stream_id, "primary")
        self.assertEqual((feed.from_cursor, feed.to_cursor), (0, 2))
        self.assertEqual([item.sequence for item in feed.items], [1, 2])
        self.assertEqual(
            [item.candidate.source.kind for item in feed.items],
            ["meeting", "email"],
        )

    def test_unknown_version_is_refused_without_echoing_value(self):
        document = fixture()
        document["schema_version"] = 999

        with self.assertRaises(FeedContractError) as raised:
            parse_candidate_feed(document)

        self.assertNotIn("999", str(raised.exception))

    def test_additional_feed_field_is_refused(self):
        document = fixture()
        document["private_hint"] = "must-not-be-echoed"

        with self.assertRaises(FeedContractError) as raised:
            parse_candidate_feed(document)

        self.assertNotIn("must-not-be-echoed", str(raised.exception))

    def test_nested_candidate_failure_is_content_free(self):
        document = fixture()
        document["items"][0]["candidate"]["task"]["text"] = " private "

        with self.assertRaises(FeedContractError) as raised:
            parse_candidate_feed(document)

        self.assertNotIn("private", str(raised.exception))

    def test_cursor_range_must_match_item_count(self):
        document = fixture()
        document["to_cursor"] = 3

        with self.assertRaisesRegex(FeedContractError, "item count"):
            parse_candidate_feed(document)

    def test_item_sequences_must_be_contiguous(self):
        document = fixture()
        document["items"][1]["sequence"] = 3

        with self.assertRaisesRegex(FeedContractError, "contiguous"):
            parse_candidate_feed(document)

    def test_boolean_cursor_is_refused(self):
        document = fixture()
        document["from_cursor"] = False

        with self.assertRaisesRegex(FeedContractError, "non-negative"):
            parse_candidate_feed(document)

    def test_page_size_is_bounded(self):
        document = fixture()
        item = document["items"][0]
        document["items"] = []
        for sequence in range(1, 502):
            copy_item = copy.deepcopy(item)
            copy_item["sequence"] = sequence
            document["items"].append(copy_item)
        document["to_cursor"] = 501

        with self.assertRaisesRegex(FeedContractError, "page limit"):
            parse_candidate_feed(document)

    def test_emitted_timestamp_requires_timezone(self):
        document = fixture()
        document["emitted_at"] = "2030-03-01T12:00:00"

        with self.assertRaisesRegex(FeedContractError, "timezone"):
            parse_candidate_feed(document)


if __name__ == "__main__":
    unittest.main()
