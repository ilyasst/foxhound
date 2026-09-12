from __future__ import annotations

import copy
import json
import unittest
from pathlib import Path

from foxhound.contracts import (
    ShadowFeedContractError,
    parse_task_shadow_feed,
    task_shadow_feed_document,
)


FIXTURES = Path(__file__).parent / "fixtures" / "contracts"


def fixture() -> dict:
    path = FIXTURES / "task-shadow-observation-feed-page-v1.json"
    return json.loads(path.read_text(encoding="utf-8"))


class TaskShadowFeedContractTests(unittest.TestCase):
    def test_accepts_bounded_contiguous_synthetic_page(self):
        feed = parse_task_shadow_feed(fixture())

        self.assertEqual((feed.from_cursor, feed.to_cursor), (0, 2))
        self.assertEqual([item.sequence for item in feed.items], [1, 2])
        self.assertEqual(
            [item.observation.disposition for item in feed.items],
            ["minted", "unmapped"],
        )

    def test_round_trip_preserves_canonical_document(self):
        document = fixture()
        self.assertEqual(
            task_shadow_feed_document(parse_task_shadow_feed(document)),
            document,
        )

    def test_empty_page_is_valid_at_one_cursor(self):
        document = fixture()
        document["to_cursor"] = 0
        document["items"] = []
        self.assertEqual(parse_task_shadow_feed(document).items, ())

    def test_rejects_cursor_gaps_ranges_and_oversized_pages(self):
        gap = fixture()
        gap["items"][1]["sequence"] = 3
        with self.assertRaisesRegex(ShadowFeedContractError, "contiguous"):
            parse_task_shadow_feed(gap)

        mismatch = fixture()
        mismatch["to_cursor"] = 3
        with self.assertRaisesRegex(ShadowFeedContractError, "item count"):
            parse_task_shadow_feed(mismatch)

        oversized = fixture()
        item = oversized["items"][0]
        oversized["items"] = [copy.deepcopy(item) for _ in range(501)]
        oversized["to_cursor"] = 501
        for sequence, entry in enumerate(oversized["items"], start=1):
            entry["sequence"] = sequence
        with self.assertRaisesRegex(ShadowFeedContractError, "page limit"):
            parse_task_shadow_feed(oversized)

    def test_rejects_invalid_observation_source_and_extra_fields(self):
        invalid = fixture()
        invalid["items"][0]["observation"]["disposition"] = "completed"
        with self.assertRaisesRegex(ShadowFeedContractError, "invalid"):
            parse_task_shadow_feed(invalid)

        source = fixture()
        source["items"][0]["observation"]["candidate"]["source"][
            "system"
        ] = "other"
        with self.assertRaises(ShadowFeedContractError):
            parse_task_shadow_feed(source)

        additional = fixture()
        additional["private_context"] = "synthetic secret"
        with self.assertRaisesRegex(
            ShadowFeedContractError, "additional fields"
        ) as raised:
            parse_task_shadow_feed(additional)
        self.assertNotIn("synthetic secret", str(raised.exception))

    def test_schema_is_strict_at_new_object_boundaries(self):
        path = (
            Path(__file__).parents[1]
            / "src" / "foxhound" / "contracts" / "schemas"
            / "task-shadow-observation-feed-v1.schema.json"
        )
        schema = json.loads(path.read_text(encoding="utf-8"))
        self.assertFalse(schema["additionalProperties"])
        self.assertFalse(schema["properties"]["items"]["items"][
            "additionalProperties"
        ])


if __name__ == "__main__":
    unittest.main()
