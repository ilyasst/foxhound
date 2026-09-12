from __future__ import annotations

import copy
import unittest

from foxhound.contracts import (
    LifecycleOutcomeContractError,
    LifecycleOutcomeFeedContractError,
    parse_task_lifecycle_outcome,
    parse_task_lifecycle_outcome_feed,
)


def outcome() -> dict:
    return {
        "schema": "foxhound.task-lifecycle-outcome",
        "schema_version": 1,
        "event_sequence": 7,
        "task_id": 3,
        "task_version": 2,
        "correlation": {"system": "gw", "task_id": 91},
        "from_status": "open",
        "to_status": "done",
        "occurred_at": "2030-01-02T03:04:05+00:00",
    }


def feed() -> dict:
    return {
        "schema": "foxhound.task-lifecycle-outcome-feed",
        "schema_version": 1,
        "producer": "foxhound",
        "stream_id": "pilot-alpha",
        "from_cursor": 0,
        "to_cursor": 1,
        "items": [{"sequence": 1, "outcome": outcome()}],
        "emitted_at": "2030-01-02T03:05:00+00:00",
    }


class LifecycleOutcomeContractTests(unittest.TestCase):
    def test_valid_outcome_has_no_task_content(self):
        parsed = parse_task_lifecycle_outcome(outcome())
        self.assertEqual(parsed.task_version, 2)
        self.assertEqual(parsed.correlation.task_id, 91)
        self.assertFalse({"text", "owner", "project"} & set(outcome()))

    def test_unknown_fields_and_invalid_transition_are_refused(self):
        extra = outcome()
        extra["text"] = "Synthetic content must not cross this boundary"
        with self.assertRaises(LifecycleOutcomeContractError):
            parse_task_lifecycle_outcome(extra)
        invalid = outcome()
        invalid["from_status"] = "done"
        with self.assertRaises(LifecycleOutcomeContractError):
            parse_task_lifecycle_outcome(invalid)

    def test_boolean_integer_naive_time_and_version_one_are_refused(self):
        for field, value in (
            ("event_sequence", True),
            ("task_id", 0),
            ("task_version", 1),
            ("occurred_at", "2030-01-02T03:04:05"),
        ):
            document = outcome()
            document[field] = value
            with self.subTest(field=field):
                with self.assertRaises(LifecycleOutcomeContractError):
                    parse_task_lifecycle_outcome(document)

    def test_feed_requires_contiguous_delivery_order(self):
        parsed = parse_task_lifecycle_outcome_feed(feed())
        self.assertEqual(parsed.to_cursor, 1)
        bad = feed()
        bad["items"][0]["sequence"] = 2
        with self.assertRaises(LifecycleOutcomeFeedContractError):
            parse_task_lifecycle_outcome_feed(bad)

    def test_feed_requires_increasing_event_provenance(self):
        document = feed()
        second = copy.deepcopy(outcome())
        second["task_version"] = 3
        second["from_status"] = "done"
        second["to_status"] = "open"
        document["to_cursor"] = 2
        document["items"].append({"sequence": 2, "outcome": second})
        with self.assertRaises(LifecycleOutcomeFeedContractError):
            parse_task_lifecycle_outcome_feed(document)


if __name__ == "__main__":
    unittest.main()
