from __future__ import annotations

import copy
import unittest

from foxhound.contracts import (
    SourceSnapshotContractError,
    parse_source_snapshot_response,
    source_snapshot_request,
    source_snapshot_request_document,
    source_snapshot_response_document,
)
from foxhound.contracts.source_snapshot import (
    SourceRefreshResult,
    SourceSnapshot,
)


DIGEST = "a" * 64
CHANGED_DIGEST = "b" * 64


def request():
    return source_snapshot_request(
        system="gw",
        kind="issue",
        record_id="github.com/example-org/example-repo",
        item_id="17",
        expected_revision=DIGEST,
    )


def response(*, status="current", snapshot=None):
    value = source_snapshot_request_document(request())
    return {
        "schema": "foxhound.source-snapshot",
        "schema_version": 1,
        "ok": True,
        **{
            key: item for key, item in value.items()
            if key not in {"schema", "schema_version"}
        },
        "status": status,
        "snapshot": snapshot,
    }


def snapshot(*, revision=DIGEST, lifecycle="active", actionability="actionable"):
    return {
        "revision": revision,
        "observed_at": "2099-01-02T03:04:05+00:00",
        "lifecycle": lifecycle,
        "actionability": actionability,
    }


class SourceSnapshotContractTest(unittest.TestCase):
    def test_request_document_is_canonical(self):
        self.assertEqual(source_snapshot_request_document(request()), {
            "schema": "foxhound.source-snapshot-request",
            "schema_version": 1,
            "system": "gw",
            "kind": "issue",
            "record_id": "github.com/example-org/example-repo",
            "item_id": "17",
            "expected_revision": DIGEST,
        })

    def test_current_round_trip(self):
        parsed = parse_source_snapshot_response(
            response(snapshot=snapshot()), request())
        self.assertTrue(parsed.usable)
        self.assertEqual(parsed.snapshot, SourceSnapshot(
            locator=request().locator,
            revision=DIGEST,
            observed_at="2099-01-02T03:04:05+00:00",
            lifecycle="active",
            actionability="actionable",
        ))
        self.assertEqual(
            source_snapshot_response_document(parsed),
            response(snapshot=snapshot()),
        )

    def test_changed_and_withdrawn_are_not_usable(self):
        changed = parse_source_snapshot_response(
            response(status="changed", snapshot=snapshot(revision=CHANGED_DIGEST)),
            request(),
        )
        withdrawn = parse_source_snapshot_response(
            response(status="withdrawn", snapshot=snapshot(
                lifecycle="withdrawn", actionability="not_actionable",
            )),
            request(),
        )
        self.assertFalse(changed.usable)
        self.assertFalse(withdrawn.usable)

    def test_unavailable_and_unsupported_do_not_carry_snapshot(self):
        for status in ("unavailable", "unsupported"):
            parsed = parse_source_snapshot_response(
                response(status=status), request())
            self.assertIsNone(parsed.snapshot)
            self.assertFalse(parsed.usable)

    def test_response_identity_and_shape_fail_closed(self):
        cases = []
        mismatched = response(snapshot=snapshot())
        mismatched["item_id"] = "18"
        cases.append(mismatched)
        unknown = response(snapshot=snapshot())
        unknown["extra"] = "no"
        cases.append(unknown)
        boolean_version = response(snapshot=snapshot())
        boolean_version["schema_version"] = True
        cases.append(boolean_version)
        stale_current = response(snapshot=snapshot(revision=CHANGED_DIGEST))
        cases.append(stale_current)
        unchanged = response(status="changed", snapshot=snapshot())
        cases.append(unchanged)
        missing = response(status="unavailable", snapshot=snapshot())
        cases.append(missing)
        naive = response(snapshot=snapshot())
        naive["snapshot"]["observed_at"] = "2099-01-02T03:04:05"
        cases.append(naive)
        for value in cases:
            with self.subTest(value=copy.deepcopy(value)):
                with self.assertRaises(SourceSnapshotContractError):
                    parse_source_snapshot_response(value, request())

    def test_result_document_rejects_wrong_locator(self):
        parsed = parse_source_snapshot_response(
            response(snapshot=snapshot()), request())
        wrong = SourceRefreshResult(
            request=parsed.request,
            status="current",
            snapshot=SourceSnapshot(
                locator=source_snapshot_request(
                    system="gw", kind="issue",
                    record_id="github.com/example-org/example-repo",
                    item_id="18", expected_revision=DIGEST,
                ).locator,
                revision=DIGEST,
                observed_at="2099-01-02T03:04:05+00:00",
                lifecycle="active",
                actionability="actionable",
            ),
        )
        with self.assertRaises(SourceSnapshotContractError):
            source_snapshot_response_document(wrong)


if __name__ == "__main__":
    unittest.main()
