"""Strict, bounded contracts for checking a task origin before work.

The source-owning system answers a refresh request; Foxhound neither reads a
producer database nor receives a source body, thread, or mailbox export.  The
response says only whether the exact source identity still names the expected
revision and exposes two bounded source-owned state fields for later policy.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Mapping, Protocol

from .task_candidate import SOURCE_KINDS, SOURCE_SYSTEMS
from .validation import (
    is_bounded_text,
    is_opaque_identifier,
    is_sha256_digest,
)


REQUEST_SCHEMA = "foxhound.source-snapshot-request"
RESPONSE_SCHEMA = "foxhound.source-snapshot"
SCHEMA_VERSION = 1
REFRESH_STATUSES = frozenset({
    "current", "changed", "withdrawn", "unavailable", "unsupported",
})
LIFECYCLE_STATES = frozenset({"active", "withdrawn"})
ACTIONABILITY_STATES = frozenset({
    "actionable", "not_actionable", "unknown",
})

class SourceSnapshotContractError(ValueError):
    """A source-snapshot request or response is outside the contract."""


@dataclass(frozen=True)
class SourceLocator:
    """One stable source object, without its mutable revision."""

    system: str
    kind: str
    record_id: str
    item_id: str


@dataclass(frozen=True)
class SourceSnapshotRequest:
    """Ask whether one exact source object still has one expected revision."""

    locator: SourceLocator
    expected_revision: str


@dataclass(frozen=True)
class SourceSnapshot:
    """Bounded source state observed at one timezone-aware instant."""

    locator: SourceLocator
    revision: str
    observed_at: str
    lifecycle: str
    actionability: str


@dataclass(frozen=True)
class SourceRefreshResult:
    """The source-owned answer to one exact freshness request."""

    request: SourceSnapshotRequest
    status: str
    snapshot: SourceSnapshot | None

    @property
    def usable(self) -> bool:
        """Whether the result proves the expected revision is still current."""
        return self.status == "current"


class SourceSnapshotResolver(Protocol):
    """Narrow source-owned port used by a later freshness fence."""

    def refresh(self, request: SourceSnapshotRequest) -> SourceRefreshResult:
        """Return a bounded result for exactly the requested source identity."""


def source_locator(
    *, system: object, kind: object, record_id: object, item_id: object,
) -> SourceLocator:
    """Validate one source locator shared by a candidate and a refresh call."""
    return SourceLocator(
        system=_choice(system, "system", SOURCE_SYSTEMS),
        kind=_choice(kind, "kind", SOURCE_KINDS),
        record_id=_opaque_id(record_id, "record_id"),
        item_id=_opaque_id(item_id, "item_id"),
    )


def source_snapshot_request(
    *, system: object, kind: object, record_id: object, item_id: object,
    expected_revision: object,
) -> SourceSnapshotRequest:
    """Validate a source-refresh request before it reaches an adapter."""
    return SourceSnapshotRequest(
        locator=source_locator(
            system=system, kind=kind, record_id=record_id, item_id=item_id,
        ),
        expected_revision=_digest(expected_revision, "expected_revision"),
    )


def source_snapshot_request_document(
    request: SourceSnapshotRequest,
) -> dict[str, Any]:
    """Return the versioned wire document for one refresh request."""
    request = _request(request)
    locator = request.locator
    return {
        "schema": REQUEST_SCHEMA,
        "schema_version": SCHEMA_VERSION,
        "system": locator.system,
        "kind": locator.kind,
        "record_id": locator.record_id,
        "item_id": locator.item_id,
        "expected_revision": request.expected_revision,
    }


def parse_source_snapshot_response(
    value: object, request: SourceSnapshotRequest,
) -> SourceRefreshResult:
    """Parse a source-owned response without accepting arbitrary source data."""
    request = _request(request)
    root = _object(value, "source snapshot response")
    _exact_fields(root, {
        "schema", "schema_version", "ok", "system", "kind", "record_id",
        "item_id", "expected_revision", "status", "snapshot",
    })
    if (root["schema"] != RESPONSE_SCHEMA
            or isinstance(root["schema_version"], bool)
            or root["schema_version"] != SCHEMA_VERSION
            or root["ok"] is not True):
        raise SourceSnapshotContractError("source snapshot response is invalid")
    expected = source_snapshot_request_document(request)
    for field in (
        "system", "kind", "record_id", "item_id", "expected_revision",
    ):
        if root[field] != expected[field]:
            raise SourceSnapshotContractError(
                "source snapshot response identity is invalid")
    status = _choice(root["status"], "status", REFRESH_STATUSES)
    snapshot = _snapshot(root["snapshot"], request.locator)
    if status in {"current", "changed", "withdrawn"} and snapshot is None:
        raise SourceSnapshotContractError("source snapshot is required")
    if status in {"unavailable", "unsupported"} and snapshot is not None:
        raise SourceSnapshotContractError("source snapshot is inconsistent")
    if status == "current" and (
        snapshot is None
        or snapshot.revision != request.expected_revision
        or snapshot.lifecycle != "active"
    ):
        raise SourceSnapshotContractError("source snapshot is not current")
    if status == "changed" and (
        snapshot is None
        or snapshot.revision == request.expected_revision
        or snapshot.lifecycle != "active"
    ):
        raise SourceSnapshotContractError("source snapshot is not changed")
    if status == "withdrawn" and (
        snapshot is None or snapshot.lifecycle != "withdrawn"
    ):
        raise SourceSnapshotContractError("source snapshot is not withdrawn")
    return SourceRefreshResult(request, status, snapshot)


def source_snapshot_response_document(
    result: SourceRefreshResult,
) -> dict[str, Any]:
    """Return a canonical response and verify its state-specific invariants."""
    if not isinstance(result, SourceRefreshResult):
        raise SourceSnapshotContractError("source snapshot result is invalid")
    request = _request(result.request)
    status = _choice(result.status, "status", REFRESH_STATUSES)
    snapshot = result.snapshot
    snapshot_document = None
    if snapshot is not None:
        snapshot = _snapshot_document(snapshot, request.locator)
        snapshot_document = {
            "revision": snapshot.revision,
            "observed_at": snapshot.observed_at,
            "lifecycle": snapshot.lifecycle,
            "actionability": snapshot.actionability,
        }
    document = {
        "schema": RESPONSE_SCHEMA,
        "schema_version": SCHEMA_VERSION,
        "ok": True,
        **{
            field: value
            for field, value in source_snapshot_request_document(request).items()
            if field not in {"schema", "schema_version"}
        },
        "status": status,
        "snapshot": snapshot_document,
    }
    parsed = parse_source_snapshot_response(document, request)
    if parsed != result:
        raise SourceSnapshotContractError("source snapshot response is inconsistent")
    return document


def _request(value: object) -> SourceSnapshotRequest:
    if not isinstance(value, SourceSnapshotRequest):
        raise SourceSnapshotContractError("source snapshot request is invalid")
    locator = value.locator
    validated = source_snapshot_request(
        system=locator.system,
        kind=locator.kind,
        record_id=locator.record_id,
        item_id=locator.item_id,
        expected_revision=value.expected_revision,
    )
    return validated


def _snapshot(value: object, locator: SourceLocator) -> SourceSnapshot | None:
    if value is None:
        return None
    root = _object(value, "source snapshot")
    _exact_fields(root, {
        "revision", "observed_at", "lifecycle", "actionability",
    })
    return SourceSnapshot(
        locator=locator,
        revision=_digest(root["revision"], "snapshot revision"),
        observed_at=_timestamp(root["observed_at"], "snapshot observed_at"),
        lifecycle=_choice(root["lifecycle"], "snapshot lifecycle",
                          LIFECYCLE_STATES),
        actionability=_choice(root["actionability"], "snapshot actionability",
                              ACTIONABILITY_STATES),
    )


def _snapshot_document(
    value: object, locator: SourceLocator,
) -> SourceSnapshot:
    if not isinstance(value, SourceSnapshot):
        raise SourceSnapshotContractError("source snapshot is invalid")
    if value.locator != locator:
        raise SourceSnapshotContractError("source snapshot identity is invalid")
    return SourceSnapshot(
        locator=locator,
        revision=_digest(value.revision, "snapshot revision"),
        observed_at=_timestamp(value.observed_at, "snapshot observed_at"),
        lifecycle=_choice(value.lifecycle, "snapshot lifecycle",
                          LIFECYCLE_STATES),
        actionability=_choice(value.actionability, "snapshot actionability",
                              ACTIONABILITY_STATES),
    )


def _object(value: object, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise SourceSnapshotContractError(f"{field} must be an object")
    return value


def _exact_fields(value: Mapping[str, Any], expected: set[str]) -> None:
    if set(value) != expected:
        raise SourceSnapshotContractError("source snapshot fields are invalid")


def _text(value: object, field: str, *, maximum: int) -> str:
    if not is_bounded_text(value, maximum=maximum):
        raise SourceSnapshotContractError(f"{field} is invalid")
    return value


def _choice(value: object, field: str, choices: frozenset[str]) -> str:
    text = _text(value, field, maximum=200)
    if text not in choices:
        raise SourceSnapshotContractError(f"{field} is invalid")
    return text


def _opaque_id(value: object, field: str) -> str:
    text = _text(value, field, maximum=200)
    if not is_opaque_identifier(text):
        raise SourceSnapshotContractError(f"{field} is invalid")
    return text


def _digest(value: object, field: str) -> str:
    text = _text(value, field, maximum=64)
    if not is_sha256_digest(text):
        raise SourceSnapshotContractError(f"{field} is invalid")
    return text


def _timestamp(value: object, field: str) -> str:
    text = _text(value, field, maximum=64)
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        raise SourceSnapshotContractError(f"{field} is invalid") from None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise SourceSnapshotContractError(f"{field} is invalid")
    return text
