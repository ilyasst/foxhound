"""Strict ordered-page contract for task lifecycle outcomes."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Mapping

from .task_lifecycle_outcome import (
    LifecycleOutcomeContractError,
    TaskLifecycleOutcome,
    parse_task_lifecycle_outcome,
    task_lifecycle_outcome_document,
)


SCHEMA_ID = "foxhound.task-lifecycle-outcome-feed"
SCHEMA_VERSION = 1
PRODUCER = "foxhound"
MAX_FEED_ITEMS = 500
MAX_CURSOR = 9_223_372_036_854_775_807
_STREAM_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,199}$")


class LifecycleOutcomeFeedContractError(ValueError):
    """A lifecycle outcome feed does not satisfy the supported contract."""


@dataclass(frozen=True)
class TaskLifecycleOutcomeFeedItem:
    sequence: int
    outcome: TaskLifecycleOutcome


@dataclass(frozen=True)
class TaskLifecycleOutcomeFeed:
    stream_id: str
    from_cursor: int
    to_cursor: int
    items: tuple[TaskLifecycleOutcomeFeedItem, ...]
    emitted_at: str
    producer: str = PRODUCER
    schema: str = SCHEMA_ID
    schema_version: int = SCHEMA_VERSION


def parse_task_lifecycle_outcome_feed(
    document: object,
) -> TaskLifecycleOutcomeFeed:
    root = _object(document, "feed")
    _exact_fields(root, "feed", {
        "schema", "schema_version", "producer", "stream_id", "from_cursor",
        "to_cursor", "items", "emitted_at",
    })
    version = root["schema_version"]
    if (root["schema"] != SCHEMA_ID or isinstance(version, bool)
            or version != SCHEMA_VERSION):
        raise LifecycleOutcomeFeedContractError("feed schema is unsupported")
    if root["producer"] != PRODUCER:
        raise LifecycleOutcomeFeedContractError("feed producer is unsupported")
    stream_id = _text(root["stream_id"], "feed.stream_id")
    if not _STREAM_ID_RE.fullmatch(stream_id):
        raise LifecycleOutcomeFeedContractError(
            "feed.stream_id has invalid format"
        )
    from_cursor = _cursor(root["from_cursor"], "feed.from_cursor")
    to_cursor = _cursor(root["to_cursor"], "feed.to_cursor")
    raw_items = root["items"]
    if not isinstance(raw_items, list) or len(raw_items) > MAX_FEED_ITEMS:
        raise LifecycleOutcomeFeedContractError("feed.items has invalid size")
    if to_cursor - from_cursor != len(raw_items):
        raise LifecycleOutcomeFeedContractError(
            "feed cursor range does not match item count"
        )
    items = []
    previous_event_sequence = 0
    for offset, raw_item in enumerate(raw_items, start=1):
        item = _object(raw_item, "feed.items entry")
        _exact_fields(item, "feed.items entry", {"sequence", "outcome"})
        sequence = _cursor(item["sequence"], "feed.items sequence")
        if sequence != from_cursor + offset:
            raise LifecycleOutcomeFeedContractError(
                "feed.items sequences are not contiguous"
            )
        try:
            outcome = parse_task_lifecycle_outcome(item["outcome"])
        except LifecycleOutcomeContractError as exc:
            raise LifecycleOutcomeFeedContractError(
                "feed.items outcome is invalid"
            ) from exc
        if outcome.event_sequence <= previous_event_sequence:
            raise LifecycleOutcomeFeedContractError(
                "feed outcome event sequences are not increasing"
            )
        previous_event_sequence = outcome.event_sequence
        items.append(TaskLifecycleOutcomeFeedItem(sequence, outcome))
    return TaskLifecycleOutcomeFeed(
        stream_id=stream_id,
        from_cursor=from_cursor,
        to_cursor=to_cursor,
        items=tuple(items),
        emitted_at=_timestamp(root["emitted_at"], "feed.emitted_at"),
    )


def task_lifecycle_outcome_feed_document(
    feed: TaskLifecycleOutcomeFeed,
) -> dict[str, Any]:
    return {
        "schema": feed.schema,
        "schema_version": feed.schema_version,
        "producer": feed.producer,
        "stream_id": feed.stream_id,
        "from_cursor": feed.from_cursor,
        "to_cursor": feed.to_cursor,
        "items": [{
            "sequence": item.sequence,
            "outcome": task_lifecycle_outcome_document(item.outcome),
        } for item in feed.items],
        "emitted_at": feed.emitted_at,
    }


def _object(value: object, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise LifecycleOutcomeFeedContractError(f"{field} must be an object")
    return value


def _exact_fields(
    value: Mapping[str, Any], field: str, expected: set[str]
) -> None:
    if expected - set(value):
        raise LifecycleOutcomeFeedContractError(
            f"{field} is missing required fields"
        )
    if set(value) - expected:
        raise LifecycleOutcomeFeedContractError(
            f"{field} contains additional fields"
        )


def _text(value: object, field: str, maximum: int = 200) -> str:
    if not isinstance(value, str):
        raise LifecycleOutcomeFeedContractError(f"{field} must be a string")
    if value != value.strip() or not value or len(value) > maximum:
        raise LifecycleOutcomeFeedContractError(
            f"{field} has invalid length or whitespace"
        )
    if any(ord(char) < 32 or ord(char) == 127 for char in value):
        raise LifecycleOutcomeFeedContractError(
            f"{field} contains control characters"
        )
    return value


def _cursor(value: object, field: str) -> int:
    if (isinstance(value, bool) or not isinstance(value, int)
            or not 0 <= value <= MAX_CURSOR):
        raise LifecycleOutcomeFeedContractError(
            f"{field} must be a non-negative integer"
        )
    return value


def _timestamp(value: object, field: str) -> str:
    text = _text(value, field, 64)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise LifecycleOutcomeFeedContractError(
            f"{field} must be an ISO-8601 timestamp"
        ) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise LifecycleOutcomeFeedContractError(
            f"{field} must include a timezone"
        )
    return text
