"""Strict ordered-page contract for passive task-shadow observations."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Mapping

from .task_shadow_observation import (
    ShadowObservationError,
    TaskShadowObservation,
    parse_task_shadow_observation,
    task_shadow_observation_document,
)


SCHEMA_ID = "foxhound.task-shadow-observation-feed"
SCHEMA_VERSION = 1
MAX_FEED_ITEMS = 500
MAX_CURSOR = 9_223_372_036_854_775_807
PRODUCERS = frozenset({"gw"})

_STREAM_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,199}$")


class ShadowFeedContractError(ValueError):
    """An observation feed page does not satisfy the supported contract."""


@dataclass(frozen=True)
class TaskShadowFeedItem:
    sequence: int
    observation: TaskShadowObservation


@dataclass(frozen=True)
class TaskShadowFeed:
    producer: str
    stream_id: str
    from_cursor: int
    to_cursor: int
    items: tuple[TaskShadowFeedItem, ...]
    emitted_at: str
    schema: str = SCHEMA_ID
    schema_version: int = SCHEMA_VERSION


def parse_task_shadow_feed(document: object) -> TaskShadowFeed:
    """Validate one bounded, contiguous shadow-observation feed page."""
    root = _object(document, "feed")
    _exact_fields(
        root,
        "feed",
        {
            "schema", "schema_version", "producer", "stream_id",
            "from_cursor", "to_cursor", "items", "emitted_at",
        },
    )
    if root["schema"] != SCHEMA_ID:
        raise ShadowFeedContractError("feed.schema is unsupported")
    version = root["schema_version"]
    if isinstance(version, bool) or version != SCHEMA_VERSION:
        raise ShadowFeedContractError("feed.schema_version is unsupported")

    producer = _choice(root["producer"], "feed.producer", PRODUCERS)
    stream_id = _stream_id(root["stream_id"])
    from_cursor = _cursor(root["from_cursor"], "feed.from_cursor")
    to_cursor = _cursor(root["to_cursor"], "feed.to_cursor")
    if to_cursor < from_cursor:
        raise ShadowFeedContractError("feed cursor range is invalid")

    raw_items = root["items"]
    if not isinstance(raw_items, list):
        raise ShadowFeedContractError("feed.items must be an array")
    if len(raw_items) > MAX_FEED_ITEMS:
        raise ShadowFeedContractError("feed.items exceeds the page limit")
    if len(raw_items) != to_cursor - from_cursor:
        raise ShadowFeedContractError(
            "feed cursor range does not match item count"
        )

    items = []
    for offset, raw_item in enumerate(raw_items, start=1):
        item = _object(raw_item, "feed.items entry")
        _exact_fields(item, "feed.items entry", {"sequence", "observation"})
        sequence = _cursor(item["sequence"], "feed.items sequence")
        if sequence != from_cursor + offset:
            raise ShadowFeedContractError(
                "feed.items sequences are not contiguous"
            )
        try:
            observation = parse_task_shadow_observation(item["observation"])
        except ShadowObservationError as exc:
            raise ShadowFeedContractError(
                "feed.items observation is invalid"
            ) from exc
        if observation.candidate.source.system != producer:
            raise ShadowFeedContractError(
                "feed producer does not match observation source"
            )
        items.append(TaskShadowFeedItem(sequence, observation))

    return TaskShadowFeed(
        producer=producer,
        stream_id=stream_id,
        from_cursor=from_cursor,
        to_cursor=to_cursor,
        items=tuple(items),
        emitted_at=_aware_timestamp(root["emitted_at"], "feed.emitted_at"),
    )


def task_shadow_feed_document(feed: TaskShadowFeed) -> dict[str, Any]:
    """Return the canonical document shape for a validated feed page."""
    return {
        "schema": feed.schema,
        "schema_version": feed.schema_version,
        "producer": feed.producer,
        "stream_id": feed.stream_id,
        "from_cursor": feed.from_cursor,
        "to_cursor": feed.to_cursor,
        "items": [
            {
                "sequence": item.sequence,
                "observation": task_shadow_observation_document(
                    item.observation
                ),
            }
            for item in feed.items
        ],
        "emitted_at": feed.emitted_at,
    }


def _object(value: object, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ShadowFeedContractError(f"{field} must be an object")
    return value


def _exact_fields(value: Mapping[str, Any], field: str,
                  expected: set[str]) -> None:
    if expected - set(value):
        raise ShadowFeedContractError(f"{field} is missing required fields")
    if set(value) - expected:
        raise ShadowFeedContractError(f"{field} contains additional fields")


def _text(value: object, field: str, maximum: int = 200) -> str:
    if not isinstance(value, str):
        raise ShadowFeedContractError(f"{field} must be a string")
    if value != value.strip() or not value or len(value) > maximum:
        raise ShadowFeedContractError(
            f"{field} has invalid length or whitespace"
        )
    if any(ord(char) < 32 or ord(char) == 127 for char in value):
        raise ShadowFeedContractError(f"{field} contains control characters")
    return value


def _choice(value: object, field: str, choices: frozenset[str]) -> str:
    text = _text(value, field)
    if text not in choices:
        raise ShadowFeedContractError(f"{field} is unsupported")
    return text


def _stream_id(value: object) -> str:
    text = _text(value, "feed.stream_id")
    if not _STREAM_ID_RE.fullmatch(text):
        raise ShadowFeedContractError("feed.stream_id has invalid format")
    return text


def _cursor(value: object, field: str) -> int:
    if (isinstance(value, bool) or not isinstance(value, int)
            or value < 0 or value > MAX_CURSOR):
        raise ShadowFeedContractError(
            f"{field} must be a non-negative integer"
        )
    return value


def _aware_timestamp(value: object, field: str) -> str:
    text = _text(value, field, maximum=64)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ShadowFeedContractError(
            f"{field} must be an ISO-8601 timestamp"
        ) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ShadowFeedContractError(f"{field} must include a timezone")
    return text
