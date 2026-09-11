"""Strict ordered-page contract for task candidate delivery."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Mapping

from .task_candidate import ContractError, TaskCandidate, parse_task_candidate


SCHEMA_ID = "foxhound.task-candidate-feed"
SCHEMA_VERSION = 1
MAX_FEED_ITEMS = 500
MAX_CURSOR = 9_223_372_036_854_775_807
PRODUCERS = frozenset({"gw"})

_STREAM_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,199}$")


class FeedContractError(ValueError):
    """A candidate feed page does not satisfy the supported contract."""


@dataclass(frozen=True)
class CandidateFeedItem:
    sequence: int
    candidate: TaskCandidate


@dataclass(frozen=True)
class CandidateFeed:
    producer: str
    stream_id: str
    from_cursor: int
    to_cursor: int
    items: tuple[CandidateFeedItem, ...]
    emitted_at: str
    schema: str = SCHEMA_ID
    schema_version: int = SCHEMA_VERSION


def parse_candidate_feed(document: object) -> CandidateFeed:
    """Validate one bounded, contiguous candidate-feed page."""
    root = _object(document, "feed")
    _exact_fields(
        root,
        "feed",
        {"schema", "schema_version", "producer", "stream_id", "from_cursor",
         "to_cursor", "items", "emitted_at"},
    )
    if root["schema"] != SCHEMA_ID:
        raise FeedContractError("feed.schema is unsupported")
    version = root["schema_version"]
    if isinstance(version, bool) or version != SCHEMA_VERSION:
        raise FeedContractError("feed.schema_version is unsupported")

    producer = _choice(root["producer"], "feed.producer", PRODUCERS)
    stream_id = _stream_id(root["stream_id"])
    from_cursor = _cursor(root["from_cursor"], "feed.from_cursor")
    to_cursor = _cursor(root["to_cursor"], "feed.to_cursor")
    if to_cursor < from_cursor:
        raise FeedContractError("feed cursor range is invalid")

    raw_items = root["items"]
    if not isinstance(raw_items, list):
        raise FeedContractError("feed.items must be an array")
    if len(raw_items) > MAX_FEED_ITEMS:
        raise FeedContractError("feed.items exceeds the page limit")
    if len(raw_items) != to_cursor - from_cursor:
        raise FeedContractError("feed cursor range does not match item count")

    items = []
    for offset, raw_item in enumerate(raw_items, start=1):
        item = _object(raw_item, "feed.items entry")
        _exact_fields(item, "feed.items entry", {"sequence", "candidate"})
        sequence = _cursor(item["sequence"], "feed.items sequence")
        if sequence != from_cursor + offset:
            raise FeedContractError("feed.items sequences are not contiguous")
        try:
            candidate = parse_task_candidate(item["candidate"])
        except ContractError as exc:
            raise FeedContractError(
                "feed.items candidate is invalid"
            ) from exc
        if candidate.source.system != producer:
            raise FeedContractError("feed producer does not match candidate source")
        items.append(CandidateFeedItem(sequence, candidate))

    return CandidateFeed(
        producer=producer,
        stream_id=stream_id,
        from_cursor=from_cursor,
        to_cursor=to_cursor,
        items=tuple(items),
        emitted_at=_aware_timestamp(root["emitted_at"], "feed.emitted_at"),
    )


def _object(value: object, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise FeedContractError(f"{field} must be an object")
    return value


def _exact_fields(value: Mapping[str, Any], field: str,
                  expected: set[str]) -> None:
    if expected - set(value):
        raise FeedContractError(f"{field} is missing required fields")
    if set(value) - expected:
        raise FeedContractError(f"{field} contains additional fields")


def _text(value: object, field: str, maximum: int = 200) -> str:
    if not isinstance(value, str):
        raise FeedContractError(f"{field} must be a string")
    if value != value.strip() or not value or len(value) > maximum:
        raise FeedContractError(f"{field} has invalid length or whitespace")
    if any(ord(char) < 32 or ord(char) == 127 for char in value):
        raise FeedContractError(f"{field} contains control characters")
    return value


def _choice(value: object, field: str, choices: frozenset[str]) -> str:
    text = _text(value, field)
    if text not in choices:
        raise FeedContractError(f"{field} is unsupported")
    return text


def _stream_id(value: object) -> str:
    text = _text(value, "feed.stream_id")
    if not _STREAM_ID_RE.fullmatch(text):
        raise FeedContractError("feed.stream_id has invalid format")
    return text


def _cursor(value: object, field: str) -> int:
    if (isinstance(value, bool) or not isinstance(value, int)
            or value < 0 or value > MAX_CURSOR):
        raise FeedContractError(f"{field} must be a non-negative integer")
    return value


def _aware_timestamp(value: object, field: str) -> str:
    text = _text(value, field, maximum=64)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise FeedContractError(f"{field} must be an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise FeedContractError(f"{field} must include a timezone")
    return text
