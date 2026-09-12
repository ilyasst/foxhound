"""Strict, content-free contract for one projected task lifecycle outcome."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Mapping


SCHEMA_ID = "foxhound.task-lifecycle-outcome"
SCHEMA_VERSION = 1
MAX_INTEGER = 9_223_372_036_854_775_807
STATUSES = frozenset({"open", "done", "dropped"})
TRANSITIONS = frozenset({
    ("open", "done"),
    ("open", "dropped"),
    ("done", "open"),
    ("dropped", "open"),
})
_SYSTEM_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,199}$")


class LifecycleOutcomeContractError(ValueError):
    """A lifecycle outcome does not satisfy the supported contract."""


@dataclass(frozen=True)
class LegacyTaskCorrelation:
    system: str
    task_id: int


@dataclass(frozen=True)
class TaskLifecycleOutcome:
    event_sequence: int
    task_id: int
    task_version: int
    correlation: LegacyTaskCorrelation
    from_status: str
    to_status: str
    occurred_at: str
    schema: str = SCHEMA_ID
    schema_version: int = SCHEMA_VERSION


def parse_task_lifecycle_outcome(document: object) -> TaskLifecycleOutcome:
    root = _object(document, "outcome")
    _exact_fields(root, "outcome", {
        "schema", "schema_version", "event_sequence", "task_id",
        "task_version", "correlation", "from_status", "to_status",
        "occurred_at",
    })
    if root["schema"] != SCHEMA_ID:
        raise LifecycleOutcomeContractError("outcome.schema is unsupported")
    version = root["schema_version"]
    if isinstance(version, bool) or version != SCHEMA_VERSION:
        raise LifecycleOutcomeContractError(
            "outcome.schema_version is unsupported"
        )
    correlation = _object(root["correlation"], "outcome.correlation")
    _exact_fields(
        correlation, "outcome.correlation", {"system", "task_id"}
    )
    system = _text(correlation["system"], "outcome.correlation.system")
    if not _SYSTEM_RE.fullmatch(system):
        raise LifecycleOutcomeContractError(
            "outcome.correlation.system has invalid format"
        )
    from_status = _status(root["from_status"], "outcome.from_status")
    to_status = _status(root["to_status"], "outcome.to_status")
    if (from_status, to_status) not in TRANSITIONS:
        raise LifecycleOutcomeContractError("outcome transition is invalid")
    task_version = _positive_integer(
        root["task_version"], "outcome.task_version"
    )
    if task_version < 2:
        raise LifecycleOutcomeContractError(
            "outcome.task_version must represent a transition"
        )
    return TaskLifecycleOutcome(
        event_sequence=_positive_integer(
            root["event_sequence"], "outcome.event_sequence"
        ),
        task_id=_positive_integer(root["task_id"], "outcome.task_id"),
        task_version=task_version,
        correlation=LegacyTaskCorrelation(
            system=system,
            task_id=_positive_integer(
                correlation["task_id"], "outcome.correlation.task_id"
            ),
        ),
        from_status=from_status,
        to_status=to_status,
        occurred_at=_timestamp(root["occurred_at"], "outcome.occurred_at"),
    )


def task_lifecycle_outcome_document(
    outcome: TaskLifecycleOutcome,
) -> dict[str, Any]:
    return {
        "schema": outcome.schema,
        "schema_version": outcome.schema_version,
        "event_sequence": outcome.event_sequence,
        "task_id": outcome.task_id,
        "task_version": outcome.task_version,
        "correlation": {
            "system": outcome.correlation.system,
            "task_id": outcome.correlation.task_id,
        },
        "from_status": outcome.from_status,
        "to_status": outcome.to_status,
        "occurred_at": outcome.occurred_at,
    }


def _object(value: object, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise LifecycleOutcomeContractError(f"{field} must be an object")
    return value


def _exact_fields(
    value: Mapping[str, Any], field: str, expected: set[str]
) -> None:
    if expected - set(value):
        raise LifecycleOutcomeContractError(
            f"{field} is missing required fields"
        )
    if set(value) - expected:
        raise LifecycleOutcomeContractError(
            f"{field} contains additional fields"
        )


def _text(value: object, field: str, maximum: int = 200) -> str:
    if not isinstance(value, str):
        raise LifecycleOutcomeContractError(f"{field} must be a string")
    if value != value.strip() or not value or len(value) > maximum:
        raise LifecycleOutcomeContractError(
            f"{field} has invalid length or whitespace"
        )
    if any(ord(char) < 32 or ord(char) == 127 for char in value):
        raise LifecycleOutcomeContractError(
            f"{field} contains control characters"
        )
    return value


def _positive_integer(value: object, field: str) -> int:
    if (isinstance(value, bool) or not isinstance(value, int)
            or not 1 <= value <= MAX_INTEGER):
        raise LifecycleOutcomeContractError(
            f"{field} must be a positive integer"
        )
    return value


def _status(value: object, field: str) -> str:
    status = _text(value, field)
    if status not in STATUSES:
        raise LifecycleOutcomeContractError(f"{field} is unsupported")
    return status


def _timestamp(value: object, field: str) -> str:
    text = _text(value, field, 64)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise LifecycleOutcomeContractError(
            f"{field} must be an ISO-8601 timestamp"
        ) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise LifecycleOutcomeContractError(f"{field} must include a timezone")
    return text
