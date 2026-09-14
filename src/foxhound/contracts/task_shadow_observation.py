"""Strict parser for passive legacy-task shadow observations."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Mapping

from foxhound.source_policy import source_kinds_accepting

from .task_candidate import (
    TaskCandidate,
    ContractError,
    parse_task_candidate,
    task_candidate_document,
)


SCHEMA_ID = "foxhound.task-shadow-observation"
SCHEMA_VERSION = 1
DISPOSITIONS = frozenset({"minted", "folded", "refused", "unmapped"})
MAPPED_DISPOSITIONS = frozenset({"minted", "folded"})
REFUSAL_REASONS = frozenset({
    "ambiguous_match",
    "identity_conflict",
    "source_unavailable",
    "unaddressable_projection",
    "unsupported_source",
})
UNMAPPED_REASONS = frozenset({
    "ambiguous_retrofit",
    "legacy_identity_absent",
})
MAX_LEGACY_TASK_ID = 9_223_372_036_854_775_807

_DIGEST_RE = re.compile(r"^[0-9a-f]{64}$")


class ShadowObservationError(ValueError):
    """An observation does not satisfy the supported contract."""


@dataclass(frozen=True)
class LegacyTaskObservation:
    task_id: int
    comparable_digest: str


@dataclass(frozen=True)
class TaskShadowObservation:
    candidate: TaskCandidate
    disposition: str
    legacy_task: LegacyTaskObservation | None
    reason_code: str | None
    observed_at: str
    schema: str = SCHEMA_ID
    schema_version: int = SCHEMA_VERSION


def comparable_task_digest(*, text: str, project: str | None,
                           owner: str | None) -> str:
    """Digest the task fields represented independently by both systems.

    Due date is deliberately absent: the legacy GW task row does not retain a
    separate due field, so including it would pretend that it can be compared.
    """
    text = _bounded_text(text, "task.text", 1, 1_000)
    owner = _optional_text(owner, "task.owner", 200)
    fields = (
        [text, _bounded_text(project, "task.project", 1, 200), owner]
        if project is not None else [text, owner]
    )
    material = json.dumps(
        fields,
        ensure_ascii=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(material).hexdigest()


def candidate_comparable_digest(candidate: TaskCandidate) -> str:
    """Return the comparable-field digest for a validated candidate."""
    if not isinstance(candidate, TaskCandidate):
        raise ShadowObservationError("candidate must be validated")
    return comparable_task_digest(
        text=candidate.task.text,
        project=candidate.task.project,
        owner=candidate.task.owner,
    )


def task_shadow_observation_document(
    observation: TaskShadowObservation,
) -> dict[str, Any]:
    """Return the canonical document shape for a validated observation."""
    return {
        "schema": observation.schema,
        "schema_version": observation.schema_version,
        "candidate": task_candidate_document(observation.candidate),
        "disposition": observation.disposition,
        "legacy_task": (
            {
                "task_id": observation.legacy_task.task_id,
                "comparable_digest": (
                    observation.legacy_task.comparable_digest
                ),
            }
            if observation.legacy_task is not None else None
        ),
        "reason_code": observation.reason_code,
        "observed_at": observation.observed_at,
    }


def parse_task_shadow_observation(document: object) -> TaskShadowObservation:
    """Validate and decode one passive shadow observation."""
    root = _object(document, "observation")
    _exact_fields(
        root,
        "observation",
        {
            "schema", "schema_version", "candidate", "disposition",
            "legacy_task", "reason_code", "observed_at",
        },
    )
    if root["schema"] != SCHEMA_ID:
        raise ShadowObservationError("observation.schema is unsupported")
    version = root["schema_version"]
    if isinstance(version, bool) or version != SCHEMA_VERSION:
        raise ShadowObservationError(
            "observation.schema_version is unsupported"
        )

    try:
        candidate = parse_task_candidate(root["candidate"])
    except ContractError as exc:
        raise ShadowObservationError(str(exc)) from exc
    if candidate.source.kind not in source_kinds_accepting(
        "accepts_shadow_observations"
    ):
        raise ShadowObservationError(
            "candidate.source.kind is unsupported for shadow observations"
        )

    disposition = _choice(
        root["disposition"], "observation.disposition", DISPOSITIONS
    )
    legacy_task = _legacy_task(root["legacy_task"])
    reason_code = root["reason_code"]

    if disposition in MAPPED_DISPOSITIONS:
        if legacy_task is None:
            raise ShadowObservationError(
                "observation.legacy_task is required for mapped disposition"
            )
        if reason_code is not None:
            raise ShadowObservationError(
                "observation.reason_code must be null for mapped disposition"
            )
    else:
        if legacy_task is not None:
            raise ShadowObservationError(
                "observation.legacy_task must be null for unmapped disposition"
            )
        allowed_reasons = (
            REFUSAL_REASONS if disposition == "refused" else UNMAPPED_REASONS
        )
        reason_code = _choice(
            reason_code, "observation.reason_code", allowed_reasons
        )

    return TaskShadowObservation(
        candidate=candidate,
        disposition=disposition,
        legacy_task=legacy_task,
        reason_code=reason_code,
        observed_at=_aware_timestamp(
            root["observed_at"], "observation.observed_at"
        ),
    )


def parse_task_shadow_observation_json(text: str) -> TaskShadowObservation:
    """Decode JSON while refusing duplicate fields at every object level."""
    try:
        document = json.loads(text, object_pairs_hook=_strict_object)
    except (json.JSONDecodeError, TypeError, _DuplicateField) as exc:
        raise ShadowObservationError("observation JSON is invalid") from exc
    return parse_task_shadow_observation(document)


class _DuplicateField(ValueError):
    pass


def _strict_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise _DuplicateField
        result[key] = value
    return result


def _legacy_task(value: object) -> LegacyTaskObservation | None:
    if value is None:
        return None
    document = _object(value, "observation.legacy_task")
    _exact_fields(
        document,
        "observation.legacy_task",
        {"task_id", "comparable_digest"},
    )
    task_id = document["task_id"]
    if (isinstance(task_id, bool) or not isinstance(task_id, int)
            or task_id < 1 or task_id > MAX_LEGACY_TASK_ID):
        raise ShadowObservationError(
            "observation.legacy_task.task_id must be a positive integer"
        )
    digest = _pattern_text(
        document["comparable_digest"],
        "observation.legacy_task.comparable_digest",
        _DIGEST_RE,
    )
    return LegacyTaskObservation(task_id=task_id, comparable_digest=digest)


def _object(value: object, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ShadowObservationError(f"{field} must be an object")
    return value


def _exact_fields(value: Mapping[str, Any], field: str,
                  expected: set[str]) -> None:
    if expected - set(value):
        raise ShadowObservationError(f"{field} is missing required fields")
    if set(value) - expected:
        raise ShadowObservationError(f"{field} contains additional fields")


def _bounded_text(value: object, field: str, minimum: int,
                  maximum: int) -> str:
    if not isinstance(value, str):
        raise ShadowObservationError(f"{field} must be a string")
    if value != value.strip():
        raise ShadowObservationError(
            f"{field} must not have surrounding whitespace"
        )
    if not minimum <= len(value) <= maximum:
        raise ShadowObservationError(f"{field} has invalid length")
    if any(ord(char) < 32 or ord(char) == 127 for char in value):
        raise ShadowObservationError(f"{field} contains control characters")
    return value


def _optional_text(value: object, field: str, maximum: int) -> str | None:
    if value is None:
        return None
    return _bounded_text(value, field, 1, maximum)


def _pattern_text(value: object, field: str,
                  pattern: re.Pattern[str]) -> str:
    text = _bounded_text(value, field, 1, 200)
    if not pattern.fullmatch(text):
        raise ShadowObservationError(f"{field} has invalid format")
    return text


def _choice(value: object, field: str,
            choices: frozenset[str]) -> str:
    text = _bounded_text(value, field, 1, 200)
    if text not in choices:
        raise ShadowObservationError(f"{field} is unsupported")
    return text


def _aware_timestamp(value: object, field: str) -> str:
    text = _bounded_text(value, field, 1, 64)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ShadowObservationError(
            f"{field} must be an ISO-8601 timestamp"
        ) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ShadowObservationError(f"{field} must include a timezone")
    return text
