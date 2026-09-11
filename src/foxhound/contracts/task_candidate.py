"""Strict parser for the ``foxhound.task-candidate`` version 1 contract.

The parser is dependency-free so a producer cannot change validation behavior
by changing an optional schema library. Error messages name only the rejected
field and rule; they never include the value, because candidate content may be
private even when its shape is safe.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Mapping


SCHEMA_ID = "foxhound.task-candidate"
SCHEMA_VERSION = 1
SOURCE_SYSTEMS = frozenset({"gw"})
SOURCE_KINDS = frozenset({"meeting", "email"})

_OPAQUE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,199}$")
_REVISION_RE = re.compile(r"^[0-9a-f]{64}$")
_CANDIDATE_ID_RE = re.compile(r"^tc_[0-9a-f]{64}$")


class ContractError(ValueError):
    """A candidate does not satisfy the supported contract."""


@dataclass(frozen=True)
class CandidateSource:
    system: str
    kind: str
    record_id: str
    item_id: str
    revision: str


@dataclass(frozen=True)
class CandidateTask:
    text: str
    project: str
    owner: str | None
    due: str | None


@dataclass(frozen=True)
class CandidateEvidence:
    document_id: str
    locator: str


@dataclass(frozen=True)
class TaskCandidate:
    candidate_id: str
    source: CandidateSource
    task: CandidateTask
    evidence: CandidateEvidence
    created_at: str
    schema: str = SCHEMA_ID
    schema_version: int = SCHEMA_VERSION


def task_candidate_document(candidate: TaskCandidate) -> dict[str, Any]:
    """Return the canonical document shape for a validated candidate."""
    return {
        "schema": candidate.schema,
        "schema_version": candidate.schema_version,
        "candidate_id": candidate.candidate_id,
        "source": {
            "system": candidate.source.system,
            "kind": candidate.source.kind,
            "record_id": candidate.source.record_id,
            "item_id": candidate.source.item_id,
            "revision": candidate.source.revision,
        },
        "task": {
            "text": candidate.task.text,
            "project": candidate.task.project,
            "owner": candidate.task.owner,
            "due": candidate.task.due,
        },
        "evidence": {
            "document_id": candidate.evidence.document_id,
            "locator": candidate.evidence.locator,
        },
        "created_at": candidate.created_at,
    }


def candidate_id_for(*, system: str, kind: str, record_id: str,
                     item_id: str) -> str:
    """Return the stable idempotency identity for a source-owned action.

    The source revision is not an input: a later rendering or correction of
    the same action must address the existing candidate.
    """
    identity = json.dumps(
        [system, kind, record_id, item_id],
        ensure_ascii=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return "tc_" + hashlib.sha256(identity).hexdigest()


def parse_task_candidate(document: object) -> TaskCandidate:
    """Validate and decode one version 1 task candidate.

    Unknown versions, missing fields, and additional fields fail closed.
    """
    root = _object(document, "candidate")
    _exact_fields(
        root,
        "candidate",
        {"schema", "schema_version", "candidate_id", "source", "task",
         "evidence", "created_at"},
    )
    if root["schema"] != SCHEMA_ID:
        raise ContractError("candidate.schema is unsupported")
    version = root["schema_version"]
    if isinstance(version, bool) or version != SCHEMA_VERSION:
        raise ContractError("candidate.schema_version is unsupported")

    source_doc = _object(root["source"], "candidate.source")
    _exact_fields(
        source_doc,
        "candidate.source",
        {"system", "kind", "record_id", "item_id", "revision"},
    )
    source = CandidateSource(
        system=_choice(source_doc["system"], "candidate.source.system",
                       SOURCE_SYSTEMS),
        kind=_choice(source_doc["kind"], "candidate.source.kind", SOURCE_KINDS),
        record_id=_opaque_id(source_doc["record_id"],
                             "candidate.source.record_id"),
        item_id=_opaque_id(source_doc["item_id"], "candidate.source.item_id"),
        revision=_pattern_text(source_doc["revision"],
                               "candidate.source.revision", _REVISION_RE),
    )

    candidate_id = _pattern_text(
        root["candidate_id"], "candidate.candidate_id", _CANDIDATE_ID_RE)
    expected_id = candidate_id_for(
        system=source.system,
        kind=source.kind,
        record_id=source.record_id,
        item_id=source.item_id,
    )
    if candidate_id != expected_id:
        raise ContractError("candidate.candidate_id does not match source identity")

    task_doc = _object(root["task"], "candidate.task")
    _exact_fields(task_doc, "candidate.task",
                  {"text", "project", "owner", "due"})
    task = CandidateTask(
        text=_bounded_text(task_doc["text"], "candidate.task.text", 1, 1_000),
        project=_bounded_text(
            task_doc["project"], "candidate.task.project", 1, 200),
        owner=_optional_text(task_doc["owner"], "candidate.task.owner", 200),
        due=_optional_date(task_doc["due"], "candidate.task.due"),
    )

    evidence_doc = _object(root["evidence"], "candidate.evidence")
    _exact_fields(evidence_doc, "candidate.evidence",
                  {"document_id", "locator"})
    evidence = CandidateEvidence(
        document_id=_opaque_id(
            evidence_doc["document_id"], "candidate.evidence.document_id"),
        locator=_opaque_id(evidence_doc["locator"],
                           "candidate.evidence.locator"),
    )

    created_at = _aware_timestamp(root["created_at"], "candidate.created_at")
    return TaskCandidate(
        candidate_id=candidate_id,
        source=source,
        task=task,
        evidence=evidence,
        created_at=created_at,
    )


def _object(value: object, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ContractError(f"{field} must be an object")
    return value


def _exact_fields(value: Mapping[str, Any], field: str,
                  expected: set[str]) -> None:
    missing = expected - set(value)
    if missing:
        raise ContractError(f"{field} is missing required fields")
    additional = set(value) - expected
    if additional:
        raise ContractError(f"{field} contains additional fields")


def _bounded_text(value: object, field: str, minimum: int, maximum: int) -> str:
    if not isinstance(value, str):
        raise ContractError(f"{field} must be a string")
    if value != value.strip():
        raise ContractError(f"{field} must not have surrounding whitespace")
    if not minimum <= len(value) <= maximum:
        raise ContractError(f"{field} has invalid length")
    if any(ord(char) < 32 or ord(char) == 127 for char in value):
        raise ContractError(f"{field} contains control characters")
    return value


def _optional_text(value: object, field: str, maximum: int) -> str | None:
    if value is None:
        return None
    return _bounded_text(value, field, 1, maximum)


def _pattern_text(value: object, field: str, pattern: re.Pattern[str]) -> str:
    text = _bounded_text(value, field, 1, 200)
    if not pattern.fullmatch(text):
        raise ContractError(f"{field} has invalid format")
    return text


def _opaque_id(value: object, field: str) -> str:
    return _pattern_text(value, field, _OPAQUE_ID_RE)


def _choice(value: object, field: str, choices: frozenset[str]) -> str:
    text = _bounded_text(value, field, 1, 200)
    if text not in choices:
        raise ContractError(f"{field} is unsupported")
    return text


def _optional_date(value: object, field: str) -> str | None:
    if value is None:
        return None
    text = _bounded_text(value, field, 10, 10)
    try:
        parsed = date.fromisoformat(text)
    except ValueError as exc:
        raise ContractError(f"{field} must be an ISO-8601 date") from exc
    if parsed.isoformat() != text:
        raise ContractError(f"{field} must be an ISO-8601 date")
    return text


def _aware_timestamp(value: object, field: str) -> str:
    text = _bounded_text(value, field, 1, 64)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ContractError(f"{field} must be an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ContractError(f"{field} must include a timezone")
    return text
