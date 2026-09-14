"""Strict parser for supported ``foxhound.task-candidate`` contracts.

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

from foxhound.source_policy import provenance_roles_for, source_kinds_accepting


SCHEMA_ID = "foxhound.task-candidate"
SCHEMA_VERSION = 1
PROJECTLESS_SCHEMA_VERSION = 2
LIFECYCLE_SCHEMA_VERSION = 3
PROVENANCE_SCHEMA_VERSION = 4
OWNER_SCHEMA_VERSION = 5
OWNER_PROVENANCE_SCHEMA_VERSION = 6
CUMULATIVE_SCHEMA_VERSION = 7
SUPPORTED_SCHEMA_VERSIONS = frozenset({
    SCHEMA_VERSION,
    PROJECTLESS_SCHEMA_VERSION,
    LIFECYCLE_SCHEMA_VERSION,
    PROVENANCE_SCHEMA_VERSION,
    OWNER_SCHEMA_VERSION,
    OWNER_PROVENANCE_SCHEMA_VERSION,
    CUMULATIVE_SCHEMA_VERSION,
})
LIFECYCLE_STATES = frozenset({"active", "withdrawn"})
SOURCE_SYSTEMS = frozenset({"gw"})
#: ``issue`` is a forge issue nominated for work. Its ``record_id`` is the
#: repository's canonical locator and its ``item_id`` the issue number, so the
#: inbox's uniqueness constraint admits one task per issue and re-emitting an
#: unchanged issue is a no-op.
#:
#: ``legacy`` is intentionally explicit: it names an open task carried across
#: a bounded authority cutover. Calling that record a meeting, email, or issue
#: would give an execution agent a false origin. It receives no special
#: lifecycle or execution authority and is not a permanent producer registry.
SOURCE_KINDS = source_kinds_accepting("accepts_candidates")

#: Identifiers are opaque to this parser: it checks their shape, never their
#: meaning. ``/`` is accepted because forge identifiers are path-shaped — a
#: repository is ``host/owner/name`` and an issue reference carries it — and
#: refusing the separator would force producers to invent an encoding, which
#: is a worse failure mode than accepting the character.
#:
#: Accepting ``/`` cannot enable traversal here: these values are only ever
#: stored as SQLite column values and compared in the
#: ``UNIQUE(source_system, source_kind, source_record_id, source_item_id)``
#: constraint. No code path builds a filesystem path from them. Widening also
#: only ever accepts MORE, so no previously valid candidate becomes invalid.
_OPAQUE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/-]{0,199}$")
_REVISION_RE = re.compile(r"^[0-9a-f]{64}$")
_CANDIDATE_ID_RE = re.compile(r"^tc_[0-9a-f]{64}$")
OWNER_KINDS = frozenset({"person", "unresolved", "external", "group"})
UNRESOLVED_OWNER_DISPLAY = "(unassigned)"
_SPEAKER_ID_RE = re.compile(r"^SPK_\d+$")
_SPEAKER_ID_IN_DISPLAY_RE = re.compile(r"(?<![A-Za-z0-9_])SPK_\d+(?!\d)")
MAX_EVIDENCE_SOURCES = 3
MAX_EVIDENCE_SOURCE_NAME = 255
MAX_EVIDENCE_EXTRACT = 1_200


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
    project: str | None
    owner: str | None
    due: str | None
    owner_ref: CandidateOwnerRef | None = None


@dataclass(frozen=True)
class CandidateOwnerRef:
    """A scoped owner identity kept separately from its card display text."""

    kind: str
    speaker_id: str | None
    canonical_speaker_id: str | None
    speaker_registry_id: str | None
    pinned: bool
    provisional: bool


@dataclass(frozen=True)
class CandidateEvidenceSource:
    name: str
    role: str
    extract: str


@dataclass(frozen=True)
class CandidateEvidence:
    document_id: str
    locator: str
    sources: tuple[CandidateEvidenceSource, ...] = ()


@dataclass(frozen=True)
class CandidateLifecycle:
    state: str
    generation: int
    changed_at: str | None


@dataclass(frozen=True)
class TaskCandidate:
    candidate_id: str
    source: CandidateSource
    task: CandidateTask
    evidence: CandidateEvidence
    created_at: str
    lifecycle: CandidateLifecycle = CandidateLifecycle("active", 0, None)
    schema: str = SCHEMA_ID
    schema_version: int = SCHEMA_VERSION


def task_candidate_document(candidate: TaskCandidate) -> dict[str, Any]:
    """Return the canonical document shape for a validated candidate."""
    task = {
        "text": candidate.task.text,
        "owner": candidate.task.owner,
        "due": candidate.task.due,
    }
    if candidate.schema_version == SCHEMA_VERSION or (
        candidate.schema_version in {
            PROVENANCE_SCHEMA_VERSION, OWNER_PROVENANCE_SCHEMA_VERSION,
            CUMULATIVE_SCHEMA_VERSION,
        }
        and candidate.task.project is not None
    ) or (
        candidate.schema_version == OWNER_SCHEMA_VERSION
        and candidate.task.project is not None
    ):
        task["project"] = candidate.task.project
    if candidate.schema_version in {
        OWNER_SCHEMA_VERSION, OWNER_PROVENANCE_SCHEMA_VERSION,
        CUMULATIVE_SCHEMA_VERSION,
    }:
        owner_ref = candidate.task.owner_ref
        task["owner_ref"] = None if owner_ref is None else {
            "kind": owner_ref.kind,
            "speaker_id": owner_ref.speaker_id,
            "canonical_speaker_id": owner_ref.canonical_speaker_id,
            "speaker_registry_id": owner_ref.speaker_registry_id,
            "pinned": owner_ref.pinned,
            "provisional": owner_ref.provisional,
        }
    document = {
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
        "task": task,
        "evidence": {
            "document_id": candidate.evidence.document_id,
            "locator": candidate.evidence.locator,
        },
        "created_at": candidate.created_at,
    }
    if candidate.schema_version in {
        PROVENANCE_SCHEMA_VERSION, OWNER_PROVENANCE_SCHEMA_VERSION,
    } or (
        candidate.schema_version == CUMULATIVE_SCHEMA_VERSION
        and candidate.evidence.sources
    ):
        document["evidence"]["sources"] = [
            {
                "name": source.name,
                "role": source.role,
                "extract": source.extract,
            }
            for source in candidate.evidence.sources
        ]
    if candidate.schema_version in {
        LIFECYCLE_SCHEMA_VERSION, CUMULATIVE_SCHEMA_VERSION,
    }:
        document["lifecycle"] = {
            "state": candidate.lifecycle.state,
            "generation": candidate.lifecycle.generation,
            "changed_at": candidate.lifecycle.changed_at,
        }
    return document


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
    """Validate and decode one supported task candidate.

    Unknown versions, missing fields, and additional fields fail closed.
    """
    root = _object(document, "candidate")
    base_fields = {
        "schema", "schema_version", "candidate_id", "source", "task",
        "evidence", "created_at",
    }
    if {"schema", "schema_version"} - set(root):
        raise ContractError("candidate is missing required fields")
    if root["schema"] != SCHEMA_ID:
        raise ContractError("candidate.schema is unsupported")
    version = root["schema_version"]
    if (isinstance(version, bool)
            or version not in SUPPORTED_SCHEMA_VERSIONS):
        raise ContractError("candidate.schema_version is unsupported")
    _exact_fields(
        root,
        "candidate",
        base_fields | ({"lifecycle"} if version in {
            LIFECYCLE_SCHEMA_VERSION, CUMULATIVE_SCHEMA_VERSION,
        } else set()),
    )

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
    base_task_fields = {"text", "owner", "due"}
    if version in {
        PROVENANCE_SCHEMA_VERSION,
        OWNER_SCHEMA_VERSION,
        OWNER_PROVENANCE_SCHEMA_VERSION,
        CUMULATIVE_SCHEMA_VERSION,
    }:
        allowed = base_task_fields | {"project"}
        if version in {
            OWNER_SCHEMA_VERSION, OWNER_PROVENANCE_SCHEMA_VERSION,
            CUMULATIVE_SCHEMA_VERSION,
        }:
            allowed.add("owner_ref")
        _required_and_allowed_fields(
            task_doc,
            "candidate.task",
            base_task_fields | (
                {"owner_ref"}
                if version in {
                    OWNER_SCHEMA_VERSION, OWNER_PROVENANCE_SCHEMA_VERSION,
                    CUMULATIVE_SCHEMA_VERSION,
                }
                else set()
            ),
            allowed,
        )
    else:
        task_fields = (
            base_task_fields | {"project"}
            if version == SCHEMA_VERSION
            else base_task_fields
        )
        _exact_fields(task_doc, "candidate.task", task_fields)
    owner = _optional_text(task_doc["owner"], "candidate.task.owner", 200)
    owner_ref = (
        _owner_ref(task_doc["owner_ref"], owner, source_kind=source.kind)
        if version in {
            OWNER_SCHEMA_VERSION, OWNER_PROVENANCE_SCHEMA_VERSION,
            CUMULATIVE_SCHEMA_VERSION,
        }
        else None
    )
    task = CandidateTask(
        text=_bounded_text(task_doc["text"], "candidate.task.text", 1, 1_000),
        project=(
            _bounded_text(
                task_doc["project"], "candidate.task.project", 1, 200
            )
            if version == SCHEMA_VERSION or (
                version in {
                    PROVENANCE_SCHEMA_VERSION,
                    OWNER_SCHEMA_VERSION,
                    OWNER_PROVENANCE_SCHEMA_VERSION,
                    CUMULATIVE_SCHEMA_VERSION,
                }
                and "project" in task_doc
            ) else None
        ),
        owner=owner,
        due=_optional_date(task_doc["due"], "candidate.task.due"),
        owner_ref=owner_ref,
    )

    evidence_doc = _object(root["evidence"], "candidate.evidence")
    evidence_fields = {"document_id", "locator"}
    if version in {
        PROVENANCE_SCHEMA_VERSION, OWNER_PROVENANCE_SCHEMA_VERSION,
    } or (version == CUMULATIVE_SCHEMA_VERSION and "sources" in evidence_doc):
        evidence_fields.add("sources")
    if version == CUMULATIVE_SCHEMA_VERSION:
        _required_and_allowed_fields(
            evidence_doc,
            "candidate.evidence",
            {"document_id", "locator"},
            {"document_id", "locator", "sources"},
        )
    else:
        _exact_fields(evidence_doc, "candidate.evidence", evidence_fields)
    sources: tuple[CandidateEvidenceSource, ...] = ()
    if version in {
        PROVENANCE_SCHEMA_VERSION, OWNER_PROVENANCE_SCHEMA_VERSION,
    }:
        if source.kind != "meeting":
            raise ContractError(
                "candidate.source.kind is unsupported for provenance"
            )
        sources = _evidence_sources(
            evidence_doc["sources"], provenance_roles_for("meeting")
        )
    elif version == CUMULATIVE_SCHEMA_VERSION and "sources" in evidence_doc:
        sources = _evidence_sources(
            evidence_doc["sources"], provenance_roles_for(source.kind)
        )
    evidence = CandidateEvidence(
        document_id=_opaque_id(
            evidence_doc["document_id"], "candidate.evidence.document_id"),
        locator=_opaque_id(evidence_doc["locator"],
                           "candidate.evidence.locator"),
        sources=sources,
    )

    created_at = _aware_timestamp(root["created_at"], "candidate.created_at")
    lifecycle = CandidateLifecycle("active", 0, None)
    if version in {LIFECYCLE_SCHEMA_VERSION, CUMULATIVE_SCHEMA_VERSION}:
        lifecycle_doc = _object(root["lifecycle"], "candidate.lifecycle")
        _exact_fields(
            lifecycle_doc,
            "candidate.lifecycle",
            {"state", "generation", "changed_at"},
        )
        state = _choice(
            lifecycle_doc["state"],
            "candidate.lifecycle.state",
            LIFECYCLE_STATES,
        )
        generation = lifecycle_doc["generation"]
        if (
            isinstance(generation, bool)
            or not isinstance(generation, int)
            or not 1 <= generation <= 9_223_372_036_854_775_807
        ):
            raise ContractError(
                "candidate.lifecycle.generation must be a positive integer"
            )
        changed_at = _aware_timestamp(
            lifecycle_doc["changed_at"], "candidate.lifecycle.changed_at"
        )
        lifecycle = CandidateLifecycle(state, generation, changed_at)
    return TaskCandidate(
        candidate_id=candidate_id,
        source=source,
        task=task,
        evidence=evidence,
        created_at=created_at,
        lifecycle=lifecycle,
        schema_version=version,
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


def _required_and_allowed_fields(
    value: Mapping[str, Any],
    field: str,
    required: set[str],
    allowed: set[str],
) -> None:
    if required - set(value):
        raise ContractError(f"{field} is missing required fields")
    if set(value) - allowed:
        raise ContractError(f"{field} contains additional fields")


def _evidence_sources(
    value: object, allowed_roles: frozenset[str]
) -> tuple[CandidateEvidenceSource, ...]:
    if not isinstance(value, list) or not 1 <= len(value) <= MAX_EVIDENCE_SOURCES:
        raise ContractError("candidate.evidence.sources has invalid length")
    sources = []
    seen = set()
    for raw_source in value:
        source = _object(raw_source, "candidate.evidence.sources entry")
        _exact_fields(
            source,
            "candidate.evidence.sources entry",
            {"name", "role", "extract"},
        )
        name = _source_name(
            source["name"], "candidate.evidence.sources entry.name"
        )
        role = _choice(
            source["role"],
            "candidate.evidence.sources entry.role",
            allowed_roles,
        )
        extract = _bounded_excerpt(
            source["extract"],
            "candidate.evidence.sources entry.extract",
        )
        identity = (name, role)
        if identity in seen:
            raise ContractError("candidate.evidence.sources contains a duplicate")
        seen.add(identity)
        sources.append(CandidateEvidenceSource(name, role, extract))
    return tuple(sources)


def _owner_ref(
    value: object, owner: str | None, *, source_kind: str
) -> CandidateOwnerRef | None:
    if value is None:
        if owner is not None:
            raise ContractError(
                "candidate.task.owner_ref is required when owner is present"
            )
        return None
    if owner is None:
        raise ContractError(
            "candidate.task.owner must be present when owner_ref is present"
        )
    reference = _object(value, "candidate.task.owner_ref")
    _exact_fields(
        reference,
        "candidate.task.owner_ref",
        {
            "kind",
            "speaker_id",
            "canonical_speaker_id",
            "speaker_registry_id",
            "pinned",
            "provisional",
        },
    )
    kind = _choice(
        reference["kind"], "candidate.task.owner_ref.kind", OWNER_KINDS
    )
    speaker_id = _optional_pattern_text(
        reference["speaker_id"],
        "candidate.task.owner_ref.speaker_id",
        _SPEAKER_ID_RE,
    )
    canonical_speaker_id = _optional_pattern_text(
        reference["canonical_speaker_id"],
        "candidate.task.owner_ref.canonical_speaker_id",
        _SPEAKER_ID_RE,
    )
    registry_id = (
        _opaque_id(
            reference["speaker_registry_id"],
            "candidate.task.owner_ref.speaker_registry_id",
        )
        if reference["speaker_registry_id"] is not None else None
    )
    pinned = _boolean(reference["pinned"], "candidate.task.owner_ref.pinned")
    provisional = _boolean(
        reference["provisional"], "candidate.task.owner_ref.provisional"
    )
    if pinned and source_kind != "legacy":
        raise ContractError(
            "candidate.task.owner_ref producer pin is unsupported"
        )

    has_observed_identity = speaker_id is not None or registry_id is not None
    if has_observed_identity and not (speaker_id and registry_id):
        raise ContractError(
            "candidate.task.owner_ref speaker identity must be scoped"
        )
    if canonical_speaker_id is not None and not (speaker_id and registry_id):
        raise ContractError(
            "candidate.task.owner_ref canonical identity must be scoped"
        )
    if kind in {"external", "group"} and (
        speaker_id or canonical_speaker_id or registry_id
    ):
        raise ContractError(
            "candidate.task.owner_ref kind cannot carry speaker identity"
        )
    if kind == "unresolved" and canonical_speaker_id is not None:
        raise ContractError(
            "candidate.task.owner_ref unresolved identity cannot be canonical"
        )
    if kind == "unresolved" and owner != UNRESOLVED_OWNER_DISPLAY:
        raise ContractError(
            "candidate.task.owner must use the unresolved display"
        )
    if _SPEAKER_ID_IN_DISPLAY_RE.search(owner):
        raise ContractError(
            "candidate.task.owner must not expose a speaker identifier"
        )
    return CandidateOwnerRef(
        kind=kind,
        speaker_id=speaker_id,
        canonical_speaker_id=canonical_speaker_id,
        speaker_registry_id=registry_id,
        pinned=pinned,
        provisional=provisional,
    )


def _source_name(value: object, field: str) -> str:
    text = _bounded_text(value, field, 1, MAX_EVIDENCE_SOURCE_NAME)
    if text in {".", ".."} or "/" in text or "\\" in text:
        raise ContractError(f"{field} must be one safe path segment")
    return text


def _bounded_excerpt(value: object, field: str) -> str:
    if not isinstance(value, str):
        raise ContractError(f"{field} must be a string")
    if value != value.strip():
        raise ContractError(f"{field} must not have surrounding whitespace")
    if not 1 <= len(value) <= MAX_EVIDENCE_EXTRACT:
        raise ContractError(f"{field} has invalid length")
    if any((ord(char) < 32 and char not in "\n\t") or ord(char) == 127
           for char in value):
        raise ContractError(f"{field} contains control characters")
    return value


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


def _optional_pattern_text(
    value: object, field: str, pattern: re.Pattern[str]
) -> str | None:
    if value is None:
        return None
    return _pattern_text(value, field, pattern)


def _boolean(value: object, field: str) -> bool:
    if not isinstance(value, bool):
        raise ContractError(f"{field} must be a boolean")
    return value


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
