"""Strict contract for GW-attested task-owner equivalence."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Mapping


REQUEST_SCHEMA = "gw.task-owner-equivalence-request"
RESPONSE_SCHEMA = "gw.task-owner-equivalence"
SCHEMA_VERSION = 1
EQUIVALENCE_BASIS = "speaker_merge"

_ALIAS_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
_CANDIDATE_ID_RE = re.compile(r"^tc_[0-9a-f]{64}$")
_DIGEST_RE = re.compile(r"^[0-9a-f]{64}$")


class OwnerEquivalenceContractError(ValueError):
    """Owner-equivalence data does not satisfy the closed contract."""


class OwnerEquivalenceResolutionError(RuntimeError):
    """A bounded resolver could not provide a usable response."""


@dataclass(frozen=True)
class OwnerEquivalenceRequest:
    alias: str
    candidate_id: str
    source_revision: str
    legacy_task_id: int
    legacy_digest: str


@dataclass(frozen=True)
class TaskOwnerEquivalence:
    request: OwnerEquivalenceRequest
    status: str
    basis: str | None
    effective_owner: str | None

    @property
    def equivalent(self) -> bool:
        return self.status == "equivalent"


def owner_equivalence_request(
    *,
    alias: object,
    candidate_id: object,
    source_revision: object,
    legacy_task_id: object,
    legacy_digest: object,
) -> OwnerEquivalenceRequest:
    return OwnerEquivalenceRequest(
        alias=_pattern(alias, "alias", _ALIAS_RE),
        candidate_id=_pattern(
            candidate_id, "candidate_id", _CANDIDATE_ID_RE
        ),
        source_revision=_pattern(
            source_revision, "source_revision", _DIGEST_RE
        ),
        legacy_task_id=_task_id(legacy_task_id),
        legacy_digest=_pattern(
            legacy_digest, "legacy_digest", _DIGEST_RE
        ),
    )


def owner_equivalence_request_document(
    request: OwnerEquivalenceRequest,
) -> dict[str, Any]:
    if not isinstance(request, OwnerEquivalenceRequest):
        raise OwnerEquivalenceContractError("owner request is invalid")
    validated = owner_equivalence_request(
        alias=request.alias,
        candidate_id=request.candidate_id,
        source_revision=request.source_revision,
        legacy_task_id=request.legacy_task_id,
        legacy_digest=request.legacy_digest,
    )
    return {
        "schema": REQUEST_SCHEMA,
        "schema_version": SCHEMA_VERSION,
        "alias": validated.alias,
        "candidate_id": validated.candidate_id,
        "source_revision": validated.source_revision,
        "legacy_task_id": validated.legacy_task_id,
        "legacy_digest": validated.legacy_digest,
    }


def parse_owner_equivalence_response(
    value: object,
    request: OwnerEquivalenceRequest,
) -> TaskOwnerEquivalence:
    expected = owner_equivalence_request_document(request)
    root = _object(value, "owner response")
    _exact_fields(
        root,
        {
            "schema", "schema_version", "ok", "alias", "candidate_id",
            "source_revision", "legacy_task_id", "legacy_digest", "status",
            "basis", "effective_owner",
        },
    )
    version = root["schema_version"]
    if (root["schema"] != RESPONSE_SCHEMA
            or isinstance(version, bool)
            or version != SCHEMA_VERSION
            or root["ok"] is not True):
        raise OwnerEquivalenceContractError(
            "owner response identity is invalid"
        )
    for field in (
        "alias", "candidate_id", "source_revision", "legacy_task_id",
        "legacy_digest",
    ):
        if root[field] != expected[field]:
            raise OwnerEquivalenceContractError(
                "owner response request identity is invalid"
            )

    status = root["status"]
    basis = root["basis"]
    effective_owner = root["effective_owner"]
    if status == "equivalent":
        if basis != EQUIVALENCE_BASIS:
            raise OwnerEquivalenceContractError(
                "owner response basis is invalid"
            )
        effective_owner = _text(
            effective_owner, "effective_owner", maximum=200
        )
    elif status == "unresolved":
        if basis is not None or effective_owner is not None:
            raise OwnerEquivalenceContractError(
                "unresolved owner response fields are inconsistent"
            )
    else:
        raise OwnerEquivalenceContractError(
            "owner response status is unsupported"
        )
    return TaskOwnerEquivalence(
        request=request,
        status=status,
        basis=basis,
        effective_owner=effective_owner,
    )


def task_owner_equivalence_document(
    result: TaskOwnerEquivalence,
) -> dict[str, Any]:
    if not isinstance(result, TaskOwnerEquivalence):
        raise OwnerEquivalenceContractError("owner response is invalid")
    request = owner_equivalence_request_document(result.request)
    document = {
        "schema": RESPONSE_SCHEMA,
        "schema_version": SCHEMA_VERSION,
        "ok": True,
        "alias": request["alias"],
        "candidate_id": request["candidate_id"],
        "source_revision": request["source_revision"],
        "legacy_task_id": request["legacy_task_id"],
        "legacy_digest": request["legacy_digest"],
        "status": result.status,
        "basis": result.basis,
        "effective_owner": result.effective_owner,
    }
    parsed = parse_owner_equivalence_response(document, result.request)
    if parsed != result:
        raise OwnerEquivalenceContractError("owner response is inconsistent")
    return document


def _object(value: object, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise OwnerEquivalenceContractError(f"{field} must be an object")
    return value


def _exact_fields(value: Mapping[str, Any], expected: set[str]) -> None:
    if set(value) != expected:
        raise OwnerEquivalenceContractError(
            "owner response fields are invalid"
        )


def _text(value: object, field: str, *, maximum: int) -> str:
    if (not isinstance(value, str) or not value or value != value.strip()
            or len(value) > maximum
            or any(ord(char) < 32 or ord(char) == 127 for char in value)):
        raise OwnerEquivalenceContractError(f"{field} is invalid")
    return value


def _pattern(value: object, field: str, pattern: re.Pattern[str]) -> str:
    text = _text(value, field, maximum=200)
    if pattern.fullmatch(text) is None:
        raise OwnerEquivalenceContractError(f"{field} is invalid")
    return text


def _task_id(value: object) -> int:
    if (isinstance(value, bool) or not isinstance(value, int)
            or not 1 <= value <= 9_223_372_036_854_775_807):
        raise OwnerEquivalenceContractError("legacy_task_id is invalid")
    return value
