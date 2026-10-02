"""Durable, manual task-research foundation.

The agent-authored draft is intentionally weaker than the published record.
Foxhound owns identity, provenance, source locators, timestamps, coverage,
publication and every state transition.  This module does not schedule tasks,
apply recommendations, discover work automatically, or contact a model.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import secrets
import sqlite3
import stat
import sys
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path, PurePosixPath
from typing import Callable, Mapping, Sequence
from urllib.parse import urlsplit

from .candidate_inbox import SCHEMA_VERSION


INPUT_SCHEMA = "foxhound.task-research-input.v1"
DRAFT_SCHEMA = "foxhound.task-research-draft.v1"
PUBLISHED_SCHEMA = "foxhound.task-research.v1"
MAX_JSON_BYTES = 512 * 1024
MAX_MARKDOWN_BYTES = 64 * 1024
MAX_PROJECTION_BYTES = 16 * 1024
MAX_CLAIM_LEASE_SECONDS = 14_400
RESERVED_FILENAMES = frozenset({".task-research.json", "Research.md"})
SOURCE_NAMESPACES = frozenset({"kb", "meeting", "email", "attachment", "repo", "web", "tool"})
RECOMMENDATION_TYPES = frozenset({
    "after_task_completed", "not_before", "raise_priority", "create_prerequisite",
})
CLAIM_STATUSES = frozenset({"supported", "inferred", "conflicting", "unknown", "unsourced"})
RESEARCH_STATUSES = frozenset({"sufficient", "inconclusive", "unreachable"})
_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,199}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")


class ResearchError(RuntimeError):
    """A research request cannot safely proceed."""


@dataclass(frozen=True)
class ResearchJob:
    job_id: str
    task_id: int
    task_version: int
    generation: int
    input_digest: str
    state: str


@dataclass(frozen=True)
class ResearchClaim:
    job: ResearchJob
    token: str


def _canonical_bytes(value: object) -> bytes:
    return (json.dumps(value, ensure_ascii=False, separators=(",", ":"),
                       sort_keys=True) + "\n").encode("utf-8")


def _digest(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _text(value: object, name: str, maximum: int, *, nullable: bool = False) -> str | None:
    if nullable and value is None:
        return None
    if not isinstance(value, str) or value != value.strip() or not value or len(value) > maximum:
        raise ResearchError(f"invalid {name}")
    if any(ord(char) < 32 and char not in "\n\t" for char in value):
        raise ResearchError(f"invalid {name}")
    return value


def _identifier(value: object, name: str) -> str:
    result = _text(value, name, 200)
    assert result is not None
    if not _ID.fullmatch(result):
        raise ResearchError(f"invalid {name}")
    return result


def _timestamp(value: object, name: str) -> str:
    result = _text(value, name, 40)
    assert result is not None
    try:
        parsed = datetime.fromisoformat(result.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ResearchError(f"invalid {name}") from exc
    if parsed.tzinfo is None:
        raise ResearchError(f"invalid {name}")
    return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def normalize_task_snapshot(document: object) -> dict[str, object]:
    """Validate and normalize all research-relevant structured task fields."""
    if not isinstance(document, Mapping) or document.get("schema_version") != INPUT_SCHEMA:
        raise ResearchError("invalid research input schema")
    task_id = document.get("task_id")
    if not isinstance(task_id, int) or isinstance(task_id, bool) or task_id < 1:
        raise ResearchError("invalid task id")
    task_version = document.get("task_version")
    if not isinstance(task_version, int) or isinstance(task_version, bool) or task_version < 1:
        raise ResearchError("invalid task version")
    text = _text(document.get("text"), "task text", 24_000)
    structured = document.get("structured")
    if not isinstance(structured, Mapping):
        raise ResearchError("invalid structured task")
    confidence = structured.get("confidence")
    if confidence is not None and (
        not isinstance(confidence, (int, float)) or isinstance(confidence, bool)
        or not 0 <= confidence <= 1
    ):
        raise ResearchError("invalid structured confidence")
    owner = document.get("owner")
    if owner is not None and not isinstance(owner, Mapping):
        raise ResearchError("invalid owner")
    working_group = document.get("working_group")
    if working_group is not None and not isinstance(working_group, Mapping):
        raise ResearchError("invalid working group")
    participants = document.get("participants", [])
    external = document.get("external_identifiers", [])
    if not isinstance(participants, list) or not all(isinstance(item, Mapping) for item in participants):
        raise ResearchError("invalid participants")
    if not isinstance(external, list) or not all(isinstance(item, Mapping) for item in external):
        raise ResearchError("invalid external identifiers")
    origin = document.get("origin")
    if origin is not None and not isinstance(origin, Mapping):
        raise ResearchError("invalid origin")
    revisions = document.get("structured_schema_revisions", {})
    if not isinstance(revisions, Mapping):
        raise ResearchError("invalid schema revisions")

    # Mapping values are already structured facts produced by Foxhound.  A
    # canonical JSON round-trip rejects non-JSON values and removes mapping
    # implementation details; set-like collections are sorted and deduped.
    normalized: dict[str, object] = {
        "schema_version": INPUT_SCHEMA,
        "task_id": task_id,
        "task_version": task_version,
        "text": text,
        "structured": {
            "action": structured.get("action"),
            "object": structured.get("object"),
            "confidence": confidence,
        },
        "due": document.get("due"),
        "owner": owner,
        "participants": participants,
        "working_group": working_group,
        "external_identifiers": external,
        "origin": origin,
        "structured_schema_revisions": revisions,
    }
    try:
        normalized = json.loads(json.dumps(normalized, sort_keys=True))
    except (TypeError, ValueError) as exc:
        raise ResearchError("research input is not JSON-safe") from exc
    for key in ("participants", "external_identifiers"):
        values = {_canonical_bytes(item): item for item in normalized[key]}
        normalized[key] = [values[item] for item in sorted(values)]
    if len(_canonical_bytes(normalized)) > 256 * 1024:
        raise ResearchError("research input is too large")
    return normalized


def task_input_digest(document: object) -> str:
    """Return the canonical SHA-256 digest of one normalized task snapshot."""
    return _digest(_canonical_bytes(normalize_task_snapshot(document)))


def _claim(value: object, name: str, source_ids: set[str]) -> dict[str, object]:
    if not isinstance(value, Mapping):
        raise ResearchError(f"invalid {name}")
    if set(value) != {"text", "status", "source_refs"}:
        raise ResearchError(f"invalid {name}")
    text = _text(value.get("text"), name, 8_000)
    status = value.get("status")
    refs = value.get("source_refs")
    if status not in CLAIM_STATUSES or not isinstance(refs, list):
        raise ResearchError(f"invalid {name}")
    if len(refs) > 16 or len(set(refs)) != len(refs) or any(ref not in source_ids for ref in refs):
        raise ResearchError(f"invalid {name} sources")
    if status not in {"unknown", "unsourced"} and not refs:
        raise ResearchError(f"ungrounded {name}")
    return {"text": text, "status": status, "source_refs": refs}


def validate_sources(document: object) -> list[dict[str, object]]:
    """Validate publisher-supplied, broker-issued source receipts."""
    if not isinstance(document, list) or len(document) > 64:
        raise ResearchError("invalid sources")
    result = []
    for position, source in enumerate(document, 1):
        if not isinstance(source, Mapping):
            raise ResearchError("invalid source")
        expected_id = f"src-{position:03d}"
        if source.get("source_id") != expected_id:
            raise ResearchError("source ids are not broker-sequential")
        locator = source.get("locator")
        if not isinstance(locator, Mapping) or set(locator) - {"namespace", "resource", "fragment"}:
            raise ResearchError("invalid source locator")
        namespace = locator.get("namespace")
        resource_raw = locator.get("resource")
        if namespace == "tool":
            if not isinstance(resource_raw, str):
                raise ResearchError("invalid source resource")
            resource = resource_raw
        else:
            resource = _text(resource_raw, "source resource", 2_000)
        if namespace not in SOURCE_NAMESPACES or resource is None:
            raise ResearchError("invalid source namespace")
        pure = PurePosixPath(resource)
        parsed = urlsplit(resource)
        if namespace == "web":
            # Public pages are cited by URL; nothing is read from disk.
            if (parsed.scheme not in {"http", "https"} or not parsed.netloc
                    or "\x00" in resource or "\\" in resource):
                raise ResearchError("unsafe source resource")
        elif namespace == "tool":
            if not (1 <= len(resource) <= 300) or "\x00" in resource or "\n" in resource or "\r" in resource:
                raise ResearchError("unsafe source resource")
            if ":" not in resource:
                raise ResearchError("unsafe source resource")
            tool_name, tool_text = resource.split(":", 1)
            if not re.fullmatch(r"[a-z][a-z0-9-]{0,31}", tool_name) or not tool_text.strip():
                raise ResearchError("unsafe source resource")
        elif (pure.is_absolute() or ".." in pure.parts or "\\" in resource
                or "\x00" in resource or parsed.scheme or "://" in resource
                or ":" in resource or re.match(r"^[A-Za-z]:", resource)):
            raise ResearchError("unsafe source resource")
        fragment = locator.get("fragment")
        if fragment is not None:
            fragment = _text(fragment, "source fragment", 500)
        digest = source.get("content_digest")
        if not isinstance(digest, str) or not _SHA256.fullmatch(digest):
            raise ResearchError("invalid source digest")
        result.append({
            "source_id": expected_id,
            "locator": {"namespace": namespace, "resource": resource, "fragment": fragment},
            "content_digest": digest,
            "title": _text(source.get("title"), "source title", 500),
        })
    return result


def validate_draft(document: object, sources: list[dict[str, object]]) -> dict[str, object]:
    """Validate agent-authored synthesis without accepting owned metadata."""
    if not isinstance(document, Mapping) or document.get("schema_version") != DRAFT_SCHEMA:
        raise ResearchError("invalid research draft schema")
    allowed = {
        "schema_version", "research_status", "objective", "requested_action",
        "current_state", "expected_deliverables", "timeline", "decisions",
        "dependencies", "constraints", "stakeholders", "related_entities",
        "findings", "conflicts", "open_questions", "scheduling_recommendations",
        "recommendation", "guide",
    }
    if set(document) - allowed:
        raise ResearchError("draft contains publisher-owned or unknown fields")
    status = document.get("research_status")
    if status not in RESEARCH_STATUSES:
        raise ResearchError("invalid research status")
    source_ids = {str(source["source_id"]) for source in sources}
    result: dict[str, object] = {
        "research_status": status,
        "objective": _claim(document.get("objective"), "objective", source_ids),
        "requested_action": _claim(document.get("requested_action"), "requested action", source_ids),
    }
    sections = (
        "current_state", "expected_deliverables", "timeline", "decisions",
        "dependencies", "constraints", "stakeholders", "related_entities",
        "findings", "conflicts", "open_questions",
    )
    for section in sections:
        values = document.get(section, [])
        if not isinstance(values, list) or len(values) > 32:
            raise ResearchError(f"invalid {section}")
        result[section] = [
            _claim(value, f"{section} claim", source_ids) for value in values
        ]
    if "recommendation" in document:
        rec_values = document.get("recommendation", [])
        if not isinstance(rec_values, list) or len(rec_values) > 32:
            raise ResearchError("invalid recommendation")
        result["recommendation"] = [
            _claim(value, "recommendation claim", source_ids) for value in rec_values
        ]
    if "guide" in document:
        result["guide"] = _claim(document.get("guide"), "guide claim", source_ids)
    recommendations = document.get("scheduling_recommendations", [])
    if not isinstance(recommendations, list) or len(recommendations) > 3:
        raise ResearchError("invalid scheduling recommendations")
    parsed = []
    for recommendation in recommendations:
        if not isinstance(recommendation, Mapping):
            raise ResearchError("invalid scheduling recommendation")
        kind = recommendation.get("type")
        confidence = recommendation.get("confidence")
        if kind not in RECOMMENDATION_TYPES or not isinstance(confidence, (int, float)) \
                or isinstance(confidence, bool) or not 0 <= confidence <= 1:
            raise ResearchError("invalid scheduling recommendation")
        item: dict[str, object] = {
            "type": kind,
            "confidence": float(confidence),
            "rationale": _claim(recommendation.get("rationale"), "recommendation rationale", source_ids),
        }
        if kind == "after_task_completed":
            predecessor = recommendation.get("related_task_id")
            if not isinstance(predecessor, int) or isinstance(predecessor, bool) or predecessor < 1:
                raise ResearchError("invalid predecessor task id")
            item["related_task_id"] = predecessor
        elif kind == "not_before":
            item["not_before"] = _timestamp(recommendation.get("not_before"), "not before")
        elif kind == "create_prerequisite":
            item["prerequisite_text"] = _text(
                recommendation.get("prerequisite_text"), "prerequisite text", 4_000
            )
        elif kind == "raise_priority":
            pass
        parsed.append(item)
    result["scheduling_recommendations"] = parsed
    return result


def render_markdown(document: Mapping[str, object]) -> str:
    """Render the sole deterministic human view of a published record."""
    identity = document["task_identity"]
    assert isinstance(identity, Mapping)
    lines = [
        "# Task Research",
        "",
        f"Task: `{identity['task_id']}` version {identity['task_version']}",
        f"Research status: **{document['research_status']}**",
        "",
    ]
    report = document["report"]
    assert isinstance(report, Mapping)
    for title, key in (("Objective", "objective"), ("Requested action", "requested_action")):
        claim = report[key]
        assert isinstance(claim, Mapping)
        refs = ", ".join(f"[{ref}]" for ref in claim["source_refs"])
        lines.extend([f"## {title}", "", f"{claim['text']} ({claim['status']}) {refs}".rstrip(), ""])
    if "guide" in report and report["guide"]:
        guide_claim = report["guide"]
        assert isinstance(guide_claim, Mapping)
        refs = ", ".join(f"[{ref}]" for ref in guide_claim["source_refs"])
        suffix = f" {refs}" if refs else ""
        lines.extend(["## Guide", "", f"{guide_claim['text']} ({guide_claim['status']}){suffix}".rstrip(), ""])
    for key in (
        "current_state", "expected_deliverables", "timeline", "decisions",
        "dependencies", "constraints", "stakeholders", "related_entities",
        "findings", "recommendation", "conflicts",
    ):
        claims = report.get(key)
        if not claims:
            continue
        lines.extend([f"## {key.replace('_', ' ').title()}", ""])
        for claim in claims:
            refs = ", ".join(f"[{ref}]" for ref in claim["source_refs"])
            suffix = f" {refs}" if refs else ""
            lines.append(f"- {claim['text']} ({claim['status']}){suffix}".rstrip())
        lines.append("")
    recommendations = document["scheduling_recommendations"]
    if recommendations:
        lines.extend(["## Scheduling recommendations", ""])
        for item in recommendations:
            rationale = item["rationale"]
            refs = ", ".join(f"[{ref}]" for ref in rationale["source_refs"])
            lines.append(f"- `{item['type']}`: {rationale['text']} {refs}".rstrip())
        lines.append("")
    lines.extend(["## Sources", ""])
    for source in document["sources"]:
        locator = source["locator"]
        fragment = f"#{locator['fragment']}" if locator["fragment"] else ""
        lines.append(
            f"- [{source['source_id']}] {source['title']} — "
            f"`{locator['namespace']}:{locator['resource']}{fragment}`"
        )
    return "\n".join(lines).rstrip() + "\n"


def consumer_projection(document: Mapping[str, object]) -> dict[str, object]:
    """Build a bounded evidence-only projection for downstream consumers."""
    report = document["report"]
    assert isinstance(report, Mapping)
    projection = {
        "schema_version": PUBLISHED_SCHEMA,
        "authority": "evidence_only",
        "task_identity": document["task_identity"],
        "research_status": document["research_status"],
        "objective": report["objective"],
        "requested_action": report["requested_action"],
        "findings": report["findings"],
        "dependencies": report["dependencies"],
        "constraints": report["constraints"],
        "open_questions": report["open_questions"],
        "scheduling_recommendations": document["scheduling_recommendations"],
    }
    if "guide" in report and report["guide"]:
        projection["guide"] = report["guide"]
    while len(_canonical_bytes(projection)) > MAX_PROJECTION_BYTES:
        for key in ("findings", "open_questions", "constraints", "dependencies"):
            values = projection[key]
            if values:
                values.pop()
                break
        else:
            raise ResearchError("mandatory research projection exceeds limit")
    return projection


def validate_provenance(document: object) -> dict[str, object]:
    """Validate supervisor-owned, content-free runtime provenance."""
    required = {
        "profile_id", "profile_revision", "model", "provider", "runtime",
        "reasoning_requested", "reasoning_effective",
    }
    if not isinstance(document, Mapping) or set(document) != required:
        raise ResearchError("invalid provenance")
    revision = document.get("profile_revision")
    if not isinstance(revision, str) or not _SHA256.fullmatch(revision):
        raise ResearchError("invalid profile revision")
    requested = document.get("reasoning_requested")
    effective = document.get("reasoning_effective")
    if requested not in {"low", "medium", "high", "xhigh"}:
        raise ResearchError("invalid reasoning configuration")
    if effective not in {"low", "medium", "high", "xhigh", "unknown"}:
        raise ResearchError("invalid effective reasoning configuration")
    return {
        "profile_id": _identifier(document.get("profile_id"), "profile id"),
        "profile_revision": revision,
        "model": _identifier(document.get("model"), "model"),
        "provider": _identifier(document.get("provider"), "provider"),
        "runtime": _identifier(document.get("runtime"), "runtime"),
        "reasoning_requested": requested,
        "reasoning_effective": effective,
    }


def validate_coverage(document: object) -> dict[str, object]:
    """Validate bounded broker-owned search coverage."""
    required = {
        "searched_namespaces", "queries", "documents_retrieved",
        "unavailable_source_ids", "knowledge_revisions",
    }
    if not isinstance(document, Mapping) or not required.issubset(set(document)):
        raise ResearchError("invalid coverage")
    allowed = required | {"degraded", "dropped_citations", "unsourced_claims"}
    if set(document) - allowed:
        raise ResearchError("invalid coverage")
    namespaces = document.get("searched_namespaces")
    unavailable = document.get("unavailable_source_ids")
    revisions = document.get("knowledge_revisions")
    queries = document.get("queries")
    documents = document.get("documents_retrieved")
    if (not isinstance(namespaces, list) or len(namespaces) > 5
            or len(set(namespaces)) != len(namespaces)
            or any(item not in SOURCE_NAMESPACES for item in namespaces)):
        raise ResearchError("invalid coverage namespaces")
    if (not isinstance(unavailable, list) or len(unavailable) > 32
            or any(not isinstance(item, str) or not _ID.fullmatch(item) for item in unavailable)):
        raise ResearchError("invalid unavailable source ids")
    if (not isinstance(revisions, Mapping) or len(revisions) > 16
            or any(not isinstance(key, str) or not _ID.fullmatch(key)
                   or not isinstance(value, str) or not _SHA256.fullmatch(value)
                   for key, value in revisions.items())):
        raise ResearchError("invalid knowledge revisions")
    if not isinstance(queries, int) or isinstance(queries, bool) or not 0 <= queries <= 20:
        raise ResearchError("invalid search count")
    if not isinstance(documents, int) or isinstance(documents, bool) or not 0 <= documents <= 50:
        raise ResearchError("invalid document count")
    res = {
        "searched_namespaces": sorted(namespaces),
        "queries": queries,
        "documents_retrieved": documents,
        "unavailable_source_ids": sorted(unavailable),
        "knowledge_revisions": dict(sorted(revisions.items())),
    }
    if "degraded" in document:
        degraded = document["degraded"]
        if not isinstance(degraded, bool):
            raise ResearchError("invalid degraded flag")
        res["degraded"] = degraded
        if "dropped_citations" in document:
            dc = document["dropped_citations"]
            if not isinstance(dc, int) or isinstance(dc, bool) or dc < 0:
                raise ResearchError("invalid dropped_citations count")
            res["dropped_citations"] = dc
        if "unsourced_claims" in document:
            uc = document["unsourced_claims"]
            if not isinstance(uc, int) or isinstance(uc, bool) or uc < 0:
                raise ResearchError("invalid unsourced_claims count")
            res["unsourced_claims"] = uc
    return res


class ResearchStore:
    """SQLite lifecycle plus receipt-gated private file publication."""

    def __init__(self, database_path: Path, cas_root: Path, *,
                 clock: Callable[[], datetime] | None = None) -> None:
        self.database_path = Path(database_path)
        self.cas_root = Path(cas_root)
        self._clock = clock or (lambda: datetime.now(timezone.utc))

    def _now(self) -> datetime:
        return self._clock().astimezone(timezone.utc)

    def _connect(self) -> sqlite3.Connection:
        if not self.database_path.is_file() or self.database_path.is_symlink():
            raise ResearchError("research database is not initialized")
        connection = sqlite3.connect(self.database_path, timeout=5)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        if int(connection.execute("PRAGMA user_version").fetchone()[0]) != SCHEMA_VERSION:
            connection.close()
            raise ResearchError("research database schema is not current")
        return connection

    @staticmethod
    def _job(row: sqlite3.Row) -> ResearchJob:
        return ResearchJob(row["job_id"], row["task_id"], row["task_version"],
                           row["generation"], row["input_digest"], row["state"])

    @staticmethod
    def _bound_task_folder(task_work_root: Path, task_folder: Path) -> tuple[Path, Path]:
        root = ResearchStore._safe_directory(Path(task_work_root), "task work root")
        folder = ResearchStore._safe_directory(Path(task_folder), "task folder")
        try:
            relative = folder.relative_to(root)
        except ValueError:
            raise ResearchError("task folder is outside its configured root")
        if not relative.parts:
            raise ResearchError("task folder is outside its configured root")
        return root, folder

    def request(self, snapshot: object, *, task_work_root: Path,
                task_folder: Path, refresh: bool = False) -> ResearchJob:
        """Explicitly queue research; no intake path calls this automatically."""
        now = self._now().isoformat().replace("+00:00", "Z")
        with closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                job = enqueue_in_transaction(
                    connection, snapshot, task_work_root=task_work_root,
                    task_folder=task_folder, now=now, refresh=refresh,
                )
            except BaseException:
                connection.rollback()
                raise
            connection.commit()
            return job

    def claim(
        self,
        worker_id: str,
        *,
        lease_seconds: int = 900,
        task_work_root: Path | None = None,
    ) -> ResearchClaim | None:
        worker_id = _identifier(worker_id, "worker id")
        # An agent Researcher may work for an hour and nothing renews the
        # lease meanwhile, so the claim must be able to outlast its budget.
        if (not isinstance(lease_seconds, int) or isinstance(lease_seconds, bool)
                or not 1 <= lease_seconds <= MAX_CLAIM_LEASE_SECONDS):
            raise ResearchError("invalid claim lease")
        bounded_root = (
            None
            if task_work_root is None
            else str(self._safe_directory(Path(task_work_root), "task work root"))
        )
        now_dt = self._now()
        now = now_dt.isoformat().replace("+00:00", "Z")
        expires = (now_dt + timedelta(seconds=lease_seconds)).isoformat().replace("+00:00", "Z")
        token = secrets.token_urlsafe(32)
        token_digest = _digest(token.encode())
        with closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            if bounded_root is None:
                row = connection.execute(
                    "SELECT * FROM task_research_jobs WHERE state='queued' "
                    "ORDER BY requested_at,job_id LIMIT 1"
                ).fetchone()
            else:
                row = connection.execute(
                    "SELECT * FROM task_research_jobs WHERE state='queued' "
                    "AND task_work_root=? ORDER BY requested_at,job_id LIMIT 1",
                    (bounded_root,),
                ).fetchone()
            if row is None:
                connection.commit()
                return None
            connection.execute(
                "UPDATE task_research_jobs SET state='running',attempts=attempts+1,updated_at=? "
                "WHERE job_id=?", (now, row["job_id"]),
            )
            connection.execute(
                "INSERT INTO task_research_claims(job_id,token_digest,worker_id,claimed_at,expires_at) "
                "VALUES(?,?,?,?,?)", (row["job_id"], token_digest, worker_id, now, expires),
            )
            connection.execute(
                "INSERT INTO task_research_events(job_id,task_id,kind,from_state,to_state,occurred_at) "
                "VALUES(?,?,'claimed','queued','running',?)", (row["job_id"], row["task_id"], now),
            )
            connection.commit()
            changed = dict(row)
            changed["state"] = "running"
            return ResearchClaim(self._job(changed), token)

    def context(self, job_id: str, token: str) -> dict[str, object]:
        """Return the stable, capability-fenced context for one claimed run."""
        now = self._now().isoformat().replace("+00:00", "Z")
        with closing(self._connect()) as connection:
            row = connection.execute(
                "SELECT j.job_id,j.task_id,j.task_version,j.input_digest,j.input_json,"
                "c.token_digest,c.expires_at FROM task_research_jobs j "
                "JOIN task_research_claims c ON c.job_id=j.job_id "
                "WHERE j.job_id=? AND j.state='running'", (job_id,),
            ).fetchone()
        if (row is None or _timestamp(row["expires_at"], "claim expiry") < now
                or not secrets.compare_digest(
                    row["token_digest"], _digest(token.encode())
                )):
            raise ResearchError("research claim is unavailable")
        return {
            "schema_version": "foxhound.task-research-context.v1",
            "job_id": row["job_id"],
            "task_identity": {
                "task_id": row["task_id"], "task_version": row["task_version"],
                "input_digest": row["input_digest"],
            },
            "task_snapshot": json.loads(row["input_json"]),
            "draft_contract": DRAFT_SCHEMA,
            "draft_filename": "draft-research.json",
            "authority": "evidence_only",
            "allowed_source_namespaces": sorted(SOURCE_NAMESPACES),
            "scheduling_recommendations_are_applied": False,
        }

    def fail(self, job_id: str, token: str, failure_code: str) -> str:
        """Release a failed claim for retry, parking it at the attempt limit."""
        failure_code = _identifier(failure_code, "failure code")
        now = self._now().isoformat().replace("+00:00", "Z")
        with closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT j.task_id,j.attempts,j.max_attempts,j.state,c.token_digest,c.expires_at "
                "FROM task_research_jobs j JOIN task_research_claims c ON c.job_id=j.job_id "
                "WHERE j.job_id=?", (job_id,),
            ).fetchone()
            if (row is None or row["state"] != "running"
                    or _timestamp(row["expires_at"], "claim expiry") < now
                    or not secrets.compare_digest(
                        row["token_digest"], _digest(token.encode())
                    )):
                connection.rollback()
                raise ResearchError("research claim is unavailable")
            if "timeout" in failure_code:
                target = "queued"
                attempts = max(0, row["attempts"] - 1)
            elif row["attempts"] >= row["max_attempts"]:
                target = "parked"
                attempts = row["attempts"]
            else:
                target = "queued"
                attempts = row["attempts"]
            kind = "parked" if target == "parked" else "retried"
            connection.execute(
                "UPDATE task_research_jobs SET state=?,attempts=?,failure_code=?,updated_at=? WHERE job_id=?",
                (target, attempts, failure_code, now, job_id),
            )
            connection.execute("DELETE FROM task_research_claims WHERE job_id=?", (job_id,))
            connection.execute(
                "INSERT INTO task_research_events(job_id,task_id,kind,from_state,to_state,occurred_at) "
                "VALUES(?,?,?,'running',?,?)", (job_id, row["task_id"], kind, target, now),
            )
            connection.commit()
            return target

    def recover_expired(self) -> dict[str, int]:
        """Requeue or park expired running leases without exposing capabilities."""
        now = self._now().isoformat().replace("+00:00", "Z")
        retried = parked = 0
        with closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            rows = connection.execute(
                "SELECT j.job_id,j.task_id,j.attempts,j.max_attempts FROM task_research_jobs j "
                "JOIN task_research_claims c ON c.job_id=j.job_id "
                "WHERE j.state='running' AND c.expires_at<=? ORDER BY j.job_id", (now,)
            ).fetchall()
            for row in rows:
                target = "parked" if row["attempts"] >= row["max_attempts"] else "queued"
                kind = "parked" if target == "parked" else "retried"
                connection.execute(
                    "UPDATE task_research_jobs SET state=?,failure_code='lease_expired',"
                    "updated_at=? WHERE job_id=?", (target, now, row["job_id"]),
                )
                connection.execute("DELETE FROM task_research_claims WHERE job_id=?", (row["job_id"],))
                connection.execute(
                    "INSERT INTO task_research_events(job_id,task_id,kind,from_state,to_state,"
                    "occurred_at) VALUES(?,?,?,'running',?,?)",
                    (row["job_id"], row["task_id"], kind, target, now),
                )
                if target == "parked":
                    parked += 1
                else:
                    retried += 1
            connection.commit()
        return {"retried": retried, "parked": parked}

    @staticmethod
    def _safe_directory(path: Path, name: str) -> Path:
        resolved = path.resolve(strict=False)
        if not path.is_dir() or path.is_symlink() or not resolved.is_dir() or path != resolved:
            raise ResearchError(f"unsafe {name}")
        if resolved == Path(resolved.anchor):
            raise ResearchError(f"unsafe {name}")
        return resolved

    @classmethod
    def _validate_cas_component(cls, base: Path, relative_str: str) -> Path:
        """Validate that each subcomponent in relative_str exists without symlinks or escaping base."""
        parts = PurePosixPath(relative_str).parts
        if not parts or any(p in (".", "..") for p in parts):
            raise ResearchError("unsafe CAS path component")
        current = base
        for part in parts:
            current = current / part
            # Check lstat on each intermediate component before traversing
            try:
                st = current.lstat()
            except OSError:
                raise ResearchError(f"CAS component missing or inaccessible: {part}")
            if os.path.islink(current) or not current.is_dir():
                raise ResearchError(f"unsafe CAS component: {part}")
            if not current.resolve(strict=True).is_relative_to(base):
                raise ResearchError("CAS component escapes root")
        return current

    @classmethod
    def _folder_for_row(cls, row: Mapping[str, object]) -> Path:
        _, folder = cls._bound_task_folder(
            Path(str(row["task_work_root"])), Path(str(row["task_folder"]))
        )
        return folder

    @staticmethod
    def _install(path: Path, payload: bytes) -> None:
        temporary = path.with_name(f".{path.name}.tmp-{secrets.token_hex(8)}")
        descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        try:
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            if path.exists() and path.is_symlink():
                raise ResearchError("research output target is a symlink")
            os.replace(temporary, path)
            os.chmod(path, 0o600)
            directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        finally:
            if temporary.exists():
                temporary.unlink()

    def publish(self, *, job_id: str, token: str, draft: object, sources: object,
                provenance: Mapping[str, object],
                coverage: Mapping[str, object]) -> dict[str, object]:
        """Validate, stage in CAS, install both views, then commit the receipt."""
        job_id = _identifier(job_id, "research job id")
        if not isinstance(token, str) or not token:
            raise ResearchError("invalid claim token")
        parsed_sources = validate_sources(sources)
        parsed_draft = validate_draft(draft, parsed_sources)
        parsed_provenance = validate_provenance(provenance)
        parsed_coverage = validate_coverage(coverage)
        cas_root = self._safe_directory(self.cas_root, "research CAS")
        now = self._now().isoformat().replace("+00:00", "Z")
        with closing(self._connect()) as connection:
            row = connection.execute(
                "SELECT j.*,c.token_digest,c.expires_at FROM task_research_jobs j "
                "JOIN task_research_claims c ON c.job_id=j.job_id WHERE j.job_id=?", (job_id,)
            ).fetchone()
            if row is None or row["state"] != "running":
                raise ResearchError("research claim is unavailable")
            if not secrets.compare_digest(row["token_digest"], _digest(token.encode())):
                raise ResearchError("research claim token mismatch")
            if _timestamp(row["expires_at"], "claim expiry") < now:
                raise ResearchError("research claim expired")
            folder = self._folder_for_row(row)
            owned_provenance = {
                "job_id": job_id,
                "generated_at": now,
                **parsed_provenance,
            }
            final = {
                "schema_version": PUBLISHED_SCHEMA,
                "authority": "evidence_only",
                "task_identity": {
                    "task_id": row["task_id"], "task_version": row["task_version"],
                    "input_digest": row["input_digest"], "generation": row["generation"],
                },
                "provenance": owned_provenance,
                "coverage": parsed_coverage,
                "research_status": parsed_draft["research_status"],
                "report": {key: value for key, value in parsed_draft.items()
                           if key not in {"research_status", "scheduling_recommendations"}},
                "scheduling_recommendations": parsed_draft["scheduling_recommendations"],
                "sources": parsed_sources,
            }
            json_payload = _canonical_bytes(final)
            markdown_payload = render_markdown(final).encode("utf-8")
            if len(json_payload) > MAX_JSON_BYTES or len(markdown_payload) > MAX_MARKDOWN_BYTES:
                raise ResearchError("published research is too large")
            json_digest = _digest(json_payload)
            markdown_digest = _digest(markdown_payload)
            prefix = cas_root / json_digest[:2]
            for directory in (prefix, prefix / json_digest):
                try:
                    directory.mkdir(mode=0o700)
                except FileExistsError:
                    pass
                if directory.is_symlink() or not directory.is_dir() \
                        or not directory.resolve(strict=True).is_relative_to(cas_root):
                    raise ResearchError("unsafe research CAS generation")
            generation_dir = prefix / json_digest
            self._install(generation_dir / ".task-research.json", json_payload)
            self._install(generation_dir / "Research.md", markdown_payload)
            connection.execute("BEGIN IMMEDIATE")
            current = connection.execute(
                "SELECT j.state,j.task_version,t.version AS current_task_version,"
                "c.token_digest,c.expires_at FROM task_research_jobs j "
                "JOIN tasks t ON t.id=j.task_id JOIN task_research_claims c ON c.job_id=j.job_id "
                "WHERE j.job_id=?", (job_id,)
            ).fetchone()
            if (current is None or current["state"] != "running"
                    or current["current_task_version"] != current["task_version"]
                    or current["expires_at"] < now
                    or not secrets.compare_digest(
                        current["token_digest"], _digest(token.encode())
                    )):
                connection.rollback()
                raise ResearchError("research job or task changed during publication")
            connection.execute(
                "UPDATE task_research_jobs SET state='publishing',research_status=?,"
                "pending_json_digest=?,pending_markdown_digest=?,updated_at=? WHERE job_id=?",
                (parsed_draft["research_status"], json_digest, markdown_digest, now, job_id),
            )
            connection.execute(
                "INSERT INTO task_research_events(job_id,task_id,kind,from_state,to_state,occurred_at) "
                "VALUES(?,?,'publishing','running','publishing',?)", (job_id, row["task_id"], now),
            )
            connection.commit()
            self._install(folder / ".task-research.json", json_payload)
            self._install(folder / "Research.md", markdown_payload)
            if _digest((folder / ".task-research.json").read_bytes()) != json_digest \
                    or _digest((folder / "Research.md").read_bytes()) != markdown_digest:
                raise ResearchError("installed research digest mismatch")
            connection.execute("BEGIN IMMEDIATE")
            final_check = connection.execute(
                "SELECT j.state, j.task_version, j.generation, j.pending_json_digest, "
                "j.pending_markdown_digest, t.version AS current_task_version "
                "FROM task_research_jobs j JOIN tasks t ON t.id=j.task_id "
                "WHERE j.job_id=?", (job_id,)
            ).fetchone()
            if (final_check is None
                    or final_check["state"] != "publishing"
                    or final_check["task_version"] != row["task_version"]
                    or final_check["current_task_version"] != row["task_version"]
                    or final_check["generation"] != row["generation"]
                    or final_check["pending_json_digest"] != json_digest
                    or final_check["pending_markdown_digest"] != markdown_digest):
                connection.rollback()
                raise ResearchError("research job or task version changed before completion")
            receipt_cursor = connection.execute(
                "INSERT INTO task_research_receipts(job_id,json_digest,markdown_digest,cas_digest,published_at) "
                "VALUES(?,?,?,?,?)", (job_id, json_digest, markdown_digest, json_digest, now),
            )
            if receipt_cursor.rowcount != 1:
                connection.rollback()
                raise ResearchError("failed to record research receipt")
            update_cursor = connection.execute(
                "UPDATE task_research_jobs SET state='completed',research_status=?,updated_at=?,"
                "completed_at=? WHERE job_id=? AND state='publishing' AND task_version=? AND generation=?",
                (parsed_draft["research_status"], now, now, job_id, row["task_version"], row["generation"]),
            )
            if update_cursor.rowcount != 1:
                connection.rollback()
                raise ResearchError("failed to complete research job")
            connection.execute("DELETE FROM task_research_claims WHERE job_id=?", (job_id,))
            connection.execute(
                "INSERT INTO task_research_events(job_id,task_id,kind,from_state,to_state,occurred_at) "
                "VALUES(?,?,'completed','publishing','completed',?)", (job_id, row["task_id"], now),
            )
            connection.commit()
            return final

    def repair(self, job_id: str) -> bool:
        """Finish or restore publication from CAS without repeating model work."""
        cas_root = self._safe_directory(self.cas_root, "research CAS")
        with closing(self._connect()) as connection:
            row = connection.execute(
                "SELECT j.*,r.json_digest,r.markdown_digest,r.cas_digest "
                "FROM task_research_jobs j LEFT JOIN task_research_receipts r "
                "ON r.job_id=j.job_id WHERE j.job_id=?", (job_id,)
            ).fetchone()
            if row is None or row["state"] not in {"publishing", "completed"}:
                return False
            folder = self._folder_for_row(row)
            json_digest = row["json_digest"] or row["pending_json_digest"]
            markdown_digest = row["markdown_digest"] or row["pending_markdown_digest"]
            if not json_digest or not markdown_digest:
                raise ResearchError("research publication has no recovery digests")
            source = self._validate_cas_component(cas_root, f"{json_digest[:2]}/{json_digest}")
            json_file = source / ".task-research.json"
            markdown_file = source / "Research.md"
            if os.path.islink(json_file) or not json_file.is_file():
                raise ResearchError("unsafe CAS json file")
            if os.path.islink(markdown_file) or not markdown_file.is_file():
                raise ResearchError("unsafe CAS markdown file")
            json_payload = json_file.read_bytes()
            markdown_payload = markdown_file.read_bytes()
            if _digest(json_payload) != json_digest or _digest(markdown_payload) != markdown_digest:
                raise ResearchError("research CAS is corrupt")
            self._install(folder / ".task-research.json", json_payload)
            self._install(folder / "Research.md", markdown_payload)
            if row["state"] == "publishing":
                now = self._now().isoformat().replace("+00:00", "Z")
                connection.execute("BEGIN IMMEDIATE")
                receipt_cursor = connection.execute(
                    "INSERT INTO task_research_receipts(job_id,json_digest,markdown_digest,"
                    "cas_digest,published_at) VALUES(?,?,?,?,?)",
                    (job_id, json_digest, markdown_digest, json_digest, now),
                )
                if receipt_cursor.rowcount != 1:
                    connection.rollback()
                    raise ResearchError("failed to record research receipt")
                update_cursor = connection.execute(
                    "UPDATE task_research_jobs SET state='completed',updated_at=?,completed_at=? "
                    "WHERE job_id=? AND state='publishing'", (now, now, job_id),
                )
                if update_cursor.rowcount != 1:
                    connection.rollback()
                    raise ResearchError("failed to complete research job")
                connection.execute("DELETE FROM task_research_claims WHERE job_id=?", (job_id,))
                connection.execute(
                    "INSERT INTO task_research_events(job_id,task_id,kind,from_state,to_state,"
                    "occurred_at) VALUES(?,?,'recovered','publishing','completed',?)",
                    (job_id, row["task_id"], now),
                )
                connection.commit()
            return True

    def projection(self, task_id: int, task_version: int,
                   input_digest: str) -> dict[str, object] | None:
        """Return only a current, receipt-authorized, bounded projection."""
        with closing(self._connect()) as connection:
            row = connection.execute(
                "SELECT r.json_digest,r.markdown_digest,j.task_work_root,j.task_folder "
                "FROM task_research_jobs j "
                "JOIN task_research_receipts r ON r.job_id=j.job_id "
                "WHERE j.task_id=? AND j.task_version=? AND j.input_digest=? "
                "AND j.state='completed' ORDER BY j.generation DESC LIMIT 1",
                (task_id, task_version, input_digest),
            ).fetchone()
        return _receipt_projection(row)


def _receipt_document(row: Mapping[str, object] | None) -> dict[str, object] | None:
    """The receipt-verified published document, or None."""
    if row is None:
        return None
    try:
        folder = ResearchStore._folder_for_row(row)
        json_payload = (folder / ".task-research.json").read_bytes()
        markdown_payload = (folder / "Research.md").read_bytes()
    except (OSError, ResearchError):
        return None
    if _digest(json_payload) != row["json_digest"] or _digest(markdown_payload) != row["markdown_digest"]:
        return None
    try:
        document = json.loads(json_payload)
    except ValueError:
        return None
    return document if isinstance(document, dict) else None


def _receipt_projection(row: Mapping[str, object] | None) -> dict[str, object] | None:
    """Read both receipt-authorized views and project them, or return None."""
    document = _receipt_document(row)
    if document is None:
        return None
    try:
        return consumer_projection(document)
    except (ValueError, KeyError, TypeError, ResearchError):
        return None


def version_document(
    connection: sqlite3.Connection, task_id: int, task_version: int,
) -> tuple[str, dict[str, object]] | None:
    """(job id, verified document) of the newest receipt for one task version.

    Unlike the consumer projection this includes the stakeholders section,
    which carries the Researcher's ownership verdict.
    """
    previous = connection.row_factory
    connection.row_factory = sqlite3.Row
    try:
        row = connection.execute(
            "SELECT j.job_id,r.json_digest,r.markdown_digest,j.task_work_root,"
            "j.task_folder FROM task_research_jobs j "
            "JOIN task_research_receipts r ON r.job_id=j.job_id "
            "WHERE j.task_id=? AND j.task_version=? AND j.state='completed' "
            "ORDER BY j.generation DESC LIMIT 1",
            (task_id, task_version),
        ).fetchone()
    finally:
        connection.row_factory = previous
    document = _receipt_document(row)
    if document is None:
        return None
    return str(row["job_id"]), document


def enqueue_in_transaction(
    connection: sqlite3.Connection,
    snapshot: object,
    *,
    task_work_root: Path,
    task_folder: Path,
    now: str,
    refresh: bool = False,
) -> ResearchJob:
    """Queue research inside a transaction the caller already holds.

    This is the whole of ``ResearchStore.request`` minus the transaction, so
    a caller that is itself deciding something about the task in the same
    database (the execution claim gate, for one) can request research
    atomically with that decision.  It runs no model and contacts nothing.
    """
    normalized = normalize_task_snapshot(snapshot)
    root, folder = ResearchStore._bound_task_folder(task_work_root, task_folder)
    payload = _canonical_bytes(normalized)
    digest = _digest(payload)
    raw_task_id = normalized["task_id"]
    assert isinstance(raw_task_id, int)
    task_id = raw_task_id
    task_version = int(normalized["task_version"])
    task = connection.execute("SELECT version,text FROM tasks WHERE id=?", (task_id,)).fetchone()
    # The broker owns the ledger snapshot.  Callers may request a specific
    # version, but cannot smuggle an unrelated task body into the durable
    # research record.
    if (task is None or task["version"] != task_version
            or normalized["text"] != task["text"]):
        raise ResearchError("task snapshot is stale or does not match ledger")
    existing = connection.execute(
        "SELECT * FROM task_research_jobs WHERE task_id=? AND task_version=? "
        "AND input_digest=? ORDER BY generation DESC LIMIT 1",
        (task_id, task_version, digest),
    ).fetchone()
    if not refresh and existing is not None and existing["state"] in {
        "queued", "running", "publishing", "completed"
    }:
        return ResearchStore._job(existing)
    active = connection.execute(
        "SELECT job_id,state FROM task_research_jobs WHERE task_id=? "
        "AND state IN ('queued','running','publishing')", (task_id,)
    ).fetchone()
    if active is not None:
        if active["state"] == "publishing":
            # Fail-closed: refuse refresh while a publication is in flight
            # to prevent canceling or racing a committing deliverable.
            raise ResearchError("task research publication is currently in progress")
        connection.execute(
            "UPDATE task_research_jobs SET state='canceled',updated_at=? WHERE job_id=?",
            (now, active["job_id"]),
        )
        connection.execute("DELETE FROM task_research_claims WHERE job_id=?", (active["job_id"],))
        connection.execute(
            "INSERT INTO task_research_events(job_id,task_id,kind,from_state,to_state,occurred_at) "
            "VALUES(?,?,'canceled',?,'canceled',?)",
            (active["job_id"], task_id, active["state"], now),
        )
    generation = connection.execute(
        "SELECT COALESCE(MAX(generation),0)+1 FROM task_research_jobs WHERE task_id=?",
        (task_id,),
    ).fetchone()[0]
    job_id = "research-" + secrets.token_hex(16)
    connection.execute(
        "INSERT INTO task_research_jobs(job_id,task_id,task_version,generation,input_digest,"
        "input_json,task_work_root,task_folder,state,attempts,max_attempts,requested_at,updated_at) "
        "VALUES(?,?,?,?,?,?,?,?,'queued',0,3,?,?)",
        (job_id, task_id, task_version, generation, digest, payload.decode(),
         str(root), str(folder), now, now),
    )
    connection.execute(
        "INSERT INTO task_research_events(job_id,task_id,kind,to_state,occurred_at) "
        "VALUES(?,?,'requested','queued',?)", (job_id, task_id, now),
    )
    row = connection.execute("SELECT * FROM task_research_jobs WHERE job_id=?", (job_id,)).fetchone()
    return ResearchStore._job(row)


def version_projection(
    connection: sqlite3.Connection, task_id: int, task_version: int,
) -> dict[str, object] | None:
    """The newest receipt-authorized projection for one exact task version."""
    previous = connection.row_factory
    connection.row_factory = sqlite3.Row
    try:
        row = connection.execute(
            "SELECT r.json_digest,r.markdown_digest,j.task_work_root,j.task_folder "
            "FROM task_research_jobs j "
            "JOIN task_research_receipts r ON r.job_id=j.job_id "
            "WHERE j.task_id=? AND j.task_version=? AND j.state='completed' "
            "ORDER BY j.generation DESC LIMIT 1",
            (task_id, task_version),
        ).fetchone()
    finally:
        connection.row_factory = previous
    return _receipt_projection(row)


def _load(path: Path) -> object:
    return json.loads(path.read_text(encoding="utf-8"))


def _safe_claim_parent(path: Path) -> Path:
    try:
        parent = path.parent
        parent_stat = parent.stat()
    except OSError:
        raise ResearchError("claim directory is unavailable")
    if not stat.S_ISDIR(parent_stat.st_mode) or parent.is_symlink() or parent_stat.st_mode & 0o077:
        raise ResearchError("claim directory is not private")
    return parent


def _write_claim(path: Path, claim: ResearchClaim) -> None:
    _safe_claim_parent(path)
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags, 0o600)
    try:
        payload = _canonical_bytes({"job_id": claim.job.job_id, "claim_token": claim.token})
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
    except Exception:
        if path.exists():
            path.unlink(missing_ok=True)
        raise


def _read_claim(path: Path) -> tuple[str, str]:
    _safe_claim_parent(path)
    try:
        info = path.lstat()
    except OSError:
        raise ResearchError("claim file is unavailable")
    if path.is_symlink() or not stat.S_ISREG(info.st_mode) or stat.S_IMODE(info.st_mode) != 0o600:
        raise ResearchError("unsafe claim file")
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags)
    try:
        with os.fdopen(descriptor, "rb") as handle:
            content = handle.read()
    except Exception:
        raise ResearchError("failed to read claim file")
    try:
        document = json.loads(content.decode("utf-8"))
    except Exception:
        raise ResearchError("invalid claim file")
    if not isinstance(document, Mapping) or set(document) != {"job_id", "claim_token"}:
        raise ResearchError("invalid claim file")
    return _identifier(document["job_id"], "job id"), _text(
        document["claim_token"], "claim token", 200
    )  # type: ignore[return-value]


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="foxhound-task-research")
    parser.add_argument("--database", required=True, type=Path)
    parser.add_argument("--cas-root", required=True, type=Path)
    commands = parser.add_subparsers(dest="command", required=True)
    request = commands.add_parser("request")
    request.add_argument("--snapshot", required=True, type=Path)
    request.add_argument("--task-work-root", required=True, type=Path)
    request.add_argument("--task-folder", required=True, type=Path)
    request.add_argument("--refresh", action="store_true")
    claim = commands.add_parser("claim")
    claim.add_argument("--worker-id", required=True)
    claim.add_argument("--claim-file", required=True, type=Path)
    context = commands.add_parser("context")
    context.add_argument("--claim-file", required=True, type=Path)
    fail = commands.add_parser("fail")
    fail.add_argument("--claim-file", required=True, type=Path)
    fail.add_argument("--failure-code", required=True)
    commands.add_parser("recover-expired")
    publish = commands.add_parser("publish")
    publish.add_argument("--claim-file", required=True, type=Path)
    publish.add_argument("--draft", required=True, type=Path)
    publish.add_argument("--sources", required=True, type=Path)
    publish.add_argument("--provenance", required=True, type=Path)
    publish.add_argument("--coverage", required=True, type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    arguments = _parser().parse_args(argv)
    store = ResearchStore(arguments.database, arguments.cas_root)
    try:
        if arguments.command == "request":
            job = store.request(
                _load(arguments.snapshot), task_work_root=arguments.task_work_root,
                task_folder=arguments.task_folder, refresh=arguments.refresh,
            )
            result = {"accepted": True, "job_id": job.job_id, "state": job.state}
        elif arguments.command == "claim":
            claim = store.claim(arguments.worker_id)
            if claim is not None:
                _write_claim(arguments.claim_file, claim)
            result = {"accepted": True, "claim": None if claim is None else {
                "job_id": claim.job.job_id, "claim_file": str(arguments.claim_file),
                "task_id": claim.job.task_id, "task_version": claim.job.task_version,
                "input_digest": claim.job.input_digest,
            }}
        elif arguments.command == "context":
            job_id, token = _read_claim(arguments.claim_file)
            result = {"accepted": True, "context": store.context(
                job_id, token
            )}
        elif arguments.command == "fail":
            job_id, token = _read_claim(arguments.claim_file)
            result = {"accepted": True, "state": store.fail(
                job_id, token, arguments.failure_code
            )}
        elif arguments.command == "recover-expired":
            result = {"accepted": True, **store.recover_expired()}
        else:
            job_id, token = _read_claim(arguments.claim_file)
            document = store.publish(
                job_id=job_id, token=token,
                draft=_load(arguments.draft), sources=_load(arguments.sources),
                provenance=_load(arguments.provenance), coverage=_load(arguments.coverage),
            )
            result = {"accepted": True, "document_digest": _digest(_canonical_bytes(document))}
    except (OSError, ValueError, sqlite3.Error, ResearchError):
        print("foxhound task research: operation refused", file=sys.stderr)
        return 70
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
