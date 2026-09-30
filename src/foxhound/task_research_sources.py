"""Bounded, read-only source evidence for task research.

The task database chooses the forge target.  Neither task prose nor model
output can redirect these reads to another repository or thread.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path

from . import forge_thread
from .knowledge_client import KnowledgeDocument
from .task_ledger import TaskLedger


_PART = re.compile(r"^[A-Za-z0-9_.-]{1,100}$")
_MAX_SNAPSHOT_BYTES = 64 * 1024


class ResearchSourceError(RuntimeError):
    """The claimed task cannot safely resolve its bounded source."""


@dataclass(frozen=True)
class BoundResearchSources:
    documents: tuple[tuple[str, KnowledgeDocument], ...]
    attempted_namespaces: tuple[str, ...] = ()
    unavailable_source_ids: tuple[str, ...] = ()


def _forge_target(record_id: str, item_id: str, kind: str) -> tuple[str, str]:
    parts = record_id.split("/")
    if (
        len(parts) != 3
        or parts[0] != "github.com"
        or not all(_PART.fullmatch(part) for part in parts[1:])
    ):
        raise ResearchSourceError("bounded forge origin is invalid")
    number = item_id.split("/", 1)[0] if kind == "review_request" else item_id
    if not number.isdigit() or int(number) < 1:
        raise ResearchSourceError("bounded forge item is invalid")
    route = "pull" if kind == "review_request" else "issues"
    return number, f"{record_id}/{route}/{number}"


def _snapshot_document(
    ledger: TaskLedger,
    *,
    task_id: int,
    origin,
    resource: str,
) -> KnowledgeDocument | None:
    raw = ledger.bound_candidate_payload(task_id)
    if raw is None or len(raw.encode("utf-8")) > _MAX_SNAPSHOT_BYTES:
        return None
    try:
        payload = json.loads(raw)
        source = payload["source"]
    except (KeyError, TypeError, ValueError):
        raise ResearchSourceError("bounded source snapshot is invalid") from None
    if not isinstance(source, dict) or (
        source.get("system"), source.get("kind"), source.get("record_id"),
        source.get("item_id"),
    ) != (origin.system, origin.kind, origin.record_id, origin.item_id):
        raise ResearchSourceError("bounded source snapshot does not match origin")
    excerpt = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    return KnowledgeDocument(
        id="repo:bound-origin-snapshot",
        path=f"{resource}/source-snapshot.json",
        excerpt=excerpt,
    )


def _live_document(origin, *, number: str, resource: str) -> KnowledgeDocument:
    if origin.kind == "issue":
        thread = forge_thread.read_issue_thread(
            repository=origin.record_id, number=number,
        )
    else:
        thread = forge_thread.read_pull_request_thread(
            repository=origin.record_id, number=number,
        )
    excerpt = json.dumps({
        "title": thread.title,
        "body": thread.body,
        "state": thread.state,
        "comments": thread.comments,
        "reviews": thread.reviews,
        "truncated": thread.truncated,
    }, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    return KnowledgeDocument(
        id="repo:bound-origin-live",
        path=f"{resource}/live-thread.json",
        excerpt=excerpt,
    )


def bound_research_sources(
    database_path: Path | str,
    *,
    task_id: int,
    task_version: int,
) -> BoundResearchSources:
    """Return evidence for the exact accepted forge origin of one task."""
    ledger = TaskLedger(database_path)
    task = ledger.get(task_id)
    if task is None or task.version != task_version:
        raise ResearchSourceError("research task version is stale")
    origin = ledger.origin(task_id)
    if origin is None or origin.kind not in {"issue", "review_request"}:
        return BoundResearchSources(())
    if origin.system != "gw":
        raise ResearchSourceError("bounded forge origin is unsupported")
    number, resource = _forge_target(origin.record_id, origin.item_id, origin.kind)
    documents: list[tuple[str, KnowledgeDocument]] = []
    snapshot = _snapshot_document(
        ledger, task_id=task_id, origin=origin, resource=resource,
    )
    if snapshot is not None:
        documents.append(("repo", snapshot))
    unavailable: tuple[str, ...] = ()
    try:
        documents.append(("repo", _live_document(
            origin, number=number, resource=resource,
        )))
    except forge_thread.ForgeThreadError:
        unavailable = ("repo:bound-origin-live",)
    return BoundResearchSources(
        tuple(documents), attempted_namespaces=("repo",),
        unavailable_source_ids=unavailable,
    )
