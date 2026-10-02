"""Agent-driven task research synthesizer powered by Hermes."""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, Callable, Mapping, Sequence
from urllib.parse import urlsplit

from . import task_research_synthesis
from .task_research_synthesis import (
    DRAFT_SCHEMA,
    SynthesisError,
    SynthesisResult,
    _json_bytes,
    _validate_resource_locator,
    validate_draft,
)


@dataclass(frozen=True)
class AgentResearchConfig:
    hermes_command: str
    model: str = "thinking_no"
    provider: str | None = None
    toolsets: str = "terminal,file,web,browser"
    max_turns: int = 120
    timeout_seconds: int = 3600
    prompt_path: Path | None = None
    knowledge_roots: tuple[tuple[str, str], ...] = ()
    profile_id: str = "researcher"
    profile_revision: str = "agent-researcher-v1"


def _default_prompt_path() -> Path:
    return Path(__file__).resolve().parent / "prompts" / "research_agent.md"


def _load_prompt(prompt_path: Path | None) -> str:
    path = prompt_path or _default_prompt_path()
    try:
        return path.read_text(encoding="utf-8")
    except OSError as exc:
        raise SynthesisError("runtime_failed") from exc


def _derive_knowledge_roots(config: AgentResearchConfig) -> list[dict[str, str]]:
    roots: list[dict[str, str]] = []
    for name, path in config.knowledge_roots:
        roots.append({"name": name, "path": str(path)})
    return roots


def _derive_starting_points(ctx: Mapping[str, Any], bound_sources: Any) -> list[dict[str, Any]]:
    starting_points: list[dict[str, Any]] = []
    origin = ctx.get("origin")
    if isinstance(origin, Mapping):
        starting_points.append(dict(origin))
    elif origin is not None:
        starting_points.append({"origin": str(origin)})
    for key in ("origin_narration", "origin_window", "participants", "calendar_event"):
        if key in ctx and ctx[key] is not None:
            starting_points.append({key: ctx[key]})

    documents = getattr(bound_sources, "documents", ())
    for layer, doc in documents:
        item: dict[str, Any] = {"layer": layer, "path": doc.path}
        if getattr(doc, "section", None):
            item["section"] = doc.section
        starting_points.append(item)
    return starting_points


def _map_locator(
    loc_str: str,
    knowledge_roots: Sequence[tuple[str, str]],
) -> tuple[str, str, str | None] | None:
    """Map locator string to (namespace, resource, fragment). Return None if unmappable."""
    if not isinstance(loc_str, str) or not loc_str.strip():
        return None
    loc = loc_str.strip()

    # Web URL check
    if loc.startswith("http://") or loc.startswith("https://"):
        fragment = None
        if "#" in loc:
            loc_base, fragment = loc.split("#", 1)
        else:
            loc_base = loc
        try:
            _validate_resource_locator(loc_base, namespace="web")
            return "web", loc_base, fragment
        except SynthesisError:
            return None

    # Check for fragment/section (e.g. path#section or path:line)
    fragment = None
    resource_candidate = loc
    if "#" in loc:
        resource_candidate, fragment = loc.split("#", 1)

    # Check knowledge roots
    norm_roots = [(name, os.path.abspath(os.path.expanduser(p))) for name, p in knowledge_roots]
    abs_cand = os.path.abspath(os.path.expanduser(resource_candidate))

    matched_root = None
    for name, rpath in norm_roots:
        if abs_cand == rpath or abs_cand.startswith(rpath.rstrip(os.sep) + os.sep):
            matched_root = (name, rpath)
            break

    if matched_root:
        name, rpath = matched_root
        rel_path = os.path.relpath(abs_cand, rpath).replace("\\", "/")
        if name == "kb":
            ns = "kb"
        elif name == "attachments":
            ns = "attachment"
        elif name == "emails":
            ns = "email"
        elif name == "repo":
            ns = "repo"
        else:
            ns = name

        try:
            _validate_resource_locator(rel_path, namespace=ns)
            return ns, rel_path, fragment
        except SynthesisError:
            return None

    # Maybe relative path directly given (e.g. repo or relative path)
    clean_rel = resource_candidate.lstrip("/")
    # Check if namespace is prefix
    for prefix, ns in (("emails/", "email"), ("attachments/", "attachment"), ("repo/", "repo"), ("kb/", "kb")):
        if clean_rel.startswith(prefix):
            rel = clean_rel[len(prefix):]
            try:
                _validate_resource_locator(rel, namespace=ns)
                return ns, rel, fragment
            except SynthesisError:
                pass

    try:
        _validate_resource_locator(clean_rel, namespace="repo")
        return "repo", clean_rel, fragment
    except SynthesisError:
        return None


def _make_source_receipt(
    namespace: str,
    resource: str,
    fragment: str | None,
    source_id: str,
) -> dict[str, Any]:
    locator = {
        "namespace": namespace,
        "resource": resource,
        "fragment": fragment,
    }
    content_digest = hashlib.sha256(_json_bytes(locator)).hexdigest()
    title = PurePosixPath(resource).name or resource
    return {
        "source_id": source_id,
        "locator": locator,
        "content_digest": content_digest,
        "title": title,
    }


def _process_evidence_and_refs(
    raw_evidence: Sequence[str] | None,
    sources_by_locator: dict[str, dict[str, Any]],
    sources_list: list[dict[str, Any]],
    knowledge_roots: Sequence[tuple[str, str]],
) -> list[str]:
    refs: list[str] = []
    if not raw_evidence:
        return refs
    for item in raw_evidence:
        if not isinstance(item, str):
            continue
        mapped = _map_locator(item, knowledge_roots)
        if mapped is None:
            continue
        ns, res, frag = mapped
        key = f"{ns}::{res}::{frag}"
        if key not in sources_by_locator:
            src_id = f"src-{len(sources_list) + 1:03d}"
            receipt = _make_source_receipt(ns, res, frag, src_id)
            sources_by_locator[key] = receipt
            sources_list.append(receipt)
        ref_id = sources_by_locator[key]["source_id"]
        if ref_id not in refs:
            refs.append(ref_id)
    return refs


def _make_claim(
    text: str,
    status: str,
    raw_evidence: Sequence[str] | None,
    sources_by_locator: dict[str, dict[str, Any]],
    sources_list: list[dict[str, Any]],
    knowledge_roots: Sequence[tuple[str, str]],
) -> dict[str, Any]:
    refs = _process_evidence_and_refs(raw_evidence, sources_by_locator, sources_list, knowledge_roots)
    if status != "unknown" and not refs:
        status = "unknown"
    return {
        "text": text,
        "status": status,
        "source_refs": refs,
    }


def _convert_research_json(
    raw: Mapping[str, Any],
    knowledge_roots: Sequence[tuple[str, str]],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    sources_by_locator: dict[str, dict[str, Any]] = {}
    sources_list: list[dict[str, Any]] = []

    # objective and requested_action <- requested_deliverable
    rd = raw.get("requested_deliverable")
    if isinstance(rd, Mapping):
        rd_text = str(rd.get("text", "")).strip() or "Perform requested task"
        rd_evidence = rd.get("evidence", [])
    elif isinstance(rd, str) and rd.strip():
        rd_text = rd.strip()
        rd_evidence = []
    else:
        rd_text = "Perform requested task"
        rd_evidence = []

    objective = _make_claim(
        rd_text,
        "supported" if rd_evidence else "inferred",
        rd_evidence,
        sources_by_locator,
        sources_list,
        knowledge_roots,
    )
    requested_action = _make_claim(
        rd_text,
        "supported" if rd_evidence else "inferred",
        rd_evidence,
        sources_by_locator,
        sources_list,
        knowledge_roots,
    )

    # constraints <- constraints
    constraints_claims = []
    raw_constraints = raw.get("constraints", [])
    if isinstance(raw_constraints, Sequence) and not isinstance(raw_constraints, (str, bytes)):
        for c in raw_constraints:
            if isinstance(c, Mapping):
                text = str(c.get("text", "")).strip()
                if not text:
                    continue
                ev = c.get("evidence", [])
                st = "supported" if ev else "inferred"
                constraints_claims.append(
                    _make_claim(text, st, ev, sources_by_locator, sources_list, knowledge_roots)
                )

    # findings <- facts
    findings_claims = []
    raw_facts = raw.get("facts", [])
    if isinstance(raw_facts, Sequence) and not isinstance(raw_facts, (str, bytes)):
        for f in raw_facts:
            if isinstance(f, Mapping):
                text = str(f.get("text", "")).strip()
                if not text:
                    continue
                raw_st = str(f.get("status", "")).strip().lower()
                if raw_st in ("confirmed", "single-source"):
                    st = "supported"
                elif raw_st == "inferred":
                    st = "inferred"
                elif raw_st == "conflicting":
                    st = "conflicting"
                else:
                    st = "supported" if f.get("evidence") else "inferred"
                ev = f.get("evidence", [])
                findings_claims.append(
                    _make_claim(text, st, ev, sources_by_locator, sources_list, knowledge_roots)
                )

    # related_entities <- entities (unresolved -> status unknown, no refs)
    has_unresolved_entity = False
    entities_claims = []
    raw_entities = raw.get("entities", [])
    if isinstance(raw_entities, Sequence) and not isinstance(raw_entities, (str, bytes)):
        for e in raw_entities:
            if isinstance(e, Mapping):
                as_written = str(e.get("as_written", "")).strip()
                status = str(e.get("status", "")).strip().lower()
                meaning = str(e.get("meaning", "")).strip()
                ev = e.get("evidence", [])
                text = f"{as_written}: {meaning}" if meaning else as_written
                if not text:
                    continue
                if status == "unresolved":
                    has_unresolved_entity = True
                    entities_claims.append({
                        "text": text,
                        "status": "unknown",
                        "source_refs": [],
                    })
                else:
                    st = "supported" if ev else "inferred"
                    entities_claims.append(
                        _make_claim(text, st, ev, sources_by_locator, sources_list, knowledge_roots)
                    )

    # stakeholders <- ownership verdict claim
    stakeholders_claims = []
    ownership = raw.get("ownership")
    if isinstance(ownership, Mapping):
        verdict = str(ownership.get("verdict", "")).strip()
        ev = ownership.get("evidence", [])
        if verdict:
            st = "supported" if ev else "inferred"
            stakeholders_claims.append(
                _make_claim(f"Owner: {verdict}", st, ev, sources_by_locator, sources_list, knowledge_roots)
            )

    # open_questions <- open_questions (status unknown)
    has_open_questions = False
    open_questions_claims = []
    raw_oq = raw.get("open_questions", [])
    if isinstance(raw_oq, Sequence) and not isinstance(raw_oq, (str, bytes)):
        for q in raw_oq:
            q_text = str(q.get("text", "") if isinstance(q, Mapping) else q).strip()
            if q_text:
                has_open_questions = True
                open_questions_claims.append({
                    "text": q_text,
                    "status": "unknown",
                    "source_refs": [],
                })

    research_status = "inconclusive" if (has_unresolved_entity or has_open_questions) else "sufficient"

    draft: dict[str, Any] = {
        "schema_version": DRAFT_SCHEMA,
        "research_status": research_status,
        "objective": objective,
        "requested_action": requested_action,
        "current_state": [],
        "expected_deliverables": [],
        "timeline": [],
        "decisions": [],
        "dependencies": [],
        "constraints": constraints_claims,
        "stakeholders": stakeholders_claims,
        "related_entities": entities_claims,
        "findings": findings_claims,
        "conflicts": [],
        "open_questions": open_questions_claims,
        "scheduling_recommendations": [],
    }
    return draft, sources_list


def agent_synthesize(
    ctx: dict[str, Any],
    *,
    config: AgentResearchConfig,
    bound_sources: Any,
    run_dir: Path,
    runner: Callable[..., Any] = subprocess.run,
    now: Any = None,
) -> SynthesisResult:
    started = time.monotonic()
    run_dir = Path(run_dir)
    run_dir.mkdir(parents=True, exist_ok=True)

    # a) write run_dir/task.json
    task_snapshot = dict(ctx.get("task_snapshot", {}))
    knowledge_roots_data = _derive_knowledge_roots(config)
    starting_points_data = _derive_starting_points(ctx, bound_sources)

    task_json_payload = {
        **task_snapshot,
        "knowledge_roots": knowledge_roots_data,
        "starting_points": starting_points_data,
    }
    (run_dir / "task.json").write_text(
        json.dumps(task_json_payload, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    # b) launch hermes agent
    prompt_text = _load_prompt(config.prompt_path)
    argv = [
        config.hermes_command,
        "--model",
        config.model,
    ]
    if config.provider:
        argv.extend(["--provider", config.provider])
    argv.extend([
        "chat",
        "--query",
        prompt_text,
        "--max-turns",
        str(config.max_turns),
        "--source",
        "tool",
        "--ignore-rules",
        "--toolsets",
        config.toolsets,
    ])

    env = dict(os.environ)
    env["TERMINAL_CWD"] = str(run_dir.resolve())
    env["FOXHOUND_VOICE_SUMMARIES"] = "0"

    try:
        proc = runner(
            argv,
            cwd=run_dir,
            env=env,
            timeout=config.timeout_seconds,
            capture_output=True,
            text=True,
        )
    except subprocess.TimeoutExpired as exc:
        raise SynthesisError("model_timeout") from exc

    research_json_path = run_dir / "research.json"
    if not research_json_path.exists():
        if proc.returncode != 0:
            raise SynthesisError("runtime_failed")
        raise SynthesisError("draft_missing")

    # c) read run_dir/research.json
    try:
        raw_json_content = research_json_path.read_text(encoding="utf-8")
        raw_research = json.loads(raw_json_content)
        if not isinstance(raw_research, Mapping):
            raise ValueError("Root must be object")
    except Exception as exc:
        raise SynthesisError("draft_missing") from exc

    draft, sources = _convert_research_json(raw_research, config.knowledge_roots)

    # e) validate_draft
    try:
        validate_draft(draft, sources)
    except SynthesisError:
        raise
    except Exception as exc:
        raise SynthesisError("invalid_draft") from exc

    elapsed_ms = max(0, round((time.monotonic() - started) * 1000))
    retrieval_revision = hashlib.sha256(_json_bytes({
        "sources": sources,
        "truncated_layers": [],
    })).hexdigest()

    searched_namespaces = {s["locator"]["namespace"] for s in sources}
    if not searched_namespaces:
        searched_namespaces = {"kb", "attachment", "email"}

    coverage = {
        "searched_namespaces": sorted(searched_namespaces),
        "queries": 0,
        "documents_retrieved": len(sources),
        "unavailable_source_ids": [],
        "knowledge_revisions": {"retrieval_snapshot": retrieval_revision},
    }
    provenance: dict[str, Any] = {
        "profile_id": config.profile_id,
        "profile_revision": config.profile_revision,
        "model": config.model,
        "provider": config.provider,
        "runtime": "hermes-agent-researcher-v1",
        "reasoning_requested": None,
        "reasoning_effective": None,
    }
    metrics = {
        "searches": 0,
        "documents": len(sources),
        "latency_ms": elapsed_ms,
        "prompt_tokens": None,
        "completion_tokens": None,
    }
    return SynthesisResult(draft, tuple(sources), coverage, provenance, metrics)
