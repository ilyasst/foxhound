"""Agent-driven task research synthesizer powered by Hermes."""

from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, Callable, Mapping, Sequence
from urllib.parse import urlsplit

from . import task_research_synthesis
from .hermes_session import extract_session_id_from_bytes
from .task_research import validate_sources
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
    read_only_commands: tuple[dict[str, str], ...] = ()
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


def _clean_locator_string(loc_str: str) -> tuple[str, str | None]:
    """Strip trailing whitespace-separated parenthesized note or URL query/trailer, extracting fragment if present."""
    loc = loc_str.strip()
    extracted_fragment: str | None = None

    # Check for trailing whitespace-separated note in parentheses
    # e.g. "path/to/file (line 29, Decisions: ...)"
    paren_idx = loc.rfind(" (")
    if paren_idx != -1 and loc.endswith(")"):
        note = loc[paren_idx + 2 : -1]
        loc = loc[:paren_idx].strip()
        # Parse "line N" / "lines N-M" (first range) from note
        # e.g. "lines 23, 42-45" -> "L23", "line 29..." -> "L29", "lines 10-20" -> "L10-L20"
        m = re.search(r"\blines?\s+(\d+)(?:\s*-\s*(\d+))?", note, re.IGNORECASE)
        if m:
            start_line = m.group(1)
            end_line = m.group(2)
            if end_line:
                extracted_fragment = f"L{start_line}-L{end_line}"
            else:
                extracted_fragment = f"L{start_line}"

    # For URLs, anything after first whitespace is stripped
    if loc.startswith("http://") or loc.startswith("https://"):
        loc = loc.split(None, 1)[0]

    return loc, extracted_fragment


def _get_basename_map(
    root_dir: str,
    cache: dict[str, dict[str, list[str]]] | None = None,
) -> dict[str, list[str]]:
    """Return a mapping of basename -> list of relative posix paths under root_dir."""
    real_root = os.path.realpath(root_dir)
    if cache is not None and real_root in cache:
        return cache[real_root]

    bmap: dict[str, list[str]] = {}
    entries_count = 0
    cap = 200000

    for dirpath, dirnames, filenames in os.walk(real_root, followlinks=False):
        for fname in filenames:
            entries_count += 1
            if entries_count > cap:
                break
            full_file = os.path.join(dirpath, fname)
            if os.path.isfile(full_file):
                rel = os.path.relpath(full_file, real_root).replace("\\", "/")
                bmap.setdefault(fname, []).append(rel)
        if entries_count > cap:
            break

    if cache is not None:
        cache[real_root] = bmap
    return bmap


def _map_locator(
    loc_str: str,
    knowledge_roots: Sequence[tuple[str, str]],
    read_only_commands: Sequence[str] | Sequence[Mapping[str, Any]] = (),
    run_dir: Path | str | None = None,
    basename_cache: dict[str, dict[str, list[str]]] | None = None,
) -> tuple[str, str, str | None] | None:
    """Map locator string to (namespace, resource, fragment). Return None if unmappable."""
    if not isinstance(loc_str, str) or not loc_str.strip():
        return None

    loc, note_fragment = _clean_locator_string(loc_str)
    if not loc:
        return None

    # Web URL check
    if loc.startswith("http://") or loc.startswith("https://"):
        fragment = None
        if "#" in loc:
            loc_base, fragment = loc.split("#", 1)
        else:
            loc_base = loc
        if not fragment and note_fragment:
            fragment = note_fragment
        try:
            _validate_resource_locator(loc_base, namespace="web")
            return "web", loc_base, fragment
        except SynthesisError:
            return None

    # Check for declared read-only commands
    declared_tool_names = set()
    for item in read_only_commands:
        if isinstance(item, str):
            declared_tool_names.add(item)
        elif isinstance(item, Mapping) and "name" in item:
            declared_tool_names.add(item["name"])

    # Normalization: prefixes "read_only_command:" or "tool:" followed by args,
    # when exactly one read-only command is declared -> "<that name>:<args>".
    if ":" in loc:
        prefix_cand, text_cand = loc.split(":", 1)
        if prefix_cand in ("read_only_command", "tool") and len(declared_tool_names) == 1:
            only_name = next(iter(declared_tool_names))
            loc = f"{only_name}:{text_cand}"

    if ":" in loc:
        prefix_cand, text_cand = loc.split(":", 1)
        if prefix_cand in declared_tool_names:
            clean_text = text_cand.strip()
            if 1 <= len(clean_text) <= 300 and "\x00" not in clean_text and "\n" not in clean_text and "\r" not in clean_text:
                resource_val = f"{prefix_cand}:{clean_text}"
                try:
                    _validate_resource_locator(resource_val, namespace="tool")
                    return "tool", resource_val, None
                except SynthesisError:
                    return None

    # Check for fragment/section (e.g. path#section or path:line)
    fragment = None
    resource_candidate = loc
    if "#" in loc:
        resource_candidate, fragment = loc.split("#", 1)
    if not fragment and note_fragment:
        fragment = note_fragment

    norm_roots = [(name, os.path.abspath(os.path.expanduser(p))) for name, p in knowledge_roots]

    # Check if locator has a "<name>:" prefix where name is a configured knowledge-root
    # or one of kb, attachment(s), email(s), repo, web
    target_root_name: str | None = None
    target_rel_path: str | None = None

    # Recognized prefix names
    canon_prefixes = {
        "kb": "kb",
        "attachment": "attachment",
        "attachments": "attachment",
        "email": "email",
        "emails": "email",
        "repo": "repo",
        "web": "web",
    }
    for r_name, _ in norm_roots:
        if r_name not in canon_prefixes:
            canon_prefixes[r_name] = r_name

    # Check for prefix: e.g. "kb:Meetings/..."
    if ":" in resource_candidate:
        prefix_part, rest_part = resource_candidate.split(":", 1)
        if prefix_part in canon_prefixes:
            target_root_name = prefix_part
            target_rel_path = rest_part

    if target_root_name is not None and target_rel_path is not None:
        # Check against matching knowledge root
        # Find root corresponding to target_root_name
        rpath = None
        for r_name, p in norm_roots:
            if r_name == target_root_name or (target_root_name in ("attachment", "attachments") and r_name in ("attachment", "attachments")) or (target_root_name in ("email", "emails") and r_name in ("email", "emails")):
                rpath = p
                break

        if rpath is not None:
            # Resolve relative path against named root
            norm_rel = os.path.normpath(target_rel_path.lstrip("/"))
            full_path = os.path.join(rpath, norm_rel)
            real_root = os.path.realpath(rpath)
            real_full = os.path.realpath(full_path)
            # Refuse paths that do not exist as regular files under that root (no symlink escape)
            if not os.path.isfile(real_full):
                # Normalization: basename resolution under that root
                base_name = os.path.basename(target_rel_path)
                bmap = _get_basename_map(rpath, basename_cache)
                candidates = bmap.get(base_name, [])
                if len(candidates) == 1:
                    norm_rel = candidates[0]
                    full_path = os.path.join(rpath, norm_rel)
                    real_full = os.path.realpath(full_path)
                else:
                    return None

            if not os.path.isfile(real_full):
                return None
            try:
                common = os.path.commonpath([real_root, real_full])
                if common != real_root:
                    return None
            except ValueError:
                return None

            ns = canon_prefixes.get(target_root_name, target_root_name)
            rel_posix = os.path.relpath(real_full, real_root).replace("\\", "/")
            try:
                _validate_resource_locator(rel_posix, namespace=ns)
                return ns, rel_posix, fragment
            except SynthesisError:
                return None

    # Check absolute path against knowledge roots
    abs_cand = os.path.abspath(os.path.expanduser(resource_candidate))
    matched_root = None
    for name, rpath in norm_roots:
        if abs_cand == rpath or abs_cand.startswith(rpath.rstrip(os.sep) + os.sep):
            matched_root = (name, rpath)
            break

    if matched_root:
        name, rpath = matched_root
        real_root = os.path.realpath(rpath)
        real_full = os.path.realpath(abs_cand)
        if not os.path.isfile(real_full):
            return None
        try:
            common = os.path.commonpath([real_root, real_full])
            if common != real_root:
                return None
        except ValueError:
            return None

        rel_posix = os.path.relpath(real_full, real_root).replace("\\", "/")
        ns = canon_prefixes.get(name, name)
        try:
            _validate_resource_locator(rel_posix, namespace=ns)
            return ns, rel_posix, fragment
        except SynthesisError:
            return None

    # Maybe relative path directly given (e.g. repo or relative path)
    clean_rel = resource_candidate.lstrip("/")
    # Check if namespace is prefix like kb/...
    for prefix, ns in (("emails/", "email"), ("attachments/", "attachment"), ("repo/", "repo"), ("kb/", "kb")):
        if clean_rel.startswith(prefix):
            rel = clean_rel[len(prefix):]
            # Try to find corresponding root
            for r_name, rpath in norm_roots:
                if (r_name == ns) or (ns == "attachment" and r_name == "attachments") or (ns == "email" and r_name == "emails"):
                    real_root = os.path.realpath(rpath)
                    real_full = os.path.realpath(os.path.join(rpath, rel))
                    if not os.path.isfile(real_full):
                        return None
                    try:
                        common = os.path.commonpath([real_root, real_full])
                        if common != real_root:
                            return None
                    except ValueError:
                        return None
                    rel_posix = os.path.relpath(real_full, real_root).replace("\\", "/")
                    try:
                        _validate_resource_locator(rel_posix, namespace=ns)
                        return ns, rel_posix, fragment
                    except SynthesisError:
                        return None

    # For a locator without a "<root>:" prefix, or an absolute path:
    # compute os.path.realpath (relative ones against run_dir)
    # and accept it only if the realpath is inside a knowledge root's realpath;
    # map to that root's namespace with the path relative to the root.
    if os.path.isabs(resource_candidate):
        target_candidate_path = resource_candidate
    elif run_dir is not None:
        target_candidate_path = os.path.join(str(run_dir), resource_candidate)
    else:
        target_candidate_path = None

    if target_candidate_path is not None:
        real_cand = os.path.realpath(target_candidate_path)
        if os.path.isfile(real_cand):
            for r_name, rpath in norm_roots:
                real_rpath = os.path.realpath(rpath)
                try:
                    common = os.path.commonpath([real_rpath, real_cand])
                    if common == real_rpath:
                        ns = canon_prefixes.get(r_name, r_name)
                        rel_posix = os.path.relpath(real_cand, real_rpath).replace("\\", "/")
                        try:
                            _validate_resource_locator(rel_posix, namespace=ns)
                            return ns, rel_posix, fragment
                        except SynthesisError:
                            return None
                except ValueError:
                    continue

    return None


def _evidence_item_to_locator_string(item: Any) -> tuple[str | None, str | None]:
    """Convert an evidence item (string or structured dict) to a legacy locator string.

    Returns (locator_string, error_message).
    """
    if isinstance(item, str):
        return item, None

    if isinstance(item, Mapping):
        # 1. Web citation: {"url": ...}
        if "url" in item:
            # A note is an explanation, never part of the locator, on every kind.
            allowed_keys = {"url", "note"}
            unknown = set(item.keys()) - allowed_keys
            if unknown:
                return None, f"Unknown key(s) in web evidence object: {sorted(unknown)}"
            url_val = item.get("url")
            if not isinstance(url_val, str) or not url_val.strip():
                return None, "Field 'url' in evidence object must be a non-empty string"
            return url_val.strip(), None

        # 2. Command citation: {"root": <cmd_name>, "command": ...}
        # Note: if "command" in item
        if "command" in item:
            allowed_keys = {"root", "command", "note"}
            unknown = set(item.keys()) - allowed_keys
            if unknown:
                return None, f"Unknown key(s) in command evidence object: {sorted(unknown)}"
            root_val = item.get("root")
            cmd_val = item.get("command")
            if not isinstance(root_val, str) or not root_val.strip():
                return None, "Field 'root' in command evidence object must be a non-empty string"
            if not isinstance(cmd_val, str) or not cmd_val.strip():
                return None, "Field 'command' in command evidence object must be a non-empty string"
            return f"{root_val.strip()}:{cmd_val.strip()}", None

        # 3. Knowledge root file citation: {"root", "path", "lines"?, "note"?}
        if "root" in item or "path" in item:
            allowed_keys = {"root", "path", "lines", "note"}
            unknown = set(item.keys()) - allowed_keys
            if unknown:
                return None, f"Unknown key(s) in file evidence object: {sorted(unknown)}"
            root_val = item.get("root")
            path_val = item.get("path")
            if not isinstance(root_val, str) or not root_val.strip():
                return None, "Field 'root' in file evidence object must be a non-empty string"
            if not isinstance(path_val, str) or not path_val.strip():
                return None, "Field 'path' in file evidence object must be a non-empty string"
            root_str = root_val.strip()
            path_str = path_val.strip()
            loc_str = f"{root_str}:{path_str}"
            lines_val = item.get("lines")
            if lines_val is not None:
                lines_str = str(lines_val).strip()
                if lines_str:
                    if lines_str.startswith("#"):
                        loc_str += lines_str
                    elif lines_str.startswith("L"):
                        loc_str += f"#{lines_str}"
                    elif "-" in lines_str:
                        start_l, end_l = lines_str.split("-", 1)
                        start_l = start_l.strip().lstrip("L")
                        end_l = end_l.strip().lstrip("L")
                        loc_str += f"#L{start_l}-L{end_l}"
                    else:
                        start_l = lines_str.lstrip("L")
                        loc_str += f"#L{start_l}"
            return loc_str, None

        return None, f"Unrecognized evidence object format: {sorted(item.keys())}"

    return None, f"Evidence must be a string or object, got {type(item).__name__}"


def _make_source_receipt(
    namespace: str,
    resource: str,
    source_id: str,
) -> dict[str, Any]:
    locator = {
        "namespace": namespace,
        "resource": resource,
        "fragment": None,
    }
    content_digest = hashlib.sha256(_json_bytes(locator)).hexdigest()
    if namespace == "web":
        parsed = urlsplit(resource)
        title = f"{parsed.netloc}{parsed.path}"
    else:
        title = PurePosixPath(resource).as_posix()
    title = title.strip()
    if not title:
        title = resource.strip() or "source"
    if len(title) > 200:
        title = title[:200].strip()
    return {
        "source_id": source_id,
        "locator": locator,
        "content_digest": content_digest,
        "title": title,
    }


def _process_evidence_and_refs(
    raw_evidence: Sequence[Any] | None,
    sources_by_locator: dict[tuple[str, str], dict[str, Any]],
    sources_list: list[dict[str, Any]],
    knowledge_roots: Sequence[tuple[str, str]],
    read_only_commands: Sequence[str] | Sequence[Mapping[str, Any]] = (),
    run_dir: Path | str | None = None,
    basename_cache: dict[str, dict[str, list[str]]] | None = None,
) -> list[str]:
    refs: list[str] = []
    if not raw_evidence:
        return refs
    for item in raw_evidence:
        loc_str, _ = _evidence_item_to_locator_string(item)
        if loc_str is None:
            continue
        mapped = _map_locator(loc_str, knowledge_roots, read_only_commands, run_dir=run_dir, basename_cache=basename_cache)
        if mapped is None:
            continue
        ns, res, frag = mapped
        key = (ns, res)
        if key not in sources_by_locator:
            src_id = f"src-{len(sources_list) + 1:03d}"
            receipt = _make_source_receipt(ns, res, src_id)
            sources_by_locator[key] = receipt
            sources_list.append(receipt)
        ref_id = sources_by_locator[key]["source_id"]
        if ref_id not in refs:
            refs.append(ref_id)
    return refs


def _make_claim(
    text: str,
    status: str,
    raw_evidence: Sequence[Any] | None,
    sources_by_locator: dict[tuple[str, str], dict[str, Any]],
    sources_list: list[dict[str, Any]],
    knowledge_roots: Sequence[tuple[str, str]],
    read_only_commands: Sequence[str] | Sequence[Mapping[str, Any]] = (),
    run_dir: Path | str | None = None,
    basename_cache: dict[str, dict[str, list[str]]] | None = None,
    degrade: bool = False,
    stats: dict[str, int] | None = None,
) -> dict[str, Any]:
    refs = _process_evidence_and_refs(
        raw_evidence, sources_by_locator, sources_list, knowledge_roots, read_only_commands, run_dir=run_dir, basename_cache=basename_cache
    )
    if degrade:
        # In degrade mode, count dropped citations
        orig_ev_count = len(raw_evidence) if raw_evidence else 0
        dropped_here = 0
        if orig_ev_count > 0:
            for item in raw_evidence:  # type: ignore
                loc_str, _ = _evidence_item_to_locator_string(item)
                mapped = None
                if loc_str:
                    mapped = _map_locator(loc_str, knowledge_roots, read_only_commands, run_dir=run_dir, basename_cache=basename_cache)
                if mapped is None:
                    dropped_here += 1
        if stats is not None:
            stats["dropped_citations"] += dropped_here
        if not refs:
            if orig_ev_count > 0:
                status = "unsourced"
                if stats is not None:
                    stats["unsourced_claims"] += 1
            else:
                if status not in {"unknown", "unsourced"}:
                    status = "unknown"
    else:
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
    read_only_commands: Sequence[str] | Sequence[Mapping[str, Any]] = (),
    run_dir: Path | str | None = None,
    basename_cache: dict[str, dict[str, list[str]]] | None = None,
    degrade: bool = False,
    stats: dict[str, int] | None = None,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    sources_by_locator: dict[tuple[str, str], dict[str, Any]] = {}
    sources_list: list[dict[str, Any]] = []

    # recommendation <- recommendation
    recommendation_claims = []
    raw_rec = raw.get("recommendation")
    if isinstance(raw_rec, Mapping):
        rec_text = str(raw_rec.get("text", "")).strip()
        if rec_text:
            rec_ev = raw_rec.get("evidence", [])
            rec_st = "supported" if rec_ev else "inferred"
            recommendation_claims.append(
                _make_claim(
                    rec_text, rec_st, rec_ev, sources_by_locator, sources_list, knowledge_roots, read_only_commands,
                    run_dir=run_dir, basename_cache=basename_cache,
                    degrade=degrade, stats=stats,
                )
            )
    elif isinstance(raw_rec, Sequence) and not isinstance(raw_rec, (str, bytes)):
        for r in raw_rec:
            if isinstance(r, Mapping):
                text = str(r.get("text", "")).strip()
                if not text:
                    continue
                ev = r.get("evidence", [])
                st = "supported" if ev else "inferred"
                recommendation_claims.append(
                    _make_claim(
                        text, st, ev, sources_by_locator, sources_list, knowledge_roots, read_only_commands,
                        run_dir=run_dir, basename_cache=basename_cache,
                        degrade=degrade, stats=stats,
                    )
                )

    # objective <- requested_deliverable
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
        read_only_commands,
        run_dir=run_dir,
        basename_cache=basename_cache,
        degrade=degrade,
        stats=stats,
    )

    # requested_action: from the recommendation's first claim when present;
    # otherwise keep today's behavior (same as objective).
    if recommendation_claims:
        requested_action = dict(recommendation_claims[0])
    else:
        requested_action = dict(objective)

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
                    _make_claim(
                        text, st, ev, sources_by_locator, sources_list, knowledge_roots, read_only_commands,
                        run_dir=run_dir, basename_cache=basename_cache,
                        degrade=degrade, stats=stats,
                    )
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
                    _make_claim(
                        text, st, ev, sources_by_locator, sources_list, knowledge_roots, read_only_commands,
                        run_dir=run_dir, basename_cache=basename_cache,
                        degrade=degrade, stats=stats,
                    )
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
                        _make_claim(
                            text, st, ev, sources_by_locator, sources_list, knowledge_roots, read_only_commands,
                            run_dir=run_dir, basename_cache=basename_cache,
                            degrade=degrade, stats=stats,
                        )
                    )

    # stakeholders <- ownership verdict claim
    stakeholders_claims = []
    ownership = raw.get("ownership")
    if isinstance(ownership, Mapping):
        verdict = str(ownership.get("verdict", "")).strip()
        ev = ownership.get("evidence", [])
        reasoning = str(ownership.get("reasoning", "") or "").strip()
        first_sentence = ""
        if reasoning:
            for part in reasoning.replace("\n", " ").split("."):
                candidate = part.strip()
                if candidate:
                    first_sentence = candidate
                    break
        if verdict:
            claim_text = f"Owner: {verdict} — {first_sentence}" if first_sentence else f"Owner: {verdict}"
            st = "supported" if ev else "inferred"
            stakeholders_claims.append(
                _make_claim(
                    claim_text, st, ev, sources_by_locator, sources_list, knowledge_roots, read_only_commands,
                    run_dir=run_dir, basename_cache=basename_cache,
                    degrade=degrade, stats=stats,
                )
            )

    # open_questions <- open_questions (status unknown)
    has_blocking_question = False
    open_questions_claims = []
    raw_oq = raw.get("open_questions", [])
    if isinstance(raw_oq, Sequence) and not isinstance(raw_oq, (str, bytes)):
        for q in raw_oq:
            if isinstance(q, Mapping):
                q_text = str(q.get("text", "")).strip()
                is_blocking = bool(q.get("blocking", False))
            else:
                q_text = str(q or "").strip()
                is_blocking = False
            if q_text:
                if is_blocking:
                    has_blocking_question = True
                    text_to_record = f"{q_text} (blocking)"
                else:
                    text_to_record = q_text
                open_questions_claims.append({
                    "text": text_to_record,
                    "status": "unknown",
                    "source_refs": [],
                })

    is_undetermined_owner = False
    if isinstance(ownership, Mapping):
        verdict_val = str(ownership.get("verdict", "")).strip().lower()
        if verdict_val == "undetermined":
            is_undetermined_owner = True

    research_status = "inconclusive" if (has_blocking_question or is_undetermined_owner) else "sufficient"

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
    if recommendation_claims:
        draft["recommendation"] = recommendation_claims
    return draft, sources_list


def check_research_output(
    run_dir: Path | str,
    knowledge_roots: Sequence[tuple[str, str]],
    read_only_commands: Sequence[str] | Sequence[Mapping[str, Any]] = (),
) -> tuple[dict[str, Any] | None, list[dict[str, Any]] | None, list[str]]:
    """Check research.json in run_dir.

    Returns (draft, sources, problems).
    If problems is non-empty, draft and sources may be None or partially constructed.
    """
    run_dir_path = Path(run_dir)
    research_json_path = run_dir_path / "research.json"
    if not research_json_path.exists():
        return None, None, ["research.json does not exist in run directory"]

    try:
        raw_json_content = research_json_path.read_text(encoding="utf-8")
    except OSError as exc:
        return None, None, [f"Failed to read research.json: {exc}"]

    try:
        raw_research = json.loads(raw_json_content)
    except Exception as exc:
        return None, None, [f"research.json is not valid JSON: {exc}"]

    if not isinstance(raw_research, Mapping):
        return None, None, ["research.json root must be a JSON object"]

    problems: list[str] = []
    basename_cache: dict[str, dict[str, list[str]]] = {}

    # Check top-level fields
    # Expected fields:
    # "ownership": Mapping
    # "requested_deliverable": Mapping | str
    # "constraints": Sequence
    # "entities": Sequence
    # "facts": Sequence
    # "open_questions": Sequence
    # "recommendation": Mapping | str (optional or Mapping)
    for f_name in ("ownership", "requested_deliverable", "constraints", "entities", "facts", "open_questions"):
        if f_name not in raw_research:
            problems.append(f"Missing required top-level field '{f_name}'")

    if "ownership" in raw_research and not isinstance(raw_research["ownership"], Mapping):
        problems.append("Top-level field 'ownership' must be an object")

    if "requested_deliverable" in raw_research and not isinstance(raw_research["requested_deliverable"], (Mapping, str)):
        problems.append("Top-level field 'requested_deliverable' must be an object or string")

    for f_name in ("constraints", "entities", "facts", "open_questions"):
        if f_name in raw_research and (not isinstance(raw_research[f_name], Sequence) or isinstance(raw_research[f_name], (str, bytes))):
            problems.append(f"Top-level field '{f_name}' must be an array")

    # Check unmappable locators across all evidence fields
    def check_evidence(ev_list: Any, field_path: str) -> None:
        if ev_list is None:
            return
        if not isinstance(ev_list, Sequence) or isinstance(ev_list, (str, bytes)):
            problems.append(f"Field '{field_path}' must be an array of evidence items")
            return
        for idx, item in enumerate(ev_list):
            item_path = f"{field_path}[{idx}]"
            loc_str, conv_err = _evidence_item_to_locator_string(item)
            if conv_err is not None:
                problems.append(f"Invalid evidence format at '{item_path}': {conv_err}")
                continue
            if loc_str is None:
                problems.append(f"Evidence at '{item_path}' must be a string or object, got {type(item).__name__}")
                continue

            # (c) any locator whose path part is task.json (with or without ./ or #L)
            loc_clean, _ = _clean_locator_string(loc_str)
            cand_path = loc_clean.split("#", 1)[0].strip()
            if cand_path.startswith("./"):
                cand_path = cand_path[2:]
            if cand_path == "task.json":
                problems.append(
                    f"Invalid evidence locator at '{item_path}': task.json is the task itself, "
                    "not evidence; cite the origin record (meeting protocol, transcript, email) instead"
                )
                continue

            mapped = _map_locator(
                loc_str,
                knowledge_roots,
                read_only_commands,
                run_dir=run_dir_path,
                basename_cache=basename_cache,
            )
            if mapped is None:
                problems.append(
                    f"Unmappable evidence locator at '{item_path}': {item!r}. "
                    "Accepted format is an evidence object or '<root-name>:<path relative to root>' (e.g. kb:Meetings/x.md#L29) "
                    "or a bare http:// or https:// URL."
                )

    if isinstance(raw_research.get("ownership"), Mapping):
        check_evidence(raw_research["ownership"].get("evidence"), "ownership.evidence")

    if isinstance(raw_research.get("requested_deliverable"), Mapping):
        rd_val = raw_research["requested_deliverable"]
        if isinstance(rd_val, Mapping):
            check_evidence(rd_val.get("evidence"), "requested_deliverable.evidence")

    if isinstance(raw_research.get("constraints"), Sequence) and not isinstance(raw_research.get("constraints"), (str, bytes)):
        for i, c in enumerate(raw_research["constraints"]):
            if isinstance(c, Mapping):
                check_evidence(c.get("evidence"), f"constraints[{i}].evidence")

    if isinstance(raw_research.get("entities"), Sequence) and not isinstance(raw_research.get("entities"), (str, bytes)):
        for i, e in enumerate(raw_research["entities"]):
            if isinstance(e, Mapping):
                check_evidence(e.get("evidence"), f"entities[{i}].evidence")

    if isinstance(raw_research.get("facts"), Sequence) and not isinstance(raw_research.get("facts"), (str, bytes)):
        for i, f in enumerate(raw_research["facts"]):
            if isinstance(f, Mapping):
                check_evidence(f.get("evidence"), f"facts[{i}].evidence")

    if isinstance(raw_research.get("recommendation"), Mapping):
        check_evidence(raw_research["recommendation"].get("evidence"), "recommendation.evidence")

    draft = None
    sources = None

    # Conversion and validation errors
    try:
        draft, sources = _convert_research_json(
            raw_research,
            knowledge_roots,
            read_only_commands,
            run_dir=run_dir_path,
            basename_cache=basename_cache,
        )
    except Exception as exc:
        problems.append(f"Conversion error: {exc}")

    if draft is not None and sources is not None:
        try:
            validate_draft(draft, sources)
        except Exception as exc:
            problems.append(f"Draft validation error: {exc}")
        try:
            validate_sources(sources)
        except Exception as exc:
            problems.append(f"Sources validation error: {exc}")

    if problems:
        return draft, sources, problems

    return draft, sources, []


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

    # In the run directory create one symlink per knowledge root (named after the root)
    # (skip if exists)
    for name, p in config.knowledge_roots:
        link_path = run_dir / name
        if not link_path.exists():
            try:
                link_path.symlink_to(Path(p).resolve())
            except OSError:
                pass

    # Write run_dir/.ripgreprc containing "--follow\n"
    (run_dir / ".ripgreprc").write_text("--follow\n", encoding="utf-8")

    task_json_payload = {
        **task_snapshot,
        "knowledge_roots": knowledge_roots_data,
        "starting_points": starting_points_data,
    }
    if config.read_only_commands:
        task_json_payload["read_only_commands"] = [
            {
                "name": cmd["name"],
                "command": cmd["command"],
                "description": cmd["description"],
            }
            for cmd in config.read_only_commands
        ]
    (run_dir / "task.json").write_text(
        json.dumps(task_json_payload, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    # Write executable ./research-check script into run_dir
    research_check_script = (
        f"#!{sys.executable}\n"
        "from foxhound.research_check import main\n"
        "raise SystemExit(main())\n"
    )
    research_check_path = run_dir / "research-check"
    research_check_path.write_text(research_check_script, encoding="utf-8")
    research_check_path.chmod(0o755)

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
        "-Q",
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
    env["RIPGREP_CONFIG_PATH"] = str((run_dir / ".ripgreprc").resolve())

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

    # Capture session ID if Hermes printed it
    proc_output = ""
    stdout_val = getattr(proc, "stdout", None)
    if isinstance(stdout_val, str):
        proc_output += stdout_val
    elif isinstance(stdout_val, bytes):
        proc_output += stdout_val.decode("utf-8", errors="replace")

    stderr_val = getattr(proc, "stderr", None)
    if isinstance(stderr_val, str):
        proc_output += stderr_val
    elif isinstance(stderr_val, bytes):
        proc_output += stderr_val.decode("utf-8", errors="replace")

    session_id = extract_session_id_from_bytes(proc_output.encode("utf-8", errors="replace"))

    repair_turns = 0
    draft, sources, problems = check_research_output(
        run_dir, config.knowledge_roots, config.read_only_commands
    )

    while problems and session_id and repair_turns < 1:
        repair_turns += 1
        bullet_problems = "\n".join(f"- {p}" for p in problems)
        repair_message = (
            "Your research.json was rejected with the following problems:\n"
            f"{bullet_problems}\n\n"
            "Please edit only the listed entries; do not rewrite the file. "
            "Do not research again. Keep all findings."
        )
        repair_argv = [
            config.hermes_command,
            "--model",
            config.model,
        ]
        if config.provider:
            repair_argv.extend(["--provider", config.provider])
        repair_argv.extend([
            "chat",
            "-Q",
            "--resume",
            session_id,
            "--query",
            repair_message,
            "--max-turns",
            "8",
            "--source",
            "tool",
            "--ignore-rules",
            "--toolsets",
            "file",
        ])
        timed_out = False
        try:
            repair_proc = runner(
                repair_argv,
                cwd=run_dir,
                env=env,
                timeout=600,
                capture_output=True,
                text=True,
            )
            # Update session_id if new one printed
            repair_out = ""
            r_stdout = getattr(repair_proc, "stdout", None)
            if isinstance(r_stdout, str):
                repair_out += r_stdout
            elif isinstance(r_stdout, bytes):
                repair_out += r_stdout.decode("utf-8", errors="replace")

            r_stderr = getattr(repair_proc, "stderr", None)
            if isinstance(r_stderr, str):
                repair_out += r_stderr
            elif isinstance(r_stderr, bytes):
                repair_out += r_stderr.decode("utf-8", errors="replace")

            new_session_id = extract_session_id_from_bytes(repair_out.encode("utf-8", errors="replace"))
            if new_session_id:
                session_id = new_session_id
        except subprocess.TimeoutExpired:
            # Repair timeout stops further repair turns, but re-checks output first
            timed_out = True

        draft, sources, problems = check_research_output(
            run_dir, config.knowledge_roots, config.read_only_commands
        )
        if timed_out:
            break

    self_check_ok = not bool(problems)
    is_degraded = False
    dropped_citations = 0
    unsourced_claims = 0

    if problems:
        research_json_path = run_dir / "research.json"
        if not research_json_path.exists():
            if proc.returncode != 0:
                raise SynthesisError("runtime_failed")
            raise SynthesisError("draft_missing")

        try:
            raw_json_content = research_json_path.read_text(encoding="utf-8")
            raw_research = json.loads(raw_json_content)
            if not isinstance(raw_research, Mapping):
                raise ValueError("Root must be object")
        except Exception as exc:
            raise SynthesisError("draft_missing") from exc

        # Degrade: drop invalid citations, mark claims left with no evidence status "unsourced"
        degrade_stats = {"dropped_citations": 0, "unsourced_claims": 0}
        try:
            draft, sources = _convert_research_json(
                raw_research,
                config.knowledge_roots,
                config.read_only_commands,
                run_dir=run_dir,
                degrade=True,
                stats=degrade_stats,
            )
            validate_draft(draft, sources)
            validate_sources(sources)
        except SynthesisError:
            raise
        except Exception as exc:
            raise SynthesisError("invalid_draft") from exc

        is_degraded = True
        dropped_citations = degrade_stats["dropped_citations"]
        unsourced_claims = degrade_stats["unsourced_claims"]

    assert draft is not None
    assert sources is not None

    elapsed_ms = max(0, round((time.monotonic() - started) * 1000))
    retrieval_revision = hashlib.sha256(_json_bytes({
        "sources": sources,
        "truncated_layers": [],
    })).hexdigest()

    searched_namespaces = {s["locator"]["namespace"] for s in sources}
    if not searched_namespaces:
        searched_namespaces = {"kb", "attachment", "email"}

    coverage: dict[str, Any] = {
        "searched_namespaces": sorted(searched_namespaces),
        "queries": 0,
        "documents_retrieved": len(sources),
        "unavailable_source_ids": [],
        "knowledge_revisions": {"retrieval_snapshot": retrieval_revision},
    }
    if is_degraded:
        coverage["degraded"] = True
        coverage["dropped_citations"] = dropped_citations
        coverage["unsourced_claims"] = unsourced_claims

    provenance: dict[str, Any] = {
        "profile_id": config.profile_id,
        "profile_revision": config.profile_revision,
        "model": config.model,
        "provider": config.provider or "local",
        "runtime": "hermes-agent-researcher-v1",
        # The receipt has no "none" level; the agent's effort is set by the
        # model route (thinking_no), so claim the least and report unknown.
        "reasoning_requested": "low",
        "reasoning_effective": "unknown",
    }
    metrics = {
        "searches": 0,
        "documents": len(sources),
        "latency_ms": elapsed_ms,
        "prompt_tokens": None,
        "completion_tokens": None,
        "repair_turns": repair_turns,
        "self_check_ok": self_check_ok,
        "degraded": is_degraded,
        "dropped_citations": dropped_citations,
        "unsourced_claims": unsourced_claims,
    }
    return SynthesisResult(draft, tuple(sources), coverage, provenance, metrics)
