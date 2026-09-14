"""Durable, explicitly configured review files for one execution task.

The private execution directory contains capabilities and compiled instructions,
so it is never copied.  This module preserves only named result inputs, the
transcript, result drafts, and paths the agent explicitly lists in the artifact
manifest.
"""

from __future__ import annotations

import json
import os
import re
import stat
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Mapping, Sequence


ARTIFACT_MANIFEST_NAME = "result-artifacts.json"
TRANSCRIPT_NAME = "agent-output.log"
RESULT_INPUT_NAMES = (
    "result-summary.txt",
    "result-work.md",
    "result-questions.json",
    "result-external-actions.json",
    "result-deliverables.json",
)
MAX_ARTIFACTS = 100
MAX_ARTIFACT_BYTES = 2 * 1024 * 1024
MAX_ARCHIVE_BYTES = 20 * 1024 * 1024
MAX_MANIFEST_BYTES = 64 * 1024
_RESERVED_NAMES = frozenset({"run-state.json", "agent-instructions.json"})
_RESULT_DRAFT_RE = re.compile(r"result-[0-9a-f]{32}\.json")
_GITHUB_RECORD_RE = re.compile(r"^github\.com/([^/]+)/([^/]+)$")
_MARKDOWN_LINK_RE = re.compile(r"\[([^\]\n]+)\]\((https?://[^)\s]+)\)")
_GITHUB_URL_RE = re.compile(r"https://github\.com/[^\s<>()]+")


class TaskArchiveError(RuntimeError):
    """A configured durable task archive cannot be written safely."""


@dataclass(frozen=True)
class TaskArchivePaths:
    working_directory: Path
    task_file: Path
    run_directory: Path


def task_slug(text: str, *, maximum: int = 64) -> str:
    """Return a portable, bounded task-folder slug."""
    if not isinstance(text, str) or not text.strip():
        return "task"
    value = text.casefold().encode("ascii", "ignore").decode("ascii")
    value = re.sub(r"[^a-z0-9]+", "-", value).strip("-")
    return (value[:maximum].rstrip("-") or "task")


def prepare_task_archive(
    *,
    working_root: Path,
    kb_root: Path,
    task_id: int,
    task_text: str,
    run_id: str,
    phase: str,
    agent_display_name: str,
    origin_kind: str | None = None,
    origin_record: str | None = None,
    origin_item: str | None = None,
) -> TaskArchivePaths:
    """Create the stable task locations and register this run."""
    if isinstance(task_id, bool) or not isinstance(task_id, int) or task_id < 1:
        raise TaskArchiveError("task archive identity is invalid")
    work = _prepare_root(working_root, "working")
    kb = _prepare_root(kb_root, "knowledge")
    basename = _task_basename(work, kb, task_id, task_text)
    task_directory = work / basename
    run_directory = task_directory / "runs" / f"{phase}-{run_id}"
    task_file = kb / f"{basename}.md"
    _make_directory(task_directory)
    _make_directory(task_directory / "runs")
    _make_directory(run_directory)
    source = _origin_markdown(origin_kind, origin_record, origin_item)
    header = _task_header(task_id, task_text, source, task_directory)
    _write_if_missing(task_directory / "README.md", header)
    _write_if_missing(task_file, header)
    stamp = datetime.now().astimezone().isoformat(timespec="seconds")
    run_entry = (
        f"\n## Run {phase} — {stamp}\n\n"
        f"- Agent: {agent_display_name}\n"
        f"- Run: `{run_directory}`\n"
        "- Evidence: see the run directory; the transcript is retained even "
        "when no result is recorded.\n"
    )
    _append_private(task_directory / "README.md", run_entry)
    _append_private(task_file, run_entry)
    return TaskArchivePaths(task_directory, task_file, run_directory)


def _task_basename(
    working_root: Path, kb_root: Path, task_id: int, task_text: str
) -> str:
    """Keep one folder name when a later task revision changes its text."""
    pattern = re.compile(rf"^T{task_id}-[a-z0-9-]+$")
    candidates: set[str] = set()
    try:
        candidates.update(
            entry.name
            for entry in working_root.iterdir()
            if entry.is_dir()
            and not entry.is_symlink()
            and pattern.fullmatch(entry.name)
        )
        candidates.update(
            entry.stem
            for entry in kb_root.iterdir()
            if entry.is_file()
            and not entry.is_symlink()
            and pattern.fullmatch(entry.stem)
        )
    except OSError as exc:
        raise TaskArchiveError("task archive root is unavailable") from exc
    if len(candidates) > 1:
        raise TaskArchiveError("task archive identity is ambiguous")
    if candidates:
        return next(iter(candidates))
    return f"T{task_id}-{task_slug(task_text)}"


def preserve_run_files(
    source_directory: Path,
    destination_directory: Path,
    *,
    include_transcript: bool = True,
) -> tuple[str, ...]:
    """Copy only the fixed allowlist and explicitly manifested artifacts."""
    candidates: list[Path] = [
        *(Path(name) for name in RESULT_INPUT_NAMES),
        Path(ARTIFACT_MANIFEST_NAME),
    ]
    if include_transcript:
        candidates.insert(0, Path(TRANSCRIPT_NAME))
    try:
        for entry in source_directory.iterdir():
            if _RESULT_DRAFT_RE.fullmatch(entry.name):
                candidates.append(Path(entry.name))
    except OSError as exc:
        raise TaskArchiveError("task run evidence is unavailable") from exc
    manifest = _artifact_manifest(source_directory)
    manifested = set(manifest)
    candidates.extend(manifest)

    copied: list[str] = []
    seen: set[Path] = set()
    total = 0
    for relative in candidates:
        if relative in seen:
            continue
        seen.add(relative)
        source = source_directory / relative
        try:
            info = source.lstat()
        except FileNotFoundError:
            if relative in manifested:
                raise TaskArchiveError("task run evidence is unavailable")
            continue
        except OSError as exc:
            raise TaskArchiveError("task run evidence is unavailable") from exc
        if not stat.S_ISREG(info.st_mode) or info.st_size > MAX_ARTIFACT_BYTES:
            if relative in {Path(name) for name in RESULT_INPUT_NAMES} or (
                relative.name == TRANSCRIPT_NAME
            ) or relative in manifested:
                raise TaskArchiveError("task run evidence is unsafe")
            continue
        if total + info.st_size > MAX_ARCHIVE_BYTES:
            raise TaskArchiveError("task run evidence is too large")
        destination = destination_directory / relative
        _make_directory(destination.parent)
        try:
            destination_info = destination.lstat()
        except FileNotFoundError:
            destination_info = None
        except OSError as exc:
            raise TaskArchiveError("task run evidence is unavailable") from exc
        if destination_info is not None:
            if not stat.S_ISREG(destination_info.st_mode):
                raise TaskArchiveError("task run evidence is unsafe")
            copied.append(relative.as_posix())
            total += info.st_size
            continue
        copied_bytes = _copy_regular_file(
            source,
            destination,
            maximum=min(MAX_ARTIFACT_BYTES, MAX_ARCHIVE_BYTES - total),
        )
        total += copied_bytes
        copied.append(relative.as_posix())
    return tuple(copied)


def append_result(
    paths: TaskArchivePaths,
    *,
    result: Mapping[str, object],
    origin_kind: str | None = None,
    origin_record: str | None = None,
    origin_item: str | None = None,
) -> None:
    """Append the human review record to both the folder and the KB file."""
    summary = str(result.get("summary") or "")
    work = str(result.get("work_markdown") or "")
    outcome = str(result.get("outcome") or "")
    links = review_links(
        "\n".join(_result_text_values(result)),
        origin_kind=origin_kind,
        origin_record=origin_record,
        origin_item=origin_item,
    )
    lines = [
        "",
        "### Result",
        "",
        f"**Outcome:** {outcome}",
        "",
        f"**Summary:** {summary}",
    ]
    if links:
        lines.extend(("", "**Review links:**", *[f"- {link}" for link in links]))
    for label, key in (
        ("Needs your input", "questions"),
        ("External actions", "external_actions"),
        ("Deliverables", "deliverables"),
    ):
        values = _result_collection(result.get(key))
        if values:
            lines.extend(("", f"**{label}:**", *[f"- {value}" for value in values]))
    if work:
        lines.extend(("", "**Work:**", "", work))
    lines.extend(("", f"**Working files:** `{paths.run_directory}`", ""))
    payload = "\n".join(lines)
    _append_private(paths.working_directory / "README.md", payload)
    _append_private(paths.task_file, payload)


def _prepare_root(path: Path, label: str) -> Path:
    if not isinstance(path, Path) or not path.is_absolute():
        raise TaskArchiveError(f"task {label} root is invalid")
    try:
        path.mkdir(mode=0o700, parents=True, exist_ok=True)
        if path.resolve(strict=True) != path or not path.is_dir() or path.is_symlink():
            raise TaskArchiveError(f"task {label} root is invalid")
    except OSError as exc:
        raise TaskArchiveError(f"task {label} root is unavailable") from exc
    return path


def _make_directory(path: Path) -> None:
    try:
        path.mkdir(mode=0o700, parents=True, exist_ok=True)
        if not path.is_dir() or path.is_symlink():
            raise TaskArchiveError("task archive directory is unsafe")
    except OSError as exc:
        raise TaskArchiveError("task archive directory is unavailable") from exc


def _task_header(
    task_id: int, task_text: str, source: str | None, working_directory: Path
) -> str:
    lines = [
        f"# T{task_id} — {task_text}",
        "",
        f"**Task:** {task_text}",
    ]
    if source:
        lines.append(f"**Source:** {source}")
    lines.extend((f"**Working folder:** `{working_directory}`", ""))
    return "\n".join(lines)


def _origin_markdown(
    kind: str | None, record: str | None, item: str | None
) -> str | None:
    if not record or not item:
        return None
    match = _GITHUB_RECORD_RE.fullmatch(record)
    if kind == "issue" and match:
        return f"[Issue #{item}](https://{record}/issues/{item})"
    return f"{record} #{item}"


def review_links(
    text: str,
    *,
    origin_kind: str | None,
    origin_record: str | None,
    origin_item: str | None,
) -> tuple[str, ...]:
    links: list[str] = []
    source = _origin_markdown(origin_kind, origin_record, origin_item)
    if source:
        links.append(source)
    linked_targets: set[str] = set()
    for match in _MARKDOWN_LINK_RE.finditer(text):
        links.append(f"[{match.group(1)}]({match.group(2)})")
        linked_targets.add(match.group(2))
    for url in _GITHUB_URL_RE.findall(text):
        url = url.rstrip(".,;:!?")
        if url not in linked_targets:
            links.append(f"[{url}]({url})")
    repository = (
        origin_record
        if _GITHUB_RECORD_RE.fullmatch(origin_record or "")
        else None
    )
    if repository:
        for number in re.findall(r"\bPR\s+#(\d+)\b", text, flags=re.IGNORECASE):
            links.append(f"[PR #{number}](https://{repository}/pull/{number})")
        for number in re.findall(r"\bissue\s+#(\d+)\b", text, flags=re.IGNORECASE):
            links.append(f"[Issue #{number}](https://{repository}/issues/{number})")
        for digest in re.findall(
            r"\bcommit\s+([0-9a-f]{7,40})\b", text, flags=re.IGNORECASE
        ):
            links.append(
                f"[Commit {digest[:12]}](https://{repository}/commit/{digest})"
            )
    return tuple(dict.fromkeys(links))


def _result_text_values(result: Mapping[str, object]) -> list[str]:
    values = [str(result.get("summary") or ""), str(result.get("work_markdown") or "")]
    for key in ("questions", "external_actions", "deliverables"):
        values.extend(_result_collection(result.get(key)))
    return values


def _result_collection(value: object) -> tuple[str, ...]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        return ()
    items: list[str] = []
    for item in value:
        if isinstance(item, str):
            items.append(item)
        elif isinstance(item, Mapping):
            primary = item.get("action") or item.get("body") or item.get("text")
            if isinstance(primary, str):
                details = [primary]
                for key in ("requires", "channel", "label", "recipient", "subject"):
                    detail = item.get(key)
                    if isinstance(detail, str):
                        details.append(f"{key}: {detail}")
                items.append(" — ".join(details))
    return tuple(items)


def _artifact_manifest(source_directory: Path) -> tuple[Path, ...]:
    path = source_directory / ARTIFACT_MANIFEST_NAME
    try:
        info = path.lstat()
    except FileNotFoundError:
        return ()
    except OSError as exc:
        raise TaskArchiveError("task artifact manifest is unavailable") from exc
    if not stat.S_ISREG(info.st_mode) or info.st_size > MAX_MANIFEST_BYTES:
        raise TaskArchiveError("task artifact manifest is unsafe")
    try:
        flags = os.O_RDONLY
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        descriptor = os.open(path, flags)
        with os.fdopen(descriptor, "r", encoding="utf-8") as handle:
            document = json.load(handle)
    except (OSError, UnicodeError, ValueError) as exc:
        raise TaskArchiveError("task artifact manifest is invalid") from exc
    if (
        not isinstance(document, list)
        or len(document) > MAX_ARTIFACTS
        or any(not isinstance(item, str) for item in document)
    ):
        raise TaskArchiveError("task artifact manifest is invalid")
    paths: list[Path] = []
    for item in document:
        relative = Path(item)
        if (
            not item
            or relative.is_absolute()
            or len(relative.parts) > 12
            or any(part in {"", ".", ".."} for part in relative.parts)
            or any(part in _RESERVED_NAMES for part in relative.parts)
        ):
            raise TaskArchiveError("task artifact manifest is invalid")
        paths.append(relative)
    return tuple(paths)


def _copy_regular_file(
    source: Path, destination: Path, *, maximum: int
) -> int:
    temporary = destination.with_name(f".{destination.name}.tmp-{os.getpid()}")
    try:
        source_flags = os.O_RDONLY
        if hasattr(os, "O_NOFOLLOW"):
            source_flags |= os.O_NOFOLLOW
        source_descriptor = os.open(source, source_flags)
        source_info = os.fstat(source_descriptor)
        if (
            not stat.S_ISREG(source_info.st_mode)
            or source_info.st_size > maximum
        ):
            os.close(source_descriptor)
            raise TaskArchiveError("task run evidence is unsafe")
        with os.fdopen(source_descriptor, "rb") as source_handle:
            flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
            if hasattr(os, "O_NOFOLLOW"):
                flags |= os.O_NOFOLLOW
            descriptor = os.open(temporary, flags, 0o600)
            try:
                with os.fdopen(descriptor, "wb", closefd=False) as target:
                    remaining = maximum
                    copied = 0
                    while True:
                        block = source_handle.read(min(64 * 1024, remaining + 1))
                        if not block:
                            break
                        if len(block) > remaining:
                            raise TaskArchiveError("task run evidence is too large")
                        target.write(block)
                        copied += len(block)
                        remaining -= len(block)
                    target.flush()
                    os.fsync(target.fileno())
            finally:
                os.close(descriptor)
        os.replace(temporary, destination)
        return copied
    except (OSError, TaskArchiveError) as exc:
        try:
            temporary.unlink(missing_ok=True)
        except OSError:
            pass
        if isinstance(exc, TaskArchiveError):
            raise
        raise TaskArchiveError("task run evidence could not be preserved") from exc


def _write_if_missing(path: Path, value: str) -> None:
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(path, flags, 0o600)
    except FileExistsError:
        if path.is_symlink() or not path.is_file():
            raise TaskArchiveError("task archive file is unsafe")
        return
    except OSError as exc:
        raise TaskArchiveError("task archive file is unavailable") from exc
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", closefd=False) as handle:
            handle.write(value)
            handle.flush()
            os.fsync(handle.fileno())
    finally:
        os.close(descriptor)


def _append_private(path: Path, value: str) -> None:
    flags = os.O_WRONLY | os.O_APPEND
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(path, flags)
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode):
            raise TaskArchiveError("task archive file is unsafe")
        with os.fdopen(descriptor, "w", encoding="utf-8", closefd=False) as handle:
            handle.write(value)
            handle.flush()
            os.fsync(handle.fileno())
    except OSError as exc:
        raise TaskArchiveError("task archive file is unavailable") from exc
    finally:
        if "descriptor" in locals():
            os.close(descriptor)
