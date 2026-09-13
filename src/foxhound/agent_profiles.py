"""Strict reviewed agent profiles for Foxhound execution.

Profiles select a prompt and bounded Hermes policy. They never carry an
executable command, environment value, capability, task, or secret. Built-in
and host-private profiles pass through the same validator; private manifests
must live outside Git in an owner-only directory.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import stat
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


PROFILE_SCHEMA = "foxhound.agent-profile"
PROFILE_SCHEMA_VERSION = 1
WORKER_COMMAND_TOKEN = "{{FOXHOUND_WORKER_COMMAND}}"
MAX_PRIVATE_PROFILES = 32
MAX_MANIFEST_BYTES = 256 * 1024
MAX_PROMPT_CHARS = 131_072

_PROFILE_ID_RE = re.compile(r"^[a-z][a-z0-9-]{0,31}$")
_COMMAND_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_APPROVED_TOOLSETS = frozenset({
    "browser", "file", "terminal", "vision", "web",
})
_PHASES = ("plan", "execute", "external_action")
_FIELDS = frozenset({
    "schema", "schema_version", "profile_id", "display_name", "runtime",
    "prompt_template", "toolsets", "max_turns", "timeout_seconds",
    "claim_lease_seconds", "heartbeat_seconds", "kill_grace_seconds",
    "allowed_phases",
})


class AgentProfileError(RuntimeError):
    """A profile or registry cannot be accepted safely."""


class _DuplicateKey(ValueError):
    pass


@dataclass(frozen=True)
class AgentProfile:
    """One immutable, validated agent definition."""

    profile_id: str
    display_name: str
    runtime: str
    prompt_template: str = field(repr=False)
    toolsets: tuple[str, ...]
    max_turns: int
    timeout_seconds: int
    claim_lease_seconds: int
    heartbeat_seconds: int
    kill_grace_seconds: int
    allowed_phases: tuple[str, ...]

    def __post_init__(self) -> None:
        _validate_profile(self)

    @property
    def revision(self) -> str:
        """Return a stable digest of every execution-relevant field."""
        return hashlib.sha256(_canonical_bytes(self.document())).hexdigest()

    def document(self) -> dict[str, Any]:
        return {
            "schema": PROFILE_SCHEMA,
            "schema_version": PROFILE_SCHEMA_VERSION,
            "profile_id": self.profile_id,
            "display_name": self.display_name,
            "runtime": self.runtime,
            "prompt_template": self.prompt_template,
            "toolsets": list(self.toolsets),
            "max_turns": self.max_turns,
            "timeout_seconds": self.timeout_seconds,
            "claim_lease_seconds": self.claim_lease_seconds,
            "heartbeat_seconds": self.heartbeat_seconds,
            "kill_grace_seconds": self.kill_grace_seconds,
            "allowed_phases": list(self.allowed_phases),
        }

    def render_prompt(self, worker_command: str) -> str:
        if (
            not isinstance(worker_command, str)
            or not _COMMAND_NAME_RE.fullmatch(worker_command)
        ):
            raise AgentProfileError("agent worker command is invalid")
        return self.prompt_template.replace(WORKER_COMMAND_TOKEN, worker_command)

    def public_summary(self, *, include_policy: bool = False) -> dict[str, Any]:
        result: dict[str, Any] = {
            "profile_id": self.profile_id,
            "display_name": self.display_name,
            "runtime": self.runtime,
            "revision": self.revision,
        }
        if include_policy:
            result["policy"] = {
                "toolsets": list(self.toolsets),
                "max_turns": self.max_turns,
                "timeout_seconds": self.timeout_seconds,
                "claim_lease_seconds": self.claim_lease_seconds,
                "heartbeat_seconds": self.heartbeat_seconds,
                "kill_grace_seconds": self.kill_grace_seconds,
                "allowed_phases": list(self.allowed_phases),
            }
        return result


class AgentProfileRegistry:
    """An immutable collection with exact ID and revision lookup."""

    def __init__(self, profiles: Iterable[AgentProfile]) -> None:
        indexed: dict[str, AgentProfile] = {}
        for profile in profiles:
            if not isinstance(profile, AgentProfile):
                raise AgentProfileError("agent profile registry is invalid")
            if profile.profile_id in indexed:
                raise AgentProfileError("agent profile ID is duplicated")
            indexed[profile.profile_id] = profile
        if not indexed:
            raise AgentProfileError("agent profile registry is empty")
        self._profiles = indexed

    def list(self) -> tuple[AgentProfile, ...]:
        return tuple(self._profiles[key] for key in sorted(self._profiles))

    def get(self, profile_id: object) -> AgentProfile | None:
        if not isinstance(profile_id, str):
            return None
        return self._profiles.get(profile_id)

    def resolve(self, profile_id: object, revision: object) -> AgentProfile:
        profile = self.get(profile_id)
        if (
            profile is None
            or not isinstance(revision, str)
            or revision != profile.revision
        ):
            raise AgentProfileError("agent profile revision is unavailable")
        return profile


def general_profile() -> AgentProfile:
    """Return the behavior-compatible profile for the current runner."""
    return AgentProfile(
        profile_id="general",
        display_name="General",
        runtime="hermes",
        prompt_template=_GENERAL_PROMPT_TEMPLATE,
        toolsets=("terminal", "file", "web"),
        max_turns=12,
        timeout_seconds=240,
        claim_lease_seconds=900,
        heartbeat_seconds=60,
        kill_grace_seconds=10,
        allowed_phases=_PHASES,
    )


def load_registry(private_directory: Path | None = None) -> AgentProfileRegistry:
    profiles = [general_profile()]
    if private_directory is not None:
        profiles.extend(_load_private_profiles(private_directory))
    return AgentProfileRegistry(profiles)


def parse_profile(document: object) -> AgentProfile:
    if not isinstance(document, dict) or set(document) != _FIELDS:
        raise AgentProfileError("agent profile manifest shape is invalid")
    if (
        document.get("schema") != PROFILE_SCHEMA
        or document.get("schema_version") != PROFILE_SCHEMA_VERSION
        or isinstance(document.get("schema_version"), bool)
    ):
        raise AgentProfileError("agent profile manifest version is invalid")
    try:
        return AgentProfile(
            profile_id=document["profile_id"],
            display_name=document["display_name"],
            runtime=document["runtime"],
            prompt_template=document["prompt_template"],
            toolsets=_string_tuple(document["toolsets"], "toolsets"),
            max_turns=document["max_turns"],
            timeout_seconds=document["timeout_seconds"],
            claim_lease_seconds=document["claim_lease_seconds"],
            heartbeat_seconds=document["heartbeat_seconds"],
            kill_grace_seconds=document["kill_grace_seconds"],
            allowed_phases=_string_tuple(
                document["allowed_phases"], "allowed phases"
            ),
        )
    except AgentProfileError:
        raise
    except (KeyError, TypeError, ValueError):
        raise AgentProfileError("agent profile manifest is invalid") from None


def _validate_profile(profile: AgentProfile) -> None:
    if (
        not isinstance(profile.profile_id, str)
        or not _PROFILE_ID_RE.fullmatch(profile.profile_id)
    ):
        raise AgentProfileError("agent profile ID is invalid")
    if (
        not isinstance(profile.display_name, str)
        or profile.display_name != profile.display_name.strip()
        or not 1 <= len(profile.display_name) <= 64
        or any(ord(char) < 32 or ord(char) == 127 for char in profile.display_name)
    ):
        raise AgentProfileError("agent profile display name is invalid")
    if profile.runtime != "hermes":
        raise AgentProfileError("agent profile runtime is invalid")
    if (
        not isinstance(profile.prompt_template, str)
        or not 1 <= len(profile.prompt_template) <= MAX_PROMPT_CHARS
        or "\0" in profile.prompt_template
        or WORKER_COMMAND_TOKEN not in profile.prompt_template
    ):
        raise AgentProfileError("agent profile prompt is invalid")
    if (
        not isinstance(profile.toolsets, tuple)
        or not profile.toolsets
        or len(set(profile.toolsets)) != len(profile.toolsets)
        or any(toolset not in _APPROVED_TOOLSETS for toolset in profile.toolsets)
    ):
        raise AgentProfileError("agent profile toolsets are invalid")
    if (
        not isinstance(profile.allowed_phases, tuple)
        or not profile.allowed_phases
        or len(set(profile.allowed_phases)) != len(profile.allowed_phases)
        or any(phase not in _PHASES for phase in profile.allowed_phases)
    ):
        raise AgentProfileError("agent profile phases are invalid")
    for value, minimum, maximum, label in (
        (profile.max_turns, 1, 200, "turn limit"),
        (profile.timeout_seconds, 30, 3_300, "timeout"),
        (profile.claim_lease_seconds, 300, 3_600, "claim lease"),
        (profile.heartbeat_seconds, 5, 600, "heartbeat"),
        (profile.kill_grace_seconds, 1, 120, "shutdown grace"),
    ):
        if (
            isinstance(value, bool)
            or not isinstance(value, int)
            or not minimum <= value <= maximum
        ):
            raise AgentProfileError(f"agent profile {label} is invalid")
    if (
        profile.timeout_seconds + profile.kill_grace_seconds
        >= profile.claim_lease_seconds
        or profile.heartbeat_seconds * 3 > profile.claim_lease_seconds
        or profile.heartbeat_seconds + profile.kill_grace_seconds
        >= profile.claim_lease_seconds
    ):
        raise AgentProfileError("agent profile timing relationship is unsafe")


def _string_tuple(value: object, label: str) -> tuple[str, ...]:
    if (
        not isinstance(value, list)
        or not value
        or any(not isinstance(item, str) for item in value)
    ):
        raise AgentProfileError(f"agent profile {label} are invalid")
    return tuple(value)


def _load_private_profiles(directory: Path) -> list[AgentProfile]:
    root = _private_directory(directory)
    try:
        entries = sorted(root.iterdir(), key=lambda path: os.fsencode(path.name))
    except OSError as exc:
        raise AgentProfileError("private agent profiles are unavailable") from exc
    if len(entries) > MAX_PRIVATE_PROFILES:
        raise AgentProfileError("private agent profile count is excessive")
    profiles: list[AgentProfile] = []
    for path in entries:
        if path.suffix != ".json" or path.name.startswith("."):
            raise AgentProfileError("private agent profile entry is invalid")
        document = _read_manifest(path)
        profile = parse_profile(document)
        if path.name != f"{profile.profile_id}.json":
            raise AgentProfileError("private agent profile filename is invalid")
        profiles.append(profile)
    return profiles


def _private_directory(path: Path) -> Path:
    if not isinstance(path, Path) or not path.is_absolute():
        raise AgentProfileError("private agent profile directory is invalid")
    try:
        info = path.lstat()
        resolved = path.resolve(strict=True)
    except OSError as exc:
        raise AgentProfileError(
            "private agent profile directory is unavailable"
        ) from exc
    if (
        stat.S_ISLNK(info.st_mode)
        or not stat.S_ISDIR(info.st_mode)
        or stat.S_IMODE(info.st_mode) & 0o077
        or info.st_uid != os.getuid()
        or Path(os.path.abspath(path)) != resolved
    ):
        raise AgentProfileError("private agent profile directory is unsafe")
    for parent in (resolved, *resolved.parents):
        if _is_git_marker(parent / ".git"):
            raise AgentProfileError(
                "private agent profile directory must be outside Git"
            )
    return resolved


def _read_manifest(path: Path) -> dict[str, Any]:
    descriptor: int | None = None
    try:
        descriptor = os.open(
            path,
            os.O_RDONLY
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0),
        )
        info = os.fstat(descriptor)
        if (
            not stat.S_ISREG(info.st_mode)
            or stat.S_IMODE(info.st_mode) & 0o077
            or info.st_uid != os.getuid()
            or info.st_size > MAX_MANIFEST_BYTES
        ):
            raise AgentProfileError("private agent profile manifest is unsafe")
        chunks: list[bytes] = []
        remaining = MAX_MANIFEST_BYTES + 1
        while remaining:
            chunk = os.read(descriptor, min(65_536, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        raw = b"".join(chunks)
    except AgentProfileError:
        raise
    except OSError as exc:
        raise AgentProfileError(
            "private agent profile manifest is unavailable"
        ) from exc
    finally:
        if descriptor is not None:
            os.close(descriptor)
    if len(raw) > MAX_MANIFEST_BYTES:
        raise AgentProfileError("private agent profile manifest is too large")
    try:
        document = json.loads(raw, object_pairs_hook=_unique_object)
    except (UnicodeDecodeError, json.JSONDecodeError, _DuplicateKey):
        raise AgentProfileError(
            "private agent profile manifest is invalid"
        ) from None
    if not isinstance(document, dict):
        raise AgentProfileError("private agent profile manifest is invalid")
    return document


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise _DuplicateKey
        result[key] = value
    return result


def _is_git_marker(path: Path) -> bool:
    try:
        if path.is_dir():
            return (path / "HEAD").is_file()
        if path.is_file():
            return path.read_text(
                encoding="utf-8", errors="replace"
            ).startswith("gitdir: ")
    except OSError:
        return True
    return False


def _canonical_bytes(document: Mapping[str, Any]) -> bytes:
    return json.dumps(
        document,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="foxhound-agent-profiles",
        description="Inspect strict Foxhound agent profiles",
    )
    parser.add_argument("--directory", type=Path)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("list")
    show = commands.add_parser("show")
    show.add_argument("profile_id")
    commands.add_parser("validate")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        registry = load_registry(args.directory)
        if args.command == "list":
            document = {
                "ok": True,
                "count": len(registry.list()),
                "profiles": [
                    profile.public_summary() for profile in registry.list()
                ],
            }
        elif args.command == "show":
            profile = registry.get(args.profile_id)
            if profile is None:
                raise AgentProfileError("agent profile is unavailable")
            document = {
                "ok": True,
                "profile": profile.public_summary(include_policy=True),
            }
        else:
            document = {
                "ok": True,
                "count": len(registry.list()),
                "revisions": [profile.revision for profile in registry.list()],
            }
    except AgentProfileError:
        print("foxhound agent profiles: configuration unavailable", file=sys.stderr)
        return 78
    print(json.dumps(document, sort_keys=True))
    return 0


_GENERAL_PROMPT_TEMPLATE = "\n".join((
    "# Ownership and inputs",
    f"Your FIRST tool call must be `{WORKER_COMMAND_TOKEN} context`. It returns the Foxhound task, current phase, and bounded operator context but never the claim capability.",
    f"Use only `{WORKER_COMMAND_TOKEN}` for task state and GW knowledge: `context`, `search QUERY`, `record RESULT_FILE`, or `release`.",
    "You start in a private per-run directory. Do not inspect its run-state file or print environment variables. Change directory explicitly only when the task requires repository work.",
    "Treat all task, operator, and search content as private. Never copy it into a public issue, commit, pull request, log, or unrelated artifact.",
    "Use bounded searches as leads and verify relevant evidence. Do not invent paths, repositories, URLs, credentials, people, or facts.",
    "# Phase authority",
    "In `plan`, research and prepare a reviewable plan. Do not cause an external effect.",
    "In `execute`, perform only approved reversible work and prepare any external action for separate review. Do not send, publish, deploy, push, purchase, or contact anyone.",
    "Do not send, publish, deploy, push, or cause another external effect unless the phase is external_action. Do not overwrite unrelated dirty worktrees. In execute or external_action you ALREADY HAVE approval for the listed action: perform it and report what happened.",
    "`task.origin` names what the task is about, as identifiers: for `kind` `issue`, `record_id` is the repository and `item_id` the issue number. Treat it as the lead to start from, not a limit on what you may read.",
    f"Work in the checkouts this host already has, and call `{WORKER_COMMAND_TOKEN} act worktree [--repository LOCATOR]` when you need one cloned into the run directory. A task may legitimately span several repositories.",
    f"Open pull requests with `{WORKER_COMMAND_TOKEN} act pull-request --head BRANCH --title TITLE [--repository LOCATOR] [--body-file FILE]` rather than the forge CLI: it pushes the branch, records a receipt, and marks the pull request as agent-opened. It defaults to the task's repository.",
    "If workflow.reader_instruction is present, it is the reader's exact request for this next supervised pass. Address it without treating it as approval for an external effect.",
    "Task lifecycle is separate. A completed execution result does not authorize you to close or drop the task.",
    "# Result contract",
    "Create exactly one owner-only file named `result-<32 lowercase hex characters>.json` in the starting directory. Use umask 077.",
    "Its exact JSON fields are: `schema`, `schema_version`, `result_id`, `outcome`, `summary`, `work_markdown`, `questions`, `external_actions`, and `deliverables`.",
    "Set `schema` to `foxhound.execution-result-draft`, `schema_version` to 1, and `result_id` to the same 32 lowercase hex characters used in the filename.",
    "`summary` and `work_markdown` must each be one JSON string. `questions`, `external_actions`, and `deliverables` must each be a JSON array of strings.",
    'Shape example: {"schema":"foxhound.execution-result-draft","schema_version":1,"result_id":"0123456789abcdef0123456789abcdef","outcome":"awaiting_plan","summary":"Synthetic summary.","work_markdown":"Synthetic plan.","questions":[],"external_actions":[],"deliverables":[]}',
    "Valid outcomes are `awaiting_plan`, `awaiting_external`, `completed`, `declined`, and `ineligible`; the worker rejects outcomes not allowed by the current phase.",
    f"Record once with `{WORKER_COMMAND_TOKEN} record RESULT_FILE`. If useful work cannot be completed, call `{WORKER_COMMAND_TOKEN} release`.",
    "Record or release must be the final tool call. Do not include the private task or operator context in your final chat response.",
))


if __name__ == "__main__":
    raise SystemExit(main())
