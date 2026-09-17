"""Editable source store for versioned private Foxhound agent profiles.

An agent's behavior is deployment policy, so its prompt components and limits
never enter Git. This module manages the owner-private store that holds them:
Markdown fragments an operator can edit, a policy document per profile, and the
immutable effective revisions compiled from both.

Publication compiles the shared fragment, the role instructions, any selected
overlay, and the policy into exactly one effective manifest whose digest is the
profile revision. A published revision is never rewritten. The catalog names
the single revision each profile currently offers for new selection; every
earlier revision stays resolvable so a workflow already pinned to it keeps its
exact policy. Disabling withdraws a profile from selection without breaking
those pins.

``install`` copies the catalog and its revisions into the owner-only directory
the card service and runner load. Command output reports identifiers, counts,
and revisions only: never prompt text, fragment content, or store paths.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sqlite3
import stat
import sys
import urllib.parse
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from .agent_profiles import (
    BUILT_IN_PROFILE_IDS,
    CATALOG_NAME,
    MAX_CATALOG_BYTES,
    MAX_MANIFEST_BYTES,
    MAX_PRIVATE_PROFILES,
    MAX_REVISIONS_PER_PROFILE,
    PROFILE_SCHEMA,
    PROFILE_SCHEMA_VERSION,
    REVISIONS_DIRECTORY,
    AgentProfile,
    AgentProfileError,
    CatalogEntry,
    catalog_document,
    parse_catalog,
    parse_profile,
    _PROFILE_ID_RE,
    _canonical_bytes,
    _private_directory,
    _private_subdirectory,
    _read_bytes,
    _read_manifest,
)


DRAFT_SCHEMA = "foxhound.agent-profile-draft"
DRAFT_SCHEMA_VERSION = 1
SHARED_DIRECTORY = "shared"
DRAFTS_DIRECTORY = "drafts"
OVERLAYS_DIRECTORY = "overlays"
POLICY_NAME = "policy.json"
FRAGMENT_SEPARATOR = "\n\n"
MAX_FRAGMENT_BYTES = 64 * 1024
MAX_FRAGMENTS = 8
DIRECTORY_MODE = 0o700
FILE_MODE = 0o600

_FRAGMENT_NAME_RE = re.compile(r"^[a-z][a-z0-9-]{0,63}\.md$")
_POLICY_FIELDS = (
    "display_name", "runtime", "toolsets", "max_turns", "timeout_seconds",
    "claim_lease_seconds", "heartbeat_seconds", "kill_grace_seconds",
    "allowed_phases",
)
_DRAFT_FIELDS = frozenset(
    ("schema", "schema_version", "profile_id", "shared", "role", "overlays")
    + _POLICY_FIELDS
)
_PROFILE_COLUMNS = ("agent_profile_id", "agent_profile_revision")


class ProfileStoreError(AgentProfileError):
    """A private profile store operation cannot be completed safely."""


@dataclass(frozen=True)
class ProfileDraft:
    """One editable profile definition: its fragments and its policy."""

    profile_id: str
    shared: tuple[str, ...]
    role: str
    overlays: tuple[str, ...]
    policy: Mapping[str, Any]

    def fragments(self) -> tuple[str, ...]:
        return (*self.shared, self.role, *self.overlays)


def parse_draft(document: object) -> ProfileDraft:
    """Validate one draft policy document without reading its fragments."""
    if not isinstance(document, dict) or set(document) != _DRAFT_FIELDS:
        raise ProfileStoreError("agent profile draft shape is invalid")
    if (
        document.get("schema") != DRAFT_SCHEMA
        or document.get("schema_version") != DRAFT_SCHEMA_VERSION
        or isinstance(document.get("schema_version"), bool)
    ):
        raise ProfileStoreError("agent profile draft version is invalid")
    profile_id = document["profile_id"]
    if (
        not isinstance(profile_id, str)
        or not _PROFILE_ID_RE.fullmatch(profile_id)
        or profile_id in BUILT_IN_PROFILE_IDS
    ):
        raise ProfileStoreError("agent profile draft ID is invalid")
    draft = ProfileDraft(
        profile_id=profile_id,
        shared=_fragment_names(document["shared"]),
        role=_fragment_name(document["role"]),
        overlays=_fragment_names(document["overlays"]),
        policy={field: document[field] for field in _POLICY_FIELDS},
    )
    names = draft.fragments()
    if len(names) > MAX_FRAGMENTS or len(set(names)) != len(names):
        raise ProfileStoreError("agent profile draft fragments are invalid")
    return draft


def _fragment_names(value: object) -> tuple[str, ...]:
    if not isinstance(value, list):
        raise ProfileStoreError("agent profile draft fragments are invalid")
    return tuple(_fragment_name(item) for item in value)


def _fragment_name(value: object) -> str:
    if not isinstance(value, str) or not _FRAGMENT_NAME_RE.fullmatch(value):
        raise ProfileStoreError("agent profile draft fragments are invalid")
    return value


def compose(source: Path, draft: ProfileDraft) -> AgentProfile:
    """Compile one draft and its fragments into an effective profile."""
    parts = [
        _read_fragment(source / SHARED_DIRECTORY / name)
        for name in draft.shared
    ]
    parts.append(
        _read_fragment(
            source / DRAFTS_DIRECTORY / draft.profile_id / draft.role
        )
    )
    parts.extend(
        _read_fragment(source / OVERLAYS_DIRECTORY / name)
        for name in draft.overlays
    )
    return _effective_profile(draft.profile_id, draft.policy, parts)


def _effective_profile(
    profile_id: str, policy: Mapping[str, Any], parts: Iterable[str]
) -> AgentProfile:
    prompt = FRAGMENT_SEPARATOR.join(
        stripped for stripped in (part.strip() for part in parts) if stripped
    )
    document: dict[str, Any] = {
        "schema": PROFILE_SCHEMA,
        "schema_version": PROFILE_SCHEMA_VERSION,
        "profile_id": profile_id,
        "prompt_template": prompt,
    }
    document.update(policy)
    return parse_profile(document)


def _read_fragment(path: Path) -> str:
    raw = _read_bytes(path, maximum=MAX_FRAGMENT_BYTES, owner_only=False)
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        raise ProfileStoreError(
            "agent profile prompt fragment is invalid"
        ) from None
    if not text.strip() or any(_is_control(item) for item in text):
        raise ProfileStoreError("agent profile prompt fragment is invalid")
    return text


def _is_control(character: str) -> bool:
    code = ord(character)
    return code == 127 or (code < 32 and character not in "\n\t")


def initialize(source: Path) -> dict[str, Any]:
    """Create an empty store, or accept one that already exists."""
    if not isinstance(source, Path) or not source.is_absolute():
        raise ProfileStoreError("private agent profile directory is invalid")
    _ensure_directory(source)
    root = _store_root(source)
    created = []
    for name in (
        SHARED_DIRECTORY, DRAFTS_DIRECTORY, OVERLAYS_DIRECTORY,
        REVISIONS_DIRECTORY,
    ):
        if _ensure_directory(root / name):
            created.append(name)
    catalog_created = not os.path.lexists(root / CATALOG_NAME)
    if catalog_created:
        _write_catalog(root, {})
    else:
        _load_catalog(root)
    return {
        "ok": True,
        "directories_created": len(created),
        "catalog_created": catalog_created,
    }


def load_drafts(source: Path) -> dict[str, ProfileDraft]:
    """Return every editable draft, keyed by profile ID."""
    root = _private_subdirectory(
        _store_root(source), DRAFTS_DIRECTORY, owner_only=False
    )
    entries = _entries(root)
    if len(entries) > MAX_PRIVATE_PROFILES:
        raise ProfileStoreError("agent profile count is excessive")
    drafts: dict[str, ProfileDraft] = {}
    for path in entries:
        try:
            info = path.lstat()
        except OSError as exc:
            raise ProfileStoreError(
                "agent profile store is unavailable"
            ) from exc
        if (
            stat.S_ISLNK(info.st_mode)
            or not stat.S_ISDIR(info.st_mode)
            or not _PROFILE_ID_RE.fullmatch(path.name)
        ):
            raise ProfileStoreError("agent profile draft entry is invalid")
        draft = parse_draft(
            _read_manifest(path / POLICY_NAME, owner_only=False)
        )
        if draft.profile_id != path.name:
            raise ProfileStoreError("agent profile draft ID is misplaced")
        drafts[draft.profile_id] = draft
    return drafts


def validate(source: Path) -> dict[str, Any]:
    """Check every catalog entry, revision, and draft in one pass."""
    root = _store_root(source)
    catalog = _load_catalog(root)
    drafts = load_drafts(root)
    resolvable = 0
    for profile_id, entry in catalog.items():
        for revision in entry.history:
            _load_revision(root, profile_id, revision)
            resolvable += 1
    pending = sorted(
        profile_id
        for profile_id, draft in drafts.items()
        if profile_id not in catalog
        or compose(root, draft).revision != catalog[profile_id].revision
    )
    return {
        "ok": True,
        "profiles": len(catalog),
        "active": sum(1 for entry in catalog.values() if entry.is_active),
        "resolvable_revisions": resolvable,
        "drafts": len(drafts),
        "pending": pending,
        "unpublished_files": _unreferenced(root, catalog),
        "missing_drafts": sorted(set(catalog) - set(drafts)),
    }


def list_profiles(source: Path) -> dict[str, Any]:
    """Report each profile's state and revision without any prompt text."""
    catalog = _load_catalog(_store_root(source))
    profiles = [
        {
            "profile_id": profile_id,
            "state": catalog[profile_id].state,
            "revision": catalog[profile_id].revision,
            "revisions": len(catalog[profile_id].history),
        }
        for profile_id in sorted(catalog)
    ]
    return {"ok": True, "count": len(profiles), "profiles": profiles}


def publish(
    source: Path,
    profile_ids: Sequence[str] = (),
    *,
    all_active: bool = False,
    databases: Sequence[Path] = (),
) -> dict[str, Any]:
    """Compile the selected drafts and advance the catalog atomically.

    When ``databases`` is non-empty, ``publish`` refuses to create a new
    revision for any profile that has active (non-terminal) workflows still
    bound to it.  A terminal status is ``completed`` or ``cancelled``; all
    other statuses block the retune.
    """
    root = _store_root(source)
    catalog = _load_catalog(root)
    drafts = load_drafts(root)
    selected = _publication_targets(catalog, drafts, profile_ids, all_active)

    # Guard: refuse to retune a profile whose revision is still needed
    # by active workflows.
    if databases:
        for profile_id in selected:
            references = sum(
                _active_workflow_references(db, profile_id)
                for db in databases
            )
            if references:
                raise ProfileStoreError(
                    "agent profile is bound to active workflows"
                )

    published: list[dict[str, Any]] = []
    unchanged: list[str] = []
    updated = dict(catalog)
    for profile_id in selected:
        profile = compose(root, drafts[profile_id])
        entry = catalog.get(profile_id)
        if entry is not None and entry.revision == profile.revision:
            unchanged.append(profile_id)
            continue
        _write_revision(root, profile)
        history = tuple(
            revision
            for revision in (entry.history if entry is not None else ())
            if revision != profile.revision
        ) + (profile.revision,)
        if len(history) > MAX_REVISIONS_PER_PROFILE:
            raise ProfileStoreError("agent profile revision history is full")
        updated[profile_id] = CatalogEntry(
            profile_id=profile_id,
            state=entry.state if entry is not None else "active",
            revision=profile.revision,
            history=history,
        )
        published.append({
            "profile_id": profile_id,
            "revision": profile.revision,
            "state": updated[profile_id].state,
            "revisions": len(history),
        })
    if published:
        _write_catalog(root, updated)
    return {"ok": True, "published": published, "unchanged": unchanged}


def _publication_targets(
    catalog: Mapping[str, CatalogEntry],
    drafts: Mapping[str, ProfileDraft],
    profile_ids: Sequence[str],
    all_active: bool,
) -> tuple[str, ...]:
    if all_active and profile_ids:
        raise ProfileStoreError("agent profile selection is ambiguous")
    if all_active:
        selected = sorted(
            profile_id
            for profile_id, entry in catalog.items()
            if entry.is_active
        )
    else:
        selected = sorted(set(profile_ids))
    if not selected:
        raise ProfileStoreError("no agent profile was selected")
    missing = [
        profile_id for profile_id in selected if profile_id not in drafts
    ]
    if missing:
        raise ProfileStoreError("agent profile draft is unavailable")
    return tuple(selected)


def set_state(source: Path, profile_id: str, state: str) -> dict[str, Any]:
    """Withdraw a profile from selection, or offer it again."""
    if state not in {"active", "disabled"}:
        raise ProfileStoreError("agent profile state is invalid")
    root = _store_root(source)
    catalog = _load_catalog(root)
    entry = catalog.get(profile_id)
    if entry is None:
        raise ProfileStoreError("agent profile is unavailable")
    if state == "active":
        _load_revision(root, profile_id, entry.revision)
    updated = dict(catalog)
    updated[profile_id] = CatalogEntry(
        profile_id=profile_id,
        state=state,
        revision=entry.revision,
        history=entry.history,
    )
    if entry.state != state:
        _write_catalog(root, updated)
    return {
        "ok": True,
        "profile_id": profile_id,
        "state": state,
        "changed": entry.state != state,
    }


def install(source: Path, target: Path) -> dict[str, Any]:
    """Copy the catalog and its revisions into the owner-only directory."""
    root = _store_root(source)
    catalog = _load_catalog(root)
    installed = _install_root(target)
    _ensure_directory(installed / REVISIONS_DIRECTORY, owner_only=True)
    copied = 0
    for profile_id, entry in catalog.items():
        _ensure_directory(
            installed / REVISIONS_DIRECTORY / profile_id, owner_only=True
        )
        for revision in entry.history:
            profile = _load_revision(root, profile_id, revision)
            payload = _canonical_bytes(profile.document())
            path = (
                installed / REVISIONS_DIRECTORY / profile_id
                / f"{revision}.json"
            )
            if os.path.lexists(path):
                if _read_bytes(path, maximum=MAX_MANIFEST_BYTES) != payload:
                    raise ProfileStoreError(
                        "installed agent profile revision conflicts"
                    )
                continue
            _write_file(path, payload)
            copied += 1
    _write_file(
        installed / CATALOG_NAME, _canonical_bytes(catalog_document(catalog))
    )
    return {
        "ok": True,
        "profiles": len(catalog),
        "revisions_copied": copied,
        "unreferenced_files": _unreferenced(installed, catalog),
    }


def diagnose(source: Path, target: Path | None = None) -> dict[str, Any]:
    """Report permission and installation state without any private path."""
    root = _store_root(source)
    catalog = _load_catalog(root)
    report: dict[str, Any] = {
        "ok": True,
        "source": _permissions(root, catalog),
        "target": None,
    }
    if target is not None:
        installed = _private_directory(target, owner_only=False)
        installed_catalog = _load_catalog(installed)
        report["target"] = _permissions(installed, installed_catalog)
        report["target"]["current"] = installed_catalog == catalog
    return report


def _permissions(
    root: Path, catalog: Mapping[str, CatalogEntry]
) -> dict[str, Any]:
    permissive = 0
    directories = [root, root / REVISIONS_DIRECTORY] + [
        root / REVISIONS_DIRECTORY / profile_id for profile_id in catalog
    ]
    for path in (
        *directories, root / CATALOG_NAME, *_revision_files(root, catalog)
    ):
        try:
            info = path.lstat()
        except OSError as exc:
            raise ProfileStoreError(
                "agent profile store is unavailable"
            ) from exc
        if stat.S_IMODE(info.st_mode) & 0o077:
            permissive += 1
    return {
        "profiles": len(catalog),
        "active": sum(1 for entry in catalog.values() if entry.is_active),
        "revisions": sum(len(entry.history) for entry in catalog.values()),
        "permissive_entries": permissive,
        "unreferenced_files": _unreferenced(root, catalog),
    }


def delete(
    source: Path, profile_id: str, databases: Sequence[Path]
) -> dict[str, Any]:
    """Remove a disabled profile once no stored work references it."""
    root = _store_root(source)
    catalog = _load_catalog(root)
    entry = catalog.get(profile_id)
    if entry is None:
        raise ProfileStoreError("agent profile is unavailable")
    if entry.is_active:
        raise ProfileStoreError("agent profile is still offered for selection")
    if not databases:
        raise ProfileStoreError("agent profile use cannot be proven")
    references = sum(
        _database_references(database, profile_id) for database in databases
    )
    if references:
        raise ProfileStoreError("agent profile revision is still referenced")
    updated = {
        other: catalog[other] for other in catalog if other != profile_id
    }
    _write_catalog(root, updated)
    removed = 0
    for revision in entry.history:
        path = root / REVISIONS_DIRECTORY / profile_id / f"{revision}.json"
        try:
            os.unlink(path)
            removed += 1
        except FileNotFoundError:
            continue
        except OSError as exc:
            raise ProfileStoreError(
                "agent profile revision cannot be removed"
            ) from exc
    return {
        "ok": True,
        "profile_id": profile_id,
        "revisions_removed": removed,
        "draft_retained": True,
    }


def _database_references(database: Path, profile_id: str) -> int:
    if not isinstance(database, Path) or not database.is_absolute():
        raise ProfileStoreError("workflow evidence is invalid")
    uri = f"file:{urllib.parse.quote(str(database))}?mode=ro"
    connection: sqlite3.Connection | None = None
    try:
        connection = sqlite3.connect(uri, uri=True, timeout=5.0)
        tables = [
            str(row[0])
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        ]
        total = 0
        for table in tables:
            quoted = '"' + table.replace('"', '""') + '"'
            columns = {
                str(row[1])
                for row in connection.execute(f"PRAGMA table_info({quoted})")
            }
            if not set(_PROFILE_COLUMNS) <= columns:
                continue
            row = connection.execute(
                f"SELECT COUNT(*) FROM {quoted} WHERE agent_profile_id = ?",
                (profile_id,),
            ).fetchone()
            total += int(row[0])
        return total
    except (sqlite3.Error, OSError) as exc:
        raise ProfileStoreError("workflow evidence is unavailable") from exc
    finally:
        if connection is not None:
            connection.close()


def _active_workflow_references(database: Path, profile_id: str) -> int:
    """Count non-terminal workflows bound to a profile in one database.

    Terminal statuses (completed, cancelled) do not count.  Active statuses
    such as awaiting_start, snoozed, queued, running, awaiting_review, and
    parked all block a retune because the workflow still depends on its
    pinned revision.
    """
    if not isinstance(database, Path) or not database.is_absolute():
        raise ProfileStoreError("workflow evidence is invalid")
    uri = f"file:{urllib.parse.quote(str(database))}?mode=ro"
    connection: sqlite3.Connection | None = None
    try:
        connection = sqlite3.connect(uri, uri=True, timeout=5.0)
        tables = [
            str(row[0])
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        ]
        total = 0
        for table in tables:
            quoted = '"' + table.replace('"', '""') + '"'
            columns = {
                str(row[1])
                for row in connection.execute(f"PRAGMA table_info({quoted})")
            }
            if not set(_PROFILE_COLUMNS) <= columns:
                continue
            if "status" not in columns:
                continue
            row = connection.execute(
                f"SELECT COUNT(*) FROM {quoted} "
                f"WHERE agent_profile_id = ? "
                f"AND status NOT IN ('completed', 'cancelled')",
                (profile_id,),
            ).fetchone()
            total += int(row[0])
        return total
    except (sqlite3.Error, OSError) as exc:
        raise ProfileStoreError("workflow evidence is unavailable") from exc
    finally:
        if connection is not None:
            connection.close()


def migrate(
    flat_directory: Path, source: Path, *, dry_run: bool = False
) -> dict[str, Any]:
    """Convert a flat private manifest directory into a versioned store.

    The flat directory is only read. Reverting means pointing the services
    back at it and removing the versioned store.
    """
    flat = _private_directory(flat_directory, owner_only=False)
    root = _store_root(source)
    catalog = _load_catalog(root)
    planned = _migration_plan(flat, root, catalog)
    if dry_run:
        return _migration_report(planned, applied=False)
    updated = dict(catalog)
    for profile in planned:
        draft = ProfileDraft(
            profile_id=profile.profile_id,
            shared=(),
            role="role.md",
            overlays=(),
            policy={
                field: profile.document()[field] for field in _POLICY_FIELDS
            },
        )
        directory = root / DRAFTS_DIRECTORY / profile.profile_id
        _ensure_directory(directory)
        _write_file(
            directory / draft.role,
            profile.prompt_template.encode("utf-8") + b"\n",
        )
        _write_file(
            directory / POLICY_NAME, _canonical_bytes(_draft_document(draft))
        )
        _write_revision(root, profile)
        updated[profile.profile_id] = CatalogEntry(
            profile_id=profile.profile_id,
            state="active",
            revision=profile.revision,
            history=(profile.revision,),
        )
    if planned:
        _write_catalog(root, updated)
    return _migration_report(planned, applied=bool(planned))


def _migration_plan(
    flat: Path, root: Path, catalog: Mapping[str, CatalogEntry]
) -> tuple[AgentProfile, ...]:
    """Check every flat manifest before the store is touched at all."""
    planned: list[AgentProfile] = []
    for path in _entries(flat):
        if path.suffix != ".json" or path.name == CATALOG_NAME:
            raise ProfileStoreError("private agent profile entry is invalid")
        profile = parse_profile(_read_manifest(path, owner_only=False))
        if path.name != f"{profile.profile_id}.json":
            raise ProfileStoreError(
                "private agent profile filename is invalid"
            )
        if profile.profile_id in catalog or os.path.lexists(
            root / DRAFTS_DIRECTORY / profile.profile_id
        ):
            raise ProfileStoreError("agent profile is already published")
        policy = {field: profile.document()[field] for field in _POLICY_FIELDS}
        recomposed = _effective_profile(
            profile.profile_id, policy, (profile.prompt_template,)
        )
        if recomposed.revision != profile.revision:
            raise ProfileStoreError("agent profile prompt cannot be migrated")
        planned.append(profile)
    return tuple(planned)


def _migration_report(
    planned: Sequence[AgentProfile], *, applied: bool
) -> dict[str, Any]:
    return {
        "ok": True,
        "migrated": [
            {"profile_id": profile.profile_id, "revision": profile.revision}
            for profile in planned
        ],
        "applied": applied,
        "source_retained": True,
    }


def _draft_document(draft: ProfileDraft) -> dict[str, Any]:
    document: dict[str, Any] = {
        "schema": DRAFT_SCHEMA,
        "schema_version": DRAFT_SCHEMA_VERSION,
        "profile_id": draft.profile_id,
        "shared": list(draft.shared),
        "role": draft.role,
        "overlays": list(draft.overlays),
    }
    document.update(draft.policy)
    return document


def _store_root(source: Path) -> Path:
    return _private_directory(source, owner_only=False)


def _install_root(target: Path) -> Path:
    if not isinstance(target, Path) or not target.is_absolute():
        raise ProfileStoreError("private agent profile directory is invalid")
    _ensure_directory(target)
    return _private_directory(target)


def _load_catalog(root: Path) -> dict[str, CatalogEntry]:
    return parse_catalog(
        _read_manifest(
            root / CATALOG_NAME,
            maximum=MAX_CATALOG_BYTES,
            owner_only=False,
        )
    )


def _load_revision(
    root: Path, profile_id: str, revision: str
) -> AgentProfile:
    path = root / REVISIONS_DIRECTORY / profile_id / f"{revision}.json"
    profile = parse_profile(_read_manifest(path, owner_only=False))
    if profile.profile_id != profile_id or profile.revision != revision:
        raise ProfileStoreError("agent profile revision is invalid")
    return profile


def _write_revision(root: Path, profile: AgentProfile) -> None:
    directory = root / REVISIONS_DIRECTORY / profile.profile_id
    _ensure_directory(root / REVISIONS_DIRECTORY)
    _ensure_directory(directory)
    path = directory / f"{profile.revision}.json"
    if os.path.lexists(path):
        _load_revision(root, profile.profile_id, profile.revision)
        return
    _write_file(path, _canonical_bytes(profile.document()))


def _write_catalog(root: Path, entries: Mapping[str, CatalogEntry]) -> None:
    if len(entries) > MAX_PRIVATE_PROFILES:
        raise ProfileStoreError("agent profile count is excessive")
    document = catalog_document(entries)
    parse_catalog(document)
    _write_file(root / CATALOG_NAME, _canonical_bytes(document))


def _revision_files(
    root: Path, catalog: Mapping[str, CatalogEntry]
) -> list[Path]:
    return [
        root / REVISIONS_DIRECTORY / profile_id / f"{revision}.json"
        for profile_id, entry in catalog.items()
        for revision in entry.history
    ]


def _unreferenced(root: Path, catalog: Mapping[str, CatalogEntry]) -> int:
    referenced = set(_revision_files(root, catalog))
    directory = root / REVISIONS_DIRECTORY
    if not directory.is_dir():
        return 0
    total = 0
    for profile_directory in _entries(directory):
        if not profile_directory.is_dir():
            total += 1
            continue
        for path in _entries(profile_directory):
            if path not in referenced:
                total += 1
    return total


def _entries(directory: Path) -> list[Path]:
    try:
        return sorted(
            directory.iterdir(), key=lambda path: os.fsencode(path.name)
        )
    except OSError as exc:
        raise ProfileStoreError("agent profile store is unavailable") from exc


def _ensure_directory(path: Path, *, owner_only: bool = False) -> bool:
    if os.path.lexists(path):
        _private_subdirectory(path.parent, path.name, owner_only=owner_only)
        return False
    try:
        path.mkdir(mode=DIRECTORY_MODE)
    except OSError as exc:
        raise ProfileStoreError(
            "agent profile store cannot be created"
        ) from exc
    return True


def _write_file(path: Path, payload: bytes) -> None:
    temporary = path.parent / f".{path.name}.incoming-{os.getpid()}"
    descriptor: int | None = None
    try:
        descriptor = os.open(
            temporary,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0),
            FILE_MODE,
        )
        with os.fdopen(descriptor, "wb") as handle:
            descriptor = None
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary, FILE_MODE)
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    except OSError as exc:
        raise ProfileStoreError(
            "agent profile store cannot be written"
        ) from exc
    finally:
        if descriptor is not None:
            os.close(descriptor)
        if os.path.lexists(temporary):
            try:
                os.unlink(temporary)
            except OSError:
                pass


def _fsync_directory(path: Path) -> None:
    descriptor: int | None = None
    try:
        descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        os.fsync(descriptor)
    except OSError:
        return
    finally:
        if descriptor is not None:
            os.close(descriptor)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="foxhound-agent-profile-store",
        description="Manage the private versioned agent-profile store",
    )
    parser.add_argument("--source", type=Path, required=True)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("initialize")
    commands.add_parser("validate")
    commands.add_parser("list")
    publication = commands.add_parser("publish")
    publication.add_argument("--profile", action="append", default=[])
    publication.add_argument("--all-active", action="store_true")
    publication.add_argument(
        "--database", type=Path, action="append", default=[]
    )
    for name in ("disable", "enable"):
        state = commands.add_parser(name)
        state.add_argument("--profile", required=True)
    installation = commands.add_parser("install")
    installation.add_argument("--target", type=Path, required=True)
    doctor = commands.add_parser("doctor")
    doctor.add_argument("--target", type=Path)
    removal = commands.add_parser("delete")
    removal.add_argument("--profile", required=True)
    removal.add_argument("--database", type=Path, action="append", default=[])
    migration = commands.add_parser("migrate")
    migration.add_argument("--flat-directory", type=Path, required=True)
    migration.add_argument("--dry-run", action="store_true")
    return parser


def _run(args: argparse.Namespace) -> dict[str, Any]:
    if args.command == "initialize":
        return initialize(args.source)
    if args.command == "validate":
        return validate(args.source)
    if args.command == "list":
        return list_profiles(args.source)
    if args.command == "publish":
        return publish(
            args.source, args.profile,
            all_active=args.all_active,
            databases=args.database,
        )
    if args.command in {"disable", "enable"}:
        return set_state(
            args.source,
            args.profile,
            "disabled" if args.command == "disable" else "active",
        )
    if args.command == "install":
        return install(args.source, args.target)
    if args.command == "doctor":
        return diagnose(args.source, args.target)
    if args.command == "delete":
        return delete(args.source, args.profile, args.database)
    return migrate(args.flat_directory, args.source, dry_run=args.dry_run)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        document = _run(args)
    except AgentProfileError as exc:
        print(f"foxhound agent profile store: {exc}", file=sys.stderr)
        return os.EX_CONFIG
    except OSError:
        print(
            "foxhound agent profile store: store is unavailable",
            file=sys.stderr,
        )
        return os.EX_CONFIG
    print(json.dumps(document, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
