"""Prepare and verify a byte-preserving scheduler cutover.

This module is deliberately offline. It never reads an installed crontab or
installs a candidate. Operators first capture the active scheduler into a
private file, then use this command to create independently reviewable
candidate and rollback artifacts.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import stat
import sys
import uuid
from dataclasses import asdict, dataclass
from enum import StrEnum
from pathlib import Path


MAX_SNAPSHOT_BYTES = 1024 * 1024


class SchedulerCutoverError(RuntimeError):
    """A scheduler snapshot cannot be transformed safely."""


class CutoverStage(StrEnum):
    """The exact authority boundary represented by the input snapshot."""

    STAGE1 = "stage1"
    STAGE2 = "stage2"
    RESIDUAL = "residual"


@dataclass(frozen=True)
class _JobSignature:
    label: str
    required_fragments: tuple[bytes, ...]

    def matches(self, line: bytes) -> bool:
        return all(fragment in line for fragment in self.required_fragments)


LIFECYCLE_JOBS = (
    _JobSignature(
        "lifecycle_closure", (b"-m gw.task_close", b"--apply")
    ),
    _JobSignature(
        "stale_archival", (b"tasks archive-stale", b"--apply")
    ),
)

RESIDUAL_JOBS = (
    _JobSignature("task_inventory", (b"--job task-inventory ",)),
    _JobSignature("workflow_scheduler", (b"--job task-workflow ",)),
    _JobSignature(
        "workflow_agent", (b"--job task-workflow-agent ",)
    ),
    _JobSignature(
        "task_review", (b"--job task-weekly-review ",)
    ),
)

REMOVED_JOBS = LIFECYCLE_JOBS + RESIDUAL_JOBS

_CREATION_REGISTRY = _JobSignature(
    "creation_registry", (b"-m gw.task_registry", b"--apply")
)


@dataclass(frozen=True)
class CutoverReport:
    """Content-free proof values for one exact scheduler snapshot."""

    source_sha256: str
    candidate_sha256: str
    rollback_sha256: str
    retained_sha256: str
    source_bytes: int
    candidate_bytes: int
    retained_bytes: int
    removed_jobs: int
    retained_creation_registry: int


def prepare_cutover(
    snapshot_path: Path,
    *,
    candidate_path: Path,
    rollback_path: Path,
    stage: CutoverStage | str = CutoverStage.STAGE1,
) -> CutoverReport:
    """Create non-overwriting, owner-only candidate and rollback artifacts."""
    snapshot = _read_private_file(Path(snapshot_path), "scheduler snapshot")
    candidate, report = _transform(snapshot, _stage(stage))
    candidate_target = Path(candidate_path)
    rollback_target = Path(rollback_path)
    _require_distinct_paths(
        Path(snapshot_path), candidate_target, rollback_target
    )
    _require_private_parent(candidate_target)
    _require_private_parent(rollback_target)
    _require_absent(candidate_target, "candidate")
    _require_absent(rollback_target, "rollback")

    published: list[Path] = []
    try:
        _publish_private(candidate_target, candidate)
        published.append(candidate_target)
        _publish_private(rollback_target, snapshot)
        published.append(rollback_target)
    except Exception:
        for path in published:
            try:
                path.unlink(missing_ok=True)
            except OSError:
                pass
        raise
    return report


def verify_cutover(
    snapshot_path: Path,
    *,
    candidate_path: Path,
    rollback_path: Path,
    stage: CutoverStage | str = CutoverStage.STAGE1,
) -> CutoverReport:
    """Verify artifacts against the exact current snapshot without mutation."""
    snapshot = _read_private_file(Path(snapshot_path), "scheduler snapshot")
    candidate = _read_private_file(Path(candidate_path), "candidate")
    rollback = _read_private_file(Path(rollback_path), "rollback")
    expected, report = _transform(snapshot, _stage(stage))
    if candidate != expected:
        raise SchedulerCutoverError(
            "candidate differs from the byte-preserving transformation"
        )
    if rollback != snapshot:
        raise SchedulerCutoverError(
            "rollback differs from the exact scheduler snapshot"
        )
    return report


def _transform(
    snapshot: bytes, stage: CutoverStage
) -> tuple[bytes, CutoverReport]:
    if not snapshot:
        raise SchedulerCutoverError("scheduler snapshot is empty")
    if b"\x00" in snapshot:
        raise SchedulerCutoverError("scheduler snapshot contains invalid data")

    counts = {signature.label: 0 for signature in REMOVED_JOBS}
    registry_count = 0
    retained: list[bytes] = []
    for line in snapshot.splitlines(keepends=True):
        active = bool(line.strip()) and not line.lstrip().startswith(b"#")
        matches = [
            job for job in REMOVED_JOBS if active and job.matches(line)
        ]
        if len(matches) > 1:
            raise SchedulerCutoverError(
                "scheduler line ambiguously identifies multiple task jobs"
            )
        registry_match = active and _CREATION_REGISTRY.matches(line)
        if matches and registry_match:
            raise SchedulerCutoverError(
                "scheduler line ambiguously identifies multiple task jobs"
            )
        if matches:
            counts[matches[0].label] += 1
        if registry_match:
            registry_count += 1
        if stage is CutoverStage.STAGE1:
            remove = bool(matches)
        elif stage is CutoverStage.STAGE2:
            remove = registry_match
        else:
            remove = bool(matches) and matches[0] in RESIDUAL_JOBS
        if not remove:
            retained.append(line)

    if stage is CutoverStage.STAGE1:
        if any(count != 1 for count in counts.values()):
            raise SchedulerCutoverError(
                "each legacy task writer must appear exactly once"
            )
        if registry_count != 1:
            raise SchedulerCutoverError(
                "temporary creation registry must appear exactly once"
            )
    elif stage is CutoverStage.STAGE2:
        if any(count != 0 for count in counts.values()):
            raise SchedulerCutoverError(
                "legacy task writers must already be absent at Stage 2"
            )
        if registry_count != 1:
            raise SchedulerCutoverError(
                "temporary creation registry must appear exactly once"
            )
    else:
        if any(counts[job.label] != 0 for job in LIFECYCLE_JOBS):
            raise SchedulerCutoverError(
                "lifecycle task writers must already be absent at residual cutover"
            )
        if any(counts[job.label] != 1 for job in RESIDUAL_JOBS):
            raise SchedulerCutoverError(
                "each residual task writer must appear exactly once"
            )
        if registry_count != 0:
            raise SchedulerCutoverError(
                "creation registry must already be absent at residual cutover"
            )

    candidate = b"".join(retained)
    retained_bytes = _retained_bytes(snapshot, stage)
    if candidate != retained_bytes:
        raise SchedulerCutoverError("retained scheduler bytes changed")
    report = CutoverReport(
        source_sha256=_digest(snapshot),
        candidate_sha256=_digest(candidate),
        rollback_sha256=_digest(snapshot),
        retained_sha256=_digest(retained_bytes),
        source_bytes=len(snapshot),
        candidate_bytes=len(candidate),
        retained_bytes=len(retained_bytes),
        removed_jobs=(
            sum(counts.values())
            if stage is CutoverStage.STAGE1
            else (
                registry_count
                if stage is CutoverStage.STAGE2
                else sum(counts[job.label] for job in RESIDUAL_JOBS)
            )
        ),
        retained_creation_registry=(
            registry_count if stage is CutoverStage.STAGE1 else 0
        ),
    )
    return candidate, report


def _retained_bytes(snapshot: bytes, stage: CutoverStage) -> bytes:
    retained = []
    for line in snapshot.splitlines(keepends=True):
        active = bool(line.strip()) and not line.lstrip().startswith(b"#")
        if stage is CutoverStage.STAGE1:
            remove = active and any(job.matches(line) for job in REMOVED_JOBS)
        elif stage is CutoverStage.STAGE2:
            remove = active and _CREATION_REGISTRY.matches(line)
        else:
            remove = active and any(job.matches(line) for job in RESIDUAL_JOBS)
        if not remove:
            retained.append(line)
    return b"".join(retained)


def _stage(value: CutoverStage | str) -> CutoverStage:
    try:
        return CutoverStage(value)
    except (TypeError, ValueError) as exc:
        raise SchedulerCutoverError("cutover stage is invalid") from exc


def _read_private_file(path: Path, description: str) -> bytes:
    try:
        before = path.lstat()
        if not stat.S_ISREG(before.st_mode) or stat.S_ISLNK(before.st_mode):
            raise SchedulerCutoverError(f"{description} must be a regular file")
        if before.st_uid != os.geteuid() or before.st_mode & 0o077:
            raise SchedulerCutoverError(f"{description} permissions are unsafe")
        flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(path, flags)
        try:
            after = os.fstat(descriptor)
            if (before.st_dev, before.st_ino) != (after.st_dev, after.st_ino):
                raise SchedulerCutoverError(f"{description} changed while opening")
            if after.st_size > MAX_SNAPSHOT_BYTES:
                raise SchedulerCutoverError(f"{description} is too large")
            data = b""
            while len(data) <= MAX_SNAPSHOT_BYTES:
                chunk = os.read(descriptor, 64 * 1024)
                if not chunk:
                    break
                data += chunk
            if len(data) > MAX_SNAPSHOT_BYTES:
                raise SchedulerCutoverError(f"{description} is too large")
            return data
        finally:
            os.close(descriptor)
    except SchedulerCutoverError:
        raise
    except OSError as exc:
        raise SchedulerCutoverError(f"{description} cannot be read") from exc


def _require_private_parent(path: Path) -> None:
    try:
        parent = path.parent.resolve(strict=True)
        info = parent.stat()
    except OSError as exc:
        raise SchedulerCutoverError("artifact directory cannot be inspected") from exc
    if (not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid()
            or info.st_mode & 0o077):
        raise SchedulerCutoverError("artifact directory permissions are unsafe")


def _require_absent(path: Path, description: str) -> None:
    try:
        path.lstat()
    except FileNotFoundError:
        return
    except OSError as exc:
        raise SchedulerCutoverError(f"{description} target cannot be inspected") from exc
    raise SchedulerCutoverError(f"{description} target already exists")


def _require_distinct_paths(*paths: Path) -> None:
    normalized = [os.path.abspath(path) for path in paths]
    if len(set(normalized)) != len(normalized):
        raise SchedulerCutoverError("snapshot and artifact paths must be distinct")


def _publish_private(path: Path, payload: bytes) -> None:
    temporary = path.parent / f".{path.name}.tmp-{uuid.uuid4().hex}"
    descriptor: int | None = None
    try:
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
        flags |= getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(temporary, flags, 0o600)
        with os.fdopen(descriptor, "wb") as handle:
            descriptor = None
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.link(temporary, path, follow_symlinks=False)
        except FileExistsError as exc:
            raise SchedulerCutoverError("artifact target already exists") from exc
        temporary.unlink()
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    except SchedulerCutoverError:
        raise
    except OSError as exc:
        raise SchedulerCutoverError("artifact cannot be published") from exc
    finally:
        if descriptor is not None:
            os.close(descriptor)
        try:
            temporary.unlink(missing_ok=True)
        except OSError:
            pass


def _digest(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Prepare or verify a private scheduler cutover."
    )
    subcommands = parser.add_subparsers(dest="command", required=True)
    for command in ("prepare", "verify"):
        child = subcommands.add_parser(command)
        child.add_argument("--snapshot", type=Path, required=True)
        child.add_argument("--candidate", type=Path, required=True)
        child.add_argument("--rollback", type=Path, required=True)
        child.add_argument(
            "--stage",
            choices=tuple(stage.value for stage in CutoverStage),
            default=CutoverStage.STAGE1.value,
        )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    operation = prepare_cutover if args.command == "prepare" else verify_cutover
    try:
        report = operation(
            args.snapshot,
            candidate_path=args.candidate,
            rollback_path=args.rollback,
            stage=args.stage,
        )
    except SchedulerCutoverError:
        print("scheduler cutover preparation failed", file=sys.stderr)
        return 1
    print(json.dumps(asdict(report), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
