"""Foxhound-owned supervision for one disposable execution-phase agent."""

from __future__ import annotations

import argparse
import contextlib
import fcntl
import json
import math
import os
import re
import secrets
import shlex
import signal
import stat
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Iterator, Mapping, Sequence

from .agent_profiles import (
    AgentProfile,
    AgentProfileError,
    AgentProfileRegistry,
    load_registry,
    render_bootstrap,
)
from .execution_worker import (
    GW_ALIAS_ENV,
    GW_ENDPOINT_ENV,
    GW_TOKEN_FILE_ENV,
    INSTRUCTIONS_NAME,
    RUN_STATE_SCHEMA,
    RUN_STATE_SCHEMA_VERSION,
    STATE_ENV,
    WORKER_SCHEMA_VERSION,
    ExecutionWorkerConfigError,
    load_knowledge_config,
)
from .task_execution import (
    ExecutionClaim,
    ExecutionWorkflow,
    TaskExecutionService,
    WorkflowOperationResult,
    WorkflowDisposition,
    WorkflowPhase,
    WorkflowStatus,
)
from .task_archive import (
    TaskArchiveError,
    TaskArchivePaths,
    TRANSCRIPT_NAME,
    prepare_task_archive,
    preserve_run_files,
    publish_deliverables,
)
from .task_ledger import TaskLedger, TaskLedgerError
from .source_policy import planning_grants as _planning_grants


NO_PROGRESS_EXIT_CODE = 70
STARTUP_EXIT_CODE = 71
TIMEOUT_EXIT_CODE = 124
_COMMAND_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_RUN_ID_RE = re.compile(r"^[0-9a-f]{32}$")
_RUNNER_SLOT_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,31}$")


class ExecutionRunnerError(RuntimeError):
    """A content-free execution-runner failure."""


class _TerminationRequested(Exception):
    def __init__(self, signum: int) -> None:
        self.signum = signum
        super().__init__("execution runner termination requested")


@dataclass(frozen=True)
class ExecutionRunnerConfig:
    database_path: Path = field(repr=False)
    run_root: Path = field(repr=False)
    gw_endpoint: str = field(repr=False)
    gw_alias: str = field(repr=False)
    gw_token_file: Path = field(repr=False)
    agent_command: str = field(default="hermes", repr=False)
    profile_registry: AgentProfileRegistry = field(
        default_factory=load_registry, repr=False
    )
    default_agent_profile: str = "general"
    worker_command: str = "foxhound-task-worker"
    #: Where this machine keeps the knowledge base. Per host, never derived:
    #: the sync roots differ across the fleet.
    knowledge_root: Path | None = field(default=None, repr=False)
    #: Explicit per-machine Syncthing destinations. They are a pair because a
    #: task must never become searchable without retaining its working evidence,
    #: or retain evidence without leaving the searchable task note.
    task_work_root: Path | None = field(default=None, repr=False)
    task_kb_root: Path | None = field(default=None, repr=False)
    allowed_phases: tuple[WorkflowPhase, ...] = tuple(WorkflowPhase)
    planning_grants: tuple[str, ...] = ()
    runner_slot: str = "default"
    poll_seconds: float = 0.1

    def __post_init__(self) -> None:
        if (
            not isinstance(self.database_path, Path)
            or not isinstance(self.run_root, Path)
            or not isinstance(self.gw_token_file, Path)
            or not isinstance(self.gw_endpoint, str)
            or not isinstance(self.gw_alias, str)
        ):
            raise ValueError("execution runner configuration is invalid")
        _agent_command_argv(self.agent_command)
        if not isinstance(self.profile_registry, AgentProfileRegistry):
            raise ValueError("agent profile registry is invalid")
        if (
            not isinstance(self.default_agent_profile, str)
        ):
            raise ValueError("execution default agent profile is invalid")
        profile = self.profile_registry.get(self.default_agent_profile)
        if profile is None or WorkflowPhase.PLAN.value not in profile.allowed_phases:
            raise ValueError("execution default agent profile is invalid")
        if not _COMMAND_NAME_RE.fullmatch(self.worker_command):
            raise ValueError("execution worker command is invalid")
        if (
            not isinstance(self.allowed_phases, tuple)
            or not self.allowed_phases
            or any(
                not isinstance(phase, WorkflowPhase)
                for phase in self.allowed_phases
            )
            or len(set(self.allowed_phases)) != len(self.allowed_phases)
        ):
            raise ValueError("execution phase allowlist is invalid")
        if (
            not isinstance(self.runner_slot, str)
            or not _RUNNER_SLOT_RE.fullmatch(self.runner_slot)
        ):
            raise ValueError("execution runner slot is invalid")
        if not isinstance(self.planning_grants, tuple):
            raise ValueError("execution planning grants are invalid")
        try:
            _planning_grants(self.planning_grants)
        except ValueError as exc:
            raise ValueError("execution planning grants are invalid") from exc
        if (
            isinstance(self.poll_seconds, bool)
            or not isinstance(self.poll_seconds, (int, float))
            or not math.isfinite(self.poll_seconds)
            or self.poll_seconds <= 0
        ):
            raise ValueError("execution poll interval is invalid")
        if (self.task_work_root is None) != (self.task_kb_root is None):
            raise ValueError("task archive roots must be configured together")
        for path in (self.task_work_root, self.task_kb_root):
            if path is not None and (
                not isinstance(path, Path) or not path.is_absolute()
            ):
                raise ValueError("task archive root is invalid")


@dataclass(frozen=True)
class ExecutionRunResult:
    outcome: str
    exit_code: int
    task_id: int | None = field(default=None, repr=False)
    forced_kill: bool = False
    #: Workflows this pass could not run and deferred, by task ID. Reported
    #: so a queue that is shedding work does not read as an empty one.
    deferred: tuple[int, ...] = ()

    @property
    def ok(self) -> bool:
        return self.exit_code == 0


def agent_prompt(worker_command: str = "foxhound-task-worker") -> str:
    """Return the public bootstrap placed in the agent's arguments."""
    try:
        return render_bootstrap(worker_command)
    except AgentProfileError as exc:
        raise ValueError("execution worker command is invalid") from exc


def hermes_argv(
    command: str,
    *,
    max_turns: int,
    worker_command: str = "foxhound-task-worker",
    toolsets: str | None = None,
) -> tuple[str, ...]:
    if (
        not isinstance(max_turns, int)
        or isinstance(max_turns, bool)
        or max_turns < 1
    ):
        raise ValueError("execution agent turn limit is invalid")
    base = _agent_command_argv(command)
    argv = [
        *base,
        "chat",
        "--quiet",
        "--query",
        agent_prompt(worker_command),
        "--max-turns",
        str(max_turns),
        "--source",
        "tool",
        "--ignore-rules",
    ]
    if toolsets:
        if not isinstance(toolsets, str) or "\0" in toolsets:
            raise ValueError("execution agent toolsets are invalid")
        argv.extend(("--toolsets", toolsets))
    return tuple(argv)


def profile_argv(
    command: str,
    profile: AgentProfile,
    *,
    worker_command: str = "foxhound-task-worker",
) -> tuple[str, ...]:
    """Build the exact Hermes invocation for one validated profile.

    The profile's own instructions are not passed here. Process arguments are
    readable outside this run, so they carry only the public bootstrap; the
    instructions reach the agent through the fenced worker instead.
    `--ignore-rules` keeps ambient rule, memory, and skill injection from
    changing behavior behind an already recorded revision.
    """
    if not isinstance(profile, AgentProfile) or profile.runtime != "hermes":
        raise ValueError("execution agent profile is invalid")
    base = _agent_command_argv(command)
    return (
        *base,
        "chat",
        "--quiet",
        "--query",
        agent_prompt(worker_command),
        "--max-turns",
        str(profile.max_turns),
        "--source",
        "tool",
        "--ignore-rules",
        "--toolsets",
        ",".join(profile.toolsets),
    )


def _agent_command_argv(command: object) -> tuple[str, ...]:
    if not isinstance(command, str) or not command.strip():
        raise ValueError("execution agent command is invalid")
    try:
        base = tuple(shlex.split(command))
    except ValueError:
        raise ValueError("execution agent command is invalid") from None
    if not base or any(not value or "\0" in value for value in base):
        raise ValueError("execution agent command is invalid")
    return base


def run_once(
    config: ExecutionRunnerConfig,
    *,
    base_environment: Mapping[str, str] | None = None,
    popen: Callable[..., subprocess.Popen] = subprocess.Popen,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
    run_id_factory: Callable[[], str] | None = None,
    terminate: Callable[..., bool] | None = None,
) -> ExecutionRunResult:
    if not isinstance(config, ExecutionRunnerConfig):
        raise ValueError("execution runner configuration is invalid")
    root = _private_run_root(config.run_root)
    database = _canonical_database(config.database_path)
    load_knowledge_config(
        config.gw_endpoint, config.gw_alias, config.gw_token_file
    )
    service = TaskExecutionService(
        database,
        profile_registry=config.profile_registry,
        default_profile_id=config.default_agent_profile,
        planning_grants=config.planning_grants,
    )
    terminator = terminate or _terminate_process_group
    with _exclusive_lock(_runner_lock_path(root, config.runner_slot)) as acquired:
        if not acquired:
            return ExecutionRunResult("already_running", 0)
        # Refill before and after the claim.  The second pass replaces the
        # queue position the slot just consumed, keeping ten plans durable
        # while two independent runners work.
        service.schedule_new()
        claim = service.claim_next(
            allowed_phases=config.allowed_phases,
        )
        deferred = service.last_claim_deferred
        if claim is None:
            return ExecutionRunResult("idle", 0, deferred=deferred)
        service.schedule_new()
        try:
            profile = config.profile_registry.resolve(
                claim.agent_profile_id, claim.agent_profile_revision
            )
        except AgentProfileError:
            _fail_claim(service, claim, "startup_failed")
            return ExecutionRunResult(
                "startup_failed", STARTUP_EXIT_CODE, claim.task_id
            )
        return _run_claim(
            config,
            service,
            claim,
            profile,
            root,
            base_environment=base_environment,
            popen=popen,
            clock=clock,
            sleep=sleep,
            run_id_factory=run_id_factory or (lambda: secrets.token_hex(16)),
            terminate=terminator,
        )


def _run_claim(
    config: ExecutionRunnerConfig,
    service: TaskExecutionService,
    claim: ExecutionClaim,
    profile: AgentProfile,
    root: Path,
    *,
    base_environment: Mapping[str, str] | None,
    popen: Callable[..., subprocess.Popen],
    clock: Callable[[], float],
    sleep: Callable[[float], None],
    run_id_factory: Callable[[], str],
    terminate: Callable[..., bool],
) -> ExecutionRunResult:
    run_id = run_id_factory()
    if not isinstance(run_id, str) or not _RUN_ID_RE.fullmatch(run_id):
        _fail_claim(service, claim, "startup_failed")
        return ExecutionRunResult("startup_failed", STARTUP_EXIT_CODE, claim.task_id)
    try:
        command = profile_argv(
            config.agent_command,
            profile,
            worker_command=config.worker_command,
        )
    except ValueError:
        _fail_claim(service, claim, "startup_failed")
        return ExecutionRunResult("startup_failed", STARTUP_EXIT_CODE, claim.task_id)
    directory = root / f"run-{run_id}"
    archive: TaskArchivePaths | None = None
    try:
        directory.mkdir(mode=0o700)
        if config.task_work_root is not None and config.task_kb_root is not None:
            origin = TaskLedger(config.database_path).origin(claim.task_id)
            archive = prepare_task_archive(
                working_root=config.task_work_root,
                kb_root=config.task_kb_root,
                task_id=claim.task_id,
                task_text=claim.text,
                run_id=run_id,
                phase=claim.phase.value,
                agent_display_name=profile.display_name,
                origin_kind=None if origin is None else origin.kind,
                origin_record=None if origin is None else origin.record_id,
                origin_item=None if origin is None else origin.item_id,
            )
        state_path = directory / "run-state.json"
        instructions_path = directory / INSTRUCTIONS_NAME
        _write_state(state_path, config, claim, profile, run_id, archive)
        _write_instructions(instructions_path, profile)
    except (OSError, TaskArchiveError, TaskLedgerError):
        _fail_claim(service, claim, "startup_failed")
        return ExecutionRunResult("startup_failed", STARTUP_EXIT_CODE, claim.task_id)

    environment = dict(os.environ if base_environment is None else base_environment)
    environment.update({
        STATE_ENV: str(state_path),
        GW_ENDPOINT_ENV: config.gw_endpoint,
        GW_ALIAS_ENV: config.gw_alias,
        GW_TOKEN_FILE_ENV: str(config.gw_token_file),
        "HERMES_CRON_SESSION": "1",
    })
    process: subprocess.Popen | None = None
    transcript = None
    forced = False
    prior_handlers: dict[int, object] = {}
    try:
        initial = service.get(claim.task_id)
        if (
            initial is None
            or initial.status is not WorkflowStatus.RUNNING
            or initial.version != claim.workflow_version
        ):
            _fail_claim(service, claim, "startup_failed")
            return ExecutionRunResult(
                "claim_lost", NO_PROGRESS_EXIT_CODE, claim.task_id
            )
        try:
            transcript = _open_transcript(directory)
        except OSError:
            transcript = None
        try:
            process = popen(
                list(command),
                env=environment,
                stdin=subprocess.DEVNULL,
                stdout=transcript or subprocess.DEVNULL,
                stderr=subprocess.STDOUT if transcript else subprocess.DEVNULL,
                cwd=str(directory),
                start_new_session=True,
                shell=False,
            )
        except OSError:
            _fail_claim(service, claim, "startup_failed")
            return ExecutionRunResult(
                "startup_failed", STARTUP_EXIT_CODE, claim.task_id
            )
        try:
            for signum in (signal.SIGTERM, signal.SIGHUP, signal.SIGINT):
                prior_handlers[signum] = signal.getsignal(signum)
                signal.signal(signum, _raise_termination)
        except ValueError:
            prior_handlers.clear()

        started = clock()
        next_heartbeat = started + profile.heartbeat_seconds
        while True:
            current = service.get(claim.task_id)
            terminal = _terminal_result(initial, current, claim.task_id)
            if terminal is not None:
                forced = terminate(
                    process, profile.kill_grace_seconds,
                    sleep=sleep, clock=clock
                )
                return ExecutionRunResult(
                    terminal, 0 if terminal == "recorded" else NO_PROGRESS_EXIT_CODE,
                    claim.task_id, forced,
                )

            now = clock()
            if now >= next_heartbeat:
                try:
                    renewed = service.renew(
                        claim.task_id,
                        expected_version=claim.workflow_version,
                        claim_token=claim.token,
                        lease_seconds=profile.claim_lease_seconds,
                    )
                except Exception:
                    forced = terminate(
                        process, profile.kill_grace_seconds,
                        sleep=sleep, clock=clock,
                    )
                    _fail_claim(service, claim, "lease_failed")
                    return ExecutionRunResult(
                        "lease_failed", NO_PROGRESS_EXIT_CODE,
                        claim.task_id, forced,
                    )
                if renewed.disposition is WorkflowDisposition.REFUSED:
                    forced = terminate(
                        process, profile.kill_grace_seconds,
                        sleep=sleep, clock=clock,
                    )
                    return ExecutionRunResult(
                        "claim_lost", NO_PROGRESS_EXIT_CODE,
                        claim.task_id, forced,
                    )
                next_heartbeat = now + profile.heartbeat_seconds

            child_exit = process.poll()
            if child_exit is not None:
                current = service.get(claim.task_id)
                terminal = _terminal_result(initial, current, claim.task_id)
                if terminal is not None:
                    return ExecutionRunResult(
                        terminal,
                        0 if terminal == "recorded" else NO_PROGRESS_EXIT_CODE,
                        claim.task_id,
                    )
                normalized = (
                    128 + abs(child_exit) if child_exit < 0 else child_exit
                )
                return _failure_result(
                    service,
                    claim,
                    initial,
                    reason="process_exit",
                    outcome="process_exit",
                    exit_code=normalized or NO_PROGRESS_EXIT_CODE,
                )

            if now - started >= profile.timeout_seconds:
                forced = terminate(
                    process, profile.kill_grace_seconds,
                    sleep=sleep, clock=clock
                )
                return _failure_result(
                    service,
                    claim,
                    initial,
                    reason="timeout",
                    outcome="timeout",
                    exit_code=TIMEOUT_EXIT_CODE,
                    forced=forced,
                )
            sleep(config.poll_seconds)
    except _TerminationRequested as exc:
        if process is not None:
            forced = terminate(
                process, profile.kill_grace_seconds,
                sleep=sleep, clock=clock
            )
        return _failure_result(
            service,
            claim,
            initial,
            reason="interrupted",
            outcome="interrupted",
            exit_code=128 + exc.signum,
            forced=forced,
        )
    except BaseException:
        if process is not None:
            terminate(
                process, profile.kill_grace_seconds,
                sleep=sleep, clock=clock
            )
        _fail_claim(service, claim, "interrupted")
        raise
    finally:
        for signum, handler in prior_handlers.items():
            signal.signal(signum, handler)
        if transcript is not None:
            with contextlib.suppress(OSError):
                transcript.close()
        _scrub_state_receipt(state_path, run_id, claim.task_id)
        with contextlib.suppress(OSError):
            instructions_path.unlink(missing_ok=True)
        if archive is not None:
            preserve_run_files(directory, archive.run_directory)
            # Then again, flattened, at the top of the task folder. The run
            # directory is the record; the folder is what the reader opens.
            with contextlib.suppress(TaskArchiveError):
                publish_deliverables(archive, directory)


def _terminal_result(
    initial: ExecutionWorkflow,
    current: ExecutionWorkflow | None,
    task_id: int,
) -> str | None:
    if current is None or current.task_id != task_id:
        return "claim_lost"
    if (
        current.status is WorkflowStatus.RUNNING
        and current.version == initial.version
    ):
        return None
    if (
        current.last_result_id is not None
        and current.last_result_id != initial.last_result_id
        and current.status in {
            WorkflowStatus.AWAITING_REVIEW,
            WorkflowStatus.COMPLETED,
        }
    ):
        return "recorded"
    if current.status is WorkflowStatus.QUEUED:
        return "released"
    return "claim_lost"


def _runner_lock_path(root: Path, slot: str) -> Path:
    """One lock per declared local execution slot, never per process."""
    if not isinstance(root, Path) or not _RUNNER_SLOT_RE.fullmatch(slot):
        raise ValueError("execution runner slot is invalid")
    return root / f".runner-{slot}.lock"


#: Owner-only, beside the result the agent writes, and never anywhere a
#: repository or a log aggregator can reach. The contents are the agent's
#: own working output and are as private as the task it was given.
def _open_transcript(directory: Path):
    """Keep what the agent said, so a failed run can be explained.

    Output used to be discarded. A supervised run that ends without
    recording anything then leaves nothing behind but the fact that it
    failed: not the refusal it was given, not the command it tried, not
    the budget it ran out of. Two runs of the same task each produced a
    complete result file and neither could be explained.
    """
    def opener(target: str, _flags: int) -> int:
        return os.open(
            target,
            os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_NOFOLLOW,
            0o600,
        )

    return open(directory / TRANSCRIPT_NAME, "wb", opener=opener)


def _fail_claim(
    service: TaskExecutionService, claim: ExecutionClaim, reason: str
) -> WorkflowOperationResult | None:
    try:
        return service.fail(
            claim.task_id,
            expected_version=claim.workflow_version,
            claim_token=claim.token,
            reason=reason,
        )
    except Exception:
        return None


def _failure_result(
    service: TaskExecutionService,
    claim: ExecutionClaim,
    initial: ExecutionWorkflow,
    *,
    reason: str,
    outcome: str,
    exit_code: int,
    forced: bool = False,
) -> ExecutionRunResult:
    failed = _fail_claim(service, claim, reason)
    if (
        failed is not None
        and failed.disposition is WorkflowDisposition.APPLIED
    ):
        return ExecutionRunResult(outcome, exit_code, claim.task_id, forced)
    try:
        terminal = _terminal_result(
            initial, service.get(claim.task_id), claim.task_id
        )
    except Exception:
        terminal = None
    if terminal in {"recorded", "released"}:
        return ExecutionRunResult(
            terminal,
            0 if terminal == "recorded" else NO_PROGRESS_EXIT_CODE,
            claim.task_id,
            forced,
        )
    return ExecutionRunResult(
        "claim_lost", NO_PROGRESS_EXIT_CODE, claim.task_id, forced
    )


def _private_run_root(path: Path) -> Path:
    if not path.is_absolute():
        raise ExecutionRunnerError("execution run root is invalid")
    try:
        if path.resolve(strict=True) != path:
            raise ExecutionRunnerError("execution run root is invalid")
        info = path.lstat()
    except OSError:
        raise ExecutionRunnerError("execution run root is unavailable") from None
    if not stat.S_ISDIR(info.st_mode) or info.st_mode & 0o077:
        raise ExecutionRunnerError("execution run root is not private")
    return path


def _canonical_database(path: Path) -> Path:
    if not path.is_absolute():
        raise ExecutionRunnerError("execution database path is invalid")
    try:
        info = path.lstat()
        if (
            path.resolve(strict=True) != path
            or not stat.S_ISREG(info.st_mode)
            or info.st_mode & 0o077
        ):
            raise ExecutionRunnerError("execution database path is invalid")
    except OSError:
        raise ExecutionRunnerError(
            "execution database is unavailable"
        ) from None
    return path


@contextlib.contextmanager
def _exclusive_lock(path: Path) -> Iterator[bool]:
    flags = os.O_RDWR | os.O_CREAT
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(path, flags, 0o600)
    except OSError:
        raise ExecutionRunnerError(
            "execution runner lock is unavailable"
        ) from None
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode) or info.st_mode & 0o077:
            raise ExecutionRunnerError("execution runner lock is not private")
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            yield False
            return
        try:
            yield True
        finally:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
    finally:
        os.close(descriptor)


def _write_state(
    path: Path,
    config: ExecutionRunnerConfig,
    claim: ExecutionClaim,
    profile: AgentProfile,
    run_id: str,
    archive: TaskArchivePaths | None,
) -> None:
    document = {
        "schema": RUN_STATE_SCHEMA,
        "schema_version": RUN_STATE_SCHEMA_VERSION,
        "run_id": run_id,
        "database_path": str(config.database_path),
        "task_id": claim.task_id,
        "task_version": claim.task_version,
        "workflow_version": claim.workflow_version,
        "phase": claim.phase.value,
        "claim_token": claim.token,
        "lease_seconds": profile.claim_lease_seconds,
        "agent_profile_id": claim.agent_profile_id,
        "agent_profile_revision": claim.agent_profile_revision,
        "knowledge_root": (
            None if config.knowledge_root is None
            else str(config.knowledge_root)
        ),
        "task_work_directory": (
            None if archive is None else str(archive.working_directory)
        ),
        "task_kb_file": None if archive is None else str(archive.task_file),
        "task_run_directory": (
            None if archive is None else str(archive.run_directory)
        ),
        "worker_command": config.worker_command,
    }
    payload = (
        json.dumps(
            document, ensure_ascii=True, separators=(",", ":"), sort_keys=True
        ) + "\n"
    ).encode("utf-8")
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(path, flags, 0o600)
    try:
        try:
            with os.fdopen(descriptor, "wb", closefd=False) as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
        except BaseException:
            try:
                os.ftruncate(descriptor, 0)
            except OSError:
                pass
            try:
                os.unlink(path)
            except OSError:
                pass
            raise
    finally:
        os.close(descriptor)


def _write_instructions(path: Path, profile: AgentProfile) -> None:
    """Leave this run's exact instructions where only its worker reads them."""
    payload = (
        json.dumps(
            profile.document(),
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ) + "\n"
    ).encode("utf-8")
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(path, flags, 0o600)
    try:
        try:
            with os.fdopen(descriptor, "wb", closefd=False) as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
        except BaseException:
            with contextlib.suppress(OSError):
                os.ftruncate(descriptor, 0)
            with contextlib.suppress(OSError):
                os.unlink(path)
            raise
    finally:
        os.close(descriptor)


def _scrub_state_receipt(
    path: Path,
    run_id: str,
    task_id: int,
) -> None:
    document = {
        "schema": "foxhound.execution-run-receipt",
        "schema_version": WORKER_SCHEMA_VERSION,
        "run_id": run_id,
        "task_id": task_id,
        "finished": True,
    }
    payload = (
        json.dumps(
            document, ensure_ascii=True, separators=(",", ":"), sort_keys=True
        ) + "\n"
    ).encode("utf-8")
    flags = os.O_WRONLY | os.O_TRUNC
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(path, flags)
        try:
            info = os.fstat(descriptor)
            if not stat.S_ISREG(info.st_mode) or info.st_mode & 0o077:
                return
            with os.fdopen(descriptor, "wb", closefd=False) as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
        finally:
            os.close(descriptor)
    except OSError:
        return


def _raise_termination(signum: int, _frame: object) -> None:
    raise _TerminationRequested(signum)


def _terminate_process_group(
    process: subprocess.Popen,
    grace_seconds: float,
    *,
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
) -> bool:
    if process.poll() is not None:
        return False
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        return False
    deadline = clock() + grace_seconds
    while process.poll() is None and clock() < deadline:
        sleep(min(0.1, max(0.001, deadline - clock())))
    if process.poll() is not None:
        return False
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        return False
    try:
        process.wait(timeout=grace_seconds)
    except subprocess.TimeoutExpired:
        pass
    return True


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="foxhound-execution-runner",
        description="Run one ready Foxhound execution phase",
    )
    parser.add_argument("--database", required=True, type=Path)
    parser.add_argument("--run-root", required=True, type=Path)
    parser.add_argument("--gw-endpoint", required=True)
    parser.add_argument("--gw-alias", required=True)
    parser.add_argument("--gw-token-file", required=True, type=Path)
    parser.add_argument("--agent-command", default="hermes")
    parser.add_argument("--agent-profile-directory", type=Path)
    parser.add_argument("--default-agent-profile", default="general")
    parser.add_argument("--worker-command", default="foxhound-task-worker")
    parser.add_argument(
        "--runner-slot", default="default",
        help="stable local execution-slot name; distinct slots may run together",
    )
    parser.add_argument(
        "--plan-without-asking", action="append", metavar="SOURCE_KIND",
        help="refill the ready plan reserve for this source kind",
    )
    parser.add_argument(
        "--knowledge-root",
        type=Path,
        help=(
            "directory holding this machine's knowledge base; the agent "
            "reads it and never writes to it"
        ),
    )
    parser.add_argument(
        "--task-work-root",
        type=Path,
        help="machine-local sync root for durable task working folders",
    )
    parser.add_argument(
        "--task-kb-root",
        type=Path,
        help="machine-local knowledge-base Tasks root for task Markdown files",
    )
    parser.add_argument(
        "--allowed-phase",
        action="append",
        choices=tuple(phase.value for phase in WorkflowPhase),
        dest="allowed_phases",
        help=(
            "claim only this workflow phase; repeat to allow multiple phases "
            "(default: all phases)"
        ),
    )
    return parser


def _diagnosis(exc: BaseException) -> str:
    """Name what failed by exception TYPE, following the cause chain.

    The runner used to print one fixed sentence for every unhandled failure.
    That sentence was true and useless: recovering why a real outage had
    stopped every task meant rebuilding the config by hand and calling
    `run_once` directly, because nothing about the cause reached the journal.

    Only class names are printed. A message is not safe to log here -- an
    `OSError` carries the path it failed on, and a dependency's message can
    carry an argument -- and the journal is not a private surface. The chain
    is what actually identifies the fault: `TaskLedgerError <- ` \
    `AgentProfileError` says which of the two it was, which is the whole
    question, and it says it without quoting anything.
    """
    names: list[str] = []
    seen: set[int] = set()
    current: BaseException | None = exc
    while current is not None and id(current) not in seen and len(names) < 5:
        seen.add(id(current))
        names.append(type(current).__name__)
        current = current.__cause__ or current.__context__
    return " <- ".join(names)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        config = ExecutionRunnerConfig(
            database_path=args.database,
            run_root=args.run_root,
            gw_endpoint=args.gw_endpoint,
            gw_alias=args.gw_alias,
            gw_token_file=args.gw_token_file,
            agent_command=args.agent_command,
            profile_registry=load_registry(args.agent_profile_directory),
            default_agent_profile=args.default_agent_profile,
            worker_command=args.worker_command,
            runner_slot=args.runner_slot,
            planning_grants=tuple(args.plan_without_asking or ()),
            knowledge_root=args.knowledge_root,
            task_work_root=args.task_work_root,
            task_kb_root=args.task_kb_root,
            allowed_phases=(
                tuple(WorkflowPhase(value) for value in args.allowed_phases)
                if args.allowed_phases
                else tuple(WorkflowPhase)
            ),
        )
        result = run_once(config)
    except (
        AgentProfileError,
        ValueError,
        ExecutionRunnerError,
        ExecutionWorkerConfigError,
        TaskArchiveError,
    ) as exc:
        print(
            "foxhound execution runner: configuration unavailable: "
            + _diagnosis(exc),
            file=sys.stderr,
        )
        return 78
    except Exception as exc:  # noqa: BLE001
        print(
            "foxhound execution runner: execution failed: " + _diagnosis(exc),
            file=sys.stderr,
        )
        return 70
    report = {"ok": result.ok, "outcome": result.outcome}
    if result.deferred:
        report["deferred"] = list(result.deferred)
    print(json.dumps(report))
    return result.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
