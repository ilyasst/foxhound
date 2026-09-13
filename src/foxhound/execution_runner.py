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
    general_profile,
    load_registry,
)
from .execution_worker import (
    GW_ALIAS_ENV,
    GW_ENDPOINT_ENV,
    GW_TOKEN_FILE_ENV,
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


NO_PROGRESS_EXIT_CODE = 70
STARTUP_EXIT_CODE = 71
TIMEOUT_EXIT_CODE = 124
_COMMAND_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_RUN_ID_RE = re.compile(r"^[0-9a-f]{32}$")


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
    worker_command: str = "foxhound-task-worker"
    allowed_phases: tuple[WorkflowPhase, ...] = tuple(WorkflowPhase)
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
            isinstance(self.poll_seconds, bool)
            or not isinstance(self.poll_seconds, (int, float))
            or not math.isfinite(self.poll_seconds)
            or self.poll_seconds <= 0
        ):
            raise ValueError("execution poll interval is invalid")


@dataclass(frozen=True)
class ExecutionRunResult:
    outcome: str
    exit_code: int
    task_id: int | None = field(default=None, repr=False)
    forced_kill: bool = False

    @property
    def ok(self) -> bool:
        return self.exit_code == 0


def agent_prompt(worker_command: str = "foxhound-task-worker") -> str:
    try:
        return general_profile().render_prompt(worker_command)
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
    """Build the exact Hermes invocation for one validated profile."""
    if not isinstance(profile, AgentProfile) or profile.runtime != "hermes":
        raise ValueError("execution agent profile is invalid")
    base = _agent_command_argv(command)
    try:
        prompt = profile.render_prompt(worker_command)
    except AgentProfileError as exc:
        raise ValueError("execution worker command is invalid") from exc
    return (
        *base,
        "chat",
        "--quiet",
        "--query",
        prompt,
        "--max-turns",
        str(profile.max_turns),
        "--source",
        "tool",
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
        database, profile_registry=config.profile_registry
    )
    terminator = terminate or _terminate_process_group
    with _exclusive_lock(root / ".runner.lock") as acquired:
        if not acquired:
            return ExecutionRunResult("already_running", 0)
        claim = service.claim_next(
            allowed_phases=config.allowed_phases,
        )
        if claim is None:
            return ExecutionRunResult("idle", 0)
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
    try:
        directory.mkdir(mode=0o700)
        state_path = directory / "run-state.json"
        _write_state(state_path, config, claim, profile, run_id)
    except OSError:
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


#: Owner-only, beside the result the agent writes, and never anywhere a
#: repository or a log aggregator can reach. The contents are the agent's
#: own working output and are as private as the task it was given.
TRANSCRIPT_NAME = "agent-output.log"


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
    parser.add_argument("--worker-command", default="foxhound-task-worker")
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
            worker_command=args.worker_command,
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
    ):
        print("foxhound execution runner: configuration unavailable", file=sys.stderr)
        return 78
    except Exception:
        print("foxhound execution runner: execution failed", file=sys.stderr)
        return 70
    print(json.dumps({"ok": result.ok, "outcome": result.outcome}))
    return result.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
