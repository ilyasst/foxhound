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
from typing import Any, Callable, Iterator, Mapping, Sequence

from .workflow_policy import WorkflowPolicyError, parse_workflow_policy
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
    WORKFLOW_POLICY_ENV,
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
    clear_missing_runtime_logs,
    prepare_task_archive,
    record_runtime_log,
    preserve_run_files,
    publish_deliverables,
)
from .runtime_session_log import (
    DEFAULT_RUNTIME_LOG_RETENTION_BYTES,
    RUNTIME_SESSION_LOG_NAME,
    RuntimeSessionLogError,
    rotate_runtime_session_logs,
    write_runtime_session_log,
)
from .task_ledger import TaskLedger, TaskLedgerError
from .source_policy import action_grants as _action_grants
from .source_policy import execution_grants as _execution_grants
from .source_policy import planning_grants as _planning_grants
from .worker_resolution import (
    WorkerMismatch,
    is_worker_command,
    resolve_worker_command,
    verify_worker,
)


NO_PROGRESS_EXIT_CODE = 70
STARTUP_EXIT_CODE = 71
TIMEOUT_EXIT_CODE = 124
_RUN_ID_RE = re.compile(r"^[0-9a-f]{32}$")
_RUNNER_SLOT_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,31}$")
# A gateway refusal with a measured request size is authoritative evidence
# that the current run cannot fit a served context window. It is deliberately
# narrower than words such as "context" or a long duration: those are normal
# parts of many successful runs and must never change retry policy.
_CONTEXT_FILTER_REFUSAL = re.compile(
    rb"\bcontext\s+filter\s*:\s*.*?\bneeds\s+~?\d+\s+tokens?\s*,\s*"
    rb"skipping\b",
    re.IGNORECASE | re.DOTALL,
)
_CONTEXT_EVIDENCE_BYTES = 256 * 1024


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
    #: The private Hermes state database from which this runner copies its
    #: own tagged structured session record after a run finishes.
    runtime_session_database: Path | None = field(default=None, repr=False)
    runtime_log_retention_bytes: int = DEFAULT_RUNTIME_LOG_RETENTION_BYTES
    allowed_phases: tuple[WorkflowPhase, ...] = tuple(WorkflowPhase)
    planning_grants: tuple[str, ...] = ()
    execution_grants: tuple[str, ...] = ()
    action_grants: tuple[str, ...] = ()
    #: One versioned workflow policy document, or None for the compatibility
    #: policy that grants nothing and checks nothing.
    workflow_policy: Mapping[str, Any] | None = None
    runner_slot: str = "default"
    poll_seconds: float = 0.1
    execution_slot_cap: int | None = None
    plan_ready_cap: int | None = None
    awaiting_reader_cap: int | None = None
    profile_routes: Mapping[str, str] = field(default_factory=dict)

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
        if not is_worker_command(self.worker_command):
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
        if not isinstance(self.execution_grants, tuple):
            raise ValueError("execution grants are invalid")
        try:
            _execution_grants(self.execution_grants)
        except ValueError as exc:
            raise ValueError("execution grants are invalid") from exc
        if not isinstance(self.action_grants, tuple):
            raise ValueError("execution action grants are invalid")
        try:
            _action_grants(self.action_grants)
        except ValueError as exc:
            raise ValueError("execution action grants are invalid") from exc
        if (
            not isinstance(self.profile_routes, Mapping)
            or any(
                not isinstance(kind, str) or not kind
                or not isinstance(profile_id, str) or not profile_id
                for kind, profile_id in self.profile_routes.items()
            )
        ):
            raise ValueError("execution agent profile routes are invalid")
        if self.workflow_policy is not None:
            try:
                parse_workflow_policy(self.workflow_policy)
            except WorkflowPolicyError as exc:
                # Rejected here rather than in the worker: a policy that only
                # fails once a run has claimed a task burns the claim.
                raise ValueError("workflow policy is invalid") from exc
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
        if self.runtime_session_database is not None and (
            not isinstance(self.runtime_session_database, Path)
            or not self.runtime_session_database.is_absolute()
        ):
            raise ValueError("runtime session database is invalid")
        if (
            isinstance(self.runtime_log_retention_bytes, bool)
            or not isinstance(self.runtime_log_retention_bytes, int)
            or self.runtime_log_retention_bytes < 1
        ):
            raise ValueError("runtime log retention is invalid")
        if self.runtime_session_database is not None and self.task_work_root is None:
            raise ValueError("runtime session logs require task archive roots")


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


def _worker_command(config: "ExecutionRunnerConfig") -> str:
    """The worker this runner means, as an absolute path where possible.

    The agent resolves whatever it is handed in its own shell, not this
    process's, so a bare name is a question answered somewhere the runner
    cannot see -- and on a deployed host ``~/.local/bin`` wins it. Naming
    the worker beside this interpreter removes the question. Issue #431.
    """
    return resolve_worker_command(config.worker_command)


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
    source: str = "tool",
) -> tuple[str, ...]:
    """Build the exact Hermes invocation for one validated profile.

    The profile's own instructions are not passed here. Process arguments are
    readable outside this run, so they carry only the public bootstrap; the
    instructions reach the agent through the fenced worker instead.
    `--ignore-rules` keeps ambient rule, memory, and skill injection from
    changing behavior behind an already recorded revision.
    """
    if (
        not isinstance(profile, AgentProfile)
        or profile.runtime != "hermes"
        or not isinstance(source, str)
        or not source
        or "\0" in source
    ):
        raise ValueError("execution agent profile is invalid")
    base = _agent_command_argv(command)
    return (
        *base,
        "chat",
        "--query",
        agent_prompt(worker_command),
        "--max-turns",
        str(profile.max_turns),
        "--source",
        source,
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
        execution_grants=config.execution_grants,
        action_grants=config.action_grants,
        profile_routes=config.profile_routes,
        execution_slot_cap=config.execution_slot_cap,
        plan_ready_cap=config.plan_ready_cap,
        awaiting_reader_cap=config.awaiting_reader_cap,
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
            worker_command=_worker_command(config),
            source="foxhound-" + run_id,
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
        WORKFLOW_POLICY_ENV: (
            "" if config.workflow_policy is None
            else json.dumps(config.workflow_policy, sort_keys=True)
        ),
        "HERMES_CRON_SESSION": "1",
        # The agent's stdout is a file, not a terminal, so a Python agent
        # block-buffers it at 8 KiB. A run that ends by being killed --
        # which the profile timeout always does, after the kill grace --
        # loses whatever is still in that buffer. The transcript then holds
        # only the unbuffered stderr line an agent happens to emit at
        # startup, and a run that worked for its whole budget is
        # indistinguishable from one that never began.
        #
        # `_open_transcript` exists to "keep what the agent said, so a
        # failed run can be explained". Buffering defeated it in exactly
        # the failure it was written for: runs whose transcripts held one
        # stderr line had, in the same wall-clock window, made hundreds of
        # model calls. Set from this dict rather than inherited, so an
        # ambient value cannot reinstate buffering. Inert for a non-Python
        # `agent_command`.
        "PYTHONUNBUFFERED": "1",
        # The transcript is a file that a person reads to explain a failed
        # run, so it carries the agent's tool previews -- `--quiet` is not
        # passed, because a transcript of the closing text alone cannot
        # distinguish a run that acted twenty times from one that never
        # acted at all. Those previews are drawn for a terminal. Declare
        # there is none: without this the file fills with colour escapes
        # and carriage-return redraw, which no pager and no diff can read.
        # Set from this dict rather than inherited, for the same reason as
        # the line above.
        "NO_COLOR": "1",
        "TERM": "dumb",
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
                    run_id=run_id,
                )

            if now - started >= profile.timeout_seconds:
                forced = terminate(
                    process, profile.kill_grace_seconds,
                    sleep=sleep, clock=clock
                )
                reason = (
                    "context_exhausted"
                    if _context_window_exhausted(
                        directory / TRANSCRIPT_NAME, transcript
                    ) else "timeout"
                )
                return _failure_result(
                    service,
                    claim,
                    initial,
                    reason=reason,
                    outcome=reason,
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
            if config.runtime_session_database is not None:
                # This block runs in a `finally`, so an exception here would
                # replace the result the run already recorded -- turning a
                # completed run into a runner-level failure, and skipping
                # the deliverable publication below. The record is evidence
                # about a run, not part of it: a runtime that never opened a
                # session, a database mid-write, or a path that has moved
                # must cost the evidence and nothing else. Same reason the
                # publication below suppresses its own error.
                with contextlib.suppress(
                    RuntimeSessionLogError, TaskArchiveError
                ):
                    write_runtime_session_log(
                        config.runtime_session_database,
                        source="foxhound-" + run_id,
                        destination=archive.run_directory,
                        turn_budget=profile.max_turns,
                    )
                    rotate_runtime_session_logs(
                        archive.working_directory,
                        retain_bytes=config.runtime_log_retention_bytes,
                        current_directory=archive.run_directory,
                    )
                    record_runtime_log(archive, RUNTIME_SESSION_LOG_NAME)
                # Outside the suppression above: whether or not this run's
                # record was written, the ledger must not keep pointing at
                # one that rotation has removed.
                with contextlib.suppress(TaskArchiveError):
                    clear_missing_runtime_logs(archive)
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
            # A granted advance queues the next phase instead of raising a
            # card, so the run that produced the result ends `queued` and
            # used to be reported as making no progress -- exit 70 on every
            # successful run, on exactly the deployments that configure a
            # grant. A new result is what separates the two: a release
            # leaves `last_result_id` untouched, and the guard above
            # already requires that it changed.
            WorkflowStatus.QUEUED,
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


def _context_window_exhausted(path: Path, transcript: object) -> bool:
    """Whether a timed-out run recorded the gateway's measured refusal.

    The runner has already established no result was recorded. The only extra
    evidence considered here is the gateway's exact context-filter line with
    a numeric request size, so no duration or free-form agent prose is used as
    a proxy for context exhaustion. Reading only the tail is both bounded and
    appropriate: the refusal is the final attempted operation before a run
    begins trying to compress or reaches its supervision timeout.
    """
    if transcript is None:
        return False
    try:
        transcript.flush()
        size = path.stat().st_size
        with path.open("rb") as handle:
            if size > _CONTEXT_EVIDENCE_BYTES:
                handle.seek(size - _CONTEXT_EVIDENCE_BYTES)
            evidence = handle.read(_CONTEXT_EVIDENCE_BYTES)
    except (AttributeError, OSError):
        return False
    return _CONTEXT_FILTER_REFUSAL.search(evidence) is not None


def _fail_claim(
    service: TaskExecutionService,
    claim: ExecutionClaim,
    reason: str,
    *,
    exit_code: int | None = None,
    run_id: str | None = None,
) -> WorkflowOperationResult | None:
    try:
        diagnostics: dict[str, object] = {}
        if exit_code is not None:
            diagnostics["exit_code"] = exit_code
            diagnostics["run_id"] = run_id
        return service.fail(
            claim.task_id,
            expected_version=claim.workflow_version,
            claim_token=claim.token,
            reason=reason,
            **diagnostics,
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
    run_id: str | None = None,
    forced: bool = False,
) -> ExecutionRunResult:
    failed = _fail_claim(
        service,
        claim,
        reason,
        exit_code=exit_code if reason == "process_exit" else None,
        run_id=run_id if reason == "process_exit" else None,
    )
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
        "worker_command": _worker_command(config),
        # These are private run authority, not runner-only switches: the
        # worker records the result and therefore decides whether its phase
        # advances without a reader card.
        "execution_grants": list(config.execution_grants),
        "action_grants": list(config.action_grants),
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


def _read_workflow_policy(path: object) -> dict[str, Any] | None:
    """Load one policy document, or None when the deployment configures none."""
    if path is None:
        return None
    try:
        document = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, ValueError):
        raise SystemExit("workflow policy could not be read") from None
    if not isinstance(document, dict):
        raise SystemExit("workflow policy could not be read")
    return document


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
    parser.add_argument(
        "--profile-route", action="append", metavar="SOURCE_KIND=PROFILE",
    )
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
        "--execute-without-asking", action="append", metavar="SOURCE_KIND",
        help="run a recorded plan for this source kind without a card",
    )
    parser.add_argument(
        "--act-without-asking", action="append", metavar="SOURCE_KIND",
        help="perform a reviewed external action for this source kind "
             "without a card",
    )
    parser.add_argument(
        "--workflow-policy",
        type=Path,
        help="path to one versioned workflow policy document; omitted means "
             "the compatibility policy, which checks no source freshness",
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
        "--runtime-session-database",
        type=Path,
        help=(
            "private Hermes state database used to copy this run's structured "
            "session record beside its task evidence"
        ),
    )
    parser.add_argument(
        "--runtime-log-retention-bytes",
        type=int,
        default=DEFAULT_RUNTIME_LOG_RETENTION_BYTES,
        help="total private structured runtime-log history kept per task",
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
    parser.add_argument(
        "--execution-slot-cap",
        type=int,
        default=None,
        help=(
            "how many workflows this machine may execute at once; -1 means "
            "no cap. Omitted keeps this machine's compiled-in default."
        ),
    )
    parser.add_argument(
        "--plan-ready-cap",
        type=int,
        default=None,
        help=(
            "how many workflows may sit ready to execute at once; -1 means "
            "no cap. Omitted keeps this machine's compiled-in default."
        ),
    )
    parser.add_argument(
        "--awaiting-reader-cap",
        type=int,
        default=None,
        help=(
            "how many workflows may wait on an operator decision at once; "
            "-1 means no cap. Omitted keeps this machine's compiled-in "
            "default."
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


def _profile_routes(values: Sequence[str] | None) -> dict[str, str]:
    result: dict[str, str] = {}
    for value in values or ():
        kind, separator, profile_id = value.partition("=")
        if not separator or not kind or not profile_id or kind in result:
            raise ValueError("execution agent profile routes are invalid")
        result[kind] = profile_id
    return result


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
            profile_routes=_profile_routes(args.profile_route),
            worker_command=args.worker_command,
            runner_slot=args.runner_slot,
            planning_grants=tuple(args.plan_without_asking or ()),
            execution_grants=tuple(args.execute_without_asking or ()),
            action_grants=tuple(args.act_without_asking or ()),
            workflow_policy=_read_workflow_policy(args.workflow_policy),
            knowledge_root=args.knowledge_root,
            task_work_root=args.task_work_root,
            task_kb_root=args.task_kb_root,
            runtime_session_database=args.runtime_session_database,
            runtime_log_retention_bytes=args.runtime_log_retention_bytes,
            allowed_phases=(
                tuple(WorkflowPhase(value) for value in args.allowed_phases)
                if args.allowed_phases
                else tuple(WorkflowPhase)
            ),
            execution_slot_cap=args.execution_slot_cap,
            plan_ready_cap=args.plan_ready_cap,
            awaiting_reader_cap=args.awaiting_reader_cap,
        )
        # Before anything is claimed. A worker that cannot parse the run
        # state this runner writes fails every run at the agent's first tool
        # call, and burns a claim each time; refusing here costs one poll.
        verify_worker(
            _worker_command(config),
            run_state_schema_version=RUN_STATE_SCHEMA_VERSION,
        )
        result = run_once(config)
    except WorkerMismatch as exc:
        print(
            "foxhound execution runner: refusing to claim: " + str(exc),
            file=sys.stderr,
        )
        return 78
    except (
        AgentProfileError,
        ValueError,
        ExecutionRunnerError,
        ExecutionWorkerConfigError,
        TaskArchiveError,
        RuntimeSessionLogError,
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
