"""Foxhound-owned durable task execution workflow state.

This module owns scheduling, reader gates, fenced worker claims, retry state,
and private execution results.  It launches no process, performs no network
request, renders no card, and never changes task lifecycle status.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import secrets
import sqlite3
from contextlib import closing
from dataclasses import dataclass, field
from datetime import datetime, time, timedelta, timezone
from enum import StrEnum
from pathlib import Path
from typing import Callable, Sequence

from .agent_profiles import (
    AgentProfile,
    AgentProfileError,
    AgentProfileRegistry,
    load_registry,
)
from .candidate_inbox import CandidateInbox, InboxError, SCHEMA_VERSION
from .source_policy import (
    planning_grants as _planning_grants,
    source_kinds_accepting,
)
from .task_ledger import TaskLedgerError, TaskStatus


# Stable callback tokens shared with GW. Their presentation and behavior are
# calendar choices, rather than durations: changing the vocabulary would make
# a new Foxhound card fail closed in an older gateway deployment.
REVIEW_SNOOZE_ACTIONS = frozenset({
    "snooze_1d", "snooze_7d", "snooze_14d", "snooze_30d",
})

#: The bare verb a card keyboard carries, alongside the explicit choices a
#: picker offers. A gateway is expected to rewrite the bare verb into its own
#: picker and never send it on, but a control the service refuses is a dead
#: button, and one dead button teaches a reader that none of them are
#: trustworthy. So it is answered here too, as the nearest choice.
_SNOOZE_ACTIONS = frozenset({"snooze", *REVIEW_SNOOZE_ACTIONS})
#: The agent a kind of work starts on, when that machine has it installed.
#: A preference, not a rule: the reader may change it at the gate, and a
#: machine without the profile falls back to its default rather than
#: refusing the task.
SOURCE_KIND_PROFILES = {
    "issue": "sigint",
    "review_request": "sigint",
}

DEFAULT_LEASE_SECONDS = 300
MIN_LEASE_SECONDS = 5
MAX_LEASE_SECONDS = 3_600
DEFAULT_MAX_ATTEMPTS = 3
MAX_ATTEMPTS = 20
#: How long a parked workflow waits before trying again on its own. Long
#: enough that a passing outage has ended, short enough that a reader who
#: does nothing still gets the work attempted the same day.
PARK_RETRY_INTERVAL = timedelta(hours=6)

RETRY_BASE_SECONDS = 60
RETRY_MAX_SECONDS = 3_600
MAX_RESULT_BYTES = 256 * 1024
MAX_SUMMARY_CHARS = 1_200
MAX_WORK_MARKDOWN_CHARS = 131_072
#: A card blurb, not a summary of record. See `work_digest.py`.
MAX_WORK_DIGEST_CHARS = 800
MAX_COLLECTION_ITEMS = 20
MAX_QUESTION_CHARS = 1_000

# Agent processes are scarce independently of planned work.  The runner claim
# transaction enforces this cap, so two hosts (or two local slots) cannot both
# observe room and start a third process.
EXECUTION_SLOT_CAP = 2
# Keep a durable planning reserve separate from the running slots.  A claimed
# plan leaves this many ready plans behind, rather than making the next slot
# wait for a periodic intake pass.
PLAN_READY_CAP = 10
# Compatibility name retained for callers that report the old aggregate.  It
# now describes only active execution, not queued planning work.
WORK_IN_PROGRESS_CAP = EXECUTION_SLOT_CAP
AWAITING_READER_CAP = 20
WORKING_STATUSES = frozenset({"queued", "running"})
READER_WAITING_STATUSES = frozenset({
    "awaiting_start", "awaiting_review", "completed",
})
MAX_ACTION_CHARS = 4_000
MAX_DELIVERABLE_CHARS = 16_000

_RESULT_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
_PROFILE_ID_RE = re.compile(r"^[a-z][a-z0-9-]{0,31}$")
_DIGEST_RE = re.compile(r"^[0-9a-f]{64}$")
FAILURE_REASONS = frozenset({
    "startup_failed",
    "process_exit",
    "timeout",
    "interrupted",
    "claim_expired",
    "lease_failed",
    "result_invalid",
})


class WorkflowStatus(StrEnum):
    AWAITING_START = "awaiting_start"
    SNOOZED = "snoozed"
    QUEUED = "queued"
    RUNNING = "running"
    AWAITING_REVIEW = "awaiting_review"
    COMPLETED = "completed"
    CANCELLED = "cancelled"
    PARKED = "parked"


class WorkflowPhase(StrEnum):
    PLAN = "plan"
    EXECUTE = "execute"
    EXTERNAL_ACTION = "external_action"


class ExecutionOutcome(StrEnum):
    AWAITING_PLAN = "awaiting_plan"
    AWAITING_EXTERNAL = "awaiting_external"
    COMPLETED = "completed"
    DECLINED = "declined"
    INELIGIBLE = "ineligible"


class WorkflowDisposition(StrEnum):
    APPLIED = "applied"
    UNCHANGED = "unchanged"
    REFUSED = "refused"


class WorkflowRefusal(StrEnum):
    INVALID_ARGUMENT = "invalid_argument"
    INVALID_ACTION = "invalid_action"
    INVALID_STATE = "invalid_state"
    NOT_FOUND = "not_found"
    STALE_TASK = "stale_task"
    STALE_WORKFLOW = "stale_workflow"
    CLAIM_MISMATCH = "claim_mismatch"
    RESULT_CONFLICT = "result_conflict"
    AGENT_PROFILE_UNAVAILABLE = "agent_profile_unavailable"


@dataclass(frozen=True)
class ExecutionWorkflow:
    task_id: int
    task_version: int
    status: WorkflowStatus
    phase: WorkflowPhase
    version: int
    due_at: str | None
    failure_count: int
    last_failure_reason: str | None
    last_failure_at: str | None
    next_attempt_at: str | None
    parked_at: str | None
    last_result_id: str | None
    created_at: str
    updated_at: str
    completed_at: str | None
    agent_profile_id: str
    agent_profile_revision: str


@dataclass(frozen=True)
class ExecutionClaim:
    task_id: int
    task_version: int
    workflow_version: int
    phase: WorkflowPhase
    token: str = field(repr=False)
    expires_at: str = ""
    text: str = field(default="", repr=False)
    owner: str | None = field(default=None, repr=False)
    due: str | None = field(default=None, repr=False)
    agent_profile_id: str = ""
    agent_profile_revision: str = ""
    lease_seconds: int = DEFAULT_LEASE_SECONDS


@dataclass(frozen=True)
class ExecutionResultEnvelope:
    result_id: str
    task_id: int
    task_version: int
    workflow_version: int
    phase: str
    claim_token: str = field(repr=False)
    outcome: str = ""
    summary: str = field(default="", repr=False)
    work_markdown: str = field(default="", repr=False)
    #: Derived from `work_markdown` by the worker, never by the agent.
    #: Empty is normal and means the card falls back to an excerpt.
    work_digest: str = field(default="", repr=False)
    questions: Sequence[str] = field(default=(), repr=False)
    external_actions: Sequence[object] = field(default=(), repr=False)
    deliverables: Sequence[object] = field(default=(), repr=False)
    task_work_directory: str | None = field(default=None, repr=False)
    task_kb_file: str | None = field(default=None, repr=False)


@dataclass(frozen=True)
class WorkflowOperationResult:
    disposition: WorkflowDisposition
    task_id: int
    version: int | None = None
    status: WorkflowStatus | None = None
    phase: WorkflowPhase | None = None
    wake_at: str | None = None
    next_attempt_at: str | None = None
    refusal: WorkflowRefusal | None = None
    agent_profile_id: str | None = None
    agent_profile_revision: str | None = None

    @property
    def accepted(self) -> bool:
        return self.disposition is not WorkflowDisposition.REFUSED


@dataclass(frozen=True)
class ExecutionReadiness:
    awaiting_start: int
    snoozed: int
    ready: int
    cooling: int
    running: int
    expired: int
    awaiting_review: int
    parked: int
    completed: int
    cancelled: int


@dataclass(frozen=True)
class ExecutionScheduleResult:
    """Content-free result of one bounded new-task scheduling pass."""

    scheduled: int
    remaining: int


@dataclass(frozen=True)
class ExecutionProfileHealth:
    """Content-free aggregate state for one exact agent profile revision."""

    agent_profile_id: str
    agent_profile_revision: str
    workflows: int
    ready: int
    running: int
    parked: int
    available: bool


class TaskExecutionService:
    """Durable workflow operations over one initialized Foxhound database."""

    def __init__(
        self,
        database_path: str | os.PathLike[str],
        *,
        clock: Callable[[], datetime] | None = None,
        token_factory: Callable[[], str] | None = None,
        max_attempts: int = DEFAULT_MAX_ATTEMPTS,
        profile_registry: AgentProfileRegistry | None = None,
        default_profile_id: str = "general",
        planning_grants: object = None,
    ) -> None:
        if (isinstance(max_attempts, bool)
                or not isinstance(max_attempts, int)
                or not 1 <= max_attempts <= MAX_ATTEMPTS):
            raise ValueError("maximum execution attempts are invalid")
        self.database_path = Path(database_path)
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._token_factory = token_factory or (
            lambda: secrets.token_urlsafe(32)
        )
        self._max_attempts = max_attempts
        registry = profile_registry or load_registry()
        if not isinstance(registry, AgentProfileRegistry):
            raise ValueError("agent profile registry is invalid")
        profile = registry.get(default_profile_id)
        if (
            profile is None
            or WorkflowPhase.PLAN.value not in profile.allowed_phases
        ):
            raise ValueError("default agent profile is unavailable")
        self._profile_registry = registry
        # Empty unless this machine says otherwise: an operator who has not
        # decided is asked, rather than having the decision made for them
        # by whichever machine edited a shared file first.
        self._planning_grants = _planning_grants(planning_grants)
        self._default_profile = profile

    def _profile_for(self, origin_kind: object) -> AgentProfile:
        """Which agent a task of this kind starts on.

        A default that ignores what the task is sends repository work to a
        compatibility profile. One review of a pull request went to
        `general`, produced nothing recordable three times, and parked —
        with the review already written.

        Falls back to the default when a preferred profile is not installed
        on this machine, because a machine that lacks it should still work
        rather than refuse every task of that kind.
        """
        preferred = SOURCE_KIND_PROFILES.get(origin_kind)
        if preferred:
            profile = self._profile_registry.get(preferred)
            if profile is not None and (
                WorkflowPhase.PLAN.value in profile.allowed_phases
            ):
                return profile
        return self._default_profile

    def initialize(self) -> None:
        CandidateInbox(self.database_path, clock=self._clock).initialize()

    def schedule_new(self, *, limit: int = 100) -> ExecutionScheduleResult:
        """Queue granted planning and create bounded new workflows.

        Agent work, ready plans, and reader-waiting decisions have separate
        capacities.  A newly granted source may already have an old Start
        gate: promote that read-only plan directly so it does not keep
        presenting a decision the host has since made.  New workflows still
        respect the reserve caps.  Existing over-cap rows are counted but
        never discarded; capacity returns as they drain.
        """
        if (
            isinstance(limit, bool)
            or not isinstance(limit, int)
            or not 1 <= limit <= 1_000
        ):
            raise ValueError("execution schedule limit is invalid")
        now = self._now()
        with closing(self._connect()) as connection:
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute("BEGIN IMMEDIATE")
            try:
                self._cancel_stale(connection, now)
                eligible = int(connection.execute(
                    "SELECT COUNT(*) FROM tasks AS t "
                    "LEFT JOIN task_execution_workflows AS w "
                    "ON w.task_id=t.id WHERE t.status='open' "
                    "AND w.task_id IS NULL AND NOT EXISTS("
                    " SELECT 1 FROM task_candidate_bindings AS b JOIN "
                    " task_candidate_lifecycle AS l ON l.candidate_id=b.candidate_id "
                    " WHERE b.task_id=t.id AND b.relation='accepted' "
                    " AND l.state='withdrawn' AND l.resolution='preserved_open'"
                    ")"
                ).fetchone()[0])
                waiting_statuses = tuple(sorted(READER_WAITING_STATUSES))
                waiting_marks = ",".join("?" for _ in waiting_statuses)
                capacity = connection.execute(
                    "SELECT "
                    "SUM(CASE WHEN w.status='running' THEN 1 ELSE 0 END) "
                    "AS running,"
                    "SUM(CASE WHEN w.status='queued' AND w.phase='plan' "
                    "AND (w.next_attempt_at IS NULL OR w.next_attempt_at<=?) "
                    "THEN 1 ELSE 0 END) AS ready_plans,"
                    "SUM(CASE WHEN t.status='open' AND "
                    f"w.status IN ({waiting_marks}) "
                    "THEN 1 ELSE 0 END) AS waiting "
                    "FROM task_execution_workflows AS w JOIN tasks AS t "
                    "ON t.id=w.task_id",
                    (now, *waiting_statuses),
                ).fetchone()
                plan_room = max(
                    0, PLAN_READY_CAP - int(capacity["ready_plans"] or 0)
                )
                waiting_room = max(
                    0, AWAITING_READER_CAP - int(capacity["waiting"] or 0)
                )
                promoted = 0
                waiting_rows = connection.execute(
                    "SELECT w.*, ("
                    " SELECT o.source_kind FROM task_candidate_bindings AS b "
                    " JOIN candidate_inbox AS o "
                    " ON o.candidate_id=b.candidate_id "
                    " WHERE b.task_id=t.id AND b.relation='accepted'"
                    ") AS origin_kind FROM task_execution_workflows AS w "
                    "JOIN tasks AS t ON t.id=w.task_id "
                    "WHERE w.status='awaiting_start' AND w.phase='plan' "
                    "AND t.status='open' AND t.version=w.task_version "
                    "AND NOT EXISTS("
                    " SELECT 1 FROM task_candidate_bindings AS blocked JOIN "
                    " task_candidate_lifecycle AS l "
                    " ON l.candidate_id=blocked.candidate_id "
                    " WHERE blocked.task_id=t.id AND blocked.relation='accepted' "
                    " AND l.state='withdrawn' "
                    " AND l.resolution='preserved_open'"
                    ") ORDER BY w.task_id"
                ).fetchall()
                for row in waiting_rows:
                    origin_kind = row["origin_kind"]
                    if _initial_status(
                        origin_kind, self._planning_grants
                    ) is not WorkflowStatus.QUEUED:
                        continue
                    version = int(row["version"]) + 1
                    profile = self._profile_for(origin_kind)
                    connection.execute(
                        "UPDATE task_execution_workflows SET status='queued',"
                        "version=?,due_at=NULL,agent_profile_id=?,"
                        "agent_profile_revision=?,updated_at=? "
                        "WHERE task_id=? AND version=? "
                        "AND status='awaiting_start' AND phase='plan'",
                        (
                            version, profile.profile_id, profile.revision,
                            now, int(row["task_id"]), int(row["version"]),
                        ),
                    )
                    self._event(
                        connection, int(row["task_id"]), "scheduled",
                        version, int(row["task_version"]), WorkflowPhase.PLAN,
                        WorkflowStatus.QUEUED, now,
                    )
                    promoted += 1

                rows = connection.execute(
                    "SELECT t.id,t.version,("
                    " SELECT o.source_kind FROM task_candidate_bindings AS b "
                    " JOIN candidate_inbox AS o "
                    " ON o.candidate_id=b.candidate_id "
                    " WHERE b.task_id=t.id AND b.relation='accepted'"
                    ") AS origin_kind FROM tasks AS t "
                    "LEFT JOIN task_execution_workflows AS w "
                    "ON w.task_id=t.id WHERE t.status='open' "
                    "AND w.task_id IS NULL AND NOT EXISTS("
                    " SELECT 1 FROM task_candidate_bindings AS blocked JOIN "
                    " task_candidate_lifecycle AS l "
                    " ON l.candidate_id=blocked.candidate_id "
                    " WHERE blocked.task_id=t.id AND blocked.relation='accepted' "
                    " AND l.state='withdrawn' AND l.resolution='preserved_open'"
                    ") ORDER BY t.id"
                )
                scheduled = 0
                for row in rows:
                    if scheduled >= limit:
                        break
                    task_id = int(row["id"])
                    task_version = int(row["version"])
                    status = _initial_status(
                        row["origin_kind"], self._planning_grants)
                    if status.value in WORKING_STATUSES:
                        if plan_room == 0:
                            continue
                        plan_room -= 1
                    else:
                        if waiting_room == 0:
                            continue
                        waiting_room -= 1
                    profile = self._profile_for(row["origin_kind"])
                    connection.execute(
                        "INSERT INTO task_execution_workflows("
                        "task_id,task_version,status,phase,version,due_at,"
                        "failure_count,created_at,updated_at,agent_profile_id,"
                        "agent_profile_revision) "
                        "VALUES(?,?,?,'plan',1,NULL,0,?,?,?,?)",
                        (
                            task_id, task_version, status.value, now, now,
                            profile.profile_id,
                            profile.revision,
                        ),
                    )
                    self._event(
                        connection,
                        task_id,
                        "scheduled",
                        1,
                        task_version,
                        WorkflowPhase.PLAN,
                        status,
                        now,
                    )
                    scheduled += 1
                connection.commit()
                return ExecutionScheduleResult(
                    scheduled=promoted + scheduled,
                    remaining=eligible - scheduled,
                )
            except Exception:
                connection.rollback()
                raise

    def schedule(
        self, task_id: int, *, expected_task_version: int
    ) -> WorkflowOperationResult:
        if not _valid_identity(task_id, expected_task_version):
            return _refused(task_id, WorkflowRefusal.INVALID_ARGUMENT)
        now = self._now()
        with closing(self._connect()) as connection:
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute("BEGIN IMMEDIATE")
            try:
                task = connection.execute(
                    "SELECT t.status,t.version,("
                    " SELECT o.source_kind FROM task_candidate_bindings AS b "
                    " JOIN candidate_inbox AS o "
                    " ON o.candidate_id=b.candidate_id "
                    " WHERE b.task_id=t.id AND b.relation='accepted'"
                    ") AS origin_kind,EXISTS("
                    " SELECT 1 FROM task_candidate_bindings AS b JOIN "
                    " task_candidate_lifecycle AS l ON l.candidate_id=b.candidate_id "
                    " WHERE b.task_id=t.id AND b.relation='accepted' "
                    " AND l.state='withdrawn' AND l.resolution='preserved_open'"
                    ") AS source_withdrawn FROM tasks AS t WHERE t.id=?",
                    (task_id,),
                ).fetchone()
                refusal = _task_guard(task, expected_task_version)
                if refusal is None and task["source_withdrawn"]:
                    refusal = WorkflowRefusal.INVALID_STATE
                if refusal is not None:
                    connection.rollback()
                    return _refused(task_id, refusal)
                row = connection.execute(
                    "SELECT * FROM task_execution_workflows WHERE task_id=?",
                    (task_id,),
                ).fetchone()
                if (row is not None
                        and int(row["task_version"]) == expected_task_version
                        and row["status"] not in {
                            WorkflowStatus.COMPLETED,
                            WorkflowStatus.CANCELLED,
                            WorkflowStatus.PARKED,
                        }):
                    connection.rollback()
                    return _operation(row, WorkflowDisposition.UNCHANGED)
                if row is None:
                    version = 1
                    status = _initial_status(
                        task["origin_kind"], self._planning_grants
                    )
                    profile = self._profile_for(task["origin_kind"])
                    connection.execute(
                        "INSERT INTO task_execution_workflows("
                        "task_id,task_version,status,phase,version,due_at,"
                        "failure_count,created_at,updated_at,agent_profile_id,"
                        "agent_profile_revision) "
                        "VALUES(?,?,?,'plan',?,NULL,0,?,?,?,?)",
                        (
                            task_id, expected_task_version, status.value, version,
                            now, now, profile.profile_id, profile.revision,
                        ),
                    )
                else:
                    version = int(row["version"]) + 1
                    status = _initial_status(
                        task["origin_kind"], self._planning_grants
                    )
                    profile = self._profile_for(task["origin_kind"])
                    connection.execute(
                        "UPDATE task_execution_workflows SET task_version=?,"
                        "status=?,phase='plan',version=?,"
                        "due_at=NULL,claim_token_digest=NULL,claimed_at=NULL,"
                        "claim_heartbeat_at=NULL,claim_expires_at=NULL,"
                        "failure_count=0,last_failure_reason=NULL,"
                        "last_failure_at=NULL,next_attempt_at=NULL,"
                        "parked_at=NULL,last_result_id=NULL,updated_at=?,"
                        "completed_at=NULL,agent_profile_id=?,"
                        "agent_profile_revision=? WHERE task_id=?",
                        (
                            expected_task_version, status.value, version, now,
                            profile.profile_id, profile.revision, task_id,
                        ),
                    )
                self._event(
                    connection, task_id, "scheduled", version,
                    expected_task_version, WorkflowPhase.PLAN,
                    status, now,
                )
                updated_row = connection.execute(
                    "SELECT * FROM task_execution_workflows WHERE task_id=?",
                    (task_id,),
                ).fetchone()
                connection.commit()
                return _operation(updated_row, WorkflowDisposition.APPLIED)
            except Exception:
                connection.rollback()
                raise

    def start_action(
        self, task_id: int, *, expected_version: int, action: str
    ) -> WorkflowOperationResult:
        if not _valid_identity(task_id, expected_version):
            return _refused(task_id, WorkflowRefusal.INVALID_ARGUMENT)
        if action not in {"start", "snooze", "cancel"}:
            return _refused(task_id, WorkflowRefusal.INVALID_ACTION)
        stamp = self._clock_value()
        with closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                result = _apply_start_action(
                    connection,
                    task_id,
                    expected_version=expected_version,
                    action=action,
                    stamp=stamp,
                )
                if not result.accepted:
                    connection.rollback()
                    return result
                connection.commit()
                return result
            except Exception:
                connection.rollback()
                raise

    def select_agent(
        self,
        task_id: int,
        *,
        expected_version: int,
        profile_id: str,
        profile_revision: str,
    ) -> WorkflowOperationResult:
        """Apply one explicit reader selection before the Start gate."""
        if not _valid_identity(task_id, expected_version):
            return _refused(task_id, WorkflowRefusal.INVALID_ARGUMENT)
        try:
            profile = self._profile_registry.resolve_current(
                profile_id, profile_revision
            )
        except AgentProfileError:
            return _refused(
                task_id, WorkflowRefusal.AGENT_PROFILE_UNAVAILABLE
            )
        if WorkflowPhase.PLAN.value not in profile.allowed_phases:
            return _refused(
                task_id, WorkflowRefusal.AGENT_PROFILE_UNAVAILABLE
            )
        stamp = self._clock_value()
        with closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                result = _apply_agent_selection(
                    connection,
                    task_id,
                    expected_version=expected_version,
                    profile=profile,
                    stamp=stamp,
                )
                if not result.accepted:
                    connection.rollback()
                    return result
                connection.commit()
                return result
            except Exception:
                connection.rollback()
                raise

    def review_action(
        self, task_id: int, *, expected_version: int, action: str
    ) -> WorkflowOperationResult:
        if not _valid_identity(task_id, expected_version):
            return _refused(task_id, WorkflowRefusal.INVALID_ARGUMENT)
        if action not in {
            "approve", "revise", "cancel", *REVIEW_SNOOZE_ACTIONS,
        }:
            return _refused(task_id, WorkflowRefusal.INVALID_ACTION)
        stamp = self._clock_value()
        with closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                result = _apply_review_action(
                    connection,
                    task_id,
                    expected_version=expected_version,
                    action=action,
                    stamp=stamp,
                )
                if not result.accepted:
                    connection.rollback()
                    return result
                connection.commit()
                return result
            except Exception:
                connection.rollback()
                raise

    def claim_next(
        self,
        *,
        allowed_phases: Sequence[WorkflowPhase | str] | None = None,
    ) -> ExecutionClaim | None:
        phases = _validated_phase_allowlist(allowed_phases)
        placeholders = ",".join("?" for _ in phases)
        stamp = self._clock_value()
        now = stamp.isoformat(timespec="seconds")
        token = self._token_factory()
        if not _valid_secret(token):
            raise TaskLedgerError("execution claim capability is invalid")
        digest = _token_digest(token)
        with closing(self._connect()) as connection:
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute("BEGIN IMMEDIATE")
            try:
                self._cancel_stale(connection, now)
                self._recover_expired(connection, stamp)
                running = int(connection.execute(
                    "SELECT COUNT(*) FROM task_execution_workflows AS w "
                    "JOIN tasks AS t ON t.id=w.task_id "
                    "WHERE w.status='running' AND t.status='open' "
                    "AND t.version=w.task_version"
                ).fetchone()[0])
                if running >= EXECUTION_SLOT_CAP:
                    connection.commit()
                    return None
                row = connection.execute(
                    "SELECT w.*,t.text,t.owner,t.due,t.status AS task_status,"
                    "t.version AS current_task_version "
                    "FROM task_execution_workflows AS w JOIN tasks AS t "
                    # `parked` is claimable once its retry time arrives.
                    # Parking stops the immediate retries; it is not a
                    # decision to abandon the work, and a reader who never
                    # answers the card should still have it attempted.
                    "ON t.id=w.task_id "
                    "WHERE w.status IN ('queued','parked') "
                    "AND (w.next_attempt_at IS NULL OR w.next_attempt_at<=?) "
                    f"AND w.phase IN ({placeholders}) "
                    "AND t.status='open' AND t.version=w.task_version "
                    "ORDER BY CASE WHEN w.failure_count=0 THEN 0 ELSE 1 END,"
                    "w.updated_at,w.task_id LIMIT 1",
                    (now, *(phase.value for phase in phases)),
                ).fetchone()
                if row is None:
                    connection.commit()
                    return None
                profile = self._resolve_profile(row)
                if row["phase"] not in profile.allowed_phases:
                    raise TaskLedgerError(
                        "execution agent profile is unavailable"
                    )
                expires = (
                    stamp + timedelta(seconds=profile.claim_lease_seconds)
                ).isoformat(timespec="seconds")
                version = int(row["version"]) + 1
                updated = connection.execute(
                    "UPDATE task_execution_workflows SET status='running',"
                    "version=?,claim_token_digest=?,claimed_at=?,"
                    "claim_heartbeat_at=?,claim_expires_at=?,updated_at=?,"
                    # Claiming a parked workflow starts a fresh round of
                    # attempts. One attempt from the limit would park it
                    # again on the first slip, which is a retry in name
                    # only. Set here so the reset and the claim are the
                    # same statement.
                    "failure_count=CASE WHEN status='parked' THEN 0 "
                    "ELSE failure_count END,"
                    # The schema requires parked_at to exist exactly while
                    # the status is parked, so leaving it set here is a
                    # constraint failure rather than a stale field.
                    "parked_at=NULL,next_attempt_at=NULL "
                    "WHERE task_id=? AND version=? "
                    "AND status IN ('queued','parked')",
                    (
                        version, digest, now, now, expires, now,
                        int(row["task_id"]), int(row["version"]),
                    ),
                )
                if updated.rowcount != 1:
                    connection.rollback()
                    return None
                self._event(
                    connection, int(row["task_id"]), "claimed", version,
                    int(row["task_version"]), WorkflowPhase(row["phase"]),
                    WorkflowStatus.RUNNING, now,
                )
                connection.commit()
                return ExecutionClaim(
                    task_id=int(row["task_id"]),
                    task_version=int(row["task_version"]),
                    workflow_version=version,
                    phase=WorkflowPhase(row["phase"]),
                    token=token,
                    expires_at=expires,
                    text=row["text"],
                    owner=row["owner"],
                    due=row["due"],
                    agent_profile_id=profile.profile_id,
                    agent_profile_revision=profile.revision,
                    lease_seconds=profile.claim_lease_seconds,
                )
            except Exception:
                connection.rollback()
                raise

    def renew(
        self,
        task_id: int,
        *,
        expected_version: int,
        claim_token: str,
        lease_seconds: int = DEFAULT_LEASE_SECONDS,
    ) -> WorkflowOperationResult:
        if (not _valid_identity(task_id, expected_version)
                or not _valid_secret(claim_token)
                or not _valid_lease(lease_seconds)):
            return _refused(task_id, WorkflowRefusal.INVALID_ARGUMENT)
        stamp = self._clock_value()
        now = stamp.isoformat(timespec="seconds")
        expires = (stamp + timedelta(seconds=lease_seconds)).isoformat(
            timespec="seconds"
        )
        digest = _token_digest(claim_token)
        with closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = self._workflow_with_task(connection, task_id)
                refusal = _running_guard(row, expected_version, digest, now)
                if refusal is None:
                    refusal = _task_guard(row, int(row["task_version"]))
                if refusal is not None:
                    connection.rollback()
                    return _refused_row(task_id, row, refusal)
                connection.execute(
                    "UPDATE task_execution_workflows SET "
                    "claim_heartbeat_at=?,claim_expires_at=?,updated_at=? "
                    "WHERE task_id=? AND version=? AND status='running' "
                    "AND claim_token_digest=?",
                    (now, expires, now, task_id, expected_version, digest),
                )
                self._event(
                    connection, task_id, "claim_renewed", expected_version,
                    int(row["task_version"]), WorkflowPhase(row["phase"]),
                    WorkflowStatus.RUNNING, now,
                )
                connection.commit()
                return WorkflowOperationResult(
                    WorkflowDisposition.APPLIED,
                    task_id,
                    expected_version,
                    WorkflowStatus.RUNNING,
                    WorkflowPhase(row["phase"]),
                    wake_at=expires,
                    agent_profile_id=row["agent_profile_id"],
                    agent_profile_revision=row["agent_profile_revision"],
                )
            except Exception:
                connection.rollback()
                raise

    def release(
        self, task_id: int, *, expected_version: int, claim_token: str
    ) -> WorkflowOperationResult:
        return self._finish_claim(
            task_id,
            expected_version=expected_version,
            claim_token=claim_token,
            failure_reason=None,
        )

    def fail(
        self,
        task_id: int,
        *,
        expected_version: int,
        claim_token: str,
        reason: str,
    ) -> WorkflowOperationResult:
        if reason not in FAILURE_REASONS or reason == "claim_expired":
            return _refused(task_id, WorkflowRefusal.INVALID_ARGUMENT)
        return self._finish_claim(
            task_id,
            expected_version=expected_version,
            claim_token=claim_token,
            failure_reason=reason,
        )

    def _finish_claim(
        self,
        task_id: int,
        *,
        expected_version: int,
        claim_token: str,
        failure_reason: str | None,
    ) -> WorkflowOperationResult:
        if (not _valid_identity(task_id, expected_version)
                or not _valid_secret(claim_token)):
            return _refused(task_id, WorkflowRefusal.INVALID_ARGUMENT)
        stamp = self._clock_value()
        now = stamp.isoformat(timespec="seconds")
        digest = _token_digest(claim_token)
        with closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = self._workflow_with_task(connection, task_id)
                refusal = _running_guard(row, expected_version, digest, now)
                if refusal is None:
                    refusal = _task_guard(row, int(row["task_version"]))
                if refusal is not None:
                    connection.rollback()
                    return _refused_row(task_id, row, refusal)
                if failure_reason is None:
                    version = expected_version + 1
                    connection.execute(
                        "UPDATE task_execution_workflows SET status='queued',"
                        "version=?,claim_token_digest=NULL,claimed_at=NULL,"
                        "claim_heartbeat_at=NULL,claim_expires_at=NULL,"
                        "updated_at=? WHERE task_id=? AND version=?",
                        (version, now, task_id, expected_version),
                    )
                    self._event(
                        connection, task_id, "released", version,
                        int(row["task_version"]),
                        WorkflowPhase(row["phase"]), WorkflowStatus.QUEUED,
                        now,
                    )
                    result = WorkflowOperationResult(
                        WorkflowDisposition.APPLIED,
                        task_id,
                        version,
                        WorkflowStatus.QUEUED,
                        WorkflowPhase(row["phase"]),
                        agent_profile_id=row["agent_profile_id"],
                        agent_profile_revision=row["agent_profile_revision"],
                    )
                else:
                    result = self._defer_failure(
                        connection, row, failure_reason, stamp,
                        event_kind=None,
                    )
                connection.commit()
                return result
            except Exception:
                connection.rollback()
                raise

    def record_result(
        self, envelope: ExecutionResultEnvelope
    ) -> WorkflowOperationResult:
        try:
            result = _validated_result(envelope)
        except (TypeError, ValueError):
            task_id = getattr(envelope, "task_id", 0)
            return _refused(task_id, WorkflowRefusal.INVALID_ARGUMENT)
        now = self._now()
        digest = result["content_digest"]
        with closing(self._connect()) as connection:
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute("BEGIN IMMEDIATE")
            try:
                existing = connection.execute(
                    "SELECT task_id,content_digest "
                    "FROM task_execution_results "
                    "WHERE result_id=?",
                    (result["result_id"],),
                ).fetchone()
                if existing is not None:
                    connection.rollback()
                    if (int(existing["task_id"]) == result["task_id"]
                            and existing["content_digest"] == digest):
                        row = self.get(result["task_id"])
                        if row is None:
                            return _refused(
                                result["task_id"],
                                WorkflowRefusal.INVALID_STATE,
                            )
                        return WorkflowOperationResult(
                            WorkflowDisposition.UNCHANGED,
                            row.task_id,
                            row.version,
                            row.status,
                            row.phase,
                            agent_profile_id=row.agent_profile_id,
                            agent_profile_revision=row.agent_profile_revision,
                        )
                    return _refused(
                        result["task_id"], WorkflowRefusal.RESULT_CONFLICT
                    )
                row = self._workflow_with_task(connection, result["task_id"])
                refusal = _running_guard(
                    row,
                    result["workflow_version"],
                    _token_digest(result["claim_token"]),
                    now,
                )
                if refusal is None:
                    refusal = _task_guard(row, result["task_version"])
                if (refusal is None
                        and row["phase"] != result["phase"]):
                    refusal = WorkflowRefusal.INVALID_STATE
                if refusal is not None:
                    connection.rollback()
                    return _refused_row(result["task_id"], row, refusal)
                target = _result_target(
                    WorkflowPhase(result["phase"]),
                    ExecutionOutcome(result["outcome"]),
                )
                if target is None:
                    connection.rollback()
                    return _refused_row(
                        result["task_id"], row,
                        WorkflowRefusal.INVALID_STATE,
                    )
                connection.execute(
                    "INSERT INTO task_execution_results("
                    "result_id,task_id,workflow_version,task_version,phase,"
                    "outcome,content_digest,summary,work_markdown,"
                    "questions_json,external_actions_json,deliverables_json,"
                    "created_at,agent_profile_id,agent_profile_revision,"
                    "task_work_directory,task_kb_file,work_digest) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        result["result_id"], result["task_id"],
                        result["workflow_version"], result["task_version"],
                        result["phase"], result["outcome"], digest,
                        result["summary"], result["work_markdown"],
                        result["questions_json"],
                        result["external_actions_json"],
                        result["deliverables_json"], now,
                        row["agent_profile_id"],
                        row["agent_profile_revision"],
                        result["task_work_directory"],
                        result["task_kb_file"],
                        result["work_digest"],
                    ),
                )
                version = result["workflow_version"] + 1
                completed = (
                    now if target is WorkflowStatus.COMPLETED else None
                )
                connection.execute(
                    "UPDATE task_execution_workflows SET status=?,version=?,"
                    "claim_token_digest=NULL,claimed_at=NULL,"
                    "claim_heartbeat_at=NULL,claim_expires_at=NULL,"
                    "failure_count=0,last_failure_reason=NULL,"
                    "last_failure_at=NULL,next_attempt_at=NULL,parked_at=NULL,"
                    "last_result_id=?,updated_at=?,completed_at=? "
                    "WHERE task_id=? AND version=? AND status='running'",
                    (
                        target, version, result["result_id"], now, completed,
                        result["task_id"], result["workflow_version"],
                    ),
                )
                self._event(
                    connection, result["task_id"], "result_recorded",
                    version, result["task_version"],
                    WorkflowPhase(result["phase"]), target, now,
                )
                connection.commit()
                return WorkflowOperationResult(
                    WorkflowDisposition.APPLIED,
                    result["task_id"],
                    version,
                    target,
                    WorkflowPhase(result["phase"]),
                    agent_profile_id=row["agent_profile_id"],
                    agent_profile_revision=row["agent_profile_revision"],
                )
            except Exception:
                connection.rollback()
                raise

    def retry(
        self, task_id: int, *, expected_version: int
    ) -> WorkflowOperationResult:
        if not _valid_identity(task_id, expected_version):
            return _refused(task_id, WorkflowRefusal.INVALID_ARGUMENT)
        now = self._now()
        with closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = self._workflow_with_task(connection, task_id)
                refusal = _workflow_guard(
                    row, expected_version,
                    {WorkflowStatus.QUEUED, WorkflowStatus.PARKED},
                )
                if refusal is None:
                    refusal = _task_guard(row, int(row["task_version"]))
                if refusal is not None:
                    connection.rollback()
                    return _refused_row(task_id, row, refusal)
                version = expected_version + 1
                connection.execute(
                    "UPDATE task_execution_workflows SET status='queued',"
                    "version=?,failure_count=0,last_failure_reason=NULL,"
                    "last_failure_at=NULL,next_attempt_at=NULL,parked_at=NULL,"
                    "updated_at=? WHERE task_id=? AND version=?",
                    (version, now, task_id, expected_version),
                )
                self._event(
                    connection, task_id, "retry_scheduled", version,
                    int(row["task_version"]), WorkflowPhase(row["phase"]),
                    WorkflowStatus.QUEUED, now,
                )
                connection.commit()
                return WorkflowOperationResult(
                    WorkflowDisposition.APPLIED,
                    task_id,
                    version,
                    WorkflowStatus.QUEUED,
                    WorkflowPhase(row["phase"]),
                    agent_profile_id=row["agent_profile_id"],
                    agent_profile_revision=row["agent_profile_revision"],
                )
            except Exception:
                connection.rollback()
                raise

    def get(self, task_id: int) -> ExecutionWorkflow | None:
        if isinstance(task_id, bool) or not isinstance(task_id, int):
            return None
        with closing(self._connect()) as connection:
            row = connection.execute(
                "SELECT * FROM task_execution_workflows WHERE task_id=?",
                (task_id,),
            ).fetchone()
        return None if row is None else _workflow(row)

    def reader_instruction(
        self,
        task_id: int,
        *,
        expected_version: int,
        claim_token: str,
    ) -> str | None:
        """Return only the discussion bound to this supervised run."""
        if (
            not _valid_identity(task_id, expected_version)
            or not _valid_secret(claim_token)
        ):
            raise TaskLedgerError("execution claim is unavailable")
        now = self._now()
        with closing(self._connect()) as connection:
            row = self._workflow_with_task(connection, task_id)
            refusal = _running_guard(
                row,
                expected_version,
                _token_digest(claim_token),
                now,
            )
            if refusal is None:
                refusal = _task_guard(row, int(row["task_version"]))
            if refusal is not None:
                raise TaskLedgerError("execution claim is unavailable")
            value = connection.execute(
                "SELECT i.value FROM execution_reader_inputs AS i "
                "WHERE i.task_id=? AND i.kind='discussion' "
                "AND i.target_workflow_version<=? AND NOT EXISTS("
                " SELECT 1 FROM task_execution_results AS r "
                " WHERE r.task_id=i.task_id "
                " AND r.workflow_version>=i.target_workflow_version"
                ") ORDER BY i.sequence DESC LIMIT 1",
                (task_id, expected_version),
            ).fetchone()
        return None if value is None else str(value["value"])

    def readiness(self) -> ExecutionReadiness:
        now = self._now()
        with closing(self._connect()) as connection:
            row = connection.execute(
                "SELECT "
                "SUM(status='awaiting_start') AS awaiting_start,"
                "SUM(status='snoozed') AS snoozed,"
                "SUM(status='queued' AND (next_attempt_at IS NULL OR "
                "next_attempt_at<=?)) AS ready,"
                "SUM(status='queued' AND next_attempt_at>?) AS cooling,"
                "SUM(status='running' AND claim_expires_at>?) AS running,"
                "SUM(status='running' AND (claim_expires_at IS NULL OR "
                "claim_expires_at<=?)) AS expired,"
                "SUM(status='awaiting_review') AS awaiting_review,"
                "SUM(status='parked') AS parked,"
                "SUM(status='completed') AS completed,"
                "SUM(status='cancelled') AS cancelled "
                "FROM task_execution_workflows",
                (now, now, now, now),
            ).fetchone()
        return ExecutionReadiness(*(
            int(row[name] or 0)
            for name in (
                "awaiting_start", "snoozed", "ready", "cooling",
                "running", "expired", "awaiting_review", "parked",
                "completed", "cancelled",
            )
        ))

    def profile_health(self) -> tuple[ExecutionProfileHealth, ...]:
        """Return aggregate workflow counts by exact profile revision."""
        now = self._now()
        with closing(self._connect()) as connection:
            rows = connection.execute(
                "SELECT agent_profile_id,agent_profile_revision,"
                "COUNT(*) AS workflows,"
                "SUM(status='queued' AND (next_attempt_at IS NULL OR "
                "next_attempt_at<=?)) AS ready,"
                "SUM(status='running') AS running,"
                "SUM(status='parked') AS parked "
                "FROM task_execution_workflows "
                "GROUP BY agent_profile_id,agent_profile_revision "
                "ORDER BY agent_profile_id,agent_profile_revision",
                (now,),
            ).fetchall()
        health: list[ExecutionProfileHealth] = []
        for row in rows:
            try:
                self._profile_registry.resolve(
                    row["agent_profile_id"], row["agent_profile_revision"]
                )
                available = True
            except AgentProfileError:
                available = False
            health.append(ExecutionProfileHealth(
                agent_profile_id=str(row["agent_profile_id"]),
                agent_profile_revision=str(row["agent_profile_revision"]),
                workflows=int(row["workflows"]),
                ready=int(row["ready"] or 0),
                running=int(row["running"] or 0),
                parked=int(row["parked"] or 0),
                available=available,
            ))
        return tuple(health)

    def event_count(self) -> int:
        with closing(self._connect()) as connection:
            return int(connection.execute(
                "SELECT COUNT(*) FROM task_execution_events"
            ).fetchone()[0])

    def result_count(self) -> int:
        with closing(self._connect()) as connection:
            return int(connection.execute(
                "SELECT COUNT(*) FROM task_execution_results"
            ).fetchone()[0])

    def _recover_expired(
        self, connection: sqlite3.Connection, stamp: datetime
    ) -> int:
        now = stamp.isoformat(timespec="seconds")
        rows = connection.execute(
            "SELECT * FROM task_execution_workflows "
            "WHERE status='running' AND (claim_expires_at IS NULL "
            "OR claim_expires_at<=?) ORDER BY task_id",
            (now,),
        ).fetchall()
        for row in rows:
            self._defer_failure(
                connection, row, "claim_expired", stamp,
                event_kind="claim_expired",
            )
        return len(rows)

    def _resolve_profile(self, row: sqlite3.Row) -> AgentProfile:
        try:
            return self._profile_registry.resolve(
                row["agent_profile_id"], row["agent_profile_revision"]
            )
        except (AgentProfileError, IndexError, KeyError, TypeError) as exc:
            raise TaskLedgerError(
                "execution agent profile is unavailable"
            ) from exc

    def _cancel_stale(
        self, connection: sqlite3.Connection, now: str
    ) -> int:
        rows = connection.execute(
            "SELECT w.* FROM task_execution_workflows AS w "
            "JOIN tasks AS t ON t.id=w.task_id "
            "WHERE w.status NOT IN ('completed','cancelled') "
            "AND (t.status!='open' OR t.version!=w.task_version OR EXISTS("
            " SELECT 1 FROM task_candidate_bindings AS b JOIN "
            " task_candidate_lifecycle AS l ON l.candidate_id=b.candidate_id "
            " WHERE b.task_id=t.id AND b.relation='accepted' "
            " AND l.state='withdrawn' AND l.resolution='preserved_open'"
            ")) "
            "ORDER BY w.task_id"
        ).fetchall()
        for row in rows:
            version = int(row["version"]) + 1
            connection.execute(
                "UPDATE task_execution_workflows SET status='cancelled',"
                "version=?,claim_token_digest=NULL,claimed_at=NULL,"
                "claim_heartbeat_at=NULL,claim_expires_at=NULL,"
                "next_attempt_at=NULL,parked_at=NULL,updated_at=?,"
                "completed_at=? WHERE task_id=? AND version=?",
                (
                    version, now, now, int(row["task_id"]),
                    int(row["version"]),
                ),
            )
            self._event(
                connection, int(row["task_id"]), "cancelled", version,
                int(row["task_version"]), WorkflowPhase(row["phase"]),
                WorkflowStatus.CANCELLED, now,
            )
        return len(rows)

    def _defer_failure(
        self,
        connection: sqlite3.Connection,
        row: sqlite3.Row,
        reason: str,
        stamp: datetime,
        *,
        event_kind: str | None,
    ) -> WorkflowOperationResult:
        if reason not in FAILURE_REASONS:
            raise ValueError("execution failure reason is invalid")
        now = stamp.isoformat(timespec="seconds")
        failures = int(row["failure_count"]) + 1
        version = int(row["version"]) + 1
        if failures >= self._max_attempts:
            # Parked, and due to try again later. A run of failures is often
            # something passing — a forge that was unreachable, a machine
            # under load — and giving up permanently on the third one turns
            # a bad hour into abandoned work. The reader is told either way:
            # parking raises a card, and if the later round fails it raises
            # another.
            status = WorkflowStatus.PARKED
            next_attempt = (stamp + PARK_RETRY_INTERVAL).isoformat(
                timespec="seconds"
            )
            parked = now
            kind = "parked"
        else:
            status = WorkflowStatus.QUEUED
            delay = min(
                RETRY_MAX_SECONDS,
                RETRY_BASE_SECONDS * (2 ** (failures - 1)),
            )
            next_attempt = (stamp + timedelta(seconds=delay)).isoformat(
                timespec="seconds"
            )
            parked = None
            kind = event_kind or "retry_scheduled"
        connection.execute(
            "UPDATE task_execution_workflows SET status=?,version=?,"
            "claim_token_digest=NULL,claimed_at=NULL,"
            "claim_heartbeat_at=NULL,claim_expires_at=NULL,"
            "failure_count=?,last_failure_reason=?,last_failure_at=?,"
            "next_attempt_at=?,parked_at=?,updated_at=? "
            "WHERE task_id=? AND version=?",
            (
                status, version, failures, reason, now, next_attempt, parked,
                now, int(row["task_id"]), int(row["version"]),
            ),
        )
        self._event(
            connection, int(row["task_id"]), kind, version,
            int(row["task_version"]), WorkflowPhase(row["phase"]),
            status, now,
        )
        return WorkflowOperationResult(
            WorkflowDisposition.APPLIED,
            int(row["task_id"]),
            version,
            status,
            WorkflowPhase(row["phase"]),
            next_attempt_at=next_attempt,
            agent_profile_id=row["agent_profile_id"],
            agent_profile_revision=row["agent_profile_revision"],
        )

    @staticmethod
    def _event(
        connection: sqlite3.Connection,
        task_id: int,
        kind: str,
        workflow_version: int,
        task_version: int,
        phase: WorkflowPhase,
        status: WorkflowStatus,
        now: str,
    ) -> None:
        profile = connection.execute(
            "SELECT agent_profile_id,agent_profile_revision "
            "FROM task_execution_workflows WHERE task_id=?",
            (task_id,),
        ).fetchone()
        if profile is None:
            raise TaskLedgerError("execution workflow is unavailable")
        connection.execute(
            "INSERT INTO task_execution_events("
            "task_id,kind,workflow_version,task_version,phase,status,"
            "occurred_at,agent_profile_id,agent_profile_revision) "
            "VALUES(?,?,?,?,?,?,?,?,?)",
            (
                task_id, kind, workflow_version, task_version, phase, status,
                now, profile["agent_profile_id"],
                profile["agent_profile_revision"],
            ),
        )

    @staticmethod
    def _workflow_with_task(
        connection: sqlite3.Connection, task_id: int
    ) -> sqlite3.Row | None:
        return connection.execute(
            "SELECT w.*,t.status AS task_status,t.version AS "
            "current_task_version FROM task_execution_workflows AS w "
            "JOIN tasks AS t ON t.id=w.task_id WHERE w.task_id=?",
            (task_id,),
        ).fetchone()

    def _connect(self) -> sqlite3.Connection:
        if not self.database_path.is_file() or self.database_path.is_symlink():
            raise TaskLedgerError("task execution database is not initialized")
        connection = sqlite3.connect(self.database_path, timeout=5)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout = 5000")
        version = int(connection.execute("PRAGMA user_version").fetchone()[0])
        if version != SCHEMA_VERSION:
            connection.close()
            raise TaskLedgerError(
                "task execution database schema is not supported"
            )
        try:
            CandidateInbox._require_schema(connection)
        except InboxError as exc:
            connection.close()
            raise TaskLedgerError(
                "task execution database schema is incomplete"
            ) from exc
        return connection

    def _clock_value(self) -> datetime:
        value = self._clock()
        if value.tzinfo is None or value.utcoffset() is None:
            raise TaskLedgerError(
                "task execution clock must include a timezone"
            )
        return value.astimezone(timezone.utc)

    def _now(self) -> str:
        return self._clock_value().isoformat(timespec="seconds")


def _apply_agent_selection(
    connection: sqlite3.Connection,
    task_id: int,
    *,
    expected_version: int,
    profile: AgentProfile,
    stamp: datetime,
) -> WorkflowOperationResult:
    """Bind one exact profile inside the caller's transaction."""
    if not _valid_identity(task_id, expected_version):
        return _refused(task_id, WorkflowRefusal.INVALID_ARGUMENT)
    if not isinstance(profile, AgentProfile):
        return _refused(task_id, WorkflowRefusal.AGENT_PROFILE_UNAVAILABLE)
    if WorkflowPhase.PLAN.value not in profile.allowed_phases:
        return _refused(task_id, WorkflowRefusal.AGENT_PROFILE_UNAVAILABLE)
    if stamp.tzinfo is None or stamp.utcoffset() is None:
        raise TaskLedgerError("task execution clock must include a timezone")
    now = stamp.astimezone(timezone.utc).isoformat(timespec="seconds")
    row = TaskExecutionService._workflow_with_task(connection, task_id)
    refusal = _workflow_guard(
        row,
        expected_version,
        {WorkflowStatus.AWAITING_START},
    )
    if refusal is None:
        refusal = _task_guard(row, int(row["task_version"]))
    if refusal is not None:
        return _refused_row(task_id, row, refusal)
    if (
        row["agent_profile_id"] == profile.profile_id
        and row["agent_profile_revision"] == profile.revision
    ):
        return _operation(row, WorkflowDisposition.UNCHANGED)
    version = expected_version + 1
    updated = connection.execute(
        "UPDATE task_execution_workflows SET version=?,"
        "agent_profile_id=?,agent_profile_revision=?,updated_at=? "
        "WHERE task_id=? AND version=? AND status='awaiting_start'",
        (
            version,
            profile.profile_id,
            profile.revision,
            now,
            task_id,
            expected_version,
        ),
    )
    if updated.rowcount != 1:
        return _refused_row(task_id, row, WorkflowRefusal.STALE_WORKFLOW)
    TaskExecutionService._event(
        connection,
        task_id,
        "agent_selected",
        version,
        int(row["task_version"]),
        WorkflowPhase.PLAN,
        WorkflowStatus.AWAITING_START,
        now,
    )
    return WorkflowOperationResult(
        WorkflowDisposition.APPLIED,
        task_id,
        version,
        WorkflowStatus.AWAITING_START,
        WorkflowPhase.PLAN,
        agent_profile_id=profile.profile_id,
        agent_profile_revision=profile.revision,
    )


def _apply_plan_review_agent_selection(
    connection: sqlite3.Connection,
    task_id: int,
    *,
    expected_version: int,
    profile: AgentProfile,
    stamp: datetime,
) -> WorkflowOperationResult:
    """Bind the exact executor chosen while a plan awaits approval.

    The plan has already been produced, so this selection is deliberately
    constrained to profiles that may execute.  Keeping the workflow at its
    review gate means the reader still has to approve the plan after making
    the choice; it cannot turn a selector tap into execution.
    """
    if not _valid_identity(task_id, expected_version):
        return _refused(task_id, WorkflowRefusal.INVALID_ARGUMENT)
    if not isinstance(profile, AgentProfile):
        return _refused(task_id, WorkflowRefusal.AGENT_PROFILE_UNAVAILABLE)
    if WorkflowPhase.EXECUTE.value not in profile.allowed_phases:
        return _refused(task_id, WorkflowRefusal.AGENT_PROFILE_UNAVAILABLE)
    if stamp.tzinfo is None or stamp.utcoffset() is None:
        raise TaskLedgerError("task execution clock must include a timezone")
    now = stamp.astimezone(timezone.utc).isoformat(timespec="seconds")
    row = TaskExecutionService._workflow_with_task(connection, task_id)
    refusal = _workflow_guard(
        row,
        expected_version,
        {WorkflowStatus.AWAITING_REVIEW},
    )
    if refusal is None and row["phase"] != WorkflowPhase.PLAN:
        refusal = WorkflowRefusal.INVALID_STATE
    if refusal is None:
        refusal = _task_guard(row, int(row["task_version"]))
    if refusal is not None:
        return _refused_row(task_id, row, refusal)
    if (
        row["agent_profile_id"] == profile.profile_id
        and row["agent_profile_revision"] == profile.revision
    ):
        return _operation(row, WorkflowDisposition.UNCHANGED)
    version = expected_version + 1
    updated = connection.execute(
        "UPDATE task_execution_workflows SET version=?,"
        "agent_profile_id=?,agent_profile_revision=?,updated_at=? "
        "WHERE task_id=? AND version=? AND status='awaiting_review' "
        "AND phase='plan'",
        (
            version,
            profile.profile_id,
            profile.revision,
            now,
            task_id,
            expected_version,
        ),
    )
    if updated.rowcount != 1:
        return _refused_row(task_id, row, WorkflowRefusal.STALE_WORKFLOW)
    TaskExecutionService._event(
        connection,
        task_id,
        "agent_selected",
        version,
        int(row["task_version"]),
        WorkflowPhase.EXECUTE,
        WorkflowStatus.AWAITING_REVIEW,
        now,
    )
    return WorkflowOperationResult(
        WorkflowDisposition.APPLIED,
        task_id,
        version,
        WorkflowStatus.AWAITING_REVIEW,
        WorkflowPhase.PLAN,
        agent_profile_id=profile.profile_id,
        agent_profile_revision=profile.revision,
    )


def _calendar_snooze_until(action: str, stamp: datetime) -> str:
    """Resolve one snooze choice to 09:00 in the service host timezone.

    The bare verb resolves here as well, to the nearest choice, so that the
    mapping lives in one place rather than at each of the two gates that
    accept it.
    """
    if action not in _SNOOZE_ACTIONS:
        raise TaskLedgerError("task execution snooze action is invalid")
    if action == "snooze":
        action = "snooze_1d"
    if stamp.tzinfo is None or stamp.utcoffset() is None:
        raise TaskLedgerError("task execution clock must include a timezone")
    current = stamp.astimezone()
    today = current.date()
    if action == "snooze_1d":
        target = today + timedelta(days=1)
    elif action == "snooze_7d":
        target = today + timedelta(days=(4 - today.weekday()) % 7)
        if target == today and current.timetz() >= time(
            9, tzinfo=current.tzinfo
        ):
            target += timedelta(days=7)
    elif action == "snooze_14d":
        target = today + timedelta(days=7 - today.weekday())
    else:
        target = today + timedelta(days=14)
    # Calling astimezone on the naive target applies the host's timezone rules
    # for the target date, including a daylight-saving transition meanwhile.
    local_target = datetime.combine(target, time(9)).astimezone()
    return local_target.astimezone(timezone.utc).isoformat(timespec="seconds")


def _apply_start_action(
    connection: sqlite3.Connection,
    task_id: int,
    *,
    expected_version: int,
    action: str,
    stamp: datetime,
) -> WorkflowOperationResult:
    """Apply one start gate inside the caller transaction."""
    if not _valid_identity(task_id, expected_version):
        return _refused(task_id, WorkflowRefusal.INVALID_ARGUMENT)
    if action not in {"start", "cancel", *_SNOOZE_ACTIONS}:
        return _refused(task_id, WorkflowRefusal.INVALID_ACTION)
    if stamp.tzinfo is None or stamp.utcoffset() is None:
        raise TaskLedgerError("task execution clock must include a timezone")
    stamp = stamp.astimezone(timezone.utc)
    now = stamp.isoformat(timespec="seconds")
    row = TaskExecutionService._workflow_with_task(connection, task_id)
    refusal = _workflow_guard(
        row,
        expected_version,
        # `parked` included: the card that reports the failure offers to
        # try again, and a button that reports a refusal would make the
        # report useless.
        {
            WorkflowStatus.AWAITING_START,
            WorkflowStatus.SNOOZED,
            WorkflowStatus.PARKED,
        },
    )
    if refusal is None:
        refusal = _task_guard(row, int(row["task_version"]))
    if (
        refusal is None
        and row["status"] == WorkflowStatus.SNOOZED
        and action == "start"
        and row["due_at"] > now
    ):
        refusal = WorkflowRefusal.INVALID_STATE
    if refusal is not None:
        return _refused_row(task_id, row, refusal)
    version = expected_version + 1
    if action == "start":
        status = WorkflowStatus.QUEUED
        wake = None
        completed = None
        kind = "start_approved"
    elif action in _SNOOZE_ACTIONS:
        # A bare `snooze` is the verb the keyboard carries; the explicit
        # choices come back from a picker. It resolves to the nearest of
        # them rather than to a raw offset from the tap, so a gate and a
        # review deferred at the same moment return at the same moment --
        # and so a card already delivered with the retired generic control
        # wakes on the same schedule as one sent today.
        status = WorkflowStatus.SNOOZED
        wake = _calendar_snooze_until(action, stamp)
        completed = None
        kind = "snoozed"
    else:
        status = WorkflowStatus.CANCELLED
        wake = None
        completed = now
        kind = "cancelled"
    # The phase is kept, not reset. A gate reached from `awaiting_start` or
    # `snoozed` is already in `plan`, so this is the same statement for them.
    # One reached from `parked` may be in `execute` or `external_action`,
    # behind a plan the reader already read and approved: sending it back to
    # `plan` would throw that approval away and silently ask the agent to
    # redo work that was accepted.
    phase = WorkflowPhase(row["phase"])
    connection.execute(
        "UPDATE task_execution_workflows SET status=?,phase=?,version=?,"
        "due_at=?,claim_token_digest=NULL,claimed_at=NULL,"
        "claim_heartbeat_at=NULL,claim_expires_at=NULL,"
        # Restarting clears what parked it, so a retry gets a full set of
        # attempts rather than immediately parking again on the next slip.
        "failure_count=0,last_failure_reason=NULL,last_failure_at=NULL,"
        "next_attempt_at=NULL,parked_at=NULL,updated_at=?,completed_at=? "
        "WHERE task_id=? AND version=?",
        (
            status,
            phase,
            version,
            wake,
            now,
            completed,
            task_id,
            expected_version,
        ),
    )
    TaskExecutionService._event(
        connection,
        task_id,
        kind,
        version,
        int(row["task_version"]),
        phase,
        status,
        now,
    )
    return WorkflowOperationResult(
        WorkflowDisposition.APPLIED,
        task_id,
        version,
        status,
        phase,
        wake_at=wake,
        agent_profile_id=row["agent_profile_id"],
        agent_profile_revision=row["agent_profile_revision"],
    )


def _apply_review_action(
    connection: sqlite3.Connection,
    task_id: int,
    *,
    expected_version: int,
    action: str,
    stamp: datetime,
) -> WorkflowOperationResult:
    """Apply one review gate inside the caller transaction."""
    if not _valid_identity(task_id, expected_version):
        return _refused(task_id, WorkflowRefusal.INVALID_ARGUMENT)
    if action not in {
        "approve", "revise", "cancel", "snooze", *REVIEW_SNOOZE_ACTIONS,
    }:
        return _refused(task_id, WorkflowRefusal.INVALID_ACTION)
    if stamp.tzinfo is None or stamp.utcoffset() is None:
        raise TaskLedgerError("task execution clock must include a timezone")
    stamp = stamp.astimezone(timezone.utc)
    now = stamp.isoformat(timespec="seconds")
    row = TaskExecutionService._workflow_with_task(connection, task_id)
    allowed_statuses = {
        WorkflowStatus.AWAITING_REVIEW,
        WorkflowStatus.SNOOZED,
    }
    if action in _SNOOZE_ACTIONS:
        allowed_statuses.add(WorkflowStatus.COMPLETED)
    refusal = _workflow_guard(
        row,
        expected_version,
        allowed_statuses,
    )
    if refusal is None:
        refusal = _task_guard(row, int(row["task_version"]))
    if (
        refusal is None
        and row["status"] == WorkflowStatus.SNOOZED
        and row["due_at"] > now
    ):
        refusal = WorkflowRefusal.INVALID_STATE
    result_row = None
    if refusal is None:
        result_row = connection.execute(
            "SELECT outcome FROM task_execution_results "
            "WHERE result_id=? AND task_id=?",
            (row["last_result_id"], task_id),
        ).fetchone()
        if result_row is None:
            refusal = WorkflowRefusal.INVALID_STATE
    if refusal is not None:
        return _refused_row(task_id, row, refusal)
    if action == "cancel":
        phase = WorkflowPhase(row["phase"])
        status = WorkflowStatus.CANCELLED
        completed = now
        kind = "cancelled"
        wake = None
    elif action == "revise":
        phase = WorkflowPhase.PLAN
        status = WorkflowStatus.QUEUED
        completed = None
        kind = "revision_requested"
        wake = None
    elif action in _SNOOZE_ACTIONS:
        phase = WorkflowPhase(row["phase"])
        status = WorkflowStatus.SNOOZED
        completed = None
        kind = "snoozed"
        wake = _calendar_snooze_until(action, stamp)
    else:
        targets = {
            ExecutionOutcome.AWAITING_PLAN: WorkflowPhase.EXECUTE,
            ExecutionOutcome.AWAITING_EXTERNAL: WorkflowPhase.EXTERNAL_ACTION,
        }
        phase = targets.get(ExecutionOutcome(result_row["outcome"]))
        if phase is None:
            return _refused_row(
                task_id, row, WorkflowRefusal.INVALID_STATE
            )
        status = WorkflowStatus.QUEUED
        completed = None
        kind = "phase_approved"
        wake = None
    version = expected_version + 1
    connection.execute(
        "UPDATE task_execution_workflows SET status=?,phase=?,version=?,"
        "due_at=?,claim_token_digest=NULL,claimed_at=NULL,"
        "claim_heartbeat_at=NULL,claim_expires_at=NULL,failure_count=0,"
        "last_failure_reason=NULL,last_failure_at=NULL,next_attempt_at=NULL,"
        "parked_at=NULL,updated_at=?,completed_at=? "
        "WHERE task_id=? AND version=?",
        (
            status,
            phase,
            version,
            wake,
            now,
            completed,
            task_id,
            expected_version,
        ),
    )
    TaskExecutionService._event(
        connection,
        task_id,
        kind,
        version,
        int(row["task_version"]),
        phase,
        status,
        now,
    )
    return WorkflowOperationResult(
        WorkflowDisposition.APPLIED,
        task_id,
        version,
        status,
        phase,
        wake_at=wake,
        agent_profile_id=row["agent_profile_id"],
        agent_profile_revision=row["agent_profile_revision"],
    )


def _workflow(row: sqlite3.Row) -> ExecutionWorkflow:
    try:
        profile_id = row["agent_profile_id"]
        profile_revision = row["agent_profile_revision"]
        if (
            not isinstance(profile_id, str)
            or not _PROFILE_ID_RE.fullmatch(profile_id)
            or not isinstance(profile_revision, str)
            or not _DIGEST_RE.fullmatch(profile_revision)
        ):
            raise ValueError("execution agent profile evidence is invalid")
        return ExecutionWorkflow(
            task_id=int(row["task_id"]),
            task_version=int(row["task_version"]),
            status=WorkflowStatus(row["status"]),
            phase=WorkflowPhase(row["phase"]),
            version=int(row["version"]),
            due_at=row["due_at"],
            failure_count=int(row["failure_count"]),
            last_failure_reason=row["last_failure_reason"],
            last_failure_at=row["last_failure_at"],
            next_attempt_at=row["next_attempt_at"],
            parked_at=row["parked_at"],
            last_result_id=row["last_result_id"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            completed_at=row["completed_at"],
            agent_profile_id=profile_id,
            agent_profile_revision=profile_revision,
        )
    except (IndexError, KeyError, TypeError, ValueError) as exc:
        raise TaskLedgerError("task execution state is invalid") from exc


def _operation(
    row: sqlite3.Row, disposition: WorkflowDisposition
) -> WorkflowOperationResult:
    return WorkflowOperationResult(
        disposition,
        int(row["task_id"]),
        int(row["version"]),
        WorkflowStatus(row["status"]),
        WorkflowPhase(row["phase"]),
        wake_at=row["due_at"],
        next_attempt_at=row["next_attempt_at"],
        agent_profile_id=row["agent_profile_id"],
        agent_profile_revision=row["agent_profile_revision"],
    )


def _refused(
    task_id: object, refusal: WorkflowRefusal
) -> WorkflowOperationResult:
    return WorkflowOperationResult(
        WorkflowDisposition.REFUSED,
        task_id if isinstance(task_id, int) and not isinstance(task_id, bool)
        else 0,
        refusal=refusal,
    )


def _refused_row(
    task_id: int, row: sqlite3.Row | None, refusal: WorkflowRefusal
) -> WorkflowOperationResult:
    if row is None:
        return _refused(task_id, refusal)
    return WorkflowOperationResult(
        WorkflowDisposition.REFUSED,
        task_id,
        int(row["version"]),
        WorkflowStatus(row["status"]),
        WorkflowPhase(row["phase"]),
        refusal=refusal,
        agent_profile_id=row["agent_profile_id"],
        agent_profile_revision=row["agent_profile_revision"],
    )


def _task_guard(
    row: sqlite3.Row | None, expected_version: int
) -> WorkflowRefusal | None:
    if row is None:
        return WorkflowRefusal.NOT_FOUND
    version = (
        int(row["current_task_version"])
        if "current_task_version" in row.keys()
        else int(row["version"])
    )
    status = (
        row["task_status"] if "task_status" in row.keys()
        else row["status"]
    )
    if version != expected_version:
        return WorkflowRefusal.STALE_TASK
    if status != TaskStatus.OPEN:
        return WorkflowRefusal.INVALID_STATE
    return None


def _workflow_guard(
    row: sqlite3.Row | None,
    expected_version: int,
    statuses: set[WorkflowStatus],
) -> WorkflowRefusal | None:
    if row is None:
        return WorkflowRefusal.NOT_FOUND
    if int(row["version"]) != expected_version:
        return WorkflowRefusal.STALE_WORKFLOW
    if row["status"] not in statuses:
        return WorkflowRefusal.INVALID_STATE
    return None


def _running_guard(
    row: sqlite3.Row | None,
    expected_version: int,
    digest: str,
    now: str,
) -> WorkflowRefusal | None:
    refusal = _workflow_guard(
        row, expected_version, {WorkflowStatus.RUNNING}
    )
    if refusal is not None:
        return refusal
    if row["claim_token_digest"] != digest:
        return WorkflowRefusal.CLAIM_MISMATCH
    if row["claim_expires_at"] is None or row["claim_expires_at"] <= now:
        return WorkflowRefusal.INVALID_STATE
    return None


def _result_target(
    phase: WorkflowPhase, outcome: ExecutionOutcome
) -> WorkflowStatus | None:
    allowed = {
        WorkflowPhase.PLAN: {
            ExecutionOutcome.AWAITING_PLAN,
            ExecutionOutcome.COMPLETED,
            ExecutionOutcome.INELIGIBLE,
        },
        WorkflowPhase.EXECUTE: {
            ExecutionOutcome.AWAITING_EXTERNAL,
            ExecutionOutcome.COMPLETED,
            ExecutionOutcome.DECLINED,
            ExecutionOutcome.INELIGIBLE,
        },
        WorkflowPhase.EXTERNAL_ACTION: {
            ExecutionOutcome.COMPLETED,
            ExecutionOutcome.DECLINED,
            ExecutionOutcome.INELIGIBLE,
        },
    }
    if outcome not in allowed[phase]:
        return None
    return WorkflowStatus.AWAITING_REVIEW


def _validated_result(envelope: ExecutionResultEnvelope) -> dict[str, object]:
    if not isinstance(envelope, ExecutionResultEnvelope):
        raise TypeError("execution result envelope is invalid")
    if not _RESULT_ID_RE.fullmatch(envelope.result_id):
        raise ValueError("execution result identity is invalid")
    if (not _valid_identity(envelope.task_id, envelope.task_version)
            or isinstance(envelope.workflow_version, bool)
            or not isinstance(envelope.workflow_version, int)
            or envelope.workflow_version < 1
            or not _valid_secret(envelope.claim_token)):
        raise ValueError("execution result identity is invalid")
    try:
        phase = WorkflowPhase(envelope.phase)
        outcome = ExecutionOutcome(envelope.outcome)
    except (TypeError, ValueError):
        raise ValueError("execution result state is invalid") from None
    if _result_target(phase, outcome) is None:
        raise ValueError("execution result transition is invalid")
    # Multi-line: a summary is read by a person on a card, and the card
    # renders line breaks. Requiring one line refused a correct 597-character
    # review for containing paragraphs, three times, until the workflow
    # parked — the length bound is the one that protects the card.
    summary = _bounded_text(
        envelope.summary, "summary", MAX_SUMMARY_CHARS, single_line=False
    )
    work = _bounded_text(
        envelope.work_markdown,
        "work markdown",
        MAX_WORK_MARKDOWN_CHARS,
        single_line=False,
    )
    questions = _text_collection(
        envelope.questions, "questions", MAX_QUESTION_CHARS,
        single_line=True,
    )
    actions = _structured_collection(
        envelope.external_actions, "external actions", MAX_ACTION_CHARS,
        primary="action", aliases=_ACTION_ALIASES, optional=_ACTION_FIELDS,
    )
    deliverables = _structured_collection(
        envelope.deliverables, "deliverables", MAX_DELIVERABLE_CHARS,
        primary="body", aliases=_DELIVERABLE_ALIASES,
        optional=_DELIVERABLE_FIELDS,
    )
    # Deliberately absent from `document` below, and so from the content
    # digest: the digest identifies what the AGENT produced, and this is
    # produced afterwards from it. Folding it in would make the same
    # result look like a different one whenever the small model phrased
    # itself differently, which is exactly the replay dedupe this digest
    # exists to perform.
    work_digest = (
        "" if not envelope.work_digest
        else _bounded_text(
            envelope.work_digest, "work digest", MAX_WORK_DIGEST_CHARS,
            single_line=False,
        )
    )
    task_work_directory = _result_path(
        envelope.task_work_directory, "task work directory"
    )
    task_kb_file = _result_path(envelope.task_kb_file, "task KB file")
    if (task_work_directory is None) != (task_kb_file is None):
        raise ValueError("execution result review paths are invalid")
    document = {
        "result_id": envelope.result_id,
        "task_id": envelope.task_id,
        "task_version": envelope.task_version,
        "workflow_version": envelope.workflow_version,
        "phase": phase.value,
        "outcome": outcome.value,
        "summary": summary,
        "work_markdown": work,
        "questions": questions,
        "external_actions": actions,
        "deliverables": deliverables,
        "task_work_directory": task_work_directory,
        "task_kb_file": task_kb_file,
    }
    raw = _canonical_json(document).encode("utf-8")
    if len(raw) > MAX_RESULT_BYTES:
        raise ValueError("execution result is too large")
    digest = hashlib.sha256(raw).hexdigest()
    if not _DIGEST_RE.fullmatch(digest):
        raise ValueError("execution result digest is invalid")
    return {
        **document,
        "work_digest": work_digest or None,
        "content_digest": digest,
        "questions_json": _canonical_json(questions),
        "external_actions_json": _canonical_json(actions),
        "deliverables_json": _canonical_json(deliverables),
        "claim_token": envelope.claim_token,
    }


def _result_path(value: object, label: str) -> str | None:
    if value is None:
        return None
    if (
        not isinstance(value, str)
        or not value.startswith("/")
        or len(value) > 4_096
        or "\0" in value
        or any(ord(character) < 32 for character in value)
    ):
        raise ValueError(f"execution result {label} is invalid")
    return value


def _initial_status(
    origin_kind: object, granted: frozenset[str]
) -> WorkflowStatus:
    """Whether this task must be asked about before it is planned.

    A gate exists so no agent time is spent on a task the reader never
    wanted. For some sources that question is already answered: enrolling
    a repository is the permission for its issues, and the gate then asks
    again about every one of them, using a card that can only show a title
    because nothing has looked at the issue yet.

    Which sources those are is a judgement about this machine and the
    operator's appetite for it, not a fact about the source. It is
    declared per machine and defaults to empty, so a machine that says
    nothing is asked about everything.

    Planning is read-only and produces no external effect, so granting it
    costs one agent pass and yields a card that can actually be judged.
    Everything after the plan is still gated.
    """
    if isinstance(origin_kind, str) and origin_kind in granted:
        return WorkflowStatus.QUEUED
    return WorkflowStatus.AWAITING_START


def _bounded_text(
    value: object, label: str, maximum: int, *, single_line: bool
) -> str:
    if not isinstance(value, str) or not value or len(value) > maximum:
        raise ValueError(f"execution result {label} is invalid")
    if value != value.strip():
        raise ValueError(f"execution result {label} is invalid")
    if any(
        (ord(char) < 32 and char not in {"\n", "\t"}) or ord(char) == 127
        for char in value
    ):
        raise ValueError(f"execution result {label} is invalid")
    if single_line and any(char in value for char in "\r\n"):
        raise ValueError(f"execution result {label} is invalid")
    return value


def _text_collection(
    value: Sequence[str], label: str, maximum: int, *, single_line: bool
) -> tuple[str, ...]:
    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
        raise ValueError(f"execution result {label} are invalid")
    items = tuple(value)
    if len(items) > MAX_COLLECTION_ITEMS:
        raise ValueError(f"execution result {label} are invalid")
    return tuple(
        _bounded_text(item, label, maximum, single_line=single_line)
        for item in items
    )


#: A structured record is normalised on the way in, so everything that reads
#: one later sees a single shape. The reader-facing fields an agent may fill
#: are named here and nowhere else.
_ACTION_FIELDS = ("requires", "channel")
_DELIVERABLE_FIELDS = ("label", "recipient", "subject")
_ACTION_ALIASES = ("action", "title", "text")
_DELIVERABLE_ALIASES = ("body", "text")


def _record_text(value: dict, aliases: tuple[str, ...]) -> object:
    for key in aliases:
        candidate = value.get(key)
        if isinstance(candidate, str) and candidate.strip():
            return candidate
    return None


def _structured_collection(
    value: object, label: str, maximum: int, *,
    primary: str, aliases: tuple[str, ...], optional: tuple[str, ...],
) -> tuple[object, ...]:
    """Accept plain lines or structured records, and store one shape.

    A plain string stays a plain string: every result written before this
    existed is still valid, and an agent with nothing structured to say
    should not have to wrap a sentence in an object. A record keeps only
    the fields a card knows how to show, each bounded like any other text,
    so a large object cannot arrive through a field that was never read.
    """
    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
        raise ValueError(f"execution result {label} are invalid")
    items = tuple(value)
    if len(items) > MAX_COLLECTION_ITEMS:
        raise ValueError(f"execution result {label} are invalid")
    records: list[object] = []
    for item in items:
        if isinstance(item, str):
            records.append(
                _bounded_text(item, label, maximum, single_line=False))
            continue
        if not isinstance(item, dict):
            raise ValueError(f"execution result {label} are invalid")
        text = _record_text(item, aliases)
        if text is None:
            raise ValueError(f"execution result {label} are invalid")
        record = {
            primary: _bounded_text(
                text, label, maximum, single_line=False),
        }
        for name in optional:
            supplied = item.get(name)
            if supplied is None:
                continue
            record[name] = _bounded_text(
                supplied, label, MAX_QUESTION_CHARS, single_line=True)
        unknown = set(item) - set(aliases) - set(optional)
        if unknown:
            raise ValueError(f"execution result {label} are invalid")
        records.append(record)
    return tuple(records)


def _canonical_json(value: object) -> str:
    return json.dumps(
        value, ensure_ascii=True, separators=(",", ":"), sort_keys=True
    )


def _valid_identity(task_id: object, version: object) -> bool:
    return (
        not isinstance(task_id, bool)
        and isinstance(task_id, int)
        and task_id >= 1
        and not isinstance(version, bool)
        and isinstance(version, int)
        and version >= 1
    )


def _valid_secret(value: object) -> bool:
    return (
        isinstance(value, str)
        and 32 <= len(value) <= 512
        and not any(char.isspace() for char in value)
    )


def _valid_lease(value: object) -> bool:
    return (
        not isinstance(value, bool)
        and isinstance(value, int)
        and MIN_LEASE_SECONDS <= value <= MAX_LEASE_SECONDS
    )


def _validated_phase_allowlist(
    values: Sequence[WorkflowPhase | str] | None,
) -> tuple[WorkflowPhase, ...]:
    if values is None:
        return tuple(WorkflowPhase)
    if isinstance(values, (str, bytes)) or not isinstance(values, Sequence):
        raise ValueError("execution phase allowlist is invalid")
    try:
        phases = tuple(WorkflowPhase(value) for value in values)
    except (TypeError, ValueError):
        raise ValueError("execution phase allowlist is invalid") from None
    if not phases or len(set(phases)) != len(phases):
        raise ValueError("execution phase allowlist is invalid")
    return phases


def _token_digest(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()
