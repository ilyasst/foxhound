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
from collections.abc import Iterable, Mapping
from contextlib import closing
from dataclasses import dataclass, field
from datetime import datetime, time, timedelta, timezone
from enum import StrEnum
from pathlib import Path
from typing import Callable, Sequence

from .task_owner import normalized_aliases, reader_owned
from .agent_profiles import (
    AgentProfile,
    AgentProfileError,
    AgentProfileRegistry,
    load_registry,
)
from .candidate_inbox import CandidateInbox, InboxError, SCHEMA_VERSION
from .source_policy import (
    action_grants as _action_grants,
    execution_grants as _execution_grants,
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
#: Source ordering is stronger than a reader's bounded queue preference.
#: Pull-request reviews lead the queue, repository issues trail it, and
#: communication plus every other non-issue source remain in the middle tier.
_SOURCE_QUEUE_ORDER_SQL = (
    "CASE origin_kind "
    "WHEN 'review_request' THEN 0 "
    "WHEN 'issue' THEN 2 ELSE 1 END,"
)

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
#: A derived sentence or three about why one attempt stopped, bounded to
#: match the column that stores it. See `failure_digest.py`.
MAX_FAILURE_DIGEST_CHARS = 800

# Agent processes are scarce independently of planned work.  The runner claim
# transaction enforces this cap, so two hosts (or two local slots) cannot both
# observe room and start a third process.
#
# These three are defaults only. A deployment overrides them by passing
# execution_slot_cap / plan_ready_cap / awaiting_reader_cap to
# TaskExecutionService, typically from a CLI flag set in that machine's own
# process supervisor configuration -- not by editing this file, which a
# deployment may keep synced to origin on its own schedule.
EXECUTION_SLOT_CAP = 2
# Keep a durable planning reserve separate from the running slots.  A claimed
# plan leaves this many ready plans behind, rather than making the next slot
# wait for a periodic intake pass.
PLAN_READY_CAP = 10
# Compatibility name retained for callers that report the old aggregate.  It
# now describes only active execution, not queued planning work.
WORK_IN_PROGRESS_CAP = EXECUTION_SLOT_CAP
AWAITING_READER_CAP = 20
# Sentinel a caller passes to mean "no cap" for one of the three overrides
# above, distinct from omitting the override (which keeps the default).
UNBOUNDED_CAP = -1
# How far down the ready queue one claim may look for a workflow it can
# actually run.  Bounded so a large queue of unresolvable pins cannot turn a
# single claim into a full table scan, and generous enough that a realistic
# run of them still lets healthy work through on the same pass.
MAX_CLAIM_SCAN = 50
WORKING_STATUSES = frozenset({"queued", "running"})
READER_WAITING_STATUSES = frozenset({
    "awaiting_start", "awaiting_review", "completed",
})
MAX_ACTION_CHARS = 4_000
MAX_DELIVERABLE_CHARS = 16_000
MAX_REPOSITORY_REFERENCES = 8

_RESULT_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
_PROFILE_ID_RE = re.compile(r"^[a-z][a-z0-9-]{0,31}$")
_DIGEST_RE = re.compile(r"^[0-9a-f]{64}$")
_RUN_ID_RE = re.compile(r"^[0-9a-f]{32}$")


def _resolve_cap(override: int | None, default: int) -> int | None:
    """Turn a constructor override into the cap this instance enforces.

    None means the caller did not override this cap: keep the module
    default. UNBOUNDED_CAP means the caller asked for no cap at all, and is
    returned as None here -- the internal representation of "unbounded" at
    every call site that checks a cap. Any other value must be a valid
    capacity.
    """
    if override is None:
        return default
    if override == UNBOUNDED_CAP:
        return None
    if isinstance(override, bool) or not isinstance(override, int) or override < 0:
        raise ValueError("capacity override is invalid")
    return override
FAILURE_REASONS = frozenset({
    "startup_failed",
    "process_exit",
    "timeout",
    "interrupted",
    "claim_expired",
    "lease_failed",
    "result_invalid",
    #: The runtime refused the request because its measured context did not
    #: fit any served window. Retrying unchanged cannot make it fit.
    "context_exhausted",
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
    RESULT_UNCHANGED = "result_unchanged"
    AGENT_PROFILE_UNAVAILABLE = "agent_profile_unavailable"


#: Statuses whose pin may be rebound. `running` is excluded deliberately: an
#: agent mid-run holds a lease sized by the revision it started under, and
#: rebinding beneath it is exactly what ADR 0024's fence protects.
_ADOPTABLE_STATUSES = (
    WorkflowStatus.QUEUED,
    WorkflowStatus.PARKED,
    WorkflowStatus.AWAITING_START,
    WorkflowStatus.SNOOZED,
    WorkflowStatus.AWAITING_REVIEW,
)

class WorkflowPriority(StrEnum):
    """The only reader-controlled ordering states for one ready workflow."""

    RAISED = "raised"
    NORMAL = "normal"
    LOWERED = "lowered"


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
    last_failure_exit_code: int | None
    last_failure_run_id: str | None
    steer_while_running: bool
    current_run_id: str | None
    last_failure_at: str | None
    next_attempt_at: str | None
    parked_at: str | None
    last_result_id: str | None
    created_at: str
    updated_at: str
    completed_at: str | None
    agent_profile_id: str
    agent_profile_revision: str
    priority: WorkflowPriority


WORKFLOW_BOARD_STATUSES = (
    "ready_to_start", "queued", "running", "plan_review",
    "external_review", "result_review", "snoozed", "parked",
    "completed", "cancelled",
)


@dataclass(frozen=True)
class WorkflowBoardEntry:
    """The bounded, reader-safe face of one current execution workflow."""

    task_id: int
    workflow_version: int
    board_status: str
    phase: WorkflowPhase
    task: str
    owner: str
    agent: str
    state_since: str


@dataclass(frozen=True)
class WorkflowBoard:
    entries: tuple[WorkflowBoardEntry, ...]
    totals: Mapping[str, int]


@dataclass(frozen=True)
class WorkflowBoardDetail:
    accepted: bool
    task_id: int
    workflow_version: int | None = None
    status: WorkflowStatus | None = None
    phase: WorkflowPhase | None = None
    updated_at: str | None = None
    summary: str = ""
    work_digest: str = ""
    #: The recorded account of the run, as the agent wrote it. `summary` and
    #: `work_digest` are short derivations of this and carry no structure, so
    #: this is the only field a reader can be shown the actual work through.
    work_markdown: str = ""
    deliverables: tuple[Mapping[str, str], ...] = ()
    refusal: WorkflowRefusal | None = None


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
    voice_summary: str = field(default="", repr=False)
    questions: Sequence[str] = field(default=(), repr=False)
    external_actions: Sequence[object] = field(default=(), repr=False)
    deliverables: Sequence[object] = field(default=(), repr=False)
    #: Addressable forge evidence, separately validated from prose so cards
    #: never have to infer a pull request, commit, or check from Markdown.
    repository_references: Sequence[object] = field(
        default=(), repr=False)
    #: A repository-origin execution defaults to impactful until its agent
    #: explicitly records that the result is analysis or research only.
    repository_impact: bool = field(default=True, repr=False)
    task_work_directory: str | None = field(default=None, repr=False)
    task_kb_file: str | None = field(default=None, repr=False)
    artifacts: Sequence[object] = field(default=(), repr=False)
    #: Which reader instruction this run was handed, if any.
    reader_instruction_sequence: int | None = field(
        default=None, repr=False)


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
    priority: WorkflowPriority | None = None

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
    #: Parked workflows whose measured context did not fit a served window.
    #: This is a subset of ``parked``, kept separate so operators can count
    #: unsatisfiable work rather than infer it from busy runner slots.
    context_exhausted: int
    #: Workflows that have stopped and been restarted more than once in the
    #: phase they are in now without ever recording a result for it.  Not a
    #: subset of ``parked``: the loop this counts spends most of its time
    #: queued or running, because a reader answering the Start card requeues
    #: it immediately.  Two such workflows once took about a fifth of all
    #: slot time over two days while recording nothing, and no number
    #: anywhere made that visible.
    unproductive: int
    completed: int
    cancelled: int


@dataclass(frozen=True)
class ProfileAdoptionResult:
    """Content-free outcome of one bounded profile-adoption pass.

    Counts only. Which workflows moved is in the ledger and its events; a
    report that named them would leak the queue's shape to anywhere this is
    printed, and an operator deciding whether to run it again needs the
    magnitudes, not the identities.
    """

    #: Workflows examined: pinned to a retired revision of an installed
    #: profile, and eligible by status.
    examined: int
    #: Workflows rebound to the installed revision.
    adopted: int
    #: Eligible workflows left alone because the pass hit its limit.
    remaining: int
    #: Adoptions that moved a workflow to a **smaller** budget. Never
    #: silent: a workflow that loses time because its profile was narrowed
    #: is a decision, and the operator is told it happened.
    narrowed: int
    #: Workflows skipped because they are running. Rebinding under a live
    #: claim is what ADR 0024's fence protects, and it stays protected.
    skipped_running: int
    #: Workflows whose retired revision could not be read, so the budget
    #: change could not be compared. Adopted anyway -- the installed
    #: revision is by definition the one the operator chose -- but counted.
    unknown_budget: int
    #: True when nothing was written.
    dry_run: bool


@dataclass(frozen=True)
class ExecutionScheduleResult:
    """Content-free result of one bounded new-task scheduling pass.

    `remaining` alone cannot say why a candidate was left behind. A pass that
    scheduled nothing because nothing was eligible and a pass that scheduled
    nothing because a capacity cap is saturated report the same two numbers,
    and only the second one means new work has stopped entering the system.
    That ambiguity is not theoretical: one deployment ran `scheduled: 0,
    remaining: 36` every five minutes for days -- a `plan_ready_cap` of 10
    against 158 ready plans -- while every pass logged `ok: true`.

    `capped` closes it. It counts candidates this pass declined to admit
    *because a cap was binding*, and deliberately not those left for the next
    pass by `limit`, which is ordinary paging and drains on its own.
    """

    scheduled: int
    remaining: int
    #: Eligible candidates held back by `plan_ready_cap` or
    #: `awaiting_reader_cap`. Non-zero means admission is closed, not idle.
    capped: int = 0


@dataclass(frozen=True)
class PendingFailureDigest:
    """One failed attempt whose transcript has not been summarised yet."""

    task_id: int
    #: The workflow version that was claimed and failed, which is also the
    #: run directory's attempt. Not the version the failure created.
    workflow_version: int
    phase: WorkflowPhase
    run_id: str
    reason: str | None


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
    #: Whether this is the revision the registry currently offers for this
    #: profile. Distinct from `available`, and the distinction is the point:
    #: a retired revision kept in the store resolves exactly and runs
    #: perfectly, so `available` is true for it. What it does not do is
    #: carry the budget the operator has since installed.
    #:
    #: A profile's revision fixes its timeout and turn limit, so a workflow
    #: pinned to a retired one is held to a budget that was replaced. On one
    #: deployment the installed repository profile allowed 3300 seconds and
    #: 120 turns while the overwhelming majority of workflows were pinned to
    #: revisions allowing 2400 and 80, or 1800 and 50 -- and every one of
    #: them reported as available, because every one of them was. Runs
    #: terminated at the retired timeout and transcripts ended at the
    #: retired turn limit, with nothing anywhere naming the cause.
    #:
    #: False for a profile the registry no longer offers at all, where
    #: `available` already says the stronger thing.
    current: bool = False


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
        steer_while_running: object = None,
        execution_grants: object = None,
        skip_planning_for: object = None,
        action_grants: object = None,
        execution_slot_cap: int | None = None,
        plan_ready_cap: int | None = None,
        awaiting_reader_cap: int | None = None,
        reader_aliases: object = None,
        profile_routes: Mapping[str, str] | None = None,
    ) -> None:
        if (isinstance(max_attempts, bool)
                or not isinstance(max_attempts, int)
                or not 1 <= max_attempts <= MAX_ATTEMPTS):
            raise ValueError("maximum execution attempts are invalid")
        self._execution_slot_cap = _resolve_cap(
            execution_slot_cap, EXECUTION_SLOT_CAP)
        self._plan_ready_cap = _resolve_cap(plan_ready_cap, PLAN_READY_CAP)
        self._awaiting_reader_cap = _resolve_cap(
            awaiting_reader_cap, AWAITING_READER_CAP)
        if reader_aliases is None:
            self._reader_aliases: frozenset[str] = frozenset()
        elif isinstance(reader_aliases, (str, bytes)) or not isinstance(
            reader_aliases, Iterable
        ):
            raise ValueError("reader aliases are invalid")
        else:
            values = list(reader_aliases)
            if any(not isinstance(alias, str) for alias in values):
                raise ValueError("reader aliases are invalid")
            self._reader_aliases = normalized_aliases(values)
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
        if profile_routes is None:
            profile_routes = {}
        if (
            not isinstance(profile_routes, Mapping)
            or any(
                not isinstance(kind, str) or not kind
                or not isinstance(profile_id, str) or not profile_id
                for kind, profile_id in profile_routes.items()
            )
        ):
            raise ValueError("agent profile routes are invalid")
        # Resolve every route now rather than at the first task that uses it.
        # A scheduler started against a catalog that lacks a routed profile
        # would otherwise fail one pass at a time, reporting a scheduling
        # failure for what is a configuration problem -- and the machine that
        # rendered its command line is not the one that installs the catalog.
        for routed in sorted(set(profile_routes.values())):
            candidate = registry.get(routed)
            if candidate is None or any(
                phase.value not in candidate.allowed_phases
                for phase in WorkflowPhase
            ):
                raise ValueError("routed agent profile is unavailable")
        self._profile_routes = dict(profile_routes)
        # Task IDs the last claim deferred because their pinned profile could
        # not be resolved.  Read by the runner so a queue that is quietly
        # shedding work says so instead of just looking idle.
        self._last_claim_deferred: tuple[int, ...] = ()
        # Empty unless this machine says otherwise: an operator who has not
        # decided is asked, rather than having the decision made for them
        # by whichever machine edited a shared file first.
        self._planning_grants = _planning_grants(planning_grants)
        # This is a notification policy, not an execution authority.  It is
        # still validated against the closed source-kind vocabulary so a typo
        # cannot silently make a run invisible.
        self._steer_while_running = _planning_grants(steer_while_running)
        # Independent of planning authority above. A machine that has
        # said a kind may be planned has not said its plan may be run.
        self._execution_grants = _execution_grants(execution_grants)
        # This declaration removes a phase rather than merely its reader
        # gate, so it needs execution authority.  It is deliberately not
        # inferred from either grant list: an operator may want unattended
        # planning while retaining the reviewable plan as the reader input.
        self._skip_planning_for = _execution_grants(
            skip_planning_for, label="skip-planning declarations"
        )
        missing_execution_grants = (
            self._skip_planning_for - self._execution_grants
        )
        if missing_execution_grants:
            raise ValueError(
                "skip-planning declarations lack execution grants: "
                + ", ".join(sorted(missing_execution_grants))
            )
        # And the planning grant, because the plan phase carries the reader's
        # start gate. Without this, adding a kind here would also delete the
        # card that asks whether to begin at all, turning an asked source
        # into an unattended one with no declaration saying so.
        missing_planning_grants = (
            self._skip_planning_for - self._planning_grants
        )
        if missing_planning_grants:
            raise ValueError(
                "skip-planning declarations lack planning grants: "
                + ", ".join(sorted(missing_planning_grants))
            )
        # Independent again. Executing a plan and performing an effect
        # other people can see are not the same permission.
        self._action_grants = _action_grants(action_grants)
        self._default_profile = profile

    def _profile_for(self, origin_kind: object) -> AgentProfile:
        """Which agent a task of this kind starts on.

        An unmapped kind uses the declared default. A mapped profile is
        validated when deployment configuration loads, so this selection is
        deterministic and never falls back to another profile.
        """
        preferred = self._profile_routes.get(origin_kind)
        if preferred is not None:
            profile = self._profile_registry.get(preferred)
            if profile is None:
                raise TaskLedgerError("routed agent profile is unavailable")
            return profile
        return self._default_profile

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
                    "AND (w.task_id IS NULL OR (w.task_version!=t.version "
                    # A re-surfaced task needs a fresh workflow whatever its
                    # source kind. Without this, ADR 0039 reopens a task whose
                    # terminal workflow still satisfies this join, and it never
                    # runs again.
                    #
                    # Keyed on the reopen EVENT, not on the old workflow's
                    # status. Status looked like the obvious discriminator and
                    # is the wrong one: `_cancel_stale` writes `cancelled`
                    # moments earlier in this same pass for a workflow whose
                    # task merely moved underneath it, and that reconciliation
                    # must keep ending in a cancellation rather than becoming
                    # an immediate re-schedule. The event says the task was
                    # reopened, which is the thing actually being asked.
                    "AND (EXISTS(SELECT 1 FROM task_events AS reopened "
                    "WHERE reopened.task_id=t.id "
                    "AND reopened.kind='status_changed' "
                    "AND reopened.from_status='done' "
                    "AND reopened.to_status='open' "
                    "AND reopened.task_version>w.task_version) "
                    "OR EXISTS(SELECT 1 FROM task_candidate_bindings AS review "
                    "JOIN candidate_inbox AS source "
                    "ON source.candidate_id=review.candidate_id "
                    "WHERE review.task_id=t.id AND review.relation='accepted' "
                    "AND source.source_kind='review_request')))) "
                    "AND NOT EXISTS("
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
                plan_room = (
                    None if self._plan_ready_cap is None else max(
                        0,
                        self._plan_ready_cap
                        - int(capacity["ready_plans"] or 0),
                    )
                )
                waiting_room = (
                    None if self._awaiting_reader_cap is None else max(
                        0,
                        self._awaiting_reader_cap
                        - int(capacity["waiting"] or 0),
                    )
                )
                promoted = 0
                waiting_rows = connection.execute(
                    "SELECT w.*," + _OWNER_COLUMNS + "("
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
                    f") ORDER BY {_SOURCE_QUEUE_ORDER_SQL}"
                    "w.task_id"
                ).fetchall()
                for row in waiting_rows:
                    origin_kind = row["origin_kind"]
                    if _initial_status(
                        origin_kind, self._planning_grants, row,
                        self._reader_aliases,
                    ) is not WorkflowStatus.QUEUED:
                        continue
                    version = int(row["version"]) + 1
                    profile = self._profile_for(origin_kind)
                    connection.execute(
                        "UPDATE task_execution_workflows SET status='queued',"
                        "version=?,due_at=NULL,steer_while_running=?,"
                        "agent_profile_id=?,"
                        "agent_profile_revision=?,updated_at=? "
                        "WHERE task_id=? AND version=? "
                        "AND status='awaiting_start' AND phase='plan'",
                        (
                            version, int(origin_kind in self._steer_while_running),
                            profile.profile_id, profile.revision,
                            now, int(row["task_id"]), int(row["version"]),
                        ),
                    )
                    _cancel_superseded_start_cards(
                        connection,
                        task_id=int(row["task_id"]),
                        workflow_version=int(row["version"]),
                        now=now,
                    )
                    self._event(
                        connection, int(row["task_id"]), "scheduled",
                        version, int(row["task_version"]), WorkflowPhase.PLAN,
                        WorkflowStatus.QUEUED, now,
                    )
                    promoted += 1

                granted = self._reconcile_granted_review_gates(
                    connection, now
                )

                rows = connection.execute(
                    "SELECT t.id,t.version,w.task_id AS workflow_task_id,"
                    "w.version AS workflow_version," + _OWNER_COLUMNS + "("
                    " SELECT o.source_kind FROM task_candidate_bindings AS b "
                    " JOIN candidate_inbox AS o "
                    " ON o.candidate_id=b.candidate_id "
                    " WHERE b.task_id=t.id AND b.relation='accepted'"
                    ") AS origin_kind FROM tasks AS t "
                    "LEFT JOIN task_execution_workflows AS w "
                    "ON w.task_id=t.id WHERE t.status='open' "
                    "AND (w.task_id IS NULL OR (w.task_version!=t.version "
                    # A re-surfaced task needs a fresh workflow whatever its
                    # source kind. Without this, ADR 0039 reopens a task whose
                    # terminal workflow still satisfies this join, and it never
                    # runs again.
                    #
                    # Keyed on the reopen EVENT, not on the old workflow's
                    # status. Status looked like the obvious discriminator and
                    # is the wrong one: `_cancel_stale` writes `cancelled`
                    # moments earlier in this same pass for a workflow whose
                    # task merely moved underneath it, and that reconciliation
                    # must keep ending in a cancellation rather than becoming
                    # an immediate re-schedule. The event says the task was
                    # reopened, which is the thing actually being asked.
                    "AND (EXISTS(SELECT 1 FROM task_events AS reopened "
                    "WHERE reopened.task_id=t.id "
                    "AND reopened.kind='status_changed' "
                    "AND reopened.from_status='done' "
                    "AND reopened.to_status='open' "
                    "AND reopened.task_version>w.task_version) "
                    "OR EXISTS(SELECT 1 FROM task_candidate_bindings AS review "
                    "JOIN candidate_inbox AS source "
                    "ON source.candidate_id=review.candidate_id "
                    "WHERE review.task_id=t.id AND review.relation='accepted' "
                    "AND source.source_kind='review_request')))) "
                    "AND NOT EXISTS("
                    " SELECT 1 FROM task_candidate_bindings AS blocked JOIN "
                    " task_candidate_lifecycle AS l "
                    " ON l.candidate_id=blocked.candidate_id "
                    " WHERE blocked.task_id=t.id AND blocked.relation='accepted' "
                    " AND l.state='withdrawn' AND l.resolution='preserved_open'"
                    f") ORDER BY {_SOURCE_QUEUE_ORDER_SQL}"
                    "t.id"
                )
                scheduled = 0
                capped = 0
                for row in rows:
                    if scheduled >= limit:
                        break
                    task_id = int(row["id"])
                    task_version = int(row["version"])
                    phase = _initial_phase(
                        row["origin_kind"], self._skip_planning_for
                    )
                    status = _initial_status(
                        row["origin_kind"], self._planning_grants, row,
                        self._reader_aliases,
                    )
                    if (
                        phase is WorkflowPhase.PLAN
                        and status.value in WORKING_STATUSES
                    ):
                        if plan_room is not None:
                            if plan_room == 0:
                                capped += 1
                                continue
                            plan_room -= 1
                    elif status.value not in WORKING_STATUSES:
                        if waiting_room is not None:
                            if waiting_room == 0:
                                capped += 1
                                continue
                            waiting_room -= 1
                    profile = self._profile_for(row["origin_kind"])
                    if row["workflow_task_id"] is None:
                        version = 1
                        connection.execute(
                            "INSERT INTO task_execution_workflows("
                            "task_id,task_version,status,phase,version,due_at,"
                            "failure_count,created_at,updated_at,agent_profile_id,"
                            "agent_profile_revision,steer_while_running) "
                            "VALUES(?,?,?,?,?,NULL,0,?,?,?,?,?)",
                            (
                                task_id, task_version, status.value, phase.value,
                                version, now, now,
                                profile.profile_id,
                                profile.revision,
                                int(row["origin_kind"] in self._steer_while_running),
                            ),
                        )
                    else:
                        version = int(row["workflow_version"]) + 1
                        connection.execute(
                            "UPDATE task_execution_workflows SET task_version=?,"
                            "status=?,phase=?,version=?,due_at=NULL,"
                            "claim_token_digest=NULL,claimed_at=NULL,"
                            "claim_heartbeat_at=NULL,claim_expires_at=NULL,"
                            "current_run_id=NULL,"
                            "failure_count=0,last_failure_reason=NULL,"
                            "last_failure_exit_code=NULL,last_failure_run_id=NULL,"
                            "last_failure_at=NULL,next_attempt_at=NULL,"
                            "parked_at=NULL,last_result_id=NULL,updated_at=?,"
                            "completed_at=NULL,agent_profile_id=?,"
                            "agent_profile_revision=?,steer_while_running=? "
                            "WHERE task_id=?",
                            (
                                task_version, status.value, phase.value, version,
                                now, profile.profile_id, profile.revision,
                                int(row["origin_kind"] in self._steer_while_running),
                                task_id,
                            ),
                        )
                    self._event(
                        connection,
                        task_id,
                        "scheduled",
                        version,
                        task_version,
                        phase,
                        status,
                        now,
                    )
                    scheduled += 1
                connection.commit()
                return ExecutionScheduleResult(
                    scheduled=promoted + granted + scheduled,
                    remaining=eligible - scheduled,
                    capped=capped,
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
                    "SELECT t.status,t.version," + _OWNER_COLUMNS + "("
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
                # A scheduled caller can arrive while the reader's snooze is
                # still cooling.  Treat that state as a durable refusal to
                # re-plan, rather than relying on the broader non-terminal
                # set below: adding another resettable status there must not
                # make a future snooze eligible for a fresh Start gate.
                if (row is not None
                        and int(row["task_version"]) == expected_task_version
                        and row["status"] == WorkflowStatus.SNOOZED
                        and row["due_at"] > now):
                    connection.rollback()
                    return _operation(row, WorkflowDisposition.UNCHANGED)
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
                    phase = _initial_phase(
                        task["origin_kind"], self._skip_planning_for
                    )
                    status = _initial_status(
                        task["origin_kind"], self._planning_grants, task,
                        self._reader_aliases,
                    )
                    profile = self._profile_for(task["origin_kind"])
                    connection.execute(
                        "INSERT INTO task_execution_workflows("
                        "task_id,task_version,status,phase,version,due_at,"
                        "failure_count,created_at,updated_at,agent_profile_id,"
                        "agent_profile_revision,steer_while_running) "
                        "VALUES(?,?,?,?,?,NULL,0,?,?,?,?,?)",
                        (
                            task_id, expected_task_version, status.value,
                            phase.value, version, now, now,
                            profile.profile_id, profile.revision,
                            int(task["origin_kind"] in self._steer_while_running),
                        ),
                    )
                else:
                    version = int(row["version"]) + 1
                    phase = _initial_phase(
                        task["origin_kind"], self._skip_planning_for
                    )
                    status = _initial_status(
                        task["origin_kind"], self._planning_grants, task,
                        self._reader_aliases,
                    )
                    profile = self._profile_for(task["origin_kind"])
                    connection.execute(
                        "UPDATE task_execution_workflows SET task_version=?,"
                        "status=?,phase=?,version=?,"
                        "due_at=NULL,claim_token_digest=NULL,claimed_at=NULL,"
                        "claim_heartbeat_at=NULL,claim_expires_at=NULL,"
                        "current_run_id=NULL,"
                        "failure_count=0,last_failure_reason=NULL,"
                        "last_failure_exit_code=NULL,last_failure_run_id=NULL,"
                        "last_failure_at=NULL,next_attempt_at=NULL,"
                        "parked_at=NULL,last_result_id=NULL,updated_at=?,"
                        "completed_at=NULL,agent_profile_id=?,"
                        "agent_profile_revision=?,steer_while_running=? "
                        "WHERE task_id=?",
                        (
                            expected_task_version, status.value, phase.value,
                            version, now,
                            profile.profile_id, profile.revision,
                            int(task["origin_kind"] in self._steer_while_running),
                            task_id,
                        ),
                    )
                self._event(
                    connection, task_id, "scheduled", version,
                    expected_task_version, phase,
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

    @property
    def last_claim_deferred(self) -> tuple[int, ...]:
        """Task IDs the last claim could not run and deferred."""
        return self._last_claim_deferred

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
                if (
                    self._execution_slot_cap is not None
                    and running >= self._execution_slot_cap
                ):
                    connection.commit()
                    return None
                # A batch, not one row. A workflow can be pinned to a
                # profile revision this machine cannot resolve -- the
                # catalog was rewritten without it, or the install has not
                # landed yet -- and resolving the head of the queue used to
                # raise straight out of the claim. One such row then stopped
                # every OTHER queued workflow behind it, for as long as it
                # stayed at the head, with the runner reporting only that it
                # had failed. An unresolvable pin is a property of that
                # workflow, so it is deferred like any other per-task
                # failure and the scan moves on.
                rows = connection.execute(
                    "SELECT w.*,t.text,t.owner,t.due,t.status AS task_status,"
                    "t.version AS current_task_version,("
                    " SELECT o.source_kind FROM task_candidate_bindings AS b "
                    " JOIN candidate_inbox AS o "
                    " ON o.candidate_id=b.candidate_id "
                    " WHERE b.task_id=t.id AND b.relation='accepted'"
                    ") AS origin_kind "
                    "FROM task_execution_workflows AS w JOIN tasks AS t "
                    # `parked` is claimable once its retry time arrives.
                    # Parking stops the immediate retries; it is not a
                    # decision to abandon the work, and a reader who never
                    # answers the card should still have it attempted.
                    "ON t.id=w.task_id "
                    "WHERE ("
                    " (w.status='queued' AND (w.next_attempt_at IS NULL "
                    "  OR w.next_attempt_at<=?))"
                    " OR (w.status='parked' AND w.next_attempt_at IS NOT NULL "
                    "     AND w.next_attempt_at<=?)"
                    ") "
                    f"AND w.phase IN ({placeholders}) "
                    "AND t.status='open' AND t.version=w.task_version "
                    f"ORDER BY {_SOURCE_QUEUE_ORDER_SQL}"
                    "CASE w.queue_priority "
                    "WHEN 'raised' THEN 0 WHEN 'normal' THEN 1 ELSE 2 END,"
                    "CASE WHEN w.failure_count=0 THEN 0 ELSE 1 END,"
                    "w.updated_at,w.task_id LIMIT ?",
                    (now, now, *(phase.value for phase in phases),
                     MAX_CLAIM_SCAN),
                ).fetchall()
                row = None
                profile = None
                deferred: list[int] = []
                for candidate in rows:
                    try:
                        resolved = self._resolve_profile(candidate)
                        if candidate["phase"] not in resolved.allowed_phases:
                            raise TaskLedgerError(
                                "execution agent profile is unavailable"
                            )
                    except TaskLedgerError:
                        # Same reason the runner already records when the
                        # profile disappears AFTER the claim, so one cause
                        # does not read as two. Retries first, then parks,
                        # which is what raises the card.
                        self._defer_failure(
                            connection, candidate, "startup_failed", stamp,
                            event_kind=None,
                        )
                        deferred.append(int(candidate["task_id"]))
                        continue
                    row = candidate
                    profile = resolved
                    break
                self._last_claim_deferred = tuple(deferred)
                if row is None or profile is None:
                    connection.commit()
                    return None
                expires = (
                    stamp + timedelta(seconds=profile.claim_lease_seconds)
                ).isoformat(timespec="seconds")
                version = int(row["version"]) + 1
                updated = connection.execute(
                    "UPDATE task_execution_workflows SET status='running',"
                    "version=?,claim_token_digest=?,claimed_at=?,"
                    "claim_heartbeat_at=?,claim_expires_at=?,updated_at=?,"
                    "current_run_id=NULL,"
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
                    "parked_at=NULL,next_attempt_at=NULL,queue_priority='normal' "
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

    def attach_run_id(
        self, task_id: int, *, expected_version: int, claim_token: str,
        run_id: str,
    ) -> WorkflowOperationResult:
        """Best-effortly name the private directory of one live claim."""
        if (
            not _valid_identity(task_id, expected_version)
            or not _valid_secret(claim_token)
            or not isinstance(run_id, str)
            or not re.fullmatch(r"[0-9a-f]{32}", run_id)
        ):
            return _refused(task_id, WorkflowRefusal.INVALID_ARGUMENT)
        now = self._now()
        with closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = self._workflow_with_task(connection, task_id)
                refusal = _running_guard(
                    row, expected_version, _token_digest(claim_token), now
                )
                if refusal is not None:
                    connection.rollback()
                    return _refused_row(task_id, row, refusal)
                changed = connection.execute(
                    "UPDATE task_execution_workflows SET current_run_id=?,"
                    "updated_at=? WHERE task_id=? AND version=? "
                    "AND status='running' AND claim_token_digest=?",
                    (run_id, now, task_id, expected_version,
                     _token_digest(claim_token)),
                )
                if changed.rowcount != 1:
                    connection.rollback()
                    return _refused_row(
                        task_id, row, WorkflowRefusal.CLAIM_MISMATCH
                    )
                connection.commit()
                return _operation(
                    self._workflow_with_task(connection, task_id),
                    WorkflowDisposition.APPLIED,
                )
            except Exception:
                connection.rollback()
                raise

    def set_priority(
        self,
        task_id: int,
        *,
        expected_version: int,
        action: str,
    ) -> WorkflowOperationResult:
        """Fence one queue-reader priority preference for one ready workflow.

        The operation deliberately accepts a closed action vocabulary instead
        of a number or an ordering position.  It applies only to a current,
        immediately claimable queued workflow; snoozed, cooling, parked,
        held, running, completed, and stale work keeps its existing lifecycle
        semantics.  The preference is consumed atomically by a successful
        claim, so it cannot silently steer a later phase.
        """
        if not _valid_identity(task_id, expected_version):
            return _refused(task_id, WorkflowRefusal.INVALID_ARGUMENT)
        priorities = {
            "raise": WorkflowPriority.RAISED,
            "lower": WorkflowPriority.LOWERED,
            "clear": WorkflowPriority.NORMAL,
        }
        priority = priorities.get(action)
        if priority is None:
            return _refused(task_id, WorkflowRefusal.INVALID_ACTION)
        now = self._now()
        with closing(self._connect()) as connection:
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute("BEGIN IMMEDIATE")
            try:
                self._cancel_stale(connection, now)
                row = self._workflow_with_task(connection, task_id)
                refusal = _workflow_guard(
                    row, expected_version, {WorkflowStatus.QUEUED}
                )
                if refusal is None:
                    refusal = _task_guard(row, int(row["task_version"]))
                if (
                    refusal is None
                    and row is not None
                    and row["next_attempt_at"] is not None
                    and row["next_attempt_at"] > now
                ):
                    refusal = WorkflowRefusal.INVALID_STATE
                if refusal is not None:
                    connection.rollback()
                    return _refused_row(task_id, row, refusal)
                if row is None:  # Kept for type narrowing after the guard.
                    connection.rollback()
                    return _refused(task_id, WorkflowRefusal.NOT_FOUND)
                current = WorkflowPriority(row["queue_priority"])
                if current is priority:
                    connection.rollback()
                    return _operation(row, WorkflowDisposition.UNCHANGED)
                version = expected_version + 1
                updated = connection.execute(
                    "UPDATE task_execution_workflows SET queue_priority=?,"
                    "version=?,updated_at=? WHERE task_id=? AND version=? "
                    "AND status='queued' AND (next_attempt_at IS NULL OR "
                    "next_attempt_at<=?)",
                    (priority, version, now, task_id, expected_version, now),
                )
                if updated.rowcount != 1:
                    connection.rollback()
                    return _refused_row(
                        task_id, self._workflow_with_task(connection, task_id),
                        WorkflowRefusal.STALE_WORKFLOW,
                    )
                event_kind = {
                    "raise": "priority_raised",
                    "lower": "priority_lowered",
                    "clear": "priority_cleared",
                }[action]
                self._event(
                    connection,
                    task_id,
                    event_kind,
                    version,
                    int(row["task_version"]),
                    WorkflowPhase(row["phase"]),
                    WorkflowStatus.QUEUED,
                    now,
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
                    priority=priority,
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
        exit_code: int | None = None,
        run_id: str | None = None,
    ) -> WorkflowOperationResult:
        if reason not in FAILURE_REASONS or reason == "claim_expired":
            return _refused(task_id, WorkflowRefusal.INVALID_ARGUMENT)
        if reason == "process_exit":
            if (exit_code is None) != (run_id is None):
                return _refused(task_id, WorkflowRefusal.INVALID_ARGUMENT)
            if exit_code is not None and (
                    isinstance(exit_code, bool) or not isinstance(exit_code, int)
                    or exit_code < 1 or exit_code > 255
                    or not isinstance(run_id, str)
                    or not _RUN_ID_RE.fullmatch(run_id)):
                return _refused(task_id, WorkflowRefusal.INVALID_ARGUMENT)
        elif exit_code is not None or run_id is not None:
            return _refused(task_id, WorkflowRefusal.INVALID_ARGUMENT)
        return self._finish_claim(
            task_id,
            expected_version=expected_version,
            claim_token=claim_token,
            failure_reason=reason,
            failure_exit_code=exit_code,
            failure_run_id=run_id,
        )

    def _finish_claim(
        self,
        task_id: int,
        *,
        expected_version: int,
        claim_token: str,
        failure_reason: str | None,
        failure_exit_code: int | None = None,
        failure_run_id: str | None = None,
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
                        "current_run_id=NULL,"
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
                        exit_code=failure_exit_code,
                        run_id=failure_run_id,
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
                if _repeats_an_earlier_answer(connection, result):
                    # A pass that was asked to change something and produced
                    # a byte-identical result did not answer the request. It
                    # is the shape of work that was done and then lost before
                    # recording, and presenting it to the reader as an answer
                    # is what made that loss invisible.
                    #
                    # The claim stays live: the agent can re-read its own run
                    # directory, find what it actually wrote, and record that
                    # instead. A refusal it can act on is worth more than a
                    # result nobody can trust.
                    self._event(
                        connection, result["task_id"], "result_unchanged",
                        int(row["version"]), int(row["task_version"]),
                        WorkflowPhase(row["phase"]),
                        WorkflowStatus(row["status"]), now,
                    )
                    connection.commit()
                    return _refused_row(
                        result["task_id"], row,
                        WorkflowRefusal.RESULT_UNCHANGED,
                    )
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
                recorded_phase = WorkflowPhase(result["phase"])
                advance = _granted_advance(
                    recorded_phase,
                    ExecutionOutcome(result["outcome"]),
                    row["origin_kind"],
                    self._execution_grants,
                    self._action_grants,
                )
                phase = recorded_phase if advance is None else advance
                if advance is not None:
                    target = WorkflowStatus.QUEUED
                connection.execute(
                    "INSERT INTO task_execution_results("
                    "result_id,task_id,workflow_version,task_version,phase,"
                    "outcome,content_digest,summary,work_markdown,"
                    "questions_json,external_actions_json,deliverables_json,"
                    "repository_references_json,repository_impact,"
                    "created_at,agent_profile_id,agent_profile_revision,"
                    "task_work_directory,task_kb_file,work_digest,"
                    "reader_instruction_sequence) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        result["result_id"], result["task_id"],
                        result["workflow_version"], result["task_version"],
                        result["phase"], result["outcome"], digest,
                        result["summary"], result["work_markdown"],
                        result["questions_json"],
                        result["external_actions_json"],
                        result["deliverables_json"],
                        result["repository_references_json"],
                        result["repository_impact"], now,
                        row["agent_profile_id"],
                        row["agent_profile_revision"],
                        result["task_work_directory"],
                        result["task_kb_file"],
                        result["work_digest"],
                        result["reader_instruction_sequence"],
                    ),
                )
                connection.executemany(
                    "INSERT INTO execution_result_artifacts("
                    "result_id,ordinal,relative_path,name,size_bytes,"
                    "content_digest,run_directory) VALUES(?,?,?,?,?,?,?)",
                    [(result["result_id"], index, artifact["relative_path"],
                      artifact["name"], artifact["size_bytes"],
                      artifact["content_digest"], artifact["run_directory"])
                     for index, artifact in enumerate(result["artifacts"])],
                )
                version = result["workflow_version"] + 1
                completed = (
                    now if target is WorkflowStatus.COMPLETED else None
                )
                connection.execute(
                    "UPDATE task_execution_workflows SET status=?,phase=?,"
                    "version=?,claim_token_digest=NULL,claimed_at=NULL,"
                    "claim_heartbeat_at=NULL,claim_expires_at=NULL,"
                    "current_run_id=NULL,"
                    "failure_count=0,last_failure_reason=NULL,"
                    "last_failure_exit_code=NULL,last_failure_run_id=NULL,"
                    "last_failure_at=NULL,next_attempt_at=NULL,parked_at=NULL,"
                    "last_result_id=?,updated_at=?,completed_at=? "
                    "WHERE task_id=? AND version=? AND status='running'",
                    (
                        target, phase, version, result["result_id"], now,
                        completed, result["task_id"],
                        result["workflow_version"],
                    ),
                )
                # Recorded against the phase that produced it, whatever
                # happens next: the result belongs to the work that made it.
                self._event(
                    connection, result["task_id"], "result_recorded",
                    version, result["task_version"],
                    recorded_phase, target, now,
                )
                if advance is not None:
                    # Named apart from `phase_approved` so the ledger never
                    # says a reader approved a phase nobody was asked about.
                    self._event(
                        connection, result["task_id"], "phase_granted",
                        version, result["task_version"], phase, target, now,
                    )
                connection.commit()
                return WorkflowOperationResult(
                    WorkflowDisposition.APPLIED,
                    result["task_id"],
                    version,
                    target,
                    phase,
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
                    "last_failure_exit_code=NULL,last_failure_run_id=NULL,"
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

    def board(self, *, limit: int = 100) -> WorkflowBoard:
        """Return current workflow state without claiming or mutating it."""
        if (isinstance(limit, bool) or not isinstance(limit, int)
                or not 1 <= limit <= 100):
            raise TaskLedgerError("workflow board limit is invalid")
        where = " WHERE t.status='open' AND w.status NOT IN ('completed','cancelled')"
        with closing(self._connect()) as connection:
            grouped = connection.execute(
                "SELECT w.status,w.phase,COUNT(*) AS total "
                "FROM task_execution_workflows AS w JOIN tasks AS t ON t.id=w.task_id"
                + where + " GROUP BY w.status,w.phase"
            ).fetchall()
            rows = connection.execute(
                "SELECT w.task_id,w.version,w.status,w.phase,w.updated_at,"
                "w.agent_profile_id,t.text,COALESCE(t.owner,'') AS owner "
                "FROM task_execution_workflows AS w JOIN tasks AS t ON t.id=w.task_id"
                + where + " ORDER BY CASE w.status "
                "WHEN 'running' THEN 0 WHEN 'awaiting_review' THEN 1 "
                "WHEN 'queued' THEN 2 WHEN 'awaiting_start' THEN 3 "
                "WHEN 'parked' THEN 4 ELSE 5 END,w.updated_at,w.task_id LIMIT ?",
                (limit,),
            ).fetchall()
        totals = {status: 0 for status in WORKFLOW_BOARD_STATUSES}
        for row in grouped:
            totals[_workflow_board_status(WorkflowStatus(row["status"]), WorkflowPhase(row["phase"]))] += int(row["total"])
        entries = tuple(WorkflowBoardEntry(
            task_id=int(row["task_id"]), workflow_version=int(row["version"]),
            board_status=_workflow_board_status(WorkflowStatus(row["status"]), WorkflowPhase(row["phase"])),
            phase=WorkflowPhase(row["phase"]), task=_board_text(row["text"], 500),
            owner=_board_text(row["owner"], 200), agent=_board_text(row["agent_profile_id"], 64),
            state_since=str(row["updated_at"]),
        ) for row in rows)
        return WorkflowBoard(entries, totals)

    def board_detail(self, task_id: int, *, expected_version: int) -> WorkflowBoardDetail:
        """Read one current workflow detail through its version fence."""
        if not _valid_identity(task_id, expected_version):
            return WorkflowBoardDetail(False, task_id if isinstance(task_id, int) else 0,
                                       refusal=WorkflowRefusal.INVALID_ARGUMENT)
        with closing(self._connect()) as connection:
            row = connection.execute(
                "SELECT w.task_id,w.version,w.status,w.phase,w.updated_at,"
                "r.summary,r.work_digest,r.work_markdown,r.deliverables_json "
                "FROM task_execution_workflows AS w "
                "LEFT JOIN task_execution_results AS r ON r.result_id=w.last_result_id "
                "WHERE w.task_id=?", (task_id,),
            ).fetchone()
        if row is None:
            return WorkflowBoardDetail(False, task_id, refusal=WorkflowRefusal.NOT_FOUND)
        if int(row["version"]) != expected_version:
            return WorkflowBoardDetail(False, task_id, refusal=WorkflowRefusal.STALE_WORKFLOW)
        return WorkflowBoardDetail(
            True, task_id, workflow_version=int(row["version"]),
            status=WorkflowStatus(row["status"]), phase=WorkflowPhase(row["phase"]),
            updated_at=str(row["updated_at"]), summary=_board_text(row["summary"], 1200),
            work_digest=_board_text(row["work_digest"], 800),
            work_markdown=_board_text(row["work_markdown"], WORK_BODY_PROJECTION_MAX),
            deliverables=_board_deliverables(row["deliverables_json"]),
        )

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
                "SELECT i.sequence,i.value FROM execution_reader_inputs AS i "
                "WHERE i.task_id=? AND i.kind='discussion' "
                "AND i.target_workflow_version<=? AND NOT EXISTS("
                " SELECT 1 FROM task_execution_results AS r "
                " WHERE r.task_id=i.task_id AND ("
                "  r.reader_instruction_sequence=i.sequence"
                # The fallback. Results recorded before deliveries existed
                # name no instruction, so for those the only thing that can
                # say an instruction was answered is still the version it
                # was aimed at. A recorded link is preferred wherever there
                # is one, because version arithmetic cannot tell a run that
                # answered an instruction from one that merely ran after it.
                "  OR (r.reader_instruction_sequence IS NULL"
                "      AND r.workflow_version>=i.target_workflow_version)"
                " )"
                ") ORDER BY i.sequence DESC LIMIT 1",
                (task_id, expected_version),
            ).fetchone()
            if value is None:
                return None
            self._record_instruction_delivery(
                connection, row, expected_version, int(value["sequence"]), now)
        return str(value["value"])

    @staticmethod
    def _record_instruction_delivery(
        connection: sqlite3.Connection,
        row: sqlite3.Row,
        workflow_version: int,
        instruction_sequence: int,
        now: str,
    ) -> None:
        """Note that this run was handed this instruction, exactly once.

        Handing the same run its instruction twice is one delivery, not two:
        a worker that rebuilds its payload after a retry must not produce a
        second event. `INSERT OR IGNORE` on the primary key makes the second
        attempt a no-op without a round trip to check first, which also makes
        it safe against two processes arriving together.
        """
        task_id = int(row["task_id"])
        cursor = connection.execute(
            "INSERT OR IGNORE INTO execution_reader_instruction_deliveries("
            "task_id,workflow_version,instruction_sequence,occurred_at) "
            "VALUES(?,?,?,?)",
            (task_id, workflow_version, instruction_sequence, now),
        )
        if cursor.rowcount:
            # The event says a handoff happened, never what was said. The
            # reader's words live in `execution_reader_inputs` and stay
            # there; an event stream is read in places a discussion is not.
            TaskExecutionService._event(
                connection, task_id, "reader_instruction_delivered",
                workflow_version, int(row["task_version"]),
                WorkflowPhase(row["phase"]), WorkflowStatus(row["status"]),
                now,
            )
        connection.commit()

    def delivered_reader_instruction_sequence(
        self, task_id: int, *, expected_version: int
    ) -> int | None:
        """The instruction this run was handed, for the result to name."""
        if not _valid_identity(task_id, expected_version):
            return None
        with closing(self._connect()) as connection:
            row = connection.execute(
                "SELECT instruction_sequence "
                "FROM execution_reader_instruction_deliveries "
                "WHERE task_id=? AND workflow_version=?",
                (task_id, expected_version),
            ).fetchone()
        return None if row is None else int(row["instruction_sequence"])

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
                "SUM(status='parked' AND "
                "last_failure_reason='context_exhausted') AS context_exhausted,"
                # Derived from the event log for the same reason the card is:
                # `failure_count` resets on park, so nothing live carries the
                # history across one.
                # Every outer column is spelled out. `e.phase=phase` reads
                # as the obvious correlation and is not one: the inner table
                # has a `phase` column too, SQLite resolves the bare name to
                # it, and the predicate quietly becomes a tautology.
                "SUM((SELECT count(*) FROM task_execution_events AS e "
                "     WHERE e.task_id=task_execution_workflows.task_id "
                "     AND e.kind='parked' "
                "     AND e.phase=task_execution_workflows.phase "
                "     AND e.task_version=task_execution_workflows.task_version"
                "    )>1 "
                "    AND NOT EXISTS(SELECT 1 FROM task_execution_results AS r "
                "     WHERE r.task_id=task_execution_workflows.task_id "
                "     AND r.phase=task_execution_workflows.phase "
                "     AND r.task_version="
                "         task_execution_workflows.task_version)"
                ") AS unproductive,"
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
                "context_exhausted", "unproductive", "completed", "cancelled",
            )
        ))

    def adopt_installed_revision(
        self, task_id: int, *, expected_version: int
    ) -> WorkflowOperationResult:
        """Rebind one workflow to the installed revision of its own profile.

        This is not :meth:`select_agent`. That answers "which agent should do
        this?" -- a choice between alternatives, fenced to before the work
        starts. This answers a different question: the workflow is pinned to a
        revision of the profile it already names, that revision is no longer
        the installed one, and the pin is keeping it on a budget the operator
        has replaced. The profile identity never changes here.

        Valid for any workflow that is not ``running``. Rebinding under a live
        claim is precisely what ADR 0024's fence protects -- an agent mid-run
        holds a lease sized by the revision it started under -- and that
        protection is unchanged.
        """
        if not _valid_identity(task_id, expected_version):
            return _refused(task_id, WorkflowRefusal.INVALID_ARGUMENT)
        stamp = self._clock_value()
        now = stamp.astimezone(timezone.utc).isoformat(timespec="seconds")
        with closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                result = self._adopt_one(
                    connection, task_id,
                    expected_version=expected_version, now=now,
                )
                if not result.accepted:
                    connection.rollback()
                    return result
                connection.commit()
                return result
            except Exception:
                connection.rollback()
                raise

    def _adopt_one(
        self,
        connection: sqlite3.Connection,
        task_id: int,
        *,
        expected_version: int,
        now: str,
    ) -> WorkflowOperationResult:
        """Apply one adoption inside the caller's transaction."""
        row = self._workflow_with_task(connection, task_id)
        refusal = _workflow_guard(row, expected_version, _ADOPTABLE_STATUSES)
        if refusal is None:
            refusal = _task_guard(row, int(row["task_version"]))
        if refusal is not None:
            return _refused_row(task_id, row, refusal)

        installed = self._profile_registry.get(row["agent_profile_id"])
        if installed is None:
            return _refused_row(
                task_id, row, WorkflowRefusal.AGENT_PROFILE_UNAVAILABLE
            )
        if row["phase"] not in installed.allowed_phases:
            # The installed revision cannot run this workflow's phase, so
            # adopting it would strand the work rather than unblock it.
            return _refused_row(
                task_id, row, WorkflowRefusal.AGENT_PROFILE_UNAVAILABLE
            )
        if row["agent_profile_revision"] == installed.revision:
            return _operation(row, WorkflowDisposition.UNCHANGED)

        version = expected_version + 1
        updated = connection.execute(
            "UPDATE task_execution_workflows SET version=?,"
            "agent_profile_revision=?,updated_at=? "
            "WHERE task_id=? AND version=? AND status!='running'",
            (version, installed.revision, now, task_id, expected_version),
        )
        if updated.rowcount != 1:
            return _refused_row(task_id, row, WorkflowRefusal.STALE_WORKFLOW)
        TaskExecutionService._event(
            connection,
            task_id,
            "agent_selected",
            version,
            int(row["task_version"]),
            WorkflowPhase(row["phase"]),
            WorkflowStatus(row["status"]),
            now,
        )
        return WorkflowOperationResult(
            WorkflowDisposition.APPLIED,
            task_id,
            version,
            WorkflowStatus(row["status"]),
            WorkflowPhase(row["phase"]),
            agent_profile_id=installed.profile_id,
            agent_profile_revision=installed.revision,
        )

    def adopt_installed_revisions(
        self, *, limit: int = 100, dry_run: bool = True
    ) -> ProfileAdoptionResult:
        """Rebind eligible workflows to their profile's installed revision.

        Bounded per pass and content-free in its report, because the operator
        case is a queue of hundreds: rebinding many workflows at once changes
        many versions and retires many cards, so a pass says how much it moved
        rather than which work it touched.

        Defaults to a dry run. The count an operator acts on and the act
        itself should not be the same keystroke.
        """
        if not isinstance(limit, int) or isinstance(limit, bool) or limit < 1:
            raise TaskLedgerError("adoption limit must be a positive integer")
        stamp = self._clock_value()
        now = stamp.astimezone(timezone.utc).isoformat(timespec="seconds")
        examined = adopted = narrowed = unknown = skipped_running = 0
        with closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                placeholders = ",".join("?" for _ in _ADOPTABLE_STATUSES)
                rows = connection.execute(
                    "SELECT w.task_id,w.version,w.status,w.phase,"
                    "w.agent_profile_id,w.agent_profile_revision "
                    "FROM task_execution_workflows AS w JOIN tasks AS t "
                    "ON t.id=w.task_id "
                    f"WHERE w.status IN ({placeholders}) "
                    "AND t.status='open' AND t.version=w.task_version "
                    "ORDER BY w.task_id",
                    tuple(s.value for s in _ADOPTABLE_STATUSES),
                ).fetchall()
                skipped_running = int(connection.execute(
                    "SELECT COUNT(*) FROM task_execution_workflows AS w "
                    "JOIN tasks AS t ON t.id=w.task_id "
                    "WHERE w.status='running' AND t.status='open'"
                ).fetchone()[0])

                for row in rows:
                    installed = self._profile_registry.get(
                        row["agent_profile_id"]
                    )
                    if installed is None:
                        continue
                    if row["agent_profile_revision"] == installed.revision:
                        continue
                    if row["phase"] not in installed.allowed_phases:
                        continue
                    examined += 1
                    if adopted >= limit:
                        continue
                    try:
                        retired = self._profile_registry.resolve(
                            row["agent_profile_id"],
                            row["agent_profile_revision"],
                        )
                    except AgentProfileError:
                        retired = None
                    if retired is None:
                        unknown += 1
                    elif (
                        installed.max_turns < retired.max_turns
                        or installed.timeout_seconds < retired.timeout_seconds
                    ):
                        narrowed += 1
                    if dry_run:
                        adopted += 1
                        continue
                    result = self._adopt_one(
                        connection, int(row["task_id"]),
                        expected_version=int(row["version"]), now=now,
                    )
                    if result.disposition is WorkflowDisposition.APPLIED:
                        adopted += 1
                if dry_run:
                    connection.rollback()
                else:
                    connection.commit()
            except Exception:
                connection.rollback()
                raise
        return ProfileAdoptionResult(
            examined=examined,
            adopted=adopted,
            remaining=max(examined - adopted, 0),
            narrowed=narrowed,
            skipped_running=skipped_running,
            unknown_budget=unknown,
            dry_run=dry_run,
        )

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
            try:
                self._profile_registry.resolve_current(
                    row["agent_profile_id"], row["agent_profile_revision"]
                )
                current = True
            except AgentProfileError:
                current = False
            health.append(ExecutionProfileHealth(
                agent_profile_id=str(row["agent_profile_id"]),
                agent_profile_revision=str(row["agent_profile_revision"]),
                workflows=int(row["workflows"]),
                ready=int(row["ready"] or 0),
                running=int(row["running"] or 0),
                parked=int(row["parked"] or 0),
                available=available,
                current=current,
            ))
        return tuple(health)

    def superseded_profile_revisions(
        self,
    ) -> tuple[ExecutionProfileHealth, ...]:
        """Health rows whose profile is installed under a newer revision.

        The answer to "how much of this queue is not getting the budget I
        installed?". Only rows whose profile ID is still offered appear: a
        profile the registry has dropped entirely is a different problem,
        reported by `available`, and conflating the two would hide it.
        """
        return tuple(
            row for row in self.profile_health()
            if not row.current
            and self._profile_registry.get(row.agent_profile_id) is not None
        )

    def failures_awaiting_digest(
        self, *, limit: int = 20
    ) -> tuple[PendingFailureDigest, ...]:
        """Failed attempts whose transcript has not been summarised yet.

        A workflow qualifies when its last attempt failed, that failure
        named a run, and no digest exists for the attempt. The attempt is
        the workflow version that was claimed -- one below the version the
        failure created, because `_defer_failure` increments it.

        Ordered oldest first so a backlog drains in the order it arrived
        rather than re-summarising whatever failed most recently.
        """
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 1_000:
            raise ValueError("failure digest limit is invalid")
        with closing(self._connect()) as connection:
            rows = connection.execute(
                "SELECT w.task_id,w.version,w.phase,w.last_failure_run_id,"
                "w.last_failure_reason,w.last_failure_at "
                "FROM task_execution_workflows AS w "
                "JOIN tasks AS t ON t.id=w.task_id "
                "WHERE t.status='open' AND w.last_failure_run_id IS NOT NULL "
                "AND w.version>1 AND NOT EXISTS("
                " SELECT 1 FROM execution_failure_digests AS d "
                " WHERE d.task_id=w.task_id AND d.workflow_version=w.version-1"
                ") ORDER BY w.last_failure_at,w.task_id LIMIT ?",
                (limit,),
            ).fetchall()
        return tuple(
            PendingFailureDigest(
                task_id=int(row["task_id"]),
                # The attempt that failed, not the version its failure
                # created. Keyed on the wrong one, a digest would describe
                # a run nobody can find and the next failure would look
                # already summarised.
                workflow_version=int(row["version"]) - 1,
                phase=WorkflowPhase(row["phase"]),
                run_id=str(row["last_failure_run_id"]),
                reason=row["last_failure_reason"],
            )
            for row in rows
        )

    def record_failure_digest(
        self,
        task_id: int,
        *,
        workflow_version: int,
        phase: WorkflowPhase | str,
        run_id: str | None,
        digest: str,
    ) -> bool:
        """Store one attempt's failure digest, once.

        Returns whether a row was written. A second attempt for the same
        (task, attempt) is a no-op rather than an error: the background pass
        is re-runnable by design, and two passes arriving together must not
        make one of them fail.
        """
        if not _valid_identity(task_id, workflow_version):
            return False
        text = digest.strip() if isinstance(digest, str) else ""
        if not text or len(text) > MAX_FAILURE_DIGEST_CHARS:
            return False
        if run_id is not None and not _RUN_ID_RE.fullmatch(str(run_id)):
            return False
        phase_value = WorkflowPhase(phase).value
        now = self._now()
        with closing(self._connect()) as connection:
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute("BEGIN IMMEDIATE")
            try:
                cursor = connection.execute(
                    "INSERT OR IGNORE INTO execution_failure_digests("
                    "task_id,workflow_version,phase,run_id,digest,created_at) "
                    "VALUES(?,?,?,?,?,?)",
                    (task_id, workflow_version, phase_value, run_id, text, now),
                )
                connection.commit()
            except Exception:
                connection.rollback()
                raise
        return bool(cursor.rowcount)

    def failure_digest(
        self, task_id: int, *, phase: WorkflowPhase | str | None = None
    ) -> str | None:
        """The most recent failure digest for this task's current phase.

        Scoped to a phase because a failure in `execute` says nothing about
        a `plan` pass that succeeded, and showing a reader the wrong
        phase's cause is worse than showing none.
        """
        if isinstance(task_id, bool) or not isinstance(task_id, int) or task_id < 1:
            return None
        with closing(self._connect()) as connection:
            if phase is None:
                row = connection.execute(
                    "SELECT phase FROM task_execution_workflows WHERE task_id=?",
                    (task_id,),
                ).fetchone()
                if row is None:
                    return None
                phase_value = str(row["phase"])
            else:
                phase_value = WorkflowPhase(phase).value
            found = connection.execute(
                "SELECT digest FROM execution_failure_digests "
                "WHERE task_id=? AND phase=? "
                "ORDER BY workflow_version DESC LIMIT 1",
                (task_id, phase_value),
            ).fetchone()
        return None if found is None else str(found["digest"])

    #: How many prior failure digests one run is told about.  An agent
    #: handed twenty failure notes is worse off than one handed two: the
    #: point is "these approaches have already been tried", and past a
    #: handful that message is already delivered while the context is not.
    PRIOR_FAILURE_LIMIT = 3

    def prior_failures(
        self,
        task_id: int,
        *,
        expected_version: int,
        claim_token: str,
        limit: int | None = None,
    ) -> tuple[str, ...]:
        """Why earlier attempts at this phase stopped, most recent first.

        The larger half of what a failed run loses.  A reader seeing the
        cause on a card is the visible half; the next run beginning from
        the task text alone, making the same plan and failing the same
        way, is the expensive one.

        Claim-guarded exactly as ``reader_instruction`` is: this is run
        context, and only the run it belongs to may read it.

        Scoped to the phase the workflow is in now, and excluding the
        current attempt, which has not failed yet.  Failing open is the
        rule everywhere in this path -- an absent digest costs the hint
        and nothing else -- so an empty tuple is an ordinary answer.
        """
        if (
            not _valid_identity(task_id, expected_version)
            or not _valid_secret(claim_token)
        ):
            raise TaskLedgerError("execution claim is unavailable")
        if limit is None:
            limit = self.PRIOR_FAILURE_LIMIT
        if isinstance(limit, bool) or not isinstance(limit, int) or limit < 1:
            raise TaskLedgerError("prior failure limit is invalid")
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
            found = connection.execute(
                "SELECT digest FROM execution_failure_digests "
                "WHERE task_id=? AND phase=? AND workflow_version<? "
                "ORDER BY workflow_version DESC LIMIT ?",
                (task_id, str(row["phase"]), expected_version, limit),
            ).fetchall()
        return tuple(str(item["digest"]) for item in found)

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

    def _reconcile_granted_review_gates(
        self, connection: sqlite3.Connection, now: str
    ) -> int:
        """Advance old review gates now covered by configured grants.

        A policy change is a machine decision, not a reader approval.  Each
        update therefore increments the workflow version and emits
        ``phase_granted``.  Existing cards retain their old version and are
        retired by the card scheduler's ordinary stale-card pass.
        """
        rows = connection.execute(
            "SELECT w.*,r.outcome,("
            " SELECT o.source_kind FROM task_candidate_bindings AS b "
            " JOIN candidate_inbox AS o ON o.candidate_id=b.candidate_id "
            " WHERE b.task_id=t.id AND b.relation='accepted'"
            ") AS origin_kind FROM task_execution_workflows AS w "
            "JOIN tasks AS t ON t.id=w.task_id "
            "JOIN task_execution_results AS r "
            "ON r.result_id=w.last_result_id AND r.task_id=w.task_id "
            "WHERE w.status='awaiting_review' "
            "AND w.phase IN ('plan','execute') "
            "AND t.status='open' AND t.version=w.task_version "
            "AND NOT EXISTS("
            " SELECT 1 FROM task_candidate_bindings AS blocked JOIN "
            " task_candidate_lifecycle AS l "
            " ON l.candidate_id=blocked.candidate_id "
            " WHERE blocked.task_id=t.id AND blocked.relation='accepted' "
            " AND l.state='withdrawn' "
            " AND l.resolution='preserved_open'"
            f") ORDER BY {_SOURCE_QUEUE_ORDER_SQL}w.task_id"
        ).fetchall()
        granted = 0
        for row in rows:
            phase = WorkflowPhase(row["phase"])
            target = _granted_advance(
                phase,
                ExecutionOutcome(row["outcome"]),
                row["origin_kind"],
                self._execution_grants,
                self._action_grants,
            )
            if target is None:
                continue
            version = int(row["version"]) + 1
            profile = self._profile_for(row["origin_kind"])
            updated = connection.execute(
                "UPDATE task_execution_workflows SET status='queued',"
                "phase=?,version=?,due_at=NULL,claim_token_digest=NULL,"
                "claimed_at=NULL,claim_heartbeat_at=NULL,"
                "claim_expires_at=NULL,failure_count=0,"
                "current_run_id=NULL,"
                "last_failure_reason=NULL,last_failure_exit_code=NULL,"
                "last_failure_run_id=NULL,last_failure_at=NULL,"
                "next_attempt_at=NULL,parked_at=NULL,agent_profile_id=?,"
                "agent_profile_revision=?,updated_at=?,completed_at=NULL "
                "WHERE task_id=? AND task_version=? AND version=? "
                "AND status='awaiting_review' AND phase=?",
                (
                    target.value, version, profile.profile_id,
                    profile.revision, now, int(row["task_id"]),
                    int(row["task_version"]), int(row["version"]),
                    phase.value,
                ),
            )
            if updated.rowcount != 1:
                continue
            self._event(
                connection, int(row["task_id"]), "phase_granted", version,
                int(row["task_version"]), target, WorkflowStatus.QUEUED,
                now,
            )
            granted += 1
        return granted

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
                    "current_run_id=NULL,"
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
        exit_code: int | None = None,
        run_id: str | None = None,
    ) -> WorkflowOperationResult:
        if reason not in FAILURE_REASONS:
            raise ValueError("execution failure reason is invalid")
        now = stamp.isoformat(timespec="seconds")
        failures = int(row["failure_count"]) + 1
        version = int(row["version"]) + 1
        if reason == "context_exhausted":
            # This is an explicit refusal from the runtime's context filter,
            # not a slow run. The next attempt would re-read the same material
            # and exceed the same measured ceiling, so it must wait for a
            # reader to reduce or split the work instead of cycling overnight.
            status = WorkflowStatus.PARKED
            next_attempt = None
            parked = now
            kind = "parked"
        elif failures >= self._max_attempts:
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
            "current_run_id=NULL,"
            "failure_count=?,last_failure_reason=?,last_failure_at=?,"
            "last_failure_exit_code=?,last_failure_run_id=?,"
            "next_attempt_at=?,parked_at=?,updated_at=? "
            "WHERE task_id=? AND version=?",
            (
                status, version, failures, reason, now, exit_code, run_id,
                next_attempt, parked, now, int(row["task_id"]),
                int(row["version"]),
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
            "current_task_version,("
            " SELECT o.source_kind FROM task_candidate_bindings AS b "
            " JOIN candidate_inbox AS o ON o.candidate_id=b.candidate_id "
            " WHERE b.task_id=t.id AND b.relation='accepted'"
            ") AS origin_kind FROM task_execution_workflows AS w "
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
        "current_run_id=NULL,"
        # Restarting clears what parked it, so a retry gets a full set of
        # attempts rather than immediately parking again on the next slip.
        "failure_count=0,last_failure_reason=NULL,"
        "last_failure_exit_code=NULL,last_failure_run_id=NULL,"
        "last_failure_at=NULL,"
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
        "current_run_id=NULL,"
        "last_failure_reason=NULL,last_failure_exit_code=NULL,"
        "last_failure_run_id=NULL,last_failure_at=NULL,next_attempt_at=NULL,"
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


def _workflow_board_status(status: WorkflowStatus, phase: WorkflowPhase) -> str:
    if status is WorkflowStatus.AWAITING_REVIEW:
        return {
            WorkflowPhase.PLAN: "plan_review",
            WorkflowPhase.EXTERNAL_ACTION: "external_review",
            WorkflowPhase.EXECUTE: "result_review",
        }[phase]
    return {
        WorkflowStatus.AWAITING_START: "ready_to_start",
        WorkflowStatus.QUEUED: "queued",
        WorkflowStatus.RUNNING: "running",
        WorkflowStatus.SNOOZED: "snoozed",
        WorkflowStatus.PARKED: "parked",
    }[status]


#: Bound for the recorded run body where it is projected to a reader. Larger
#: than the derived fields beside it because it is the whole account of a run
#: rather than a sentence about one, and small enough that a detail read stays
#: one bounded response.
WORK_BODY_PROJECTION_MAX = 16_000


def _board_text(value: object, maximum: int) -> str:
    if not isinstance(value, str):
        return ""
    return value[:maximum]


def _board_deliverables(value: object) -> tuple[Mapping[str, str], ...]:
    if not isinstance(value, str):
        return ()
    try:
        records = json.loads(value)
    except (TypeError, ValueError):
        return ()
    if not isinstance(records, list) or len(records) > MAX_COLLECTION_ITEMS:
        return ()
    projected: list[Mapping[str, str]] = []
    for record in records:
        if not isinstance(record, Mapping):
            return ()
        text = record.get("body", record.get("text"))
        if not isinstance(text, str):
            return ()
        item = {"text": _board_text(text, 3_000)}
        for key in ("label", "recipient", "subject"):
            candidate = record.get(key, "")
            if not isinstance(candidate, str):
                return ()
            item[key] = _board_text(candidate, 300)
        projected.append(item)
    return tuple(projected)


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
            last_failure_exit_code=row["last_failure_exit_code"],
            last_failure_run_id=row["last_failure_run_id"],
            steer_while_running=bool(row["steer_while_running"]),
            current_run_id=row["current_run_id"],
            last_failure_at=row["last_failure_at"],
            next_attempt_at=row["next_attempt_at"],
            parked_at=row["parked_at"],
            last_result_id=row["last_result_id"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            completed_at=row["completed_at"],
            agent_profile_id=profile_id,
            agent_profile_revision=profile_revision,
            priority=WorkflowPriority(row["queue_priority"]),
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
        priority=WorkflowPriority(row["queue_priority"]),
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


#: Each gate a machine may stand down: the result that asks for approval,
#: and the phase approving it would open. Held as data so adding a gate is a
#: row rather than another branch, and so the two grants stay visibly
#: parallel — neither is a special case of the other.
_GRANTED_ADVANCES = {
    (WorkflowPhase.PLAN, ExecutionOutcome.AWAITING_PLAN):
        WorkflowPhase.EXECUTE,
    (WorkflowPhase.EXECUTE, ExecutionOutcome.AWAITING_EXTERNAL):
        WorkflowPhase.EXTERNAL_ACTION,
}


def _granted_advance(
    phase: WorkflowPhase,
    outcome: ExecutionOutcome,
    origin_kind: object,
    execution_granted: frozenset[str],
    action_granted: frozenset[str],
) -> WorkflowPhase | None:
    """The phase a standing grant moves this result to, or None to ask.

    A result asks for approval by returning `awaiting_plan` or
    `awaiting_external`. For a source whose enrolment already settled that
    question, the card it would raise has one plausible answer, and a queue
    of such cards costs the reader the attention that the cards needing a
    decision were meant to get.

    Only those two transitions are granted. `completed`, `declined` and
    `ineligible` end the work and are the reader's to see; nothing about a
    grant should hide a result.

    Each gate reads its own grant, so granting one never implies the other
    in either direction.
    """
    target = _GRANTED_ADVANCES.get((phase, outcome))
    if target is None:
        return None
    granted = (
        execution_granted if phase is WorkflowPhase.PLAN else action_granted
    )
    if not isinstance(origin_kind, str) or origin_kind not in granted:
        return None
    return target


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
    repository_references = _repository_references(
        envelope.repository_references)
    artifacts = _artifact_records(envelope.artifacts)
    if not isinstance(envelope.repository_impact, bool):
        raise ValueError("execution result repository impact is invalid")
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
    instruction = _result_instruction(envelope.reader_instruction_sequence)
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
        "repository_references": repository_references,
        "repository_impact": envelope.repository_impact,
        "task_work_directory": task_work_directory,
        "task_kb_file": task_kb_file,
        "artifacts": artifacts,
    }
    raw = _canonical_json(document).encode("utf-8")
    if len(raw) > MAX_RESULT_BYTES:
        raise ValueError("execution result is too large")
    digest = hashlib.sha256(raw).hexdigest()
    if not _DIGEST_RE.fullmatch(digest):
        raise ValueError("execution result digest is invalid")
    return {
        **document,
        # Provenance, not content. A result that says the same thing is the
        # same answer whether or not a reader prompted it, so the digest --
        # which exists to recognise a repeat -- must not move because the
        # instruction did.
        "reader_instruction_sequence": instruction,
        "work_digest": work_digest or None,
        "content_digest": digest,
        "artifacts": artifacts,
        "questions_json": _canonical_json(questions),
        "external_actions_json": _canonical_json(actions),
        "deliverables_json": _canonical_json(deliverables),
        "repository_references_json": _canonical_json(repository_references),
        "claim_token": envelope.claim_token,
    }


#: The fields that are the agent's answer. Everything else on a result is
#: bookkeeping: identifiers, the phase, the locations it was authored in, and
#: the queue blurb.
#:
#: `questions` is deliberately absent. An agent re-asks a different set almost
#: every pass, and letting a changed question list prove the answer changed is
#: what a repeated report already looks like in practice: the same prose,
#: recorded three passes running, under four distinct content digests.
#:
#: `outcome` is deliberately present. The same prose recorded as `completed`
#: rather than `awaiting_plan` is a different answer -- the difference between
#: proposing something and declaring it done -- and excluding it made a
#: terminal outcome impossible to record over unchanged text.
#:
#: The deliverables, external actions and repository references are present
#: because a correction that attaches the deliverable it forgot HAS changed
#: its answer. Excluding them taught an agent to pad its wording to get past
#: the guard, and told a reader "same answer" above an effect list that had
#: changed completely.
_ANSWER_COLUMNS = (
    "outcome",
    "summary",
    "work_markdown",
    "external_actions_json",
    "deliverables_json",
    "repository_references_json",
    "repository_impact",
)


def _cycle_started_at(connection: sqlite3.Connection, task_id: int) -> int:
    """The workflow version this task was most recently scheduled at.

    A task that is cancelled and scheduled again, or dropped back into the
    queue, begins a new cycle. Results from the cycle before it describe work
    the reader already saw and closed, and comparing against them would refuse
    a new cycle's opening result -- forever, since every retry would be refused
    identically, until the claim expired and the workflow parked.
    """
    row = connection.execute(
        "SELECT workflow_version FROM task_execution_events "
        "WHERE task_id=? AND kind='scheduled' "
        "ORDER BY sequence DESC LIMIT 1",
        (task_id,),
    ).fetchone()
    return 0 if row is None else int(row["workflow_version"])


def _repeats_an_earlier_answer(
    connection: sqlite3.Connection, result: Mapping[str, object]
) -> bool:
    """Whether this result says exactly what an earlier one in this cycle said.

    Compared against every prior result in the cycle rather than only the most
    recent. A pass that reproduces the one before last is the same failure as
    one that reproduces the last, and checking only the newest let an X, Y, X
    sequence through -- which is the shape the reports that prompted this
    actually had.

    Scoped to the same `task_version`, so editing the task always opens a
    clean slate, and to the same phase, so planning and executing are never
    held against each other.
    """
    predicate = " AND ".join(f"{column}=?" for column in _ANSWER_COLUMNS)
    row = connection.execute(
        "SELECT 1 FROM task_execution_results "
        "WHERE task_id=? AND task_version=? AND phase=? "
        "AND workflow_version>=? "
        f"AND {predicate} LIMIT 1",
        (
            result["task_id"], result["task_version"], result["phase"],
            _cycle_started_at(connection, int(result["task_id"])),
            *(result[column] for column in _ANSWER_COLUMNS),
        ),
    ).fetchone()
    return row is not None


def _result_instruction(value: object) -> int | None:
    """The reader-instruction sequence a result may name, if it names one."""
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError("execution result reader instruction is invalid")
    return value


def _artifact_records(value: object) -> list[dict[str, object]]:
    if (
        not isinstance(value, Sequence)
        or isinstance(value, (str, bytes))
        or len(value) > 100
    ):
        raise ValueError("execution result artifacts are invalid")
    records: list[dict[str, object]] = []
    paths: set[str] = set()
    for item in value:
        if not isinstance(item, Mapping) or set(item) != {
            "relative_path", "name", "size_bytes", "content_digest",
            "run_directory",
        }:
            raise ValueError("execution result artifacts are invalid")
        path = item["relative_path"]
        name = item["name"]
        size = item["size_bytes"]
        digest = item["content_digest"]
        run = item["run_directory"]
        path_parts = path.split("/") if isinstance(path, str) else ()
        if (
            not isinstance(path, str)
            or not path
            or path.startswith("/")
            or "\\" in path
            or any(part in {"", ".", ".."} for part in path_parts)
            or Path(path).name != name
            or path in paths
            or not isinstance(name, str)
            or not name
            or name in {".", ".."}
            or "/" in name
            or "\\" in name
            or isinstance(size, bool)
            or not isinstance(size, int)
            or not 0 <= size <= 2 * 1024 * 1024
            or not isinstance(digest, str)
            or not _DIGEST_RE.fullmatch(digest)
            or _result_path(run, "artifact run directory") is None
        ):
            raise ValueError("execution result artifacts are invalid")
        paths.add(path)
        records.append({
            "relative_path": path,
            "name": name,
            "size_bytes": size,
            "content_digest": digest,
            "run_directory": run,
        })
    return records


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


def _cancel_superseded_start_cards(
    connection: sqlite3.Connection,
    *,
    task_id: int,
    workflow_version: int,
    now: str,
) -> None:
    """Retire Start cards in the same transaction as a planning grant."""
    rows = connection.execute(
        "SELECT id,version,transport,delivery_ref FROM execution_review_cards "
        "WHERE task_id=? AND workflow_version=? AND kind='start' "
        "AND status IN ('pending','delivering','delivered') ORDER BY id",
        (task_id, workflow_version),
    ).fetchall()
    for row in rows:
        card_id = int(row["id"])
        card_version = int(row["version"]) + 1
        updated = connection.execute(
            "UPDATE execution_review_cards SET status='cancelled',"
            "version=?,claim_token_digest=NULL,claim_expires_at=NULL,"
            "superseded_delivery_ref=delivery_ref,"
            "superseded_transport=transport,transport=NULL,delivery_ref=NULL,"
            "resolved_at=?,updated_at=? WHERE id=? AND version=? "
            "AND status IN ('pending','delivering','delivered')",
            (card_version, now, now, card_id, int(row["version"])),
        )
        if updated.rowcount != 1:
            raise TaskLedgerError("execution card state changed")
        if row["transport"] is not None and row["delivery_ref"] is not None:
            connection.execute(
                "INSERT OR IGNORE INTO execution_card_retractions("
                "card_id,transport,delivery_ref,state,created_at,updated_at) "
                "VALUES(?,?,?,'pending',?,?)",
                (card_id, row["transport"], row["delivery_ref"], now, now),
            )
        connection.execute(
            "INSERT INTO execution_review_card_events("
            "card_id,task_id,kind,card_version,workflow_version,action,"
            "occurred_at) VALUES(?,?,'cancelled',?,?,NULL,?)",
            (card_id, task_id, card_version, workflow_version, now),
        )


#: Owner columns every selection feeding `_initial_status` must carry.
_OWNER_COLUMNS = (
    "t.owner,t.owner_kind,t.owner_ref_version,t.owner_provisional,"
)


def _initial_status(
    origin_kind: object,
    granted: frozenset[str],
    row: Mapping[str, object] | None = None,
    reader_aliases: frozenset[str] = frozenset(),
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
    if row is not None and reader_owned(row, reader_aliases):
        return WorkflowStatus.QUEUED
    return WorkflowStatus.AWAITING_START


def _initial_phase(
    origin_kind: object, skip_planning_for: frozenset[str]
) -> WorkflowPhase:
    """Choose the first phase for a newly-created workflow only."""
    if isinstance(origin_kind, str) and origin_kind in skip_planning_for:
        return WorkflowPhase.EXECUTE
    return WorkflowPhase.PLAN


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
_ACTION_FIELDS = ("requires", "channel", "target")
_DELIVERABLE_FIELDS = ("label", "recipient", "subject")
_ACTION_ALIASES = ("action", "title", "text")
_DELIVERABLE_ALIASES = ("body", "text")
_REPOSITORY_REFERENCE_PATTERNS = {
    "pull-request": re.compile(
        r"^https://github\.com/[^/\s]+/[^/\s]+/pull/[1-9][0-9]*$"),
    "commit": re.compile(
        r"^https://github\.com/[^/\s]+/[^/\s]+/commit/[0-9a-fA-F]{7,64}$"),
    "check": re.compile(
        r"^https://github\.com/[^/\s]+/[^/\s]+/actions/runs/[1-9][0-9]*"
        r"(?:/job/[1-9][0-9]*)?$")
}


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
            allowed = sorted(set(aliases) | set(optional))
            bad_fields = ", ".join(sorted(unknown))
            raise ValueError(
                f"execution result {label} contain unsupported fields: "
                f"{bad_fields}; allowed fields are {', '.join(allowed)}"
            )
        records.append(record)
    return tuple(records)


def _repository_references(value: object) -> tuple[dict[str, str], ...]:
    """Validate the small forge-evidence vocabulary cards can render.

    The origin remains the source of the repository and issue/review link.
    These are the additional, independently addressable records produced by
    implementation: a pull request, commit, or check run.  Free-form prose
    still belongs in deliverables; accepting it here would make a card infer
    links again and recreate the ambiguity this field removes.
    """
    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
        raise ValueError("execution result repository references are invalid")
    items = tuple(value)
    if len(items) > MAX_REPOSITORY_REFERENCES:
        raise ValueError("execution result repository references are invalid")
    references: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for item in items:
        if not isinstance(item, dict) or set(item) != {"kind", "url"}:
            raise ValueError("execution result repository references are invalid")
        kind, url = item.get("kind"), item.get("url")
        pattern = _REPOSITORY_REFERENCE_PATTERNS.get(kind)
        if not isinstance(url, str) or pattern is None or not pattern.fullmatch(url):
            raise ValueError("execution result repository references are invalid")
        reference = (kind, url)
        if reference not in seen:
            seen.add(reference)
            references.append({"kind": kind, "url": url})
    return tuple(references)


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
