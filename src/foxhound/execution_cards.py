"""Transport-neutral reader cards for Foxhound execution gates."""

from __future__ import annotations

import hashlib
import hmac
import html
import json
import os
import re
import secrets
import sqlite3
import stat
import urllib.parse
from contextlib import closing
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import StrEnum
from pathlib import Path
from typing import Callable, Mapping, Sequence

from . import task_relations
from .agent_profiles import (
    AgentProfile,
    AgentProfileError,
    AgentProfileRegistry,
    load_registry,
)
from .candidate_inbox import CandidateInbox, InboxError, SCHEMA_VERSION
from .card_provenance import (
    ADDRESSABLE_ORIGINS,
    CardSourceEvidence,
    origin_lines as shared_origin_lines,
    origin_url,
    stored_origin_sources,
)
from .knowledge_client import KnowledgeClientError, OwnerUpcomingMeeting
from .task_execution import (
    ExecutionOutcome,
    REVIEW_SNOOZE_ACTIONS,
    TaskExecutionService,
    WorkflowDisposition,
    WorkflowOperationResult,
    WorkflowPhase,
    WorkflowRefusal,
    WorkflowStatus,
    _ANSWER_COLUMNS,
    _apply_agent_selection,
    _apply_plan_review_agent_selection,
    _apply_review_action,
    _apply_start_action,
)
from .task_ledger import (
    TaskLedgerError,
    TaskStatus,
    TransitionDisposition,
    _apply_task_transition,
)
from .task_archive import MAX_ARTIFACT_BYTES, review_links
from .task_owner import canonical_owner_display, normalized_owner


CALLBACK_PREFIX = "fhe"
AGENT_CALLBACK_PREFIX = "fha"
CALLBACK_DATA_LIMIT = 64
AGENT_SELECTION_TOKEN_CHARS = 20
MAX_REVISION_NOTE_CHARS = 400

#: How much of an agent's plan or work a card may carry.
#:
#: A card is a glance with buttons under it. The whole text already lives in
#: `result-work.md` in the task working folder and in the KB task file, both
#: of which the card names a few lines above this block, so putting it on the
#: card a second time buys nothing and costs the only thing the card has --
#: the reader reaching the buttons. One real plan-review card ran to 12,057
#: characters over 224 lines, of which the plan was 79%.
#:
#: Bounded rather than dropped: the card asks the reader to approve a plan,
#: and a plan nobody can see is not one anybody can approve. The opening is
#: what states the approach; the rest is the working.
MAX_WORK_EXCERPT_CHARS = 1_200

#: How much of one prepared draft a card may carry. A deliverable is shown
#: so it can be judged rather than named, so this is generous; it exists to
#: stop a single attached document from becoming the card.
MAX_DELIVERABLE_BODY_CHARS = 3_000

#: How many records an ADVISORY list may show. "Potential external actions"
#: and the deliverables of a plan or a result describe work, and the fourth
#: one rarely changes the answer. Never applied to the list on an external
#: review: that card asks the reader to authorise those exact effects, and
#: hiding one would make the sentence above it false.
MAX_ADVISORY_RECORDS = 3

MAX_CARD_BODY_BYTES = 24 * 1024
MAX_RENDER_SOURCE_LINE_CHARS = 500
MAX_TRUNCATED_CARD_BODY_BYTES = 3_500
ACTIVE_STATUSES = ("pending", "delivering", "delivered")
REVIEW_DIRECT_ACTIONS = {
    "approve", "revise", "cancel", "done", "drop", "snooze",
    *REVIEW_SNOOZE_ACTIONS,
}
#: One control, not four. The intervals live behind it: a gateway rewrites
#: this verb into its own picker and offers the same four choices there. Four
#: rows of deferral on the keyboard made every card argue for putting itself
#: off, and on a start gate they outnumbered the two controls that answer the
#: question the gate asks.
#:
#: The interval each choice resolves to is decided here, in
#: `_calendar_snooze_until`; a picker only names them. The verb stays plain
#: `snooze` because that is what the gateway looks for.
SNOOZE_BUTTON_ROW = (("🕓 Snooze", "snooze"),)
READER_INPUT_KINDS = {"discussion", "reassignment"}
MAX_DISCUSSION_CHARS = 16_000
#: Matches the storage bound in `failure_digest`. Stated here too so the
#: renderer cannot be surprised by a longer row.
MAX_FAILURE_DIGEST_CARD_CHARS = 800
MAX_OWNER_CHARS = 200
OWNER_HOLD_INTERVAL = timedelta(days=21)
MAX_OWNER_HOLD_CHECKS = 100
OWNER_HOLD_ACTION = "until_meeting"
_OWNER_HOLD_SELECT = (
    "SELECT h.*,t.status AS task_status_current,"
    "t.version AS task_version_current,t.owner AS owner_current,"
    "t.owner_ref_version AS owner_ref_version_current,"
    "t.owner_kind AS owner_kind_current,"
    "t.owner_speaker_id AS owner_speaker_id_current,"
    "t.owner_canonical_speaker_id AS owner_canonical_speaker_id_current,"
    "t.owner_speaker_registry_id AS owner_speaker_registry_id_current,"
    "t.owner_pinned AS owner_pinned_current,"
    "t.owner_provisional AS owner_provisional_current,"
    "w.status AS workflow_status_current,"
    "w.version AS workflow_version_current,"
    "w.task_version AS workflow_task_version_current,"
    "w.due_at AS workflow_due_at "
    "FROM task_execution_owner_holds AS h "
    "JOIN tasks AS t ON t.id=h.task_id "
    "JOIN task_execution_workflows AS w ON w.task_id=h.task_id"
)


class ExecutionCardKind(StrEnum):
    START = "start"
    PLAN_REVIEW = "plan_review"
    EXTERNAL_REVIEW = "external_review"
    RESULT_REVIEW = "result_review"
    STEER = "steer"


# Closed work-state vocabulary for the execution board.  It is intentionally
# separate from the card delivery states below.
EXECUTION_BOARD_STATUSES = (
    "ready_to_start", "queued", "running", "plan_review",
    "external_review", "result_review", "snoozed", "parked",
    "completed", "cancelled",
)


class ExecutionCardStatus(StrEnum):
    PENDING = "pending"
    DELIVERING = "delivering"
    DELIVERED = "delivered"
    RESOLVED = "resolved"
    CANCELLED = "cancelled"


class ExecutionCardDisposition(StrEnum):
    APPLIED = "applied"
    UNCHANGED = "unchanged"
    REFUSED = "refused"


class ExecutionCardRefusal(StrEnum):
    INVALID_ARGUMENT = "invalid_argument"
    INVALID_ACTION = "invalid_action"
    NOT_FOUND = "not_found"
    STALE_VERSION = "stale_version"
    INVALID_STATE = "invalid_state"
    CLAIM_MISMATCH = "claim_mismatch"
    #: This deployment serves no result artifacts at all, whatever the card
    #: is. Distinct from an empty listing, which is an answer: this one says
    #: the question cannot be answered here, and a caller that can tell them
    #: apart can say so to a reader instead of leaving them waiting for a
    #: file that is not coming.
    ARTIFACTS_UNAVAILABLE = "artifacts_unavailable"


@dataclass(frozen=True)
class ExecutionCardScheduleResult:
    disposition: ExecutionCardDisposition
    created: int = 0
    cancelled: int = 0
    refusal: ExecutionCardRefusal | None = None


@dataclass(frozen=True)
class ExecutionCardRequeueResult:
    """A bounded re-presentation pass over unanswered delivered cards."""

    requeued: int = 0


@dataclass(frozen=True)
class ExecutionCardStats:
    pending: int
    delivering: int
    delivered: int
    active: int
    steer_pending: int = 0
    steer_delivering: int = 0
    steer_delivered: int = 0


@dataclass(frozen=True)
class ExecutionCardScopedStats:
    pending: int
    delivering: int
    delivered: int
    elsewhere: int
    active: int
    steer_pending: int = 0
    steer_delivering: int = 0
    steer_delivered: int = 0


@dataclass(frozen=True)
class SteerDigestWorkItem:
    card_id: int
    card_version: int
    task_id: int
    workflow_version: int
    phase: WorkflowPhase
    run_id: str


@dataclass(frozen=True)
class ExecutionBoard:
    """Current reader-visible execution cards and true per-column totals."""

    cards: tuple["ExecutionReviewCard", ...]
    totals: Mapping[str, int]


@dataclass(frozen=True)
class ClaimAtCeiling:
    held_count: int
    ceiling: int


# Steer cards normally disappear without a reader action.  They therefore
# never share the scarce decision-card allowance: a cluster of slow runs must
# not prevent an approval or result from being shown.
EXECUTION_CARD_CLAIM_CEILINGS = {
    "queue_view": 2, "drip": 20, "queue_view_steer": 3, "drip_steer": 5,
}
EXECUTION_CARD_RELEASE_REASONS = frozenset({"surface_full", "client_rejected"})
BOARD_CARD_LIMIT = 100
STEER_DIGEST_REFRESH_INTERVAL = timedelta(minutes=30)
STEER_DIGEST_MAX_ATTEMPTS = 3


@dataclass(frozen=True)
class ExecutionReviewCard:
    id: int
    task_id: int
    task_version: int
    workflow_version: int
    work_revision_id: int | None
    kind: ExecutionCardKind
    phase: WorkflowPhase
    result_id: str | None
    status: ExecutionCardStatus
    version: int
    created_at: str
    workflow_status: WorkflowStatus
    agent_profile_id: str
    agent_profile_revision: str = field(repr=False)
    agent_display_name: str = field(repr=False)
    task_text: str = field(repr=False)
    owner: str | None = field(repr=False)
    due: str | None = field(repr=False)
    first_raised: str | None = field(repr=False)
    last_mentioned: str | None = field(repr=False)
    owner_hold_eligible: bool = field(default=False, repr=False)
    owner_hold_reason: str = field(default="", repr=False)
    summary: str = field(default="", repr=False)
    work_markdown: str = field(default="", repr=False)
    #: A few sentences derived from `work_markdown`; empty when the
    #: worker could not produce one, which the card must survive.
    work_digest: str = field(default="", repr=False)
    steer_digest: str = field(default="", repr=False)
    claimed_at: str | None = field(default=None, repr=False)
    questions: tuple[str, ...] = field(default=(), repr=False)
    external_actions: tuple[CardRecord, ...] = field(default=(), repr=False)
    deliverables: tuple[CardRecord, ...] = field(default=(), repr=False)
    repository_references: tuple["RepositoryReference", ...] = field(
        default=(), repr=False)
    revisions: int = 0
    revision_note: str = field(default="", repr=False)
    #: True only when this result says exactly what the preceding result in
    #: its phase said, by the same definition the ledger refuses on.
    unchanged_from_previous: bool = False
    origin_kind: str = field(default="", repr=False)
    origin_record: str = field(default="", repr=False)
    origin_item: str = field(default="", repr=False)
    prior_task_id: int | None = None
    #: Recorded relations to other tasks, live ones only.
    relations: tuple["CardRelation", ...] = field(
        default_factory=tuple)
    #: How a parked workflow got there. A reader told only that nothing
    #: happened cannot tell a task nobody reached from one that was
    #: abandoned.
    failure_count: int = 0
    #: Attempts, parks, recorded results and agent seconds for the phase
    #: this workflow is in now, across every park.  `failure_count` above
    #: is the live counter, which resets on park; these do not.
    phase_attempts: int = 0
    phase_parks: int = 0
    phase_results: int = 0
    phase_seconds: int = 0
    failure_reason: str = field(default="", repr=False)
    failure_exit_code: int | None = field(default=None, repr=False)
    failure_run_id: str | None = field(default=None, repr=False)
    #: Why the last attempt stopped, in the summariser's words. A reason
    #: and an exit code say how the process ended, which cannot tell an
    #: exhausted turn budget from a saturated backend from a refused
    #: worker operation -- and those need different answers from the
    #: reader. Empty is normal and common: the summary is derived from a
    #: remote model and the card must read correctly without it.
    failure_digest: str = field(default="", repr=False)
    task_work_directory: str = field(default="", repr=False)
    task_kb_file: str = field(default="", repr=False)
    origin_sources: tuple["CardSourceEvidence", ...] = field(
        default=(), repr=False
    )
    outcome: ExecutionOutcome | None = None


@dataclass(frozen=True)
class ExecutionCardDeliveryClaim:
    card: ExecutionReviewCard
    token: str = field(repr=False)
    expires_at: str
    #: The delivery handle of the presentation this claim replaces, and the
    #: transport that issued it, or `None` for a first presentation.  A
    #: consumer withdraws that message before posting the replacement, and
    #: only when the transport is its own: a handle means nothing to a
    #: surface that did not issue it.
    superseded_delivery_ref: str | None = None
    superseded_transport: str | None = None


@dataclass(frozen=True)
class ExecutionCardRetractionClaim:
    """One consumer-owned request to remove an obsolete presentation."""

    card_id: int
    transport: str
    delivery_ref: str
    token: str = field(repr=False)
    expires_at: str


@dataclass(frozen=True)
class ExecutionCardPresentation:
    """One delivered card, rendered again exactly as it was delivered.

    Its own type for the same reason `ExecutionCardBrief` is: nothing was
    operated on, and what a caller wants from it -- the card as a surface
    should show it -- is not something an operation result carries.

    It exists because a surface that replaces a card's controls with a
    sub-menu no longer holds the controls it replaced, and they are decided
    here. Without this, opening that sub-menu is irreversible: a reader who
    taps it by mistake is left with a card they can only defer.
    """

    disposition: ExecutionCardDisposition
    card_id: int
    card_version: int | None = None
    card: ExecutionReviewCard | None = field(default=None, repr=False)
    refusal: ExecutionCardRefusal | None = None

    @property
    def accepted(self) -> bool:
        return self.disposition is not ExecutionCardDisposition.REFUSED


@dataclass(frozen=True)
class ExecutionCardBrief:
    """One task described for an agent that is not this one.

    Its own type rather than an operation result: nothing was operated on,
    and the one thing a caller wants from it — the text — is not something
    any other result carries.
    """

    disposition: ExecutionCardDisposition
    card_id: int
    card_version: int | None = None
    text: str = field(default="", repr=False)
    refusal: ExecutionCardRefusal | None = None

    @property
    def accepted(self) -> bool:
        return self.disposition is not ExecutionCardDisposition.REFUSED


@dataclass(frozen=True)
class ExecutionCardDeliverables:
    """Prepared deliverables from one still-actionable review card.

    Reading them is deliberately not an action on the card.  A chat surface
    sends this text as a separate message so a reader can inspect or copy a
    draft without losing the review controls underneath the original card.
    """

    disposition: ExecutionCardDisposition
    card_id: int
    card_version: int | None = None
    text: str = field(default="", repr=False)
    refusal: ExecutionCardRefusal | None = None

    @property
    def accepted(self) -> bool:
        return self.disposition is not ExecutionCardDisposition.REFUSED


@dataclass(frozen=True)
class ExecutionCardArtifact:
    """Public metadata and verified bytes for one recorded result artifact."""

    ordinal: int
    name: str
    size_bytes: int
    content: bytes = field(default=b"", repr=False)


@dataclass(frozen=True)
class ExecutionCardArtifacts:
    """A version-fenced list of files belonging to one delivered result."""

    disposition: ExecutionCardDisposition
    card_id: int
    card_version: int | None = None
    artifacts: tuple[ExecutionCardArtifact, ...] = field(
        default=(), repr=False
    )
    refusal: ExecutionCardRefusal | None = None

    @property
    def accepted(self) -> bool:
        return self.disposition is not ExecutionCardDisposition.REFUSED


@dataclass(frozen=True)
class ExecutionCardDetail:
    """Bounded, non-mutating current-run projection for a queue reader."""

    disposition: ExecutionCardDisposition
    card_id: int
    card_version: int | None = None
    workflow_version: int | None = None
    status: WorkflowStatus | None = None
    phase: WorkflowPhase | None = None
    updated_at: str | None = None
    due_at: str | None = None
    completed_at: str | None = None
    outcome: ExecutionOutcome | None = None
    summary: str = field(default="", repr=False)
    work_digest: str = field(default="", repr=False)
    #: The recorded account of the run. The two fields above are short
    #: derivations of it; this is the one a reader can be shown the work
    #: through. Bounded where it is serialized, as they are.
    work_markdown: str = field(default="", repr=False)
    deliverables: tuple[CardRecord, ...] = field(default=(), repr=False)
    failure_reason: str | None = None
    failure_exit_code: int | None = None
    failure_run_id: str | None = None
    refusal: ExecutionCardRefusal | None = None

    @property
    def accepted(self) -> bool:
        return self.disposition is not ExecutionCardDisposition.REFUSED


@dataclass(frozen=True)
class ExecutionCardOperationResult:
    disposition: ExecutionCardDisposition
    card_id: int
    card_version: int | None = None
    card_status: ExecutionCardStatus | None = None
    workflow_version: int | None = None
    workflow_status: WorkflowStatus | None = None
    workflow_phase: WorkflowPhase | None = None
    wake_at: str | None = None
    refusal: ExecutionCardRefusal | None = None

    @property
    def accepted(self) -> bool:
        return self.disposition is not ExecutionCardDisposition.REFUSED


@dataclass(frozen=True)
class ExecutionAgentOption:
    display_name: str = field(repr=False)
    selection_token: str = field(repr=False)
    selected: bool = False


@dataclass(frozen=True)
class ExecutionAgentSelectorResult:
    disposition: ExecutionCardDisposition
    card_id: int
    card_version: int | None = None
    card: ExecutionReviewCard | None = field(default=None, repr=False)
    options: tuple[ExecutionAgentOption, ...] = ()
    refusal: ExecutionCardRefusal | None = None

    @property
    def accepted(self) -> bool:
        return self.disposition is not ExecutionCardDisposition.REFUSED


class ExecutionCardService:
    """Durable delivery and atomic reader actions for execution gates."""

    def __init__(
        self,
        database_path: str | os.PathLike[str],
        *,
        clock: Callable[[], datetime] | None = None,
        token_factory: Callable[[], str] | None = None,
        profile_registry: AgentProfileRegistry | None = None,
        owner_condition: Callable[
            [str, Mapping[str, object]], OwnerUpcomingMeeting
        ] | None = None,
        reader_aliases: Sequence[str] = (),
        artifact_root: str | os.PathLike[str] | None = None,
        steer_plan_threshold: timedelta = timedelta(minutes=20),
        steer_execute_threshold: timedelta = timedelta(minutes=20),
    ) -> None:
        self.database_path = Path(database_path)
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._token_factory = token_factory or (
            lambda: secrets.token_urlsafe(32)
        )
        registry = profile_registry or load_registry()
        if not isinstance(registry, AgentProfileRegistry):
            raise ValueError("agent profile registry is invalid")
        self._profile_registry = registry
        if owner_condition is not None and not callable(owner_condition):
            raise ValueError("owner meeting condition is invalid")
        aliases = tuple(reader_aliases)
        if any(
            not isinstance(alias, str)
            or not alias
            or alias != alias.strip()
            or len(alias) > MAX_OWNER_CHARS
            for alias in aliases
        ):
            raise ValueError("reader aliases are invalid")
        self._owner_condition = owner_condition
        self._reader_aliases = frozenset(
            normalized_owner(alias) for alias in aliases
        )
        if min(steer_plan_threshold, steer_execute_threshold) <= timedelta(0):
            raise ValueError("steer card threshold is invalid")
        self._steer_thresholds = {
            WorkflowPhase.PLAN: steer_plan_threshold,
            WorkflowPhase.EXECUTE: steer_execute_threshold,
            # External work is an execute-like pass. It has no separate
            # operator knob, but must remain announceable: a Steer card has
            # no result and is valid for every workflow phase.
            WorkflowPhase.EXTERNAL_ACTION: steer_execute_threshold,
        }
        if artifact_root is None:
            self._artifact_root = None
        else:
            root = Path(artifact_root)
            if not root.is_dir() or root.is_symlink():
                raise ValueError("execution artifact root is invalid")
            self._artifact_root = root.resolve(strict=True)

    def schedule(self, *, limit: int = 100) -> ExecutionCardScheduleResult:
        if not _valid_limit(limit):
            return ExecutionCardScheduleResult(
                ExecutionCardDisposition.REFUSED,
                refusal=ExecutionCardRefusal.INVALID_ARGUMENT,
            )
        stamp = self._clock_value()
        self._poll_owner_holds(stamp, limit=min(limit, MAX_OWNER_HOLD_CHECKS))
        now = stamp.isoformat(timespec="seconds")
        with closing(self._connect()) as connection:
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute("BEGIN IMMEDIATE")
            try:
                cancelled = self._cancel_stale(connection, now)
                rows = connection.execute(
                    "SELECT w.task_id,w.task_version,w.status,w.phase,"
                    "w.version,w.last_result_id,w.steer_while_running,"
                    "w.claimed_at,r.outcome "
                    "FROM task_execution_workflows AS w "
                    "JOIN tasks AS t ON t.id=w.task_id "
                    "LEFT JOIN task_execution_results AS r "
                    "ON r.result_id=w.last_result_id AND r.task_id=w.task_id "
                    "WHERE t.status='open' AND t.version=w.task_version "
                    "AND NOT EXISTS("
                    " SELECT 1 FROM execution_review_cards AS active "
                    " WHERE active.task_id=w.task_id AND active.status IN "
                    " ('pending','delivering','delivered')"
                    ") AND ("
                    # `parked` is included deliberately. It means the
                    # agent gave up after repeated failure, and that is
                    # exactly when a reader needs telling — it used to be
                    # terminal and silent, so a task sat open forever with
                    # its workflow quietly abandoned and no card anywhere.
                    # No "is anything else busy" condition. It used to
                    # suppress every gate whenever ANY workflow anywhere was
                    # queued, running or awaiting review — one machine had
                    # 143 tasks invisible behind seven in flight, with a
                    # free card surface and nothing to show on it. How many
                    # cards a reader sees at once is the drip's business,
                    # and it already bounds that; a gate is how work gets
                    # queued in the first place, so suppressing it while
                    # work runs is how a queue empties and never refills.
                    # A parked workflow is offered as a gate in whatever
                    # phase it stopped in. The gate used to be pinned to
                    # `plan` because the schema pinned the card there, so a
                    # workflow that gave up during `execute` produced no
                    # card at all and the task simply went quiet. The pin
                    # was a proxy for "a gate carries no result", which the
                    # constraint still says on its own.
                    " (w.status='awaiting_start' OR "
                    "  w.status='parked' OR "
                    "  (w.status='snoozed' AND w.due_at<=? "
                    "   AND w.last_result_id IS NULL)) OR "
                    " (w.status='snoozed' AND w.due_at<=? "
                    "  AND w.last_result_id IS NOT NULL) OR "
                    " (w.status='awaiting_review' AND w.phase='plan' "
                    "  AND r.outcome='awaiting_plan') OR "
                    " (w.status='awaiting_review' AND w.phase='execute' "
                    "  AND r.outcome='awaiting_external') OR "
                    " (w.status IN ('awaiting_review','completed') "
                    "  AND r.outcome IN ('completed','declined','ineligible'))"
                    " OR (w.status='running' AND w.steer_while_running=1 "
                    "  AND ((w.phase='plan' AND w.claimed_at<=?) "
                    "    OR (w.phase='execute' AND w.claimed_at<=?) "
                    "    OR (w.phase='external_action' AND w.claimed_at<=?)) "
                    "  AND NOT EXISTS(SELECT 1 FROM execution_review_cards AS prior "
                    "   WHERE prior.task_id=w.task_id AND prior.workflow_version=w.version "
                    "   AND prior.kind='steer'))"
                    ") ORDER BY CASE "
                    # A result is the only finished work in this queue.
                    # Make its report visible before asking a reader to
                    # begin or approve another piece of work.
                    "WHEN r.outcome IN ('completed','declined','ineligible') "
                    "THEN 0 "
                    # Both kinds in this band need a decision to continue
                    # work already under way.  An external authorisation is
                    # not a completed result, but should not sit behind a
                    # task that has not started either.
                    "WHEN w.status IN ('awaiting_review','snoozed') AND ("
                    " (w.phase='plan' AND r.outcome='awaiting_plan') OR "
                    " (w.phase='execute' AND r.outcome='awaiting_external')"
                    ") THEN 1 "
                    "WHEN w.status='running' THEN 3 ELSE 2 END,w.updated_at,w.task_id LIMIT ?",
                    (
                        now,
                        now,
                        (stamp - self._steer_thresholds[WorkflowPhase.PLAN])
                        .isoformat(timespec="seconds"),
                        (stamp - self._steer_thresholds[WorkflowPhase.EXECUTE])
                        .isoformat(timespec="seconds"),
                        (stamp - self._steer_thresholds[
                            WorkflowPhase.EXTERNAL_ACTION
                        ]).isoformat(timespec="seconds"),
                        limit,
                    ),
                ).fetchall()
                for row in rows:
                    kind = _kind_for_workflow(row)
                    result_id = (
                        None
                        if kind in {ExecutionCardKind.START, ExecutionCardKind.STEER}
                        else row["last_result_id"]
                    )
                    cursor = connection.execute(
                        # The work revision is the source state this approval
                        # is being asked about. Recorded so a later outcome
                        # can name what the reader was actually shown; it is
                        # not a staleness check, which _current_card owns.
                        "INSERT INTO execution_review_cards("
                        "task_id,task_version,workflow_version,kind,phase,"
                        "result_id,status,version,created_at,updated_at,"
                        "work_revision_id) "
                        "VALUES(?,?,?,?,?,?,'pending',1,?,?,"
                        "(SELECT r.id FROM work_revisions AS r "
                        " JOIN work_items AS w ON w.id=r.work_item_id "
                        " WHERE w.task_id=? ORDER BY r.id DESC LIMIT 1))",
                        (
                            int(row["task_id"]),
                            int(row["task_version"]),
                            int(row["version"]),
                            kind,
                            row["phase"],
                            result_id,
                            now,
                            now,
                            int(row["task_id"]),
                        ),
                    )
                    self._event(
                        connection,
                        card_id=int(cursor.lastrowid),
                        task_id=int(row["task_id"]),
                        kind="scheduled",
                        card_version=1,
                        workflow_version=int(row["version"]),
                        action=None,
                        now=now,
                    )
                connection.commit()
                created = len(rows)
                return ExecutionCardScheduleResult(
                    ExecutionCardDisposition.APPLIED
                    if created or cancelled
                    else ExecutionCardDisposition.UNCHANGED,
                    created=created,
                    cancelled=cancelled,
                )
            except Exception:
                connection.rollback()
                raise

    def due(self, *, limit: int = 20) -> tuple[ExecutionReviewCard, ...]:
        """Return current, unheld cards without acquiring a lease or lock."""
        if not _valid_limit(limit):
            raise TaskLedgerError("execution card due limit is invalid")
        now = self._now()
        with closing(self._connect()) as connection:
            rows = connection.execute(
                self._card_select()
                + " AND c.status='pending' AND ("
                + "w.status<>'snoozed' OR w.due_at<=?) "
                + "ORDER BY c.id LIMIT ?",
                (now, limit),
            ).fetchall()
        return tuple(
            self._render_card(row) for row in rows if _current_card(row)
        )

    def board(self, *, limit: int = BOARD_CARD_LIMIT) -> ExecutionBoard:
        """Project current unclaimed execution work without acquiring a lease."""
        if (isinstance(limit, bool) or not isinstance(limit, int)
                or not 1 <= limit <= BOARD_CARD_LIMIT):
            raise TaskLedgerError("execution board limit is invalid")
        with closing(self._connect()) as connection:
            rows = connection.execute(
                self._card_select() + " AND c.status='pending' ORDER BY c.id"
            ).fetchall()
        cards = [self._render_card(row) for row in rows if _current_card(row)]
        totals: dict[str, int] = {
            status: 0 for status in EXECUTION_BOARD_STATUSES
        }
        for card in cards:
            totals[execution_board_status(card)] += 1
        return ExecutionBoard(cards=tuple(cards[:limit]), totals=totals)

    def resolve_queue_view(
        self, card_id: int, *, expected_version: int, action: str,
        input_kind: str | None = None, value: str | None = None,
        selection_token: str | None = None, consumer_digest: str,
    ) -> ExecutionCardOperationResult | "ClaimAtCeiling":
        """Claim, internally deliver, and resolve one queue card atomically."""
        if (not _valid_identity(card_id, expected_version)
                or not _valid_digest(consumer_digest)):
            return _refused(card_id, ExecutionCardRefusal.INVALID_ARGUMENT)
        allowed = {"start", "snooze", OWNER_HOLD_ACTION, *REVIEW_DIRECT_ACTIONS,
                   "discussion", "reassignment", "comment_and_go"}
        if action not in allowed:
            return _refused(card_id, ExecutionCardRefusal.INVALID_ACTION)
        if action in {"discussion", "reassignment"}:
            if input_kind != action or not _valid_reader_input(input_kind, value):
                return _refused(card_id, ExecutionCardRefusal.INVALID_ARGUMENT)
        elif action == "comment_and_go":
            if input_kind != "discussion" or not _valid_reader_input("discussion", value):
                return _refused(card_id, ExecutionCardRefusal.INVALID_ARGUMENT)
        elif action == "select_agent":
            return _refused(card_id, ExecutionCardRefusal.INVALID_ACTION)
        elif input_kind is not None or value is not None or selection_token is not None:
            return _refused(card_id, ExecutionCardRefusal.INVALID_ARGUMENT)
        stamp = self._clock_value()
        now = stamp.isoformat(timespec="seconds")
        with closing(self._connect()) as connection:
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = connection.execute(self._card_select() + " AND c.id=?", (card_id,)).fetchone()
                refusal = _card_guard(row, expected_version)
                if refusal is None and row["status"] != ExecutionCardStatus.PENDING:
                    refusal = ExecutionCardRefusal.INVALID_STATE
                if refusal is None and row is not None and (row["workflow_status_current"] == WorkflowStatus.SNOOZED
                                        and row["workflow_due_at"] > now):
                    refusal = ExecutionCardRefusal.INVALID_STATE
                if refusal is None and not _current_card(row):
                    refusal = ExecutionCardRefusal.STALE_VERSION
                kind = ExecutionCardKind(row["kind"]) if row is not None else None
                steer = kind is ExecutionCardKind.STEER
                ceiling_key = "queue_view_steer" if steer else "queue_view"
                # Retired run summaries are excluded here as they are from
                # the row lookup above: a row left behind by an earlier build
                # must not consume the capacity the console holds for cards
                # that do need an answer.
                held = connection.execute(
                    "SELECT count(*) FROM execution_review_cards WHERE "
                    "status IN ('delivering','delivered') AND consumer_digest=? "
                    "AND summary_only=0 AND kind "
                    + ("='steer'" if steer else "<>'steer'"),
                    (consumer_digest,),
                ).fetchone()[0]
                if refusal is None and held >= EXECUTION_CARD_CLAIM_CEILINGS[ceiling_key]:
                    connection.commit()
                    return ClaimAtCeiling(int(held), EXECUTION_CARD_CLAIM_CEILINGS[ceiling_key])
                if refusal is None and action in {"start", "approve", "done"} and not _card_fits(self._render_card(row)):
                    refusal = ExecutionCardRefusal.INVALID_STATE
                if refusal is None and action not in {"discussion", "reassignment", "comment_and_go", "select_agent"} and action not in _direct_actions_for_kind(kind):
                    refusal = ExecutionCardRefusal.INVALID_ACTION
                if refusal is not None:
                    connection.rollback()
                    return _refused_row(card_id, row, refusal)
                # This is an internal delivery transition: no transport or
                # claim capability crosses the queue_view boundary.
                version = expected_version + 1
                changed = connection.execute(
                    "UPDATE execution_review_cards SET status='delivered',version=?,consumer_digest=?,"
                    "transport='queue_view',delivery_ref=NULL,delivered_at=?,updated_at=? "
                    "WHERE id=? AND version=? AND status='pending'",
                    (version, consumer_digest, now, now, card_id, expected_version),
                )
                if changed.rowcount != 1:
                    connection.rollback()
                    return _refused_row(card_id, row, ExecutionCardRefusal.STALE_VERSION)
                self._event(connection, card_id=card_id, task_id=int(row["task_id"]), kind="delivery_claimed",
                             card_version=version, workflow_version=int(row["workflow_version"]), action=None, now=now,
                             consumer_digest=consumer_digest)
                self._event(connection, card_id=card_id, task_id=int(row["task_id"]), kind="delivered",
                             card_version=version, workflow_version=int(row["workflow_version"]), action=None, now=now,
                             consumer_digest=consumer_digest)
                if action in {"discussion", "reassignment"}:
                    # Preserve the established input semantics in this same transaction.
                    target = int(row["workflow_version"]) + 1
                    connection.execute(
                        "INSERT INTO execution_reader_inputs(card_id,task_id,card_version,task_version,workflow_version,target_workflow_version,kind,value,prior_value,occurred_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
                        (card_id, int(row["task_id"]), version, int(row["task_version"]), int(row["workflow_version"]), target, input_kind, value, row["owner"] if input_kind == "reassignment" else None, now),
                    )
                    if input_kind == "reassignment":
                        task_version = int(row["task_version"]) + 1
                        if connection.execute("UPDATE tasks SET owner=?,version=?,updated_at=?,owner_ref_version=1,owner_kind='external',owner_speaker_id=NULL,owner_canonical_speaker_id=NULL,owner_speaker_registry_id=NULL,owner_pinned=1,owner_provisional=0 WHERE id=? AND version=? AND status='open'", (value, task_version, now, int(row["task_id"]), int(row["task_version"]))).rowcount != 1:
                            raise TaskLedgerError("task ownership state changed")
                        status, phase, resolution = WorkflowStatus.AWAITING_START, WorkflowPhase.PLAN, "reassign"
                    else:
                        task_version = int(row["task_version"])
                        status = (WorkflowStatus.AWAITING_START
                                  if kind is ExecutionCardKind.START
                                  else WorkflowStatus.QUEUED)
                        phase = (WorkflowPhase(row["phase"])
                                 if kind is ExecutionCardKind.STEER
                                 else WorkflowPhase.PLAN)
                        resolution = "discuss"
                    workflow = connection.execute("UPDATE task_execution_workflows SET task_version=?,status=?,phase=?,version=?,due_at=NULL,claim_token_digest=NULL,claimed_at=NULL,claim_heartbeat_at=NULL,claim_expires_at=NULL,current_run_id=NULL,updated_at=? WHERE task_id=? AND version=?", (task_version, status, phase, target, now, int(row["task_id"]), int(row["workflow_version"])))
                    if workflow.rowcount != 1: raise TaskLedgerError("execution workflow state changed")
                    TaskExecutionService._event(connection, int(row["task_id"]), "reassigned" if input_kind == "reassignment" else "discussion_requested", target, task_version, phase, status, now)
                    workflow_version, workflow_status, workflow_phase = target, status, phase
                else:
                    if action == "comment_and_go":
                        connection.execute("INSERT INTO execution_reader_inputs(card_id,task_id,card_version,task_version,workflow_version,target_workflow_version,kind,value,prior_value,occurred_at) VALUES(?,?,?,?,?,?,?,?,?,?)", (card_id, int(row["task_id"]), version, int(row["task_version"]), int(row["workflow_version"]), int(row["workflow_version"])+1, "discussion", value, None, now))
                    workflow = (
                        _apply_review_lifecycle_action(connection, row, action=action, now=now)
                        if action in {"done", "drop"}
                        else _apply_start_action(connection, int(row["task_id"]),
                                                 expected_version=int(row["workflow_version"]),
                                                 action="start", stamp=stamp)
                        if kind is ExecutionCardKind.START
                        else _apply_steer_discussion(connection, row, stamp=stamp)
                        if kind is ExecutionCardKind.STEER
                        else _apply_review_action(connection, int(row["task_id"]),
                                                  expected_version=int(row["workflow_version"]),
                                                  action="approve" if action == "comment_and_go" else action,
                                                  stamp=stamp)
                    )
                    if workflow.disposition is WorkflowDisposition.REFUSED:
                        connection.rollback(); return _refused_row(card_id, row, _workflow_refusal(workflow.refusal))
                    workflow_version, workflow_status, workflow_phase = workflow.version, workflow.status, workflow.phase
                    resolution = _stored_action(action)
                final_version = version + 1
                connection.execute("UPDATE execution_review_cards SET status='resolved',version=?,claim_token_digest=NULL,consumer_digest=NULL,resolution=?,resolved_at=?,updated_at=? WHERE id=? AND version=? AND status='delivered'", (final_version, resolution, now, now, card_id, version))
                self._event(connection, card_id=card_id, task_id=int(row["task_id"]), kind="resolved", card_version=final_version, workflow_version=int(workflow_version), action=resolution, now=now)
                connection.commit()
                return ExecutionCardOperationResult(ExecutionCardDisposition.APPLIED, card_id, card_version=final_version, card_status=ExecutionCardStatus.RESOLVED, workflow_version=int(workflow_version), workflow_status=workflow_status, workflow_phase=workflow_phase)
            except Exception:
                connection.rollback(); raise

    def claim_next(
        self, *, lease_seconds: int = 60, consumer_digest: str | None = None,
        consumer_role: str = "drip",
    ) -> ExecutionCardDeliveryClaim | "ClaimAtCeiling" | None:
        if not _valid_lease(lease_seconds):
            raise TaskLedgerError("execution card delivery lease is invalid")
        if consumer_digest is None:
            consumer_digest = _token_digest("legacy-execution-card-consumer")
        if not _valid_digest(consumer_digest):
            raise TaskLedgerError("execution card consumer digest is invalid")
        try:
            ceiling = EXECUTION_CARD_CLAIM_CEILINGS[consumer_role]
        except (KeyError, TypeError) as exc:
            raise TaskLedgerError("execution card consumer role is invalid") from exc
        stamp = self._clock_value()
        now = stamp.isoformat(timespec="seconds")
        expires = (stamp + timedelta(seconds=lease_seconds)).isoformat(
            timespec="seconds"
        )
        token = self._token_factory()
        if not _valid_secret(token):
            raise TaskLedgerError(
                "execution card token factory returned invalid state"
            )
        digest = _token_digest(token)
        with closing(self._connect()) as connection:
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute("BEGIN IMMEDIATE")
            try:
                self._cancel_stale(connection, now)
                expired = connection.execute(
                    "SELECT id,task_id,workflow_version,version "
                    "FROM execution_review_cards WHERE status='delivering' "
                    "AND claim_expires_at<=? ORDER BY id",
                    (now,),
                ).fetchall()
                for row in expired:
                    version = int(row["version"]) + 1
                    connection.execute(
                        "UPDATE execution_review_cards SET status='pending',"
                        "version=?,claim_token_digest=NULL,"
                        "claim_expires_at=NULL,consumer_digest=NULL,updated_at=? "
                        "WHERE id=? AND version=? AND status='delivering'",
                        (version, now, int(row["id"]), int(row["version"])),
                    )
                    self._event(
                        connection,
                        card_id=int(row["id"]),
                        task_id=int(row["task_id"]),
                        kind="delivery_expired",
                        card_version=version,
                        workflow_version=int(row["workflow_version"]),
                        action=None,
                        now=now,
                    )
                row = connection.execute(
                    self._card_select()
                    + " AND c.status='pending' "
                    "ORDER BY CASE c.kind "
                    "WHEN 'result_review' THEN 0 "
                    "WHEN 'plan_review' THEN 1 "
                    "WHEN 'external_review' THEN 1 "
                    "WHEN 'start' THEN 2 "
                    "ELSE 3 END,c.created_at,c.id LIMIT 1"
                ).fetchone()
                if row is None:
                    connection.commit()
                    return None
                if not _current_card(row):
                    raise TaskLedgerError(
                        "execution card state is invalid")
                band, held = self._band(connection, row, consumer_role,
                                        consumer_digest)
                band_ceiling = EXECUTION_CARD_CLAIM_CEILINGS[band]
                if held >= band_ceiling:
                    # The reader's surface is full.  The card that did not
                    # fit stays pending and is offered again; the ceiling is
                    # never relaxed to spend the claim on something else.
                    connection.commit()
                    return ClaimAtCeiling(int(held), band_ceiling)
                self._render_card(row)
                version = int(row["version"]) + 1
                updated = connection.execute(
                    "UPDATE execution_review_cards SET status='delivering',"
                    "version=?,claim_token_digest=?,claim_expires_at=?,"
                    "consumer_digest=?,"
                    "transport=NULL,delivery_ref=NULL,delivered_at=NULL,"
                    "updated_at=? WHERE id=? AND version=? AND status='pending'",
                    (
                        version,
                        digest,
                        expires,
                        consumer_digest,
                        now,
                        int(row["id"]),
                        int(row["version"]),
                    ),
                )
                if updated.rowcount != 1:
                    connection.rollback()
                    return None
                self._event(
                    connection,
                    card_id=int(row["id"]),
                    task_id=int(row["task_id"]),
                    kind="delivery_claimed",
                    card_version=version,
                    workflow_version=int(row["workflow_version"]),
                    action=None,
                    now=now,
                    consumer_digest=consumer_digest,
                )
                connection.commit()
                values = dict(row)
                values.update(status=ExecutionCardStatus.DELIVERING, version=version)
                return ExecutionCardDeliveryClaim(
                    self._render_card(values),
                    token,
                    expires,
                    row["superseded_delivery_ref"],
                    row["superseded_transport"],
                )
            except Exception:
                connection.rollback()
                raise

    def complete_delivery(
        self,
        card_id: int,
        *,
        expected_version: int,
        claim_token: str,
        transport: str,
        delivery_ref: str,
    ) -> ExecutionCardOperationResult:
        if not _valid_identity(card_id, expected_version):
            return _refused(card_id, ExecutionCardRefusal.INVALID_ARGUMENT)
        if (
            not _valid_secret(claim_token)
            or not _valid_opaque(transport, 64)
            or not _valid_opaque(delivery_ref, 200)
        ):
            return _refused(card_id, ExecutionCardRefusal.INVALID_ARGUMENT)
        now = self._now()
        digest = _token_digest(claim_token)
        with closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = connection.execute(
                    "SELECT * FROM execution_review_cards WHERE id=?",
                    (card_id,),
                ).fetchone()
                refusal = _card_guard(row, expected_version)
                if refusal is not None:
                    connection.rollback()
                    return _refused_row(card_id, row, refusal)
                if row["status"] == ExecutionCardStatus.DELIVERED:
                    connection.rollback()
                    if (
                        row["transport"] == transport
                        and row["delivery_ref"] == delivery_ref
                    ):
                        return _operation(row, ExecutionCardDisposition.UNCHANGED)
                    return _refused_row(
                        card_id, row, ExecutionCardRefusal.INVALID_STATE
                    )
                if row["status"] != ExecutionCardStatus.DELIVERING:
                    connection.rollback()
                    return _refused_row(
                        card_id, row, ExecutionCardRefusal.INVALID_STATE
                    )
                if row["claim_token_digest"] != digest:
                    connection.rollback()
                    return _refused_row(
                        card_id, row, ExecutionCardRefusal.CLAIM_MISMATCH
                    )
                connection.execute(
                    "UPDATE execution_review_cards SET status='delivered',"
                    "claim_token_digest=NULL,claim_expires_at=NULL,transport=?,"
                    "delivery_ref=?,delivered_at=?,updated_at=?,"
                    "superseded_delivery_ref=NULL,superseded_transport=NULL "
                    "WHERE id=? AND version=?",
                    (
                        transport,
                        delivery_ref,
                        now,
                        now,
                        card_id,
                        expected_version,
                    ),
                )
                self._event(
                    connection,
                    card_id=card_id,
                    task_id=int(row["task_id"]),
                    kind="delivered",
                    card_version=expected_version,
                    workflow_version=int(row["workflow_version"]),
                    action=None,
                    now=now,
                    consumer_digest=row["consumer_digest"],
                )
                connection.commit()
                values = dict(row)
                values.update(
                    status=ExecutionCardStatus.DELIVERED,
                    claim_token_digest=None,
                    claim_expires_at=None,
                    transport=transport,
                    delivery_ref=delivery_ref,
                    delivered_at=now,
                    superseded_delivery_ref=None,
                    superseded_transport=None,
                )
                return _operation(values, ExecutionCardDisposition.APPLIED)
            except Exception:
                connection.rollback()
                raise

    def fail_delivery(
        self, card_id: int, *, expected_version: int, claim_token: str
    ) -> ExecutionCardOperationResult:
        if (
            not _valid_identity(card_id, expected_version)
            or not _valid_secret(claim_token)
        ):
            return _refused(card_id, ExecutionCardRefusal.INVALID_ARGUMENT)
        now = self._now()
        digest = _token_digest(claim_token)
        with closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = connection.execute(
                    "SELECT * FROM execution_review_cards WHERE id=?",
                    (card_id,),
                ).fetchone()
                refusal = _card_guard(row, expected_version)
                if refusal is not None:
                    connection.rollback()
                    return _refused_row(card_id, row, refusal)
                if row["status"] != ExecutionCardStatus.DELIVERING:
                    connection.rollback()
                    return _refused_row(
                        card_id, row, ExecutionCardRefusal.INVALID_STATE
                    )
                if row["claim_token_digest"] != digest:
                    connection.rollback()
                    return _refused_row(
                        card_id, row, ExecutionCardRefusal.CLAIM_MISMATCH
                    )
                version = expected_version + 1
                connection.execute(
                    "UPDATE execution_review_cards SET status='pending',"
                    "version=?,claim_token_digest=NULL,claim_expires_at=NULL,"
                    "updated_at=? WHERE id=? AND version=?",
                    (version, now, card_id, expected_version),
                )
                self._event(
                    connection,
                    card_id=card_id,
                    task_id=int(row["task_id"]),
                    kind="delivery_failed",
                    card_version=version,
                    workflow_version=int(row["workflow_version"]),
                    action=None,
                    now=now,
                )
                connection.commit()
                values = dict(row)
                values.update(
                    status=ExecutionCardStatus.PENDING,
                    version=version,
                    claim_token_digest=None,
                    claim_expires_at=None,
                )
                return _operation(values, ExecutionCardDisposition.APPLIED)
            except Exception:
                connection.rollback()
                raise

    def release_delivery(
        self,
        card_id: int,
        *,
        expected_version: int,
        claim_token: str,
        consumer_digest: str,
        reason: str,
    ) -> ExecutionCardOperationResult:
        if (
            not _valid_identity(card_id, expected_version)
            or not _valid_secret(claim_token)
            or not _valid_digest(consumer_digest)
            or reason not in EXECUTION_CARD_RELEASE_REASONS
        ):
            return _refused(card_id, ExecutionCardRefusal.INVALID_ARGUMENT)
        now = self._now()
        digest = _token_digest(claim_token)
        with closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = connection.execute(
                    "SELECT * FROM execution_review_cards WHERE id=?",
                    (card_id,),
                ).fetchone()
                refusal = _card_guard(row, expected_version)
                if refusal is not None:
                    connection.rollback()
                    return _refused_row(card_id, row, refusal)
                if row["status"] != ExecutionCardStatus.DELIVERING:
                    connection.rollback()
                    return _refused_row(
                        card_id, row, ExecutionCardRefusal.INVALID_STATE
                    )
                if row["claim_token_digest"] != digest:
                    connection.rollback()
                    return _refused_row(
                        card_id, row, ExecutionCardRefusal.CLAIM_MISMATCH
                    )
                if row["consumer_digest"] != consumer_digest:
                    connection.rollback()
                    return _refused_row(
                        card_id, row, ExecutionCardRefusal.CLAIM_MISMATCH
                    )
                version = expected_version + 1
                connection.execute(
                    "UPDATE execution_review_cards SET status='pending',"
                    "version=?,claim_token_digest=NULL,claim_expires_at=NULL,"
                    "consumer_digest=NULL,updated_at=? WHERE id=? AND version=?",
                    (version, now, card_id, expected_version),
                )
                self._event(
                    connection,
                    card_id=card_id,
                    task_id=int(row["task_id"]),
                    kind="claim_released",
                    card_version=version,
                    workflow_version=int(row["workflow_version"]),
                    action=reason,
                    now=now,
                    consumer_digest=consumer_digest,
                )
                connection.commit()
                values = dict(row)
                values.update(
                    status=ExecutionCardStatus.PENDING,
                    version=version,
                    claim_token_digest=None,
                    claim_expires_at=None,
                    consumer_digest=None,
                )
                return _operation(values, ExecutionCardDisposition.APPLIED)
            except Exception:
                connection.rollback()
                raise

    def retry_delivery(
        self, card_id: int, *, expected_version: int
    ) -> ExecutionCardOperationResult:
        """Requeue a current card whose acknowledged presentation failed.

        This is a local operator repair, not part of the transport API. It
        changes only delivery metadata and versions the card so callbacks on
        the superseded presentation become stale.
        """
        if not _valid_identity(card_id, expected_version):
            return _refused(card_id, ExecutionCardRefusal.INVALID_ARGUMENT)
        now = self._now()
        with closing(self._connect()) as connection:
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = connection.execute(
                    self._card_select() + " AND c.id=?", (card_id,)
                ).fetchone()
                refusal = _card_guard(row, expected_version)
                if (
                    refusal is None
                    and row["status"] != ExecutionCardStatus.DELIVERED
                ):
                    refusal = ExecutionCardRefusal.INVALID_STATE
                if refusal is None and not _current_card(row):
                    refusal = ExecutionCardRefusal.STALE_VERSION
                if refusal is not None:
                    connection.rollback()
                    return _refused_row(card_id, row, refusal)
                version = expected_version + 1
                updated = connection.execute(
                    "UPDATE execution_review_cards SET status='pending',"
                    "version=?,claim_token_digest=NULL,claim_expires_at=NULL,"
                    "transport=NULL,delivery_ref=NULL,delivered_at=NULL,"
                    "updated_at=? WHERE id=? AND version=? "
                    "AND status='delivered'",
                    (version, now, card_id, expected_version),
                )
                if updated.rowcount != 1:
                    raise TaskLedgerError("execution card state changed")
                self._event(
                    connection,
                    card_id=card_id,
                    task_id=int(row["task_id"]),
                    kind="delivery_failed",
                    card_version=version,
                    workflow_version=int(row["workflow_version"]),
                    action=None,
                    now=now,
                )
                connection.commit()
                values = dict(row)
                values.update(
                    status=ExecutionCardStatus.PENDING,
                    version=version,
                    claim_token_digest=None,
                    claim_expires_at=None,
                    transport=None,
                    delivery_ref=None,
                    delivered_at=None,
                )
                return _operation(values, ExecutionCardDisposition.APPLIED)
            except Exception:
                connection.rollback()
                raise

    def recover_delivery(
        self, card_id: int, *, expected_version: int
    ) -> ExecutionCardOperationResult:
        """Release or retry an expired, unacknowledged execution-card delivery.

        This is a local operator repair. It expires a stalled claim without
        waiting for the next active consumer fetch, ensuring subsequent cards
        can proceed without duplicating an acknowledged delivery.
        """
        if not _valid_identity(card_id, expected_version):
            return _refused(card_id, ExecutionCardRefusal.INVALID_ARGUMENT)
        now = self._now()
        with closing(self._connect()) as connection:
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = connection.execute(
                    self._card_select() + " AND c.id=?", (card_id,)
                ).fetchone()
                refusal = _card_guard(row, expected_version)
                if (
                    refusal is None
                    and row["status"] != ExecutionCardStatus.DELIVERING
                ):
                    refusal = ExecutionCardRefusal.INVALID_STATE
                if refusal is None and row["claim_expires_at"] > now:
                    refusal = ExecutionCardRefusal.INVALID_STATE
                if refusal is None and not _current_card(row):
                    refusal = ExecutionCardRefusal.STALE_VERSION
                if refusal is not None:
                    connection.rollback()
                    return _refused_row(card_id, row, refusal)
                version = expected_version + 1
                updated = connection.execute(
                    "UPDATE execution_review_cards SET status='pending',"
                    "version=?,claim_token_digest=NULL,claim_expires_at=NULL,"
                    "consumer_digest=NULL,updated_at=? WHERE id=? AND version=? "
                    "AND status='delivering'",
                    (version, now, card_id, expected_version),
                )
                if updated.rowcount != 1:
                    connection.rollback()
                    return _refused_row(
                        card_id, row, ExecutionCardRefusal.INVALID_STATE
                    )
                self._event(
                    connection,
                    card_id=card_id,
                    task_id=int(row["task_id"]),
                    kind="delivery_expired",
                    card_version=version,
                    workflow_version=int(row["workflow_version"]),
                    action=None,
                    now=now,
                )
                connection.commit()
                values = dict(row)
                values.update(
                    status=ExecutionCardStatus.PENDING,
                    version=version,
                    claim_token_digest=None,
                    claim_expires_at=None,
                    consumer_digest=None,
                )
                return _operation(values, ExecutionCardDisposition.APPLIED)
            except Exception:
                connection.rollback()
                raise

    def requeue_unanswered(self, *, limit: int = 100) -> ExecutionCardRequeueResult:
        """Re-present current cards left unanswered for at least one hour.

        The old chat message remains a historical presentation, but its
        callbacks are version-stale before a replacement can be claimed.

        This records `requeued`, not `delivery_failed`. It borrowed the
        failure kind once, and `delivery_health` counts those against a
        threshold of three in fifteen minutes -- so an hourly requeue of three
        unanswered cards reported delivery as unhealthy on a system that was
        delivering fine, and a real transport failure became indistinguishable
        from routine re-presentation.
        """
        if not _valid_limit(limit):
            return ExecutionCardRequeueResult()
        stamp = self._clock_value()
        now = stamp.isoformat(timespec="seconds")
        due = (stamp - timedelta(hours=1)).isoformat(timespec="seconds")
        with closing(self._connect()) as connection:
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute("BEGIN IMMEDIATE")
            try:
                self._cancel_stale(connection, now)
                rows = connection.execute(
                    self._card_select()
                    + " AND c.status='delivered' AND c.kind<>'steer' "
                    "AND c.delivered_at<=? "
                    "ORDER BY c.delivered_at,c.id LIMIT ?",
                    (due, limit),
                ).fetchall()
                requeued = 0
                for row in rows:
                    if not _current_card(row):
                        continue
                    card_id = int(row["id"])
                    version = int(row["version"]) + 1
                    updated = connection.execute(
                        "UPDATE execution_review_cards SET status='pending',"
                        "version=?,claim_token_digest=NULL,claim_expires_at=NULL,"
                        "superseded_delivery_ref=delivery_ref,"
                        "superseded_transport=transport,"
                        "transport=NULL,delivery_ref=NULL,delivered_at=NULL,"
                        "updated_at=? WHERE id=? AND version=? "
                        "AND status='delivered'",
                        (version, now, card_id, int(row["version"])),
                    )
                    if updated.rowcount != 1:
                        raise TaskLedgerError("execution card state changed")
                    self._event(
                        connection,
                        card_id=card_id,
                        task_id=int(row["task_id"]),
                        kind="requeued",
                        card_version=version,
                        workflow_version=int(row["workflow_version"]),
                        action=None,
                        now=now,
                    )
                    requeued += 1
                connection.commit()
                return ExecutionCardRequeueResult(requeued=requeued)
            except Exception:
                connection.rollback()
                raise

    def steer_cards_awaiting_digest(
        self, *, limit: int = 20
    ) -> tuple[SteerDigestWorkItem, ...]:
        """Return current live cards whose optional digest is still absent."""
        if not _valid_limit(limit):
            return ()
        refresh_due = (
            self._clock_value() - STEER_DIGEST_REFRESH_INTERVAL
        ).isoformat(timespec="seconds")
        with closing(self._connect()) as connection:
            rows = connection.execute(
                "SELECT c.id,c.version,c.task_id,c.workflow_version,c.phase,"
                "w.current_run_id FROM execution_review_cards AS c "
                "JOIN task_execution_workflows AS w ON w.task_id=c.task_id "
                "JOIN tasks AS t ON t.id=c.task_id "
                "WHERE c.kind='steer' AND (c.steer_digest IS NULL OR EXISTS("
                "SELECT 1 FROM execution_steer_digest_refreshes AS d "
                "WHERE d.card_id=c.id AND d.attempts<? "
                "AND d.last_refreshed_at<=?)) "
                "AND c.status IN ('pending','delivering','delivered') "
                "AND w.status='running' AND w.version=c.workflow_version "
                "AND w.phase=c.phase AND w.current_run_id IS NOT NULL "
                "AND t.status='open' AND t.version=c.task_version "
                "ORDER BY c.created_at,c.id LIMIT ?",
                (STEER_DIGEST_MAX_ATTEMPTS, refresh_due, limit),
            ).fetchall()
        return tuple(
            SteerDigestWorkItem(
                int(row["id"]), int(row["version"]), int(row["task_id"]),
                int(row["workflow_version"]), WorkflowPhase(row["phase"]),
                str(row["current_run_id"]),
            ) for row in rows
        )

    def record_steer_digest(
        self, item: SteerDigestWorkItem, digest: str,
    ) -> bool:
        """Store one bounded derived digest only while its card stays live."""
        if (
            not isinstance(item, SteerDigestWorkItem)
            or not isinstance(digest, str)
            or not 1 <= len(digest) <= 800
        ):
            return False
        now = self._now()
        with closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                changed = connection.execute(
                    "UPDATE execution_review_cards SET steer_digest=?,"
                    "updated_at=? WHERE id=? AND version=? AND kind='steer' "
                    "AND status IN "
                    "('pending','delivering','delivered') AND EXISTS("
                    "SELECT 1 FROM task_execution_workflows AS w "
                    "JOIN tasks AS t ON t.id=w.task_id WHERE w.task_id="
                    "execution_review_cards.task_id AND w.status='running' "
                    "AND w.version=execution_review_cards.workflow_version "
                    "AND w.phase=execution_review_cards.phase "
                    "AND w.current_run_id=? AND t.status='open' "
                    "AND t.version=execution_review_cards.task_version)",
                    (digest, now, item.card_id, item.card_version, item.run_id),
                )
                if changed.rowcount != 1:
                    connection.rollback()
                    return False
                refresh = connection.execute(
                    "INSERT INTO execution_steer_digest_refreshes("
                    "card_id,attempts,last_refreshed_at) VALUES(?,1,?) "
                    "ON CONFLICT(card_id) DO UPDATE SET attempts=attempts+1,"
                    "last_refreshed_at=excluded.last_refreshed_at "
                    "WHERE attempts<?",
                    (item.card_id, now, STEER_DIGEST_MAX_ATTEMPTS),
                )
                if refresh.rowcount != 1:
                    connection.rollback()
                    return False
                connection.commit()
                return True
            except Exception:
                connection.rollback()
                raise

    def claim_retraction(
        self, *, lease_seconds: int = 60,
    ) -> ExecutionCardRetractionClaim | None:
        """Lease one obsolete presentation to exactly one consumer."""
        if not _valid_lease(lease_seconds):
            raise TaskLedgerError("execution card delivery lease is invalid")
        stamp = self._clock_value()
        now = stamp.isoformat(timespec="seconds")
        expires = (stamp + timedelta(seconds=lease_seconds)).isoformat(
            timespec="seconds")
        token = self._token_factory()
        if not _valid_secret(token):
            raise TaskLedgerError("execution card token factory returned invalid state")
        with closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                # A timed-out consumer did not retract anything; make it
                # eligible again unless it has exhausted the small retry cap.
                connection.execute(
                    "UPDATE execution_card_retractions SET state=CASE "
                    "WHEN attempts>=3 THEN 'abandoned' ELSE 'pending' END,"
                    "claim_token_digest=NULL,claim_expires_at=NULL,updated_at=? "
                    "WHERE state='delivering' AND claim_expires_at<=?",
                    (now, now),
                )
                row = connection.execute(
                    "SELECT card_id,transport,delivery_ref FROM "
                    "execution_card_retractions WHERE state='pending' "
                    "AND attempts<3 ORDER BY updated_at,card_id LIMIT 1"
                ).fetchone()
                if row is None:
                    connection.commit()
                    return None
                changed = connection.execute(
                    "UPDATE execution_card_retractions SET state='delivering',"
                    "attempts=attempts+1,claim_token_digest=?,"
                    "claim_expires_at=?,updated_at=? WHERE card_id=? "
                    "AND state='pending' AND attempts<3",
                    (_token_digest(token), expires, now, int(row["card_id"])),
                )
                if changed.rowcount != 1:
                    connection.rollback()
                    return None
                connection.commit()
                return ExecutionCardRetractionClaim(
                    int(row["card_id"]), str(row["transport"]),
                    str(row["delivery_ref"]), token, expires,
                )
            except Exception:
                connection.rollback()
                raise

    def complete_retraction(self, claim: ExecutionCardRetractionClaim) -> bool:
        """Acknowledge successful removal and retire its durable handle."""
        if not isinstance(claim, ExecutionCardRetractionClaim):
            return False
        now = self._now()
        with closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                card = connection.execute(
                    "SELECT task_id,version,workflow_version FROM "
                    "execution_review_cards WHERE id=?",
                    (claim.card_id,),
                ).fetchone()
                changed = connection.execute(
                    "UPDATE execution_card_retractions SET state='completed',"
                    "claim_token_digest=NULL,claim_expires_at=NULL,updated_at=? "
                    "WHERE card_id=? AND state='delivering' "
                    "AND claim_token_digest=?",
                    (now, claim.card_id, _token_digest(claim.token)),
                )
                if changed.rowcount:
                    connection.execute(
                        "UPDATE execution_review_cards SET "
                        "superseded_delivery_ref=NULL,superseded_transport=NULL "
                        "WHERE id=?", (claim.card_id,)
                    )
                    if card is None:
                        raise TaskLedgerError("execution card is missing")
                    self._event(
                        connection,
                        card_id=claim.card_id,
                        task_id=int(card["task_id"]),
                        kind="retracted",
                        card_version=int(card["version"]),
                        workflow_version=int(card["workflow_version"]),
                        action=None,
                        now=now,
                    )
                connection.commit()
                return changed.rowcount == 1
            except Exception:
                connection.rollback()
                raise

    def fail_retraction(self, claim: ExecutionCardRetractionClaim) -> bool:
        """Keep a failed transport attempt retryable, within the fixed cap."""
        if not isinstance(claim, ExecutionCardRetractionClaim):
            return False
        now = self._now()
        with closing(self._connect()) as connection:
            changed = connection.execute(
                "UPDATE execution_card_retractions SET state=CASE "
                "WHEN attempts>=3 THEN 'abandoned' ELSE 'pending' END,"
                "claim_token_digest=NULL,claim_expires_at=NULL,updated_at=? "
                "WHERE card_id=? AND state='delivering' AND claim_token_digest=?",
                (now, claim.card_id, _token_digest(claim.token)),
            )
            connection.commit()
            return changed.rowcount == 1

    def act(
        self, card_id: int, *, expected_version: int, action: str
    ) -> ExecutionCardOperationResult:
        if not _valid_identity(card_id, expected_version):
            return _refused(card_id, ExecutionCardRefusal.INVALID_ARGUMENT)
        if action not in {
            "start", "snooze", OWNER_HOLD_ACTION, *REVIEW_DIRECT_ACTIONS
        }:
            return _refused(card_id, ExecutionCardRefusal.INVALID_ACTION)
        stamp = self._clock_value()
        now = stamp.isoformat(timespec="seconds")
        with closing(self._connect()) as connection:
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = connection.execute(
                    self._card_select() + " AND c.id=?", (card_id,)
                ).fetchone()
                refusal = _card_guard(row, expected_version)
                if refusal is None and row["status"] != ExecutionCardStatus.DELIVERED:
                    refusal = ExecutionCardRefusal.INVALID_STATE
                if refusal is None and not _current_card(row):
                    refusal = ExecutionCardRefusal.STALE_VERSION
                if refusal is None and action not in _direct_actions_for_kind(
                    ExecutionCardKind(row["kind"])
                ):
                    refusal = ExecutionCardRefusal.INVALID_ACTION
                if (
                    refusal is None
                    and action in {"start", "approve", "done"}
                    and not _card_fits(self._render_card(row))
                ):
                    refusal = ExecutionCardRefusal.INVALID_STATE
                if refusal is not None:
                    connection.rollback()
                    return _refused_row(card_id, row, refusal)

                card_kind = ExecutionCardKind(row["kind"])
                if (
                    card_kind is ExecutionCardKind.START
                    and action in {"done", "drop"}
                ):
                    workflow = _apply_review_lifecycle_action(
                        connection,
                        row,
                        action=action,
                        now=now,
                    )
                elif (
                    card_kind is ExecutionCardKind.START
                    and action == OWNER_HOLD_ACTION
                ):
                    card = self._render_card(row)
                    if not card.owner_hold_eligible:
                        connection.rollback()
                        return _refused_row(
                            card_id, row, ExecutionCardRefusal.INVALID_ACTION
                        )
                    workflow = _apply_owner_hold(
                        connection, row, stamp=stamp
                    )
                elif card_kind is ExecutionCardKind.START:
                    workflow = _apply_start_action(
                        connection,
                        int(row["task_id"]),
                        expected_version=int(row["workflow_version"]),
                        action=action,
                        stamp=stamp,
                    )
                elif action in {"done", "drop"}:
                    workflow = _apply_review_lifecycle_action(
                        connection,
                        row,
                        action=action,
                        now=now,
                    )
                else:
                    workflow = _apply_review_action(
                        connection,
                        int(row["task_id"]),
                        expected_version=int(row["workflow_version"]),
                        action=action,
                        stamp=stamp,
                    )
                if workflow.disposition is WorkflowDisposition.REFUSED:
                    connection.rollback()
                    return _refused_row(
                        card_id,
                        row,
                        _workflow_refusal(workflow.refusal),
                    )
                version = expected_version + 1
                updated = connection.execute(
                    "UPDATE execution_review_cards SET status='resolved',"
                    "version=?,claim_token_digest=NULL,claim_expires_at=NULL,"
                    "resolution=?,resolved_at=?,updated_at=? "
                    "WHERE id=? AND version=? AND status='delivered'",
                    (
                        version,
                        _stored_action(action),
                        now,
                        now,
                        card_id,
                        expected_version,
                    ),
                )
                if updated.rowcount != 1:
                    raise TaskLedgerError("execution card state changed")
                self._event(
                    connection,
                    card_id=card_id,
                    task_id=int(row["task_id"]),
                    kind="resolved",
                    card_version=version,
                    workflow_version=int(workflow.version),
                    action=_stored_action(action),
                    now=now,
                )
                connection.commit()
                return ExecutionCardOperationResult(
                    ExecutionCardDisposition.APPLIED,
                    card_id,
                    card_version=version,
                    card_status=ExecutionCardStatus.RESOLVED,
                    workflow_version=workflow.version,
                    workflow_status=workflow.status,
                    workflow_phase=workflow.phase,
                    wake_at=workflow.wake_at,
                )
            except Exception:
                connection.rollback()
                raise

    def agent_options(
        self, card_id: int, *, expected_version: int
    ) -> ExecutionAgentSelectorResult:
        """Return bounded choices for a current Start or plan-review card."""
        if not _valid_identity(card_id, expected_version):
            return _agent_refused(
                card_id, ExecutionCardRefusal.INVALID_ARGUMENT
            )
        with closing(self._connect()) as connection:
            row = connection.execute(
                self._card_select() + " AND c.id=?", (card_id,)
            ).fetchone()
            refusal = _agent_card_refusal(row, expected_version)
            if refusal is not None:
                return _agent_refused_row(card_id, row, refusal)
            card = self._render_card(row)
            options = _agent_options(self._profile_registry, card)
            return ExecutionAgentSelectorResult(
                ExecutionCardDisposition.UNCHANGED,
                card_id,
                card_version=expected_version,
                card=card,
                options=options,
            )

    def view(
        self, card_id: int, *, expected_version: int
    ) -> ExecutionCardPresentation:
        """One delivered card, rendered again, so a surface can restore it.

        A read, like `brief`: asking to see a card again is not answering
        it. Nothing is written, no version moves, and the card stays
        delivered -- so a reader who backs out of a sub-menu is exactly
        where they were, and a tap on the restored controls still addresses
        the same version it did before.

        Delivered and current are both required, unlike `brief`. A card that
        was never delivered has no presentation to restore, and one that has
        been resolved or superseded must not be handed back looking
        answerable; the caller gets the same stale refusal it would get for
        acting on it.

        Rendered through `_render_card`, not `_card`, because the reader
        aliases decide whether the owner hold is offered. Restoring a card
        without them would quietly drop a control the reader had a moment
        ago, which is the failure this read exists to prevent.
        """
        def refused(reason: ExecutionCardRefusal) -> ExecutionCardPresentation:
            return ExecutionCardPresentation(
                ExecutionCardDisposition.REFUSED, card_id, refusal=reason)

        if not _valid_identity(card_id, expected_version):
            return refused(ExecutionCardRefusal.INVALID_ARGUMENT)
        with closing(self._connect()) as connection:
            row = connection.execute(
                self._card_select() + " AND c.id=?", (card_id,)
            ).fetchone()
            refusal = _card_guard(row, expected_version)
            if refusal is None and row["status"] != ExecutionCardStatus.DELIVERED:
                refusal = ExecutionCardRefusal.INVALID_STATE
            if refusal is None and not _current_card(row):
                refusal = ExecutionCardRefusal.STALE_VERSION
            if refusal is not None:
                return refused(refusal)
            card = self._render_card(row)
        return ExecutionCardPresentation(
            ExecutionCardDisposition.UNCHANGED,
            card_id,
            card_version=expected_version,
            card=card,
        )

    def brief(
        self, card_id: int, *, expected_version: int
    ) -> ExecutionCardBrief:
        """The task, described for an agent that is not this one.

        A read: asking for it changes no workflow state, because a reader
        deciding to take the work elsewhere has not answered the card.
        """
        def refused(reason: ExecutionCardRefusal) -> ExecutionCardBrief:
            return ExecutionCardBrief(
                ExecutionCardDisposition.REFUSED, card_id, refusal=reason)

        if not _valid_identity(card_id, expected_version):
            return refused(ExecutionCardRefusal.INVALID_ARGUMENT)
        with closing(self._connect()) as connection:
            row = connection.execute(
                self._card_select() + " AND c.id=?", (card_id,)
            ).fetchone()
            if row is None:
                return refused(ExecutionCardRefusal.NOT_FOUND)
            if int(row["version"]) != expected_version:
                return refused(ExecutionCardRefusal.STALE_VERSION)
            if not _current_card(row):
                return refused(ExecutionCardRefusal.STALE_VERSION)
            card = _card(row, self._profile_registry)
        return ExecutionCardBrief(
            ExecutionCardDisposition.UNCHANGED,
            card_id,
            card_version=expected_version,
            text=task_brief(card),
        )

    def deliverables(
        self, card_id: int, *, expected_version: int
    ) -> ExecutionCardDeliverables:
        """Return prepared drafts from an eligible delivered review card.

        This is intentionally narrower than ``brief``: the button exists
        only on Plan Review and Result Review cards that have deliverables,
        and it is meaningful only while the reader still has that exact
        delivered card.  The read neither resolves the card nor advances any
        workflow state.
        """
        def refused(
            reason: ExecutionCardRefusal,
        ) -> ExecutionCardDeliverables:
            return ExecutionCardDeliverables(
                ExecutionCardDisposition.REFUSED, card_id, refusal=reason
            )

        if not _valid_identity(card_id, expected_version):
            return refused(ExecutionCardRefusal.INVALID_ARGUMENT)
        with closing(self._connect()) as connection:
            row = connection.execute(
                self._card_select() + " AND c.id=?", (card_id,)
            ).fetchone()
            refusal = _card_guard(row, expected_version)
            if (
                refusal is None
                and row["status"] != ExecutionCardStatus.DELIVERED
            ):
                refusal = ExecutionCardRefusal.INVALID_STATE
            if refusal is None and not _current_card(row):
                refusal = ExecutionCardRefusal.STALE_VERSION
            if refusal is not None:
                return refused(refusal)
            card = self._render_card(row)
            if (
                card.kind not in {
                    ExecutionCardKind.PLAN_REVIEW,
                    ExecutionCardKind.RESULT_REVIEW,
                }
                or not card.deliverables
            ):
                return refused(ExecutionCardRefusal.INVALID_STATE)
        return ExecutionCardDeliverables(
            ExecutionCardDisposition.UNCHANGED,
            card_id,
            card_version=expected_version,
            text=card_deliverables(card),
        )

    @property
    def serves_artifacts(self) -> bool:
        """Whether this deployment can serve result artifacts at all.

        Configuration, not state: it does not change while the process runs,
        which is what makes it worth reporting once at start-up rather than
        discovering one refusal at a time.
        """
        return self._artifact_root is not None

    def artifacts(
        self, card_id: int, *, expected_version: int
    ) -> ExecutionCardArtifacts:
        """List exact recorded files for one current delivered review card."""
        if self._artifact_root is None:
            return ExecutionCardArtifacts(
                ExecutionCardDisposition.REFUSED, card_id,
                refusal=ExecutionCardRefusal.ARTIFACTS_UNAVAILABLE,
            )
        if not _valid_identity(card_id, expected_version):
            return ExecutionCardArtifacts(
                ExecutionCardDisposition.REFUSED, card_id,
                refusal=ExecutionCardRefusal.INVALID_ARGUMENT,
            )
        with closing(self._connect()) as connection:
            row = connection.execute(
                self._card_select() + " AND c.id=?", (card_id,)
            ).fetchone()
            refusal = _artifact_card_refusal(row, expected_version)
            if refusal is not None:
                return ExecutionCardArtifacts(
                    ExecutionCardDisposition.REFUSED, card_id, refusal=refusal
                )
            artifact_rows = connection.execute(
                "SELECT ordinal,name,size_bytes FROM execution_result_artifacts "
                "WHERE result_id=? ORDER BY ordinal",
                (row["workflow_result_id"],),
            ).fetchall()
        return ExecutionCardArtifacts(
            ExecutionCardDisposition.UNCHANGED,
            card_id,
            card_version=expected_version,
            artifacts=tuple(
                ExecutionCardArtifact(
                    ordinal=int(item["ordinal"]), name=str(item["name"]),
                    size_bytes=int(item["size_bytes"]),
                ) for item in artifact_rows
            ),
        )

    def artifact(
        self, card_id: int, *, expected_version: int, ordinal: int
    ) -> ExecutionCardArtifacts:
        """Read one exact result artifact after validating its saved digest."""
        listed = self.artifacts(card_id, expected_version=expected_version)
        if not listed.accepted:
            return listed
        if isinstance(ordinal, bool) or not isinstance(ordinal, int) or ordinal < 0:
            return ExecutionCardArtifacts(
                ExecutionCardDisposition.REFUSED, card_id,
                refusal=ExecutionCardRefusal.INVALID_ARGUMENT,
            )
        with closing(self._connect()) as connection:
            row = connection.execute(
                self._card_select() + " AND c.id=?", (card_id,)
            ).fetchone()
            refusal = _artifact_card_refusal(row, expected_version)
            if refusal is not None:
                return ExecutionCardArtifacts(
                    ExecutionCardDisposition.REFUSED, card_id, refusal=refusal
                )
            record = connection.execute(
                "SELECT ordinal,relative_path,name,size_bytes,content_digest,"
                "run_directory FROM execution_result_artifacts "
                "WHERE result_id=? AND ordinal=?",
                (row["workflow_result_id"], ordinal),
            ).fetchone()
        if record is None:
            return ExecutionCardArtifacts(
                ExecutionCardDisposition.REFUSED, card_id,
                refusal=ExecutionCardRefusal.NOT_FOUND,
            )
        try:
            content = self._read_recorded_artifact(record)
        except (OSError, ValueError):
            return ExecutionCardArtifacts(
                ExecutionCardDisposition.REFUSED, card_id,
                refusal=ExecutionCardRefusal.INVALID_STATE,
            )
        return ExecutionCardArtifacts(
            ExecutionCardDisposition.UNCHANGED,
            card_id,
            card_version=expected_version,
            artifacts=(ExecutionCardArtifact(
                ordinal=int(record["ordinal"]), name=str(record["name"]),
                size_bytes=int(record["size_bytes"]), content=content,
            ),),
        )

    def _read_recorded_artifact(self, record: Mapping[str, object]) -> bytes:
        if self._artifact_root is None:
            raise ValueError("execution artifact root is unavailable")
        run_directory = Path(str(record["run_directory"]))
        relative = Path(str(record["relative_path"]))
        if run_directory.is_absolute() is False or relative.is_absolute():
            raise ValueError("recorded artifact path is invalid")
        if not run_directory.is_relative_to(self._artifact_root):
            raise ValueError("recorded artifact is outside configured root")
        run_parts = run_directory.relative_to(self._artifact_root).parts
        if any(part in {"", ".", ".."} for part in run_parts + relative.parts):
            raise ValueError("recorded artifact path is invalid")
        current = self._artifact_root
        for part in run_parts + relative.parts[:-1]:
            current = current / part
            info = current.lstat()
            if not stat.S_ISDIR(info.st_mode) or current.is_symlink():
                raise ValueError("recorded artifact path is unsafe")
        target = current / relative.name
        descriptor = os.open(target, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
        try:
            info = os.fstat(descriptor)
            expected_size = int(record["size_bytes"])
            if (not stat.S_ISREG(info.st_mode) or expected_size > MAX_ARTIFACT_BYTES
                    or info.st_size != expected_size):
                raise ValueError("recorded artifact content is unsafe")
            with os.fdopen(descriptor, "rb", closefd=False) as source:
                content = source.read(MAX_ARTIFACT_BYTES + 1)
        finally:
            os.close(descriptor)
        if len(content) != int(record["size_bytes"]):
            raise ValueError("recorded artifact changed during read")
        digest = hashlib.sha256(content).hexdigest()
        if not hmac.compare_digest(digest, str(record["content_digest"])):
            raise ValueError("recorded artifact digest differs")
        return content

    def detail(
        self, card_id: int, *, expected_version: int
    ) -> ExecutionCardDetail:
        """Return a bounded current-run view without claiming the card.

        Only a current, unheld queue card is disclosed.  A queue reader can
        therefore refresh a card it saw in ``due()`` but cannot probe a card
        being delivered to another consumer.  The result projection is kept
        deliberately separate from ``ExecutionReviewCard`` so paths,
        prompts, profile revisions, and other private provenance never cross
        this boundary.
        """
        def refused(reason: ExecutionCardRefusal) -> ExecutionCardDetail:
            return ExecutionCardDetail(
                ExecutionCardDisposition.REFUSED, card_id, refusal=reason
            )

        if not _valid_identity(card_id, expected_version):
            return refused(ExecutionCardRefusal.INVALID_ARGUMENT)
        now = self._clock_value().isoformat(timespec="seconds")
        with closing(self._connect()) as connection:
            row = connection.execute(
                self._card_select() + " AND c.id=?", (card_id,)
            ).fetchone()
            refusal = _card_guard(row, expected_version)
            if refusal is None and row["status"] != ExecutionCardStatus.PENDING:
                refusal = ExecutionCardRefusal.CLAIM_MISMATCH
            if refusal is None and row["workflow_status_current"] == WorkflowStatus.SNOOZED:
                due_at = row["workflow_due_at"]
                if due_at is not None and due_at > now:
                    refusal = ExecutionCardRefusal.INVALID_STATE
            if refusal is None and not _current_card(row):
                refusal = ExecutionCardRefusal.STALE_VERSION
            if refusal is not None:
                return refused(refusal)
            outcome = row["result_outcome"]
            return ExecutionCardDetail(
                ExecutionCardDisposition.UNCHANGED,
                card_id,
                card_version=expected_version,
                workflow_version=int(row["workflow_version_current"]),
                status=WorkflowStatus(row["workflow_status_current"]),
                phase=WorkflowPhase(row["workflow_phase_current"]),
                updated_at=str(row["workflow_updated_at"]),
                due_at=row["workflow_due_at"],
                completed_at=row["workflow_completed_at"],
                outcome=None if outcome is None else ExecutionOutcome(outcome),
                summary="" if row["summary"] is None else str(row["summary"]),
                work_digest="" if row["work_digest"] is None else str(row["work_digest"]),
                work_markdown="" if row["work_markdown"] is None else str(row["work_markdown"]),
                deliverables=_stored_collection(row["deliverables_json"]),
                failure_reason=row["workflow_failure_reason"],
                failure_exit_code=row["workflow_failure_exit_code"],
                failure_run_id=row["workflow_failure_run_id"],
            )

    def select_agent(
        self,
        card_id: int,
        *,
        expected_version: int,
        selection_token: str,
    ) -> ExecutionAgentSelectorResult:
        """Select an eligible exact profile and refresh its card."""
        if (
            not _valid_identity(card_id, expected_version)
            or not _valid_agent_selection_token(selection_token)
        ):
            return _agent_refused(
                card_id, ExecutionCardRefusal.INVALID_ARGUMENT
            )
        stamp = self._clock_value()
        now = stamp.isoformat(timespec="seconds")
        with closing(self._connect()) as connection:
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = connection.execute(
                    self._card_select() + " AND c.id=?", (card_id,)
                ).fetchone()
                refusal = _agent_card_refusal(row, expected_version)
                if refusal is not None:
                    connection.rollback()
                    return _agent_refused_row(card_id, row, refusal)
                current = self._render_card(row)
                phase = _agent_selector_phase(current)
                matches = [
                    profile
                    for profile in _eligible_profiles(
                        self._profile_registry, phase=phase
                    )
                    if _agent_selection_token(profile) == selection_token
                ]
                if len(matches) != 1:
                    connection.rollback()
                    return _agent_refused(
                        card_id, ExecutionCardRefusal.INVALID_ARGUMENT
                    )
                profile = matches[0]
                apply_selection = (
                    _apply_agent_selection
                    if current.kind is ExecutionCardKind.START
                    else _apply_plan_review_agent_selection
                )
                workflow = apply_selection(
                    connection,
                    int(row["task_id"]),
                    expected_version=int(row["workflow_version"]),
                    profile=profile,
                    stamp=stamp,
                )
                if workflow.disposition is WorkflowDisposition.REFUSED:
                    connection.rollback()
                    return _agent_refused_row(
                        card_id,
                        row,
                        _workflow_refusal(workflow.refusal),
                    )
                if workflow.disposition is WorkflowDisposition.UNCHANGED:
                    connection.rollback()
                    return ExecutionAgentSelectorResult(
                        ExecutionCardDisposition.UNCHANGED,
                        card_id,
                        card_version=expected_version,
                        card=current,
                    )
                card_version = expected_version + 1
                updated = connection.execute(
                    "UPDATE execution_review_cards SET version=?,"
                    "workflow_version=?,updated_at=? WHERE id=? AND version=? "
                    "AND status='delivered'",
                    (
                        card_version,
                        workflow.version,
                        now,
                        card_id,
                        expected_version,
                    ),
                )
                if updated.rowcount != 1:
                    raise TaskLedgerError("execution card state changed")
                self._event(
                    connection,
                    card_id=card_id,
                    task_id=int(row["task_id"]),
                    kind="refreshed",
                    card_version=card_version,
                    workflow_version=int(workflow.version),
                    action="agent",
                    now=now,
                )
                values = dict(row)
                values.update(
                    version=card_version,
                    workflow_version=workflow.version,
                    workflow_version_current=workflow.version,
                    workflow_agent_profile_id=profile.profile_id,
                    workflow_agent_profile_revision=profile.revision,
                )
                card = self._render_card(values)
                connection.commit()
                return ExecutionAgentSelectorResult(
                    ExecutionCardDisposition.APPLIED,
                    card_id,
                    card_version=card_version,
                    card=card,
                )
            except Exception:
                connection.rollback()
                raise

    def submit_input(
        self,
        card_id: int,
        *,
        expected_version: int,
        kind: str,
        value: str,
    ) -> ExecutionCardOperationResult:
        """Apply one version-bound discussion or reassignment response."""
        if (
            not _valid_identity(card_id, expected_version)
            or kind not in READER_INPUT_KINDS
            or not _valid_reader_input(kind, value)
        ):
            return _refused(card_id, ExecutionCardRefusal.INVALID_ARGUMENT)
        stamp = self._clock_value()
        now = stamp.isoformat(timespec="seconds")
        with closing(self._connect()) as connection:
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = connection.execute(
                    self._card_select() + " AND c.id=?", (card_id,)
                ).fetchone()
                refusal = _card_guard(row, expected_version)
                if (
                    refusal is None
                    and row["status"] != ExecutionCardStatus.DELIVERED
                ):
                    refusal = ExecutionCardRefusal.INVALID_STATE
                if refusal is None and not _current_card(row):
                    refusal = ExecutionCardRefusal.STALE_VERSION
                # A gate used to refuse both, on the reasoning that there
                # was no work yet to talk about. But the note a reader wants
                # to leave is most useful BEFORE the first pass, not after
                # reading a plan that ignored it, and a task is most often
                # noticed as someone else's at the moment it is offered.
                if (
                    refusal is None
                    and kind == "reassignment"
                    and row["owner"] == value
                ):
                    refusal = ExecutionCardRefusal.INVALID_STATE
                if refusal is not None:
                    connection.rollback()
                    return _refused_row(card_id, row, refusal)

                target_workflow_version = int(row["workflow_version"]) + 1
                connection.execute(
                    "INSERT INTO execution_reader_inputs("
                    "card_id,task_id,card_version,task_version,"
                    "workflow_version,target_workflow_version,kind,value,"
                    "prior_value,occurred_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
                    (
                        card_id,
                        int(row["task_id"]),
                        expected_version,
                        int(row["task_version"]),
                        int(row["workflow_version"]),
                        target_workflow_version,
                        kind,
                        value,
                        row["owner"] if kind == "reassignment" else None,
                        now,
                    ),
                )
                if kind == "discussion":
                    task_version = int(row["task_version"])
                    status = (
                        WorkflowStatus.AWAITING_START
                        if ExecutionCardKind(row["kind"])
                        is ExecutionCardKind.START
                        else WorkflowStatus.QUEUED
                    )
                    phase = (
                        WorkflowPhase(row["phase"])
                        if ExecutionCardKind(row["kind"])
                        is ExecutionCardKind.STEER
                        else WorkflowPhase.PLAN
                    )
                    event_kind = "discussion_requested"
                    resolution = "discuss"
                    workflow_update = connection.execute(
                        "UPDATE task_execution_workflows SET "
                        "status=?,phase=?,version=?,due_at=NULL,"
                        "claim_token_digest=NULL,claimed_at=NULL,"
                        "claim_heartbeat_at=NULL,claim_expires_at=NULL,"
                        "current_run_id=NULL,"
                        "failure_count=0,last_failure_reason=NULL,"
                        "last_failure_exit_code=NULL,last_failure_run_id=NULL,"
                        "last_failure_at=NULL,next_attempt_at=NULL,"
                        "parked_at=NULL,updated_at=?,completed_at=NULL "
                        "WHERE task_id=? AND version=?",
                        (
                            status,
                            phase,
                            target_workflow_version,
                            now,
                            int(row["task_id"]),
                            int(row["workflow_version"]),
                        ),
                    )
                else:
                    task_version = int(row["task_version"]) + 1
                    status = WorkflowStatus.AWAITING_START
                    phase = WorkflowPhase.PLAN
                    event_kind = "reassigned"
                    resolution = "reassign"
                    task_update = connection.execute(
                        "UPDATE tasks SET owner=?,version=?,updated_at=?,"
                        "owner_ref_version=1,owner_kind='external',"
                        "owner_speaker_id=NULL,"
                        "owner_canonical_speaker_id=NULL,"
                        "owner_speaker_registry_id=NULL,owner_pinned=1,"
                        "owner_provisional=0 "
                        "WHERE id=? AND version=? AND status='open'",
                        (
                            value,
                            task_version,
                            now,
                            int(row["task_id"]),
                            int(row["task_version"]),
                        ),
                    )
                    if task_update.rowcount != 1:
                        raise TaskLedgerError("task ownership state changed")
                    connection.execute(
                        "INSERT INTO task_owner_events("
                        "task_id,task_version,card_id,from_owner,to_owner,"
                        "occurred_at) VALUES(?,?,?,?,?,?)",
                        (
                            int(row["task_id"]),
                            task_version,
                            card_id,
                            row["owner"],
                            value,
                            now,
                        ),
                    )
                    workflow_update = connection.execute(
                        "UPDATE task_execution_workflows SET task_version=?,"
                        "status='awaiting_start',phase='plan',version=?,"
                        "due_at=NULL,claim_token_digest=NULL,claimed_at=NULL,"
                        "claim_heartbeat_at=NULL,claim_expires_at=NULL,"
                        "current_run_id=NULL,"
                        "failure_count=0,last_failure_reason=NULL,"
                        "last_failure_exit_code=NULL,last_failure_run_id=NULL,"
                        "last_failure_at=NULL,next_attempt_at=NULL,"
                        "parked_at=NULL,last_result_id=NULL,updated_at=?,"
                        "completed_at=NULL WHERE task_id=? AND version=?",
                        (
                            task_version,
                            target_workflow_version,
                            now,
                            int(row["task_id"]),
                            int(row["workflow_version"]),
                        ),
                    )

                if workflow_update.rowcount != 1:
                    raise TaskLedgerError("execution workflow state changed")

                TaskExecutionService._event(
                    connection,
                    int(row["task_id"]),
                    event_kind,
                    target_workflow_version,
                    task_version,
                    phase,
                    status,
                    now,
                )
                version = expected_version + 1
                updated = connection.execute(
                    "UPDATE execution_review_cards SET status='resolved',"
                    "version=?,resolution=?,resolved_at=?,updated_at=? "
                    "WHERE id=? AND version=? AND status='delivered'",
                    (
                        version,
                        resolution,
                        now,
                        now,
                        card_id,
                        expected_version,
                    ),
                )
                if updated.rowcount != 1:
                    raise TaskLedgerError("execution card state changed")
                self._event(
                    connection,
                    card_id=card_id,
                    task_id=int(row["task_id"]),
                    kind="resolved",
                    card_version=version,
                    workflow_version=target_workflow_version,
                    action=resolution,
                    now=now,
                )
                connection.commit()
                return ExecutionCardOperationResult(
                    ExecutionCardDisposition.APPLIED,
                    card_id,
                    card_version=version,
                    card_status=ExecutionCardStatus.RESOLVED,
                    workflow_version=target_workflow_version,
                    workflow_status=status,
                    workflow_phase=phase,
                )
            except Exception:
                connection.rollback()
                raise

    def comment_and_go(
        self,
        card_id: int,
        *,
        expected_version: int,
        value: str,
    ) -> ExecutionCardOperationResult:
        """Persist a reader note and advance this card in one transaction.

        The note targets the workflow version the chosen transition creates,
        so the next agent sees it exactly once.  Unlike ``discuss``, this
        does not first send the workflow back to planning and then require a
        second card decision.
        """
        if (
            not _valid_identity(card_id, expected_version)
            or not _valid_reader_input("discussion", value)
        ):
            return _refused(card_id, ExecutionCardRefusal.INVALID_ARGUMENT)
        stamp = self._clock_value()
        now = stamp.isoformat(timespec="seconds")
        with closing(self._connect()) as connection:
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = connection.execute(
                    self._card_select() + " AND c.id=?", (card_id,)
                ).fetchone()
                refusal = _card_guard(row, expected_version)
                if (
                    refusal is None
                    and row["status"] != ExecutionCardStatus.DELIVERED
                ):
                    refusal = ExecutionCardRefusal.INVALID_STATE
                if refusal is None and not _current_card(row):
                    refusal = ExecutionCardRefusal.STALE_VERSION
                kind = None if row is None else ExecutionCardKind(row["kind"])
                action = {
                    ExecutionCardKind.START: "start",
                    ExecutionCardKind.PLAN_REVIEW: "approve",
                    ExecutionCardKind.EXTERNAL_REVIEW: "approve",
                    ExecutionCardKind.STEER: "discuss",
                }.get(kind)
                if refusal is None and action is None:
                    refusal = ExecutionCardRefusal.INVALID_ACTION
                if (
                    refusal is None
                    and not _card_fits(self._render_card(row))
                ):
                    refusal = ExecutionCardRefusal.INVALID_STATE
                if refusal is not None:
                    connection.rollback()
                    return _refused_row(card_id, row, refusal)

                target_workflow_version = int(row["workflow_version"]) + 1
                connection.execute(
                    "INSERT INTO execution_reader_inputs("
                    "card_id,task_id,card_version,task_version,"
                    "workflow_version,target_workflow_version,kind,value,"
                    "prior_value,occurred_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
                    (
                        card_id,
                        int(row["task_id"]),
                        expected_version,
                        int(row["task_version"]),
                        int(row["workflow_version"]),
                        target_workflow_version,
                        "discussion",
                        value,
                        None,
                        now,
                    ),
                )
                if kind is ExecutionCardKind.START:
                    workflow = _apply_start_action(
                        connection,
                        int(row["task_id"]),
                        expected_version=int(row["workflow_version"]),
                        action=action,
                        stamp=stamp,
                    )
                elif kind is ExecutionCardKind.STEER:
                    workflow = _apply_steer_discussion(
                        connection, row, stamp=stamp
                    )
                else:
                    workflow = _apply_review_action(
                        connection,
                        int(row["task_id"]),
                        expected_version=int(row["workflow_version"]),
                        action=action,
                        stamp=stamp,
                    )
                if (
                    workflow.disposition is WorkflowDisposition.REFUSED
                    or workflow.version != target_workflow_version
                ):
                    connection.rollback()
                    return _refused_row(
                        card_id,
                        row,
                        ExecutionCardRefusal.INVALID_STATE
                        if workflow.disposition is not WorkflowDisposition.REFUSED
                        else _workflow_refusal(workflow.refusal),
                    )

                version = expected_version + 1
                updated = connection.execute(
                    "UPDATE execution_review_cards SET status='resolved',"
                    "version=?,claim_token_digest=NULL,claim_expires_at=NULL,"
                    "resolution=?,resolved_at=?,updated_at=? "
                    "WHERE id=? AND version=? AND status='delivered'",
                    (
                        version,
                        _stored_action(action),
                        now,
                        now,
                        card_id,
                        expected_version,
                    ),
                )
                if updated.rowcount != 1:
                    raise TaskLedgerError("execution card state changed")
                self._event(
                    connection,
                    card_id=card_id,
                    task_id=int(row["task_id"]),
                    kind="resolved",
                    card_version=version,
                    workflow_version=target_workflow_version,
                    action=action,
                    now=now,
                )
                connection.commit()
                return ExecutionCardOperationResult(
                    ExecutionCardDisposition.APPLIED,
                    card_id,
                    card_version=version,
                    card_status=ExecutionCardStatus.RESOLVED,
                    workflow_version=target_workflow_version,
                    workflow_status=workflow.status,
                    workflow_phase=workflow.phase,
                )
            except Exception:
                connection.rollback()
                raise

    def stats(self) -> ExecutionCardStats:
        """What is actually occupying the reader's surface.

        A card counts only while it can still be answered. Once the work
        behind it has moved on, its buttons are refused, so it is not
        holding a place in any sense the reader would recognise — but it
        used to be counted anyway. With room for one card, a single
        superseded card meant no card could ever be scheduled again, and
        the sweep that cancels it lives behind the scheduling call the
        drip had already returned from. The surface simply went quiet and
        stayed quiet.
        """
        with closing(self._connect()) as connection:
            rows = [
                row
                for row in connection.execute(
                    self._card_select()
                    + " AND c.status IN ('pending','delivering','delivered')"
                ).fetchall()
                if _current_card(row)
            ]
        counts: dict[str, int] = {}
        steer_counts: dict[str, int] = {}
        for row in rows:
            status = str(row["status"])
            target = steer_counts if row["kind"] == "steer" else counts
            target[status] = target.get(status, 0) + 1
        return ExecutionCardStats(
            pending=counts.get("pending", 0),
            delivering=counts.get("delivering", 0),
            delivered=counts.get("delivered", 0),
            active=sum(counts.values()),
            steer_pending=steer_counts.get("pending", 0),
            steer_delivering=steer_counts.get("delivering", 0),
            steer_delivered=steer_counts.get("delivered", 0),
        )

    def stats_scoped(self, *, consumer_digest: str) -> ExecutionCardScopedStats:
        if not _valid_digest(consumer_digest):
            raise TaskLedgerError("execution card consumer digest is invalid")
        with closing(self._connect()) as connection:
            row = connection.execute(
                "SELECT "
                "SUM(CASE WHEN status='pending' AND kind<>'steer' THEN 1 ELSE 0 END) AS pending,"
                "SUM(CASE WHEN status='delivering' AND kind<>'steer' AND consumer_digest=? THEN 1 ELSE 0 END) AS delivering,"
                "SUM(CASE WHEN status='delivered' AND kind<>'steer' AND consumer_digest=? THEN 1 ELSE 0 END) AS delivered,"
                "SUM(CASE WHEN status IN ('delivering','delivered') AND kind<>'steer' AND consumer_digest IS NOT NULL AND consumer_digest<>? THEN 1 ELSE 0 END) AS elsewhere,"
                "SUM(CASE WHEN status IN ('pending','delivering','delivered') AND kind<>'steer' THEN 1 ELSE 0 END) AS active,"
                "SUM(CASE WHEN status='pending' AND kind='steer' THEN 1 ELSE 0 END) AS steer_pending,"
                "SUM(CASE WHEN status='delivering' AND kind='steer' AND consumer_digest=? THEN 1 ELSE 0 END) AS steer_delivering,"
                "SUM(CASE WHEN status='delivered' AND kind='steer' AND consumer_digest=? THEN 1 ELSE 0 END) AS steer_delivered "
                # These numbers describe the surface a reader is looking
                # at, and a retired run summary is not on it.  Counting one
                # left behind by an earlier build would report it forever as
                # backlog waiting for an answer.
                "FROM execution_review_cards WHERE summary_only=0",
                (consumer_digest, consumer_digest, consumer_digest,
                 consumer_digest, consumer_digest),
            ).fetchone()
        return ExecutionCardScopedStats(*(int(row[name] or 0) for name in (
            "pending", "delivering", "delivered", "elsewhere", "active",
            "steer_pending", "steer_delivering", "steer_delivered",
        )))

    def count(self) -> int:
        with closing(self._connect()) as connection:
            return int(
                connection.execute(
                    "SELECT COUNT(*) FROM execution_review_cards"
                ).fetchone()[0]
            )

    def event_count(self) -> int:
        with closing(self._connect()) as connection:
            return int(
                connection.execute(
                    "SELECT COUNT(*) FROM execution_review_card_events"
                ).fetchone()[0]
            )

    def _poll_owner_holds(self, stamp: datetime, *, limit: int) -> None:
        """Refresh bounded durable holds without keeping a write lock."""
        now = stamp.isoformat(timespec="seconds")
        with closing(self._connect()) as connection:
            rows = connection.execute(
                _OWNER_HOLD_SELECT + " "
                "WHERE h.status='active' ORDER BY h.id LIMIT ?",
                (limit,),
            ).fetchall()
        for row in rows:
            if not _current_owner_hold(row):
                self._finish_owner_hold(row, now=now, reason="stale")
                continue
            if row["backstop_at"] <= now:
                self._finish_owner_hold(row, now=now, reason="backstop")
                continue
            if self._owner_condition is None:
                continue
            try:
                # By keyword. The one implementation of this hook takes
                # its arguments keyword-only, so calling it positionally
                # raised TypeError rather than answering -- and TypeError
                # is not what the `except` below catches, so it escaped
                # `schedule` and returned 500 for the whole call. Nothing
                # noticed until an owner hold went active, because the
                # loop this sits in has no rows to walk until then: the
                # card surface then stopped topping up entirely, on a
                # path whose only job is to keep it filled.
                result = self._owner_condition(
                    owner=str(row["owner_display"]),
                    owner_ref=_owner_ref(row),
                )
            except KnowledgeClientError:
                continue
            if not isinstance(result, OwnerUpcomingMeeting):
                continue
            self._record_owner_condition(row, result=result, now=now)

    def _record_owner_condition(
        self,
        row: Mapping[str, object],
        *,
        result: OwnerUpcomingMeeting,
        now: str,
    ) -> None:
        with closing(self._connect()) as connection:
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute("BEGIN IMMEDIATE")
            try:
                current = connection.execute(
                    _OWNER_HOLD_SELECT + " WHERE h.id=?",
                    (int(row["id"]),),
                ).fetchone()
                if (
                    current is None
                    or current["status"] != "active"
                    or not _current_owner_hold(current)
                ):
                    connection.rollback()
                    return
                row = current
                connection.execute(
                    "UPDATE task_execution_owner_holds SET "
                    "last_checked_at=?,last_evidence_revision=?,"
                    "last_match=? WHERE id=? AND status='active'",
                    (
                        result.checked_at,
                        result.evidence_revision,
                        int(result.match),
                        int(row["id"]),
                    ),
                )
                _owner_hold_event(
                    connection,
                    row,
                    kind="condition_checked",
                    matched=result.match,
                    evidence_revision=result.evidence_revision,
                    now=now,
                )
                connection.commit()
            except Exception:
                connection.rollback()
                raise
        if result.match:
            self._finish_owner_hold(row, now=now, reason="meeting")

    def _finish_owner_hold(
        self, row: Mapping[str, object], *, now: str, reason: str
    ) -> None:
        with closing(self._connect()) as connection:
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute("BEGIN IMMEDIATE")
            try:
                current = connection.execute(
                    _OWNER_HOLD_SELECT + " WHERE h.id=?",
                    (int(row["id"]),),
                ).fetchone()
                if current is None or current["status"] != "active":
                    connection.rollback()
                    return
                if not _current_owner_hold(current):
                    reason = "stale"
                row = current
                if reason == "stale":
                    connection.execute(
                        "UPDATE task_execution_owner_holds SET "
                        "status='cancelled',release_reason='stale',"
                        "released_at=? WHERE id=? AND status='active'",
                        (now, int(row["id"])),
                    )
                    _owner_hold_event(
                        connection, row, kind="cancelled", now=now
                    )
                    connection.commit()
                    return
                workflow_version = int(row["workflow_version"]) + 1
                updated = connection.execute(
                    "UPDATE task_execution_workflows SET "
                    "status='awaiting_start',phase='plan',version=?,"
                    "due_at=NULL,claim_token_digest=NULL,claimed_at=NULL,"
                    "claim_heartbeat_at=NULL,claim_expires_at=NULL,"
                    "current_run_id=NULL,"
                    "failure_count=0,last_failure_reason=NULL,"
                    "last_failure_exit_code=NULL,last_failure_run_id=NULL,"
                    "last_failure_at=NULL,next_attempt_at=NULL,"
                    "parked_at=NULL,updated_at=?,completed_at=NULL "
                    "WHERE task_id=? AND task_version=? AND version=? "
                    "AND status='snoozed' AND due_at=?",
                    (
                        workflow_version,
                        now,
                        int(row["task_id"]),
                        int(row["task_version"]),
                        int(row["workflow_version"]),
                        row["backstop_at"],
                    ),
                )
                if updated.rowcount != 1:
                    connection.execute(
                        "UPDATE task_execution_owner_holds SET "
                        "status='cancelled',release_reason='stale',"
                        "released_at=? WHERE id=? AND status='active'",
                        (now, int(row["id"])),
                    )
                    _owner_hold_event(
                        connection, row, kind="cancelled", now=now
                    )
                    connection.commit()
                    return
                connection.execute(
                    "UPDATE task_execution_owner_holds SET status='released',"
                    "release_reason=?,released_at=? "
                    "WHERE id=? AND status='active'",
                    (reason, now, int(row["id"])),
                )
                _owner_hold_event(
                    connection, row, kind="released", now=now
                )
                TaskExecutionService._event(
                    connection,
                    int(row["task_id"]),
                    "scheduled",
                    workflow_version,
                    int(row["task_version"]),
                    WorkflowPhase.PLAN,
                    WorkflowStatus.AWAITING_START,
                    now,
                )
                connection.commit()
            except Exception:
                connection.rollback()
                raise

    def _cancel_stale(
        self, connection: sqlite3.Connection, now: str
    ) -> int:
        rows = connection.execute(
            self._card_select()
            + " AND c.status IN ('pending','delivering','delivered') "
            "ORDER BY c.id"
        ).fetchall()
        cancelled = 0
        for row in rows:
            if _current_card(row):
                continue
            version = int(row["version"]) + 1
            connection.execute(
                "UPDATE execution_review_cards SET status='cancelled',"
                "version=?,claim_token_digest=NULL,claim_expires_at=NULL,"
                "superseded_delivery_ref=delivery_ref,"
                "superseded_transport=transport,"
                "transport=NULL,delivery_ref=NULL,"
                "resolved_at=?,updated_at=? WHERE id=? AND version=?",
                (version, now, now, int(row["id"]), int(row["version"])),
            )
            if row["transport"] is not None and row["delivery_ref"] is not None:
                connection.execute(
                    "INSERT OR IGNORE INTO execution_card_retractions("
                    "card_id,transport,delivery_ref,state,created_at,updated_at) "
                    "VALUES(?,?,?,'pending',?,?)",
                    (int(row["id"]), row["transport"], row["delivery_ref"], now, now),
                )
            self._event(
                connection,
                card_id=int(row["id"]),
                task_id=int(row["task_id"]),
                kind="cancelled",
                card_version=version,
                workflow_version=int(row["workflow_version"]),
                action=None,
                now=now,
            )
            cancelled += 1
        return cancelled

    @staticmethod
    def _band(
        connection: sqlite3.Connection,
        row: Mapping[str, object],
        consumer_role: str,
        consumer_digest: str,
    ) -> tuple[str, int]:
        """Name this card's capacity band and how much of it is already held.

        The two bands are disjoint by construction, which is the property
        that matters: a card can only ever be counted against the ceiling it
        is itself bounded by, so no band can be made to appear emptier -- or
        fuller -- by what is happening in another.

        Both exclude ``summary_only=1``.  Run summaries are retired and no
        longer produced, but rows written by an earlier build survive in the
        table, and a delivered one counted here would occupy a band that
        nothing can ever free.
        """
        if row["kind"] == ExecutionCardKind.STEER:
            suffix, predicate = "_steer", "summary_only=0 AND kind='steer'"
        else:
            suffix, predicate = "", "summary_only=0 AND kind<>'steer'"
        held = connection.execute(
            "SELECT count(*) FROM execution_review_cards WHERE "
            "status IN ('delivering','delivered') AND consumer_digest=? "
            "AND " + predicate,
            (consumer_digest,),
        ).fetchone()[0]
        return consumer_role + suffix, int(held)

    @staticmethod
    def _card_select() -> str:
        """Read reader-action cards, and only those.

        ``summary_only=1`` marked a run summary: an informational row that
        reported an automatically advanced phase and asked nothing.  Those
        are retired and nothing writes one any more, but rows written before
        that remain in the table, and a straggling build mid-promotion can
        still insert one.  Excluding them here is what keeps such a row
        inert rather than claimable -- it carries ``kind='result_review'``,
        so a read that admitted it would render a decision card for a phase
        the workflow has already moved past.
        """
        scope = "c.summary_only=0"
        return ((
            "SELECT c.*,t.text AS task_text,t.owner,t.owner_ref_version,"
            "t.owner_kind,t.owner_speaker_id,t.owner_canonical_speaker_id,"
            "t.owner_speaker_registry_id,t.owner_pinned,t.owner_provisional,"
            "t.due,"
            "t.status AS task_status_current,t.version AS task_version_current,"
            "w.status AS workflow_status_current,"
            "w.claimed_at AS workflow_claimed_at,"
            "w.failure_count AS workflow_failure_count,"
            "("
            # The attempt that failed is one below the version its failure
            # created, and the digest is scoped to the phase the workflow
            # is in now: an `execute` failure says nothing about a `plan`
            # pass that succeeded.
            " SELECT d.digest FROM execution_failure_digests AS d "
            " WHERE d.task_id=w.task_id AND d.phase=w.phase "
            " ORDER BY d.workflow_version DESC LIMIT 1"
            ") AS workflow_failure_digest,"
            # Cumulative history for the phase the workflow is in now,
            # derived rather than counted live.  `claim_next` resets
            # `failure_count` on park -- deliberately, so a retry does not
            # begin one slip from parking again -- which means the live
            # counter says how many attempts happened since the last park,
            # not how many have happened.  A workflow looping through
            # park, reader-answers-start, reclaim therefore reported "1
            # failed attempt" on its twentieth.  The event log cannot
            # drift from what happened, and scoping to the phase keeps a
            # plan that succeeded from being reported against an execute
            # that is stuck.
            "("
            " SELECT count(*) FROM task_execution_events AS e "
            " WHERE e.task_id=w.task_id AND e.kind='claimed' "
            " AND e.phase=w.phase AND e.task_version=w.task_version"
            ") AS workflow_phase_attempts,"
            "("
            " SELECT count(*) FROM task_execution_events AS e "
            " WHERE e.task_id=w.task_id AND e.kind='parked' "
            " AND e.phase=w.phase AND e.task_version=w.task_version"
            ") AS workflow_phase_parks,"
            "("
            " SELECT count(*) FROM task_execution_results AS r "
            " WHERE r.task_id=w.task_id AND r.phase=w.phase "
            " AND r.task_version=w.task_version"
            ") AS workflow_phase_results,"
            # How much agent time those attempts consumed: each claim until
            # whatever ended it.  A reader deciding whether to authorise
            # another run is deciding how to spend the next one of these.
            "("
            " SELECT CAST(ROUND(COALESCE(SUM(("
            "  julianday(COALESCE(("
            "   SELECT MIN(f.occurred_at) FROM task_execution_events AS f "
            "   WHERE f.task_id=e.task_id AND f.sequence>e.sequence "
            "   AND f.kind IN ('released','claim_expired',"
            "                  'retry_scheduled','parked','result_recorded')"
            "  ),e.occurred_at))-julianday(e.occurred_at))*86400),0)) "
            " AS INTEGER) FROM task_execution_events AS e "
            " WHERE e.task_id=w.task_id AND e.kind='claimed' "
            " AND e.phase=w.phase AND e.task_version=w.task_version"
            ") AS workflow_phase_seconds,"
            "w.last_failure_reason AS workflow_failure_reason,"
            "w.last_failure_exit_code AS workflow_failure_exit_code,"
            "w.last_failure_run_id AS workflow_failure_run_id,"
            "w.phase AS workflow_phase_current,"
            "w.version AS workflow_version_current,"
            "w.task_version AS workflow_task_version_current,"
            "w.updated_at AS workflow_updated_at,"
            "w.due_at AS workflow_due_at,"
            "w.completed_at AS workflow_completed_at,"
            "w.agent_profile_id AS workflow_agent_profile_id,"
            "w.agent_profile_revision AS workflow_agent_profile_revision,"
            "w.last_result_id AS workflow_result_id,"
            "r.task_id AS result_task_id,r.workflow_version AS result_version,"
            "r.task_version AS result_task_version,r.phase AS result_phase,"
            "r.outcome AS result_outcome,r.summary,r.work_markdown,"
            "r.work_digest,"
            "r.questions_json,r.external_actions_json,r.deliverables_json,"
            "r.repository_references_json,"
            "r.task_work_directory,r.task_kb_file,"
            "(SELECT min(h.created_at) "
            " FROM task_candidate_bindings AS b "
            " JOIN candidate_revision_history AS h "
            " ON h.candidate_id=b.candidate_id "
            " WHERE b.task_id=c.task_id) AS first_raised,"
            "(SELECT max(h.created_at) "
            " FROM task_candidate_bindings AS b "
            " JOIN candidate_revision_history AS h "
            " ON h.candidate_id=b.candidate_id "
            " WHERE b.task_id=c.task_id) AS last_mentioned,"
            # How many plans this task has already produced, and the last
            # thing the reader said. Counted rather than stored: a pass is a
            # result, so the ledger already knows, and a second column would
            # be a second thing to keep true.
            "(SELECT count(*)-1 FROM task_execution_results AS prior "
            " WHERE prior.task_id=c.task_id AND prior.phase=r.phase)"
            " AS revision_count,"
            "(SELECT i.value FROM execution_reader_inputs AS i "
            " WHERE i.task_id=c.task_id AND i.kind='discussion' "
            " ORDER BY i.sequence DESC LIMIT 1) AS revision_note,"
            # Whether this pass actually changed anything. Read from the
            # ledger's own definition of an answer rather than a second one
            # kept here: a card that says "unchanged" while the ledger would
            # have accepted the result as new is worse than no marker, and
            # two lists in two files drift the first time either is widened.
            "(SELECT " + " AND ".join(
                f"prior.{column} IS r.{column}" for column in _ANSWER_COLUMNS
            ) + " FROM task_execution_results AS prior "
            " WHERE prior.task_id=c.task_id AND prior.phase=r.phase "
            " AND prior.workflow_version<r.workflow_version "
            " ORDER BY prior.workflow_version DESC LIMIT 1) "
            " AS unchanged_from_previous,"
            # Where the task came from. A reader asked to authorise work on
            # an issue cannot answer without being told which issue.
            "(SELECT o.source_kind FROM task_candidate_bindings AS b "
            " JOIN candidate_inbox AS o ON o.candidate_id=b.candidate_id "
            " WHERE b.task_id=c.task_id AND b.relation='accepted') "
            " AS origin_kind,"
            "(SELECT o.source_record_id FROM task_candidate_bindings AS b "
            " JOIN candidate_inbox AS o ON o.candidate_id=b.candidate_id "
            " WHERE b.task_id=c.task_id AND b.relation='accepted') "
            " AS origin_record,"
            "(SELECT o.source_item_id FROM task_candidate_bindings AS b "
            " JOIN candidate_inbox AS o ON o.candidate_id=b.candidate_id "
            " WHERE b.task_id=c.task_id AND b.relation='accepted') "
            " AS origin_item,"
            # The task that reviewed an earlier state of the same thing.
            # An identity like `7/<state>` makes each state its own task, so
            # without this an agent starts from nothing every time a pull
            # request moves, and a reader cannot tell a second pass from a
            # duplicate.
            "(SELECT b2.task_id FROM task_candidate_bindings AS b2 "
            " JOIN candidate_inbox AS o2 "
            " ON o2.candidate_id=b2.candidate_id "
            " JOIN task_candidate_bindings AS b1 "
            " ON b1.task_id=c.task_id AND b1.relation='accepted' "
            " JOIN candidate_inbox AS o1 "
            " ON o1.candidate_id=b1.candidate_id "
            " WHERE b2.relation='accepted' AND b2.task_id<>c.task_id "
            " AND o2.source_kind=o1.source_kind "
            " AND o2.source_record_id=o1.source_record_id "
            " AND " + _ITEM_STEM.format(column="o2.source_item_id") + "="
            + _ITEM_STEM.format(column="o1.source_item_id") +
            " ORDER BY b2.task_id DESC LIMIT 1) AS prior_task_id,"
            # Recorded relations, as distinct from the derived predecessor
            # above: that one is inferred from source identity and cannot
            # reach across sources, carries no basis and cannot be taken
            # back. Both render through one path, so a reader is never shown
            # two answers to "what came before this".
            "(SELECT group_concat("
            " r.kind || char(31) ||"
            " (CASE WHEN r.subject_id=c.task_id THEN 'out' ELSE 'in' END)"
            " || char(31) ||"
            " (CASE WHEN r.subject_id=c.task_id THEN r.object_id"
            "       ELSE r.subject_id END) || char(31) ||"
            " COALESCE(r.note,''), char(30)) "
            " FROM task_relations AS r "
            " WHERE r.withdrawn_at IS NULL "
            "   AND (r.subject_id=c.task_id OR r.object_id=c.task_id)) "
            " AS task_relations,"
            "(SELECT h.payload_json FROM task_candidate_bindings AS b "
            " JOIN candidate_revision_history AS h "
            " ON h.candidate_id=b.candidate_id "
            " AND h.source_revision=b.source_revision "
            " WHERE b.task_id=c.task_id AND b.relation='accepted') "
            " AS origin_payload "
            "FROM execution_review_cards AS c "
            "JOIN tasks AS t ON t.id=c.task_id "
            "JOIN task_execution_workflows AS w ON w.task_id=c.task_id "
            "LEFT JOIN task_execution_results AS r ON r.result_id=c.result_id"
        ) + " WHERE " + scope)

    @staticmethod
    def _event(
        connection: sqlite3.Connection,
        *,
        card_id: int,
        task_id: int,
        kind: str,
        card_version: int,
        workflow_version: int,
        action: str | None,
        now: str,
        consumer_digest: str | None = None,
    ) -> None:
        connection.execute(
            "INSERT INTO execution_review_card_events("
            "card_id,task_id,kind,card_version,workflow_version,action,"
            "consumer_digest,occurred_at) VALUES(?,?,?,?,?,?,?,?)",
            (
                card_id,
                task_id,
                kind,
                card_version,
                workflow_version,
                action,
                consumer_digest,
                now,
            ),
        )

    def _connect(self) -> sqlite3.Connection:
        if not self.database_path.is_file() or self.database_path.is_symlink():
            raise TaskLedgerError("execution card database is not initialized")
        connection = sqlite3.connect(self.database_path, timeout=5)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout = 5000")
        version = int(connection.execute("PRAGMA user_version").fetchone()[0])
        if version != SCHEMA_VERSION:
            connection.close()
            raise TaskLedgerError(
                "execution card database schema is not supported"
            )
        try:
            CandidateInbox._require_schema(connection)
        except InboxError as exc:
            connection.close()
            raise TaskLedgerError(
                "execution card database schema is incomplete"
            ) from exc
        return connection

    def _render_card(
        self, row: Mapping[str, object]
    ) -> ExecutionReviewCard:
        return _card(
            row,
            self._profile_registry,
            reader_aliases=self._reader_aliases,
            condition_available=self._owner_condition is not None,
        )

    def _clock_value(self) -> datetime:
        value = self._clock()
        if value.tzinfo is None or value.utcoffset() is None:
            raise TaskLedgerError(
                "execution card clock must include a timezone"
            )
        return value.astimezone(timezone.utc)

    def _now(self) -> str:
        return self._clock_value().isoformat(timespec="seconds")


def execution_board_status(card: ExecutionReviewCard) -> str:
    """Return the one closed work-state token for a board card."""
    if not isinstance(card, ExecutionReviewCard):
        raise TaskLedgerError("execution board card is invalid")
    status = card.workflow_status
    if status is WorkflowStatus.AWAITING_REVIEW:
        return {
            ExecutionCardKind.PLAN_REVIEW: "plan_review",
            ExecutionCardKind.EXTERNAL_REVIEW: "external_review",
            ExecutionCardKind.RESULT_REVIEW: "result_review",
        }.get(card.kind, "ready_to_start")
    return {
        WorkflowStatus.AWAITING_START: "ready_to_start",
        WorkflowStatus.QUEUED: "queued",
        WorkflowStatus.RUNNING: "running",
        WorkflowStatus.SNOOZED: "snoozed",
        WorkflowStatus.PARKED: "parked",
        WorkflowStatus.COMPLETED: "completed",
        WorkflowStatus.CANCELLED: "cancelled",
    }[status]


def render_execution_review_card(
    card: ExecutionReviewCard,
) -> tuple[str, dict[str, list[list[dict[str, str]]]]]:
    if not isinstance(card, ExecutionReviewCard):
        raise TaskLedgerError("execution review card is invalid")
    plain = "\n".join(_card_lines(card))
    complete = "\n".join(_html_card_lines(card))
    approvable = len(complete.encode("utf-8")) <= MAX_CARD_BODY_BYTES
    body = (
        complete
        if approvable
        else _escape_bounded(
            plain,
            MAX_TRUNCATED_CARD_BODY_BYTES,
            suffix=(
                "\n\nContent is too long to approve in this card. "
                "Approval is disabled."
            ),
        )
    )
    keyboard = {
        "inline_keyboard": [
            [
                {
                    "text": label,
                    "callback_data": _callback(
                        card.id, card.version, action
                    ),
                }
                for label, action in row
            ]
            for row in _button_rows(card, approvable=approvable)
        ]
    }
    return body, keyboard


def render_execution_agent_selector(
    result: ExecutionAgentSelectorResult,
) -> tuple[str, dict[str, list[list[dict[str, str]]]]]:
    """Render one bounded Telegram-compatible agent choice view."""
    if not isinstance(result, ExecutionAgentSelectorResult):
        raise TaskLedgerError("execution agent selector is invalid")
    if not result.accepted or result.card is None or not result.options:
        raise TaskLedgerError("execution agent selector is unavailable")
    body, _ = render_execution_review_card(result.card)
    target = _agent_selector_label(result.card)
    suffix = f"\n\n<b>Choose the agent for {target}:</b>"
    if len((body + suffix).encode("utf-8")) > MAX_CARD_BODY_BYTES:
        body = _escape_bounded(
            "\n".join(_card_lines(result.card)),
            MAX_TRUNCATED_CARD_BODY_BYTES,
            suffix=f"\n\nChoose the agent for {target}:",
        )
        suffix = ""
    keyboard = {
        "inline_keyboard": [
            [{
                "text": (
                    f"✓ {option.display_name}"
                    if option.selected
                    else option.display_name
                ),
                "callback_data": _agent_callback(
                    result.card.id,
                    result.card.version,
                    option.selection_token,
                ),
            }]
            for option in result.options
        ]
    }
    return body + suffix, keyboard


def parse_execution_review_callback(
    value: object,
) -> tuple[int, int, str] | None:
    if not isinstance(value, str) or not 1 <= len(value) <= CALLBACK_DATA_LIMIT:
        return None
    parts = value.split("|")
    if len(parts) != 4 or parts[0] != CALLBACK_PREFIX:
        return None
    try:
        card_id = int(parts[1])
        version = int(parts[2])
    except ValueError:
        return None
    if not _valid_identity(card_id, version):
        return None
    if parts[1] != str(card_id) or parts[2] != str(version):
        return None
    if parts[3] not in {
        "start", "snooze", "cancel", "approve", "revise", "discuss",
        "comment_go", "deliverables",
        "done", "reassign", "drop", "agent", "brief", OWNER_HOLD_ACTION,
        *REVIEW_SNOOZE_ACTIONS,
    }:
        return None
    return card_id, version, parts[3]


def parse_execution_agent_callback(
    value: object,
) -> tuple[int, int, str] | None:
    if not isinstance(value, str) or not 1 <= len(value) <= CALLBACK_DATA_LIMIT:
        return None
    parts = value.split("|")
    if len(parts) != 4 or parts[0] != AGENT_CALLBACK_PREFIX:
        return None
    try:
        card_id = int(parts[1])
        version = int(parts[2])
    except ValueError:
        return None
    if not _valid_identity(card_id, version):
        return None
    if parts[1] != str(card_id) or parts[2] != str(version):
        return None
    if not _valid_agent_selection_token(parts[3]):
        return None
    return card_id, version, parts[3]


def _kind_for_workflow(row: Mapping[str, object]) -> ExecutionCardKind:
    if (
        row["status"] == WorkflowStatus.RUNNING
        and int(row["steer_while_running"]) == 1
    ):
        return ExecutionCardKind.STEER
    if (
        row["status"] in {
            WorkflowStatus.AWAITING_START,
            WorkflowStatus.SNOOZED,
            # A workflow that gave up is asked about like one that has not
            # begun: the question is whether to run it, and the card says
            # it already tried.
            WorkflowStatus.PARKED,
        }
        and (
            row["last_result_id"] is None
            # A parked workflow may carry a result from an earlier phase
            # that succeeded. It is still a "run this again?" question, and
            # requiring no result here left it matching the eligibility
            # query and no card kind at all — which raised, and took the
            # whole sweep down with it, including cards that were fine.
            or row["status"] == WorkflowStatus.PARKED
        )
    ):
        return ExecutionCardKind.START
    if (
        row["status"] in {WorkflowStatus.AWAITING_REVIEW, WorkflowStatus.SNOOZED}
        and row["phase"] == WorkflowPhase.PLAN
        and row["outcome"] == ExecutionOutcome.AWAITING_PLAN
    ):
        return ExecutionCardKind.PLAN_REVIEW
    if (
        row["status"] in {WorkflowStatus.AWAITING_REVIEW, WorkflowStatus.SNOOZED}
        and row["phase"] == WorkflowPhase.EXECUTE
        and row["outcome"] == ExecutionOutcome.AWAITING_EXTERNAL
    ):
        return ExecutionCardKind.EXTERNAL_REVIEW
    if (
        row["status"] in {
            WorkflowStatus.AWAITING_REVIEW,
            WorkflowStatus.COMPLETED,
            WorkflowStatus.SNOOZED,
        }
        and row["outcome"] in {
            ExecutionOutcome.COMPLETED,
            ExecutionOutcome.DECLINED,
            ExecutionOutcome.INELIGIBLE,
        }
    ):
        return ExecutionCardKind.RESULT_REVIEW
    raise TaskLedgerError("execution workflow cannot be rendered as a card")


def _current_card(row: Mapping[str, object]) -> bool:
    try:
        kind = ExecutionCardKind(row["kind"])
        if (
            row["task_status_current"] != TaskStatus.OPEN
            or int(row["task_version_current"]) != int(row["task_version"])
            or int(row["workflow_task_version_current"])
            != int(row["task_version"])
            or int(row["workflow_version_current"])
            != int(row["workflow_version"])
            or row["workflow_phase_current"] != row["phase"]
        ):
            return False
        if kind is ExecutionCardKind.START:
            return (
                row["result_id"] is None
                and row["workflow_status_current"]
                in {
                    WorkflowStatus.AWAITING_START,
                    WorkflowStatus.SNOOZED,
                    WorkflowStatus.PARKED,
                }
            )
        if kind is ExecutionCardKind.STEER:
            return (
                row["result_id"] is None
                and row["workflow_status_current"] == WorkflowStatus.RUNNING
            )
        expected = {
            ExecutionCardKind.PLAN_REVIEW: {ExecutionOutcome.AWAITING_PLAN},
            ExecutionCardKind.EXTERNAL_REVIEW: {
                ExecutionOutcome.AWAITING_EXTERNAL
            },
            ExecutionCardKind.RESULT_REVIEW: {
                ExecutionOutcome.COMPLETED,
                ExecutionOutcome.DECLINED,
                ExecutionOutcome.INELIGIBLE,
            },
        }[kind]
        statuses = {
            WorkflowStatus.AWAITING_REVIEW,
            WorkflowStatus.SNOOZED,
        }
        if kind is ExecutionCardKind.RESULT_REVIEW:
            statuses.add(WorkflowStatus.COMPLETED)
        return (
            row["workflow_status_current"] in statuses
            and row["workflow_result_id"] == row["result_id"]
            and row["result_task_id"] == row["task_id"]
            and row["result_task_version"] == row["task_version"]
            and int(row["result_version"]) < int(row["workflow_version"])
            and row["result_phase"] == row["phase"]
            and row["result_outcome"] in expected
        )
    except (KeyError, TypeError, ValueError):
        return False


@dataclass(frozen=True)
class CardRelation:
    """One recorded relation, from the point of view of the card's task."""

    kind: str
    other_task_id: int
    #: True when this card's task is the subject — the one doing the
    #: superseding. The direction decides which sentence a reader is shown.
    outgoing: bool
    note: str | None


#: `kind`, direction, other task, and an optional note, one relation per row.
_RELATION_FIELDS = 4


def _stored_relations(value: object) -> tuple[CardRelation, ...]:
    """Parse the relations the card query collected for this task."""
    if not isinstance(value, str) or not value:
        return ()
    out: list[CardRelation] = []
    for record in value.split("\x1e"):
        parts = record.split("\x1f")
        if len(parts) != _RELATION_FIELDS:
            continue
        kind, direction, other, note = parts
        if kind not in task_relations.KINDS or direction not in ("out", "in"):
            continue
        try:
            other_id = int(other)
        except ValueError:
            continue
        out.append(CardRelation(kind, other_id, direction == "out",
                                note or None))
    return tuple(out)


def _card(
    row: Mapping[str, object],
    registry: AgentProfileRegistry,
    *,
    reader_aliases: frozenset[str] = frozenset(),
    condition_available: bool = False,
) -> ExecutionReviewCard:
    try:
        kind = ExecutionCardKind(row["kind"])
        profile_id = str(row["workflow_agent_profile_id"])
        profile_revision = str(row["workflow_agent_profile_revision"])
        try:
            profile = registry.resolve(profile_id, profile_revision)
            profile_name = profile.display_name
        except AgentProfileError:
            if kind is ExecutionCardKind.START:
                raise
            profile_name = profile_id
        owner_display = canonical_owner_display(row["owner"], row["owner_kind"])
        return ExecutionReviewCard(
            id=int(row["id"]),
            task_id=int(row["task_id"]),
            task_version=int(row["task_version"]),
            workflow_version=int(row["workflow_version"]),
            work_revision_id=(
                int(row["work_revision_id"]) if row["work_revision_id"] is not None else None
            ),
            kind=kind,
            phase=WorkflowPhase(row["phase"]),
            result_id=row["result_id"],
            status=ExecutionCardStatus(row["status"]),
            version=int(row["version"]),
            created_at=str(row["created_at"]),
            workflow_status=WorkflowStatus(row["workflow_status_current"]),
            failure_count=int(row["workflow_failure_count"] or 0),
            phase_attempts=int(row["workflow_phase_attempts"] or 0),
            phase_parks=int(row["workflow_phase_parks"] or 0),
            phase_results=int(row["workflow_phase_results"] or 0),
            phase_seconds=int(row["workflow_phase_seconds"] or 0),
            failure_reason=str(row["workflow_failure_reason"] or ""),
            failure_exit_code=row["workflow_failure_exit_code"],
            failure_run_id=row["workflow_failure_run_id"],
            failure_digest=_bounded_failure_digest(
                row["workflow_failure_digest"]
            ),
            agent_profile_id=profile_id,
            agent_profile_revision=profile_revision,
            agent_display_name=profile_name,
            task_text=str(row["task_text"]),
            owner=owner_display,
            due=row["due"],
            first_raised=row["first_raised"],
            last_mentioned=row["last_mentioned"],
            owner_hold_eligible=not bool(owner_hold_reason := _owner_hold_reason(
                row,
                owner_display,
                reader_aliases=reader_aliases,
                condition_available=condition_available,
            )),
            owner_hold_reason=owner_hold_reason,
            summary="" if row["summary"] is None else str(row["summary"]),
            work_markdown=(
                ""
                if row["work_markdown"] is None
                else str(row["work_markdown"])
            ),
            work_digest=(
                "" if row["work_digest"] is None
                else str(row["work_digest"])
            ),
            steer_digest=(
                "" if row["steer_digest"] is None
                else str(row["steer_digest"])
            ),
            claimed_at=row["workflow_claimed_at"],
            questions=_stored_lines(row["questions_json"]),
            external_actions=_stored_collection(row["external_actions_json"]),
            deliverables=_stored_collection(row["deliverables_json"]),
            repository_references=_stored_repository_references(
                row["repository_references_json"]),
            outcome=(
                None
                if row["result_outcome"] is None
                else ExecutionOutcome(row["result_outcome"])
            ),
            revisions=max(0, int(row["revision_count"] or 0)),
            revision_note=str(row["revision_note"] or ""),
            unchanged_from_previous=bool(row["unchanged_from_previous"]),
            origin_kind=str(row["origin_kind"] or ""),
            origin_record=str(row["origin_record"] or ""),
            origin_item=str(row["origin_item"] or ""),
            prior_task_id=(
                None if row["prior_task_id"] is None
                else int(row["prior_task_id"])
            ),
            relations=_stored_relations(row["task_relations"]),
            task_work_directory=str(row["task_work_directory"] or ""),
            task_kb_file=str(row["task_kb_file"] or ""),
            origin_sources=stored_origin_sources(row["origin_payload"]),
        )
    except (AgentProfileError, KeyError, TypeError, ValueError) as exc:
        raise TaskLedgerError("execution review card state is invalid") from exc


def _owner_hold_reason(
    row: Mapping[str, object],
    owner_display: str | None,
    *,
    reader_aliases: frozenset[str],
    condition_available: bool,
) -> str:
    if not condition_available or not reader_aliases:
        return "Meeting-aware hold is unavailable on this card service."
    if owner_display in {None, "(unassigned)"}:
        return "Owner needs confirmation before it can be held to a meeting."
    if normalized_owner(owner_display) in reader_aliases:
        return "Until next meeting applies only to another owner."
    if row["owner_kind"] not in {"person", "external"}:
        return "Until next meeting applies only to an individual owner."
    if row["owner_ref_version"] != 1 or row["owner_provisional"] != 0:
        return "Owner identity needs confirmation before it can be held to a meeting."
    scoped = (
        row["owner_speaker_id"],
        row["owner_canonical_speaker_id"],
        row["owner_speaker_registry_id"],
    )
    if all(value is None for value in scoped) or all(
            isinstance(value, str) and bool(value) for value in scoped):
        return ""
    return "Owner identity needs confirmation before it can be held to a meeting."


def _owner_ref(row: Mapping[str, object]) -> dict[str, object]:
    return {
        "kind": row["owner_kind"],
        "speaker_id": row["owner_speaker_id"],
        "canonical_speaker_id": row["owner_canonical_speaker_id"],
        "speaker_registry_id": row["owner_speaker_registry_id"],
        "pinned": bool(row["owner_pinned"]),
        "provisional": bool(row["owner_provisional"]),
    }


def _current_owner_hold(row: Mapping[str, object]) -> bool:
    return (
        row["task_status_current"] == TaskStatus.OPEN
        and row["task_version_current"] == row["task_version"]
        and row["workflow_task_version_current"] == row["task_version"]
        and row["workflow_version_current"] == row["workflow_version"]
        and row["workflow_status_current"] == WorkflowStatus.SNOOZED
        and row["workflow_due_at"] == row["backstop_at"]
        and canonical_owner_display(
            row["owner_current"], row["owner_kind_current"]
        ) == row["owner_display"]
        and row["owner_ref_version_current"] == row["owner_ref_version"]
        and row["owner_kind_current"] == row["owner_kind"]
        and row["owner_speaker_id_current"] == row["owner_speaker_id"]
        and row["owner_canonical_speaker_id_current"]
        == row["owner_canonical_speaker_id"]
        and row["owner_speaker_registry_id_current"]
        == row["owner_speaker_registry_id"]
        and row["owner_pinned_current"] == row["owner_pinned"]
        and row["owner_provisional_current"] == row["owner_provisional"]
    )


def _apply_owner_hold(
    connection: sqlite3.Connection,
    row: Mapping[str, object],
    *,
    stamp: datetime,
) -> WorkflowOperationResult:
    now = stamp.isoformat(timespec="seconds")
    wake = (stamp + OWNER_HOLD_INTERVAL).isoformat(timespec="seconds")
    task_id = int(row["task_id"])
    workflow_version = int(row["workflow_version"]) + 1
    updated = connection.execute(
        "UPDATE task_execution_workflows SET status='snoozed',phase='plan',"
        "version=?,due_at=?,claim_token_digest=NULL,claimed_at=NULL,"
        "claim_heartbeat_at=NULL,claim_expires_at=NULL,failure_count=0,"
        "current_run_id=NULL,"
        "last_failure_reason=NULL,last_failure_exit_code=NULL,"
        "last_failure_run_id=NULL,last_failure_at=NULL,next_attempt_at=NULL,"
        "parked_at=NULL,updated_at=?,completed_at=NULL "
        "WHERE task_id=? AND version=? AND task_version=? "
        "AND status IN ('awaiting_start','snoozed','parked')",
        (
            workflow_version,
            wake,
            now,
            task_id,
            int(row["workflow_version"]),
            int(row["task_version"]),
        ),
    )
    if updated.rowcount != 1:
        return WorkflowOperationResult(
            WorkflowDisposition.REFUSED,
            task_id,
            refusal=WorkflowRefusal.STALE_WORKFLOW,
        )
    cursor = connection.execute(
        "INSERT INTO task_execution_owner_holds("
        "task_id,task_version,workflow_version,status,owner_display,"
        "owner_ref_version,owner_kind,owner_speaker_id,"
        "owner_canonical_speaker_id,owner_speaker_registry_id,owner_pinned,"
        "owner_provisional,backstop_at,created_at) "
        "VALUES(?,?,?,'active',?,?,?,?,?,?,?,?,?,?)",
        (
            task_id,
            int(row["task_version"]),
            workflow_version,
            canonical_owner_display(row["owner"], row["owner_kind"]),
            int(row["owner_ref_version"]),
            row["owner_kind"],
            row["owner_speaker_id"],
            row["owner_canonical_speaker_id"],
            row["owner_speaker_registry_id"],
            int(row["owner_pinned"]),
            int(row["owner_provisional"]),
            wake,
            now,
        ),
    )
    hold_row = {"id": int(cursor.lastrowid), "task_id": task_id}
    _owner_hold_event(connection, hold_row, kind="created", now=now)
    TaskExecutionService._event(
        connection,
        task_id,
        "snoozed",
        workflow_version,
        int(row["task_version"]),
        WorkflowPhase.PLAN,
        WorkflowStatus.SNOOZED,
        now,
    )
    return WorkflowOperationResult(
        WorkflowDisposition.APPLIED,
        task_id,
        workflow_version,
        WorkflowStatus.SNOOZED,
        WorkflowPhase.PLAN,
        wake_at=wake,
        agent_profile_id=row["workflow_agent_profile_id"],
        agent_profile_revision=row["workflow_agent_profile_revision"],
    )


def _owner_hold_event(
    connection: sqlite3.Connection,
    row: Mapping[str, object],
    *,
    kind: str,
    now: str,
    matched: bool | None = None,
    evidence_revision: str | None = None,
) -> None:
    connection.execute(
        "INSERT INTO task_execution_owner_hold_events("
        "hold_id,task_id,kind,matched,evidence_revision,occurred_at) "
        "VALUES(?,?,?,?,?,?)",
        (
            int(row["id"]),
            int(row["task_id"]),
            kind,
            None if matched is None else int(matched),
            evidence_revision,
            now,
        ),
    )


@dataclass(frozen=True)
class CardRecord:
    """One thing an agent recorded, as much of it as a card can show.

    A plain line an agent wrote arrives with `text` set and nothing else,
    which is what every result written before records existed looks like.
    A structured one fills the fields a reader needs in order to approve
    it without opening anything: what the action needs before it can run,
    or who a draft is addressed to and what it says.
    """

    text: str
    requires: str = ""
    channel: str = ""
    target: str = ""
    label: str = ""
    recipient: str = ""
    subject: str = ""

    @property
    def structured(self) -> bool:
        return bool(self.requires or self.channel or self.target or self.label
                    or self.recipient or self.subject)


@dataclass(frozen=True)
class RepositoryReference:
    """Validated forge evidence retained separately from result prose."""

    kind: str
    url: str


def _stored_lines(value: object) -> tuple[str, ...]:
    """Questions are prose, so a record is flattened back to its sentence."""
    return tuple(record.text for record in _stored_collection(value))


def _stored_collection(value: object) -> tuple[CardRecord, ...]:
    if value is None:
        return ()
    try:
        parsed = json.loads(value)
    except (TypeError, ValueError):
        raise TaskLedgerError("execution review card result is invalid") from None
    if not isinstance(parsed, list):
        raise TaskLedgerError("execution review card result is invalid")
    records: list[CardRecord] = []
    for item in parsed:
        if isinstance(item, str):
            records.append(CardRecord(item))
            continue
        if not isinstance(item, dict):
            raise TaskLedgerError(
                "execution review card result is invalid")
        text = item.get("action") or item.get("body") or ""
        fields = {
            name: item.get(name) or ""
            for name in ("requires", "channel", "target", "label", "recipient",
                         "subject")
        }
        if not isinstance(text, str) or not text or any(
            not isinstance(field_value, str)
            for field_value in fields.values()
        ):
            raise TaskLedgerError(
                "execution review card result is invalid")
        records.append(CardRecord(text, **fields))
    return tuple(records)


def _stored_repository_references(
    value: object,
) -> tuple[RepositoryReference, ...]:
    """Read only the bounded forge-reference shape the result validator wrote."""
    if value is None:
        return ()
    try:
        parsed = json.loads(value)
    except (TypeError, ValueError):
        raise TaskLedgerError("execution review card result is invalid") from None
    if not isinstance(parsed, list):
        raise TaskLedgerError("execution review card result is invalid")
    references: list[RepositoryReference] = []
    for item in parsed:
        if (
            not isinstance(item, dict)
            or set(item) != {"kind", "url"}
            or item.get("kind") not in {"pull-request", "commit", "check"}
            or not isinstance(item.get("url"), str)
            or not item["url"].startswith("https://github.com/")
        ):
            raise TaskLedgerError("execution review card result is invalid")
        references.append(RepositoryReference(item["kind"], item["url"]))
    return tuple(references)


#: The part of an identifier before its state, if it has one. A review
#: task is `7/<state>`; an issue is just `42`. Written once because it has
#: to mean the same thing on both sides of a comparison.
_ITEM_STEM = (
    "(CASE WHEN instr({column},'/')>0 "
    "THEN substr({column},1,instr({column},'/')-1) ELSE {column} END)"
)

#: A brief is meant to be pasted somewhere else, so it carries no
#: identifiers only this machine can resolve and no capability of any kind.
MAX_BRIEF_CHARS = 12_000
MAX_BRIEF_BYTES = 24_000
MAX_DELIVERABLES_CHARS = 12_000
MAX_DELIVERABLES_BYTES = 24_000


def task_brief(card: ExecutionReviewCard) -> str:
    """One self-contained description of this task, for an outside agent.

    Not the prompt this agent runs. That one is written for a worker with
    bounded local commands — `context`, `search`, `record`, `act` — and
    pasted elsewhere it describes tools that do not exist and a recording
    protocol that cannot be followed. What an outside reader needs is the
    work: what is being asked, where it lives, what is already known, and
    what would count as done.

    Deliberately plain text. It is going into a chat box somewhere else,
    and markup that renders here is noise there.
    """
    lines = [card.task_text.strip() or f"Task {card.task_id}"]
    if card.origin_record and card.origin_item:
        source_url = origin_url(
            kind=card.origin_kind,
            record=card.origin_record,
            item=card.origin_item,
        )
        lines.append("")
        if source_url:
            lines.append(f"Source: {source_url}")
        elif card.origin_sources:
            shown_kind = card.origin_kind.replace("_", " ").title()
            lines.append(f"Source: {shown_kind or 'Unknown source'}")
        else:
            lines.append(f"Source: {card.origin_record} {card.origin_item}")
    if card.origin_sources:
        lines += ["", "Source evidence:"]
        for source in card.origin_sources:
            role = source.role.replace("_", " ")
            lines.append(f"- {source.name} ({role})")
            lines.extend(f"  {line}" for line in source.extract.splitlines())
    if card.owner:
        lines.append(f"Owner: {card.owner}")
    if card.due:
        lines.append(f"Due: {card.due}")
    if card.summary.strip():
        lines += ["", "Where this stands:", card.summary.strip()]
    if card.questions:
        lines += ["", "Open questions:"]
        lines += [f"- {question}" for question in card.questions]
    if card.external_actions:
        lines += ["", "Effects this would need, none of them taken yet:"]
        for record in card.external_actions:
            lines.append(f"- {record.text}")
            if record.requires:
                lines.append(f"  still needs: {record.requires}")
    if card.work_markdown.strip():
        lines += ["", "Work so far:", card.work_markdown.strip()]
    for record in card.deliverables:
        if not record.text.strip():
            continue
        heading = record.label or "Draft"
        lines += ["", f"{heading}:"]
        if record.recipient:
            lines.append(f"To: {record.recipient}")
        if record.subject:
            lines.append(f"Subject: {record.subject}")
        lines.append(record.text.strip())
    brief = "\n".join(lines).strip()
    if (
        len(brief) > MAX_BRIEF_CHARS
        or len(brief.encode("utf-8")) > MAX_BRIEF_BYTES
    ):
        # Truncated instructions stop mid-sentence, which is worse than a
        # short brief that says so.
        suffix = "\n\n[…truncated. Open the source for the rest.]"
        character_limit = MAX_BRIEF_CHARS - len(suffix)
        byte_limit = MAX_BRIEF_BYTES - len(suffix.encode("utf-8"))
        fragments: list[str] = []
        size = 0
        for character in brief[:character_limit]:
            width = len(character.encode("utf-8"))
            if size + width > byte_limit:
                break
            fragments.append(character)
            size += width
        brief = "".join(fragments).rstrip() + suffix
    return brief


def card_deliverables(card: ExecutionReviewCard) -> str:
    """One bounded Markdown message containing a card's prepared drafts.

    The transport, rather than the execution authority, chooses how Markdown
    looks in its chat UI.  Keeping the source Markdown here preserves the
    deliverable a reader was asked to review, while the bounded response keeps
    a malicious or accidentally huge result from turning a tap into an
    unbounded loopback response.
    """
    if card.kind not in {
        ExecutionCardKind.PLAN_REVIEW,
        ExecutionCardKind.RESULT_REVIEW,
    } or not card.deliverables:
        raise TaskLedgerError("execution card has no review deliverables")
    lines = ["# Deliverables"]
    for record in card.deliverables:
        lines.extend(("", f"## {record.label or 'Prepared draft'}"))
        if record.recipient:
            lines.append(f"To: {record.recipient}")
        if record.subject:
            lines.append(f"Subject: {record.subject}")
        if record.channel:
            lines.append(f"Channel: {record.channel}")
        lines.extend(("", record.text))
    return _bounded_deliverables("\n".join(lines).strip())


def _bounded_failure_digest(value: object) -> str:
    """A stored digest, or "" for anything that is not one.

    The column bounds this already; re-bounding here means a card cannot
    be broken by a row written before that bound existed, and means a
    NULL and a blank reach the renderer as the same thing.
    """
    if not isinstance(value, str):
        return ""
    text = " ".join(value.split()).strip()
    return text[:MAX_FAILURE_DIGEST_CARD_CHARS]


def _bounded_deliverables(value: str) -> str:
    """Fit an outbound deliverables message within the read contract."""
    suffix = "\n\n[…truncated. Open the task working folder for the rest.]"
    if (
        len(value) <= MAX_DELIVERABLES_CHARS
        and len(value.encode("utf-8")) <= MAX_DELIVERABLES_BYTES
    ):
        return value
    character_limit = MAX_DELIVERABLES_CHARS - len(suffix)
    byte_limit = MAX_DELIVERABLES_BYTES - len(suffix.encode("utf-8"))
    fragments: list[str] = []
    size = 0
    for character in value[:character_limit]:
        width = len(character.encode("utf-8"))
        if size + width > byte_limit:
            break
        fragments.append(character)
        size += width
    return "".join(fragments).rstrip() + suffix

def _continues_lines(
    card: ExecutionReviewCard, *, html: bool
) -> list[str]:
    """Name the task that looked at an earlier state of the same thing.

    Some sources are identified by state as well as by thing, because
    looking at a pull request as it stood on Monday and as it stands on
    Thursday are two jobs. Without naming the first, an agent starts from
    nothing each time, and a reader cannot tell a second pass from a
    duplicate card.

    There are two heading builders, so this lives in one place rather than
    being written twice and drifting.
    """
    lines: list[str] = []
    named: set[int] = set()
    for relation in card.relations:
        other = relation.other_task_id
        if relation.kind == "duplicate_of":
            shown = f"Same task as T{other}"
        elif relation.outgoing:
            shown = f"Continues T{other}"
        else:
            shown = f"Continued by T{other}"
        if relation.note:
            note = _escape(relation.note) if html else relation.note
            shown = f"{shown} — {note}"
        named.add(other)
        lines.append(f"↩ <b>{shown}</b>" if html else f"↩ {shown}")
    # The derived predecessor, unless a recorded relation already names it.
    # Two sentences about the same pair is worse than either alone.
    if card.prior_task_id is not None and card.prior_task_id not in named:
        shown = f"Continues T{card.prior_task_id}"
        lines.append(f"↩ <b>{shown}</b>" if html else f"↩ {shown}")
    return lines


def _origin_lines(card: ExecutionReviewCard, *, html: bool) -> list[str]:
    """Say where the work came from, and make it reachable.

    A gate asks the reader to authorise work on something. Naming the task
    is not the same as naming the thing: two issues can share a title, and
    an issue number is what the reader will search for afterwards.
    """
    return shared_origin_lines(
        kind=card.origin_kind,
        record=card.origin_record,
        item=card.origin_item,
        sources=card.origin_sources,
        html_output=html,
    )


def _card_date(value: str | None) -> str:
    return "" if not value else str(value)[:10]


def _approximate_duration(seconds: int) -> str:
    """Agent time as a reader thinks about it, or nothing at all.

    Deliberately coarse.  This number exists to answer "is this worth
    another run?", and a figure to the second would invite it to be read as
    accounting rather than as the order of magnitude it is.
    """
    if seconds < 60:
        return ""
    hours, remainder = divmod(int(seconds), 3600)
    minutes = remainder // 60
    if hours and minutes:
        return f"{hours}h {minutes}m"
    if hours:
        return f"{hours}h"
    return f"{minutes}m"


def _start_card_lines(
    card: ExecutionReviewCard, *, html: bool
) -> list[str]:
    """Render the established pre-work decision card."""
    handle = f"T{card.task_id}"
    if html:
        head = f"🚦 <b>Start this task?</b>  <code>{handle}</code>"
        task = f"<b>{_escape(card.task_text)}</b>"
    else:
        head = f"🚦 Start this task?  {handle}"
        task = card.task_text
    lines = [head, "", task]
    if card.owner:
        lines.append(
            f"👤 <b>Owner:</b> {_escape(card.owner)}"
            if html else f"👤 Owner: {card.owner}"
        )
    if card.owner_hold_reason:
        lines.append(
            f"🗓 <i>{_escape(card.owner_hold_reason)}</i>"
            if html else f"🗓 {card.owner_hold_reason}"
        )
    first = _card_date(card.first_raised)
    if first:
        lines.append(
            f"📌 <b>First raised:</b> {_escape(first)}"
            if html else f"📌 First raised: {first}"
        )
    last = _card_date(card.last_mentioned)
    if last and last != first:
        lines.append(
            f"🕑 <b>Last mentioned:</b> {_escape(last)}"
            if html else f"🕑 Last mentioned: {last}"
        )
    lines.extend(_origin_lines(card, html=html))
    lines.extend(_continues_lines(card, html=html))
    # Which agent would run this, before it runs. A reader who cannot see
    # it cannot tell that a pull request is about to be reviewed by a
    # compatibility profile, which is how one review was lost.
    agent = (_escape(card.agent_display_name) if html
             else card.agent_display_name)
    lines.append(f"🤖 <b>Agent:</b> {agent}" if html else f"🤖 Agent: {agent}")
    if card.workflow_status is WorkflowStatus.PARKED:
        # A reader who is never told has no way to distinguish a task
        # nobody has reached from one the agent abandoned.
        # The count across every park, not the count since the last one.
        # A reader told "1 failed attempt" answers Start, because that is
        # what a first hiccup deserves; the same card on the twentieth
        # attempt deserves a different answer, and used to look identical.
        attempts = max(card.phase_attempts, card.failure_count)
        stopped = (
            f"⚠️ Stopped after {attempts} failed attempt"
            f"{'s' if attempts != 1 else ''}"
            + (f" in {card.phase.value}" if card.phase_attempts else "")
            + (f", across {card.phase_parks} parks"
               if card.phase_parks > 1 else "")
            + (f" ({card.failure_reason})" if card.failure_reason else "")
        )
        lines.append(f"<b>{_escape(stopped)}</b>" if html else stopped)
        # Whether anything was ever recorded for this phase is the fact
        # that most changes the answer, and it was not on the card at all.
        recorded = (
            "no result recorded for this phase" if not card.phase_results
            else f"{card.phase_results} result"
                 f"{'s' if card.phase_results != 1 else ''} recorded"
                 " for this phase"
        )
        spent = _approximate_duration(card.phase_seconds)
        history = (
            f"⏱ {spent} of agent time, {recorded}" if spent
            else f"⏱ {recorded[0].upper()}{recorded[1:]}"
        )
        lines.append(f"<b>{_escape(history)}</b>" if html else history)
        if card.failure_exit_code is not None:
            diagnostic = f"Last agent exit code: {card.failure_exit_code}"
            lines.append(
                f"<b>{_escape(diagnostic)}</b>" if html else diagnostic
            )
        if card.failure_run_id is not None:
            reference = f"Run diagnostic: {card.failure_run_id}"
            lines.append(
                f"<code>{_escape(reference)}</code>" if html else reference
            )
        if card.failure_digest:
            # The difference between a reader who can judge whether to
            # retry and one who is guessing. A reason and an exit code say
            # the process stopped; this says what stopped it.
            lines.append("")
            lines.append(
                f"🔍 <b>Why it stopped:</b> {_escape(card.failure_digest)}"
                if html else f"🔍 Why it stopped: {card.failure_digest}"
            )
        if card.failure_reason == "context_exhausted":
            return lines + [
                "",
                "This work did not fit the runtime context window. Automatic "
                "retries are stopped; reduce its scope or split it before "
                "starting another run.",
            ]
        # The blanket claim that nothing was recorded is now checked rather
        # than asserted: a workflow can park in a phase that did record, and
        # telling the reader otherwise contradicts the line above it.
        explanation = (
            "Continue tries again. The runs so far left nothing recorded."
            if not card.phase_results
            else "Continue tries again from where this phase already got to."
        )
        if card.phase_parks > 1 and not card.phase_results:
            explanation += (
                " This has already stopped and been restarted "
                f"{card.phase_parks} times without recording anything, so a "
                "further identical attempt is unlikely to end differently."
            )
        return lines + ["", explanation]
    explanation = "No agent has looked at this yet. "
    explanation += (
        "<b>Continue</b> starts the investigation."
        if html else "Continue starts the investigation."
    )
    return [*lines, "", explanation]


def _heading_lines(card: ExecutionReviewCard, *, html: bool) -> list[str]:
    """Identify the task before describing it.

    A card the reader cannot name is a card they cannot refer to, ask about,
    or find again. The identifier goes first for the same reason it does in
    GW: on a phone the first line is often the whole notification.
    """
    handle = f"T{card.task_id}"
    title = "Task workflow"
    if html:
        head = f"🤖 <b>{title}</b>  <code>{handle}</code>"
        body = f"<b>{_escape(card.task_text)}</b>"
    else:
        head = f"🤖 {title}  {handle}"
        body = card.task_text
    lines = [head, body, ""]
    phase = _phase_name(card.phase)
    if card.kind is ExecutionCardKind.START:
        # The phase names what WOULD run. Naming it here would read as
        # though it already had, which is the one thing a start gate must
        # not imply — unless it has. A workflow parked after `plan` was
        # planned, approved and attempted, so calling it "not started"
        # hides the very history the reader needs to judge the retry.
        if card.workflow_status is WorkflowStatus.PARKED:
            phase = f"{phase} — stopped here"
        else:
            phase = "not started"
    elif card.outcome is ExecutionOutcome.COMPLETED:
        # Marked rather than renamed: the same card carrying its last
        # update, recognisable at a glance as finished. The prose that
        # used to follow -- "close it with Mark as done" -- is now the
        # label on the button directly below, so it was the card saying
        # the same thing twice.
        phase = f"{phase} ✅ done"
    # One chip line rather than four labelled ones. Phase, agent,
    # revision and owner are all answers to "which run of what is this",
    # and none of them is the question the card asks; four stacked labels
    # made them look like four things to read before reaching one.
    chips = [phase]
    if card.kind is not ExecutionCardKind.START:
        chips.append(card.agent_display_name)
    if card.revisions:
        chips.append(f"rev {card.revisions}")
    if card.owner:
        chips.append(card.owner)
    if card.due:
        chips.append(f"due {card.due}")
    chip = " · ".join(chip for chip in chips if chip)
    lines.append(f"<i>{_escape(chip)}</i>" if html else chip)
    if card.unchanged_from_previous:
        # Precise about WHAT is unchanged. "Same answer" is a claim about the
        # whole card, and a reader who reads it above an effect list they are
        # being asked to authorise has been told something the card cannot
        # know. This says only what was actually compared.
        note = (
            "⚠️ Unchanged from the previous revision — "
            "this pass produced no new answer"
        )
        lines.append(f"<b>{_escape(note)}</b>" if html else note)
    if card.kind is ExecutionCardKind.START:
        # Provenance belongs to the card that FIRST asks. A start gate is
        # that card here -- it proposes work on something the reader may
        # not have seen -- and the task card is that card on the other
        # surface. By the time a plan, a result or an external effect comes
        # back, the reader has already been shown where the task came from
        # and has answered about it once, so quoting the sources again is
        # a third telling: on one card in use the handoff quoted the task
        # text that was already the card's second line, and the transcript
        # said the same thing a third time in speech. Roughly 1,400
        # characters of it sat between the task title and "External action
        # awaiting your approval", which is the line the card exists to
        # put in front of someone.
        #
        # The evidence is not lost: it is on the task card, in the
        # candidate payload, and in the KB task file this card names under
        # Review files.
        lines.extend(_origin_lines(card, html=html))
    lines.extend(_continues_lines(card, html=html))
    return lines


def _steer_card_lines(
    card: ExecutionReviewCard, *, html: bool
) -> list[str]:
    """Render a live-run control without implying live note injection."""
    handle = f"T{card.task_id}"
    phase = card.phase.value.replace("_", " ")
    agent = _escape(card.agent_display_name) if html else card.agent_display_name
    heading = (
        f"🧭 <b>Run in progress</b>  <code>{handle}</code>"
        if html else f"🧭 Run in progress  {handle}"
    )
    task = f"<b>{_escape(card.task_text)}</b>" if html else card.task_text
    lines = [heading, "", task, f"🤖 {'<b>Agent:</b>' if html else 'Agent:'} {agent}",
             f"⏳ {'<b>Phase:</b>' if html else 'Phase:'} {phase}"]
    elapsed = _elapsed_steer_duration(card.claimed_at, card.created_at)
    if elapsed is not None:
        lines.append(
            f"🕑 <b>Running for at least:</b> {elapsed}"
            if html else f"🕑 Running for at least: {elapsed}"
        )
    if card.steer_digest:
        lines.extend(("", (
            f"🔍 <b>What it is doing:</b> {_escape(card.steer_digest)}"
            if html else f"🔍 What it is doing: {card.steer_digest}"
        )))
    lines.extend(("", (
        "Update stops this pass and starts a new one with your note; "
        "the running agent cannot read it mid-pass."
    )))
    return lines


def _elapsed_steer_duration(
    started_at: str | None, observed_at: str,
) -> str | None:
    """Describe the elapsed runtime at the point this card was raised.

    The card is a durable presentation, so use its creation time rather than
    a renderer's wall clock. That keeps repeated views truthful and avoids a
    clock-dependent rendering result while still reporting the duration that
    made the run worth announcing.
    """
    if not started_at:
        return None
    try:
        started = datetime.fromisoformat(started_at)
        observed = datetime.fromisoformat(observed_at)
    except ValueError:
        return None
    if started.tzinfo is None or observed.tzinfo is None:
        return None
    seconds = int((observed - started).total_seconds())
    if seconds < 0:
        return None
    if seconds < 60:
        return f"{seconds} second{'s' if seconds != 1 else ''}"
    minutes, seconds = divmod(seconds, 60)
    if minutes < 60:
        return f"{minutes} minute{'s' if minutes != 1 else ''}"
    hours, minutes = divmod(minutes, 60)
    if minutes:
        return (
            f"{hours} hour{'s' if hours != 1 else ''} "
            f"{minutes} minute{'s' if minutes != 1 else ''}"
        )
    return f"{hours} hour{'s' if hours != 1 else ''}"


def _asked_for_lines(card: ExecutionReviewCard, *, html: bool) -> list[str]:
    """The reader's own words, quoted back.

    Without them a third pass reads exactly like a first, and an hour later
    the reader cannot recall what they asked this one to fix.
    """
    if not (card.revisions and card.revision_note):
        return []
    shown = card.revision_note
    if len(shown) > MAX_REVISION_NOTE_CHARS:
        shown = shown[:MAX_REVISION_NOTE_CHARS].rstrip() + "…"
    if html:
        return ["", "<b>You asked for:</b>",
                "<blockquote>" + _escape(shown) + "</blockquote>"]
    return ["", "You asked for:", shown]


#: A `[label](target)` produced by `review_links`. Anything else it returns
#: is a record identifier rather than a destination.
_ADDRESSABLE_LINK_RE = re.compile(r"\[[^\]]*\]\([^)\s]+\)")
_GITHUB_REPOSITORY_RE = re.compile(r"^github\.com/([^/\s]+)/([^/\s]+)$")


def _review_lines(card: ExecutionReviewCard, *, html: bool) -> list[str]:
    """Put the durable evidence and forge references before the long work."""
    # No working folder and no KB path. Two absolute filesystem paths,
    # each long enough to wrap twice, neither of them tappable on the
    # phone this card is read on: three lines that could not be acted on
    # from the surface they appeared on. GW's card never carried them.
    # The files still exist and the task handle still names them.
    lines: list[str] = []
    repository = _repository_review_line(card, html=html)
    if repository:
        lines.extend(("", repository))
    if card.repository_references:
        lines.extend(("", "<b>Repository evidence:</b>" if html
                      else "Repository evidence:"))
        for reference in card.repository_references:
            label = {
                "pull-request": "Pull request",
                "commit": "Commit",
                "check": "Check",
            }[reference.kind]
            if html:
                lines.append(
                    f'• <a href="{_escape(reference.url)}">{label}</a>')
            else:
                lines.append(f"- [{label}]({reference.url})")
    links = review_links(
        "\n".join((
            card.summary,
            card.work_markdown,
            *(question for question in card.questions),
            *(record.text for record in card.external_actions),
            *(record.text for record in card.deliverables),
        )),
        origin_kind=card.origin_kind,
        origin_record=card.origin_record,
        origin_item=card.origin_item,
    )
    # Only the ones that are links. `review_links` also yields a plain
    # `<record> #<item>` for an origin it cannot address, which is right for
    # the archive file -- a durable record should name its source however it
    # can -- and wrong here: on the card it renders as a bare digest under a
    # heading promising somewhere to go, duplicating the `From:` line a few
    # lines above and going nowhere. One card in use carried a bare record
    # digest and an action anchor as its only "link" -- nothing a reader
    # could open, search for, or recognise.
    addressable = [
        link for link in links if _ADDRESSABLE_LINK_RE.fullmatch(link)
        and not any(reference.url in link
                    for reference in card.repository_references)
    ]
    if addressable:
        lines.extend(("", "<b>Review links:</b>" if html else "Review links:"))
        lines.extend(
            f"• {_markdown_inline(link)}" if html else f"- {link}"
            for link in addressable
        )
    return lines


def _repository_review_line(card: ExecutionReviewCard, *, html: bool) -> str:
    """Name a forge repository when the task origin identifies one.

    A card can translate ``Issue #17`` into a destination, but the label
    alone does not tell a reader which repository the destination belongs to.
    Keep this deliberately narrow: an unknown record is not a repository and
    must not become an invented or untappable context line.
    """
    match = _GITHUB_REPOSITORY_RE.fullmatch(card.origin_record)
    if match is None:
        return ""
    name = f"{match.group(1)}/{match.group(2)}"
    target = "https://github.com/" + urllib.parse.quote(name, safe="/")
    if html:
        return f'<b>Repository:</b> <a href="{target}">{_escape(name)}</a>'
    return f"Repository: [{name}]({target})"


def _card_lines(card: ExecutionReviewCard) -> list[str]:
    if card.kind is ExecutionCardKind.START:
        return _start_card_lines(card, html=False)
    if card.kind is ExecutionCardKind.STEER:
        return _steer_card_lines(card, html=False)
    if card.kind is ExecutionCardKind.EXTERNAL_REVIEW:
        lines = [
            *_heading_lines(card, html=False),
            "",
            "External action awaiting your approval:",
            *_listed(card.external_actions, empty="None supplied."),
            "",
            "Approve only if these exact external effects are intended.",
            "",
            f"Summary: {card.summary}",
            *_review_lines(card, html=False),
            *_asked_for_lines(card, html=False),
        ]
        if card.questions:
            lines.extend(("", "Needs your input:",
                      *[f"- {q}" for q in card.questions]))
        if card.deliverables:
            # The effect being approved is often "send this". A reader
            # cannot judge that from a summary of it.
            lines.extend(("", "Deliverables:", *_drafted(card.deliverables)))
        return lines
    if card.kind is ExecutionCardKind.RESULT_REVIEW:
        lines = [
            *_heading_lines(card, html=False),
            "",
            f"Outcome: {card.outcome}",
            f"Summary: {card.summary}",
            *_review_lines(card, html=False),
            *_asked_for_lines(card, html=False),
        ]
        if card.questions:
            lines.extend(("", "Needs your input:",
                      *[f"- {q}" for q in card.questions]))
        if card.deliverables:
            lines.extend(_advisory(
                card.deliverables, heading="Deliverables", html=False,
                render=_drafted))
        lines.extend(_work_lines(card, label="Work", html=False))
        return lines
    lines = [
        *_heading_lines(card, html=False),
        "",
        f"Summary: {card.summary}",
        *_review_lines(card, html=False),
        *_asked_for_lines(card, html=False),
    ]
    if card.questions:
        lines.extend(("", "Needs your input:",
                      *[f"- {q}" for q in card.questions]))
    if card.deliverables:
        lines.extend(_advisory(
            card.deliverables, heading="Deliverables", html=False,
            render=_drafted))
    lines.extend(_work_lines(card, label="Plan", html=False))
    return lines


#: What a phase is called to a reader. The enum names the machinery; these
#: name the thing the reader is being asked about.
PHASE_NAMES = {
    WorkflowPhase.PLAN: "plan refinement",
    WorkflowPhase.EXECUTE: "plan execution",
    WorkflowPhase.EXTERNAL_ACTION: "external action",
}


def _phase_name(phase: WorkflowPhase) -> str:
    return PHASE_NAMES.get(phase, phase.value)


def _record_detail_lines(record: CardRecord) -> list[str]:
    """The sub-lines under one record: what it needs, and where it goes."""
    details = []
    if record.requires:
        details.append("Needs: " + record.requires)
    if record.channel and record.channel.casefold() not in record.text.casefold():
        details.append("Channel: " + record.channel)
    if record.target:
        details.append("Target: " + record.target)
    return details


def _work_excerpt(value: str) -> tuple[str, int]:
    """The opening of the work, cut on a line boundary, and what is left.

    Whole lines, because the renderer reads a line at a time: a cut in the
    middle of one turns a heading into body text or a table row into a
    stray pipe, which misrepresents the plan rather than shortening it. The
    first line is kept even when it alone exceeds the budget -- a plan whose
    opening line is 4,000 characters still has to show the reader something
    -- and only that case is cut mid-line.
    """
    if len(value) <= MAX_WORK_EXCERPT_CHARS:
        return value, 0
    kept: list[str] = []
    used = 0
    for line in value.split("\n"):
        if kept and used + len(line) + 1 > MAX_WORK_EXCERPT_CHARS:
            break
        used += len(line) + 1
        kept.append(line)
    while kept and not kept[-1].strip():
        kept.pop()
    excerpt = "\n".join(kept)
    if len(excerpt) > MAX_WORK_EXCERPT_CHARS:
        excerpt = excerpt[:MAX_WORK_EXCERPT_CHARS].rstrip()
    return excerpt, len(value) - len(excerpt)


def _work_lines(
    card: ExecutionReviewCard, *, label: str, html: bool
) -> list[str]:
    """The bounded plan or work, and where the rest of it is.

    The pointer is not decoration. A reader who cannot tell that the text
    stopped early will read a truncated plan as the whole plan and approve
    it, which is the one failure a length limit could introduce.
    """
    if card.work_digest:
        # A few sentences ABOUT the whole plan beat the first screenful OF
        # it. The excerpt below can only ever show the opening, and an
        # agent's opening is its framing -- "Task", "Current state",
        # "Evidence consulted" -- while the thing the reader has to agree
        # with is usually somewhere in the middle. On one real plan the
        # excerpt reached none of the eight thousand characters that held
        # the actual recommendation; the digest led with it.
        subject = label.lower()
        pointer = (
            f"<i>Condensed — the full {subject} is in "
            f"<code>result-work.md</code> in the task working folder.</i>"
            if html else
            f"Condensed — the full {subject} is in result-work.md in the "
            f"task working folder."
        )
        body = (_markdown_lines(card.work_digest) if html
                else [card.work_digest])
        return ["", f"<b>{label}:</b>" if html else f"{label}:", *body,
                pointer]
    if not card.work_markdown:
        return []
    excerpt, remaining = _work_excerpt(card.work_markdown)
    if html:
        lines = ["", f"<b>{label}:</b>", *_markdown_lines(excerpt)]
    else:
        lines = ["", f"{label}:", excerpt]
    if not remaining:
        return lines
    where = "in the task working folder"
    subject = label.lower()
    if html:
        lines.append(
            f"<i>… {remaining} more characters — the full {subject} is in "
            f"<code>result-work.md</code> {where}.</i>"
        )
    else:
        lines.append(
            f"... {remaining} more characters — the full {subject} is in "
            f"result-work.md {where}."
        )
    return lines


def _bounded_draft_body(value: str) -> str:
    """One draft, bounded, and honest about having been bounded."""
    if len(value) <= MAX_DELIVERABLE_BODY_CHARS:
        return value
    return (value[:MAX_DELIVERABLE_BODY_CHARS].rstrip()
            + "\n… (truncated — full text in result-work.md)")


def _advisory(
    values: Sequence[CardRecord],
    *,
    heading: str,
    html: bool,
    render,
) -> list[str]:
    """A capped list of records that DESCRIBE work, with the count of any
    it did not show. Nothing is hidden silently: a list that says three of
    seven reads as a summary, and one that says three reads as all of them.
    """
    shown = tuple(values)[:MAX_ADVISORY_RECORDS]
    hidden = len(values) - len(shown)
    head = f"<b>{heading}:</b>" if html else f"{heading}:"
    lines = ["", head, *render(shown)]
    if hidden:
        more = f"and {hidden} more — see result-work.md"
        lines.append(f"• <i>{_escape(more)}</i>" if html else f"- {more}")
    return lines


def _listed(values: Sequence[CardRecord], *, empty: str = "None.") -> list[str]:
    if not values:
        return [empty]
    lines: list[str] = []
    for record in values:
        lines.append(f"- {record.text}")
        lines.extend(f"  {detail}"
                     for detail in _record_detail_lines(record))
    return lines


def _drafted(values: Sequence[CardRecord]) -> list[str]:
    """A prepared draft, shown in full rather than named.

    A deliverable a reader cannot read is a deliverable they cannot approve,
    so the body goes on the card. Anything without one is just a line.
    """
    lines: list[str] = []
    for record in values:
        if not record.structured:
            lines.append(f"- {record.text}")
            continue
        heading = record.label or "Prepared draft"
        lines.extend(("", heading))
        if record.recipient:
            lines.append("To: " + record.recipient)
        if record.subject:
            lines.append("Subject: " + record.subject)
        lines.append(_bounded_draft_body(record.text))
    return lines


def _html_card_lines(card: ExecutionReviewCard) -> list[str]:
    if card.kind is ExecutionCardKind.START:
        return _start_card_lines(card, html=True)
    if card.kind is ExecutionCardKind.STEER:
        return _steer_card_lines(card, html=True)
    if card.kind is ExecutionCardKind.EXTERNAL_REVIEW:
        lines = [
            *_heading_lines(card, html=True),
            "",
            "<b>External action awaiting your approval:</b>",
            *_html_listed(card.external_actions, empty="None supplied."),
            "",
            "Approve only if these exact external effects are intended.",
            "",
            *_labelled_html_lines("Summary", card.summary),
            *_review_lines(card, html=True),
            *_asked_for_lines(card, html=True),
        ]
        if card.questions:
            lines.extend((
                "",
                "<b>Needs your input:</b>",
                *_html_question_lines(card.questions),
            ))
        if card.deliverables:
            # The effect being approved is often "send this". A reader
            # cannot judge that from a summary of it.
            lines.extend((
                "",
                "<b>Deliverables:</b>",
                *_html_drafted(card.deliverables),
            ))
        return lines
    if card.kind is ExecutionCardKind.RESULT_REVIEW:
        lines = [
            *_heading_lines(card, html=True),
            "",
            *_labelled_html_lines("Outcome", str(card.outcome)),
            *_labelled_html_lines("Summary", card.summary),
            *_review_lines(card, html=True),
            *_asked_for_lines(card, html=True),
        ]
        if card.questions:
            lines.extend(("", "<b>Needs your input:</b>",
                          *_html_question_lines(card.questions)))
        if card.deliverables:
            lines.extend(_advisory(
                card.deliverables, heading="Deliverables", html=True,
                render=_html_drafted))
        lines.extend(_work_lines(card, label="Work", html=True))
        return lines
    lines = [
        *_heading_lines(card, html=True),
        "",
        *_labelled_html_lines("Summary", card.summary),
        *_review_lines(card, html=True),
        *_asked_for_lines(card, html=True),
    ]
    if card.questions:
        lines.extend((
            "",
            "<b>Needs your input:</b>",
            *_html_question_lines(card.questions),
        ))
    if card.deliverables:
        lines.extend(_advisory(
            card.deliverables, heading="Deliverables", html=True,
            render=_html_drafted))
    # No "potential external actions" here. Nothing is authorised at this
    # phase, the plan below already says what the agent means to do, and
    # the card that actually asks -- the external review -- lists the
    # effects in full under a sentence that is the whole point of it.
    # Listing them twice taught the reader to skim the list that matters.
    lines.extend(_work_lines(card, label="Plan", html=True))
    return lines


def _html_listed(
    values: Sequence[CardRecord], *, empty: str = "None."
) -> list[str]:
    if not values:
        return [empty]
    lines: list[str] = []
    for record in values:
        segments = _escaped_source_lines(record.text)
        lines.append(f"• {segments[0]}")
        lines.extend(f"  {segment}" for segment in segments[1:])
        for detail in _record_detail_lines(record):
            if detail.startswith("Target: https://"):
                target = detail.removeprefix("Target: ")
                lines.append(
                    f'  Target: <a href="{_escape(target)}">'
                    f'{_escape(target)}</a>'
                )
            else:
                lines.append(f"  {_escape(detail)}")
    return lines


def _html_question_lines(values: Sequence[str]) -> list[str]:
    lines: list[str] = []
    for value in values:
        segments = _escaped_source_lines(value)
        lines.append(f"• {segments[0]}")
        lines.extend(f"  {segment}" for segment in segments[1:])
    return lines


def _html_drafted(values: Sequence[CardRecord]) -> list[str]:
    """A prepared draft in full, so it can be judged without opening a file.

    The body goes in a <pre> block: it is someone else's text, often an
    email, and re-flowing it would misrepresent what would actually be
    sent.
    """
    lines: list[str] = []
    for record in values:
        if not record.structured:
            segments = _escaped_source_lines(record.text)
            lines.append(f"• {segments[0]}")
            lines.extend(f"  {segment}" for segment in segments[1:])
            continue
        heading = _escape(record.label or "Prepared draft")
        lines.extend(("", f"<b>{heading}</b>"))
        if record.recipient:
            lines.append("To: " + _escape(record.recipient))
        if record.subject:
            lines.append("Subject: " + _escape(record.subject))
        lines.append(
            "<pre>" + _escape(_bounded_draft_body(record.text)) + "</pre>")
    return lines


def _labelled_html_lines(label: str, value: str) -> list[str]:
    segments = _escaped_source_lines(value)
    return [f"<b>{label}:</b> {segments[0]}", *segments[1:]]


def _escaped_source_lines(value: str) -> list[str]:
    return [
        _escape(segment)
        for line in value.split("\n")
        for segment in _source_line_segments(line)
    ]


def _source_line_segments(value: str) -> list[str]:
    if not value:
        return [""]
    return [
        value[offset:offset + MAX_RENDER_SOURCE_LINE_CHARS]
        for offset in range(0, len(value), MAX_RENDER_SOURCE_LINE_CHARS)
    ]


_MAX_MARKDOWN_TABLE_COLUMNS = 12
_MAX_MARKDOWN_TABLE_ROWS = 200


def _markdown_table_cells(line: str) -> list[str] | None:
    """Split a table row while preserving pipes escaped or inside code."""
    text = line.strip()
    if "|" not in text:
        return None
    cells: list[str] = []
    cell: list[str] = []
    code_ticks = 0
    index = 0
    while index < len(text):
        char = text[index]
        if char == "\\" and index + 1 < len(text):
            following = text[index + 1]
            if following in {"\\", "|"}:
                cell.append(following)
                index += 2
                continue
        if char == "`":
            end = index + 1
            while end < len(text) and text[end] == "`":
                end += 1
            ticks = end - index
            if code_ticks == 0:
                code_ticks = ticks
            elif code_ticks == ticks:
                code_ticks = 0
            cell.append(text[index:end])
            index = end
            continue
        if char == "|" and code_ticks == 0:
            cells.append("".join(cell).strip())
            cell = []
        else:
            cell.append(char)
        index += 1
    cells.append("".join(cell).strip())
    if text.startswith("|"):
        cells.pop(0)
    backslashes = 0
    for char in reversed(text[:-1]):
        if char != "\\":
            break
        backslashes += 1
    if text.endswith("|") and backslashes % 2 == 0:
        cells.pop()
    return cells if len(cells) >= 2 else None


def _markdown_table_alignment(cell: str) -> str | None:
    marker = cell.strip()
    if not re.fullmatch(r":?-{3,}:?", marker):
        return None
    if marker.startswith(":") and marker.endswith(":"):
        return "center"
    if marker.endswith(":"):
        return "right"
    return "left"


def _markdown_table_cell_text(cell: str) -> str:
    """Flatten inline Markdown before measuring a monospace table cell."""
    text = re.sub(r"\[([^\]]+)\]\(([^)\s]+)\)", r"\1", cell)
    text = re.sub(r"`([^`]*)`", r"\1", text)
    text = re.sub(
        r"\*\*([^*]+)\*\*|__([^_]+)__",
        lambda match: match.group(1) or match.group(2),
        text,
    )
    text = re.sub(r"(?<!\*)\*(?!\s)([^*\n]+?)\*(?!\*)", r"\1", text)
    return text.strip()


def _pad_markdown_table_cell(text: str, width: int, alignment: str) -> str:
    room = width - len(text)
    if alignment == "right":
        return " " * room + text
    if alignment == "center":
        left = room // 2
        return " " * left + text + " " * (room - left)
    return text + " " * room


def _markdown_table(
    source_lines: list[str], start: int
) -> tuple[str, int] | None:
    """Render one bounded Markdown table and return its first unused line."""
    if start + 1 >= len(source_lines):
        return None
    header = _markdown_table_cells(source_lines[start])
    delimiter = _markdown_table_cells(source_lines[start + 1])
    if header is None or delimiter is None or len(header) != len(delimiter):
        return None
    if not 2 <= len(header) <= _MAX_MARKDOWN_TABLE_COLUMNS:
        return None
    if any(
        len(cell) > MAX_RENDER_SOURCE_LINE_CHARS
        for cell in [*header, *delimiter]
    ):
        return None
    alignments = [_markdown_table_alignment(cell) for cell in delimiter]
    if any(alignment is None for alignment in alignments):
        return None

    rows: list[list[str]] = []
    end = start + 2
    while end < len(source_lines):
        row = _markdown_table_cells(source_lines[end])
        if row is None or len(row) > len(header):
            break
        if len(rows) >= _MAX_MARKDOWN_TABLE_ROWS or any(
            len(cell) > MAX_RENDER_SOURCE_LINE_CHARS for cell in row
        ):
            return None
        rows.append(row + [""] * (len(header) - len(row)))
        end += 1

    rendered_rows = [list(map(_markdown_table_cell_text, header))]
    rendered_rows.extend(
        [list(map(_markdown_table_cell_text, row)) for row in rows]
    )
    widths = [
        max(3, *(len(row[column]) for row in rendered_rows))
        for column in range(len(header))
    ]
    resolved_alignments = [
        alignment for alignment in alignments if alignment is not None
    ]

    def render(row: list[str]) -> str:
        return " │ ".join(
            _pad_markdown_table_cell(
                value, widths[column], resolved_alignments[column]
            )
            for column, value in enumerate(row)
        ).rstrip()

    separator = "─┼─".join("─" * width for width in widths)
    display = [render(rendered_rows[0]), separator]
    display.extend(render(row) for row in rendered_rows[1:])
    escaped = _escape("\n".join(display))
    return f"<pre>{escaped}</pre>", end


def _markdown_lines(value: str) -> list[str]:
    lines: list[str] = []
    source_lines = value.split("\n")
    cursor = 0
    while cursor < len(source_lines):
        table = _markdown_table(source_lines, cursor)
        if table is not None:
            rendered, cursor = table
            lines.append(rendered)
            continue
        raw = source_lines[cursor]
        cursor += 1
        for index, line in enumerate(_source_line_segments(raw.rstrip())):
            heading = re.match(r"^(#{1,6})\s+(.*)$", line) if not index else None
            bullet = re.match(r"^\s*[-*+]\s+(.*)$", line) if not index else None
            ordered = (
                re.match(r"^\s*(\d+[.)])\s+(.*)$", line)
                if not index
                else None
            )
            if heading:
                lines.append(f"<b>{_markdown_inline(heading.group(2))}</b>")
            elif bullet:
                lines.append(f"• {_markdown_inline(bullet.group(1))}")
            elif ordered:
                lines.append(
                    f"{_escape(ordered.group(1))} "
                    f"{_markdown_inline(ordered.group(2))}"
                )
            else:
                lines.append(_markdown_inline(line))
    return lines


def _markdown_inline(value: str) -> str:
    fragments: list[str] = []
    # Telegram rejects C0 controls, including the NUL marker this renderer
    # used originally. A transport sanitizer then left the numeric fragment
    # indices visible as a row of unhelpful zeroes under Review links. Pick a
    # control-free marker that cannot occur in the input instead.
    marker_prefix = "FOXHOUNDINLINEFRAGMENT"
    while marker_prefix in value:
        marker_prefix += "X"

    def stash(fragment: str) -> str:
        fragments.append(fragment)
        return f"{marker_prefix}{len(fragments) - 1}END"

    value = re.sub(
        r"`([^`\n]+)`",
        lambda match: stash(f"<code>{_escape(match.group(1))}</code>"),
        value,
    )

    def link(match: re.Match[str]) -> str:
        target = match.group(2)
        if not _safe_link(target):
            return stash(_escape(match.group(0)))
        return stash(
            f'<a href="{html.escape(target, quote=True)}">'
            f"{_escape(match.group(1))}</a>"
        )

    value = re.sub(r"\[([^\]\n]+)\]\(([^)\s]+)\)", link, value)
    value = _escape(value)
    value = re.sub(
        r"\*\*([^*\n]+)\*\*|__([^_\n]+)__",
        lambda match: f"<b>{match.group(1) or match.group(2)}</b>",
        value,
    )
    value = re.sub(
        r"(?<!\*)\*(?!\s)([^*\n]+?)\*(?!\*)",
        lambda match: f"<i>{match.group(1)}</i>",
        value,
    )
    return re.sub(
        re.escape(marker_prefix) + r"(\d+)END",
        lambda match: fragments[int(match.group(1))],
        value,
    )


def _safe_link(value: str) -> bool:
    if (
        not value
        or len(value) > 2_048
        or any(character.isspace() or ord(character) < 32 for character in value)
    ):
        return False
    try:
        parsed = urllib.parse.urlsplit(value)
        port = parsed.port
    except ValueError:
        return False
    return (
        parsed.scheme in {"http", "https"}
        and parsed.hostname is not None
        and parsed.username is None
        and parsed.password is None
        and (port is None or 1 <= port <= 65_535)
    )


def _escape(value: str) -> str:
    return html.escape(value, quote=False)


def _button_rows(
    card: ExecutionReviewCard, *, approvable: bool
) -> tuple[tuple[tuple[str, str], ...], ...]:
    kind = card.kind
    if kind is ExecutionCardKind.START:
        if (
            card.workflow_status is WorkflowStatus.PARKED
            and card.failure_reason == "context_exhausted"
        ):
            # A measured context refusal cannot be repaired by repeating the
            # same run, so this set carries no retry. Everything that is not
            # a retry stays: a reader who has already done the work by hand
            # still needs Done, and one who cannot act yet still needs
            # Snooze. Drop abandons a task rather than settling it, and must
            # not be the only way to close work that merely did not fit.
            rows = (
                (("✏️ Reduce scope", "discuss"),),
                SNOOZE_BUTTON_ROW,
                (("🗑 Drop", "drop"), ("👥 Reassign", "reassign")),
            )
            if approvable:
                rows = ((("✅ Done", "done"),),) + rows
            return rows + ((("📋 Task brief", "brief"),),)
        rows: tuple[tuple[tuple[str, str], ...], ...] = (
            (("✅ Done", "done"), ("▶️ Continue", "start")),
            (("🗑 Drop", "drop"), ("✏️ Update", "discuss")),
            (("💬 Comment and Go", "comment_go"),),
            SNOOZE_BUTTON_ROW,
            (("👥 Reassign", "reassign"),),
        )
        if card.owner_hold_eligible and card.owner:
            rows += ((
                (_owner_hold_button_label(card.owner), OWNER_HOLD_ACTION),
            ),)
        if card.workflow_status is WorkflowStatus.AWAITING_START:
            # Only before it starts: once a workflow is running, changing
            # the agent underneath it would rebind work already in flight.
            rows += ((("🤖 Agent", "agent"),),)
        rows += ((("📋 Task brief", "brief"),),)
        return rows if approvable else rows[1:]
    if kind is ExecutionCardKind.STEER:
        rows = (
            (("✏️ Update", "discuss"),),
            (("💬 Comment and Go", "comment_go"),),
            (("✅ Done", "done"), ("🗑 Drop", "drop")),
        )
        return rows + ((("📋 Task brief", "brief"),),) if approvable else (
            (("✏️ Update", "discuss"),), (("🗑 Drop", "drop"),),
            (("📋 Task brief", "brief"),),
        )
    stop_row = (("👥 Reassign", "reassign"), ("🗑 Drop task", "drop"))
    if kind is ExecutionCardKind.EXTERNAL_REVIEW:
        rows = (
            (("✅ Authorize action", "approve"), ("⛔ Not now", "revise")),
            (("💬 Comment and Go", "comment_go"),),
            SNOOZE_BUTTON_ROW,
            (("💬 Discuss", "discuss"), ("✅ Mark as done", "done")),
            stop_row,
        )
    elif kind is ExecutionCardKind.RESULT_REVIEW:
        rows = (
            (("✅ Mark as done", "done"),),
            (("💬 Discuss", "discuss"),),
            SNOOZE_BUTTON_ROW,
            stop_row,
        )
    else:
        rows = (
            (("🔎 Investigate further", "revise"), ("💬 Discuss", "discuss")),
            (("▶️ Execute plan", "approve"), ("🤖 Agents", "agent")),
            (("💬 Comment and Go", "comment_go"),),
            SNOOZE_BUTTON_ROW,
            (("✅ Mark as done", "done"),),
            stop_row,
        )
    if (
        kind in {ExecutionCardKind.PLAN_REVIEW, ExecutionCardKind.RESULT_REVIEW}
        and card.deliverables
    ):
        rows += ((("📦 Deliverables", "deliverables"),),)
    if approvable:
        return rows + ((("📋 Task brief", "brief"),),)
    reduced = tuple(
        tuple(
            button for button in row
            if button[1] not in {"approve", "comment_go", "done"}
        )
        for row in rows
        if any(
            button[1] not in {"approve", "comment_go", "done"}
            for button in row
        )
    )
    return reduced + ((("📋 Task brief", "brief"),),)


def _owner_hold_button_label(owner: str) -> str:
    prefix = "🗓 Until next meeting with "
    maximum = 64
    if len((prefix + owner).encode("utf-8")) <= maximum:
        return prefix + owner
    suffix = "…"
    budget = maximum - len((prefix + suffix).encode("utf-8"))
    encoded = owner.encode("utf-8")[:budget]
    while encoded:
        try:
            shortened = encoded.decode("utf-8")
            break
        except UnicodeDecodeError:
            encoded = encoded[:-1]
    else:
        shortened = ""
    return prefix + shortened.rstrip() + suffix


def _direct_actions_for_kind(kind: ExecutionCardKind) -> set[str]:
    if kind is ExecutionCardKind.START:
        # The intervals the review cards already accept. A gate is the card
        # most likely to be deferred, because it is asked before any work
        # has been done, and "tomorrow" is rarely the right answer for a
        # task waiting on someone else, a release, or a month end.
        return {
            "start", "snooze", "cancel", "done", "drop",
            OWNER_HOLD_ACTION,
            *REVIEW_SNOOZE_ACTIONS,
        }
    if kind is ExecutionCardKind.RESULT_REVIEW:
        return {"done", "drop", "snooze", *REVIEW_SNOOZE_ACTIONS}
    if kind is ExecutionCardKind.STEER:
        return {"done", "drop"}
    return {
        "approve", "revise", "cancel", "done", "drop", "snooze",
        *REVIEW_SNOOZE_ACTIONS,
    }


def _stored_action(action: str) -> str:
    return (
        "snooze"
        if action in {*REVIEW_SNOOZE_ACTIONS, OWNER_HOLD_ACTION}
        else action
    )


def _valid_reader_input(kind: str, value: object) -> bool:
    maximum = MAX_DISCUSSION_CHARS if kind == "discussion" else MAX_OWNER_CHARS
    return (
        isinstance(value, str)
        and value == value.strip()
        and 1 <= len(value) <= maximum
        and not any(ord(character) == 0 for character in value)
        and (kind != "reassignment" or "\n" not in value)
    )


def _apply_review_lifecycle_action(
    connection: sqlite3.Connection,
    row: Mapping[str, object],
    *,
    action: str,
    now: str,
) -> WorkflowOperationResult:
    transition = _apply_task_transition(
        connection,
        task_id=int(row["task_id"]),
        expected_version=int(row["task_version"]),
        action=action,
        now=now,
    )
    if transition.disposition is TransitionDisposition.REFUSED:
        return WorkflowOperationResult(
            WorkflowDisposition.REFUSED,
            int(row["task_id"]),
            refusal=WorkflowRefusal.STALE_TASK,
        )
    workflow_status = (
        WorkflowStatus.COMPLETED if action == "done" else WorkflowStatus.CANCELLED
    )
    workflow_version = int(row["workflow_version"]) + 1
    phase = WorkflowPhase(row["phase"])
    updated = connection.execute(
        "UPDATE task_execution_workflows SET task_version=?,status=?,"
        "version=?,due_at=NULL,claim_token_digest=NULL,claimed_at=NULL,"
        "claim_heartbeat_at=NULL,claim_expires_at=NULL,failure_count=0,"
        "current_run_id=NULL,"
        "last_failure_reason=NULL,last_failure_exit_code=NULL,"
        "last_failure_run_id=NULL,last_failure_at=NULL,next_attempt_at=NULL,"
        "parked_at=NULL,updated_at=?,completed_at=? "
        "WHERE task_id=? AND version=?",
        (
            int(transition.version),
            workflow_status,
            workflow_version,
            now,
            now,
            int(row["task_id"]),
            int(row["workflow_version"]),
        ),
    )
    if updated.rowcount != 1:
        return WorkflowOperationResult(
            WorkflowDisposition.REFUSED,
            int(row["task_id"]),
            refusal=WorkflowRefusal.STALE_WORKFLOW,
        )
    TaskExecutionService._event(
        connection,
        int(row["task_id"]),
        "task_completed" if action == "done" else "task_dropped",
        workflow_version,
        int(transition.version),
        phase,
        workflow_status,
        now,
    )
    return WorkflowOperationResult(
        WorkflowDisposition.APPLIED,
        int(row["task_id"]),
        workflow_version,
        workflow_status,
        phase,
    )


def _apply_steer_discussion(
    connection: sqlite3.Connection,
    row: Mapping[str, object],
    *,
    stamp: datetime,
) -> WorkflowOperationResult:
    """Pre-empt a live pass so its next claim reads the stored note."""
    now = stamp.isoformat(timespec="seconds")
    version = int(row["workflow_version"]) + 1
    phase = WorkflowPhase(row["phase"])
    changed = connection.execute(
        "UPDATE task_execution_workflows SET status='queued',version=?,"
        "due_at=NULL,claim_token_digest=NULL,claimed_at=NULL,"
        "claim_heartbeat_at=NULL,claim_expires_at=NULL,current_run_id=NULL,"
        "updated_at=? WHERE task_id=? AND version=? AND status='running'",
        (version, now, int(row["task_id"]), int(row["workflow_version"])),
    )
    if changed.rowcount != 1:
        return WorkflowOperationResult(
            WorkflowDisposition.REFUSED, int(row["task_id"]),
            refusal=WorkflowRefusal.STALE_WORKFLOW,
        )
    TaskExecutionService._event(
        connection, int(row["task_id"]), "discussion_requested", version,
        int(row["task_version"]), phase, WorkflowStatus.QUEUED, now,
    )
    return WorkflowOperationResult(
        WorkflowDisposition.APPLIED, int(row["task_id"]), version,
        WorkflowStatus.QUEUED, phase,
    )


def _callback(card_id: int, version: int, action: str) -> str:
    value = f"{CALLBACK_PREFIX}|{card_id}|{version}|{action}"
    if len(value.encode("utf-8")) > CALLBACK_DATA_LIMIT:
        raise TaskLedgerError("execution review callback is too large")
    return value


def _agent_callback(card_id: int, version: int, token: str) -> str:
    if not _valid_agent_selection_token(token):
        raise TaskLedgerError("execution agent callback is invalid")
    value = f"{AGENT_CALLBACK_PREFIX}|{card_id}|{version}|{token}"
    if len(value.encode("utf-8")) > CALLBACK_DATA_LIMIT:
        raise TaskLedgerError("execution agent callback is too large")
    return value


def _agent_selection_token(profile: AgentProfile) -> str:
    source = f"{profile.profile_id}\0{profile.revision}".encode("utf-8")
    return hashlib.sha256(source).hexdigest()[:AGENT_SELECTION_TOKEN_CHARS]


def _valid_agent_selection_token(value: object) -> bool:
    return bool(
        isinstance(value, str)
        and re.fullmatch(
            rf"[0-9a-f]{{{AGENT_SELECTION_TOKEN_CHARS}}}", value
        )
    )


def _agent_selector_phase(card: ExecutionReviewCard) -> WorkflowPhase:
    if card.kind is ExecutionCardKind.START:
        return WorkflowPhase.PLAN
    if card.kind is ExecutionCardKind.PLAN_REVIEW:
        return WorkflowPhase.EXECUTE
    raise TaskLedgerError("execution agent selection is unavailable")


def _agent_selector_label(card: ExecutionReviewCard) -> str:
    return {
        WorkflowPhase.PLAN: "planning",
        WorkflowPhase.EXECUTE: "execution",
    }[_agent_selector_phase(card)]


def _eligible_profiles(
    registry: AgentProfileRegistry, *, phase: WorkflowPhase
) -> tuple[AgentProfile, ...]:
    return tuple(
        profile
        for profile in registry.list()
        if phase.value in profile.allowed_phases
    )


def _agent_options(
    registry: AgentProfileRegistry, card: ExecutionReviewCard
) -> tuple[ExecutionAgentOption, ...]:
    phase = _agent_selector_phase(card)
    options = tuple(
        ExecutionAgentOption(
            profile.display_name,
            _agent_selection_token(profile),
            selected=(
                profile.profile_id == card.agent_profile_id
                and profile.revision == card.agent_profile_revision
            ),
        )
        for profile in _eligible_profiles(
            registry, phase=phase
        )
    )
    tokens = [option.selection_token for option in options]
    if not options or len(tokens) != len(set(tokens)):
        raise TaskLedgerError("execution agent choices are unavailable")
    selected = sum(option.selected for option in options)
    if selected > 1:
        raise TaskLedgerError("selected execution agent is unavailable")
    if selected == 0:
        try:
            historical = registry.resolve(
                card.agent_profile_id, card.agent_profile_revision
            )
        except AgentProfileError as exc:
            raise TaskLedgerError(
                "selected execution agent is unavailable"
            ) from exc
        if (
            card.kind is ExecutionCardKind.PLAN_REVIEW
            and phase.value not in historical.allowed_phases
        ):
            # The profile that wrote the plan need not be able to perform
            # it.  The empty checkmark makes that explicit while allowing a
            # reader to select one of the installed executors.
            return options
        current = registry.get(card.agent_profile_id)
        if current is None or historical.revision == current.revision:
            raise TaskLedgerError("selected execution agent is unavailable")
    return options


def _card_fits(card: ExecutionReviewCard) -> bool:
    return len("\n".join(_html_card_lines(card)).encode("utf-8")) <= (
        MAX_CARD_BODY_BYTES
    )


def _escape_bounded(value: str, maximum: int, *, suffix: str) -> str:
    encoded = html.escape(value, quote=False).encode("utf-8")
    if len(encoded) <= maximum:
        return encoded.decode("utf-8")
    suffix_bytes = ("\n…" + suffix).encode("utf-8")
    limit = maximum - len(suffix_bytes)
    fragments: list[str] = []
    size = 0
    for character in value:
        escaped = html.escape(character, quote=False)
        width = len(escaped.encode("utf-8"))
        if size + width > limit:
            break
        fragments.append(escaped)
        size += width
    return "".join(fragments) + suffix_bytes.decode("utf-8")


def _card_guard(
    row: Mapping[str, object] | None, expected_version: int
) -> ExecutionCardRefusal | None:
    if row is None:
        return ExecutionCardRefusal.NOT_FOUND
    if int(row["version"]) != expected_version:
        return ExecutionCardRefusal.STALE_VERSION
    return None


def _artifact_card_refusal(
    row: Mapping[str, object] | None, expected_version: int,
) -> ExecutionCardRefusal | None:
    """Apply the deliverables button's state fence to file reads."""
    refusal = _card_guard(row, expected_version)
    if refusal is not None:
        return refusal
    if row["status"] != ExecutionCardStatus.DELIVERED:
        return ExecutionCardRefusal.INVALID_STATE
    if not _current_card(row):
        return ExecutionCardRefusal.STALE_VERSION
    if ExecutionCardKind(row["kind"]) not in {
        ExecutionCardKind.PLAN_REVIEW,
        ExecutionCardKind.RESULT_REVIEW,
    } or row["workflow_result_id"] is None:
        return ExecutionCardRefusal.INVALID_STATE
    return None


def _agent_card_refusal(
    row: Mapping[str, object] | None, expected_version: int
) -> ExecutionCardRefusal | None:
    refusal = _card_guard(row, expected_version)
    if refusal is not None:
        return refusal
    if row["status"] != ExecutionCardStatus.DELIVERED:
        return ExecutionCardRefusal.INVALID_STATE
    if not _current_card(row):
        return ExecutionCardRefusal.STALE_VERSION
    kind = ExecutionCardKind(row["kind"])
    if kind is ExecutionCardKind.START:
        if row["workflow_status_current"] != WorkflowStatus.AWAITING_START:
            return ExecutionCardRefusal.INVALID_STATE
        return None
    if kind is not ExecutionCardKind.PLAN_REVIEW:
        return ExecutionCardRefusal.INVALID_ACTION
    if (
        row["workflow_status_current"] != WorkflowStatus.AWAITING_REVIEW
        or row["workflow_phase_current"] != WorkflowPhase.PLAN
    ):
        return ExecutionCardRefusal.INVALID_STATE
    return None


def _workflow_refusal(
    refusal: WorkflowRefusal | None,
) -> ExecutionCardRefusal:
    if refusal in {WorkflowRefusal.STALE_TASK, WorkflowRefusal.STALE_WORKFLOW}:
        return ExecutionCardRefusal.STALE_VERSION
    return ExecutionCardRefusal.INVALID_STATE


def _operation(
    row: Mapping[str, object], disposition: ExecutionCardDisposition
) -> ExecutionCardOperationResult:
    return ExecutionCardOperationResult(
        disposition,
        int(row["id"]),
        card_version=int(row["version"]),
        card_status=ExecutionCardStatus(row["status"]),
        workflow_version=int(row["workflow_version"]),
    )


def _refused(
    card_id: object, refusal: ExecutionCardRefusal
) -> ExecutionCardOperationResult:
    return ExecutionCardOperationResult(
        ExecutionCardDisposition.REFUSED,
        card_id if isinstance(card_id, int) and not isinstance(card_id, bool) else 0,
        refusal=refusal,
    )


def _refused_row(
    card_id: int,
    row: Mapping[str, object] | None,
    refusal: ExecutionCardRefusal,
) -> ExecutionCardOperationResult:
    if row is None:
        return _refused(card_id, refusal)
    return ExecutionCardOperationResult(
        ExecutionCardDisposition.REFUSED,
        card_id,
        card_version=int(row["version"]),
        card_status=ExecutionCardStatus(row["status"]),
        workflow_version=int(row["workflow_version"]),
        refusal=refusal,
    )


def _agent_refused(
    card_id: object, refusal: ExecutionCardRefusal
) -> ExecutionAgentSelectorResult:
    return ExecutionAgentSelectorResult(
        ExecutionCardDisposition.REFUSED,
        card_id if isinstance(card_id, int) and not isinstance(card_id, bool) else 0,
        refusal=refusal,
    )


def _agent_refused_row(
    card_id: int,
    row: Mapping[str, object] | None,
    refusal: ExecutionCardRefusal,
) -> ExecutionAgentSelectorResult:
    if row is None:
        return _agent_refused(card_id, refusal)
    return ExecutionAgentSelectorResult(
        ExecutionCardDisposition.REFUSED,
        card_id,
        card_version=int(row["version"]),
        refusal=refusal,
    )


def _valid_identity(card_id: object, version: object) -> bool:
    return all(
        isinstance(value, int) and not isinstance(value, bool) and value > 0
        for value in (card_id, version)
    )


def _valid_limit(value: object) -> bool:
    return (
        isinstance(value, int)
        and not isinstance(value, bool)
        and 1 <= value <= 1_000
    )


def _valid_lease(value: object) -> bool:
    return (
        isinstance(value, int)
        and not isinstance(value, bool)
        and 5 <= value <= 300
    )


def _valid_secret(value: object) -> bool:
    return (
        isinstance(value, str)
        and 32 <= len(value) <= 512
        and not any(character.isspace() for character in value)
    )


def _valid_digest(value: object) -> bool:
    return isinstance(value, str) and bool(re.fullmatch(r"[0-9a-f]{64}", value))


def _valid_opaque(value: object, maximum: int) -> bool:
    return (
        isinstance(value, str)
        and 1 <= len(value) <= maximum
        and value == value.strip()
        and not any(ord(character) < 32 or ord(character) == 127 for character in value)
    )


def _token_digest(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()
