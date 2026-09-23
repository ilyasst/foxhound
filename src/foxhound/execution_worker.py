"""Narrow private CLI operations for one supervised execution claim.

The worker reads a capability from an owner-only run-state file.  It never
prints that capability and never accepts task or workflow identity from an
agent-authored result draft.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import stat
import sys
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Mapping, Sequence

from .knowledge_client import (
    GwKnowledgeClient,
    KnowledgeClientConfig,
    KnowledgeClientError,
    KnowledgeSearchResult,
)
from .contracts import SourceSnapshotContractError
from . import work_digest, voice_summary, tts_client
from .task_execution import (
    ExecutionResultEnvelope,
    TaskExecutionService,
    WorkflowDisposition,
    WorkflowPhase,
    WorkflowStatus,
    _validated_result,
)
from .source_policy import action_grants as _action_grants
from .source_policy import execution_grants as _execution_grants
from . import forge_action
from . import forge_thread
from .release_revision import describe as _describe_revision
from .worker_resolution import (
    REPORT_SCHEMA,
    REPORT_SCHEMA_VERSION,
    is_worker_command,
)
from .workflow_policy import (
    CHECKPOINTS,
    WorkflowPolicy,
    WorkflowPolicyError,
    parse_workflow_policy,
    policy_from_legacy,
)
from .agent_profiles import (
    MAX_MANIFEST_BYTES,
    AgentProfileError,
    parse_profile,
)
from .task_ledger import TaskLedger, TaskLedgerError, TaskStatus
from .task_archive import (
    ARTIFACT_MANIFEST_NAME,
    TaskArchiveError,
    TaskArchivePaths,
    append_result,
    preserve_run_files,
    recorded_artifacts,
)


RUN_STATE_SCHEMA = "foxhound.execution-run-state"
RUN_STATE_SCHEMA_VERSION = 6
INSTRUCTIONS_NAME = "agent-instructions.json"
WORK_CONTEXT_SCHEMA = "foxhound.execution-work-context"
WORK_CONTEXT_SCHEMA_VERSION = 8
WORKER_SEARCH_SCHEMA = "foxhound.execution-worker-search"
RESULT_DRAFT_SCHEMA = "foxhound.execution-result-draft"
RESULT_DRAFT_READY_SCHEMA = "foxhound.execution-result-draft-ready"
RESULT_RECEIPT_SCHEMA = "foxhound.execution-result-receipt"
REPOSITORY_RECEIPTS_NAME = "repository-action-receipts.json"
REPOSITORY_RECEIPTS_SCHEMA = "foxhound.repository-action-receipts"
WORKER_SCHEMA_VERSION = 1

STATE_ENV = "FOXHOUND_EXECUTION_STATE"
GW_ENDPOINT_ENV = "FOXHOUND_GW_ENDPOINT"
GW_ALIAS_ENV = "FOXHOUND_GW_ALIAS"
GW_TOKEN_FILE_ENV = "FOXHOUND_GW_TOKEN_FILE"
#: The whole workflow policy, as one versioned document.  It replaced a pair
#: of comma-separated source-kind lists: those said what to check without
#: saying under which policy, so a run could not name the policy it was
#: fenced by and enabling a check was a per-host edit with no revision.
WORKFLOW_POLICY_ENV = "FOXHOUND_WORKFLOW_POLICY"

MAX_STATE_BYTES = 16 * 1024
MAX_INSTRUCTIONS_BYTES = MAX_MANIFEST_BYTES
MAX_DRAFT_BYTES = 256 * 1024
_RUN_ID_RE = re.compile(r"^[0-9a-f]{32}$")
_RESULT_NAME_RE = re.compile(r"^result-([0-9a-f]{32})\.json$")
_PROFILE_ID_RE = re.compile(r"^[a-z][a-z0-9-]{0,31}$")
_REVISION_RE = re.compile(r"^[0-9a-f]{64}$")
_RESULT_INPUTS = (
    "result-summary.txt",
    "result-work.md",
    "result-questions.json",
    "result-external-actions.json",
    "result-deliverables.json",
    "result-repository-references.json",
    "result-repository-impact.json",
    REPOSITORY_RECEIPTS_NAME,
    ARTIFACT_MANIFEST_NAME,
)


def _local_today() -> str:
    """Return the host's authoritative local calendar date."""
    return datetime.now().astimezone().date().isoformat()


def _read_handoff(task_work_directory: str | None, phase: str) -> str | None:
    if not task_work_directory:
        return None
    path = Path(task_work_directory) / f"handoff-{phase}.md"
    if not path.is_file():
        return None
    try:
        raw = path.read_bytes()
        if not raw:
            return None
        if len(raw) > 16384:
            content = raw[:16384].decode("utf-8", errors="replace")
            return content + "\n\n[TRUNCATED: handoff file exceeded 16384 bytes]"
        return raw.decode("utf-8", errors="replace")
    except Exception:
        return None


def _local_calendar() -> dict[str, object]:
    today = datetime.fromisoformat(_local_today()).date()
    next_week_start = today + timedelta(days=7 - today.weekday())
    next_week_end = next_week_start + timedelta(days=6)
    weekdays = (
        "Monday", "Tuesday", "Wednesday", "Thursday", "Friday",
        "Saturday", "Sunday",
    )
    return {
        "today": today.isoformat(),
        "today_weekday": weekdays[today.weekday()],
        "next_week": {
            "start": next_week_start.isoformat(),
            "start_weekday": weekdays[next_week_start.weekday()],
            "end": next_week_end.isoformat(),
            "end_weekday": weekdays[next_week_end.weekday()],
        },
    }


def _worker_operations(phase: WorkflowPhase) -> list[str]:
    operations = ["context", "search", "draft", "record", "release"]
    # A working tree is available in every phase, planning included. It is a
    # clone in the run's own directory and causes no external effect; nothing
    # is pushed from it except through act.pull-request, which is gated below.
    # Withholding it from planning did not stop a run that needed to write —
    # it only removed the sanctioned place to do so, leaving the host's shared
    # checkouts as the nearest writable repository.
    operations.append("act.worktree")
    if phase is WorkflowPhase.EXTERNAL_ACTION:
        operations.append("act.pull-request")
        operations.extend(("act.comment", "act.issue", "act.review", "act.mail"))
    # Read-only thread access is available in plan and execute, not just
    # external_action: the point is to read review feedback *before* repeating
    # the work, and by external_action the work is already done.
    if phase in (WorkflowPhase.PLAN, WorkflowPhase.EXECUTE):
        operations.append("thread")
    return operations


_LOCAL_RESEARCH_CLIENTS = {
    "outlook": (
        "folders", "inbox", "search", "read", "thread", "draft", "attachment",
    ),
    "moodle": (
        "renew", "whoami", "courses", "assignments", "submissions",
        "assessment",
    ),
    "qmd": ("query", "search", "get", "multi-get", "ls", "status"),
}


def _local_research_clients(phase: WorkflowPhase) -> dict[str, list[str]]:
    """Approved clients this runner can actually invoke.

    Profiles are portable across workers, while local research clients are
    deliberately host-specific. Advertising a command that is absent turns
    an agent's first useful action into a misleading failure, so this is a
    small runtime fact rather than a profile promise.
    """
    clients = {}
    
    for name, operations in _LOCAL_RESEARCH_CLIENTS.items():
        if shutil.which(name) is None:
            continue
            
        allowed = list(operations)
        if name == "outlook":
            if phase not in (WorkflowPhase.PLAN, WorkflowPhase.EXECUTE):
                continue
            if "draft" in allowed and phase is not WorkflowPhase.EXECUTE:
                allowed.remove("draft")
        elif name == "moodle":
            if phase not in (WorkflowPhase.PLAN, WorkflowPhase.EXECUTE):
                continue
                
        clients[name] = allowed
        
    return clients


class ExecutionWorkerError(RuntimeError):
    """A content-free worker-boundary failure."""


class ExecutionWorkerConfigError(ExecutionWorkerError):
    pass


class ExecutionWorkerClaimError(ExecutionWorkerError):
    pass


class ExecutionWorkerDraftError(ExecutionWorkerError):
    """A refusal the agent is allowed to hear the reason for.

    `reason` is a bounded enum token from the ledger — `stale_version`,
    `claim_mismatch`, `invalid_state` and so on. It names a state, never
    task content, so it is safe to put in front of an agent and in a
    process's standard error.
    """

    def __init__(self, message: str, *, reason: str | None = None) -> None:
        super().__init__(message)
        self.reason = reason


@dataclass(frozen=True)
class ExecutionRunState:
    run_id: str
    database_path: Path = field(repr=False)
    task_id: int
    task_version: int
    workflow_version: int
    phase: WorkflowPhase
    claim_token: str = field(repr=False)
    lease_seconds: int
    agent_profile_id: str
    agent_profile_revision: str
    worker_command: str
    execution_grants: frozenset[str] = field(default_factory=frozenset)
    action_grants: frozenset[str] = field(default_factory=frozenset)
    knowledge_root: str | None = None
    deployment_roots: Mapping[str, str] = field(default_factory=dict)
    task_work_directory: str | None = None
    task_kb_file: str | None = None
    task_run_directory: str | None = None


class ExecutionWorker:
    """Narrow operations available to a disposable task agent."""

    def __init__(
        self,
        state_path: str | os.PathLike[str],
        knowledge_config: KnowledgeClientConfig,
        *,
        policy: WorkflowPolicy | None = None,
    ) -> None:
        if not isinstance(knowledge_config, KnowledgeClientConfig):
            raise ExecutionWorkerConfigError(
                "execution worker knowledge configuration is invalid"
            )
        self._state_path = Path(state_path)
        self._knowledge_config = knowledge_config
        if policy is None:
            # No policy configured is the same answer as a policy that grants
            # nothing and checks nothing: today's behaviour, with a revision.
            policy = policy_from_legacy()
        if not isinstance(policy, WorkflowPolicy):
            raise ExecutionWorkerConfigError(
                "execution worker policy is invalid"
            )
        self._policy = policy

    def context(self) -> dict[str, Any]:
        state, service = self._fresh_active("phase")
        instructions = self._instructions(state)
        context = GwKnowledgeClient(self._knowledge_config).execution_context()
        self._renew(service, state)
        task = TaskLedger(state.database_path).get(state.task_id)
        if (
            task is None
            or task.status is not TaskStatus.OPEN
            or task.version != state.task_version
        ):
            raise ExecutionWorkerClaimError("execution claim is unavailable")
        origin = TaskLedger(state.database_path).origin(state.task_id)

        working_group_context = None
        try:
            wg_doc = GwKnowledgeClient(self._knowledge_config).working_groups(
                query=task.text,
                person_name=task.owner,
            )
            matched = wg_doc.get("matched_group")
            if matched:
                working_group_context = {
                    "name": matched.get("name"),
                    "dominant_people": matched.get("dominant_people", []),
                    "keywords": matched.get("keywords", []),
                }
        except Exception:
            working_group_context = None

        return {
            "schema": WORK_CONTEXT_SCHEMA,
            "schema_version": WORK_CONTEXT_SCHEMA_VERSION,
            "runtime": {
                # Agents cannot safely infer the host's local date from task
                # timestamps or their model cutoff.  This is the authoritative
                # date for deadlines, drafts, and proposed actions.
                **_local_calendar(),
                "toolsets": instructions["toolsets"],
                # Which policy this run is fenced by.  A decision or a final
                # outcome is recorded against a policy revision, so the run
                # has to be able to say which one was in force; an
                # environment variable could not answer that.
                "policy_id": self._policy.policy_id,
                "policy_revision": self._policy.revision,
            },
            "capabilities": {
                # This is descriptive evidence from the worker, not authority
                # supplied by task text.  It prevents a profile from routing
                # work to an ambient Hermes tool that this run does not have.
                "knowledge_layers": ["kb", "secondary", "emails"],
                # Installed, task-scoped local research clients.  These are
                # named here so an agent does not have to guess from an
                # ambient host path or mistake a zero-result GW search for a
                # lack of mail or knowledge access.  Client guidance remains
                # profile-versioned; this contract names only the approved
                # read/research surface.
                "local_research_clients": _local_research_clients(state.phase),
                # Stable symbolic roots supplied by deployment configuration.
                # They are runtime facts, rather than profile policy, so an
                # identical reviewed profile works on hosts with different
                # mounts or with no optional root at all.
                "deployment_roots": dict(state.deployment_roots),
                "worker_operations": _worker_operations(state.phase),
                "external_effects_allowed": (
                    state.phase is WorkflowPhase.EXTERNAL_ACTION
                ),
            },
            "task": {
                "id": task.id,
                "version": task.version,
                "text": task.text,
                "owner": task.owner,
                "due": task.due,
                "working_group": working_group_context,
                # The agent is told to take its repository and issue identity
                # from here and to infer nothing from the task text. Leaving
                # it out did not make the agent careful, it made it blind: it
                # planned a greenfield application for an issue that already
                # had a repository, and asked the reader where to put it.
                "origin": None if origin is None else {
                    "system": origin.system,
                    "kind": origin.kind,
                    "record_id": origin.record_id,
                    "item_id": origin.item_id,
                },
            },
            "agent": instructions,
            "knowledge": {
                # The agent reads this directory with its ordinary file
                # tools. Search finds the fragment; the directory is how it
                # reads the discussion the fragment came from.
                "root": state.knowledge_root,
            },
            # Where this task's durable output lives. The agent is told to
            # put deliverables in the task folder, so it has to be told
            # where that is: it runs inside the run directory and cannot
            # read run state to find its parent. Naming it here also means
            # a deliverable path in the result is one the reader can open.
            "workspace": {
                "task_folder": state.task_work_directory,
                "run_folder": state.task_run_directory,
            },
            "workflow": {
                "version": state.workflow_version,
                "phase": state.phase.value,
                # The count across every park, not the count since the
                # last one.  `failure_count` resets when `claim_next`
                # reclaims a parked workflow -- deliberately, so a retry
                # does not begin one slip from parking again -- so reading
                # it here told an agent on its twentieth attempt that it
                # was on its first, which is the single fact most likely
                # to make it repeat what has already failed.  #498 made
                # the durable per-phase count available; this uses it.
                "attempt_count": service.phase_attempts(
                    state.task_id,
                    expected_version=state.workflow_version,
                    claim_token=state.claim_token,
                ),
                "agent_profile_id": state.agent_profile_id,
                "agent_profile_revision": state.agent_profile_revision,
                "reader_instruction": service.reader_instruction(
                    state.task_id,
                    expected_version=state.workflow_version,
                    claim_token=state.claim_token,
                ),
                # Why earlier attempts at this phase stopped, most recent
                # first and bounded. Evidence about what has already been
                # tried and failed -- not a plan, and not a limit on what
                # may be read. Without it the next attempt begins from the
                # task text alone, makes the same plan, and fails the same
                # way; on one deployment that pattern took roughly a fifth
                # of all execution capacity over two days.
                #
                # Deliberately not classified into causes the agent can act
                # on and causes it cannot. A saturated backend is not the
                # agent's to fix and telling it to work around one invites
                # exactly the scope substitution in #360 -- but nothing
                # here can tell that apart from an exhausted turn budget
                # reliably, and a wrong hint is worse than none.
                "prior_failures": list(service.prior_failures(
                    state.task_id,
                    expected_version=state.workflow_version,
                    claim_token=state.claim_token,
                )),
                "handoff": _read_handoff(state.task_work_directory, state.phase.value),
            },
            "operator": {
                "revision": context.revision,
                "display_name": context.display_name,
                "operator_context": context.operator_context,
                "self_aliases": list(context.self_aliases),
                "institution_domains": list(context.institution_domains),
            },
        }

    def _instructions(self, state: ExecutionRunState) -> dict[str, Any]:
        """Return the instructions of the revision this claim is pinned to.

        The runner leaves the effective manifest beside the run state. Its
        digest is the revision, so instructions that were substituted, edited,
        or left over from another profile cannot be presented as this one's.
        """
        document = _read_private_json(
            self._state_path.parent / INSTRUCTIONS_NAME,
            maximum=MAX_INSTRUCTIONS_BYTES,
            label="execution instructions",
        )
        try:
            profile = parse_profile(document)
            if (
                profile.profile_id != state.agent_profile_id
                or profile.revision != state.agent_profile_revision
            ):
                raise AgentProfileError("agent profile revision is unavailable")
            rendered = profile.render_prompt(state.worker_command)
        except AgentProfileError:
            raise ExecutionWorkerConfigError(
                "execution instructions are unavailable"
            ) from None
        return {
            "profile_id": profile.profile_id,
            "revision": profile.revision,
            "display_name": profile.display_name,
            "instructions": rendered,
            "toolsets": list(profile.toolsets),
        }

    def search(
        self,
        query: str,
        *,
        layers: Sequence[str] = ("kb",),
        context_lines: int = 0,
        max_matches_per_document: int | None = None,
        max_results_per_layer: int = 10,
    ) -> dict[str, Any]:
        state, service = self._active()
        result = GwKnowledgeClient(self._knowledge_config).search(
            query,
            layers=layers,
            context_lines=context_lines,
            max_matches_per_document=max_matches_per_document,
            max_results_per_layer=max_results_per_layer,
        )
        self._renew(service, state)
        return _search_document(result)

    def act_worktree(self, *, repository: str | None = None) -> dict[str, Any]:
        """Prepare a working tree for this task's repository.

        Available in every phase, planning included. The phase boundary this
        worker enforces is the EFFECT, not the edit: nothing is pushed from
        here, and a proposal still requires act.pull-request in
        external_action. A planning run that has to write in order to answer
        its own question -- apply a candidate patch, run the suite against it,
        check that a proposed fix builds -- gets a tree of its own to do it in.

        Refusing it did not prevent that writing. It only meant the run found
        somewhere else to write, and the nearest writable repository on a host
        is a shared long-lived checkout that other work depends on.
        """
        state, service = self._active()
        origin = TaskLedger(state.database_path).origin(state.task_id)
        if origin is None:
            raise ExecutionWorkerClaimError(
                "this task has no origin, so it names no repository"
            )
        try:
            path, branch, base = forge_action.prepare_worktree(
                repository=repository or origin.record_id,
                issue=origin.item_id,
                parent=self._state_path.parent,
            )
        except forge_action.ForgeActionError as exc:
            raise ExecutionWorkerClaimError(str(exc)) from exc
        self._renew(service, state)
        return {
            "repository": repository or origin.record_id,
            "issue": origin.item_id,
            "path": str(path),
            "branch": branch,
            "base": base,
        }

    def act_review(self, *, body_file: str,
                   repository: str | None = None) -> dict[str, Any]:
        """Post one review on the pull request this task is about.

        Refused outside `external_action`, like every other forge write:
        the phase IS the approval. A reader approved posting this, and
        commenting while planning would bypass the gate that makes the
        approval mean anything.

        The pull request comes from the task's binding. A review task's
        identity names a state — `7/<when>` — so the number is the part
        before the state: a review of Thursday's state is still a review of
        pull request 7.
        """
        state, service = self._fresh_active("effect")
        if state.phase is not WorkflowPhase.EXTERNAL_ACTION:
            raise ExecutionWorkerClaimError(
                "an external action is only available in the external_action "
                "phase"
            )
        origin = TaskLedger(state.database_path).origin(state.task_id)
        if origin is None:
            raise ExecutionWorkerClaimError(
                "this task has no origin, so it names nothing to review"
            )
        if origin.kind != "review_request":
            raise ExecutionWorkerClaimError(
                "this task is not about a pull request awaiting review"
            )
        body = _read_private_text(
            self._state_path.parent / body_file,
            maximum=60_000, label="review body")
        try:
            receipt = forge_action.post_review(
                repository=repository or origin.record_id,
                number=origin.item_id.split("/", 1)[0],
                task_id=state.task_id,
                body=body,
            )
        except forge_action.ForgeActionError as exc:
            raise ExecutionWorkerClaimError(str(exc)) from exc
        result = {
            "kind": "review",
            "repository": receipt.repository,
            "number": receipt.number,
            "url": receipt.url,
        }
        _append_repository_receipt(self._state_path.parent, result)
        # Renewed only after the write succeeded, so a lease that lapses
        # mid-post is not extended by the attempt itself.
        self._renew(service, state)
        return result

    def act_mail(
        self, *, to: str, subject: str, body_file: str,
        attachments: str | None = None,
    ) -> dict[str, Any]:
        """Send an approved outbound message."""
        import subprocess
        state, service = self._fresh_active("effect")
        if state.phase is not WorkflowPhase.EXTERNAL_ACTION:
            raise ExecutionWorkerClaimError(
                "an external action is only available in the external_action phase"
            )
        if not to or "@" not in to:
            raise ExecutionWorkerClaimError("recipient address is invalid")

        body = _read_private_text(
            self._state_path.parent / body_file,
            maximum=60_000, label="message body",
        )

        cmd = ["outlook", "send", "--to", to, "--subject", subject]

        if attachments:
            for att in attachments.split(","):
                path = (self._state_path.parent / att.strip()).resolve()
                if not path.is_relative_to(self._state_path.parent.resolve()):
                    raise ExecutionWorkerClaimError(
                        "attachment must be a result artifact in the task folder"
                    )
                if not path.is_file():
                    raise ExecutionWorkerClaimError(f"attachment missing: {att}")
                cmd.extend(["--attachment", str(path)])

        try:
            subprocess.run(
                cmd, input=body, text=True, capture_output=True, check=True,
            )
        except subprocess.CalledProcessError as exc:
            raise ExecutionWorkerClaimError(
                f"mail failed: {exc.stderr.strip() or 'unknown error'}"
            ) from exc

        result = {
            "kind": "outbound-mail",
            "to": to,
            "subject": subject,
        }
        self._renew(service, state)
        return result

    def act_issue(self, *, title: str, body_file: str,
                  repository: str | None = None) -> dict[str, Any]:
        """Open one approved issue for a finding this task cannot itself fix.

        Refused outside `external_action`, like every other forge write: the
        phase IS the approval.

        This is the write that turns a finding into work. A comment is inert
        by design -- a pull request candidate is shaped from title, body and
        diff, and comments are never read -- so a finding with no pull request
        behind it had nowhere to go and was lost with the run directory.

        It is also the only write here that can create work for the system
        that issued it, because an open issue on an enrolled repository
        becomes a candidate and then a task. `forge_action` holds the bounds
        that follow from that, and checks them against the forge rather than
        against run state, since a task outlives any one run.
        """
        state, service = self._fresh_active("effect")
        if state.phase is not WorkflowPhase.EXTERNAL_ACTION:
            raise ExecutionWorkerClaimError(
                "an external action is only available in the external_action "
                "phase"
            )
        origin = TaskLedger(state.database_path).origin(state.task_id)
        if origin is None:
            raise ExecutionWorkerClaimError(
                "this task has no origin, so it names no repository to file on"
            )
        body = _read_private_text(
            self._state_path.parent / body_file,
            maximum=60_000, label="issue body")
        try:
            receipt = forge_action.open_issue(
                repository=repository or origin.record_id,
                task_id=state.task_id,
                title=title,
                body=body,
            )
        except forge_action.ForgeActionError as exc:
            raise ExecutionWorkerClaimError(str(exc)) from exc
        result = {
            "kind": "issue",
            "repository": receipt.repository,
            "number": receipt.number,
            "url": receipt.url,
        }
        _append_repository_receipt(self._state_path.parent, result)
        # Renewed only after the write succeeded, so a lease that lapses
        # mid-write is not extended by the attempt itself.
        self._renew(service, state)
        return result

    def act_comment(self, *, body_file: str) -> dict[str, Any]:
        """Post an approved status update on this task's own origin issue.

        The origin supplies both repository and issue number. The agent can
        choose the reviewed body but never redirect this external write to a
        similarly named record.
        """
        state, service = self._fresh_active("effect")
        if state.phase is not WorkflowPhase.EXTERNAL_ACTION:
            raise ExecutionWorkerClaimError(
                "an external action is only available in the external_action "
                "phase"
            )
        origin = TaskLedger(state.database_path).origin(state.task_id)
        if origin is None or origin.kind != "issue":
            raise ExecutionWorkerClaimError(
                "this task is not about an issue that can receive a comment"
            )
        body = _read_private_text(
            self._state_path.parent / body_file,
            maximum=60_000, label="issue comment body")
        try:
            receipt = forge_action.post_issue_comment(
                repository=origin.record_id, number=origin.item_id,
                task_id=state.task_id, body=body)
        except forge_action.ForgeActionError as exc:
            raise ExecutionWorkerClaimError(str(exc)) from exc
        result = {
            "kind": "issue-comment",
            "repository": receipt.repository,
            "number": receipt.number,
            "url": receipt.url,
        }
        _append_repository_receipt(self._state_path.parent, result)
        self._renew(service, state)
        return result

    def act_pull_request(self, *, head: str, title: str,
                         body_file: str | None,
                         repository: str | None = None) -> dict[str, Any]:
        """Open a pull request against this task's own origin.

        Refused outside `external_action`: the phase IS the approval. A reader
        approved an action for this phase, and performing forge writes while
        planning or executing would bypass the gate that makes the approval
        mean anything.
        """
        state, service = self._fresh_active("effect")
        if state.phase is not WorkflowPhase.EXTERNAL_ACTION:
            raise ExecutionWorkerClaimError(
                "an external action is only available in the external_action "
                "phase"
            )
        origin = TaskLedger(state.database_path).origin(state.task_id)
        if origin is None:
            raise ExecutionWorkerClaimError(
                "this task has no origin, so it names nothing to act on"
            )
        body = ""
        if body_file:
            body = _read_private_text(
                self._state_path.parent / body_file,
                maximum=60_000, label="pull request body")
        target = repository or origin.record_id
        worktree = (self._state_path.parent
                    / f"repo-{target.rsplit('/', 1)[-1]}-{origin.item_id}")
        try:
            if worktree.is_dir():
                # The branch is pushed from the tree this phase prepared, so
                # what is proposed is what was written here.
                forge_action.push_branch(
                    repository=target, path=worktree, head_branch=head,
                    base=forge_action.default_branch(target))
            receipt = forge_action.open_pull_request(
                repository=repository or origin.record_id,
                issue=origin.item_id,
                task_id=state.task_id,
                head=head,
                title=title,
                body=body,
            )
        except forge_action.ForgeActionError as exc:
            raise ExecutionWorkerClaimError(str(exc)) from exc
        result = {
            "kind": "pull-request",
            "repository": receipt.repository,
            "issue": receipt.issue,
            "number": receipt.number,
            "url": receipt.url,
            "head": receipt.head,
            "base": receipt.base,
        }
        _append_repository_receipt(self._state_path.parent, result)
        # Renewed only after the action succeeded: a lease that lapses mid-write
        # must not be extended by the attempt itself.
        self._renew(service, state)
        return result

    def read_thread(self) -> dict[str, Any]:
        """Read comments and review state on this task's own forge thread.

        Read-only: performs no write, requires no approval gate. Available
        in `plan` and `execute` so an agent can learn what a prior review
        said before repeating the work. The target comes from the task's
        binding, so the agent cannot redirect it to another thread.

        For an issue-origin task, returns comments on the issue. For a
        review_request task, returns reviews and comments on the pull
        request.
        """
        state, service = self._active()
        origin = TaskLedger(state.database_path).origin(state.task_id)
        if origin is None:
            raise ExecutionWorkerClaimError(
                "this task has no origin, so it names no thread to read")
        if origin.system != "gw":
            raise ExecutionWorkerClaimError(
                "thread reading is only available for forge-bound tasks")

        repository = origin.record_id
        try:
            if origin.kind == "issue":
                thread_result = forge_thread.read_issue_thread(
                    repository=repository,
                    number=origin.item_id,
                )
            elif origin.kind == "review_request":
                # The item_id for a review_request contains the PR number
                # followed by a state separator (e.g., "7/<when>").
                pr_number = origin.item_id.split("/", 1)[0]
                thread_result = forge_thread.read_pull_request_thread(
                    repository=repository,
                    number=pr_number,
                )
            else:
                raise ExecutionWorkerClaimError(
                    f"thread reading is not available for {origin.kind!r}")
        except forge_thread.ForgeThreadError as exc:
            raise ExecutionWorkerClaimError(str(exc)) from exc

        self._renew(service, state)
        return {
            "kind": thread_result.kind,
            "repository": thread_result.repository,
            "number": thread_result.number,
            "url": thread_result.url,
            "comments": thread_result.comments,
            "reviews": thread_result.reviews,
            "truncated": thread_result.truncated,
            "comment_count": len(thread_result.comments),
            "review_count": len(thread_result.reviews),
        }

    def record(self, draft_name: str) -> dict[str, Any]:
        state = load_run_state(self._state_path)
        service = TaskExecutionService(
            state.database_path,
            execution_grants=state.execution_grants,
            action_grants=state.action_grants,
        )
        draft_path, draft = load_result_draft(
            self._state_path.parent, draft_name
        )
        draft = _repository_result(state, draft, self._state_path.parent)
        envelope = ExecutionResultEnvelope(
            result_id=draft["result_id"],
            task_id=state.task_id,
            task_version=state.task_version,
            workflow_version=state.workflow_version,
            phase=state.phase.value,
            claim_token=state.claim_token,
            outcome=draft["outcome"],
            summary=draft["summary"],
            work_markdown=draft["work_markdown"],
            # Here rather than in `draft`, so the draft file keeps the
            # exact field set older drafts were written with, and rather
            # than at render time, where a remote call would sit inside
            # the transaction a card claim is waiting on. Returns "" on
            # any failure; the card then shows an excerpt instead.
            work_digest=work_digest.digest(draft["work_markdown"]),
            voice_summary=draft.get("voice_summary", "") or voice_summary.generate(
                draft["work_markdown"],
                summary=draft["summary"],
                deliverables=draft["deliverables"],
            ),
            questions=draft["questions"],
            external_actions=draft["external_actions"],
            deliverables=draft["deliverables"],
            repository_references=draft["repository_references"],
            repository_impact=draft["repository_impact"],
            task_work_directory=state.task_work_directory,
            task_kb_file=state.task_kb_file,
            # Which reader instruction this run was handed, so a result can
            # say what it was answering. Read from the delivery record rather
            # than re-selected, so it names what the agent actually got.
            reader_instruction_sequence=(
                service.delivered_reader_instruction_sequence(
                    state.task_id, expected_version=state.workflow_version)),
        )
        if state.task_run_directory is not None:
            if envelope.voice_summary:
                try:
                    audio = tts_client.synthesize(envelope.voice_summary)
                    if audio:
                        audio_path = self._state_path.parent / "voice_summary.wav"
                        tmp_audio = self._state_path.parent / "voice_summary.wav.tmp"
                        tmp_audio.write_bytes(audio)
                        os.chmod(tmp_audio, 0o600)
                        tmp_audio.replace(audio_path)
                        manifest_path = (
                            self._state_path.parent / ARTIFACT_MANIFEST_NAME
                        )
                        manifest = []
                        if manifest_path.exists():
                            try:
                                manifest = json.loads(
                                    manifest_path.read_text(encoding="utf-8"))
                            except Exception:
                                manifest = []
                        if "voice_summary.wav" not in manifest:
                            manifest.append("voice_summary.wav")
                            tmp_m = manifest_path.with_suffix(".tmp")
                            tmp_m.write_text(
                                json.dumps(manifest), encoding="utf-8")
                            os.chmod(tmp_m, 0o600)
                            tmp_m.replace(manifest_path)
                except Exception:
                    pass
            ledger = TaskLedger(state.database_path)
            task = ledger.get(state.task_id)
            origin = ledger.origin(state.task_id)
            if task is None:
                raise ExecutionWorkerClaimError("execution claim is unavailable")
            paths = TaskArchivePaths(
                Path(state.task_work_directory or ""),
                Path(state.task_kb_file or ""),
                Path(state.task_run_directory),
            )
            try:
                preserve_run_files(
                    self._state_path.parent,
                    paths.run_directory,
                    include_transcript=False,
                )
                envelope = replace(
                    envelope, artifacts=recorded_artifacts(paths.run_directory))
                append_result(
                    paths,
                    result=draft,
                    origin_kind=None if origin is None else origin.kind,
                    origin_record=None if origin is None else origin.record_id,
                    origin_item=None if origin is None else origin.item_id,
                )
            except TaskArchiveError:
                raise ExecutionWorkerDraftError(
                    "execution result review files could not be preserved"
                ) from None
        result = service.record_result(envelope)
        if result.disposition is WorkflowDisposition.REFUSED:
            # The ledger says exactly why. Discarding it left an agent to
            # guess: one tried to record three times, was told only
            # "operation refused" each time, and released a complete and
            # correct result rather than a wrong one.
            raise ExecutionWorkerDraftError(
                "execution result was refused",
                reason=None if result.refusal is None else result.refusal.value,
            )
        receipt = {
            "schema": RESULT_RECEIPT_SCHEMA,
            "schema_version": WORKER_SCHEMA_VERSION,
            "result_id": draft["result_id"],
            "disposition": result.disposition.value,
            "workflow_version": result.version,
            "status": result.status.value if result.status else None,
        }
        try:
            _replace_private_json(draft_path, receipt)
            _remove_result_inputs(self._state_path.parent)
        except (OSError, ExecutionWorkerError):
            # The durable database result is authoritative. A private draft
            # left behind is safer than reporting a false execution failure.
            pass
        return receipt

    def draft(self, *, outcome: str) -> dict[str, Any]:
        """Build one schema-valid draft without exposing result text in argv."""
        state, service = self._active()
        run_directory = self._state_path.parent
        # A result is authored where the reader will look for it. The task
        # folder is a synchronised directory the owner opens from another
        # machine to review the work; the run directory is private scratch on
        # this host that nobody browses. Reading only the run directory meant
        # an agent that put its result where the reader wanted it had its
        # finished work refused as missing, which is exactly backwards.
        #
        # The run directory stays a valid location so nothing that already
        # records keeps working by accident, but the task folder is tried
        # first because that is the intended home.
        search_directories = _result_search_path(state, run_directory)
        task_folder_not_before = _claim_started_at(self._state_path)
        result_id = state.run_id
        draft = _repository_result(state, {
            "outcome": outcome,
            "summary": _read_result_text(
                _locate_result(
                    search_directories, "result-summary.txt",
                    task_folder_not_before=task_folder_not_before,
                ),
                label="execution result summary",
            ),
            "work_markdown": _read_result_text(
                _locate_result(
                    search_directories, "result-work.md",
                    task_folder_not_before=task_folder_not_before,
                ),
                label="execution result work",
            ),
            "questions": _read_optional_string_array(
                _locate_result(
                    search_directories, "result-questions.json",
                    task_folder_not_before=task_folder_not_before,
                ),
                label="execution result questions",
            ),
            "external_actions": _read_optional_string_array(
                _locate_result(
                    search_directories, "result-external-actions.json",
                    task_folder_not_before=task_folder_not_before,
                ),
                label="execution result external actions",
            ),
            "deliverables": _read_optional_string_array(
                _locate_result(
                    search_directories, "result-deliverables.json",
                    task_folder_not_before=task_folder_not_before,
                ),
                label="execution result deliverables",
            ),
            "repository_references": _read_optional_repository_references(
                _locate_result(
                    search_directories, "result-repository-references.json",
                    task_folder_not_before=task_folder_not_before,
                ),
                label="execution result repository references",
            ),
            "repository_impact": _read_optional_repository_impact(
                _locate_result(
                    search_directories, "result-repository-impact.json",
                    task_folder_not_before=task_folder_not_before,
                ),
                label="execution result repository impact",
            ),
        }, run_directory)
        envelope = ExecutionResultEnvelope(
            result_id=result_id,
            task_id=state.task_id,
            task_version=state.task_version,
            workflow_version=state.workflow_version,
            phase=state.phase.value,
            claim_token=state.claim_token,
            outcome=draft["outcome"],
            summary=draft["summary"],
            work_markdown=draft["work_markdown"],
            questions=draft["questions"],
            external_actions=draft["external_actions"],
            deliverables=draft["deliverables"],
            repository_references=draft["repository_references"],
            repository_impact=draft["repository_impact"],
            task_work_directory=state.task_work_directory,
            task_kb_file=state.task_kb_file,
            # Which reader instruction this run was handed, so a result can
            # say what it was answering. Read from the delivery record rather
            # than re-selected, so it names what the agent actually got.
            reader_instruction_sequence=(
                service.delivered_reader_instruction_sequence(
                    state.task_id, expected_version=state.workflow_version)),
        )
        try:
            validated = _validated_result(envelope)
        except (TypeError, ValueError) as exc:
            # The ledger says which input it refused and why. Discarding
            # that left an agent to guess: one wrote a correct review, was
            # told only "operation refused" three times, and the workflow
            # parked with the review still on disk. The message names a
            # field and a rule, never task content.
            raise ExecutionWorkerDraftError(
                f"execution result inputs are invalid: {exc}",
                reason="invalid_result_inputs",
            ) from None
        self._renew(service, state)
        draft_name = f"result-{result_id}.json"
        _write_new_private_json(
            run_directory / draft_name,
            {
                "schema": RESULT_DRAFT_SCHEMA,
                "schema_version": WORKER_SCHEMA_VERSION,
                "result_id": result_id,
                "outcome": validated["outcome"],
                "summary": validated["summary"],
                "work_markdown": validated["work_markdown"],
                "questions": list(validated["questions"]),
                "external_actions": list(validated["external_actions"]),
                "deliverables": list(validated["deliverables"]),
                "repository_references": list(
                    validated["repository_references"]),
                "repository_impact": validated["repository_impact"],
            },
        )
        return {
            "schema": RESULT_DRAFT_READY_SCHEMA,
            "schema_version": WORKER_SCHEMA_VERSION,
            "draft": draft_name,
        }

    def release(self) -> dict[str, Any]:
        state, service = self._active()
        if _result_inputs_present(
            _result_search_path(state, self._state_path.parent),
            task_folder_not_before=_claim_started_at(self._state_path),
        ):
            if state.phase is WorkflowPhase.PLAN:
                ready = self.draft(outcome="awaiting_plan")
                return self.record(ready["draft"])
            raise ExecutionWorkerDraftError(
                "execution result inputs must be drafted or removed before "
                "release"
            )
        result = service.release(
            state.task_id,
            expected_version=state.workflow_version,
            claim_token=state.claim_token,
        )
        if result.disposition is WorkflowDisposition.REFUSED:
            raise ExecutionWorkerClaimError("execution claim is unavailable")
        return {
            "schema": "foxhound.execution-release-receipt",
            "schema_version": WORKER_SCHEMA_VERSION,
            "disposition": result.disposition.value,
            "status": result.status.value if result.status else None,
        }

    def _active(self) -> tuple[ExecutionRunState, TaskExecutionService]:
        state = load_run_state(self._state_path)
        service = TaskExecutionService(
            state.database_path,
            execution_grants=state.execution_grants,
            action_grants=state.action_grants,
        )
        self._renew(service, state)
        return state, service

    def _fresh_active(
        self, checkpoint: str
    ) -> tuple[ExecutionRunState, TaskExecutionService]:
        """Fail closed when a bound source no longer matches this claim.

        Calls at phase entry and immediately before each forge effect are
        deliberately separate: a long-running agent must not publish against
        a source that changed after it started.
        """
        # Checked before anything else and regardless of what is bound: a
        # caller naming a checkpoint that does not exist is a mistake in this
        # file, and it must not depend on whether a task happens to have a
        # source for it to be noticed.
        if checkpoint not in CHECKPOINTS:
            raise ExecutionWorkerConfigError(
                "source freshness checkpoint is invalid")
        state, service = self._active()
        try:
            request = TaskLedger(state.database_path).source_snapshot_request(
                state.task_id
            )
        except (SourceSnapshotContractError, ValueError):
            raise ExecutionWorkerClaimError("source freshness is unavailable") from None
        try:
            required = request is not None and self._policy.checks_freshness(
                checkpoint, request.locator.kind)
        except WorkflowPolicyError:
            raise ExecutionWorkerConfigError(
                "source freshness checkpoint is invalid") from None
        if not required:
            return state, service
        try:
            result = GwKnowledgeClient(self._knowledge_config).refresh_source(
                request
            )
        except KnowledgeClientError:
            raise ExecutionWorkerClaimError("source freshness is unavailable") from None
        if not result.usable:
            raise ExecutionWorkerClaimError(
                "source freshness no longer matches this execution claim"
            )
        return state, service

    @staticmethod
    def _renew(
        service: TaskExecutionService, state: ExecutionRunState
    ) -> None:
        result = service.renew(
            state.task_id,
            expected_version=state.workflow_version,
            claim_token=state.claim_token,
            lease_seconds=state.lease_seconds,
        )
        if (
            result.disposition is WorkflowDisposition.REFUSED
            or result.status is not WorkflowStatus.RUNNING
        ):
            raise ExecutionWorkerClaimError("execution claim is unavailable")


def load_run_state(path: str | os.PathLike[str]) -> ExecutionRunState:
    state_path = Path(path)
    document = _read_private_json(
        state_path, maximum=MAX_STATE_BYTES, label="execution run state"
    )
    base_fields = {
        "schema", "schema_version", "run_id", "database_path",
        "task_id", "task_version", "workflow_version", "phase",
        "claim_token", "lease_seconds", "agent_profile_id",
        "agent_profile_revision", "knowledge_root", "worker_command",
    }
    version = document.get("schema_version")
    archive_fields = {
        "task_work_directory", "task_kb_file", "task_run_directory",
    }
    grant_fields = {"execution_grants", "action_grants"}
    root_fields = {"deployment_roots"}
    _exact_fields(
        document,
        base_fields | (
            archive_fields if version in {4, 5, RUN_STATE_SCHEMA_VERSION} else set()
        ) | (grant_fields if version in {5, RUN_STATE_SCHEMA_VERSION} else set())
        | (root_fields if version == RUN_STATE_SCHEMA_VERSION else set()),
        "execution run state",
    )
    if (
        document["schema"] != RUN_STATE_SCHEMA
        or document["schema_version"] not in {3, 4, 5, RUN_STATE_SCHEMA_VERSION}
        or isinstance(document["schema_version"], bool)
        or not isinstance(document["run_id"], str)
        or not _RUN_ID_RE.fullmatch(document["run_id"])
    ):
        raise ExecutionWorkerConfigError("execution run state is invalid")
    database = _canonical_existing_file(
        document["database_path"], "execution database"
    )
    for name in ("task_id", "task_version", "workflow_version"):
        value = document[name]
        if isinstance(value, bool) or not isinstance(value, int) or value < 1:
            raise ExecutionWorkerConfigError("execution run state is invalid")
    lease = document["lease_seconds"]
    if (
        isinstance(lease, bool)
        or not isinstance(lease, int)
        or not 5 <= lease <= 3_600
    ):
        raise ExecutionWorkerConfigError("execution run state is invalid")
    token = document["claim_token"]
    if (
        not isinstance(token, str)
        or not 32 <= len(token) <= 512
        or any(char.isspace() for char in token)
    ):
        raise ExecutionWorkerConfigError("execution run state is invalid")
    profile_id = document["agent_profile_id"]
    profile_revision = document["agent_profile_revision"]
    worker_command = document["worker_command"]
    if (
        not isinstance(profile_id, str)
        or not _PROFILE_ID_RE.fullmatch(profile_id)
        or not isinstance(profile_revision, str)
        or not _REVISION_RE.fullmatch(profile_revision)
        or not isinstance(worker_command, str)
        or not is_worker_command(worker_command)
    ):
        raise ExecutionWorkerConfigError("execution run state is invalid")
    try:
        phase = WorkflowPhase(document["phase"])
    except (TypeError, ValueError):
        raise ExecutionWorkerConfigError(
            "execution run state is invalid"
        ) from None
    task_work_directory = _archive_path(
        document.get("task_work_directory"), kind="directory"
    )
    task_kb_file = _archive_path(document.get("task_kb_file"), kind="file")
    task_run_directory = _archive_path(
        document.get("task_run_directory"), kind="directory"
    )
    if len({value is None for value in (
        task_work_directory, task_kb_file, task_run_directory
    )}) != 1:
        raise ExecutionWorkerConfigError("execution run state is invalid")
    try:
        execution_grants = _execution_grants(document.get("execution_grants"))
        action_grants = _action_grants(document.get("action_grants"))
    except ValueError:
        raise ExecutionWorkerConfigError("execution run state is invalid") from None
    return ExecutionRunState(
        run_id=document["run_id"],
        database_path=database,
        task_id=document["task_id"],
        task_version=document["task_version"],
        workflow_version=document["workflow_version"],
        phase=phase,
        claim_token=token,
        lease_seconds=lease,
        agent_profile_id=profile_id,
        agent_profile_revision=profile_revision,
        worker_command=worker_command,
        execution_grants=execution_grants,
        action_grants=action_grants,
        knowledge_root=_knowledge_root(document["knowledge_root"]),
        deployment_roots=_deployment_roots(
            document.get("deployment_roots") if version == RUN_STATE_SCHEMA_VERSION else {}
        ),
        task_work_directory=task_work_directory,
        task_kb_file=task_kb_file,
        task_run_directory=task_run_directory,
    )


def _archive_path(value: object, *, kind: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value or "\0" in value:
        raise ExecutionWorkerConfigError("execution run state is invalid")
    path = Path(value)
    valid = path.is_dir() if kind == "directory" else path.is_file()
    if not path.is_absolute() or path.is_symlink() or not valid:
        raise ExecutionWorkerConfigError("execution run state is invalid")
    return str(path)


def _knowledge_root(value: object) -> str | None:
    """Where this machine keeps its knowledge base, if it has one.

    None is ordinary: a host may run an agent without one. It is a
    directory the agent reads and never writes, and the path is per host
    because the sync roots differ across the fleet.
    """
    if value is None:
        return None
    if not isinstance(value, str) or not value:
        raise ExecutionWorkerConfigError("execution run state is invalid")
    path = Path(value)
    if not path.is_absolute() or not path.is_dir():
        raise ExecutionWorkerConfigError("execution run state is invalid")
    return str(path)


def _deployment_roots(value: object) -> dict[str, str]:
    if not isinstance(value, dict):
        raise ExecutionWorkerConfigError("execution run state is invalid")
    roots: dict[str, str] = {}
    for name, raw_path in value.items():
        if (
            not isinstance(name, str)
            or not re.fullmatch(r"[a-z][a-z0-9_-]{0,31}", name)
            or not isinstance(raw_path, str)
        ):
            raise ExecutionWorkerConfigError("execution run state is invalid")
        path = Path(raw_path)
        # Absoluteness is a property of the recorded value; existence is not.
        # The runner already refuses to start with a root that is not a
        # directory, and a mount that drops mid-run must not make the run's
        # own state unreadable.
        if not path.is_absolute():
            raise ExecutionWorkerConfigError("execution run state is invalid")
        roots[name] = str(path)
    return dict(sorted(roots.items()))


def load_result_draft(
    run_directory: Path, name: str
) -> tuple[Path, dict[str, Any]]:
    if not isinstance(name, str):
        raise ExecutionWorkerDraftError("execution result draft is invalid")
    supplied = Path(name)
    if supplied.is_absolute():
        if supplied.parent != run_directory:
            raise ExecutionWorkerDraftError(
                "execution result draft is invalid"
            )
        name = supplied.name
    elif supplied.parent != Path("."):
        raise ExecutionWorkerDraftError("execution result draft is invalid")
    match = _RESULT_NAME_RE.fullmatch(name)
    if match is None:
        raise ExecutionWorkerDraftError("execution result draft is invalid")
    path = run_directory / name
    try:
        document = _read_private_json(
            path, maximum=MAX_DRAFT_BYTES, label="execution result draft"
        )
    except ExecutionWorkerConfigError as exc:
        raise ExecutionWorkerDraftError(
            "execution result draft is invalid"
        ) from exc
    fields = {
        "schema", "schema_version", "result_id", "outcome", "summary",
        "work_markdown", "questions", "external_actions", "deliverables",
    }
    # A ready draft may predate structured evidence. It is private, short
    # lived, and already bound to this claim, so preserve it as an empty
    # collection rather than forcing an agent to recreate a correct result.
    supplied = set(document)
    allowed_optionals = {"repository_references", "repository_impact", "voice_summary"}
    if not (fields <= supplied <= fields | allowed_optionals):
        raise ExecutionWorkerDraftError("execution result draft is invalid")
    document.setdefault("repository_references", [])
    document.setdefault("repository_impact", True)
    document.setdefault("voice_summary", "")
    if (
        document["schema"] != RESULT_DRAFT_SCHEMA
        or document["schema_version"] != WORKER_SCHEMA_VERSION
        or isinstance(document["schema_version"], bool)
        or document["result_id"] != match.group(1)
    ):
        raise ExecutionWorkerDraftError("execution result draft is invalid")
    return path, document


def load_worker_from_environment(
    environ: Mapping[str, str] | None = None,
) -> ExecutionWorker:
    values = os.environ if environ is None else environ
    state = values.get(STATE_ENV, "")
    endpoint = values.get(GW_ENDPOINT_ENV, "")
    alias = values.get(GW_ALIAS_ENV, "")
    token_path = values.get(GW_TOKEN_FILE_ENV, "")
    if not all((state, endpoint, alias, token_path)):
        raise ExecutionWorkerConfigError(
            "execution worker configuration is unavailable"
        )
    config = load_knowledge_config(endpoint, alias, token_path)
    return ExecutionWorker(
        state, config,
        policy=_workflow_policy(values.get(WORKFLOW_POLICY_ENV, "")),
    )


def _workflow_policy(value: object) -> WorkflowPolicy | None:
    """Parse the deployed policy, or fall back to the compatibility one."""
    if not isinstance(value, str):
        raise ExecutionWorkerConfigError("workflow policy configuration is invalid")
    if not value.strip():
        return None
    try:
        return parse_workflow_policy(json.loads(value))
    except (ValueError, TypeError):
        # Failing closed on an unreadable policy would stop every run; failing
        # open would silently drop a check a deployment asked for. Refuse to
        # start instead, which is loud and happens once.
        raise ExecutionWorkerConfigError(
            "workflow policy configuration is invalid") from None


def load_knowledge_config(
    endpoint: str, alias: str, token_path: str | os.PathLike[str]
) -> KnowledgeClientConfig:
    token = _read_private_text(
        Path(token_path), maximum=4_097, label="knowledge token"
    ).strip()
    try:
        return KnowledgeClientConfig(
            endpoint=endpoint,
            alias=alias,
            token=token,
        )
    except KnowledgeClientError as exc:
        raise ExecutionWorkerConfigError(
            "execution worker configuration is invalid"
        ) from exc


def _search_document(result: KnowledgeSearchResult) -> dict[str, Any]:
    return {
        "schema": WORKER_SEARCH_SCHEMA,
        "schema_version": WORKER_SCHEMA_VERSION,
        "layers": [
            {
                "name": layer.name,
                "total_results": layer.total_results,
                "truncated": layer.truncated,
                "documents": [
                    {
                        "id": document.id,
                        "path": document.path,
                        "excerpt": document.excerpt,
                        "kb_path": document.kb_path,
                        "section": document.section,
                        "ranking_score": document.ranking_score,
                    }
                    for document in layer.documents
                ],
            }
            for layer in result.layers
        ],
    }


def _read_private_json(
    path: Path, *, maximum: int, label: str
) -> dict[str, Any]:
    raw = _read_private_bytes(path, maximum=maximum, label=label)
    try:
        value = json.loads(raw, object_pairs_hook=_strict_object)
    except (UnicodeDecodeError, ValueError, TypeError):
        raise ExecutionWorkerConfigError(f"{label} is invalid") from None
    if not isinstance(value, dict):
        raise ExecutionWorkerConfigError(f"{label} is invalid")
    return value


def _result_search_path(state, run_directory: Path) -> tuple[Path, ...]:
    """Where an authored result may legitimately live, in preference order.

    The task folder first: it is the synchronised directory the owner reviews
    from, so a result written there is a result the reader can actually open.
    The run directory second, because results authored in the agent's own
    working directory were the only ones accepted before this and must keep
    recording unchanged.

    A task folder is only offered when the workflow carries one; a claim
    without one falls back to the run directory alone rather than guessing.
    """
    directories: list[Path] = []
    task_folder = getattr(state, "task_work_directory", None)
    if task_folder:
        candidate = Path(task_folder)
        if candidate.is_absolute():
            directories.append(candidate)
    directories.append(run_directory)
    return tuple(directories)


def _claim_started_at(state_path: Path) -> int | None:
    """The private run-state mtime, used to fence shared result inputs.

    The runner writes this file after it creates the task archive and before
    it launches the worker.  It is therefore the durable start marker for the
    claim.  If it cannot be read, accepting a task-folder file would risk
    harvesting a prior claim, so callers deliberately fall through to private
    run inputs instead.
    """
    try:
        return state_path.stat().st_mtime_ns
    except OSError:
        return None


def _fenced_out(
    index: int,
    directories: tuple[Path, ...],
    candidate: Path,
    task_folder_not_before: int | None,
) -> bool:
    """Whether a shared task-folder file predates this claim.

    Only the task folder is fenced, and only when a private run directory
    exists to fall back to.  A claim that cannot read its own anchor fences
    everything shared, because harvesting a prior claim is the worse failure.
    """
    if index != 0 or len(directories) <= 1:
        return False
    if task_folder_not_before is None:
        return True
    try:
        return candidate.stat().st_mtime_ns < task_folder_not_before
    except OSError:
        return True


def _locate_result(
    directories: tuple[Path, ...],
    name: str,
    *,
    task_folder_not_before: int | None = None,
) -> Path:
    """The first directory that actually holds `name`.

    When none does, the first candidate is returned so the caller reports the
    location the reader was most likely aiming at, rather than the private
    scratch directory they never chose.
    """
    fenced_out = False
    for index, directory in enumerate(directories):
        candidate = directory / name
        try:
            if candidate.is_file():
                # The task folder is deliberately durable and shared between
                # runs.  It can be a source for this claim only when this
                # exact file was written after the run-state anchor.  A stale
                # file must never outrank the current run's private input.
                if _fenced_out(
                    index, directories, candidate, task_folder_not_before
                ):
                    fenced_out = True
                    continue
                return candidate
        except OSError:
            continue
    # Nothing matched.  Naming the task folder is the friendlier report when
    # the agent simply never wrote the file -- but not when a file IS there
    # and was rejected as stale.  Returning it then hands the caller the very
    # path the fence just refused, and the readers read it happily, which
    # restores the whole defect for any name this run did not author itself.
    return (directories[-1] if fenced_out else directories[0]) / name


def _result_read_failure(path: Path, exc: Exception) -> str:
    """A safe, specific reason a result file could not be read.

    Paths here are the agent's own working locations, which it supplied or
    was given; echoing one back tells it nothing it did not already know.
    """
    reason = str(exc)
    if not path.exists():
        return f"not found at {path}"
    if path.is_dir():
        return f"is a directory, not a file: {path}"
    try:
        mode = path.lstat().st_mode
    except OSError:
        return f"could not be read at {path}"
    if mode & 0o077:
        return (
            f"is readable by others at {path}; results must be owner-only "
            f"(chmod 600)"
        )
    if "not private" in reason:
        return (
            f"sits in a directory readable by others: {path.parent}; the "
            f"folder must be owner-only (chmod 700)"
        )
    try:
        if path.stat().st_size > MAX_DRAFT_BYTES:
            return f"is larger than {MAX_DRAFT_BYTES} bytes: {path}"
    except OSError:
        pass
    return f"could not be decoded as UTF-8: {path}"


def _read_result_text(path: Path, *, label: str) -> str:
    """Read one authored result file, saying which condition actually failed.

    Every underlying refusal used to arrive as "<label> is invalid", which
    covers a missing file, a world-readable one, an oversized one and a
    mis-encoded one alike. An agent told only "invalid" goes looking for a
    content rule that does not exist -- and the one observed doing so spent
    the rest of its turn budget reading worker source, then lost finished
    work it had merely written somewhere else. Naming the condition, and the
    path we looked at, is the difference between a fixable mistake and a
    dead run.
    """
    try:
        value = _read_private_text(path, maximum=MAX_DRAFT_BYTES, label=label)
    except ExecutionWorkerConfigError as exc:
        raise ExecutionWorkerDraftError(
            f"{label}: {_result_read_failure(path, exc)}"
        ) from exc
    if value.endswith("\n"):
        value = value[:-1]
        if value.endswith("\r"):
            value = value[:-1]
    return value


def _read_optional_string_array(
    path: Path, *, label: str
) -> list[object]:
    """Read result lines and structured action records.

    Most result collections are short display lines.  External actions may
    instead be records so an execute-phase handoff can name its exact target.
    Keep that distinction here: accepting only strings makes the documented
    origin-targeted action impossible to express.
    """
    try:
        path.lstat()
    except FileNotFoundError:
        return []
    except OSError as exc:
        raise ExecutionWorkerDraftError(f"{label} is unavailable") from exc
    try:
        raw = _read_private_bytes(path, maximum=MAX_DRAFT_BYTES, label=label)
        value = json.loads(raw, object_pairs_hook=_strict_object)
    except (
        ExecutionWorkerConfigError,
        UnicodeDecodeError,
        ValueError,
        TypeError,
    ) as exc:
        raise ExecutionWorkerDraftError(f"{label} is invalid") from exc
    if not isinstance(value, list):
        raise ExecutionWorkerDraftError(f"{label} is invalid")
    for item in value:
        # A line, or a record whose fields are all lines. The ledger decides
        # which field names it will keep; this only refuses shapes that
        # could never be one, so an agent learns the real rule from the
        # rejection rather than from here.
        if isinstance(item, str):
            continue
        if isinstance(item, dict) and all(
            isinstance(field_value, str) for field_value in item.values()
        ):
            continue
        raise ExecutionWorkerDraftError(f"{label} is invalid")
    return value


def _read_optional_repository_references(
    path: Path, *, label: str,
) -> list[object]:
    """Read the structured evidence input without accepting arbitrary JSON."""
    try:
        path.lstat()
    except FileNotFoundError:
        return []
    except OSError as exc:
        raise ExecutionWorkerDraftError(f"{label} is unavailable") from exc
    try:
        raw = _read_private_bytes(path, maximum=MAX_DRAFT_BYTES, label=label)
        value = json.loads(raw, object_pairs_hook=_strict_object)
    except (
        ExecutionWorkerConfigError,
        UnicodeDecodeError,
        ValueError,
        TypeError,
    ) as exc:
        raise ExecutionWorkerDraftError(f"{label} is invalid") from exc
    if (not isinstance(value, list) or any(
            not isinstance(item, dict) for item in value)):
        raise ExecutionWorkerDraftError(f"{label} is invalid")
    return value


def _read_optional_repository_impact(
    path: Path, *, label: str,
) -> bool:
    """Read the explicit exception for analysis-only repository work.

    The safe default is true: an absent file must not let implementation
    silently bypass its repository follow-through.
    """
    try:
        path.lstat()
    except FileNotFoundError:
        return True
    except OSError as exc:
        raise ExecutionWorkerDraftError(f"{label} is unavailable") from exc
    try:
        raw = _read_private_bytes(path, maximum=64, label=label)
        value = json.loads(raw, object_pairs_hook=_strict_object)
    except (
        ExecutionWorkerConfigError,
        UnicodeDecodeError,
        ValueError,
        TypeError,
    ) as exc:
        raise ExecutionWorkerDraftError(f"{label} is invalid") from exc
    if not isinstance(value, bool):
        raise ExecutionWorkerDraftError(f"{label} is invalid")
    return value


def _read_private_text(path: Path, *, maximum: int, label: str) -> str:
    raw = _read_private_bytes(path, maximum=maximum, label=label)
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError:
        raise ExecutionWorkerConfigError(f"{label} is invalid") from None


def _read_private_bytes(path: Path, *, maximum: int, label: str) -> bytes:
    _require_private_path(path, label)
    flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(path, flags)
    except OSError:
        raise ExecutionWorkerConfigError(f"{label} is unavailable") from None
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode) or info.st_mode & 0o077:
            raise ExecutionWorkerConfigError(f"{label} is not private")
        with os.fdopen(descriptor, "rb", closefd=False) as handle:
            raw = handle.read(maximum + 1)
    finally:
        os.close(descriptor)
    if len(raw) > maximum:
        raise ExecutionWorkerConfigError(f"{label} is too large")
    return raw


def _require_private_path(path: Path, label: str) -> None:
    if not path.is_absolute():
        raise ExecutionWorkerConfigError(f"{label} path is invalid")
    try:
        if path.resolve(strict=True) != path:
            raise ExecutionWorkerConfigError(f"{label} path is invalid")
        parent = path.parent.stat()
        info = path.lstat()
    except OSError:
        raise ExecutionWorkerConfigError(f"{label} is unavailable") from None
    if (
        not stat.S_ISDIR(parent.st_mode)
        or parent.st_mode & 0o077
        or not stat.S_ISREG(info.st_mode)
        or info.st_mode & 0o077
    ):
        raise ExecutionWorkerConfigError(f"{label} is not private")


def _canonical_existing_file(value: object, label: str) -> Path:
    if not isinstance(value, str) or not value:
        raise ExecutionWorkerConfigError(f"{label} path is invalid")
    path = Path(value)
    if not path.is_absolute():
        raise ExecutionWorkerConfigError(f"{label} path is invalid")
    try:
        info = path.lstat()
        if (
            path.resolve(strict=True) != path
            or not stat.S_ISREG(info.st_mode)
            or info.st_mode & 0o077
        ):
            raise ExecutionWorkerConfigError(f"{label} path is invalid")
    except OSError:
        raise ExecutionWorkerConfigError(f"{label} is unavailable") from None
    return path


def _replace_private_json(path: Path, document: dict[str, Any]) -> None:
    payload = (
        json.dumps(
            document, ensure_ascii=True, separators=(",", ":"), sort_keys=True
        ) + "\n"
    ).encode("utf-8")
    _require_private_path(path, "execution result receipt")
    flags = os.O_WRONLY | os.O_TRUNC
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(path, flags)
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode) or info.st_mode & 0o077:
            raise ExecutionWorkerDraftError(
                "execution result receipt is not private"
            )
        with os.fdopen(descriptor, "wb", closefd=False) as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
    finally:
        os.close(descriptor)


def _write_new_private_json(path: Path, document: dict[str, Any]) -> None:
    payload = (
        json.dumps(
            document, ensure_ascii=True, separators=(",", ":"), sort_keys=True
        ) + "\n"
    ).encode("utf-8")
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(path, flags, 0o600)
    except OSError:
        raise ExecutionWorkerDraftError(
            "execution result draft is unavailable"
        ) from None
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


def _repository_origin(state: ExecutionRunState):
    """Return the addressable forge origin for this run, if it has one."""
    origin = TaskLedger(state.database_path).origin(state.task_id)
    if (
        origin is None
        or origin.kind not in {"issue", "review_request"}
        or not origin.record_id.startswith("github.com/")
    ):
        return None
    return origin


def _repository_result(
    state: ExecutionRunState, draft: Mapping[str, Any], run_directory: Path,
) -> dict[str, Any]:
    """Require a visible repository follow-through for forge work.

    Planning may remain a local review decision.  Once implementation is
    complete, however, GitHub work must wait for an approved outside action;
    an external completion is accepted only when the bounded worker action
    left a durable receipt.  The receipt becomes a card deliverable instead of
    relying on the agent to copy a URL from a terminal response.
    """
    result = dict(draft)
    repository_impact = result.get("repository_impact", True)
    if not isinstance(repository_impact, bool):
        raise ExecutionWorkerDraftError("repository impact is invalid")
    result["repository_impact"] = repository_impact
    origin = _repository_origin(state)
    if origin is None:
        return result
    references = list(result.get("repository_references") or ())
    outcome = result.get("outcome")
    actions = result.get("external_actions")
    if not result.get("deliverables") and not (
        state.phase is WorkflowPhase.EXTERNAL_ACTION
        and outcome == "completed"
    ):
        raise ExecutionWorkerDraftError(
            "repository result must name a deliverable"
        )
    if (
        repository_impact
        and outcome == "ineligible"
        and state.phase in (WorkflowPhase.PLAN, WorkflowPhase.EXECUTE)
    ):
        # `ineligible` means the prerequisites for the work do not exist and
        # nothing smaller is valid. A run that changed the repository has
        # already demonstrated otherwise, so the two cannot both be true.
        #
        # This closes the last way to end repository work without publishing
        # it. `completed` is refused just below; `ineligible` was not, and it
        # does not advance a phase either, so the workflow went to review and
        # an ordinary `done` closed it as finished while nothing had been
        # pushed. The run directory is reclaimed afterwards, so the branch the
        # result named stopped existing -- a silent loss that reads as success
        # in every count.
        #
        # A genuinely blocked run keeps its outcome by reporting the truth
        # about its effect: `repository_impact: false` with `ineligible` is
        # still accepted.
        raise ExecutionWorkerDraftError(
            "repository work that changed the repository cannot be ineligible"
        )
    origin_kind = getattr(origin, "kind", "")
    publication_is_the_work = (
        isinstance(origin_kind, str)
        and _publication_is_the_deliverable(origin_kind)
    )
    if state.phase is WorkflowPhase.EXECUTE and repository_impact:
        if outcome == "completed" and not references:
            raise ExecutionWorkerDraftError(
                "repository execution must await an approved follow-through; "
                "request an external action, or name the existing follow-through in "
                "repository references if it is already published. "
                "For analysis-only work write JSON false to "
                "result-repository-impact.json"
            )
    if state.phase is WorkflowPhase.PLAN and repository_impact:
        if outcome == "completed" and not references:
            raise ExecutionWorkerDraftError(
                "a planning run that changed the repository must record "
                "awaiting_plan; for analysis-only work write JSON false to "
                "result-repository-impact.json"
            )
    if (
        state.phase in (WorkflowPhase.PLAN, WorkflowPhase.EXECUTE)
        and publication_is_the_work
        and not repository_impact
        and outcome == "completed"
        and not references
    ):
        # The guard above asks a run that changed the repository to publish
        # what it changed, and offers `repository_impact: false` to work that
        # changed nothing. For most origins that exemption is right: an issue
        # can ask a question, and the answer belongs on the card.
        #
        # A review is the exception. It reads a pull request and changes no
        # files, so the flag is truthfully false and the run took the
        # exemption -- but its entire deliverable was the remote write the
        # exemption excused. `completed` was accepted with no action and no
        # receipt, the workflow went to review, and an ordinary `done` closed
        # it while the findings existed only in the run directory, which is
        # reclaimed afterwards. Analysis-only and review are the same shape to
        # every check here and opposite in what they owe.
        #
        # `_required_repository_receipt_kinds` already knows what each origin
        # owes; it was consulted only in `external_action`, which a completed
        # execute phase never reaches.
        #
        # Naming follow-through that already exists still completes the task.
        # That is how a re-surfaced task stops instead of repeating published
        # work, so it stays open to a run that writes nothing itself.
        raise ExecutionWorkerDraftError(
            f"repository {origin_kind} completion requires published "
            "follow-through: request the action on its origin, or name the "
            "existing follow-through in repository references"
        )
    if (
        state.phase is WorkflowPhase.EXECUTE
        and (repository_impact or publication_is_the_work)
        and outcome == "awaiting_external"
        and not _has_origin_follow_through_action(actions, origin)
    ):
        expected = _origin_target_url(origin)
        target_info = (
            f": result-external-actions.json must contain an action object with "
            f"'target': '{expected}' and 'action': '...'"
            if expected else ""
        )
        raise ExecutionWorkerDraftError(
            f"repository execution must request an action targeting its origin{target_info}"
        )
    if (
        state.phase is WorkflowPhase.EXTERNAL_ACTION
        and outcome == "completed"
    ):
        receipts = _repository_receipts(run_directory)
        if not receipts:
            raise ExecutionWorkerDraftError(
                "repository completion requires a worker action receipt"
            )
        required = _required_repository_receipt_kinds(origin.kind)
        received = {receipt["kind"] for receipt in receipts}
        if origin.kind == "review_request" and "issue-comment" in received:
            received = received | {"review"}
        missing = required - received
        if missing:
            names = " and ".join(sorted(missing))
            raise ExecutionWorkerDraftError(
                f"repository {origin.kind} completion requires {names} receipt"
            )
        result["deliverables"] = [
            *list(result.get("deliverables") or ()),
            *[
                "Repository follow-through: "
                f"[{receipt['kind']}]({receipt['url']})"
                for receipt in receipts
            ],
        ]
        references.extend(
            {"kind": "pull-request", "url": receipt["url"]}
            for receipt in receipts
            if receipt["kind"] == "pull-request"
        )
    origin_record = getattr(origin, "record_id", None)
    if isinstance(origin_record, str):
        for reference in references:
            if not isinstance(reference, dict) or not _reference_matches_origin(
                    reference.get("url"), origin_record):
                raise ExecutionWorkerDraftError(
                    "repository references must belong to the task origin"
                )
    result["repository_references"] = references
    return result


def _origin_target_url(origin: object) -> str | None:
    """Derive the canonical forge target URL for an issue or review request."""
    record_id = getattr(origin, "record_id", None)
    item_id = getattr(origin, "item_id", None)
    kind = getattr(origin, "kind", None)
    if not all(isinstance(value, str) and value for value in
               (record_id, item_id, kind)):
        return None
    path = "issues" if kind == "issue" else "pull"
    return f"https://{record_id}/{path}/{item_id.split('/', 1)[0]}"


def _has_origin_follow_through_action(
    actions: object, origin: object,
) -> bool:
    """Require an approval card to name the exact pending forge update."""
    if not isinstance(actions, list):
        return False
    expected = _origin_target_url(origin)
    if expected is None:
        return False
    return any(
        isinstance(action, dict) and action.get("target") == expected
        for action in actions
    )


def _reference_matches_origin(url: object, record_id: str) -> bool:
    """Keep structured evidence on the same forge repository as its task."""
    if not isinstance(url, str) or not url.startswith("https://github.com/"):
        return False
    remainder = url.removeprefix("https://")
    parts = remainder.split("/")
    return len(parts) >= 3 and "/".join(parts[:3]) == record_id


def _required_repository_receipt_kinds(origin_kind: str) -> frozenset[str]:
    """Return the visible follow-through required to complete a forge task.

    An issue implementation is not discoverable unless it has both a proposed
    change and an update on the originating issue.  A review task instead
    requires a durable review receipt.  The worker creates every receipt, so
    this check cannot be satisfied by an agent-authored URL.
    """
    if origin_kind == "issue":
        return frozenset({"pull-request", "issue-comment"})
    if origin_kind == "review_request":
        return frozenset({"review"})
    return frozenset()


#: Receipt kinds that can only exist because the run changed the repository.
#: A pull request needs a branch; a comment or a review needs neither.
_CHANGE_BACKED_RECEIPT_KINDS = frozenset({"pull-request"})


def _publication_is_the_deliverable(origin_kind: str) -> bool:
    """True when this origin owes a write that no repository change produces.

    Derived rather than listed, so a new origin kind is classified by what it
    owes instead of by being remembered here. An origin whose follow-through
    includes a pull request has a deliverable that can exist locally first,
    and analysis about it is a coherent result on its own. An origin whose
    follow-through is only a comment or a review has nothing to show for the
    run except the write itself.
    """
    required = _required_repository_receipt_kinds(origin_kind)
    return bool(required) and not (required & _CHANGE_BACKED_RECEIPT_KINDS)


def _repository_receipts(run_directory: Path) -> tuple[dict[str, str], ...]:
    path = run_directory / REPOSITORY_RECEIPTS_NAME
    try:
        path.lstat()
    except FileNotFoundError:
        return ()
    except OSError as exc:
        raise ExecutionWorkerDraftError(
            "repository action receipts are unavailable"
        ) from exc
    try:
        document = _read_private_json(
            path, maximum=MAX_DRAFT_BYTES,
            label="repository action receipts",
        )
    except ExecutionWorkerConfigError as exc:
        raise ExecutionWorkerDraftError(
            "repository action receipts are invalid"
        ) from exc
    if (
        document.get("schema") != REPOSITORY_RECEIPTS_SCHEMA
        or document.get("schema_version") != WORKER_SCHEMA_VERSION
        or set(document) != {"schema", "schema_version", "receipts"}
        or not isinstance(document.get("receipts"), list)
        or not document["receipts"]
        or len(document["receipts"]) > 8
    ):
        raise ExecutionWorkerDraftError("repository action receipts are invalid")
    receipts: list[dict[str, str]] = []
    for value in document["receipts"]:
        if (
            not isinstance(value, dict)
            or set(value) != {"kind", "repository", "url"}
            or not isinstance(value.get("kind"), str)
            or not isinstance(value.get("repository"), str)
            or not isinstance(value.get("url"), str)
            or value["kind"] not in {
                "issue", "issue-comment", "pull-request", "review"}
            or not value["repository"].startswith("github.com/")
            or not value["url"].startswith("https://github.com/")
        ):
            raise ExecutionWorkerDraftError(
                "repository action receipts are invalid"
            )
        receipts.append({key: value[key] for key in ("kind", "repository", "url")})
    return tuple(receipts)


def _append_repository_receipt(
    run_directory: Path, receipt: Mapping[str, object],
) -> None:
    """Durably retain a successful bounded forge action for final recording."""
    try:
        normalized = {
            key: str(receipt[key])
            for key in ("kind", "repository", "url")
        }
    except (KeyError, TypeError):
        raise ExecutionWorkerClaimError("repository action receipt is invalid")
    if (
        normalized["kind"] not in {
            "issue", "issue-comment", "pull-request", "review"}
        or not normalized["repository"].startswith("github.com/")
        or not normalized["url"].startswith("https://github.com/")
    ):
        raise ExecutionWorkerClaimError("repository action receipt is invalid")
    path = run_directory / REPOSITORY_RECEIPTS_NAME
    try:
        existing = list(_repository_receipts(run_directory))
    except ExecutionWorkerDraftError as exc:
        raise ExecutionWorkerClaimError("repository action receipt is unavailable") from exc
    if normalized not in existing:
        existing.append(normalized)
    document = {
        "schema": REPOSITORY_RECEIPTS_SCHEMA,
        "schema_version": WORKER_SCHEMA_VERSION,
        "receipts": existing,
    }
    try:
        if path.exists():
            _replace_private_json(path, document)
        else:
            _write_new_private_json(path, document)
    except (OSError, ExecutionWorkerError) as exc:
        raise ExecutionWorkerClaimError("repository action receipt is unavailable") from exc


def _remove_result_inputs(run_directory: Path) -> None:
    for name in _RESULT_INPUTS:
        try:
            (run_directory / name).unlink(missing_ok=True)
        except OSError:
            pass


def _result_inputs_present(
    directories: tuple[Path, ...],
    *,
    task_folder_not_before: int | None = None,
) -> bool:
    """Fail closed when this claim has any agent-authored result artifact.

    Searches everywhere a result may legitimately be authored, under the same
    fence `_locate_result` applies.  Looking only at the private run directory
    meant an agent that authored into the task folder -- the intended,
    documented location -- was released as though it had produced nothing, and
    its work was dropped without a draft and without a refusal.

    A stale task-folder file is NOT an artifact of this claim, so the fence
    has to apply here too; otherwise every release on a task whose folder
    holds an older result would refuse or auto-draft forever.
    """
    for index, directory in enumerate(directories):
        for name in _RESULT_INPUTS:
            candidate = directory / name
            try:
                candidate.lstat()
            except FileNotFoundError:
                continue
            except OSError:
                return True
            if _fenced_out(
                index, directories, candidate, task_folder_not_before
            ):
                continue
            return True
    return False


def _exact_fields(
    value: Mapping[str, Any],
    fields: set[str],
    label: str,
    *,
    error_type: type[ExecutionWorkerError] = ExecutionWorkerConfigError,
) -> None:
    if set(value) != fields:
        raise error_type(f"{label} fields are invalid")


def _strict_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    value: dict[str, object] = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("duplicate JSON field")
        value[key] = item
    return value


def _report() -> dict[str, Any]:
    """Describe this worker well enough for a runner to accept or refuse it.

    ``run_state_schema_version`` is the load-bearing field: it is the contract
    between what a runner writes and what this worker can parse. The revision
    is for a human reading a refusal, and does not gate anything.
    """
    return {
        "schema": REPORT_SCHEMA,
        "schema_version": REPORT_SCHEMA_VERSION,
        "run_state_schema_version": RUN_STATE_SCHEMA_VERSION,
        "work_context_schema_version": WORK_CONTEXT_SCHEMA_VERSION,
        "worker_schema_version": WORKER_SCHEMA_VERSION,
        "revision": _describe_revision(__file__),
        "module": __file__,
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="foxhound-task-worker",
        description="Fenced Foxhound task-execution operations",
    )
    subcommands = parser.add_subparsers(dest="operation", required=True)
    subcommands.add_parser("context")
    search = subcommands.add_parser("search")
    search.add_argument("query")
    search.add_argument(
        "--layer", action="append", choices=("kb", "secondary", "emails")
    )
    search.add_argument("--context-lines", type=int, default=0)
    search.add_argument("--max-matches-per-document", type=int)
    search.add_argument("--max-results-per-layer", type=int, default=10)
    act = subcommands.add_parser(
        "act", help="perform the approved external action for this phase")
    act_kinds = act.add_subparsers(dest="action_kind", required=True)
    worktree = act_kinds.add_parser(
        "worktree", help="clone a repository to work in (defaults to the task's)")
    worktree.add_argument(
        "--repository",
        help="canonical locator; defaults to the task origin. Real work spans "
             "repositories.")
    pull_request = act_kinds.add_parser("pull-request")
    pull_request.add_argument("--head", required=True,
                              help="branch holding the proposed change")
    pull_request.add_argument("--title", required=True)
    pull_request.add_argument(
        "--repository", help="canonical locator; defaults to the task origin")
    pull_request.add_argument(
        "--body-file",
        help="file beside the run state holding the pull request body")
    review = act_kinds.add_parser(
        "review", help="post a review on the pull request this task is about")
    review.add_argument(
        "--body-file", required=True,
        help="file beside the run state holding the review")
    review.add_argument(
        "--repository", help="canonical locator; defaults to the task origin")
    issue = act_kinds.add_parser(
        "issue", help="open an approved issue for a finding this task cannot fix")
    issue.add_argument("--title", required=True)
    issue.add_argument(
        "--body-file", required=True,
        help="file beside the run state holding the issue body")
    issue.add_argument(
        "--repository", help="canonical locator; defaults to the task origin")
    comment = act_kinds.add_parser(
        "comment", help="post an approved status update on the origin issue")
    comment.add_argument(
        "--body-file", required=True,
        help="file beside the run state holding the status update")
    mail = act_kinds.add_parser(
        "mail", help="send an approved outbound message")
    mail.add_argument("--to", required=True, help="validated recipient address")
    mail.add_argument("--subject", required=True)
    mail.add_argument(
        "--body-file", required=True,
        help="file beside the run state holding the message text")
    mail.add_argument(
        "--attachments",
        help="comma-separated paths to validated result artifacts")
    thread = subcommands.add_parser(
        "thread", help="read comments and reviews on this task's own thread")
    record = subcommands.add_parser("record")
    record.add_argument("draft")
    draft = subcommands.add_parser(
        "draft", help="build a validated draft from private result inputs"
    )
    draft.add_argument(
        "--outcome", required=True, help="result outcome for the current phase"
    )
    subcommands.add_parser("release")
    # Answerable without a run. The runner uses it to check, before it
    # claims anything, that the worker an agent would reach can read the
    # run state it is about to write.
    subcommands.add_parser("report")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.operation == "report":
        # Deliberately outside the try below: this must answer for a worker
        # that has no run to load, which is the only situation it is for.
        print(json.dumps(_report(), sort_keys=True))
        return 0
    try:
        worker = load_worker_from_environment()
        if args.operation == "context":
            result = worker.context()
        elif args.operation == "search":
            result = worker.search(
                args.query,
                layers=args.layer or ("kb",),
                context_lines=args.context_lines,
                max_matches_per_document=args.max_matches_per_document,
                max_results_per_layer=args.max_results_per_layer,
            )
        elif args.operation == "act" and args.action_kind == "worktree":
            result = worker.act_worktree(repository=args.repository)
        elif args.operation == "act" and args.action_kind == "review":
            result = worker.act_review(
                body_file=args.body_file, repository=args.repository)
        elif args.operation == "act" and args.action_kind == "issue":
            result = worker.act_issue(
                title=args.title, body_file=args.body_file,
                repository=args.repository)
        elif args.operation == "act" and args.action_kind == "comment":
            result = worker.act_comment(body_file=args.body_file)
        elif args.operation == "act" and args.action_kind == "mail":
            result = worker.act_mail(
                to=args.to, subject=args.subject, body_file=args.body_file,
                attachments=args.attachments
            )
        elif args.operation == "act":
            result = worker.act_pull_request(
                head=args.head, title=args.title, body_file=args.body_file,
                repository=args.repository)
        elif args.operation == "thread":
            result = worker.read_thread()
        elif args.operation == "draft":
            result = worker.draft(outcome=args.outcome)
        elif args.operation == "record":
            result = worker.record(args.draft)
        else:
            result = worker.release()
    except ExecutionWorkerConfigError:
        print("foxhound task worker: configuration unavailable", file=sys.stderr)
        return 78
    except ExecutionWorkerClaimError:
        print("foxhound task worker: claim unavailable", file=sys.stderr)
        return 75
    except ExecutionWorkerDraftError as exc:
        # The message, not only the token. An agent told "refused" cannot
        # tell a thing it could fix from one it cannot, so it does the safe
        # thing and gives up — which cost a complete review and three runs.
        #
        # Every message raised as this error is built from field names,
        # rules and enum values. None of them may carry task content, and
        # any new one must keep that true: this goes to an agent's terminal
        # and into its transcript.
        reason = getattr(exc, "reason", None)
        detail = str(exc) or "operation refused"
        print(
            f"foxhound task worker: {detail}"
            + (f" [{reason}]" if reason else ""),
            file=sys.stderr,
        )
        return 65
    except (KnowledgeClientError, TaskLedgerError):
        print("foxhound task worker: operation refused", file=sys.stderr)
        return 65
    except Exception:
        print("foxhound task worker: operation failed", file=sys.stderr)
        return 70
    print(json.dumps(result, ensure_ascii=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
