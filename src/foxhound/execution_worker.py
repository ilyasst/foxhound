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
import stat
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping, Sequence

from .knowledge_client import (
    GwKnowledgeClient,
    KnowledgeClientConfig,
    KnowledgeClientError,
    KnowledgeSearchResult,
)
from .task_execution import (
    ExecutionResultEnvelope,
    TaskExecutionService,
    WorkflowDisposition,
    WorkflowPhase,
    WorkflowStatus,
    _validated_result,
)
from . import forge_action
from .task_ledger import TaskLedger, TaskLedgerError, TaskStatus


RUN_STATE_SCHEMA = "foxhound.execution-run-state"
RUN_STATE_SCHEMA_VERSION = 2
WORK_CONTEXT_SCHEMA = "foxhound.execution-work-context"
WORK_CONTEXT_SCHEMA_VERSION = 2
WORKER_SEARCH_SCHEMA = "foxhound.execution-worker-search"
RESULT_DRAFT_SCHEMA = "foxhound.execution-result-draft"
RESULT_DRAFT_READY_SCHEMA = "foxhound.execution-result-draft-ready"
RESULT_RECEIPT_SCHEMA = "foxhound.execution-result-receipt"
WORKER_SCHEMA_VERSION = 1

STATE_ENV = "FOXHOUND_EXECUTION_STATE"
GW_ENDPOINT_ENV = "FOXHOUND_GW_ENDPOINT"
GW_ALIAS_ENV = "FOXHOUND_GW_ALIAS"
GW_TOKEN_FILE_ENV = "FOXHOUND_GW_TOKEN_FILE"

MAX_STATE_BYTES = 16 * 1024
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
)


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
    knowledge_root: str | None = None


class ExecutionWorker:
    """Narrow operations available to a disposable task agent."""

    def __init__(
        self,
        state_path: str | os.PathLike[str],
        knowledge_config: KnowledgeClientConfig,
    ) -> None:
        if not isinstance(knowledge_config, KnowledgeClientConfig):
            raise ExecutionWorkerConfigError(
                "execution worker knowledge configuration is invalid"
            )
        self._state_path = Path(state_path)
        self._knowledge_config = knowledge_config

    def context(self) -> dict[str, Any]:
        state, service = self._active()
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
        return {
            "schema": WORK_CONTEXT_SCHEMA,
            "schema_version": WORK_CONTEXT_SCHEMA_VERSION,
            "task": {
                "id": task.id,
                "version": task.version,
                "text": task.text,
                "owner": task.owner,
                "due": task.due,
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
            "knowledge": {
                # The agent reads this directory with its ordinary file
                # tools. Search finds the fragment; the directory is how it
                # reads the discussion the fragment came from.
                "root": state.knowledge_root,
            },
            "workflow": {
                "version": state.workflow_version,
                "phase": state.phase.value,
                "agent_profile_id": state.agent_profile_id,
                "agent_profile_revision": state.agent_profile_revision,
                "reader_instruction": service.reader_instruction(
                    state.task_id,
                    expected_version=state.workflow_version,
                    claim_token=state.claim_token,
                ),
            },
            "operator": {
                "revision": context.revision,
                "display_name": context.display_name,
                "operator_context": context.operator_context,
                "self_aliases": list(context.self_aliases),
                "institution_domains": list(context.institution_domains),
            },
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

        Available from `execute` onward: the change has to be written before
        it can be proposed. Nothing is pushed here.
        """
        state, service = self._active()
        if state.phase is WorkflowPhase.PLAN:
            raise ExecutionWorkerClaimError(
                "a working tree is not prepared while planning"
            )
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

    def act_pull_request(self, *, head: str, title: str,
                         body_file: str | None,
                         repository: str | None = None) -> dict[str, Any]:
        """Open a pull request against this task's own origin.

        Refused outside `external_action`: the phase IS the approval. A reader
        approved an action for this phase, and performing forge writes while
        planning or executing would bypass the gate that makes the approval
        mean anything.
        """
        state, service = self._active()
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
        # Renewed only after the action succeeded: a lease that lapses mid-write
        # must not be extended by the attempt itself.
        self._renew(service, state)
        return {
            "kind": "pull-request",
            "repository": receipt.repository,
            "issue": receipt.issue,
            "number": receipt.number,
            "url": receipt.url,
            "head": receipt.head,
            "base": receipt.base,
        }

    def record(self, draft_name: str) -> dict[str, Any]:
        state = load_run_state(self._state_path)
        draft_path, draft = load_result_draft(
            self._state_path.parent, draft_name
        )
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
            questions=draft["questions"],
            external_actions=draft["external_actions"],
            deliverables=draft["deliverables"],
        )
        result = TaskExecutionService(state.database_path).record_result(
            envelope
        )
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
        result_id = state.run_id
        envelope = ExecutionResultEnvelope(
            result_id=result_id,
            task_id=state.task_id,
            task_version=state.task_version,
            workflow_version=state.workflow_version,
            phase=state.phase.value,
            claim_token=state.claim_token,
            outcome=outcome,
            summary=_read_result_text(
                run_directory / "result-summary.txt",
                label="execution result summary",
            ),
            work_markdown=_read_result_text(
                run_directory / "result-work.md",
                label="execution result work",
            ),
            questions=_read_optional_string_array(
                run_directory / "result-questions.json",
                label="execution result questions",
            ),
            external_actions=_read_optional_string_array(
                run_directory / "result-external-actions.json",
                label="execution result external actions",
            ),
            deliverables=_read_optional_string_array(
                run_directory / "result-deliverables.json",
                label="execution result deliverables",
            ),
        )
        try:
            validated = _validated_result(envelope)
        except (TypeError, ValueError):
            raise ExecutionWorkerDraftError(
                "execution result inputs are invalid"
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
            },
        )
        return {
            "schema": RESULT_DRAFT_READY_SCHEMA,
            "schema_version": WORKER_SCHEMA_VERSION,
            "draft": draft_name,
        }

    def release(self) -> dict[str, Any]:
        state = load_run_state(self._state_path)
        result = TaskExecutionService(state.database_path).release(
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
        service = TaskExecutionService(state.database_path)
        self._renew(service, state)
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
    _exact_fields(
        document,
        {
            "schema", "schema_version", "run_id", "database_path",
            "task_id", "task_version", "workflow_version", "phase",
            "claim_token", "lease_seconds", "agent_profile_id",
            "agent_profile_revision", "knowledge_root",
        },
        "execution run state",
    )
    if (
        document["schema"] != RUN_STATE_SCHEMA
        or document["schema_version"] != RUN_STATE_SCHEMA_VERSION
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
    if (
        not isinstance(profile_id, str)
        or not _PROFILE_ID_RE.fullmatch(profile_id)
        or not isinstance(profile_revision, str)
        or not _REVISION_RE.fullmatch(profile_revision)
    ):
        raise ExecutionWorkerConfigError("execution run state is invalid")
    try:
        phase = WorkflowPhase(document["phase"])
    except (TypeError, ValueError):
        raise ExecutionWorkerConfigError(
            "execution run state is invalid"
        ) from None
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
        knowledge_root=_knowledge_root(document["knowledge_root"]),
    )


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


def load_result_draft(
    run_directory: Path, name: str
) -> tuple[Path, dict[str, Any]]:
    if not isinstance(name, str):
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
    _exact_fields(
        document,
        {
            "schema", "schema_version", "result_id", "outcome", "summary",
            "work_markdown", "questions", "external_actions", "deliverables",
        },
        "execution result draft",
        error_type=ExecutionWorkerDraftError,
    )
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
    return ExecutionWorker(state, config)


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


def _read_result_text(path: Path, *, label: str) -> str:
    try:
        value = _read_private_text(path, maximum=MAX_DRAFT_BYTES, label=label)
    except ExecutionWorkerConfigError as exc:
        raise ExecutionWorkerDraftError(f"{label} is invalid") from exc
    if value.endswith("\n"):
        value = value[:-1]
        if value.endswith("\r"):
            value = value[:-1]
    return value


def _read_optional_string_array(
    path: Path, *, label: str
) -> list[object]:
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


def _remove_result_inputs(run_directory: Path) -> None:
    for name in _RESULT_INPUTS:
        try:
            (run_directory / name).unlink(missing_ok=True)
        except OSError:
            pass


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
    record = subcommands.add_parser("record")
    record.add_argument("draft")
    draft = subcommands.add_parser(
        "draft", help="build a validated draft from private result inputs"
    )
    draft.add_argument(
        "--outcome", required=True, help="result outcome for the current phase"
    )
    subcommands.add_parser("release")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
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
        elif args.operation == "act":
            result = worker.act_pull_request(
                head=args.head, title=args.title, body_file=args.body_file,
                repository=args.repository)
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
        reason = getattr(exc, "reason", None)
        print(
            "foxhound task worker: operation refused"
            + (f" ({reason})" if reason else ""),
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
