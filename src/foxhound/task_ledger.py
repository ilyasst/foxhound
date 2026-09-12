"""Foxhound-owned durable task identity and lifecycle.

Candidate and shadow-feed imports stay passive.  The only bootstrap operation
in this module is an explicit, transactional conversion of current, agreed
legacy observations.  Legacy identifiers are retained solely in a private
correlation table; Foxhound task identifiers come from its own task table.
"""

from __future__ import annotations

import json
import os
import sqlite3
from collections import defaultdict
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import StrEnum
from pathlib import Path
from typing import Callable

from .candidate_inbox import CandidateInbox, InboxError, SCHEMA_VERSION
from .contracts import (
    ContractError,
    EQUIVALENCE_BASIS,
    OwnerEquivalenceContractError,
    OwnerEquivalenceResolutionError,
    TaskCandidate,
    TaskOwnerEquivalence,
    comparable_task_digest,
    owner_equivalence_request,
    parse_task_candidate,
)


class TaskLedgerError(RuntimeError):
    """The task ledger cannot safely read or mutate its private state."""


class BootstrapDisposition(StrEnum):
    APPLIED = "applied"
    UNCHANGED = "unchanged"
    REFUSED = "refused"


class BootstrapRefusal(StrEnum):
    STATE_CONFLICT = "state_conflict"
    INVALID_STATE = "invalid_state"


class TaskStatus(StrEnum):
    OPEN = "open"
    DONE = "done"
    DROPPED = "dropped"


class TransitionDisposition(StrEnum):
    APPLIED = "applied"
    REFUSED = "refused"


class TransitionRefusal(StrEnum):
    INVALID_ACTION = "invalid_action"
    INVALID_STATE = "invalid_state"
    NOT_FOUND = "not_found"
    STALE_VERSION = "stale_version"


@dataclass(frozen=True)
class BootstrapResult:
    """Aggregate-only result safe for operational reporting."""

    disposition: BootstrapDisposition
    tasks_created: int = 0
    bindings_created: int = 0
    bindings_unchanged: int = 0
    candidates_pending: int = 0
    candidates_refused: int = 0
    candidates_unmapped: int = 0
    candidates_divergent: int = 0
    candidates_owner_equivalent: int = 0
    owner_equivalences_created: int = 0
    owner_equivalences_unchanged: int = 0
    incomplete_groups: int = 0
    refusal: BootstrapRefusal | None = None

    @property
    def accepted(self) -> bool:
        return self.disposition is not BootstrapDisposition.REFUSED


@dataclass(frozen=True)
class TaskRecord:
    id: int
    status: TaskStatus
    text: str
    owner: str | None
    due: str | None
    version: int
    created_at: str
    updated_at: str
    closed_at: str | None


@dataclass(frozen=True)
class TransitionResult:
    """Content-free result for one optimistic lifecycle transition."""

    disposition: TransitionDisposition
    task_id: int
    version: int | None = None
    status: TaskStatus | None = None
    refusal: TransitionRefusal | None = None

    @property
    def accepted(self) -> bool:
        return self.disposition is TransitionDisposition.APPLIED


@dataclass(frozen=True)
class _ObservedCandidate:
    candidate: TaskCandidate
    disposition: str
    legacy_task_id: int
    effective_owner: str | None


class _BootstrapConflict(ValueError):
    pass


class TaskLedger:
    """Durable Foxhound task operations over one private inbox database."""

    def __init__(
        self,
        database_path: str | os.PathLike[str],
        *,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.database_path = Path(database_path)
        self._clock = clock or (lambda: datetime.now(timezone.utc))

    def initialize(self) -> None:
        CandidateInbox(self.database_path, clock=self._clock).initialize()

    def bootstrap_from_shadow(
        self,
        *,
        producer: str = "gw",
        owner_resolver: Callable[..., TaskOwnerEquivalence] | None = None,
    ) -> BootstrapResult:
        """Explicitly materialize current, agreed mapped observations.

        Import paths never call this operation.  New groups require exactly
        one ``minted`` observation; ``folded`` observations attach to that
        independently identified Foxhound task.  Any contradictory durable
        state rolls back the complete invocation.
        """
        if producer != "gw":
            return BootstrapResult(
                BootstrapDisposition.REFUSED,
                refusal=BootstrapRefusal.INVALID_STATE,
            )
        snapshot, resolutions = self._resolve_owner_equivalences(
            owner_resolver
        )
        now = self._now()
        with closing(self._connect()) as connection:
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute("BEGIN IMMEDIATE")
            try:
                rows = self._bootstrap_rows(connection)
                if self._row_snapshot(rows) != snapshot:
                    raise _BootstrapConflict
                groups: dict[int, list[_ObservedCandidate]] = defaultdict(list)
                pending = refused = unmapped = divergent = 0
                owner_equivalent = 0
                equivalences_created = equivalences_unchanged = 0
                for row in rows:
                    if row["comparison"] is None:
                        pending += 1
                        continue
                    if row["comparison"] == "refused":
                        refused += 1
                        continue
                    if row["comparison"] == "unmapped":
                        unmapped += 1
                        continue
                    if (row["comparison"] not in {"agreed", "divergent"}
                            or row["disposition"] not in {"minted", "folded"}
                            or row["legacy_task_id"] is None):
                        raise _BootstrapConflict
                    try:
                        candidate = parse_task_candidate(
                            json.loads(row["payload_json"])
                        )
                    except (json.JSONDecodeError, TypeError, ContractError) as exc:
                        raise _BootstrapConflict from exc
                    if (candidate.candidate_id != row["candidate_id"]
                            or candidate.source.revision
                            != row["source_revision"]):
                        raise _BootstrapConflict
                    effective_owner = candidate.task.owner
                    if row["comparison"] == "divergent":
                        resolution = self._owner_equivalence(
                            row,
                            candidate,
                            resolutions.get((
                                candidate.candidate_id,
                                candidate.source.revision,
                            )),
                        )
                        if resolution is None:
                            divergent += 1
                            continue
                        effective_owner, is_new = resolution
                        owner_equivalent += 1
                        if is_new:
                            connection.execute(
                                "INSERT INTO task_owner_equivalences("
                                "candidate_id,source_revision,legacy_task_id,"
                                "legacy_digest,effective_owner,basis,resolved_at) "
                                "VALUES(?,?,?,?,?,?,?)",
                                (
                                    candidate.candidate_id,
                                    candidate.source.revision,
                                    int(row["legacy_task_id"]),
                                    row["comparable_digest"],
                                    effective_owner,
                                    EQUIVALENCE_BASIS,
                                    now,
                                ),
                            )
                            equivalences_created += 1
                        else:
                            equivalences_unchanged += 1
                    groups[int(row["legacy_task_id"])].append(
                        _ObservedCandidate(
                            candidate=candidate,
                            disposition=row["disposition"],
                            legacy_task_id=int(row["legacy_task_id"]),
                            effective_owner=effective_owner,
                        )
                    )

                tasks_created = bindings_created = bindings_unchanged = 0
                incomplete_groups = 0
                for legacy_task_id, observations in sorted(groups.items()):
                    minted = [
                        item for item in observations
                        if item.disposition == "minted"
                    ]
                    correlation = connection.execute(
                        "SELECT task_id FROM task_bootstrap_correlations "
                        "WHERE producer=? AND legacy_task_id=?",
                        (producer, legacy_task_id),
                    ).fetchone()
                    if len(minted) > 1:
                        raise _BootstrapConflict
                    if correlation is None and not minted:
                        incomplete_groups += 1
                        continue

                    reference = minted[0].candidate if minted else None
                    if reference is not None:
                        reference_owner = minted[0].effective_owner
                        for item in observations:
                            task = item.candidate.task
                            if (task.text != reference.task.text
                                    or item.effective_owner != reference_owner):
                                raise _BootstrapConflict

                    if correlation is None:
                        task_id = self._insert_task(
                            connection, reference, reference_owner, now
                        )
                        task_version = 1
                        connection.execute(
                            "INSERT INTO task_bootstrap_correlations("
                            "producer,legacy_task_id,task_id,created_at) "
                            "VALUES(?,?,?,?)",
                            (producer, legacy_task_id, task_id, now),
                        )
                        tasks_created += 1
                    else:
                        task_id = int(correlation["task_id"])
                        task_row = connection.execute(
                            "SELECT text,owner,version FROM tasks WHERE id=?",
                            (task_id,),
                        ).fetchone()
                        if task_row is None:
                            raise _BootstrapConflict
                        for item in observations:
                            task = item.candidate.task
                            if (task.text != task_row["text"]
                                    or item.effective_owner
                                    != task_row["owner"]):
                                raise _BootstrapConflict
                        task_version = int(task_row["version"])
                        accepted = connection.execute(
                            "SELECT candidate_id FROM task_candidate_bindings "
                            "WHERE task_id=? AND relation='accepted'",
                            (task_id,),
                        ).fetchone()
                        if (minted and accepted is not None
                                and accepted["candidate_id"]
                                != minted[0].candidate.candidate_id):
                            raise _BootstrapConflict

                    for item in observations:
                        relation = (
                            "accepted"
                            if item.disposition == "minted" else "folded"
                        )
                        existing = connection.execute(
                            "SELECT source_revision,task_id,relation "
                            "FROM task_candidate_bindings WHERE candidate_id=?",
                            (item.candidate.candidate_id,),
                        ).fetchone()
                        if existing is not None:
                            if (
                                existing["source_revision"]
                                != item.candidate.source.revision
                                or int(existing["task_id"]) != task_id
                                or existing["relation"] != relation
                            ):
                                raise _BootstrapConflict
                            bindings_unchanged += 1
                            continue
                        connection.execute(
                            "INSERT INTO task_candidate_bindings("
                            "candidate_id,source_revision,task_id,relation,"
                            "decided_at) VALUES(?,?,?,?,?)",
                            (
                                item.candidate.candidate_id,
                                item.candidate.source.revision,
                                task_id,
                                relation,
                                now,
                            ),
                        )
                        bindings_created += 1
                        if relation == "folded":
                            connection.execute(
                                "INSERT INTO task_events("
                                "task_id,kind,task_version,candidate_id,"
                                "source_revision,from_status,to_status,"
                                "occurred_at) VALUES(?,?,?,?,?,?,?,?)",
                                (
                                    task_id,
                                    "candidate_folded",
                                    task_version,
                                    item.candidate.candidate_id,
                                    item.candidate.source.revision,
                                    None,
                                    None,
                                    now,
                                ),
                            )

                connection.commit()
                disposition = (
                    BootstrapDisposition.APPLIED
                    if (tasks_created or bindings_created
                        or equivalences_created)
                    else BootstrapDisposition.UNCHANGED
                )
                return BootstrapResult(
                    disposition,
                    tasks_created=tasks_created,
                    bindings_created=bindings_created,
                    bindings_unchanged=bindings_unchanged,
                    candidates_pending=pending,
                    candidates_refused=refused,
                    candidates_unmapped=unmapped,
                    candidates_divergent=divergent,
                    candidates_owner_equivalent=owner_equivalent,
                    owner_equivalences_created=equivalences_created,
                    owner_equivalences_unchanged=equivalences_unchanged,
                    incomplete_groups=incomplete_groups,
                )
            except _BootstrapConflict:
                connection.rollback()
                return BootstrapResult(
                    BootstrapDisposition.REFUSED,
                    refusal=BootstrapRefusal.STATE_CONFLICT,
                )
            except Exception:
                connection.rollback()
                raise

    def _resolve_owner_equivalences(
        self,
        resolver: Callable[..., TaskOwnerEquivalence] | None,
    ) -> tuple[
        tuple[tuple[object, ...], ...],
        dict[tuple[str, str], TaskOwnerEquivalence],
    ]:
        """Call the read-only resolver without holding a database write lock."""
        with closing(self._connect()) as connection:
            rows = self._bootstrap_rows(connection)
            snapshot = self._row_snapshot(rows)
            requests = []
            if resolver is not None:
                for row in rows:
                    if (row["comparison"] != "divergent"
                            or row["disposition"] not in {"minted", "folded"}
                            or row["legacy_task_id"] is None
                            or row["comparable_digest"] is None
                            or row["equivalence_candidate_id"] is not None):
                        continue
                    try:
                        candidate = parse_task_candidate(
                            json.loads(row["payload_json"])
                        )
                    except (
                        json.JSONDecodeError,
                        TypeError,
                        ContractError,
                    ):
                        continue
                    requests.append((
                        candidate.candidate_id,
                        candidate.source.revision,
                        int(row["legacy_task_id"]),
                        row["comparable_digest"],
                    ))

        resolved = {}
        if resolver is not None:
            for candidate_id, revision, task_id, digest in requests:
                try:
                    result = resolver(
                        candidate_id=candidate_id,
                        source_revision=revision,
                        legacy_task_id=task_id,
                        legacy_digest=digest,
                    )
                except (
                    OwnerEquivalenceResolutionError,
                    OwnerEquivalenceContractError,
                ):
                    continue
                resolved[(candidate_id, revision)] = result
        return snapshot, resolved

    @staticmethod
    def _bootstrap_rows(connection: sqlite3.Connection) -> list[sqlite3.Row]:
        return connection.execute(
            "SELECT c.candidate_id,c.source_revision,c.payload_json,"
            "o.disposition,o.legacy_task_id,o.comparable_digest,o.comparison,"
            "e.candidate_id AS equivalence_candidate_id,"
            "e.source_revision AS equivalence_source_revision,"
            "e.legacy_task_id AS equivalence_legacy_task_id,"
            "e.legacy_digest AS equivalence_legacy_digest,"
            "e.effective_owner AS equivalence_effective_owner,"
            "e.basis AS equivalence_basis,e.resolved_at AS equivalence_resolved_at "
            "FROM candidate_inbox AS c "
            "LEFT JOIN task_shadow_observations AS o "
            "ON o.candidate_id=c.candidate_id "
            "AND o.source_revision=c.source_revision "
            "LEFT JOIN task_owner_equivalences AS e "
            "ON e.candidate_id=c.candidate_id "
            "AND e.source_revision=c.source_revision "
            "ORDER BY c.candidate_id"
        ).fetchall()

    @staticmethod
    def _row_snapshot(
        rows: list[sqlite3.Row],
    ) -> tuple[tuple[object, ...], ...]:
        return tuple(tuple(row) for row in rows)

    @staticmethod
    def _owner_equivalence(
        row: sqlite3.Row,
        candidate: TaskCandidate,
        pending: TaskOwnerEquivalence | None,
    ) -> tuple[str, bool] | None:
        if row["equivalence_candidate_id"] is not None:
            if (
                row["equivalence_candidate_id"] != candidate.candidate_id
                or row["equivalence_source_revision"]
                != candidate.source.revision
                or row["equivalence_legacy_task_id"]
                != row["legacy_task_id"]
                or row["equivalence_legacy_digest"]
                != row["comparable_digest"]
                or row["equivalence_basis"] != EQUIVALENCE_BASIS
                or not _valid_effective_owner(
                    row["equivalence_effective_owner"]
                )
                or not _valid_timestamp(row["equivalence_resolved_at"])
            ):
                raise _BootstrapConflict
            effective_owner = row["equivalence_effective_owner"]
            if comparable_task_digest(
                text=candidate.task.text,
                project=candidate.task.project,
                owner=effective_owner,
            ) != row["comparable_digest"]:
                raise _BootstrapConflict
            return effective_owner, False

        if not isinstance(pending, TaskOwnerEquivalence) or not pending.equivalent:
            return None
        try:
            request = owner_equivalence_request(
                alias=pending.request.alias,
                candidate_id=pending.request.candidate_id,
                source_revision=pending.request.source_revision,
                legacy_task_id=pending.request.legacy_task_id,
                legacy_digest=pending.request.legacy_digest,
            )
        except (AttributeError, OwnerEquivalenceContractError):
            return None
        if (
            request.candidate_id != candidate.candidate_id
            or request.source_revision != candidate.source.revision
            or request.legacy_task_id != row["legacy_task_id"]
            or request.legacy_digest != row["comparable_digest"]
            or pending.basis != EQUIVALENCE_BASIS
            or not _valid_effective_owner(pending.effective_owner)
            or comparable_task_digest(
                text=candidate.task.text,
                project=candidate.task.project,
                owner=pending.effective_owner,
            ) != row["comparable_digest"]
        ):
            return None
        return pending.effective_owner, True

    def transition(
        self, task_id: int, *, expected_version: int, action: str
    ) -> TransitionResult:
        if (isinstance(task_id, bool) or not isinstance(task_id, int)
                or task_id < 1):
            return TransitionResult(
                TransitionDisposition.REFUSED,
                task_id=0,
                refusal=TransitionRefusal.NOT_FOUND,
            )
        if (isinstance(expected_version, bool)
                or not isinstance(expected_version, int)
                or expected_version < 1):
            return TransitionResult(
                TransitionDisposition.REFUSED,
                task_id=task_id,
                refusal=TransitionRefusal.STALE_VERSION,
            )
        if action not in {"done", "drop", "reopen"}:
            return TransitionResult(
                TransitionDisposition.REFUSED,
                task_id=task_id,
                refusal=TransitionRefusal.INVALID_ACTION,
            )
        now = self._now()
        with closing(self._connect()) as connection:
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute("BEGIN IMMEDIATE")
            try:
                result = _apply_task_transition(
                    connection,
                    task_id=task_id,
                    expected_version=expected_version,
                    action=action,
                    now=now,
                )
                if result.accepted:
                    connection.commit()
                else:
                    connection.rollback()
                return result
            except Exception:
                connection.rollback()
                raise

    def get(self, task_id: int) -> TaskRecord | None:
        with closing(self._connect()) as connection:
            row = connection.execute(
                "SELECT * FROM tasks WHERE id=?", (task_id,)
            ).fetchone()
        return None if row is None else _task_record(row)

    def count(self) -> int:
        with closing(self._connect()) as connection:
            row = connection.execute("SELECT COUNT(*) AS total FROM tasks").fetchone()
            return int(row["total"])

    def binding_count(self) -> int:
        with closing(self._connect()) as connection:
            row = connection.execute(
                "SELECT COUNT(*) AS total FROM task_candidate_bindings"
            ).fetchone()
            return int(row["total"])

    @staticmethod
    def _insert_task(
        connection: sqlite3.Connection,
        candidate: TaskCandidate,
        effective_owner: str | None,
        now: str,
    ) -> int:
        task = candidate.task
        cursor = connection.execute(
            "INSERT INTO tasks(status,text,owner,due,version,created_at,"
            "updated_at,closed_at) VALUES('open',?,?,?,?,?,?,NULL)",
            (task.text, effective_owner, task.due, 1, now, now),
        )
        task_id = int(cursor.lastrowid)
        connection.execute(
            "INSERT INTO task_events("
            "task_id,kind,task_version,candidate_id,source_revision,"
            "from_status,to_status,occurred_at) VALUES(?,?,?,?,?,?,?,?)",
            (
                task_id,
                "created",
                1,
                candidate.candidate_id,
                candidate.source.revision,
                None,
                TaskStatus.OPEN,
                now,
            ),
        )
        return task_id

    def _connect(self) -> sqlite3.Connection:
        if not self.database_path.is_file() or self.database_path.is_symlink():
            raise TaskLedgerError("task ledger is not initialized")
        connection = sqlite3.connect(self.database_path, timeout=5)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout = 5000")
        version = int(connection.execute("PRAGMA user_version").fetchone()[0])
        if version != SCHEMA_VERSION:
            connection.close()
            raise TaskLedgerError("task ledger schema is not supported")
        try:
            CandidateInbox._require_schema(connection)
        except InboxError as exc:
            connection.close()
            raise TaskLedgerError("task ledger schema is incomplete") from exc
        return connection

    def _now(self) -> str:
        value = self._clock()
        if value.tzinfo is None or value.utcoffset() is None:
            raise TaskLedgerError("task ledger clock must include a timezone")
        return value.isoformat(timespec="seconds")


def _task_record(row: sqlite3.Row) -> TaskRecord:
    try:
        return TaskRecord(
            id=int(row["id"]),
            status=TaskStatus(row["status"]),
            text=row["text"],
            owner=row["owner"],
            due=row["due"],
            version=int(row["version"]),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            closed_at=row["closed_at"],
        )
    except (IndexError, KeyError, TypeError, ValueError) as exc:
        raise InboxError("task ledger contains invalid state") from exc


def _valid_effective_owner(value: object) -> bool:
    return (
        isinstance(value, str)
        and bool(value)
        and value == value.strip()
        and len(value) <= 200
        and not any(ord(char) < 32 or ord(char) == 127 for char in value)
    )


def _valid_timestamp(value: object) -> bool:
    if not isinstance(value, str):
        return False
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return False
    return parsed.tzinfo is not None and parsed.utcoffset() is not None


def _apply_task_transition(
    connection: sqlite3.Connection,
    *,
    task_id: int,
    expected_version: int,
    action: str,
    now: str,
) -> TransitionResult:
    """Apply one lifecycle transition inside the caller's transaction."""
    targets = {
        "done": (frozenset({TaskStatus.OPEN}), TaskStatus.DONE),
        "drop": (frozenset({TaskStatus.OPEN}), TaskStatus.DROPPED),
        "reopen": (
            frozenset({TaskStatus.DONE, TaskStatus.DROPPED}),
            TaskStatus.OPEN,
        ),
    }
    if action not in targets:
        return TransitionResult(
            TransitionDisposition.REFUSED,
            task_id=task_id,
            refusal=TransitionRefusal.INVALID_ACTION,
        )
    sources, target = targets[action]
    row = connection.execute(
        "SELECT status,version FROM tasks WHERE id=?", (task_id,)
    ).fetchone()
    if row is None:
        return TransitionResult(
            TransitionDisposition.REFUSED,
            task_id=task_id,
            refusal=TransitionRefusal.NOT_FOUND,
        )
    current_version = int(row["version"])
    current_status = TaskStatus(row["status"])
    if current_version != expected_version:
        return TransitionResult(
            TransitionDisposition.REFUSED,
            task_id=task_id,
            version=current_version,
            status=current_status,
            refusal=TransitionRefusal.STALE_VERSION,
        )
    if current_status not in sources:
        return TransitionResult(
            TransitionDisposition.REFUSED,
            task_id=task_id,
            version=current_version,
            status=current_status,
            refusal=TransitionRefusal.INVALID_STATE,
        )
    next_version = current_version + 1
    closed_at = None if target is TaskStatus.OPEN else now
    cursor = connection.execute(
        "UPDATE tasks SET status=?,version=?,updated_at=?,closed_at=? "
        "WHERE id=? AND version=?",
        (target, next_version, now, closed_at, task_id, current_version),
    )
    if cursor.rowcount != 1:
        return TransitionResult(
            TransitionDisposition.REFUSED,
            task_id=task_id,
            version=current_version,
            status=current_status,
            refusal=TransitionRefusal.STALE_VERSION,
        )
    connection.execute(
        "INSERT INTO task_events("
        "task_id,kind,task_version,candidate_id,source_revision,"
        "from_status,to_status,occurred_at) VALUES(?,?,?,?,?,?,?,?)",
        (
            task_id,
            "status_changed",
            next_version,
            None,
            None,
            current_status,
            target,
            now,
        ),
    )
    return TransitionResult(
        TransitionDisposition.APPLIED,
        task_id=task_id,
        version=next_version,
        status=target,
    )
