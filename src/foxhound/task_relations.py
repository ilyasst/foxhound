"""What one task has to do with another, recorded rather than inferred.

A task in the ledger knows nothing about any other task. The card surface can
already name a predecessor for a source whose identity encodes state — two
reviews of the same pull request share a stem — by joining on that identity at
render time. That costs nothing to keep in sync, because there is nothing to
keep in sync, and it cannot express any of what this module is for:

  * a relation between tasks from different sources, which share no stem;
  * a basis, so a reader can see why the claim was made;
  * an actor, so a machine's inference and a reader's confirmation are
    distinguishable facts rather than the same shape;
  * a decision that can be taken back.

Relations are append-only and reversible. Withdrawing one records a withdrawal
beside the assertion instead of deleting it: "we decided these were the same
and then decided they were not" is the history worth keeping, and a deleted row
keeps none of it.

Linking is not closing. Nothing here changes a task's status, and nothing here
decides that two tasks ARE the same — that is a judgement made elsewhere and
recorded here.
"""
from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone

#: The vocabulary, declared once. A kind is a claim about what a reader should
#: do with the pair, not a similarity score.
#:
#: ``supersedes``   — the subject continues the object's ask. The object is
#:                    finished with, whatever its status says; the work moved.
#: ``duplicate_of`` — the subject and the object are the same ask, reached
#:                    from different sources.
KINDS = ("supersedes", "duplicate_of")

#: Who asserted it. Two values, because there are two kinds of authority here
#: and conflating them is how an inference acquires a reader's credibility.
ASSERTERS = ("machine", "reader")

MAX_BASIS = 500
MAX_NOTE = 500
MAX_ACTOR = 200

#: A task may not be buried under relations. The bound is generous for a
#: reader and small enough that a runaway detector is visible as a refusal
#: rather than as a card nobody can read.
MAX_LIVE_RELATIONS_PER_TASK = 20


class TaskRelationError(ValueError):
    """A relation cannot be asserted or withdrawn safely."""


@dataclass(frozen=True)
class TaskRelation:
    id: int
    subject_id: int
    object_id: int
    kind: str
    basis: str
    asserted_by: str
    actor: str | None
    note: str | None
    created_at: str
    withdrawn_at: str | None
    withdrawn_by: str | None

    @property
    def live(self) -> bool:
        return self.withdrawn_at is None


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _bounded(value, field: str, maximum: int, *, required: bool):
    if value is None:
        if required:
            raise TaskRelationError(f"{field} is required")
        return None
    if not isinstance(value, str):
        raise TaskRelationError(f"{field} must be text")
    value = value.strip()
    if not 1 <= len(value) <= maximum:
        raise TaskRelationError(f"{field} has invalid length")
    if any(ord(char) < 32 or ord(char) == 127 for char in value):
        raise TaskRelationError(f"{field} contains control characters")
    return value


def _task_id(value, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise TaskRelationError(f"{field} is invalid")
    return value


def _row(row) -> TaskRelation:
    return TaskRelation(
        id=int(row["id"]),
        subject_id=int(row["subject_id"]),
        object_id=int(row["object_id"]),
        kind=row["kind"],
        basis=row["basis"],
        asserted_by=row["asserted_by"],
        actor=row["actor"],
        note=row["note"],
        created_at=row["created_at"],
        withdrawn_at=row["withdrawn_at"],
        withdrawn_by=row["withdrawn_by"],
    )


def assert_relation(
    connection: sqlite3.Connection,
    *,
    subject_id: int,
    object_id: int,
    kind: str,
    basis: str,
    asserted_by: str,
    actor: str | None = None,
    note: str | None = None,
) -> TaskRelation:
    """Record that one task relates to another, and why.

    Refuses a self-relation, an unknown kind or asserter, a relation to a task
    that does not exist, a cycle, and a task already carrying its bounded
    share. It does not refuse a relation a reader might disagree with — that
    is what withdrawal is for.
    """
    subject_id = _task_id(subject_id, "subject task")
    object_id = _task_id(object_id, "object task")
    if subject_id == object_id:
        raise TaskRelationError("a task cannot relate to itself")
    if kind not in KINDS:
        raise TaskRelationError("relation kind is unsupported")
    if asserted_by not in ASSERTERS:
        raise TaskRelationError("relation asserter is unsupported")
    basis = _bounded(basis, "relation basis", MAX_BASIS, required=True)
    actor = _bounded(actor, "relation actor", MAX_ACTOR, required=False)
    note = _bounded(note, "relation note", MAX_NOTE, required=False)

    for task_id in (subject_id, object_id):
        known = connection.execute(
            "SELECT 1 FROM tasks WHERE id=?", (task_id,)).fetchone()
        if known is None:
            raise TaskRelationError(
                "relation names a task that does not exist")

    if _reaches(connection, object_id, subject_id, kind):
        # Following the arrows already leads back here. A supersession loop
        # has no earliest task, so nothing downstream can decide which end a
        # reader should be looking at.
        raise TaskRelationError("relation would close a cycle")

    for task_id in (subject_id, object_id):
        live = connection.execute(
            "SELECT COUNT(*) FROM task_relations "
            "WHERE withdrawn_at IS NULL AND (subject_id=? OR object_id=?)",
            (task_id, task_id),
        ).fetchone()[0]
        if live >= MAX_LIVE_RELATIONS_PER_TASK:
            raise TaskRelationError("task already carries its bounded share")

    try:
        cursor = connection.execute(
            "INSERT INTO task_relations("
            "subject_id,object_id,kind,basis,asserted_by,actor,note,"
            "created_at) VALUES(?,?,?,?,?,?,?,?)",
            (subject_id, object_id, kind, basis, asserted_by, actor, note,
             _now()),
        )
    except sqlite3.IntegrityError as exc:
        raise TaskRelationError(
            "that relation is already recorded") from exc
    return get(connection, int(cursor.lastrowid))


def withdraw(connection: sqlite3.Connection, relation_id: int, *,
             withdrawn_by: str) -> TaskRelation:
    """Take a relation back, keeping the record that it was made."""
    if withdrawn_by not in ASSERTERS:
        raise TaskRelationError("relation asserter is unsupported")
    relation = get(connection, relation_id)
    if not relation.live:
        raise TaskRelationError("relation is already withdrawn")
    connection.execute(
        "UPDATE task_relations SET withdrawn_at=?, withdrawn_by=? WHERE id=?",
        (_now(), withdrawn_by, relation_id),
    )
    return get(connection, relation_id)


def get(connection: sqlite3.Connection, relation_id: int) -> TaskRelation:
    connection.row_factory = sqlite3.Row
    row = connection.execute(
        "SELECT * FROM task_relations WHERE id=?", (relation_id,)).fetchone()
    if row is None:
        raise TaskRelationError("relation does not exist")
    return _row(row)


def for_task(connection: sqlite3.Connection, task_id: int, *,
             include_withdrawn: bool = False) -> tuple[TaskRelation, ...]:
    """Every relation this task is either end of, newest last."""
    connection.row_factory = sqlite3.Row
    clause = "" if include_withdrawn else " AND withdrawn_at IS NULL"
    rows = connection.execute(
        "SELECT * FROM task_relations WHERE (subject_id=? OR object_id=?)"
        + clause + " ORDER BY id",
        (int(task_id), int(task_id)),
    ).fetchall()
    return tuple(_row(row) for row in rows)


def _reaches(connection: sqlite3.Connection, start: int, target: int,
             kind: str) -> bool:
    """Whether live relations of one kind lead from start to target."""
    seen: set[int] = set()
    frontier = [int(start)]
    while frontier:
        current = frontier.pop()
        if current == target:
            return True
        if current in seen:
            continue
        seen.add(current)
        frontier.extend(
            int(row[0]) for row in connection.execute(
                "SELECT object_id FROM task_relations "
                "WHERE subject_id=? AND kind=? AND withdrawn_at IS NULL",
                (current, kind),
            )
        )
    return False
