#!/usr/bin/env python3
"""Raise a plain task review card, the way `schedule()` once did.

The periodic "☑️ Task done?" card is no longer raised automatically. It
asked a question the reader had no new information to answer -- answering
"still open" only moved the same question a week out -- and it was loudest
exactly when it was least useful: a task with no execution workflow is not
execution-held, so a stalled admission gate turned every task the system had
refused to start into a card asking whether it was finished.

The delivery machinery it used to be the fixture for -- claim, render,
acknowledge, repair, expire, requeue, stats, append-only history -- is shared
by every card kind and is still worth testing. Tests that need "a card
exists, now exercise the surface around it" materialise one here instead of
going through a scheduler that no longer offers it.

Raising a card this way is deliberately not a public service method: nothing
in production should be able to put this question in front of a reader again
by accident.
"""

from __future__ import annotations

import sqlite3
from contextlib import closing
from datetime import datetime
from pathlib import Path


def raise_review_cards(
    database: Path | str, now: datetime | str, *, limit: int = 100
) -> int:
    """Insert a pending review card for each open, uncarded task."""
    stamp = (
        now if isinstance(now, str) else now.isoformat(timespec="seconds")
    )
    raised = 0
    with closing(sqlite3.connect(database)) as connection:
        connection.execute("PRAGMA foreign_keys = ON")
        rows = connection.execute(
            "SELECT t.id,t.version,"
            "(SELECT b.source_revision FROM task_candidate_bindings AS b "
            " WHERE b.task_id=t.id AND b.relation='accepted') AS revision "
            "FROM tasks AS t WHERE t.status='open' "
            "AND NOT EXISTS(SELECT 1 FROM task_review_cards AS active "
            " WHERE active.task_id=t.id AND active.status IN "
            " ('pending','delivering','delivered','snoozed')) "
            "ORDER BY t.created_at,t.id LIMIT ?",
            (limit,),
        ).fetchall()
        # The bound revision is snapshotted rather than left NULL: `claim_next`
        # fences a card against the task's current source revision, and that is
        # what makes an edited source retire a card already in flight.
        for task_id, task_version, revision in rows:
            cursor = connection.execute(
                "INSERT INTO task_review_cards("
                "task_id,task_version,source_revision,status,version,"
                "due_at,created_at,updated_at) "
                "VALUES(?,?,?,'pending',1,?,?,?)",
                (task_id, task_version, revision, stamp, stamp, stamp),
            )
            connection.execute(
                "INSERT INTO task_review_card_events("
                "card_id,task_id,kind,card_version,task_version,action,"
                "occurred_at) VALUES(?,?,'scheduled',1,?,NULL,?)",
                (cursor.lastrowid, task_id, task_version, stamp),
            )
            raised += 1
        connection.commit()
    return raised
