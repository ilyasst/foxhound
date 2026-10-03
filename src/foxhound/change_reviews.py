"""Keep/Undo cards for automatic changes the reader may reverse (#844).

An automatic change (first kind: ``source_closed``, a task closed because its
issue closed or its pull request merged) records one review in the same
transaction as the change. The review is delivered as a Keep/Undo card through
``/v1/change-review-cards/*``, whose request and response shapes are those of
the scheduling review cards, so a consumer delivers both families with one
client (gw: callback prefix ``fhc``).

Delivery: pending -> delivering (leased claim) -> delivered, or back to
pending when delivery fails or the lease expires. Keep and Undo resolve a
delivered card. Undo reopens the task through the ledger's own ``reopen``
transition; the scheduler then plans the open task again. It is refused when
the task moved after the automatic change.
"""
from __future__ import annotations

import hashlib
import json
import secrets
import sqlite3
from contextlib import closing
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

from .task_scheduling import (
    SchedulingCardActionResult,
    SchedulingDisposition,
    SchedulingRefusal,
)

CALLBACK_PREFIX = "fhc"
KINDS = frozenset({"source_closed"})
MAX_SUMMARY_CHARS = 200
MAX_TITLE_CHARS = 300


@dataclass(frozen=True)
class ChangeReviewCard:
    card_id: int
    version: int
    task_id: int
    kind: str
    summary: str
    task_text: str


@dataclass(frozen=True)
class ChangeReviewClaim:
    card: ChangeReviewCard
    token: str = field(repr=False)
    expires_at: str


def record(
    connection: sqlite3.Connection,
    *,
    kind: str,
    task_id: int,
    before: dict[str, Any],
    after: dict[str, Any],
    summary: str,
    now: str,
) -> None:
    """Record one reviewable automatic change, in the caller's transaction."""
    if kind not in KINDS:
        raise ValueError(f"unknown automatic change kind: {kind}")
    connection.execute(
        "INSERT INTO automatic_change_reviews(kind,task_id,before_state,"
        "after_state,summary,version,status,created_at,updated_at) "
        "VALUES(?,?,?,?,?,1,'pending',?,?)",
        (kind, task_id, json.dumps(before, sort_keys=True),
         json.dumps(after, sort_keys=True), summary[:MAX_SUMMARY_CHARS],
         now, now),
    )


def render_change_review_card(card: ChangeReviewCard) -> tuple[str, dict[str, Any]]:
    """Body and Keep/Undo controls of one card."""
    title = card.task_text.strip().splitlines()[0] if card.task_text.strip() else ""
    if len(title) > MAX_TITLE_CHARS:
        title = title[: MAX_TITLE_CHARS - 1] + "…"
    body = (
        f"✅ Closed automatically: T{card.task_id} {title}\n"
        f"{card.summary}.\n"
        "Undo reopens the task."
    )
    markup = {"inline_keyboard": [[
        {"text": "Keep", "callback_data": f"{CALLBACK_PREFIX}|{card.card_id}|{card.version}|keep"},
        {"text": "Undo", "callback_data": f"{CALLBACK_PREFIX}|{card.card_id}|{card.version}|undo"},
    ]]}
    return body, markup


class ChangeReviewService:
    def __init__(
        self,
        database_path: str | Path,
        *,
        clock: Callable[[], datetime] | None = None,
        token_factory: Callable[[], str] | None = None,
    ) -> None:
        self.database_path = Path(database_path)
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._token_factory = token_factory or (lambda: secrets.token_urlsafe(32))

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.database_path, isolation_level=None, timeout=5.0)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        return connection

    def _now(self) -> datetime:
        stamp = self._clock()
        if stamp.tzinfo is None or stamp.utcoffset() is None:
            raise ValueError("change review clock must include a timezone")
        return stamp

    def claim_next(self, *, consumer_digest: str, lease_seconds: int = 60) -> ChangeReviewClaim | None:
        stamp = self._now()
        now = stamp.isoformat(timespec="seconds")
        expires = (stamp + timedelta(seconds=lease_seconds)).isoformat(timespec="seconds")
        token = self._token_factory()
        with closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                # An expired lease returns its card to the queue first.
                for row in connection.execute(
                    "SELECT id,version,claim_expires_at FROM automatic_change_reviews "
                    "WHERE status='delivering'"
                ).fetchall():
                    if _timestamp(row["claim_expires_at"]) <= stamp:
                        connection.execute(
                            "UPDATE automatic_change_reviews SET status='pending',"
                            "version=version+1,claim_token_digest=NULL,"
                            "claim_expires_at=NULL,consumer_digest=NULL,updated_at=? "
                            "WHERE id=? AND version=?",
                            (now, int(row["id"]), int(row["version"])),
                        )
                row = connection.execute(
                    "SELECT r.id,r.version,r.task_id,r.kind,r.summary,t.text "
                    "FROM automatic_change_reviews r JOIN tasks t ON t.id=r.task_id "
                    "WHERE r.status='pending' ORDER BY r.id LIMIT 1"
                ).fetchone()
                if row is None:
                    connection.execute("COMMIT")
                    return None
                version = int(row["version"]) + 1
                connection.execute(
                    "UPDATE automatic_change_reviews SET status='delivering',version=?,"
                    "claim_token_digest=?,claim_expires_at=?,consumer_digest=?,"
                    "updated_at=? WHERE id=? AND version=?",
                    (version, _digest(token), expires, consumer_digest, now,
                     int(row["id"]), int(row["version"])),
                )
                connection.execute("COMMIT")
            except BaseException:
                connection.execute("ROLLBACK")
                raise
        return ChangeReviewClaim(
            ChangeReviewCard(int(row["id"]), version, int(row["task_id"]),
                             str(row["kind"]), str(row["summary"]), str(row["text"] or "")),
            token, expires,
        )

    def complete_delivery(self, card_id: int, *, expected_version: int,
                          claim_token: str, transport: str,
                          delivery_ref: str) -> SchedulingCardActionResult:
        return self._finish_claim(card_id, expected_version, claim_token,
                                  delivered=True, delivery_ref=delivery_ref,
                                  transport=transport)

    def fail_delivery(self, card_id: int, *, expected_version: int,
                      claim_token: str) -> SchedulingCardActionResult:
        return self._finish_claim(card_id, expected_version, claim_token,
                                  delivered=False)

    def _finish_claim(self, card_id: int, expected_version: int, claim_token: str,
                      *, delivered: bool, delivery_ref: str | None = None,
                      transport: str | None = None) -> SchedulingCardActionResult:
        stamp = self._now()
        now = stamp.isoformat(timespec="seconds")
        with closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = connection.execute(
                    "SELECT * FROM automatic_change_reviews WHERE id=?", (card_id,)
                ).fetchone()
                if (delivered and row is not None and row["status"] == "delivered"
                        and int(row["version"]) == expected_version):
                    connection.execute("COMMIT")
                    return _result(SchedulingDisposition.UNCHANGED, card_id, expected_version)
                refusal = _claim_refusal(row, expected_version, claim_token, stamp)
                if refusal is not None:
                    connection.execute("ROLLBACK")
                    return _result(SchedulingDisposition.REFUSED, card_id, None, refusal)
                if delivered:
                    connection.execute(
                        "UPDATE automatic_change_reviews SET status='delivered',"
                        "claim_token_digest=NULL,claim_expires_at=NULL,"
                        "transport=?,delivery_ref=?,updated_at=? WHERE id=? AND version=?",
                        (transport, delivery_ref, now, card_id, expected_version),
                    )
                    version = expected_version
                else:
                    version = expected_version + 1
                    connection.execute(
                        "UPDATE automatic_change_reviews SET status='pending',version=?,"
                        "claim_token_digest=NULL,claim_expires_at=NULL,"
                        "consumer_digest=NULL,updated_at=? WHERE id=? AND version=?",
                        (version, now, card_id, expected_version),
                    )
                connection.execute("COMMIT")
            except BaseException:
                connection.execute("ROLLBACK")
                raise
        return _result(SchedulingDisposition.APPLIED, card_id, version)

    def act(self, card_id: int, *, expected_version: int, action: str) -> SchedulingCardActionResult:
        if action not in {"keep", "undo"}:
            return _result(SchedulingDisposition.REFUSED, card_id, None,
                           SchedulingRefusal.INVALID_ARGUMENT)
        now = self._now().isoformat(timespec="seconds")
        with closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = connection.execute(
                    "SELECT * FROM automatic_change_reviews WHERE id=?", (card_id,)
                ).fetchone()
                if row is None:
                    connection.execute("ROLLBACK")
                    return _result(SchedulingDisposition.REFUSED, card_id, None,
                                   SchedulingRefusal.NOT_FOUND)
                if int(row["version"]) != expected_version:
                    connection.execute("ROLLBACK")
                    return _result(SchedulingDisposition.REFUSED, card_id, None,
                                   SchedulingRefusal.STALE_CARD)
                if row["status"] != "delivered":
                    connection.execute("ROLLBACK")
                    return _result(SchedulingDisposition.REFUSED, card_id, None,
                                   SchedulingRefusal.INVALID_STATE)
                if action == "undo":
                    refusal = _undo(connection, row, now)
                    if refusal is not None:
                        connection.execute("ROLLBACK")
                        return _result(SchedulingDisposition.REFUSED, card_id, None, refusal)
                version = expected_version + 1
                connection.execute(
                    "UPDATE automatic_change_reviews SET status=?,version=?,"
                    "resolved_at=?,updated_at=? WHERE id=? AND version=?",
                    ("kept" if action == "keep" else "undone", version, now, now,
                     card_id, expected_version),
                )
                connection.execute("COMMIT")
            except BaseException:
                connection.execute("ROLLBACK")
                raise
        return _result(SchedulingDisposition.APPLIED, card_id, version)


def _undo(connection: sqlite3.Connection, row: sqlite3.Row, now: str) -> SchedulingRefusal | None:
    """Reopen the task the automatic change closed, if nothing moved since."""
    from .task_ledger import _apply_task_transition

    after = json.loads(row["after_state"])
    task = connection.execute(
        "SELECT version,status FROM tasks WHERE id=?", (int(row["task_id"]),)
    ).fetchone()
    if task is None:
        return SchedulingRefusal.NOT_FOUND
    if int(task["version"]) != int(after["task"]["version"]):
        return SchedulingRefusal.UNDO_CONFLICT
    transition = _apply_task_transition(
        connection, task_id=int(row["task_id"]),
        expected_version=int(task["version"]), action="reopen", now=now,
    )
    return None if transition.accepted else SchedulingRefusal.UNDO_CONFLICT


def _claim_refusal(row: sqlite3.Row | None, expected_version: int,
                   claim_token: str, stamp: datetime) -> SchedulingRefusal | None:
    if row is None:
        return SchedulingRefusal.NOT_FOUND
    if int(row["version"]) != expected_version:
        return SchedulingRefusal.STALE_CARD
    if row["status"] != "delivering":
        return SchedulingRefusal.INVALID_STATE
    stored = row["claim_token_digest"]
    if not isinstance(stored, str) or not secrets.compare_digest(stored, _digest(claim_token)):
        return SchedulingRefusal.CLAIM_MISMATCH
    if _timestamp(row["claim_expires_at"]) <= stamp:
        return SchedulingRefusal.INVALID_STATE
    return None


def _result(disposition: SchedulingDisposition, card_id: int, version: int | None,
            refusal: SchedulingRefusal | None = None) -> SchedulingCardActionResult:
    return SchedulingCardActionResult(disposition=disposition, refusal=refusal,
                                      card_id=card_id, card_version=version)


def _digest(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _timestamp(value: object) -> datetime:
    parsed = datetime.fromisoformat(str(value))
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
