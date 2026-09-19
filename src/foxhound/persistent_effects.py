"""Durable, idempotent execution for target-bound external effects."""

from __future__ import annotations

import os
import sqlite3
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

from .candidate_inbox import CandidateInbox, InboxError, SCHEMA_VERSION
from .effect_intents import ATTEMPT_STATES, EffectExecutor, EffectIntent, EffectReceipt


_TERMINAL_STATES = frozenset({"completed", "failed", "cancelled"})


class PersistentEffectError(RuntimeError):
    """A durable effect cannot safely be recorded or replayed."""


class PersistentEffectExecutor:
    """Store an intent before executing it and retain its terminal receipt.

    The wrapped adapter receives the typed, target-bound intent unchanged.  A
    completed receipt is returned before the adapter is called, which makes a
    retry of the same idempotency key safe.  Failed and cancelled receipts are
    retained for attribution, but remain retryable.
    """

    def __init__(
        self,
        database_path: str | os.PathLike[str],
        executor: EffectExecutor,
        *,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.database_path = Path(database_path)
        self._executor = executor
        self._clock = clock or (lambda: datetime.now(timezone.utc))

    def prepare(self, intent: EffectIntent) -> EffectReceipt:
        return self._executor.prepare(intent)

    def preflight(self, intent: EffectIntent) -> EffectReceipt:
        return self._executor.preflight(intent)

    def execute(self, intent: EffectIntent) -> EffectReceipt:
        """Execute once, unless a durable completed receipt already exists."""
        self._persist_intent(intent)
        completed = self._completed_receipt(intent)
        if completed is not None:
            return completed
        # Consult the adapter before attempting a retry: if it can confirm
        # the effect already landed externally, adopt that answer instead of
        # calling execute() again and risking a duplicate.
        inspection = self._executor.inspect(intent)
        if inspection is not None:
            self._validate_terminal_or_progress_receipt(intent, inspection)
            if inspection.state == "completed":
                self._record_receipt(inspection)
                return inspection
            # inspect() returned a non-completed receipt (prepared/running/failed/
            # cancelled). The effect did not land externally, so proceed.
        try:
            receipt = self._executor.execute(intent)
            self._validate_terminal_receipt(intent, receipt)
        except Exception:
            self._record_receipt(
                EffectReceipt(intent.intent_id, "failed", None, False)
            )
            raise
        self._record_receipt(receipt)
        return receipt

    def inspect(self, intent: EffectIntent) -> EffectReceipt | None:
        return self._executor.inspect(intent)

    def cancel(self, intent: EffectIntent) -> EffectReceipt:
        receipt = self._executor.cancel(intent)
        self._validate_terminal_or_progress_receipt(intent, receipt)
        self._record_receipt(receipt)
        return receipt

    def _persist_intent(self, intent: EffectIntent) -> None:
        now = self._now()
        with closing(self._connect()) as connection:
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute("BEGIN IMMEDIATE")
            try:
                revision = connection.execute(
                    "SELECT 1 FROM work_revisions AS r JOIN work_items AS w "
                    "ON w.id=r.work_item_id WHERE w.id=? AND r.id=?",
                    (intent.work_item_id, intent.work_revision_id),
                ).fetchone()
                if revision is None:
                    raise PersistentEffectError(
                        "effect intent does not name its work revision"
                    )
                existing = connection.execute(
                    "SELECT intent_id,work_item_id,work_revision_id,kind,"
                    "target,payload_digest,idempotency_key,freshness_required "
                    "FROM effect_intents WHERE idempotency_key=? OR intent_id=?",
                    (intent.idempotency_key, intent.intent_id),
                ).fetchall()
                if existing:
                    if len(existing) != 1 or not self._matches(intent, existing[0]):
                        raise PersistentEffectError(
                            "effect idempotency key conflicts with an intent"
                        )
                    connection.commit()
                    return
                connection.execute(
                    "INSERT INTO effect_intents("
                    "intent_id,work_item_id,work_revision_id,kind,target,"
                    "payload_digest,idempotency_key,freshness_required,created_at"
                    ") VALUES(?,?,?,?,?,?,?,?,?)",
                    (
                        intent.intent_id,
                        intent.work_item_id,
                        intent.work_revision_id,
                        intent.kind,
                        intent.target,
                        intent.payload_digest,
                        intent.idempotency_key,
                        int(intent.freshness_required),
                        now,
                    ),
                )
                connection.commit()
            except Exception:
                connection.rollback()
                raise

    def _completed_receipt(self, intent: EffectIntent) -> EffectReceipt | None:
        with closing(self._connect()) as connection:
            row = connection.execute(
                "SELECT state,receipt_id,reversible FROM effect_receipts "
                "WHERE intent_id=? AND state='completed'",
                (intent.intent_id,),
            ).fetchone()
        if row is None:
            return None
        return EffectReceipt(
            intent.intent_id, row["state"], row["receipt_id"],
            bool(row["reversible"]),
        )

    def _record_receipt(self, receipt: EffectReceipt) -> None:
        now = self._now()
        with closing(self._connect()) as connection:
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute("BEGIN IMMEDIATE")
            try:
                intent = connection.execute(
                    "SELECT work_item_id,work_revision_id FROM effect_intents "
                    "WHERE intent_id=?", (receipt.intent_id,)
                ).fetchone()
                if intent is None:
                    raise PersistentEffectError("effect receipt has no intent")
                connection.execute(
                    "INSERT INTO effect_receipts("
                    "intent_id,work_item_id,work_revision_id,state,receipt_id,"
                    "reversible,recorded_at) VALUES(?,?,?,?,?,?,?) "
                    "ON CONFLICT(intent_id) DO UPDATE SET "
                    "work_item_id=excluded.work_item_id,"
                    "work_revision_id=excluded.work_revision_id,"
                    "state=excluded.state,receipt_id=excluded.receipt_id,"
                    "reversible=excluded.reversible,"
                    "recorded_at=excluded.recorded_at",
                    (
                        receipt.intent_id,
                        int(intent["work_item_id"]),
                        int(intent["work_revision_id"]),
                        receipt.state,
                        receipt.receipt_id,
                        int(receipt.reversible),
                        now,
                    ),
                )
                connection.commit()
            except Exception:
                connection.rollback()
                raise

    @staticmethod
    def _matches(intent: EffectIntent, row: sqlite3.Row) -> bool:
        return (
            row["intent_id"] == intent.intent_id
            and int(row["work_item_id"]) == intent.work_item_id
            and int(row["work_revision_id"]) == intent.work_revision_id
            and row["kind"] == intent.kind
            and row["target"] == intent.target
            and row["payload_digest"] == intent.payload_digest
            and row["idempotency_key"] == intent.idempotency_key
            and bool(row["freshness_required"]) == intent.freshness_required
        )

    @staticmethod
    def _validate_terminal_or_progress_receipt(
        intent: EffectIntent, receipt: object,
    ) -> None:
        if (
            not isinstance(receipt, EffectReceipt)
            or receipt.intent_id != intent.intent_id
            or receipt.state not in ATTEMPT_STATES
        ):
            raise PersistentEffectError(
                "effect adapter returned an invalid receipt"
            )

    @staticmethod
    def _validate_terminal_receipt(
        intent: EffectIntent, receipt: object,
    ) -> None:
        if (
            not isinstance(receipt, EffectReceipt)
            or receipt.intent_id != intent.intent_id
            or receipt.state not in _TERMINAL_STATES
        ):
            raise PersistentEffectError("effect adapter returned an invalid receipt")

    def _connect(self) -> sqlite3.Connection:
        if not self.database_path.is_file() or self.database_path.is_symlink():
            raise PersistentEffectError("effect database is not initialized")
        connection = sqlite3.connect(self.database_path, timeout=5)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout = 5000")
        version = int(connection.execute("PRAGMA user_version").fetchone()[0])
        if version != SCHEMA_VERSION:
            connection.close()
            raise PersistentEffectError("effect database schema is not supported")
        try:
            CandidateInbox._require_schema(connection)
        except InboxError as exc:
            connection.close()
            raise PersistentEffectError("effect database schema is incomplete") from exc
        return connection

    def _now(self) -> str:
        value = self._clock()
        if value.tzinfo is None or value.utcoffset() is None:
            raise PersistentEffectError("effect clock must include a timezone")
        return value.isoformat(timespec="seconds")
