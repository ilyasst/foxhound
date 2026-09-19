"""Typed, target-bound external effects and their durable receipts."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Protocol

from .contracts.validation import (
    is_bounded_text,
    is_external_receipt_reference,
    is_positive_row_id,
    is_sha256_digest,
)


_ID = re.compile(r"^[a-z][a-z0-9-]{0,63}$")
EFFECT_KINDS = frozenset({"forge", "email", "teams"})
ATTEMPT_STATES = frozenset({"prepared", "running", "completed", "failed", "cancelled"})


class EffectIntentError(ValueError):
    """An effect intent or receipt is outside the supported contract."""


@dataclass(frozen=True)
class EffectIntent:
    intent_id: str
    work_item_id: int
    work_revision_id: int
    kind: str
    target: str
    payload_digest: str
    idempotency_key: str
    freshness_required: bool

    def __post_init__(self) -> None:
        if (not _ID.fullmatch(self.intent_id) or self.kind not in EFFECT_KINDS
                or not is_positive_row_id(self.work_item_id)
                or not is_positive_row_id(self.work_revision_id)
                or not is_bounded_text(self.target, maximum=500)
                or not is_sha256_digest(self.payload_digest)
                or not is_sha256_digest(self.idempotency_key)
                or not isinstance(self.freshness_required, bool)):
            raise EffectIntentError("effect intent is invalid")


@dataclass(frozen=True)
class EffectReceipt:
    intent_id: str
    state: str
    receipt_id: str | None
    reversible: bool

    def __post_init__(self) -> None:
        if (not _ID.fullmatch(self.intent_id) or self.state not in ATTEMPT_STATES
                or (self.receipt_id is not None
                    and not is_external_receipt_reference(self.receipt_id))
                or not isinstance(self.reversible, bool)):
            raise EffectIntentError("effect receipt is invalid")


class EffectExecutor(Protocol):
    """Narrow adapter: receives an exact intent, never free-form authority."""
    def prepare(self, intent: EffectIntent) -> EffectReceipt: ...
    def preflight(self, intent: EffectIntent) -> EffectReceipt: ...
    def execute(self, intent: EffectIntent) -> EffectReceipt: ...
    def inspect(self, intent: EffectIntent) -> EffectReceipt | None:
        """Return the current state of an effect, or ``None`` if unknown.

        When the adapter can determine that the external effect already landed
        it must return a ``completed`` receipt.  When it can determine that the
        effect did not land it should return a non-terminal receipt (e.g.
        ``prepared`` or ``running``).  Returning ``None`` means the adapter
        cannot answer — the caller must not treat that as ``not done`` because
        doing so would risk a duplicate external effect.
        """
        ...
    def cancel(self, intent: EffectIntent) -> EffectReceipt: ...
