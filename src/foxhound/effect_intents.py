"""Typed, target-bound external effects and their durable receipts."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Protocol


_ID = re.compile(r"^[a-z][a-z0-9-]{0,63}$")
_DIGEST = re.compile(r"^[0-9a-f]{64}$")
EFFECT_KINDS = frozenset({"forge", "email", "teams"})
ATTEMPT_STATES = frozenset({"prepared", "running", "completed", "failed", "cancelled"})


class EffectIntentError(ValueError):
    pass


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
                or not isinstance(self.work_item_id, int) or self.work_item_id < 1
                or not isinstance(self.work_revision_id, int) or self.work_revision_id < 1
                or not isinstance(self.target, str) or not self.target or len(self.target) > 500
                or not _DIGEST.fullmatch(self.payload_digest)
                or not _DIGEST.fullmatch(self.idempotency_key)
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
                or (self.receipt_id is not None and not _ID.fullmatch(self.receipt_id))
                or not isinstance(self.reversible, bool)):
            raise EffectIntentError("effect receipt is invalid")


class EffectExecutor(Protocol):
    """Narrow adapter: receives an exact intent, never free-form authority."""
    def prepare(self, intent: EffectIntent) -> EffectReceipt: ...
    def preflight(self, intent: EffectIntent) -> EffectReceipt: ...
    def execute(self, intent: EffectIntent) -> EffectReceipt: ...
    def inspect(self, intent: EffectIntent) -> EffectReceipt: ...
    def cancel(self, intent: EffectIntent) -> EffectReceipt: ...
