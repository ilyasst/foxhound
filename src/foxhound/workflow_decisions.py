"""Domain decision and final-outcome records, independent of card transport."""

from __future__ import annotations

import re
from dataclasses import dataclass


_ID = re.compile(r"^[a-z][a-z0-9-]{0,63}$")
_DIGEST = re.compile(r"^[0-9a-f]{64}$")
DECISION_KINDS = frozenset({"start", "continue", "send"})
RESPONSES = frozenset({"approve", "revise", "discard", "snooze"})
DISPOSITIONS = frozenset({"completed", "declined", "ineligible", "cancelled"})


class WorkflowDecisionError(ValueError):
    """A decision or final outcome is outside the supported contract."""


def _row_id(value: object) -> bool:
    """Whether a value is a usable positive row identity.

    ``bool`` is an ``int`` subclass, so a bare ``isinstance`` check accepts
    ``True`` as the row id ``1``. These ids and the workflow version are the
    fence a decision is answered against; a boolean slipping through binds a
    reader's answer to work they never saw.
    """
    return isinstance(value, int) and not isinstance(value, bool) and value >= 1


@dataclass(frozen=True)
class DecisionRequest:
    decision_id: str
    work_item_id: int
    work_revision_id: int
    workflow_version: int
    policy_revision: str
    kind: str
    allowed_responses: frozenset[str]

    def __post_init__(self) -> None:
        if (not _ID.fullmatch(self.decision_id)
                or self.kind not in DECISION_KINDS
                or not _row_id(self.work_item_id)
                or not _row_id(self.work_revision_id)
                or not _row_id(self.workflow_version)
                or not isinstance(self.policy_revision, str)
                or not _DIGEST.fullmatch(self.policy_revision)
                or not self.allowed_responses
                or not self.allowed_responses <= RESPONSES):
            raise WorkflowDecisionError("decision request is invalid")


@dataclass(frozen=True)
class DecisionResponse:
    decision_id: str
    expected_workflow_version: int
    response: str

    def __post_init__(self) -> None:
        if (not _ID.fullmatch(self.decision_id)
                or not _row_id(self.expected_workflow_version)
                or self.response not in RESPONSES):
            raise WorkflowDecisionError("decision response is invalid")


@dataclass(frozen=True)
class FinalOutcome:
    work_item_id: int
    work_revision_id: int
    policy_revision: str
    disposition: str
    receipt_ids: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if (not _row_id(self.work_item_id)
                or not _row_id(self.work_revision_id)
                or not isinstance(self.policy_revision, str)
                or not _DIGEST.fullmatch(self.policy_revision)
                or self.disposition not in DISPOSITIONS
                or any(not _ID.fullmatch(value) for value in self.receipt_ids)):
            raise WorkflowDecisionError("final outcome is invalid")
