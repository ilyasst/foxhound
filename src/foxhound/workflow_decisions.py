"""Domain decision and final-outcome records, independent of card transport."""

from __future__ import annotations

import re
from dataclasses import dataclass


_ID = re.compile(r"^[a-z][a-z0-9-]{0,63}$")
DECISION_KINDS = frozenset({"start", "continue", "send"})
RESPONSES = frozenset({"approve", "revise", "discard", "snooze"})
DISPOSITIONS = frozenset({"completed", "declined", "ineligible", "cancelled"})


class WorkflowDecisionError(ValueError):
    pass


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
        if (not _ID.fullmatch(self.decision_id) or self.kind not in DECISION_KINDS
                or not isinstance(self.work_item_id, int) or self.work_item_id < 1
                or not isinstance(self.work_revision_id, int) or self.work_revision_id < 1
                or not isinstance(self.workflow_version, int) or self.workflow_version < 1
                or not isinstance(self.policy_revision, str) or len(self.policy_revision) != 64
                or not self.allowed_responses or not self.allowed_responses <= RESPONSES):
            raise WorkflowDecisionError("decision request is invalid")


@dataclass(frozen=True)
class DecisionResponse:
    decision_id: str
    expected_workflow_version: int
    response: str

    def __post_init__(self) -> None:
        if (not _ID.fullmatch(self.decision_id)
                or not isinstance(self.expected_workflow_version, int)
                or self.expected_workflow_version < 1 or self.response not in RESPONSES):
            raise WorkflowDecisionError("decision response is invalid")


@dataclass(frozen=True)
class FinalOutcome:
    work_item_id: int
    work_revision_id: int
    policy_revision: str
    disposition: str
    receipt_ids: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if (not isinstance(self.work_item_id, int) or self.work_item_id < 1
                or not isinstance(self.work_revision_id, int) or self.work_revision_id < 1
                or not isinstance(self.policy_revision, str) or len(self.policy_revision) != 64
                or self.disposition not in DISPOSITIONS
                or any(not _ID.fullmatch(value) for value in self.receipt_ids)):
            raise WorkflowDecisionError("final outcome is invalid")
