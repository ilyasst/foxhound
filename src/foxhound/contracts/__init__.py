"""Versioned external contracts accepted by Foxhound."""

from .task_candidate import (
    CandidateEvidence,
    CandidateSource,
    CandidateTask,
    ContractError,
    TaskCandidate,
    candidate_id_for,
    parse_task_candidate,
)

__all__ = (
    "CandidateEvidence",
    "CandidateSource",
    "CandidateTask",
    "ContractError",
    "TaskCandidate",
    "candidate_id_for",
    "parse_task_candidate",
)
