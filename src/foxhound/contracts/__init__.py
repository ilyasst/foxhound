"""Versioned external contracts accepted by Foxhound."""

from .task_candidate import (
    CandidateEvidence,
    CandidateSource,
    CandidateTask,
    ContractError,
    TaskCandidate,
    candidate_id_for,
    parse_task_candidate,
    task_candidate_document,
)
from .candidate_feed import (
    CandidateFeed,
    CandidateFeedItem,
    FeedContractError,
    parse_candidate_feed,
)

__all__ = (
    "CandidateEvidence",
    "CandidateFeed",
    "CandidateFeedItem",
    "CandidateSource",
    "CandidateTask",
    "ContractError",
    "FeedContractError",
    "TaskCandidate",
    "candidate_id_for",
    "parse_task_candidate",
    "parse_candidate_feed",
    "task_candidate_document",
)
