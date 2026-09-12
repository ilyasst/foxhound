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
from .task_shadow_observation import (
    LegacyTaskObservation,
    ShadowObservationError,
    TaskShadowObservation,
    candidate_comparable_digest,
    comparable_task_digest,
    parse_task_shadow_observation,
    parse_task_shadow_observation_json,
    task_shadow_observation_document,
)
from .task_shadow_feed import (
    ShadowFeedContractError,
    TaskShadowFeed,
    TaskShadowFeedItem,
    parse_task_shadow_feed,
    task_shadow_feed_document,
)

__all__ = (
    "CandidateEvidence",
    "CandidateFeed",
    "CandidateFeedItem",
    "CandidateSource",
    "CandidateTask",
    "ContractError",
    "FeedContractError",
    "LegacyTaskObservation",
    "ShadowObservationError",
    "ShadowFeedContractError",
    "TaskCandidate",
    "TaskShadowObservation",
    "TaskShadowFeed",
    "TaskShadowFeedItem",
    "candidate_id_for",
    "candidate_comparable_digest",
    "comparable_task_digest",
    "parse_task_candidate",
    "parse_candidate_feed",
    "parse_task_shadow_observation",
    "parse_task_shadow_observation_json",
    "parse_task_shadow_feed",
    "task_candidate_document",
    "task_shadow_observation_document",
    "task_shadow_feed_document",
)
