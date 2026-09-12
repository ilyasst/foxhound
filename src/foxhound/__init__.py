"""Foxhound task lifecycle and execution service."""

from .candidate_inbox import (
    CandidateInbox,
    FeedImportDisposition,
    FeedImportRefusal,
    FeedImportResult,
    ImportDisposition,
    ImportRefusal,
    ImportResult,
    InboxError,
    ShadowComparisonReport,
    ShadowFeedImportDisposition,
    ShadowFeedImportRefusal,
    ShadowFeedImportResult,
)

__version__ = "0.1.0"

__all__ = (
    "CandidateInbox",
    "FeedImportDisposition",
    "FeedImportRefusal",
    "FeedImportResult",
    "ImportDisposition",
    "ImportRefusal",
    "ImportResult",
    "InboxError",
    "ShadowComparisonReport",
    "ShadowFeedImportDisposition",
    "ShadowFeedImportRefusal",
    "ShadowFeedImportResult",
)
