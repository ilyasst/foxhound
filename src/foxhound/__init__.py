"""Foxhound task lifecycle and execution service."""

from .candidate_inbox import (
    CandidateInbox,
    ImportDisposition,
    ImportRefusal,
    ImportResult,
    InboxError,
)

__version__ = "0.1.0"

__all__ = (
    "CandidateInbox",
    "ImportDisposition",
    "ImportRefusal",
    "ImportResult",
    "InboxError",
)
