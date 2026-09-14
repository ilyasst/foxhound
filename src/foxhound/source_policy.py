"""Declared policy for candidate source kinds at Foxhound boundaries.

Adding a source kind is an authority decision, not merely a parser change.
This registry keeps acceptance and planning authority reviewable in one place.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SourcePolicy:
    """Capabilities granted to one candidate source kind."""

    accepts_candidates: bool
    accepts_shadow_observations: bool
    accepts_native_intake: bool
    pre_authorized_planning: bool


SOURCE_POLICIES = {
    "meeting": SourcePolicy(True, True, True, False),
    "email": SourcePolicy(True, True, True, False),
    "teams": SourcePolicy(True, True, True, False),
    "issue": SourcePolicy(True, True, True, True),
    "legacy": SourcePolicy(True, True, True, False),
}


def source_kinds_accepting(capability: str) -> frozenset[str]:
    """Return kinds explicitly granted ``capability``.

    An unknown capability is a programmer error and fails closed rather than
    silently broadening authority.
    """
    if capability not in SourcePolicy.__dataclass_fields__:
        raise ValueError("unknown source-policy capability")
    return frozenset(
        kind
        for kind, policy in SOURCE_POLICIES.items()
        if getattr(policy, capability)
    )
