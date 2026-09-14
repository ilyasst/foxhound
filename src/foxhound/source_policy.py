"""Declared policy for candidate source kinds at Foxhound boundaries.

Adding a source kind is an authority decision, not merely a parser change.
This registry keeps acceptance and planning authority reviewable in one place.

A kind is declared here before anything produces it, so that the authority
question is answered while it is still cheap. Declaring one grants nothing
on its own: planning authority defaults to false, and a producer has to
exist before any candidate of that kind can arrive.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass


@dataclass(frozen=True)
class SourcePolicy:
    """Capabilities granted to one candidate source kind."""

    accepts_candidates: bool
    accepts_shadow_observations: bool
    accepts_native_intake: bool
    #: Whether this kind's origin names something a reader can open. An
    #: issue has a number and an address; a meeting action has an opaque
    #: record id that no link can be built from. A card states the origin
    #: either way — a wrong link is worse than none — but only an
    #: addressable one is offered as a link.
    addressable_origin: bool = False


SOURCE_POLICIES = {
    "meeting": SourcePolicy(True, True, True),
    "email": SourcePolicy(True, True, True),
    "teams": SourcePolicy(True, True, True),
    "issue": SourcePolicy(True, True, True, addressable_origin=True),
    "legacy": SourcePolicy(True, True, True),
    #: A pull request awaiting review. Addressable like an issue: the same
    #: host/owner/name and a number.
    "review_request": SourcePolicy(True, True, True, addressable_origin=True),
    #: Being named on something that is not otherwise yours — the category
    #: most easily missed, because nobody assigned it.
    "mention": SourcePolicy(True, True, True, addressable_origin=True),
    #: A commitment with a date. Distinct from a meeting, which records
    #: what was said rather than what falls due.
    "calendar": SourcePolicy(True, True, True),
    #: Machine-generated: a failing job, a red check, an expiring
    #: credential. High volume, and the kind most likely to need gating
    #: rules of its own.
    "alert": SourcePolicy(True, True, True),
}



def planning_grants(
    requested: object, *, label: str = "planning grants"
) -> frozenset[str]:
    """Validate one machine's declared planning authority.

    Empty is the default and the cautious answer: a machine that declares
    nothing asks before planning anything. A machine cannot grant a kind
    the code does not permit, nor one it has never heard of — a typo must
    not silently widen authority, and must not silently narrow it either.
    """
    if requested is None:
        return frozenset()
    if isinstance(requested, (str, bytes)) or not isinstance(
        requested, Iterable
    ):
        raise ValueError(f"{label} are invalid")
    grants = frozenset(str(kind) for kind in requested)
    unknown = grants - set(SOURCE_POLICIES)
    if unknown:
        raise ValueError(f"{label} name unknown source kinds")
    return grants


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
