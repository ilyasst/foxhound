"""Strict, revisioned policy for one workflow instead of independent knobs."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Mapping

from .source_policy import source_kind_grants
from .contracts.validation import is_bounded_text


STAGES = ("plan", "execute", "external_action")
#: Ordered by strictness.  ``before_effect`` implies ``before_phase``: a
#: deployment willing to pay for a check before publishing is not asking for
#: fewer checks on the way there.
FRESHNESS = ("none", "before_phase", "before_effect")
CHECKPOINTS = ("phase", "effect")


class WorkflowPolicyError(ValueError):
    """A workflow policy document is outside the supported contract."""


@dataclass(frozen=True)
class WorkflowPolicy:
    policy_id: str
    revision: str
    grants: Mapping[str, frozenset[str]]
    freshness: str
    #: The source kinds the freshness mode is enforced for.  Separate from
    #: the mode because a producer and a source adapter arrive per kind: a
    #: deployment may be able to prove an issue is current long before it can
    #: prove anything about an email, and enabling a check it cannot answer
    #: fails every run of that kind closed.
    freshness_kinds: frozenset[str]
    effects: frozenset[str]
    final_decision: bool

    def grants_stage(self, stage: str, source_kind: object) -> bool:
        if stage not in STAGES:
            raise WorkflowPolicyError("workflow policy stage is invalid")
        return source_kind in self.grants[stage]

    def checks_freshness(self, checkpoint: str, source_kind: object) -> bool:
        """Whether this policy requires a source check here, for this kind."""
        if checkpoint not in CHECKPOINTS:
            raise WorkflowPolicyError("workflow policy checkpoint is invalid")
        if source_kind not in self.freshness_kinds:
            return False
        if self.freshness == "before_effect":
            return True
        return self.freshness == "before_phase" and checkpoint == "phase"


def policy_from_legacy(
    *, plan: object = None, execute: object = None, external_action: object = None,
) -> WorkflowPolicy:
    """Compatibility policy retaining the deployment's existing behavior."""
    grants = {
        "plan": source_kind_grants(plan, label="planning grants"),
        "execute": source_kind_grants(execute, label="execution grants"),
        "external_action": source_kind_grants(
            external_action, label="action grants"),
    }
    return WorkflowPolicy(
        policy_id="legacy-source-grants",
        revision=_revision(
            policy_id="legacy-source-grants", grants=grants,
            freshness="none", freshness_kinds=frozenset(),
            effects=frozenset(), final_decision=True,
        ),
        grants=grants, freshness="none", freshness_kinds=frozenset(),
        effects=frozenset(), final_decision=True,
    )


def _revision(
    *, policy_id: str, grants: Mapping[str, frozenset[str]], freshness: str,
    freshness_kinds: frozenset[str], effects: frozenset[str],
    final_decision: bool,
) -> str:
    """Hash the normalized policy, so equal policies share one revision.

    Grant order, repeated entries, and document key order are not part of a
    policy's identity.  Hashing the caller's document as written would give
    one policy several revisions, and the version fence a decision or a final
    outcome carries would then mismatch for no policy change at all.
    """
    canonical = json.dumps(
        {
            "policy_id": policy_id,
            "grants": {stage: sorted(grants[stage]) for stage in STAGES},
            "freshness": freshness,
            "freshness_kinds": sorted(freshness_kinds),
            "effects": sorted(effects),
            "final_decision": final_decision,
        },
        separators=(",", ":"), sort_keys=True,
    )
    return hashlib.sha256(canonical.encode()).hexdigest()


def parse_workflow_policy(value: object) -> WorkflowPolicy:
    if not isinstance(value, Mapping) or set(value) != {
        "policy_id", "grants", "freshness", "freshness_kinds", "effects",
        "final_decision",
    }:
        raise WorkflowPolicyError("workflow policy is invalid")
    policy_id = value["policy_id"]
    if not is_bounded_text(policy_id, maximum=64):
        raise WorkflowPolicyError("workflow policy identity is invalid")
    raw_grants = value["grants"]
    if not isinstance(raw_grants, Mapping) or set(raw_grants) != set(STAGES):
        raise WorkflowPolicyError("workflow policy grants are invalid")
    grants = {stage: source_kind_grants(raw_grants[stage], label="policy grants")
              for stage in STAGES}
    freshness = value["freshness"]
    if freshness not in FRESHNESS:
        raise WorkflowPolicyError("workflow policy freshness is invalid")
    freshness_kinds = source_kind_grants(
        value["freshness_kinds"], label="policy freshness kinds")
    if freshness == "none" and freshness_kinds:
        # Naming kinds under a mode that checks nothing reads as enabled and
        # behaves as disabled, which is the confusion this object exists to
        # remove.
        raise WorkflowPolicyError("workflow policy freshness is inconsistent")
    effects = value["effects"]
    if not isinstance(effects, list) or any(
        not isinstance(item, str) or item not in {"forge", "email", "teams"}
        for item in effects
    ):
        raise WorkflowPolicyError("workflow policy effects are invalid")
    final_decision = value["final_decision"]
    if not isinstance(final_decision, bool):
        raise WorkflowPolicyError("workflow policy final decision is invalid")
    return WorkflowPolicy(
        policy_id,
        _revision(
            policy_id=policy_id, grants=grants, freshness=freshness,
            freshness_kinds=freshness_kinds, effects=frozenset(effects),
            final_decision=final_decision,
        ),
        grants, freshness, freshness_kinds, frozenset(effects), final_decision,
    )
