"""Strict, revisioned policy for one workflow instead of independent knobs."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Mapping

from .source_policy import source_kind_grants
from .contracts.validation import is_bounded_text


STAGES = ("plan", "execute", "external_action")
FRESHNESS = ("none", "before_phase", "before_effect")


class WorkflowPolicyError(ValueError):
    """A workflow policy document is outside the supported contract."""


@dataclass(frozen=True)
class WorkflowPolicy:
    policy_id: str
    revision: str
    grants: Mapping[str, frozenset[str]]
    freshness: str
    effects: frozenset[str]
    final_decision: bool

    def grants_stage(self, stage: str, source_kind: object) -> bool:
        if stage not in STAGES:
            raise WorkflowPolicyError("workflow policy stage is invalid")
        return source_kind in self.grants[stage]


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
            freshness="none", effects=frozenset(), final_decision=True,
        ),
        grants=grants, freshness="none", effects=frozenset(), final_decision=True,
    )


def _revision(
    *, policy_id: str, grants: Mapping[str, frozenset[str]], freshness: str,
    effects: frozenset[str], final_decision: bool,
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
            "effects": sorted(effects),
            "final_decision": final_decision,
        },
        separators=(",", ":"), sort_keys=True,
    )
    return hashlib.sha256(canonical.encode()).hexdigest()


def parse_workflow_policy(value: object) -> WorkflowPolicy:
    if not isinstance(value, Mapping) or set(value) != {
        "policy_id", "grants", "freshness", "effects", "final_decision"
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
            effects=frozenset(effects), final_decision=final_decision,
        ),
        grants, freshness, frozenset(effects), final_decision,
    )
