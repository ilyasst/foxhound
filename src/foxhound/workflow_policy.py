"""Strict, revisioned policy for one workflow instead of independent knobs."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Mapping

from .source_policy import SOURCE_POLICIES, source_kind_grants


STAGES = ("plan", "execute", "external_action")
FRESHNESS = ("none", "before_phase", "before_effect")


class WorkflowPolicyError(ValueError):
    pass


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
    payload = {
        "policy_id": "legacy-source-grants", "grants": {
            stage: sorted(values) for stage, values in grants.items()
        },
        "freshness": "none", "effects": [], "final_decision": True,
    }
    canonical = json.dumps(payload, separators=(",", ":"), sort_keys=True)
    return WorkflowPolicy(
        policy_id="legacy-source-grants",
        revision=hashlib.sha256(canonical.encode()).hexdigest(),
        grants=grants, freshness="none", effects=frozenset(), final_decision=True,
    )


def parse_workflow_policy(value: object) -> WorkflowPolicy:
    if not isinstance(value, Mapping) or set(value) != {
        "policy_id", "grants", "freshness", "effects", "final_decision"
    }:
        raise WorkflowPolicyError("workflow policy is invalid")
    policy_id = value["policy_id"]
    if not isinstance(policy_id, str) or not policy_id or len(policy_id) > 64:
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
    canonical = json.dumps(value, separators=(",", ":"), sort_keys=True)
    return WorkflowPolicy(policy_id, hashlib.sha256(canonical.encode()).hexdigest(),
                          grants, freshness, frozenset(effects), final_decision)
