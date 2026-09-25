"""Private, offline persistence for candidates and shadow observations.

The inbox is deliberately transport-agnostic. Callers select the database path
and deliver complete contract documents; this module performs no discovery,
networking, scheduling, logging, or task creation.

Source revisions are content digests, not sequence numbers. The inbox detects a
contradictory replay of one revision. Ordered producer feeds use a separate,
monotonic cursor to decide delivery order without interpreting revisions.
Candidate revision history lets passive observations bind to the exact payload
that the producer handled without rolling the current candidate backward.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
import stat
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import StrEnum
from pathlib import Path
from typing import Callable

from .contracts import (
    CandidateFeed,
    ContractError,
    FeedContractError,
    ShadowFeedContractError,
    TaskShadowFeed,
    TaskCandidate,
    TaskShadowObservation,
    candidate_comparable_digest,
    parse_candidate_feed,
    parse_task_candidate,
    parse_task_shadow_feed,
    task_candidate_document,
    task_shadow_feed_document,
    task_shadow_observation_document,
)
from .contracts.task_candidate import (
    CUMULATIVE_SCHEMA_VERSION,
    SOURCE_HISTORY_SCHEMA_VERSION,
    STRUCTURED_TASK_SCHEMA_VERSION,
)


SCHEMA_VERSION = 60
_STREAM_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,199}$")
_MAX_SQLITE_INTEGER = 9_223_372_036_854_775_807
_CUMULATIVE_SCHEMA_VERSIONS = {
    CUMULATIVE_SCHEMA_VERSION,
    STRUCTURED_TASK_SCHEMA_VERSION,
    SOURCE_HISTORY_SCHEMA_VERSION,
}

_SCHEMA_COLUMNS = {
    "candidate_inbox": (
        "candidate_id",
        "source_system",
        "source_kind",
        "source_record_id",
        "source_item_id",
        "source_revision",
        "payload_json",
        "created_at",
        "first_imported_at",
        "updated_at",
    ),
    "candidate_feed_cursors": (
        "producer",
        "stream_id",
        "cursor",
        "updated_at",
    ),
    "candidate_feed_receipts": (
        "producer",
        "stream_id",
        "from_cursor",
        "to_cursor",
        "page_digest",
        "imported_at",
    ),
    "candidate_feed_items": (
        "producer",
        "stream_id",
        "sequence",
        "candidate_id",
        "source_revision",
        "imported_at",
    ),
    "candidate_revision_history": (
        "candidate_id",
        "source_revision",
        "payload_json",
        "created_at",
        "imported_at",
    ),
    "task_shadow_observations": (
        "candidate_id",
        "source_revision",
        "disposition",
        "legacy_task_id",
        "comparable_digest",
        "reason_code",
        "comparison",
        "payload_json",
        "observed_at",
        "first_imported_at",
    ),
    "task_shadow_feed_cursors": (
        "producer",
        "stream_id",
        "cursor",
        "updated_at",
    ),
    "task_shadow_feed_receipts": (
        "producer",
        "stream_id",
        "from_cursor",
        "to_cursor",
        "page_digest",
        "imported_at",
    ),
    "tasks": (
        "id",
        "status",
        "text",
        "owner",
        "due",
        "version",
        "created_at",
        "updated_at",
        "closed_at",
        "owner_ref_version",
        "owner_kind",
        "owner_speaker_id",
        "owner_canonical_speaker_id",
        "owner_speaker_registry_id",
        "owner_pinned",
        "owner_provisional",
        "object",
        "action",
        "confidence",
    ),
    "task_participants": (
        "task_id",
        "position",
        "kind",
        "speaker_id",
        "canonical_speaker_id",
        "speaker_registry_id",
    ),
    "speaker_registry_entries": (
        "speaker_registry_id",
        "speaker_id",
        "canonical_speaker_id",
        "display_name",
        "updated_at",
    ),
    "task_duplicate_proposal_routes": (
        "proposal_id",
        "route",
    ),
    "task_candidate_bindings": (
        "candidate_id",
        "source_revision",
        "task_id",
        "relation",
        "decided_at",
    ),
    "task_bootstrap_correlations": (
        "producer",
        "legacy_task_id",
        "task_id",
        "created_at",
    ),
    "task_events": (
        "sequence",
        "task_id",
        "kind",
        "task_version",
        "candidate_id",
        "source_revision",
        "from_status",
        "to_status",
        "occurred_at",
    ),
    "native_candidate_intakes": (
        "producer",
        "stream_id",
        "activation_cursor",
        "cursor",
        "activated_at",
        "updated_at",
    ),
    "native_candidate_intake_events": (
        "sequence",
        "producer",
        "stream_id",
        "kind",
        "from_cursor",
        "to_cursor",
        "tasks_created",
        "tasks_revised",
        "candidates_unchanged",
        "occurred_at",
    ),
    "native_intake_historical_refusals": (
        "candidate_id",
        "source_revision",
        "producer",
        "stream_id",
        "reason_code",
        "refused_at",
    ),
    "shadow_import_cycles": (
        "sequence",
        "stream_id",
        "started_at",
        "completed_at",
        "candidate_previous_cursor",
        "candidate_current_cursor",
        "candidates_inserted",
        "candidates_updated",
        "observation_previous_cursor",
        "observation_current_cursor",
        "observations_inserted",
        "comparison_total",
        "comparison_agreed",
        "comparison_divergent",
        "comparison_refused",
        "comparison_unmapped",
    ),
    "task_relations": (
        "id",
        "subject_id",
        "object_id",
        "kind",
        "basis",
        "asserted_by",
        "actor",
        "note",
        "created_at",
        "withdrawn_at",
        "withdrawn_by",
    ),
    "task_completion_evidence": (
        "id",
        "task_id",
        "evidence_digest",
        "source_kind",
        "source_record_id",
        "source_item_id",
        "observed_at",
        "quotation",
        "reason",
        "detector",
        "confidence",
        "state",
        "card_id",
        "created_at",
        "settled_at",
    ),
    "task_duplicate_proposals": (
        "id",
        "left_task_id",
        "right_task_id",
        "left_task_version",
        "right_task_version",
        "basis",
        "detector",
        "state",
        "created_at",
        "updated_at",
        "settled_at",
        "card_id",
    ),
    "task_duplicate_proposal_events": (
        "sequence",
        "proposal_id",
        "kind",
        "actor",
        "occurred_at",
    ),
    "task_duplicate_assessments": (
        "left_task_id",
        "right_task_id",
        "detector",
        "verdict",
        "latency_ms",
        "prompt_tokens",
        "completion_tokens",
        "assessed_at",
    ),
    "task_fused_title_jobs": (
        "task_id",
        "state",
        "title",
        "attempts",
        "last_attempt_at",
        "created_at",
        "updated_at",
    ),
    "task_owner_equivalences": (
        "candidate_id",
        "source_revision",
        "legacy_task_id",
        "legacy_digest",
        "effective_owner",
        "basis",
        "resolved_at",
    ),
    "task_review_cards": (
        "id",
        "task_id",
        "task_version",
        "status",
        "version",
        "due_at",
        "claim_token_digest",
        "claim_expires_at",
        "transport",
        "delivery_ref",
        "delivered_at",
        "resolution",
        "review_after",
        "created_at",
        "updated_at",
        "resolved_at",
        "consumer_digest",
        "source_revision",
    ),
    "task_review_card_events": (
        "sequence",
        "card_id",
        "task_id",
        "kind",
        "card_version",
        "task_version",
        "action",
        "occurred_at",
    ),
    "task_execution_workflows": (
        "task_id",
        "task_version",
        "status",
        "phase",
        "version",
        "due_at",
        "claim_token_digest",
        "claimed_at",
        "claim_heartbeat_at",
        "claim_expires_at",
        "failure_count",
        "last_failure_reason",
        "last_failure_at",
        "next_attempt_at",
        "parked_at",
        "last_result_id",
        "created_at",
        "updated_at",
        "completed_at",
        "agent_profile_id",
        "agent_profile_revision",
        "queue_priority",
        "last_failure_exit_code",
        "last_failure_run_id",
        "steer_while_running",
        "current_run_id",
    ),
    "task_execution_results": (
        "result_id",
        "task_id",
        "workflow_version",
        "task_version",
        "phase",
        "outcome",
        "content_digest",
        "summary",
        "work_markdown",
        "questions_json",
        "external_actions_json",
        "deliverables_json",
        "created_at",
        "agent_profile_id",
        "agent_profile_revision",
        "task_work_directory",
        "task_kb_file",
        "work_digest",
        "repository_references_json",
        "repository_impact",
        "reader_instruction_sequence",
    ),
    "execution_reader_instruction_deliveries": (
        "task_id",
        "workflow_version",
        "instruction_sequence",
        "occurred_at",
    ),
    "execution_result_artifacts": (
        "result_id", "ordinal", "relative_path", "name", "size_bytes",
        "content_digest", "run_directory",
    ),
    "execution_failure_digests": (
        "task_id", "workflow_version", "phase", "run_id", "digest",
        "created_at",
    ),
    "task_execution_events": (
        "sequence",
        "task_id",
        "kind",
        "workflow_version",
        "task_version",
        "phase",
        "status",
        "occurred_at",
        "agent_profile_id",
        "agent_profile_revision",
    ),
    "execution_review_cards": (
        "id",
        "task_id",
        "task_version",
        "workflow_version",
        "kind",
        "phase",
        "result_id",
        "status",
        "version",
        "claim_token_digest",
        "claim_expires_at",
        "transport",
        "delivery_ref",
        "delivered_at",
        "resolution",
        "created_at",
        "updated_at",
        "resolved_at",
        "consumer_digest",
        "superseded_delivery_ref",
        "superseded_transport",
        # Appended by v39. Declared last because the check below compares the
        # column tuple in order, and ALTER TABLE adds to the end.
        "work_revision_id",
        "steer_digest",
    ),
    "execution_review_card_events": (
        "sequence",
        "card_id",
        "task_id",
        "kind",
        "card_version",
        "workflow_version",
        "action",
        "occurred_at",
        "consumer_digest",
    ),
    "execution_card_retractions": (
        "card_id", "transport", "delivery_ref", "state", "attempts",
        "claim_token_digest", "claim_expires_at", "created_at", "updated_at",
    ),
    "execution_steer_digest_refreshes": (
        "card_id", "attempts", "last_refreshed_at",
    ),
    "execution_reader_inputs": (
        "sequence",
        "card_id",
        "task_id",
        "card_version",
        "task_version",
        "workflow_version",
        "target_workflow_version",
        "kind",
        "value",
        "prior_value",
        "occurred_at",
    ),
    "effect_intents": (
        "intent_id",
        "work_item_id",
        "work_revision_id",
        "kind",
        "target",
        "payload_digest",
        "idempotency_key",
        "freshness_required",
        "created_at",
    ),
    "effect_receipts": (
        "intent_id",
        "work_item_id",
        "work_revision_id",
        "state",
        "receipt_id",
        "reversible",
        "recorded_at",
    ),
    "task_owner_events": (
        "sequence",
        "task_id",
        "task_version",
        "card_id",
        "from_owner",
        "to_owner",
        "occurred_at",
    ),
    "task_execution_owner_holds": (
        "id",
        "task_id",
        "task_version",
        "workflow_version",
        "status",
        "owner_display",
        "owner_ref_version",
        "owner_kind",
        "owner_speaker_id",
        "owner_canonical_speaker_id",
        "owner_speaker_registry_id",
        "owner_pinned",
        "owner_provisional",
        "backstop_at",
        "last_checked_at",
        "last_evidence_revision",
        "last_match",
        "release_reason",
        "created_at",
        "released_at",
    ),
    "task_execution_owner_hold_events": (
        "sequence",
        "hold_id",
        "task_id",
        "kind",
        "matched",
        "evidence_revision",
        "occurred_at",
    ),
    "candidate_lifecycle": (
        "candidate_id",
        "source_revision",
        "state",
        "generation",
        "changed_at",
        "updated_at",
    ),
    "task_candidate_lifecycle": (
        "candidate_id",
        "source_revision",
        "task_version",
        "state",
        "resolution",
        "changed_at",
        "decided_at",
    ),
}

# Every historical map is derived from the current one by subtraction, so
# anything added now has to be taken back out of the version before it
# existed -- otherwise a migration step verifies its own future. Five
# subtractions, applied in version order: `source_revision` (task review
# cards) arrives at v26, duplicate proposals arrive at v27, and their card
# binding arrives at v28, `repository_references_json` arrives at v29,
# `consumer_digest` arrives at v30, `repository_impact` at v31, and
# `queue_priority` at v32, and fused-title jobs at v33,
# `task_completion_evidence` arrives at v25,
# `consumer_digest` (task review cards, ADR 0036 decision 2) at v23,
# `work_digest` at v22, and `task_relations` at v21 -- so the v24 state has
# the new table removed, the v22 state also has the card column removed but
# keeps `work_digest`, the v21 state has neither column but keeps
# `task_relations`, and the v20 state has none of the five.
#: Every historical checkpoint derives from the current map by removing what
#: was added after it, so a table or column added now has to be stripped here
#: or the mid-migration checks demand it from a database that predates it.
#: Semantic assessments arrive at v43, and the delivery record and the result
#: column that names it both arrive at v42. Every earlier checkpoint excludes
#: the fields it has not yet introduced.
_SCHEMA_V48_COLUMNS = {
    name: tuple(column for column in columns if not (
        name == "task_execution_workflows"
        and column in {"steer_while_running", "current_run_id"}
    ) and not (
        name == "execution_review_cards" and column == "steer_digest"
    ))
    for name, columns in _SCHEMA_COLUMNS.items()
    if name not in {"execution_card_retractions", "execution_steer_digest_refreshes"}
}

_SCHEMA_V42_COLUMNS = {
    name: columns
    for name, columns in _SCHEMA_V48_COLUMNS.items()
    if name not in {
        "task_duplicate_assessments",
        "execution_result_artifacts",
        "execution_failure_digests",
    }
}

_SCHEMA_V41_COLUMNS = {
    name: tuple(column for column in columns if not (
        name == "task_execution_results"
        and column == "reader_instruction_sequence"
    ))
    for name, columns in _SCHEMA_V42_COLUMNS.items()
    if name != "execution_reader_instruction_deliveries"
}

_SCHEMA_V39_COLUMNS = {
    name: columns
    for name, columns in _SCHEMA_V41_COLUMNS.items()
    if name not in {"effect_intents", "effect_receipts"}
}

_SCHEMA_V38_COLUMNS = {
    name: tuple(column for column in columns if not (
        name == "execution_review_cards" and column == "work_revision_id"
    ))
    for name, columns in _SCHEMA_V39_COLUMNS.items()
}

_SCHEMA_V34_COLUMNS = {
    name: tuple(column for column in columns if not (
        name == "execution_review_cards"
        and column in {"superseded_delivery_ref", "superseded_transport"}
    ))
    for name, columns in _SCHEMA_V38_COLUMNS.items()
}

_SCHEMA_V33_COLUMNS = {
    name: tuple(column for column in columns if not (
        name == "task_execution_workflows"
        and column in {"last_failure_exit_code", "last_failure_run_id"}
    ))
    for name, columns in _SCHEMA_V34_COLUMNS.items()
}

_SCHEMA_V32_COLUMNS = {
    name: columns
    for name, columns in _SCHEMA_V33_COLUMNS.items()
    if name != "task_fused_title_jobs"
}

_SCHEMA_V31_COLUMNS = {
    name: tuple(column for column in columns if not (
        name == "task_execution_workflows" and column == "queue_priority"
    ))
    for name, columns in _SCHEMA_V32_COLUMNS.items()
}

_SCHEMA_V30_COLUMNS = {
    name: tuple(column for column in columns if not (
        name == "task_execution_results"
        and column == "repository_impact"
    ) and not (
        name == "task_execution_workflows"
        and column == "queue_priority"
    ))
    for name, columns in _SCHEMA_V32_COLUMNS.items()
}

_SCHEMA_V29_COLUMNS = {
    name: tuple(column for column in columns if not (
        name == "execution_review_cards"
        and column == "consumer_digest"
    ))
    for name, columns in _SCHEMA_V30_COLUMNS.items()
}

_SCHEMA_V28_COLUMNS = {
    name: tuple(column for column in columns if not (
        name == "task_execution_results"
        and column == "repository_references_json"
    ))
    for name, columns in _SCHEMA_V29_COLUMNS.items()
}

_SCHEMA_V26_COLUMNS = {
    name: columns
    for name, columns in _SCHEMA_V28_COLUMNS.items()
    if name not in {"task_duplicate_proposals", "task_duplicate_proposal_events"}
}

_SCHEMA_V27_COLUMNS = {
    name: tuple(column for column in columns if not (
        (name == "task_duplicate_proposals" and column == "card_id")
        or (name == "execution_review_cards" and column == "consumer_digest")
    ))
    for name, columns in _SCHEMA_V28_COLUMNS.items()
}

_SCHEMA_V24_COLUMNS = {
    name: tuple(
        column for column in columns
        if not ((name == "task_review_cards" and column == "source_revision")
                or (name == "execution_review_cards" and column == "consumer_digest"))
    )
    for name, columns in _SCHEMA_V28_COLUMNS.items()
    if name not in {
        "task_completion_evidence",
        "task_duplicate_proposals",
        "task_duplicate_proposal_events",
    }
}

_SCHEMA_V25_COLUMNS = {
    name: tuple(
        column for column in columns
        if not ((name == "task_review_cards" and column == "source_revision")
                or (name == "execution_review_cards" and column == "consumer_digest"))
    )
    for name, columns in _SCHEMA_V28_COLUMNS.items()
    if name not in {"task_duplicate_proposals", "task_duplicate_proposal_events"}
}

_SCHEMA_V22_COLUMNS = {
    name: tuple(column for column in columns if column != "consumer_digest")
    for name, columns in _SCHEMA_V24_COLUMNS.items()
}

_SCHEMA_V21_COLUMNS = {
    name: tuple(column for column in columns if column != "work_digest")
    for name, columns in _SCHEMA_V22_COLUMNS.items()
}

_SCHEMA_V20_COLUMNS = {
    name: columns
    for name, columns in _SCHEMA_V21_COLUMNS.items()
    if name != "task_relations"
}

_SCHEMA_V18_COLUMNS = {
    name: columns
    for name, columns in _SCHEMA_V20_COLUMNS.items()
    if name not in {
        "task_execution_owner_holds",
        "task_execution_owner_hold_events",
    }
}

_OWNER_COLUMNS = {
    "owner_ref_version",
    "owner_kind",
    "owner_speaker_id",
    "owner_canonical_speaker_id",
    "owner_speaker_registry_id",
    "owner_pinned",
    "owner_provisional",
}

_SCHEMA_V17_COLUMNS = {
    name: tuple(column for column in columns if column not in _OWNER_COLUMNS)
    for name, columns in _SCHEMA_V18_COLUMNS.items()
}

_SCHEMA_V16_COLUMNS = {
    name: tuple(
        column
        for column in columns
        if column not in {"task_work_directory", "task_kb_file"}
    )
    for name, columns in _SCHEMA_V17_COLUMNS.items()
}

_SCHEMA_V15_COLUMNS = {
    name: columns
    for name, columns in _SCHEMA_V16_COLUMNS.items()
    if name != "native_intake_historical_refusals"
}

_SCHEMA_V14_COLUMNS = {
    name: columns
    for name, columns in _SCHEMA_V15_COLUMNS.items()
    if name not in {"candidate_lifecycle", "task_candidate_lifecycle"}
}

_SCHEMA_V11_COLUMNS = {
    name: tuple(
        column
        for column in columns
        if column not in {"agent_profile_id", "agent_profile_revision"}
    )
    for name, columns in _SCHEMA_V14_COLUMNS.items()
}

# V54 appends the informational-delivery marker.  It is added after every
# historical map above has been derived, so that a database migrating from an
# earlier version is not asked to already have a column that did not exist at
# that point.
_SCHEMA_COLUMNS["execution_review_cards"] += ("summary_only",)

# V56 appends the claiming-consumer identity to task review cards (ADR 0036).
# Added here for the same reason as V54: historical schema maps derived above
# should not expect it.
_SCHEMA_COLUMNS["task_review_cards"] += ("claiming_consumer",)

# The versioned maps above are used to validate historical schemas while they
# migrate.  V45 is additive, so remove its tables and task columns from every
# predecessor map rather than teaching an old migration to expect the future.
def _before_structured_tasks(
    schema: dict[str, tuple[str, ...]]
) -> dict[str, tuple[str, ...]]:
    return {
        name: tuple(
            column for column in columns
            if not (name == "tasks" and column in {"object", "action", "confidence"})
        )
        for name, columns in schema.items()
        if name not in {
            "task_participants", "speaker_registry_entries",
            "task_duplicate_proposal_routes",
        }
    }


for _schema_map_name in (
    "_SCHEMA_V11_COLUMNS", "_SCHEMA_V14_COLUMNS", "_SCHEMA_V15_COLUMNS",
    "_SCHEMA_V16_COLUMNS", "_SCHEMA_V17_COLUMNS", "_SCHEMA_V18_COLUMNS",
    "_SCHEMA_V20_COLUMNS", "_SCHEMA_V21_COLUMNS", "_SCHEMA_V22_COLUMNS",
    "_SCHEMA_V24_COLUMNS", "_SCHEMA_V25_COLUMNS", "_SCHEMA_V26_COLUMNS",
    "_SCHEMA_V27_COLUMNS", "_SCHEMA_V28_COLUMNS", "_SCHEMA_V29_COLUMNS",
    "_SCHEMA_V30_COLUMNS", "_SCHEMA_V31_COLUMNS", "_SCHEMA_V32_COLUMNS",
    "_SCHEMA_V33_COLUMNS", "_SCHEMA_V34_COLUMNS", "_SCHEMA_V38_COLUMNS",
    "_SCHEMA_V39_COLUMNS", "_SCHEMA_V41_COLUMNS", "_SCHEMA_V42_COLUMNS",
):
    globals()[_schema_map_name] = _before_structured_tasks(
        globals()[_schema_map_name]
    )
del _schema_map_name

_SCHEMA_OBJECTS = {
    "task_candidate_bindings_one_accepted": "index",
    "task_events_no_update": "trigger",
    "task_events_no_delete": "trigger",
    "shadow_import_cycles_no_update": "trigger",
    "shadow_import_cycles_no_delete": "trigger",
    "task_relations_no_delete": "trigger",
    "task_relations_only_withdraw": "trigger",
    "task_relations_live": "index",
    "task_relations_object": "index",
    "task_completion_evidence_identity": "index",
    "task_completion_evidence_open": "index",
    "task_completion_evidence_no_delete": "trigger",
    "task_completion_evidence_settle_only": "trigger",
    "task_duplicate_proposals_pair": "index",
    "task_duplicate_proposals_open": "index",
    "task_duplicate_proposal_events_no_update": "trigger",
    "task_duplicate_proposal_events_no_delete": "trigger",
    "task_duplicate_proposals_no_delete": "trigger",
    "task_duplicate_proposals_settle_only": "trigger",
    "task_duplicate_assessments_pair": "index",
    "task_duplicate_assessments_no_update": "trigger",
    "task_duplicate_assessments_no_delete": "trigger",
    "task_fused_title_jobs_pending": "index",
    "task_fused_title_jobs_no_delete": "trigger",
    "task_owner_equivalences_no_update": "trigger",
    "task_owner_equivalences_no_delete": "trigger",
    "task_review_cards_one_active": "index",
    "task_review_card_events_no_update": "trigger",
    "task_review_card_events_no_delete": "trigger",
    "task_execution_workflows_ready": "index",
    "task_execution_results_no_update": "trigger",
    "task_execution_results_no_delete": "trigger",
    "task_execution_events_no_update": "trigger",
    "task_execution_events_no_delete": "trigger",
    "execution_review_cards_one_active": "index",
    "execution_review_card_events_no_update": "trigger",
    "execution_review_card_events_no_delete": "trigger",
    "candidate_feed_items_no_update": "trigger",
    "candidate_feed_items_no_delete": "trigger",
    "native_candidate_intakes_identity_immutable": "trigger",
    "native_candidate_intakes_no_delete": "trigger",
    "native_candidate_intake_events_no_update": "trigger",
    "native_candidate_intake_events_no_delete": "trigger",
    "native_intake_historical_refusals_no_update": "trigger",
    "native_intake_historical_refusals_no_delete": "trigger",
    "execution_reader_inputs_no_update": "trigger",
    "execution_reader_inputs_no_delete": "trigger",
    "execution_reader_instruction_deliveries": "table",
    "execution_failure_digests": "table",
    "task_owner_events_no_update": "trigger",
    "task_owner_events_no_delete": "trigger",
    "task_execution_owner_holds_one_active": "index",
    "task_execution_owner_hold_events_no_update": "trigger",
    "task_execution_owner_hold_events_no_delete": "trigger",
}

_SCHEMA_V1 = """
CREATE TABLE candidate_inbox (
    candidate_id       TEXT PRIMARY KEY,
    source_system      TEXT NOT NULL,
    source_kind        TEXT NOT NULL,
    source_record_id   TEXT NOT NULL,
    source_item_id     TEXT NOT NULL,
    source_revision    TEXT NOT NULL,
    payload_json       TEXT NOT NULL,
    created_at         TEXT NOT NULL,
    first_imported_at  TEXT NOT NULL,
    updated_at         TEXT NOT NULL,
    UNIQUE(source_system, source_kind, source_record_id, source_item_id)
);
"""

_SCHEMA_V2 = (
    """
CREATE TABLE candidate_feed_cursors (
    producer   TEXT NOT NULL,
    stream_id  TEXT NOT NULL,
    cursor     INTEGER NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY(producer, stream_id)
);
""",
    """
CREATE TABLE candidate_feed_receipts (
    producer    TEXT NOT NULL,
    stream_id   TEXT NOT NULL,
    from_cursor INTEGER NOT NULL,
    to_cursor   INTEGER NOT NULL,
    page_digest TEXT NOT NULL,
    imported_at TEXT NOT NULL,
    PRIMARY KEY(producer, stream_id, from_cursor, to_cursor)
);
""",
)

_SCHEMA_V3 = (
    """
CREATE TABLE candidate_revision_history (
    candidate_id    TEXT NOT NULL,
    source_revision TEXT NOT NULL,
    payload_json    TEXT NOT NULL,
    created_at      TEXT NOT NULL,
    imported_at     TEXT NOT NULL,
    PRIMARY KEY(candidate_id, source_revision)
);
""",
    """
INSERT INTO candidate_revision_history(
    candidate_id,source_revision,payload_json,created_at,imported_at
)
SELECT candidate_id,source_revision,payload_json,created_at,first_imported_at
FROM candidate_inbox;
""",
    """
CREATE TABLE task_shadow_observations (
    candidate_id       TEXT NOT NULL,
    source_revision    TEXT NOT NULL,
    disposition        TEXT NOT NULL,
    legacy_task_id     INTEGER,
    comparable_digest  TEXT,
    reason_code        TEXT,
    comparison         TEXT NOT NULL,
    payload_json       TEXT NOT NULL,
    observed_at        TEXT NOT NULL,
    first_imported_at  TEXT NOT NULL,
    PRIMARY KEY(candidate_id, source_revision),
    FOREIGN KEY(candidate_id, source_revision)
        REFERENCES candidate_revision_history(candidate_id, source_revision)
);
""",
    """
CREATE TABLE task_shadow_feed_cursors (
    producer   TEXT NOT NULL,
    stream_id  TEXT NOT NULL,
    cursor     INTEGER NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY(producer, stream_id)
);
""",
    """
CREATE TABLE task_shadow_feed_receipts (
    producer    TEXT NOT NULL,
    stream_id   TEXT NOT NULL,
    from_cursor INTEGER NOT NULL,
    to_cursor   INTEGER NOT NULL,
    page_digest TEXT NOT NULL,
    imported_at TEXT NOT NULL,
    PRIMARY KEY(producer, stream_id, from_cursor, to_cursor)
);
""",
)

_SCHEMA_V4 = (
    """
CREATE TABLE tasks (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    status     TEXT NOT NULL CHECK(status IN ('open','done','dropped')),
    text       TEXT NOT NULL,
    owner      TEXT,
    due        TEXT,
    version    INTEGER NOT NULL CHECK(version >= 1),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    closed_at  TEXT
);
""",
    """
CREATE TABLE task_candidate_bindings (
    candidate_id    TEXT PRIMARY KEY,
    source_revision TEXT NOT NULL,
    task_id         INTEGER NOT NULL,
    relation        TEXT NOT NULL CHECK(relation IN ('accepted','folded')),
    decided_at      TEXT NOT NULL,
    FOREIGN KEY(candidate_id, source_revision)
        REFERENCES candidate_revision_history(candidate_id, source_revision),
    FOREIGN KEY(task_id) REFERENCES tasks(id)
);
""",
    """
CREATE UNIQUE INDEX task_candidate_bindings_one_accepted
    ON task_candidate_bindings(task_id)
    WHERE relation='accepted';
""",
    """
CREATE TABLE task_bootstrap_correlations (
    producer       TEXT NOT NULL,
    legacy_task_id INTEGER NOT NULL CHECK(legacy_task_id > 0),
    task_id        INTEGER NOT NULL UNIQUE,
    created_at     TEXT NOT NULL,
    PRIMARY KEY(producer, legacy_task_id),
    FOREIGN KEY(task_id) REFERENCES tasks(id)
);
""",
    """
CREATE TABLE task_events (
    sequence        INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id         INTEGER NOT NULL,
    kind            TEXT NOT NULL CHECK(
                        kind IN ('created','candidate_folded','status_changed')
                    ),
    task_version    INTEGER NOT NULL CHECK(task_version >= 1),
    candidate_id    TEXT,
    source_revision TEXT,
    from_status     TEXT,
    to_status       TEXT,
    occurred_at     TEXT NOT NULL,
    FOREIGN KEY(task_id) REFERENCES tasks(id)
);
""",
    """
CREATE TRIGGER task_events_no_update
BEFORE UPDATE ON task_events
BEGIN
    SELECT RAISE(ABORT, 'task events are append-only');
END;
""",
    """
CREATE TRIGGER task_events_no_delete
BEFORE DELETE ON task_events
BEGIN
    SELECT RAISE(ABORT, 'task events are append-only');
END;
""",
)

_SCHEMA_V5 = (
    """
CREATE TABLE shadow_import_cycles (
    sequence                    INTEGER PRIMARY KEY AUTOINCREMENT,
    stream_id                   TEXT NOT NULL,
    started_at                  TEXT NOT NULL,
    completed_at                TEXT NOT NULL,
    candidate_previous_cursor   INTEGER NOT NULL
                                CHECK(candidate_previous_cursor >= 0),
    candidate_current_cursor    INTEGER NOT NULL
                                CHECK(candidate_current_cursor >= 0),
    candidates_inserted         INTEGER NOT NULL
                                CHECK(candidates_inserted >= 0),
    candidates_updated          INTEGER NOT NULL
                                CHECK(candidates_updated >= 0),
    observation_previous_cursor INTEGER NOT NULL
                                CHECK(observation_previous_cursor >= 0),
    observation_current_cursor  INTEGER NOT NULL
                                CHECK(observation_current_cursor >= 0),
    observations_inserted       INTEGER NOT NULL
                                CHECK(observations_inserted >= 0),
    comparison_total            INTEGER NOT NULL CHECK(comparison_total >= 0),
    comparison_agreed           INTEGER NOT NULL CHECK(comparison_agreed >= 0),
    comparison_divergent        INTEGER NOT NULL
                                CHECK(comparison_divergent >= 0),
    comparison_refused          INTEGER NOT NULL
                                CHECK(comparison_refused >= 0),
    comparison_unmapped         INTEGER NOT NULL
                                CHECK(comparison_unmapped >= 0),
    CHECK(candidate_current_cursor >= candidate_previous_cursor),
    CHECK(observation_current_cursor >= observation_previous_cursor),
    CHECK(comparison_total = comparison_agreed + comparison_divergent
          + comparison_refused + comparison_unmapped)
);
""",
    """
CREATE TRIGGER shadow_import_cycles_no_update
BEFORE UPDATE ON shadow_import_cycles
BEGIN
    SELECT RAISE(ABORT, 'shadow import cycle receipts are append-only');
END;
""",
    """
CREATE TRIGGER shadow_import_cycles_no_delete
BEFORE DELETE ON shadow_import_cycles
BEGIN
    SELECT RAISE(ABORT, 'shadow import cycle receipts are append-only');
END;
""",
)

_SCHEMA_V6 = (
    """
CREATE TABLE task_owner_equivalences (
    candidate_id    TEXT NOT NULL,
    source_revision TEXT NOT NULL,
    legacy_task_id  INTEGER NOT NULL CHECK(legacy_task_id > 0),
    legacy_digest   TEXT NOT NULL,
    effective_owner TEXT NOT NULL,
    basis           TEXT NOT NULL CHECK(basis = 'speaker_merge'),
    resolved_at     TEXT NOT NULL,
    PRIMARY KEY(candidate_id, source_revision),
    FOREIGN KEY(candidate_id, source_revision)
        REFERENCES task_shadow_observations(candidate_id, source_revision)
);
""",
    """
CREATE TRIGGER task_owner_equivalences_no_update
BEFORE UPDATE ON task_owner_equivalences
BEGIN
    SELECT RAISE(ABORT, 'task owner equivalences are append-only');
END;
""",
    """
CREATE TRIGGER task_owner_equivalences_no_delete
BEFORE DELETE ON task_owner_equivalences
BEGIN
    SELECT RAISE(ABORT, 'task owner equivalences are append-only');
END;
""",
)

_SCHEMA_V7 = (
    """
CREATE TABLE task_review_cards (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id            INTEGER NOT NULL,
    task_version       INTEGER NOT NULL CHECK(task_version >= 1),
    status             TEXT NOT NULL CHECK(status IN (
                           'pending','delivering','delivered','snoozed',
                           'resolved','cancelled'
                       )),
    version            INTEGER NOT NULL CHECK(version >= 1),
    due_at             TEXT NOT NULL,
    claim_token_digest TEXT,
    claim_expires_at   TEXT,
    transport          TEXT,
    delivery_ref       TEXT,
    delivered_at       TEXT,
    resolution         TEXT CHECK(resolution IS NULL OR resolution IN (
                           'done','keep_open','drop'
                       )),
    review_after       TEXT,
    created_at         TEXT NOT NULL,
    updated_at         TEXT NOT NULL,
    resolved_at        TEXT,
    FOREIGN KEY(task_id) REFERENCES tasks(id)
);
""",
    """
CREATE UNIQUE INDEX task_review_cards_one_active
    ON task_review_cards(task_id)
    WHERE status IN ('pending','delivering','delivered','snoozed');
""",
    """
CREATE TABLE task_review_card_events (
    sequence     INTEGER PRIMARY KEY AUTOINCREMENT,
    card_id      INTEGER NOT NULL,
    task_id      INTEGER NOT NULL,
    kind         TEXT NOT NULL CHECK(kind IN (
                     'scheduled','delivery_claimed','delivered',
                     'delivery_failed','delivery_expired','snoozed',
                     'resolved','cancelled'
                 )),
    card_version INTEGER NOT NULL CHECK(card_version >= 1),
    task_version INTEGER NOT NULL CHECK(task_version >= 1),
    action       TEXT CHECK(action IS NULL OR action IN (
                     'done','keep_open','drop','snooze'
                 )),
    occurred_at  TEXT NOT NULL,
    FOREIGN KEY(card_id) REFERENCES task_review_cards(id),
    FOREIGN KEY(task_id) REFERENCES tasks(id)
);
""",
    """
CREATE TRIGGER task_review_card_events_no_update
BEFORE UPDATE ON task_review_card_events
BEGIN
    SELECT RAISE(ABORT, 'task review card events are append-only');
END;
""",
    """
CREATE TRIGGER task_review_card_events_no_delete
BEFORE DELETE ON task_review_card_events
BEGIN
    SELECT RAISE(ABORT, 'task review card events are append-only');
END;
""",
)

_SCHEMA_V8 = (
    """
CREATE TABLE task_execution_workflows (
    task_id                INTEGER PRIMARY KEY,
    task_version           INTEGER NOT NULL CHECK(task_version >= 1),
    status                 TEXT NOT NULL CHECK(status IN (
                               'awaiting_start','snoozed','queued','running',
                               'awaiting_review','completed','cancelled','parked'
                           )),
    phase                  TEXT NOT NULL CHECK(phase IN (
                               'plan','execute','external_action'
                           )),
    version                INTEGER NOT NULL CHECK(version >= 1),
    due_at                 TEXT,
    claim_token_digest     TEXT,
    claimed_at             TEXT,
    claim_heartbeat_at     TEXT,
    claim_expires_at       TEXT,
    failure_count          INTEGER NOT NULL DEFAULT 0
                               CHECK(failure_count >= 0),
    last_failure_reason    TEXT CHECK(last_failure_reason IS NULL OR
                               last_failure_reason IN (
                                   'startup_failed','process_exit','timeout',
                                   'interrupted','claim_expired','lease_failed',
                                   'result_invalid'
                               )),
    last_failure_at        TEXT,
    next_attempt_at        TEXT,
    parked_at              TEXT,
    last_result_id         TEXT,
    created_at             TEXT NOT NULL,
    updated_at             TEXT NOT NULL,
    completed_at           TEXT,
    CHECK(
        (status = 'running' AND claim_token_digest IS NOT NULL
         AND length(claim_token_digest) = 64 AND claimed_at IS NOT NULL
         AND claim_heartbeat_at IS NOT NULL AND claim_expires_at IS NOT NULL)
        OR
        (status != 'running' AND claim_token_digest IS NULL
         AND claimed_at IS NULL AND claim_heartbeat_at IS NULL
         AND claim_expires_at IS NULL)
    ),
    CHECK(status != 'snoozed' OR due_at IS NOT NULL),
    CHECK((status = 'parked') = (parked_at IS NOT NULL)),
    CHECK(
        (status IN ('completed','cancelled')) = (completed_at IS NOT NULL)
    ),
    FOREIGN KEY(task_id) REFERENCES tasks(id)
);
""",
    """
CREATE INDEX task_execution_workflows_ready
    ON task_execution_workflows(status, next_attempt_at, due_at, updated_at);
""",
    """
CREATE TABLE task_execution_results (
    result_id             TEXT PRIMARY KEY CHECK(
                              length(result_id) BETWEEN 1 AND 128
                          ),
    task_id               INTEGER NOT NULL,
    workflow_version      INTEGER NOT NULL CHECK(workflow_version >= 1),
    task_version          INTEGER NOT NULL CHECK(task_version >= 1),
    phase                 TEXT NOT NULL CHECK(phase IN (
                              'plan','execute','external_action'
                          )),
    outcome               TEXT NOT NULL CHECK(outcome IN (
                              'awaiting_plan','awaiting_external','completed',
                              'declined','ineligible'
                          )),
    content_digest        TEXT NOT NULL CHECK(length(content_digest) = 64),
    summary               TEXT NOT NULL CHECK(length(summary) <= 1200),
    work_markdown         TEXT NOT NULL CHECK(length(work_markdown) <= 131072),
    questions_json        TEXT NOT NULL CHECK(length(questions_json) <= 65536),
    external_actions_json TEXT NOT NULL
                              CHECK(length(external_actions_json) <= 65536),
    deliverables_json     TEXT NOT NULL
                              CHECK(length(deliverables_json) <= 65536),
    created_at            TEXT NOT NULL,
    FOREIGN KEY(task_id) REFERENCES task_execution_workflows(task_id)
);
""",
    """
CREATE TRIGGER task_execution_results_no_update
BEFORE UPDATE ON task_execution_results
BEGIN
    SELECT RAISE(ABORT, 'task execution results are append-only');
END;
""",
    """
CREATE TRIGGER task_execution_results_no_delete
BEFORE DELETE ON task_execution_results
BEGIN
    SELECT RAISE(ABORT, 'task execution results are append-only');
END;
""",
    """
CREATE TABLE task_execution_events (
    sequence         INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id          INTEGER NOT NULL,
    kind             TEXT NOT NULL CHECK(kind IN (
                         'scheduled','start_approved','snoozed','cancelled',
                         'claimed','claim_renewed','released','claim_expired',
                         'retry_scheduled','parked','result_recorded',
                         'phase_approved','revision_requested'
                     )),
    workflow_version INTEGER NOT NULL CHECK(workflow_version >= 1),
    task_version     INTEGER NOT NULL CHECK(task_version >= 1),
    phase            TEXT NOT NULL CHECK(phase IN (
                         'plan','execute','external_action'
                     )),
    status           TEXT NOT NULL CHECK(status IN (
                         'awaiting_start','snoozed','queued','running',
                         'awaiting_review','completed','cancelled','parked'
                     )),
    occurred_at      TEXT NOT NULL,
    FOREIGN KEY(task_id) REFERENCES task_execution_workflows(task_id)
);
""",
    """
CREATE TRIGGER task_execution_events_no_update
BEFORE UPDATE ON task_execution_events
BEGIN
    SELECT RAISE(ABORT, 'task execution events are append-only');
END;
""",
    """
CREATE TRIGGER task_execution_events_no_delete
BEFORE DELETE ON task_execution_events
BEGIN
    SELECT RAISE(ABORT, 'task execution events are append-only');
END;
""",
)

_SCHEMA_V9 = (
    """
CREATE TABLE execution_review_cards (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id            INTEGER NOT NULL,
    task_version       INTEGER NOT NULL CHECK(task_version >= 1),
    workflow_version   INTEGER NOT NULL CHECK(workflow_version >= 1),
    kind               TEXT NOT NULL CHECK(kind IN (
                           'start','plan_review','external_review'
                       )),
    phase              TEXT NOT NULL CHECK(phase IN (
                           'plan','execute'
                       )),
    result_id          TEXT,
    status             TEXT NOT NULL CHECK(status IN (
                           'pending','delivering','delivered',
                           'resolved','cancelled'
                       )),
    version            INTEGER NOT NULL CHECK(version >= 1),
    claim_token_digest TEXT,
    claim_expires_at   TEXT,
    transport          TEXT,
    delivery_ref       TEXT,
    delivered_at       TEXT,
    resolution         TEXT CHECK(resolution IS NULL OR resolution IN (
                           'start','snooze','cancel','approve','revise'
                       )),
    created_at         TEXT NOT NULL,
    updated_at         TEXT NOT NULL,
    resolved_at        TEXT,
    CHECK(
        (kind = 'start' AND phase = 'plan' AND result_id IS NULL)
        OR (kind = 'plan_review' AND phase = 'plan' AND result_id IS NOT NULL)
        OR (kind = 'external_review' AND phase = 'execute'
            AND result_id IS NOT NULL)
    ),
    CHECK(
        (status = 'delivering' AND claim_token_digest IS NOT NULL
         AND length(claim_token_digest) = 64 AND claim_expires_at IS NOT NULL)
        OR (status != 'delivering' AND claim_token_digest IS NULL
            AND claim_expires_at IS NULL)
    ),
    CHECK(
        (status = 'resolved' AND resolution IS NOT NULL
         AND resolved_at IS NOT NULL)
        OR (status = 'cancelled' AND resolution IS NULL
            AND resolved_at IS NOT NULL)
        OR (status IN ('pending','delivering','delivered')
            AND resolution IS NULL AND resolved_at IS NULL)
    ),
    FOREIGN KEY(task_id) REFERENCES task_execution_workflows(task_id),
    FOREIGN KEY(result_id) REFERENCES task_execution_results(result_id)
);
""",
    """
CREATE UNIQUE INDEX execution_review_cards_one_active
    ON execution_review_cards(task_id)
    WHERE status IN ('pending','delivering','delivered');
""",
    """
CREATE TABLE execution_review_card_events (
    sequence         INTEGER PRIMARY KEY AUTOINCREMENT,
    card_id          INTEGER NOT NULL,
    task_id          INTEGER NOT NULL,
    kind             TEXT NOT NULL CHECK(kind IN (
                         'scheduled','delivery_claimed','delivered',
                         'delivery_failed','delivery_expired','resolved',
                         'cancelled'
                     )),
    card_version     INTEGER NOT NULL CHECK(card_version >= 1),
    workflow_version INTEGER NOT NULL CHECK(workflow_version >= 1),
    action           TEXT CHECK(action IS NULL OR action IN (
                         'start','snooze','cancel','approve','revise'
                     )),
    occurred_at      TEXT NOT NULL,
    FOREIGN KEY(card_id) REFERENCES execution_review_cards(id),
    FOREIGN KEY(task_id) REFERENCES task_execution_workflows(task_id)
);
""",
    """
CREATE TRIGGER execution_review_card_events_no_update
BEFORE UPDATE ON execution_review_card_events
BEGIN
    SELECT RAISE(ABORT, 'execution review card events are append-only');
END;
""",
    """
CREATE TRIGGER execution_review_card_events_no_delete
BEFORE DELETE ON execution_review_card_events
BEGIN
    SELECT RAISE(ABORT, 'execution review card events are append-only');
END;
""",
)

_SCHEMA_V10 = (
    """
CREATE TABLE candidate_feed_items (
    producer        TEXT NOT NULL,
    stream_id       TEXT NOT NULL,
    sequence        INTEGER NOT NULL CHECK(sequence > 0),
    candidate_id    TEXT NOT NULL,
    source_revision TEXT NOT NULL,
    imported_at     TEXT NOT NULL,
    PRIMARY KEY(producer, stream_id, sequence),
    FOREIGN KEY(candidate_id, source_revision)
        REFERENCES candidate_revision_history(candidate_id, source_revision)
);
""",
    """
CREATE TRIGGER candidate_feed_items_no_update
BEFORE UPDATE ON candidate_feed_items
BEGIN
    SELECT RAISE(ABORT, 'candidate feed items are append-only');
END;
""",
    """
CREATE TRIGGER candidate_feed_items_no_delete
BEFORE DELETE ON candidate_feed_items
BEGIN
    SELECT RAISE(ABORT, 'candidate feed items are append-only');
END;
""",
    """
CREATE TABLE native_candidate_intakes (
    producer          TEXT NOT NULL,
    stream_id         TEXT NOT NULL,
    activation_cursor INTEGER NOT NULL CHECK(activation_cursor >= 0),
    cursor            INTEGER NOT NULL CHECK(cursor >= activation_cursor),
    activated_at      TEXT NOT NULL,
    updated_at        TEXT NOT NULL,
    PRIMARY KEY(producer, stream_id)
);
""",
    """
CREATE TRIGGER native_candidate_intakes_identity_immutable
BEFORE UPDATE OF producer,stream_id,activation_cursor,activated_at
ON native_candidate_intakes
BEGIN
    SELECT RAISE(ABORT, 'native candidate intake activation is immutable');
END;
""",
    """
CREATE TRIGGER native_candidate_intakes_no_delete
BEFORE DELETE ON native_candidate_intakes
BEGIN
    SELECT RAISE(ABORT, 'native candidate intake activation is permanent');
END;
""",
    """
CREATE TABLE native_candidate_intake_events (
    sequence             INTEGER PRIMARY KEY AUTOINCREMENT,
    producer             TEXT NOT NULL,
    stream_id            TEXT NOT NULL,
    kind                 TEXT NOT NULL CHECK(kind IN ('activated','advanced')),
    from_cursor          INTEGER NOT NULL CHECK(from_cursor >= 0),
    to_cursor            INTEGER NOT NULL CHECK(to_cursor >= from_cursor),
    tasks_created        INTEGER NOT NULL CHECK(tasks_created >= 0),
    tasks_revised        INTEGER NOT NULL CHECK(tasks_revised >= 0),
    candidates_unchanged INTEGER NOT NULL CHECK(candidates_unchanged >= 0),
    occurred_at          TEXT NOT NULL,
    FOREIGN KEY(producer, stream_id)
        REFERENCES native_candidate_intakes(producer, stream_id)
);
""",
    """
CREATE TRIGGER native_candidate_intake_events_no_update
BEFORE UPDATE ON native_candidate_intake_events
BEGIN
    SELECT RAISE(ABORT, 'native candidate intake events are append-only');
END;
""",
    """
CREATE TRIGGER native_candidate_intake_events_no_delete
BEFORE DELETE ON native_candidate_intake_events
BEGIN
    SELECT RAISE(ABORT, 'native candidate intake events are append-only');
END;
""",
    """
DROP TRIGGER task_events_no_update;
""",
    """
DROP TRIGGER task_events_no_delete;
""",
    """
ALTER TABLE task_events RENAME TO task_events_v9;
""",
    """
CREATE TABLE task_events (
    sequence        INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id         INTEGER NOT NULL,
    kind            TEXT NOT NULL CHECK(kind IN (
                        'created','candidate_folded','candidate_revised',
                        'status_changed'
                    )),
    task_version    INTEGER NOT NULL CHECK(task_version >= 1),
    candidate_id    TEXT,
    source_revision TEXT,
    from_status     TEXT,
    to_status       TEXT,
    occurred_at     TEXT NOT NULL,
    FOREIGN KEY(task_id) REFERENCES tasks(id)
);
""",
    """
INSERT INTO task_events(
    sequence,task_id,kind,task_version,candidate_id,source_revision,
    from_status,to_status,occurred_at
)
SELECT sequence,task_id,kind,task_version,candidate_id,source_revision,
       from_status,to_status,occurred_at
FROM task_events_v9;
""",
    """
DROP TABLE task_events_v9;
""",
    """
CREATE TRIGGER task_events_no_update
BEFORE UPDATE ON task_events
BEGIN
    SELECT RAISE(ABORT, 'task events are append-only');
END;
""",
    """
CREATE TRIGGER task_events_no_delete
BEFORE DELETE ON task_events
BEGIN
    SELECT RAISE(ABORT, 'task events are append-only');
END;
""",
)

_SCHEMA_V11_CARD_TABLE = (
    _SCHEMA_V9[0]
    .replace(
        "'start','plan_review','external_review'",
        "'start','plan_review','external_review','result_review'",
    )
    .replace("'plan','execute'", "'plan','execute','external_action'")
    .replace(
        "'start','snooze','cancel','approve','revise'",
        "'start','snooze','cancel','approve','revise','discuss','done',"
        "'reassign','drop'",
    )
    .replace(
        "            AND result_id IS NOT NULL)\n    ),",
        "            AND result_id IS NOT NULL)\n"
        "        OR (kind = 'result_review' AND result_id IS NOT NULL)\n"
        "    ),",
    )
)
_SCHEMA_V11_CARD_EVENT_TABLE = _SCHEMA_V9[2].replace(
    "'start','snooze','cancel','approve','revise'",
    "'start','snooze','cancel','approve','revise','discuss','done',"
    "'reassign','drop'",
)
_SCHEMA_V11_EXECUTION_EVENT_TABLE = _SCHEMA_V8[5].replace(
    "'phase_approved','revision_requested'",
    "'phase_approved','revision_requested','discussion_requested',"
    "'task_completed','task_dropped','reassigned'",
)

_SCHEMA_V11 = (
    "DROP TRIGGER execution_review_card_events_no_update;",
    "DROP TRIGGER execution_review_card_events_no_delete;",
    "DROP INDEX execution_review_cards_one_active;",
    "ALTER TABLE execution_review_card_events "
    "RENAME TO execution_review_card_events_v10;",
    "ALTER TABLE execution_review_cards "
    "RENAME TO execution_review_cards_v10;",
    _SCHEMA_V11_CARD_TABLE,
    _SCHEMA_V9[1],
    _SCHEMA_V11_CARD_EVENT_TABLE,
    """
INSERT INTO execution_review_cards(
    id,task_id,task_version,workflow_version,kind,phase,result_id,status,
    version,claim_token_digest,claim_expires_at,transport,delivery_ref,
    delivered_at,resolution,created_at,updated_at,resolved_at
)
SELECT id,task_id,task_version,workflow_version,kind,phase,result_id,status,
       version,claim_token_digest,claim_expires_at,transport,delivery_ref,
       delivered_at,resolution,created_at,updated_at,resolved_at
FROM execution_review_cards_v10;
""",
    """
INSERT INTO execution_review_card_events(
    sequence,card_id,task_id,kind,card_version,workflow_version,action,
    occurred_at
)
SELECT sequence,card_id,task_id,kind,card_version,workflow_version,action,
       occurred_at
FROM execution_review_card_events_v10;
""",
    "DROP TABLE execution_review_card_events_v10;",
    "DROP TABLE execution_review_cards_v10;",
    _SCHEMA_V9[3],
    _SCHEMA_V9[4],
    """
CREATE TABLE execution_reader_inputs (
    sequence                INTEGER PRIMARY KEY AUTOINCREMENT,
    card_id                 INTEGER NOT NULL,
    task_id                 INTEGER NOT NULL,
    card_version            INTEGER NOT NULL CHECK(card_version >= 1),
    task_version            INTEGER NOT NULL CHECK(task_version >= 1),
    workflow_version        INTEGER NOT NULL CHECK(workflow_version >= 1),
    target_workflow_version INTEGER NOT NULL
                                CHECK(target_workflow_version >= 1),
    kind                    TEXT NOT NULL CHECK(kind IN (
                                'discussion','reassignment'
                            )),
    value                   TEXT NOT NULL CHECK(
                                (kind = 'discussion'
                                 AND length(value) BETWEEN 1 AND 16000)
                                OR
                                (kind = 'reassignment'
                                 AND length(value) BETWEEN 1 AND 200
                                 AND instr(value, char(10)) = 0)
                            ),
    prior_value             TEXT CHECK(
                                prior_value IS NULL
                                OR length(prior_value) <= 200
                            ),
    occurred_at             TEXT NOT NULL,
    FOREIGN KEY(card_id) REFERENCES execution_review_cards(id),
    FOREIGN KEY(task_id) REFERENCES task_execution_workflows(task_id)
);
""",
    """
CREATE TRIGGER execution_reader_inputs_no_update
BEFORE UPDATE ON execution_reader_inputs
BEGIN
    SELECT RAISE(ABORT, 'execution reader inputs are append-only');
END;
""",
    """
CREATE TRIGGER execution_reader_inputs_no_delete
BEFORE DELETE ON execution_reader_inputs
BEGIN
    SELECT RAISE(ABORT, 'execution reader inputs are append-only');
END;
""",
    """
CREATE TABLE task_owner_events (
    sequence     INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id      INTEGER NOT NULL,
    task_version INTEGER NOT NULL CHECK(task_version >= 1),
    card_id      INTEGER NOT NULL,
    from_owner   TEXT,
    to_owner     TEXT NOT NULL CHECK(length(to_owner) BETWEEN 1 AND 200),
    occurred_at  TEXT NOT NULL,
    FOREIGN KEY(task_id) REFERENCES tasks(id),
    FOREIGN KEY(card_id) REFERENCES execution_review_cards(id)
);
""",
    """
CREATE TRIGGER task_owner_events_no_update
BEFORE UPDATE ON task_owner_events
BEGIN
    SELECT RAISE(ABORT, 'task owner events are append-only');
END;
""",
    """
CREATE TRIGGER task_owner_events_no_delete
BEFORE DELETE ON task_owner_events
BEGIN
    SELECT RAISE(ABORT, 'task owner events are append-only');
END;
""",
    "DROP TRIGGER task_execution_events_no_update;",
    "DROP TRIGGER task_execution_events_no_delete;",
    "ALTER TABLE task_execution_events "
    "RENAME TO task_execution_events_v10;",
    _SCHEMA_V11_EXECUTION_EVENT_TABLE,
    """
INSERT INTO task_execution_events(
    sequence,task_id,kind,workflow_version,task_version,phase,status,
    occurred_at
)
SELECT sequence,task_id,kind,workflow_version,task_version,phase,status,
       occurred_at
FROM task_execution_events_v10;
""",
    "DROP TABLE task_execution_events_v10;",
    _SCHEMA_V8[6],
    _SCHEMA_V8[7],
)

_V12_COMPATIBILITY_PROFILE_ID = "general"
_V12_COMPATIBILITY_PROFILE_REVISION = (
    "f0171b0e9e09e547d9b344223d31b6de1bc0e6d13cb5b8c891fda9d0a7b0db94"
)
_SCHEMA_V12_EXECUTION_EVENT_TABLE = _SCHEMA_V11_EXECUTION_EVENT_TABLE.replace(
    "'task_completed','task_dropped','reassigned'",
    "'task_completed','task_dropped','reassigned','agent_selected'",
).replace(
    "    occurred_at      TEXT NOT NULL,",
    f"    occurred_at      TEXT NOT NULL,\n"
    f"    agent_profile_id  TEXT NOT NULL DEFAULT "
    f"'{_V12_COMPATIBILITY_PROFILE_ID}' CHECK(\n"
    f"                          length(agent_profile_id) BETWEEN 1 AND 32\n"
    f"                          AND agent_profile_id GLOB '[a-z]*'\n"
    f"                          AND agent_profile_id NOT GLOB "
    f"'*[^a-z0-9-]*'\n"
    f"                      ),\n"
    f"    agent_profile_revision TEXT NOT NULL DEFAULT "
    f"'{_V12_COMPATIBILITY_PROFILE_REVISION}' CHECK(\n"
    f"                          length(agent_profile_revision) = 64\n"
    f"                          AND agent_profile_revision NOT GLOB "
    f"'*[^0-9a-f]*'\n"
    f"                      ),",
)

_SCHEMA_V12 = (
    "ALTER TABLE task_execution_workflows ADD COLUMN agent_profile_id "
    f"TEXT NOT NULL DEFAULT '{_V12_COMPATIBILITY_PROFILE_ID}' "
    "CHECK(length(agent_profile_id) BETWEEN 1 AND 32 "
    "AND agent_profile_id GLOB '[a-z]*' "
    "AND agent_profile_id NOT GLOB '*[^a-z0-9-]*');",
    "ALTER TABLE task_execution_workflows ADD COLUMN agent_profile_revision "
    f"TEXT NOT NULL DEFAULT '{_V12_COMPATIBILITY_PROFILE_REVISION}' "
    "CHECK(length(agent_profile_revision) = 64 "
    "AND agent_profile_revision NOT GLOB '*[^0-9a-f]*');",
    "ALTER TABLE task_execution_results ADD COLUMN agent_profile_id "
    f"TEXT NOT NULL DEFAULT '{_V12_COMPATIBILITY_PROFILE_ID}' "
    "CHECK(length(agent_profile_id) BETWEEN 1 AND 32 "
    "AND agent_profile_id GLOB '[a-z]*' "
    "AND agent_profile_id NOT GLOB '*[^a-z0-9-]*');",
    "ALTER TABLE task_execution_results ADD COLUMN agent_profile_revision "
    f"TEXT NOT NULL DEFAULT '{_V12_COMPATIBILITY_PROFILE_REVISION}' "
    "CHECK(length(agent_profile_revision) = 64 "
    "AND agent_profile_revision NOT GLOB '*[^0-9a-f]*');",
    "DROP TRIGGER task_execution_events_no_update;",
    "DROP TRIGGER task_execution_events_no_delete;",
    "ALTER TABLE task_execution_events RENAME TO task_execution_events_v11;",
    _SCHEMA_V12_EXECUTION_EVENT_TABLE,
    f"""
INSERT INTO task_execution_events(
    sequence,task_id,kind,workflow_version,task_version,phase,status,
    occurred_at,agent_profile_id,agent_profile_revision
)
SELECT sequence,task_id,kind,workflow_version,task_version,phase,status,
       occurred_at,'{_V12_COMPATIBILITY_PROFILE_ID}',
       '{_V12_COMPATIBILITY_PROFILE_REVISION}'
FROM task_execution_events_v11;
""",
    "DROP TABLE task_execution_events_v11;",
    _SCHEMA_V8[6],
    _SCHEMA_V8[7],
)

_SCHEMA_V13_CARD_EVENT_TABLE = (
    _SCHEMA_V11_CARD_EVENT_TABLE
    .replace(
        "'cancelled'\n                     )),",
        "'cancelled','refreshed'\n                     )),",
    )
    .replace("'reassign','drop'", "'reassign','drop','agent'")
)
_SCHEMA_V13 = (
    "DROP TRIGGER execution_review_card_events_no_update;",
    "DROP TRIGGER execution_review_card_events_no_delete;",
    "ALTER TABLE execution_review_card_events "
    "RENAME TO execution_review_card_events_v12;",
    _SCHEMA_V13_CARD_EVENT_TABLE,
    """
INSERT INTO execution_review_card_events(
    sequence,card_id,task_id,kind,card_version,workflow_version,action,
    occurred_at
)

SELECT sequence,card_id,task_id,kind,card_version,workflow_version,action,
       occurred_at
FROM execution_review_card_events_v12;
""",
    "DROP TABLE execution_review_card_events_v12;",
    _SCHEMA_V9[3],
    _SCHEMA_V9[4],
)

_SCHEMA_V14_EQUIVALENCE_TABLE = _SCHEMA_V6[0].replace(
    "CHECK(basis = 'speaker_merge')",
    "CHECK(basis IN ('speaker_merge','people_directory'))",
)
_SCHEMA_V14 = (
    "DROP TRIGGER task_owner_equivalences_no_update;",
    "DROP TRIGGER task_owner_equivalences_no_delete;",
    "ALTER TABLE task_owner_equivalences "
    "RENAME TO task_owner_equivalences_v13;",
    _SCHEMA_V14_EQUIVALENCE_TABLE,
    """
INSERT INTO task_owner_equivalences(
    candidate_id,source_revision,legacy_task_id,legacy_digest,
    effective_owner,basis,resolved_at
)
SELECT candidate_id,source_revision,legacy_task_id,legacy_digest,
       effective_owner,basis,resolved_at
FROM task_owner_equivalences_v13;
""",
    "DROP TABLE task_owner_equivalences_v13;",
    _SCHEMA_V6[1],
    _SCHEMA_V6[2],
)

_SCHEMA_V15 = (
    """
CREATE TABLE IF NOT EXISTS candidate_lifecycle (
    candidate_id    TEXT PRIMARY KEY,
    source_revision TEXT NOT NULL,
    state           TEXT NOT NULL CHECK(state IN ('active','withdrawn')),
    generation      INTEGER NOT NULL CHECK(generation >= 0),
    changed_at      TEXT,
    updated_at      TEXT NOT NULL,
    FOREIGN KEY(candidate_id, source_revision)
        REFERENCES candidate_revision_history(candidate_id, source_revision)
);
""",
    """
INSERT OR IGNORE INTO candidate_lifecycle(
    candidate_id,source_revision,state,generation,changed_at,updated_at
)
SELECT candidate_id,source_revision,'active',0,NULL,updated_at
FROM candidate_inbox;
""",
    """
CREATE TABLE IF NOT EXISTS task_candidate_lifecycle (
    candidate_id    TEXT PRIMARY KEY,
    source_revision TEXT NOT NULL,
    task_version    INTEGER NOT NULL CHECK(task_version >= 1),
    state           TEXT NOT NULL CHECK(state IN ('active','withdrawn')),
    resolution      TEXT NOT NULL CHECK(
                        resolution IN (
                            'current','preserved_open','reader_conflict'
                        )
                    ),
    changed_at      TEXT,
    decided_at      TEXT NOT NULL,
    FOREIGN KEY(candidate_id) REFERENCES task_candidate_bindings(candidate_id),
    FOREIGN KEY(candidate_id, source_revision)
        REFERENCES candidate_revision_history(candidate_id, source_revision)
);
""",
    """
INSERT OR IGNORE INTO task_candidate_lifecycle(
    candidate_id,source_revision,task_version,state,resolution,
    changed_at,decided_at
)
SELECT b.candidate_id,b.source_revision,t.version,'active','current',
       NULL,b.decided_at
FROM task_candidate_bindings AS b
JOIN tasks AS t ON t.id=b.task_id;
""",
    "DROP TRIGGER task_events_no_update;",
    "DROP TRIGGER task_events_no_delete;",
    "ALTER TABLE task_events RENAME TO task_events_v13;",
    """
CREATE TABLE task_events (
    sequence        INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id         INTEGER NOT NULL,
    kind            TEXT NOT NULL CHECK(kind IN (
                        'created','candidate_folded','candidate_revised',
                        'candidate_withdrawn','candidate_withdrawal_conflict',
                        'candidate_reactivated','candidate_reactivation_conflict',
                        'status_changed'
                    )),
    task_version    INTEGER NOT NULL CHECK(task_version >= 1),
    candidate_id    TEXT,
    source_revision TEXT,
    from_status     TEXT,
    to_status       TEXT,
    occurred_at     TEXT NOT NULL,
    FOREIGN KEY(task_id) REFERENCES tasks(id)
);
""",
    """
INSERT INTO task_events(
    sequence,task_id,kind,task_version,candidate_id,source_revision,
    from_status,to_status,occurred_at
)
SELECT sequence,task_id,kind,task_version,candidate_id,source_revision,
       from_status,to_status,occurred_at
FROM task_events_v13;
""",
    "DROP TABLE task_events_v13;",
    """
CREATE TRIGGER task_events_no_update
BEFORE UPDATE ON task_events
BEGIN
    SELECT RAISE(ABORT, 'task events are append-only');
END;
""",
    """
CREATE TRIGGER task_events_no_delete
BEFORE DELETE ON task_events
BEGIN
    SELECT RAISE(ABORT, 'task events are append-only');
END;
""",
)


_SCHEMA_V16 = (
    """
CREATE TABLE IF NOT EXISTS native_intake_historical_refusals (
    candidate_id    TEXT NOT NULL,
    source_revision TEXT NOT NULL,
    producer        TEXT NOT NULL,
    stream_id       TEXT NOT NULL,
    reason_code     TEXT NOT NULL CHECK(
                        reason_code = 'preserved_legacy_owner'
                    ),
    refused_at      TEXT NOT NULL,
    PRIMARY KEY(candidate_id, source_revision),
    FOREIGN KEY(candidate_id, source_revision)
        REFERENCES candidate_revision_history(candidate_id, source_revision)
);
""",
    """
CREATE TRIGGER IF NOT EXISTS native_intake_historical_refusals_no_update
BEFORE UPDATE ON native_intake_historical_refusals
BEGIN
    SELECT RAISE(ABORT, 'historical refusals are append-only');
END;
""",
    """
CREATE TRIGGER IF NOT EXISTS native_intake_historical_refusals_no_delete
BEFORE DELETE ON native_intake_historical_refusals
BEGIN
    SELECT RAISE(ABORT, 'historical refusals are append-only');
END;
""",
)

_SCHEMA_V17 = (
    "ALTER TABLE task_execution_results ADD COLUMN task_work_directory TEXT "
    "CHECK(task_work_directory IS NULL OR "
    "(length(task_work_directory) BETWEEN 1 AND 4096 "
    "AND substr(task_work_directory,1,1)='/'));",
    "ALTER TABLE task_execution_results ADD COLUMN task_kb_file TEXT "
    "CHECK(task_kb_file IS NULL OR "
    "(length(task_kb_file) BETWEEN 1 AND 4096 "
    "AND substr(task_kb_file,1,1)='/'));",
)

_SCHEMA_V18 = (
    "ALTER TABLE tasks ADD COLUMN owner_ref_version INTEGER NOT NULL "
    "DEFAULT 0 CHECK(owner_ref_version IN (0,1));",
    "ALTER TABLE tasks ADD COLUMN owner_kind TEXT "
    "CHECK(owner_kind IS NULL OR owner_kind IN "
    "('person','unresolved','external','group'));",
    "ALTER TABLE tasks ADD COLUMN owner_speaker_id TEXT;",
    "ALTER TABLE tasks ADD COLUMN owner_canonical_speaker_id TEXT;",
    "ALTER TABLE tasks ADD COLUMN owner_speaker_registry_id TEXT;",
    "ALTER TABLE tasks ADD COLUMN owner_pinned INTEGER NOT NULL "
    "DEFAULT 0 CHECK(owner_pinned IN (0,1));",
    "ALTER TABLE tasks ADD COLUMN owner_provisional INTEGER NOT NULL "
    "DEFAULT 1 CHECK(owner_provisional IN (0,1));",
)

_SCHEMA_V19 = (
    """
CREATE TABLE IF NOT EXISTS task_execution_owner_holds (
    id                          INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id                     INTEGER NOT NULL,
    task_version                INTEGER NOT NULL CHECK(task_version >= 1),
    workflow_version            INTEGER NOT NULL CHECK(workflow_version >= 1),
    status                      TEXT NOT NULL CHECK(status IN (
                                    'active','released','cancelled'
                                )),
    owner_display               TEXT NOT NULL CHECK(
                                    length(owner_display) BETWEEN 1 AND 200
                                ),
    owner_ref_version           INTEGER NOT NULL CHECK(owner_ref_version = 1),
    owner_kind                  TEXT NOT NULL CHECK(owner_kind IN (
                                    'person','external'
                                )),
    owner_speaker_id            TEXT,
    owner_canonical_speaker_id  TEXT,
    owner_speaker_registry_id   TEXT,
    owner_pinned                INTEGER NOT NULL CHECK(owner_pinned IN (0,1)),
    owner_provisional           INTEGER NOT NULL CHECK(owner_provisional = 0),
    backstop_at                 TEXT NOT NULL,
    last_checked_at             TEXT,
    last_evidence_revision      TEXT CHECK(
                                    last_evidence_revision IS NULL
                                    OR length(last_evidence_revision) = 64
                                ),
    last_match                  INTEGER CHECK(last_match IS NULL OR last_match IN (0,1)),
    release_reason              TEXT CHECK(release_reason IS NULL OR release_reason IN (
                                    'meeting','backstop','stale'
                                )),
    created_at                  TEXT NOT NULL,
    released_at                 TEXT,
    CHECK(
        (status = 'active' AND release_reason IS NULL AND released_at IS NULL)
        OR (status != 'active' AND release_reason IS NOT NULL
            AND released_at IS NOT NULL)
    ),
    CHECK(
        (owner_speaker_id IS NULL AND owner_canonical_speaker_id IS NULL
         AND owner_speaker_registry_id IS NULL)
        OR (owner_speaker_id IS NOT NULL
            AND owner_canonical_speaker_id IS NOT NULL
            AND owner_speaker_registry_id IS NOT NULL)
    ),
    FOREIGN KEY(task_id) REFERENCES task_execution_workflows(task_id)
);
""",
    """
CREATE UNIQUE INDEX IF NOT EXISTS task_execution_owner_holds_one_active
    ON task_execution_owner_holds(task_id)
    WHERE status = 'active';
""",
    """
CREATE TABLE IF NOT EXISTS task_execution_owner_hold_events (
    sequence          INTEGER PRIMARY KEY AUTOINCREMENT,
    hold_id           INTEGER NOT NULL,
    task_id           INTEGER NOT NULL,
    kind              TEXT NOT NULL CHECK(kind IN (
                          'created','condition_checked','released','cancelled'
                      )),
    matched           INTEGER CHECK(matched IS NULL OR matched IN (0,1)),
    evidence_revision TEXT CHECK(
                          evidence_revision IS NULL
                          OR length(evidence_revision) = 64
                      ),
    occurred_at       TEXT NOT NULL,
    FOREIGN KEY(hold_id) REFERENCES task_execution_owner_holds(id),
    FOREIGN KEY(task_id) REFERENCES task_execution_workflows(task_id)
);
""",
    """
CREATE TRIGGER IF NOT EXISTS task_execution_owner_hold_events_no_update
BEFORE UPDATE ON task_execution_owner_hold_events
BEGIN
    SELECT RAISE(ABORT, 'task execution owner hold events are append-only');
END;
""",
    """
CREATE TRIGGER IF NOT EXISTS task_execution_owner_hold_events_no_delete
BEFORE DELETE ON task_execution_owner_hold_events
BEGIN
    SELECT RAISE(ABORT, 'task execution owner hold events are append-only');
END;
""",
)


# A start gate is the "run this?" question, and until now the schema pinned
# it to `plan`. That pin was really a proxy for "carries no result", which
# the same branch already states. A workflow that parks during `execute` or
# `external_action` needs exactly this card — the agent gave up and the
# reader must be told — so the phase pin is dropped and the result pin kept.
_SCHEMA_V20_CARD_TABLE = _SCHEMA_V11_CARD_TABLE.replace(
    "(kind = 'start' AND phase = 'plan' AND result_id IS NULL)",
    "(kind = 'start' AND result_id IS NULL)",
)

_SCHEMA_V22 = (
    # Derived, not reported. The agent writes `work_markdown`; this is a
    # few sentences of it produced afterwards by a small model so the card
    # can show what the plan SAYS instead of its first screenful. Nullable
    # because it is allowed to be missing: the model is remote, the card
    # must render without it, and every result written before this column
    # existed has none.
    "ALTER TABLE task_execution_results ADD COLUMN work_digest TEXT "
    "CHECK(work_digest IS NULL OR length(work_digest) BETWEEN 1 AND 800);",
)

# ADR 0036 decision 2: a card belongs to exactly one consumer for as long as
# it is claimed. `claim_next()` will record the resolved consumer identity
# here -- the digest of the accepting bearer token, the same digest function
# already used for `claim_token_digest` -- at the moment of claim, one step
# earlier than `transport`, which is only ever populated at delivery. This
# migration adds only the column. Nothing yet writes or reads it: every
# existing row is left NULL, exactly like `claim_token_digest` and
# `transport` already are for a card that has never been claimed, so a
# database with no second consumer configured is unaffected. Matches
# `claim_token_digest`'s shape -- plain nullable TEXT holding a fixed-length
# sha256 hex digest, no CHECK -- rather than `work_digest`'s bounded free
# text, because a digest's length is already fixed by the hash function.
_SCHEMA_V23 = (
    "ALTER TABLE task_review_cards ADD COLUMN consumer_digest TEXT;",
)

# One more event kind, so a revision the reader's own decision overtook can be
# recorded as what it is. `candidate_revised` would claim the revision was
# folded into the task, and the withdrawal path already established the
# vocabulary for the other case: `candidate_withdrawal_conflict` and
# `candidate_reactivation_conflict` both mean "the producer said something
# about a task the reader had already settled, and we recorded it without
# applying it".
#
# SQLite cannot alter a CHECK in place and nothing holds a foreign key to this
# table, so the plain rename-copy-drop is enough here -- the same shape this
# table's own earlier CHECK widenings used. The triggers come off first
# because the table they guard is about to be renamed out from under them,
# and go back on last so the window where events are mutable is inside this
# transaction and nowhere else.
_SCHEMA_V24 = (
    "DROP TRIGGER task_events_no_update;",
    "DROP TRIGGER task_events_no_delete;",
    "ALTER TABLE task_events RENAME TO task_events_v23;",
    """
CREATE TABLE task_events (
    sequence        INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id         INTEGER NOT NULL,
    kind            TEXT NOT NULL CHECK(kind IN (
                        'created','candidate_folded','candidate_revised',
                        'candidate_revision_conflict',
                        'candidate_withdrawn','candidate_withdrawal_conflict',
                        'candidate_reactivated','candidate_reactivation_conflict',
                        'status_changed'
                    )),
    task_version    INTEGER NOT NULL CHECK(task_version >= 1),
    candidate_id    TEXT,
    source_revision TEXT,
    from_status     TEXT,
    to_status       TEXT,
    occurred_at     TEXT NOT NULL,
    FOREIGN KEY(task_id) REFERENCES tasks(id)
);
""",
    """
INSERT INTO task_events(
    sequence,task_id,kind,task_version,candidate_id,source_revision,
    from_status,to_status,occurred_at
)
SELECT sequence,task_id,kind,task_version,candidate_id,source_revision,
       from_status,to_status,occurred_at
FROM task_events_v23;
""",
    "DROP TABLE task_events_v23;",
    """
CREATE TRIGGER task_events_no_update
BEFORE UPDATE ON task_events
BEGIN
    SELECT RAISE(ABORT, 'task events are append-only');
END;
""",
    """
CREATE TRIGGER task_events_no_delete
BEFORE DELETE ON task_events
BEGIN
    SELECT RAISE(ABORT, 'task events are append-only');
END;
""",
)

#: One reason to believe an open task is finished, and what the reader said.
#:
#: Nothing here closes anything. A row is a question waiting to be asked, and
#: the answer to it -- the closure itself goes through the ordinary ledger
#: transition, exactly as it does when a reader closes a task unprompted.
#:
#: The identity is the EVIDENCE, not the judgement about it. Two runs of a
#: detector that quote the same sentence from the same source are one question,
#: however differently they word their reasoning, because the reader is being
#: asked to look at that sentence. That is what makes re-detection idempotent,
#: and it is also what makes a rejection durable: a refused row still occupies
#: the pair, so the same sentence can never raise the question twice.
#:
#: Append-only. A settled row keeps the quotation it was settled on, so the
#: share of detections a reader accepted stays recoverable afterwards and a
#: detector that is usually wrong is visible rather than merely irritating.
_SCHEMA_V25 = (
    """
CREATE TABLE IF NOT EXISTS task_completion_evidence (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id          INTEGER NOT NULL,
    -- Identity of the quoted evidence, not of this row: see above.
    evidence_digest  TEXT NOT NULL CHECK(length(evidence_digest) = 64),
    source_kind      TEXT NOT NULL
                     CHECK(length(source_kind) BETWEEN 1 AND 64),
    source_record_id TEXT NOT NULL
                     CHECK(length(source_record_id) BETWEEN 1 AND 200),
    source_item_id   TEXT NOT NULL CHECK(length(source_item_id) <= 200),
    -- When the source is dated, which is what the card shows. Not when this
    -- row was written: a meeting held in March read in April closes the task
    -- as of March, and the reader needs the earlier date to recognise it.
    observed_at      TEXT NOT NULL
                     CHECK(length(observed_at) BETWEEN 4 AND 40),
    -- Verbatim, and never empty. A done-check with nothing to quote is the
    -- card this table exists to stop us sending.
    quotation        TEXT NOT NULL
                     CHECK(length(quotation) BETWEEN 1 AND 1200),
    -- Why this evidence was read as closing THIS task rather than a similar
    -- one. The reader cannot check a match they were never shown.
    reason           TEXT NOT NULL CHECK(length(reason) BETWEEN 1 AND 500),
    detector         TEXT NOT NULL CHECK(length(detector) BETWEEN 1 AND 64),
    confidence       TEXT NOT NULL
                     CHECK(confidence IN ('high','medium','low')),
    state            TEXT NOT NULL DEFAULT 'proposed' CHECK(
                         state IN ('proposed','accepted','rejected','superseded')),
    -- The card carrying the question, once one does. Null while unasked, and
    -- null again if that card is cancelled before anyone answers it.
    card_id          INTEGER,
    created_at       TEXT NOT NULL,
    settled_at       TEXT,
    CHECK((state = 'proposed') = (settled_at IS NULL)),
    FOREIGN KEY(task_id) REFERENCES tasks(id),
    FOREIGN KEY(card_id) REFERENCES task_review_cards(id)
);
""",
    # One question per piece of evidence, for the life of the task. Unlike the
    # live-relation index this one has no WHERE clause, and that is the point:
    # a settled row keeps occupying the pair, so an answered question is never
    # asked again.
    """
CREATE UNIQUE INDEX IF NOT EXISTS task_completion_evidence_identity
    ON task_completion_evidence(task_id, evidence_digest);
""",
    """
CREATE INDEX IF NOT EXISTS task_completion_evidence_open
    ON task_completion_evidence(task_id) WHERE state = 'proposed';
""",
    """
CREATE TRIGGER IF NOT EXISTS task_completion_evidence_no_delete
BEFORE DELETE ON task_completion_evidence
BEGIN
    SELECT RAISE(ABORT, 'task completion evidence is append-only');
END;
""",
    # Binding and settling are the only changes. Everything the reader was
    # shown is frozen at insert, and a settled row never moves again -- an
    # accepted detection that could later be rewritten as rejected would make
    # the accept ratio a measure of the last pass rather than of the detector.
    """
CREATE TRIGGER IF NOT EXISTS task_completion_evidence_settle_only
BEFORE UPDATE ON task_completion_evidence
BEGIN
    SELECT RAISE(ABORT, 'task completion evidence may only be bound or settled')
    WHERE OLD.task_id          <> NEW.task_id
       OR OLD.evidence_digest  <> NEW.evidence_digest
       OR OLD.source_kind      <> NEW.source_kind
       OR OLD.source_record_id <> NEW.source_record_id
       OR OLD.source_item_id   <> NEW.source_item_id
       OR OLD.observed_at      <> NEW.observed_at
       OR OLD.quotation        <> NEW.quotation
       OR OLD.reason           <> NEW.reason
       OR OLD.detector         <> NEW.detector
       OR OLD.confidence       <> NEW.confidence
       OR OLD.created_at       <> NEW.created_at
       OR OLD.state            <> 'proposed';
END;
""",
)

# A review card is an assertion about the source revision the reader was
# shown, not merely about the task row. A task's version is deliberately
# unchanged for provenance-only enrichment, so it cannot be the fence for a
# later source comment. The migration snapshots the currently-bound revision
# on historical cards; cards without a bound candidate stay NULL.
_SCHEMA_V26 = (
    "ALTER TABLE task_review_cards ADD COLUMN source_revision TEXT "
    "CHECK(source_revision IS NULL OR length(source_revision) = 64);",
    "UPDATE task_review_cards SET source_revision=("
    " SELECT b.source_revision FROM task_candidate_bindings AS b "
    " WHERE b.task_id=task_review_cards.task_id "
    " AND b.relation='accepted') WHERE source_revision IS NULL;",
)


# A private, durable question that two independently accepted source tasks may
# describe one commitment.  Its identity is the unordered task pair, not the
# detector's wording: a repeated scan must not ask the reader again after a
# rejection.  The current state is intentionally small; the append-only event
# history retains every machine recommendation and reader decision.
_SCHEMA_V27 = (
    """
CREATE TABLE IF NOT EXISTS task_duplicate_proposals (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    left_task_id       INTEGER NOT NULL,
    right_task_id      INTEGER NOT NULL,
    left_task_version  INTEGER NOT NULL CHECK(left_task_version >= 1),
    right_task_version INTEGER NOT NULL CHECK(right_task_version >= 1),
    basis              TEXT NOT NULL CHECK(length(basis) BETWEEN 1 AND 1200),
    detector           TEXT NOT NULL CHECK(length(detector) BETWEEN 1 AND 64),
    state              TEXT NOT NULL CHECK(state IN (
                           'proposed','confirmed','rejected'
                       )),
    created_at         TEXT NOT NULL,
    updated_at         TEXT NOT NULL,
    settled_at         TEXT,
    CHECK(left_task_id < right_task_id),
    CHECK((state = 'proposed') = (settled_at IS NULL)),
    FOREIGN KEY(left_task_id) REFERENCES tasks(id),
    FOREIGN KEY(right_task_id) REFERENCES tasks(id)
);
""",
    """
CREATE UNIQUE INDEX IF NOT EXISTS task_duplicate_proposals_pair
    ON task_duplicate_proposals(left_task_id, right_task_id);
""",
    """
CREATE INDEX IF NOT EXISTS task_duplicate_proposals_open
    ON task_duplicate_proposals(left_task_id, right_task_id)
    WHERE state = 'proposed';
""",
    """
CREATE TABLE IF NOT EXISTS task_duplicate_proposal_events (
    sequence    INTEGER PRIMARY KEY AUTOINCREMENT,
    proposal_id INTEGER NOT NULL,
    kind        TEXT NOT NULL CHECK(kind IN (
                    'proposed','confirmed','rejected','reopened'
                )),
    actor       TEXT NOT NULL CHECK(length(actor) BETWEEN 1 AND 200),
    occurred_at TEXT NOT NULL,
    FOREIGN KEY(proposal_id) REFERENCES task_duplicate_proposals(id)
);
""",
    """
CREATE TRIGGER IF NOT EXISTS task_duplicate_proposal_events_no_update
BEFORE UPDATE ON task_duplicate_proposal_events
BEGIN
    SELECT RAISE(ABORT, 'task duplicate proposal events are append-only');
END;
""",
    """
CREATE TRIGGER IF NOT EXISTS task_duplicate_proposal_events_no_delete
BEFORE DELETE ON task_duplicate_proposal_events
BEGIN
    SELECT RAISE(ABORT, 'task duplicate proposal events are append-only');
END;
""",
    """
CREATE TRIGGER IF NOT EXISTS task_duplicate_proposals_no_delete
BEFORE DELETE ON task_duplicate_proposals
BEGIN
    SELECT RAISE(ABORT, 'task duplicate proposals are append-only');
END;
""",
    # Detector inputs are immutable.  Only a reader decision can settle an
    # unanswered proposal, and only an explicit reopening can make a rejected
    # proposal unanswered again; the event table records both decisions.
    """
CREATE TRIGGER IF NOT EXISTS task_duplicate_proposals_settle_only
BEFORE UPDATE ON task_duplicate_proposals
BEGIN
    SELECT RAISE(ABORT, 'task duplicate proposal may only be settled or reopened')
    WHERE OLD.left_task_id       <> NEW.left_task_id
       OR OLD.right_task_id      <> NEW.right_task_id
       OR OLD.left_task_version  <> NEW.left_task_version
       OR OLD.right_task_version <> NEW.right_task_version
       OR OLD.basis              <> NEW.basis
       OR OLD.detector           <> NEW.detector
       OR OLD.created_at         <> NEW.created_at
       OR NOT (
           (OLD.state = 'proposed' AND NEW.state IN ('confirmed','rejected')
            AND NEW.settled_at IS NOT NULL)
           OR
           (OLD.state = 'rejected' AND NEW.state = 'proposed'
            AND NEW.settled_at IS NULL)
       );
END;
""",
)


# A duplicate proposal is presented by one ordinary task-review card.  The
# binding is private and may be released when that card becomes stale, so an
# unanswered proposal can be offered again without duplicating its decision
# record.
_SCHEMA_V28 = (
    "DROP TRIGGER task_duplicate_proposals_settle_only;",
    """
CREATE TRIGGER task_duplicate_proposals_settle_only
BEFORE UPDATE ON task_duplicate_proposals
BEGIN
    SELECT RAISE(ABORT, 'task duplicate proposal may only be settled, reopened, or rebound')
    WHERE OLD.left_task_id       <> NEW.left_task_id
       OR OLD.right_task_id      <> NEW.right_task_id
       OR OLD.left_task_version  <> NEW.left_task_version
       OR OLD.right_task_version <> NEW.right_task_version
       OR OLD.basis              <> NEW.basis
       OR OLD.detector           <> NEW.detector
       OR OLD.created_at         <> NEW.created_at
       OR NOT (
           (OLD.state = 'proposed' AND NEW.state IN ('confirmed','rejected')
            AND NEW.settled_at IS NOT NULL AND OLD.card_id IS NEW.card_id)
           OR
           (OLD.state IN ('rejected','confirmed') AND NEW.state = 'proposed'
            AND NEW.settled_at IS NULL
            AND (OLD.card_id IS NEW.card_id OR NEW.card_id IS NULL))
           OR
           (OLD.state = 'proposed' AND NEW.state = 'proposed'
            AND OLD.settled_at IS NULL AND NEW.settled_at IS NULL
            AND ((OLD.card_id IS NULL AND NEW.card_id IS NOT NULL)
                 OR (OLD.card_id IS NOT NULL AND NEW.card_id IS NULL)))
       );
END;
""",
)

# Consumer attribution is nullable so cards claimed by a pre-activation
# binary remain unowned and can drain through the legacy path.
_SCHEMA_V30 = (
    "ALTER TABLE execution_review_cards ADD COLUMN consumer_digest TEXT "
    "CHECK(consumer_digest IS NULL OR length(consumer_digest) = 64);",
)


# Structured forge evidence is separate from result prose.  Existing result
# rows are historical facts, so they receive the empty collection rather than
# a guessed set of links extracted from their old Markdown.
_SCHEMA_V29 = (
    "ALTER TABLE task_execution_results ADD COLUMN "
    "repository_references_json TEXT NOT NULL DEFAULT '[]' "
    "CHECK(length(repository_references_json) <= 65536);",
)


# Existing results predate the explicit distinction, so preserve the safe
# interpretation: their repository-origin execution remains impactful.
_SCHEMA_V31 = (
    "ALTER TABLE task_execution_results ADD COLUMN "
    "repository_impact INTEGER NOT NULL DEFAULT 1 "
    "CHECK(repository_impact IN (0,1));",
)


# Queue priority is a deliberately closed, three-state reader preference.  It
# is not a score and it does not carry reader-provided ordering data.
_SCHEMA_V32_EXECUTION_EVENT_TABLE = _SCHEMA_V12_EXECUTION_EVENT_TABLE.replace(
    "'task_completed','task_dropped','reassigned','agent_selected'",
    "'task_completed','task_dropped','reassigned','agent_selected',"
    "'priority_raised','priority_lowered','priority_cleared'",
)
_SCHEMA_V32 = (
    "ALTER TABLE task_execution_workflows ADD COLUMN queue_priority "
    "TEXT NOT NULL DEFAULT 'normal' CHECK(queue_priority IN "
    "('raised','normal','lowered'));",
    "CREATE INDEX task_execution_workflows_priority_ready "
    "ON task_execution_workflows("
    "status,next_attempt_at,queue_priority,failure_count,updated_at,task_id);",
    "DROP TRIGGER task_execution_events_no_update;",
    "DROP TRIGGER task_execution_events_no_delete;",
    "ALTER TABLE task_execution_events RENAME TO task_execution_events_v31;",
    _SCHEMA_V32_EXECUTION_EVENT_TABLE,
    "INSERT INTO task_execution_events("
    "sequence,task_id,kind,workflow_version,task_version,phase,status,"
    "occurred_at,agent_profile_id,agent_profile_revision) "
    "SELECT sequence,task_id,kind,workflow_version,task_version,phase,status,"
    "occurred_at,agent_profile_id,agent_profile_revision "
    "FROM task_execution_events_v31;",
    "DROP TABLE task_execution_events_v31;",
    _SCHEMA_V8[6],
    _SCHEMA_V8[7],
)


# A derived title is deliberately separate from the immutable task text.  A
# worker owns the small state machine: confirmation queues `pending`, a worker
# leases it as `running`, and only a validated gateway response becomes
# `ready`.  `idle` records that the supporting relation was withdrawn.
_SCHEMA_V33 = (
    """
CREATE TABLE task_fused_title_jobs (
    task_id INTEGER PRIMARY KEY REFERENCES tasks(id),
    state TEXT NOT NULL CHECK(state IN ('idle','pending','running','ready')),
    title TEXT CHECK(title IS NULL OR (
        length(title) BETWEEN 1 AND 160
        AND instr(title, char(10)) = 0 AND instr(title, char(13)) = 0
    )),
    attempts INTEGER NOT NULL DEFAULT 0 CHECK(attempts >= 0),
    last_attempt_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    CHECK((state = 'ready' AND title IS NOT NULL)
          OR (state <> 'ready' AND title IS NULL))
);
""",
    "CREATE INDEX task_fused_title_jobs_pending "
    "ON task_fused_title_jobs(state,updated_at,task_id);",
    """
CREATE TRIGGER task_fused_title_jobs_no_delete
BEFORE DELETE ON task_fused_title_jobs
BEGIN
    SELECT RAISE(ABORT, 'fused title jobs are retained');
END;
""",
)


# A process exit is a safe, bounded machine diagnostic.  It never contains
# transcript text, a filesystem path, or a command line; the opaque run id
# only correlates the workflow with its private archive.
_SCHEMA_V34 = (
    "ALTER TABLE task_execution_workflows ADD COLUMN "
    "last_failure_exit_code INTEGER CHECK(last_failure_exit_code IS NULL OR "
    "(last_failure_exit_code >= 1 AND last_failure_exit_code <= 255));",
    "ALTER TABLE task_execution_workflows ADD COLUMN "
    "last_failure_run_id TEXT CHECK(last_failure_run_id IS NULL OR "
    "(length(last_failure_run_id) = 32 AND "
    "last_failure_run_id GLOB '[0-9a-f]*'));",
)


# Re-presenting an unanswered card used to drop the handle of the message it
# was replacing, so the surface accumulated a column of identical cards: the
# reader saw the same decision repeated with no way to tell which one the
# controls still answered.  Re-presentation now carries the
# superseded handle forward so the consumer can withdraw its predecessor
# before posting the replacement, which is what the legacy task-card re-ask
# has always done.  The transport is carried with it because a handle is only
# meaningful to the surface that issued it, and a consumer must be able to
# tell that the ref it is holding is one of its own.
_SCHEMA_V35 = (
    "ALTER TABLE execution_review_cards ADD COLUMN "
    "superseded_delivery_ref TEXT;",
    "ALTER TABLE execution_review_cards ADD COLUMN "
    "superseded_transport TEXT;",
)


# `phase_granted` records that a machine's standing grant advanced a
# workflow, and exists so the ledger never claims a reader approved a phase
# nobody was asked about. The distinction matters for the same reason
# `candidate_revision_conflict` is not `candidate_revised`: an event log that
# overstates human involvement cannot be used to audit it.
_SCHEMA_V36_EXECUTION_EVENT_TABLE = _SCHEMA_V32_EXECUTION_EVENT_TABLE.replace(
    "'phase_approved','revision_requested'",
    "'phase_approved','phase_granted','revision_requested'",
)
_SCHEMA_V36 = (
    "DROP TRIGGER task_execution_events_no_update;",
    "DROP TRIGGER task_execution_events_no_delete;",
    "ALTER TABLE task_execution_events RENAME TO task_execution_events_v35;",
    _SCHEMA_V36_EXECUTION_EVENT_TABLE,
    "INSERT INTO task_execution_events("
    "sequence,task_id,kind,workflow_version,task_version,phase,status,"
    "occurred_at,agent_profile_id,agent_profile_revision) "
    "SELECT sequence,task_id,kind,workflow_version,task_version,phase,status,"
    "occurred_at,agent_profile_id,agent_profile_revision "
    "FROM task_execution_events_v35;",
    "DROP TABLE task_execution_events_v35;",
    _SCHEMA_V8[6],
    _SCHEMA_V8[7],
)

# A continuing ask has one durable identity even as its accepted source
# revision advances.  The task remains the compatibility projection; these
# rows preserve why a new revision superseded prior runnable work.
#
# Every statement here is replayable: a migration may be re-run against a
# database that already carries these rows, and one task may hold several
# accepted bindings that share a source revision.  The backfill therefore
# ignores conflicts rather than failing the whole migration on the
# UNIQUE(work_item_id,source_revision) constraint.
_SCHEMA_V37 = (
    "CREATE TABLE IF NOT EXISTS work_items (id INTEGER PRIMARY KEY,task_id INTEGER NOT NULL UNIQUE REFERENCES tasks(id),state TEXT NOT NULL DEFAULT 'active' CHECK(state IN ('active','withdrawn','closed')),created_at TEXT NOT NULL,updated_at TEXT NOT NULL);",
    "CREATE TABLE IF NOT EXISTS work_revisions (id INTEGER PRIMARY KEY,work_item_id INTEGER NOT NULL REFERENCES work_items(id),candidate_id TEXT NOT NULL,source_revision TEXT NOT NULL,task_version INTEGER NOT NULL,created_at TEXT NOT NULL,UNIQUE(work_item_id,source_revision));",
    "CREATE INDEX IF NOT EXISTS work_revisions_current ON work_revisions(work_item_id,id DESC);",
    "INSERT OR IGNORE INTO work_items(task_id,created_at,updated_at) SELECT id,created_at,updated_at FROM tasks;",
    "INSERT OR IGNORE INTO work_revisions(work_item_id,candidate_id,source_revision,task_version,created_at) SELECT w.id,b.candidate_id,b.source_revision,t.version,b.decided_at FROM task_candidate_bindings b JOIN work_items w ON w.task_id=b.task_id JOIN tasks t ON t.id=b.task_id WHERE b.relation='accepted';",
    "CREATE TRIGGER IF NOT EXISTS work_revision_on_accepted_binding AFTER INSERT ON task_candidate_bindings WHEN NEW.relation='accepted' BEGIN INSERT OR IGNORE INTO work_items(task_id,created_at,updated_at) SELECT NEW.task_id,NEW.decided_at,NEW.decided_at; INSERT OR IGNORE INTO work_revisions(work_item_id,candidate_id,source_revision,task_version,created_at) SELECT id,NEW.candidate_id,NEW.source_revision,(SELECT version FROM tasks WHERE id=NEW.task_id),NEW.decided_at FROM work_items WHERE task_id=NEW.task_id; END;",
    "CREATE TRIGGER IF NOT EXISTS work_revision_on_source_update AFTER UPDATE OF source_revision ON task_candidate_bindings WHEN NEW.relation='accepted' BEGIN INSERT OR IGNORE INTO work_revisions(work_item_id,candidate_id,source_revision,task_version,created_at) SELECT id,NEW.candidate_id,NEW.source_revision,(SELECT version FROM tasks WHERE id=NEW.task_id),NEW.decided_at FROM work_items WHERE task_id=NEW.task_id; END;",
)

# Binding writes also acknowledge evidence-only and reader-conflict updates.
# A work revision instead records a source state actually folded into work, so
# it is appended explicitly by the ledger rather than by a broad UPDATE trigger.
#
# Split into three parts because this migration has to be replayable, and the
# rebuild is the one piece that is not. Replaying v37 re-creates the broad
# update trigger this version exists to remove, so the drops must run every
# time; and the rebuild relabels every row it copies as 'accepted', so running
# it over an already-converted table would silently erase the
# 'source_advance' distinction it was written to introduce.
_SCHEMA_V38_DROP = (
    "DROP TRIGGER IF EXISTS work_revision_on_accepted_binding;",
    "DROP TRIGGER IF EXISTS work_revision_on_source_update;",
)
_SCHEMA_V38_REBUILD = (
    "DROP INDEX IF EXISTS work_revisions_current;",
    "ALTER TABLE work_revisions RENAME TO work_revisions_v37;",
    "CREATE TABLE work_revisions (id INTEGER PRIMARY KEY,work_item_id INTEGER NOT NULL REFERENCES work_items(id),candidate_id TEXT NOT NULL,source_revision TEXT NOT NULL,task_version INTEGER NOT NULL,kind TEXT NOT NULL CHECK(kind IN ('accepted','source_advance')),created_at TEXT NOT NULL);",
    "INSERT INTO work_revisions(id,work_item_id,candidate_id,source_revision,task_version,kind,created_at) SELECT id,work_item_id,candidate_id,source_revision,task_version,'accepted',created_at FROM work_revisions_v37;",
    "DROP TABLE work_revisions_v37;",
    "CREATE INDEX work_revisions_current ON work_revisions(work_item_id,id DESC);",
)
_SCHEMA_V38_TRIGGER = (
    "CREATE TRIGGER work_revision_on_accepted_binding AFTER INSERT ON task_candidate_bindings WHEN NEW.relation='accepted' BEGIN INSERT OR IGNORE INTO work_items(task_id,created_at,updated_at) SELECT NEW.task_id,NEW.decided_at,NEW.decided_at; INSERT INTO work_revisions(work_item_id,candidate_id,source_revision,task_version,kind,created_at) SELECT id,NEW.candidate_id,NEW.source_revision,(SELECT version FROM tasks WHERE id=NEW.task_id),'accepted',NEW.decided_at FROM work_items WHERE task_id=NEW.task_id; END;",
)


# An execution approval says a reader agreed to an action. It recorded which
# task version and workflow version were in force, but not which source state
# the reader was actually looking at, so afterwards nothing could say what was
# approved. The work revision names exactly that.
#
# Nullable and not backfilled on purpose: cards raised before this column
# existed were approved against a source state nobody recorded, and inventing
# one now would be a worse answer than admitting it is unknown.
_SCHEMA_V39 = (
    "CREATE INDEX IF NOT EXISTS execution_review_cards_work_revision ON execution_review_cards(work_revision_id);",
)

# An effect is made durable before it crosses an external boundary.  The
# idempotency key belongs to the intent rather than a receipt because retries
# need to identify an already-completed effect before calling an adapter.
# Payloads never enter this schema: their SHA-256 digest is enough to detect a
# contradictory replay without retaining the text that would be sent.
_SCHEMA_V41 = (
    "CREATE TABLE IF NOT EXISTS effect_intents (intent_id TEXT PRIMARY KEY,work_item_id INTEGER NOT NULL REFERENCES work_items(id),work_revision_id INTEGER NOT NULL REFERENCES work_revisions(id),kind TEXT NOT NULL CHECK(kind IN ('forge','email','teams')),target TEXT NOT NULL,payload_digest TEXT NOT NULL,idempotency_key TEXT NOT NULL UNIQUE,freshness_required INTEGER NOT NULL CHECK(freshness_required IN (0,1)),created_at TEXT NOT NULL);",
    "CREATE TABLE IF NOT EXISTS effect_receipts (intent_id TEXT PRIMARY KEY REFERENCES effect_intents(intent_id),work_item_id INTEGER NOT NULL REFERENCES work_items(id),work_revision_id INTEGER NOT NULL REFERENCES work_revisions(id),state TEXT NOT NULL CHECK(state IN ('completed','failed','cancelled')),receipt_id TEXT,reversible INTEGER NOT NULL CHECK(reversible IN (0,1)),recorded_at TEXT NOT NULL);",
)


# A refused repeat is still something that happened to the workflow. Without
# an event, the only trace of a pass that answered nothing is its absence from
# the result table -- which reads exactly like a pass that never ran.
_SCHEMA_V40_EXECUTION_EVENT_TABLE = _SCHEMA_V36_EXECUTION_EVENT_TABLE.replace(
    "'phase_approved','phase_granted','revision_requested'",
    "'phase_approved','phase_granted','revision_requested',"
    "'result_unchanged'",
)
_SCHEMA_V40 = (
    "DROP TRIGGER task_execution_events_no_update;",
    "DROP TRIGGER task_execution_events_no_delete;",
    "ALTER TABLE task_execution_events RENAME TO task_execution_events_v39;",
    _SCHEMA_V40_EXECUTION_EVENT_TABLE,
    "INSERT INTO task_execution_events("
    "sequence,task_id,kind,workflow_version,task_version,phase,status,"
    "occurred_at,agent_profile_id,agent_profile_revision) "
    "SELECT sequence,task_id,kind,workflow_version,task_version,phase,status,"
    "occurred_at,agent_profile_id,agent_profile_revision "
    "FROM task_execution_events_v39;",
    "DROP TABLE task_execution_events_v39;",
    _SCHEMA_V8[6],
    _SCHEMA_V8[7],
)


# Which reader instruction a run was actually handed. Three failures were
# indistinguishable without it -- the instruction was never selected, it was
# selected and disregarded, or it was acted on and the work was lost before
# recording -- and telling them apart meant diffing files on disk by hand.
#
# One row per (task, workflow version): a run is handed at most one
# instruction, and handing it twice is the same delivery, not a second one.
# The reader's words are NOT copied here; they already live in
# `execution_reader_inputs` and belong in one place.
_SCHEMA_V42_EXECUTION_EVENT_TABLE = _SCHEMA_V40_EXECUTION_EVENT_TABLE.replace(
    "'revision_requested',"
    "'result_unchanged'",
    "'revision_requested',"
    "'result_unchanged','reader_instruction_delivered'",
)
_SCHEMA_V42 = (
    "ALTER TABLE task_execution_results ADD COLUMN "
    "reader_instruction_sequence INTEGER REFERENCES "
    "execution_reader_inputs(sequence);",
    """CREATE TABLE execution_reader_instruction_deliveries (
    task_id              INTEGER NOT NULL,
    workflow_version     INTEGER NOT NULL CHECK(workflow_version >= 1),
    instruction_sequence INTEGER NOT NULL,
    occurred_at          TEXT NOT NULL,
    PRIMARY KEY(task_id, workflow_version),
    FOREIGN KEY(task_id) REFERENCES task_execution_workflows(task_id),
    FOREIGN KEY(instruction_sequence)
        REFERENCES execution_reader_inputs(sequence)
);""",
    "DROP TRIGGER task_execution_events_no_update;",
    "DROP TRIGGER task_execution_events_no_delete;",
    "ALTER TABLE task_execution_events RENAME TO task_execution_events_v41;",
    _SCHEMA_V42_EXECUTION_EVENT_TABLE,
    "INSERT INTO task_execution_events("
    "sequence,task_id,kind,workflow_version,task_version,phase,status,"
    "occurred_at,agent_profile_id,agent_profile_revision) "
    "SELECT sequence,task_id,kind,workflow_version,task_version,phase,status,"
    "occurred_at,agent_profile_id,agent_profile_revision "
    "FROM task_execution_events_v41;",
    "DROP TABLE task_execution_events_v41;",
    _SCHEMA_V8[6],
    _SCHEMA_V8[7],
)


# A local semantic evaluator records only opaque pair identities, its closed
# verdict, and aggregate-cost inputs. It never stores a prompt, model reply,
# or task-derived explanation. The reader's proposal ledger remains the source
# of truth for labels.
_SCHEMA_V43 = (
    """
CREATE TABLE IF NOT EXISTS task_duplicate_assessments (
    left_task_id      INTEGER NOT NULL REFERENCES tasks(id),
    right_task_id     INTEGER NOT NULL REFERENCES tasks(id),
    detector          TEXT NOT NULL CHECK(length(detector) BETWEEN 1 AND 64),
    verdict           TEXT NOT NULL CHECK(verdict IN (
                          'redundant','intersecting','interconnected'
                      )),
    latency_ms        INTEGER NOT NULL CHECK(latency_ms >= 0),
    prompt_tokens     INTEGER NOT NULL CHECK(prompt_tokens >= 0),
    completion_tokens INTEGER NOT NULL CHECK(completion_tokens >= 0),
    assessed_at       TEXT NOT NULL,
    PRIMARY KEY(left_task_id,right_task_id,detector),
    CHECK(left_task_id < right_task_id)
);
""",
    "CREATE INDEX IF NOT EXISTS task_duplicate_assessments_pair "
    "ON task_duplicate_assessments(left_task_id,right_task_id);",
    """
CREATE TRIGGER IF NOT EXISTS task_duplicate_assessments_no_update
BEFORE UPDATE ON task_duplicate_assessments
BEGIN
    SELECT RAISE(ABORT, 'task duplicate assessments are immutable');
END;
""",
    """
CREATE TRIGGER IF NOT EXISTS task_duplicate_assessments_no_delete
BEFORE DELETE ON task_duplicate_assessments
BEGIN
    SELECT RAISE(ABORT, 'task duplicate assessments are retained');
END;
""",
)


# A proposal records the task versions it was raised against, and the
# settle-only trigger makes those versions immutable on purpose: a proposal is
# a fixed record of "these two tasks, as they were, may be one commitment".
#
# That leaves a gap. The ask fences on an exact version match, so once either
# task moves the proposal can never be carded; the pair index allowed one
# proposal per pair for all time, so no fresh one could replace it. The pair
# became a question nobody could be asked, and it counted as unsettled
# forever -- which permanently closes the gate that requires every proposal to
# be settled before a detector may be measured.
#
# `superseded` is the terminal state for a question that stopped being worth
# asking, as distinct from one a reader answered. Pair uniqueness now ignores
# superseded rows, so the detector can raise the pair again at current
# versions with a current basis, which is the honest way to re-ask: a new
# question, not an old one edited to look new.
_SCHEMA_V44 = (
    "DROP TRIGGER IF EXISTS task_duplicate_proposals_settle_only;",
    "DROP TRIGGER IF EXISTS task_duplicate_proposals_no_delete;",
    "DROP INDEX IF EXISTS task_duplicate_proposals_pair;",
    "DROP INDEX IF EXISTS task_duplicate_proposals_open;",
    "ALTER TABLE task_duplicate_proposals RENAME TO task_duplicate_proposals_v43;",
    """
CREATE TABLE task_duplicate_proposals (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    left_task_id       INTEGER NOT NULL,
    right_task_id      INTEGER NOT NULL,
    left_task_version  INTEGER NOT NULL CHECK(left_task_version >= 1),
    right_task_version INTEGER NOT NULL CHECK(right_task_version >= 1),
    basis              TEXT NOT NULL CHECK(length(basis) BETWEEN 1 AND 1200),
    detector           TEXT NOT NULL CHECK(length(detector) BETWEEN 1 AND 64),
    state              TEXT NOT NULL CHECK(state IN (
                           'proposed','confirmed','rejected','superseded'
                       )),
    created_at         TEXT NOT NULL,
    updated_at         TEXT NOT NULL,
    settled_at         TEXT,
    card_id            INTEGER REFERENCES task_review_cards(id),
    CHECK(left_task_id < right_task_id),
    CHECK((state = 'proposed') = (settled_at IS NULL)),
    FOREIGN KEY(left_task_id) REFERENCES tasks(id),
    FOREIGN KEY(right_task_id) REFERENCES tasks(id)
);
""",
    "INSERT INTO task_duplicate_proposals(id,left_task_id,right_task_id,left_task_version,right_task_version,basis,detector,state,created_at,updated_at,settled_at,card_id) SELECT id,left_task_id,right_task_id,left_task_version,right_task_version,basis,detector,state,created_at,updated_at,settled_at,card_id FROM task_duplicate_proposals_v43;",
    "DROP TABLE task_duplicate_proposals_v43;",
    """
CREATE UNIQUE INDEX task_duplicate_proposals_pair
    ON task_duplicate_proposals(left_task_id, right_task_id)
    WHERE state <> 'superseded';
""",
    """
CREATE INDEX task_duplicate_proposals_open
    ON task_duplicate_proposals(left_task_id, right_task_id)
    WHERE state = 'proposed';
""",
    """
CREATE TRIGGER task_duplicate_proposals_no_delete
BEFORE DELETE ON task_duplicate_proposals
BEGIN
    SELECT RAISE(ABORT, 'task duplicate proposals are append-only');
END;
""",
    """
CREATE TRIGGER task_duplicate_proposals_settle_only
BEFORE UPDATE ON task_duplicate_proposals
BEGIN
    SELECT RAISE(ABORT, 'task duplicate proposal may only be settled, reopened, rebound, or superseded')
    WHERE OLD.left_task_id       <> NEW.left_task_id
       OR OLD.right_task_id      <> NEW.right_task_id
       OR OLD.left_task_version  <> NEW.left_task_version
       OR OLD.right_task_version <> NEW.right_task_version
       OR OLD.basis              <> NEW.basis
       OR OLD.detector           <> NEW.detector
       OR OLD.created_at         <> NEW.created_at
       OR NOT (
           (OLD.state = 'proposed' AND NEW.state IN ('confirmed','rejected')
            AND NEW.settled_at IS NOT NULL AND OLD.card_id IS NEW.card_id)
           OR
           (OLD.state IN ('rejected','confirmed') AND NEW.state = 'proposed'
            AND NEW.settled_at IS NULL
            AND (OLD.card_id IS NEW.card_id OR NEW.card_id IS NULL))
           OR
           (OLD.state = 'proposed' AND NEW.state = 'proposed'
            AND OLD.settled_at IS NULL AND NEW.settled_at IS NULL
            AND ((OLD.card_id IS NULL AND NEW.card_id IS NOT NULL)
                 OR (OLD.card_id IS NOT NULL AND NEW.card_id IS NULL)))
           OR
           -- Expiring an unaskable question. Only from `proposed`, only while
           -- no card is bound: a question already in front of a reader is
           -- theirs to answer, not ours to withdraw.
           (OLD.state = 'proposed' AND NEW.state = 'superseded'
            AND NEW.settled_at IS NOT NULL AND OLD.card_id IS NULL
            AND NEW.card_id IS NULL)
       );
END;
""",
)

# Why a run without a result stopped, in a few sentences, for the attempt it
# describes. The ledger already records HOW a process ended -- a reason and an
# exit code -- and that classification cannot distinguish an exhausted turn
# budget from a saturated backend from a refused worker operation, which imply
# completely different next actions. The cause was written down exactly once,
# in a transcript on disk that nothing read.
#
# One row per (task, workflow version): that pair is one attempt, because
# `claim_next` increments the version and `renew` does not. Phase is carried
# because a failure in `execute` says nothing about a `plan` pass that
# succeeded, and a reader shown the wrong phase's cause is worse off than one
# shown none. `run_id` records which transcript it came from, so a digest can
# be traced to its evidence.
#
# Derived and best-effort. The absence of a row is a normal state and carries
# no meaning beyond "we have no digest": the model is remote, and a failure to
# summarise a failure must never become a second failure.
_SCHEMA_V47 = (
    """CREATE TABLE IF NOT EXISTS execution_failure_digests (
    task_id          INTEGER NOT NULL,
    workflow_version INTEGER NOT NULL CHECK(workflow_version >= 1),
    phase            TEXT NOT NULL CHECK(phase IN (
                         'plan','execute','external_action'
                     )),
    run_id           TEXT CHECK(run_id IS NULL OR
                         (length(run_id) = 32 AND run_id GLOB '[0-9a-f]*')),
    digest           TEXT NOT NULL CHECK(length(digest) BETWEEN 1 AND 800),
    created_at       TEXT NOT NULL,
    PRIMARY KEY(task_id, workflow_version),
    FOREIGN KEY(task_id) REFERENCES task_execution_workflows(task_id)
);""",
)


# An announced run is decided at workflow admission, rather than by the card
# scheduler reading its own copy of deployment policy.  The run id is nullable
# because attaching it is deliberately best-effort.  A steer card has no
# result: it describes work that has not completed yet.  SQLite cannot widen
# the card-kind and card-shape checks in place, so the card table is rebuilt
# while retaining all columns added since its original introduction.
_SCHEMA_V49_CARD_TABLE = (
    _SCHEMA_V20_CARD_TABLE
    .replace(
        "'start','plan_review','external_review','result_review'",
        "'start','plan_review','external_review','result_review','steer'",
    )
    .replace(
        "        OR (kind = 'result_review' AND result_id IS NOT NULL)\n    ),",
        "        OR (kind = 'result_review' AND result_id IS NOT NULL)\n"
        "        OR (kind = 'steer' AND result_id IS NULL)\n    ),",
    )
    .replace(
        "    resolved_at        TEXT,",
        "    resolved_at        TEXT,\n"
        "    consumer_digest    TEXT CHECK(consumer_digest IS NULL OR "
        "length(consumer_digest) = 64),\n"
        "    superseded_delivery_ref TEXT,\n"
        "    superseded_transport TEXT,\n"
        "    work_revision_id   INTEGER REFERENCES work_revisions(id),\n"
        "    steer_digest       TEXT CHECK(steer_digest IS NULL OR "
        "length(steer_digest) <= 800),",
    )
)

_SCHEMA_V50 = (
    """CREATE TABLE IF NOT EXISTS execution_card_retractions (
    card_id INTEGER PRIMARY KEY REFERENCES execution_review_cards(id),
    transport TEXT NOT NULL CHECK(length(transport) BETWEEN 1 AND 200),
    delivery_ref TEXT NOT NULL CHECK(length(delivery_ref) BETWEEN 1 AND 2000),
    state TEXT NOT NULL CHECK(state IN ('pending','delivering','completed','abandoned')),
    attempts INTEGER NOT NULL DEFAULT 0 CHECK(attempts BETWEEN 0 AND 3),
    claim_token_digest TEXT,
    claim_expires_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    CHECK((state='delivering') = (claim_token_digest IS NOT NULL AND claim_expires_at IS NOT NULL)),
    CHECK(state NOT IN ('completed','abandoned') OR claim_token_digest IS NULL)
);""",
    "CREATE INDEX IF NOT EXISTS execution_card_retractions_pending ON execution_card_retractions(state,updated_at,card_id);",
)

# A live run may change meaningfully after its first digest. Refresh it at
# most twice more, after a bounded interval, so a run that spans hours cannot
# turn a best-effort model call into a standing load source.
_SCHEMA_V51 = (
    """CREATE TABLE IF NOT EXISTS execution_steer_digest_refreshes (
    card_id INTEGER PRIMARY KEY REFERENCES execution_review_cards(id),
    attempts INTEGER NOT NULL CHECK(attempts BETWEEN 1 AND 3),
    last_refreshed_at TEXT NOT NULL
);""",
)

# A successful retraction is a separate delivery-lifecycle fact. Keep it in
# the immutable card-event ledger rather than inferring it from cleared
# delivery handles, which are deliberately removed after acknowledgement.
_SCHEMA_V52_CARD_EVENT_TABLE = _SCHEMA_V13_CARD_EVENT_TABLE.replace(
    "'cancelled','refreshed'",
    "'cancelled','refreshed','retracted'",
)
_SCHEMA_V52 = (
    "DROP TRIGGER execution_review_card_events_no_update;",
    "DROP TRIGGER execution_review_card_events_no_delete;",
    "ALTER TABLE execution_review_card_events "
    "RENAME TO execution_review_card_events_v51;",
    _SCHEMA_V52_CARD_EVENT_TABLE,
    """
INSERT INTO execution_review_card_events(
    sequence,card_id,task_id,kind,card_version,workflow_version,action,
    occurred_at
)
SELECT sequence,card_id,task_id,kind,card_version,workflow_version,action,
       occurred_at
FROM execution_review_card_events_v51;
""",
    "DROP TABLE execution_review_card_events_v51;",
    _SCHEMA_V9[3],
    _SCHEMA_V9[4],
)


# Re-presenting an unanswered card is not a delivery failure. `requeue_unanswered`
# borrowed the `delivery_failed` kind to record it, and `delivery_health` counts
# every such event in a 15-minute window against a threshold of three -- so an
# hourly requeue of three or more unanswered cards raised
# `recent_delivery_failures_exceeded` on a system that was delivering fine.
#
# The visible cost was a watchdog that cried wolf. The real cost is that a
# genuine transport failure became indistinguishable from routine re-presentation,
# so the check that exists to catch broken delivery could not.
_SCHEMA_V53_CARD_EVENT_TABLE = _SCHEMA_V52_CARD_EVENT_TABLE.replace(
    "'cancelled','refreshed','retracted'",
    "'cancelled','refreshed','retracted','requeued'",
)
_SCHEMA_V53 = (
    "DROP TRIGGER execution_review_card_events_no_update;",
    "DROP TRIGGER execution_review_card_events_no_delete;",
    "ALTER TABLE execution_review_card_events "
    "RENAME TO execution_review_card_events_v52;",
    _SCHEMA_V53_CARD_EVENT_TABLE,
    """
INSERT INTO execution_review_card_events(
    sequence,card_id,task_id,kind,card_version,workflow_version,action,
    occurred_at
)
SELECT sequence,card_id,task_id,kind,card_version,workflow_version,action,
       occurred_at
FROM execution_review_card_events_v52;
""",
    "DROP TABLE execution_review_card_events_v52;",
    _SCHEMA_V9[3],
    _SCHEMA_V9[4],
)


# A run summary is a delivery record, not a reader gate.  It rides the
# established private transport as an ordinary execution card row -- the
# transport validates `kind` against a closed set, so inventing a kind here
# would refuse the claim, and a refused claim holds the drip's only slot and
# stops every card reaching the reader.  The distinction is carried by this
# column instead, and by two separate partial indexes: one active reader-action
# card per task as before, and independently at most one active summary per
# task.  The second index is what bounds the summary queue: a newer summary
# supersedes the one it replaces rather than queueing behind it.
_SCHEMA_V54 = (
    "ALTER TABLE execution_review_cards ADD COLUMN summary_only INTEGER "
    "NOT NULL DEFAULT 0 CHECK(summary_only IN (0,1));",
    # Replayable, as the additive migrations around it are: a rehearsal may
    # reset `user_version` while leaving these definitions in place.
    "DROP INDEX IF EXISTS execution_review_cards_one_active;",
    "CREATE UNIQUE INDEX execution_review_cards_one_active "
    "ON execution_review_cards(task_id) "
    "WHERE status IN ('pending','delivering','delivered') "
    "AND summary_only=0;",
    "DROP INDEX IF EXISTS execution_review_cards_one_active_summary;",
    "CREATE UNIQUE INDEX execution_review_cards_one_active_summary "
    "ON execution_review_cards(task_id) "
    "WHERE status IN ('pending','delivering','delivered') "
    "AND summary_only=1;",
)


# Run summaries are retired.  Nothing writes one now, so the index that
# bounded the summary queue has nothing left to bound, and the rows already
# in the table would otherwise sit `pending` forever -- never claimed,
# because every read excludes them, and never retired, because the sweep
# that retires stale cards reads the same population.  Settling them here is
# what leaves the table saying only what is still true.
#
# The column stays.  Dropping it would rewrite a table that the card service
# reads on every claim, to erase a distinction the history still needs: these
# rows really were summaries, and a `result_review` row that lost the marker
# would read as a decision card nobody ever answered.
_SCHEMA_V55 = (
    "UPDATE execution_review_cards SET status='cancelled',"
    "version=version+1,claim_token_digest=NULL,claim_expires_at=NULL,"
    "consumer_digest=NULL,"
    "resolved_at=COALESCE(resolved_at,updated_at) "
    "WHERE summary_only=1 "
    "AND status IN ('pending','delivering','delivered');",
    "DROP INDEX IF EXISTS execution_review_cards_one_active_summary;",
)


# ADR 0036 decision 2: record the claiming consumer's identity on a
# task review card at claim time. Migration adds structure (ADR 0010).
_SCHEMA_V56 = (
    "ALTER TABLE task_review_cards ADD COLUMN claiming_consumer TEXT;",
)


# Settle duplicate proposals stuck in 'proposed' for tasks that already carry an
# active duplicate_of relation (Issue #571).
_SCHEMA_V57 = (
    "UPDATE task_duplicate_proposals "
    "SET state='superseded', settled_at=COALESCE(settled_at, updated_at), updated_at=datetime('now') "
    "WHERE state='proposed' AND card_id IS NULL AND ("
    "EXISTS (SELECT 1 FROM task_relations WHERE subject_id=left_task_id AND kind='duplicate_of' AND withdrawn_at IS NULL) "
    "OR EXISTS (SELECT 1 FROM task_relations WHERE subject_id=right_task_id AND kind='duplicate_of' AND withdrawn_at IS NULL));",
)


# A work revision already names the candidate projection folded into a task.
# Candidate v9 additionally names the producer's immutable source-history row.
# Legacy rows remain explicitly unknown: migration must not guess which source
# observation preceded an old candidate.
_SCHEMA_V58_COLUMNS = (
    "source_history_source",
    "source_history_stream_id",
    "source_history_item_id",
    "source_history_position",
    "source_history_revision",
)
_SCHEMA_V58 = (
    "ALTER TABLE work_revisions ADD COLUMN source_history_source TEXT;",
    "ALTER TABLE work_revisions ADD COLUMN source_history_stream_id TEXT;",
    "ALTER TABLE work_revisions ADD COLUMN source_history_item_id TEXT;",
    "ALTER TABLE work_revisions ADD COLUMN source_history_position INTEGER "
    "CHECK(source_history_position IS NULL OR source_history_position > 0);",
    "ALTER TABLE work_revisions ADD COLUMN source_history_revision TEXT "
    "CHECK(source_history_revision IS NULL OR "
    "length(source_history_revision) = 64);",
    "DROP TRIGGER IF EXISTS work_revision_on_accepted_binding;",
    """CREATE TRIGGER work_revision_on_accepted_binding
AFTER INSERT ON task_candidate_bindings WHEN NEW.relation='accepted'
BEGIN
    INSERT OR IGNORE INTO work_items(task_id,created_at,updated_at)
    SELECT NEW.task_id,NEW.decided_at,NEW.decided_at;
    INSERT INTO work_revisions(
        work_item_id,candidate_id,source_revision,task_version,kind,created_at,
        source_history_source,source_history_stream_id,source_history_item_id,
        source_history_position,source_history_revision
    )
    SELECT w.id,NEW.candidate_id,NEW.source_revision,
           (SELECT version FROM tasks WHERE id=NEW.task_id),'accepted',
           NEW.decided_at,
           json_extract(i.payload_json,'$.source.history.source'),
           json_extract(i.payload_json,'$.source.history.stream_id'),
           json_extract(i.payload_json,'$.source.history.item_id'),
           json_extract(i.payload_json,'$.source.history.position'),
           json_extract(i.payload_json,'$.source.history.revision')
    FROM work_items AS w
    JOIN candidate_inbox AS i ON i.candidate_id=NEW.candidate_id
    WHERE w.task_id=NEW.task_id;
END;""",
)


# Releasing an execution-card claim when the review surface is full or
# locally rejected is a normal lease relinquishment, not a transport
# delivery failure. Keep it in the immutable event ledger as a distinct
# neutral event kind with bounded reasons.
_SCHEMA_V59_CARD_EVENT_TABLE = (
    _SCHEMA_V53_CARD_EVENT_TABLE
    .replace(
        "'cancelled','refreshed','retracted','requeued'",
        "'cancelled','refreshed','retracted','requeued','claim_released'",
    )
    .replace(
        "'reassign','drop','agent'",
        "'reassign','drop','agent','surface_full','client_rejected'",
    )
)
_SCHEMA_V59 = (
    "DROP TRIGGER execution_review_card_events_no_update;",
    "DROP TRIGGER execution_review_card_events_no_delete;",
    "ALTER TABLE execution_review_card_events "
    "RENAME TO execution_review_card_events_v58;",
    _SCHEMA_V59_CARD_EVENT_TABLE,
    """
INSERT INTO execution_review_card_events(
    sequence,card_id,task_id,kind,card_version,workflow_version,action,
    occurred_at
)
SELECT sequence,card_id,task_id,kind,card_version,workflow_version,action,
       occurred_at
FROM execution_review_card_events_v58;
""",
    "DROP TABLE execution_review_card_events_v58;",
    _SCHEMA_V9[3],
    _SCHEMA_V9[4],
)


# Consumer ownership on a card is mutable: release clears it and a later
# claim may replace it.  Delivery-health decisions must not recover the
# consumer for an immutable event by joining back to that mutable row.
# Record the bounded digest on the event at occurrence time instead.  Old
# events remain explicitly unknown because their original consumer cannot be
# reconstructed safely after the fact.
_SCHEMA_V60_CARD_EVENT_TABLE = _SCHEMA_V59_CARD_EVENT_TABLE.replace(
    "    occurred_at      TEXT NOT NULL,\n"
    "    FOREIGN KEY(card_id)",
    "    occurred_at      TEXT NOT NULL,\n"
    "    consumer_digest  TEXT CHECK(consumer_digest IS NULL OR "
    "length(consumer_digest) = 64),\n"
    "    FOREIGN KEY(card_id)",
)
_SCHEMA_V60 = (
    "DROP TRIGGER execution_review_card_events_no_update;",
    "DROP TRIGGER execution_review_card_events_no_delete;",
    "ALTER TABLE execution_review_card_events "
    "RENAME TO execution_review_card_events_v59;",
    _SCHEMA_V60_CARD_EVENT_TABLE,
    """
INSERT INTO execution_review_card_events(
    sequence,card_id,task_id,kind,card_version,workflow_version,action,
    consumer_digest,occurred_at
)
SELECT sequence,card_id,task_id,kind,card_version,workflow_version,action,
       NULL,occurred_at
FROM execution_review_card_events_v59;
""",
    "DROP TABLE execution_review_card_events_v59;",
    _SCHEMA_V9[3],
    _SCHEMA_V9[4],
)


# Context exhaustion is a separate terminal condition for one attempt. The
# workflow table has a closed reason vocabulary, so admitting it requires a
# table rebuild rather than silently recording it as an ordinary timeout.
# The migration below takes the current definition from SQLite, preserving
# every later-added column and constraint while widening only this vocabulary.
_CONTEXT_EXHAUSTED_REASON = "'result_invalid','context_exhausted'"

_SCHEMA_V46 = (
    """CREATE TABLE IF NOT EXISTS execution_result_artifacts (
    result_id       TEXT NOT NULL REFERENCES task_execution_results(result_id),
    ordinal         INTEGER NOT NULL CHECK(ordinal >= 0),
    relative_path   TEXT NOT NULL CHECK(length(relative_path) BETWEEN 1 AND 1024),
    name            TEXT NOT NULL CHECK(length(name) BETWEEN 1 AND 255),
    size_bytes      INTEGER NOT NULL CHECK(size_bytes BETWEEN 0 AND 2097152),
    content_digest  TEXT NOT NULL CHECK(length(content_digest)=64),
    run_directory   TEXT NOT NULL CHECK(length(run_directory) BETWEEN 1 AND 4096),
    PRIMARY KEY(result_id, ordinal),
    UNIQUE(result_id, relative_path)
);""",
)


# Structured fields enrich a task without replacing the reader-facing text.
# Participants are a collection of identity references, so they are never
# packed into a JSON column or copied display names.
_SCHEMA_V45 = (
    "ALTER TABLE tasks ADD COLUMN object TEXT ",
    "ALTER TABLE tasks ADD COLUMN action TEXT ",
    "ALTER TABLE tasks ADD COLUMN confidence REAL ",
    """
CREATE TABLE task_participants (
    task_id                     INTEGER NOT NULL REFERENCES tasks(id),
    position                    INTEGER NOT NULL CHECK(position >= 0),
    kind                        TEXT NOT NULL CHECK(kind IN (
                                   'person','unresolved','external','group'
                               )),
    speaker_id                  TEXT,
    canonical_speaker_id        TEXT,
    speaker_registry_id         TEXT,
    PRIMARY KEY(task_id, position),
    UNIQUE(task_id, kind, speaker_id, canonical_speaker_id, speaker_registry_id),
    CHECK(
        (speaker_id IS NULL AND speaker_registry_id IS NULL)
        OR (speaker_id IS NOT NULL AND speaker_registry_id IS NOT NULL)
    ),
    CHECK(canonical_speaker_id IS NULL OR speaker_id IS NOT NULL),
    CHECK(kind NOT IN ('external','group') OR (
        speaker_id IS NULL AND canonical_speaker_id IS NULL
        AND speaker_registry_id IS NULL
    )),
    CHECK(kind <> 'unresolved' OR canonical_speaker_id IS NULL)
);
""",
    """
CREATE TABLE task_duplicate_proposal_routes (
    proposal_id INTEGER NOT NULL REFERENCES task_duplicate_proposals(id),
    route       TEXT NOT NULL CHECK(route IN (
                    'words','reread','object','participant','legacy'
                )),
    PRIMARY KEY(proposal_id, route)
);
""",
    """
CREATE TABLE speaker_registry_entries (
    speaker_registry_id   TEXT NOT NULL,
    speaker_id            TEXT NOT NULL,
    canonical_speaker_id  TEXT,
    display_name          TEXT NOT NULL,
    updated_at            TEXT NOT NULL,
    PRIMARY KEY(speaker_registry_id, speaker_id)
);
""",
    """
INSERT OR IGNORE INTO task_duplicate_proposal_routes(proposal_id,route)
SELECT id,'legacy' FROM task_duplicate_proposals;
""",
)


_SCHEMA_V20 = (
    # SQLite cannot alter a CHECK in place, and three tables carry a foreign
    # key to this one. Renaming the card table would rewrite all three to
    # follow the rename, leaving them pointed at the table this migration
    # drops. So the replacement is built under its own name and renamed into
    # place last: nothing ever references `_v20`, so that final rename
    # rewrites nothing, and the children keep naming `execution_review_cards`
    # throughout. Foreign keys are disabled around the whole step, because
    # dropping the old table is otherwise read as orphaning every child row.
    _SCHEMA_V20_CARD_TABLE.replace(
        "CREATE TABLE execution_review_cards (",
        "CREATE TABLE execution_review_cards_v20 (",
    ),
    "INSERT INTO execution_review_cards_v20("
    "id,task_id,task_version,workflow_version,kind,phase,result_id,status,"
    "version,claim_token_digest,claim_expires_at,transport,delivery_ref,"
    "delivered_at,resolution,created_at,updated_at,resolved_at) "
    "SELECT id,task_id,task_version,workflow_version,kind,phase,result_id,"
    "status,version,claim_token_digest,claim_expires_at,transport,"
    "delivery_ref,delivered_at,resolution,created_at,updated_at,resolved_at "
    "FROM execution_review_cards;",
    "DROP TABLE execution_review_cards;",
    "ALTER TABLE execution_review_cards_v20 "
    "RENAME TO execution_review_cards;",
    _SCHEMA_V9[1],
)


#: A durable, attributed, reversible statement that two tasks are related.
#:
#: The card surface already answers "what came before this task" by joining on
#: source identity: two review tasks for the same pull request share a stem, so
#: one can be named as the other's predecessor without storing anything. That
#: works, costs nothing to keep in sync, and cannot express any of what this
#: table is for — a relation between tasks from different sources, a basis, an
#: actor, or a decision a reader made and can take back.
#:
#: Append-only, like every other attributed record here. A relation is
#: withdrawn by recording a withdrawal, never by deleting the assertion: "we
#: decided these were the same and then decided they were not" is the history
#: worth keeping, and a deleted row keeps none of it.
_SCHEMA_V21 = (
    """
CREATE TABLE IF NOT EXISTS task_relations (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    subject_id    INTEGER NOT NULL,
    object_id     INTEGER NOT NULL,
    kind          TEXT NOT NULL CHECK(kind IN ('supersedes','duplicate_of')),
    -- What the assertion rests on, in the asserter's own terms. Bounded, and
    -- never empty: a relation nobody can justify is one nobody can review.
    basis         TEXT NOT NULL CHECK(length(basis) BETWEEN 1 AND 500),
    -- Who said so. A machine's inference and a reader's confirmation are the
    -- same shape and must not be the same fact.
    asserted_by   TEXT NOT NULL CHECK(asserted_by IN ('machine','reader')),
    actor         TEXT CHECK(actor IS NULL OR length(actor) BETWEEN 1 AND 200),
    note          TEXT CHECK(note IS NULL OR length(note) BETWEEN 1 AND 500),
    created_at    TEXT NOT NULL,
    withdrawn_at  TEXT,
    withdrawn_by  TEXT CHECK(
        withdrawn_by IS NULL OR withdrawn_by IN ('machine','reader')),
    CHECK(subject_id <> object_id),
    CHECK((withdrawn_at IS NULL) = (withdrawn_by IS NULL)),
    FOREIGN KEY(subject_id) REFERENCES tasks(id),
    FOREIGN KEY(object_id) REFERENCES tasks(id)
);
""",
    # One live relation of a kind between an ordered pair. A withdrawn one
    # does not occupy the slot, so the same pair can be asserted again later
    # — a reader who changes their mind twice is not a constraint violation.
    """
CREATE UNIQUE INDEX IF NOT EXISTS task_relations_live
    ON task_relations(subject_id, object_id, kind)
    WHERE withdrawn_at IS NULL;
""",
    """
CREATE INDEX IF NOT EXISTS task_relations_object ON task_relations(object_id);
""",
    """
CREATE TRIGGER IF NOT EXISTS task_relations_no_delete
BEFORE DELETE ON task_relations
BEGIN
    SELECT RAISE(ABORT, 'task relations are append-only');
END;
""",
    # Only the withdrawal columns may ever change, and only once: withdrawing
    # a withdrawn relation would overwrite when it happened.
    """
CREATE TRIGGER IF NOT EXISTS task_relations_only_withdraw
BEFORE UPDATE ON task_relations
BEGIN
    SELECT RAISE(ABORT, 'task relations may only be withdrawn')
    WHERE OLD.subject_id  <> NEW.subject_id
       OR OLD.object_id   <> NEW.object_id
       OR OLD.kind        <> NEW.kind
       OR OLD.basis       <> NEW.basis
       OR OLD.asserted_by <> NEW.asserted_by
       OR OLD.created_at  <> NEW.created_at
       OR OLD.withdrawn_at IS NOT NULL;
END;
""",
)


class InboxError(RuntimeError):
    """The inbox cannot safely initialize or read its state."""


class ImportDisposition(StrEnum):
    INSERTED = "inserted"
    UNCHANGED = "unchanged"
    UPDATED = "updated"
    REFUSED = "refused"


class ImportRefusal(StrEnum):
    INVALID_CONTRACT = "invalid_contract"
    REVISION_CONFLICT = "revision_conflict"
    CREATED_AT_CONFLICT = "created_at_conflict"
    STALE_GENERATION = "stale_generation"
    GENERATION_CONFLICT = "generation_conflict"
    GENERATION_GAP = "generation_gap"


class FeedImportDisposition(StrEnum):
    APPLIED = "applied"
    REPLAYED = "replayed"
    EMPTY = "empty"
    REFUSED = "refused"


class FeedImportRefusal(StrEnum):
    INVALID_CONTRACT = "invalid_contract"
    CURSOR_GAP = "cursor_gap"
    CURSOR_OVERLAP = "cursor_overlap"
    CURSOR_REUSE = "cursor_reuse"
    CANDIDATE_CONFLICT = "candidate_conflict"


class ShadowFeedImportDisposition(StrEnum):
    APPLIED = "applied"
    REPLAYED = "replayed"
    EMPTY = "empty"
    REFUSED = "refused"


class ShadowFeedImportRefusal(StrEnum):
    INVALID_CONTRACT = "invalid_contract"
    CURSOR_GAP = "cursor_gap"
    CURSOR_OVERLAP = "cursor_overlap"
    CURSOR_REUSE = "cursor_reuse"
    CANDIDATE_MISSING = "candidate_missing"
    CANDIDATE_CONFLICT = "candidate_conflict"
    OBSERVATION_CONFLICT = "observation_conflict"


@dataclass(frozen=True)
class ImportResult:
    """Content-free result safe for aggregate reporting."""

    disposition: ImportDisposition
    refusal: ImportRefusal | None = None

    @property
    def accepted(self) -> bool:
        return self.disposition is not ImportDisposition.REFUSED


@dataclass(frozen=True)
class FeedImportResult:
    """Content-free aggregate result for one atomic feed-page import."""

    disposition: FeedImportDisposition
    inserted: int = 0
    updated: int = 0
    unchanged: int = 0
    refusal: FeedImportRefusal | None = None

    @property
    def accepted(self) -> bool:
        return self.disposition is not FeedImportDisposition.REFUSED


@dataclass(frozen=True)
class ShadowFeedImportResult:
    """Content-free aggregate result for one atomic observation page."""

    disposition: ShadowFeedImportDisposition
    inserted: int = 0
    unchanged: int = 0
    refusal: ShadowFeedImportRefusal | None = None

    @property
    def accepted(self) -> bool:
        return self.disposition is not ShadowFeedImportDisposition.REFUSED


@dataclass(frozen=True)
class ShadowComparisonReport:
    """Content-free aggregate status for all persisted observations."""

    total: int
    agreed: int
    divergent: int
    refused: int
    unmapped: int


@dataclass(frozen=True)
class ShadowImportCycleReceipt:
    """Content-free durable record of one completed import cycle."""

    stream_id: str
    started_at: str
    completed_at: str
    candidate_previous_cursor: int
    candidate_current_cursor: int
    candidates_inserted: int
    candidates_updated: int
    observation_previous_cursor: int
    observation_current_cursor: int
    observations_inserted: int
    comparison: ShadowComparisonReport


class CandidateInbox:
    """A versioned SQLite inbox at one explicitly selected path."""

    def __init__(self, database_path: str | os.PathLike[str], *,
                 clock: Callable[[], datetime] | None = None) -> None:
        self.database_path = Path(database_path)
        self._clock = clock or (lambda: datetime.now(timezone.utc))

    def _migrate(self) -> None:
        """Create or upgrade the inbox schema for the database command only."""
        self._prepare_database_file()
        with closing(self._connect()) as connection:
            version = self._schema_version(connection)
            if version > SCHEMA_VERSION:
                raise InboxError(
                    "candidate inbox schema is newer than this application"
                )
            if version == 0:
                connection.execute("BEGIN IMMEDIATE")
                try:
                    connection.execute(_SCHEMA_V1)
                    connection.execute("PRAGMA user_version = 1")
                    connection.commit()
                except Exception:
                    connection.rollback()
                    raise
                version = 1
            if version == 1:
                self._require_tables(connection, ("candidate_inbox",))
                connection.execute("BEGIN IMMEDIATE")
                try:
                    for statement in _SCHEMA_V2:
                        connection.execute(statement)
                    connection.execute("PRAGMA user_version = 2")
                    connection.commit()
                except Exception:
                    connection.rollback()
                    raise
                version = 2
            if version == 2:
                self._require_tables(
                    connection,
                    (
                        "candidate_inbox",
                        "candidate_feed_cursors",
                        "candidate_feed_receipts",
                    ),
                )
                connection.execute("BEGIN IMMEDIATE")
                try:
                    for statement in _SCHEMA_V3:
                        connection.execute(statement)
                    connection.execute("PRAGMA user_version = 3")
                    connection.commit()
                except Exception:
                    connection.rollback()
                    raise
                version = 3
            if version == 3:
                self._require_tables(
                    connection,
                    (
                        "candidate_inbox",
                        "candidate_feed_cursors",
                        "candidate_feed_receipts",
                        "candidate_revision_history",
                        "task_shadow_observations",
                        "task_shadow_feed_cursors",
                        "task_shadow_feed_receipts",
                    ),
                )
                connection.execute("PRAGMA foreign_keys = ON")
                connection.execute("BEGIN IMMEDIATE")
                try:
                    for statement in _SCHEMA_V4:
                        connection.execute(statement)
                    connection.execute("PRAGMA user_version = 4")
                    connection.commit()
                except Exception:
                    connection.rollback()
                    raise
                version = 4
            if version == 4:
                self._require_tables(
                    connection,
                    (
                        "candidate_inbox",
                        "candidate_feed_cursors",
                        "candidate_feed_receipts",
                        "candidate_revision_history",
                        "task_shadow_observations",
                        "task_shadow_feed_cursors",
                        "task_shadow_feed_receipts",
                        "tasks",
                        "task_candidate_bindings",
                        "task_bootstrap_correlations",
                        "task_events",
                    ),
                )
                connection.execute("BEGIN IMMEDIATE")
                try:
                    for statement in _SCHEMA_V5:
                        connection.execute(statement)
                    connection.execute("PRAGMA user_version = 5")
                    connection.commit()
                except Exception:
                    connection.rollback()
                    raise
                version = 5
            if version == 5:
                self._require_tables(
                    connection,
                    (
                        "candidate_inbox",
                        "candidate_feed_cursors",
                        "candidate_feed_receipts",
                        "candidate_revision_history",
                        "task_shadow_observations",
                        "task_shadow_feed_cursors",
                        "task_shadow_feed_receipts",
                        "tasks",
                        "task_candidate_bindings",
                        "task_bootstrap_correlations",
                        "task_events",
                        "shadow_import_cycles",
                    ),
                )
                connection.execute("PRAGMA foreign_keys = ON")
                connection.execute("BEGIN IMMEDIATE")
                try:
                    for statement in _SCHEMA_V6:
                        connection.execute(statement)
                    connection.execute("PRAGMA user_version = 6")
                    connection.commit()
                except Exception:
                    connection.rollback()
                    raise
                version = 6
            if version == 6:
                self._require_tables(
                    connection,
                    (
                        "candidate_inbox",
                        "candidate_feed_cursors",
                        "candidate_feed_receipts",
                        "candidate_revision_history",
                        "task_shadow_observations",
                        "task_shadow_feed_cursors",
                        "task_shadow_feed_receipts",
                        "tasks",
                        "task_candidate_bindings",
                        "task_bootstrap_correlations",
                        "task_events",
                        "shadow_import_cycles",
                        "task_owner_equivalences",
                    ),
                )
                connection.execute("PRAGMA foreign_keys = ON")
                connection.execute("BEGIN IMMEDIATE")
                try:
                    for statement in _SCHEMA_V7:
                        connection.execute(statement)
                    connection.execute("PRAGMA user_version = 7")
                    connection.commit()
                except Exception:
                    connection.rollback()
                    raise
                version = 7
            if version == 7:
                self._require_tables(
                    connection,
                    (
                        "candidate_inbox",
                        "candidate_feed_cursors",
                        "candidate_feed_receipts",
                        "candidate_revision_history",
                        "task_shadow_observations",
                        "task_shadow_feed_cursors",
                        "task_shadow_feed_receipts",
                        "tasks",
                        "task_candidate_bindings",
                        "task_bootstrap_correlations",
                        "task_events",
                        "shadow_import_cycles",
                        "task_owner_equivalences",
                        "task_review_cards",
                        "task_review_card_events",
                    ),
                )
                connection.execute("PRAGMA foreign_keys = ON")
                connection.execute("BEGIN IMMEDIATE")
                try:
                    for statement in _SCHEMA_V8:
                        connection.execute(statement)
                    connection.execute("PRAGMA user_version = 8")
                    connection.commit()
                except Exception:
                    connection.rollback()
                    raise
                version = 8
            if version == 8:
                self._require_tables(
                    connection,
                    (
                        "candidate_inbox",
                        "candidate_feed_cursors",
                        "candidate_feed_receipts",
                        "candidate_revision_history",
                        "task_shadow_observations",
                        "task_shadow_feed_cursors",
                        "task_shadow_feed_receipts",
                        "tasks",
                        "task_candidate_bindings",
                        "task_bootstrap_correlations",
                        "task_events",
                        "shadow_import_cycles",
                        "task_owner_equivalences",
                        "task_review_cards",
                        "task_review_card_events",
                        "task_execution_workflows",
                        "task_execution_results",
                        "task_execution_events",
                    ),
                    columns=_SCHEMA_V11_COLUMNS,
                )
                connection.execute("PRAGMA foreign_keys = ON")
                connection.execute("BEGIN IMMEDIATE")
                try:
                    for statement in _SCHEMA_V9:
                        connection.execute(statement)
                    connection.execute("PRAGMA user_version = 9")
                    connection.commit()
                except Exception:
                    connection.rollback()
                    raise
                version = 9
            if version == 9:
                self._require_tables(
                    connection,
                    (
                        "candidate_inbox",
                        "candidate_feed_cursors",
                        "candidate_feed_receipts",
                        "candidate_revision_history",
                        "task_shadow_observations",
                        "task_shadow_feed_cursors",
                        "task_shadow_feed_receipts",
                        "tasks",
                        "task_candidate_bindings",
                        "task_bootstrap_correlations",
                        "task_events",
                        "shadow_import_cycles",
                        "task_owner_equivalences",
                        "task_review_cards",
                        "task_review_card_events",
                        "task_execution_workflows",
                        "task_execution_results",
                        "task_execution_events",
                        "execution_review_cards",
                        "execution_review_card_events",
                    ),
                    columns=_SCHEMA_V11_COLUMNS,
                )
                connection.execute("PRAGMA foreign_keys = ON")
                connection.execute("BEGIN IMMEDIATE")
                try:
                    for statement in _SCHEMA_V10:
                        connection.execute(statement)
                    connection.execute("PRAGMA user_version = 10")
                    connection.commit()
                except Exception:
                    connection.rollback()
                    raise
                version = 10
            if version == 10:
                self._require_tables(
                    connection,
                    (
                        "tasks",
                        "task_execution_workflows",
                        "task_execution_results",
                        "task_execution_events",
                        "execution_review_cards",
                        "execution_review_card_events",
                        "native_candidate_intakes",
                        "native_candidate_intake_events",
                    ),
                    columns=_SCHEMA_V11_COLUMNS,
                )
                connection.execute("PRAGMA foreign_keys = ON")
                connection.execute("BEGIN IMMEDIATE")
                try:
                    for statement in _SCHEMA_V11:
                        connection.execute(statement)
                    connection.execute("PRAGMA user_version = 11")
                    connection.commit()
                except Exception:
                    connection.rollback()
                    raise
                version = 11
            if version == 11:
                self._require_tables(
                    connection,
                    (
                        "tasks",
                        "task_execution_workflows",
                        "task_execution_results",
                        "task_execution_events",
                        "execution_review_cards",
                        "execution_review_card_events",
                        "execution_reader_inputs",
                        "task_owner_events",
                    ),
                    columns=_SCHEMA_V11_COLUMNS,
                )
                connection.execute("PRAGMA foreign_keys = ON")
                connection.execute("BEGIN IMMEDIATE")
                try:
                    for statement in _SCHEMA_V12:
                        connection.execute(statement)
                    connection.execute("PRAGMA user_version = 12")
                    connection.commit()
                except Exception:
                    connection.rollback()
                    raise
                version = 12
            if version == 12:
                self._require_tables(
                    connection,
                    (
                        "tasks",
                        "task_execution_workflows",
                        "task_execution_results",
                        "task_execution_events",
                        "execution_review_cards",
                        "execution_review_card_events",
                    ),
                    columns=_SCHEMA_V14_COLUMNS,
                )
                connection.execute("PRAGMA foreign_keys = ON")
                connection.execute("BEGIN IMMEDIATE")
                try:
                    for statement in _SCHEMA_V13:
                        connection.execute(statement)
                    connection.execute("PRAGMA user_version = 13")
                    connection.commit()
                except Exception:
                    connection.rollback()
                    raise
                version = 13
            if version == 13:
                self._require_tables(
                    connection,
                    ("task_owner_equivalences",),
                    columns=_SCHEMA_V14_COLUMNS,
                )
                connection.execute("PRAGMA foreign_keys = ON")
                connection.execute("BEGIN IMMEDIATE")
                try:
                    for statement in _SCHEMA_V14:
                        connection.execute(statement)
                    connection.execute("PRAGMA user_version = 14")
                    connection.commit()
                except Exception:
                    connection.rollback()
                    raise
                version = 14
            if version == 14:
                self._require_tables(
                    connection,
                    tuple(_SCHEMA_V14_COLUMNS),
                    columns=_SCHEMA_V14_COLUMNS,
                    allow_appended_columns=True,
                )
                connection.execute("PRAGMA foreign_keys = ON")
                connection.execute("BEGIN IMMEDIATE")
                try:
                    for statement in _SCHEMA_V15:
                        connection.execute(statement)
                    connection.execute("PRAGMA user_version = 15")
                    connection.commit()
                except Exception:
                    connection.rollback()
                    raise
                version = 15
            if version == 15:
                self._require_tables(
                    connection,
                    tuple(_SCHEMA_V15_COLUMNS),
                    columns=_SCHEMA_V15_COLUMNS,
                    allow_appended_columns=True,
                )
                connection.execute("PRAGMA foreign_keys = ON")
                connection.execute("BEGIN IMMEDIATE")
                try:
                    for statement in _SCHEMA_V16:
                        connection.execute(statement)
                    connection.execute("PRAGMA user_version = 16")
                    connection.commit()
                except Exception:
                    connection.rollback()
                    raise
                version = 16
            if version == 16:
                self._require_tables(
                    connection,
                    tuple(_SCHEMA_V16_COLUMNS),
                    columns=_SCHEMA_V16_COLUMNS,
                    allow_appended_columns=True,
                )
                connection.execute("PRAGMA foreign_keys = ON")
                connection.execute("BEGIN IMMEDIATE")
                try:
                    for statement in _SCHEMA_V17:
                        connection.execute(statement)
                    connection.execute("PRAGMA user_version = 17")
                    connection.commit()
                except Exception:
                    connection.rollback()
                    raise
                version = 17
            if version == 17:
                self._require_tables(
                    connection,
                    tuple(_SCHEMA_V17_COLUMNS),
                    columns=_SCHEMA_V17_COLUMNS,
                    allow_appended_columns=True,
                )
                connection.execute("PRAGMA foreign_keys = ON")
                connection.execute("BEGIN IMMEDIATE")
                try:
                    for statement in _SCHEMA_V18:
                        connection.execute(statement)
                    connection.execute("PRAGMA user_version = 18")
                    connection.commit()
                except Exception:
                    connection.rollback()
                    raise
                version = 18
            if version == 18:
                self._require_tables(
                    connection,
                    tuple(_SCHEMA_V18_COLUMNS),
                    columns=_SCHEMA_V18_COLUMNS,
                    allow_appended_columns=True,
                )
                connection.execute("PRAGMA foreign_keys = ON")
                connection.execute("BEGIN IMMEDIATE")
                try:
                    for statement in _SCHEMA_V19:
                        connection.execute(statement)
                    connection.execute("PRAGMA user_version = 19")
                    connection.commit()
                except Exception:
                    connection.rollback()
                    raise
                version = 19
            if version == 19:
                self._require_tables(
                    connection,
                    ("execution_review_cards",),
                    columns=_SCHEMA_V18_COLUMNS,
                    allow_appended_columns=True,
                )
                # Off before the transaction, not inside it: the pragma is a
                # no-op once one is open, and the rebuild below drops a table
                # three others reference.
                connection.execute("PRAGMA foreign_keys = OFF")
                connection.execute("BEGIN IMMEDIATE")
                try:
                    for statement in _SCHEMA_V20:
                        connection.execute(statement)
                    connection.execute("PRAGMA user_version = 20")
                    connection.commit()
                except Exception:
                    connection.rollback()
                    raise
                finally:
                    connection.execute("PRAGMA foreign_keys = ON")
                orphans = connection.execute(
                    "PRAGMA foreign_key_check"
                ).fetchall()
                if orphans:
                    raise InboxError(
                        "candidate inbox schema is incomplete"
                    )
                version = 20
            if version == 20:
                # The default expectation is the version-14 map, which is
                # several tables and seven owner columns behind this point.
                self._require_tables(
                    connection, ("tasks",), columns=_SCHEMA_V20_COLUMNS,
                    allow_appended_columns=True)
                connection.execute("PRAGMA foreign_keys = ON")
                connection.execute("BEGIN IMMEDIATE")
                try:
                    for statement in _SCHEMA_V21:
                        connection.execute(statement)
                    connection.execute("PRAGMA user_version = 21")
                    connection.commit()
                except Exception:
                    connection.rollback()
                    raise
                version = 21
            if version == 21:
                connection.execute("BEGIN IMMEDIATE")
                try:
                    for statement in _SCHEMA_V22:
                        connection.execute(statement)
                    connection.execute("PRAGMA user_version = 22")
                    connection.commit()
                except Exception:
                    connection.rollback()
                    raise
                version = 22
            if version == 22:
                connection.execute("BEGIN IMMEDIATE")
                try:
                    for statement in _SCHEMA_V23:
                        connection.execute(statement)
                    connection.execute("PRAGMA user_version = 23")
                    connection.commit()
                except Exception:
                    connection.rollback()
                    raise
                version = 23
            if version == 23:
                connection.execute("BEGIN IMMEDIATE")
                try:
                    for statement in _SCHEMA_V24:
                        connection.execute(statement)
                    connection.execute("PRAGMA user_version = 24")
                    connection.commit()
                except Exception:
                    connection.rollback()
                    raise
                version = 24
            if version == 24:
                # The default expectation is the version-14 map, and this step
                # reads the card table it points a foreign key at.
                self._require_tables(
                    connection,
                    ("tasks", "task_review_cards"),
                    columns=_SCHEMA_V24_COLUMNS,
                    allow_appended_columns=True,
                )
                connection.execute("PRAGMA foreign_keys = ON")
                connection.execute("BEGIN IMMEDIATE")
                try:
                    for statement in _SCHEMA_V25:
                        connection.execute(statement)
                    connection.execute("PRAGMA user_version = 25")
                    connection.commit()
                except Exception:
                    connection.rollback()
                    raise
                version = 25
            if version == 25:
                self._require_tables(
                    connection,
                    ("task_review_cards",),
                    columns=_SCHEMA_V25_COLUMNS,
                    allow_appended_columns=True,
                )
                connection.execute("BEGIN IMMEDIATE")
                try:
                    for statement in _SCHEMA_V26:
                        connection.execute(statement)
                    connection.execute("PRAGMA user_version = 26")
                    connection.commit()
                except Exception:
                    connection.rollback()
                    raise
                version = 26
            if version == 26:
                self._require_tables(
                    connection,
                    ("tasks",),
                    columns=_SCHEMA_V26_COLUMNS,
                    allow_appended_columns=True,
                )
                connection.execute("PRAGMA foreign_keys = ON")
                connection.execute("BEGIN IMMEDIATE")
                try:
                    for statement in _SCHEMA_V27:
                        connection.execute(statement)
                    connection.execute("PRAGMA user_version = 27")
                    connection.commit()
                except Exception:
                    connection.rollback()
                    raise
                version = 27
            if version == 27:
                connection.execute("PRAGMA foreign_keys = ON")
                connection.execute("BEGIN IMMEDIATE")
                try:
                    columns = tuple(item["name"] for item in connection.execute(
                        "PRAGMA table_info(task_duplicate_proposals)"
                    ))
                    if "card_id" not in columns:
                        connection.execute(
                            "ALTER TABLE task_duplicate_proposals ADD COLUMN "
                            "card_id INTEGER REFERENCES task_review_cards(id);"
                        )
                    for statement in _SCHEMA_V28:
                        connection.execute(statement)
                    connection.execute("PRAGMA user_version = 28")
                    connection.commit()
                except Exception:
                    connection.rollback()
                    raise
                version = 28
            if version == 28:
                row = connection.execute(
                    "SELECT type FROM sqlite_master WHERE "
                    "name='task_execution_results'"
                ).fetchone()
                if row is None or row["type"] != "table":
                    raise InboxError("candidate inbox schema is incomplete")
                connection.execute("BEGIN IMMEDIATE")
                try:
                    columns = tuple(item["name"] for item in connection.execute(
                        "PRAGMA table_info(task_execution_results)"
                    ))
                    if "repository_references_json" not in columns:
                        for statement in _SCHEMA_V29:
                            connection.execute(statement)
                    connection.execute("PRAGMA user_version = 29")
                    connection.commit()
                except Exception:
                    connection.rollback()
                    raise
                version = 29
            if version == 29:
                connection.execute("PRAGMA foreign_keys = ON")
                connection.execute("BEGIN IMMEDIATE")
                try:
                    columns = tuple(row[1] for row in connection.execute(
                        "PRAGMA table_info(execution_review_cards)"
                    )) if connection.execute(
                        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='execution_review_cards'"
                    ).fetchone() else ()
                    if columns and "consumer_digest" not in columns:
                        for statement in _SCHEMA_V30:
                            connection.execute(statement)
                    connection.execute("PRAGMA user_version = 30")
                    connection.commit()
                except Exception:
                    connection.rollback()
                    raise
                version = 30
            if version == 30:
                row = connection.execute(
                    "SELECT type FROM sqlite_master WHERE "
                    "name='task_execution_results'"
                ).fetchone()
                if row is None or row["type"] != "table":
                    raise InboxError("candidate inbox schema is incomplete")
                connection.execute("BEGIN IMMEDIATE")
                try:
                    columns = tuple(item["name"] for item in connection.execute(
                        "PRAGMA table_info(task_execution_results)"
                    ))
                    if "repository_impact" not in columns:
                        for statement in _SCHEMA_V31:
                            connection.execute(statement)
                    connection.execute("PRAGMA user_version = 31")
                    connection.commit()
                except Exception:
                    connection.rollback()
                    raise
                version = 31
            if version == 31:
                self._require_tables(
                    connection,
                    ("task_execution_workflows", "task_execution_events"),
                    columns=_SCHEMA_V31_COLUMNS,
                    allow_appended_columns=True,
                )
                connection.execute("PRAGMA foreign_keys = ON")
                connection.execute("BEGIN IMMEDIATE")
                try:
                    columns = tuple(item["name"] for item in connection.execute(
                        "PRAGMA table_info(task_execution_workflows)"
                    ))
                    if "queue_priority" not in columns:
                        for statement in _SCHEMA_V32:
                            connection.execute(statement)
                    connection.execute("PRAGMA user_version = 32")
                    connection.commit()
                except Exception:
                    connection.rollback()
                    raise
                version = 32
            if version == 32:
                connection.execute("PRAGMA foreign_keys = ON")
                connection.execute("BEGIN IMMEDIATE")
                try:
                    row = connection.execute(
                        "SELECT type FROM sqlite_master WHERE "
                        "name='task_fused_title_jobs'"
                    ).fetchone()
                    if row is None:
                        for statement in _SCHEMA_V33:
                            connection.execute(statement)
                    elif row["type"] != "table":
                        raise InboxError("candidate inbox schema is incomplete")
                    connection.execute("PRAGMA user_version = 33")
                    connection.commit()
                except Exception:
                    connection.rollback()
                    raise
                version = 33
            if version == 33:
                self._require_tables(
                    connection,
                    ("task_execution_workflows",),
                    columns=_SCHEMA_V32_COLUMNS,
                    allow_appended_columns=True,
                )
                connection.execute("BEGIN IMMEDIATE")
                try:
                    columns = tuple(item["name"] for item in connection.execute(
                        "PRAGMA table_info(task_execution_workflows)"
                    ))
                    if "last_failure_exit_code" not in columns:
                        for statement in _SCHEMA_V34:
                            connection.execute(statement)
                    connection.execute("PRAGMA user_version = 34")
                    connection.commit()
                except Exception:
                    connection.rollback()
                    raise
                version = 34
            if version == 34:
                self._require_tables(
                    connection,
                    ("execution_review_cards",),
                    columns=_SCHEMA_V34_COLUMNS,
                    allow_appended_columns=True,
                )
                connection.execute("BEGIN IMMEDIATE")
                try:
                    columns = tuple(item["name"] for item in connection.execute(
                        "PRAGMA table_info(execution_review_cards)"
                    ))
                    if "superseded_delivery_ref" not in columns:
                        for statement in _SCHEMA_V35:
                            connection.execute(statement)
                    connection.execute("PRAGMA user_version = 35")
                    connection.commit()
                except Exception:
                    connection.rollback()
                    raise
                version = 35
            if version == 35:
                self._require_tables(connection, ("task_execution_events",))
                connection.execute("BEGIN IMMEDIATE")
                try:
                    definition = connection.execute(
                        "SELECT sql FROM sqlite_master WHERE type='table' "
                        "AND name='task_execution_events'"
                    ).fetchone()
                    if definition is None:
                        raise InboxError(
                            "candidate inbox schema is incomplete")
                    if "'phase_granted'" not in definition["sql"]:
                        for statement in _SCHEMA_V36:
                            connection.execute(statement)
                    connection.execute("PRAGMA user_version = 36")
                    connection.commit()
                except Exception:
                    connection.rollback()
                    raise
                version = 36
            if version == 36:
                connection.execute("BEGIN IMMEDIATE")
                try:
                    for statement in _SCHEMA_V37:
                        connection.execute(statement)
                    connection.execute("PRAGMA user_version = 37")
                    connection.commit()
                except Exception:
                    connection.rollback()
                    raise
                version = 37
            if version == 37:
                connection.execute("PRAGMA foreign_keys = OFF")
                connection.execute("BEGIN IMMEDIATE")
                try:
                    for statement in _SCHEMA_V38_DROP:
                        connection.execute(statement)
                    columns = tuple(item["name"] for item in connection.execute(
                        "PRAGMA table_info(work_revisions)"
                    ))
                    if "kind" not in columns:
                        for statement in _SCHEMA_V38_REBUILD:
                            connection.execute(statement)
                    for statement in _SCHEMA_V38_TRIGGER:
                        connection.execute(statement)
                    connection.execute("PRAGMA user_version = 38")
                    connection.commit()
                except Exception:
                    connection.rollback()
                    raise
                finally:
                    connection.execute("PRAGMA foreign_keys = ON")
                version = 38
            if version == 38:
                connection.execute("PRAGMA foreign_keys = ON")
                connection.execute("BEGIN IMMEDIATE")
                try:
                    columns = tuple(item["name"] for item in connection.execute(
                        "PRAGMA table_info(execution_review_cards)"
                    ))
                    if "work_revision_id" not in columns:
                        connection.execute(
                            "ALTER TABLE execution_review_cards ADD COLUMN "
                            "work_revision_id INTEGER REFERENCES "
                            "work_revisions(id);"
                        )
                    connection.execute("PRAGMA user_version = 39")
                    connection.commit()
                except Exception:
                    connection.rollback()
                    raise
                version = 39
            if version == 39:
                connection.execute("BEGIN IMMEDIATE")
                try:
                    definition = connection.execute(
                        "SELECT sql FROM sqlite_master WHERE type='table' "
                        "AND name='task_execution_events'"
                    ).fetchone()
                    if definition is None:
                        raise InboxError("candidate inbox schema is incomplete")
                    # Replayable: a database already carrying the widened
                    # CHECK must not be rebuilt a second time.
                    if "'result_unchanged'" not in definition["sql"]:
                        for statement in _SCHEMA_V40:
                            connection.execute(statement)
                    connection.execute("PRAGMA user_version = 40")
                    connection.commit()
                except Exception:
                    connection.rollback()
                    raise
                version = 40
            if version == 40:
                connection.execute("PRAGMA foreign_keys = ON")
                connection.execute("BEGIN IMMEDIATE")
                try:
                    for statement in _SCHEMA_V41:
                        connection.execute(statement)
                    connection.execute("PRAGMA user_version = 41")
                    connection.commit()
                except Exception:
                    connection.rollback()
                    raise
                version = 41
            if version == 41:
                connection.execute("PRAGMA foreign_keys = OFF")
                connection.execute("BEGIN IMMEDIATE")
                try:
                    columns = tuple(item["name"] for item in connection.execute(
                        "PRAGMA table_info(task_execution_results)"
                    ))
                    # Replayable: a database that already carries the column,
                    # the table or the widened CHECK must not be rebuilt.
                    if "reader_instruction_sequence" not in columns:
                        connection.execute(_SCHEMA_V42[0])
                    connection.execute(
                        _SCHEMA_V42[1].replace(
                            "CREATE TABLE", "CREATE TABLE IF NOT EXISTS", 1))
                    definition = connection.execute(
                        "SELECT sql FROM sqlite_master WHERE type='table' "
                        "AND name='task_execution_events'"
                    ).fetchone()
                    if definition is None:
                        raise InboxError("candidate inbox schema is incomplete")
                    widened = "'reader_instruction_delivered'"
                    if widened not in definition["sql"]:
                        for statement in _SCHEMA_V42[2:]:
                            connection.execute(statement)
                    connection.execute("PRAGMA user_version = 42")
                    connection.commit()
                except Exception:
                    connection.rollback()
                    raise
                finally:
                    connection.execute("PRAGMA foreign_keys = ON")
                version = 42
            if version == 42:
                connection.execute("PRAGMA foreign_keys = ON")
                connection.execute("BEGIN IMMEDIATE")
                try:
                    for statement in _SCHEMA_V43:
                        connection.execute(statement)
                    connection.execute("PRAGMA user_version = 43")
                    connection.commit()
                except Exception:
                    connection.rollback()
                    raise
                version = 43
            if version == 43:
                connection.execute("PRAGMA foreign_keys = OFF")
                # `task_duplicate_proposal_events` has a foreign key to the
                # table being rebuilt. Without this, RENAME helpfully rewrites
                # that reference to point at the temporary name, and dropping
                # the temporary table then leaves the events table referring
                # to something that no longer exists.
                connection.execute("PRAGMA legacy_alter_table = ON")
                connection.execute("BEGIN IMMEDIATE")
                try:
                    definition = connection.execute(
                        "SELECT sql FROM sqlite_master WHERE type='table' "
                        "AND name='task_duplicate_proposals'"
                    ).fetchone()
                    if definition is None:
                        raise InboxError("candidate inbox schema is incomplete")
                    # Replayable: a database already carrying the widened
                    # CHECK must not be rebuilt a second time.
                    if "'superseded'" not in definition["sql"]:
                        for statement in _SCHEMA_V44:
                            connection.execute(statement)
                    connection.execute("PRAGMA user_version = 44")
                    connection.commit()
                except Exception:
                    connection.rollback()
                    raise
                finally:
                    connection.execute("PRAGMA legacy_alter_table = OFF")
                    connection.execute("PRAGMA foreign_keys = ON")
                version = 44
            if version == 44:
                connection.execute("PRAGMA foreign_keys = ON")
                connection.execute("BEGIN IMMEDIATE")
                try:
                    columns = {
                        row["name"] for row in connection.execute(
                            "PRAGMA table_info(tasks)"
                        )
                    }
                    for name, statement in zip(
                        ("object", "action", "confidence"), _SCHEMA_V45[:3]
                    ):
                        if name not in columns:
                            connection.execute(statement)
                    connection.execute(
                        _SCHEMA_V45[3].replace(
                            "CREATE TABLE", "CREATE TABLE IF NOT EXISTS", 1
                        )
                    )
                    connection.execute(
                        _SCHEMA_V45[4].replace(
                            "CREATE TABLE", "CREATE TABLE IF NOT EXISTS", 1
                        )
                    )
                    connection.execute(
                        _SCHEMA_V45[5].replace(
                            "CREATE TABLE", "CREATE TABLE IF NOT EXISTS", 1
                        )
                    )
                    connection.execute(_SCHEMA_V45[6])
                    connection.execute("PRAGMA user_version = 45")
                    connection.commit()
                except Exception:
                    connection.rollback()
                    raise
                version = 45
            if version == 45:
                connection.execute("BEGIN IMMEDIATE")
                try:
                    connection.execute(_SCHEMA_V46[0])
                    connection.execute("PRAGMA user_version = 46")
                    connection.commit()
                except Exception:
                    connection.rollback()
                    raise
                version = 46
            if version == 46:
                connection.execute("BEGIN IMMEDIATE")
                try:
                    connection.execute(_SCHEMA_V47[0])
                    connection.execute("PRAGMA user_version = 47")
                    connection.commit()
                except Exception:
                    connection.rollback()
                    raise
                version = 47
            if version == 47:
                connection.execute("PRAGMA foreign_keys = OFF")
                connection.execute("PRAGMA legacy_alter_table = ON")
                connection.execute("BEGIN IMMEDIATE")
                try:
                    definition = connection.execute(
                        "SELECT sql FROM sqlite_master WHERE type='table' "
                        "AND name='task_execution_workflows'"
                    ).fetchone()
                    if definition is None:
                        raise InboxError("candidate inbox schema is incomplete")
                    workflow_sql = str(definition["sql"])
                    if "'context_exhausted'" not in workflow_sql:
                        widened = workflow_sql.replace(
                            "'result_invalid'", _CONTEXT_EXHAUSTED_REASON,
                        )
                        if widened == workflow_sql:
                            raise InboxError(
                                "candidate inbox schema is incomplete"
                            )
                        connection.execute(
                            "DROP INDEX task_execution_workflows_ready"
                        )
                        connection.execute(
                            "DROP INDEX task_execution_workflows_priority_ready"
                        )
                        connection.execute(
                            "ALTER TABLE task_execution_workflows RENAME TO "
                            "task_execution_workflows_v47"
                        )
                        connection.execute(widened)
                        connection.execute(
                            "INSERT INTO task_execution_workflows "
                            "SELECT * FROM task_execution_workflows_v47"
                        )
                        connection.execute("DROP TABLE task_execution_workflows_v47")
                        connection.execute(_SCHEMA_V8[1])
                        connection.execute(_SCHEMA_V32[1])
                    connection.execute("PRAGMA user_version = 48")
                    connection.commit()
                except Exception:
                    connection.rollback()
                    raise
                finally:
                    connection.execute("PRAGMA legacy_alter_table = OFF")
                    connection.execute("PRAGMA foreign_keys = ON")
                version = 48
            if version == 48:
                connection.execute("PRAGMA foreign_keys = OFF")
                connection.execute("PRAGMA legacy_alter_table = ON")
                connection.execute("BEGIN IMMEDIATE")
                try:
                    workflow_columns = {row["name"] for row in connection.execute(
                        "PRAGMA table_info(task_execution_workflows)"
                    )}
                    if "steer_while_running" not in workflow_columns:
                        connection.execute(
                            "ALTER TABLE task_execution_workflows ADD COLUMN "
                            "steer_while_running INTEGER NOT NULL DEFAULT 0 "
                            "CHECK(steer_while_running IN (0,1))"
                        )
                    if "current_run_id" not in workflow_columns:
                        connection.execute(
                            "ALTER TABLE task_execution_workflows ADD COLUMN "
                            "current_run_id TEXT CHECK(current_run_id IS NULL OR "
                            "(length(current_run_id)=32 AND "
                            "current_run_id GLOB '[0-9a-f]*'))"
                        )
                    card_columns = {row["name"] for row in connection.execute(
                        "PRAGMA table_info(execution_review_cards)"
                    )}
                    if "steer_digest" not in card_columns:
                        connection.execute(
                            "DROP INDEX execution_review_cards_one_active"
                        )
                        connection.execute(
                            "ALTER TABLE execution_review_cards RENAME TO "
                            "execution_review_cards_v48"
                        )
                        connection.execute(_SCHEMA_V49_CARD_TABLE)
                        connection.execute(
                            "INSERT INTO execution_review_cards("
                        "id,task_id,task_version,workflow_version,kind,phase,"
                        "result_id,status,version,claim_token_digest,"
                        "claim_expires_at,transport,delivery_ref,delivered_at,"
                        "resolution,created_at,updated_at,resolved_at,"
                        "consumer_digest,superseded_delivery_ref,"
                        "superseded_transport,work_revision_id) "
                        "SELECT id,task_id,task_version,workflow_version,kind,"
                        "phase,result_id,status,version,claim_token_digest,"
                        "claim_expires_at,transport,delivery_ref,delivered_at,"
                        "resolution,created_at,updated_at,resolved_at,"
                        "consumer_digest,superseded_delivery_ref,"
                        "superseded_transport,work_revision_id "
                            "FROM execution_review_cards_v48"
                        )
                        connection.execute("DROP TABLE execution_review_cards_v48")
                        connection.execute(_SCHEMA_V9[1])
                    connection.execute("PRAGMA user_version = 49")
                    connection.commit()
                except Exception:
                    connection.rollback()
                    raise
                finally:
                    connection.execute("PRAGMA legacy_alter_table = OFF")
                    connection.execute("PRAGMA foreign_keys = ON")
                version = 49
            if version == 49:
                connection.execute("BEGIN IMMEDIATE")
                try:
                    for statement in _SCHEMA_V50:
                        connection.execute(statement)
                    connection.execute("PRAGMA user_version = 50")
                    connection.commit()
                except Exception:
                    connection.rollback()
                    raise
                version = 50
            if version == 50:
                connection.execute("BEGIN IMMEDIATE")
                try:
                    connection.execute(_SCHEMA_V51[0])
                    connection.execute("PRAGMA user_version = 51")
                    connection.commit()
                except Exception:
                    connection.rollback()
                    raise
                version = 51
            if version == 51:
                connection.execute("PRAGMA foreign_keys = OFF")
                connection.execute("PRAGMA legacy_alter_table = ON")
                connection.execute("BEGIN IMMEDIATE")
                try:
                    for statement in _SCHEMA_V52:
                        connection.execute(statement)
                    connection.execute("PRAGMA user_version = 52")
                    connection.commit()
                except Exception:
                    connection.rollback()
                    raise
                finally:
                    connection.execute("PRAGMA legacy_alter_table = OFF")
                    connection.execute("PRAGMA foreign_keys = ON")
                version = 52
            if version == 52:
                connection.execute("PRAGMA foreign_keys = OFF")
                connection.execute("PRAGMA legacy_alter_table = ON")
                connection.execute("BEGIN IMMEDIATE")
                try:
                    for statement in _SCHEMA_V53:
                        connection.execute(statement)
                    connection.execute("PRAGMA user_version = 53")
                    connection.commit()
                except Exception:
                    connection.rollback()
                    raise
                finally:
                    connection.execute("PRAGMA legacy_alter_table = OFF")
                    connection.execute("PRAGMA foreign_keys = ON")
                version = 53
            if version == 53:
                connection.execute("BEGIN IMMEDIATE")
                try:
                    columns = {
                        row["name"] for row in connection.execute(
                            "PRAGMA table_info(execution_review_cards)"
                        )
                    }
                    # Migration rehearsals may keep a later additive column
                    # while resetting user_version, so this upgrade stays
                    # replayable exactly as its additive predecessors are.
                    if "summary_only" not in columns:
                        connection.execute(_SCHEMA_V54[0])
                    for statement in _SCHEMA_V54[1:]:
                        connection.execute(statement)
                    connection.execute("PRAGMA user_version = 54")
                    connection.commit()
                except Exception:
                    connection.rollback()
                    raise
                version = 54
            if version == 54:
                connection.execute("BEGIN IMMEDIATE")
                try:
                    for statement in _SCHEMA_V55:
                        connection.execute(statement)
                    connection.execute("PRAGMA user_version = 55")
                    connection.commit()
                except Exception:
                    connection.rollback()
                    raise
                version = 55
            if version == 55:
                connection.execute("BEGIN IMMEDIATE")
                try:
                    columns = {
                        row["name"] for row in connection.execute(
                            "PRAGMA table_info(task_review_cards)"
                        )
                    }
                    if "claiming_consumer" not in columns:
                        connection.execute(_SCHEMA_V56[0])
                    connection.execute("PRAGMA user_version = 56")
                    connection.commit()
                except Exception:
                    connection.rollback()
                    raise
                version = 56
            if version == 56:
                connection.execute("BEGIN IMMEDIATE")
                try:
                    for statement in _SCHEMA_V57:
                        connection.execute(statement)
                    connection.execute("PRAGMA user_version = 57")
                    connection.commit()
                except Exception:
                    connection.rollback()
                    raise
                version = 57
            if version == 57:
                connection.execute("PRAGMA foreign_keys = ON")
                connection.execute("BEGIN IMMEDIATE")
                try:
                    columns = {
                        row["name"] for row in connection.execute(
                            "PRAGMA table_info(work_revisions)"
                        )
                    }
                    for column, statement in zip(
                        _SCHEMA_V58_COLUMNS, _SCHEMA_V58[:5], strict=True
                    ):
                        if column not in columns:
                            connection.execute(statement)
                    for statement in _SCHEMA_V58[5:]:
                        connection.execute(statement)
                    connection.execute("PRAGMA user_version = 58")
                    connection.commit()
                except Exception:
                    connection.rollback()
                    raise
                version = 58
            if version == 58:
                connection.execute("PRAGMA foreign_keys = OFF")
                connection.execute("PRAGMA legacy_alter_table = ON")
                connection.execute("BEGIN IMMEDIATE")
                try:
                    for statement in _SCHEMA_V59:
                        connection.execute(statement)
                    connection.execute("PRAGMA user_version = 59")
                    connection.commit()
                except Exception:
                    connection.rollback()
                    raise
                finally:
                    connection.execute("PRAGMA legacy_alter_table = OFF")
                    connection.execute("PRAGMA foreign_keys = ON")
                version = 59
            if version == 59:
                connection.execute("PRAGMA foreign_keys = OFF")
                connection.execute("PRAGMA legacy_alter_table = ON")
                connection.execute("BEGIN IMMEDIATE")
                try:
                    for statement in _SCHEMA_V60:
                        connection.execute(statement)
                    connection.execute("PRAGMA user_version = 60")
                    connection.commit()
                except Exception:
                    connection.rollback()
                    raise
                finally:
                    connection.execute("PRAGMA legacy_alter_table = OFF")
                    connection.execute("PRAGMA foreign_keys = ON")
                version = 60
            self._require_schema(connection)

    def import_document(self, document: object) -> ImportResult:
        """Validate and transactionally insert, replay, or update a candidate."""
        try:
            candidate = parse_task_candidate(document)
        except ContractError:
            return ImportResult(
                ImportDisposition.REFUSED,
                ImportRefusal.INVALID_CONTRACT,
            )

        payload = _canonical_payload(candidate)
        imported_at = self._now()
        with closing(self._connect()) as connection:
            self._require_current_schema(connection)
            connection.execute("BEGIN IMMEDIATE")
            try:
                result = self._apply_candidate(
                    connection, candidate, payload, imported_at)
                if result.accepted:
                    connection.commit()
                else:
                    connection.rollback()
                return result
            except Exception:
                connection.rollback()
                raise

    def import_feed(self, document: object) -> FeedImportResult:
        """Validate and atomically import one ordered candidate-feed page."""
        try:
            feed = parse_candidate_feed(document)
        except FeedContractError:
            return FeedImportResult(
                FeedImportDisposition.REFUSED,
                refusal=FeedImportRefusal.INVALID_CONTRACT,
            )

        page_digest = _feed_digest(feed)
        imported_at = self._now()
        with closing(self._connect()) as connection:
            self._require_current_schema(connection)
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = connection.execute(
                    "SELECT cursor FROM candidate_feed_cursors "
                    "WHERE producer=? AND stream_id=?",
                    (feed.producer, feed.stream_id),
                ).fetchone()
                current_cursor = 0 if row is None else int(row["cursor"])

                if feed.from_cursor < current_cursor:
                    receipt = connection.execute(
                        "SELECT page_digest FROM candidate_feed_receipts "
                        "WHERE producer=? AND stream_id=? AND from_cursor=? "
                        "AND to_cursor=?",
                        (feed.producer, feed.stream_id, feed.from_cursor,
                         feed.to_cursor),
                    ).fetchone()
                    connection.rollback()
                    if receipt is None:
                        return FeedImportResult(
                            FeedImportDisposition.REFUSED,
                            refusal=FeedImportRefusal.CURSOR_OVERLAP,
                        )
                    if receipt["page_digest"] != page_digest:
                        return FeedImportResult(
                            FeedImportDisposition.REFUSED,
                            refusal=FeedImportRefusal.CURSOR_REUSE,
                        )
                    return FeedImportResult(FeedImportDisposition.REPLAYED)

                if feed.from_cursor > current_cursor:
                    connection.rollback()
                    return FeedImportResult(
                        FeedImportDisposition.REFUSED,
                        refusal=FeedImportRefusal.CURSOR_GAP,
                    )

                if feed.from_cursor == feed.to_cursor:
                    connection.rollback()
                    return FeedImportResult(FeedImportDisposition.EMPTY)

                counts = {
                    ImportDisposition.INSERTED: 0,
                    ImportDisposition.UPDATED: 0,
                    ImportDisposition.UNCHANGED: 0,
                }
                for item in feed.items:
                    payload = _canonical_payload(item.candidate)
                    result = self._apply_candidate(
                        connection, item.candidate, payload, imported_at)
                    if not result.accepted:
                        connection.rollback()
                        return FeedImportResult(
                            FeedImportDisposition.REFUSED,
                            refusal=FeedImportRefusal.CANDIDATE_CONFLICT,
                        )
                    counts[result.disposition] += 1
                    connection.execute(
                        "INSERT INTO candidate_feed_items("
                        "producer,stream_id,sequence,candidate_id,"
                        "source_revision,imported_at) VALUES(?,?,?,?,?,?)",
                        (
                            feed.producer,
                            feed.stream_id,
                            item.sequence,
                            item.candidate.candidate_id,
                            item.candidate.source.revision,
                            imported_at,
                        ),
                    )

                connection.execute(
                    "INSERT INTO candidate_feed_receipts("
                    "producer,stream_id,from_cursor,to_cursor,page_digest,"
                    "imported_at) VALUES(?,?,?,?,?,?)",
                    (feed.producer, feed.stream_id, feed.from_cursor,
                     feed.to_cursor, page_digest, imported_at),
                )
                connection.execute(
                    "INSERT INTO candidate_feed_cursors("
                    "producer,stream_id,cursor,updated_at) VALUES(?,?,?,?) "
                    "ON CONFLICT(producer,stream_id) DO UPDATE SET "
                    "cursor=excluded.cursor,updated_at=excluded.updated_at",
                    (feed.producer, feed.stream_id, feed.to_cursor,
                     imported_at),
                )
                connection.commit()
                return FeedImportResult(
                    FeedImportDisposition.APPLIED,
                    inserted=counts[ImportDisposition.INSERTED],
                    updated=counts[ImportDisposition.UPDATED],
                    unchanged=counts[ImportDisposition.UNCHANGED],
                )
            except Exception:
                connection.rollback()
                raise

    def import_shadow_feed(self, document: object) -> ShadowFeedImportResult:
        """Validate and atomically import one ordered observation page."""
        try:
            feed = parse_task_shadow_feed(document)
        except ShadowFeedContractError:
            return ShadowFeedImportResult(
                ShadowFeedImportDisposition.REFUSED,
                refusal=ShadowFeedImportRefusal.INVALID_CONTRACT,
            )

        page_digest = _shadow_feed_digest(feed)
        imported_at = self._now()
        with closing(self._connect()) as connection:
            self._require_current_schema(connection)
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = connection.execute(
                    "SELECT cursor FROM task_shadow_feed_cursors "
                    "WHERE producer=? AND stream_id=?",
                    (feed.producer, feed.stream_id),
                ).fetchone()
                current_cursor = 0 if row is None else int(row["cursor"])

                if feed.from_cursor < current_cursor:
                    receipt = connection.execute(
                        "SELECT page_digest FROM task_shadow_feed_receipts "
                        "WHERE producer=? AND stream_id=? AND from_cursor=? "
                        "AND to_cursor=?",
                        (feed.producer, feed.stream_id, feed.from_cursor,
                         feed.to_cursor),
                    ).fetchone()
                    connection.rollback()
                    if receipt is None:
                        return ShadowFeedImportResult(
                            ShadowFeedImportDisposition.REFUSED,
                            refusal=ShadowFeedImportRefusal.CURSOR_OVERLAP,
                        )
                    if receipt["page_digest"] != page_digest:
                        return ShadowFeedImportResult(
                            ShadowFeedImportDisposition.REFUSED,
                            refusal=ShadowFeedImportRefusal.CURSOR_REUSE,
                        )
                    return ShadowFeedImportResult(
                        ShadowFeedImportDisposition.REPLAYED
                    )

                if feed.from_cursor > current_cursor:
                    connection.rollback()
                    return ShadowFeedImportResult(
                        ShadowFeedImportDisposition.REFUSED,
                        refusal=ShadowFeedImportRefusal.CURSOR_GAP,
                    )

                if feed.from_cursor == feed.to_cursor:
                    connection.rollback()
                    return ShadowFeedImportResult(
                        ShadowFeedImportDisposition.EMPTY
                    )

                inserted = 0
                unchanged = 0
                for item in feed.items:
                    was_inserted, refusal = self._apply_shadow_observation(
                        connection, item.observation, imported_at
                    )
                    if refusal is not None:
                        connection.rollback()
                        return ShadowFeedImportResult(
                            ShadowFeedImportDisposition.REFUSED,
                            refusal=refusal,
                        )
                    if was_inserted:
                        inserted += 1
                    else:
                        unchanged += 1

                connection.execute(
                    "INSERT INTO task_shadow_feed_receipts("
                    "producer,stream_id,from_cursor,to_cursor,page_digest,"
                    "imported_at) VALUES(?,?,?,?,?,?)",
                    (feed.producer, feed.stream_id, feed.from_cursor,
                     feed.to_cursor, page_digest, imported_at),
                )
                connection.execute(
                    "INSERT INTO task_shadow_feed_cursors("
                    "producer,stream_id,cursor,updated_at) VALUES(?,?,?,?) "
                    "ON CONFLICT(producer,stream_id) DO UPDATE SET "
                    "cursor=excluded.cursor,updated_at=excluded.updated_at",
                    (feed.producer, feed.stream_id, feed.to_cursor,
                     imported_at),
                )
                connection.commit()
                return ShadowFeedImportResult(
                    ShadowFeedImportDisposition.APPLIED,
                    inserted=inserted,
                    unchanged=unchanged,
                )
            except Exception:
                connection.rollback()
                raise

    def shadow_feed_cursor(self, producer: str, stream_id: str) -> int:
        """Return one content-free observation cursor, or zero initially."""
        with closing(self._connect()) as connection:
            self._require_current_schema(connection)
            row = connection.execute(
                "SELECT cursor FROM task_shadow_feed_cursors "
                "WHERE producer=? AND stream_id=?",
                (producer, stream_id),
            ).fetchone()
            return 0 if row is None else int(row["cursor"])

    def shadow_report(self) -> ShadowComparisonReport:
        """Return aggregate comparison counts without candidate content."""
        with closing(self._connect()) as connection:
            self._require_current_schema(connection)
            rows = connection.execute(
                "SELECT comparison,COUNT(*) AS total "
                "FROM task_shadow_observations GROUP BY comparison"
            ).fetchall()
        counts = {row["comparison"]: int(row["total"]) for row in rows}
        if set(counts) - {"agreed", "divergent", "refused", "unmapped"}:
            raise InboxError("task shadow observations contain invalid state")
        return ShadowComparisonReport(
            total=sum(counts.values()),
            agreed=counts.get("agreed", 0),
            divergent=counts.get("divergent", 0),
            refused=counts.get("refused", 0),
            unmapped=counts.get("unmapped", 0),
        )

    def feed_cursor(self, producer: str, stream_id: str) -> int:
        """Return one content-free feed cursor, or zero before first import."""
        with closing(self._connect()) as connection:
            self._require_current_schema(connection)
            row = connection.execute(
                "SELECT cursor FROM candidate_feed_cursors "
                "WHERE producer=? AND stream_id=?",
                (producer, stream_id),
            ).fetchone()
            return 0 if row is None else int(row["cursor"])

    def append_shadow_import_cycle(
        self, receipt: ShadowImportCycleReceipt
    ) -> int:
        """Append one validated success receipt and return its sequence."""
        _validate_shadow_cycle_receipt(receipt)
        with closing(self._connect()) as connection:
            self._require_current_schema(connection)
            connection.execute("BEGIN IMMEDIATE")
            try:
                report = receipt.comparison
                cursor = connection.execute(
                    "INSERT INTO shadow_import_cycles("
                    "stream_id,started_at,completed_at,"
                    "candidate_previous_cursor,candidate_current_cursor,"
                    "candidates_inserted,candidates_updated,"
                    "observation_previous_cursor,observation_current_cursor,"
                    "observations_inserted,comparison_total,"
                    "comparison_agreed,comparison_divergent,"
                    "comparison_refused,comparison_unmapped) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        receipt.stream_id,
                        receipt.started_at,
                        receipt.completed_at,
                        receipt.candidate_previous_cursor,
                        receipt.candidate_current_cursor,
                        receipt.candidates_inserted,
                        receipt.candidates_updated,
                        receipt.observation_previous_cursor,
                        receipt.observation_current_cursor,
                        receipt.observations_inserted,
                        report.total,
                        report.agreed,
                        report.divergent,
                        report.refused,
                        report.unmapped,
                    ),
                )
                sequence = int(cursor.lastrowid)
                connection.commit()
                return sequence
            except Exception:
                connection.rollback()
                raise

    def get(self, candidate_id: str) -> TaskCandidate | None:
        """Return one validated stored candidate for internal application use."""
        with closing(self._connect()) as connection:
            self._require_current_schema(connection)
            row = connection.execute(
                "SELECT payload_json FROM candidate_inbox WHERE candidate_id=?",
                (candidate_id,),
            ).fetchone()
        if row is None:
            return None
        try:
            document = json.loads(row["payload_json"])
            return parse_task_candidate(document)
        except (json.JSONDecodeError, ContractError) as exc:
            raise InboxError(
                "candidate inbox contains an invalid stored payload"
            ) from exc

    def count(self) -> int:
        """Return a content-free candidate count."""
        with closing(self._connect()) as connection:
            self._require_current_schema(connection)
            row = connection.execute(
                "SELECT COUNT(*) AS total FROM candidate_inbox"
            ).fetchone()
            return int(row["total"])

    @staticmethod
    def _apply_shadow_observation(
        connection: sqlite3.Connection,
        observation: TaskShadowObservation,
        imported_at: str,
    ) -> tuple[bool, ShadowFeedImportRefusal | None]:
        candidate = observation.candidate
        candidate_payload = _canonical_payload(candidate)
        revision = connection.execute(
            "SELECT payload_json FROM candidate_revision_history "
            "WHERE candidate_id=? AND source_revision=?",
            (candidate.candidate_id, candidate.source.revision),
        ).fetchone()
        if revision is None:
            return False, ShadowFeedImportRefusal.CANDIDATE_MISSING
        if revision["payload_json"] != candidate_payload:
            return False, ShadowFeedImportRefusal.CANDIDATE_CONFLICT

        payload = _canonical_shadow_observation(observation)
        existing = connection.execute(
            "SELECT payload_json FROM task_shadow_observations "
            "WHERE candidate_id=? AND source_revision=?",
            (candidate.candidate_id, candidate.source.revision),
        ).fetchone()
        if existing is not None:
            if existing["payload_json"] == payload:
                return False, None
            return False, ShadowFeedImportRefusal.OBSERVATION_CONFLICT

        legacy = observation.legacy_task
        if observation.disposition in {"minted", "folded"}:
            comparison = (
                "agreed"
                if legacy.comparable_digest
                == candidate_comparable_digest(candidate)
                else "divergent"
            )
        else:
            comparison = observation.disposition
        connection.execute(
            "INSERT INTO task_shadow_observations("
            "candidate_id,source_revision,disposition,legacy_task_id,"
            "comparable_digest,reason_code,comparison,payload_json,"
            "observed_at,first_imported_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
            (
                candidate.candidate_id,
                candidate.source.revision,
                observation.disposition,
                None if legacy is None else legacy.task_id,
                None if legacy is None else legacy.comparable_digest,
                observation.reason_code,
                comparison,
                payload,
                observation.observed_at,
                imported_at,
            ),
        )
        return True, None

    @staticmethod
    def _apply_candidate(connection: sqlite3.Connection,
                         candidate: TaskCandidate, payload: str,
                         imported_at: str) -> ImportResult:
        row = connection.execute(
            "SELECT c.source_revision,c.payload_json,c.created_at,"
            "l.state AS lifecycle_state,l.generation AS lifecycle_generation,"
            "l.changed_at AS lifecycle_changed_at "
            "FROM candidate_inbox AS c LEFT JOIN candidate_lifecycle AS l "
            "ON l.candidate_id=c.candidate_id WHERE c.candidate_id=?",
            (candidate.candidate_id,),
        ).fetchone()
        history = connection.execute(
            "SELECT payload_json,created_at FROM candidate_revision_history "
            "WHERE candidate_id=? AND source_revision=?",
            (candidate.candidate_id, candidate.source.revision),
        ).fetchone()
        if row is None:
            if history is not None:
                raise InboxError("candidate revision history is inconsistent")
            connection.execute(
                "INSERT INTO candidate_inbox("
                "candidate_id,source_system,source_kind,source_record_id,"
                "source_item_id,source_revision,payload_json,created_at,"
                "first_imported_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
                (
                    candidate.candidate_id,
                    candidate.source.system,
                    candidate.source.kind,
                    candidate.source.record_id,
                    candidate.source.item_id,
                    candidate.source.revision,
                    payload,
                    candidate.created_at,
                    imported_at,
                    imported_at,
                ),
            )
            connection.execute(
                "INSERT INTO candidate_revision_history("
                "candidate_id,source_revision,payload_json,created_at,"
                "imported_at) VALUES(?,?,?,?,?)",
                (
                    candidate.candidate_id,
                    candidate.source.revision,
                    payload,
                    candidate.created_at,
                    imported_at,
                ),
            )
            connection.execute(
                "INSERT INTO candidate_lifecycle(candidate_id,source_revision,"
                "state,generation,changed_at,updated_at) VALUES(?,?,?,?,?,?)",
                (
                    candidate.candidate_id,
                    candidate.source.revision,
                    candidate.lifecycle.state,
                    candidate.lifecycle.generation,
                    candidate.lifecycle.changed_at,
                    imported_at,
                ),
            )
            return ImportResult(ImportDisposition.INSERTED)

        if row["created_at"] != candidate.created_at:
            return ImportResult(
                ImportDisposition.REFUSED,
                ImportRefusal.CREATED_AT_CONFLICT,
            )
        if row["lifecycle_generation"] is None:
            raise InboxError("candidate lifecycle state is incomplete")
        current_generation = int(row["lifecycle_generation"])
        incoming_generation = candidate.lifecycle.generation
        contract_upgrade = _is_cumulative_contract_upgrade(
            row["payload_json"], payload
        )
        if incoming_generation < current_generation and not contract_upgrade:
            return ImportResult(
                ImportDisposition.REFUSED,
                ImportRefusal.STALE_GENERATION,
            )
        if (
            incoming_generation == current_generation
            and incoming_generation > 0
            and (
                row["source_revision"] != candidate.source.revision
                or row["payload_json"] != payload
            )
            and not contract_upgrade
        ):
            return ImportResult(
                ImportDisposition.REFUSED,
                ImportRefusal.GENERATION_CONFLICT,
            )
        if (
            incoming_generation > current_generation
            and current_generation > 0
            and incoming_generation != current_generation + 1
            and not contract_upgrade
        ):
            return ImportResult(
                ImportDisposition.REFUSED,
                ImportRefusal.GENERATION_GAP,
            )
        if history is not None:
            if history["created_at"] != candidate.created_at:
                return ImportResult(
                    ImportDisposition.REFUSED,
                    ImportRefusal.CREATED_AT_CONFLICT,
                )
            if history["payload_json"] != payload:
                return ImportResult(
                    ImportDisposition.REFUSED,
                    ImportRefusal.REVISION_CONFLICT,
                )
            if (row["source_revision"] == candidate.source.revision
                    and row["payload_json"] != payload):
                raise InboxError("candidate inbox and history are inconsistent")
            return ImportResult(ImportDisposition.UNCHANGED)

        if row["source_revision"] == candidate.source.revision:
            raise InboxError("candidate revision history is incomplete")

        connection.execute(
            "INSERT INTO candidate_revision_history("
            "candidate_id,source_revision,payload_json,created_at,imported_at) "
            "VALUES(?,?,?,?,?)",
            (
                candidate.candidate_id,
                candidate.source.revision,
                payload,
                candidate.created_at,
                imported_at,
            ),
        )
        connection.execute(
            "UPDATE candidate_inbox SET source_revision=?,payload_json=?,"
            "updated_at=? WHERE candidate_id=?",
            (
                candidate.source.revision,
                payload,
                imported_at,
                candidate.candidate_id,
            ),
        )
        connection.execute(
            "UPDATE candidate_lifecycle SET source_revision=?,state=?,"
            "generation=?,changed_at=?,updated_at=? WHERE candidate_id=?",
            (
                candidate.source.revision,
                candidate.lifecycle.state,
                candidate.lifecycle.generation,
                candidate.lifecycle.changed_at,
                imported_at,
                candidate.candidate_id,
            ),
        )
        return ImportResult(ImportDisposition.UPDATED)

    def _prepare_database_file(self) -> None:
        parent = self.database_path.parent
        if not parent.is_dir():
            raise InboxError("candidate inbox parent directory does not exist")
        if self.database_path.is_symlink():
            raise InboxError("candidate inbox database must not be a symbolic link")
        if self.database_path.exists():
            mode = self.database_path.stat().st_mode
            if not stat.S_ISREG(mode):
                raise InboxError("candidate inbox database must be a regular file")
            os.chmod(self.database_path, 0o600)
            return
        try:
            descriptor = os.open(
                self.database_path,
                os.O_CREAT | os.O_EXCL | os.O_WRONLY,
                0o600,
            )
        except FileExistsError as exc:
            raise InboxError(
                "candidate inbox database path changed during creation"
            ) from exc
        else:
            os.close(descriptor)

    def _connect(self) -> sqlite3.Connection:
        if not self.database_path.is_file() or self.database_path.is_symlink():
            raise InboxError("candidate inbox is not initialized")
        connection = sqlite3.connect(self.database_path, timeout=5)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout = 5000")
        return connection

    @staticmethod
    def _schema_version(connection: sqlite3.Connection) -> int:
        return int(connection.execute("PRAGMA user_version").fetchone()[0])

    def _require_current_schema(self, connection: sqlite3.Connection) -> None:
        version = self._schema_version(connection)
        if version != SCHEMA_VERSION:
            raise InboxError("candidate inbox schema is not initialized or supported")
        self._require_schema(connection)

    @staticmethod
    def _require_schema(connection: sqlite3.Connection) -> None:
        CandidateInbox._require_tables(
            connection, tuple(_SCHEMA_COLUMNS), columns=_SCHEMA_COLUMNS
        )
        for name, expected_type in _SCHEMA_OBJECTS.items():
            row = connection.execute(
                "SELECT type FROM sqlite_master WHERE name=?", (name,)
            ).fetchone()
            if row is None or row["type"] != expected_type:
                raise InboxError("candidate inbox schema is incomplete")

    @staticmethod
    def _require_tables(
        connection: sqlite3.Connection,
        tables: tuple[str, ...],
        *,
        columns: dict[str, tuple[str, ...]] | None = None,
        allow_appended_columns: bool = False,
    ) -> None:
        expected_schema = _SCHEMA_V14_COLUMNS if columns is None else columns
        for table in tables:
            expected_columns = expected_schema[table]
            row = connection.execute(
                "SELECT type FROM sqlite_master WHERE name=?", (table,)
            ).fetchone()
            if row is None or row["type"] != "table":
                raise InboxError("candidate inbox schema is incomplete")
            columns = tuple(
                item["name"]
                for item in connection.execute(f"PRAGMA table_info({table})")
            )
            # Additive migrations append columns.  A predecessor with a
            # durable later-column prefix may resume safely, but final schema
            # validation below remains exact and rejects unknown columns.
            if columns != expected_columns and not (
                allow_appended_columns
                and columns[:len(expected_columns)] == expected_columns
            ) and not (
                # A synthetic historical-migration rehearsal can begin from
                # a newer database and remove only the columns it is about to
                # reintroduce. Steer columns arrived after every checkpoint
                # below, so tolerate precisely those durable future suffixes.
                table == "task_execution_workflows"
                and set(columns) == set(expected_columns) | {
                    "steer_while_running", "current_run_id"}
                and len(columns) == len(expected_columns) + 2
            ) and not (
                table == "execution_review_cards"
                and set(columns) == set(expected_columns) | {"steer_digest"}
                and len(columns) == len(expected_columns) + 1
            ) and not (
                table == "task_review_cards"
                and set(columns) == set(expected_columns) | {"claiming_consumer"}
                and len(columns) == len(expected_columns) + 1
            ) and not (
                table == "execution_review_card_events"
                and columns == expected_columns + ("consumer_digest",)
            ) and not (
                table == "tasks"
                and tuple(column for column in columns if column not in {
                    "object", "action", "confidence"
                }) == expected_columns
            ) and not (
                # ALTER TABLE preserves data but a historical rehearsal that
                # removes and later restores profile columns can move the
                # final two steer fields ahead of them. Their names, not
                # their physical SQLite order, define this row shape.
                table == "task_execution_workflows"
                and set(columns) == set(expected_columns)
                and len(columns) == len(expected_columns)
            ) and not (
                table in {"tasks", "task_review_cards"}
                and set(columns) == set(expected_columns)
                and len(columns) == len(expected_columns)
            ):
                raise InboxError("candidate inbox schema is incomplete")

    def _now(self) -> str:
        value = self._clock()
        if value.tzinfo is None or value.utcoffset() is None:
            raise InboxError("candidate inbox clock must include a timezone")
        return value.isoformat(timespec="seconds")


def _validate_shadow_cycle_receipt(receipt: object) -> None:
    if not isinstance(receipt, ShadowImportCycleReceipt):
        raise InboxError("shadow import cycle receipt is invalid")
    stream_id = receipt.stream_id
    if not isinstance(stream_id, str) or not _STREAM_ID_RE.fullmatch(stream_id):
        raise InboxError("shadow import cycle stream ID is invalid")
    started = _receipt_timestamp(receipt.started_at)
    completed = _receipt_timestamp(receipt.completed_at)
    if completed < started:
        raise InboxError("shadow import cycle timestamps are invalid")

    counts = (
        receipt.candidate_previous_cursor,
        receipt.candidate_current_cursor,
        receipt.candidates_inserted,
        receipt.candidates_updated,
        receipt.observation_previous_cursor,
        receipt.observation_current_cursor,
        receipt.observations_inserted,
        receipt.comparison.total,
        receipt.comparison.agreed,
        receipt.comparison.divergent,
        receipt.comparison.refused,
        receipt.comparison.unmapped,
    )
    if any(isinstance(value, bool) or not isinstance(value, int)
           or not 0 <= value <= _MAX_SQLITE_INTEGER
           for value in counts):
        raise InboxError("shadow import cycle counts are invalid")
    if (receipt.candidate_current_cursor
            < receipt.candidate_previous_cursor
            or receipt.observation_current_cursor
            < receipt.observation_previous_cursor):
        raise InboxError("shadow import cycle cursors are invalid")
    report = receipt.comparison
    if report.total != (
        report.agreed + report.divergent + report.refused + report.unmapped
    ):
        raise InboxError("shadow import cycle comparison is invalid")


def _receipt_timestamp(value: object) -> datetime:
    if not isinstance(value, str) or not value or len(value) > 64:
        raise InboxError("shadow import cycle timestamp is invalid")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise InboxError("shadow import cycle timestamp is invalid") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise InboxError("shadow import cycle timestamp is invalid")
    return parsed


def _is_cumulative_contract_upgrade(
    current_payload: str, incoming_payload: str,
) -> bool:
    """Recognize an additive cumulative-contract upgrade of one source state."""
    try:
        current = json.loads(current_payload)
        incoming = json.loads(incoming_payload)
        current_version = current["schema_version"]
        incoming_version = incoming["schema_version"]
    except (json.JSONDecodeError, KeyError, TypeError):
        return False
    if (
        current_version not in _CUMULATIVE_SCHEMA_VERSIONS
        or incoming_version not in _CUMULATIVE_SCHEMA_VERSIONS
        or incoming_version <= current_version
    ):
        return False

    def shared_shape(document: dict[str, object]) -> dict[str, object]:
        normalized = json.loads(json.dumps(document))
        normalized.pop("schema_version", None)
        source = normalized.get("source")
        task = normalized.get("task")
        if not isinstance(source, dict) or not isinstance(task, dict):
            return normalized
        source.pop("revision", None)
        source.pop("history", None)
        lifecycle = normalized.get("lifecycle")
        if isinstance(lifecycle, dict):
            lifecycle.pop("generation", None)
        if current_version < STRUCTURED_TASK_SCHEMA_VERSION:
            for field in ("object", "action", "participants", "confidence"):
                task.pop(field, None)
        return normalized

    return shared_shape(current) == shared_shape(incoming)


def _canonical_payload(candidate: TaskCandidate) -> str:
    return json.dumps(
        task_candidate_document(candidate),
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )


def _feed_digest(feed: CandidateFeed) -> str:
    document = {
        "schema": feed.schema,
        "schema_version": feed.schema_version,
        "producer": feed.producer,
        "stream_id": feed.stream_id,
        "from_cursor": feed.from_cursor,
        "to_cursor": feed.to_cursor,
        "items": [
            {
                "sequence": item.sequence,
                "candidate": task_candidate_document(item.candidate),
            }
            for item in feed.items
        ],
        "emitted_at": feed.emitted_at,
    }
    payload = json.dumps(
        document,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _canonical_shadow_observation(
    observation: TaskShadowObservation,
) -> str:
    return json.dumps(
        task_shadow_observation_document(observation),
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )


def _shadow_feed_digest(feed: TaskShadowFeed) -> str:
    payload = json.dumps(
        task_shadow_feed_document(feed),
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()
