# ADR 0023: Producer candidate lifecycle

Status: accepted.

## Context

Candidate revisions update an action description, but a digest cannot say
whether a payload came before or after a producer withdrew its evidence claim.
Mutable sources can remove an action through an edit or remove its evidence
entirely. Treating absence as a withdrawal would be unsafe because absence can
also mean a partial export or interrupted delivery.

Foxhound remains the sole authority for durable task lifecycle. A producer must
not turn evidence withdrawal into task completion, deletion, or execution.

## Decision

Task-candidate contract version 3 adds an explicit `lifecycle` object:

- `state` is `active` or `withdrawn`;
- `generation` is a positive, per-candidate monotonic integer; and
- `changed_at` is the time the producer changed that lifecycle projection.

The source revision remains a digest of the complete projection. It detects an
altered replay; it is not used for ordering. The generation orders states for
one stable candidate identity. The inbox accepts the next generation, treats
an exact replay as unchanged, and refuses an older, skipped, or contradictory
generation. Versions 1 and 2 retain their existing behavior as active
generation zero, so producers that never emit lifecycle state do not change.

The feed cursor still orders delivery globally. Candidate lifecycle and task
lifecycle are stored separately so receipt application, native intake, and
task disposition can resume independently after interruption.

## Durable task disposition

A withdrawal before native binding advances intake without creating a task. A
withdrawal after binding records the new source state and appends a task event,
but leaves the durable task open. If the task is still the exact accepted
version and no execution workflow has advanced beyond its start gate, Foxhound
increments the task version to invalidate stale cards and workflows and marks
the open task as source-withdrawn. Scheduling then withholds new cards and
agent work.

If the reader has changed or closed the task, or work has already become
active, Foxhound records a withdrawal conflict and preserves the reader-owned
state and workflow. Reactivation follows the same fence: an untouched
source-withdrawn task can take the new projection, while a reader-conflicted
task is never overwritten. Neither path marks work done or erases history.

Task status outcome export remains unchanged because source withdrawal is not
a Foxhound task status transition. The append-only task event and lifecycle
tables are the audit record.

## Rollout

1. Deploy the v3 parser and schema migration while producers still emit v1/v2.
2. Verify old candidates, feeds, intake, cards, and workflows remain unchanged.
3. Enable v3 active candidates for one synthetic producer stream.
4. Exercise active, revision, withdrawal, replay, and reactivation in order.
5. Enable mutable-source withdrawals only after the consumer cursor has passed
   the first verified v3 active generation.

## Rollback

Stop v3 production first. Do not emit v1/v2 for an identity that has advanced
to v3: generation zero is deliberately stale. A pre-v3 Foxhound binary must
not open the migrated database. Retain the database and feed cursor, restore a
v3-capable binary, and resume from the last committed cursor. No reverse
migration or task-history deletion is permitted.
