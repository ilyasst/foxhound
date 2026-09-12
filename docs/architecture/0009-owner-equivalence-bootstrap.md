# ADR 0009: Owner-equivalence evidence during shadow bootstrap

Status: accepted for the pre-cutover pilot.

## Context

A candidate and its immutable legacy observation can differ only because GW
merged one source speaker identity into another after the original task was
created. Rewriting the candidate or observation would destroy migration
evidence. Accepting every owner mismatch would weaken the shadow comparison
boundary and could create a task with the wrong assignee.

## Decision

Foxhound defines closed version-1 request and response contracts for one
GW-attested owner-equivalence check. A request binds the configured GW alias,
candidate ID, source revision, legacy task ID, and observed comparable digest.
The only accepted equivalence basis is `speaker_merge`. The response must echo
the complete request identity and supply one bounded effective owner.

The GW client exposes only a fixed POST to
`/v1/task-owner-equivalence`, using the same authentication, endpoint,
redirect, proxy, timeout, media-type, JSON, and response-size restrictions as
bounded knowledge search. Missing, refused, unreachable, malformed, or
identity-mismatched responses provide no authority and do not create a task.

The task ledger requests evidence only for current divergent mapped
observations that do not already have persisted evidence. Network calls occur
without a database write lock. The ledger then starts one write transaction
and compares the exact candidate, observation, and existing-evidence snapshot
with the snapshot seen before resolution. Any change refuses and rolls back
the complete bootstrap invocation.

Foxhound independently recomputes the comparable digest using the immutable
candidate text and project plus the attested effective owner. The result must
equal the immutable legacy digest, proving that owner substitution alone
explains the divergence. Accepted evidence is stored in an append-only table
before the task and binding are committed in the same transaction. Exact
replays reuse stored evidence and do not call GW again.

The created Foxhound task uses the effective owner. Candidate and observation
payloads remain unchanged. Aggregate bootstrap results report only counts and
closed refusal categories.

## Failure and rollback

Schema version 6 adds only the append-only evidence table and triggers; it
creates no tasks during migration. Before cutover, rollback is to stop passing
an owner resolver to the explicit bootstrap. Stored evidence remains inert and
auditable. A corrupt stored evidence row, missing append-only trigger, changed
input snapshot, inconsistent grouping, or digest mismatch fails closed.

## Out of scope

This decision does not define how GW proves speaker merges, automatically run
bootstrap, change candidate discovery, revise historical records, render
cards, schedule tasks, execute agents, or disable legacy task handling. The GW
producer endpoint and deployment are separate controlled changes.
