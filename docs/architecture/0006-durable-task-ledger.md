# ADR 0006: Durable Foxhound task ledger and shadow bootstrap

Status: accepted for the pre-cutover pilot.

## Context

The passive inbox can prove which current candidate revisions agreed with
GW's legacy registry, but it formerly had no Foxhound task identity. Cards or
workers must not activate against candidate rows directly: candidate import is
delivery, not a task-acceptance decision, and it may be retried or revised.

## Decision

Schema version 4 adds Foxhound-owned tasks, candidate bindings, a temporary
legacy correlation table, and append-only task events. Task IDs are allocated
by Foxhound and have no semantic relationship to legacy task IDs. Project is
not copied into the task row; it remains source filing metadata on version-1
candidates while GW removes that field from later producer contracts.

The ledger exposes an explicit `bootstrap_from_shadow` operation. Neither
candidate import nor observation import invokes it. The operation considers
only an observation for the candidate's current revision and materializes only
`agreed` `minted`/`folded` groups. A new legacy group requires exactly one
`minted` member; its task becomes the Foxhound task and `folded` members attach
to it. Refused, unmapped, divergent, stale, pending, and incomplete groups
produce no task.

The complete bootstrap invocation is one transaction. Exact retries do not
rewrite tasks, bindings, correlations, or events. Multiple minted members,
changed bindings, missing correlated tasks, malformed stored candidates, or
inconsistent task projections refuse and roll back the invocation. The
correlation table is private migration state and is not an external identity
contract.

Tasks begin `open`. Explicit operations can move an open task to `done` or
`dropped`, and can reopen either terminal state. Every transition requires the
caller's expected task version; a stale or invalid transition writes nothing.
Successful transitions increment the version and append a status event in the
same transaction. Database triggers refuse updates and deletions of event
rows.

## Failure and rollback

Schema migration preserves candidate and observation state and creates no task
rows. Before any card or worker cutover, rollback is simply to stop calling
the bootstrap or lifecycle API. Existing passive imports remain usable.

Once a later slice grants Foxhound authority, task-ledger rollback will require
the explicit authority protocol defined by that slice; this ADR does not grant
authority itself.

## Out of scope

This slice does not automatically accept new candidates, resolve new
deduplication decisions, revise a bound task from a later candidate revision,
render cards, schedule work, access producer knowledge, execute an agent,
project lifecycle state into GW documents, or disable the legacy registry.
