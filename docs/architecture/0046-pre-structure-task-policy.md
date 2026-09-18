# ADR 0046: Leave existing tasks unstructured

## Status

Accepted.

## Decision

Do not backfill structured fields onto tasks created before candidate-contract
version 8. Their text, owner, due date, task version, and review history remain
unchanged. New candidates may carry structure; an older task remains fully
findable, cardable, answerable, and eligible for the existing wording route.

The structure-status command reports the count of structured and unstructured
tasks without exposing task content. That makes the gap visible without turning
a reporting command into a backfill.

## Rationale

Re-extracting retained sources can revise a task after a reader has already
answered or acted on it. Extracting the already-extracted task text compounds
that uncertainty. Either operation would rewrite a task beneath its own
history. The recall benefit does not justify that silent reinterpretation.

This is deliberately reversible as a policy: a future, separately reviewed
decision can define an auditable enrichment workflow that only adds absent
fields and never changes text, owner, or due date. It must not be smuggled in
as a convenience migration.
