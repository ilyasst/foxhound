# ADR 0054: Task source-history baselines

## Status

Accepted.

## Context

Foxhound already keeps every candidate projection and records a task's first
accepted projection as a work revision. That projection digest cannot identify
the producer's exact source-history row: several source observations may yield
the same task wording, and evidence-only observations deliberately do not
advance work.

## Decision

Candidate contract version 9 carries a bounded producer-history reference:
source, stream, item, ordered position, and revision. Foxhound validates the
reference and stores it on the work revision created by an accepted binding.
When native intake folds a later candidate into work, the new work revision
stores that candidate's reference. Evidence-only binding movement still
creates no work revision.

The task ledger exposes the first and latest accepted work revisions as a
content-free read model. Existing candidates and migrated work revisions keep
a null history reference. Null means that the producer history is unknown;
Foxhound does not infer a historical state from the current binding.

Execution cards continue to point at a work-revision identifier, so an
approval names the same source-history state without duplicating it on the
card.

## Deployment

The schema migration only adds nullable columns and rebuilds the accepted-
binding trigger. It does not backfill source history or change task, workflow,
or card state. Because it advances the database schema, deploy it with the
schema-changing release procedure in ADR 0043 before enabling version 9 at a
producer. A version 9-capable Foxhound may safely run while producers still
emit older candidates.

## Consequences

Foxhound can distinguish the task's creation baseline from its latest accepted
work without connecting to a source provider. Producer observation history
remains producer-owned, and candidate intake remains the only authority path.
