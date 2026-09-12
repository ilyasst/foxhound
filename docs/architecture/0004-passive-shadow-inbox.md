# ADR 0004: Durable passive shadow inbox

Status: accepted for the passive comparison phase.

## Context

A single shadow observation can describe the legacy decision for one exact
candidate revision, but durable comparison also requires ordered delivery,
idempotent retry, and proof that Foxhound previously received that revision.
The candidate inbox formerly retained only the latest revision.

## Decision

Foxhound accepts bounded, contiguous pages using the
`foxhound.task-shadow-observation-feed` version 1 contract. Producer and stream
cursors are independent from candidate-feed cursors. A complete page is
applied in one transaction and receives a digest-bound receipt. Exact retries
are replays; gaps, unreceipted overlaps, and changed uses of an old cursor are
refused.

Inbox schema version 3 adds immutable candidate revision history. Migration
from schema version 2 copies each current candidate revision into that history.
Every later candidate import records a new revision before changing the current
projection. Re-delivery of an older known revision does not roll the current
projection backward.

An observation is accepted only when its complete canonical candidate payload
matches an imported candidate ID and revision. One observation is retained per
candidate revision. Exact repeats are unchanged and contradictions refuse the
entire page.

Mapped observations are classified as `agreed` when the legacy comparable
digest matches the candidate digest and `divergent` otherwise. `refused` and
`unmapped` remain separate outcomes. The default report exposes counts only;
candidate content and legacy identifiers are not included.

## Recovery and ordering

The observation producer must publish a candidate revision before publishing
its observation. If an observation page arrives first, Foxhound refuses it as
missing candidate state and does not advance the observation cursor. The page
can be retried unchanged after the candidate feed catches up.

## Out of scope

This slice does not read a producer outbox, write observations in GW, create a
Foxhound task, change task state, schedule work, render cards, or execute an
agent. Observation content remains private state outside the repository.
