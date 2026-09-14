# ADR 0033: Structured task-owner identity

## Status

Accepted

## Context

A task owner was historically a display string. That representation cannot
distinguish a verified person from an unresolved identifier, cannot survive a
rename, and cannot safely support owner-conditioned workflow actions. Matching
display text would silently join unrelated people or miss the same person
under two labels.

## Decision

Task-candidate version 5 keeps the card label in `task.owner` and adds a
strict `task.owner_ref`. Version 6 composes that owner shape with the bounded
meeting provenance introduced by version 4; it does not discard the extracts
that justify a task merely to add identity. The reference records a closed
owner kind, observed
and canonical speaker identifiers, their registry identity, whether the
resolution is provisional, and whether a reader pinned it. A speaker
identifier is accepted only with its registry. Unresolved owners render as
`(unassigned)`; raw speaker identifiers are never valid card labels.

Foxhound schema version 18 persists those fields separately. Rows created from
older candidate contracts remain reference version zero and provisional. A
reader reassignment creates a version-one external reference, clears the
provisional flag, and pins the choice. Native candidate revisions may still
update task text and due date, but they preserve a pinned owner.

Authority is intentionally divided:

- The producer owns the source-derived label, reference, and provisional
  assessment it emits. It cannot confer reader approval. Only the explicit
  bounded legacy handoff may carry a historical reader pin from the retiring
  authority.
- Foxhound validates and durably versions that claim. It neither resolves an
  incomplete identity nor reconstructs one from display text.
- The reader owns an explicit reassignment. That decision is append-only and
  pinned; later producer revisions retain authority over other task fields but
  cannot replace the selected owner.

## Consequences

Future owner-aware actions can match canonical, registry-scoped identity
without parsing display text. Producers can adopt versions 5 and 6
incrementally; versions 1 through 4 remain readable. An older or incomplete
owner label is
not silently promoted to verified identity, so it remains ineligible for
identity-conditioned automation until reconciled.
