# ADR 0044: Bounded source snapshots before automated work

## Status

Accepted for the source-freshness foundation.

## Context

Foxhound persists the revision a producer supplied with a task candidate. That
is sufficient to make reader controls stale when new evidence arrives, but it
does not prove that an already claimed agent run still sees the current source.
The source-owning system must make that judgement: giving Foxhound direct
database access or unconstrained source reads would violate the task boundary.

## Decision

Foxhound defines a dependency-free version-1 contract with three records:

- `SourceLocator` identifies one exact source object by system, kind, record,
  and item. It deliberately excludes mutable content and revision.
- `SourceSnapshotRequest` binds that locator to the revision the workflow
  expects.
- `SourceSnapshot` carries only the observed revision, timezone-aware
  observation time, lifecycle, and one bounded actionability state. It carries
  no source body, participants, messages, or credentials.

A source-owned `SourceSnapshotResolver` answers with exactly one of
`current`, `changed`, `withdrawn`, `unavailable`, or `unsupported`. Only
`current` proves the expected active revision. A later workflow slice will
require that result immediately after claim and immediately before an external
effect. This ADR adds no endpoint and changes no current workflow behavior.

## Consequences

- Source adapters can be implemented for GitHub, email, and Teams without
  putting source-specific logic in the execution ledger.
- An unavailable source fails closed rather than being treated as unchanged.
- The existing candidate feed remains the only import path; a snapshot is a
  bounded read check, not a second task writer.
- Producers and Foxhound need a compatible adapter route before a freshness
  fence is enabled for that source kind.
