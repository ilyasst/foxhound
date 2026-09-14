# ADR 0032: Readable task-source provenance

## Status

Accepted.

## Context

The first three task-candidate versions identify evidence with opaque record
and item identifiers. Those identifiers are sufficient for idempotency, but a
person reviewing a meeting-derived task cannot use them to understand why the
task exists. Rendering the record digest as `From` exposes an implementation
detail and withholds the actual review evidence.

## Decision

Task-candidate version 4 adds `evidence.sources` for meeting candidates. Each
entry has exactly a portable file basename, a closed source role, and a
bounded extract. The contract permits one through three entries, rejects path
separators and traversal names, and caps each extract at 1,200 characters.
Versions 1 through 3 remain valid.

Execution cards render a version-4 meeting origin as `From: Meeting`, followed
by every validated source name and extract. The opaque source identity remains
in the private ledger for correlation but is hidden from the card. Older
candidates retain the identifier fallback because they contain no better
evidence.

When a new candidate revision changes only provenance, native intake advances
the accepted binding without advancing the task version. This preserves an
active workflow and its cards while making subsequent renders read the new
evidence. A task-content change continues to advance the task version.

## Consequences

- A reviewer can see the source files and supporting context on the card.
- The producer can enrich an already accepted task without duplicating or
  staling it.
- Candidate payloads remain private; errors name only failed fields and rules.
- The contract does not accept absolute paths, source directories, arbitrary
  roles, unbounded excerpts, or additional source fields.
