# ADR 0050: Cumulative candidate provenance

## Status

Accepted

## Context

Task cards need the literal, bounded evidence that caused a task to be
offered. Candidate versions 4 and 6 carried that evidence only for meetings.
Other accepted source kinds therefore left readers with an opaque origin
identifier, and adding lifecycle or structured ownership required choosing a
different candidate shape instead of composing the capabilities.

## Decision

Task-candidate version 7 is the cumulative producer contract. It combines the
version-6 task shape, including structured ownership, with the version-3
lifecycle and optional bounded source evidence. It accepts every candidate
kind already authorized by the source-policy registry. The same registry
declares a closed evidence-role vocabulary for each kind, so a new source kind
cannot accidentally inherit another source's document semantics.

Evidence contains one to three records. Each record has a safe basename, a
source-specific role, and an extract limited to 1,200 characters. Earlier
candidate versions remain unchanged and readable.

Task-review and execution cards project evidence through one shared renderer.
They name each source file and show its escaped extract. When a producer has
not supplied evidence, the card explicitly says so instead of presenting an
opaque identifier as sufficient context.

For an addressable issue or review request, the shared renderer keeps the
clickable repository-and-number identity ahead of those extracts. Evidence
explains the origin; it must not make the origin unreachable.

A revision that changes only evidence advances the accepted candidate binding
and creates an audit event, but does not increment the task version or cancel
active work. Text, owner, and due-date changes retain the existing task-version
boundary.

## Consequences

Every accepted producer can explain a task in the same bounded shape, while
readers get consistent evidence on the first card and later workflow cards.
Producer rollout can be incremental: missing evidence is visible, not fatal,
and versions 1 through 6 continue to parse exactly as before.
