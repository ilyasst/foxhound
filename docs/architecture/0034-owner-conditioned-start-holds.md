# ADR 0034: Owner-conditioned Start-card holds

Status: accepted.

## Context

Foxhound deliberately presents a first Start card for every open task. This
keeps work owned by another person visible, but a fixed one-day snooze makes
the reader repeatedly dismiss work that should return at the next meeting
with that owner. Suppressing those cards before the reader sees them would
hide work and would make an uncertain owner label act as authorization.

GW already owns meeting and people evidence. Foxhound owns task and workflow
state. Neither service should copy private calendar content into the other's
database or logs.

## Decision

A Start card may offer `Until next meeting with Person B` only when all of the
following are true:

- the card is the ordinary version-fenced Start card;
- the task has a version-1, non-provisional structured owner reference;
- the owner kind is one person or an explicitly reassigned external person;
- speaker-scoped references are either complete or wholly absent;
- the canonical display is neither empty nor unresolved; and
- accent-folded exact comparison does not match a reader alias supplied by
  the strict GW execution-context response.

The label uses only the canonical display. It removes legacy speaker tokens
and is truncated on a UTF-8 boundary to the transport's 64-byte limit.
Unresolved, group, provisional, and reader-owned cases fail closed without the
button. All Start cards remain visible initially.

Clicking the control resolves the exact delivered card and changes the
workflow to `snoozed` in the same SQLite transaction. A separate durable hold
ledger stores the task and workflow versions, exact structured owner snapshot,
creation time, and a backstop exactly 21 days later. Its event ledger is
append-only. The action never queues or claims an agent.

Each bounded card-scheduling pass snapshots at most its requested limit of
active holds before making network requests. Foxhound sends GW only the owner
display and exact structured reference. GW returns only a boolean condition,
check time, and evidence revision. A false condition records content-free
evidence and leaves the hold active. An unavailable or malformed response
changes nothing. A true condition or the exact 21-day boundary releases the
hold, advances the workflow to `awaiting_start`, and lets normal scheduling
create a fresh card. The resolved old callback remains stale.

Task, workflow, or owner identity changes cancel the old condition rather than
applying evidence to a different person. Database locks are not held during GW
requests, and each result is rechecked against current state before mutation.

## Deployment

Roll out in dependency order:

1. deploy the authenticated GW owner-upcoming-meeting condition route;
2. deploy the trusted gateway allowlist and held-card presentation; then
3. configure and restart the Foxhound card service with its GW endpoint,
   alias, and owner-only token file.

Omitting all GW configuration keeps the legacy Start keyboard. Supplying only
part of it is a startup error. Existing active holds still honor their local
21-day backstop after restart if the condition client is unavailable.

## Consequences

Foxhound gains the useful GW behavior without making calendar content part of
task state. It stores a small amount of identity-bound condition history and
must poll a read-only dependency, but failures cannot release work early.
Fixed snoozes, non-Start cards, and agent selection remain unchanged.
