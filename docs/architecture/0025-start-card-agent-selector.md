# ADR 0025: Reader-controlled Start-card agent selection

## Status

Accepted.

## Context

Persisting an exact agent profile protects execution from configuration drift,
but a reader still needs to see and change that choice before authorizing
planning. Putting profile identity directly in a Telegram callback can exceed
the transport's 64-byte limit, and changing the workflow separately from its
delivered card would leave valid-looking stale controls on screen.

## Decision

Every Start card resolves the workflow's exact installed profile revision and
displays its reviewed name. The Agent button is present only when the current
delivered card still matches a workflow in `awaiting_start`.

The interaction has two authenticated loopback operations. Agent options
validate the delivered card and return only installed profiles whose phase
policy permits planning. Each button carries the card ID, card version, and a
20-character opaque digest derived from the exact profile ID and revision.
This encoding remains at or below 64 bytes even when both integer identities
are at SQLite's maximum. The callback carries no prompt or execution policy.

Agent selection maps that digest against the same immutable registry and
repeats the card, task, workflow, status, and version guards inside one
`BEGIN IMMEDIATE` transaction. Selecting a different exact profile increments
the workflow version, appends its `agent_selected` event, advances the same
delivered card to the new workflow and card versions, and appends a content-free
`refreshed` card event. The response contains a newly rendered Start-card body
and keyboard for the gateway to edit into the existing message. Selecting the
already-bound exact revision is idempotent and returns the current presentation
without a write.

Malformed, oversized, unknown, ambiguous, phase-ineligible, unavailable, or
stale selections change nothing. Agent selection never changes task lifecycle,
queues a phase, launches Hermes, or grants external-action authority. The card
server loads private profiles using the same strict owner-only directory input
as the execution runner; there is no fallback when a selected revision is
missing.

## Consequences

- The reader can verify and control the receiving agent at the Start gate.
- Telegram receives bounded callbacks and a complete edit-ready presentation.
- Old buttons become stale immediately after a changed selection.
- Profile installation remains a reviewed deployment action outside Git.

## Failure and rollback

Stop routing Agent callbacks or omit private profiles from the card server.
Existing Start, snooze, and cancel controls remain governed by their original
versions. Do not update workflow bindings directly or let the gateway access
SQLite as a fallback.
