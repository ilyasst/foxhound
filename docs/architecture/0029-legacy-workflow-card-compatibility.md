# ADR 0029: Legacy workflow-card compatibility

## Status

Accepted.

## Context

Moving execution authority must not make the reader learn a second task-card
language at the same time. The first Foxhound Start presentation used different
headings, context, labels, and row arrangement from the established workflow
card. It also omitted start-gate completion, and its rendered Drop control
reached a service path that refused the action.

## Decision

Foxhound treats the established workflow-card presentation as a compatibility
contract. A Start card shows the task reference, owner when known, first and
last source dates when known, and states that no agent has looked at the task.
Project metadata is deliberately absent from the header. Its controls have
this fixed layout:

| Row | Left | Right |
|---|---|---|
| 1 | Done | Continue |
| 2 | Drop | Update |
| 3 | Snooze 24h | Reassign |

The wire actions remain Foxhound's bounded, versioned callbacks: Continue maps
to `start`, Update to private `discussion`, and the other controls retain their
names. Done and Drop at a Start gate atomically close both task lifecycle and
workflow without running agent work. Update records private steering, leaves
the workflow awaiting Start, and resolves the old transport presentation so a
fresh version can be delivered. Continue remains the only Start-card action
that queues planning.

Plan, external-action, and result cards retain their existing content and
approval boundaries while restoring Reassign beside Drop task in their final
row. Every post-run review card identifies the bound agent by its resolved
display name, so the reader knows whose work is being reviewed. Start cards
retain the pre-run statement that no agent has looked at the task yet.
Oversized presentations still omit affirmative controls, and forged or stale
callbacks remain refused.

Execution workflows continue to store an exact agent profile revision. The
legacy-compatible Start keyboard does not expose a seventh Agent control.
Authenticated agent-options and agent-selection operations remain available
to trusted integrations, and deployment scheduling remains responsible for
choosing the profile before card creation.

## Consequences

- Readers keep one familiar task decision vocabulary during authority cutover.
- Start-card Done and Drop no longer require a preliminary agent run.
- An Update cannot accidentally start planning.
- Profile binding and all execution approval gates remain unchanged.

## Failure and rollback

Stop execution-card delivery to preserve every pending workflow gate. Reverting
only the renderer is unsafe because it would reintroduce a Drop control whose
historical service path refused it; presentation and start-action semantics
must move together.
