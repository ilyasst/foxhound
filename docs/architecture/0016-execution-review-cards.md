# ADR 0016: Durable execution review cards

Status: accepted for the execution-ownership migration.

## Context

The execution ledger has three reader boundaries: starting planning, approving
a plan, and approving a proposed external action. Calling ledger methods
directly from a chat callback would provide no durable delivery identity and
would make delayed, duplicated, or stale callbacks difficult to distinguish.
Ordinary task-review cards cannot represent these gates because they own task
lifecycle decisions rather than execution workflow state.

## Decision

`foxhound.execution_cards.ExecutionCardService` owns a separate durable card
projection. Its explicit scheduler creates cards only for current open tasks
whose execution workflow is awaiting start, awaiting plan review,
external-action review, or final-result review. Schema migration is passive:
it creates no card and changes no workflow.

At most one execution card is active for a task. Each card binds the exact
task version, workflow version, phase, kind, and, for reviews, immutable result
identity. Scheduling and delivery cancel projections that no longer match.
Delivery claims use a random capability whose digest is stored with a bounded
expiry. Failure and expiry make the same card retryable under a new version;
delivery acknowledgement removes the capability.

The four card kinds have closed action sets:

| Card | Allowed reader actions |
|---|---|
| Start | Complete, continue into planning, comment and continue into planning, drop, update instructions, snooze to a calendar morning, or reassign |
| Plan review | Investigate, discuss, execute, comment and continue into execution, snooze, complete, reassign, or drop |
| External review | Authorize the exact action, return for revision, discuss, comment and authorize, snooze, complete, reassign, or drop |
| Result review | Discuss, snooze, complete, reassign, or drop |

Comment and Go first collects one bounded private note, then atomically applies
the listed transition. A gateway must submit that note to
`/v1/execution-cards/comment-and-go` using the exact card id and version from
the callback; it must not send a separate discussion response followed by a
second action. The note targets the resulting workflow version, so the next
agent run receives it once and the answered card is resolved.

Every card renders the same four snooze choices directly: tomorrow at 09:00,
Friday at 09:00 (the following Friday when that time has passed), next Monday
at 09:00, and the date two weeks from today at 09:00. These are resolved in the
card-service host's local timezone and stored as UTC. The callback tokens remain
the bounded vocabulary already shared with GW; their meaning is defined by the
visible calendar label, not by the historical duration-like token name.

A delivered action and every affected execution workflow or task-lifecycle
transition occur in one `BEGIN IMMEDIATE` transaction. The card, task, and
workflow must still match all bound versions and state. Any refusal or write
failure rolls back all of them. Completion and drop write the ordinary task
lifecycle event, making the existing outcome feed authoritative. Reassignment
versions the task, records an append-only owner event, discards the obsolete
result binding, and returns execution to a fresh Start gate.

Every Start projection still resolves the workflow's exact installed profile
revision. Profile selection remains an authenticated integration operation,
but the legacy-compatible Start keyboard contains only its established six
task-decision controls. Selecting a different eligible profile updates the
workflow binding and the same delivered card in one transaction, versions
both, appends content-free workflow and card events, and returns the refreshed
presentation. Selecting the current exact revision is unchanged. No selection
queues work or changes task lifecycle.

Discussion and reassignment use a separate bounded input operation because
their values cannot safely fit in callback data. The gateway may collect text,
but Foxhound validates and commits it against the exact delivered card version.
Discussion from a Start card records an Update and returns to a fresh Start
projection without queuing work. Discussion from a review card queues a
planning pass. In both cases it becomes the private reader instruction for the
next run and the next immutable result consumes it. Snooze choices are fixed at
1, 7, 14, or 30 days and retain the same review kind when the deadline is
reached.

Card bodies are private, line-oriented HTML projections with a strict local
service byte ceiling and a distinct callback namespace. A small Markdown
subset in the immutable worker result becomes Telegram-compatible headings,
emphasis, code, safe HTTP links, and bullets. Every dynamic non-markup value
is escaped, and unsupported or unsafe links remain inert text.

The ceiling is larger than one Telegram message but remains below the bounded
local client and service response limits. The transport may split the complete
line-oriented projection and attaches the keyboard only to its final chunk.
If the complete body does not fit the local-service ceiling, the projection
clearly reports truncation, removes affirmative Start, Approve, and Complete
buttons, and rejects a forged affirmative callback. Non-affirmative controls
remain available as appropriate. This prevents approval or closure based on
content the reader could not see.

If a transport acknowledgement succeeded but the resulting presentation is
unreadable, a local operator may requeue that still-current delivered card.
The repair clears only delivery metadata and increments the card version, so
the old presentation becomes stale without advancing or rerunning workflow.
This repair is intentionally absent from the remote transport API.

Events are append-only and contain card/workflow identity, versions, action,
and time but no task or result content. Agent refresh events contain no name,
profile ID, or revision; the workflow event remains the authoritative profile
evidence. Operation and aggregate results are content-free. Private card
dataclasses exclude all content fields from their representations.

## Failure and rollback

Rollback is to stop calling the execution-card scheduler and delivery methods.
Existing workflow gates remain intact and can be inspected directly by an
approved private operator tool. Active cards can be left for forensic state or
cancelled by a later reconciliation pass; they never grant authority after the
bound workflow changes.

## Out of scope

This component adds no chat transport, recurring scheduler, agent launch,
production configuration, backlog migration, GW write, or automatic workflow
scheduling.
