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
whose execution workflow is awaiting start, awaiting plan review, or awaiting
external-action review. Schema migration is passive: it creates no card and
changes no workflow.

At most one execution card is active for a task. Each card binds the exact
task version, workflow version, phase, kind, and, for reviews, immutable result
identity. Scheduling and delivery cancel projections that no longer match.
Delivery claims use a random capability whose digest is stored with a bounded
expiry. Failure and expiry make the same card retryable under a new version;
delivery acknowledgement removes the capability.

The three card kinds have closed action sets:

| Card | Allowed reader actions |
|---|---|
| Start | Start planning, snooze one day, cancel execution |
| Plan review | Approve plan, request revision, cancel execution |
| External review | Approve the proposed external action, request revision, cancel execution |

A delivered action and its execution workflow transition occur in one
`BEGIN IMMEDIATE` transaction. The card and workflow must both still match all
bound versions and state. Any refusal or write failure rolls back both. These
operations never alter task lifecycle.

Card bodies are private, HTML-escaped projections with a strict byte ceiling
and a distinct callback namespace. If the complete body does not fit, the
projection clearly reports truncation, removes the affirmative Start or
Approve button, and rejects a forged affirmative callback. Revision, snooze,
and cancellation remain available as appropriate. This prevents approval of
content the reader could not see.

Events are append-only and contain card/workflow identity, versions, action,
and time but no task or result content. Operation and aggregate results are
content-free. Private card dataclasses exclude all content fields from their
representations.

## Failure and rollback

Rollback is to stop calling the execution-card scheduler and delivery methods.
Existing workflow gates remain intact and can be inspected directly by an
approved private operator tool. Active cards can be left for forensic state or
cancelled by a later reconciliation pass; they never grant authority after the
bound workflow changes.

## Out of scope

This slice adds no HTTP route, chat transport, recurring scheduler, agent
launch, production configuration, backlog migration, GW write, or automatic
workflow scheduling.
