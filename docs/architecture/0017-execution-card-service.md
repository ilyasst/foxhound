# ADR 0017: Loopback execution-card service

Status: accepted for the execution-ownership migration.

## Context

A private chat gateway needs to deliver and act on execution review cards, but
direct SQLite access would duplicate card logic and expand the database trust
boundary. Sending execution cards through the ordinary task-card namespace
would also obscure which callbacks control task lifecycle and which control an
execution gate.

## Decision

The existing `foxhound-task-cards` loopback service gains a distinct
`/v1/execution-cards/*` route family. It exposes the same narrow adapter shape
as task-review cards:

- aggregate active-card statistics;
- an explicit bounded scheduling pass;
- one digest-fenced delivery claim;
- delivery acknowledgement or failure;
- one versioned reader action; and
- one bounded, versioned discussion or reassignment input.

Each route uses the existing exact request contract, bearer authentication,
canonical IPv4 loopback bind, body and response limits, request timeout,
serialized application handling, no-store response policy, and content-free
error boundary. Execution routes use distinct response schemas and delivery
keys. Only a successful claim returns private rendered content and its
short-lived delivery capability. A complete rendered body may exceed one chat
message while remaining within the bounded response contract; safe transport
chunking does not weaken the card's single versioned decision capability.
Free-text input is accepted only at `/v1/execution-cards/input`; prompt-only
callbacks make no durable change, and stale or malformed responses fail
without partial task, workflow, card, or event writes.

The service audit record contains only the allowlisted route, HTTP method,
status, and coarse duration. It contains no task/card identity, content,
delivery reference, or capability. The ordinary `/v1/task-cards/*` routes and
responses remain unchanged.

The execution-card adapter is explicitly supplied to the application. If it
is absent, execution routes return a content-free service-unavailable response
instead of falling back to direct workflow operations. The packaged service
validates both card stores before listening, but startup neither schedules a
card nor advances any task or workflow state.

## Failure and rollback

Rollback is to stop the loopback service or omit the execution-card adapter.
Durable cards and workflow gates remain in SQLite. A delivery claim expires
without granting workflow authority, and a later invocation can retry it. Do
not give the gateway database access as a fallback.

## Out of scope

This slice does not implement a chat client, install a scheduler, launch an
agent, migrate a backlog, deploy a service, or change GW.
