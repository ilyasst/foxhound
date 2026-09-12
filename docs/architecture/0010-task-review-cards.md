# ADR 0010: Durable task review cards

Status: accepted for the staged extraction.

## Context

Foxhound can own a durable task without yet exposing that task to a reader.
Delegating review-card state or reader decisions to a knowledge producer would
restore two task authorities and would make a delayed or repeated callback
capable of changing the wrong task revision.

Card delivery also has a failure window: a sender can stop after taking work,
or a callback can arrive after the card was replaced. Delivery therefore needs
its own lease and version fence rather than relying on a transport message ID.

## Decision

Schema version 7 adds Foxhound-owned review cards and append-only card events.
Migration creates only the schema. The explicit `TaskCardService.schedule`
operation creates cards for open tasks, in stable task-creation order, with at
most one active card per task.

A sender atomically claims one due card for a bounded lease. The claim carries
a secret capability that is stored only as a digest. Rendering happens from
the claimed private projection and embeds the card ID and version in bounded
callbacks. A successful transport send is acknowledged with the capability
and opaque delivery references. A failed or expired claim becomes eligible
again under a new version. Transports should additionally use a stable
idempotency key because no local database can prove whether a remote send
succeeded before its acknowledgement was lost.

Reader actions require the delivered card's exact version and its captured
task version. `done` and `drop` update the task, append the normal lifecycle
event, resolve the card, and append the card event in one transaction.
`keep_open` resolves the card and permits a new review after seven days.
`snooze` retains the card and makes it deliverable again after exactly three
days. A stale, replayed, malformed, or out-of-order operation writes nothing.

Card content is private runtime data. Operation results and scheduler results
contain only state, versions, timestamps, and aggregate counts suitable for
content-free operational reporting.

## Activation and rollback

Initializing an existing database only adds empty tables. Activation requires
an operator or later service to call the explicit scheduler and then claim a
card. Before the first claim, rollback is to stop scheduling and restore the
pre-migration private database backup if the older binary must run.

After a reader action, the Foxhound task ledger and its append-only events are
authoritative. Rollback must preserve those decisions and project them through
the lifecycle-outcome boundary; restoring an older database would lose user
intent and is not permitted.

## Out of scope

This decision does not add a network service, chat transport, host scheduler,
free-text update, reassignment, knowledge projection, workflow gate, or agent
execution.
