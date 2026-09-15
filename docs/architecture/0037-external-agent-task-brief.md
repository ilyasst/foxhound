# ADR 0037: Read-only task briefs for outside agents

## Status

Accepted

## Context

The execution worker's private prompt is coupled to Foxhound's bounded local
commands and recording protocol. Copying it into another agent would describe
tools that do not exist there. A reader still needs a way to hand a Task
workflow card to an agent of their own.

## Decision

Every Task workflow card offers a `Task brief` control. The corresponding
authenticated service route is a version-fenced read: it neither resolves the
card nor changes the workflow. A stale card is refused rather than returning a
brief for superseded work.

The brief is composed from the card's durable task text, addressable origin,
bounded source filenames and extracts, owner and due date, current summary,
questions, proposed external effects, work, and deliverables. It contains no
claim token, database path, worker command, or local review-file path. The
result is bounded plain text so a gateway can present it as a tap-to-copy block
without treating it as instructions for that gateway.

An addressable issue or review request remains linked in the card's `From`
line when source evidence is also present. Evidence supplements the origin; it
does not replace the link. For unaddressable sources with evidence, the card
names the source kind and shows the useful filenames and extracts instead of
an opaque storage key.

## Consequences

- A reader can hand the work to another agent without answering the card.
- The outside agent receives the issue body and other bounded source context,
  not only the task title.
- Gateways may add the new read-only control before producers render it.
- Card and brief content stay bounded and safe for transport.

## Failure and rollback

A malformed or stale brief response is refused visibly. Rolling back the card
button leaves the read route harmless and does not affect workflow state.
