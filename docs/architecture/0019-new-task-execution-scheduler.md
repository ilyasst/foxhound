# ADR 0019: Bounded new-task execution scheduler

Status: accepted for the task-authority cutover.

## Context

Foxhound can durably represent execution workflows and reader gates, but a
deployment previously needed ad hoc code to create the first workflow for a
newly accepted task. Calling the per-task scheduling operation indiscriminately
is unsafe because it can explicitly reset a terminal workflow.

## Decision

`TaskExecutionService.schedule_new` creates workflows only for open tasks that
have no row in the execution workflow ledger. Selection is ordered by
Foxhound task identity, bounded by a caller-supplied limit, and performed in
one write transaction. Every new workflow begins in `awaiting_start` and
appends its `scheduled` event atomically.

The `foxhound-execution-schedule` one-shot command exposes that operation for
a host scheduler. It requires an existing private database and reports only
the number scheduled and remaining. Exact retries schedule nothing. A task
with any existing workflow is ineligible, regardless of whether that workflow
is awaiting a gate, queued, running, under review, completed, cancelled,
snoozed, or parked. Retrying or resetting those states remains a separate,
explicit reader or operator decision.

The command does not advance the Start gate, render or deliver cards, query a
knowledge system, schedule itself, launch an agent, or change task lifecycle.

## Rollback

Stop invoking the command. Workflows already awaiting Start remain inert and
may be cancelled through their reader gate. No agent can claim them until the
reader explicitly selects Start.
