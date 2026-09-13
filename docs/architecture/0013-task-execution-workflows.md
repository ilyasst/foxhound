# ADR 0013: Durable task execution workflows

Status: accepted for the execution-ownership migration.

## Context

Foxhound owns task lifecycle and review cards, but an agent cannot safely run
from a task row alone. Execution also needs an explicit reader gate, durable
phase state, an exclusive claim, lease renewal, idempotent results, bounded
retry behavior, and a record of decisions. Keeping those controls in GW would
leave two task authorities after card cutover.

## Decision

`foxhound.task_execution.TaskExecutionService` owns one durable execution
workflow per Foxhound task. Scheduling is explicit, task-version fenced, and
initially stops at `awaiting_start`; it never launches work. Start, snooze, and
cancel decisions are workflow-version fenced. A start queues the `plan` phase.

A worker can claim one ready phase under a random capability whose digest is
the only form stored in SQLite. Claims have bounded expiries and can be
renewed, released, or failed only with the matching capability and workflow
version. Expired claims are recovered before a new claim. Failures use bounded
exponential cooldowns and park after a configured attempt limit; an explicit
retry clears that failure state.

Results are strict, bounded private envelopes. Identity, task and workflow
versions, phase, capability, outcome, text fields, collection sizes, and total
canonical size are validated before a transaction begins. The immutable
result stores its digest and private work. Reusing a result identity with the
same digest is a no-op; changed reuse fails closed.

The phase transition is closed:

| Claimed phase | Accepted result outcomes |
|---|---|
| `plan` | `awaiting_plan`, `completed`, `ineligible` |
| `execute` | `awaiting_external`, `completed`, `declined`, `ineligible` |
| `external_action` | `completed`, `declined`, `ineligible` |

An `awaiting_plan` result requires a reader approval before `execute` can be
queued. An `awaiting_external` result requires a separate approval before
`external_action` can be queued. A revision decision returns to `plan`.

Terminal worker outcomes also stop at `awaiting_review`; only the reader's
version-bound completion or drop decision closes task lifecycle. A bounded
private discussion input queues one new planning pass. The worker receives it
only while holding the matching supervised claim. Retries of that logical pass
retain the instruction, while recording the next immutable result consumes it.
The instruction is not an external-action authorization.

Workflow events are append-only and content-free. Aggregate readiness contains
counts only. Task lifecycle remains separate from worker output: a successful
execution result does not silently close or drop its task. The execution-card
aggregate may atomically apply an explicit reader completion or drop to both
ledgers.

Each workflow is also bound to one exact reviewed agent profile revision.
Selection is a separate reader operation allowed only before Start; it versions
the workflow and invalidates older cards. Claims refuse unavailable or
phase-ineligible revisions. Results and events retain the content-free profile
evidence. See [ADR 0024](0024-workflow-agent-binding.md).

## Failure and rollback

Schema initialization is passive. Before any workflow is scheduled, rollback
is to stop using the service. Once execution history exists, stop new
scheduling and claims but preserve workflows, results, and events. Never
restore a GW execution writer against the same tasks merely because a runner
is unavailable.

A crashed worker retains no authority after lease expiry. A failed or expired
claim advances only retry metadata; it cannot change task lifecycle. A stale
task version cancels pending execution before another claim is granted.

## Out of scope

This slice does not render execution cards, build knowledge context, call GW,
launch an agent, read deployment configuration, write work files, or schedule
itself. Those adapters must consume this ledger in later changes.
