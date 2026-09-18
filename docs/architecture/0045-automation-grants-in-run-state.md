# ADR 0045: Carry automation grants in private run state

## Status

Accepted.

## Context

`plan_without_asking`, `execute_without_asking` and `act_without_asking` are
declared per machine and validated against the source policy. The first is
applied while scheduling, by a component that holds the configured value. The
other two can only be applied where a phase result is recorded.

That is not the runner. The runner claims the workflow, writes private run
state, and supervises an agent; the agent invokes the worker, and the worker
records the result. So the two grants were configured on one process and
needed by another, and a service constructed without them reads two empty
sets and asks for a card every time. Both grants were inert on every
deployment that set them, in a way no rendered configuration could reveal.

Run state is already the runner's private channel to the worker. It carries
the database path, the workflow identity and the claim capability, so a
process that can read or forge it can already record any result it likes.
Grants add no capability to that file that it did not already confer.

## Decision

Private run state carries `execution_grants` and `action_grants`, at schema
version 5. The runner writes both lists always, empty when nothing is
granted, so an absent field and an empty one never come to mean different
things. The worker validates them through the source policy on load — an
unknown kind refuses the run rather than silently widening or narrowing
authority — and passes them to the service that records the result.

Schema 3 and 4 state remains readable, with empty grants. This is not
courtesy: a release is promoted while runs are in flight, so state written by
the previous runner will be read by the new worker.

A granted advance queues the next phase rather than raising a card, which
leaves the run `queued` — the same status an agent that released without
recording leaves behind. The two are told apart by the result: a release does
not change `last_result_id`. A run that recorded one is reported as
`recorded`, whether or not a grant moved it on.

## Consequences

The grant applies at the moment of recording, so no card is created and
none has to be retired. Reconciling workflows that were already waiting when
policy changed is a separate concern, and stays in scheduling.

The runner and the worker must be the same build. `worker_command` is a bare
command name rendered into the agent's prompt and resolved through `PATH` by
the agent itself, not invoked by the runner, so the two can diverge. A newer
runner writing schema 5 to an older worker fails the run at startup — loudly,
which is the intended direction, but it means a partial promotion stops work
rather than degrading it.

## Failure and rollback

Reverting to a release that predates schema 5 refuses every in-flight run
whose state was already written, until those runs are abandoned and their
workflows requeued. Grants themselves roll back through configuration: empty
lists restore the reader gates without a code change.
