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

The grant applies at the moment of recording, so no reader-action card is
created and none has to be retired. The recorded result also creates one
delivery-only run summary. It has no controls, does not occupy the active
reader-card index or a consumer's actionable-card ceiling, and settles on a
successful delivery acknowledgement without changing workflow state. A
gateway may deliver that summary even while an actionable card is visible.
Reconciling workflows that were already waiting when policy changed is a
separate concern, and stays in scheduling.

The runner and the worker must be the same build. This paragraph once noted
that `worker_command` was a bare name resolved through `PATH` by the agent,
so the two could diverge, and judged the resulting failure loud. It was not
loud: it surfaced as an exit code from a subprocess nobody was watching, once
per claimed run, with no result recorded and the claim consumed. The worker is
now resolved from the running release and checked before anything is claimed —
see [ADR 0046](0046-worker-resolved-from-the-running-release.md). A partial
promotion still stops work rather than degrading it, which remains intended.

## Failure and rollback

Reverting to a release that predates schema 5 refuses every in-flight run
whose state was already written, until those runs are abandoned and their
workflows requeued. Grants themselves roll back through configuration: empty
lists restore the reader gates without a code change.
