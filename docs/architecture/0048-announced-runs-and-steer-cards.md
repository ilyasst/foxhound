# ADR 0048: Announced runs and Steer cards

## Status

Accepted.

## Context

Automatic work previously had either a Start gate before it began or a final
card after it finished. A reader could not intervene in a run that had gone
long. Reusing `start` would make one card mean both “no agent has started” and
“an agent is already running”, with incompatible controls.

## Decision

A `steer` card binds to a running workflow version and phase. Its admission
policy is written once to the workflow row from `workflow.steer_while_running`,
so changing deployment configuration cannot retroactively announce a run.
Cards are raised only after a per-phase duration threshold; short runs should
not leave obsolete messages behind. Repository sources are normally granted
through to a final card without steer announcements. Communication sources may
keep their existing gates and opt into announced runs.

A note pre-empts the pass and queues a new one carrying the note. It cannot be
injected into a live agent: the worker reads reader input once while building
its payload. A reader pre-emption is not a failure. Steer cards have their own
delivery capacity, since a status card that needs no answer must not displace a
decision card. Their digest is derived asynchronously from an untrusted,
bounded transcript tail and is allowed to be absent. A live digest can refresh
after thirty minutes, at most three times total, so a multi-hour run cannot
produce unbounded capability calls.

The runner records a reader pre-emption as the existing `discussion_requested`
ledger event. Its displaced child observes a normal `released` terminal state
and exits successfully, rather than looking like an agent or supervisor
failure.

## Consequences

Steering discards the current pass’s work, which can be expensive during a
long execute pass. A digest depends on a remote capability and may be absent.
Retraction is best-effort at the delivery boundary; until a consumer supports
it, a retired ledger card can leave an obsolete presentation behind. Operators
must deliberately choose source kinds: no configuration edit announces work
that was already admitted.

## Failure and rollback

If the digest service is unavailable, the card remains and simply has no
digest. Reverting the declaration stops new announcements but leaves existing
workflow decisions intact. Reverting code that cannot read the new database
schema requires the normal schema-release procedure in ADR 0043.
