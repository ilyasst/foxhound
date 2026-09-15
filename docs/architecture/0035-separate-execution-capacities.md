# ADR 0035: Separate agent-work capacity from reader-waiting cards

Status: accepted.

## Context

The execution-card scheduler suppressed every Start card while any workflow
was queued, running, or awaiting review. One unrelated workflow could
therefore hide the entire Start-gated backlog. Separately, a planning grant
could let the new-work scheduler queue a whole batch without accounting for
the amount of agent work already in flight.

GW solved these as two different capacity problems. Work consumes fleet
capacity; a decision waiting on a reader consumes attention. A waiting card
must not make an idle work slot look occupied, and active work must not make
an unrelated reader decision disappear.

## Decision

Foxhound adopts three independent bounds:

- at most two workflow phases may be `running`;
- at least ten eligible planning phases are kept durably `queued`, excluding
  the two running slots; and
- at most twenty newly scheduled workflows may occupy an active
  reader-waiting state; and
- the gateway continues to present at most one execution card at a time.

The new-work scheduler refills the ready-plan reserve independently in stable
task order. Claiming is the only operation that enters a running slot, and it
counts active slots inside the same transaction as the claim.
A source with explicit planning authority enters the work capacity; every
other source enters the Start-card waiting capacity. A full capacity skips
that class without preventing the other class from filling available room.
The content-free `remaining` count includes every eligible task left
unscheduled by the caller limit or either capacity.

Existing over-cap workflows are never deleted, demoted, or rewritten. They
drain through the normal lifecycle. Start-card scheduling no longer asks
whether any unrelated workflow is busy; it relies on the existing per-task
active-card invariant and the gateway's one-card presentation limit. Review
and result cards remain ordered ahead of Start cards when the same scheduling
pass discovers both.

Owner-conditioned holds, snooze clocks, task and workflow version fencing,
and the rule that every task is initially visible remain unchanged.

## Consequences

Agent work may continue while a reader decision waits, and a running agent no
longer silences the remaining Start-gated queue. Planning grants cannot fill
the durable reserve without bound. A deployment with state already above a
capacity remains safe but will schedule nothing further in that class until
enough workflows leave it.
