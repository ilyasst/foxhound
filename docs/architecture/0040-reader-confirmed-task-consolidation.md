# ADR 0040: Reader-confirmed cross-source task consolidation

Status: accepted.

## Context

One commitment can be discovered in more than one source.  Each source item
has a distinct durable candidate identity, so correctly accepting those items
can still produce two durable tasks for one piece of work.  A text match is not
enough to decide that the tasks are the same: source wording, timing, and
context differ, and a false merge can hide work owed by two different people.

The existing task-relation record can preserve a reversible `duplicate_of`
claim, but it intentionally neither selects a canonical task nor controls
cards, workflows, or task provenance.

## Decision

Foxhound creates tasks normally, then may make a **reader-confirmed
consolidation proposal** for two eligible tasks.  Each native-intake batch that
adds a task scans the complete eligible open queue and the preceding 30 days
of closed tasks, so the new task and older tasks are considered together.  A
detector can recommend a pair, but it never applies a relation or changes
either task.  An unanswered proposal never blocks candidate intake, task
cards, scheduling, or execution.  Operator-run scans remain available for
deliberate reconciliation; task revisions alone do not trigger a scan.

### Eligibility and freshness

A pair is eligible when both tasks are open, or when one newly open task is
compared with a task closed in the preceding 30 days.  The two tasks must have
different source kinds.  Owner identity raises confidence but does not gate a
reader-only proposal: confirmed, unresolved, and differently scoped owners
may all be reviewed, while no detector result can consolidate them itself.
The detector requires at least two meaningful shared terms covering at least
60 percent of the shorter task's meaningful-term set, and offers only the
strongest match for each newly added task.

The proposal records the task versions it compared.  Confirmation is refused
without a write when either version has changed, the pair no longer has an
open task and at most one closed task, or an execution run is claimed, running,
or awaiting a result review.  The caller must make or receive a fresh proposal;
Foxhound never carries a similarity judgement over changed task state.

### Canonical task and preserved history

When both tasks are open, the lower durable task identifier is canonical.  For
a newly open task compared with a recently closed task, the closed task is
canonical so a reader can say the new task was already completed.  The other
task remains an immutable historical task row with its status, events,
candidate binding, and work products intact; it is not deleted, marked done,
or marked dropped.

Confirmation atomically records a reader-asserted `duplicate_of` relation from
the noncanonical task to the canonical task and suppresses future ordinary-card
and execution scheduling for the noncanonical task.  Any unstarted,
undelivered card or workflow belonging to that task is cancelled in the same
transaction with an auditable cancellation event.  A delivered card is made
stale by its version fence and reports that no action is needed.  No task
lifecycle status changes.

Every canonical-task read surface resolves live duplicate relations and shows
the ordered source provenance of the canonical task and its noncanonical
duplicates as one task raised more than once.  Later revisions of either bound
candidate remain attached to their original task and are included through that
same live relation.  A source revision never silently chooses a different
canonical task.

### Refusal and reversal

A reader may reject a proposal.  Rejection is durable for that unordered pair
and its detector basis, so the same pair is not proposed again merely because
a scan repeats.  A reader may withdraw a confirmed relation.  Withdrawal never
rewrites the prior assertion, candidate bindings, task events, or work
products.  It removes the scheduling suppression; normal scheduling may create
a fresh card for an eligible still-open task, but no historic card is revived.

### Concurrency and authority

Proposal confirmation, task-version checks, relation assertion, suppression,
and affected-card cancellation are one SQLite transaction.  Idempotent retry
returns the already-recorded decision.  A stale card, stale proposal version,
or concurrent task transition writes nothing beyond an explicit refusal or
existing recorded decision.

The proposal store contains private task extracts and detector signals.  Public
documentation, tests, logs, metrics, issue text, and command output contain
only synthetic examples or content-free counts.  Metrics distinguish proposed,
confirmed, rejected, reversed, and refused decisions; they never include task
text or source references.

## Consequences

The implementation needs a private proposal ledger, a detector, card actions,
and relation-aware scheduling and rendering.  It must not repurpose the
same-source candidate-identity rule: that rule prevents accidental duplicate
minting, while this decision manages distinct source items that are later found
to describe the same commitment.

The conservative eligibility rule deliberately defers tasks with active work
or incompatible ownership.  A future, separately decided workflow may support
consolidating those cases; it must not weaken these fences implicitly.
