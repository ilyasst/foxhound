# ADR 0039: Source-revision task re-surfacing

## Status

Accepted.

## Context

A candidate identity is stable while its source revision changes. The ledger
therefore updates one durable task rather than creating a task for every source
comment. Updating task text alone is insufficient: a reader who was already
shown the old source can otherwise answer a card for work that has changed.

A task version is also insufficient as the sole fence. A producer may add
source evidence without changing task text, owner, or due date; keeping that
task version stable preserves an active workflow, but a source update can still
matter to the reader.

## Decision

Each task review card snapshots the accepted candidate's source revision when
it is scheduled. A card is stale when either its task version or source
revision differs from the current binding. Stale cards are cancelled by the
ordinary scheduler, and their action is refused. An in-flight delivery is also
refused if the source moved before acknowledgement.

The scheduler treats a bound revision as unseen unless a card carrying that
revision has previously been delivered. A newer unseen revision bypasses the
normal `review_after` delay once, then ordinary card pacing and the one-active-
card ceiling continue to apply. Repeated revisions before a new card reaches a
reader leave at most one current card; no reader receives a card per source
change.

An open snooze is superseded by a newer source revision. The reader deferred
the earlier source state, not an unknown later one.

A task in `done` re-surfaces when its bound revision moves: the ledger reopens
it and applies the candidate content. A task that reached `done` without its
work reaching the outside world would otherwise be unreachable forever, and the
only remedy would be to recreate it outside the ledger. A task in `dropped`
stays terminal, because a reader who dropped it rejected the work itself and a
source edit is not grounds to overrule that. A task with a workflow still in
flight also keeps recording a conflict rather than reopening underneath a run.

Re-surfacing a terminal task reintroduces the loop this ADR originally closed
by refusing it: the agent's own follow-through comment enters issue provenance,
so a comment can move the revision that re-surfaces the task the comment was
reporting on. **That loop is now bounded at the agent rather than at the
ledger.** An agent that finds its follow-through already published records
completion against the existing receipt instead of repeating the work. This is
a weaker guarantee than the structural refusal it replaces: it costs one run per
cycle to decide to do nothing, and it holds only while the agent applies the
rule. Making it structural again would mean carrying comment authorship into
provenance so that a self-authored revision could be ignored.

Forge issue provenance may contain title, body, and one bounded comment
extract. The comment is labelled as source evidence, not a claimed semantic
diff or complete discussion. Its source name and extract follow the existing
portable-name and size limits; producer fixtures remain synthetic.

## Consequences

- A card visibly says that its source changed since the reader last saw it.
- Evidence-only changes invalidate reader actions without changing task state
  or unnecessarily staling active execution work.
- Candidate-history storage can retain a bounded comment extract, so it stays
  private state subject to the same access and retention controls as existing
  task evidence.
- A producer that only changes its digest can still trigger safe re-surfacing,
  but cannot explain the change until it supplies evidence such as the bounded
  comment extract.
