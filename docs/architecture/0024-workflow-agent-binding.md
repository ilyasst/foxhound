# ADR 0024: Durable workflow agent binding

## Status

Accepted.

## Context

A reviewed registry alone does not prove which agent policy was authorized for
a task. Looking up a default only when work starts would let configuration
changes silently alter queued work, while accepting a profile name from task
content would cross the reader-authorization boundary.

Runner-level time and tool overrides would also make the persisted profile
identity misleading.

## Decision

Every execution workflow stores an agent profile ID and its exact SHA-256
revision. Schema-11 workflows, results, and events migrate to the built-in
General compatibility revision without changing task version, workflow
version, status, phase, due time, result identity, or any reader gate. Newly
scheduled workflows receive the deployment's explicit deterministic default;
task text and source material are not inputs to this choice.

`select_agent` is a distinct version-fenced reader operation. It accepts only
an exact revision in the installed registry, requires that the profile can
plan, and is valid only in `awaiting_start`. An exact replay is unchanged.
Changing the selection increments the workflow version, appends an
`agent_selected` event, and therefore makes earlier cards stale. It never
starts work or changes task lifecycle.

Every claim resolves the stored ID and revision and checks that the profile
allows the queued phase before changing durable state. Unavailable, changed,
or phase-ineligible profiles leave the workflow queued. Claims carry the same
evidence. Immutable results and all workflow events copy it from the durable
workflow rather than trusting agent output. Aggregate profile health reports
only IDs, revisions, availability, and counts.

The supervised runner loads the reviewed registry. Its atomic claim operation
resolves the recorded evidence before changing durable state, and the runner
constructs the Hermes invocation from that resolved profile. The profile is
the sole source of instructions, tool families, turn limit, internal timeout,
claim lease, heartbeat, and shutdown grace. Only the policy fields reach the
invocation: the instructions are delivered through the fenced worker instead,
as [ADR 0028](0028-fenced-instruction-delivery.md) describes. The runtime
command and private profile directory remain deployment inputs; task content
cannot set either. The worker's owner-only run state carries profile evidence,
the profile-derived lease, and the worker command, but no instructions.

## Consequences

- A queued workflow cannot silently adopt a changed prompt or runtime policy.
- Longer local-GPU work is configured per reviewed profile instead of by a
  global timeout.
- Removing or editing a selected profile intentionally stops new claims until
  a reader selects an available revision or the reviewed profile is restored.
- Deployments upgrading from runner-level policy flags must remove those flags
  and install the selected profiles before enabling the runner.

## Amendment: a retired revision's budget is reported, not silently kept

The pin above is deliberate and unchanged: a queued workflow cannot silently
adopt a changed policy. What that sentence does not say is the cost when the
change is an *improvement*.

A profile's revision fixes its turn limit, internal timeout, claim lease,
heartbeat and shutdown grace. `select_agent` is the only way to move a binding,
accepts only a revision the registry currently offers, and is valid only in
`awaiting_start`. So a workflow that is `queued`, `running`, `parked`, or
waiting in a later phase has no path to a raised budget — not with reader
consent, not at all. It keeps the budget it was scheduled under for life.

This was invisible. `profile_health` already resolved each pinned revision and
reported `available`, and for a retired revision kept in the store for replay
that flag is *true*: it resolves exactly and runs perfectly. It simply carries
a budget the operator has replaced. On one deployment the installed repository
profile allowed 3300 seconds and 120 turns while the great majority of
workflows were pinned to revisions allowing 2400 and 80, or 1800 and 50. Runs
terminated at the retired timeout and agent transcripts ended at the retired
turn limit. Every row reported healthy, because every row was.

`profile_health` therefore also reports `current`: whether the pinned revision
is the one the registry now offers for that profile. `available` and `current`
answer different questions and are kept separate — a profile the registry has
dropped entirely is a different fault with no remedy of this kind, and
conflating the two would hide the worse one. The delivery-health report carries
the aggregate, at health schema version 2.

This amendment adds reporting only. Whether a workflow may adopt the installed
revision of the profile it already names, and what would trigger that, is
deliberately not decided here: an automatic adoption is the case closest to
contradicting this ADR, and the argument should be made against measured
evidence rather than ahead of it. The reporting exists so that evidence can be
gathered.

## Amendment: adoption is an operator's act, not a configuration side effect

The amendment above gathered evidence and deferred the decision. The evidence
arrived. On one deployment, four retired revisions held twenty-eight workflows,
every one of them queued and none running — a queue entirely composed of work
held to budgets the operator had already replaced, and entirely safe to move.

So a workflow may now adopt the installed revision of the profile it already
names. Three properties keep that from contradicting the pin.

**It never changes which profile a workflow names.** Adoption resolves the
workflow's own `agent_profile_id` and moves only the revision. Choosing a
different profile remains `select_agent`'s question, a reader's decision, and
its `awaiting_start` fence is untouched. The two operations answer different
questions and are deliberately not merged: "which agent should do this?" is a
choice between alternatives, while "should this keep a budget that no longer
exists?" has only one sensible answer once an operator has replaced it.

**It never touches a running workflow.** An agent mid-run holds a claim lease
sized by the revision it started under. Rebinding beneath it would change the
budget of work already in flight, which is the case this ADR's fence exists to
prevent, and that protection is unchanged.

**An operator triggers it, and the ledger says so.** This is the part that was
genuinely undecided, and the alternatives were not equivalent:

- *Automatically, in the scheduler.* Recovers the whole queue with no operator
  action. It is also the case closest to contradicting this ADR's central
  claim, because a configuration change alone would alter queued work. The
  argument that the profile ID is unchanged and the old revision is gone is a
  reasonable one, but it is exactly the argument this ADR exists to force
  someone to make deliberately.
- *A reader control on a card.* Honest, and useless at this scale. A queue of
  hundreds will not be rebound one card at a time.
- *An explicit operation.* Chosen. `foxhound-profile-adoption` is a one-shot
  command, dry run by default, bounded per pass, reporting counts rather than
  task identities. Every adoption increments the workflow version and emits an
  `agent_selected` event, so stale cards retire through the existing path and
  the change is in the ledger rather than inferred from behaviour.

Two rules follow from the same reasoning and are not negotiable in an
implementation:

**A narrowed budget is reported, never silent.** Adoption is allowed to move a
workflow to a *smaller* budget — the installed revision is by definition the one
the operator chose — but a workflow that loses time because its profile was
narrowed is a decision, and the pass says how many that was. Losing budget by
accident and losing it on purpose look identical afterwards; only the report
distinguishes them at the time.

**The pass is bounded and content-free.** Rebinding many workflows retires many
cards, so a pass has a limit and reports what it left behind. It reports
magnitudes only: naming the work would put ledger content wherever the output
is printed.

Automatic adoption remains undecided and is deliberately left so. The explicit
operation is the conservative half; if the counts it reports turn out to be
routine rather than exceptional, that is the evidence a future amendment would
need — and it will have been gathered the same way this one was.
