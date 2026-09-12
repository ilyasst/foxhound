# ADR 0021: Staged task-authority cutover

Status: proposed for the pilot cutover.

## Context

Foxhound can already import candidate and comparison feeds, allocate durable
task identities, serve lifecycle and execution-review cards, run gated agent
phases, and export lifecycle outcomes. Those capabilities do not make a safe
authority switch by themselves. A deployment can still create two writers by
leaving a legacy lifecycle job active, expose two card surfaces, or restore an
old scheduler snapshot that silently drops unrelated ingestion work added
after the snapshot was captured.

Candidate discovery and durable task acceptance are also separate boundaries.
The pilot still uses the producer's legacy registry as a temporary creation
bridge: it records the `minted` or `folded` decision that Foxhound verifies
before allocating its own task. That bridge may remain during the first
lifecycle/execution switch, but it is not the final producer-only boundary.

## Decision

The migration uses three explicit stages. A deployment advances only after all
exit conditions for the current stage are proven from private, aggregate
evidence.

### Stage 0: plan-only comparison

The producer remains authoritative for lifecycle, ordinary task cards,
scheduling, and execution. Foxhound may import and reconcile candidates,
schedule its own execution workflow, and generate reviewable plans. Its runner
must atomically restrict claims to `plan`, and lifecycle projection must remain
stopped.

Exit conditions:

- every imported candidate is either bound or has an explicit refusal;
- every migration correlation resolves to an existing task on both sides;
- correlated lifecycle statuses agree;
- no workflow or card delivery claim is abandoned;
- at least one plan and one reader revision or approval are recorded;
- no `execute` or `external_action` claim occurred during the comparison; and
- the prior authority continued without duplicate effects.

### Stage 1: Foxhound lifecycle and execution authority

The switch is performed only while both execution runners are stopped and no
claim is live. The operator captures a new owner-only snapshot of the active
scheduler immediately before editing it. The cutover candidate is derived
from that snapshot, not from a historical candidate, and a structural check
must prove that every unrelated source, pipeline, and knowledge-maintenance
job is byte-for-byte preserved.

The cutover removes the producer's lifecycle closure, stale archival,
inventory, workflow scheduling, workflow agent, and task-review jobs. Source
ingestion, knowledge organization, candidate discovery, and the temporary
creation registry remain. The card gateway switches ordinary task cards to
Foxhound's authenticated loopback service; in that mode the legacy task-card
sweeps must be inactive. Execution-review cards continue through their
separate Foxhound route.

After the producer-side writers and legacy card surface are proven inactive,
the deployment enables, in order:

1. Foxhound task-card scheduling and delivery;
2. Foxhound execution-workflow scheduling;
3. lifecycle outcome export and knowledge projection; and
4. the unrestricted Foxhound runner, with no plan-only phase filter.

The runner remains gated by durable reader decisions. Enabling unrestricted
claim selection does not approve a plan, execution phase, or external action.

Stage 1 is accepted only when one task traverses the intended reader gates,
records its work product, reaches its expected lifecycle state, and projects
that state into the knowledge system without a second writer.

### Stage 2: producer-only candidate boundary

The temporary creation registry and shadow-decision bootstrap are removed only
after Foxhound can explicitly accept current producer candidates, allocate and
deduplicate durable task identity, and handle candidate revisions without
consulting producer task state. The producer then owns only ingestion,
knowledge organization, semantic candidate discovery, bounded read-only
context, and projection of Foxhound outcomes.

The final state has no producer-owned task lifecycle, task card, scheduler,
agent runner, retry state, or durable task identity. Obsolete paths are removed
through reviewed changes after the validation period rather than left as an
accidental fallback.

## Cutover gates

Before Stage 1, private deployment evidence must establish all of the
following:

- the candidate and comparison feed cursors are contiguous and current;
- the bootstrap reports no pending, refused-by-accident, unmapped, divergent,
  or incomplete candidate group;
- correlated task counts and statuses agree, with zero missing rows;
- lifecycle export and projection cursors agree and have no unapplied page;
- Foxhound has no live workflow or card-delivery claim;
- the plan-only canary is resolved;
- every legacy task writer to be removed appears exactly once;
- every retained non-task job appears unchanged in the generated candidate;
- current configuration and scheduler snapshots are owner-only; and
- the rollback procedure has been rehearsed against those exact snapshots.

After Stage 1, evidence must establish:

- all removed producer jobs are absent and retained jobs are still present;
- the gateway has exactly one ordinary task-card authority;
- the Foxhound runner lacks the plan-only restriction and no producer runner
  is active;
- lifecycle projection is advancing only from Foxhound's append-only feed;
- stale callbacks fail closed; and
- every external action still requires a separate reader approval.

No private task text, identifier, source material, scheduler line, path,
hostname, or configuration value belongs in repository or hosting evidence.
Only aggregate counts, boolean invariants, and synthetic reproductions may be
published.

## Rollback

Before Foxhound records a post-switch lifecycle event, rollback is mechanical:
stop Foxhound scheduling, runner, and projection; restore the card gateway to
the previous authority; restore the freshly captured scheduler snapshot; and
verify that the legacy writers each appear exactly once.

After Foxhound records a lifecycle event, restoring legacy writers is not a
mechanical rollback. First stop both sides, project Foxhound outcomes through
the last complete cursor, reconcile every correlated status, and make an
explicit authority decision. Never re-enable a stale producer snapshot over
newer Foxhound state.

A failed agent process, expired lease, or parked workflow is not itself a
reason to transfer authority. Foxhound's retry and recovery rules remain the
owner of that workflow until an explicit reconciled rollback is completed.

## Consequences

The lifecycle/execution switch can happen before the temporary creation bridge
is removed, allowing Foxhound to handle tasks sooner without pretending the
final candidate boundary already exists. The cost is a clearly tracked second
stage before the extraction is complete. Historical cutover candidates are
diagnostic artifacts only; they can never be treated as executable rollback
state.
