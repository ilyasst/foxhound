# ADR 0022: Producer-independent candidate intake

Status: accepted for the final task-authority boundary.

## Context

The migration bootstrap verifies producer-owned task decisions before
allocating Foxhound task identity. That is useful for reconciling the existing
task population, but it leaves the producer involved in new task acceptance.
The final boundary requires the producer to discover and order candidates
without minting task identity, choosing task lifecycle, or supplying a legacy
task decision.

Candidate import is deliberately passive. Turning every imported current row
into a task would lose delivery order, make a partial retry ambiguous, and
allow an old page to be mistaken for post-cutover work. A durable boundary and
an exact ordered suffix are required.

## Decision

Schema version 10 records immutable provenance for every newly imported
candidate-feed item as `(producer, stream, sequence, candidate identity,
revision)`. Migration creates no provenance for historical pages and creates
no tasks. Existing inbox, task, card, workflow, and event state is preserved.

An explicit activation fixes the current candidate-feed cursor as the
historical prefix. It succeeds only when:

- the producer and stream identity and expected cursor are valid;
- the expected cursor exactly equals the durable feed cursor; and
- every current producer candidate is either bound at its current revision or
  has an explicit historical refusal.

When a comparison is divergent but an operator has independently decided to
preserve the legacy task, `refuse-divergent` can record that historical
candidate revision as `preserved_legacy_owner`. The operation is deliberately
narrow: it runs only before activation, selects current active and unbound
divergent revisions from one producer stream, and requires the aggregate count
to equal an explicit expectation before writing anything. Its rows cannot be
updated or deleted. An exact retry is unchanged; any other historical
divergence continues to block activation.

The activation record and its append-only event are committed atomically.
Its producer, stream, boundary cursor, and activation time cannot change, and
the record cannot be deleted. An exact activation retry is unchanged; a
different cursor is refused. Once any stream is activated for a producer, the
legacy shadow bootstrap refuses before it can call an owner resolver and
rechecks the fence in its write transaction.

After activation, an explicit bounded intake pass reads only immutable feed
provenance immediately after its durable cursor. It requires a contiguous
sequence and exact candidate revision history. In one transaction it:

- creates one open Foxhound task and accepted binding for a new candidate
  identity;
- treats an exact already-accepted revision as unchanged;
- applies a later revision only to that accepted open task, updating its text,
  owner, and due value and incrementing the task version; and
- advances the native cursor and appends an aggregate intake event.

A guarded cutover may have a finite open backlog that predates source-level
candidate handoffs. The producer may offer those records once with the
explicit `legacy` source kind. This prevents a migrated task from masquerading
as a meeting, email action, or forge issue. It still crosses the same immutable
feed and ordered native-intake transaction, receives no pre-authorization, and
starts behind the ordinary lifecycle and execution gates. The kind is a
migration origin, not permission for the producer to retain a task registry.

A post-boundary producer task observation, missing provenance, non-contiguous
sequence, missing task, folded binding, terminal task, malformed stored
candidate, or any other contradiction refuses and rolls back the complete
pass. Native intake does not infer semantic equivalence between distinct
candidate identities. Any future merge policy requires its own explicit,
auditable contract.

Task versioning is the invalidation boundary. A revision makes cards and
workflows bound to an earlier version stale; their existing guards prevent an
old approval, callback, or result from acting on revised content.

The `foxhound-native-intake` one-shot command exposes separate
`refuse-divergent`, `activate`, and `run` operations. It requires an absolute
regular database file in an
owner-only directory and emits only dispositions, cursors, and aggregate
counts. It does not connect to the producer, inspect knowledge or persona
state, schedule cards or workflows, run an agent, or perform lifecycle work.

## Deployment boundary

Activation is the final Stage 2 authority switch, not a routine migration
step. Before activation, operators stop the producer creation registry and
shadow-decision bootstrap, import their final complete prefix, reconcile every
existing candidate and task, and pass the exact current cursor to activation.
Only after activation may the native intake command be scheduled after the
candidate importer.

The database schema migration is passive and can be deployed earlier.
Rollback before activation is to leave native intake unscheduled. Activation
is one-way within the state database. After activation, and especially after a
post-boundary candidate is accepted, producer task creation must not resume
without stopping both paths and performing an explicit state reconciliation.

## Consequences

GW can remain the knowledge and semantic-candidate producer while Foxhound
becomes the only durable task-acceptance authority. Ordered provenance,
transactional cursor movement, immutable audit events, and task-version fences
make retries deterministic and contradictions visible. The stricter boundary
also intentionally turns an accidental post-cutover legacy decision into an
operational refusal instead of silently selecting one authority.
