# ADR 0058: Bounded scheduling conditions and reversible change sets

## Status

Accepted for the local scheduling ledger. Researcher integration remains a
separate change.

## Decision

Foxhound represents prerequisites and time holds as eligibility conditions,
not arbitrary list positions. Version 1 admits only:

- `after_task_completed`: hold one task until another local task is done;
- `not_before`: hold one task until an absolute timezone-aware timestamp.

A task may have at most three active/review-needed conditions, including at
most two prerequisite edges and one time hold. Dependency chains are limited
to eight edges and cycles are rejected before any row is written. A dropped or
missing prerequisite moves its dependent condition to `needs_review`; it never
silently releases work. Any evaluator error fails closed for that task while
allowing the bounded queue scan to consider unrelated work.

The execution claim transaction refreshes and checks conditions before it can
claim a workflow. Condition and change-set transitions have append-only event
rows.

## Trusted recommendation boundary

`TaskSchedulingService.apply(ValidatedSchedulingRecommendation)` is the only
integration interface intended for a future Researcher. The object must first
be produced by deterministic Foxhound validation. It contains a closed kind,
task and workflow version fences, a bounded rationale, and broker-issued source
references. It is not a model tool and it accepts no arbitrary operation name.

The four accepted change-set kinds are:

- add an `after_task_completed` condition;
- add a `not_before` condition;
- raise an immediately claimable workflow from normal to the existing raised
  tier;
- create one bounded local prerequisite and add the dependency condition.

Automated priority never overwrites a non-normal reader preference. It cannot
move running, completed, cancelled, or review-waiting work. Creating a
prerequisite does not grant execution authority; normal workflow scheduling
decides how that new task is surfaced.

## Notification and Undo

Applying a recommendation atomically creates its change set, conditions or
priority mutation, optional prerequisite, audit event, and a durable scheduling
review card. The card offers `Undo` and `Keep`.

Scheduling cards use their own delivery stream and never consume ordinary task
or execution review cards. A card moves through `pending`, `delivering`,
`delivered`, and `resolved`. Claims use bounded leases and opaque capability
tokens whose SHA-256 digests are the only token material stored in SQLite.
Expired claims return to `pending` with a new card version. Delivery failure
also requeues with a new version; repeating the same successful delivery
acknowledgement is idempotent.

The authenticated loopback service exposes separate scheduling claim,
delivered, delivery-failed, and action routes. Its rendered callback payload is
`fhs|<card-id>|<version>|<keep-or-undo>`, bounded to the transport limit. Keep
or Undo is refused until delivery has been durably acknowledged, and every
action remains version-fenced. The notification says what changed, identifies
the involved task or date, provides the bounded rationale, and states that the
change is already active.

Undo checks the original task version, the exact resulting workflow version,
the change-set state, and any automated priority value before changing
anything. A newly created prerequisite can be dropped only while it remains at
task version 1, open, with its workflow still pristine and unclaimed. If any
fence changed, Foxhound records
`undo_conflict` and performs no inverse operation. It never partially undoes a
compound change.

## Deliberate exclusions

This ledger does not connect a model, Researcher profile, retrieval source, or
internet capability. It does not assign numeric queue positions, modify owner
or due-date fields, reorder unrelated tasks, interrupt running work, or infer
authority from cited evidence. Those remain separate policy decisions.
