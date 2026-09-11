# ADR 0001: Task ownership and knowledge integration boundary

Status: accepted for the staged extraction.

## Context

Knowledge ingestion, task lifecycle, card interaction, scheduling, and agent
execution currently share implementation and persistence. Moving all of them
at once would make rollback difficult and could create two writers for the same
task.

Meeting and email processing also use the word "creation" for two different
operations:

1. discovering a possible action in source material; and
2. accepting that candidate as a durable task with identity and lifecycle.

Those operations need different owners.

## Decision

Source systems own ingestion, knowledge organization, and semantic discovery of
task candidates. Foxhound owns acceptance of a candidate as a durable task and
all subsequent task behavior.

The initial producer is identified as `gw`. Its meeting candidates may
originate in an upstream meeting protocol, while its email candidates may be
derived during email ingestion. That provenance does not transfer durable task
ownership back to the producer.

Foxhound owns:

- durable task identity and deduplication;
- task status, ownership, dates, confirmations, and history;
- task cards and task-specific replies;
- scheduling, workflow gates, claims, leases, and retries;
- prompt construction, agent execution, and private work products.

The knowledge system owns:

- source ingestion and classification;
- knowledge-base documents, entities, and project organization;
- semantic extraction of task candidates;
- bounded, read-only context retrieval;
- projection of Foxhound lifecycle outcomes into knowledge documents.

## Integration contracts

The first contract is `foxhound.task-candidate` version 1. It contains a small
action description and opaque source references. It contains no source body,
transcript, email address, filesystem path, host detail, environment value, or
credential.

Candidate identity is derived from:

```text
source.system + source.kind + source.record_id + source.item_id
```

The identifier is `tc_` followed by the lowercase SHA-256 digest of the UTF-8
encoding of that four-value JSON array, serialized without spaces. The
synthetic identity `["gw","meeting","record-001","action-01"]` therefore
produces the test-vector identifier ending in `4bbd8df4200fb56d...`.

`source.revision` is intentionally excluded. A changed revision updates the
same candidate; it does not create a duplicate durable task.

`created_at` is the time the producer first emitted the candidate, not the time
of the source material. It is preserved across retries.

Future boundaries will use separate contracts for lifecycle events and bounded
knowledge context. A task candidate is not permission to read producer state.

## Invariants

1. Foxhound and a producer never share a writable database.
2. Exactly one component is authoritative for durable task state at a time.
3. Delivery is idempotent and safe to retry.
4. Unknown contract versions and additional fields fail closed.
5. Validation errors identify fields but never echo rejected values.
6. Foxhound does not read producer environment files or persona files.
7. Knowledge documents are written only by their owning knowledge system.
8. Task cards and agent execution remain disabled until an explicit later
   cutover.
9. Tests, fixtures, documentation, and reports use only synthetic data.
10. A digest derived from operational identifiers is still operational data;
    it must not be treated as anonymized or published.

## Migration

The first phase validates candidate documents without connecting to a producer.
Later phases will add producer export, shadow import and comparison, execution
handoff, card handoff, authority cutover, and legacy removal in that order.

Each phase must have an independently tested rollback and must complete on one
pilot installation before any broader rollout.

## Out of scope

This decision does not add persistence, networking, task mutation, cards,
scheduling, agent execution, production-data access, or knowledge projection.
