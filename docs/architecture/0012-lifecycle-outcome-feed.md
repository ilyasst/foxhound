# ADR 0012: Correlated lifecycle outcome feed

Status: accepted for the pre-cutover pilot.

## Context

Foxhound records optimistic-version-fenced lifecycle transitions and immutable
task events. Once Foxhound owns lifecycle decisions, GW must be able to reflect
those outcomes in knowledge views without reading Foxhound's database or
becoming a second lifecycle authority.

## Decision

Foxhound provides a manual offline exporter for a version 1
`foxhound.task-lifecycle-outcome-feed`. Only `status_changed` events for tasks
with an existing `gw` bootstrap correlation enter this migration stream.
Foxhound-native tasks and non-status events remain outside it.

The delivery cursor is contiguous over projectable outcomes. Each outcome also
retains its original immutable Foxhound event sequence, task identity, task
version, GW legacy task correlation, transition endpoints, and occurrence
time. It carries no task text, owner, project, candidate identity, source
record, evidence, card identity, or execution content.

The existing outbox is revalidated as an exact prefix of current immutable
Foxhound state before an append. Pages use canonical JSON, bounded item and
byte counts, private permissions, an exclusive advisory lock, and atomic link
publication. Exact retries append nothing. Changed history, a cursor mismatch,
an unsafe path, malformed state, unknown files, or mixed streams fail closed.

The producer writes pages only. It never connects to GW, changes a knowledge
document, acknowledges consumption, schedules itself, handles a card, or runs
an agent. GW must independently validate and receipt this stream in a separate
change before the task-card cutover can activate.

## Authority and rollback

The feed is a record of decisions already committed by Foxhound. A consumer
may derive knowledge projections from it but must not interpret its own cached
projection as permission to transition a Foxhound task.

Before any lifecycle event exists, rollback is to stop export and remove an
empty private outbox. Afterwards, immutable event pages must be retained until
an acknowledgement and compaction protocol is designed. Stopping export does
not undo or transfer Foxhound lifecycle authority.

## Operator contract

The database and outbox must be absolute, private, real paths outside Git. The
outbox must already exist and its stream identity must remain stable:

```sh
install -d -m 700 /srv/example/private-lifecycle-outbox
python -m foxhound.task_lifecycle_outcome_export \
  --database /srv/example/private-foxhound-state/foxhound.sqlite3 \
  --outbox /srv/example/private-lifecycle-outbox \
  --stream-id pilot-alpha
```

Success output contains counts and cursors only. Failure output is generic.
