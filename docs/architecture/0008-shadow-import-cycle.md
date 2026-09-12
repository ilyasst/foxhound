# ADR 0008: Ordered Foxhound shadow import cycle

Status: accepted for the pre-cutover pilot.

## Context

Foxhound has separate read-only adapters for GW candidate and shadow-
observation outboxes. Running them independently leaves ordering, overlap
protection, partial-failure recovery, and operator reporting to deployment
scripts. A recurring service needs one safe application boundary before any
task, card, or worker authority can move.

## Decision

The ``foxhound.shadow_cycle`` command runs one bounded import cycle:

1. validate the two producer outboxes and Foxhound state location;
2. acquire a non-blocking exclusive lock adjacent to the selected database;
3. import the complete candidate ledger;
4. import the complete observation ledger;
5. append a content-free success receipt to the Foxhound database.

The outboxes must be distinct, existing private directories outside Git
worktrees. The database parent must have the same privacy boundary and cannot
be inside either producer outbox. The lock is a mode-600 regular file named
``.foxhound-shadow-cycle.lock`` in the database parent.

Schema version 5 adds append-only ``shadow_import_cycles`` receipts. Each row
contains only the stream identity, start and completion timestamps, before and
after cursors, import counts, and aggregate comparison counts. Database
triggers refuse receipt updates and deletion.

If the candidate import commits and the observation import then fails, no
cycle receipt is written. The next invocation safely replays the candidate
pages, resumes observation import, and records success only after both sides
complete. Exact successful retries are also receipted because they prove that
both complete producer ledgers were validated at that time.

## Operator contract

```sh
python -m foxhound.shadow_cycle \
  --candidate-outbox /srv/example/private-candidate-outbox \
  --observation-outbox /srv/example/private-observation-outbox \
  --database /srv/example/private-foxhound-state/foxhound.sqlite3 \
  --stream-id example-shadow
```

Standard output is an aggregate JSON summary. Failure output is generic and
does not expose source paths, task content, stream identity, or underlying
exception values.

## Boundary

This command does not create or accept tasks, invoke shadow bootstrap, change
task lifecycle, render cards, schedule itself, query GW knowledge, acknowledge
or modify producer files, or execute agents. It is the repeatable passive
consumer cycle that a later scheduler may invoke.
