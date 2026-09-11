# Foxhound

Foxhound will own durable task lifecycle and execution while knowledge systems
remain responsible for discovering task candidates and organizing their source
material.

The first implementation is deliberately limited to versioned task-candidate
and ordered-feed contracts. It has no service connection, scheduler, card
transport, agent runner, or production-data access.

The offline candidate inbox stores validated candidates in an explicitly
selected SQLite database. It does not create active tasks or connect to a
producer. Applications must keep that database in private host-local state,
outside a repository checkout.

An ordered feed page carries a bounded, contiguous producer cursor range. The
inbox atomically stores every candidate in the page, a replay receipt, and the
new cursor. Exact page retries are accepted; gaps, overlaps, altered retries,
and candidate conflicts fail closed without partial writes.

The offline shadow importer can read a GW-owned immutable page ledger into the
inbox without writing to the producer outbox:

```sh
install -d -m 700 /srv/example/private-foxhound-state
python -m foxhound.candidate_feed_import \
  --outbox /srv/example/private-candidate-outbox \
  --database /srv/example/private-foxhound-state/candidate-inbox.sqlite3 \
  --stream-id pilot-alpha
```

The command is manual and content-free in its output. It does not acknowledge
or remove feed pages, create tasks, connect to knowledge data, schedule work,
or dispatch agents.

Run the contract tests with:

```sh
PYTHONPATH=src python3 -m unittest discover -s tests -v
```

See [ADR 0001](docs/architecture/0001-task-boundary.md) for the component
boundary and migration invariants. See
[ADR 0002](docs/architecture/0002-offline-shadow-import.md) for the offline
producer-outbox connection.
