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

Run the contract tests with:

```sh
PYTHONPATH=src python3 -m unittest discover -s tests -v
```

See [ADR 0001](docs/architecture/0001-task-boundary.md) for the component
boundary and migration invariants.
