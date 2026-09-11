# Foxhound

Foxhound will own durable task lifecycle and execution while knowledge systems
remain responsible for discovering task candidates and organizing their source
material.

The first implementation is deliberately limited to the versioned task
candidate contract. It has no service connection, scheduler, card transport,
agent runner, or production-data access.

Run the contract tests with:

```sh
PYTHONPATH=src python3 -m unittest discover -s tests -v
```

See [ADR 0001](docs/architecture/0001-task-boundary.md) for the component
boundary and migration invariants.
