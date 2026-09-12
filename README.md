# Foxhound

Foxhound will own durable task lifecycle and execution while knowledge systems
remain responsible for discovering task candidates and organizing their source
material.

The implementation includes versioned candidate and passive shadow-observation
contracts plus a Foxhound-owned durable task ledger. It has no service
connection, scheduler, card transport, agent runner, or production-data
access.

Foxhound can retrieve bounded task context through an explicitly configured,
authenticated, read-only GW search endpoint. The client accepts loopback HTTP
or validated HTTPS, refuses redirects, ignores process proxy settings, and
strictly validates the bounded response before returning private excerpts. It
does not read GW files, persona configuration, environment, or writable state.

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

After candidates have been imported, a separate read-only adapter can apply a
GW-owned shadow-observation ledger and report aggregate comparisons:

```sh
python -m foxhound.task_shadow_feed_import \
  --outbox /srv/example/private-observation-outbox \
  --database /srv/example/private-foxhound-state/candidate-inbox.sqlite3 \
  --stream-id pilot-alpha
```

The observation importer uses its own producer lock and cursor. It does not
change producer files or activate any candidate.

For repeated operation, Foxhound provides one ordered cycle that imports both
ledgers under an exclusive local lock and appends an immutable aggregate
success receipt only after both imports complete:

```sh
python -m foxhound.shadow_cycle \
  --candidate-outbox /srv/example/private-candidate-outbox \
  --observation-outbox /srv/example/private-observation-outbox \
  --database /srv/example/private-foxhound-state/foxhound.sqlite3 \
  --stream-id example-shadow
```

A failure between the two imports leaves no false success receipt; the next
cycle replays the committed candidate prefix and resumes safely. This command
still does not bootstrap tasks, schedule itself, create cards, or run agents.

After a comparison ledger is complete, an application may explicitly invoke
the task ledger's shadow bootstrap. Imports never invoke it. Only current,
agreed mapped observations can become tasks, and Foxhound allocates its own
task identity while retaining legacy grouping as private migration state.
Lifecycle transitions are optimistic-version fenced and append immutable
events. See [ADR 0006](docs/architecture/0006-durable-task-ledger.md).

Run the contract tests with:

```sh
PYTHONPATH=src python3 -m unittest discover -s tests -v
```

See [ADR 0001](docs/architecture/0001-task-boundary.md) for the component
boundary and migration invariants. See
[ADR 0002](docs/architecture/0002-offline-shadow-import.md) for the offline
producer-outbox connection. See
[ADR 0003](docs/architecture/0003-task-shadow-observation.md) for the passive
candidate-to-legacy-task observation contract. See
[ADR 0004](docs/architecture/0004-passive-shadow-inbox.md) for ordered durable
observation ingestion and content-free comparison reports. See
[ADR 0005](docs/architecture/0005-shadow-observation-import.md) for the
read-only observation-ledger adapter. See
[ADR 0006](docs/architecture/0006-durable-task-ledger.md) for durable task
identity, lifecycle, and the explicit shadow-bootstrap boundary.
See [ADR 0007](docs/architecture/0007-bounded-gw-knowledge-client.md) for the
read-only knowledge retrieval boundary.
See [ADR 0008](docs/architecture/0008-shadow-import-cycle.md) for the ordered,
overlap-safe passive import cycle and its durable success receipts.
