# Foxhound

Foxhound will own durable task lifecycle and execution while knowledge systems
remain responsible for discovering task candidates and organizing their source
material.

The implementation includes versioned candidate and passive shadow-observation
contracts, a Foxhound-owned durable task ledger, and transport-neutral durable
task review cards. Its only task-card network surface is an opt-in
authenticated loopback service. It has no chat transport, recurring host
scheduler, or implicit production-data access. Its execution runner is an
explicit one-shot command and performs no work unless a workflow has already
passed its reader gate.

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
agreed mapped observations can normally become tasks. An owner-only divergence
may also become a task when the explicitly supplied GW client returns an
identity-bound speaker-merge attestation and the resulting comparable digest
exactly matches the immutable observation. Foxhound persists that evidence
append-only, allocates its own task identity, and retains legacy grouping only
as private migration state. Resolver calls happen outside the database write
transaction and the complete input snapshot is revalidated before commit.
Lifecycle transitions are optimistic-version fenced and append immutable
events. See [ADR 0006](docs/architecture/0006-durable-task-ledger.md) and
[ADR 0009](docs/architecture/0009-owner-equivalence-bootstrap.md).

The same operation is available as a content-free one-shot command for a host
scheduler. Both the database and token must be in private directories:

```sh
foxhound-task-bootstrap \\
  --database /srv/example/private-foxhound-state/foxhound.sqlite3 \\
  --gw-endpoint http://127.0.0.1:8787 \\
  --gw-alias example-operator \\
  --gw-token-file /srv/example/private-foxhound-state/knowledge.token
```

It neither imports feeds nor schedules cards or execution. See
[ADR 0018](docs/architecture/0018-one-shot-shadow-bootstrap.md).

The final candidate boundary replaces that temporary legacy bootstrap with a
one-way native intake activation. Activation fixes the exact reconciled feed
cursor; every later contiguous candidate is then accepted by stable candidate
identity without consulting producer task state:

```sh
foxhound-native-intake activate \
  --database /srv/example/private-foxhound-state/foxhound.sqlite3 \
  --stream-id pilot-alpha \
  --expected-cursor 42

foxhound-native-intake run \
  --database /srv/example/private-foxhound-state/foxhound.sqlite3 \
  --stream-id pilot-alpha \
  --limit 100
```

Activation refuses an unreconciled historical prefix and permanently disables
the legacy shadow bootstrap for that producer. A run advances one bounded,
contiguous page in a transaction. New candidate identities create at most one
task; later revisions update only the accepted open task and increment its
version, making older cards and workflows stale. Producer task decisions,
folded bindings, terminal tasks, gaps, and contradictory state fail closed
without advancing the intake cursor. Both commands report aggregate metadata
only. See [ADR 0022](docs/architecture/0022-native-candidate-intake.md).

Correlated status transitions can be exported manually as a content-free,
append-only offline feed for a knowledge system to project. Only tasks carrying
the temporary GW bootstrap correlation enter this stream; native Foxhound
tasks do not. The exporter neither connects to nor mutates the consumer:

```sh
python -m foxhound.task_lifecycle_outcome_export \
  --database /srv/example/private-foxhound-state/foxhound.sqlite3 \
  --outbox /srv/example/private-lifecycle-outbox \
  --stream-id pilot-alpha
```

See [ADR 0012](docs/architecture/0012-lifecycle-outcome-feed.md).

An application may explicitly schedule review cards for open tasks, claim one
due card under a bounded delivery lease, render it for a private card surface,
and acknowledge delivery before accepting a reader action. Done and Drop are
atomic with the task lifecycle transition; Still open schedules a later
review; Snooze defers the same card for exactly three days. Card and task
versions reject stale callbacks without partial writes. Schema initialization
does not create or deliver cards. See
[ADR 0010](docs/architecture/0010-task-review-cards.md).

Foxhound also provides an opt-in authenticated loopback service for a trusted
local card gateway. It exposes only scheduling, one leased claim, delivery
acknowledgement/failure, and reader action. The database must already be
migrated, every application request is strict and authenticated, and access
logs contain no task/card identifiers or content. Starting the service creates
no cards. Its aggregate stats route lets a gateway cap on-screen delivery
without listing private tasks or cards. See
[ADR 0011](docs/architecture/0011-local-task-card-service.md).

Foxhound now also owns a transport-neutral execution workflow ledger. An
explicitly scheduled open task stops at a reader start gate, then advances
through separately approved plan, execution, and external-action phases under
optimistic versions and digest-fenced expiring claims. Results are bounded,
private, immutable records; failures cool down and eventually park. The ledger
does not launch an agent, call GW, or close a task. See
[ADR 0013](docs/architecture/0013-task-execution-workflows.md).

The authenticated read-only GW client can also retrieve one digest-bound,
allowlisted execution-context snapshot. It includes only the display name,
operator context, self aliases, and institution domains for the configured
alias. It excludes task state, paths, environment variables, credentials,
machine inventory, and producer configuration. See
[ADR 0014](docs/architecture/0014-execution-context-client.md).

Foxhound provides a one-shot supervised runner for one queued execution phase.
It claims the durable workflow before starting a disposable agent, renews the
lease, discards agent stdout and stderr, and maps process startup, exit,
timeout, interruption, and lease failures into the workflow's retry policy.
The agent receives no claim capability in its prompt, arguments, output, or
result draft. It can obtain only the current private work context, bounded GW
search, durable result recording, and claim release through the narrow
`foxhound-task-worker` command. Result identity and all task/workflow fencing
are injected from owner-only run state rather than trusted from agent output.

The runner neither schedules itself nor advances reader gates. A deployment
must keep the database, run directory, and knowledge token in approved private
host-local state, initialize them separately, and invoke the runner only after
the matching GW execution-context provider is available. A synthetic shape is:

```sh
foxhound-execution-runner \
  --database /srv/example/private-foxhound-state/foxhound.sqlite3 \
  --run-root /srv/example/private-foxhound-state/execution-runs \
  --gw-endpoint http://127.0.0.1:8787 \
  --gw-alias example-operator \
  --gw-token-file /srv/example/private-foxhound-state/knowledge.token \
  --agent-profile-directory /srv/example/private-agent-profiles
```

Starting this command with no queued workflow exits successfully without
launching an agent. See
[ADR 0015](docs/architecture/0015-supervised-execution-runner.md).

Agent definitions are strict, revisioned Foxhound profiles. The built-in
`general` profile preserves the existing Hermes policy. A deployment may load
additional reviewed JSON manifests from an absolute owner-only directory
outside Git, then inspect their content-free identity and policy metadata:

```sh
foxhound-agent-profiles --directory /srv/example/private-agent-profiles validate
foxhound-agent-profiles --directory /srv/example/private-agent-profiles list
foxhound-agent-profiles --directory /srv/example/private-agent-profiles show general
```

Private manifests cannot provide executable commands, environment variables,
secrets, or arbitrary tool names. Prompts and private paths are never emitted
by these commands. Profile selection is intentionally not inferred from task
content. See [ADR 0023](docs/architecture/0023-agent-profile-registry.md).

Every execution workflow stores the selected profile ID and exact revision.
The runner resolves that immutable evidence within atomic claim selection,
then derives the Hermes prompt, tools, turn limit, timeout, lease, heartbeat,
and shutdown grace from the profile. Missing, changed, or phase-ineligible
profiles fail closed without claiming or launching an agent. There are no
runner-level overrides for those profile policies. See
[ADR 0024](docs/architecture/0024-workflow-agent-binding.md).

For a parallel comparison in which Foxhound may prepare plans but must never
execute work, restrict the runner's atomic claim selection:

```sh
foxhound-execution-runner \
  --database /srv/example/private-foxhound-state/foxhound.sqlite3 \
  --run-root /srv/example/private-foxhound-state/execution-runs \
  --gw-endpoint http://127.0.0.1:8787 \
  --gw-alias example-operator \
  --gw-token-file /srv/example/private-foxhound-state/knowledge.token \
  --allowed-phase plan
```

The option is repeatable. Omitting it preserves the normal all-phase runner.
Disallowed queued work remains untouched and no agent is launched for it.
Phase restriction does not change lifecycle or card authority; a shadow
deployment must separately keep lifecycle projection disabled. See
[ADR 0020](docs/architecture/0020-phase-restricted-runner.md).

A production authority switch is a separate, staged operation. The pilot
first proves plan-only behavior, then transfers lifecycle, cards, and execution
while preserving candidate discovery, and finally removes the temporary
producer-side creation bridge. Scheduler rollback material must always be
captured from current state; an older candidate may omit unrelated ingestion
jobs added later. See
[ADR 0021](docs/architecture/0021-staged-task-authority-cutover.md).

Before Stage 1, prepare the scheduler candidate from a freshly captured,
owner-only snapshot. The command refuses to continue unless all six legacy
task-writer jobs and the retained temporary creation registry each appear
exactly once. It emits only counts and digests; the snapshot and both artifacts
remain outside the repository:

```sh
install -d -m 700 /srv/example/private-cutover
foxhound-scheduler-cutover prepare \
  --snapshot /srv/example/private-cutover/scheduler.current \
  --candidate /srv/example/private-cutover/scheduler.stage1 \
  --rollback /srv/example/private-cutover/scheduler.rollback
foxhound-scheduler-cutover verify \
  --snapshot /srv/example/private-cutover/scheduler.current \
  --candidate /srv/example/private-cutover/scheduler.stage1 \
  --rollback /srv/example/private-cutover/scheduler.rollback
```

The command does not inspect or install the active scheduler. Installing the
candidate and restoring the rollback artifact remain explicit operator steps.
After native candidate intake has been activated, capture the then-current
Stage 1 scheduler and explicitly prepare Stage 2. This mode refuses any
remaining Stage 1 writer and removes the temporary creation registry exactly
once:

```sh
foxhound-scheduler-cutover prepare --stage stage2 \
  --snapshot /srv/example/private-cutover/scheduler.stage2-current \
  --candidate /srv/example/private-cutover/scheduler.stage2 \
  --rollback /srv/example/private-cutover/scheduler.stage2-rollback
foxhound-scheduler-cutover verify --stage stage2 \
  --snapshot /srv/example/private-cutover/scheduler.stage2-current \
  --candidate /srv/example/private-cutover/scheduler.stage2 \
  --rollback /srv/example/private-cutover/scheduler.stage2-rollback
```

Newly accepted open tasks can be projected to the reader Start gate with an
explicit bounded pass:

```sh
foxhound-execution-schedule \
  --database /srv/example/private-foxhound-state/foxhound.sqlite3 \
  --limit 100
```

The command schedules only tasks that have never had an execution workflow;
it cannot reset a completed, cancelled, parked, or otherwise existing
workflow. It does not advance Start or launch an agent. See
[ADR 0019](docs/architecture/0019-new-task-execution-scheduler.md).

Execution gates have their own transport-neutral durable review cards. An
explicit scheduling pass projects only workflows currently awaiting start,
plan review, external-action review, or final-result review. Each card is
bound to exact task, workflow, phase, and result state; delivery uses an
expiring digest-fenced claim. Plan and result cards can request more
investigation, collect one private discussion instruction, execute, snooze for
a bounded interval, complete, reassign, or drop as appropriate. External
effects still require their own exact authorization. Every delivered decision
advances all affected task and workflow state and resolves the card in one
SQLite transaction, so a stale or failed input changes nothing. Card rendering
is HTML-escaped and transport-bounded. If the complete private content cannot
fit, approval and completion are absent from the keyboard and forged
affirmative callbacks are refused. See
[ADR 0016](docs/architecture/0016-execution-review-cards.md).

The authenticated loopback card service exposes execution cards through a
separate `/v1/execution-cards/*` route family. A trusted local gateway can read
aggregate stats, run the explicit scheduler, claim one rendered card,
acknowledge or retry delivery, submit one versioned reader action, and submit
one bounded discussion or reassignment response. The
existing `/v1/task-cards/*` contracts are unchanged. Starting the service
still creates no card and advances no gate. See
[ADR 0017](docs/architecture/0017-execution-card-service.md).

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
See [ADR 0009](docs/architecture/0009-owner-equivalence-bootstrap.md) for the
bounded owner-equivalence attestation and fail-closed bootstrap rules.
See [ADR 0010](docs/architecture/0010-task-review-cards.md) for durable card
scheduling, delivery leases, reader actions, and activation rollback.
See [ADR 0011](docs/architecture/0011-local-task-card-service.md) for the
authenticated loopback card-gateway boundary.
See [ADR 0012](docs/architecture/0012-lifecycle-outcome-feed.md) for the
content-free correlated lifecycle outcome export boundary.
See [ADR 0013](docs/architecture/0013-task-execution-workflows.md) for durable
execution scheduling, gates, claims, results, and retry state.
See [ADR 0014](docs/architecture/0014-execution-context-client.md) for the
strict allowlisted GW persona-variable boundary.
See [ADR 0015](docs/architecture/0015-supervised-execution-runner.md) for the
one-shot runner, private worker capability, and failure boundary.
See [ADR 0016](docs/architecture/0016-execution-review-cards.md) for durable
execution-gate delivery and atomic reader decisions.
See [ADR 0017](docs/architecture/0017-execution-card-service.md) for the
authenticated loopback execution-card adapter.
See [ADR 0018](docs/architecture/0018-one-shot-shadow-bootstrap.md) for the
explicit deployment boundary around verified shadow activation.
See [ADR 0019](docs/architecture/0019-new-task-execution-scheduler.md) for the
bounded projection of new open tasks to the reader Start gate.
See [ADR 0020](docs/architecture/0020-phase-restricted-runner.md) for the
atomic phase allowlist used by plan-only parallel comparisons.
See [ADR 0021](docs/architecture/0021-staged-task-authority-cutover.md) for the
staged transfer of lifecycle, card, execution, and candidate authority.
See [ADR 0022](docs/architecture/0022-native-candidate-intake.md) for the
one-way producer-independent candidate acceptance boundary.
