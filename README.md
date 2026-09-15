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
task; later task-content revisions update only the accepted open task and
increment its version, making older cards and workflows stale. A version-4
revision that changes only bounded meeting-source provenance advances the
binding without advancing the task version, so active work is preserved.
Producer task decisions,
folded bindings, terminal tasks, gaps, and contradictory state fail closed
without advancing the intake cursor. Both commands report aggregate metadata
only. See [ADR 0022](docs/architecture/0022-native-candidate-intake.md).

Lifecycle-aware candidate version 3 distinguishes an action revision from an
explicit evidence withdrawal and orders each identity with a monotonic
generation. Withdrawal never completes or deletes a Foxhound task: untouched
open tasks are withheld from cards and execution, while reader-modified or
already-active tasks are preserved as explicit conflicts. Existing version 1
and 2 producers retain active generation-zero behavior. See
[ADR 0023](docs/architecture/0023-candidate-lifecycle.md).

Meeting candidate version 4 carries one to three validated source basenames
with bounded supporting extracts. Cards render those readable sources instead
of an opaque record identifier. Versions 1 through 3 remain accepted. See
[ADR 0032](docs/architecture/0032-readable-task-source-provenance.md).

Candidate version 5 separates the owner label shown on cards from a scoped
owner reference. Foxhound persists the observed and canonical speaker IDs with
their registry, resolution kind, confidence state, and human pin. Existing
candidate versions remain accepted as explicitly provisional legacy owner
labels. A reader reassignment is pinned and cannot be replaced by a later
producer revision. See
[ADR 0033](docs/architecture/0033-structured-task-owner-identity.md).

Meeting, email, Teams, and forge-issue candidates retain their actual source
kind. A
bounded authority cutover may additionally use the explicit `legacy` kind for
still-open backlog records that predate those handoffs; it receives no special
authority and must not become a permanent producer task registry.

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

Every configured bearer token is paired with exactly one role from a closed
set: `drip` (the existing chat gateway's pattern) or `queue_view` (reserved
for a future console; no route uses it yet). A request's role is always the
role of whichever configured token authenticated it — no route reads or
trusts a role, scope, or consumer field from the request itself. A single
`--token-file PATH` needs no change and no new configuration: with exactly
one token configured, it is the `drip` role, exactly reproducing today's
behavior. Configuring more than one token requires naming each one's role
explicitly, `ROLE=PATH`:

```sh
foxhound-task-cards \
  --database /srv/example/private-foxhound-state/foxhound.sqlite3 \
  --token-file drip=/srv/example/private-foxhound-state/card-gateway.token \
  --token-file queue_view=/srv/example/private-foxhound-state/console.token \
  --bind 127.0.0.1 \
  --port 8790
```

A configured token whose role cannot be resolved to `drip` or `queue_view`
never lets the server start — construction validates every configured
role against the closed set up front, so this can only arise from a
configuration defect, and the service refuses to guess through it rather
than starting in a state it cannot resolve. Configuration maps each role
to exactly one token, so two tokens cannot share a role; two callers of
the same role share that one token file. See
[ADR 0036](docs/architecture/0036-consumer-scoped-card-claims.md) decision 1.

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
The agent receives no claim capability in its instructions, arguments, output,
or result draft, and no instructions in its arguments either. Its command line
carries only a public bootstrap requiring the first `foxhound-task-worker
context` call; that call returns the instructions of the revision the claim is
pinned to, from an owner-only file the runner writes into the run directory and
removes when supervision ends. Hermes is launched with `--ignore-rules`, so
ambient rule, memory, and skill injection cannot change what a recorded
revision means, and the agent is told to read each checkout's own contributor
instructions after a worktree is prepared. It can obtain only the current
private work context, bounded GW search, durable result recording, and claim
release through the narrow `foxhound-task-worker` command. See
[ADR 0028](docs/architecture/0028-fenced-instruction-delivery.md). Result identity and all task/workflow fencing
are injected from owner-only run state rather than trusted from agent output.
Before recording, the worker can construct a correctly named draft from fixed
owner-only summary, work, and optional JSON-array files beside that run
state. Private result content is never passed in command-line arguments, and
the existing strict hand-authored draft path remains compatible. A plan-phase
release with result inputs validates and records them as `awaiting_plan`;
later phases refuse to discard inputs because their outcome cannot be inferred
safely. Work-context
version 4 also reports authoritative local calendar anchors, exact profile
toolsets, and phase-derived worker capabilities so versioned guidance does not
depend on ambient Hermes configuration or model date arithmetic. See
[ADR 0030](docs/architecture/0030-versioned-runtime-guidance.md).

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
  --agent-profile-directory /srv/example/private-agent-profiles \
  --task-work-root /srv/example/private-sync/ProjectAlpha/Tasks \
  --task-kb-root /srv/example/private-sync/ProjectAlpha-KB/Tasks
```

The two task roots are an inseparable, explicit per-machine mapping. Each run
creates `T<id>-<slug>/` under the working root and `T<id>-<slug>.md` under the
KB root. The folder retains its human README, standard result files, transcript,
and only agent files explicitly named in `result-artifacts.json`; run state and
compiled instructions are never copied. Result cards show both paths and
reviewable forge links before the detailed work. Omitting both options keeps
the pre-archive behavior; providing only one fails configuration. See
[ADR 0031](docs/architecture/0031-durable-task-review-files.md).

Starting this command with no queued workflow exits successfully without
launching an agent. See
[ADR 0015](docs/architecture/0015-supervised-execution-runner.md).

Agent definitions are strict, revisioned Foxhound profiles. The built-in
`general` profile preserves the existing Hermes prompt, tools, and phase
policy. Its current bounded local-work budget is 50 turns and 30 minutes, with
a 45-minute renewable claim lease. A former built-in revision remains
resolution-only so workflows already pinned to it retain their exact policy;
it is not offered for new selection. Actual role profiles and prompts belong
in one explicitly configured, owner-only directory outside every Git checkout.
That private directory can be shared across projects through an approved
synchronization system, so one coding agent definition can serve multiple
repositories without being copied into them. The card service and execution
runner must receive the same directory. The repository provides only a
clearly fictional example manifest at
`examples/agent-profiles/example-coder.json`. A deployment can inspect
content-free identity and policy metadata for its private manifests:

```sh
foxhound-agent-profiles --directory /srv/example/private-agent-profiles validate
foxhound-agent-profiles --directory /srv/example/private-agent-profiles list
foxhound-agent-profiles --directory /srv/example/private-agent-profiles show general
```

Private manifests cannot provide executable commands, environment variables,
secrets, or arbitrary tool names. Prompts and private paths are never emitted
by these commands. Profile selection is intentionally not inferred from task
content. See [ADR 0023](docs/architecture/0023-agent-profile-registry.md).
See [ADR 0026](docs/architecture/0026-shared-private-agent-profiles.md) for the
shared private-profile deployment contract.

That directory can hold one flat manifest per profile, or the versioned store
described below.

### Versioned private profile store

A profile is edited as Markdown, not as one long JSON string, and a published
revision is never rewritten. The editable source holds shared instructions used
by every agent, role instructions per profile, optional project overlays, and
one policy document per profile. `examples/agent-profile-store/` contains a
clearly fictional source store with exactly that shape:

```text
shared/hermes.md                    instructions shared by every agent
drafts/example-scout/policy.json    one profile's limits, tools, and fragments
drafts/example-scout/role.md        that profile's role instructions
overlays/example-project.md         an optional narrowing overlay
catalog.json                        what each profile currently offers
revisions/example-scout/<sha256>.json   immutable compiled revisions
```

Publishing compiles the shared fragments, the role instructions, the selected
overlays, and the policy into one effective manifest whose digest is the
revision, then advances the catalog atomically. Editing shared instructions
therefore changes every active profile, so republish them together:

```sh
foxhound-agent-profile-store --source /srv/example/private-agent-source   publish --all-active
foxhound-agent-profile-store --source /srv/example/private-agent-source   install --target /srv/example/private-agent-profiles
```

Instructions that several agents share, including approved reusable Hermes
prompt material, belong in the store's shared component rather than in this
repository or in ambient host configuration: publication composes them into
each profile, where the effective revision covers them.

`initialize`, `validate`, `list`, `publish`, `disable`, `enable`, `install`,
`doctor`, `delete`, and `migrate` are the available operations. Only an active
profile's current revision is offered for new selection; every other published
revision still resolves exactly for a workflow already pinned to it, including
the revisions of a profile that has been disabled. `delete` is refused unless
the profile is already disabled and the execution databases passed with
`--database` prove that no stored work references it. `migrate` converts a flat
directory, only reads it, and refuses unless recompiling reproduces each
original revision digest, so existing pins keep working.

The source may be distributed by a synchronization system that does not
preserve file modes, so `doctor` reports permissive entries as counts rather
than enforcing them there; `install` writes the owner-only `0700`/`0600` copy
that the card service and runner actually load. Command output carries
identifiers, states, revisions, and counts only. See
[ADR 0027](docs/architecture/0027-versioned-private-profile-store.md).

Every execution workflow stores the selected profile ID and exact revision.
The runner resolves that immutable evidence within atomic claim selection,
then derives the Hermes prompt, tools, turn limit, timeout, lease, heartbeat,
and shutdown grace from the profile. Profile selection accepts only the
current revision, while execution may resolve an explicitly retained built-in
revision already stored by a workflow. Other missing, changed, or
phase-ineligible profiles fail closed without claiming or launching an agent.
There are no runner-level overrides for those profile policies. See
[ADR 0024](docs/architecture/0024-workflow-agent-binding.md).

Every execution workflow remains bound to an exact selected profile revision.
The established Start-card presentation deliberately keeps agent selection
out of its six task-decision controls; deployment scheduling selects the
profile before the card is created. The authenticated agent-options and
agent-selection operations remain available to trusted integrations and
atomically version both workflow and card without starting work or changing
task lifecycle. The card service and runner must load the same owner-only
profile directory. See
[ADR 0029](docs/architecture/0029-legacy-workflow-card-compatibility.md).

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
If an older deployment was interrupted after lifecycle writers and the
creation registry were removed but before the four inventory/workflow/review
jobs were removed, neither normal stage matches. The explicit `residual` mode
is bounded recovery tooling for only that exact boundary. It refuses if either
lifecycle writer or the registry is present, or unless every residual writer
appears exactly once:

```sh
foxhound-scheduler-cutover prepare --stage residual \
  --snapshot /srv/example/private-cutover/scheduler.residual-current \
  --candidate /srv/example/private-cutover/scheduler.residual \
  --rollback /srv/example/private-cutover/scheduler.residual-rollback
foxhound-scheduler-cutover verify --stage residual \
  --snapshot /srv/example/private-cutover/scheduler.residual-current \
  --candidate /srv/example/private-cutover/scheduler.residual \
  --rollback /srv/example/private-cutover/scheduler.residual-rollback
```

Residual recovery is not a normal third authority stage and must not be used
to infer live state; its input is still an explicit owner-private snapshot.
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
workflow. It fills two independent GW-compatible capacities: at most five
newly scheduled workflows may be queued or running, while at most twenty may
actively wait for a reader. Existing over-cap rows are preserved and drain
normally. It does not advance Start or launch an agent. See
[ADR 0019](docs/architecture/0019-new-task-execution-scheduler.md) and
[ADR 0035](docs/architecture/0035-separate-execution-capacities.md).
Deployments with an installed role-specific profile can bind new workflows to
that deterministic default before presenting the Start gate:

```sh
foxhound-execution-schedule \
  --database /srv/example/private-foxhound-state/foxhound.sqlite3 \
  --agent-profile-directory /srv/example/private-agent-profiles \
  --default-agent-profile example-specialist \
  --limit 100
```

Both options are explicit deployment policy. The command fails before
scheduling if the installed directory is unsafe, the profile is unavailable,
or that profile cannot plan. Omitting them retains the built-in General
default.

Execution gates have their own transport-neutral durable review cards. An
explicit scheduling pass projects only workflows currently awaiting start,
plan review, external-action review, or final-result review. Each card is
bound to exact task, workflow, phase, and result state; delivery uses an
expiring digest-fenced claim. Unrelated queued, running, or review-waiting
workflows do not suppress a Start card; the gateway's one-card presentation
limit queues reader decisions without silencing them. Start cards retain the
established task context and Done/Continue, Drop/Update, Snooze and Reassign
layout. Every card kind carries exactly one Snooze control rather than a row
of intervals: a card surface is expected to rewrite that control into its own
picker and send back one of `snooze_1d`, `snooze_7d`, `snooze_14d` or
`snooze_30d`, each of which resolves to 09:00 on a local calendar date. The
bare control is answered too, as the nearest of those choices, so a surface
that does not offer a picker still defers the card rather than leaving a
button that does nothing. When a task has
a resolved, non-provisional person owner other than the reader, a deployment
with the GW condition boundary enabled also shows **Until next meeting with
Person B**. The first Start card is always shown; Foxhound never silently
suppresses other-owned work. That action resolves the card without starting
an agent, stores the exact structured owner reference, and wakes a fresh Start
card at the first matching upcoming meeting or exactly 21 days later.
Unresolved, group, provisional, and reader-owned tasks never receive the
control. GW failures leave the hold in place. Update records private steering
but does not start work; Continue is the explicit planning
approval. Plan and result cards can request more
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
acknowledge or retry delivery, submit one versioned reader action, submit
one bounded discussion or reassignment response, and read one delivered card's
presentation back. That last route is a read like the task brief: it renders a
current delivered card exactly as the delivery rendered it, at an exact
expected version, and writes nothing. It exists so a surface that replaces a
card's controls with a sub-menu -- the snooze picker is the one in use -- can
put the card back when the reader backs out, instead of stranding them with a
card they can only defer. An undelivered, superseded or differently versioned
card is refused and carries no presentation at all. Its agent-options and
agent-selection operations remain a bounded integration contract with opaque
callbacks and a refreshed Start-card presentation; they are not exposed as an
extra control in the legacy-compatible Start keyboard.
The packaged service accepts `--agent-profile-directory`; deployments with
private profiles must give it the same directory as the execution runner. The
owner-conditioned control is disabled unless all three of `--gw-endpoint`,
`--gw-alias`, and `--gw-token-file` are supplied. On startup the service reads
the strict GW execution context once to identify reader aliases; scheduling
passes later call only the content-free owner-upcoming-meeting condition.
Deploy the GW condition route first, then a gateway version that forwards the
`until_meeting` action, and only then restart Foxhound with those arguments.
The existing `/v1/task-cards/*` contracts are unchanged. Starting the service
still creates no card and advances no gate. See
[ADR 0017](docs/architecture/0017-execution-card-service.md) and
[ADR 0034](docs/architecture/0034-owner-conditioned-start-holds.md).

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
See [ADR 0023](docs/architecture/0023-agent-profile-registry.md) for strict,
reviewed, host-private agent definitions.
See [ADR 0024](docs/architecture/0024-workflow-agent-binding.md) for exact
profile evidence throughout execution.
See [ADR 0025](docs/architecture/0025-start-card-agent-selector.md) for the
reader-controlled Start-card selection protocol.
See [ADR 0026](docs/architecture/0026-shared-private-agent-profiles.md) for
shared private role profiles and the synthetic public example.
See [ADR 0027](docs/architecture/0027-versioned-private-profile-store.md) for
immutable published profile revisions and editable private prompt fragments.
See [ADR 0028](docs/architecture/0028-fenced-instruction-delivery.md) for
private revision-pinned prompt delivery through the worker.
See [ADR 0029](docs/architecture/0029-legacy-workflow-card-compatibility.md) for
the legacy-compatible card projection.
See [ADR 0030](docs/architecture/0030-versioned-runtime-guidance.md) for
versioned operating guidance and explicit runtime capabilities.
See [ADR 0034](docs/architecture/0034-owner-conditioned-start-holds.md) for
identity-bound Start-card holds and the 21-day fail-safe.
