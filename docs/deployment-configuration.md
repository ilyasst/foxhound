# Private deployment configuration

Deployed code and deployed configuration are separate concerns with the same
discipline.  Units import a pinned release checkout, never a development tree;
see [ADR 0043](architecture/0043-pinned-release-checkout.md) for what a deploy
consists of and which units must be restarted.  The configuration file below is
private host state and lives outside any checkout.

`foxhound-deployment-config` makes the settings that belong to one deployment
explicit. The JSON file is private host state: keep it outside the checkout,
make it owner-only (`0600`), and do not commit it, paste it into issues, or
send its rendered command lines to logs. It contains paths but never token
values.

Version 15 is current. Eight things about it are worth knowing before an
upgrade, because none of them announces itself:

- `card_service.task_work_root` is **required** once delivery is enabled, and
  it is what switches result artifacts on. Without it the artifact routes are
  present, authenticated, and permanently empty: every request is refused
  with `artifacts_unavailable`, which a caller can tell apart from a result
  that produced no files, and which the service also reports once at
  start-up. A deployment left on an earlier version has the routes and never
  serves a file through them.
- Versions 6 and 7 added `workflow.execute_without_asking` and
  `workflow.act_without_asking`. An empty list is what their absence meant,
  so carrying them across as empty changes nothing about what runs
  unattended.
- Version 10 adds `workflow.skip_planning_for`. It is a separate list because
  removing an ask does not remove a phase; each listed kind must also be in
  both `workflow.plan_without_asking` and `workflow.execute_without_asking`.
- Version 11 makes profile routing a deployment choice. `default_agent_profile`
  remains the required fallback, while each `agent_profile_routes` entry names
  a selector and an installed profile. The initial selector is `source_kind`;
  the entry shape leaves room for additional selectors without replacing the
  routing list. Older documents migrate to an empty route list, so every task
  uses their already-declared default until routes are added deliberately.
- Version 12 copies the runtime's structured session record into each task
  run. When `task_work_root` and `task_kb_root` are configured, set
  `runtime_session_database` to the private Hermes `state.db`; it is read-only
  input, while the copied `runtime-session.json` is Foxhound-owned task
  evidence. `runtime_log_retention_bytes` is the per-task history limit and
  defaults to 30 MiB. These logs can contain private tool arguments and
  results: they are not artifacts and must never be committed or delivered.
- Version 13 adds `execution_runners[].deployment_roots`: named roots a
  portable profile can refer to while each host resolves them. An empty object
  is what its absence meant.
- Version 14 adds `execution_runners[].agent_model` and
  `agent_provider`, which choose the inference backend for **that runner's**
  agents. Both `null` is the default and means the agent runtime's own
  configured backend: nothing is added to the command, and the runtime's
  configuration is neither read nor written. This distinction is the point of
  the fields — a deployment that wants one slot on a different backend states
  it here, next to the slot, instead of changing a runtime default that every
  other user of that runtime on the machine also gets. A provider without a
  model is rejected at load; see
  [ADR 0048](architecture/0048-per-runner-inference-backend.md).

Version 5 covers every enabled component that reads or writes the shared
database: the task-card service, scheduler, one or more runners, feed import,
native intake, execution-card requeue, lifecycle-outcome export, fused task
titles, and duplicate-card scheduling. It is intentionally strict: every field
below is required when that component is enabled, unknown fields are rejected,
and all paths are absolute. The one exception is noted with the component it
applies to: `task_card_requeue` may be omitted while a deployment written
before it existed is brought forward.

- Version 15 adds `workflow.steer_while_running`. It is a list of source
  kinds whose newly admitted runs may raise a Steer card after they have been
  running for the configured threshold. It is independent of
  `plan_without_asking`: without planning authority, the Start gate remains
  and the declaration is inert. Changing the list never retroactively changes
  a workflow that is already admitted.
```json
{
  "schema": "foxhound.deployment-config",
  "schema_version": 15,
  "database": "/srv/example/private-foxhound-state/foxhound.sqlite3",
  "agent_profile_directory": null,
  "card_service": {
    "enabled": true,
    "bind": "<canonical IPv4 loopback address>",
    "port": 8790,
    "request_timeout_seconds": 5,
    "task_token_files": {
      "drip": "/srv/example/private-foxhound-state/task-drip.token"
    },
    "execution_card_delivery": true,
    "execution_token_files": {
      "drip": "/srv/example/private-foxhound-state/execution-drip.token"
    },
    "gw_endpoint": "http://<canonical IPv4 loopback address>:8787",
    "gw_alias": "example-operator",
    "gw_token_file": "/srv/example/private-foxhound-state/gw.token",
    "task_work_root": "/srv/example/private-task-work"
  },
  "workflow": {
    "default_agent_profile": "general",
    "agent_profile_routes": [{
      "selector": {"source_kind": "issue"},
      "profile_id": "example-repository-agent"
    }],
    "reader_aliases": [],
    "plan_without_asking": ["issue"],
    "steer_while_running": ["email", "teams", "meeting", "calendar", "alert", "mention", "legacy"],
    "execute_without_asking": ["issue"],
    "skip_planning_for": ["issue"],
    "act_without_asking": ["issue"],
    "execution_slot_cap": 2,
    "plan_ready_cap": 10,
    "awaiting_reader_cap": 20,
    "reader_aliases": []
  },
  "execution_runners": [{
    "enabled": true,
    "run_root": "/srv/example/private-foxhound-runs",
    "gw_endpoint": "http://<canonical IPv4 loopback address>:8787",
    "gw_alias": "example-operator",
    "gw_token_file": "/srv/example/private-foxhound-state/gw.token",
    "agent_command": "hermes",
    "agent_model": null,
    "agent_provider": null,
    "worker_command": "foxhound-task-worker",
    "runner_slot": "primary",
    "knowledge_root": null,
    "deployment_roots": {},
    "task_work_root": null,
    "task_kb_root": null,
    "runtime_session_database": null,
    "runtime_log_retention_bytes": 31457280
  }],
  "database_consumers": {
    "candidate_feed_import": {
      "enabled": true,
      "outbox": "/srv/example/private-foxhound-state/candidate-outbox",
      "stream_id": "example-candidates"
    },
    "native_intake_run": {
      "enabled": true,
      "producer": "gw",
      "stream_id": "example-native",
      "limit": 100
    },
    "execution_card_requeue": {
      "enabled": true,
      "limit": 100
    },
    "execution_card_schedule": {
      "enabled": true,
      "limit": 100
    },
    "task_card_requeue": {
      "enabled": true,
      "limit": 100
    },
    "lifecycle_outcome_export": {
      "enabled": true,
      "outbox": "/srv/example/private-foxhound-state/lifecycle-outbox",
      "stream_id": "example-lifecycle",
      "max_page_items": 100
    },
    "fused_task_titles": {
      "enabled": true,
      "endpoint": "http://<canonical IPv4 loopback address>:8800"
    },
    "duplicate_card_schedule": {
      "enabled": true,
      "limit": 100
    }
  }
}
```

The two loopback placeholders in this public example must be replaced with
the canonical IPv4 loopback address before private validation.

Set a disabled `card_service`, runner, or database consumer to exactly
`{"enabled": false}`. The `execution_runners` list permits each running slot
to be declared separately; enabled slots must have distinct names. The
workflow section remains required because it owns the shared policy and
limits. Earlier versions remain readable for a controlled transition, but
only version 13 can declare the complete deployment boundary.

`task_card_requeue` may be omitted from an existing version 5 document during
the transition. Rendering `task-card-requeue` then refuses safely; add it with
an enabled or disabled declaration before using that component.

`execution_card_schedule` may be omitted on the same terms, and for the same
reason: it was added after hosts were already running. It creates the review
cards that workflows waiting at a gate are owed. Leaving it out means cards are
created only when a delivering side asks for them as a side effect of topping up
its own surface — which makes a card's existence conditional on that surface
having room, and leaves a console reader unable to answer work that is waiting.
A host with a console should enable it.

Validate before changing a service definition or restarting anything:

```sh
foxhound-deployment-config \
  --config /srv/example/private-foxhound-state/deployment.json \
  validate
```

Validation checks the selected database and profile configuration, the
loopback bind and port, capacity and planning rules, and the same owner-only
token-file and role rules used by the running services. When execution-card
delivery is enabled, `drip` must be present in both token maps; this catches a
card path that could start but could not deliver. Failures are deliberately
content-free.

Render the arguments for a supported component only after validation:

```sh
foxhound-deployment-config \
  --config /srv/example/private-foxhound-state/deployment.json \
  render --component task-cards
```

Supported names are `task-cards`, `execution-schedule`,
`execution-runner:<slot>`, `candidate-feed-import`, `native-intake-run`,
`execution-card-requeue`, `task-card-requeue`, `lifecycle-outcome-export`, and
`fused-task-titles`.
`duplicate-card-schedule` is also available. Use the configured slot name when
rendering a runner. Rendering reads no token contents. Its JSON output does
contain private paths, so consume it only in the private deployment mechanism,
never in a repository, issue, or shared log.

Private service units can use `exec` to start a declared component from the
same installed release as the configuration command:

```sh
/srv/example/releases/current/bin/foxhound-deployment-config \
  --config /srv/example/private-foxhound-state/deployment.json \
  exec --component execution-runner:primary
```

`exec` replaces itself with the matching console script beside
`foxhound-deployment-config`; it does not use a shell or search `PATH`. Keep
the release selector and unit definitions private. A promotion changes that
selector only after preflight succeeds, so every database component starts
from one selected release with arguments rendered from the same document.

`worker_command` follows the same principle, and needs to, because the worker
is run by the agent rather than by the runner: a bare name is resolved to the
matching console script beside the running interpreter, and it is that absolute
path which reaches the agent's prompt and private run state. An absolute
`worker_command` is honoured as given, for a layout where that is wrong. Either
way the runner asks the worker what run-state schema it speaks before claiming
any work, and refuses to claim when the answer does not match what it writes.
See [ADR 0046](architecture/0046-worker-resolved-from-the-running-release.md).

## A unit must not reach into a development checkout

Not every step of a unit is foxhound. The candidate sync and the lifecycle
bridge each shell out to the sibling `gw` package, and those `ExecStart` lines
name an interpreter explicitly rather than going through `exec` above. Two
separate decisions hide in one line there, and only one of them is obvious:

- **Which code runs.** For `gw` steps this is a pinned root on `PYTHONPATH`.
- **Which interpreter runs it.** This is the path at the front of the command.

Both must come from somewhere a deployment controls. **Neither may be a path
inside a development checkout** — not the interpreter, not a script, not a
module root. A clone's virtual environment is rebuilt by whoever is working in
it and removed outright by unrelated tool updates, so a unit pointed at one
fails for reasons that have nothing to do with the host, the release, or the
code it runs, and the failure arrives at whatever hour someone else happened
to run `pip`.

This is easy to get wrong because the visible half looks right. One deployment
ran its issue-intake step — the front of the whole intake path — on a shared
clone's interpreter for weeks while taking its `gw` code correctly from the
pinned root. Nothing in the unit looked unusual.

Audit a host in one line:

```sh
grep -h '^ExecStart=' ~/.config/systemd/user/foxhound-*.service{,.d/*.conf} \
  | grep -- '-checkout\|/src/\|/Repositories/'
```

Any output is a unit to repoint. Prefer an interpreter owned by a deployed
component or by a service that is itself deployed; if a host genuinely has no
such interpreter, that is the thing to fix, not the unit.

## Deployment roots

`deployment_roots` maps a stable symbolic name to an absolute directory on this
machine, for example `{"sync_drive": "/srv/example/drive"}`. The runner passes
each one to the worker, which publishes them to the agent as
`capabilities.deployment_roots`.

They exist so a profile prompt never names a path. A profile revision renders
identically on every host, and a path does not: the reviewed prompt names the
root, and each host resolves it. A name absent here is absent in the work
context, so an agent can tell "not configured on this host" from "configured
and empty". The values are runtime facts and do not enter the profile revision.

A store fragment that names a path instead is refused by `validate` and
`publish`; see [ADR 0043](architecture/0043-pinned-release-checkout.md) for the
order that requires, because the store is brought into compliance before the
release that enforces it is promoted.

## Gates a machine may stand down

A task passes reader gates on its way through a workflow. Three of them are
declared per machine, as lists of source kinds, and all default to empty: a
machine that says nothing is asked about everything.

`plan_without_asking` skips the Start gate. Enrolling a source is the
permission to plan its tasks, so the card that would ask again can only show
a title — nothing has looked at the work yet. Planning is read-only and
produces no external effect.

`execute_without_asking` skips the plan-approval gate. A recorded plan runs
instead of waiting for a card. Grant it for a source where the decision to
work every task was already made when the source was enrolled, and where a
plan card would therefore have one plausible answer.

`skip_planning_for` removes the plan phase entirely for newly scheduled tasks,
so their first agent run is `execute` and no plan result is recorded. It is
separate from both grants: adding either grant alone never removes a phase.
Every listed kind must also be granted both `plan_without_asking` and
`execute_without_asking`. Both are required because the plan phase carries the
reader's start gate as well as the plan itself: a kind that is still asked
about before planning would otherwise lose that question too, with no
declaration saying so. Existing workflow rows are never rewritten when the
declaration changes.

`steer_while_running` does not grant an advance or add a gate. It permits a
newly admitted run of that source kind to raise a status card if it remains
running. The task-card service defaults to a 20-minute threshold for both
plan and execute passes; deployments may set separate values with
`--steer-plan-threshold-seconds` and `--steer-execute-threshold-seconds`.
An `external_action` pass uses the execute threshold.
The card lets a reader stop the pass and queue a new one with a note; it never
injects text into an agent that is already running.

`act_without_asking` skips the external-action gate. A reviewed external
action runs instead of waiting for a second card. This is the strongest of
the three and the only one whose subject is an effect other people can see,
so grant it only where the reach of the action is bounded by construction
rather than by the agent's judgement.

For forge work it is: `forge_action` reads the target repository from the
task's accepted candidate binding rather than from an argument, so acting on
the wrong repository is not a mistake an agent can make, and the operations
offered are opening a pull request, commenting, and reviewing. There is no
merge and no push to a default branch - no such capability exists to grant.
Whoever merges the resulting pull request remains the decision this does not
touch, and protecting the default branch at the forge is what makes that
enforcement rather than policy.

The three are independent and none implies another, in any direction.
Granting planning spends agent time on work nobody has judged; granting
execution performs the work that plan described; granting action lets the
result leave the machine. An operator may reasonably want the first alone,
or the first two.

A granted advance is recorded as `phase_granted`, never as `phase_approved`:
the event log must not say a reader approved a phase nobody was asked about.
Results that end the work — `completed`, `declined`, `ineligible` — are never
granted past, so a task still produces its completion card.

## Fused task titles

`fused-task-titles` is a one-shot database consumer. Run its private timer
through `foxhound-deployment-config exec --component fused-task-titles` after
task-card delivery is available. Its endpoint is a canonical loopback HTTP
gateway selected in the private deployment document. The worker asks for the
`thinking_no` capability and prints aggregate counts only.

## Failure digests

`foxhound-failure-digest` is a one-shot pass that explains runs which ended
without a result. It takes `--database` and `--run-root`; the run root must be
the one the runner writes to, because that is where the transcripts are. Run it
from a private timer, after the runner.

For each failed attempt without a digest it reads the **tail** of that run's
transcript, asks the capability gateway for the `light` capability, and stores a
few sentences against the attempt. The card for a parked workflow then states
the cause beside the attempt count, so a reader deciding whether to continue can
see whether continuing could work.

Everything fails open. A pruned run root, an absent transcript, a busy or
unconfigured gateway, and a reply that does not fit are all skipped rows: the
pass exits 0 and prints counts. Set `FOXHOUND_DIGEST=0` to disable summarising
entirely — a deployment with no gateway behaves exactly as it did before this
existed, minus the digests. `FOXHOUND_DIGEST_ENDPOINT` overrides the gateway,
shared with the other derived summaries.

The pass is re-runnable: one digest per attempt, and a second pass over the same
attempt does nothing.

## Duplicate review cards

`duplicate-card-schedule` is a bounded one-shot database consumer. Run it from
a private timer after native intake. It binds only proposed duplicate pairs to
reader cards; it does not create ordinary task-review cards. The existing
delivery consumer then claims and sends those cards to Telegram.

## Multi-runner deployment

The execution runner has two separate concurrency concepts. First, `--execution-slot-cap` limits the number of tasks in the active `execute` phase system-wide to prevent out-of-memory cascades across concurrent worker processes. Second, each execution runner process handles only one task workflow at a time because it blocks on bounded agent work. Thus, raising the slot cap alone does not enable concurrent agent execution.

To actually run multiple tasks at once, you must deploy multiple independent runner processes. This is safely supported through a generic systemd template unit for the runners, combined with explicitly configured, distinct `runner_slot` identifiers in the shared configuration file.

### Adding runners safely

Because the single-runner behavior is explicit and backward compatible, scaling out involves configuring slots and transitioning to instantiated services:

1. **Assign distinct slot identities:** In your private deployment JSON file, declare each runner under the `execution_runners` list. Set each runner's `enabled` to `true`, and ensure each gets a unique string as its `runner_slot`.
2. **Apply the shared slot cap:** Declare `execution_slot_cap` once under the `workflow` section of the deployment JSON (for example, 2 or 3). Every rendered runner inherits it, so the ceiling cannot drift between instances. The database enforces it globally across all stable slot names.
3. **Use a systemd template:** Instead of a single static `foxhound-execution-runner.service`, define a generic template `foxhound-execution-runner@.service`. The instance name `%i` becomes the runner slot.

```ini
[Unit]
Description=Foxhound Execution Runner (%i)
After=network.target

[Service]
Type=simple
User=foxhound
# The runner reads its slot, cap, agent environment, and paths from the shared
# JSON. Name the slot in the component itself; do not repeat those as flags.
ExecStart=/opt/foxhound/venv/bin/foxhound-deployment-config \
    --config /srv/example/private-foxhound-state/deployment.json \
    exec --component execution-runner:%i
Restart=on-failure
RestartSec=10

[Install]
WantedBy=multi-user.target
```

4. **Start instances without interrupting:** A fenced run on an existing runner is shielded. When adding runners, do not restart an active instance. Simply instantiate the additional workers:
   ```bash
   systemctl enable --now foxhound-execution-runner@slot-2.service
   systemctl enable --now foxhound-execution-runner@slot-3.service
   ```
   The original single runner can eventually be migrated to `foxhound-execution-runner@slot-1.service` during a natural idle window, or safely disabled while the others pick up the load. To scale down, `systemctl stop` a worker; active execution state remains durable and the scheduled task will either finish its phase and exit or time out gracefully.

## Claim schema versions and rollout order (Issue #688, GW #1156)

Foxhound card claim endpoints (`/v1/task-cards/claim` and `/v1/execution-cards/claim`) expose a bounded `source_kind` field under claim schema version 2.

Rollout must proceed in the following order:

1. **Deploy consumer compatibility first**: Update delivery consumers (such as GW) to accept both claim schema version 1 and version 2, while routing configuration remains unconfigured (all cards continue using the configured default destination).
2. **Deploy Foxhound version 2**: Deploy Foxhound with version 2 claim responses enabled. Consumers accept version 2 claims and deliver to their default destination.
3. **Enable consumer per-source routing**: Configure topic-by-source mapping in the consumer (e.g. `GW_CARDS_SOURCE_TOPICS`).

