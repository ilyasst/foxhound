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

Version 10 is current. Three things about it are worth knowing before an
upgrade, because neither announces itself:

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

Version 5 covers every enabled component that reads or writes the shared
database: the task-card service, scheduler, one or more runners, feed import,
native intake, execution-card requeue, lifecycle-outcome export, fused task
titles, and duplicate-card scheduling. It is intentionally strict: every field
below is required when that component is enabled, unknown fields are rejected,
and all paths are absolute. The one exception is noted with the component it
applies to: `task_card_requeue` may be omitted while a deployment written
before it existed is brought forward.

```json
{
  "schema": "foxhound.deployment-config",
  "schema_version": 10,
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
    "plan_without_asking": ["issue"],
    "execute_without_asking": ["issue"],
    "skip_planning_for": ["issue"],
    "act_without_asking": ["issue"],
    "execution_slot_cap": 2,
    "plan_ready_cap": 10,
    "awaiting_reader_cap": 20
  },
  "execution_runners": [{
    "enabled": true,
    "run_root": "/srv/example/private-foxhound-runs",
    "gw_endpoint": "http://<canonical IPv4 loopback address>:8787",
    "gw_alias": "example-operator",
    "gw_token_file": "/srv/example/private-foxhound-state/gw.token",
    "agent_command": "hermes",
    "worker_command": "foxhound-task-worker",
    "runner_slot": "primary",
    "knowledge_root": null,
    "task_work_root": null,
    "task_kb_root": null
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
only version 10 can declare the complete deployment boundary.

`task_card_requeue` may be omitted from an existing version 5 document during
the transition. Rendering `task-card-requeue` then refuses safely; add it with
an enabled or disabled declaration before using that component.

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
