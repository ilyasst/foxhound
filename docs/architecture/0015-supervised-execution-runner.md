# ADR 0015: Supervised execution runner

Status: accepted for the execution-ownership migration.

## Context

Foxhound owns durable execution gates and claims, but no process consumes a
queued phase. Reusing the GW dispatcher would leave execution authority in
both systems. Giving an agent direct database access or placing a claim token
in its prompt, arguments, output, or result would weaken the ledger's fences
and make accidental disclosure more likely.

## Decision

`foxhound.execution_runner` is a one-shot supervisor. Under an exclusive
owner-local lock, it claims at most one ready workflow and creates a private,
disposable run directory. It writes the capability and authoritative task,
workflow, and phase identity to an owner-only run-state file, then starts the
configured agent with an argument vector, a new process group, no shell, no
stdin, and its output captured to an owner-only file inside the run
directory. That output was discarded until a run that produced a complete
result file, failed to record it, and left nothing behind that could explain
why. It is as private as the result beside it and never leaves the host.

The agent's fixed prompt names the authority of each phase. Planning cannot
cause an external effect, execution cannot perform an external action, and
the external-action phase is usable only after its separate durable reader
approval. Execution never changes task lifecycle state.

The selected workflow profile now supplies that prompt as well as the Hermes
tool allowlist, turn limit, timeout, claim lease, heartbeat, and shutdown
grace. The runner resolves the exact recorded revision from its reviewed
registry before the claim and has no command-line overrides for those policy
fields. See [ADR 0024](0024-workflow-agent-binding.md).

The only supported task operations inside a run are exposed by
`foxhound.execution_worker`:

- `context` renews the claim and returns the current Foxhound task plus the
  strict GW execution-context snapshot;
- `search` renews the claim around one bounded, read-only GW search;
- `draft` reads fixed owner-only result inputs beside the run state, validates
  them against the current phase, and atomically creates a correctly named
  schema-valid draft without placing private result text in process arguments;
- `record` accepts one strict owner-only draft, injects authoritative identity
  and the capability from run state, and records it in the ledger; and
- `release` gives up the fenced claim without recording work.

The worker rejects symlinks, permissive private-state paths, duplicate JSON
fields, unknown draft fields, mismatched result filenames and identifiers,
and stale task, workflow, phase, or capability state. It emits content-free
errors and never returns either credential. A successful result is durable
before the draft is replaced by a content-free receipt and its fixed private
input files are removed. The legacy hand-authored draft format remains valid,
and reviewed role prompts can direct agents through the safer builder.

The supervisor polls the workflow and renews its lease independently of the
agent. A durable result or release terminates any remaining child process.
Startup errors, unexpected process exit, timeout, interruption, and lease
failure enter the ledger's bounded retry policy. A concurrent durable result
wins over process-failure reporting. The run-state capability is removed from
the final receipt; if the supervisor disappears, the bounded lease expires.

The runner is deliberately one-shot. A host scheduler may invoke it, but
Foxhound does not install or configure that scheduler. An idle invocation does
not start an agent. The database, run root, token, and resulting private work
remain outside the repository.

## Failure and rollback

Rollback is to stop invoking the runner. Queued work and all recorded results
remain durable; an in-flight claim becomes unusable after lease expiry and is
then recovered by the ledger. Do not restore a second writer in GW for the
same tasks.

An unavailable or invalid GW execution-context provider fails before a claim
is taken. After a claim, worker retrieval failures do not bypass the provider
or authorize direct GW configuration access; the agent must release or the
supervisor records a bounded failure.

## Out of scope

This slice does not create tasks, schedule workflows, render start or approval
cards, install a recurring service, migrate production backlog, send or
publish external actions without the external-action gate, deploy the GW
provider, or disable legacy GW execution.
