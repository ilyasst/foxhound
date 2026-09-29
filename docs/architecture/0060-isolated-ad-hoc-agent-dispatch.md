# ADR 0060: Keep ad hoc agent lanes outside the task queue

## Status

Accepted.

## Context

Repository maintenance sometimes benefits from parallel agent lanes.  Those
lanes are not user tasks: inserting them into the durable task ledger changes
queue state, produces reader cards, and makes a temporary implementation aid
look like production work.  Calling the agent runtime directly avoids that
pollution but otherwise loses Foxhound's useful boundaries: explicit backend
selection, private inputs, bounded concurrency, an observable lifecycle, and
safe cancellation.

## Decision

`foxhound-agent-dispatch` supervises these temporary lanes without touching
the task database.  `start` accepts an existing working directory and a prompt
file.  It copies the prompt into a mode-0700 job directory as a mode-0600 file;
prompt text is never placed in process arguments or ordinary status output.
The child sees a constant bootstrap telling it where to read the prompt.

Every job receives a minimal, private `HERMES_HOME`.  The selected model,
provider, and reasoning level are written there, so an invocation can use a
different reasoning level without rewriting the shared runtime configuration.
Provider credentials are read from a named entry in a private dotenv file and
exist only in the child environment; they are not copied into job files.

The dispatcher defaults to two concurrent jobs and rejects a second active job
in the same resolved working directory.  It records a process start identity
from `/proc`, not a PID alone, and the supervisor refreshes a durable heartbeat.
The heartbeat lets read-only status and concurrency checks remain correct when
their process namespace cannot see the supervisor.  Cancellation is stricter:
it is permitted only while the exact process identity is locally visible and
targets the supervisor, which terminates and, if needed, kills the runtime's
separate process group before recording the terminal state.  `status`, `wait`,
`log`, and `cancel` operate on the random job identifier returned by `start`.

The dispatcher is intentionally not a shortcut around Foxhound's task
lifecycle.  It has no ledger, workflow, review-card, worker, or deployment
authority.  A dispatched agent may make only the changes authorized by its
prompt and the rules of its working directory.  Durable work still belongs in
the task queue.

## Example

```sh
foxhound-agent-dispatch start \
  --prompt-file /srv/example/private/lane-a.txt \
  --working-directory /srv/example/worktrees/lane-a \
  --model example-model-a \
  --provider example-provider \
  --reasoning low

foxhound-agent-dispatch status 0123456789abcdef0123456789abcdef
foxhound-agent-dispatch wait 0123456789abcdef0123456789abcdef --timeout-seconds 30
foxhound-agent-dispatch log 0123456789abcdef0123456789abcdef
foxhound-agent-dispatch cancel 0123456789abcdef0123456789abcdef
```

## Consequences

Ad hoc lanes become reproducible and observable without contaminating the
production queue or changing the runtime's global model settings.  Their
status is local host state, not a durable task record.  If work must survive
host loss or require reader review, it must use the normal workflow instead.
