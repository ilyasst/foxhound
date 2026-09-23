# ADR 0046: Resolve the task worker from the running release

## Status

Accepted.

## Context

[ADR 0043](0043-pinned-release-checkout.md) says deployed units run from an
immutable release directory and never a development tree. That holds for the
execution runner. It did not hold for the worker.

The runner does not spawn the worker. It spawns an agent, hands it a prompt
naming `foxhound-task-worker`, and the agent runs that name in its own shell.
`worker_command` was validated as a bare console-script name, so the name was
resolved through `PATH` — the *agent's* `PATH`, not the runner's. A deployment
whose interactive shell profile prepends a user-level script directory, which
is the ordinary layout, therefore ran whatever same-named script happened to be
installed there. A development `pip install -e .` puts exactly such a script
there, pointing at a working tree.

Setting `PATH` on the runner unit does not fix this, because the agent's shell
re-derives its own `PATH` from the user's profile. This was tried first and did
not hold.

Nothing reported the discrepancy. The runner logs the release revision it
runs; the worker it never spawns logs nothing comparable, so a runner on the
release and a worker on a working tree were indistinguishable from a correct
deployment.

They stayed indistinguishable for as long as both happened to understand the
same run-state schema. The first release to change `RUN_STATE_SCHEMA_VERSION`
made it fatal: the runner wrote run state at the new version, the worker
refused a schema it did not know and exited 78, and every claimed run died at
the agent's first tool call with no result recorded. Each failure also
consumed a claim. Two promotions of such a release were rolled back before the
cause was understood, and the release was correct both times — nothing in it
could have been tested to reveal this, because the fault was in how the worker
was located rather than in what it did.

## Decision

Two changes, which address different halves of the problem.

**Resolve the worker beside the running interpreter.** A bare `worker_command`
is resolved to `Path(sys.executable).parent / command` when an executable file
is there, and that absolute path is what goes into the agent's prompt and into
private run state. A release then names its own worker and the agent has
nothing left to resolve. This mirrors what `deployment_config.execute_component`
already does for components.

The interpreter path is used as given rather than resolved, so what resolution
follows is however the interpreter was invoked. In the deployed shape that
means the release itself, not the selector: a console script installed by `pip`
carries the interpreter's path in its shebang, recorded when the virtual
environment was created, and that is the release directory's own path. Units
start a console script, so the process sees the release path and the worker
resolves inside that same release.

The worker is therefore pinned to the exact release the runner is, and
advancing the selector while a run is in flight cannot change which worker that
run's agent reaches. That is the property we want: the agent has to keep
talking to the worker that matches the run state its runner wrote.

`worker_command` may now also be given as an absolute path, which is returned
unchanged: a layout we did not anticipate keeps an escape hatch. Both forms are
validated more narrowly than the filesystem allows — no whitespace, no shell
metacharacters — because the value is templated into a prompt and used as
`argv[0]`.

**Ask the worker before claiming anything.** `foxhound-task-worker report`
prints the schema versions and revision of the worker that would run, without
needing a run to load. The runner calls it once at startup and refuses to claim
when the answer is incompatible.

The refusal is on the run-state schema, not on the revision. That schema is the
contract that actually has to hold — it is what one side writes and the other
parses — and it is the one whose breach caused the outage. Revision is carried
for diagnosis and deliberately does not gate: a worker built from the same
contract but installed another way is not a fault, and refusing it would make
every development checkout unrunnable to no purpose.

Resolution alone would be enough while the assumptions behind it hold. The
handshake exists because the next way these two can drift will not be a way
anyone has thought of yet, and the cost of not noticing is every workflow in
the queue.

## Consequences

A mismatch now costs one poll instead of every queued workflow, and says so on
stderr naming both schema versions, rather than surfacing as an exit code 78
from a subprocess nobody was watching.

A deployment with no worker beside its interpreter and none on `PATH` now
refuses to start rather than failing once per claimed run. This is intended:
such a deployment could not have completed a run anyway.

Falling back to the bare name when no neighbouring worker exists keeps
deployments with a different layout working. They are covered by the handshake
rather than by resolution, which is the weaker of the two guarantees — but it
is a guarantee, and it is strictly more than they had.

Private run state now records an absolute `worker_command` on ordinary
deployments. Documents written before this change carry a bare name and remain
valid; both forms are accepted.

## Failure and rollback

Reverting restores `PATH` resolution and removes the startup check. Run-state
documents written while this was deployed carry absolute worker paths, which a
reverted worker rejects, so a revert should be paired with draining in-flight
runs — or with re-pinning `worker_command` to a bare name, which the reverted
code accepts.

The `report` subcommand is additive. A worker predating it exits non-zero, which
the runner treats as a mismatch and refuses — correctly, since such a worker is
by definition not the running release.
