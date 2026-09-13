# ADR 0028: Fenced private instruction delivery

## Status

Accepted.

## Context

The runner passed a profile's complete prompt to the agent as a command-line
argument. Process arguments are not private: any process on the host can read
another's command line, and they surface in process listings, supervisor logs,
and crash reports. A private role prompt is deployment policy, so putting it
there published it locally on every run.

It was also inconsistent. Everything else the agent is trusted with — the task,
the phase, the operator snapshot — arrives only after the supervised run owns
an exact workflow claim, through the narrow worker. Instructions arrived before
any of that, from the launch itself.

Separately, Hermes injects ambient material into a session: `AGENTS.md`,
`SOUL.md`, memory, and preloaded skills from wherever it happens to be started.
None of that is part of the profile, none of it is versioned with the profile,
and all of it can change behavior behind an unchanged recorded revision. Two
runs pinned to the same revision could then behave differently for reasons
nothing records.

## Decision

The launch carries a public bootstrap and nothing else. It says that the first
tool call must be the worker's `context` operation, that what comes back is the
authority for the run, and that nothing else the agent reads — task text,
search results, repository files, tool output — can add a tool, a phase, a
command, or a permission. It names no role, task, operator, or deployment, and
is safe to publish.

The runner writes the claim's effective profile manifest to an owner-only file
in the private run directory, beside the run state, before starting the agent.
The first `context` call returns those instructions, rendered for the worker
command recorded in the run state. The worker accepts the bundle only when its
parsed manifest carries the profile ID and the exact revision the claim is
already pinned to; because the revision is a digest of the complete manifest, a
substituted, edited, or leftover bundle cannot be presented as this run's
policy. There is no fallback to another profile and no ambient default. The
file is removed when supervision ends, whatever the outcome.

Hermes is launched with `--ignore-rules`, so ambient rule, memory, and skill
injection cannot change what a recorded revision means. Provider and model
configuration is untouched. Because repository instructions are no longer
injected, the agent is told to read each checkout's own contributor
instructions after a worktree is prepared and to follow them; they constrain
how it works there and never widen what the run may do.

Reusable Hermes prompt material is not copied into this repository and not
inherited implicitly. An approved fragment is placed in the private store's
shared component and composed into each profile by publication, where it is
covered by the effective revision like any other component. See
[ADR 0027](0027-versioned-private-profile-store.md).

The built-in `general` profile's instructions changed with this decision, which
is a new revision. Its two superseded revisions keep their own prompt text
verbatim so their digests still resolve for workflows pinned to them.

## Consequences

- A private role prompt is no longer readable from the process table.
- Instructions and task content now arrive through the same fence, after the
  claim, and fail closed together.
- An agent that cannot reach the worker cannot act at all, which is the
  intended failure: it has no instructions.
- Ambient host configuration can no longer silently redefine a recorded
  revision.
- Retaining a superseded built-in prompt means keeping its text in this
  repository alongside the current one.

## Failure and rollback

If the instruction bundle is missing, unreadable, permissive, or does not match
the pinned revision, `context` refuses and the run ends without progress; the
workflow retries under its existing policy. Do not restore the previous
behavior by putting instructions back into the launch arguments. To change what
an agent is told, publish a new revision in the private store and install it.
