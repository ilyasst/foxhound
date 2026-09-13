# ADR 0024: Durable workflow agent binding

## Status

Accepted.

## Context

A reviewed registry alone does not prove which agent policy was authorized for
a task. Looking up a default only when work starts would let configuration
changes silently alter queued work, while accepting a profile name from task
content would cross the reader-authorization boundary.

Runner-level time and tool overrides would also make the persisted profile
identity misleading.

## Decision

Every execution workflow stores an agent profile ID and its exact SHA-256
revision. Schema-11 workflows, results, and events migrate to the built-in
General compatibility revision without changing task version, workflow
version, status, phase, due time, result identity, or any reader gate. Newly
scheduled workflows receive the deployment's explicit deterministic default;
task text and source material are not inputs to this choice.

`select_agent` is a distinct version-fenced reader operation. It accepts only
an exact revision in the installed registry, requires that the profile can
plan, and is valid only in `awaiting_start`. An exact replay is unchanged.
Changing the selection increments the workflow version, appends an
`agent_selected` event, and therefore makes earlier cards stale. It never
starts work or changes task lifecycle.

Every claim resolves the stored ID and revision and checks that the profile
allows the queued phase before changing durable state. Unavailable, changed,
or phase-ineligible profiles leave the workflow queued. Claims carry the same
evidence. Immutable results and all workflow events copy it from the durable
workflow rather than trusting agent output. Aggregate profile health reports
only IDs, revisions, availability, and counts.

The supervised runner loads the reviewed registry. Its atomic claim operation
resolves the recorded evidence before changing durable state, and the runner
constructs the Hermes invocation from that resolved profile. The profile is
the sole source of instructions, tool families, turn limit, internal timeout,
claim lease, heartbeat, and shutdown grace. Only the policy fields reach the
invocation: the instructions are delivered through the fenced worker instead,
as [ADR 0028](0028-fenced-instruction-delivery.md) describes. The runtime
command and private profile directory remain deployment inputs; task content
cannot set either. The worker's owner-only run state carries profile evidence,
the profile-derived lease, and the worker command, but no instructions.

## Consequences

- A queued workflow cannot silently adopt a changed prompt or runtime policy.
- Longer local-GPU work is configured per reviewed profile instead of by a
  global timeout.
- Removing or editing a selected profile intentionally stops new claims until
  a reader selects an available revision or the reviewed profile is restored.
- Deployments upgrading from runner-level policy flags must remove those flags
  and install the selected profiles before enabling the runner.
