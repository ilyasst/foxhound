# ADR 0030: Version runtime guidance with agent profiles

## Status

Accepted.

## Context

Foxhound starts Hermes with ambient rules disabled. That is intentional:
workflows are pinned to an exact agent-profile revision, while machine-local
rules, skills, and memory can change without producing a new revision. Allowing
those ambient inputs to govern a run would make the recorded revision an
incomplete description of its behavior.

Disabling ambient rules also removes useful operating guidance. A profile can
then describe its specialist role well while omitting practical constraints
such as date grounding, evidence-first work, concise execution, tool routing,
correspondence boundaries, and early result preservation. The worker formerly
reported the task and phase but not the host's current local date or the tools
and worker operations actually available to that claim.

## Decision

Foxhound continues to invoke Hermes with ambient rules disabled. Reviewed
operating guidance belongs in the versioned profile prompt, alongside the
role-specific instructions that already determine its revision.

The built-in profile and the fictional shared-profile example establish these
invariants:

- use the worker-reported local date instead of inferring the date from task
  age or model knowledge, resolve `next week` as the subsequent calendar
  week, and verify weekday/date pairs;
- stay bounded to the requested objective and include background only when it
  affects a decision, action, or deliverable;
- inspect evidence before declaring it missing, stop when the result is
  supported, and label assumptions;
- use only the runtime toolsets and worker operations reported by the worker;
- distinguish preparing correspondence from contacting someone;
- preserve the plan/execute/external-action authority boundary; and
- create and validate the result draft early enough to record useful work
  before the turn limit.

The execution work-context schema advances to version 3. Its `runtime` object
contains the authoritative local `today` value and the exact profile toolsets.
Its `capabilities` object lists bounded knowledge layers, worker operations
available in the current phase, and whether external effects are allowed.
These values describe server-enforced capabilities; task text and retrieved
content cannot add to them.

Profiles remain free to add role-specific style, domain methods, and tool
routing, but those instructions must agree with the reported capabilities.
Guidance for an unavailable ambient integration is not copied into a profile.

## Consequences

- A recorded profile revision once again covers the execution guidance that
  materially shapes the run.
- Agents can ground proposed dates and route work without guessing what the
  host exposes.
- Profile prompts become somewhat longer, but the additional text replaces
  mutable ambient policy rather than duplicating it at runtime.
- Changing shared private guidance creates and deploys new immutable profile
  revisions; workflows already pinned to older revisions continue unchanged.

## Failure and rollback

If the new context cannot be assembled, the claim fails closed as before. A
consumer that requires an older work-context schema must be upgraded before
deployment. Rolling back the code restores schema version 2; rolling back a
private profile means selecting a previously published immutable revision, not
editing a revision in place.
