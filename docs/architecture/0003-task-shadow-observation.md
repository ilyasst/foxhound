# ADR 0003: Legacy task shadow observations

Status: accepted for the passive comparison phase.

## Context

Foxhound can import source-owned task candidates, but a candidate ID is not a
legacy GW task ID. GW may fold several candidates into one task, while two
identical-looking actions in one source can remain separate tasks. Text cannot
reconstruct that association safely after the decision.

The passive pilot needs to observe the legacy decision without giving
Foxhound access to GW's writable database or allowing an observation to create
an active Foxhound task.

## Decision

The `foxhound.task-shadow-observation` version 1 contract binds one complete,
strictly validated candidate revision to one legacy disposition:

- `minted`: GW created the referenced task for this candidate;
- `folded`: GW associated this candidate with an existing referenced task;
- `refused`: GW deliberately created no task for a closed refusal reason; or
- `unmapped`: historical or source-limited evidence cannot establish a safe
  association.

`minted` and `folded` require a positive legacy task ID and a deterministic
digest. `refused` and `unmapped` require no task reference and use separate
closed reason vocabularies. These combinations are structural: a consumer
cannot reinterpret an absent task reference as refusal or a refusal as a
pending task.

For a version-1 candidate, the comparable digest is SHA-256 over the compact
JSON array:

```text
[task text, project, owner]
```

Those are the task fields represented independently by both current systems.
Due date is deliberately excluded because the legacy GW task row does not
retain it separately. Exclusion is a declared comparison limit, not a claim
that due dates agree.

For a project-less version-2 candidate, the corresponding array is:

```text
[task text, owner]
```

The embedded candidate version therefore determines the comparison shape.
This preserves every version-1 digest while allowing later observations to
avoid inventing a project value.

The observation carries the complete candidate so Foxhound can verify its
identity and revision against the candidate inbox before comparing the legacy
digest. Candidate content remains private state and is never suitable for
logs or repository artifacts.

## Validation and privacy

The dependency-free parser rejects unknown versions, fields, dispositions,
reason codes, invalid task references, invalid digests, naive timestamps, and
inconsistent disposition fields. A JSON entry point rejects duplicate fields
at every nesting level. Errors name fields and rules without echoing values.

The packaged JSON Schema mirrors the parser and references the existing task
candidate schema. Synthetic meeting and email fixtures are the only examples
committed to the repository.

## Out of scope

This contract does not record observations in GW, define ordered observation
delivery, compare an imported observation with the inbox, create Foxhound
tasks, acknowledge producer state, schedule work, render cards, or execute an
agent. Those remain separate consumer-first slices.
