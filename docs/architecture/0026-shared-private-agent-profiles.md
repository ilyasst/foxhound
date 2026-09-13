# ADR 0026: Shared private agent profiles

## Status

Accepted.

## Context

Agent prompts are deployment policy. They can encode operating procedures and
change more frequently than Foxhound's public profile schema. Committing a real
coding or administrative profile would publish that policy and would also
encourage divergent copies in individual project repositories.

Foxhound still needs stable agent identity, exact execution-policy revisions,
and the same selectable agents in its card service and runner. A coding agent
may serve many repositories, so its definition belongs at the deployment level
rather than inside any one project.

## Decision

Foxhound ships only the `general` compatibility identity as a built-in agent.
Its current revision is the only built-in revision exposed for selection; a
former revision is retained solely to honor workflows already pinned to its
exact policy. All actual role profiles and prompt templates are provisioned in
a single absolute owner-only directory outside every Git checkout. An approved
private synchronization system may distribute the editable source of that
directory across machines and projects. Each deployment gives the identical
installed directory to both the task-card service and execution runner with
`--agent-profile-directory`.

That directory originally held one flat JSON manifest per profile.
[ADR 0027](0027-versioned-private-profile-store.md) supersedes that layout with
an editable source of prompt fragments, immutable compiled revisions, and a
catalog of what is currently offered; the synchronized source is published and
then installed into the owner-only directory the services read. Both layouts
load, so the migration is explicit rather than forced.

Installed directory permissions are `0700` and installed manifest permissions
are `0600`. The
strict loader additionally checks ownership, refuses symlinks and Git
checkouts, validates the allowlisted schema, and derives a stable revision from
the complete execution policy. Workflows persist only the profile ID and
revision. Inspection output never includes prompt text or private paths.

The repository contains
`examples/agent-profiles/example-coder.json` solely to demonstrate the public
schema. Its identity and prompt are visibly fictional. It is parsed in tests
to verify representative coding limits and exact Hermes argument construction,
but it is not loaded into the default registry and is not deployment policy.

## Consequences

- One privately managed role profile can be selected for work in every project.
- Profile prompts and deployment locations do not enter Git or forge metadata.
- Updating a manifest changes its revision, so already-bound workflows fail
  closed instead of silently receiving new policy.
- The card service and runner must be configured consistently; otherwise a
  selected private agent may be displayed but cannot be claimed.
- Public tests prove the generic contract with fictional material. Actual
  deployment validation is reported only as content-free private evidence.

## Failure and rollback

If the installed profile directory is unavailable, stop claiming workflows
bound to its profiles or explicitly select `general` before starting a new
workflow. Do not copy a private manifest into a repository as a workaround and
do not silently fall back when an exact persisted revision is unavailable.
