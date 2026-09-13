# ADR 0023: Strict agent profile registry

## Status

Accepted.

## Context

The supervised runner historically accepted one global Hermes command, prompt,
toolset string, and set of limits. Supporting distinct agents by copying those
arguments would make identity ambiguous and could let task content influence
execution authority.

Agent prompts and deployment locations can also contain private operational
context. They must remain outside Git and out of content-free administrative
output.

## Decision

Foxhound owns a strict versioned profile contract and registry. A profile has a
bounded identifier and display name, the fixed `hermes` runtime, a prompt
template, an allowlisted set of tool families, execution limits, safe lease
timing, and an explicit phase allowlist. The complete execution-relevant
document has a stable SHA-256 revision.

The built-in `general` profile reproduces the existing runner prompt and
limits. Additional profiles are JSON manifests in an explicitly configured,
absolute, owner-only directory outside every Git checkout. The directory and
regular manifest files must be owned by the current user; symlinks and
group/world permissions are refused. JSON shape and duplicate keys are checked
strictly. Manifests cannot specify commands, arguments, environment variables,
secrets, arbitrary tools, or unknown fields.

The inspection command lists IDs, display names, runtimes, and revisions. It
may show bounded policy fields, but it never emits prompt text, private paths,
or parser errors. An unavailable ID or changed revision is a refusal; there is
no fallback to `general`.

This registry is deliberately not yet a workflow selector. A later migration
will persist the selected ID and exact revision before the runner consumes
profiles.

## Consequences

- A deployment can prepare reviewed, host-private agent prompts without adding
  private material to the repository.
- Profile changes create new revisions and can be fenced by durable workflows.
- The first registry change does not alter which profile executes existing
  work.
- Deployments must provision private manifests separately and keep their
  permissions restrictive.
