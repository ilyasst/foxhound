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

The built-in `general` profile reproduces the existing runner prompt, toolsets,
and phase permissions. Synthetic comparison trials exhausted its former
12-turn budget in three representative cases; longer successful trials needed
22 and 26 turns to record a result. Its current local-work budget is therefore
50 turns and 1,800 seconds, with a 2,700-second claim lease, 60-second
heartbeat, and 30-second shutdown grace. This matches the already validated
fictional coding-profile envelope, leaves headroom above the successful trials,
and preserves the required timing relationships.

The former built-in `general` revision remains available only for exact
resolution by workflows already pinned to it. Registry listing, ordinary ID
lookup, and agent selection expose only the current revision. A historical
Start card can still offer the current revision for an explicit reader
reselection. A historical revision cannot duplicate another exact ID/revision
pair, and it does not need a currently listed profile ID: a profile that is no
longer offered at all still resolves exactly for work already pinned to it,
without appearing in any selector.

Actual role profiles live in one explicitly configured, absolute, owner-only
directory outside every Git checkout. A deployment may share the editable
source of that directory across projects through an approved private
synchronization system. The directory holds either one flat manifest per
profile or the versioned store of
[ADR 0027](0027-versioned-private-profile-store.md): a catalog of the revisions
currently offered plus the immutable revision manifests it names. Directories
and regular manifest files must be owned by the current user; symlinks and
group/world permissions are refused. JSON shape and duplicate keys are checked
strictly. Manifests cannot specify commands, arguments, environment variables,
secrets, arbitrary tools, or unknown fields.

The repository may contain visibly fictional example manifests for contract
documentation and tests. Examples are not built-in agents and must not be used
as deployment policy. The shared private deployment contract is recorded in
[ADR 0026](0026-shared-private-agent-profiles.md).

The inspection command lists IDs, display names, runtimes, and revisions. It
may show bounded policy fields, but it never emits prompt text, private paths,
or parser errors. An unavailable ID or changed revision is a refusal; there is
no fallback to `general`.

Workflow selection and runner consumption are defined separately in
[ADR 0024](0024-workflow-agent-binding.md).

## Consequences

- A deployment can prepare reviewed, host-private agent prompts without adding
  private material to the repository.
- Profile changes create new revisions and can be fenced by durable workflows.
- A private profile that is withdrawn from selection keeps resolving for the
  work already pinned to it.
- Existing workflows pinned to the retained former `general` revision keep its
  exact execution policy until explicitly reselected.
- The first registry change does not alter which profile executes existing
  work.
- Deployments must provision private manifests separately and keep their
  permissions restrictive.
