# ADR 0014: Strict execution-context client

Status: accepted for the execution-ownership migration.

## Context

A Foxhound-owned runner needs operator context as well as knowledge search.
Loading GW persona configuration, environment variables, databases, or files
would give Foxhound unrelated private state and recreate a shared authority.
Making the request task-specific would also keep GW aware of Foxhound task and
workflow state.

## Decision

The existing authenticated `GwKnowledgeClient` gains one fixed
`/v1/execution-context` operation. Its exact versioned request contains only
the configured alias. Its exact response echoes that alias and contains one
allowlist:

- a bounded display name;
- bounded operator-context text;
- at most 32 bounded, unique self aliases; and
- at most 32 bounded, unique canonical institution domains.

A SHA-256 revision covers the canonical JSON representation of all four
variables. Foxhound recomputes it before accepting the snapshot. Unknown
fields, invalid types or text, excessive content, duplicate values, malformed
domains, identity mismatches, and revision mismatches fail closed.

The snapshot deliberately contains no task or workflow identity, path,
environment-variable name or value, credential, source or pipeline setting,
machine inventory, or write capability. Foxhound remains task authority and
uses the separate read-only search operation for knowledge retrieval.

## Failure and rollback

An unavailable or invalid snapshot is an execution-input failure; it must not
be replaced with direct configuration or filesystem access. The execution
claim can be released or retried under the workflow ledger's existing policy.
Rollback is to stop calling this operation. Search and owner equivalence remain
independent.

The matching GW provider is intentionally a separate producer change. Until
that provider is deployed, this client method fails closed at the transport
boundary and no existing behavior changes.
