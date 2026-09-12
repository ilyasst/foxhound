# ADR 0007: Bounded read-only GW knowledge client

Status: accepted for the pre-cutover pilot.

## Context

Task execution needs relevant knowledge, but access to a task candidate is not
permission to open GW files, import GW code, load a persona, or share its
writable database. Outerheaven already runs the authenticated read-only GW
search service as a separate process, so Foxhound can consume that narrow
capability instead of acquiring producer internals.

## Decision

Foxhound provides a dependency-free client for the version-1 GW search
contract. Configuration is passed explicitly in memory and contains one base
endpoint, one operator-defined alias, one bearer token, a timeout, and a
response-size ceiling. The client does not discover or read configuration from
the environment, a persona file, a repository, or GW state.

Unencrypted HTTP is accepted only for a literal loopback IP address. Other
connections require HTTPS with the standard certificate validation. Endpoints
with credentials, paths, query strings, or fragments are refused. Requests do
not use process proxy configuration and redirects are refused, so private task
queries and bearer credentials cannot be redirected or forwarded implicitly.

The search operation is a POST to `/v1/search`. Query length, layer selection,
context, per-document matches, per-layer results, timeout, and response bytes
are bounded. The default layer is `kb`; email or secondary-layer access must be
requested explicitly.

The complete response is validated before use. Schema and version, echoed
query and parameters, layer order, aggregate counts, document identifiers,
relative paths, excerpts, optional KB paths and sections, and optional ranking
metadata all have closed shapes. Absolute paths, parent traversal, duplicate
JSON fields, additional fields, inconsistent counts, unexpected media types,
and oversized bodies fail closed.

ADR 0009 adds a second fixed, read-only route to the same bounded client for
identity-bound task-owner equivalence. It does not broaden knowledge search or
grant either component task-mutation authority.

Returned excerpts remain private task context. Failures contain only a closed
classification and rule; they never echo the endpoint, token, query, response,
path, or excerpt.

## Failure and rollback

Search is synchronous and read-only. A timeout, authentication refusal,
transport failure, or invalid response produces no local or producer write.
Rollback is to stop constructing the client or remove its explicit runtime
configuration. Tasks and passive imports remain available without knowledge
retrieval.

## Out of scope

This client does not read a full document, retrieve GW variables, write the
knowledge base, mutate tasks, poll in the background, render cards, schedule
work, or execute an agent. A bounded task-context contract for selected GW
variables remains a separate producer-and-consumer slice.
