# ADR 0017: Loopback execution-card service

Status: accepted for the execution-ownership migration.

## Context

A private chat gateway needs to deliver and act on execution review cards, but
direct SQLite access would duplicate card logic and expand the database trust
boundary. Sending execution cards through the ordinary task-card namespace
would also obscure which callbacks control task lifecycle and which control an
execution gate.

## Decision

The existing `foxhound-task-cards` loopback service gains a distinct
`/v1/execution-cards/*` route family. It exposes the same narrow adapter shape
as task-review cards:

- aggregate active-card statistics;
- an explicit bounded scheduling pass;
- one digest-fenced delivery claim;
- delivery acknowledgement or failure;
- one versioned reader action; and
- one bounded, versioned discussion or reassignment input;
- one bounded, versioned Comment-and-Go input that advances the card atomically;
- one bounded, versioned deliverables read for a still-delivered Plan Review
  or Result Review card; and
- one version-fenced result-artifact listing and one verified file read for
  the same delivered review cards; and
- bounded eligible-agent options for a current Start card; and
- one opaque-token agent selection that returns a refreshed presentation.

Each route uses the existing exact request contract, bearer authentication,
canonical IPv4 loopback bind, body and response limits, request timeout,
serialized application handling, no-store response policy, and content-free
error boundary. Execution routes use distinct response schemas and delivery
keys. Only a successful claim returns private rendered content and its
short-lived delivery capability.

### Claim schema versions and source routing

The `/v1/execution-cards/claim` response uses the `foxhound.execution-card-service.claim` schema:

- **Version 1 (`schema_version: 1`)**: Contains `card_id`, `card_version`, `kind`, `phase`, `claim_token`, `expires_at`, `delivery_key`, `superseded_delivery_ref`, `superseded_transport`, `body`, `reply_markup`, and optional `voice_artifact_name`.
- **Version 2 (`schema_version: 2`)**: The current default response shape (Issue #688). Extends the claim payload with `source_kind: str | null`, populated from the underlying task workflow's origin (`issue`, `review_request`, `meeting`, `email`, `teams`, `calendar`, `mention`, `alert`, `legacy`, or `null`).

Clients may request version 1 explicitly via `claim_version: 1` in the request document. A complete rendered body may exceed one chat
message while remaining within the bounded response contract; safe transport
chunking does not weaken the card's single versioned decision capability.
Free-text input is accepted only at `/v1/execution-cards/input`; prompt-only
callbacks make no durable change, and stale or malformed responses fail
without partial task, workflow, card, or event writes.

Each execution result permanently records the manifest-authorised artifact
name, relative path, byte size, digest, and archive run directory in the
ledger. The public artifact list discloses only ordinal, filename, and size.
The byte route is available only when the packaged server is configured with a
canonical `--task-work-root`; it refuses paths outside that root, symlinks,
non-regular files, changed sizes, and changed digests. Archive paths and
digests never appear in a response.

Trusted integrations open the agent selector through the dedicated service
operation; the legacy-compatible Start keyboard does not add an Agent button.
Each choice uses a separate namespace and an opaque 20-character
digest of the exact profile ID and revision. Even with maximum SQLite integer
identities, the complete callback is at most Telegram's 64-byte limit. The
service maps the token only against installed profiles eligible for planning;
unknown, ambiguous, malformed, oversized, stale, or phase-ineligible choices
change nothing. A successful changed selection updates the workflow and the
same delivered card atomically, then returns its new body and keyboard so the
gateway can edit the existing message. Exact replay returns the current
presentation unchanged.

The service audit record contains only the allowlisted route, HTTP method,
status, and coarse duration. It contains no task/card identity, content,
delivery reference, or capability. The ordinary `/v1/task-cards/*` routes and
responses remain unchanged.

The packaged server loads the same strict registry format as the runner via
`--agent-profile-directory`. A deployment that installs private profiles must
pass the same owner-only directory to both processes. An unavailable selected
revision fails Start-card materialization rather than silently falling back;
later review cards remain deliverable so completed work cannot become trapped.

The execution-card adapter is explicitly supplied to the application. If it
is absent, execution routes return a content-free service-unavailable response
instead of falling back to direct workflow operations. The packaged service
validates both card stores before listening, but startup neither schedules a
card nor advances any task or workflow state.

## Failure and rollback

Rollback is to stop the loopback service or omit the execution-card adapter.
Durable cards and workflow gates remain in SQLite. A delivery claim expires
without granting workflow authority, and a later invocation can retry it. Do
not give the gateway database access as a fallback.

## Out of scope

This slice does not implement a chat client, install a scheduler, launch an
agent, migrate a backlog, deploy a service, or change GW.
