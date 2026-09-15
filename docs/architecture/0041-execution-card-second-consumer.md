# ADR 0041: Second-consumer access to execution cards

Status: accepted for the execution-card console follow-up.

## Context

ADRs 0016 and 0017 define a durable execution-card aggregate and its
loopback service. The aggregate has its own lifecycle, delivery lease,
version fence, four card kinds, and rendered payload. ADR 0035 separately
limits running workflow phases, queued planning phases, and workflows waiting
for a reader. None of those decisions authorizes a second reader of execution
cards.

ADR 0036 is the decision for a second consumer of *task review cards*. Its
conclusions cannot simply be copied: an execution card can contain a reviewed
agent binding, worker output, questions, deliverables, and proposed external
effects. It also controls an execution workflow rather than only task
lifecycle, and its capacity must not be confused with the workflow capacities
in ADR 0035.

## Decision

### 1. Authorization is scoped per aggregate

Consumer permissions are configured independently for the task-card and
execution-card aggregates. A credential may be granted `drip`, `queue_view`,
or no access for each aggregate; the request never supplies a role or
aggregate identity. The service derives consumer identity from the digest of
the authenticating bearer token and fails closed when the configured role is
absent or unknown.

The compatibility profile grants `drip` permission for execution cards when no
second-consumer permission is configured. A console credential granted
`queue_view` for execution cards has no permission on task cards unless that
permission is separately configured. A credential granted `queue_view` for
task cards likewise cannot read or act on execution cards. This prevents a role
intended for one aggregate from becoming an accidental service-wide authority.

**Invariant 1:** no request field can select, widen, or impersonate a
consumer; an absent, ambiguous, or unknown aggregate role is rejected before
the aggregate is touched.

### 2. The console gets a non-mutating, bounded execution projection

The `queue_view` role receives a read equivalent to the service's existing
due-card selection. It returns only current `pending` and due `snoozed` cards,
in the existing result-review, plan/external-review, then start ordering. A
read creates no claim, lease, event, version, workflow change, or capacity
usage. Delivered or delivering cards are never returned to a second consumer;
they appear only through aggregate counts.

The projection is a separate allowlist, not serialization of
`ExecutionReviewCard`. It includes the card id and exact card version needed
for a later fenced decision, kind and phase, safe task and owner display,
bounded summary/work digest, questions, and the exact bounded proposed
effects or deliverables required to judge a review. It may include an agent's
display name, but never profile identifiers or revisions, raw prompts,
working-directory or knowledge-base paths, claim tokens, delivery references,
or hidden provenance. Dynamic values are escaped and bounded by the existing
service response ceiling. The complete card remains available to the existing
`drip` delivery path under ADRs 0016/0017.

**Invariant 2:** a queue read discloses only the allowlisted projection and
never changes durable state or exposes another consumer's card identity.

### 3. Execution actions resolve at submit time

The console does not claim a card merely to display it or while a reader is
thinking. Its one resolve request contains the card id, exact card version,
action, and (where required) one bounded validated note or selection. The
service atomically verifies that the card is still pending or due-snoozed at
that version, claims it for the authenticated execution `queue_view`
consumer, and applies the existing action against the exact task and workflow
versions. The internal claim, delivery-state transition, workflow/task
transition, card resolution, and append-only events commit together; a stale
version, invalid action, changed workflow, or validation failure commits
nothing. The response is content-free apart from the operation disposition and
safe refusal code.

This is safe for execution cards because the projection contains all
reader-decision material needed by each allowed action, while the service
rechecks the bound workflow, phase, result identity, agent binding, and
task version in the same transaction. Free-text discussion, reassignment,
Comment-and-Go, and agent selection remain bounded, separately validated
inputs; they are not inferred from a projection or accepted through callback
data. No execution action is authorized from card id alone.

**Invariant 3:** every successful second-consumer action is one version-fenced
transaction; retries with the old version are stale and cannot repeat it.

The existing `drip` flow is unchanged: it may claim, acknowledge delivery,
and use the established versioned action/input operations. The service must
not make the console call those separate transport operations.

### 4. Ceilings are independent from workflow and aggregate capacities

ADR 0035's limits on running phases, queued planning reserve, and active
reader-waiting workflows remain unchanged. Those are workflow/scheduling
capacities, not consumer lease limits. The execution-card service adds a
fixed per-role held-card ceiling, counting only `delivering` and `delivered`
cards attributed to that consumer:

| Role | Execution-card ceiling | Reason |
| --- | ---: | --- |
| `queue_view` | 2 | headroom for two genuinely overlapping resolve requests; browsing holds none |
| `drip` | 20 | fixed compatibility ceiling; changes require revalidation against the drip contract |

Submit-time resolve normally holds a claim only for one request, but the
ceiling remains a server-side backstop against buggy retries or partial
failure. Reaching it is reported distinctly from an empty queue and reveals
only the caller's own held count and ceiling. It never blocks task-card
capacity, workflow scheduling, or another consumer's execution claims.

**Invariant 4:** no consumer can exceed its fixed execution role ceiling, and
no execution-card ceiling is used as an ADR 0035 workflow-capacity signal.

### 5. Stats are per aggregate and content-free

Execution stats gain the same consumer-scoped shape as task-card stats, but
only within the execution-card aggregate: pending and snoozed are shared;
delivering and delivered count the caller's cards; `elsewhere` counts cards
held by other attributable consumers without naming a card, task, role, or
credential; and `active` remains the system-wide execution-card total.
Legacy execution stats keep their existing version and fields for the
compatibility caller. Scoped stats are additive and explicitly versioned.

No task-card count is folded into execution stats, and no execution-card
content is exposed by stats, audit records, or an at-ceiling response.

**Invariant 5:** every count is scoped to the execution aggregate and every
cross-consumer disclosure is a content-free number.

### 6. Migration, rollback, failure, and repair

Migration adds one nullable consumer-identity column to
`execution_review_cards`; it changes no existing row and creates no card. A
pre-activation row with no consumer is included in global status counts but
belongs to no consumer and is not `elsewhere`. After activation, every claim
requires a resolved role and records the digest at claim time. Existing
cards claimed before activation therefore drain normally without reassignment.

Before a console credential is enabled, no new route is required and the
compatibility profile remains valid. After activation, removing the console
credential stops new queue reads/resolves; it does not cancel cards,
rewrite workflows, or revoke an already committed action. A rollback that
stops the new routes leaves durable cards and workflow gates intact; the
drip consumer may continue draining them. There is no direct database
fallback.

Lease expiry and delivery failure return an execution card to pending under a
new version. A crash after a transport delivery acknowledgement but before a
legacy drip action can leave a delivered card, as already permitted by ADR
0016/0017; it must never be guessed or automatically replayed. The existing
local operator repair requeues only the still-current delivered card, clears
delivery metadata, and increments its version. The repair is not exposed to
the remote console and cannot advance a workflow. Resolve's atomic sequence
has no reader-controlled think-time window; if its transaction fails, all of
its writes roll back.

**Invariant 6:** migration is additive, rollback is non-destructive, failures
fail closed without partial workflow authority, and repair invalidates stale
presentations before retry.

## Rejected alternatives

- **Service-wide roles:** rejected because task and execution cards have
  different payload sensitivity and authority; granting a console role on one
  aggregate must not grant it on the other.
- **Expose the full execution-card dataclass:** rejected because it would
  disclose profile bindings, prompts, paths, and internal provenance that the
  reader does not need to choose a card action.
- **Claim cards to populate the page or hold them while reading:** rejected
  because abandoned browser sessions would consume delivery leases and
  require a new liveness/recovery protocol.
- **Let the console call claim, delivered, and action separately:** rejected
  because a crash between those calls creates partial execution authority and
  makes version-fenced replay ambiguous.
- **Share ADR 0035's workflow ceilings or one global card ceiling:** rejected
  because scheduling capacity and consumer delivery capacity answer different
  questions and must remain independently observable.

## Out of scope

This ADR does not implement HTTP routes, schema names, a browser client,
configuration-file syntax, scheduling, agent launching, or changes to task
review cards. Those follow-up changes must preserve the numbered invariants
above and use synthetic evidence only.
