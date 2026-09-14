# ADR 0036: Consumer-scoped task-card claims for a second gateway

Status: accepted.

## Context

ADR 0011 sized the task-card service for exactly one client, "a trusted local
card gateway," singular. Every pacing and delivery mechanism in
`task_cards.py` follows from that assumption:

- `stats()` sums `pending`, `delivering`, `delivered`, `snoozed`, and `active`
  across the whole table with no consumer dimension.
- `claim_next()` atomically claims one due card under one lease. Nothing in
  the schema or the claim path records who is claiming; a caller's identity
  first appears only in `complete_delivery`, which records a `transport`
  value, and only after the card has already left the pacing pool.
- Nothing server-side limits how many cards one caller can hold at once. The
  "one card at a time" behavior described in ADR 0011 is a convention the
  existing chat gateway happens to follow by pacing its own requests; it is
  not enforced by `claim_next()`, which will hand out a fresh card on every
  call regardless of how many the same caller already holds.
- `act()` is, by contrast, already consumer-agnostic: it authorizes a reader
  action purely from the card's id, its exact version, and its `delivered`
  status. It does not check `claim_token_digest` or `transport`. Whoever
  currently has the delivered card's id and version can resolve it.
- Authentication is a single shared bearer token for the whole server
  (`TaskCardApplication.token`, compared with `hmac.compare_digest`). The
  server has no existing notion of "which caller" beyond "an authorized
  caller."
- A non-mutating, content-bearing read of pending/snoozed cards already
  exists as `TaskCardService.due()`, but it is not reachable over HTTP:
  `task_card_server.py` exposes only `stats`, `schedule`, `claim`,
  `delivered`, `delivery-failed`, and `action`. The only way to obtain a
  card's private text through the trusted-local-service boundary today is to
  `claim` it, which mutates it into `delivering`.

A second consumer is now wanted: a local browser console, one operator, one
persona, on the same host, reached over the same canonical IPv4 loopback bind
ADR 0011 already requires. It renders the same review cards as a page instead
of dripping them as chat messages. Its purpose is to show the queue, not to
drip it — a materially different consumption pattern from the existing
gateway, encoded nowhere in the current design.

Adding the console on top of the current design breaks three things quietly:
the pacing formula from ADR 0011 (`limit - (delivering + delivered)`)
double-counts across two independent readers of the same global sum; a card
delivered to one transport is structurally absent from the other, with no
signal in either that the rest of the queue exists; and the one-card, one-lease
drip model does not describe what a queue page needs, which is many cards
visible at once.

## Decision

### 1. Consumer identity is derived from authentication, never asserted by a request

Each bearer token the server is configured to accept is paired, in server
configuration, with exactly one fixed **role** drawn from a closed set:
`drip` (the existing chat gateway's pattern — cards are claimed and shown one
at a time) or `queue_view` (the console's pattern — cards are claimed to
populate a page). A request's consumer identity is the digest of whichever
configured token authenticated it (the same digest function already used for
claim-token capabilities), not a client-supplied field. No route gains a
`consumer` request parameter.

This is a correctness choice as much as a trust one: a self-declared
`consumer` string can collide or be mistyped even under one operator and one
persona (a second process pointed at the wrong role, a copy-pasted config),
silently corrupting the same pacing arithmetic this decision exists to fix.
Deriving identity from the credential that is already required for every
request removes an entire class of self-inflicted misattribution, and it
matches the repository's existing rule that identifiers used for
authorization are never taken from caller-supplied values (callback data,
claim tokens, and card content are all validated server-side, never trusted
as asserted).

The existing single-token deployment keeps working with no configuration
change: its one token defaults to role `drip` when no role is configured for
it, exactly reproducing today's behavior for the one caller that exists
today. Introducing the console is the only case that requires new
configuration — a second token file and its `queue_view` role — and that
configuration is additive to the running service, not a change to the first
token's behavior.

**Invariants:**

1. A request's consumer identity is always the accepting token's digest;
   no route accepts or trusts a client-supplied consumer field.
2. Every configured token has exactly one configured role at all times; a
   token accepted by `authorized()` whose role cannot be resolved causes
   every consumer-scoped operation (`claim`, `stats`) to refuse the request
   with a fixed error rather than default to any role. This is the "unknown
   consumer fails closed" case: it can only arise from a configuration
   defect, and the service must not guess through it.
3. A single configured token defaults to role `drip` and requires no new
   configuration; this is what keeps a single-gateway installation working
   unchanged.

### 2. A card belongs to exactly one consumer for as long as it is claimed

`claim_next()` records the resolved consumer identity on the card at the
moment of claim — extending, and moving earlier, the precedent `transport`
already set at delivery. `complete_delivery` and `fail_delivery` continue to
require only the card id, version, and claim token, exactly as today; they
need no consumer parameter because identity is already bound at claim time.

While a card is `delivering` or `delivered`, its content, id, and version are
visible only to the consumer whose claim produced that state. A different
consumer never receives that card's content, id, or version through `stats`,
`claim`, or any other route while it is held. The existing invariants are
unchanged: at most one active card per task, and `claim_next` claims
atomically.

Once a card is resolved (`done`, `keep_open`, `drop`) or returns to `pending`
(via `snooze`, lease expiry, or the operator repair in decision 5), it has no
consumer affinity. The next `claim_next` call to win the atomic race — from
either consumer — claims it next. Nothing stipulates that the same task's
future cards return to the consumer that handled it before.

**Rejected alternative: present the same card on both surfaces at once
(fan-out).** This was rejected because it requires `act()`'s existing
consumer-agnostic authorization — today a convenience, since only one surface
ever has the id and version — to become an arbiter of concurrent actions from
two surfaces that each believe they exclusively hold the decision. That is a
materially larger change to the version-fence model than this issue asks for,
and it does not match what the console actually needs: visibility that other
work exists, not an editable second copy of a decision already in flight
elsewhere.

### 3. `stats()` reports each caller's own load plus a content-free signal that other work exists

`stats()` keeps its existing shared fields for state nobody has claimed yet —
`pending` and `snoozed` are identical for every caller, because they are not
owned by anyone. For claimed state, the response is scoped to the caller's
resolved consumer identity:

- `delivering`, `delivered` — count only cards held by the calling consumer
  ("mine"). A gateway paces itself exactly as ADR 0011 already specifies,
  `limit - (delivering + delivered)`, and now the two terms are its own load,
  not the sum of every consumer's load.
- `elsewhere` (new) — a content-free count of cards currently `delivering` or
  `delivered` under any other consumer identity. It carries no card or task
  identifier and no other consumer's identity, consistent with ADR 0010's
  rule that aggregate reporting stays content-free. It exists so a queue that
  looks stalled has a visible, honest explanation instead of none.
- `active` — unchanged: the system-wide total, independent of caller.

No route gains a request parameter for this; the scoping comes from the same
authenticated identity used for claiming. A row whose `consumer` value cannot
be attributed (only possible for cards claimed by a pre-migration binary,
before this decision's column existed) is counted in `active` and in its raw
status bucket but excluded from every specific consumer's "mine" count and
from `elsewhere`. It cannot grow after activation, because decision 4 makes
claiming without a resolved consumer identity impossible, and it drains to
zero as those specific rows resolve.

**Rejected alternative: keep one global `stats()` response and let each
gateway subtract its own previously-observed claims client-side.** Rejected
because a restarted or newly started gateway process has no memory of what it
previously claimed, so client-side subtraction drifts from the true count
after any restart. The server already has ground truth per row; it should be
the one computing the count, not every client separately.

### 4. A per-role concurrency ceiling bounds how many cards one consumer may hold

`claim_next()` refuses to hand out a new card to a consumer already holding
its role's ceiling of cards (`delivering` + `delivered`, i.e. the same two
terms `stats()` now reports as "mine"). "Refuses" means the same thing an
empty queue means today: `claim_next` returns no card, not an error. A
consumer at its ceiling simply stops receiving new cards until it resolves or
releases one it already holds.

| Role | Ceiling (delivering + delivered) | Existing precedent |
|---|---|---|
| `drip` | one | ADR 0011's description of the existing chat gateway |
| `queue_view` | twenty | `TaskCardService.due()`'s existing default page size — the closest existing precedent in this codebase for "one screenful of queue" |

Setting the `drip` ceiling to one turns today's caller convention (the chat
gateway happens to pace itself to one card) into a server-enforced invariant.
This is a behavior hardening, not a behavior change, for any correctly
behaved existing gateway.

**Invariants:**

4. `claim_next` never lets a consumer's held count (`delivering` +
   `delivered` under its own identity) exceed its role's fixed ceiling.
5. Ceilings are fixed per role, not per request and not operator-configurable
   at runtime; changing them is a decision for a future ADR.

**Rejected alternative: unbounded concurrent claims for `queue_view`.**
Rejected because nothing else stops a `queue_view` consumer from claiming the
entire backlog in one burst — the per-task uniqueness index only prevents
claiming the same task's card twice, not claiming every task's card. An
unbounded console would starve the `drip` consumer of everything, turning
"show the queue" into "own the queue."

**Rejected alternative: a caller-supplied `max_claims` per request.**
Rejected for the same reason request-supplied consumer identity was rejected
in decision 1 — it reopens a self-asserted value where a fixed, reasoned
server-side constant is safer, and it departs from this repository's existing
preference (ADR 0035) for small fixed capacities over runtime-configurable
ones.

### 5. A gateway that goes away

The existing lease expiry already recovers a card stuck in `delivering`: an
unacknowledged claim expires and the card returns to `pending` under a new
version, exactly as today, unchanged by this decision.

A card that has reached `delivered` has no expiry today, and this decision
does not add one. An automatic timeout for `delivered` would require the
service to infer whether a browser tab or chat process is still alive, and
the current design has no heartbeat or liveness protocol to infer that from
— inventing one is out of scope here.

Instead, this decision extends the existing local-operator recovery already
established for execution cards in ADR 0016 — an explicit, transport-absent
administrative requeue of a still-current delivered card — to task review
cards, as the sanctioned way to release a card whose owning consumer has gone
away. The repair clears delivery metadata and increments the card version,
exactly as ADR 0016 describes for execution cards, so the stale presentation
becomes unusable without rerunning any lifecycle transition. It remains
absent from the remote/authenticated API, available only to a trusted local
operator tool.

This interacts with decision 4: previously, a vanished gateway could strand
at most one card (the `drip` ceiling). A vanished `queue_view` consumer can
now strand up to twenty. This decision accepts that trade-off in exchange for
the console being able to show a real queue at all, and names the operator
repair as the intended mitigation — but this specific trade-off has not been
exercised against a real vanished-console scenario, and is the weakest point
of this decision (see the closing note).

### 6. Migration and rollback

**Schema.** One nullable column, recording the claiming consumer's identity,
is added to `task_review_cards` alongside the existing `transport` column;
it is populated at claim time rather than at delivery. This follows the same
migration discipline already used for this table (ADR 0010: migration adds
structure, never mutates the meaning of existing rows). The exact column
name, migration number, and schema-version bump are implementation details
left to the follow-up change; this decision fixes only the column's shape
(nullable, populated at claim, one value per configured token) and semantics.

**Configuration.** Each accepted bearer token is paired with a role at
startup. The existing single-token invocation shape from ADR 0011 is
unchanged; a role defaults to `drip` when none is configured, so an upgraded
server with its original one-token configuration behaves exactly as before
with no edit to that configuration. Enabling the console means adding a
second token file and declaring its role as `queue_view` — the only
configuration step this decision requires of anyone, and only for
installations that actually want a second gateway.

**Pre-activation rollback** (schema migrated, no second token configured
yet): there is nothing to roll back. Every card is claimed under the sole
configured token, which defaults to role `drip`; the new column is always
populated with that one identity, and the server's observable behavior is
identical to ADR 0011's.

**Post-activation rollback** (a second, `queue_view` consumer has actively
claimed cards): revoke its token first, so no further request can authenticate
as it. Cards it already holds are not reassigned or deleted automatically.
Ones still `delivering` drain through the existing lease expiry, unchanged.
Ones already `delivered` require the decision-5 operator repair, because the
surviving `drip` consumer was never given their card ids or versions — only
the content-free `elsewhere` count in its own `stats()` response — and cannot
reach them through the ordinary `action` route without first learning them
from that repair tool. Restoring an older, pre-migration database is not
permitted once any reader action has been recorded against a card claimed
under either identity, consistent with the standing rule in ADR 0010 that a
recorded reader decision is authoritative and must never be rolled back by
restoring an older database.

**Absent or older schema.** The service's existing startup gate — it refuses
to run against an absent, incomplete, or older database, per ADR 0011 —
already prevents a pre-migration binary from misreading a migrated database,
and a migrated binary from misreading a database it has not migrated. This
decision relies on that existing gate rather than adding a second one.

## Invariants (summary)

1. Consumer identity is always the accepting token's digest; it is never
   accepted as a client-supplied value on any route.
2. A token with no configured role causes every consumer-scoped operation to
   refuse, never to default silently.
3. A single-token deployment requires no configuration change and behaves
   exactly as ADR 0011 describes, under an implicit `drip` role.
4. `claim_next` records the claiming consumer's identity at the moment of
   claim; `complete_delivery` and `fail_delivery` require no consumer
   parameter because identity is already bound.
5. While `delivering` or `delivered`, a card's content, id, and version are
   visible only to the consumer that holds its claim.
6. A resolved or re-pooled card carries no consumer affinity into its next
   claim.
7. `stats()` reports `pending` and `snoozed` identically to every caller,
   `delivering`/`delivered` scoped to the caller's own held cards, a
   content-free `elsewhere` count of cards held by other consumers, and an
   unchanged system-wide `active` total.
8. `claim_next` never lets a consumer exceed its role's fixed ceiling
   (`drip`: one; `queue_view`: twenty) of held cards.
9. Ceilings are fixed per role by this decision, not accepted from a request
   and not runtime-configurable.
10. A `delivering` card recovers only through the existing lease expiry; a
    `delivered` card recovers only through an explicit, transport-absent
    local operator repair, never automatically.
11. `act()`'s existing authorization — card id, exact version, `delivered`
    status, nothing else — is unchanged by this decision.

## Out of scope

This decision does not specify the exact schema migration or its version
number, the exact response-schema-version bump needed to carry the new
`elsewhere` field, the exact configuration file or flag syntax for pairing a
token with a role, or any console implementation or rendering. Those are
left to a follow-up implementation change to be reviewed against this record.

It also does not design more than two concurrently configured consumers or
any role beyond `drip` and `queue_view`; the two-role set is deliberately
closed, and extending it needs its own decision. It does not change `act()`'s
existing consumer-agnostic authorization. It does not design an automatic
liveness or heartbeat mechanism for a `delivered` card, and it does not
design a way to explicitly hand a specific card's content from one named
consumer to another — the only handoff this decision provides is the
decision-5 repair, which returns a card to the shared, unowned pool rather
than moving it directly to a chosen consumer.

## Closing note on confidence

The `queue_view` ceiling of twenty, and the choice to size it after
`due()`'s existing default rather than after any measurement of how large a
console's queue actually gets, is a judgment call, not a validated one. The
decision-5 trade-off it implies — a vanished console can now strand up to
twenty cards instead of one — has not been exercised against a real
vanished-console scenario. If that number turns out to be badly sized in
either direction, or if operators need to release a stranded batch faster
than the repair tool in decision 5 allows one card at a time, that would be
grounds to revisit this record rather than to patch around it.
