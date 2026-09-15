# ADR 0036: Consumer-scoped task-card claims for a second gateway

Status: proposed.

Implementation note (issue 194): the existing `/v1/task-cards/stats` route
and response remain unchanged (schema version 1 and its original field set)
for legacy/drip clients. Consumer-scoped stats, including `elsewhere`, are
available at the explicitly versioned `/v2/task-cards/stats` route, whose
response uses `STATS_SCHEMA_VERSION` 2. Both routes continue to validate the
shared request contract at `SERVICE_VERSION` 1, so existing callers are not
silently forced to understand the new field.

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
- Nothing server-side limits how many cards one caller can hold at once.
  ADR 0011 describes claiming "one card under a bounded delivery lease" —
  one card per request, not one card held in total — and its very next
  sentence assumes a caller may hold more than one: "A gateway computes its
  on-screen load as `delivering + delivered`," arithmetic that would be
  pointless if the answer could only ever be zero or one. The existing chat
  gateway in fact paces itself to a configurable on-screen count, not to a
  single card, and reaches it by calling `claim_next()` repeatedly in one
  pass until its own count of `delivering + delivered` closes the gap.
  Nothing server-side enforces even that self-imposed count: `claim_next()`
  hands out a fresh card on every call regardless of how many the same
  caller already holds.
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
`drip` (the existing chat gateway's pattern — cards are claimed up to the
gateway's own configured on-screen count and dripped into chat) or
`queue_view` (the console's pattern — a card is read without claiming, and
claimed only at the moment a reader acts on it; see decision 4). A request's
consumer identity is the digest of whichever
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
disclosed only to the consumer whose claim produced that state: no route
(`stats`, `claim`, or any other) hands that card's content, id, or version to
a different consumer while it is held. This is enforced by non-disclosure,
not by an access check on the card itself — `act()` stays consumer-agnostic
(summary invariant 13 below), so anyone who does come to hold a delivered card's
exact id and version can resolve it regardless of who claimed it. The
guarantee therefore depends entirely on this decision never handing that
identifier to a second consumer through any route, and it is only as strong
as that. Decision 3 already concedes that a legacy, unattributable row can
exist; such a row's identity is not "held" by anyone in the sense this
invariant describes, and it is deliberately excluded from every consumer's
"mine" accounting rather than treated as evidence the guarantee has an
authorization backstop it does not have. The existing invariants are
unchanged: at most one active card per task, and `claim_next` claims
atomically.

Once a card is resolved (`done`, `keep_open`, `drop`) or returns to `pending`
(via `snooze`, lease expiry, or the operator repair in decision 6), it has no
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
from `elsewhere`. It cannot grow after activation, because decision 1's
fail-closed rule makes claiming without a resolved consumer identity
impossible, and it drains to zero as those specific rows resolve.

**Rejected alternative: keep one global `stats()` response and let each
gateway subtract its own previously-observed claims client-side.** Rejected
because a restarted or newly started gateway process has no memory of what it
previously claimed, so client-side subtraction drifts from the true count
after any restart. The server already has ground truth per row; it should be
the one computing the count, not every client separately.

### 4. The console reads the queue read-only and claims only at the moment of action

Rendering a queue page must not require holding a stack of leases just to
have their content ready. `TaskCardService.due()` already exists as a
non-mutating, content-bearing projection of `pending` and `snoozed` cards
(noted in Context); this decision authorizes exposing it over HTTP, gated to
the `queue_view` role, as the console's sole means of populating its page.
Reading it claims nothing, mutates nothing, and issues no lease. This
amends ADR 0011's statement that "only a successful claim contains private
card text": for the `queue_view` role specifically, a read of the queue also
does, because that role's entire purpose is to display many cards without
committing to deliver any of them.

The console never displays a card that is `delivering` or `delivered` under
any consumer, including one it is itself in the middle of acting on —
`due()`'s existing query already excludes both statuses, and decision 2
requires that exclusion regardless: content held by one consumer must not
reach a second one. A card currently on the chat gateway's screen is
represented on the console only as one unit of the content-free `elsewhere`
count from decision 3, never by its own text, id, or version — the console
can know "N cards are elsewhere," never "which N." This is a consequence of
decision 2, not a defect introduced here.

The reader decides what to do with a card entirely from the content the read
already gave them; nothing about deciding requires the card to be claimed
first. Claim-on-demand has two sub-variants that follow from that, and this
decision chooses between them explicitly rather than by default:

- **Hold across think-time.** Claim the card the moment the reader starts
  acting on it, then carry that claim through delivery and action as
  separate steps, however long that takes. This was this record's earlier
  choice.
- **Resolve at submit time.** Do not claim anything until the reader has
  already chosen an action. A single request carries the card's id, its
  exact version, and the chosen action together; the service claims that
  specific row by identifier if it is still `pending` or `snoozed` at that
  version, refusing immediately if it has moved (the same stale-version
  refusal `act()` already gives, for the same reason), and — only if the
  claim succeeds — carries it through delivery and the action in the same
  server-side sequence before returning a single result. Nothing about a
  reader's browsing, reading, or deciding ever claims a card; only the act of
  submitting a decision does, and the claim's entire lifetime is that one
  request.

This decision adopts **resolve at submit time**. It shrinks the abandonable
window from a reader's think-time — unbounded, since nothing forces a reader
to finish what they start — to the duration of one server-side request
sequence. A reader cannot abandon a card between claiming it and deciding
what to do with it, because by construction they have already decided before
any claim exists. Decision 6 depends on this choice and revisits what, if
anything, decisions 5 and 6 still need as a result.

**Rejected alternative: claim-to-display, where `queue_view` claims up to a
fixed ceiling of cards purely to have their content ready to render.** This
was this record's original decision, and it is rejected here in favor of
claim-on-demand for three reasons. First, it manufactures a much larger
stranding surface in proportion to its own ceiling: claim-to-display's
ceiling had to be sized for "one screenful," so an abandoned session strands
a large fraction of a large number, where claim-on-demand's ceiling (decision
5) is sized only for how many resolve requests can genuinely overlap, so an
abandoned or interrupted resolve strands at most that same small headroom —
a proportional cost closer to the `drip` role's, not the outsized one this
record's earlier draft accepted. Second, its ceiling had to
be picked from something; the only candidate in this codebase was `due()`'s
own default page size, which sizes a read, not a lease, and using it to size
a lease ceiling was exactly the kind of unverified number this decision
should not be making twice. Third, it treats "look at the queue" and "commit
to acting on one card" as one operation when the service already has two —
`due()` and `claim_next()` — built for exactly that distinction; collapsing
them back together to avoid building a claim-by-identifier path traded a
smaller, well-understood new mechanism for a larger, less-examined one.
Claim-on-demand is not free — it needs both a new HTTP-reachable read and a
new claim-by-identifier capability, neither of which exists today — but that
cost is smaller and more targeted than a role-ceiling system paired with the
extended manual repair decision 6 would otherwise have had to lean on as its
primary mitigation.

**Rejected sub-variant: hold across think-time.** This record adopted this
sub-variant through its previous two revisions before rejecting it here.
Under it, a claimed card sits `delivering` and then `delivered` for however
long the reader takes to finish acting on it, with nothing forcing that to be
short — closing the page, switching away, or simply pausing all leave the
claim held with no natural end. Two earlier revisions tried to bound the
consequence of that — a fixed ceiling, and a report that let a consumer see
its own held count — without removing the cause, and the ceiling's own
mitigation depended on a remedy ("reopen the console and finish the
unfinished card") that no route in this record actually provided, since nothing
returns a consumer's own held cards under that sub-variant. Resolve at submit
time removes the abandonable window instead of bounding it: there is no
point in the reader's interaction where a card is claimed but not yet decided,
because the claim is created by, and only by, the act of submitting a
decision.

### 5. A per-role concurrency ceiling, now mostly a backstop against a buggy caller

`claim_next()` (including the claim-by-identifier step inside resolve)
refuses to hand out a card to a consumer already holding its role's ceiling
of cards (`delivering` + `delivered` — the same two terms `stats()` reports
as "mine"). Reaching that ceiling and finding no due card are different
conditions, and this decision keeps them reported differently: the response
distinguishes "no due card exists for anyone" from "you already hold your
role's ceiling." The at-ceiling report contains nothing about any other
consumer or any card — only the caller's own held count and its own role's
ceiling, both of which the caller already implicitly knows from its own
request history. Reporting them back is self-disclosure, not a breach of
decision 2's guarantee.

Decision 4's move to resolve-at-submit-time changes what this ceiling is
for. Earlier drafts sized `queue_view`'s ceiling for a reader's ordinary
think-time — working one card, switching to another before finishing, a
second open tab — because a claim could sit open for as long as the reader
took to decide. Under resolve at submit time, deciding no longer claims
anything: a `queue_view` consumer claims a card only for the duration of one
already-decided, server-side resolve sequence. There is no longer a
legitimate reason for it to hold more than one such claim at a time except
the narrow case of two resolve requests — from two open tabs, or two
near-simultaneous clicks — genuinely overlapping in flight. The ceiling
shrinks accordingly:

| Role | Ceiling (delivering + delivered) | Basis |
|---|---|---|
| `queue_view` | two | One for whichever resolve sequence is currently executing, and one slot of headroom so a second, genuinely concurrent resolve from another tab is not refused purely for bad timing. No longer sized for reader think-time, because resolve at submit time gives no legitimate reason to hold a claim while a reader is still deciding. This is still a judgment call, not a value read off another system, and should be revisited if concurrent resolves prove to need more room in practice. |
| `drip` | twenty | The existing chat gateway's own on-screen pacing setting — a value the operator configures on the gateway side — defaults to five and is permitted up to twenty; the gateway sizes each pacing pass as that configured cap minus its own current on-screen count. Unaffected by decision 4's change, since `drip` never adopted claim-on-demand. |

The `drip` figure is unchanged from the previous revision and for the same
reason: it is the gateway's own documented maximum, not a number invented in
this record, and lowering it below that maximum would silently throttle a
correctly configured gateway.

**Invariants:**

4. `claim_next` (both the due-order path and the claim-by-identifier step
   inside resolve) never lets a consumer's held count exceed its role's
   fixed ceiling: two for `queue_view`, twenty for `drip`.
5. Ceilings are fixed per role, not accepted from a request and not
   runtime-configurable. Changing either number is a decision for a future
   ADR, and any future reduction of the `drip` ceiling must first confirm
   what the chat gateway's own pacing setting actually permits at that time.
6. A claim attempt that fails because the caller already holds its role's
   ceiling reports that fact — its own held count and its own ceiling, and
   nothing else — distinctly from a claim attempt that fails because no due
   card exists. Neither report ever names a card, a task, or another
   consumer.

**Rejected alternative: no ceiling at all, relying entirely on resolve at
submit time to keep `queue_view`'s footprint small, and on the drip
gateway's client-side pacing for `drip`.** Rejected because neither is a
server-side guarantee: a bug in either consumer (a retry loop, a resolve
handler that claims but never completes, a mis-sized pacing pass) would
otherwise have nothing stopping it from claiming without bound. A small
fixed ceiling costs nothing for a correctly behaved caller of either role
and catches exactly that failure mode — which is also why the ceiling
survives decision 4's change at all, even though it is no longer defending
against reader think-time.

**Rejected alternative: a caller-supplied `max_claims` per request.**
Rejected for the same reason request-supplied consumer identity was rejected
in decision 1 — it reopens a self-asserted value where a fixed, reasoned
server-side constant is safer, and it departs from this repository's existing
preference (ADR 0035) for small fixed capacities over runtime-configurable
ones.

**Rejected alternative: drop the at-ceiling report now that resolve at
submit time removes the ordinary case it was written to explain.** Kept
instead, but repurposed. It no longer exists to help a reader understand why
their browsing is being throttled — under resolve at submit time, ordinary
browsing never claims anything, so ordinary use essentially never reaches
the ceiling at all. What reaching it now indicates is genuinely abnormal:
either a caller bug repeatedly claiming without completing, or a leftover
card stuck from the rare partial-sequence failure decision 6 describes. Both
are exactly the conditions worth surfacing distinctly rather than folding
into a generic "no card" response, so the report is kept as a bug and
anomaly signal rather than removed as dead weight from the previous model.

### 6. A consumer that goes away

The existing lease expiry already recovers a card stuck in `delivering`: an
unacknowledged claim expires and the card returns to `pending` under a new
version, exactly as today, unchanged by this decision.

A card that has reached `delivered` has no expiry today, for either role,
and this decision does not add one. An automatic timeout for `delivered`
would require the service to infer whether a browser tab or chat process is
still alive, and the current design has no heartbeat or liveness protocol to
infer that from — inventing one is out of scope here. Note that this gap
already exists for the single gateway running today: nothing in
`task_cards.py` can recover a task review card that reached `delivered` and
was then never acted on, regardless of how many gateways exist. This
decision does not introduce that exposure; it inherits it.

The remedy is the local-operator recovery already established for execution
cards in ADR 0016 — an explicit, transport-absent administrative requeue of
a still-current delivered card — extended here to task review cards. The
repair clears delivery metadata and increments the card version, exactly as
ADR 0016 describes, so the stale presentation becomes unusable without
rerunning any lifecycle transition. It remains absent from the remote,
authenticated API, available only to a trusted local operator tool.

**What "abandoned" means under resolve at submit time.** A reader who opens
the console, reads several cards, and walks away without acting has claimed
nothing — under decision 4, browsing and deciding never claim a card;
submitting a decision does. There is no version of "the reader closed the
page before finishing" left to abandon, because there is no window between
claiming a card and finishing it that a reader's own behavior controls. This
is the specific defect an earlier revision of this record had: it offered an
at-ceiling report that told a reader they had an unfinished claim and could
"resolve or explicitly abandon" it, when no route existed that let them see
which card that was, or act on it, once the page that made the claim was
gone. Resolve at submit time removes the scenario that sentence was
describing, rather than requiring a route to make it true.

What remains is narrower and does not depend on reader behavior at all: a
resolve sequence — claim by identifier, then delivery, then the action —
that is interrupted after the card reaches `delivered` but before the
action commits. A process crash between those two steps, a lost database
connection, or an ordinary refusal at the final action step (the task moved
under it, the version fence caught a race) can all leave a card `delivered`
with nothing further happening, through no lapse of the reader's and nothing
the console's own logic can retry, since by the time it would retry, the
card is no longer at the version any retry would expect. Resolve at submit
time shrinks this window from a reader's unbounded think-time to the
duration of one server-side sequence, but it does not close it; a failure
partway through still strands a card, just far more rarely and far more
briefly than before.

This is not a new hazard invented by this decision. It is the same class the
`drip` gateway already accepts today: its own delivery loop stops rather
than releases a card when a transport acknowledgment's outcome is ambiguous,
specifically to avoid the worse failure of duplicating an already-delivered
card. `queue_view`'s resolve sequence makes the same trade for the same
reason — completing the delivery step before the action step means a crash
in between must not silently retry and risk applying an action twice, so it
leaves the card exactly where it stopped rather than guessing. Decision 5's
ceiling bounds how many such leftovers can accumulate before new resolves
are refused; it does not, and is not meant to, prevent any single one.

The remedy for a card that does end up stuck this way is the same local-
operator recovery described above, and it is now sized correctly for what
remains: a rare, crash-class event common to both roles, not a routine
consequence of ordinary reading. This decision does not add a `queue_view`-
specific recovery path beyond it — a self-read route returning a consumer's
own held cards, which would have been necessary to make the previous
revision's remedy sentence true, is no longer needed, because the case it
would have served (an ordinary reader coming back to finish what they left
open) no longer arises under resolve at submit time. What is left is a
crash-adjacent failure indistinguishable in kind from `drip`'s own accepted
risk, which has never needed one either.

**Rejected alternative: bound a `queue_view`-held `delivered` card with a
short action-completion window.** Considered in the previous revision for
the think-time model this one replaces, where it would have addressed only
the vanished-or-abandoned case while leaving the ordinary multiple-tabs case
to headroom regardless. Under resolve at submit time the case it was
proposed for — a reader who has claimed a card and is still deciding what to
do with it — no longer exists, so there is nothing left for a completion
window to bound that the sequence's own execution time does not already
bound implicitly. It remains rejected, now because it has no remaining
target rather than because its trade-offs were unfavorable.

### 7. Migration and rollback

**Schema.** One nullable column, recording the claiming consumer's identity,
is added to `task_review_cards` alongside the existing `transport` column;
it is populated at claim time rather than at delivery. This follows the same
migration discipline already used for this table (ADR 0010: migration adds
structure, never mutates the meaning of existing rows). The exact column
name, migration number, and schema-version bump are implementation details
left to the follow-up change; this decision fixes only the column's shape
(nullable, populated at claim, one value per configured token) and semantics.

**New routes.** Decision 4 requires two HTTP-reachable operations that do not
exist today: a `queue_view`-gated read equivalent to `due()`, and a resolve
operation that takes a card id, its exact version, and a chosen action, and
performs claim-by-identifier, delivery, and the action as one server-side
sequence behind a single response. Both are additive to the route table; no
existing route's request or response shape changes because of them, and
resolve does not reuse or extend the existing `claim`, `delivered`, or
`action` request shapes as separate client-visible steps — a `queue_view`
consumer never calls them individually. Resolve's response carries the same
three outcomes decision 5 requires of any claim path (resolved, empty, or
at-ceiling), plus whatever refusal `act()` would have given had it been
called directly, since resolve's final internal step is exactly that call.
Exact paths, request/response schemas, and contract-version numbers are
implementation details left to the follow-up change; this record fixes only
that resolve is one request from the console's perspective regardless of how
many internal steps implement it, and what each outcome may and may not
disclose.

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
Ones already `delivered` require the decision-6 operator repair, because the
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
   disclosed only to the consumer whose claim produced that state; this is a
   non-disclosure guarantee, not an access check, since `act()` (invariant
   13) remains consumer-agnostic once a card's id and version are known.
6. A resolved or re-pooled card carries no consumer affinity into its next
   claim.
7. `stats()` reports `pending` and `snoozed` identically to every caller,
   `delivering`/`delivered` scoped to the caller's own held cards, a
   content-free `elsewhere` count of cards held by other consumers, and an
   unchanged system-wide `active` total.
8. The `queue_view` role reads pending and snoozed cards without claiming or
   mutating anything, and never claims in due order to pre-populate a
   display. It claims a specific card by identifier only as the first
   internal step of a single resolve sequence — claim, deliver, act — that
   runs only after a reader has already chosen an action, never while they
   are still deciding.
9. `claim_next` (due-order, or by-identifier inside resolve) never lets a
   consumer exceed its role's fixed ceiling (`queue_view`: two; `drip`:
   twenty, matching the existing chat gateway's own configurable maximum)
   of held cards.
10. Ceilings are fixed per role by this decision, not accepted from a
    request and not runtime-configurable; lowering `drip`'s below the chat
    gateway's own configured range is not permitted without first checking
    that range.
11. A claim attempt refused because the caller already holds its role's
    ceiling is reported distinctly from one refused because no due card
    exists, using only the caller's own held count and ceiling; it never
    names a card, a task, or another consumer.
12. A `delivering` card recovers only through the existing lease expiry; a
    `delivered` card recovers only through an explicit, transport-absent
    local operator repair, never automatically, for either role. No
    consumer-specific recovery route exists beyond that repair: this record
    considered and rejected both a bounded action-completion window and a
    self-read route for `queue_view` (decision 6), because resolve at
    submit time leaves only a crash-class failure for the repair to cover,
    the same class `drip` already accepts without either mechanism.
13. `act()`'s existing authorization — card id, exact version, `delivered`
    status, nothing else — is unchanged by this decision, and is exactly
    the final step resolve performs internally for `queue_view`.

## Out of scope

This decision does not specify the exact schema migration or its version
number, the exact response-schema-version bump needed to carry the new
`elsewhere` field, the exact route, request/response schema, or internal
transaction boundaries for the new `queue_view` read and resolve operations,
the exact configuration file or flag syntax for pairing a token with a role,
or any console implementation or rendering. Those are left to a follow-up
implementation change to be reviewed against this record. In particular,
whether resolve's internal claim, delivery, and action steps share one
database transaction or run as a tightly-sequenced series of the existing
separate ones is an implementation choice this record does not make; it
only requires that the sequence be one request from the console's
perspective and that a failure partway through never apply the reader's
chosen action without also recording the delivery it depended on.

It also does not design more than two concurrently configured consumers or
any role beyond `drip` and `queue_view`; the two-role set is deliberately
closed, and extending it needs its own decision. It does not change `act()`'s
existing consumer-agnostic authorization. It does not design an automatic
liveness or heartbeat mechanism for a `delivered` card — including the
bounded action-completion window and the self-read route decision 6
considers and rejects for `queue_view` — and it does not design a way to
explicitly hand a specific card's content from one named consumer to
another — the only handoff this decision provides is the decision-6 repair,
which returns a card to the shared, unowned pool rather than moving it
directly to a chosen consumer. It does not decide anything about the exact
source or mechanism of the chat gateway's own on-screen pacing setting;
decision 5 treats that setting's current default and range as a given fact
to size against, not something this record controls.

## Closing note on confidence

This record's first draft set the `drip` ceiling to one on an unverified
assumption about how the existing chat gateway behaves, rather than checking
it against that gateway's own code. That was the real defect in this
decision, not the `queue_view` number originally flagged here as the weak
point — a reminder that an ADR's invariants are only as good as the facts
they were checked against, not how carefully the surrounding reasoning reads.

The residual uncertainty this draft is aware of now is the coupling itself:
decision 5 fixes the `drip` ceiling at twenty because that is the chat
gateway's own current maximum, but that maximum lives in the gateway's own
configuration, outside this record's control. If it changes upward in the
future, this ceiling becomes exactly the kind of silent throttle this
revision exists to remove, and nothing here re-checks that automatically —
only invariant 10's stated obligation to re-verify before lowering the
ceiling exists to catch it, and that obligation depends on whoever makes the
next change actually reading it. Resolve's internal claim-by-identifier step
is also new and has not been exercised: its behavior when a reader's click
races a card's own resolution elsewhere relies entirely on the same
stale-version refusal `act()` already uses, which is a reasonable bet but an
unverified one.

A second draft of this record compared the two roles' stranding risk in raw
card counts, corrected that to compare them as a share of each role's own
ceiling instead, and concluded from the proportional view that `queue_view`
was the structurally easier role to fully exhaust — because ordinary reading
and tab-switching could each leave a claim open indefinitely, at a ceiling
small enough for a handful of such lapses to close it entirely. This
revision retires that specific conclusion, not by refuting the proportional
argument but by removing what it was measuring: under resolve at submit
time, ordinary reading and tab-switching no longer claim anything at all, so
there is no longer an "ordinary lapse" that erodes `queue_view`'s capacity
the way there was through the previous two drafts. That is a real
improvement, not a relabeling, and it is the reason decisions 5 and 6 now
carry less machinery than before — the ceiling is a bug backstop rather than
a rationed resource, and the at-ceiling report is an anomaly signal rather
than routine guidance a reader is expected to see.

What this draft is least sure of is different from what the previous ones
were. First, how small the surviving crash-class window actually is depends
on an implementation choice this record explicitly declines to make —
whether resolve's claim, delivery, and action steps share one database
transaction or run as a tightly-sequenced series of the existing separate
ones. The record requires only that a partial failure never apply the
reader's action without also recording the delivery it depended on; it does
not require true atomicity, and the two are not the same guarantee. Second,
the `queue_view` ceiling of two is, like the earlier three, an unvalidated
judgment call, sized now for genuinely concurrent resolve requests rather
than for reader think-time — a narrower and less familiar situation to
reason about, and one this record has even less real-use evidence for than
it had for three. Third, and most simply: this record does not claim
resolve at submit time eliminates a `queue_view` card from ever being
stranded, only that it changes the cause from something an ordinary reader
does every day to something that requires a fault. If that turns out to be
wrong — if the eventual implementation's resolve sequence fails partway
through often enough to matter — the honest next step is to revisit this
record's claim, not to quietly reintroduce the machinery it just removed.
