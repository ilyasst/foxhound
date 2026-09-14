# ADR 0036: Consumer-scoped task-card claims for a second gateway

Status: proposed.

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

When the reader acts on a specific card shown in the console, the console
must first claim that exact card — not merely "the next due card," which is
all `claim_next()` can do. This decision therefore also authorizes a second
claim path, claim-by-identifier: given a card id and the exact version the
console just read, it claims that specific row under the `queue_view`
identity if it is still `pending` or `snoozed` at that version, and refuses
if it has moved (the same stale-version refusal `act()` already gives for
the same reason). A successful claim proceeds through the existing
delivering → delivered → act lifecycle exactly as the drip gateway's claims
do. The console is expected to carry each claim straight through to delivery
and action rather than holding it open. Ordinarily this means a `queue_view`
consumer holds very few claims at once — one for a single reader working one
card at a time, or a small handful if the reader has more than one page open,
which is an entirely normal way to use a browser and not a misuse this record
should design against. Decision 5 gives that headroom a fixed size rather
than assuming a single claim is always enough.

**Rejected alternative: claim-to-display, where `queue_view` claims up to a
fixed ceiling of cards purely to have their content ready to render.** This
was this record's original decision, and it is rejected here in favor of
claim-on-demand for three reasons. First, it manufactures a much larger
stranding surface in proportion to its own ceiling: claim-to-display's
ceiling had to be sized for "one screenful," so an abandoned session strands
a large fraction of a large number, where claim-on-demand's ceiling (decision
5) is sized for how many cards a reader plausibly has open at once, so an
abandoned session strands at most that same small headroom — a proportional
cost closer to the `drip` role's, not the outsized one this record's earlier
draft accepted. Second, its ceiling had to
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

### 5. A per-role concurrency ceiling, sized for real use and self-explaining when reached

`claim_next()` (including the claim-by-identifier path) refuses to hand out
a card to a consumer already holding its role's ceiling of cards
(`delivering` + `delivered` — the same two terms `stats()` reports as
"mine"). Reaching that ceiling and finding no due card are different
conditions, and this decision requires them to be reported differently: the
response distinguishes "no due card exists for anyone" from "you already
hold your role's ceiling." The at-ceiling report contains nothing about any
other consumer or any card — only the caller's own held count and its own
role's ceiling, both of which the caller already implicitly knows from its
own request history. Reporting them back is self-disclosure, not a breach of
decision 2's guarantee, and it is what lets a console tell its reader "you
already have one open, finish it first" instead of showing a queue that
looks empty or stalled for no stated reason.

Sizing the ceiling matters independently of that fix. This record's earlier
draft set `queue_view` to one on the assumption that claim-on-demand gives a
console no reason to hold more than one claim. That assumption does not
survive contact with an ordinary reader: acting on a second card before
resolving the first, or simply having more than one browser tab open, are
both unremarkable ways to use a page, and a ceiling of one refuses both
silently, before the reporting fix above even has a chance to explain why.
The ceiling is sized instead for a small amount of ordinary concurrent
reader activity, not for the smallest number that would technically work:

| Role | Ceiling (delivering + delivered) | Basis |
|---|---|---|
| `queue_view` | three | Sized for ordinary reader concurrency — one card actually in front of the reader, one more they switched to before finishing the first, and one tab of headroom — not for a vanished-consumer worst case. This is a judgment call about how a person uses a browser, not a value read off another system's configuration; unlike the `drip` figure below, there is no external source of truth to cite, and it should be revisited if real use shows it too small or unnecessarily large. |
| `drip` | twenty | The existing chat gateway's own on-screen pacing setting — a value the operator configures on the gateway side — defaults to five and is permitted up to twenty; the gateway sizes each pacing pass as that configured cap minus its own current on-screen count. |

The `drip` figure corrects this record's earlier draft, which set the
ceiling to one on the mistaken premise that the chat gateway only ever holds
a single card. It does not: it already holds up to its own configured cap,
by default five and as high as twenty, and each of its pacing passes claims
however many cards close that gap. A server-side ceiling of one would have
throttled that gateway to a fifth of its already-configured rate, and done
so silently — `claim_next` returning nothing looks identical to an empty
queue, which is precisely the quiet-failure class this record exists to
close. Twenty is the gateway's own documented maximum, not a number invented
in this record, and this decision commits to keeping the `drip` ceiling at
least that high for as long as that maximum stays where it is; lowering it
below the gateway's own configured range would reintroduce the same silent
throttle by a different route.

**Invariants:**

4. `claim_next` (both the due-order and claim-by-identifier paths) never
   lets a consumer's held count exceed its role's fixed ceiling: three for
   `queue_view`, twenty for `drip`.
5. Ceilings are fixed per role, not accepted from a request and not
   runtime-configurable. Changing either number is a decision for a future
   ADR, and any future reduction of the `drip` ceiling must first confirm
   what the chat gateway's own pacing setting actually permits at that time,
   exactly the check this draft skipped the first time.
6. A claim attempt that fails because the caller already holds its role's
   ceiling reports that fact — its own held count and its own ceiling, and
   nothing else — distinctly from a claim attempt that fails because no due
   card exists. Neither report ever names a card, a task, or another
   consumer.

**Rejected alternative: no ceiling at all, relying entirely on decision 4's
claim-on-demand model and the drip gateway's client-side pacing.** Rejected
because neither is a server-side guarantee: a bug in either consumer (a
retry loop, a mis-sized pacing pass) would otherwise have nothing stopping
it from claiming without bound. A small fixed ceiling costs nothing for a
correctly behaved caller of either role and catches exactly that failure
mode.

**Rejected alternative: a caller-supplied `max_claims` per request.**
Rejected for the same reason request-supplied consumer identity was rejected
in decision 1 — it reopens a self-asserted value where a fixed, reasoned
server-side constant is safer, and it departs from this repository's existing
preference (ADR 0035) for small fixed capacities over runtime-configurable
ones.

**Rejected alternative: raise `queue_view`'s ceiling without also making the
at-ceiling refusal distinguishable from an empty queue (or the reverse).**
Rejected because the two only work as a pair. Headroom alone still fails
silently once it is exhausted — it only raises how much ordinary use it
takes to get there, which is exactly the mistake this record's earlier draft
made with the number one and would still be making with a bigger number.
Distinguishability alone, with the ceiling still at one, would let a console
explain a failure that a single ordinary reader action still causes
constantly. Neither is a fix by itself; this decision adopts both.

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
decision does not introduce that exposure; it inherits it, and closes it the
same way for both roles rather than leaving it unaddressed.

The remedy is the local-operator recovery already established for execution
cards in ADR 0016 — an explicit, transport-absent administrative requeue of
a still-current delivered card — extended here to task review cards. The
repair clears delivery metadata and increments the card version, exactly as
ADR 0016 describes, so the stale presentation becomes unusable without
rerunning any lifecycle transition. It remains absent from the remote,
authenticated API, available only to a trusted local operator tool.

**The abandoned reader action is the case that matters here, not only a
vanished process.** A console does not need to crash to strand a card: a
reader claims one by acting on it, the console carries it to `delivered`,
and the reader simply closes the page, switches away, or is interrupted
before resolving it — an entirely ordinary lapse, not a failure of any
component. That card now sits `delivered` forever, exactly as a `drip`
gateway's acknowledged-but-never-acted card would, with no lease to expire
it, per the earlier paragraphs of this decision.

Card counts alone understate what that costs a `queue_view` consumer,
because the two roles do not have the same ceiling. One abandoned card costs
`drip` one twentieth of its capacity to claim further work — noticeable, but
far from disabling. The same one abandoned card costs `queue_view`, at its
ceiling of three, a third of its capacity, and a second abandoned card
(equally ordinary — two forgotten tabs, not one) costs it two-thirds.
Measuring the earlier draft's "at most one card" against `drip`'s absolute
worst case of twenty made claim-on-demand look like a strict improvement in
every respect; measured as a share of each role's own ceiling, `queue_view`
is structurally the more exposed role, because its ceiling is deliberately
the smaller of the two.

This is exactly why decision 5 does not treat headroom as the whole answer.
Three invariants now work together on this specific case: the ceiling gives
a reader room for a small amount of ordinary concurrent activity before any
abandonment matters at all; the distinguishable at-ceiling report lets the
console tell the reader "you have unfinished cards claimed" as capacity
tightens, so an attentive reader can resolve or explicitly abandon one
before the ceiling is reached rather than discovering the problem only once
it is silent; and the operator repair below remains the backstop for
whatever this does not catch — a reader who never comes back at all. None of
the three removes the exposure by itself; together they make it visible
early, keep its ordinary cost low, and bound its worst case to a small,
named number instead of leaving it structurally hidden as an unowned
"'active' minus everything else."

The remedy for a card that does end up stuck despite all of that is the same
one described above: the local-operator recovery extended from ADR 0016.
Running it against a `queue_view`-held card is identical in mechanism to
running it against a `drip`-held one; nothing about this decision gives the
two roles different recovery paths, only different points at which an
operator is likely to need one.

**Rejected alternative: bound a `queue_view`-held `delivered` card with a
short action-completion window instead of, or in addition to, the headroom
and reporting fix above.** Considered, because a claim-on-demand console is
a genuinely different case from a chat card that may legitimately sit
unanswered for a day — the console commits to a claim only once a reader has
already clicked something, so in the overwhelmingly common case it would
resolve within seconds. Rejected as this decision's mechanism for three
reasons. First, "overwhelmingly common" is not "always": a slow connection,
a confirmation the reader pauses to read, or a backgrounded browser tab
throttling its own network activity can all legitimately take longer than a
short window without the reader having abandoned anything, and a window
short enough to bound stranding meaningfully would then convert a rare
silent stall into a routine, confusing action failure for entirely correct
use. Second, it only addresses the vanished-or-abandoned case; it does
nothing for the ordinary multiple-tabs case this section opened with, which
still needs headroom regardless of whether an expiry also exists. Third, it
would add a new kind of expiry — `delivered` is permanent by design today,
and giving it a role-specific timeout reopens a version of the exact
liveness-inference problem this decision already declines to solve for
`drip`, just at a shorter timescale, rather than reusing a mechanism (a
fixed per-role constant, a self-reported capacity signal) this record has
already justified elsewhere. If headroom and reporting later prove
insufficient in practice, a bounded window remains available as a targeted
follow-up specifically for `queue_view`, but it is not adopted here.

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
exist today: a `queue_view`-gated read equivalent to `due()`, and a
claim-by-identifier path alongside the existing due-order `claim`. Both are
additive to the route table; no existing route's request or response shape
changes because of them. The existing `claim` response's `status` field
gains a third value per decision 5 (claimed, empty, or at-ceiling), reported
identically by the claim-by-identifier path. Exact paths, request/response
schemas, and contract-version numbers are implementation details left to the
follow-up change; this record fixes only that the three outcomes exist and
what the at-ceiling one may and may not disclose.

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
   mutating anything, and claims a specific card by identifier and exact
   version only when a reader acts on it; it does not claim in due order to
   pre-populate a display.
9. `claim_next` (due-order or by-identifier) never lets a consumer exceed
   its role's fixed ceiling (`queue_view`: three; `drip`: twenty, matching
   the existing chat gateway's own configurable maximum) of held cards.
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
    local operator repair, never automatically, for either role — this
    record considered and rejected a bounded action-completion window for
    `queue_view` as a substitute (decision 6).
13. `act()`'s existing authorization — card id, exact version, `delivered`
    status, nothing else — is unchanged by this decision.

## Out of scope

This decision does not specify the exact schema migration or its version
number, the exact response-schema-version bump needed to carry the new
`elsewhere` field, the exact routes or request/response schemas for the new
`queue_view` read and claim-by-identifier operations, the exact
configuration file or flag syntax for pairing a token with a role, or any
console implementation or rendering. Those are left to a follow-up
implementation change to be reviewed against this record.

It also does not design more than two concurrently configured consumers or
any role beyond `drip` and `queue_view`; the two-role set is deliberately
closed, and extending it needs its own decision. It does not change `act()`'s
existing consumer-agnostic authorization. It does not design an automatic
liveness or heartbeat mechanism for a `delivered` card — including the
bounded action-completion window decision 6 considers and rejects for
`queue_view` — and it does not design a way to explicitly hand a specific
card's content from one named
consumer to another — the only handoff this decision provides is the
decision-6 repair, which returns a card to the shared, unowned pool rather
than moving it directly to a chosen consumer. It does not decide anything
about the exact source or mechanism of the chat gateway's own on-screen
pacing setting; decision 5 treats that setting's current default and range
as a given fact to size against, not something this record controls.

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
next change actually reading it. The claim-by-identifier path in decision 4
is also new and has not been exercised: its behavior when a reader's click
races a card's own resolution elsewhere relies entirely on the same
stale-version refusal `act()` already uses, which is a reasonable bet but an
unverified one.

A second draft of this record compared the two roles' stranding risk in raw
card counts and concluded `drip`'s exposure (up to twenty) was now the
larger of the two, since `queue_view`'s had shrunk from twenty to one. That
comparison was itself the wrong measure, corrected in this revision: what
matters is how much of a role's own capacity an ordinary lapse can remove.
`queue_view` remains the structurally easier role to fully exhaust — it
takes three ordinary abandoned reader actions to disable it completely,
against `drip` needing something close to a full, fully-abandoned on-screen
load (up to twenty independently un-acted cards, or a process vanishing
while carrying one) to reach the same state. Decision 5's headroom and
distinguishable refusal narrow that gap without closing it, and this record
does not claim they close it — a `queue_view` consumer can still reach
exhaustion from ordinary use alone, faster than `drip` realistically will,
and the honest position is that the repair tool in decision 6, not a design
guarantee, is what bounds the consequence when it does. The number three
itself is the same kind of judgment call the `drip` ceiling was mistakenly
treated as in the first draft, except this time it is labeled as one
plainly, in decision 5's own table, rather than asserted as settled fact.
