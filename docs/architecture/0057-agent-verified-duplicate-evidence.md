# ADR 0057: Verify duplicate candidates against bounded knowledge

## Status

Accepted.

## Decision

Stage 2 consumes versioned candidate pairs from Stage 1. A short SQLite
transaction claims one current pair and charges one run to a durable UTC-day
budget. GW search and local inference then happen without a database write
transaction. Each call has an explicit timeout. A failed, timed-out, or
unusable answer releases the claim for a later pass and cannot block another
pair in the current pass.

The verifier receives task text, owner, source kind, dates, and structured
fields. It searches the bounded GW knowledge layers for meetings, Teams
material, and email, then asks a model at a loopback-only endpoint for exactly
one verdict: `same`, `related`, or `different`. The reply also carries a
confidence and one to five citations. Every citation's document identifier and
locator must name an exact returned GW document, and its excerpt must be an
exact bounded substring of that document. Additional fields, invented sources,
invalid confidence, and incomplete replies fail closed.

Every valid verdict and its private citations are stored against the Stage 1
candidate identifier, which already identifies both task versions. `related`
and `different` stop there. Only `same` invokes the existing duplicate proposal
ledger under detector `agent-verified-v1`; it never merges tasks. The
verification links to the resulting proposal so #718 can present citations
without putting private text in the proposal basis or operational output.

The claim, day counter, verdict, and proposal writes contain no model or GW
calls. A second pass skips an already verified candidate. When either task is
revised, Stage 1 emits a candidate with new versions and Stage 2 verifies that
new identity independently. Stale-version candidates are never sent to the
agent.

## Consequences

Agent failures consume daily budget because they consumed the scarce run. A
bad model cannot smuggle an invented citation into storage, logs expose only
aggregate counts and total latency, and concurrency cannot exceed the same
durable budget through separate processes.

Deployments opt in with explicit per-pass, per-day, and per-pair bounds. They
reuse the card service's validated GW endpoint, alias, and owner-only token
file. The model endpoint retains the local semantic evaluator's loopback and
no-redirect rule; loopback alone does not prove where an operator's gateway
ultimately performs inference.
