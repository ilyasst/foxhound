# ADR 0018: One-shot verified shadow bootstrap

Status: accepted for the task-authority cutover.

## Context

GW can export verified candidate and legacy decision feeds, and Foxhound can
import them passively. The durable task ledger already has a transactional
shadow bootstrap, including a bounded GW owner-equivalence check, but a host
could invoke it only through ad hoc application code.

## Decision

Foxhound exposes `foxhound-task-bootstrap` as an explicit one-shot command.
The caller supplies an existing private Foxhound database and the configured
loopback or validated HTTPS GW knowledge endpoint, alias, and private token
file. The command constructs the existing strict read-only client and passes
only its identity-bound owner-equivalence operation to the existing
transactional bootstrap.

Routine output contains only the disposition and aggregate counts. Invalid
paths and configuration fail before the ledger is opened. The database and
token files and their immediate parent directories must exclude group and
other access. Resolver transport or response failures leave affected
divergent candidates inactive; they do not prevent already-agreed groups from
being materialized. Exact retries remain idempotent.

The command does not export or import feeds, discover candidates, schedule
itself, render or deliver cards, advance task lifecycle, schedule execution,
or launch an agent. A deployment composes those independently fenced
operations in its own one-shot service.

## Rollback

Stop invoking the command. Passive imports remain usable, existing Foxhound
tasks remain authoritative, and no producer state is changed.
