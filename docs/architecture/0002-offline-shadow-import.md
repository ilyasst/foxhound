# ADR 0002: Offline candidate-feed shadow import

Status: accepted for the pilot.

## Context

GW can export verified task candidates as immutable ordered-feed pages, and
Foxhound can atomically apply an individual page. A pilot connection needs to
join those boundaries without giving Foxhound write access to producer state
or turning candidates into active tasks.

## Decision

Foxhound provides a manually invoked filesystem adapter. The caller supplies
an existing private GW outbox, an existing private Foxhound state directory,
a candidate-inbox database name, and the expected stream ID. Both locations
must be absolute, outside Git worktrees, and inaccessible to group and other
users.

The adapter opens GW's existing exporter lock read-only and acquires a shared,
non-blocking advisory lock. While holding it, Foxhound validates the entire
ledger before initializing or changing its database. It accepts only the
exporter's lock and canonical `page-<first>-<last>.json` files. Every page must
be a private regular file, satisfy the version 1 feed contract, match its
filename and requested stream, and extend one contiguous chain from cursor
zero. Repeated revisions and changed creation times are refused.

After validation, every page is passed in order to the existing transactional
candidate inbox. Prior pages are verified through their durable receipts and
replayed without writes. New pages atomically update candidates, their receipt,
and the stream cursor. A refusal stops the run immediately. Pages committed
before a database-level refusal remain a valid prefix and later pages remain
unapplied.

The adapter reports only aggregate counts and cursors. It never creates,
rewrites, acknowledges, deletes, or compacts producer files. Feed pages remain
GW-owned and must be retained until a separate acknowledgment and compaction
contract exists.

## Failure and rollback

Structural ledger failures occur before database mutation. Lock contention
also fails immediately. A stopped process can rerun the same command: page
receipts make the operation idempotent, and a previously committed prefix is
replayed before import continues.

The pilot database is a shadow inbox and has no task authority. Rollback is to
stop invoking the command and retain or discard that private shadow database
according to local operational policy. No producer rollback is required
because the producer outbox is never mutated.

## Out of scope

This decision adds no scheduler, watcher, network transport, background
service, acknowledgment, task acceptance, task card, agent execution,
knowledge-base access, environment access, or production activation. Bounded
knowledge context will require a separate contract.
