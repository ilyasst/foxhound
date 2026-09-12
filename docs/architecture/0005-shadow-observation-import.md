# ADR 0005: Read-only shadow observation import

Status: accepted for the passive comparison phase.

## Context

Foxhound can persist observation pages but must not share writable state with
GW. A manual transport is needed before either side gains a network service or
automatic scheduling.

## Decision

Foxhound reads a GW-owned immutable observation ledger from a private absolute
directory outside every Git worktree. The directory, lock, and page files must
exclude group and other access and must not traverse symbolic links. A shared,
non-blocking advisory lock on `.task-shadow-feed.lock` provides a stable
snapshot while the producer uses the exclusive side of the same lock.

Pages use canonical JSON and filenames that encode their bounded, contiguous
cursor range. Before touching the Foxhound database, the importer validates the
entire visible chain, filenames, stream identity, file safety, and uniqueness
of candidate revisions. Temporary or unknown entries fail closed.

After snapshot validation, pages are committed individually in order. This
preserves a valid prefix if a later page refers to candidate state that has not
arrived yet. Retrying the unchanged ledger replays receipts and resumes at the
first unapplied page. The importer never changes producer files.

Successful command output contains cursors, counts, and aggregate comparison
classes only. Failure output is generic and does not expose candidate content,
paths, identifiers, or legacy task references.

## Out of scope

This adapter does not publish observations from GW, acknowledge producer
state, poll automatically, create or change tasks, render cards, schedule work,
or execute agents.
