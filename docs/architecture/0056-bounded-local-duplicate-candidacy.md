# ADR 0056: Bound local duplicate candidacy outside intake

## Status

Accepted.

## Decision

Native intake records a durable duplicate-check obligation in the same SQLite
transaction that creates, reactivates, or materially revises a task. A
revision is material here only when text, owner identity, object, action, or
participants change. Intake performs no inference and creates no duplicate
proposal or card.

A timer-driven consumer processes a bounded number of obligations. It retains
the existing eligibility, identifier-conflict, and same-reading gates, then
offers independent wording, source re-read, participant, and embedding
signals. It writes versioned candidate pairs for Stage 2, with both a per-task
top-K bound and a per-run pair bound. Only Stage 2 may turn a verified same-task
verdict into a reader proposal.

The embedding route uses `intfloat/multilingual-e5-base` through the local
`sentence-transformers` runtime. Model files must already be installed on the
host: the consumer requests local files only and never downloads during a
pass. Inputs use the model's `query: ` prefix and vectors are normalized before
storage and comparison. A vector is cached by task version, comparable text
digest, and model identifier.

The similarity cutoff is measured from settled reader proposals. Every pass
requires both confirmed and rejected labels represented in the current vector
set. It selects the threshold with greatest recall at a precision of at least
0.80; if the labels admit no such threshold, it selects the best F1 threshold.
The command reports only aggregate label count, threshold, precision, and
recall. There is no sample-derived fallback. Without labels, model files, or a
working runtime, lexical signals still commit and the embedding obligation is
left queued for retry.

GW candidate v11's nullable opaque `person_id` is accepted for owners and
participants. When present for a person, it joins participant identity across
meeting and email speaker registries. The `working_group` route is reserved in
storage but emits no signal until gw#1201 and gw#1104 define a stable meaning
across rebuilds.

## Consequences

An intake failure cannot advance past a missing queue write, while local model
latency and failure cannot hold the intake transaction open. Deploy the release
and local model to every consumer host before enabling the optional
`duplicate_stage1` database consumer. Its task, top-K, and global pair limits
are explicit deployment settings rather than hidden operational constants.

Calibration is intentionally conservative about evidence quality: no reader
labels means no embedding decisions. The Stage 1 table is not reader-facing
and cannot create cards, preserving the separation between cheap recall and
cited verification.
