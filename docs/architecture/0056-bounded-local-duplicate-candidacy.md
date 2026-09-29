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

Source re-read is supporting-only, like owner and working-group agreement.
Several independent actions routinely originate in one record, so a shared
record may strengthen a pair found by words, embeddings, or participants but
cannot create an eligible pair alone. Stage 2 also applies this rule while
claiming so candidates written by an older release cannot bypass it. A bounded,
dry-run-first reconciliation command recalculates stored scores and removes
unverified candidates that carry only supporting routes; verified evidence and
reader history are outside its write set.

The embedding route uses `intfloat/multilingual-e5-base`, served by the
fleet's loopback caproute gateway as the `embedding-multilingual` capability.
An earlier revision loaded the model in-process through `sentence-transformers`;
that shipped PyTorch in every release and competed for a GPU shared with speech
recognition and model serving, where it failed with CUDA out-of-memory. The
gateway's vectors match the reference model at cosine >= 0.999.

Vectors are fetched **before** the pass takes its write transaction. Intake
needs the same lock and waits only seconds for it, so a network call made while
holding it could fail intake. Work that arrives between the fetch and the
transaction is embedded on the next pass. Inputs use the model's `query: `
prefix and vectors are normalized before storage and comparison. A vector is
cached by task version, comparable text digest, and model identifier
(`caproute:<capability>`), so vectors produced another way are never mixed in.

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
