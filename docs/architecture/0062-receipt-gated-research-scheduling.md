# ADR 0062: Receipt-gated research scheduling

## Status

Accepted for the bounded Researcher v1 pipeline.

## Decision

A published Researcher report remains evidence-only. A separate deterministic
boundary may project its scheduling recommendations into the bounded scheduling
ledger only when all of these checks hold:

- the exact canonical report digest has an immutable completed research receipt;
- task identity, task version, research generation, and source references match
  that receipt and report;
- the overall research status is `sufficient`;
- recommendation confidence is at least `0.8`; and
- the rationale is `supported` and cites at least one broker-issued source.

Recommendations that do not meet the automatic threshold stay visible in the
report but do not mutate the queue. The scheduling service still enforces its
condition-count, cycle, depth, date, priority, and workflow-version limits.

Each accepted change is active immediately and creates its own scheduling
review card. Delivery must be acknowledged before Keep or Undo is accepted.
Undo uses the scheduling ledger's fenced inverse operation. Replaying the same
published recommendation returns the original change and card rather than
applying it again, including when the first application changed the workflow
version.

## Consequences

Model output cannot directly write queue state. Publication alone also cannot
write queue state: the immutable receipt and deterministic policy boundary are
both required. Up to three recommendations may be present in a report, while
the scheduling ledger independently caps active conditions and refuses unsafe
or stale changes without weakening the report.
