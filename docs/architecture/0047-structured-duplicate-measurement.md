# ADR 0047: Measure structured duplicate retrieval by route

## Status

Accepted.

## Decision

Duplicate quality reporting includes a content-free line for every candidacy
route and a separate structure-only line. Every line reports proposed,
confirmed, rejected, awaiting, label count, and confirmation rate. The rate is
null until there is at least one reader answer.

The structure-only line includes a proposal only when an object or participant
route raised it and neither wording nor same-source re-read would have raised
it. The overlap is retained for audit but is not counted as the retrieval
benefit.

Capture the same report immediately before enabling a producer that emits
version-8 candidates, then compare later reports from the same queue after
reader answers settle. Do not tune retrieval while collecting that comparison.

## Consequences

The command contains no task text or proposal basis. A small label count remains
visibly small instead of being presented as a persuasive rate. If the
structure-only line does not show confirmed work that wording missed, this
feature has not justified its complexity.
