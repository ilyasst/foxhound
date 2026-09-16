# ADR 0042: Derived fused task titles

Status: accepted.

Reader-confirmed task consolidation preserves every original task text and
provenance record.  A confirmation only writes the relation and queues a
durable title job for the canonical task; it never calls a model.

The `foxhound-fused-task-titles` one-shot worker claims one pending job and
asks the configured OpenAI-compatible capability gateway for `thinking_no`.
It sends bounded source text as untrusted data and accepts only a bounded,
plain one-line response.  A valid response becomes a derived display title.
The original `tasks.text` field is never changed.

Transport errors and malformed model output return the job to `pending`, so a
later worker pass can retry without affecting the confirmed relation.  A
withdrawn duplicate relation clears the title when it was the final fused
source, or queues a replacement title when other fused sources remain.

The worker endpoint is private deployment state selected through the validated
deployment configuration; an environment default remains available for local
operation. The worker emits aggregate counts only and never logs task text or
gateway responses.
