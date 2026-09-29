# ADR 0061: Manual task-research foundation

## Status

Accepted for the foundation; automatic triggers, model execution, source
brokering, and scheduling-condition application are separate changes.

## Decision

Task research is a dedicated durable pre-workflow. It is requested explicitly,
not on task intake. A request is keyed by task identity, task version, and a
canonical digest of every research-relevant structured field. One active job
exists per task; repeated requests for the same current input converge.

The lifecycle is `queued`, `running`, `publishing`, then `completed`. Exhausted
work may be `parked`; stale work may be `canceled`. Claims contain a random
capability, while SQLite stores only its SHA-256 digest. Events are append-only
and receipts are immutable.

An agent writes `foxhound.task-research-draft.v1`. It may synthesize grounded
claims and evidence-only scheduling recommendations, but it cannot author task
identity, provenance, coverage, source locators, source digests, timestamps, or
publication state. Foxhound combines a valid draft with broker-issued source
receipts to publish `foxhound.task-research.v1`.

V1 logical source namespaces are `kb`, `meeting`, `email`, `attachment`, and
`repo`. There is no web namespace or enable-web flag. Source IDs are sequential
and broker-owned.

The current machine record is `.task-research.json`; `Research.md` is its sole
deterministic rendering. Both names are reserved from ordinary task artifacts.
The publisher first records the generation in a private content-addressed store,
installs both files with mode 0600, verifies their digests, and only then commits
the receipt and `completed` state. Consumers accept only a matching completed
receipt and receive an evidence-only projection no larger than 16 KiB. Repair
restores receipt-authorized files from the content-addressed copy without model
work.

The accepted recommendation vocabulary is deliberately narrow:

- `after_task_completed`
- `not_before`
- `raise_priority`
- `create_prerequisite`

This foundation records those recommendations but never applies them. A later
policy service owns condition validation, queue mutation, notification, and
undo.

## Manual runtime interface

`foxhound-task-research` is the stable integration surface for an isolated
Researcher runtime:

1. `request --snapshot <input.json>` queues normalized input.
2. `claim --worker-id <id>` returns a private claim token.
3. `context --job-id <id> --token <token>` returns
   `foxhound.task-research-context.v1` and names `draft-research.json` as the
   required scratch output.
4. A source broker prepares sequential source receipts in a separate JSON file.
5. `publish --job-id <id> --token <token> --draft draft-research.json
   --sources <receipts.json> --task-folder <folder> --provenance <run.json>
   --coverage <coverage.json>` validates and publishes.

All commands also require explicit `--database` and `--cas-root` arguments.
Tokens and private task content must not be logged or committed.
