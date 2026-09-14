# ADR 0031: Keep durable task review files in explicit machine locations

## Status

Accepted.

## Context

Execution cards are intentionally concise. They cannot replace the working
record needed to verify a result, and a run that ends before recording still
needs to leave useful evidence for the next pass. The private run directory
already captures the agent transcript, but it also contains a claim capability
and compiled private instructions. Persisting that directory wholesale would
copy material that must not survive or synchronize.

Machines do not share one filesystem layout. The execution owner on each
machine therefore needs to name both the non-KB working root and the KB Tasks
root; neither can be inferred from a repository locator.

## Decision

The execution runner accepts two optional, paired paths:
`--task-work-root` and `--task-kb-root`. When configured, every claimed run
creates these stable task locations:

- `<task-work-root>/T<id>-<slug>/`, containing a human README and per-run
  evidence directories; and
- `<task-kb-root>/T<id>-<slug>.md`, containing the searchable task and result
  record.

The unit of configuration is the machine running the execution owner. Paths
are absolute and explicit. Supplying only one root is invalid.

Evidence preservation uses an allowlist. Foxhound copies the transcript,
standard result inputs, generated result draft, and `result-artifacts.json`.
The manifest may name a bounded set of relative regular files produced during
the run. It cannot traverse out of the run, name run state or instructions,
follow symlinks, or exceed bounded item and byte limits. Unlisted files are not
copied.

The worker adds the working-folder and KB-file paths to the append-only result
record. Review cards show those locations before the detailed work. They also
show the source issue and verified Markdown, pull-request, issue, and commit
references derivable from the result and its GitHub origin. Even a completed
result that required no new implementation receives the same archive.

Foxhound owns archive creation and updates; it does not expose the destination
paths to the agent. Old or specialist profiles therefore cannot bypass the
allowlist merely by omitting new guidance. Profiles may provide structured
external-action and deliverable records, and may list additional evidence in
the explicit artifact manifest. Current profile guidance also requires
descriptive Markdown links for each verified issue, pull request, commit, and
check needed to review the result.

## Consequences

- Review cards remain summaries while pointing to durable evidence.
- A failed run retains its transcript and any explicitly declared working
  files, allowing a later pass to continue from what was learned.
- KB search can find the task description, result, questions, deliverables,
  and work record without indexing raw run state.
- The ledger schema advances to version 17 and run-state schema to version 4.
- Existing deployments remain compatible when both archive options are
  omitted; schema-3 run state remains readable during a rolling upgrade.

## Failure and rollback

Archive roots are prepared before the agent starts. An unavailable or unsafe
configured root fails the claim without launching an agent. A failure while
preserving review evidence prevents the worker from recording the result.
Rollback removes both runner options; existing task folders and KB files remain
ordinary private synchronized files and can be retained or archived manually.
