# ADR 0059: Bounded Researcher synthesis adapter

## Status

Proposed for a manual pilot. Automatic triggering, durable job lifecycle,
publication, scheduling-condition application, and queue mutation are outside
this adapter.

## Decision

One invocation consumes a claimed `foxhound.task-research-context.v1`, performs
deterministic read-only searches through the existing GW knowledge client, and
asks a configured loopback model gateway for one
`foxhound.task-research-draft.v1` JSON object. The model name, endpoint,
dialect, reasoning effort, timeout, and profile revision are deployment input;
no model is selected in source code.

All searches request the `kb`, `secondary`, and `emails` layers together. Query
generation is deterministic from the structured action, object, task text,
external identifiers, and working-group identifier. Defaults are capped at 20
searches, 50 unique documents, 256 KiB of evidence, and 15 minutes for the
whole invocation. A single invocation handles one claimed job and has no
database handle.

Retrieved task and evidence text are marked as untrusted data in the prompt.
The model receives broker-assigned sequential source IDs and may cite only
those IDs. GW `kb` documents become `kb` receipts, `emails` documents become
`email` receipts, and the generic raw `secondary` layer becomes `attachment`
receipts. Receipt digests bind the exact evidence excerpt and its GW identity;
the durable publisher remains responsible for authoritative publication.

The model may return evidence-only scheduling recommendations with the bounded
vocabulary `after_task_completed`, `not_before`, `raise_priority`, and
`create_prerequisite`. Maximum three recommendations may be proposed.
`after_task_completed` requires integer `related_task_id` (the predecessor);
`not_before` requires an ISO timestamp in UTC ending in `Z`;
`create_prerequisite` requires `prerequisite_text`; and `raise_priority` takes no
extra target fields. The prompt explicitly says that the model cannot apply
them. This adapter contains no queue, condition, task-creation, or notification
tool.

The prompt also states the validated draft type map explicitly:
`research_status` uses the three contract values; `objective` and
`requested_action` are single claim objects; every report section is an array
of claim objects, including singleton sections; and
`scheduling_recommendations` is an array. The validator remains authoritative
and does not coerce strings or keyed objects into those shapes.

For the OpenAI-compatible dialect, the request also carries that contract as a
strict named JSON schema. The schema requires every top-level field, claim
shape, report-section array, status enum, and scheduling-recommendation
variant, with additional properties disabled throughout. Runner-dialect calls
retain their native JSON mode. Model-side schema enforcement is an early
quality gate only: strict parsing, receipt grounding, citation checks, and the
draft validator remain authoritative after generation.

The adapter scope is bounded host retrieval plus one-shot local model synthesis
for tonight, not a completed multi-turn broker or OS sandbox. It has no
database or queue mutation authority and no internet retrieval in v1.

The adapter writes five owner-only scratch files into an existing empty private
directory:

- `draft-research.json`
- `source-receipts.json`
- `research-coverage.json`
- `research-provenance.json`
- `research-metrics.json`

### Trusted scratch containment and private context invariants

To prevent symlink attacks, directory traversal, and unauthorized filesystem access,
the adapter enforces strict filesystem containment and permission boundaries:

1. **Context file validation**:
   The input context file passed to `--context` must be an absolute path pointing to a
   regular file owned by the current process user (`os.geteuid()`) with strict owner-private
   permissions (`0o600` / `stat.st_mode & 0o077 == 0`). Every path component from the root to
   the file must be a non-symlink, owner-private directory. File descriptors are opened with
   `O_NOFOLLOW` and inspected via `fstat` before reading.
2. **Output scratch directory containment**:
   The output scratch directory passed to `--output-directory` must be an absolute path
   pointing to an empty directory owned by the current process user with owner-private
   permissions (`0o700` / `stat.st_mode & 0o077 == 0`). Every component of its path must be
   a non-symlink, owner-private directory.
3. **Configured or explicit trusted scratch root**:
   When `--scratch-root <path>` or the `FOXHOUND_RESEARCH_SCRATCH_ROOT` environment variable
   is specified, the output scratch directory must resolve strictly inside that trusted root.
   Containment is verified component by component, verifying that no intermediate path segment
   is a symlink, contains `..` components, or has unsafe group/world permissions.

### Reasoning provenance invariants

The adapter distinguishes requested reasoning effort from effective reasoning effort in
`research-provenance.json`:
- `reasoning_requested`: Reflects the configured effort level (`low`, `medium`, `high`).
- `reasoning_effective`: Defaults to `"unknown"`. The mere presence of reasoning tokens or
  reasoning content in the completion demonstrates that reasoning occurred, but does not prove
  that the backend honored the requested effort level. `reasoning_effective` is only set to a
  known effort level when the backend explicitly and unambiguously returns a trustworthy
  `reasoning_effective` field in the response envelope or usage details.

Errors exposed to the caller are fixed codes and never include a query,
evidence excerpt, task field, model response, endpoint detail, or token.
Supported codes distinguish invalid configuration/context/output, retrieval
failure or absence, elapsed budget, model timeout/failure, oversized response,
malformed JSON, invented citations, and invalid draft shape.

Before strict JSON parsing, the adapter may remove at most one complete leading
`<think>...</think>` protocol block and at most one whole-response Markdown
fence labelled `json` or left unlabelled. This accommodates bounded wrappers
emitted by reasoning backends without extracting JSON from arbitrary prose.
Unclosed, repeated, nested, mismatched, or prose-surrounded wrappers remain
malformed. Duplicate-key, exact-schema, source-reference, and citation checks
apply unchanged after unwrapping.

There is no web search, public-network model endpoint, durable write, queue
write, or task-folder write in this component.

## Manual integration with the task-research foundation

The durable foundation owns request, claim, context, failure, and publication.
Its `context` command returns an envelope accepted directly by the adapter:

```sh
foxhound-task-research --database /srv/example/state.sqlite3 \
  --cas-root /srv/example/research-cas context \
  --job-id research-example --token "$CLAIM_TOKEN" > /srv/example/scratch/context.json

python -m foxhound.task_research_synthesis \
  --context /srv/example/scratch/context.json \
  --output-directory /srv/example/scratch/output \
  --model "$RESEARCH_MODEL" \
  --endpoint http://127.0.0.1:8800 \
  --dialect openai --reasoning high --timeout 900 \
  --profile-id researcher --profile-revision "$PROFILE_REVISION" \
  --provider local \
  --gw-endpoint "$GW_ENDPOINT" --gw-alias example \
  --gw-token-file /srv/example/secrets/gw-token

foxhound-task-research --database /srv/example/state.sqlite3 \
  --cas-root /srv/example/research-cas publish \
  --job-id research-example --token "$CLAIM_TOKEN" \
  --draft /srv/example/scratch/output/draft-research.json \
  --sources /srv/example/scratch/output/source-receipts.json \
  --coverage /srv/example/scratch/output/research-coverage.json \
  --provenance /srv/example/scratch/output/research-provenance.json \
  --task-folder /srv/example/tasks/T101-example
```

### Agent researcher

The research runner (`foxhound-task-research-runner`) supports an alternative agent-based synthesis mode via Hermes (`--synthesizer agent`). In agent mode, the runner invokes an interactive Hermes agent within a private run directory instead of making a single model call over keyword snippets.

New CLI flags:
- `--synthesizer {single,agent}`: synthesis implementation (`single` model call or Hermes `agent`, default: `single`).
- `--hermes-command PATH`: path to the Hermes CLI or executable (required when `--synthesizer agent`).
- `--agent-toolsets TOOLSETS`: comma-separated toolsets permitted to the agent researcher (default: `terminal,file,web,browser`).
- `--agent-max-turns TURNS`: maximum agent turns before giving up (default: 120).
- `--agent-timeout SECONDS`: total timeout in seconds for agent research execution (default: 3600).
- `--knowledge-root NAME=PATH`: named knowledge root directory exposed to the agent researcher (repeatable, must be absolute paths).

Production orchestration must pass a failure code to the foundation's `fail`
command when synthesis exits nonzero. It must not log the context, claim token,
scratch artifacts, or model response. The metrics file contains only counts and
latency and is not an input to publication.

Until the foundation change is present on the same branch, this module uses an
isolated validator matching its proposed draft contract. The Python API accepts
the foundation's authoritative validator explicitly:

```python
synthesize(
    context,
    knowledge=knowledge,
    config=config,
    validator=foxhound.task_research.validate_draft,
)
```

This is the only temporary seam. The adapter does not duplicate the
foundation's job, claim, receipt, CAS, rendering, or database code.
