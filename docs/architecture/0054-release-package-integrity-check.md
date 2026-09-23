# ADR 0054: Release package integrity check

## Status

Accepted.

## Context

[ADR 0043](0043-pinned-release-checkout.md) says deployed units import a
release checkout and never a development tree. [ADR 0046](0046-worker-resolved-from-the-running-release.md)
pins the worker to the release by resolving it beside the running interpreter,
so `PATH` resolution and a mismatched worker revision cannot cause an outage.

Both checks assume the release's own environment still contains the release's
own package. Nothing enforced that.

A single ordinary command — an editable install issued with the release
environment's interpreter, pointed at a development checkout — replaces the
release's own package with whatever branch that checkout happens to sit on,
while every path, unit, and symlink still reads as correct:

```
<release>/venv/bin/pip install -e /path/to/dev-checkout
```

The worker check [ADR 0046](0046-worker-resolved-from-the-running-release.md)
cannot catch this class: it compares the agent's worker against the release
environment's answer, so when that environment is the thing that drifted, both
sides agree and the check passes. The worker resolves to the same release
directory, the schema versions match (both import the drifted code), and the
runner proceeds to claim work. The result can be catastrophic: if the
checkout's task-ledger schema is one version ahead of the deployed database,
every claimed run dies with `TaskLedgerError` at the agent's first tool call,
and the revision each component reports is still the release directory name
because that is derived from the directory, not from what is imported.

Observed on one host: an execution agent doing repository work ran an editable
install into the release environment. Recovery took three hours; nothing
reported the cause, and the fault was invisible in the release's own
diagnostics.

## Decision

**Resolve the running package's location and compare it against the release
root.** Before claiming, the runner resolves where the `foxhound` package
actually lives on disk (via `importlib.util.find_spec`) and checks whether
that path is inside the selected release directory. When it falls outside,
the runner refuses to claim with a content-free message and the existing
configuration-unavailable exit code (78).

**The check applies only to release deployments.** A development checkout run
directly, and a test suite run from a working tree, must not be refused. The
check detects a release deployment by examining `sys.executable` for the
standard `releases/<hex-revision>/venv/bin/python3` layout. When the
interpreter does not sit inside such a directory, the check is skipped entirely.

**The refusal is content-free.** No checkout path, branch name, or host detail
appears in the error message. The message reads:
`running package is not inside the selected release`.

**Why a worker-side check cannot see this.** The worker and the runner import
from the same environment. When an editable install has replaced the release's
package, both sides import the drifted code and agree on every metric they can
compare. The mismatch is between what the release directory *contains* and what
the release directory *should contain* — a structural property that only the
runner, which knows the release root from its own interpreter path, can verify
against the package's actual filesystem location.

## Implementation

`worker_resolution.verify_release_integrity()` — resolves the `foxhound`
package's location on disk and checks it against the release root. Called from
`execution_runner.main()` immediately after `verify_worker`, before any claim
is made.

`worker_resolution.PackageOutsideRelease` — new exception class for the
refusal. Caught in `main()` alongside `WorkerMismatch`, producing exit code
78 with the "refusing to claim" prefix.

The check is additive: a release running its own package is unaffected, and a
development checkout is never refused.

## Consequences

A release that has been compromised by an editable install now refuses to
claim before any database access, costing one poll rather than a claimed
workflow. The refusal is visible on stderr with a content-free diagnosis.

A release whose environment cannot be introspected (e.g. a non-standard layout
where the interpreter path does not match the expected pattern) is not
refused: the check returns to the existing behavior rather than adding a new
failure mode.

The check does not protect against a release that was built from the wrong
source (e.g. installing a different branch into the release environment during
the build). That is a build-time failure that belongs in the build procedure,
not in the runner.

## Testing

Synthetic tests in `tests/test_worker_resolution.py::ReleaseIntegrityTests`:

- A release layout that imports its own package claims normally.
- A release whose package resolves outside the release refuses with
  `PackageOutsideRelease`.
- A non-release invocation is unaffected.
- Missing package location or release root are handled gracefully (skipped).
- The error message is content-free (no paths, no branch names).

Integration test in `tests/test_execution_runner.py`:

- The runner catches `PackageOutsideRelease` and returns exit code 78
  without calling `run_once`.
