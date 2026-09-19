# Repository Working Rules

These rules apply to every human and automated agent working in this
repository. If another instruction conflicts with this file, follow the more
privacy-protective rule and stop for clarification when the conflict cannot be
resolved safely.

## Treat the entire repository as public

This project may involve personal or operational data, but that data must never
enter the repository. The repository's private visibility is an access control,
not a privacy boundary. Every artifact that can reach Git or the hosting
service must be safe to publish without further editing.

This rule covers source and configuration files, documentation, tests and
fixtures, snapshots, generated artifacts, recorded logs, branch and tag names,
commit messages, issues, pull requests, reviews, comments, CI output, and
release notes.

- Never include real people, organizations, customers, internal projects,
  hosts, domains, usernames, email addresses, IP addresses, filesystem paths,
  credentials, tokens, identifiers, schedules, infrastructure details, raw
  records, or production inputs and outputs.
- Do not rely on redaction, truncation, hashing, pseudonyms, encryption, a
  private repository, or `.gitignore` to make sensitive material acceptable.
  If a value originated from real data, assume it may be identifying unless it
  has been explicitly established otherwise.
- Use invented examples such as `Person A`, `Example Org`, `Project Alpha`,
  `host-a`, `/srv/example/...`, `example.com`, RFC-reserved IP addresses, and
  clearly synthetic dates and records.
- Preserve only the technical shape needed to reproduce or explain behavior:
  generic control flow, sanitized counts, synthetic schemas, and the invariant
  that failed.
- Keep real data outside the repository in an approved private system. It is
  acceptable to state that behavior was verified against private evidence; do
  not quote, attach, summarize, or identify that evidence.
- When in doubt, do not commit, post, or push the material. Create a synthetic
  reproduction or ask for a privacy review.

Before every remote write, inspect the exact diff and all accompanying text for
personal data, secrets, operational details, and copied records. This includes
issue bodies, branch names, commit messages, pull-request metadata, review
comments, and command output. Automated scanning is helpful but does not
replace this review: see `CONFIDENTIALITY.md` for what `tools/check_public_diff.py`
actually catches (four narrow, mechanical classes) and, more importantly, the
much longer list of things — client, project, and person names among them —
that no script here can see.

If unsafe material reaches the remote, stop related work. Do not repeat the
material in a cleanup discussion. Remove or sanitize the affected artifact,
rotate any exposed secret, assess whether Git history must be rewritten, and
resume only with synthetic evidence.

## Issue-to-merge workflow

After the repository bootstrap, all changes follow this workflow:

1. Create a sanitized issue that defines the problem, expected outcome, and
   acceptance criteria. Never place private evidence in the issue.
2. Update local `main` from `origin/main` without adding local commits to it.
3. Create a dedicated worktree and a short-lived branch from `main`. Name the
   branch `issue-<number>-<generic-slug>`; the slug itself must reveal no
   personal or operational information.
4. Make and test the change only in that worktree. Keep commits focused and
   reference the issue using sanitized language. Every commit must use the
   contributor's GitHub-provided private `users.noreply.github.com` address for
   both author and committer email metadata; never use a personal or
   organizational email address in Git metadata. This no-reply identity is the
   only permitted exception to the rule against publishing real usernames.
5. Inspect the complete staged diff and commit metadata, then push the branch
   and open a sanitized pull request targeting `main`. Link the issue and state
   how the acceptance criteria were verified using synthetic evidence.
6. Merge only after a full local suite run passes and review passes. The
   automated checks on the pull request do not substitute for the local run
   while they cannot start; see "Running the tests" below. Prefer squash
   merging unless preserving separate commits has a concrete benefit.
7. Confirm the merge, delete the remote branch, remove the worktree, delete the
   local branch, and prune stale worktree references.

The normal repository state is exactly one branch, `main`. Do not maintain
long-lived development, release, environment, or personal branches. Do not
commit directly to `main`, force-push it, or bypass the issue and pull-request
workflow after bootstrap.

## Engineering safeguards

- Tests, demos, screenshots, and fixtures must be wholly synthetic and visibly
  fictional. Prefer the smallest dataset that exercises the behavior.
- Do not add raw-data directories, exports, database dumps, packet captures,
  production logs, or local debug traces, even temporarily or in ignored
  files. Store local sensitive inputs outside the repository checkout.
- Do not log personal data or secrets. Error messages and telemetry must use
  safe metadata and coarse, non-identifying summaries.
- Keep generated files and dependency caches out of Git. Review new file types
  and update ignore rules before tools generate local artifacts.
- Minimize access to private data. Code and tests should work with synthetic
  data by default; access real data only when the task explicitly requires it
  and an approved environment provides it.
- Work in a virtual environment, and never install this package onto the user
  site — not with `--user`, not editable, not with an override that defeats a
  packaging guard. The console scripts this project installs include the task
  worker, and shell profiles conventionally prepend the user script directory
  to `PATH`. A user-site install therefore makes *your working tree* the worker
  that every agent on that machine runs, whatever any unit configures, because
  the agent resolves the command in its own shell rather than the runner's.
  Since [ADR 0046](docs/architecture/0046-worker-resolved-from-the-running-release.md)
  the runner resolves its own worker and refuses to claim work when the two
  disagree, so this cannot corrupt runs silently; it will, however, stop a
  deployment. The install is one unremarkable line and the consequence shows up
  somewhere else, so it is worth stating: use a virtual environment.
- A change is not complete until tests pass, documentation is current, the
  publication-safety review is complete, and the issue/branch/worktree cleanup
  has been performed.

## Running the tests

The full suite is the merge gate. It takes about two minutes, so there is no
reason to run a subset and guess.

```sh
python3 -m venv .venv
.venv/bin/pip install -e .          # not optional -- see below
.venv/bin/pip install pytest
.venv/bin/python -m pytest tests/ -q
```

**Install the package first.** `resolve_worker_command` looks for a console
script beside `sys.executable` and falls back to a bare name when none is
there, so a suite run against an uninstalled tree takes the fallback and
passes while every developer checkout and every deployed release fails. A run
that skipped the install would have reported green on the commit that broke
production. Confirm with `command -v foxhound-task-worker` before trusting a
green result.

Run it in the worktree the change lives in, with its own virtual environment.
`git stash` is shared across every worktree of this repository, so never use
it to "temporarily undo" a change and compare — add a throwaway worktree
instead:

```sh
git worktree add /tmp/verify-main origin/main --detach
```

## Deploying

`main` moving does not move production.  Units import a pinned release, and
advancing one is a separate, named operation.

Before deploying anything, read
[ADR 0043](docs/architecture/0043-pinned-release-checkout.md) and
[ADR 0046](docs/architecture/0046-worker-resolved-from-the-running-release.md).
0043 covers what a release is and how one is advanced; 0046 covers the worker,
which the runner locates from the release rather than through `PATH`, and which
it checks before claiming work.  In particular,
a release that changes the database schema does **not** follow the ordinary
procedure: deployed code refuses a database newer than itself, so the units
are stopped across the migration.  Ask the candidate release whether this
applies before you start —
`<release>/venv/bin/foxhound-database inspect --database <db>` answers it, and
answering it afterwards means answering it during an outage.

## Publication-safety tooling

Install the pre-commit hook once per checkout (each worktree needs its own):

```sh
git config core.hooksPath tools/hooks
```

It runs `tools/check_public_diff.py` over the staged diff before every
commit, and separately refuses to commit on `main`. See `CONFIDENTIALITY.md`
for what the guard does, how to run it by hand, and — read this part even if
you skip the rest — an explicit list of what it cannot detect.

A GitHub Actions workflow runs the same guard and the test suite on every
pull request, but **it cannot start**: Actions billing is unavailable for
this account. A red check therefore means *not run*, not *failed*, and a
check that is red or absent is never evidence that a change is good. Run the
suite yourself; see the section below.
