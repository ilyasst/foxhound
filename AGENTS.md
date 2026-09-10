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
replace this review.

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
6. Merge only after required checks and review pass. Prefer squash merging
   unless preserving separate commits has a concrete benefit.
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
- A change is not complete until tests pass, documentation is current, the
  publication-safety review is complete, and the issue/branch/worktree cleanup
  has been performed.
