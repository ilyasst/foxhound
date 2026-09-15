# Confidentiality

`AGENTS.md` already states the rule this document supports: this repository's
private visibility is an access control, not a privacy boundary, and every
artifact that reaches Git or the hosting service — source, docs, tests,
fixtures, commit messages, issues, and pull requests — must be safe to
publish without further editing. `AGENTS.md` also says "automated scanning is
helpful but does not replace this review." This document describes the one
piece of automated scanning that exists, and is explicit about how little of
the review it actually replaces.

## What is enforced, and what is not

`tools/check_public_diff.py` scans added diff lines and rejects four
mechanical classes:

- non-example email addresses
- user home paths (`/home/…`, `/Users/…`, `C:\Users\…`)
- non-example IPv4 addresses
- remote host identifiers in `ssh` / `scp` / `rsync` commands

It also accepts an optional, locally-supplied list of exact private literals
via the `FOXHOUND_PUBLIC_DIFF_PRIVATE_TERMS` environment variable (one term
per line) — useful if you know of a specific name or string that must never
appear, but this is a manual opt-in, not something the guard discovers on its
own.

Run it by hand the same way the pre-commit hook does:

```sh
git diff --cached --unified=0 --no-color | python3 tools/check_public_diff.py
```

or against a branch instead of the staged diff:

```sh
git diff --unified=0 --no-color origin/main... | python3 tools/check_public_diff.py
```

### Install the hook

```sh
git config core.hooksPath tools/hooks
```

This points Git at `tools/hooks/pre-commit`, which runs the guard over the
staged diff on every commit and refuses the commit if it finds something. It
also refuses to commit on `main` — see the comment at the top of that file
for why. Both checks can be bypassed with `git commit --no-verify`; you
should not need to.

There is deliberately no CI workflow running this. GitHub Actions billing is
not available for this account, so a scheduled or triggered workflow would
never execute — it would sit as a permanently red, uninformative check rather
than a working gate. Do not add one expecting it to run; the pre-commit hook
above is the only place this check runs.

## What it cannot see — and this is the part that matters

**A clean result from this guard is not evidence that a diff is safe to
publish.** It is a mechanical pattern match over four narrow, syntactic
shapes. It has no idea what any of the following are, and will say nothing
about them even when they appear in plain text in an added line:

- a client name, a customer name, or an organization
- a project name or an internal codename
- a knowledge-base folder, a repository, or any other named internal asset
- a person — their real name, role, or anything that identifies them
- a schedule, an internal identifier, or an infrastructure detail that isn't
  shaped like an email, a home path, an IPv4 address, or a remote-command host
- a line of ordinary prose that happens to describe or quote private content
- anything inside an image, a screenshot, or a binary attachment

The four classes above were chosen because they are the ones a regular
expression can reliably tell apart from a synthetic example. Everything else
— which, per `AGENTS.md`, is most of what actually needs to stay out of this
repository — is still entirely the author's judgment call. Treat the guard as
a seatbelt for typos and copy-paste accidents, not as a reviewer.

## Reporting

If a real identifier reaches a commit anyway, treat it as disclosed.
Rewriting the message does not unpublish it. Follow the recovery steps in
`AGENTS.md`: stop related work, do not repeat the material while cleaning up,
sanitize the artifact, rotate anything credential-shaped, and assess whether
history needs to be rewritten before resuming.
