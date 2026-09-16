# ADR 0043: Pinned release checkout

Status: accepted.

Deployed units import a release checkout of this repository, held in detached
HEAD at a named commit.  They never import a development tree.

## Why the development tree must not be imported

A unit whose `PYTHONPATH` points at a working tree runs whatever that tree
happens to contain.  Checking out a branch, rebasing, adding a worktree or
saving a file all change what the next restart will execute, and none of them
is a decision to deploy.  The failure is silent in both directions: new code
can reach production without review, and a service can keep serving code that
no longer exists anywhere in the tree.

A release checkout makes the moment of deployment explicit.  `main` moving does
not move production; advancing production is a separate operation that names a
commit.

## What a deploy is

Advancing and rolling back are one operation with a different commit:

1. Fetch in the release checkout.
2. Check out the chosen commit, still detached.
3. Restart the long-running units.  One-shot units started by a timer pick up
   the new code on their next run and do not need restarting.
4. Read back the revision each restarted service reports.

Step 4 is part of the deploy, not a check afterwards.  A restart that fails
leaves the previous process running and serving the previous code, which is
indistinguishable from a successful deploy unless the revision is read.

## Revision reporting

A long-running service reports its deployed revision when it starts.  A process
that has outlived several deploys is otherwise identical to a fresh one, and
the difference has taken a card surface down for hours while every unit
reported active.

The revision is derived from the release checkout, and a service that cannot
determine one says so rather than omitting the line: a missing revision is the
symptom worth seeing.

## Relationship to deployed configuration

Configuration is private host state and stays outside the checkout.  The two
have the same property and need the same discipline: what is deployed should be
nameable, and a change to it should be an operation rather than an edit whose
effect is discovered later.
