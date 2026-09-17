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

Those four steps are the whole procedure only when the release changes code
alone.  A release that also changes the database schema needs the longer one
below, and following the short one instead takes the deployment down.

## When the release also changes the schema

Deployed code refuses a database newer than itself.  It does not ignore tables
it has never heard of and carry on; it stops:

```
TaskLedgerError: task ledger schema is not supported
InboxError: candidate inbox schema is newer than this application
```

There is therefore no window in which the old code tolerates the new schema.
From the moment the migration commits until the units are running new code,
every long-running unit and every in-flight run fails.  The migration and the
repoint are one operation, and the units are down across it.

Check before deploying, not during.  The candidate release can inspect the
live database without touching it, and says whether it needs an upgrade:

```sh
<release>/venv/bin/foxhound-database inspect --database <db>
```

`"state": "current"` means the short procedure above is enough.
`"state": "upgrade_required"` means this one:

1. Build the new release and its environment.  This touches nothing that is
   running, so do it before taking anything down, and confirm the built
   release reports the revision you expect.
2. Stop the timers.  A one-shot that starts during the migration comes up on
   old code against a new schema, which is the failure this ordering exists to
   avoid.
3. Stop the in-flight runs, rather than waiting for them.  A claim that ends
   without releasing is re-queued when its lease expires, so a stopped run is
   retried rather than lost.  Waiting instead means a maintenance window as
   long as the slowest agent.
4. Stop the long-running units.
5. Back up the database, with the SQLite backup API rather than a file copy:
   a copy taken beside a live write-ahead log is not necessarily a database.
6. Migrate, using the new release's environment.
7. Repoint the release symlink, replacing it atomically rather than deleting
   and recreating it.
8. Start the long-running units, then the timers.
9. Read back both the reported revision and the schema state.

Capture two things before step 6, because after it they are the only way back:
the release directory currently deployed, and the path of the backup.  Rolling
back is repointing the symlink at the first and restoring the second, in that
order.

A release that changes the schema is also the one case where deploying hosts
one at a time does not reduce risk in the usual way: each host carries its own
database and migrates independently, so a failure on the second host is a
second incident rather than a warning about the first.

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
