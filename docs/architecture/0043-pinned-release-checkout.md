# ADR 0043: Pinned release checkout

Status: accepted.

Deployed units import a release of this repository: an export of the tracked
tree at a named commit, in a directory named for that commit, with its own
environment.  A symlink selects which one is deployed.  They never import a
development tree.

A release has no git metadata.  It is not a checkout and cannot be fetched or
checked out in place; advancing production moves the selector to a different
release, and building one is the separate procedure below.

That held for every unit but one: the worker is run by the agent, not by
the runner, and was located through the agent's `PATH` rather than from the
release.  [ADR 0046](0046-worker-resolved-from-the-running-release.md)
closes that, and is required reading alongside this one.

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

1. Build the release you are advancing to, if it does not already exist, by
   the procedure in "How a release is built".  Nothing running is touched
   until step 2, so this is safe to do at any time.
2. Repoint the selector symlink at it, replacing it atomically with a rename
   rather than deleting and recreating it.  **This is the step that deploys.**
   Everything else restarts processes or confirms the result.
3. Restart the long-running units.  One-shot units started by a timer pick up
   the new code on their next run and do not need restarting.
4. Read back the revision each restarted service reports.

Step 4 is part of the deploy, not a check afterwards.  A restart that fails
leaves the previous process running and serving the previous code, which is
indistinguishable from a successful deploy unless the revision is read.

A one-shot that was already in flight when the selector moved keeps running the
previous release and finishes there.  So a run claimed shortly after a
promotion can show the previous behaviour from a perfectly correct deploy, and
the only way to tell that apart from a deploy that did not take is to compare
when that process started against when the selector moved.  Read back a
restarted service, which cannot be ambiguous, before reading anything into a
run.

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
7. Repoint the selector symlink, replacing it atomically with a rename rather
   than deleting and recreating it.  As in the short procedure, this is the
   step that deploys.
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

## How a release is built

The procedures above begin with a release that already exists.  Building one is
four steps, and each has a way of being subtly wrong that does not announce
itself.

1. **Export the tracked tree at the chosen commit** into a directory under the
   releases root.  An export, not a copy of a working tree: a copy carries
   whatever was uncommitted at the time, and the release then is not the commit
   it is named for, while looking exactly as though it were.
2. **Name the directory for the revision.**  This is load-bearing rather than a
   convention.  A promoted release has no git metadata, so the revision every
   component reports is derived from the directory name; name it anything else
   and each one reports an unknown revision, which removes the single line that
   makes a stale process visible.
3. **Create the environment inside the release and install the release into
   it, not as an editable install.**  An editable install resolves back to the
   tree it was installed from, which reintroduces precisely the coupling to a
   working tree that a release exists to remove.
4. **Repoint the symlink last**, once the release answers with the revision
   expected of it.  Until that point nothing running has been touched, which is
   what makes the first three steps safe to do at any time.

None of this is currently scripted.  That is tolerable while promotion is rare
and deliberate, but it means the procedure is reproduced from memory each time,
and steps 1 to 3 are the ones where a mistake produces a release that runs and
is wrong rather than one that fails.

## Before promoting a release that changes cards

Card buttons are a contract with the system that delivers them, and this
repository cannot test the other half.

Every button carries a verb.  The delivering side validates each verb against
its own set and refuses one it does not know — refusing the entire delivery
sweep, not the single card that carries it.  A release that adds a button
therefore stops *all* card delivery on every host running it, from the moment
the symlink moves until the other side is advanced.

The delivering side's verb set must be a superset of the buttons emitted here.
So it is advanced first, and rolled back last.

Nothing enforces this and no test here can: a new verb is valid on this side,
and the failure appears only in the deliverer's log, once per sweep interval.
Treat a release that adds or renames a button as a two-repository deploy with a
required order, not as a release.

## Before promoting a release that adds a store validation rule

The private profile store is host state and lives outside this repository, so
no test here can see its contents.  A release that adds a rule the store must
satisfy is therefore a change whose failure appears only on the host, and only
once the symlink has moved.

The rules compound because the store commands share one validation path.  A
fragment the new release refuses takes down every store operation at once:
`validate` fails, `publish` refuses, the install unit runs `validate` as its
first step and aborts before installing anything, and the host mirror validates
both stores and stops.  The effect is not that one profile cannot be edited; it
is that no profile can be published or installed on any host running the new
release.

It is also worse than a stalled install. The card service `Requires` the
install unit, so an install that exits non-zero takes the card service down
with it: the service does not start, and `systemctl start` reports only
`A dependency job for foxhound-task-cards.service failed`, naming neither the
store nor the fragment. Observed on the first promotion of the absolute-path
guard. Read the install unit's own status before reading anything into the
card service's.

So the store is brought into compliance first, and the release is promoted
after.  Rolling the release back restores the old rule, but a store edited in
the meantime is not rolled back with it.

The candidate release can answer this before anything is promoted, because
validation only reads:

```sh
<release>/venv/bin/foxhound-agent-profile-store --source <store> validate
```

Run it on every host that has a store, not only the one being promoted first.
A store that validates under the deployed release and fails under the candidate
is the whole signal; treat it exactly as the card-verb ordering above, as a
required order rather than a check to perform afterwards.

The first rule of this kind is the absolute-path prompt guard: a fragment that
names a machine path is refused, because a profile revision renders identically
on every host and a path does not.  Replace the path with a symbolic root the
worker reports in `capabilities.deployment_roots`, publish, install, and then
promote.

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
