# Agent Guard

A host runs agents beside long-lived developer checkouts of the same repositories. Those checkouts are shared. The Agent Guard prevents unintended modifications by automated agents on shared checkouts.

## Installation
Run `tools/install-agent-guard.sh` to install the guard in your local checkout.

## Protection details
The guard uses the Git `reference-transaction` hook. It aborts any ref updates (commits, resets, branch switches) if an unattended agent session marker (`HERMES_CRON_SESSION`, `HERMES_SESSION_SOURCE`, or `FOXHOUND_WORKFLOW_STATE`) is present.
If an agent attempts these operations, they are refused with a message naming the sanctioned `act.worktree` alternative, and the refusal is appended to `agent-guard-rejection.log` in the agent's `$TERMINAL_CWD`.

## What it does NOT cover
- Working tree modifications (creating, modifying, deleting untracked or tracked files without committing).
- `git stash` operations.
- Actions performed by human operators (when agent markers are not set).
