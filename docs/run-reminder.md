# Run Reminder Hook

`foxhound-run-reminder` is a runtime hook command for agent execution passes.
It is invoked by the agent runner (such as Hermes) via the `pre_llm_call` hook
to remind agents to maintain their handoff notes and record their pass results
before the deadline expires.

## Purpose and Behavior

When executing long-running tasks, agents can lose track of time or forget to
update their handoff notes midway through their passes. If a pass times out or
encounters an unrecoverable failure without a handoff note, retry attempts
cannot benefit from prior investigation and work.

The hook inspects the current run state referenced by `FOXHOUND_EXECUTION_STATE`.
If the variable is unset or points to an invalid/unreadable state, the hook is a
silent no-op and outputs `{}`.

When inside a Foxhound pass, the hook measures elapsed time against the pass
budget:

- **≥ 85% elapsed budget**: Reminds the agent of remaining minutes and prompts
  to record the result or update the handoff note and release:
  `Foxhound reminder: about N minutes remain in this pass. Record your result now with \`record --outcome OUTCOME\`, or update your handoff note and call \`release --handoff\`.`
  (This notification takes precedence over earlier reminders.)
- **≥ 2/3 elapsed budget**: If a handoff note exists for this claim but its last
  modification was more than 10 minutes ago, prompts the agent to update it:
  `Foxhound reminder: update your handoff note at <path> with what changed since you last wrote it.`
- **≥ 1/3 elapsed budget**: If no handoff note has been written yet for this claim,
  prompts the agent to write one:
  `Foxhound reminder: write your handoff note now at <path>: what you established (with absolute paths), what changed and where, what remains, and the next step.`
- **Otherwise**: Emits `{}`.

The hook finishes quickly (< 200 ms), never raises errors, and outputs exactly
one valid JSON object with exit code 0.

## Hermes Configuration

Configure the hook in Hermes profile configuration:

```yaml
hooks:
  pre_llm_call:
    - command: /srv/example/foxhound/current/venv/bin/foxhound-run-reminder
```

For non-interactive runs, hooks must be accepted automatically without prompting.
Set the environment variable:

```sh
export HERMES_ACCEPT_HOOKS=1
```

or configure Hermes with:

```yaml
hooks_auto_accept: true
```
