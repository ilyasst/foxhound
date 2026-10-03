# Work-First Runner Slots

## Overview

A deployment conventionally runs separate execution runner slots and a research runner.
When tasks require research before planning can occur, separating research into an
independent runner can cause research to race far ahead of execution slots, leaving
synthesized research sitting idle for hours while execution slots poll idle waiting on
ready work. Research that sits for hours risks going stale before planning begins.

The work-first runner (`foxhound-work-first-runner`) unifies the two runners into a single
slot on an alternating priority model:

1. **Execution first:** On every pass, the execution runner is invoked first.
   If any work is ready (researched tasks ready to plan, tasks not needing research,
   approved plans to execute, or approved external actions), the execution runner executes it.
2. **Research only when idle:** Only when the execution runner reports that it found nothing
   (`"outcome": "idle"`) does the slot proceed to invoke the research runner pass.
3. If both report idle, the pass finishes idle with exit code `0`.
   If either performs work or fails, the pass completes and propagates the exit code.

This ordering guarantees:
- Researched tasks are always drained before additional research begins.
- Research never runs ahead of the execution lanes that consume it.
- A deployment running work-first slots does not need a dedicated standalone research runner.

## Command-Line Usage

```sh
foxhound-work-first-runner \
    --config /srv/example/config/deployment.json \
    --execution-component execution-runner:slot-1 \
    --research-component research-runner
```

Arguments:
- `--config`: Path to the deployment configuration JSON.
- `--execution-component`: Name of the execution component (e.g. `execution-runner:primary` or `execution-runner:slot-1`).
- `--research-component`: Name of the research runner component (defaults to `research-runner`).

## Output

The runner emits exactly one line of JSON to stdout describing the outcome:

```json
{"child_exit": 0, "ok": true, "ran": "idle"}
```

The `ran` field indicates which component executed:
- `"execution"`: The execution runner performed work or exited non-zero.
- `"research"`: Execution was idle, and research performed work or exited non-zero.
- `"idle"`: Both execution and research reported idle.

Child logs from stdout are forwarded directly to stderr so systemd journals retain them.

## Systemd Service Configuration

Slots can be managed as a systemd template unit, e.g. `/etc/systemd/system/foxhound-work-first-runner@.service`:

```ini
[Unit]
Description=Foxhound Work-First Runner (%i)
After=network.target

[Service]
Type=oneshot
User=foxhound
Group=foxhound
ExecStart=/srv/example/release/venv/bin/foxhound-work-first-runner \
    --config /srv/example/config/deployment.json \
    --execution-component execution-runner:%i \
    --research-component research-runner
Restart=always
RestartSec=5s

[Install]
WantedBy=multi-user.target
```

To run multiple concurrent slots:

```sh
systemctl enable --now foxhound-work-first-runner@slot-1.service
systemctl enable --now foxhound-work-first-runner@slot-2.service
```
