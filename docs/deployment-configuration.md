# Private deployment configuration

`foxhound-deployment-config` makes the settings that belong to one deployment
explicit. The JSON file is private host state: keep it outside the checkout,
make it owner-only (`0600`), and do not commit it, paste it into issues, or
send its rendered command lines to logs. It contains paths but never token
values.

The first version covers the task-card service, one execution scheduler, and
one execution runner. It is intentionally strict: every field below is
required when that component is enabled, unknown fields are rejected, and all
paths are absolute.

```json
{
  "schema": "foxhound.deployment-config",
  "schema_version": 1,
  "database": "/srv/example/private-foxhound-state/foxhound.sqlite3",
  "agent_profile_directory": null,
  "card_service": {
    "enabled": true,
    "bind": "<canonical IPv4 loopback address>",
    "port": 8790,
    "request_timeout_seconds": 5,
    "task_token_files": {
      "drip": "/srv/example/private-foxhound-state/task-drip.token"
    },
    "execution_card_delivery": true,
    "execution_token_files": {
      "drip": "/srv/example/private-foxhound-state/execution-drip.token"
    }
  },
  "workflow": {
    "default_agent_profile": "general",
    "plan_without_asking": ["issue"],
    "execution_slot_cap": 2,
    "plan_ready_cap": 10,
    "awaiting_reader_cap": 20
  },
  "execution_runner": {
    "enabled": true,
    "run_root": "/srv/example/private-foxhound-runs",
    "gw_endpoint": "http://<canonical IPv4 loopback address>:8787",
    "gw_alias": "example-operator",
    "gw_token_file": "/srv/example/private-foxhound-state/gw.token",
    "agent_command": "hermes",
    "worker_command": "foxhound-task-worker",
    "runner_slot": "primary",
    "knowledge_root": null,
    "task_work_root": null,
    "task_kb_root": null
  }
}
```

The two loopback placeholders in this public example must be replaced with
the canonical IPv4 loopback address before private validation.

Set a disabled `card_service` or `execution_runner` to exactly
`{"enabled": false}`. The workflow section remains required because it owns
the shared policy and limits.

Validate before changing a service definition or restarting anything:

```sh
foxhound-deployment-config \
  --config /srv/example/private-foxhound-state/deployment.json \
  validate
```

Validation checks the selected database and profile configuration, the
loopback bind and port, capacity and planning rules, and the same owner-only
token-file and role rules used by the running services. When execution-card
delivery is enabled, `drip` must be present in both token maps; this catches a
card path that could start but could not deliver. Failures are deliberately
content-free.

Render the arguments for a supported component only after validation:

```sh
foxhound-deployment-config \
  --config /srv/example/private-foxhound-state/deployment.json \
  render --component task-cards
```

Supported names are `task-cards`, `execution-schedule`, and
`execution-runner`. Rendering reads no token contents. Its JSON output does
contain private paths, so consume it only in the private deployment mechanism,
never in a repository, issue, or shared log.
