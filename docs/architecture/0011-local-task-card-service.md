# ADR 0011: Authenticated local task-card service

Status: accepted for the staged extraction.

## Context

The task-card aggregate must remain inside Foxhound, while an existing card
gateway needs to deliver presentations and forward reader actions. Sharing the
SQLite database would give the gateway an unrestricted write path and couple
task persistence to transport deployment.

## Decision

Foxhound exposes five fixed POST routes for a trusted local card gateway:

- schedule due review cards;
- claim one card under a bounded delivery lease;
- acknowledge a successful delivery;
- release a failed delivery; and
- apply a version-fenced reader action.

An additional read-only POST route returns aggregate pending, delivering,
delivered, snoozed, and total active counts. A gateway computes its on-screen
load as `delivering + delivered`; it never needs task/card identities merely
to pace delivery.

Every application route requires one bearer token loaded from a nonsymlink,
owner-owned, mode-0600 regular file. Request contracts are versioned and exact:
unknown or duplicate JSON fields, invalid types, extra headers that change body
framing, and over-limit bodies fail before a task operation begins. Responses
are also versioned. Only a successful claim contains private card text, its
bounded callback keyboard, and a short-lived claim capability.

The pilot server accepts only canonical IPv4 loopback binds. It processes one
request at a time and applies a socket deadline before reading request headers,
which makes concurrency and resource use explicitly bounded. It is not a
remote API. A future multi-host gateway requires a separate authenticated TLS
or private-network design rather than a bind override.

The server logs only method, allow-listed route name, status, and duration. It
does not log client addresses, raw request lines, headers, bodies, bearer or
claim capabilities, task/card identifiers, delivery references, or card
content. Error responses use fixed messages and never echo rejected values.

## Deployment and activation

The database must already be explicitly migrated to the current Foxhound
schema. The server refuses an absent, incomplete, or older database and never
migrates it on startup. A synthetic service invocation is:

```sh
foxhound-task-cards \
  --database /srv/example/private-foxhound-state/foxhound.sqlite3 \
  --token-file /srv/example/private-foxhound-state/card-gateway.token \
  --bind 127.0.0.1 \
  --port 8790
```

Starting the service creates no card and performs no task mutation. Activation
still requires an authenticated scheduling request followed by a claim from
the gateway.

## Rollback

Before gateway activation, stop the service and retain the database. During a
delivery attempt, stop new gateway requests and let the short lease expire or
release the claim explicitly. Reader actions are durable task intent and must
never be rolled back by restoring an older database.

## Out of scope

This service does not discover tasks, access GW, expose general task mutation,
run a card transport, schedule itself, project lifecycle events, edit task
content, reassign owners, create workflow gates, or execute agents.
