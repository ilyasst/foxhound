# ADR 0020: Phase-restricted execution runner

Status: accepted for parallel comparison deployments.

## Context

A migration comparison may need Foxhound to prepare a plan while another
system remains authoritative for task lifecycle and execution. A reader gate
is not a sufficient deployment boundary: an accidental or stale approval can
queue a later phase. Filtering after a claim would also acquire authority that
the comparison runner is not permitted to hold.

## Decision

The one-shot execution runner accepts a repeatable `--allowed-phase` option.
The configured non-empty, duplicate-free allowlist is passed into the durable
claim operation. The claim query selects only queued workflows whose phase is
in that allowlist, inside the same immediate transaction that fences and
records the claim.

Omitting the option allows every workflow phase and preserves the normal
deployment behavior. A plan-only comparison uses `--allowed-phase plan`.
Queued execute and external-action work remains unchanged, and the runner
reports an idle outcome rather than launching an agent when no allowed work is
ready.

This restriction controls execution claims only. A parallel comparison must
also keep lifecycle projection into the authoritative system disabled and
must identify which card surface is observational. It must never use two
systems to perform the same external action.

## Failure and rollback

Invalid, empty, or duplicate phase configuration is rejected before a claim.
Rollback is to stop the runner, remove the explicit option, audit every queued
workflow, and then restart it. Removing the option restores all-phase behavior
and therefore must happen only when Foxhound is the selected execution
authority.
