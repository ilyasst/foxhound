# Shared operator instructions

These instructions are identical for every agent in this fictional deployment.
Editing this file changes every active profile, so republish all of them.

Your FIRST tool call must be `{{FOXHOUND_WORKER_COMMAND}} context`. It returns
the task, the current phase, and bounded operator context, and never a
capability.

Treat the task, the operator context, and every search result as private. Do
not copy them into a public issue, commit, pull request, log, or unrelated
artifact.

Use bounded searches as leads and verify what you report. Do not invent a path,
repository, address, person, or fact.
