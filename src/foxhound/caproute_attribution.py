"""Say what a model call was for, so the fleet can be sized from it.

Every model call here goes through caproute, and which models each
machine should run is decided from what caproute records. caproute names
the process on the far end of the socket by itself, so it already knows
a call came from foxhound. What it cannot know is what the call was for:
`operation`, `job`, `run_id` and the work item are things only the caller
holds.

Two callers, one contract:

* the agent runner exports these as ``CAPROUTE_*`` environment variables
  before spawning the agent, because one agent is spawned per claim and
  the job is then constant for the life of that process;
* the small direct callers in this package — digests, titles, duplicate
  adjudication — name their own operation here, because they are one
  call each and have no child process to inherit anything.

Ids and low-cardinality names only. These become HTTP headers and land
in a router's log, so no task text, no prompt, no titles.
"""

from __future__ import annotations

import os

#: Long enough for a uuid or a profile id, short enough that a header
#: cannot become a payload. caproute truncates at the same bound.
MAX_VALUE_CHARS = 160

_ENVIRONMENT_FIELDS = (
    ("X-Caproute-Operation", "CAPROUTE_OPERATION"),
    ("X-Caproute-Job", "CAPROUTE_JOB"),
    ("X-Caproute-Run-Id", "CAPROUTE_RUN_ID"),
    ("X-Caproute-Work-Item-Type", "CAPROUTE_WORK_ITEM_TYPE"),
    ("X-Caproute-Work-Item-Id", "CAPROUTE_WORK_ITEM_ID"),
)


def _clean(value: object) -> str:
    """Bounded and printable: this is going into a header and a log line."""
    text = "".join(
        character for character in str(value)
        if character.isprintable() and character not in "\r\n\t"
    )
    return text.strip()[:MAX_VALUE_CHARS]


def request_headers(operation: str, **context: object) -> dict[str, str]:
    """Headers naming this call, ready to merge into a urllib request.

    `operation` is what the call does, not what it is about — "work_digest",
    not the text being digested. Extra context is accepted by field name
    (``job``, ``run_id``, ``work_item_type``, ``work_item_id``) and anything
    empty is left out rather than sent as a placeholder, because a router
    distinguishes "no answer" from "the answer is unknown".

    Environment values fill whatever the caller did not name, so a direct
    call made inside an agent run inherits that run's identity for free.
    """
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "X-Caproute-App": _clean(os.environ.get("CAPROUTE_APP") or "foxhound"),
        "X-Caproute-Process": "foxhound",
        "X-Caproute-Pid": str(os.getpid()),
    }
    operation = _clean(operation)
    if operation:
        headers["X-Caproute-Operation"] = operation
    named = {
        "X-Caproute-Job": context.get("job"),
        "X-Caproute-Run-Id": context.get("run_id"),
        "X-Caproute-Work-Item-Type": context.get("work_item_type"),
        "X-Caproute-Work-Item-Id": context.get("work_item_id"),
    }
    for header, value in named.items():
        cleaned = _clean(value) if value is not None else ""
        if cleaned:
            headers[header] = cleaned
    for header, variable in _ENVIRONMENT_FIELDS:
        if header in headers:
            continue
        cleaned = _clean(os.environ.get(variable) or "")
        if cleaned:
            headers[header] = cleaned
    return headers
