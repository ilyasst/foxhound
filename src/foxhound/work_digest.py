"""Condense an agent's work markdown into a few sentences for a card.

A card is a glance with buttons under it, and a plan is a document. The
renderer's fallback is to show the document's opening, which is the worst
few hundred words to pick: an agent opens with framing -- what the task
is, what it looked at -- and the sentence the reader has to agree with is
usually somewhere in the middle. On one plan the opening reached none of
the eight thousand characters that carried the recommendation.

So the condensing happens here, once, when the result is recorded, and
the card renders the outcome for free. It does NOT happen at render time:
a card body is built inside a database transaction that a caller is
waiting on with a short timeout, and putting a remote model call there
would make card delivery depend on the fleet being up.

Everything here fails open. No digest is a normal outcome -- the model is
remote, it can be busy, and the card renders without one. A failure must
never cost the reader the card.

Stdlib only, like the rest of the package: the endpoint is an
OpenAI-compatible HTTP API and `urllib` is enough to speak it.
"""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.request


#: The capability to ask for, not a model name. The local gateway routes
#: it to whichever host is actually serving something that qualifies. A
#: pinned model name would be a second place to update every time the
#: fleet changes, and would fail closed when that host is down.
CAPABILITY = "light"

#: Enough of a plan to summarise. Plans run to 128k characters; the model
#: behind `light` is small, and feeding it everything buys a worse digest
#: than feeding it the first several pages.
MAX_INPUT_CHARS = 12_000

#: Bounded on both sides: this goes on a card, and the column that stores
#: it refuses anything longer.
MAX_DIGEST_CHARS = 800

#: Short. The call is on the path that records a result, and a result
#: that cannot be recorded is worse than a card without a digest.
TIMEOUT_SECONDS = 45.0

DEFAULT_ENDPOINT = "http://127.0.0.1:8800"

_SYSTEM = (
    "You compress an agent work plan into a card blurb. "
    "Write 2 to 4 short sentences, plain prose, no markdown, no headings, "
    "no lists, under 500 characters. Say what will be done and the one "
    "thing that most affects whether the reader approves it. "
    "Write nothing else."
)


def endpoint() -> str:
    return os.environ.get("FOXHOUND_DIGEST_ENDPOINT") or DEFAULT_ENDPOINT


def enabled() -> bool:
    """Off is a supported configuration, not a degraded one."""
    return os.environ.get("FOXHOUND_DIGEST", "1").strip().lower() not in {
        "0", "false", "no", "off",
    }


def digest(work_markdown: str, *, opener=None) -> str:
    """A few sentences about `work_markdown`, or "" if we could not get any.

    Never raises. Every failure -- unreachable gateway, busy model, a
    reply in an unexpected shape, a model that ignored the instruction and
    wrote an essay -- returns "" and lets the card fall back to an
    excerpt.
    """
    text = (work_markdown or "").strip()
    if not text or not enabled():
        return ""
    # Already card-sized: condensing it would spend a model call to make
    # a worse copy of something the reader could have just read.
    if len(text) <= MAX_DIGEST_CHARS:
        return ""
    document = {
        "model": CAPABILITY,
        "messages": [
            {"role": "system", "content": _SYSTEM},
            {"role": "user", "content": text[:MAX_INPUT_CHARS]},
        ],
        "temperature": 0.2,
        "max_tokens": 300,
        "stream": False,
    }
    request = urllib.request.Request(
        f"{endpoint()}/v1/chat/completions",
        data=json.dumps(document).encode("utf-8"),
        headers={"Content-Type": "application/json",
                 "Accept": "application/json"},
        method="POST",
    )
    open_request = (opener or urllib.request).urlopen
    try:
        with open_request(request, timeout=TIMEOUT_SECONDS) as response:
            raw = response.read(64 * 1024)
        reply = json.loads(raw.decode("utf-8"))
        content = reply["choices"][0]["message"]["content"]
    except Exception:  # noqa: BLE001 - every failure is the same failure
        return ""
    return _clean(content)


def _clean(value: object) -> str:
    """Take the prose and refuse the rest.

    A small model asked for plain sentences will sometimes return a
    heading, a bulleted list, or a fenced block anyway. Those render as
    structure on the card and would make the digest look like the
    document it replaces, so the shape is enforced here rather than
    hoped for in the prompt.
    """
    if not isinstance(value, str):
        return ""
    kept = []
    for line in value.strip().split("\n"):
        line = line.strip()
        if not line or line.startswith(("#", "```", ">", "|")):
            continue
        if line[:2] in {"- ", "* ", "+ "}:
            line = line[2:].strip()
        kept.append(line)
    text = " ".join(kept).strip()
    if len(text) > MAX_DIGEST_CHARS:
        head = text[:MAX_DIGEST_CHARS]
        stop = max(head.rfind(". "), head.rfind("! "), head.rfind("? "))
        text = (head[:stop + 1] if stop > MAX_DIGEST_CHARS // 2
                else head.rstrip())
    return text
