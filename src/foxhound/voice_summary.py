"""Generate a user-facing voice summary from an agent's task result.

Provides a 3-5 sentence explanation of what was accomplished from the
user's perspective in plain English with no technical jargon.

Everything here fails open. No summary is a normal outcome: every failure
(unreachable gateway, busy model, unexpected format) returns "" and lets
the task proceed without blocking completion.
"""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from typing import Sequence
from foxhound.caproute_attribution import request_headers

CAPABILITY = "light"
MAX_INPUT_CHARS = 12_000
MAX_SUMMARY_CHARS = 1_000
TIMEOUT_SECONDS = 30.0
DEFAULT_ENDPOINT = "http://127.0.0.1:8800"

_SYSTEM = (
    "Provide a 3-5 sentence explanation of what this task accomplished from the "
    "user's perspective, simple English, no jargon. Write plain sentences only, "
    "no markdown, no headings, no bullet points, no lists. Write nothing else."
)


def endpoint() -> str:
    return os.environ.get("FOXHOUND_DIGEST_ENDPOINT") or os.environ.get(
        "FOXHOUND_VOICE_SUMMARY_ENDPOINT", DEFAULT_ENDPOINT)


def capability() -> str:
    return os.environ.get("FOXHOUND_VOICE_SUMMARY_CAPABILITY", CAPABILITY)


def enabled() -> bool:
    return os.environ.get("FOXHOUND_VOICE_SUMMARY", "1").strip().lower() not in {
        "0", "false", "no", "off",
    }


def generate(
    work_markdown: str,
    *,
    title: str = "",
    summary: str = "",
    deliverables: Sequence[object] = (),
    opener=None,
) -> str:
    """A 3-5 sentence plain English summary, or "" if generation fails."""
    if not enabled():
        return ""

    parts = []
    if title:
        parts.append(f"Task: {title}")
    if summary:
        parts.append(f"Summary: {summary}")
    if deliverables:
        parts.append(f"Deliverables: {json.dumps(list(deliverables))}")
    if work_markdown:
        parts.append(f"Work:\n{work_markdown}")

    full_text = "\n\n".join(parts).strip()
    if not full_text:
        return ""

    document = {
        "model": capability(),
        "messages": [
            {"role": "system", "content": _SYSTEM},
            {"role": "user", "content": full_text[:MAX_INPUT_CHARS]},
        ],
        "temperature": 0.2,
        "max_tokens": 300,
        "stream": False,
    }
    request = urllib.request.Request(
        f"{endpoint()}/v1/chat/completions",
        data=json.dumps(document).encode("utf-8"),
        headers=request_headers("voice_summary"),
        method="POST",
    )
    open_request = (opener or urllib.request).urlopen
    try:
        with open_request(request, timeout=TIMEOUT_SECONDS) as response:
            raw = response.read(64 * 1024)
        reply = json.loads(raw.decode("utf-8"))
        content = reply["choices"][0]["message"]["content"]
    except Exception:  # noqa: BLE001 - fails open
        return ""
    return _clean(content)


def _clean(value: object) -> str:
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
    if len(text) > MAX_SUMMARY_CHARS:
        head = text[:MAX_SUMMARY_CHARS]
        stop = max(head.rfind(". "), head.rfind("! "), head.rfind("? "))
        text = (head[:stop + 1] if stop > MAX_SUMMARY_CHARS // 2
                else head.rstrip())
    return text
