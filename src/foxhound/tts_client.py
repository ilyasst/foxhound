"""TTS synthesis client for task voice summaries.

Calls an OpenAI-compatible /v1/audio/speech endpoint to generate WAV audio.
Fails open on any error or timeout so task execution is never blocked.
"""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.request

DEFAULT_ENDPOINT = "http://127.0.0.1:8802"
TIMEOUT_SECONDS = 15.0


def endpoint() -> str:
    return os.environ.get("FOXHOUND_TTS_ENDPOINT", DEFAULT_ENDPOINT)


def voice() -> str | None:
    return os.environ.get("FOXHOUND_TTS_VOICE") or None


def enabled() -> bool:
    return os.environ.get("FOXHOUND_TTS", "1").strip().lower() not in {
        "0", "false", "no", "off",
    }


def synthesize(
    text: str,
    *,
    voice_name: str | None = None,
    opener=None,
) -> bytes | None:
    """Generate audio bytes for text, or None if synthesis fails."""
    clean_text = (text or "").strip()
    if not clean_text or not enabled():
        return None

    payload = {
        "model": "qwen3-tts",
        "input": clean_text,
        "response_format": "wav",
    }
    v = voice_name or voice()
    if v:
        payload["voice"] = v

    url = f"{endpoint().rstrip('/')}/v1/audio/speech"
    request = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    open_request = (opener or urllib.request).urlopen
    try:
        with open_request(request, timeout=TIMEOUT_SECONDS) as response:
            if getattr(response, "status", 200) != 200:
                return None
            return response.read()
    except Exception:  # noqa: BLE001 - fails open
        return None
