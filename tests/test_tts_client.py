"""Tests for synthesizing task voice summaries via TTS endpoint."""

from __future__ import annotations

import io
import json
import unittest
from unittest import mock

from foxhound import tts_client


class _Response(io.BytesIO):
    def __init__(self, data: bytes, status: int = 200):
        super().__init__(data)
        self.status = status

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
        return False


def _opener(result):
    calls = []

    def urlopen(request, timeout=None):
        calls.append((request, timeout))
        if isinstance(result, Exception):
            raise result
        return result() if callable(result) else result

    return mock.Mock(urlopen=urlopen), calls


class TtsClientTests(unittest.TestCase):
    def test_synthesize_posts_to_endpoint_and_returns_bytes(self):
        audio_payload = b"RIFF1234WAVEfmt "
        opener, calls = _opener(lambda: _Response(audio_payload))
        result = tts_client.synthesize(
            "Hello from testing.",
            voice_name="amy",
            opener=opener,
        )
        self.assertEqual(result, audio_payload)
        self.assertEqual(len(calls), 1)
        request, timeout = calls[0]
        self.assertEqual(timeout, tts_client.TIMEOUT_SECONDS)
        sent = json.loads(request.data.decode("utf-8"))
        self.assertEqual(sent["input"], "Hello from testing.")
        self.assertEqual(sent["voice"], "amy")
        self.assertEqual(sent["response_format"], "wav")

    def test_empty_input_returns_none(self):
        self.assertIsNone(tts_client.synthesize(""))
        self.assertIsNone(tts_client.synthesize("   "))

    def test_error_or_timeout_fails_open_returning_none(self):
        opener, _ = _opener(Exception("synthetic connection refused"))
        result = tts_client.synthesize("Test text", opener=opener)
        self.assertIsNone(result)

    def test_non_200_response_returns_none(self):
        opener, _ = _opener(lambda: _Response(b"error", status=503))
        result = tts_client.synthesize("Test text", opener=opener)
        self.assertIsNone(result)
