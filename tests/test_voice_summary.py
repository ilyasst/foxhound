"""Tests for generating user-facing voice summary on task execution completion."""

from __future__ import annotations

import io
import json
import unittest
from unittest import mock

from foxhound import voice_summary


class _Response(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
        return False


def _reply(content):
    return _Response(json.dumps(
        {"choices": [{"message": {"content": content}}]}
    ).encode("utf-8"))


def _opener(result):
    calls = []

    def urlopen(request, timeout=None):
        calls.append((request, timeout))
        if isinstance(result, Exception):
            raise result
        return result() if callable(result) else result

    return mock.Mock(urlopen=urlopen), calls


class VoiceSummaryTests(unittest.TestCase):
    def setUp(self):
        self.work = "Synthetic work report detailing accomplished tasks."

    def test_voice_summary_is_generated_with_prompt_and_fails_open(self):
        expected_text = (
            "The system updated the database schema to support voice notes. "
            "All background jobs now report status cleanly. "
            "Users can now hear task completion summaries directly."
        )
        opener, calls = _opener(lambda: _reply(expected_text))
        result = voice_summary.generate(
            self.work,
            title="Update voice delivery",
            summary="Completed audio work",
            deliverables=["artifact-1.wav"],
            opener=opener,
        )
        self.assertEqual(result, expected_text)
        self.assertEqual(len(calls), 1)
        request, timeout = calls[0]
        data = json.loads(request.data.decode("utf-8"))
        self.assertEqual(data["model"], voice_summary.capability())
        self.assertEqual(timeout, voice_summary.TIMEOUT_SECONDS)
        self.assertIn("Task: Update voice delivery", data["messages"][1]["content"])

    def test_voice_summary_cleans_markdown_and_lists(self):
        raw_text = (
            "### Summary\n"
            "- First achievement accomplished.\n"
            "* Second thing done cleanly.\n"
            "Final wrap up sentence."
        )
        opener, _ = _opener(lambda: _reply(raw_text))
        result = voice_summary.generate(self.work, opener=opener)
        self.assertEqual(
            result,
            "First achievement accomplished. Second thing done cleanly. Final wrap up sentence."
        )

    def test_backend_failure_returns_empty_string(self):
        opener, _ = _opener(Exception("synthetic connection error"))
        result = voice_summary.generate(self.work, opener=opener)
        self.assertEqual(result, "")

    def test_empty_work_returns_empty_string(self):
        result = voice_summary.generate("")
        self.assertEqual(result, "")
