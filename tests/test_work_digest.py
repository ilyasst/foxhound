"""The card blurb derived from an agent's work markdown.

Every test here is about one property: a failure to produce a digest must
cost the reader nothing but the digest. The model is remote, small, and
not under our control, so the interesting cases are all the ones where it
misbehaves.
"""

from __future__ import annotations

import io
import json
import unittest
from unittest import mock

from foxhound import work_digest


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


class WorkDigestTests(unittest.TestCase):
    def setUp(self):
        self.plan = "Synthetic plan sentence. " * 200

    def test_a_plan_is_condensed_and_the_capability_is_asked_for_by_name(self):
        opener, calls = _opener(lambda: _reply(
            "The agent will draft the report. Approval turns on the venue."
        ))
        result = work_digest.digest(self.plan, opener=opener)
        self.assertEqual(
            result,
            "The agent will draft the report. Approval turns on the venue.",
        )
        request, timeout = calls[0]
        sent = json.loads(request.data)
        # A capability, not a model: the gateway routes it to whichever
        # host is serving something that qualifies, so a host going down
        # is not an outage here.
        self.assertEqual(sent["model"], work_digest.CAPABILITY)
        self.assertEqual(timeout, work_digest.TIMEOUT_SECONDS)
        # The plan is the user turn; the instruction is not mixed into it.
        self.assertEqual(sent["messages"][1]["content"][:16],
                         self.plan[:16])

    def test_every_failure_is_an_empty_digest_and_never_an_exception(self):
        """The card renders without one. It must not fail to render at all."""
        for broken in (
            OSError("unreachable"),
            ValueError("not json"),
            TimeoutError("busy"),
        ):
            opener, _ = _opener(broken)
            self.assertEqual(work_digest.digest(self.plan, opener=opener), "")
        for shape in ("{}", '{"choices": []}', '{"choices": [{}]}'):
            opener, _ = _opener(_Response(shape.encode("utf-8")))
            self.assertEqual(work_digest.digest(self.plan, opener=opener), "")
        opener, _ = _opener(lambda: _reply(None))
        self.assertEqual(work_digest.digest(self.plan, opener=opener), "")

    def test_a_short_plan_is_not_worth_a_model_call(self):
        """Condensing something already card-sized spends a call to make a
        worse copy of what the reader could simply have read."""
        opener, calls = _opener(lambda: _reply("Should never be asked for."))
        self.assertEqual(work_digest.digest("Short plan.", opener=opener), "")
        self.assertEqual(work_digest.digest("", opener=opener), "")
        self.assertEqual(calls, [])

    def test_structure_is_stripped_rather_than_hoped_away(self):
        """A small model told to write prose will sometimes write a
        document anyway, and a digest shaped like a document is the thing
        this whole change exists to stop putting on a card."""
        opener, _ = _opener(lambda: _reply(
            "# Plan\n\n- First the report.\n* Then the email.\n"
            "```\ncode\n```\n> quoted\nApproval turns on the venue.\n"
        ))
        result = work_digest.digest(self.plan, opener=opener)
        self.assertEqual(
            result,
            "First the report. Then the email. code "
            "Approval turns on the venue.",
        )
        for marker in ("#", "```", ">", "\n", "- ", "* "):
            self.assertNotIn(marker, result)

    def test_an_overlong_digest_is_cut_at_a_sentence(self):
        opener, _ = _opener(lambda: _reply("Sentence number one. " * 200))
        result = work_digest.digest(self.plan, opener=opener)
        self.assertLessEqual(len(result), work_digest.MAX_DIGEST_CHARS)
        self.assertTrue(result.endswith("."))

    def test_the_call_can_be_turned_off_without_breaking_anything(self):
        opener, calls = _opener(lambda: _reply("Never asked."))
        with mock.patch.dict("os.environ", {"FOXHOUND_DIGEST": "0"}):
            self.assertEqual(work_digest.digest(self.plan, opener=opener), "")
        self.assertEqual(calls, [])


if __name__ == "__main__":
    unittest.main()
