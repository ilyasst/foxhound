#!/usr/bin/env python3
"""Attribution headers: what a call was for, never what it was about."""

from __future__ import annotations

import os
import unittest
from unittest import mock

from foxhound.caproute_attribution import MAX_VALUE_CHARS, request_headers

_VARS = ("CAPROUTE_APP", "CAPROUTE_OPERATION", "CAPROUTE_JOB",
         "CAPROUTE_RUN_ID", "CAPROUTE_WORK_ITEM_TYPE", "CAPROUTE_WORK_ITEM_ID")


def _headers(operation="op", env=None, **context):
    env = env or {}
    with mock.patch.dict(os.environ, env, clear=False):
        for name in _VARS:
            if name not in env:
                os.environ.pop(name, None)
        return request_headers(operation, **context)


class RequestHeaderTests(unittest.TestCase):
    def test_the_request_still_carries_its_content_type(self):
        """These replace a headers dict; the HTTP basics must survive."""
        got = _headers()
        self.assertEqual(got["Content-Type"], "application/json")
        self.assertEqual(got["Accept"], "application/json")

    def test_the_operation_is_named(self):
        self.assertEqual(_headers("work_digest")["X-Caproute-Operation"],
                         "work_digest")

    def test_the_app_defaults_to_foxhound(self):
        self.assertEqual(_headers()["X-Caproute-App"], "foxhound")

    def test_explicit_context_is_carried(self):
        got = _headers(job="prof", run_id="r1", work_item_type="task",
                       work_item_id="T9")
        self.assertEqual(got["X-Caproute-Job"], "prof")
        self.assertEqual(got["X-Caproute-Run-Id"], "r1")
        self.assertEqual(got["X-Caproute-Work-Item-Type"], "task")
        self.assertEqual(got["X-Caproute-Work-Item-Id"], "T9")

    def test_an_absent_field_is_omitted_rather_than_sent_empty(self):
        """A router distinguishes no answer from an unknown answer."""
        got = _headers()
        for header in ("X-Caproute-Job", "X-Caproute-Run-Id",
                       "X-Caproute-Work-Item-Id"):
            self.assertNotIn(header, got)

    def test_a_direct_call_inside_an_agent_run_inherits_that_run(self):
        got = _headers("work_digest", env={"CAPROUTE_RUN_ID": "run-42",
                                           "CAPROUTE_WORK_ITEM_ID": "T7"})
        self.assertEqual(got["X-Caproute-Run-Id"], "run-42")
        self.assertEqual(got["X-Caproute-Work-Item-Id"], "T7")
        # Its own operation still wins over the inherited one.
        self.assertEqual(got["X-Caproute-Operation"], "work_digest")

    def test_an_explicit_value_beats_the_environment(self):
        got = _headers(run_id="explicit", env={"CAPROUTE_RUN_ID": "ambient"})
        self.assertEqual(got["X-Caproute-Run-Id"], "explicit")

    def test_the_environment_can_rename_the_app(self):
        self.assertEqual(_headers(env={"CAPROUTE_APP": "foxhound-e2e"})
                         ["X-Caproute-App"], "foxhound-e2e")

    def test_control_characters_cannot_forge_a_header(self):
        got = _headers("digest", job="a\r\nX-Injected: yes\tb")
        self.assertNotIn("\r", got["X-Caproute-Job"])
        self.assertNotIn("\n", got["X-Caproute-Job"])
        self.assertNotIn("\t", got["X-Caproute-Job"])
        self.assertNotIn("X-Injected", got)

    def test_values_are_bounded(self):
        got = _headers(run_id="x" * 1000)
        self.assertEqual(len(got["X-Caproute-Run-Id"]), MAX_VALUE_CHARS)

    def test_a_blank_operation_is_not_sent(self):
        self.assertNotIn("X-Caproute-Operation", _headers("   "))

    def test_no_task_text_can_be_smuggled_through_a_known_field(self):
        """Ids only. Whatever is passed is bounded and stripped."""
        got = _headers(work_item_id="T9\nSubject: confidential thing")
        self.assertEqual(got["X-Caproute-Work-Item-Id"],
                         "T9Subject: confidential thing"[:MAX_VALUE_CHARS])
        self.assertNotIn("\n", got["X-Caproute-Work-Item-Id"])


class CallSiteTests(unittest.TestCase):
    """Each direct caller names itself, so the router can tell them apart."""

    def test_every_direct_caller_uses_a_distinct_operation(self):
        import ast
        import pathlib
        names = []
        root = pathlib.Path(__file__).resolve().parents[1] / "src" / "foxhound"
        for path in root.glob("*.py"):
            tree = ast.parse(path.read_text())
            for node in ast.walk(tree):
                if (isinstance(node, ast.Call)
                        and isinstance(node.func, ast.Name)
                        and node.func.id == "request_headers"
                        and node.args
                        and isinstance(node.args[0], ast.Constant)):
                    names.append(node.args[0].value)
        self.assertGreaterEqual(len(names), 4)
        self.assertEqual(len(names), len(set(names)), names)


if __name__ == "__main__":
    unittest.main()
