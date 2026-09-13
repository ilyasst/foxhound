"""The bounded external action: what it refuses, and what it cannot choose.

Foxhound bounds *when* an action may happen. This module bounds *what* it is —
specifically the target, which is read from the task's binding and is not an
argument. These tests are mostly about refusals, because the refusals are the
feature.
"""
from __future__ import annotations

import unittest
from unittest import mock

from foxhound import forge_action
from foxhound.forge_action import ForgeActionError, open_pull_request


def _gh(results):
    """Stub the subprocess boundary; everything above it is the real code."""
    calls = []

    def runner(*args, timeout=None):
        calls.append(args)
        for prefix, result in results:
            if args[:len(prefix)] == prefix:
                return result
        return (1, "", "unexpected call")

    runner.calls = calls
    return runner


OK_DEFAULT_BRANCH = (("gh", "api"), (0, "main\n", ""))
OK_CREATE = (("gh", "pr", "create"), (0, "https://example.com/acme/w/pull/7\n", ""))
OK_VIEW = (("gh", "pr", "view"),
           (0, '{"number": 7, "url": "https://example.com/acme/w/pull/7"}', ""))


class TargetIsNotTheAgentsToChoose(unittest.TestCase):
    def test_a_pull_request_is_opened_against_the_task_repository(self) -> None:
        runner = _gh([OK_DEFAULT_BRANCH, OK_CREATE, OK_VIEW])
        with mock.patch.object(forge_action, "_run", runner):
            receipt = open_pull_request(
                origin_kind="issue", repository="github.com/acme/widget",
                issue="2", task_id=4, head="fix/thing", title="Fix the thing",
                body="Body.")
        self.assertEqual(receipt.repository, "github.com/acme/widget")
        self.assertEqual(receipt.number, 7)
        self.assertEqual(receipt.base, "main")
        create = next(c for c in runner.calls if c[:3] == ("gh", "pr", "create"))
        # The repository is passed from the binding, never from a caller
        # argument — there is no parameter that could name another one.
        self.assertIn("acme/widget", create)

    def test_the_body_carries_provenance(self) -> None:
        # The credential may belong to a person; the pull request should still
        # say an agent opened it.
        runner = _gh([OK_DEFAULT_BRANCH, OK_CREATE, OK_VIEW])
        with mock.patch.object(forge_action, "_run", runner):
            open_pull_request(
                origin_kind="issue", repository="github.com/acme/widget",
                issue="2", task_id=4, head="fix/thing", title="T", body="Body.")
        create = next(c for c in runner.calls if c[:3] == ("gh", "pr", "create"))
        body = create[create.index("--body") + 1]
        self.assertIn("Opened by Foxhound for task 4", body)
        self.assertIn("github.com/acme/widget#2", body)
        self.assertTrue(body.startswith("Body."))


class Refusals(unittest.TestCase):
    def test_a_task_that_is_not_a_forge_issue_is_refused(self) -> None:
        with self.assertRaises(ForgeActionError):
            open_pull_request(
                origin_kind="meeting", repository="github.com/acme/widget",
                issue="2", task_id=4, head="h", title="T", body="")

    def test_a_head_equal_to_base_is_refused(self) -> None:
        # Almost always means the work was done on the default branch.
        runner = _gh([OK_DEFAULT_BRANCH])
        with mock.patch.object(forge_action, "_run", runner):
            with self.assertRaises(ForgeActionError) as caught:
                open_pull_request(
                    origin_kind="issue", repository="github.com/acme/widget",
                    issue="2", task_id=4, head="main", title="T", body="")
        self.assertIn("must propose a branch", str(caught.exception))
        self.assertFalse(any(c[:3] == ("gh", "pr", "create")
                             for c in runner.calls))

    def test_an_unsupported_forge_is_refused(self) -> None:
        with self.assertRaises(ForgeActionError):
            open_pull_request(
                origin_kind="issue", repository="bitbucket.org/acme/widget",
                issue="2", task_id=4, head="h", title="T", body="")

    def test_a_non_canonical_repository_is_refused(self) -> None:
        with self.assertRaises(ForgeActionError):
            open_pull_request(
                origin_kind="issue", repository="widget", issue="2",
                task_id=4, head="h", title="T", body="")

    def test_a_missing_head_or_title_is_refused(self) -> None:
        for head, title in (("", "T"), ("h", "   ")):
            with self.subTest(head=head, title=title):
                with self.assertRaises(ForgeActionError):
                    open_pull_request(
                        origin_kind="issue",
                        repository="github.com/acme/widget", issue="2",
                        task_id=4, head=head, title=title, body="")

    def test_a_forge_refusal_is_surfaced_not_swallowed(self) -> None:
        runner = _gh([OK_DEFAULT_BRANCH,
                      (("gh", "pr", "create"), (1, "", "pull request already exists"))])
        with mock.patch.object(forge_action, "_run", runner):
            with self.assertRaises(ForgeActionError) as caught:
                open_pull_request(
                    origin_kind="issue", repository="github.com/acme/widget",
                    issue="2", task_id=4, head="fix/thing", title="T", body="")
        self.assertIn("already exists", str(caught.exception))

    def test_an_unnameable_base_stops_before_creating(self) -> None:
        runner = _gh([(("gh", "api"), (1, "", "Not Found"))])
        with mock.patch.object(forge_action, "_run", runner):
            with self.assertRaises(ForgeActionError):
                open_pull_request(
                    origin_kind="issue", repository="github.com/acme/widget",
                    issue="2", task_id=4, head="fix/thing", title="T", body="")
        self.assertFalse(any(c[:3] == ("gh", "pr", "create")
                             for c in runner.calls))

    def test_nothing_here_pushes_or_forces(self) -> None:
        runner = _gh([OK_DEFAULT_BRANCH, OK_CREATE, OK_VIEW])
        with mock.patch.object(forge_action, "_run", runner):
            open_pull_request(
                origin_kind="issue", repository="github.com/acme/widget",
                issue="2", task_id=4, head="fix/thing", title="T", body="")
        flat = " ".join(" ".join(call) for call in runner.calls)
        for forbidden in (" push", "--force", "-f ", "merge"):
            self.assertNotIn(forbidden, flat)


class AgentGuidance(unittest.TestCase):
    def test_the_agent_is_pointed_at_the_bounded_action(self) -> None:
        from foxhound.execution_runner import agent_prompt

        prompt = agent_prompt()
        self.assertIn("act pull-request", prompt)
        self.assertIn("not yours to choose", prompt)
        # Using the forge CLI directly would bypass every bound in this module.
        self.assertIn("Do not open pull requests with the forge CLI", prompt)

    def test_the_worker_command_is_substituted(self) -> None:
        from foxhound.execution_runner import agent_prompt

        prompt = agent_prompt("other-worker")
        self.assertIn("other-worker act pull-request", prompt)
        self.assertNotIn("{worker", prompt)


if __name__ == "__main__":
    unittest.main()
