"""Read the task's own forge thread: what it returns, what it refuses.

This tests the forge_thread module in isolation, stubbing the subprocess
boundary so every test runs without network access.
"""
from __future__ import annotations

import json
import unittest
from unittest import mock

from foxhound import forge_thread
from foxhound.forge_thread import ForgeThreadError, ThreadResult


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


# Synthetic issue thread with comments
ISSUE_THREAD_DATA = {
    "number": 42,
    "url": "https://github.com/example-org/example-repo/issues/42",
    "state": "open",
    "comments": {
        "nodes": [
            {
                "author": {"login": "reviewer-a"},
                "body": "The implementation adds a 575-line module the issue asked it not to add.",
                "createdAt": "2030-01-02T10:00:00Z",
            },
            {
                "author": {"login": "reviewer-b"},
                "body": "Also, the comparison conflates two outcomes.",
                "createdAt": "2030-01-02T11:00:00Z",
            },
        ],
        "pageInfo": {"hasNextPage": False, "endCursor": None},
    },
}

# Synthetic PR thread with reviews
PR_THREAD_DATA = {
    "number": 7,
    "url": "https://github.com/example-org/example-repo/pull/7",
    "state": "open",
    "reviews": {
        "nodes": [
            {
                "author": {"login": "reviewer-a"},
                "state": "COMMENTED",
                "body": "The head branch conflicts with main. Please rebase.",
                "createdAt": "2030-01-02T10:00:00Z",
            },
            {
                "author": {"login": "reviewer-b"},
                "state": "COMMENTED",
                "body": "Two concerns: the module is too large and the tests use real data.",
                "createdAt": "2030-01-02T12:00:00Z",
            },
        ],
    },
    "comments": {
        "nodes": [
            {
                "author": {"login": "author-x"},
                "body": "I will address the review feedback.",
                "createdAt": "2030-01-02T13:00:00Z",
            },
        ],
    },
}


class IssueThreadReads(unittest.TestCase):
    def test_returns_comments_on_the_task_issue(self) -> None:
        runner = _gh([
            (("gh", "api"), (0, json.dumps(ISSUE_THREAD_DATA), "")),
        ])
        with mock.patch.object(forge_thread, "_run", runner):
            result = forge_thread.read_issue_thread(
                repository="github.com/example-org/example-repo",
                number="42",
            )
        self.assertIsInstance(result, ThreadResult)
        self.assertEqual(result.kind, "issue")
        self.assertEqual(result.number, 42)
        self.assertEqual(result.repository, "github.com/example-org/example-repo")
        self.assertEqual(result.url, ISSUE_THREAD_DATA["url"])
        self.assertEqual(len(result.comments), 2)
        self.assertEqual(result.comments[0]["author"], "reviewer-a")
        self.assertFalse(result.truncated)
        self.assertEqual(result.reviews, [])

    def test_uses_only_the_task_binding_not_a_caller_argument(self) -> None:
        # The repository comes from the binding, not from a free argument.
        runner = _gh([
            (("gh", "api"), (0, json.dumps(ISSUE_THREAD_DATA), "")),
        ])
        with mock.patch.object(forge_thread, "_run", runner):
            forge_thread.read_issue_thread(
                repository="github.com/example-org/example-repo",
                number="42",
            )
        call = runner.calls[0]
        # The repository path appears in the API endpoint, not as a separate arg
        call_text = " ".join(call)
        self.assertIn("example-org/example-repo", call_text)
        self.assertIn("issues/42", call_text)


class PullRequestThreadReads(unittest.TestCase):
    def test_returns_reviews_and_comments_on_the_task_pr(self) -> None:
        runner = _gh([
            (("gh", "api"), (0, json.dumps(PR_THREAD_DATA), "")),
        ])
        with mock.patch.object(forge_thread, "_run", runner):
            result = forge_thread.read_pull_request_thread(
                repository="github.com/example-org/example-repo",
                number="7",
            )
        self.assertIsInstance(result, ThreadResult)
        self.assertEqual(result.kind, "pull-request")
        self.assertEqual(result.number, 7)
        self.assertEqual(len(result.reviews), 2)
        self.assertEqual(result.reviews[0]["author"], "reviewer-a")
        self.assertEqual(result.reviews[0]["state"], "COMMENTED")
        self.assertEqual(len(result.comments), 1)
        self.assertFalse(result.truncated)

    def test_empty_reviews_and_comments(self) -> None:
        empty_data = {
            "number": 99,
            "url": "https://github.com/example-org/example-repo/pull/99",
            "state": "open",
            "reviews": {"nodes": []},
            "comments": {"nodes": []},
        }
        runner = _gh([
            (("gh", "api"), (0, json.dumps(empty_data), "")),
        ])
        with mock.patch.object(forge_thread, "_run", runner):
            result = forge_thread.read_pull_request_thread(
                repository="github.com/example-org/example-repo",
                number="99",
            )
        self.assertEqual(result.reviews, [])
        self.assertEqual(result.comments, [])
        self.assertFalse(result.truncated)


class Refusals(unittest.TestCase):
    def test_non_canonical_repository_is_refused(self) -> None:
        with self.assertRaises(ForgeThreadError):
            forge_thread.read_issue_thread(
                repository="widget", number="1")

    def test_unsupported_forge_is_refused(self) -> None:
        with self.assertRaises(ForgeThreadError):
            forge_thread.read_issue_thread(
                repository="bitbucket.org/acme/widget", number="1")

    def test_invalid_issue_number_is_refused(self) -> None:
        for bad_number in ("", "abc", "0", "-1"):
            with self.subTest(number=bad_number):
                with self.assertRaises(ForgeThreadError):
                    forge_thread.read_issue_thread(
                        repository="github.com/acme/widget",
                        number=bad_number)

    def test_invalid_pr_number_is_refused(self) -> None:
        for bad_number in ("", "abc", "0"):
            with self.subTest(number=bad_number):
                with self.assertRaises(ForgeThreadError):
                    forge_thread.read_pull_request_thread(
                        repository="github.com/acme/widget",
                        number=bad_number)

    def test_forge_error_is_surfaced(self) -> None:
        runner = _gh([
            (("gh", "api"), (1, "", "Not Found")),
        ])
        with mock.patch.object(forge_thread, "_run", runner):
            with self.assertRaises(ForgeThreadError) as caught:
                forge_thread.read_issue_thread(
                    repository="github.com/acme/widget", number="1")
        self.assertIn("Not Found", str(caught.exception))

    def test_unparseable_response_is_surfaced(self) -> None:
        runner = _gh([
            (("gh", "api"), (0, "not json", "")),
        ])
        with mock.patch.object(forge_thread, "_run", runner):
            with self.assertRaises(ForgeThreadError) as caught:
                forge_thread.read_issue_thread(
                    repository="github.com/acme/widget", number="1")
        self.assertIn("unparseable", str(caught.exception))


class Truncation(unittest.TestCase):
    def test_long_comment_list_is_truncated(self) -> None:
        # Create a thread with more comments than the limit
        many_comments = {
            "number": 42,
            "url": "https://github.com/example-org/example-repo/issues/42",
            "state": "open",
            "comments": {
                "nodes": [
                    {
                        "author": {"login": f"user-{i}"},
                        "body": f"Comment number {i}",
                        "createdAt": "2030-01-02T10:00:00Z",
                    }
                    for i in range(150)
                ],
                "pageInfo": {"hasNextPage": False},
            },
        }
        runner = _gh([
            (("gh", "api"), (0, json.dumps(many_comments), "")),
        ])
        with mock.patch.object(forge_thread, "_run", runner):
            result = forge_thread.read_issue_thread(
                repository="github.com/example-org/example-repo",
                number="42",
            )
        self.assertTrue(result.truncated)
        self.assertLessEqual(len(result.comments), forge_thread._MAX_COMMENTS)

    def test_long_pr_thread_is_truncated(self) -> None:
        # Create a PR with reviews and comments that exceed the char limit
        big_body = "x" * 500
        many_items = {
            "number": 7,
            "url": "https://github.com/example-org/example-repo/pull/7",
            "state": "open",
            "reviews": {
                "nodes": [
                    {
                        "author": {"login": f"reviewer-{i}"},
                        "state": "COMMENTED",
                        "body": big_body,
                        "createdAt": "2030-01-02T10:00:00Z",
                    }
                    for i in range(50)
                ],
            },
            "comments": {
                "nodes": [
                    {
                        "author": {"login": f"commenter-{i}"},
                        "body": big_body,
                        "createdAt": "2030-01-02T10:00:00Z",
                    }
                    for i in range(50)
                ],
            },
        }
        runner = _gh([
            (("gh", "api"), (0, json.dumps(many_items), "")),
        ])
        with mock.patch.object(forge_thread, "_run", runner):
            result = forge_thread.read_pull_request_thread(
                repository="github.com/example-org/example-repo",
                number="7",
            )
        self.assertTrue(result.truncated)
