"""The bounded external action: what it refuses, and what it cannot choose.

Foxhound bounds *when* an action may happen. This module bounds *what* it is —
specifically the target, which is read from the task's binding and is not an
argument. These tests are mostly about refusals, because the refusals are the
feature.
"""
from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from foxhound import forge_action
from foxhound.forge_action import (
    ForgeActionError,
    MAX_ISSUES_PER_TASK,
    open_issue,
    open_pull_request,
)


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
                repository="github.com/acme/widget",
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
                repository="github.com/acme/widget",
                issue="2", task_id=4, head="fix/thing", title="T", body="Body.")
        create = next(c for c in runner.calls if c[:3] == ("gh", "pr", "create"))
        body = create[create.index("--body") + 1]
        self.assertIn("Opened by Foxhound for task 4", body)
        self.assertIn("github.com/acme/widget#2", body)
        self.assertTrue(body.startswith("Body."))


class Refusals(unittest.TestCase):
    def test_a_head_equal_to_base_is_refused(self) -> None:
        # Almost always means the work was done on the default branch.
        runner = _gh([OK_DEFAULT_BRANCH])
        with mock.patch.object(forge_action, "_run", runner):
            with self.assertRaises(ForgeActionError) as caught:
                open_pull_request(
                    repository="github.com/acme/widget",
                    issue="2", task_id=4, head="main", title="T", body="")
        self.assertIn("must propose a branch", str(caught.exception))
        self.assertFalse(any(c[:3] == ("gh", "pr", "create")
                             for c in runner.calls))

    def test_an_unsupported_forge_is_refused(self) -> None:
        with self.assertRaises(ForgeActionError):
            open_pull_request(
                repository="bitbucket.org/acme/widget",
                issue="2", task_id=4, head="h", title="T", body="")

    def test_a_non_canonical_repository_is_refused(self) -> None:
        with self.assertRaises(ForgeActionError):
            open_pull_request(
                repository="widget", issue="2",
                task_id=4, head="h", title="T", body="")

    def test_a_missing_head_or_title_is_refused(self) -> None:
        for head, title in (("", "T"), ("h", "   ")):
            with self.subTest(head=head, title=title):
                with self.assertRaises(ForgeActionError):
                    open_pull_request(
                        
                        repository="github.com/acme/widget", issue="2",
                        task_id=4, head=head, title=title, body="")

    def test_a_forge_refusal_is_surfaced_not_swallowed(self) -> None:
        runner = _gh([OK_DEFAULT_BRANCH,
                      (("gh", "pr", "create"), (1, "", "pull request already exists"))])
        with mock.patch.object(forge_action, "_run", runner):
            with self.assertRaises(ForgeActionError) as caught:
                open_pull_request(
                    repository="github.com/acme/widget",
                    issue="2", task_id=4, head="fix/thing", title="T", body="")
        self.assertIn("already exists", str(caught.exception))

    def test_an_unnameable_base_stops_before_creating(self) -> None:
        runner = _gh([(("gh", "api"), (1, "", "Not Found"))])
        with mock.patch.object(forge_action, "_run", runner):
            with self.assertRaises(ForgeActionError):
                open_pull_request(
                    repository="github.com/acme/widget",
                    issue="2", task_id=4, head="fix/thing", title="T", body="")
        self.assertFalse(any(c[:3] == ("gh", "pr", "create")
                             for c in runner.calls))

    def test_nothing_here_pushes_or_forces(self) -> None:
        runner = _gh([OK_DEFAULT_BRANCH, OK_CREATE, OK_VIEW])
        with mock.patch.object(forge_action, "_run", runner):
            open_pull_request(
                repository="github.com/acme/widget",
                issue="2", task_id=4, head="fix/thing", title="T", body="")
        flat = " ".join(" ".join(call) for call in runner.calls)
        for forbidden in (" push", "--force", "-f ", "merge"):
            self.assertNotIn(forbidden, flat)


class PreparedWorktree(unittest.TestCase):
    def test_the_branch_is_derived_not_chosen(self) -> None:
        # One less thing an agent can get wrong, and a second attempt at the
        # same issue reuses the branch instead of littering the repository.
        self.assertEqual(forge_action.branch_for("42"), "foxhound/issue-42")

    def test_another_repository_may_be_prepared(self) -> None:
        # Real work spans repositories. The task's own is the default, not a
        # limit on what the agent may read.
        with tempfile.TemporaryDirectory() as td:
            runner = _gh([OK_DEFAULT_BRANCH,
                          (("gh", "repo", "clone"), (0, "", "")),
                          (("git",), (0, "", ""))])
            with mock.patch.object(forge_action, "_run", runner):
                path, _branch, _base = forge_action.prepare_worktree(
                    repository="github.com/acme/other", issue="2",
                    parent=Path(td))
            clone = next(c for c in runner.calls
                         if c[:3] == ("gh", "repo", "clone"))
            self.assertIn("acme/other", clone)
            self.assertIn("other", path.name)

    def test_an_existing_tree_is_not_clobbered(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            (Path(td) / "repo-widget-2").mkdir()
            runner = _gh([OK_DEFAULT_BRANCH])
            with mock.patch.object(forge_action, "_run", runner):
                with self.assertRaises(ForgeActionError) as caught:
                    forge_action.prepare_worktree(
                        
                        repository="github.com/acme/widget", issue="2",
                        parent=Path(td))
            self.assertIn("already prepared", str(caught.exception))

    def test_the_clone_is_blobless_not_shallow(self) -> None:
        # A push from a shallow clone is refused by some forges, and finding
        # that out at the push is finding out too late.
        with tempfile.TemporaryDirectory() as td:
            runner = _gh([OK_DEFAULT_BRANCH,
                          (("gh", "repo", "clone"), (0, "", "")),
                          (("git",), (0, "", ""))])
            with mock.patch.object(forge_action, "_run", runner):
                path, branch, base = forge_action.prepare_worktree(
                    repository="github.com/acme/widget",
                    issue="2", parent=Path(td))
            clone = next(c for c in runner.calls if c[:3] == ("gh", "repo", "clone"))
            self.assertIn("--filter=blob:none", clone)
            self.assertNotIn("--depth", " ".join(clone))
            self.assertEqual(branch, "foxhound/issue-2")
            self.assertEqual(base, "main")
            self.assertEqual(path.name, "repo-widget-2")

    def test_a_repository_publication_hook_is_enabled_in_the_new_worktree(self):
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "repo-widget-2"
            runner = _gh([OK_DEFAULT_BRANCH,
                          (("gh", "repo", "clone"), (0, "", "")),
                          (("git",), (0, "", ""))])
            with (mock.patch.object(forge_action, "_run", runner),
                  mock.patch.object(forge_action, "_configure_repository_hook") as configure):
                forge_action.prepare_worktree(
                    repository="github.com/acme/widget", issue="2",
                    parent=Path(td))
            configure.assert_called_once_with(path, "github.com/acme/widget")

    def test_failure_to_enable_a_repository_publication_hook_refuses_worktree(self):
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "repo"
            hook = path / "tools" / "hooks" / "pre-commit"
            hook.parent.mkdir(parents=True)
            hook.write_text("#!/bin/sh\n", encoding="utf-8")
            runner = _gh([(("git", "-C", str(path), "config", "core.hooksPath"),
                           (1, "", "refused"))])
            with mock.patch.object(forge_action, "_run", runner):
                with self.assertRaisesRegex(ForgeActionError, "guard could not be enabled"):
                    forge_action._configure_repository_hook(
                        path, "github.com/acme/widget")

    def test_a_repository_hook_configuration_uses_the_checked_in_path(self):
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "repo"
            hook = path / "tools" / "hooks" / "pre-commit"
            hook.parent.mkdir(parents=True)
            hook.write_text("#!/bin/sh\n", encoding="utf-8")
            runner = _gh([(("git", "-C", str(path), "config", "core.hooksPath"),
                           (0, "", ""))])
            with mock.patch.object(forge_action, "_run", runner):
                forge_action._configure_repository_hook(
                    path, "github.com/acme/widget")
            self.assertEqual(
                runner.calls,
                [("git", "-C", str(path), "config", "core.hooksPath", "tools/hooks")],
            )


class ReviewIsBounded(unittest.TestCase):
    def test_the_review_lands_on_the_task_s_own_pull_request(self):
        runner = _gh([(("gh", "pr", "review"), (0, "", "")),
                      (("gh", "pr", "view"),
                       (0, '{"url": "https://example.com/acme/w/pull/7"}', ""))])
        with mock.patch.object(forge_action, "_run", runner):
            receipt = forge_action.post_review(
                repository="github.com/acme/widget", number="7",
                task_id=4, body="It looks fine.")
        self.assertEqual(receipt.number, 7)
        comment = next(c for c in runner.calls
                       if c[:3] == ("gh", "pr", "review"))
        # The repository comes from the binding; no argument could name
        # another one.
        self.assertIn("acme/widget", comment)
        self.assertIn("7", comment)

    def test_the_review_says_an_agent_wrote_it(self):
        # The credential may belong to a person. A reader of the pull
        # request should still be able to tell.
        runner = _gh([(("gh", "pr", "review"), (0, "", "")),
                      (("gh", "pr", "view"), (0, '{"url": "u"}', ""))])
        with mock.patch.object(forge_action, "_run", runner):
            forge_action.post_review(
                repository="github.com/acme/widget", number="7",
                task_id=4, body="It looks fine.")
        comment = next(c for c in runner.calls
                       if c[:3] == ("gh", "pr", "review"))
        body = comment[comment.index("--body") + 1]
        self.assertTrue(body.startswith("It looks fine."))
        self.assertIn("Foxhound for task 4", body)

    def test_an_empty_review_is_refused(self):
        # Worse than none: it reads as a considered verdict of nothing.
        runner = _gh([])
        with mock.patch.object(forge_action, "_run", runner):
            for body in ("", "   ", "\n"):
                with self.subTest(body=body):
                    with self.assertRaises(ForgeActionError):
                        forge_action.post_review(
                            repository="github.com/acme/widget", number="7",
                            task_id=4, body=body)
        self.assertEqual(runner.calls, [])

    def test_an_unusable_target_is_refused_before_writing(self):
        runner = _gh([])
        with mock.patch.object(forge_action, "_run", runner):
            for change in (
                {"repository": "widget"},
                {"repository": "bitbucket.org/acme/widget"},
                {"number": "not-a-number"},
                {"number": "0"},
                {"number": ""},
            ):
                with self.subTest(change=change):
                    values = {"repository": "github.com/acme/widget",
                              "number": "7", "task_id": 4, "body": "x"}
                    values.update(change)
                    with self.assertRaises(ForgeActionError):
                        forge_action.post_review(**values)
        self.assertEqual(runner.calls, [])

    def test_a_refusal_by_the_forge_is_surfaced(self):
        runner = _gh([(("gh", "pr", "review"),
                       (1, "", "pull request is locked"))])
        with mock.patch.object(forge_action, "_run", runner):
            with self.assertRaises(ForgeActionError) as caught:
                forge_action.post_review(
                    repository="github.com/acme/widget", number="7",
                    task_id=4, body="x")
        self.assertIn("locked", str(caught.exception))

    def test_a_review_states_findings_rather_than_a_verdict(self):
        """Approving or rejecting a pull request is a statement about
        whether it should merge, and that is the reader's to make. The
        agent's job is to say what it found.
        """
        runner = _gh([(("gh", "pr", "review"), (0, "", "")),
                      (("gh", "pr", "view"), (0, '{"url": "u"}', ""))])
        with mock.patch.object(forge_action, "_run", runner):
            forge_action.post_review(
                repository="github.com/acme/widget", number="7",
                task_id=4, body="x")
        flat = " ".join(" ".join(call) for call in runner.calls)
        for forbidden in ("--approve", "--request-changes", "merge"):
            self.assertNotIn(forbidden, flat)


class IssueCommentIsBounded(unittest.TestCase):
    def test_the_comment_lands_on_the_task_s_own_issue(self):
        runner = _gh([(
            ("gh", "issue", "comment"), (0, "", "")),
            (("gh", "issue", "view"),
             (0, '{"url": "https://example.com/acme/w/issues/7"}', "")),
        ])
        with mock.patch.object(forge_action, "_run", runner):
            receipt = forge_action.post_issue_comment(
                repository="github.com/acme/widget", number="7",
                task_id=4, body="Synthetic progress update.")

        self.assertEqual(receipt.number, 7)
        comment = next(
            call for call in runner.calls
            if call[:3] == ("gh", "issue", "comment")
        )
        self.assertIn("acme/widget", comment)
        self.assertIn("7", comment)
        body = comment[comment.index("--body") + 1]
        self.assertIn("Synthetic progress update.", body)
        self.assertIn("Foxhound for task 4", body)

    def test_unusable_issue_comment_target_is_refused_before_writing(self):
        runner = _gh([])
        with mock.patch.object(forge_action, "_run", runner):
            for change in (
                {"repository": "widget"},
                {"repository": "bitbucket.org/acme/widget"},
                {"number": "not-a-number"},
                {"number": "0"},
                {"body": "  "},
            ):
                with self.subTest(change=change):
                    values = {
                        "repository": "github.com/acme/widget",
                        "number": "7", "task_id": 4, "body": "x",
                    }
                    values.update(change)
                    with self.assertRaises(ForgeActionError):
                        forge_action.post_issue_comment(**values)
        self.assertEqual(runner.calls, [])


class PushIsBounded(unittest.TestCase):
    def test_the_refname_is_explicit_on_both_sides(self) -> None:
        # A misconfigured local push default cannot redirect it.
        runner = _gh([(("git",), (0, "", ""))])
        with mock.patch.object(forge_action, "_run", runner):
            forge_action.push_branch(
                repository="github.com/acme/widget", path=Path("/tmp/x"),
                head_branch="foxhound/issue-2", base="main")
        push = runner.calls[0]
        self.assertIn("refs/heads/foxhound/issue-2:refs/heads/foxhound/issue-2",
                      push)

    def test_pushing_the_base_is_refused(self) -> None:
        runner = _gh([(("git",), (0, "", ""))])
        with mock.patch.object(forge_action, "_run", runner):
            with self.assertRaises(ForgeActionError):
                forge_action.push_branch(
                    repository="github.com/acme/widget", path=Path("/tmp/x"),
                    head_branch="main", base="main")
        self.assertEqual(runner.calls, [])

    def test_a_rejected_push_is_surfaced(self) -> None:
        runner = _gh([(("git",), (1, "", "protected branch hook declined"))])
        with mock.patch.object(forge_action, "_run", runner):
            with self.assertRaises(ForgeActionError) as caught:
                forge_action.push_branch(
                    repository="github.com/acme/widget", path=Path("/tmp/x"),
                    head_branch="foxhound/issue-2", base="main")
        self.assertIn("declined", str(caught.exception))


class AgentGuidance(unittest.TestCase):
    """The agent reads these through the fenced worker, not its arguments."""

    def _instructions(self, worker_command: str = "foxhound-task-worker"):
        from foxhound.agent_profiles import general_profile

        return general_profile().render_prompt(worker_command)

    def test_the_agent_is_pointed_at_the_bounded_action(self) -> None:
        prompt = self._instructions()
        self.assertIn("act pull-request", prompt)
        self.assertIn("act worktree", prompt)
        # The bound that matters is the EFFECT and its phase, which is the
        # same sentence GW's own agent runs under — not a restriction on what
        # the agent may read or where it may work.
        self.assertIn("unless the phase is external_action", prompt)
        self.assertIn("Do not overwrite unrelated dirty worktrees", prompt)

    def test_the_origin_is_a_lead_not_a_limit(self) -> None:
        prompt = self._instructions()
        self.assertIn("not a limit on what you may read", prompt)
        # A task may legitimately need more than one repository.
        self.assertIn("span several repositories", prompt)

    def test_the_worker_command_is_substituted(self) -> None:
        from foxhound.execution_runner import agent_prompt

        prompt = self._instructions("other-worker")
        self.assertIn("other-worker act pull-request", prompt)
        self.assertNotIn("{worker", prompt)
        bootstrap = agent_prompt("other-worker")
        self.assertIn("other-worker context", bootstrap)
        self.assertNotIn("{worker", bootstrap)


if __name__ == "__main__":
    unittest.main()


def _issue_list(items):
    return (("gh", "issue", "list"), (0, json.dumps(items), ""))


OK_ISSUE_CREATE = (
    ("gh", "issue", "create"),
    (0, "https://example.com/acme/w/issues/9\n", ""),
)
OK_ISSUE_VIEW = (
    ("gh", "issue", "view"),
    (0, '{"number": 9, "url": "https://example.com/acme/w/issues/9"}', ""),
)


class OpeningAnIssue(unittest.TestCase):
    """The only write here that creates work for the system that made it."""

    def test_an_issue_is_opened_on_the_task_repository(self) -> None:
        runner = _gh([_issue_list([]), OK_ISSUE_CREATE, OK_ISSUE_VIEW])
        with mock.patch.object(forge_action, "_run", runner):
            receipt = open_issue(
                repository="github.com/acme/widget", task_id=4,
                title="Template example cannot run", body="Body.")
        self.assertEqual(receipt.repository, "github.com/acme/widget")
        self.assertEqual(receipt.number, 9)
        create = next(
            c for c in runner.calls if c[:3] == ("gh", "issue", "create"))
        self.assertIn("acme/widget", create)

    def test_the_body_carries_provenance(self) -> None:
        runner = _gh([_issue_list([]), OK_ISSUE_CREATE, OK_ISSUE_VIEW])
        with mock.patch.object(forge_action, "_run", runner):
            open_issue(repository="github.com/acme/widget", task_id=4,
                       title="T", body="Body.")
        create = next(
            c for c in runner.calls if c[:3] == ("gh", "issue", "create"))
        body = create[create.index("--body") + 1]
        self.assertIn("Opened by Foxhound for task 4", body)
        self.assertTrue(body.startswith("Body."))

    def test_a_title_already_open_is_refused_and_names_it(self) -> None:
        """Re-filing a finding is how a re-surfaced task becomes a loop."""
        runner = _gh([_issue_list([{
            "number": 3, "title": "Template  example CANNOT run",
            "body": "", "url": "https://example.com/acme/w/issues/3",
        }])])
        with mock.patch.object(forge_action, "_run", runner):
            with self.assertRaises(ForgeActionError) as caught:
                open_issue(
                    repository="github.com/acme/widget", task_id=4,
                    title="Template example cannot run", body="Body.")
        self.assertIn("#3 is already open", str(caught.exception))
        self.assertNotIn(
            ("gh", "issue", "create"),
            [c[:3] for c in runner.calls],
        )

    def test_a_task_cannot_exceed_its_issue_ceiling(self) -> None:
        """Counted from the forge: a task outlives any one run directory."""
        marker = "Opened by Foxhound for task 4,"
        runner = _gh([_issue_list([
            {"number": n, "title": f"Earlier finding {n}",
             "body": f"Something.\n\n---\n_{marker} from work._",
             "url": f"https://example.com/acme/w/issues/{n}"}
            for n in range(1, MAX_ISSUES_PER_TASK + 1)
        ])])
        with mock.patch.object(forge_action, "_run", runner):
            with self.assertRaises(ForgeActionError) as caught:
                open_issue(repository="github.com/acme/widget", task_id=4,
                           title="One more finding", body="Body.")
        self.assertIn("which is the limit", str(caught.exception))

    def test_another_tasks_issues_do_not_consume_the_ceiling(self) -> None:
        runner = _gh([
            _issue_list([
                {"number": n, "title": f"Earlier finding {n}",
                 "body": "---\n_Opened by Foxhound for task 99, from work._",
                 "url": f"https://example.com/acme/w/issues/{n}"}
                for n in range(1, MAX_ISSUES_PER_TASK + 2)
            ]),
            OK_ISSUE_CREATE, OK_ISSUE_VIEW,
        ])
        with mock.patch.object(forge_action, "_run", runner):
            receipt = open_issue(
                repository="github.com/acme/widget", task_id=4,
                title="A new finding", body="Body.")
        self.assertEqual(receipt.number, 9)

    def test_an_unreadable_list_does_not_forbid_every_issue(self) -> None:
        """A preflight that cannot see must not become one that refuses."""
        runner = _gh([
            (("gh", "issue", "list"), (1, "", "forge unavailable")),
            OK_ISSUE_CREATE, OK_ISSUE_VIEW,
        ])
        with mock.patch.object(forge_action, "_run", runner):
            receipt = open_issue(repository="github.com/acme/widget",
                                 task_id=4, title="T", body="Body.")
        self.assertEqual(receipt.number, 9)

    def test_a_body_is_required(self) -> None:
        # A title alone is something a reader has to interpret.
        runner = _gh([])
        with mock.patch.object(forge_action, "_run", runner):
            with self.assertRaises(ForgeActionError) as caught:
                open_issue(repository="github.com/acme/widget", task_id=4,
                           title="T", body="   ")
        self.assertIn("body is required", str(caught.exception))

    def test_a_non_github_host_is_refused(self) -> None:
        with self.assertRaises(ForgeActionError):
            open_issue(repository="git.example.com/acme/widget", task_id=4,
                       title="T", body="Body.")

    @mock.patch("foxhound.forge_action._run")
    def test_post_review_with_hold_refuses_approval(self, _run):
        _run.side_effect = [
            # The view check
            (0, '{"reviewDecision": "CHANGES_REQUESTED", "labels": []}', ""),
        ]
        with self.assertRaisesRegex(ForgeActionError, "cannot approve a pull request with an outstanding hold"):
            forge_action.post_review(
                repository="github.com/owner/repo",
                number="123",
                task_id=1,
                body="Looks good",
                verdict="approve",
            )

    @mock.patch("foxhound.forge_action._run")
    def test_post_review_with_hold_label_refuses_approval(self, _run):
        _run.side_effect = [
            # The view check
            (0, '{"reviewDecision": "REVIEW_REQUIRED", "labels": [{"name": "hold"}]}', ""),
        ]
        with self.assertRaisesRegex(ForgeActionError, "cannot approve a pull request with an outstanding hold"):
            forge_action.post_review(
                repository="github.com/owner/repo",
                number="123",
                task_id=1,
                body="Looks good",
                verdict="approve",
            )
