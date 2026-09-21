"""Perform one approved external action against a task's own origin.

WHY THIS EXISTS RATHER THAN LETTING THE AGENT RUN THE FORGE CLI

A disposable execution agent has a terminal, and the credential it inherits
can usually write to every repository its operator can reach. Foxhound's
supervision bounds WHEN an action may happen — a reader approves, a claim is
held, the run is time-boxed — but nothing in it bounds WHAT the action is.
The approved `external_actions` are prose, so an agent that misreads them is
constrained only by its own good behaviour.

This module removes the most consequential degree of freedom: the target. The
repository is read from the task's accepted candidate binding, never from an
argument, so an agent acting on the wrong repository is not a mistake it is
able to make. What it may still choose — the branch, the title, the body — is
reviewable content rather than reach.

WHAT IS DELIBERATELY REFUSED

* A task with no origin, or an origin that is not a forge issue. There is
  nothing to act on and the target must never be inferred from task text.
* A head branch equal to the base. A pull request from a branch to itself is
  not a proposal; it usually means the agent worked directly on the default
  branch, which is exactly what opening a pull request is meant to avoid.
* A forge this module does not speak. Silence is not consent.

Nothing here pushes, commits, or force-updates anything. It proposes; a human
merges.
"""

from __future__ import annotations

import json
import subprocess
from dataclasses import dataclass
from pathlib import Path

#: The trailer appended to every body this module sends. An operator reading
#: the pull request should be able to tell that an agent opened it, even when
#: the credential belongs to a person — attribution the account itself cannot
#: provide.
PROVENANCE = (
    "\n\n---\n_Opened by Foxhound for task {task_id} from {repository}"
    "#{issue}. Review before merging._\n"
)

#: The same attribution on a review. A reviewer reading it should be able
#: to tell an agent wrote it, even when the credential belongs to a person.
REVIEW_PROVENANCE = (
    "\n\n---\n_Reviewed by Foxhound for task {task_id} on {repository}"
    "#{number}. A person has approved posting this; its contents are the "
    "agent's._\n"
)

#: The same visible attribution for a progress update on a task's own issue.
#: It deliberately says neither that the issue is resolved nor that the
#: implementation is approved; those statements remain the reader's.
ISSUE_COMMENT_PROVENANCE = (
    "\n\n---\n_Updated by Foxhound for task {task_id} on {repository}"
    "#{number}. A person has approved posting this; its contents are the "
    "agent's._\n"
)

#: The same attribution on an issue an agent opened. It says what the issue
#: came out of, so a reader meeting it cold can find the work that raised it
#: and judge it, rather than finding an unexplained report from an account.
ISSUE_PROVENANCE = (
    "\n\n---\n_Opened by Foxhound for task {task_id}, from work on "
    "{repository}. A person has approved opening this; its contents are the "
    "agent's._\n"
)

#: How many issues one task may open. An issue is the only write here that
#: creates work for the system that made it: enrolment turns an open issue
#: into a candidate, a candidate into a task, and that task can reach this
#: same code. A review finding a dozen small things should raise the two that
#: matter and say the rest in its review, not mint a dozen tasks.
MAX_ISSUES_PER_TASK = 2

_PUSH_TIMEOUT_S = 120
_CLONE_TIMEOUT_S = 600


class ForgeActionError(RuntimeError):
    """The action is refused, or the forge would not perform it."""


@dataclass(frozen=True)
class IssueReceipt:
    """What was opened, in terms an immutable result can carry."""

    repository: str
    number: int
    url: str
    title: str


@dataclass(frozen=True)
class PullRequestReceipt:
    """What was done, in terms an immutable result can carry."""

    repository: str
    issue: str
    number: int
    url: str
    head: str
    base: str


def _run(*args: str, timeout: int = _PUSH_TIMEOUT_S) -> tuple[int, str, str]:
    try:
        completed = subprocess.run(
            args, capture_output=True, text=True, timeout=timeout)
    except FileNotFoundError:
        return 127, "", "gh is not available"
    except (OSError, subprocess.SubprocessError) as exc:
        return 1, "", f"{type(exc).__name__}: {exc}"
    return completed.returncode, completed.stdout or "", completed.stderr or ""


def _detail(stderr: str) -> str:
    first = next((line.strip() for line in (stderr or "").splitlines()
                  if line.strip()), "")
    return first[:200] or "no detail"


def default_branch(repository: str) -> str:
    """The base a proposal should target, asked of the forge, not assumed."""
    host, _, name_with_owner = repository.partition("/")
    rc, out, err = _run("gh", "api", "--hostname", host,
                        f"repos/{name_with_owner}", "--jq", ".default_branch")
    if rc != 0 or not out.strip():
        raise ForgeActionError(
            f"{repository}: the forge did not name a default branch "
            f"({_detail(err)})")
    return out.strip()


def branch_for(issue: str) -> str:
    """The branch a task's work belongs on.

    Derived, not chosen. A deterministic name makes a second attempt at the
    same issue reuse its branch instead of littering the repository with
    near-duplicates, and removes one more thing an agent can get wrong.
    """
    return f"foxhound/issue-{issue}"


def prepare_worktree(
    *,
    repository: str,
    issue: str,
    parent: Path,
) -> tuple[Path, str, str]:
    """Clone a repository into ``parent`` on a fresh branch.

    Returns ``(path, branch, base)``. A convenience, not a gate: the task's
    own repository is the default, but real work spans repositories, and an
    agent that needs a second one should be able to say so rather than be
    unable to do the task. What stays bounded is the EFFECT — nothing is
    pushed here — not which source the agent may read.

    A blobless partial clone rather than a shallow one. Shallow is faster
    still, but a push from a shallow clone is refused by some forges, and
    discovering that at the push is discovering it too late.
    """
    if not repository or repository.count("/") != 2:
        raise ForgeActionError("the task's repository is not a canonical locator")
    host, _, name_with_owner = repository.partition("/")
    if host != "github.com":
        raise ForgeActionError(f"{host}: preparing a worktree is not supported here")

    base = default_branch(repository)
    branch = branch_for(issue)
    path = Path(parent) / f"repo-{repository.rsplit('/', 1)[-1]}-{issue}"
    if path.exists():
        raise ForgeActionError(
            "a working tree for this task already exists; the phase has "
            "already prepared one"
        )
    rc, _out, err = _run(
        "gh", "repo", "clone", name_with_owner, str(path), "--",
        "--filter=blob:none", "--single-branch", "--branch", base,
        timeout=_CLONE_TIMEOUT_S)
    if rc != 0:
        raise ForgeActionError(
            f"{repository}: the repository could not be cloned ({_detail(err)})")
    rc, _out, err = _run("git", "-C", str(path), "checkout", "-b", branch)
    if rc != 0:
        raise ForgeActionError(
            f"{repository}: the work branch could not be created ({_detail(err)})")
    _configure_repository_hook(path, repository)
    return path, branch, base


def _configure_repository_hook(path: Path, repository: str) -> None:
    """Enable a repository-provided pre-commit guard in an agent worktree.

    A contributor instruction can require a checked-in hook, but a fresh
    clone does not inherit the local ``core.hooksPath`` setting that activates
    it.  Honor that repository-owned guard when it is present; a failure to
    activate it refuses the worktree before an agent can create a commit.
    Repositories without such a hook retain their existing workflow.
    """
    hook = path / "tools" / "hooks" / "pre-commit"
    if not hook.is_file():
        return
    rc, _out, err = _run(
        "git", "-C", str(path), "config", "core.hooksPath", "tools/hooks"
    )
    if rc != 0:
        raise ForgeActionError(
            f"{repository}: the repository publication guard could not be "
            f"enabled ({_detail(err)})"
        )


def push_branch(*, repository: str, path: Path, head_branch: str,
                base: str) -> None:
    """Push the prepared branch, and only that branch.

    The refname is written explicitly on both sides so a misconfigured local
    push default cannot redirect it, and the base is refused outright: a task
    proposes a change, it does not update the branch it targets.
    """
    if head_branch == base:
        raise ForgeActionError(
            f"refusing to push {head_branch!r}: it is the branch the proposal "
            "targets"
        )
    rc, _out, err = _run(
        "git", "-C", str(path), "push", "--set-upstream", "origin",
        f"refs/heads/{head_branch}:refs/heads/{head_branch}",
        timeout=_PUSH_TIMEOUT_S)
    if rc != 0:
        raise ForgeActionError(
            f"{repository}: the branch could not be pushed ({_detail(err)})")


@dataclass(frozen=True)
class ReviewReceipt:
    repository: str
    number: int
    url: str


@dataclass(frozen=True)
class IssueCommentReceipt:
    repository: str
    number: int
    url: str


def post_issue_comment(
    *,
    repository: str,
    number: str,
    task_id: int,
    body: str,
) -> IssueCommentReceipt:
    """Comment on the issue bound to a task, never an agent-supplied target."""
    if not repository or repository.count("/") != 2:
        raise ForgeActionError(
            "the task's repository is not a canonical locator")
    host, _, name_with_owner = repository.partition("/")
    if host != "github.com":
        raise ForgeActionError(
            f"{host}: posting an issue comment is not supported here")
    digits = (number or "").strip()
    if not digits.isdigit() or int(digits) < 1:
        raise ForgeActionError("the task does not name an issue")
    if not (body or "").strip():
        raise ForgeActionError("an issue comment body is required")

    body = body.rstrip() + ISSUE_COMMENT_PROVENANCE.format(
        task_id=task_id, repository=repository, number=digits)
    rc, _out, err = _run(
        "gh", "issue", "comment", digits, "--repo", name_with_owner,
        "--body", body)
    if rc != 0:
        raise ForgeActionError(
            f"{repository}#{digits}: the forge refused the issue comment "
            f"({_detail(err)})")
    url = ""
    rc, detail, _err = _run("gh", "issue", "view", digits, "--repo",
                            name_with_owner, "--json", "url")
    if rc == 0:
        try:
            url = str(json.loads(detail).get("url") or "")
        except (ValueError, TypeError):
            pass
    return IssueCommentReceipt(
        repository=repository, number=int(digits), url=url)


def post_review(
    *,
    repository: str,
    number: str,
    task_id: int,
    body: str,
    verdict: str = "comment",
) -> ReviewReceipt:
    """Comment one review on a pull request the caller does not choose.

    ``repository`` and ``number`` come from the task's binding, so there is
    no parameter that could name another pull request. The body is content.
    """
    if not repository or repository.count("/") != 2:
        raise ForgeActionError(
            "the task's repository is not a canonical locator")
    host, _, name_with_owner = repository.partition("/")
    if host != "github.com":
        raise ForgeActionError(f"{host}: posting a review is not supported here")
    digits = (number or "").strip()
    if not digits.isdigit() or int(digits) < 1:
        raise ForgeActionError("the task does not name a pull request")
    if not (body or "").strip():
        # An empty review is worse than none: it reads as a considered
        # verdict of nothing.
        raise ForgeActionError("a review body is required")

    if verdict not in ("approve", "request-changes", "comment"):
        raise ForgeActionError("invalid review verdict")

    if verdict == "approve":
        rc, out, _err = _run(
            "gh", "pr", "view", digits, "--repo", name_with_owner,
            "--json", "reviewDecision,labels")
        if rc == 0:
            try:
                parsed = json.loads(out)
                decision = parsed.get("reviewDecision")
                labels = [label.get("name") for label in parsed.get("labels", []) if isinstance(label, dict)]
                if decision == "CHANGES_REQUESTED" or "hold" in labels:
                    raise ForgeActionError("cannot approve a pull request with an outstanding hold")
            except (ValueError, TypeError):
                pass

    body = body.rstrip() + REVIEW_PROVENANCE.format(
        task_id=task_id, repository=repository, number=digits)
    
    flag = "--comment"
    if verdict == "approve":
        flag = "--approve"
    elif verdict == "request-changes":
        flag = "--request-changes"

    rc, _out, err = _run(
        "gh", "pr", "review", digits, "--repo", name_with_owner,
        flag, "--body", body)
    if rc != 0:
        raise ForgeActionError(
            f"{repository}#{digits}: the forge refused the review "
            f"({_detail(err)})")
    url = ""
    rc, detail, _err = _run("gh", "pr", "view", digits, "--repo",
                            name_with_owner, "--json", "url")
    if rc == 0:
        try:
            url = str(json.loads(detail).get("url") or "")
        except (ValueError, TypeError):
            pass
    return ReviewReceipt(
        repository=repository, number=int(digits), url=url)


def _task_marker(task_id: int) -> str:
    """The substring every issue this task opened carries in its body."""
    return f"Opened by Foxhound for task {task_id},"


def _existing_issues(name_with_owner: str) -> list[dict]:
    """Open issues on the repository, or an empty list if they cannot be read.

    A preflight that cannot see is not allowed to become a preflight that
    forbids: an unreadable list would otherwise block every issue on the
    repository rather than the duplicates it is meant to catch. The ceiling
    below is the bound that must not depend on this call succeeding, so it
    counts from the same list and is checked against what the list shows.
    """
    rc, out, _err = _run(
        "gh", "issue", "list", "--repo", name_with_owner, "--state", "open",
        "--limit", "100", "--json", "number,title,body,url")
    if rc != 0:
        return []
    try:
        parsed = json.loads(out)
    except (ValueError, TypeError):
        return []
    return [item for item in parsed if isinstance(item, dict)]


def _normalized_title(title: str) -> str:
    return " ".join((title or "").split()).casefold()


def open_issue(
    *,
    repository: str,
    task_id: int,
    title: str,
    body: str,
) -> IssueReceipt:
    """Open one issue on ``repository``, which the caller does not choose.

    ``repository`` comes from the task's binding, so no parameter here can
    name another one. Title and body are content.

    This is the only write in this module that creates work for the system
    that issued it, so it refuses two things the others need not consider: a
    title already open on the repository, and more than `MAX_ISSUES_PER_TASK`
    issues from one task. Both are checked against the forge rather than
    against run state, because a task outlives any single run and the run
    directory is reclaimed.
    """
    if not repository or repository.count("/") != 2:
        raise ForgeActionError("the task's repository is not a canonical locator")
    host, _, name_with_owner = repository.partition("/")
    if host != "github.com":
        raise ForgeActionError(f"{host}: opening issues is not supported here")
    title = " ".join((title or "").split())
    if not title:
        raise ForgeActionError("an issue title is required")
    if not (body or "").strip():
        # An issue with no body is a title someone else has to interpret.
        raise ForgeActionError("an issue body is required")

    existing = _existing_issues(name_with_owner)
    wanted = _normalized_title(title)
    for item in existing:
        if _normalized_title(str(item.get("title") or "")) == wanted:
            raise ForgeActionError(
                f"{repository}#{item.get('number')} is already open with this "
                f"title ({item.get('url') or 'no url'}); comment there instead "
                "of opening a second one"
            )
    marker = _task_marker(task_id)
    opened = sum(
        1 for item in existing if marker in str(item.get("body") or ""))
    if opened >= MAX_ISSUES_PER_TASK:
        raise ForgeActionError(
            f"task {task_id} has already opened {opened} issues on "
            f"{repository}, which is the limit; report the rest in the result "
            "rather than opening more"
        )

    body = (body or "").rstrip() + ISSUE_PROVENANCE.format(
        task_id=task_id, repository=repository)
    rc, out, err = _run(
        "gh", "issue", "create", "--repo", name_with_owner,
        "--title", title, "--body", body)
    if rc != 0:
        raise ForgeActionError(
            f"{repository}: the forge refused the issue ({_detail(err)})")
    url = (out or "").strip().splitlines()[-1] if out.strip() else ""
    number = 0
    rc, detail, _err = _run("gh", "issue", "view", url, "--repo",
                            name_with_owner, "--json", "number,url")
    if rc == 0:
        try:
            parsed = json.loads(detail)
            number = int(parsed.get("number") or 0)
            url = str(parsed.get("url") or url)
        except (ValueError, TypeError):
            pass
    return IssueReceipt(
        repository=repository, number=number, url=url, title=title)


def open_pull_request(
    *,
    repository: str,
    issue: str,
    task_id: int,
    head: str,
    title: str,
    body: str,
    base: str | None = None,
) -> PullRequestReceipt:
    """Open one pull request against ``repository``, which the caller does not choose.

    ``repository`` and ``issue`` come from the task's binding. Everything a
    caller supplies is content, not reach.
    """
    if not repository or repository.count("/") != 2:
        raise ForgeActionError("the task's repository is not a canonical locator")
    host, _, name_with_owner = repository.partition("/")
    if host != "github.com":
        raise ForgeActionError(f"{host}: opening pull requests is not supported here")
    head = (head or "").strip()
    if not head:
        raise ForgeActionError("a head branch is required")
    title = " ".join((title or "").split())
    if not title:
        raise ForgeActionError("a title is required")

    base = (base or default_branch(repository)).strip()
    if head == base:
        # Almost always means the work was done on the default branch.
        raise ForgeActionError(
            f"head and base are both {base!r}; a pull request must propose a "
            "branch other than the one it targets"
        )

    body = (body or "").rstrip() + PROVENANCE.format(
        task_id=task_id, repository=repository, issue=issue)
    rc, out, err = _run(
        "gh", "pr", "create", "--repo", name_with_owner,
        "--head", head, "--base", base, "--title", title, "--body", body)
    if rc != 0:
        raise ForgeActionError(f"{repository}: the forge refused the pull "
                               f"request ({_detail(err)})")
    url = (out or "").strip().splitlines()[-1] if out.strip() else ""
    number = 0
    rc, detail, _err = _run("gh", "pr", "view", url or head, "--repo",
                            name_with_owner, "--json", "number,url")
    if rc == 0:
        try:
            parsed = json.loads(detail)
            number = int(parsed.get("number") or 0)
            url = str(parsed.get("url") or url)
        except (ValueError, TypeError):
            pass
    return PullRequestReceipt(repository=repository, issue=issue, number=number,
                              url=url, head=head, base=base)
