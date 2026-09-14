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

_PUSH_TIMEOUT_S = 120
_CLONE_TIMEOUT_S = 600


class ForgeActionError(RuntimeError):
    """The action is refused, or the forge would not perform it."""


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
    return path, branch, base


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


def post_review(
    *,
    repository: str,
    number: str,
    task_id: int,
    body: str,
) -> ReviewReceipt:
    """Comment one review on a pull request the caller does not choose.

    ``repository`` and ``number`` come from the task's binding, so there is
    no parameter that could name another pull request. The body is content.

    Posted as a comment rather than an approval or a rejection. Approving a
    pull request is a statement about whether it should merge, and that is
    the reader's to make; an agent's job here is to say what it found.
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

    body = body.rstrip() + REVIEW_PROVENANCE.format(
        task_id=task_id, repository=repository, number=digits)
    rc, _out, err = _run(
        "gh", "pr", "comment", digits, "--repo", name_with_owner,
        "--body", body)
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
