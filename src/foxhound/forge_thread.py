"""Read the forge thread for a task's own issue or pull request.

WHY THIS EXISTS

An agent can open a pull request, comment on an issue, and submit a review,
but cannot read any of it back. When a reviewer rejects a pull request and
explains why, the agent that retries the task has no way to learn what the
review said. It re-reads the issue and reproduces the same change.

This module provides a bounded, read-only operation that returns the comments
and review state on the task's own thread. The target is read from the task's
origin binding, never from an argument, so the agent cannot redirect it.

SCOPE

* One issue or one pull request that the task itself owns.
* Returns comments and review state.
* Output is bounded: long threads are truncated rather than returned whole.
* Read-only: performs no write, requires no approval gate.

WHAT IS DELIBERATELY REFUSED

* A task with no origin. There is nothing to read and the target must never
  be inferred from task text.
* A forge this module does not speak. Silence is not consent.
* Arbitrary repository or issue. The origin binding is the sole authority.
"""

from __future__ import annotations

import json
import subprocess
from dataclasses import dataclass
from typing import Any

#: Maximum characters in the raw thread payload before truncation.
#: A review that runs this long is uncommon; a 200-line thread with
#: full content fits easily, and a 500-line thread gets trimmed.
_MAX_THREAD_CHARS = 40_000

#: Maximum number of comments returned before truncation.
_MAX_COMMENTS = 100

_PING_TIMEOUT = 30


class ForgeThreadError(RuntimeError):
    """The thread read is refused or the forge would not serve it."""


@dataclass(frozen=True)
class ThreadResult:
    """Bounded read of one forge thread."""

    repository: str
    #: The kind of thread: "issue" or "pull-request".
    kind: str
    #: Issue or PR number.
    number: int
    #: List of comment objects, each with author, body, and created_at.
    comments: list[dict[str, Any]]
    #: Review objects (only for pull requests), each with author, state, body.
    reviews: list[dict[str, Any]]
    #: True if the output was truncated because it exceeded limits.
    truncated: bool
    #: URL of the thread.
    url: str


def _run(*args: str, timeout: int = _PING_TIMEOUT) -> tuple[int, str, str]:
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


def read_issue_thread(
    *,
    repository: str,
    number: str,
) -> ThreadResult:
    """Read comments on an issue the task owns.

    The repository and number come from the task's binding, not from
    caller arguments. Returns a bounded set of comments and metadata.
    """
    if not repository or repository.count("/") != 2:
        raise ForgeThreadError(
            "the task's repository is not a canonical locator")
    host, _, name_with_owner = repository.partition("/")
    if host != "github.com":
        raise ForgeThreadError(
            f"{host}: reading issue threads is not supported here")
    digits = (number or "").strip()
    if not digits.isdigit() or int(digits) < 1:
        raise ForgeThreadError("the task does not name an issue")

    # Fetch issue metadata and comments via the GitHub API
    fields = (
        "number,url,state,comments{nodes{author{login},body,createdAt},"
        "pageInfo{hasNextPage,endCursor}}"
    )
    rc, out, err = _run(
        "gh", "api", "--hostname", host,
        f"repos/{name_with_owner}/issues/{digits}",
        "-f", f"fields={fields}")
    if rc != 0:
        raise ForgeThreadError(
            f"{repository}#{digits}: the forge did not return thread data "
            f"({_detail(err)})")

    try:
        data = json.loads(out)
    except (ValueError, TypeError):
        raise ForgeThreadError(
            f"{repository}#{digits}: the forge returned unparseable data")

    url = data.get("url", "")
    issue_number = data.get("number", 0)

    # Extract comments
    comments = []
    comments_data = data.get("comments", {}) or {}
    nodes = comments_data.get("nodes", []) or []

    for node in nodes:
        author_info = node.get("author") or {}
        author = author_info.get("login", "")
        body = node.get("body", "") or ""
        created_at = node.get("createdAt", "")
        comments.append({
            "author": author,
            "body": body,
            "created_at": created_at,
        })

    # Truncate if needed
    truncated = False
    if len(comments) > _MAX_COMMENTS:
        comments = comments[:_MAX_COMMENTS]
        truncated = True

    return ThreadResult(
        repository=repository,
        kind="issue",
        number=issue_number,
        comments=comments,
        reviews=[],
        truncated=truncated,
        url=url,
    )


def read_pull_request_thread(
    *,
    repository: str,
    number: str,
) -> ThreadResult:
    """Read comments and reviews on a pull request the task owns.

    The repository and number come from the task's binding. Returns
    a bounded set of comments, reviews, and metadata.
    """
    if not repository or repository.count("/") != 2:
        raise ForgeThreadError(
            "the task's repository is not a canonical locator")
    host, _, name_with_owner = repository.partition("/")
    if host != "github.com":
        raise ForgeThreadError(
            f"{host}: reading pull request threads is not supported here")
    digits = (number or "").strip()
    if not digits.isdigit() or int(digits) < 1:
        raise ForgeThreadError("the task does not name a pull request")

    # Fetch PR metadata
    pr_fields = "number,url,state,reviews{nodes{author{login},state,body,createdAt}},comments{nodes{author{login},body,createdAt}}"
    rc, out, err = _run(
        "gh", "api", "--hostname", host,
        f"repos/{name_with_owner}/pulls/{digits}",
        "-f", f"fields={pr_fields}")
    if rc != 0:
        raise ForgeThreadError(
            f"{repository}#{digits}: the forge did not return thread data "
            f"({_detail(err)})")

    try:
        data = json.loads(out)
    except (ValueError, TypeError):
        raise ForgeThreadError(
            f"{repository}#{digits}: the forge returned unparseable data")

    url = data.get("url", "")
    pr_number = data.get("number", 0)

    # Extract reviews
    reviews = []
    reviews_data = data.get("reviews", {}) or {}
    review_nodes = reviews_data.get("nodes", []) or []
    for node in review_nodes:
        author_info = node.get("author") or {}
        author = author_info.get("login", "")
        body = node.get("body", "") or ""
        state = node.get("state", "")
        created_at = node.get("createdAt", "")
        reviews.append({
            "author": author,
            "state": state,
            "body": body,
            "created_at": created_at,
        })

    # Extract comments (PR-level comments, not review comments)
    comments = []
    comments_data = data.get("comments", {}) or {}
    comment_nodes = comments_data.get("nodes", []) or []
    for node in comment_nodes:
        author_info = node.get("author") or {}
        author = author_info.get("login", "")
        body = node.get("body", "") or ""
        created_at = node.get("createdAt", "")
        comments.append({
            "author": author,
            "body": body,
            "created_at": created_at,
        })

    # Check total size and truncate if needed
    truncated = False
    all_text = json.dumps(comments + reviews)
    if len(all_text) > _MAX_THREAD_CHARS:
        # Truncate comments first, then reviews
        while len(all_text) > _MAX_THREAD_CHARS * 0.6 and comments:
            comments.pop()
        while len(all_text) > _MAX_THREAD_CHARS * 0.6 and reviews:
            reviews.pop()
        truncated = True

    if len(comments) > _MAX_COMMENTS:
        comments = comments[:_MAX_COMMENTS]
        truncated = True
    if len(reviews) > _MAX_COMMENTS:
        reviews = reviews[:_MAX_COMMENTS]
        truncated = True

    return ThreadResult(
        repository=repository,
        kind="pull-request",
        number=pr_number,
        comments=comments,
        reviews=reviews,
        truncated=truncated,
        url=url,
    )
