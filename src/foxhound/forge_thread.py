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

#: The worker prints this result into an agent conversation, so preserve the
#: useful review evidence but never let one thread consume the run's context.
_MAX_THREAD_CHARS = 30_000

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


def _thread_target(repository: str, number: str, *, label: str) -> tuple[str, str, int]:
    if not repository or repository.count("/") != 2:
        raise ForgeThreadError("the task's repository is not a canonical locator")
    host, _, name_with_owner = repository.partition("/")
    if host != "github.com":
        raise ForgeThreadError(f"{host}: reading {label} threads is not supported here")
    digits = (number or "").strip()
    if not digits.isdigit() or int(digits) < 1:
        raise ForgeThreadError(f"the task does not name a {label}")
    owner, separator, name = name_with_owner.partition("/")
    if not separator or not owner or not name:
        raise ForgeThreadError("the task's repository is not a canonical locator")
    return owner, name, int(digits)


def _read_graphql(*, owner: str, name: str, number: int, object_name: str, query: str) -> dict[str, Any]:
    rc, out, err = _run(
        "gh", "api", "graphql",
        "-f", f"query={query}",
        "-f", f"owner={owner}",
        "-f", f"name={name}",
        "-F", f"number={number}",
    )
    if rc != 0:
        raise ForgeThreadError(f"the forge did not return thread data ({_detail(err)})")
    try:
        payload = json.loads(out)
        thread = payload["data"]["repository"][object_name]
    except (KeyError, TypeError, ValueError):
        raise ForgeThreadError("the forge returned unparseable thread data") from None
    if not isinstance(thread, dict):
        raise ForgeThreadError("the forge did not find the task's thread")
    return thread


def _entries(nodes: object, *, review: bool = False) -> list[dict[str, Any]]:
    if not isinstance(nodes, list):
        return []
    result = []
    for node in nodes:
        if not isinstance(node, dict):
            continue
        author = node.get("author") or {}
        entry = {
            "author": author.get("login", "") if isinstance(author, dict) else "",
            "body": node.get("body", "") if isinstance(node.get("body", ""), str) else "",
            "created_at": node.get("createdAt", "") if isinstance(node.get("createdAt", ""), str) else "",
        }
        if review:
            entry["state"] = node.get("state", "") if isinstance(node.get("state", ""), str) else ""
        result.append(entry)
    return result


def _connection_entries(thread: dict[str, Any], name: str, *, review: bool = False) -> tuple[list[dict[str, Any]], int]:
    connection = thread.get(name) or {}
    if not isinstance(connection, dict):
        return [], 0
    entries = _entries(connection.get("nodes"), review=review)
    total = connection.get("totalCount", len(entries))
    return entries, total if isinstance(total, int) and total >= len(entries) else len(entries)


def _bound_entries(comments: list[dict[str, Any]], reviews: list[dict[str, Any]], *, comment_total: int, review_total: int) -> tuple[list[dict[str, Any]], list[dict[str, Any]], bool]:
    comments = comments[:_MAX_COMMENTS]
    reviews = reviews[:_MAX_COMMENTS]
    truncated = comment_total > len(comments) or review_total > len(reviews)

    def size() -> int:
        return len(json.dumps({"comments": comments, "reviews": reviews}, ensure_ascii=True))

    while size() > _MAX_THREAD_CHARS:
        candidates = [
            entry for entry in (*comments, *reviews)
            if entry["body"] and entry["body"] != "[truncated]"
        ]
        if candidates:
            entry = max(candidates, key=lambda value: len(value["body"]))
            excess = size() - _MAX_THREAD_CHARS
            keep = max(0, len(entry["body"]) - excess - len("[truncated]"))
            entry["body"] = entry["body"][:keep] + "[truncated]"
        elif comments:
            comments.pop()
        elif reviews:
            reviews.pop()
        else:
            break
        truncated = True
    return comments, reviews, truncated


_ISSUE_QUERY = """
query($owner: String!, $name: String!, $number: Int!) {
  repository(owner: $owner, name: $name) {
    issue(number: $number) {
      number url state
      comments(first: 100) { totalCount nodes { author { login } body createdAt } }
    }
  }
}
"""

_PULL_REQUEST_QUERY = """
query($owner: String!, $name: String!, $number: Int!) {
  repository(owner: $owner, name: $name) {
    pullRequest(number: $number) {
      number url state
      comments(first: 100) { totalCount nodes { author { login } body createdAt } }
      reviews(first: 100) { totalCount nodes { author { login } state body createdAt } }
    }
  }
}
"""


def read_issue_thread(
    *,
    repository: str,
    number: str,
) -> ThreadResult:
    """Read comments on an issue the task owns.

    The repository and number come from the task's binding, not from
    caller arguments. Returns a bounded set of comments and metadata.
    """
    owner, name, issue_number = _thread_target(repository, number, label="issue")
    data = _read_graphql(
        owner=owner, name=name, number=issue_number,
        object_name="issue", query=_ISSUE_QUERY,
    )
    comments, comment_total = _connection_entries(data, "comments")
    comments, _reviews, truncated = _bound_entries(
        comments, [], comment_total=comment_total, review_total=0,
    )

    return ThreadResult(
        repository=repository,
        kind="issue",
        number=issue_number,
        comments=comments,
        reviews=[],
        truncated=truncated,
        url=data.get("url", ""),
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
    owner, name, pr_number = _thread_target(repository, number, label="pull request")
    data = _read_graphql(
        owner=owner, name=name, number=pr_number,
        object_name="pullRequest", query=_PULL_REQUEST_QUERY,
    )
    comments, comment_total = _connection_entries(data, "comments")
    reviews, review_total = _connection_entries(data, "reviews", review=True)
    comments, reviews, truncated = _bound_entries(
        comments, reviews,
        comment_total=comment_total, review_total=review_total,
    )

    return ThreadResult(
        repository=repository,
        kind="pull-request",
        number=pr_number,
        comments=comments,
        reviews=reviews,
        truncated=truncated,
        url=data.get("url", ""),
    )
