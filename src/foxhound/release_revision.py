"""The revision of the release checkout a process is importing.

A long-running service that has outlived several deploys is otherwise
indistinguishable from a freshly started one, and that difference has kept a
stale process serving retired code while every unit reported active.  Reporting
the revision at start-up makes it visible in the same place an operator is
already looking.

Nothing here is required for the service to run: an unknown revision is
reported as unknown rather than raised, because refusing to start over a
missing label would be worse than the label being missing.
"""

from __future__ import annotations

import subprocess
from pathlib import Path


UNKNOWN = "unknown"


def describe(module_file: str | Path) -> str:
    """One short, content-free description of the imported checkout.

    The path is deployment state, so it never appears in the result: only the
    abbreviated commit, whether the checkout is detached, and whether it has
    uncommitted edits -- which on a release checkout means someone has been
    working in it and the commit no longer names what is running.
    """
    root = Path(module_file).resolve().parent.parent.parent
    revision = _git(root, "rev-parse", "--short=12", "HEAD")
    if not revision:
        # A promoted release is a copy without git metadata, and its revision
        # is the directory it was installed into.  Reporting `unknown` there
        # would withhold the answer on exactly the layout this line exists to
        # describe.
        named = _named_release(root)
        return f"{named} (release)" if named else UNKNOWN
    branch = _git(root, "rev-parse", "--abbrev-ref", "HEAD")
    where = "detached" if branch == "HEAD" else (branch or UNKNOWN)
    status = _git(root, "status", "--porcelain")
    state = "modified" if status else "clean"
    return f"{revision} ({where}) {state}"


def _named_release(root: Path) -> str:
    """The revision a release directory is named for, if it is named for one."""
    for parent in (root, *root.parents):
        if parent.parent.name == "releases" and _looks_like_revision(parent.name):
            return parent.name
    return ""


def _looks_like_revision(name: str) -> bool:
    return (7 <= len(name) <= 40
            and all(character in "0123456789abcdef" for character in name))


def _git(root: Path, *arguments: str) -> str:
    try:
        completed = subprocess.run(
            ("git", "-C", str(root), *arguments),
            capture_output=True, text=True, timeout=5, check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return ""
    if completed.returncode != 0:
        return ""
    return completed.stdout.strip()
