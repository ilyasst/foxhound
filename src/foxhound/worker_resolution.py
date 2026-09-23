"""Locate the task worker that belongs to the running release, and prove it.

The runner never spawns the worker. It spawns an agent, and the agent runs the
command named in its own prompt, in its own shell. So "which worker" is not
decided by the runner's environment at all -- it is decided by whatever that
shell's ``PATH`` resolves. On a deployed host ``~/.profile`` and ``~/.bashrc``
prepend ``~/.local/bin``, which is exactly where a development
``pip install -e .`` leaves a same-named console script. The release's own
worker loses that race every time, and a ``PATH=`` drop-in on the runner unit
cannot win it back, because the agent's shell is not the runner's.

Nothing reports the discrepancy. The runner logs the release revision it is
running; the worker it never spawns logs nothing comparable. A runner on the
release and a worker on a working tree look exactly like a healthy deployment
-- until a release changes ``RUN_STATE_SCHEMA_VERSION``. Then the runner writes
run state at the new version, the worker refuses a schema it does not know and
exits 78, and *every* claimed run dies at the agent's first tool call with no
result recorded. See issue #431.

Two things close that, and they are complementary:

``resolve_worker_command``
    Names the worker shipped beside the running interpreter, absolutely, so
    the agent has nothing to resolve and ``PATH`` never gets a vote.

``worker_report`` / ``WorkerMismatch``
    Asks the worker that *would* run what run-state schema it speaks, and lets
    the caller refuse before claiming. Resolution alone is silent when it
    fails; this is the part that makes a future divergence loud instead of
    fatal, which matters because the next way these two can drift will not be
    a way anyone has thought of yet.
"""

from __future__ import annotations

import importlib
import importlib.util
import json
import os
import re
import subprocess
import sys
from pathlib import Path


#: A bare console-script name, resolved through ``PATH``. Kept because every
#: existing deployment's configuration and every stored run-state document
#: names the worker this way.
COMMAND_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")

#: An absolute path to a worker. Deliberately narrower than the filesystem
#: allows: this value is templated into an agent's prompt and used as argv[0],
#: so no whitespace and no shell metacharacters, whatever a path could legally
#: contain.
COMMAND_PATH_RE = re.compile(r"^/[A-Za-z0-9._/-]{1,4095}$")

#: Seconds to wait for a worker to describe itself. It loads one module and
#: prints a dict; a worker that cannot do that promptly is already a fault.
REPORT_TIMEOUT_SECONDS = 30

REPORT_SCHEMA = "foxhound.worker-report"
REPORT_SCHEMA_VERSION = 1

#: A release directory name looks like a hex revision (7-40 characters).
#: Matches the pattern used in ``release_revision._looks_like_revision``.
_RELEASE_REVISION_RE = re.compile(r"^[0-9a-f]{7,40}$")


class WorkerMismatch(RuntimeError):
    """The worker that would run is not compatible with this runner."""


class PackageOutsideRelease(RuntimeError):
    """The imported foxhound package is not inside the selected release."""


def is_worker_command(value: object) -> bool:
    """Whether ``value`` may name the worker."""
    if not isinstance(value, str):
        return False
    if value.startswith("/"):
        return bool(COMMAND_PATH_RE.fullmatch(value))
    return bool(COMMAND_NAME_RE.fullmatch(value))


def resolve_worker_command(
    command: str, *, interpreter: str | os.PathLike[str] | None = None
) -> str:
    """Return ``command`` as an absolute path beside the running interpreter.

    An absolute ``command`` is returned unchanged: the deployment has already
    said which worker it means, and second-guessing that would take away the
    one escape hatch a non-standard layout has.

    A bare name is looked for next to ``sys.executable``. That directory is the
    release's own ``venv/bin`` when a deployed unit is running, and a virtual
    environment's ``bin`` when a developer is; in both cases it is the worker
    that matches the code making the call. The interpreter path is used as
    given rather than resolved, so a deployment whose ``current`` symlink moves
    keeps following it.

    Falling back to the bare name when no such file exists is deliberate. Some
    deployments legitimately have no console script beside the interpreter, and
    turning that into a hard failure here would break them for the sake of a
    check ``worker_report`` already makes properly.
    """
    if not is_worker_command(command):
        raise ValueError("execution worker command is invalid")
    if command.startswith("/"):
        return command
    candidate = Path(interpreter or sys.executable).parent / command
    if not candidate.is_file() or not os.access(candidate, os.X_OK):
        return command
    resolved = str(candidate)
    if not is_worker_command(resolved):
        # A path we cannot safely put in a prompt is not an improvement on the
        # bare name, and the handshake still covers what PATH then finds.
        return command
    return resolved


def worker_report(
    command: str,
    *,
    run: object = None,
    timeout: float = REPORT_TIMEOUT_SECONDS,
) -> dict[str, object]:
    """Ask ``command`` to describe itself.

    Raises :class:`WorkerMismatch` if it cannot be asked or does not answer in
    the documented shape -- an unaskable worker is not a worker this runner
    should hand a claim to.
    """
    if not is_worker_command(command):
        raise ValueError("execution worker command is invalid")
    runner = run if run is not None else subprocess.run
    try:
        completed = runner(  # type: ignore[operator]
            [command, "report"],
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise WorkerMismatch("execution worker could not be run") from exc
    if completed.returncode != 0:
        raise WorkerMismatch("execution worker did not report")
    try:
        document = json.loads(completed.stdout)
    except (TypeError, ValueError) as exc:
        raise WorkerMismatch("execution worker report is invalid") from exc
    if (
        not isinstance(document, dict)
        or document.get("schema") != REPORT_SCHEMA
        or document.get("schema_version") != REPORT_SCHEMA_VERSION
        or not isinstance(document.get("run_state_schema_version"), int)
        or isinstance(document.get("run_state_schema_version"), bool)
    ):
        raise WorkerMismatch("execution worker report is invalid")
    return document


def verify_worker(
    command: str,
    *,
    run_state_schema_version: int,
    run: object = None,
    timeout: float = REPORT_TIMEOUT_SECONDS,
) -> dict[str, object]:
    """Return ``command``'s report, or raise if it cannot read this run state.

    The refusal is on the run-state schema rather than on the revision. That is
    the contract that actually has to hold -- it is what the runner writes and
    the worker parses -- and it is the one whose breach produced the outage.
    Revision is carried in the report for diagnosis, and deliberately does not
    gate: a worker built from the same commit as the runner but installed a
    different way is not a fault, and refusing it would make every development
    checkout unrunnable to no purpose.
    """
    document = worker_report(command, run=run, timeout=timeout)
    reported = document["run_state_schema_version"]
    if reported != run_state_schema_version:
        raise WorkerMismatch(
            "execution worker speaks run-state schema "
            f"{reported}, runner writes {run_state_schema_version}"
        )
    return document


def _is_release_deployment() -> bool:
    """Return True when the interpreter lives inside a release directory.

    A release deployment has its interpreter inside a virtualenv that sits
    inside a release directory named for a hex revision (e.g.
    ``releases/fa5560ec8869/venv/bin/python3``).

    A development checkout — whether run directly or through a venv inside
    the checkout — will not have a ``releases`` parent with a hex-revision
    name, so this returns False and the integrity check is skipped.
    """
    interp = Path(sys.executable).resolve()
    for parent in interp.parents:
        if parent.parent.name == "releases" and _RELEASE_REVISION_RE.fullmatch(
            parent.name
        ):
            return True
    return False


def _release_root() -> Path | None:
    """Return the release directory, or None if this is not a release deployment.

    The release root is the directory named for the revision, directly inside
    a ``releases`` parent. It is the interpreter's parent's parent when the
    interpreter sits in the standard ``<release>/venv/bin/python3`` layout.
    """
    interp = Path(sys.executable).resolve()
    for parent in interp.parents:
        if parent.parent.name == "releases" and _RELEASE_REVISION_RE.fullmatch(
            parent.name
        ):
            return parent
    return None


def _running_package_root(package: str = "foxhound") -> Path | None:
    """Return the filesystem root that contains the imported package.

    For a normal installed package this is the directory that contains
    ``<package>/`` (e.g. ``site-packages``).  For an editable install it is
    the working tree root.  Returns None if the package cannot be found.
    """
    spec = importlib.util.find_spec(package)
    if spec is None:
        return None
    origin = spec.origin
    if origin is None:
        # Namespace package or similar; cannot determine location.
        return None
    return Path(origin).resolve().parent.parent


def verify_release_integrity(
    *,
    package: str = "foxhound",
    release_root: Path | None = None,
    is_release: bool | None = None,
) -> None:
    """Refuse to claim when the imported package is not inside the release.

    An editable install pointed at a development checkout replaces the
    release's own package while every path and symlink still reads as
    correct.  This check resolves where the ``foxhound`` package actually
    lives on disk and refuses when it sits outside the selected release.

    The check only applies to release deployments.  A development checkout
    run directly, or a test suite run from a working tree, must not be
    refused — the check exists to protect a release deployment, not to
    block development.  ``is_release`` controls this: when ``True`` (or
    unset, in which case the interpreter path is inspected) the check runs;
    when explicitly ``False`` it is skipped.

    Raises :class:`PackageOutsideRelease` with a content-free message when
    the package resolves outside the release root.
    """
    # Determine whether this is a release deployment.
    if is_release is None:
        is_release = _is_release_deployment()
    if not is_release:
        # Development checkout, test suite, or non-release invocation: skip.
        return

    # Determine the release root.
    if release_root is None:
        release_root = _release_root()
    if release_root is None:
        # Could not identify the release root; skip rather than refusing a
        # deployment that might be valid under a non-standard layout.
        return

    # Resolve where the package actually lives.
    package_root = _running_package_root(package)
    if package_root is None:
        # Cannot determine the package's location.  This is a fault, but not
        # one we can diagnose content-free; skip so the runner still starts.
        return

    # The package must be inside the release root.
    try:
        package_root.relative_to(release_root)
    except ValueError:
        raise PackageOutsideRelease(
            "running package is not inside the selected release"
        )
