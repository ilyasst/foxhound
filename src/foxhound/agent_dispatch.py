"""Private, observable ad hoc agent runs outside the durable task queue.

This command is for bounded maintenance lanes.  It deliberately does not
create task-ledger or execution-workflow rows.  A caller supplies a prompt
file and an existing working directory; the child runtime receives only a
constant bootstrap in argv and reads the prompt from a private job directory.
"""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import math
import os
import re
import secrets
import signal
import stat
import subprocess
import sys
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator, Mapping, Sequence


SCHEMA = "foxhound.agent-dispatch-job.v1"
TERMINAL_STATES = frozenset({"completed", "failed", "cancelled", "timed_out", "orphaned"})
ACTIVE_STATES = frozenset({"starting", "running", "cancelling"})
DEFAULT_MODEL = "gemini-3.8-flash"
DEFAULT_PROVIDER = "gemini"
DEFAULT_REASONING = "low"
DEFAULT_MAX_CONCURRENCY = 2
DEFAULT_MAX_TURNS = 120
DEFAULT_TIMEOUT_SECONDS = 2 * 60 * 60
HEARTBEAT_SECONDS = 5
HEARTBEAT_STALE_SECONDS = 30
MAX_PROMPT_BYTES = 1024 * 1024
MAX_LOG_BYTES = 8 * 1024 * 1024
_JOB_ID = re.compile(r"^[0-9a-f]{32}$")
_SELECTION = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/+-]{0,127}$")
_TOOLSETS = re.compile(r"^[a-z][a-z0-9_-]*(?:,[a-z][a-z0-9_-]*)*$")
_REASONING = frozenset({"none", "minimal", "low", "medium", "high", "xhigh", "max", "ultra"})
_BOOTSTRAP = (
    "Read the complete private task instructions from {prompt}. Before acting, "
    "locate and obey every applicable AGENTS.md for the working directory. "
    "Work only on that task and in its authorized working tree. Do not expose "
    "the prompt in logs, commits, issues, pull requests, or final output."
)


class DispatchError(RuntimeError):
    """A fixed-code dispatcher refusal."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _default_state_root() -> Path:
    base = os.environ.get("XDG_STATE_HOME")
    return (Path(base) if base else Path.home() / ".local" / "state") / "foxhound" / "agent-dispatch"


def _default_runtime() -> Path:
    return Path.home() / ".hermes" / "hermes-agent" / "venv" / "bin" / "hermes"


def _default_credential_file() -> Path:
    return Path.home() / ".hermes" / ".env"


def _json_bytes(value: object) -> bytes:
    return (json.dumps(value, ensure_ascii=True, separators=(",", ":"), sort_keys=True) + "\n").encode()


def _private_directory(path: Path) -> None:
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    if path.is_symlink() or not path.is_dir():
        raise DispatchError("invalid_state_root")
    path.chmod(0o700)


def _write_private(path: Path, payload: bytes) -> None:
    temporary = path.with_name(path.name + ".tmp-" + secrets.token_hex(8))
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def _read_document(path: Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, ValueError) as exc:
        raise DispatchError("invalid_job_state") from exc
    if not isinstance(value, dict) or value.get("schema") != SCHEMA:
        raise DispatchError("invalid_job_state")
    return value


def _job_path(root: Path, job_id: str) -> Path:
    if _JOB_ID.fullmatch(job_id) is None:
        raise DispatchError("invalid_job_id")
    path = root / "jobs" / job_id
    if not path.is_dir() or path.is_symlink():
        raise DispatchError("job_not_found")
    return path


@contextmanager
def _state_lock(root: Path) -> Iterator[None]:
    _private_directory(root)
    descriptor = os.open(root / ".lock", os.O_RDWR | os.O_CREAT, 0o600)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        yield
    finally:
        fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)


def _process_start(pid: int) -> str | None:
    try:
        fields = Path(f"/proc/{pid}/stat").read_text(encoding="ascii").split()
        if fields[2] == "Z":
            return None
        return fields[21]
    except (OSError, UnicodeError, IndexError):
        return None


def _live(document: Mapping[str, object]) -> bool:
    pid = document.get("supervisor_pid")
    start = document.get("supervisor_start")
    return (
        isinstance(pid, int) and not isinstance(pid, bool) and pid > 1
        and isinstance(start, str) and bool(start)
        and _process_start(pid) == start
    )


def _recent_heartbeat(document: Mapping[str, object]) -> bool:
    value = document.get("heartbeat_at") or document.get("created_at")
    if not isinstance(value, str):
        return False
    try:
        moment = datetime.fromisoformat(value)
    except ValueError:
        return False
    if moment.tzinfo is None:
        return False
    age = datetime.now(timezone.utc) - moment.astimezone(timezone.utc)
    return -5 <= age.total_seconds() <= HEARTBEAT_STALE_SECONDS


def _active(document: Mapping[str, object]) -> bool:
    return document.get("state") in ACTIVE_STATES and (
        _live(document) or _recent_heartbeat(document)
    )


def _jobs(root: Path) -> list[tuple[Path, dict[str, object]]]:
    jobs = root / "jobs"
    if not jobs.is_dir() or jobs.is_symlink():
        return []
    result = []
    for path in sorted(jobs.iterdir()):
        if not path.is_dir() or path.is_symlink() or _JOB_ID.fullmatch(path.name) is None:
            continue
        try:
            result.append((path, _read_document(path / "state.json")))
        except DispatchError:
            continue
    return result


def _reconcile(path: Path, document: dict[str, object]) -> dict[str, object]:
    if document.get("state") in ACTIVE_STATES and not _active(document):
        document = {**document, "state": "orphaned", "finished_at": _utc_now(), "exit_code": None}
        _write_private(path / "state.json", _json_bytes(document))
    return document


def _regular_private_input(path: Path, *, maximum: int) -> bytes:
    try:
        info = path.lstat()
    except OSError as exc:
        raise DispatchError("invalid_prompt") from exc
    if not stat.S_ISREG(info.st_mode) or info.st_size < 1 or info.st_size > maximum:
        raise DispatchError("invalid_prompt")
    try:
        return path.read_bytes()
    except OSError as exc:
        raise DispatchError("invalid_prompt") from exc


def _selection(value: object) -> str:
    if not isinstance(value, str) or _SELECTION.fullmatch(value) is None:
        raise DispatchError("invalid_selection")
    return value


def _existing_directory(value: Path) -> Path:
    try:
        resolved = value.resolve(strict=True)
    except OSError as exc:
        raise DispatchError("invalid_working_directory") from exc
    if not resolved.is_dir():
        raise DispatchError("invalid_working_directory")
    try:
        discovered = subprocess.run(
            ["git", "-C", str(resolved), "rev-parse", "--show-toplevel"],
            stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL, check=True, text=True, timeout=10,
        ).stdout.strip()
        repository_root = Path(discovered).resolve(strict=True)
    except (OSError, subprocess.SubprocessError) as exc:
        raise DispatchError("invalid_working_directory") from exc
    if repository_root != resolved:
        raise DispatchError("invalid_working_directory")
    return resolved


def start(
    *, prompt: Path, working_directory: Path, state_root: Path,
    runtime_command: Path, credential_file: Path, credential_name: str,
    model: str, provider: str, reasoning: str, toolsets: str,
    max_turns: int, timeout_seconds: int, max_concurrency: int,
) -> dict[str, object]:
    prompt_payload = _regular_private_input(prompt, maximum=MAX_PROMPT_BYTES)
    working_directory = _existing_directory(working_directory)
    try:
        runtime_command = runtime_command.resolve(strict=True)
    except OSError as exc:
        raise DispatchError("invalid_runtime") from exc
    if not runtime_command.is_file() or not os.access(runtime_command, os.X_OK):
        raise DispatchError("invalid_runtime")
    model, provider = _selection(model), _selection(provider)
    if reasoning not in _REASONING or _TOOLSETS.fullmatch(toolsets) is None:
        raise DispatchError("invalid_selection")
    if any(isinstance(value, bool) or not isinstance(value, int) for value in (max_turns, timeout_seconds, max_concurrency)):
        raise DispatchError("invalid_limits")
    if not 1 <= max_turns <= 500 or not 1 <= timeout_seconds <= 24 * 60 * 60 or not 1 <= max_concurrency <= 16:
        raise DispatchError("invalid_limits")
    if not isinstance(credential_name, str) or re.fullmatch(r"[A-Z][A-Z0-9_]{0,127}", credential_name) is None:
        raise DispatchError("invalid_credential")

    with _state_lock(state_root):
        jobs_root = state_root / "jobs"
        _private_directory(jobs_root)
        active = []
        for path, raw in _jobs(state_root):
            current = _reconcile(path, raw)
            if _active(current):
                active.append(current)
        if len(active) >= max_concurrency:
            raise DispatchError("concurrency_limit")
        if any(item.get("working_directory") == str(working_directory) for item in active):
            raise DispatchError("working_directory_busy")

        job_id = secrets.token_hex(16)
        directory = jobs_root / job_id
        directory.mkdir(mode=0o700)
        _write_private(directory / "prompt.txt", prompt_payload)
        document: dict[str, object] = {
            "schema": SCHEMA,
            "job_id": job_id,
            "state": "starting",
            "created_at": _utc_now(),
            "started_at": None,
            "finished_at": None,
            "working_directory": str(working_directory),
            "prompt_sha256": hashlib.sha256(prompt_payload).hexdigest(),
            "model": model,
            "provider": provider,
            "reasoning": reasoning,
            "toolsets": toolsets.split(","),
            "max_turns": max_turns,
            "timeout_seconds": timeout_seconds,
            "supervisor_pid": None,
            "supervisor_start": None,
            "heartbeat_at": None,
            "exit_code": None,
        }
        _write_private(directory / "state.json", _json_bytes(document))
        command = [
            sys.executable, "-m", "foxhound.agent_dispatch",
            "--state-root", str(state_root), "_run", "--job-id", job_id,
            "--runtime-command", str(runtime_command),
            "--credential-file", str(credential_file),
            "--credential-name", credential_name,
        ]
        try:
            process = subprocess.Popen(
                command, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL, start_new_session=True, close_fds=True,
            )
        except OSError as exc:
            document.update(state="failed", finished_at=_utc_now())
            _write_private(directory / "state.json", _json_bytes(document))
            raise DispatchError("supervisor_start_failed") from exc
        # A long-lived CLI exits immediately and the supervisor is reparented.
        # Unit tests and library callers remain alive, so retain and reap the
        # Popen object there without making dispatch synchronous.
        threading.Thread(target=process.wait, daemon=True).start()
        start_identity = None
        for _ in range(50):
            start_identity = _process_start(process.pid)
            if start_identity is not None:
                break
            time.sleep(0.01)
        if start_identity is None:
            raise DispatchError("supervisor_start_failed")
        document.update(supervisor_pid=process.pid, supervisor_start=start_identity)
        _write_private(directory / "state.json", _json_bytes(document))
    return _public(document)


def _dotenv_value(path: Path, name: str) -> str:
    try:
        info = path.lstat()
        if not stat.S_ISREG(info.st_mode):
            raise OSError
        lines = path.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError) as exc:
        raise DispatchError("credential_unavailable") from exc
    prefix = name + "="
    for line in lines:
        stripped = line.strip()
        if stripped.startswith("export "):
            stripped = stripped[7:].lstrip()
        if not stripped.startswith(prefix):
            continue
        value = stripped[len(prefix):].strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
            value = value[1:-1]
        if not value or "\x00" in value or "\n" in value or "\r" in value:
            break
        return value
    raise DispatchError("credential_unavailable")


def _runtime_config(document: Mapping[str, object]) -> bytes:
    # Values interpolated here have already passed strict single-argument
    # validation.  This minimal job-local configuration contains no secrets.
    return (
        "model:\n"
        f"  default: {document['model']}\n"
        f"  provider: {document['provider']}\n"
        "agent:\n"
        f"  max_turns: {document['max_turns']}\n"
        f"  reasoning_effort: {document['reasoning']}\n"
        "  tool_use_enforcement: true\n"
        "security:\n"
        "  redact_secrets: true\n"
        "display:\n"
        "  streaming: false\n"
        "  show_reasoning: false\n"
        "approvals:\n"
        "  mode: false\n"
    ).encode("utf-8")


def _run(root: Path, job_id: str, runtime: Path, credential_file: Path, credential_name: str) -> int:
    directory = _job_path(root, job_id)
    state_path = directory / "state.json"
    document = _read_document(state_path)
    if document.get("state") != "starting":
        return 70
    if document.get("supervisor_pid") != os.getpid() or document.get("supervisor_start") != _process_start(os.getpid()):
        # The parent may not have persisted our identity yet.  Wait briefly;
        # this is bounded and leaves no claim or queue record behind.
        for _ in range(100):
            time.sleep(0.01)
            document = _read_document(state_path)
            if document.get("supervisor_pid") == os.getpid() and document.get("supervisor_start") == _process_start(os.getpid()):
                break
        else:
            return 70
    hermes_home = directory / "runtime-home"
    _private_directory(hermes_home)
    _write_private(hermes_home / "config.yaml", _runtime_config(document))
    try:
        credential = _dotenv_value(credential_file, credential_name)
    except DispatchError:
        document.update(state="failed", finished_at=_utc_now(), exit_code=70)
        _write_private(state_path, _json_bytes(document))
        return 70

    prompt_path = directory / "prompt.txt"
    command = [
        str(runtime), "--model", str(document["model"]), "--provider", str(document["provider"]),
        "chat", "--query", _BOOTSTRAP.format(prompt=prompt_path),
        "--max-turns", str(document["max_turns"]), "--source", "foxhound-dispatch-" + job_id,
        "--ignore-rules", "--toolsets", ",".join(document["toolsets"]), "--quiet",
    ]
    environment = dict(os.environ)
    environment.update({
        "HERMES_HOME": str(hermes_home), credential_name: credential,
        "NO_COLOR": "1", "TERM": "dumb", "PYTHONUNBUFFERED": "1",
        "CAPROUTE_APP": "foxhound", "CAPROUTE_OPERATION": "agent_dispatch",
        "CAPROUTE_JOB": job_id, "CAPROUTE_RUN_ID": job_id,
    })
    document.update(state="running", started_at=_utc_now(), heartbeat_at=_utc_now())
    _write_private(state_path, _json_bytes(document))
    transcript_path = directory / "transcript.log"
    descriptor = os.open(transcript_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    exit_code = 70
    state = "failed"
    termination_requested = False

    def request_termination(_signum: int, _frame: object) -> None:
        nonlocal termination_requested
        termination_requested = True

    prior_term = signal.signal(signal.SIGTERM, request_termination)
    try:
        with os.fdopen(descriptor, "wb") as transcript:
            process = subprocess.Popen(
                command, cwd=str(document["working_directory"]), env=environment,
                stdin=subprocess.DEVNULL, stdout=transcript, stderr=subprocess.STDOUT,
                close_fds=True, start_new_session=True,
            )
            deadline = time.monotonic() + int(document["timeout_seconds"])
            next_heartbeat = time.monotonic() + HEARTBEAT_SECONDS
            terminating_at: float | None = None
            terminal_reason: str | None = None
            while True:
                child_exit = process.poll()
                if child_exit is not None:
                    exit_code = child_exit
                    if terminal_reason is not None:
                        state = terminal_reason
                    else:
                        state = "completed" if child_exit == 0 else "failed"
                    break
                now = time.monotonic()
                if now >= next_heartbeat:
                    with _state_lock(root):
                        latest = _read_document(state_path)
                        if (
                            latest.get("supervisor_pid") == os.getpid()
                            and latest.get("supervisor_start") == _process_start(os.getpid())
                            and latest.get("state") in {"running", "orphaned"}
                        ):
                            latest.update(state="running", heartbeat_at=_utc_now())
                            _write_private(state_path, _json_bytes(latest))
                    next_heartbeat = now + HEARTBEAT_SECONDS
                if termination_requested and terminal_reason is None:
                    terminal_reason = "cancelled"
                    terminating_at = now
                    os.killpg(process.pid, signal.SIGTERM)
                elif now >= deadline and terminal_reason is None:
                    terminal_reason = "timed_out"
                    terminating_at = now
                    os.killpg(process.pid, signal.SIGTERM)
                elif (
                    terminal_reason is not None and terminating_at is not None
                    and now - terminating_at >= 10
                ):
                    try:
                        os.killpg(process.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                    exit_code = process.wait()
                    state = terminal_reason
                    break
                time.sleep(0.1)
            if state == "timed_out":
                exit_code = 124
            elif state == "cancelled" and exit_code == 0:
                exit_code = 128 + signal.SIGTERM
    except OSError:
        exit_code, state = 70, "failed"
    finally:
        signal.signal(signal.SIGTERM, prior_term)
    with _state_lock(root):
        latest = _read_document(state_path)
        if latest.get("state") == "cancelling":
            state = "cancelled"
        latest.update(state=state, finished_at=_utc_now(), exit_code=exit_code)
        _write_private(state_path, _json_bytes(latest))
    return exit_code


def _public(document: Mapping[str, object]) -> dict[str, object]:
    keys = (
        "schema", "job_id", "state", "created_at", "started_at", "finished_at",
        "working_directory", "prompt_sha256", "model", "provider", "reasoning",
        "toolsets", "max_turns", "timeout_seconds", "heartbeat_at", "exit_code",
    )
    return {key: document.get(key) for key in keys}


def status(root: Path, job_id: str | None) -> object:
    with _state_lock(root):
        if job_id is not None:
            path = _job_path(root, job_id)
            return _public(_reconcile(path, _read_document(path / "state.json")))
        return [_public(_reconcile(path, document)) for path, document in _jobs(root)]


def wait(root: Path, job_id: str, timeout_seconds: float) -> dict[str, object]:
    if isinstance(timeout_seconds, bool) or not isinstance(timeout_seconds, (int, float)) or not math.isfinite(timeout_seconds) or timeout_seconds < 0:
        raise DispatchError("invalid_limits")
    deadline = time.monotonic() + timeout_seconds
    while True:
        document = status(root, job_id)
        assert isinstance(document, dict)
        if document.get("state") in TERMINAL_STATES:
            return document
        if time.monotonic() >= deadline:
            return document
        time.sleep(min(0.25, max(0.0, deadline - time.monotonic())))


def cancel(root: Path, job_id: str) -> dict[str, object]:
    with _state_lock(root):
        directory = _job_path(root, job_id)
        state_path = directory / "state.json"
        document = _reconcile(directory, _read_document(state_path))
        if document.get("state") in TERMINAL_STATES:
            return _public(document)
        if document.get("state") == "starting":
            raise DispatchError("job_not_ready")
        if not _live(document):
            raise DispatchError("stale_process")
        pid = int(document["supervisor_pid"])
        try:
            if os.getpgid(pid) != pid:
                raise DispatchError("stale_process")
        except ProcessLookupError as exc:
            raise DispatchError("stale_process") from exc
        document.update(state="cancelling")
        _write_private(state_path, _json_bytes(document))
        os.killpg(pid, signal.SIGTERM)
    return _public(document)


def log(root: Path, job_id: str, tail_bytes: int) -> bytes:
    if isinstance(tail_bytes, bool) or not isinstance(tail_bytes, int) or not 1 <= tail_bytes <= MAX_LOG_BYTES:
        raise DispatchError("invalid_limits")
    path = _job_path(root, job_id) / "transcript.log"
    try:
        with path.open("rb") as handle:
            handle.seek(0, os.SEEK_END)
            size = handle.tell()
            handle.seek(max(0, size - tail_bytes))
            return handle.read(tail_bytes)
    except OSError as exc:
        raise DispatchError("log_unavailable") from exc


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="foxhound-agent-dispatch")
    parser.add_argument("--state-root", type=Path, default=_default_state_root())
    subparsers = parser.add_subparsers(dest="command", required=True)
    start_parser = subparsers.add_parser("start")
    start_parser.add_argument("--prompt-file", required=True, type=Path)
    start_parser.add_argument("--working-directory", required=True, type=Path)
    start_parser.add_argument("--runtime-command", type=Path, default=_default_runtime())
    start_parser.add_argument("--credential-file", type=Path, default=_default_credential_file())
    start_parser.add_argument("--credential-name", default="GEMINI_API_KEY")
    start_parser.add_argument("--model", default=DEFAULT_MODEL)
    start_parser.add_argument("--provider", default=DEFAULT_PROVIDER)
    start_parser.add_argument("--reasoning", choices=sorted(_REASONING), default=DEFAULT_REASONING)
    start_parser.add_argument("--toolsets", default="terminal,file")
    start_parser.add_argument("--max-turns", type=int, default=DEFAULT_MAX_TURNS)
    start_parser.add_argument("--timeout-seconds", type=int, default=DEFAULT_TIMEOUT_SECONDS)
    start_parser.add_argument("--max-concurrency", type=int, default=DEFAULT_MAX_CONCURRENCY)
    status_parser = subparsers.add_parser("status")
    status_parser.add_argument("job_id", nargs="?")
    wait_parser = subparsers.add_parser("wait")
    wait_parser.add_argument("job_id")
    wait_parser.add_argument("--timeout-seconds", type=float, default=30.0)
    log_parser = subparsers.add_parser("log")
    log_parser.add_argument("job_id")
    log_parser.add_argument("--tail-bytes", type=int, default=64 * 1024)
    cancel_parser = subparsers.add_parser("cancel")
    cancel_parser.add_argument("job_id")
    run_parser = subparsers.add_parser("_run", help=argparse.SUPPRESS)
    run_parser.add_argument("--job-id", required=True)
    run_parser.add_argument("--runtime-command", required=True, type=Path)
    run_parser.add_argument("--credential-file", required=True, type=Path)
    run_parser.add_argument("--credential-name", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    arguments = _parser().parse_args(argv)
    try:
        if arguments.command == "start":
            result = start(
                prompt=arguments.prompt_file, working_directory=arguments.working_directory,
                state_root=arguments.state_root, runtime_command=arguments.runtime_command,
                credential_file=arguments.credential_file, credential_name=arguments.credential_name,
                model=arguments.model, provider=arguments.provider, reasoning=arguments.reasoning,
                toolsets=arguments.toolsets, max_turns=arguments.max_turns,
                timeout_seconds=arguments.timeout_seconds, max_concurrency=arguments.max_concurrency,
            )
        elif arguments.command == "status":
            result = status(arguments.state_root, arguments.job_id)
        elif arguments.command == "wait":
            result = wait(arguments.state_root, arguments.job_id, arguments.timeout_seconds)
        elif arguments.command == "cancel":
            result = cancel(arguments.state_root, arguments.job_id)
        elif arguments.command == "log":
            sys.stdout.buffer.write(log(arguments.state_root, arguments.job_id, arguments.tail_bytes))
            return 0
        else:
            return _run(
                arguments.state_root, arguments.job_id, arguments.runtime_command,
                arguments.credential_file, arguments.credential_name,
            )
    except DispatchError as exc:
        print(json.dumps({"accepted": False, "error_code": exc.code}, sort_keys=True))
        return 70
    print(json.dumps({"accepted": True, "result": result}, sort_keys=True))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
