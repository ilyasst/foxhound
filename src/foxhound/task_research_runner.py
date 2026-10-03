"""One-shot Researcher orchestration runner.

Claims at most one queued research job from the task database, sets up a
private scratch directory under the configured scratch root, resolves
capability-fenced context without exposing it, synthesizes evidence-only
draft and coverage artifacts via task_research_synthesis, publishes via
the receipt boundary of ResearchStore to the task folder, and removes transient
scratch artifacts on success.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import sys
import sqlite3
import stat
import subprocess
import tempfile
import time
from collections.abc import Mapping
from dataclasses import dataclass
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Sequence

from .execution_worker import ExecutionWorkerConfigError, load_knowledge_config
from .knowledge_client import (
    GwKnowledgeClient,
    KnowledgeClientError,
)
from .task_duplicate_semantic import (
    DEFAULT_DIALECT,
    DIALECTS,
)
from .task_research import (
    ResearchError,
    ResearchStore,
)
from .task_research_agent import (
    AgentResearchConfig,
    agent_synthesize,
)
from .task_research_sources import ResearchSourceError, bound_research_sources
from .task_research_synthesis import (
    DEFAULT_MAX_DOCUMENTS,
    DEFAULT_MAX_SEARCHES,
    DEFAULT_TIMEOUT_SECONDS,
    KnowledgeSearch,
    SynthesisConfig,
    SynthesisError,
    SynthesisResult,
    synthesize,
)


@dataclass(frozen=True)
class ResearchRunResult:
    """Content-free aggregate status for one research runner pass."""

    claimed: bool
    completed: bool
    failure_code: str | None = None
    state: str = "idle"


def _validate_owner_private_dir(path: Path) -> Path:
    """Validate that path is an absolute, non-symlink, owner-private directory."""
    if not path.is_absolute():
        raise ResearchError("path must be absolute")
    if any(part in {"", ".", ".."} for part in path.parts):
        raise ResearchError("invalid path components")

    try:
        cur = Path(path.parts[0])
        for part in path.parts[1:]:
            cur = cur / part
            info = cur.lstat()
            if not stat.S_ISDIR(info.st_mode) or stat.S_ISLNK(info.st_mode):
                raise ResearchError("path component is not a directory or is a symlink")
            # The root itself must be private; above it, refuse only what
            # another account could write to and so swap out from under us.
            # Requiring every ancestor to be private rejected ordinary homes
            # (0701) and synced folders (0775) the execution runners accept.
            if cur == path:
                if info.st_uid != os.geteuid() or info.st_mode & 0o077:
                    raise ResearchError("path component is not owner-private")
            elif info.st_mode & 0o002 and not info.st_mode & stat.S_ISVTX:
                raise ResearchError("path component is writable by others")
    except OSError as exc:
        raise ResearchError("inaccessible path") from exc

    resolved = path.resolve(strict=True)
    if resolved != path:
        raise ResearchError("path contains symlinks")
    return resolved


def _ensure_private_scratch_root(scratch_root: Path) -> Path:
    """Validate or create the scratch root directory with 0o700 permissions."""
    if not scratch_root.is_absolute():
        raise ResearchError("scratch root must be absolute")
    if any(part in {"", ".", ".."} for part in scratch_root.parts):
        raise ResearchError("invalid scratch root components")

    if not scratch_root.exists():
        scratch_root.mkdir(parents=True, mode=0o700)
    return _validate_owner_private_dir(scratch_root)


def run_once(
    *,
    database: Path,
    cas_root: Path,
    task_work_root: Path,
    scratch_root: Path,
    model: str,
    endpoint: str,
    dialect: str = DEFAULT_DIALECT,
    reasoning: str = "high",
    timeout: float = DEFAULT_TIMEOUT_SECONDS,
    knowledge_timeout: float = 30.0,
    max_searches: int = DEFAULT_MAX_SEARCHES,
    max_documents: int = DEFAULT_MAX_DOCUMENTS,
    profile_id: str = "researcher",
    profile_revision: str = "0" * 64,
    provider: str = "local",
    gw_endpoint: str | None = None,
    gw_alias: str | None = None,
    gw_token_file: Path | None = None,
    worker_id: str = "foxhound-research-runner",
    lease_seconds: int = 1800,
    knowledge_override: KnowledgeSearch | None = None,
    opener=None,
    clock: Callable[[], datetime] | None = None,
    synthesizer: str = "single",
    hermes_command: str | None = None,
    agent_toolsets: str = "terminal,file,web,browser",
    agent_max_turns: int = 120,
    apply_scheduling: bool = False,
    agent_timeout: int = 3600,
    knowledge_roots: tuple[tuple[str, str], ...] = (),
    read_only_commands: tuple[dict[str, str], ...] = (),
    reader_aliases: tuple[str, ...] = (),
    agent_runner=None,
) -> ResearchRunResult:
    """Claim at most one queued research job and execute it through publication."""
    if synthesizer not in ("single", "agent"):
        raise ValueError(f"unknown synthesizer: {synthesizer}")
    if synthesizer == "agent" and not hermes_command:
        raise ValueError("hermes_command is required when synthesizer is 'agent'")
    db_path = Path(database).resolve()
    cas_path = Path(cas_root).resolve()
    work_root_path = _validate_owner_private_dir(Path(task_work_root))
    trusted_scratch_root = _ensure_private_scratch_root(Path(scratch_root))

    # At start of run_once, delete failed-* directories older than 7 days
    now_ts = (clock() if clock else datetime.now(timezone.utc)).timestamp()
    cutoff_ts = now_ts - (7 * 86400)
    for entry in trusted_scratch_root.iterdir():
        if entry.is_dir() and entry.name.startswith("failed-"):
            try:
                mtime = entry.stat().st_mtime
                if mtime < cutoff_ts:
                    shutil.rmtree(entry, ignore_errors=True)
            except OSError:
                pass

    store = ResearchStore(db_path, cas_path, clock=clock)
    try:
        store.recover_expired()
    except ResearchError:
        pass
    claim = store.claim(
        worker_id,
        lease_seconds=lease_seconds,
        task_work_root=work_root_path,
    )
    if claim is None:
        return ResearchRunResult(claimed=False, completed=False, state="idle")

    job_id = claim.job.job_id
    token = claim.token
    run_scratch: Path | None = None
    started_wall = time.monotonic()
    synthesis_result: SynthesisResult | None = None

    def _record_metrics(outcome: str) -> None:
        try:
            wall_seconds = max(0.0, time.monotonic() - started_wall)
            now_dt = clock() if clock else datetime.now(timezone.utc)
            ts = now_dt.strftime("%Y%m%dT%H%M%SZ")
            metrics_dir = trusted_scratch_root / "metrics"
            if not metrics_dir.exists():
                metrics_dir.mkdir(parents=True, mode=0o700)
            else:
                metrics_dir.chmod(0o700)
            doc: dict[str, Any] = {
                "job_id": job_id,
                "task_id": claim.job.task_id,
                "task_version": claim.job.task_version,
                "outcome": outcome,
                "wall_seconds": wall_seconds,
            }
            if synthesis_result is not None and synthesis_result.metrics:
                doc.update(synthesis_result.metrics)
            data = (json.dumps(doc, sort_keys=True, indent=2) + "\n").encode("utf-8")
            target = metrics_dir / f"{job_id}-{ts}.json"
            tmp = target.with_name(f"{target.name}.tmp-{os.urandom(6).hex()}")
            fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            try:
                with open(fd, "wb", closefd=True) as f:
                    f.write(data)
                    f.flush()
                    os.fsync(f.fileno())
                tmp.replace(target)
            finally:
                if tmp.exists():
                    try:
                        tmp.unlink()
                    except OSError:
                        pass
        except Exception:
            pass

    def fail_or_repair(failure_code: str) -> ResearchRunResult:
        """Record an ordinary failure, or finish an interrupted publication."""
        try:
            state = store.fail(job_id, token, failure_code)
        except ResearchError:
            try:
                if store.repair(job_id):
                    return ResearchRunResult(
                        claimed=True,
                        completed=True,
                        state="completed",
                    )
            except (ResearchError, OSError):
                pass
            state = "failed"
        return ResearchRunResult(
            claimed=True,
            completed=False,
            failure_code=failure_code,
            state=state,
        )

    try:
        # Create a unique, private scratch directory strictly contained within scratch_root
        run_scratch = Path(tempfile.mkdtemp(prefix=f"run-{claim.job.task_id}-", dir=str(trusted_scratch_root)))
        os.chmod(run_scratch, 0o700)
        _validate_owner_private_dir(run_scratch)

        # Context contains capability-fenced data; never log it
        ctx = store.context(job_id, token)

        # Verify task matches configured task_work_root
        task_snapshot = ctx["task_snapshot"]
        assert isinstance(task_snapshot, dict)
        task_id = int(task_snapshot["task_id"])
        task_version = int(task_snapshot["task_version"])

        try:
            supplied = bound_research_sources(
                db_path,
                task_id=task_id,
                task_version=task_version,
            )
        except ResearchSourceError as exc:
            raise SynthesisError("source_refused") from exc

        if synthesizer == "agent":
            agent_run_dir = Path(tempfile.mkdtemp(prefix="agent-", dir=str(run_scratch)))
            os.chmod(agent_run_dir, 0o700)
            _validate_owner_private_dir(agent_run_dir)
            agent_config = AgentResearchConfig(
                hermes_command=hermes_command,  # type: ignore[arg-type]
                model=model,
                provider=(provider if provider != "local" else None),
                toolsets=agent_toolsets,
                max_turns=agent_max_turns,
                timeout_seconds=agent_timeout,
                knowledge_roots=knowledge_roots,
                read_only_commands=read_only_commands,
                reader_aliases=reader_aliases,
                profile_id=profile_id,
                profile_revision=profile_revision,
                job_id=job_id,
            )
            synthesis_result = agent_synthesize(
                ctx,
                config=agent_config,
                bound_sources=supplied,
                run_dir=agent_run_dir,
                runner=agent_runner or subprocess.run,
            )
        else:
            # Prepare synthesis dependencies
            if knowledge_override is not None:
                knowledge = knowledge_override
            else:
                if not gw_endpoint or not gw_alias or not gw_token_file:
                    raise SynthesisError("invalid_config")
                k_cfg = load_knowledge_config(
                    gw_endpoint,
                    gw_alias,
                    gw_token_file,
                    timeout_seconds=knowledge_timeout,
                )
                knowledge = GwKnowledgeClient(k_cfg)

            synthesis_config = SynthesisConfig(
                model=model,
                endpoint=endpoint,
                dialect=dialect,
                timeout_seconds=timeout,
                knowledge_timeout_seconds=knowledge_timeout,
                reasoning=reasoning,
                max_searches=max_searches,
                max_documents=max_documents,
                profile_id=profile_id,
                profile_revision=profile_revision,
                provider=provider,
            )

            synthesis_result = synthesize(
                ctx,
                knowledge=knowledge,  # type: ignore[arg-type]
                config=synthesis_config,
                bound_sources=supplied,
                opener=opener,
            )

        # Publish synthesized report using the existing receipt boundary
        published = store.publish(
            job_id=job_id,
            token=token,
            draft=synthesis_result.draft,
            sources=list(synthesis_result.sources),
            provenance=synthesis_result.provenance,
            coverage=synthesis_result.coverage,
        )
        if apply_scheduling:
            # The report is published either way: a recommendation that
            # cannot be applied is still visible in Research.md, and must
            # never turn a finished research pass into a failed one.
            try:
                from .task_research_scheduling import apply_published_recommendations
                apply_published_recommendations(db_path, published)
            except Exception as exc:
                print(
                    "foxhound research runner: scheduling recommendations "
                    f"not applied: {type(exc).__name__}",
                    file=sys.stderr,
                )
            # A cited deadline or effort is stored beside the task (#846):
            # it orders the queue without revising the task.
            try:
                from .task_timing import record_research_timing
                stamp = (clock() if clock else datetime.now(timezone.utc)).isoformat()
                with closing(sqlite3.connect(db_path, timeout=10)) as timing_db:
                    if record_research_timing(
                        timing_db, task_id, job_id, published, stamp,
                    ):
                        timing_db.commit()
            except Exception as exc:
                print(
                    "foxhound research runner: research timing not stored: "
                    f"{type(exc).__name__}",
                    file=sys.stderr,
                )

        outcome = "degraded" if bool(synthesis_result.coverage.get("degraded")) else "success"
        _record_metrics(outcome)

        # Scratch directory cleanup on success
        if run_scratch.exists():
            shutil.rmtree(run_scratch, ignore_errors=True)
            run_scratch = None

        return ResearchRunResult(claimed=True, completed=True, state="completed")

    except SynthesisError as exc:
        _record_metrics("failure")
        return fail_or_repair(exc.code)
    except (ResearchError, ResearchSourceError, KnowledgeClientError, ExecutionWorkerConfigError):
        _record_metrics("failure")
        return fail_or_repair("runtime_failed")
    except Exception:
        _record_metrics("failure")
        return fail_or_repair("unexpected_error")
    finally:
        if run_scratch is not None and run_scratch.exists():
            # Failure occurred: rename to failed-<job_id>-<timestamp> inside scratch_root
            ts = int((clock() if clock else datetime.now(timezone.utc)).timestamp())
            failed_dir = trusted_scratch_root / f"failed-{job_id}-{ts}"
            try:
                run_scratch.rename(failed_dir)
            except OSError:
                shutil.rmtree(run_scratch, ignore_errors=True)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="foxhound-task-research-runner",
        description="One-shot Researcher orchestration command.",
    )
    parser.add_argument("--database", required=True, type=Path)
    parser.add_argument("--cas-root", required=True, type=Path)
    parser.add_argument("--task-work-root", required=True, type=Path)
    parser.add_argument("--scratch-root", required=True, type=Path)
    parser.add_argument("--model", required=True)
    parser.add_argument("--endpoint", required=True)
    parser.add_argument("--dialect", choices=sorted(DIALECTS), default=DEFAULT_DIALECT)
    parser.add_argument("--reasoning", choices=("low", "medium", "high"), default="high")
    parser.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT_SECONDS)
    parser.add_argument(
        "--knowledge-timeout",
        type=float,
        default=30.0,
        help="deadline in seconds for each read-only GW search (default: 30)",
    )
    parser.add_argument("--max-searches", type=int, default=DEFAULT_MAX_SEARCHES)
    parser.add_argument("--max-documents", type=int, default=DEFAULT_MAX_DOCUMENTS)
    parser.add_argument("--profile-id", default="researcher")
    parser.add_argument("--profile-revision", default="0" * 64)
    parser.add_argument("--provider", default="local")
    parser.add_argument("--gw-endpoint", default=None)
    parser.add_argument("--gw-alias", default=None)
    parser.add_argument("--gw-token-file", default=None, type=Path)
    parser.add_argument("--worker-id", default="foxhound-research-runner")
    parser.add_argument("--lease-seconds", type=int, default=1800)
    parser.add_argument(
        "--synthesizer",
        choices=("single", "agent"),
        default="single",
        help="synthesis implementation: single model call or Hermes agent",
    )
    parser.add_argument(
        "--hermes-command",
        default=None,
        help="path to hermes CLI or executable; required when --synthesizer is agent",
    )
    parser.add_argument(
        "--agent-toolsets",
        default="terminal,file,web,browser",
        help="comma-separated toolsets permitted to the agent researcher",
    )
    parser.add_argument(
        "--agent-max-turns",
        type=int,
        default=120,
        help="maximum agent turns before giving up (default: 120)",
    )
    parser.add_argument(
        "--agent-timeout",
        type=int,
        default=3600,
        help="total timeout in seconds for agent research execution (default: 3600)",
    )
    parser.add_argument(
        "--knowledge-root",
        action="append",
        default=[],
        metavar="NAME=PATH",
        help="named knowledge root exposed to the agent researcher (repeatable)",
    )
    parser.add_argument(
        "--read-only-command",
        action="append",
        default=[],
        metavar="JSON",
        help="JSON string defining a read-only command: {name, command, description} (repeatable)",
    )
    parser.add_argument(
        "--apply-scheduling", action="store_true",
        help="apply the published report's scheduling recommendations",
    )
    parser.add_argument(
        "--reader-alias",
        action="append",
        default=[],
        help="reader alias exposed to the agent researcher in task.json (repeatable)",
    )
    return parser


def _parse_read_only_commands(
    parser: argparse.ArgumentParser, entries: Sequence[str]
) -> tuple[dict[str, str], ...]:
    reserved_names = {
        "kb", "attachment", "attachments", "email", "emails", "repo", "web", "meeting",
    }
    seen_names: set[str] = set()
    commands: list[dict[str, str]] = []
    for entry in entries:
        try:
            data = json.loads(entry)
        except Exception:
            parser.error(f"invalid --read-only-command '{entry}': invalid JSON")
        if not isinstance(data, Mapping) or set(data) != {"name", "command", "description"}:
            parser.error(f"invalid --read-only-command '{entry}': must be JSON object with keys name, command, description")
        name = data["name"]
        if not isinstance(name, str) or not re.fullmatch(r"[a-z][a-z0-9-]{0,31}", name):
            parser.error(f"invalid --read-only-command '{entry}': name must be identifier [a-z][a-z0-9-]{{0,31}}")
        if name in reserved_names:
            parser.error(f"invalid --read-only-command '{entry}': name '{name}' is reserved")
        if name in seen_names:
            parser.error(f"invalid --read-only-command '{entry}': duplicate command name '{name}'")
        seen_names.add(name)

        raw_cmd = data["command"]
        if not isinstance(raw_cmd, str) or not Path(raw_cmd).is_absolute():
            parser.error(f"invalid --read-only-command '{entry}': command must be an absolute path")

        desc = data["description"]
        if not isinstance(desc, str) or not desc.strip() or len(desc) > 2000:
            parser.error(f"invalid --read-only-command '{entry}': description must be non-empty and <= 2000 characters")

        commands.append({
            "name": name,
            "command": str(Path(raw_cmd)),
            "description": desc,
        })
    return tuple(commands)


def _parse_knowledge_roots(parser: argparse.ArgumentParser, entries: Sequence[str]) -> tuple[tuple[str, str], ...]:
    roots: list[tuple[str, str]] = []
    for entry in entries:
        name, separator, raw_path = entry.partition("=")
        if not separator or not name:
            parser.error(f"invalid --knowledge-root '{entry}': must be NAME=PATH")
        path = Path(raw_path)
        if not path.is_absolute():
            parser.error(f"invalid --knowledge-root '{entry}': PATH must be absolute")
        roots.append((name, str(path)))
    return tuple(roots)


def main(argv: Sequence[str] | None = None) -> int:
    parser = _parser()
    arguments = parser.parse_args(argv)
    knowledge_roots = _parse_knowledge_roots(parser, arguments.knowledge_root)
    read_only_commands = _parse_read_only_commands(parser, arguments.read_only_command)
    reader_aliases = tuple(arguments.reader_alias)
    try:
        result = run_once(
            database=arguments.database,
            cas_root=arguments.cas_root,
            task_work_root=arguments.task_work_root,
            scratch_root=arguments.scratch_root,
            model=arguments.model,
            endpoint=arguments.endpoint,
            dialect=arguments.dialect,
            reasoning=arguments.reasoning,
            timeout=arguments.timeout,
            knowledge_timeout=arguments.knowledge_timeout,
            max_searches=arguments.max_searches,
            max_documents=arguments.max_documents,
            profile_id=arguments.profile_id,
            profile_revision=arguments.profile_revision,
            provider=arguments.provider,
            gw_endpoint=arguments.gw_endpoint,
            gw_alias=arguments.gw_alias,
            gw_token_file=arguments.gw_token_file,
            worker_id=arguments.worker_id,
            lease_seconds=arguments.lease_seconds,
            synthesizer=arguments.synthesizer,
            hermes_command=arguments.hermes_command,
            agent_toolsets=arguments.agent_toolsets,
            agent_max_turns=arguments.agent_max_turns,
            apply_scheduling=arguments.apply_scheduling,
            agent_timeout=arguments.agent_timeout,
            knowledge_roots=knowledge_roots,
            read_only_commands=read_only_commands,
            reader_aliases=reader_aliases,
        )
    except (ResearchError, ValueError, OSError):
        print(json.dumps({"accepted": False, "error_code": "configuration_unavailable"}, sort_keys=True))
        return 78
    except Exception:
        print(json.dumps({"accepted": False, "error_code": "runner_failed"}, sort_keys=True))
        return 70

    output = {
        "accepted": True,
        "claimed": result.claimed,
        "completed": result.completed,
        "state": result.state,
    }
    if result.failure_code is not None:
        output["failure_code"] = result.failure_code
    print(json.dumps(output, sort_keys=True))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
