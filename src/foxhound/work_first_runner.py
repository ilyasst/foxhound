"""Work-first runner: prioritize execution passes before research passes.

Why:
Research and the work that uses it must happen close together. When research
runs ahead of execution slots, research sits for hours before planning and
goes stale (source threads move, readers act, or context drifts). A separate
research runner racing ahead of execution creates a large gap by design.

By coupling execution and research in a single work-first slot, ready work
(tasks ready to plan, tasks not needing research, approved plans ready to
execute, approved external actions) is always drained before research starts.
Research runs only when the execution runner reports idle, ensuring research
never runs ahead of the slots that consume it.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
from typing import Sequence

from foxhound.deployment_config import (
    DeploymentConfigError,
    component_command,
    load_deployment_config,
)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="foxhound-work-first-runner",
        description="Run execution runner first, research runner only if idle",
    )
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--execution-component", required=True)
    parser.add_argument("--research-component", default="research-runner")
    return parser


def _parse_last_json_line(text: str) -> dict | None:
    for line in reversed(text.splitlines()):
        line = line.strip()
        if line.startswith("{") and line.endswith("}"):
            try:
                data = json.loads(line)
                if isinstance(data, dict):
                    return data
            except json.JSONDecodeError:
                continue
    return None


def _run_child(argv: list[str], extra_env: dict[str, str]) -> tuple[int, dict | None]:
    env = {**os.environ, **extra_env}
    process = subprocess.Popen(
        argv,
        env=env,
        stdout=subprocess.PIPE,
        stderr=None,
        text=True,
    )

    def handle_sigterm(signum: int, frame: object) -> None:
        try:
            process.terminate()
        except OSError:
            pass

    old_handler = signal.signal(signal.SIGTERM, handle_sigterm)
    try:
        stdout_data, _ = process.communicate()
    finally:
        signal.signal(signal.SIGTERM, old_handler)

    if stdout_data:
        sys.stderr.write(stdout_data)
        sys.stderr.flush()

    parsed = _parse_last_json_line(stdout_data)
    exit_code = process.returncode
    return exit_code, parsed


def main(argv: Sequence[str] | None = None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)

    try:
        config = load_deployment_config(args.config)
        script_directory = Path(sys.argv[0]).resolve().parent
        exec_argv, exec_env = component_command(
            config,
            args.execution_component,
            script_directory=script_directory,
        )
        research_argv, research_env = component_command(
            config,
            args.research_component,
            script_directory=script_directory,
        )
    except DeploymentConfigError:
        print(json.dumps({"ok": False, "error_code": "configuration_unavailable"}, sort_keys=True))
        return 78

    exec_code, exec_json = _run_child(exec_argv, exec_env)

    if exec_code == 0 and exec_json is not None and exec_json.get("outcome") == "idle":
        research_code, research_json = _run_child(research_argv, research_env)
        if research_code == 0 and research_json is not None and research_json.get("claimed") is False:
            report = {
                "child_exit": research_code,
                "ok": True,
                "ran": "idle",
            }
            print(json.dumps(report, sort_keys=True))
            return 0

        report = {
            "child_exit": research_code,
            "ok": research_code == 0,
            "ran": "research",
        }
        print(json.dumps(report, sort_keys=True))
        return research_code

    report = {
        "child_exit": exec_code,
        "ok": exec_code == 0,
        "ran": "execution",
    }
    print(json.dumps(report, sort_keys=True))
    return exec_code


if __name__ == "__main__":
    raise SystemExit(main())
