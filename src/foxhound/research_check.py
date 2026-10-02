"""In-pass research output validation for agent research."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

from foxhound.task_research_agent import check_research_output


def main(argv: list[str] | None = None) -> int:
    current_dir = Path.cwd()
    task_json_path = current_dir / "task.json"

    knowledge_roots: list[tuple[str, str]] = []
    read_only_commands: list[dict[str, Any]] = []

    if task_json_path.exists():
        try:
            task_data = json.loads(task_json_path.read_text(encoding="utf-8"))
            if isinstance(task_data, dict):
                raw_roots = task_data.get("knowledge_roots", [])
                if isinstance(raw_roots, list):
                    for item in raw_roots:
                        if isinstance(item, dict) and "name" in item and "path" in item:
                            knowledge_roots.append((str(item["name"]), str(item["path"])))
                raw_cmds = task_data.get("read_only_commands", [])
                if isinstance(raw_cmds, list):
                    for cmd in raw_cmds:
                        if isinstance(cmd, dict):
                            read_only_commands.append(cmd)
                        elif isinstance(cmd, str):
                            read_only_commands.append({"name": cmd, "command": cmd, "description": ""})
        except Exception:
            pass

    draft, sources, problems = check_research_output(
        current_dir,
        knowledge_roots=knowledge_roots,
        read_only_commands=read_only_commands,
    )

    if not problems:
        print("OK research.json is publishable")
        return 0

    count = len(problems)
    noun = "problem" if count == 1 else "problems"
    print(f"FAIL {count} {noun}:")
    for prob in problems:
        print(f"- {prob}")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
