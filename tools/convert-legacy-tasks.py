#!/usr/bin/env python3
"""Convert legacy (append-only) task READMEs into the rendered format.

This tool finds task directories that have a README.md but no .task-log.json,
parses the old append-only format, generates .task-log.json, and re-renders
the README from the log using the same rendering logic as task_archive.py.

Usage:
    python tools/convert-legacy-tasks.py TASK_DIR ...
    python tools/convert-legacy-tasks.py --all TASKS_ROOT

Exit codes:
    0 — all processed (may include skips for already-converted tasks)
    1 — usage error or at least one conversion error
"""

import json
import re
import sys
from pathlib import Path


# ---------------------------------------------------------------------------
# Render engine — mirrors task_archive._render_task_document exactly
# so output is byte-identical to what the worker would produce.
# ---------------------------------------------------------------------------

MAX_HISTORY_RUNS = 50
MAX_HISTORY_SUMMARY_CHARS = 200


def _single_line(value: str, maximum: int) -> str:
    text = " ".join(str(value).split())
    if len(text) > maximum:
        text = text[:maximum].rstrip() + "..."
    return text


def _render_task_document(log: dict) -> str:
    """Render a task README from its .task-log.json log."""
    runs = [e for e in log.get("runs", []) if isinstance(e, dict)]
    current = None
    for entry in reversed(runs):
        if entry.get("outcome"):
            current = entry
            break
    latest = runs[-1] if runs else None
    task_id = log.get("task_id")
    task_text = str(log.get("task_text") or "").strip()
    title = _single_line(task_text, 120) or "Task"

    lines = [f"# T{task_id} \u2014 {title}", ""]
    status = str(current.get("outcome")) if current else "no result recorded yet"
    phase = str((latest or {}).get("phase") or "")
    agent = str((latest or {}).get("agent") or "")
    lines.append(f"**Status:** {status}")
    if phase:
        lines.append(f"**Phase:** {phase}")
    if agent:
        lines.append(f"**Agent:** {agent}")
    source = log.get("source")
    if source:
        lines.append(f"**Source:** {source}")
    lines.append(f"**Folder:** `{log.get('working_directory')}`")
    lines.append("")

    if task_text:
        lines.extend(("## Objective", "", task_text, ""))

    if current:
        lines.append(f"## Current result \u2014 {current.get('stamp')}")
        lines.append("")
        summary = str(current.get("summary") or "").strip()
        lines.extend((summary or "_No summary recorded._", ""))
        folder = log.get("working_directory")
        if folder and current.get("run"):
            lines.extend((
                f"**Evidence:** `{Path(str(folder)) / 'runs' / str(current['run'])}`",
                "",
            ))

    for heading, key in (
        ("Next action", "external_actions"),
        ("Questions for the reader", "questions"),
        ("Deliverables", "deliverables"),
        ("Review links", "review_links"),
    ):
        values = [str(v) for v in (current or {}).get(key, []) if str(v)]
        if values:
            lines.append(f"## {heading}")
            lines.append("")
            lines.extend(f"- {value}" for value in values)
            lines.append("")

    if runs:
        lines.extend(("## History", ""))
        for entry in runs[-MAX_HISTORY_RUNS:]:
            outcome = entry.get("outcome") or "no result recorded"
            note = _single_line(
                str(entry.get("summary") or ""), MAX_HISTORY_SUMMARY_CHARS
            )
            line = (
                f"- {entry.get('stamp')} \u2014 {entry.get('phase')} \u2014 {outcome}"
                f" \u2014 `runs/{entry.get('run')}`"
            )
            lines.append(f"{line}\n  {note}" if note else line)
        if len(runs) > MAX_HISTORY_RUNS:
            lines.append(
                f"- _({len(runs) - MAX_HISTORY_RUNS} earlier run(s) not "
                "listed; all remain under `runs/`.)_"
            )
        lines.append("")

    work = str((current or {}).get("work") or "").strip()
    if work:
        lines.extend((
            "## Work",
            "",
            "_The current result in full. Earlier results are in their own "
            "run directories, listed under History._",
            "",
            work,
            "",
        ))
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Parser for the old append-only README format
# ---------------------------------------------------------------------------

_HEADER_RE = re.compile(r"^# T(\d+) \u2014 (.+)$")
_TASK_LINE_RE = re.compile(r"^\*\*Task:\*\* (.+)$")
_SOURCE_LINE_RE = re.compile(r"^\*\*Source:\*\* (.+)$")
_FOLDER_LINE_RE = re.compile(r"^\*\*Working folder:\*\* `(.+)`$")

_RUN_HEADER_RE = re.compile(r"^## Run (\w+) \u2014 (.+)$")

_AGENT_RE = re.compile(r"^- Agent: (.+)$")
_RUN_PATH_RE = re.compile(r"^- Run: (.+)$")

_RESULT_HEADER = re.compile(r"^### Result$")

_OUTCOME_RE = re.compile(r"^\*\*Outcome:\*\* (.+)$")
_SUMMARY_RE = re.compile(r"^\*\*Summary:\*\* (.+)$")
_REVIEW_LINKS_HEADER = re.compile(r"^\*\*Review links:\*\*$")
_NEEDS_INPUT_HEADER = re.compile(r"^\*\*Needs your input:\*\*$")
_EXTERNAL_ACTIONS_HEADER = re.compile(r"^\*\*External actions:\*\*$")
_DELIVERABLES_HEADER = re.compile(r"^\*\*Deliverables:\*\*$")
_WORK_HEADER = re.compile(r"^\*\*Work:\*\*$")


def _strip_backticks(s: str) -> str:
    """Remove backticks and Unicode quote variants from path strings."""
    return s.strip().strip("`\u2018\u2019\u201c\u201d")


def _format_source(raw: str) -> str:
    """Normalise a source string into a Markdown link when possible."""
    if not raw or raw.startswith("["):
        return raw
    # "github.com/user/repo #NN" or "github.com/user/repo NN"
    m = re.match(r"(github\.com/[^#\s]+)\s*#?(\d+)", raw)
    if m:
        repo, num = m.groups()
        return f"[#{num}](https://{repo}/issues/{num})"
    return raw


def parse_legacy_readme(text: str) -> dict | None:
    """Parse an old append-only README and return a task-log dict."""
    lines = text.split("\n")
    task_id = None
    title = None
    task_text = None
    source = None
    working_dir = None
    runs = []

    i = 0
    while i < len(lines):
        line = lines[i]
        m = _HEADER_RE.match(line)
        if m:
            task_id = int(m.group(1))
            title = m.group(2).strip()
            i += 1
            continue
        m = _TASK_LINE_RE.match(line)
        if m:
            task_text = m.group(1).strip()
            i += 1
            continue
        m = _SOURCE_LINE_RE.match(line)
        if m:
            source = m.group(1).strip()
            i += 1
            continue
        m = _FOLDER_LINE_RE.match(line)
        if m:
            working_dir = m.group(1).strip()
            i += 1
            continue
        if _RUN_HEADER_RE.match(line):
            break
        i += 1

    if task_id is None:
        return None

    while i < len(lines):
        line = lines[i]
        m = _RUN_HEADER_RE.match(line)
        if not m:
            i += 1
            continue

        phase = m.group(1).strip()
        stamp = m.group(2).strip()
        agent = ""
        run_slug = None
        outcome = None
        summary = ""
        work = ""
        review_links: list[str] = []
        questions: list[str] = []
        external_actions: list[str] = []
        deliverables: list[str] = []
        in_result = False
        in_review_links = False
        in_needs_input = False
        in_external_actions = False
        in_deliverables = False
        in_work = False

        i += 1
        while i < len(lines):
            cur = lines[i]

            # Next run or top-level heading
            if re.match(r"^## ", cur) or re.match(r"^# [^#]", cur):
                break

            m2 = _AGENT_RE.match(cur)
            if m2:
                agent = m2.group(1).strip()
                i += 1
                continue

            m2 = _RUN_PATH_RE.match(cur)
            if m2:
                run_slug = _strip_backticks(m2.group(1)).split("/")[-1]
                i += 1
                continue

            if _RESULT_HEADER.match(cur):
                in_result = True
                i += 1
                continue

            if in_result:
                m2 = _OUTCOME_RE.match(cur)
                if m2:
                    outcome = m2.group(1).strip()
                    i += 1
                    continue
                m2 = _SUMMARY_RE.match(cur)
                if m2:
                    summary = m2.group(1).strip()
                    i += 1
                    continue

                # Collection headers
                if _REVIEW_LINKS_HEADER.match(cur):
                    in_review_links = True
                    in_work = in_needs_input = in_external_actions = in_deliverables = False
                    i += 1
                    continue
                if _NEEDS_INPUT_HEADER.match(cur):
                    in_needs_input = True
                    in_review_links = in_work = in_external_actions = in_deliverables = False
                    i += 1
                    continue
                if _EXTERNAL_ACTIONS_HEADER.match(cur):
                    in_external_actions = True
                    in_review_links = in_work = in_needs_input = in_deliverables = False
                    i += 1
                    continue
                if _DELIVERABLES_HEADER.match(cur):
                    in_deliverables = True
                    in_review_links = in_work = in_needs_input = in_external_actions = False
                    i += 1
                    continue
                if _WORK_HEADER.match(cur):
                    in_work = True
                    in_review_links = in_needs_input = in_external_actions = in_deliverables = False
                    i += 1
                    continue

                # Collect list items
                collecting = in_review_links or in_needs_input or in_external_actions or in_deliverables
                if cur.strip().startswith("- ") and collecting:
                    item = cur.strip()[2:].strip()
                    if in_review_links:
                        review_links.append(item)
                    elif in_needs_input:
                        questions.append(item)
                    elif in_external_actions:
                        external_actions.append(item)
                    elif in_deliverables:
                        deliverables.append(item)
                    i += 1
                    continue

                # Non-list line ends a collection
                if cur.strip() and not cur.strip().startswith("-") and collecting:
                    in_review_links = in_needs_input = in_external_actions = in_deliverables = False

                # Work block
                if in_work:
                    if cur.strip():
                        work += cur + "\n"
                    i += 1
                    continue

            i += 1

        run_entry = {
            "stamp": stamp,
            "phase": phase,
            "agent": agent,
            "run": run_slug,
            "outcome": outcome,
            "summary": summary,
        }
        if work.strip():
            run_entry["work"] = work.strip()
        if review_links:
            run_entry["review_links"] = review_links
        if questions:
            run_entry["questions"] = questions
        if external_actions:
            run_entry["external_actions"] = external_actions
        if deliverables:
            run_entry["deliverables"] = deliverables

        runs.append(run_entry)
        i += 1

    if task_text is None:
        task_text = title or ""

    return {
        "task_id": task_id,
        "task_text": task_text,
        "source": _format_source(source or ""),
        "working_directory": working_dir,
        "runs": runs,
    }


# ---------------------------------------------------------------------------
# Conversion entry point
# ---------------------------------------------------------------------------

def convert_task_directory(task_dir: Path) -> str:
    """Convert one task directory. Returns status string."""
    readme_path = task_dir / "README.md"
    log_path = task_dir / ".task-log.json"

    if log_path.exists():
        return "skip"
    if not readme_path.exists():
        return "skip"

    text = readme_path.read_text(encoding="utf-8")
    parsed = parse_legacy_readme(text)
    if parsed is None:
        return f"error: could not parse README"

    if not parsed.get("working_directory"):
        parsed["working_directory"] = str(task_dir)

    log = {
        "schema": "foxhound.task-log",
        "schema_version": 1,
        "task_id": parsed["task_id"],
        "task_text": parsed["task_text"],
        "source": parsed.get("source", ""),
        "working_directory": parsed["working_directory"],
        "runs": parsed["runs"],
    }

    log_path.write_text(
        json.dumps(log, ensure_ascii=False, indent=1, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    rendered = _render_task_document(log)
    readme_path.write_text(rendered, encoding="utf-8")

    return "ok"


def main() -> int:
    if len(sys.argv) < 2:
        print("Usage: convert-legacy-tasks.py [--all TASKS_ROOT] [TASK_DIR ...]", file=sys.stderr)
        return 1

    if sys.argv[1] == "--all":
        if len(sys.argv) < 3:
            print("Usage: convert-legacy-tasks.py --all /path/to/Tasks/", file=sys.stderr)
            return 1
        root = Path(sys.argv[2])
        if not root.is_dir():
            print(f"Error: {root} is not a directory", file=sys.stderr)
            return 1
        targets = sorted(
            d for d in root.iterdir()
            if d.is_dir() and not d.is_symlink() and d.name.startswith("T")
        )
    else:
        targets = [Path(a) for a in sys.argv[1:]]

    converted = 0
    skipped = 0
    errors = 0
    for task_dir in targets:
        if not task_dir.is_dir():
            print(f"SKIP: Not a directory: {task_dir}", file=sys.stderr)
            errors += 1
            continue
        try:
            status = convert_task_directory(task_dir)
            if status == "ok":
                print(f"CONVERTED: {task_dir.name}")
                converted += 1
            elif status == "skip":
                skipped += 1
            else:
                print(f"ERROR: {task_dir.name}: {status}", file=sys.stderr)
                errors += 1
        except Exception as exc:
            print(f"ERROR: {task_dir.name}: {exc}", file=sys.stderr)
            errors += 1

    print(f"\nDone: {converted} converted, {skipped} skipped, {errors} errors")
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
