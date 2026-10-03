"""Runtime reminder hook for running agent passes.

This module provides the `foxhound-run-reminder` CLI command, designed to be
invoked as a `pre_llm_call` hook by the agent runtime (e.g. Hermes).

The reminder alerts the agent:
- at 1/3 of the pass budget if no handoff note has been written for this claim yet;
- at 2/3 of the pass budget if the handoff note has not been updated in the
  last 10 minutes;
- at 85% of the pass budget to record the outcome or release with handoff before
  the budget expires.
"""

from __future__ import annotations

import json
import math
import os
import sys
import time
from pathlib import Path

from .execution_worker import STATE_ENV, _claim_started_at, load_run_state
from .task_archive import locate_handoff

_now = time.time


def _compute_reminder(now: float) -> dict[str, str]:
    state_env_val = os.environ.get(STATE_ENV)
    if not state_env_val:
        return {}

    state_path = Path(state_env_val)
    claim_start_ns = _claim_started_at(state_path)
    if claim_start_ns is None:
        return {}
    claim_start_seconds = claim_start_ns / 1_000_000_000.0

    try:
        run_state = load_run_state(state_path)
    except Exception:
        return {}

    budget_seconds: float | None = None
    if run_state.pass_budget_seconds is not None:
        budget_seconds = float(run_state.pass_budget_seconds)
    elif run_state.pass_deadline is not None:
        budget_seconds = run_state.pass_deadline.timestamp() - claim_start_seconds

    if budget_seconds is None or budget_seconds <= 0:
        return {}

    elapsed = now - claim_start_seconds
    fraction = elapsed / budget_seconds

    phase_str = run_state.phase.value if hasattr(run_state.phase, "value") else str(run_state.phase)
    task_work_dir = run_state.task_work_directory

    if fraction >= 0.85:
        remaining_seconds = max(0.0, budget_seconds - elapsed)
        remaining_minutes = max(1, math.ceil(remaining_seconds / 60.0))
        return {
            "context": (
                f"Foxhound reminder: about {remaining_minutes} minutes remain in this pass. "
                "Record your result now with `record --outcome OUTCOME`, "
                "or update your handoff note and call `release --handoff`."
            )
        }

    handoff_note_path: Path | None = None
    if task_work_dir:
        handoff_note_path = locate_handoff(Path(task_work_dir), phase_str)

    note_exists_for_claim = False
    note_mtime_seconds: float | None = None
    if handoff_note_path is not None:
        try:
            mtime_ns = handoff_note_path.stat().st_mtime_ns
            note_mtime_seconds = mtime_ns / 1_000_000_000.0
            if mtime_ns >= claim_start_ns:
                note_exists_for_claim = True
        except OSError:
            handoff_note_path = None

    if task_work_dir:
        path_str = str(Path(task_work_dir) / f"handoff-{phase_str}.md")
        path_instruction = f"at {path_str}"
    else:
        path_str = ""
        path_instruction = "in the task folder"

    if fraction >= 2.0 / 3.0:
        if note_exists_for_claim and note_mtime_seconds is not None:
            if now - note_mtime_seconds > 600.0:
                if task_work_dir:
                    return {
                        "context": (
                            f"Foxhound reminder: update your handoff note at {path_str} "
                            "with what changed since you last wrote it."
                        )
                    }
                else:
                    return {
                        "context": (
                            "Foxhound reminder: update your handoff note in the task folder "
                            "with what changed since you last wrote it."
                        )
                    }

    if fraction >= 1.0 / 3.0:
        if not note_exists_for_claim:
            return {
                "context": (
                    f"Foxhound reminder: write your handoff note now {path_instruction}: "
                    "what you established (with absolute paths), what changed and where, "
                    "what remains, and the next step."
                )
            }

    return {}


def main(argv: list[str] | None = None) -> int:
    try:
        try:
            sys.stdin.read()
        except Exception:
            pass

        result = _compute_reminder(_now())
        sys.stdout.write(json.dumps(result, sort_keys=True) + "\n")
        sys.stdout.flush()
        return 0
    except Exception:
        try:
            sys.stdout.write("{}\n")
            sys.stdout.flush()
        except Exception:
            pass
        return 0


if __name__ == "__main__":
    sys.exit(main())
