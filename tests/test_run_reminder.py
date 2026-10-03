"""Tests for foxhound-run-reminder."""

from __future__ import annotations

import io
import json
import os
import sqlite3
from contextlib import closing
from pathlib import Path
from unittest.mock import patch

import pytest

from foxhound import run_reminder
from foxhound.execution_worker import RUN_STATE_SCHEMA_VERSION, STATE_ENV


def _init_dummy_db(db_path: Path) -> None:
    with closing(sqlite3.connect(db_path)) as connection:
        connection.execute(
            "CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT)"
        )
        connection.commit()
    db_path.chmod(0o600)


def _write_state_file(
    path: Path,
    *,
    db_path: Path,
    task_work_dir: Path | None = None,
    task_kb_file: Path | None = None,
    task_run_dir: Path | None = None,
    pass_budget_seconds: int | None = 3600,
    pass_deadline: str | None = "2030-01-02T04:00:00+00:00",
    schema_version: int = RUN_STATE_SCHEMA_VERSION,
    phase: str = "plan",
) -> None:
    if task_work_dir is not None:
        if task_kb_file is None:
            task_kb_file = task_work_dir / "task.md"
            if not task_kb_file.exists():
                task_kb_file.write_text("# Task\n", encoding="utf-8")
        if task_run_dir is None:
            task_run_dir = task_work_dir / "runs" / ("0" * 32)
            task_run_dir.mkdir(parents=True, exist_ok=True)

    document = {
        "schema": "foxhound.execution-run-state",
        "schema_version": schema_version,
        "run_id": "0" * 32,
        "database_path": str(db_path),
        "task_id": 1,
        "task_version": 1,
        "workflow_version": 1,
        "phase": phase,
        "claim_token": "a" * 32,
        "lease_seconds": 60,
        "agent_profile_id": "profile-a",
        "agent_profile_revision": "b" * 64,
        "knowledge_root": None,
        "worker_command": "foxhound-task-worker",
        "task_work_directory": str(task_work_dir) if task_work_dir else None,
        "task_kb_file": str(task_kb_file) if task_kb_file else None,
        "task_run_directory": str(task_run_dir) if task_run_dir else None,
        "execution_grants": [],
        "action_grants": [],
        "deployment_roots": {},
        "pass_budget_seconds": pass_budget_seconds,
        "pass_deadline": pass_deadline,
    }
    path.write_text(json.dumps(document), encoding="utf-8")
    path.chmod(0o600)


def test_outside_a_run_when_env_unset(capsys: pytest.CaptureFixture[str]) -> None:
    with patch.dict(os.environ, {}, clear=True):
        code = run_reminder.main([])
    assert code == 0
    captured = capsys.readouterr()
    assert json.loads(captured.out) == {}


def test_unreadable_state(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    bad_state = tmp_path / "nonexistent.json"
    with patch.dict(os.environ, {STATE_ENV: str(bad_state)}):
        code = run_reminder.main([])
    assert code == 0
    captured = capsys.readouterr()
    assert json.loads(captured.out) == {}


def test_corrupt_state_json(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    bad_state = tmp_path / "corrupt.json"
    bad_state.write_text("not json", encoding="utf-8")
    bad_state.chmod(0o600)
    with patch.dict(os.environ, {STATE_ENV: str(bad_state)}):
        code = run_reminder.main([])
    assert code == 0
    captured = capsys.readouterr()
    assert json.loads(captured.out) == {}


def test_before_one_third(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    db = tmp_path / "test.db"
    _init_dummy_db(db)
    task_dir = tmp_path / "task"
    task_dir.mkdir()
    state_file = tmp_path / "state.json"
    _write_state_file(state_file, db_path=db, task_work_dir=task_dir, pass_budget_seconds=3600)

    # Set state file mtime to 1000.0
    os.utime(state_file, (1000.0, 1000.0))

    # Elapsed 600s out of 3600s -> f = 600/3600 = 0.166 < 1/3
    with patch.object(run_reminder, "_now", return_value=1600.0):
        with patch.dict(os.environ, {STATE_ENV: str(state_file)}):
            code = run_reminder.main([])

    assert code == 0
    captured = capsys.readouterr()
    assert json.loads(captured.out) == {}


def test_one_third_without_note(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    db = tmp_path / "test.db"
    _init_dummy_db(db)
    task_dir = tmp_path / "task"
    task_dir.mkdir()
    state_file = tmp_path / "state.json"
    _write_state_file(state_file, db_path=db, task_work_dir=task_dir, pass_budget_seconds=3600)

    os.utime(state_file, (1000.0, 1000.0))

    # Elapsed 1500s out of 3600s -> f = 1500/3600 = 0.416 (between 1/3 and 2/3)
    with patch.object(run_reminder, "_now", return_value=2500.0):
        with patch.dict(os.environ, {STATE_ENV: str(state_file)}):
            code = run_reminder.main([])

    assert code == 0
    captured = capsys.readouterr()
    data = json.loads(captured.out)
    expected_path = str(task_dir / "handoff-plan.md")
    assert f"at {expected_path}" in data["context"]
    assert "write your handoff note now" in data["context"]


def test_one_third_with_preexisting_note_before_claim(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    # A note modified BEFORE claim start should not count as written during this claim
    db = tmp_path / "test.db"
    _init_dummy_db(db)
    task_dir = tmp_path / "task"
    task_dir.mkdir()
    note_file = task_dir / "handoff-plan.md"
    note_file.write_text("old note", encoding="utf-8")
    os.utime(note_file, (500.0, 500.0))

    state_file = tmp_path / "state.json"
    _write_state_file(state_file, db_path=db, task_work_dir=task_dir, pass_budget_seconds=3600)
    os.utime(state_file, (1000.0, 1000.0))

    with patch.object(run_reminder, "_now", return_value=2500.0):
        with patch.dict(os.environ, {STATE_ENV: str(state_file)}):
            code = run_reminder.main([])

    assert code == 0
    captured = capsys.readouterr()
    data = json.loads(captured.out)
    assert "write your handoff note now" in data["context"]


def test_one_third_with_note_present_during_claim(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    # If a note is written during this claim, 1/3 check is satisfied -> returns {}
    db = tmp_path / "test.db"
    _init_dummy_db(db)
    task_dir = tmp_path / "task"
    task_dir.mkdir()
    state_file = tmp_path / "state.json"
    _write_state_file(state_file, db_path=db, task_work_dir=task_dir, pass_budget_seconds=3600)
    os.utime(state_file, (1000.0, 1000.0))

    note_file = task_dir / "handoff-plan.md"
    note_file.write_text("current note", encoding="utf-8")
    os.utime(note_file, (1200.0, 1200.0))

    with patch.object(run_reminder, "_now", return_value=2500.0):
        with patch.dict(os.environ, {STATE_ENV: str(state_file)}):
            code = run_reminder.main([])

    assert code == 0
    captured = capsys.readouterr()
    assert json.loads(captured.out) == {}


def test_two_thirds_stale_note(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    db = tmp_path / "test.db"
    _init_dummy_db(db)
    task_dir = tmp_path / "task"
    task_dir.mkdir()
    state_file = tmp_path / "state.json"
    _write_state_file(state_file, db_path=db, task_work_dir=task_dir, pass_budget_seconds=3600)
    os.utime(state_file, (1000.0, 1000.0))

    # Note written at 1100.0 (during claim)
    note_file = task_dir / "handoff-plan.md"
    note_file.write_text("current note", encoding="utf-8")
    os.utime(note_file, (1100.0, 1100.0))

    # Elapsed 2600s -> f = 2600/3600 = 0.722 (between 2/3 and 0.85).
    # Now is 3600.0; note is at 1100.0; 3600 - 1100 = 2500s > 600s (stale).
    with patch.object(run_reminder, "_now", return_value=3600.0):
        with patch.dict(os.environ, {STATE_ENV: str(state_file)}):
            code = run_reminder.main([])

    assert code == 0
    captured = capsys.readouterr()
    data = json.loads(captured.out)
    assert "update your handoff note" in data["context"]
    expected_path = str(task_dir / "handoff-plan.md")
    assert expected_path in data["context"]


def test_two_thirds_fresh_note(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    db = tmp_path / "test.db"
    _init_dummy_db(db)
    task_dir = tmp_path / "task"
    task_dir.mkdir()
    state_file = tmp_path / "state.json"
    _write_state_file(state_file, db_path=db, task_work_dir=task_dir, pass_budget_seconds=3600)
    os.utime(state_file, (1000.0, 1000.0))

    # Elapsed 2600s -> f = 2600/3600 = 0.722. Now = 3600.0.
    # Note updated at 3300.0 (300s ago <= 600s, not stale).
    note_file = task_dir / "handoff-plan.md"
    note_file.write_text("fresh note", encoding="utf-8")
    os.utime(note_file, (3300.0, 3300.0))

    with patch.object(run_reminder, "_now", return_value=3600.0):
        with patch.dict(os.environ, {STATE_ENV: str(state_file)}):
            code = run_reminder.main([])

    assert code == 0
    captured = capsys.readouterr()
    assert json.loads(captured.out) == {}


def test_eighty_five_percent_wins(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    db = tmp_path / "test.db"
    _init_dummy_db(db)
    task_dir = tmp_path / "task"
    task_dir.mkdir()
    state_file = tmp_path / "state.json"
    _write_state_file(state_file, db_path=db, task_work_dir=task_dir, pass_budget_seconds=3600)
    os.utime(state_file, (1000.0, 1000.0))

    # Elapsed 3100s -> f = 3100/3600 = 0.861 >= 0.85
    # Remaining: 500s -> ceil(500/60) = 9 minutes
    with patch.object(run_reminder, "_now", return_value=4100.0):
        with patch.dict(os.environ, {STATE_ENV: str(state_file)}):
            code = run_reminder.main([])

    assert code == 0
    captured = capsys.readouterr()
    data = json.loads(captured.out)
    assert "about 9 minutes remain in this pass" in data["context"]
    assert "record --outcome OUTCOME" in data["context"]
    assert "release --handoff" in data["context"]


def test_eighty_five_percent_remaining_minutes_min_one(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db = tmp_path / "test.db"
    _init_dummy_db(db)
    task_dir = tmp_path / "task"
    task_dir.mkdir()
    state_file = tmp_path / "state.json"
    _write_state_file(state_file, db_path=db, task_work_dir=task_dir, pass_budget_seconds=3600)
    os.utime(state_file, (1000.0, 1000.0))

    # Elapsed 3595s -> 5s remain -> ceil(5/60) = 1
    with patch.object(run_reminder, "_now", return_value=4595.0):
        with patch.dict(os.environ, {STATE_ENV: str(state_file)}):
            code = run_reminder.main([])

    assert code == 0
    captured = capsys.readouterr()
    data = json.loads(captured.out)
    assert "about 1 minutes remain in this pass" in data["context"]


def test_derive_budget_from_deadline(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    db = tmp_path / "test.db"
    _init_dummy_db(db)
    task_dir = tmp_path / "task"
    task_dir.mkdir()
    state_file = tmp_path / "state.json"
    # When pass_budget_seconds is None on ExecutionRunState (e.g. from an earlier schema or custom object),
    # budget is derived from pass_deadline - claim_start_seconds.
    _write_state_file(
        state_file,
        db_path=db,
        task_work_dir=task_dir,
        pass_budget_seconds=4000,
        pass_deadline="1970-01-01T01:23:20+00:00",  # 5000s after epoch
    )
    # Claim start = 1000.0 -> budget = 5000 - 1000 = 4000.0
    os.utime(state_file, (1000.0, 1000.0))

    # Mock load_run_state to return pass_budget_seconds=None to test the fallback branch
    real_load = run_reminder.load_run_state(state_file)
    object.__setattr__(real_load, "pass_budget_seconds", None)

    # Now = 4500.0 -> elapsed = 3500.0 -> f = 3500/4000 = 0.875 >= 0.85
    with patch.object(run_reminder, "load_run_state", return_value=real_load):
        with patch.object(run_reminder, "_now", return_value=4500.0):
            with patch.dict(os.environ, {STATE_ENV: str(state_file)}):
                code = run_reminder.main([])

    assert code == 0
    captured = capsys.readouterr()
    data = json.loads(captured.out)
    assert "about 9 minutes remain in this pass" in data["context"]


def test_no_task_folder(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    db = tmp_path / "test.db"
    _init_dummy_db(db)
    state_file = tmp_path / "state.json"
    _write_state_file(state_file, db_path=db, task_work_dir=None, pass_budget_seconds=3600)
    os.utime(state_file, (1000.0, 1000.0))

    # Elapsed 1500s -> f = 0.416 >= 1/3 and no task folder
    with patch.object(run_reminder, "_now", return_value=2500.0):
        with patch.dict(os.environ, {STATE_ENV: str(state_file)}):
            code = run_reminder.main([])

    assert code == 0
    captured = capsys.readouterr()
    data = json.loads(captured.out)
    assert "in the task folder" in data["context"]
    assert "at /" not in data["context"]


def test_main_tolerates_stdin(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    monkeypatch.setattr("sys.stdin", io.StringIO('{"hook": "payload"}'))
    with patch.dict(os.environ, {}, clear=True):
        code = run_reminder.main([])
    assert code == 0
    captured = capsys.readouterr()
    assert json.loads(captured.out) == {}
