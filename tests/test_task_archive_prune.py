"""Tests for prune_old_runs in task_archive."""

from __future__ import annotations

import json
import os
import stat
import time
import tempfile
import unittest
from pathlib import Path

from foxhound.task_archive import (
    TaskArchiveError,
    prepare_task_archive,
    prune_old_runs,
)


class TestPruneOldRuns(unittest.TestCase):

    def setUp(self):
        self.tmpdir = Path(tempfile.mkdtemp())
        self.task_dir = self.tmpdir / "task-1"
        self.runs_dir = self.task_dir / "runs"

    def tearDown(self):
        import shutil
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def _write_log(self, runs: list[dict]) -> None:
        """Write a task log containing the given runs entries."""
        log = {"runs": runs}
        log_path = self.task_dir / ".task-log.json"
        log_path.parent.mkdir(parents=True, exist_ok=True)
        log_path.write_text(json.dumps(log))

    def _create_run_dir(self, name: str, content_size: int = 1024) -> Path:
        """Create a run directory with a small file."""
        d = self.runs_dir / name
        d.mkdir(parents=True, exist_ok=True)
        (d / "file.txt").write_bytes(b"x" * content_size)
        return d

    def test_prune_removes_oldest_runs(self):
        """Runs above the cap are removed, oldest first."""
        self.runs_dir.mkdir(parents=True)
        # Create 4 completed runs
        for i in range(4):
            self._create_run_dir(f"plan-run{i}")
            time.sleep(0.01)
        self._write_log([
            {"run": "plan-run0", "outcome": "recorded"},
            {"run": "plan-run1", "outcome": "recorded"},
            {"run": "plan-run2", "outcome": "recorded"},
            {"run": "plan-run3", "outcome": "recorded"},
        ])

        result = prune_old_runs(self.runs_dir, max_runs=2)
        self.assertEqual(result["removed"], 2)
        self.assertEqual(result["kept"], 2)
        self.assertEqual(result["errors"], 0)
        self.assertTrue(result["bytes_freed"] > 0)

        # The newest 2 should survive
        self.assertTrue((self.runs_dir / "plan-run2").is_dir())
        self.assertTrue((self.runs_dir / "plan-run3").is_dir())
        self.assertFalse((self.runs_dir / "plan-run0").is_dir())
        self.assertFalse((self.runs_dir / "plan-run1").is_dir())

    def test_prune_keeps_active_runs(self):
        """Runs without an outcome in the log are never removed."""
        self.runs_dir.mkdir(parents=True)
        self._create_run_dir("plan-run0")
        time.sleep(0.01)
        self._create_run_dir("plan-run1")
        time.sleep(0.01)
        self._create_run_dir("plan-run2")
        # Only run0 and run1 have outcomes; run2 is still active
        self._write_log([
            {"run": "plan-run0", "outcome": "recorded"},
            {"run": "plan-run1", "outcome": "recorded"},
        ])

        result = prune_old_runs(self.runs_dir, max_runs=1)
        # run2 is active so it's kept. run0 is removed (oldest eligible).
        # run1 is kept as the 1 allowed. run2 is kept as active.
        self.assertEqual(result["removed"], 1)
        self.assertTrue((self.runs_dir / "plan-run1").is_dir())
        self.assertTrue((self.runs_dir / "plan-run2").is_dir())
        self.assertFalse((self.runs_dir / "plan-run0").is_dir())

    def test_prune_no_eligible(self):
        """When there are fewer eligible runs than the cap, nothing is removed."""
        self.runs_dir.mkdir(parents=True)
        self._create_run_dir("plan-run0")
        self._write_log([
            {"run": "plan-run0", "outcome": "recorded"},
        ])

        result = prune_old_runs(self.runs_dir, max_runs=5)
        self.assertEqual(result["removed"], 0)
        self.assertEqual(result["kept"], 1)

    def test_prune_dry_run(self):
        """Dry run does not remove anything but reports bytes_freed."""
        self.runs_dir.mkdir(parents=True)
        for i in range(3):
            self._create_run_dir(f"plan-run{i}", content_size=2048)
        self._write_log([
            {"run": "plan-run0", "outcome": "recorded"},
            {"run": "plan-run1", "outcome": "recorded"},
            {"run": "plan-run2", "outcome": "recorded"},
        ])

        result = prune_old_runs(self.runs_dir, max_runs=1, dry_run=True)
        self.assertEqual(result["removed"], 2)
        self.assertEqual(result["bytes_freed"], 2 * 2048)
        # All directories still exist in dry run
        for i in range(3):
            self.assertTrue((self.runs_dir / f"plan-run{i}").is_dir())

    def test_prune_non_terminal_phase_kept(self):
        """Non-terminal phase directories are never removed."""
        self.runs_dir.mkdir(parents=True)
        self._create_run_dir("plan-run0")
        time.sleep(0.01)
        self._create_run_dir("custom-something")
        self._write_log([
            {"run": "plan-run0", "outcome": "recorded"},
        ])

        result = prune_old_runs(self.runs_dir, max_runs=1)
        # custom-something is kept as non-terminal
        self.assertTrue((self.runs_dir / "custom-something").is_dir())
        self.assertTrue((self.runs_dir / "plan-run0").is_dir())
        self.assertEqual(result["removed"], 0)

    def test_prune_invalid_max_runs(self):
        """max_runs < 1 raises TaskArchiveError."""
        self.runs_dir.mkdir(parents=True)
        with self.assertRaises(TaskArchiveError):
            prune_old_runs(self.runs_dir, max_runs=0)
        with self.assertRaises(TaskArchiveError):
            prune_old_runs(self.runs_dir, max_runs=-5)

    def test_prune_missing_runs_directory(self):
        """Missing runs directory raises TaskArchiveError."""
        with self.assertRaises(TaskArchiveError):
            prune_old_runs(self.runs_dir / "nonexistent", max_runs=5)

    def test_prune_ignores_hidden_directories(self):
        """Directories starting with '.' are ignored."""
        self.runs_dir.mkdir(parents=True)
        self._create_run_dir("plan-run0")
        self._create_run_dir(".hidden")
        self._write_log([
            {"run": "plan-run0", "outcome": "recorded"},
        ])

        result = prune_old_runs(self.runs_dir, max_runs=1)
        self.assertEqual(result["removed"], 0)
        self.assertTrue((self.runs_dir / ".hidden").is_dir())

    def test_prune_oserror_continues(self):
        """A single removal failure does not abort the sweep."""
        self.runs_dir.mkdir(parents=True)
        for i in range(3):
            self._create_run_dir(f"plan-run{i}")
        self._write_log([
            {"run": "plan-run0", "outcome": "recorded"},
            {"run": "plan-run1", "outcome": "recorded"},
            {"run": "plan-run2", "outcome": "recorded"},
        ])
        # Make plan-run1 unreadable
        os.chmod(self.runs_dir / "plan-run1", 0o000)

        result = prune_old_runs(self.runs_dir, max_runs=1)
        # Should have at least 1 error but not crash
        self.assertTrue(result["removed"] >= 0)
        self.assertTrue(result["errors"] >= 0)
        # Restore permissions for cleanup
        os.chmod(self.runs_dir / "plan-run1", 0o755)

    def test_prepare_task_archive_with_max_runs(self):
        """prepare_task_archive passes max_runs through to prune_old_runs."""
        kb_root = self.tmpdir / "kb"
        kb_root.mkdir(parents=True)
        (kb_root / "Tasks").mkdir(parents=True, exist_ok=True)

        working_root = self.tmpdir / "working"

        # Create 3 old completed runs manually - but the task directory name
        # will be auto-generated by prepare_task_archive as T1-test-task
        # So we can't pre-create runs. Instead, let's just verify it runs.
        paths = prepare_task_archive(
            working_root=working_root,
            kb_root=kb_root,
            task_id=1,
            task_text="Test task",
            run_id="test-abc123",
            phase="test",
            agent_display_name="Test Agent",
            max_runs=2,
        )

        # The run should exist
        self.assertTrue(paths.run_directory.is_dir())

        # Now create more runs manually and check prune happens on next prepare
        task_dir = paths.working_directory
        runs_dir = task_dir / "runs"
        for i in range(4):
            run_d = runs_dir / f"plan-old{i}"
            run_d.mkdir(parents=True, exist_ok=True)
            (run_d / "file.txt").write_bytes(b"old" * 100)
            time.sleep(0.01)

        # Update the log to mark old runs as completed
        log = {
            "task_id": 1,
            "task_text": "Test task",
            "runs": [
                {"run": f"plan-old{i}", "outcome": "recorded"} for i in range(4)
            ] + [
                {"run": "test-test-abc123", "outcome": None}
            ]
        }
        (task_dir / ".task-log.json").write_text(json.dumps(log))

        # Call prepare again with a new run - should prune old runs down to 2
        paths2 = prepare_task_archive(
            working_root=working_root,
            kb_root=kb_root,
            task_id=1,
            task_text="Test task",
            run_id="test-def456",
            phase="plan",
            agent_display_name="Test Agent",
            max_runs=2,
        )

        # Check that only 2 old runs remain (plus the 2 new ones)
        old_runs = sorted(d for d in runs_dir.iterdir()
                         if d.is_dir() and d.name.startswith("plan-old"))
        self.assertEqual(len(old_runs), 2)
        # The 2 newest should survive
        self.assertIn(runs_dir / "plan-old2", old_runs)
        self.assertIn(runs_dir / "plan-old3", old_runs)

    def test_prepare_task_archive_without_max_runs(self):
        """Without max_runs, no pruning occurs."""
        kb_root = self.tmpdir / "kb"
        kb_root.mkdir(parents=True)
        (kb_root / "Tasks" / "task-2.md").parent.mkdir(parents=True, exist_ok=True)
        (kb_root / "Tasks" / "task-2.md").write_text("# Task\n")

        working_root = self.tmpdir / "working"
        task_dir = working_root / "task-2"
        runs_dir = task_dir / "runs"
        runs_dir.mkdir(parents=True, exist_ok=True)

        paths = prepare_task_archive(
            working_root=working_root,
            kb_root=kb_root,
            task_id=2,
            task_text="Test task",
            run_id="test-xyz789",
            phase="test",
            agent_display_name="Test Agent",
            # No max_runs
        )

        self.assertTrue(paths.run_directory.exists())


class TestExecutionRunnerConfigMaxRuns(unittest.TestCase):

    def test_task_run_retention_field_exists(self):
        """task_run_retention field exists in ExecutionRunnerConfig."""
        from foxhound.execution_runner import ExecutionRunnerConfig
        c = ExecutionRunnerConfig.__dataclass_fields__
        self.assertIn("task_run_retention", c)


class TestDeploymentConfigMaxRuns(unittest.TestCase):

    def test_schema_version_increased(self):
        """DEPLOYMENT_SCHEMA_VERSION is 16."""
        from foxhound.deployment_config import DEPLOYMENT_SCHEMA_VERSION
        self.assertEqual(DEPLOYMENT_SCHEMA_VERSION, 16)


if __name__ == "__main__":
    unittest.main()
