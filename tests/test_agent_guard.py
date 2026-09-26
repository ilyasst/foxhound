"""Synthetic tests for the Agent Guard hook installation and ref protection."""

from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

INSTALL_SCRIPT = (
    Path(__file__).resolve().parents[1] / "tools" / "install-agent-guard.sh"
)


# Environment variables that mark an unattended agent session.
AGENT_MARKER_VARS = frozenset({
    "HERMES_CRON_SESSION",
    "HERMES_SESSION_SOURCE",
    "FOXHOUND_WORKFLOW_STATE",
})

# Git environment variables that leak parent repository state into child git commands.
GIT_LEAK_VARS = frozenset({
    "GIT_DIR",
    "GIT_WORK_TREE",
    "GIT_INDEX_FILE",
    "GIT_OBJECT_DIRECTORY",
    "GIT_COMMON_DIR",
    "GIT_PREFIX",
})


class AgentGuardTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.repo = self.root / "example-repo"
        self.repo.mkdir(mode=0o700)
        self._git(["init", "-b", "main"], cwd=self.repo)
        self._git(["config", "user.name", "Person A"], cwd=self.repo)
        self._git(["config", "user.email", "person-a@example.com"], cwd=self.repo)
        (self.repo / "file.txt").write_text("initial", encoding="utf-8")
        self._git(["add", "file.txt"], cwd=self.repo)
        self._git(["commit", "-m", "initial commit"], cwd=self.repo)

    def _base_env(self) -> dict[str, str]:
        """Return environment stripped of agent markers and git repository pointers."""
        env = dict(os.environ)
        for var in AGENT_MARKER_VARS | GIT_LEAK_VARS:
            env.pop(var, None)
        return env

    def _git(self, args: list[str], *, cwd: Path, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
        full_env = self._base_env()
        if env is not None:
            full_env.update(env)
        return subprocess.run(
            ["git", *args],
            cwd=cwd,
            env=full_env,
            capture_output=True,
            text=True,
            check=False,
        )

    def _install_guard(self, cwd: Path) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [str(INSTALL_SCRIPT)],
            cwd=cwd,
            env=self._base_env(),
            capture_output=True,
            text=True,
            check=False,
        )

    def test_installation_in_standard_checkout_and_idempotence(self) -> None:
        result = self._install_guard(self.repo)
        self.assertEqual(result.returncode, 0)
        self.assertIn("Agent Guard installed.", result.stdout)

        hook_path = self.repo / ".git" / "hooks" / "reference-transaction"
        self.assertTrue(hook_path.is_file())
        self.assertTrue(os.access(hook_path, os.X_OK))

        second = self._install_guard(self.repo)
        self.assertEqual(second.returncode, 0)
        self.assertIn("Agent Guard is already installed.", second.stdout)

    def test_installation_respects_core_hooks_path_and_preserves_existing_hook(self) -> None:
        hooks_dir = self.repo / "tools" / "hooks"
        hooks_dir.mkdir(parents=True, mode=0o700)
        pre_commit = hooks_dir / "pre-commit"
        pre_commit.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
        pre_commit.chmod(0o755)

        self._git(["config", "core.hooksPath", "tools/hooks"], cwd=self.repo)

        result = self._install_guard(self.repo)
        self.assertEqual(result.returncode, 0)

        guard_hook = hooks_dir / "reference-transaction"
        self.assertTrue(guard_hook.is_file())
        self.assertTrue(os.access(guard_hook, os.X_OK))
        self.assertTrue(pre_commit.is_file())

    def test_installation_works_in_git_worktree(self) -> None:
        worktree_path = self.root / "worktree"
        self._git(
            ["worktree", "add", str(worktree_path), "-b", "feature-worktree"],
            cwd=self.repo,
        )

        result = self._install_guard(worktree_path)
        self.assertEqual(result.returncode, 0)
        self.assertIn("Agent Guard installed.", result.stdout)

    def test_unattended_agent_ref_updates_are_refused_and_logged(self) -> None:
        self._install_guard(self.repo)

        agent_cwd = self.root / "agent-run-dir"
        agent_cwd.mkdir(mode=0o700)
        agent_env = {
            "HERMES_CRON_SESSION": "1",
            "TERMINAL_CWD": str(agent_cwd),
        }

        # 1. Commit refusal
        (self.repo / "file.txt").write_text("agent modified", encoding="utf-8")
        self._git(["add", "file.txt"], cwd=self.repo)
        commit = self._git(["commit", "-m", "agent commit"], cwd=self.repo, env=agent_env)
        self.assertNotEqual(commit.returncode, 0)
        self.assertIn("blocked for unattended agents", commit.stderr)
        self.assertIn("act worktree", commit.stderr)

        log_file = agent_cwd / "agent-guard-rejection.log"
        self.assertTrue(log_file.is_file())
        self.assertIn("blocked for unattended agents", log_file.read_text(encoding="utf-8"))

        # 2. Branch switch refusal
        checkout = self._git(["checkout", "-b", "agent-branch"], cwd=self.repo, env=agent_env)
        self.assertNotEqual(checkout.returncode, 0)
        self.assertIn("blocked for unattended agents", checkout.stderr)

        # 3. Reset refusal
        reset = self._git(["reset", "--hard", "HEAD"], cwd=self.repo, env=agent_env)
        self.assertNotEqual(reset.returncode, 0)
        self.assertIn("blocked for unattended agents", reset.stderr)

    def test_human_operations_without_agent_marker_are_unaffected(self) -> None:
        self._install_guard(self.repo)

        (self.repo / "file.txt").write_text("human modified", encoding="utf-8")
        self._git(["add", "file.txt"], cwd=self.repo)
        commit = self._git(["commit", "-m", "human commit"], cwd=self.repo)
        self.assertEqual(commit.returncode, 0)

        branch = self._git(["checkout", "-b", "human-branch"], cwd=self.repo)
        self.assertEqual(branch.returncode, 0)

        reset = self._git(["reset", "--hard", "HEAD~1"], cwd=self.repo)
        self.assertEqual(reset.returncode, 0)


if __name__ == "__main__":
    unittest.main()
