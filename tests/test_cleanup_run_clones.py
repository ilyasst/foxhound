from pathlib import Path
import subprocess

from foxhound.execution_runner import cleanup_run_clones

def _setup_git(repo_dir: Path):
    subprocess.run(["git", "-C", str(repo_dir), "init"], check=True)
    subprocess.run(["git", "-C", str(repo_dir), "config", "user.email", "test@example.com"], check=True)
    subprocess.run(["git", "-C", str(repo_dir), "config", "user.name", "Test"], check=True)

def test_cleanup_removes_clone_without_unpushed_commits(tmp_path: Path):
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    repo_dir = run_dir / "repo-test-123"
    repo_dir.mkdir()
    
    _setup_git(repo_dir)
    subprocess.run(["git", "-C", str(repo_dir), "commit", "--allow-empty", "-m", "Initial commit"], check=True)
    subprocess.run(["git", "-C", str(repo_dir), "branch", "-m", "main"], check=True)
    
    remote_dir = tmp_path / "remote.git"
    remote_dir.mkdir()
    subprocess.run(["git", "-C", str(remote_dir), "init", "--bare", "--initial-branch=main"], check=True)
    subprocess.run(["git", "-C", str(repo_dir), "remote", "add", "origin", str(remote_dir)], check=True)
    subprocess.run(["git", "-C", str(repo_dir), "push", "-u", "origin", "main"], check=True)
    
    cleanup_run_clones(run_dir)
    assert not repo_dir.exists()

def test_cleanup_preserves_clone_with_unpushed_commits(tmp_path: Path):
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    repo_dir = run_dir / "repo-test-123"
    repo_dir.mkdir()
    
    _setup_git(repo_dir)
    subprocess.run(["git", "-C", str(repo_dir), "commit", "--allow-empty", "-m", "Initial commit"], check=True)
    
    cleanup_run_clones(run_dir)
    assert repo_dir.exists()

def test_cleanup_ignores_non_repos(tmp_path: Path):
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    non_repo = run_dir / "repo-test-123"
    non_repo.mkdir()
    
    cleanup_run_clones(run_dir)
    assert non_repo.exists()
