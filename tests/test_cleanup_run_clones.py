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


def test_cleanup_preserves_a_clone_when_the_check_itself_fails(tmp_path: Path):
    """A check that could not run must not be read as permission to delete.

    These clones are live while the runner works in them, so `git log` exits
    non-zero for ordinary reasons -- an `index.lock`, a repository caught
    mid-operation, a permissions hiccup. The first version of this cleanup
    treated every non-zero exit as "no unpushed commits" and removed the
    directory, which deleted precisely when it was least sure.
    """
    clone = tmp_path / "repo-unreadable"
    (clone / ".git").mkdir(parents=True)
    keep = clone / "work.txt"
    keep.write_text("work that only exists here")

    # `.git` is a directory but not a repository, so git exits 128.
    probe = subprocess.run(
        ["git", "-C", str(clone), "log", "--oneline", "--all", "--not", "--remotes"],
        capture_output=True, text=True, check=False,
    )
    assert probe.returncode != 0, "precondition: the check must fail here"

    cleanup_run_clones(tmp_path)

    assert clone.is_dir(), "an unreadable clone must be preserved, not removed"
    assert keep.read_text() == "work that only exists here"
