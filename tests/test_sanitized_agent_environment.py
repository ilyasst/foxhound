import os
from pathlib import Path
import pytest
from foxhound.execution_runner import sanitized_agent_environment, ExecutionRunnerError

def test_sanitized_agent_environment(tmp_path: Path):
    credentials_dir = tmp_path / "creds"
    credentials_dir.mkdir()

    # Missing gitconfig
    with pytest.raises(ExecutionRunnerError, match="agent forge credentials directory or its gitconfig is missing"):
        sanitized_agent_environment({"GH_TOKEN": "secret"}, credentials_dir)

    (credentials_dir / "gitconfig").touch()

    base = {
        "GH_TOKEN": "secret",
        "GITHUB_TOKEN": "secret",
        "GH_ENTERPRISE_TOKEN": "secret",
        "GITHUB_ENTERPRISE_TOKEN": "secret",
        "GIT_ASKPASS": "secret",
        "SSH_ASKPASS": "secret",
        "SSH_AUTH_SOCK": "secret",
        "GIT_CONFIG_PARAMETERS": "secret",
        "GIT_CONFIG_COUNT": "1",
        "GIT_CONFIG_KEY_0": "user.email",
        "GIT_CONFIG_VALUE_0": "test@example.com",
        "OTHER_VAR": "keep",
    }

    env = sanitized_agent_environment(base, credentials_dir)

    assert "GH_TOKEN" not in env
    assert "GITHUB_TOKEN" not in env
    assert "GH_ENTERPRISE_TOKEN" not in env
    assert "GITHUB_ENTERPRISE_TOKEN" not in env
    assert "GIT_ASKPASS" not in env
    assert "SSH_ASKPASS" not in env
    assert "SSH_AUTH_SOCK" not in env
    assert "GIT_CONFIG_PARAMETERS" not in env
    assert "GIT_CONFIG_COUNT" not in env
    assert "GIT_CONFIG_KEY_0" not in env
    assert "GIT_CONFIG_VALUE_0" not in env
    assert env["OTHER_VAR"] == "keep"

    assert env["GH_CONFIG_DIR"] == str(credentials_dir / "gh")
    assert env["GIT_CONFIG_GLOBAL"] == str(credentials_dir / "gitconfig")
    assert env["GIT_CONFIG_NOSYSTEM"] == "1"
    assert env["GIT_TERMINAL_PROMPT"] == "0"
