from __future__ import annotations

import json
import os
import stat
import subprocess
import sys
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

from foxhound.task_research_agent import (
    AgentResearchConfig,
    agent_synthesize,
)
from foxhound.task_research_synthesis import (
    DRAFT_SCHEMA,
    SynthesisError,
    validate_draft,
)


def _write_fake_hermes(
    script_path: Path,
    output_json: dict[str, Any] | None = None,
    exit_code: int = 0,
    sleep_seconds: float = 0.0,
) -> None:
    code_lines = [
        "#!/usr/bin/env python3",
        "import sys, time, json, os",
    ]
    if sleep_seconds > 0:
        code_lines.append(f"time.sleep({sleep_seconds})")
    if output_json is not None:
        raw = json.dumps(output_json)
        code_lines.append(
            f"with open('research.json', 'w') as f: f.write({json.dumps(raw)})"
        )
    code_lines.append(f"sys.exit({exit_code})")
    script_path.write_text("\n".join(code_lines) + "\n")
    script_path.chmod(script_path.stat().st_mode | stat.S_IEXEC)


def test_agent_synthesize_success(tmp_path: Path) -> None:
    fake_hermes = tmp_path / "fake_hermes.py"
    kb_dir = tmp_path / "sync_kb"
    kb_dir.mkdir()
    kb_file = kb_dir / "notes.txt"
    kb_file.write_text("sample content")

    valid_research = {
        "ownership": {
            "verdict": "reader",
            "evidence": [str(kb_file)],
        },
        "requested_deliverable": {
            "text": "Produce research summary",
            "evidence": ["https://example.com/spec"],
        },
        "constraints": [
            {"text": "Strict deadline", "binding": True, "evidence": ["https://example.com/deadline"]}
        ],
        "entities": [
            {
                "as_written": "Project A",
                "status": "resolved",
                "meaning": "Alpha Project",
                "evidence": [str(kb_file)],
            }
        ],
        "facts": [
            {
                "text": "Repo is active",
                "status": "confirmed",
                "evidence": ["https://example.com/repo"],
            },
            {
                "text": "Inferred requirement",
                "status": "inferred",
                "evidence": [],
            },
            {
                "text": "Conflicting dates",
                "status": "conflicting",
                "evidence": [str(kb_file)],
            },
        ],
        "open_questions": [],
        "recommendation": {"text": "Proceed", "evidence": []},
    }

    _write_fake_hermes(fake_hermes, output_json=valid_research)

    config = AgentResearchConfig(
        hermes_command=str(fake_hermes),
        model="test-model",
        provider="test-provider",
        knowledge_roots=(("kb", str(kb_dir)),),
    )

    ctx = {
        "task_snapshot": {
            "task_id": 123,
            "title": "Test Task",
        },
        "origin": {"kind": "issue", "record_id": "repo/test"},
    }

    run_dir = tmp_path / "run"
    result = agent_synthesize(ctx, config=config, bound_sources=None, run_dir=run_dir)

    assert result.draft["schema_version"] == DRAFT_SCHEMA
    assert result.draft["research_status"] == "sufficient"
    assert result.draft["objective"]["status"] == "supported"
    assert len(result.draft["objective"]["source_refs"]) > 0

    # web url source accepted in validate_draft
    validate_draft(result.draft, list(result.sources))

    # Check sources namespaces
    namespaces = {s["locator"]["namespace"] for s in result.sources}
    assert "web" in namespaces
    assert "kb" in namespaces

    # Verify task.json written
    task_json = json.loads((run_dir / "task.json").read_text())
    assert task_json["task_id"] == 123
    assert task_json["knowledge_roots"] == [{"name": "kb", "path": str(kb_dir)}]
    assert len(task_json["starting_points"]) > 0


def test_agent_synthesize_argv_env_cwd(tmp_path: Path) -> None:
    recorded_args = {}

    def mock_runner(argv, cwd, env, timeout, capture_output, text):
        recorded_args["argv"] = argv
        recorded_args["cwd"] = cwd
        recorded_args["env"] = env
        recorded_args["timeout"] = timeout
        # Write research.json
        (cwd / "research.json").write_text(json.dumps({
            "requested_deliverable": {"text": "Done", "evidence": []},
        }))
        res = MagicMock()
        res.returncode = 0
        return res

    config = AgentResearchConfig(
        hermes_command="hermes",
        model="custom-model",
        provider="custom-prov",
        toolsets="terminal,file",
        max_turns=50,
        timeout_seconds=1200,
    )

    ctx = {"task_snapshot": {"task_id": 456}}
    run_dir = tmp_path / "run_argv"

    agent_synthesize(
        ctx,
        config=config,
        bound_sources=None,
        run_dir=run_dir,
        runner=mock_runner,
    )

    argv = recorded_args["argv"]
    assert argv[0] == "hermes"
    assert argv[1:3] == ["--model", "custom-model"]
    assert argv[3:5] == ["--provider", "custom-prov"]
    assert argv[5] == "chat"
    assert "--max-turns" in argv and argv[argv.index("--max-turns") + 1] == "50"
    assert "--toolsets" in argv and argv[argv.index("--toolsets") + 1] == "terminal,file"
    assert "--ignore-rules" in argv
    assert "--source" in argv and argv[argv.index("--source") + 1] == "tool"

    assert recorded_args["cwd"] == run_dir
    assert recorded_args["env"]["TERMINAL_CWD"] == str(run_dir.resolve())
    assert recorded_args["env"]["FOXHOUND_VOICE_SUMMARIES"] == "0"
    assert recorded_args["timeout"] == 1200


def test_agent_synthesize_unresolved_entity_inconclusive(tmp_path: Path) -> None:
    fake_hermes = tmp_path / "fake_hermes.py"
    research_data = {
        "requested_deliverable": {"text": "Goal", "evidence": []},
        "entities": [
            {"as_written": "Mystery Corp", "status": "unresolved", "meaning": "unresolved"}
        ],
    }
    _write_fake_hermes(fake_hermes, output_json=research_data)

    config = AgentResearchConfig(hermes_command=str(fake_hermes))
    run_dir = tmp_path / "run"
    result = agent_synthesize({"task_snapshot": {}}, config=config, bound_sources=None, run_dir=run_dir)

    assert result.draft["research_status"] == "inconclusive"
    entity_claim = result.draft["related_entities"][0]
    assert entity_claim["status"] == "unknown"
    assert entity_claim["source_refs"] == []
    validate_draft(result.draft, list(result.sources))


def test_agent_synthesize_open_questions_inconclusive(tmp_path: Path) -> None:
    fake_hermes = tmp_path / "fake_hermes.py"
    research_data = {
        "requested_deliverable": {"text": "Goal", "evidence": []},
        "open_questions": ["What is the timeline?"],
    }
    _write_fake_hermes(fake_hermes, output_json=research_data)

    config = AgentResearchConfig(hermes_command=str(fake_hermes))
    run_dir = tmp_path / "run"
    result = agent_synthesize({"task_snapshot": {}}, config=config, bound_sources=None, run_dir=run_dir)

    assert result.draft["research_status"] == "inconclusive"
    assert len(result.draft["open_questions"]) == 1
    assert result.draft["open_questions"][0]["status"] == "unknown"
    validate_draft(result.draft, list(result.sources))


def test_agent_synthesize_timeout(tmp_path: Path) -> None:
    fake_hermes = tmp_path / "fake_hermes.py"
    _write_fake_hermes(fake_hermes, output_json={}, sleep_seconds=2.0)

    config = AgentResearchConfig(hermes_command=str(fake_hermes), timeout_seconds=1)
    run_dir = tmp_path / "run"

    with pytest.raises(SynthesisError) as exc_info:
        agent_synthesize({"task_snapshot": {}}, config=config, bound_sources=None, run_dir=run_dir)
    assert exc_info.value.code == "model_timeout"


def test_agent_synthesize_draft_missing_zero_exit(tmp_path: Path) -> None:
    fake_hermes = tmp_path / "fake_hermes.py"
    _write_fake_hermes(fake_hermes, output_json=None, exit_code=0)

    config = AgentResearchConfig(hermes_command=str(fake_hermes))
    run_dir = tmp_path / "run"

    with pytest.raises(SynthesisError) as exc_info:
        agent_synthesize({"task_snapshot": {}}, config=config, bound_sources=None, run_dir=run_dir)
    assert exc_info.value.code == "draft_missing"


def test_agent_synthesize_runtime_failed_nonzero_exit(tmp_path: Path) -> None:
    fake_hermes = tmp_path / "fake_hermes.py"
    _write_fake_hermes(fake_hermes, output_json=None, exit_code=1)

    config = AgentResearchConfig(hermes_command=str(fake_hermes))
    run_dir = tmp_path / "run"

    with pytest.raises(SynthesisError) as exc_info:
        agent_synthesize({"task_snapshot": {}}, config=config, bound_sources=None, run_dir=run_dir)
    assert exc_info.value.code == "runtime_failed"


def test_agent_synthesize_unmappable_evidence_downgrades_to_unknown(tmp_path: Path) -> None:
    fake_hermes = tmp_path / "fake_hermes.py"
    research_data = {
        "requested_deliverable": {
            "text": "Do work",
            "evidence": ["invalid/../path"],
        },
    }
    _write_fake_hermes(fake_hermes, output_json=research_data)

    config = AgentResearchConfig(hermes_command=str(fake_hermes))
    run_dir = tmp_path / "run"
    result = agent_synthesize({"task_snapshot": {}}, config=config, bound_sources=None, run_dir=run_dir)

    assert result.draft["objective"]["status"] == "unknown"
    assert result.draft["objective"]["source_refs"] == []
    assert len(result.sources) == 0
    validate_draft(result.draft, list(result.sources))
