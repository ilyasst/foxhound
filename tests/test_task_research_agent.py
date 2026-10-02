from __future__ import annotations

import json
import os
import stat
import subprocess
import sys
from contextlib import closing
from datetime import datetime, timezone
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
    session_id: str | None = None,
) -> None:
    code_lines = [
        "#!/usr/bin/env python3",
        "import sys, time, json, os",
    ]
    if session_id:
        code_lines.append(f"print('session_id: {session_id}')")
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
        (cwd / "research.json").write_text(json.dumps({
            "ownership": {"verdict": "reader", "evidence": []},
            "requested_deliverable": {"text": "Done", "evidence": []},
            "constraints": [],
            "entities": [],
            "facts": [],
            "open_questions": [],
        }))
        res = MagicMock()
        res.returncode = 0
        res.stdout = ""
        res.stderr = ""
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
        "ownership": {"verdict": "reader", "evidence": []},
        "requested_deliverable": {"text": "Goal", "evidence": []},
        "constraints": [],
        "facts": [],
        "entities": [
            {"as_written": "Mystery Corp", "status": "unresolved", "meaning": "unresolved"}
        ],
        "open_questions": [],
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
        "ownership": {"verdict": "reader", "evidence": []},
        "requested_deliverable": {"text": "Goal", "evidence": []},
        "constraints": [],
        "entities": [],
        "facts": [],
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


def test_agent_synthesize_unmappable_evidence_reported_as_problem(tmp_path: Path) -> None:
    fake_hermes = tmp_path / "fake_hermes.py"
    research_data = {
        "ownership": {"verdict": "reader", "evidence": []},
        "requested_deliverable": {
            "text": "Do work",
            "evidence": ["invalid/../path"],
        },
        "constraints": [],
        "entities": [],
        "facts": [],
        "open_questions": [],
    }
    _write_fake_hermes(fake_hermes, output_json=research_data)

    config = AgentResearchConfig(hermes_command=str(fake_hermes))
    run_dir = tmp_path / "run"
    with pytest.raises(SynthesisError) as exc_info:
        agent_synthesize({"task_snapshot": {}}, config=config, bound_sources=None, run_dir=run_dir)
    assert exc_info.value.code == "invalid_draft"


def test_agent_locators_four_shapes_and_safeguards(tmp_path: Path) -> None:
    from foxhound.task_research import validate_sources as vr_sources

    # Temp knowledge roots
    kb_root = tmp_path / "sync_kb"
    kb_root.mkdir()
    attachments_root = tmp_path / "attachments"
    attachments_root.mkdir()

    # Create files for the 4 shapes
    meeting_md = kb_root / "Meetings" / "20300101_100000_mix_protocol.md"
    meeting_md.parent.mkdir(parents=True, exist_ok=True)
    meeting_md.write_text("line 1\nline 29 Decisions\n")

    wg_md = kb_root / "Working_Groups" / "Alpha.md"
    wg_md.parent.mkdir(parents=True, exist_ok=True)
    wg_md.write_text("line 1\nline 23\nline 42\n")

    transcript_txt = attachments_root / "Meetings" / "20300101_100000_mix_transcript.txt"
    transcript_txt.parent.mkdir(parents=True, exist_ok=True)
    transcript_txt.write_text("transcript text")

    # Outside root for symlink test
    outside_file = tmp_path / "secret.txt"
    outside_file.write_text("sensitive")
    symlink_file = kb_root / "Meetings" / "escape_symlink.txt"
    symlink_file.symlink_to(outside_file)

    k_roots = (("kb", str(kb_root)), ("attachments", str(attachments_root)))

    # Test the four shapes
    ev1 = "kb:Meetings/20300101_100000_mix_protocol.md (line 29, Decisions: Person B will prepare ...)"
    ev2 = "kb:Working_Groups/Alpha.md (lines 23, 42-45)"
    ev3 = "https://example.org/funding/options/ (accessed 2030-01-01)"
    ev4 = "attachments:Meetings/20300101_100000_mix_transcript.txt#L120-L140"

    from foxhound.task_research_agent import _map_locator
    map1 = _map_locator(ev1, k_roots)
    assert map1 == ("kb", "Meetings/20300101_100000_mix_protocol.md", "L29")

    map2 = _map_locator(ev2, k_roots)
    assert map2 == ("kb", "Working_Groups/Alpha.md", "L23")

    map3 = _map_locator(ev3, k_roots)
    assert map3 == ("web", "https://example.org/funding/options/", None)

    map4 = _map_locator(ev4, k_roots)
    assert map4 == ("attachment", "Meetings/20300101_100000_mix_transcript.txt", "L120-L140")

    # Nonexistent file -> dropped (None)
    assert _map_locator("kb:Meetings/nonexistent.md", k_roots) is None

    # Symlink escaping root -> dropped (None)
    assert _map_locator("kb:Meetings/escape_symlink.txt", k_roots) is None

    # Full conversion with synthesize output
    fake_hermes = tmp_path / "fake_hermes.py"
    research_json = {
        "ownership": {"verdict": "reader", "evidence": [ev1]},
        "requested_deliverable": {"text": "Do task", "evidence": [ev2]},
        "constraints": [],
        "entities": [],
        "facts": [
            {"text": "Fact 1", "status": "confirmed", "evidence": [ev3]},
            {"text": "Fact 2", "status": "confirmed", "evidence": [ev4]},
            # Duplicate resource with different fragment or note
            {"text": "Fact 3", "status": "confirmed", "evidence": ["kb:Working_Groups/Alpha.md (line 99)"]},
        ],
        "open_questions": [],
    }
    _write_fake_hermes(fake_hermes, output_json=research_json)
    config = AgentResearchConfig(hermes_command=str(fake_hermes), knowledge_roots=k_roots)
    run_dir = tmp_path / "run_full"
    res = agent_synthesize({"task_snapshot": {}}, config=config, bound_sources=None, run_dir=run_dir)

    # Dedup check: Working_Groups/Alpha.md was referenced twice, but sources should only have it once
    resources = [(dict(s["locator"])["namespace"], dict(s["locator"])["resource"]) for s in res.sources]  # type: ignore[index]
    assert len(resources) == len(set(resources))
    assert len(res.sources) == 4  # protocol, alpha, example.org, transcript

    # Titles check
    for s in res.sources:
        title = str(s["title"])  # type: ignore[index]
        assert title == title.strip()
        assert len(title) <= 200
        if dict(s["locator"])["namespace"] == "web":  # type: ignore[index]
            assert title == "example.org/funding/options/"
        else:
            assert title == dict(s["locator"])["resource"]  # type: ignore[index]

    # Validate sources and draft
    validated_sources = vr_sources(list(res.sources))
    assert len(validated_sources) == 4
    validate_draft(res.draft, list(res.sources))


def test_agent_runner_failed_dir_and_cleanup(tmp_path: Path) -> None:
    from foxhound.task_research_runner import run_once
    from tests.test_task_research_runner import _setup_test_env, _queue_job

    now = datetime(2030, 1, 15, 12, 0, tzinfo=timezone.utc)
    paths = _setup_test_env(tmp_path)
    _queue_job(paths)

    scratch = paths["scratch_root"]

    # Also create an old failed dir (older than 7 days) and a fresh failed dir
    old_failed = scratch / "failed-old-100"
    old_failed.mkdir()
    # Set mtime to 10 days ago relative to now
    old_mtime = now.timestamp() - (10 * 86400)
    os.utime(old_failed, (old_mtime, old_mtime))

    recent_failed = scratch / "failed-recent-200"
    recent_failed.mkdir()
    recent_mtime = now.timestamp() - (2 * 86400)
    os.utime(recent_failed, (recent_mtime, recent_mtime))

    # Hermes script that fails
    fake_hermes = tmp_path / "fake_hermes_fail.py"
    _write_fake_hermes(fake_hermes, exit_code=1)

    result = run_once(
        database=paths["database"],
        cas_root=paths["cas_root"],
        task_work_root=paths["task_work_root"],
        scratch_root=scratch,
        model="synthetic-model",
        endpoint="http://127.0.0.1:8800",
        synthesizer="agent",
        hermes_command=str(fake_hermes),
        clock=lambda: now,
    )

    assert result.claimed is True
    assert result.completed is False

    # Check that old failed dir was deleted
    assert not old_failed.exists()
    # Recent failed dir remains
    assert recent_failed.exists()

    # Check that a new failed-* directory was created in scratch_root
    created_failed = [d for d in scratch.iterdir() if d.is_dir() and d.name.startswith("failed-")]
    assert any(d.name != "failed-recent-200" for d in created_failed)


def test_agent_synthesize_repair_flow(tmp_path: Path) -> None:
    calls = []

    kb_dir = tmp_path / "sync_kb"
    kb_dir.mkdir()
    kb_file = kb_dir / "doc.txt"
    kb_file.write_text("valid content")

    valid_json = {
        "ownership": {"verdict": "reader", "evidence": [str(kb_file)]},
        "requested_deliverable": {"text": "Deliverable", "evidence": []},
        "constraints": [],
        "entities": [],
        "facts": [],
        "open_questions": [],
    }

    def fake_runner(argv, cwd, env, timeout, capture_output, text):
        calls.append(list(argv))
        res = MagicMock()
        res.returncode = 0
        res.stderr = ""
        if len(calls) == 1:
            # First turn: print session id, write invalid json (unmappable locator and missing field)
            res.stdout = "session_id: sess-12345\nStarted run."
            invalid_json = {
                "ownership": {"verdict": "reader", "evidence": ["unmappable/locator"]},
                "requested_deliverable": {"text": "Deliverable", "evidence": []},
                # Missing constraints, entities, facts, open_questions
            }
            (cwd / "research.json").write_text(json.dumps(invalid_json))
        else:
            # Repair turn: write fixed json
            res.stdout = "Fixed."
            (cwd / "research.json").write_text(json.dumps(valid_json))
        return res

    config = AgentResearchConfig(
        hermes_command="hermes",
        model="test-model",
        knowledge_roots=(("kb", str(kb_dir)),),
    )
    run_dir = tmp_path / "run_repair"
    result = agent_synthesize({"task_snapshot": {}}, config=config, bound_sources=None, run_dir=run_dir, runner=fake_runner)

    assert len(calls) == 2
    first_argv = calls[0]
    repair_argv = calls[1]

    assert "-Q" in first_argv
    assert "--resume" in repair_argv
    resume_idx = repair_argv.index("--resume")
    assert repair_argv[resume_idx + 1] == "sess-12345"
    assert "--query" in repair_argv
    query_text = repair_argv[repair_argv.index("--query") + 1]
    assert "Unmappable evidence locator" in query_text
    assert "Missing required top-level field" in query_text
    assert "kb:Meetings/x.md#L29" in query_text
    assert "--toolsets" in repair_argv and repair_argv[repair_argv.index("--toolsets") + 1] == "file"
    assert "--max-turns" in repair_argv and repair_argv[repair_argv.index("--max-turns") + 1] == "8"

    assert result.metrics["repair_turns"] == 1
    assert result.draft["research_status"] == "sufficient"


def test_agent_synthesize_no_repair_without_session_id(tmp_path: Path) -> None:
    calls = []

    def fake_runner(argv, cwd, env, timeout, capture_output, text):
        calls.append(list(argv))
        res = MagicMock()
        res.returncode = 0
        res.stdout = "No session info printed"
        res.stderr = ""
        # Write invalid json
        (cwd / "research.json").write_text(json.dumps({"invalid": True}))
        return res

    config = AgentResearchConfig(hermes_command="hermes")
    run_dir = tmp_path / "run_no_sess"
    with pytest.raises(SynthesisError) as exc_info:
        agent_synthesize({"task_snapshot": {}}, config=config, bound_sources=None, run_dir=run_dir, runner=fake_runner)
    assert exc_info.value.code == "invalid_draft"
    assert len(calls) == 1  # No repair attempted


def test_agent_synthesize_no_repair_on_timeout(tmp_path: Path) -> None:
    calls = []

    def fake_runner(argv, cwd, env, timeout, capture_output, text):
        calls.append(list(argv))
        raise subprocess.TimeoutExpired(cmd=argv, timeout=timeout)

    config = AgentResearchConfig(hermes_command="hermes")
    run_dir = tmp_path / "run_timeout"
    with pytest.raises(SynthesisError) as exc_info:
        agent_synthesize({"task_snapshot": {}}, config=config, bound_sources=None, run_dir=run_dir, runner=fake_runner)
    assert exc_info.value.code == "model_timeout"
    assert len(calls) == 1  # No repair attempted


def test_agent_synthesize_two_failed_repairs_raises_original_error(tmp_path: Path) -> None:
    calls = []

    def fake_runner(argv, cwd, env, timeout, capture_output, text):
        calls.append(list(argv))
        res = MagicMock()
        res.returncode = 0
        res.stdout = "session_id: sess-999"
        res.stderr = ""
        # Keep writing invalid json with missing fields
        (cwd / "research.json").write_text(json.dumps({
            "requested_deliverable": {"text": "Goal", "evidence": []},
        }))
        return res

    config = AgentResearchConfig(hermes_command="hermes")
    run_dir = tmp_path / "run_two_failed"
    with pytest.raises(SynthesisError) as exc_info:
        agent_synthesize({"task_snapshot": {}}, config=config, bound_sources=None, run_dir=run_dir, runner=fake_runner)
    assert exc_info.value.code == "invalid_draft"
    assert len(calls) == 3  # 1 initial + 2 repair turns
