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

    # Verify research-check was written and is executable
    check_bin = run_dir / "research-check"
    assert check_bin.exists()
    assert os.access(check_bin, os.X_OK)

    # Verify metrics
    assert result.metrics["self_check_ok"] is True
    assert result.metrics["repair_turns"] == 0
    assert result.metrics["degraded"] is False
    assert result.metrics["dropped_citations"] == 0
    assert result.metrics["unsourced_claims"] == 0

    # Verify task.json written
    task_json = json.loads((run_dir / "task.json").read_text())
    assert task_json["task_id"] == 123
    assert task_json["knowledge_roots"] == [{"name": "kb", "path": str(kb_dir)}]
    assert "read_only_commands" not in task_json
    assert len(task_json["starting_points"]) > 0


def test_agent_synthesize_task_json_contains_read_only_commands(tmp_path: Path) -> None:
    fake_hermes = tmp_path / "fake_hermes.py"
    valid_research = {
        "ownership": {"verdict": "reader", "evidence": []},
        "requested_deliverable": {"text": "Produce research summary", "evidence": []},
        "constraints": [],
        "entities": [],
        "facts": [],
        "open_questions": [],
        "recommendation": {"text": "Proceed", "evidence": []},
    }
    _write_fake_hermes(fake_hermes, output_json=valid_research)

    ro_cmds = (
        {
            "name": "calendar-cli",
            "command": "/srv/example/bin/cal",
            "description": "check calendar",
        },
    )
    config = AgentResearchConfig(
        hermes_command=str(fake_hermes),
        model="test-model",
        read_only_commands=ro_cmds,
    )
    ctx = {
        "task_snapshot": {"task_id": 456, "title": "Test Task RO"},
        "origin": {"kind": "issue", "record_id": "repo/test"},
    }
    run_dir = tmp_path / "run_ro"
    agent_synthesize(ctx, config=config, bound_sources=None, run_dir=run_dir)

    task_json = json.loads((run_dir / "task.json").read_text())
    assert task_json["task_id"] == 456
    assert task_json["read_only_commands"] == [
        {
            "name": "calendar-cli",
            "command": "/srv/example/bin/cal",
            "description": "check calendar",
        }
    ]


def test_agent_synthesize_origin_documents_resolved(tmp_path: Path) -> None:
    calls = []

    def mock_runner(argv, cwd, timeout=None, capture_output=False, text=False, **kwargs):
        calls.append((argv, cwd))
        if argv[0] == "/srv/example/bin/origin":
            res = MagicMock()
            res.returncode = 0
            res.stdout = json.dumps({
                "schema": "gw.source-documents",
                "status": "resolved",
                "kind": "email",
                "paths": ["kb/mail.eml"],
            })
            res.stderr = ""
            return res
        # Fake Hermes run
        (cwd / "research.json").write_text(json.dumps({
            "ownership": {"verdict": "reader", "evidence": []},
            "requested_deliverable": {"text": "Done", "evidence": []},
            "constraints": [],
            "entities": [],
            "facts": [],
            "open_questions": [],
            "recommendation": {"text": "Proceed", "evidence": []},
        }))
        res = MagicMock()
        res.returncode = 0
        res.stdout = ""
        res.stderr = ""
        return res

    config = AgentResearchConfig(
        hermes_command="hermes",
        read_only_commands=({
            "name": "origin",
            "command": "/srv/example/bin/origin",
            "description": "fetch origin",
        },),
    )
    ctx = {
        "task_snapshot": {"task_id": 789},
        "origin": {"kind": "email", "record_id": "rec-123", "item_id": "item-456"},
    }
    run_dir = tmp_path / "run_origin_resolved"
    agent_synthesize(ctx, config=config, bound_sources=None, run_dir=run_dir, runner=mock_runner)

    assert calls[0][0] == ["/srv/example/bin/origin", "rec-123", "--item", "item-456"]
    assert calls[0][1] == run_dir
    task_json = json.loads((run_dir / "task.json").read_text())
    assert task_json["starting_points"][0]["origin_documents"]["status"] == "resolved"


def test_agent_synthesize_origin_documents_failed_or_unresolved(tmp_path: Path) -> None:
    calls = []

    def mock_runner(argv, cwd, timeout=None, capture_output=False, text=False, **kwargs):
        calls.append(argv)
        if argv[0] == "/srv/example/bin/origin":
            res = MagicMock()
            res.returncode = 1
            res.stdout = ""
            res.stderr = "network error"
            return res
        (cwd / "research.json").write_text(json.dumps({
            "ownership": {"verdict": "reader", "evidence": []},
            "requested_deliverable": {"text": "Done", "evidence": []},
            "constraints": [],
            "entities": [],
            "facts": [],
            "open_questions": [],
            "recommendation": {"text": "Proceed", "evidence": []},
        }))
        res = MagicMock()
        res.returncode = 0
        res.stdout = ""
        res.stderr = ""
        return res

    config = AgentResearchConfig(
        hermes_command="hermes",
        read_only_commands=({
            "name": "origin",
            "command": "/srv/example/bin/origin",
            "description": "fetch origin",
        },),
    )
    ctx = {
        "task_snapshot": {"task_id": 789},
        "origin": {"kind": "email", "record_id": "rec-123"},
    }
    run_dir = tmp_path / "run_origin_fail"
    agent_synthesize(ctx, config=config, bound_sources=None, run_dir=run_dir, runner=mock_runner)

    assert calls[0] == ["/srv/example/bin/origin", "rec-123"]
    task_json = json.loads((run_dir / "task.json").read_text())
    assert task_json["starting_points"][-1]["origin_documents"] is None
    assert "network error" in task_json["starting_points"][-1]["origin_error"]


def test_agent_synthesize_no_origin_command(tmp_path: Path) -> None:
    calls = []

    def mock_runner(argv, cwd, timeout=None, capture_output=False, text=False, **kwargs):
        calls.append(argv)
        (cwd / "research.json").write_text(json.dumps({
            "ownership": {"verdict": "reader", "evidence": []},
            "requested_deliverable": {"text": "Done", "evidence": []},
            "constraints": [],
            "entities": [],
            "facts": [],
            "open_questions": [],
            "recommendation": {"text": "Proceed", "evidence": []},
        }))
        res = MagicMock()
        res.returncode = 0
        res.stdout = ""
        res.stderr = ""
        return res

    config = AgentResearchConfig(hermes_command="hermes")
    ctx = {
        "task_snapshot": {"task_id": 789},
        "origin": {"kind": "email", "record_id": "rec-123"},
    }
    run_dir = tmp_path / "run_no_origin"
    agent_synthesize(ctx, config=config, bound_sources=None, run_dir=run_dir, runner=mock_runner)

    assert len(calls) == 1  # Only hermes
    task_json = json.loads((run_dir / "task.json").read_text())
    assert not any("origin_documents" in sp for sp in task_json["starting_points"])


def test_agent_synthesize_reader_aliases(tmp_path: Path) -> None:
    def mock_runner(argv, cwd, timeout=None, capture_output=False, text=False, **kwargs):
        (cwd / "research.json").write_text(json.dumps({
            "ownership": {"verdict": "reader", "evidence": []},
            "requested_deliverable": {"text": "Done", "evidence": []},
            "constraints": [],
            "entities": [],
            "facts": [],
            "open_questions": [],
            "recommendation": {"text": "Proceed", "evidence": []},
        }))
        res = MagicMock()
        res.returncode = 0
        res.stdout = ""
        res.stderr = ""
        return res

    config = AgentResearchConfig(
        hermes_command="hermes",
        reader_aliases=("Alice", "Bob"),
    )
    ctx = {"task_snapshot": {"task_id": 100}}
    run_dir = tmp_path / "run_aliases"
    agent_synthesize(ctx, config=config, bound_sources=None, run_dir=run_dir, runner=mock_runner)

    task_json = json.loads((run_dir / "task.json").read_text())
    assert task_json["reader"] == {"aliases": ["Alice", "Bob"]}


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
    assert recorded_args["env"]["RIPGREP_CONFIG_PATH"] == str((run_dir / ".ripgreprc").resolve())
    assert recorded_args["env"]["CAPROUTE_APP"] == "foxhound"
    assert recorded_args["env"]["CAPROUTE_OPERATION"] == "research"
    assert recorded_args["env"]["CAPROUTE_JOB"] == ""
    assert recorded_args["env"]["CAPROUTE_RUN_ID"] == ""
    assert (run_dir / ".ripgreprc").read_text(encoding="utf-8") == "--follow\n"
    assert recorded_args["timeout"] == 1200


def test_agent_synthesize_unresolved_entity_sufficient(tmp_path: Path) -> None:
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

    assert result.draft["research_status"] == "sufficient"
    entity_claim = result.draft["related_entities"][0]
    assert entity_claim["status"] == "unknown"
    assert entity_claim["source_refs"] == []
    validate_draft(result.draft, list(result.sources))


def test_agent_synthesize_open_questions_non_blocking_sufficient(tmp_path: Path) -> None:
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

    assert result.draft["research_status"] == "sufficient"
    open_qs = result.draft["open_questions"]
    assert isinstance(open_qs, list) and len(open_qs) == 1
    assert open_qs[0]["status"] == "unknown"
    assert open_qs[0]["text"] == "What is the timeline? (for the reader)"
    validate_draft(result.draft, list(result.sources))


def test_open_questions_kind_and_legacy_status(tmp_path: Path) -> None:
    from foxhound.task_research_agent import _convert_research_json

    kb_dir = tmp_path / "kb"
    kb_dir.mkdir()
    k_roots = (("kb", str(kb_dir)),)

    base = {
        "ownership": {"verdict": "reader", "evidence": []},
        "requested_deliverable": {"text": "Goal", "evidence": []},
        "constraints": [],
        "entities": [],
        "facts": [],
    }

    # 1. for_reader-only -> "sufficient", suffix " (for the reader)"
    raw_reader = dict(base, open_questions=[{"text": "What is the budget?", "kind": "for_reader"}])
    draft, sources = _convert_research_json(raw_reader, k_roots)
    assert draft["research_status"] == "sufficient"
    assert draft["open_questions"][0]["text"] == "What is the budget? (for the reader)"
    validate_draft(draft, sources)

    # 2. task_work-only -> "sufficient", suffix " (task work)"
    raw_work = dict(base, open_questions=[{"text": "Find meeting room capacity", "kind": "task_work"}])
    draft, sources = _convert_research_json(raw_work, k_roots)
    assert draft["research_status"] == "sufficient"
    assert draft["open_questions"][0]["text"] == "Find meeting room capacity (task work)"
    validate_draft(draft, sources)

    # 3. one kind=blocking -> "inconclusive", suffix " (blocking)"
    raw_blocking = dict(base, open_questions=[
        {"text": "What is the budget?", "kind": "for_reader"},
        {"text": "Which project repo?", "kind": "blocking"},
    ])
    draft, sources = _convert_research_json(raw_blocking, k_roots)
    assert draft["research_status"] == "inconclusive"
    assert draft["open_questions"][0]["text"] == "What is the budget? (for the reader)"
    assert draft["open_questions"][1]["text"] == "Which project repo? (blocking)"
    validate_draft(draft, sources)

    # 4. legacy {"blocking": true} without kind -> "inconclusive", suffix " (blocking)"
    raw_legacy_true = dict(base, open_questions=[{"text": "Missing key doc", "blocking": True}])
    draft, sources = _convert_research_json(raw_legacy_true, k_roots)
    assert draft["research_status"] == "inconclusive"
    assert draft["open_questions"][0]["text"] == "Missing key doc (blocking)"
    validate_draft(draft, sources)

    # 5. legacy {"blocking": false} -> "sufficient", suffix " (for the reader)"
    raw_legacy_false = dict(base, open_questions=[{"text": "Preferred time?", "blocking": False}])
    draft, sources = _convert_research_json(raw_legacy_false, k_roots)
    assert draft["research_status"] == "sufficient"
    assert draft["open_questions"][0]["text"] == "Preferred time? (for the reader)"
    validate_draft(draft, sources)

    # 6. plain string -> "for_reader", suffix " (for the reader)"
    raw_plain = dict(base, open_questions=["A plain question"])
    draft, sources = _convert_research_json(raw_plain, k_roots)
    assert draft["research_status"] == "sufficient"
    assert draft["open_questions"][0]["text"] == "A plain question (for the reader)"
    validate_draft(draft, sources)


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


def test_agent_synthesize_unmappable_evidence_degrades_and_publishes(tmp_path: Path) -> None:
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
    result = agent_synthesize({"task_snapshot": {}}, config=config, bound_sources=None, run_dir=run_dir)
    assert result.metrics["degraded"] is True
    assert result.metrics["dropped_citations"] == 1
    assert result.metrics["unsourced_claims"] == 1


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

    # Declared tool command mapping
    ro_cmds = ({"name": "calendar-cli", "command": "/srv/example/bin/calendar-readonly", "description": "view calendar"},)
    map_tool = _map_locator("calendar-cli:events --from 2030-01-01", k_roots, ro_cmds)
    assert map_tool == ("tool", "calendar-cli:events --from 2030-01-01", None)

    # Undeclared tool command mapping returns None
    assert _map_locator("mail-cli:messages --unread", k_roots, ro_cmds) is None

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

    envs = []

    def fake_runner(argv, cwd, env, timeout, capture_output, text):
        calls.append(list(argv))
        envs.append(dict(env))
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
        job_id="job-repair-99",
    )
    run_dir = tmp_path / "run_repair"
    result = agent_synthesize({"task_snapshot": {}}, config=config, bound_sources=None, run_dir=run_dir, runner=fake_runner)

    assert len(calls) == 2
    for call_env in envs:
        assert call_env["CAPROUTE_APP"] == "foxhound"
        assert call_env["CAPROUTE_OPERATION"] == "research"
        assert call_env["CAPROUTE_JOB"] == "job-repair-99"
        assert call_env["CAPROUTE_RUN_ID"] == "job-repair-99"
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


def test_agent_synthesize_no_repair_without_session_id_degrades(tmp_path: Path) -> None:
    calls = []

    def fake_runner(argv, cwd, env, timeout, capture_output, text):
        calls.append(list(argv))
        res = MagicMock()
        res.returncode = 0
        res.stdout = "No session info printed"
        res.stderr = ""
        # Write json with unknown evidence
        (cwd / "research.json").write_text(json.dumps({
            "ownership": {"verdict": "reader", "evidence": ["unknown:source"]},
            "requested_deliverable": {"text": "Goal", "evidence": []},
            "constraints": [],
            "entities": [],
            "facts": [],
            "open_questions": [],
        }))
        return res

    config = AgentResearchConfig(hermes_command="hermes")
    run_dir = tmp_path / "run_no_sess"
    result = agent_synthesize({"task_snapshot": {}}, config=config, bound_sources=None, run_dir=run_dir, runner=fake_runner)
    assert result.metrics["degraded"] is True
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


def test_agent_synthesize_one_failed_repair_degrades(tmp_path: Path) -> None:
    calls = []

    def fake_runner(argv, cwd, env, timeout, capture_output, text):
        calls.append(list(argv))
        res = MagicMock()
        res.returncode = 0
        res.stdout = "session_id: sess-999"
        res.stderr = ""
        # Keep writing json with invalid locator
        (cwd / "research.json").write_text(json.dumps({
            "ownership": {"verdict": "reader", "evidence": ["bad:locator"]},
            "requested_deliverable": {"text": "Goal", "evidence": []},
            "constraints": [],
            "entities": [],
            "facts": [],
            "open_questions": [],
        }))
        return res

    config = AgentResearchConfig(hermes_command="hermes")
    run_dir = tmp_path / "run_failed_repair"
    result = agent_synthesize({"task_snapshot": {}}, config=config, bound_sources=None, run_dir=run_dir, runner=fake_runner)
    assert result.metrics["degraded"] is True
    assert len(calls) == 2  # 1 initial + 1 repair turn (max repair turns = 1)


def test_agent_synthesize_repair_timeout_rechecks_and_publishes(tmp_path: Path) -> None:
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
        if len(calls) == 1:
            res = MagicMock()
            res.returncode = 0
            res.stdout = "session_id: sess-123"
            res.stderr = ""
            # Invalid json on turn 1
            (cwd / "research.json").write_text(json.dumps({
                "ownership": {"verdict": "reader", "evidence": ["unmappable/locator"]},
            }))
            return res
        else:
            # Repair turn writes valid json but times out
            (cwd / "research.json").write_text(json.dumps(valid_json))
            assert timeout == 600
            raise subprocess.TimeoutExpired(cmd=argv, timeout=timeout)

    config = AgentResearchConfig(
        hermes_command="hermes",
        model="test-model",
        knowledge_roots=(("kb", str(kb_dir)),),
    )
    run_dir = tmp_path / "run_repair_timeout"
    result = agent_synthesize({"task_snapshot": {}}, config=config, bound_sources=None, run_dir=run_dir, runner=fake_runner)

    assert result.draft["research_status"] == "sufficient"
    assert len(calls) == 2


def test_agent_synthesize_continuation_flow(tmp_path: Path) -> None:
    calls = []
    envs = []
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
        envs.append(dict(env))
        res = MagicMock()
        res.returncode = 0
        res.stderr = ""
        if len(calls) == 1:
            res.stdout = "session_id: sess-cont-123\nInterrupted."
        else:
            res.stdout = "Finished."
            (cwd / "research.json").write_text(json.dumps(valid_json))
        return res

    config = AgentResearchConfig(
        hermes_command="hermes",
        model="test-model",
        knowledge_roots=(("kb", str(kb_dir)),),
        job_id="job-cont-123",
    )
    run_dir = tmp_path / "run_continuation"
    result = agent_synthesize({"task_snapshot": {}}, config=config, bound_sources=None, run_dir=run_dir, runner=fake_runner)

    assert len(calls) == 2
    for call_env in envs:
        assert call_env["CAPROUTE_APP"] == "foxhound"
        assert call_env["CAPROUTE_OPERATION"] == "research"
        assert call_env["CAPROUTE_JOB"] == "job-cont-123"
        assert call_env["CAPROUTE_RUN_ID"] == "job-cont-123"
    cont_argv = calls[1]
    assert "--resume" in cont_argv
    assert cont_argv[cont_argv.index("--resume") + 1] == "sess-cont-123"
    assert "--query" in cont_argv
    query_text = cont_argv[cont_argv.index("--query") + 1]
    assert "Your research was interrupted before research.json was written." in query_text
    assert "--max-turns" in cont_argv and cont_argv[cont_argv.index("--max-turns") + 1] == "40"
    assert result.metrics["continuation_turns"] == 1
    assert result.draft["research_status"] == "sufficient"


def test_agent_synthesize_missing_output_no_session_fails(tmp_path: Path) -> None:
    calls = []

    def fake_runner(argv, cwd, env, timeout, capture_output, text):
        calls.append(list(argv))
        res = MagicMock()
        res.returncode = 0
        res.stdout = "No session info"
        res.stderr = ""
        return res

    config = AgentResearchConfig(hermes_command="hermes")
    run_dir = tmp_path / "run_missing_no_sess"
    with pytest.raises(SynthesisError) as exc_info:
        agent_synthesize({"task_snapshot": {}}, config=config, bound_sources=None, run_dir=run_dir, runner=fake_runner)
    assert exc_info.value.code == "draft_missing"
    assert len(calls) == 1


def test_agent_synthesize_missing_output_runtime_failed(tmp_path: Path) -> None:
    calls = []

    def fake_runner(argv, cwd, env, timeout, capture_output, text):
        calls.append(list(argv))
        res = MagicMock()
        res.returncode = 1
        res.stdout = "No session info"
        res.stderr = "Fatal error"
        return res

    config = AgentResearchConfig(hermes_command="hermes")
    run_dir = tmp_path / "run_missing_fail"
    with pytest.raises(SynthesisError) as exc_info:
        agent_synthesize({"task_snapshot": {}}, config=config, bound_sources=None, run_dir=run_dir, runner=fake_runner)
    assert exc_info.value.code == "runtime_failed"
    assert len(calls) == 1


def test_agent_synthesize_main_pass_timeout_no_continuation(tmp_path: Path) -> None:
    calls = []

    def fake_runner(argv, cwd, env, timeout, capture_output, text):
        calls.append(list(argv))
        raise subprocess.TimeoutExpired(cmd=argv, timeout=timeout)

    config = AgentResearchConfig(hermes_command="hermes")
    run_dir = tmp_path / "run_main_timeout"
    with pytest.raises(SynthesisError) as exc_info:
        agent_synthesize({"task_snapshot": {}}, config=config, bound_sources=None, run_dir=run_dir, runner=fake_runner)
    assert exc_info.value.code == "model_timeout"
    assert len(calls) == 1


def test_agent_synthesize_prompt_additions() -> None:
    prompt_path = Path(__file__).resolve().parent.parent / "src" / "foxhound" / "prompts" / "research_agent.md"
    content = prompt_path.read_text(encoding="utf-8")
    assert "`task.json` is in your current directory." in content
    assert "file search in files mode" in content
    assert "Never open databases" in content and "sqlite3" in content
    assert "declared read-only commands" in content
    assert "a search without a path covers them" in content
    assert "empty working directory" not in content


def test_agent_synthesize_state_db_access_audit(tmp_path: Path) -> None:
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

    # Case 1: stdout contains sqlite3
    def fake_runner_sqlite(argv, cwd, env, timeout, capture_output, text):
        res = MagicMock()
        res.returncode = 0
        res.stdout = "Running sqlite3 query on state.db"
        res.stderr = ""
        (cwd / "research.json").write_text(json.dumps(valid_json))
        return res

    config = AgentResearchConfig(
        hermes_command="hermes",
        model="test-model",
        knowledge_roots=(("kb", str(kb_dir)),),
    )
    run_dir = tmp_path / "run_audit_sqlite"
    result = agent_synthesize({"task_snapshot": {}}, config=config, bound_sources=None, run_dir=run_dir, runner=fake_runner_sqlite)
    assert result.metrics["state_db_access"] is True

    # Case 2: clean run without sqlite3
    def fake_runner_clean(argv, cwd, env, timeout, capture_output, text):
        res = MagicMock()
        res.returncode = 0
        res.stdout = "Clean run"
        res.stderr = ""
        (cwd / "research.json").write_text(json.dumps(valid_json))
        return res

    run_dir_clean = tmp_path / "run_audit_clean"
    result_clean = agent_synthesize({"task_snapshot": {}}, config=config, bound_sources=None, run_dir=run_dir_clean, runner=fake_runner_clean)
    assert result_clean.metrics["state_db_access"] is False


def test_agent_synthesize_knowledge_roots_symlinks_and_ripgreprc(tmp_path: Path) -> None:
    kb_dir = tmp_path / "kb_dir"
    kb_dir.mkdir()
    att_dir = tmp_path / "att_dir"
    att_dir.mkdir()

    calls = []

    def fake_runner(argv, cwd, env, timeout, capture_output, text):
        calls.append((argv, env))
        res = MagicMock()
        res.returncode = 0
        res.stdout = ""
        res.stderr = ""
        (cwd / "research.json").write_text(json.dumps({
            "ownership": {"verdict": "reader", "evidence": []},
            "requested_deliverable": {"text": "Goal", "evidence": []},
            "constraints": [],
            "entities": [],
            "facts": [],
            "open_questions": [],
        }))
        return res

    config = AgentResearchConfig(
        hermes_command="hermes",
        knowledge_roots=(("kb", str(kb_dir)), ("attachments", str(att_dir))),
    )
    run_dir = tmp_path / "run_symlinks"
    agent_synthesize({"task_snapshot": {}}, config=config, bound_sources=None, run_dir=run_dir, runner=fake_runner)

    assert (run_dir / "kb").is_symlink()
    assert (run_dir / "kb").resolve() == kb_dir.resolve()
    assert (run_dir / "attachments").is_symlink()
    assert (run_dir / "attachments").resolve() == att_dir.resolve()
    assert (run_dir / ".ripgreprc").exists()
    assert (run_dir / ".ripgreprc").read_text(encoding="utf-8") == "--follow\n"
    assert calls[0][1]["RIPGREP_CONFIG_PATH"] == str((run_dir / ".ripgreprc").resolve())


def test_agent_map_locator_run_dir_and_symlink_paths(tmp_path: Path) -> None:
    from foxhound.task_research_agent import _map_locator

    kb_dir = tmp_path / "kb_dir"
    kb_dir.mkdir()
    doc_file = kb_dir / "Meetings" / "doc.md"
    doc_file.parent.mkdir(parents=True, exist_ok=True)
    doc_file.write_text("hello")

    outside_dir = tmp_path / "outside"
    outside_dir.mkdir()
    secret = outside_dir / "secret.txt"
    secret.write_text("secret")

    run_dir = tmp_path / "run_map"
    run_dir.mkdir()
    (run_dir / "kb").symlink_to(kb_dir)
    (run_dir / "outside_sym").symlink_to(outside_dir)

    k_roots = (("kb", str(kb_dir)),)

    # run_dir relative path through symlink: kb/Meetings/doc.md
    mapped = _map_locator("kb/Meetings/doc.md#L10", k_roots, run_dir=run_dir)
    assert mapped == ("kb", "Meetings/doc.md", "L10")

    # absolute path through symlink: run_dir / kb / Meetings / doc.md
    sym_abs = str(run_dir / "kb" / "Meetings" / "doc.md")
    mapped_abs = _map_locator(sym_abs, k_roots, run_dir=run_dir)
    assert mapped_abs == ("kb", "Meetings/doc.md", None)

    # symlink escape still refused
    sym_escape = str(run_dir / "outside_sym" / "secret.txt")
    assert _map_locator(sym_escape, k_roots, run_dir=run_dir) is None
    assert _map_locator("outside_sym/secret.txt", k_roots, run_dir=run_dir) is None


def test_agent_locator_basename_resolution(tmp_path: Path) -> None:
    from foxhound.task_research_agent import _map_locator

    kb_dir = tmp_path / "kb_dir"
    kb_dir.mkdir()
    admin_dir = kb_dir / "Admin"
    admin_dir.mkdir()
    students_file = admin_dir / "Students.md"
    students_file.write_text("students content")

    # Ambiguous file in two places
    dup1 = kb_dir / "Folder1" / "Dup.md"
    dup1.parent.mkdir(parents=True, exist_ok=True)
    dup1.write_text("dup1")
    dup2 = kb_dir / "Folder2" / "Dup.md"
    dup2.parent.mkdir(parents=True, exist_ok=True)
    dup2.write_text("dup2")

    k_roots = (("kb", str(kb_dir)),)

    # Unique basename match: kb:Students.md -> Admin/Students.md
    mapped = _map_locator("kb:Students.md#L5", k_roots)
    assert mapped == ("kb", "Admin/Students.md", "L5")

    # Ambiguous match -> None
    assert _map_locator("kb:Dup.md", k_roots) is None

    # Absent match -> None
    assert _map_locator("kb:Absent.md", k_roots) is None


def test_agent_locator_single_command_prefix_normalization(tmp_path: Path) -> None:
    from foxhound.task_research_agent import _map_locator

    ro_one = [{"name": "outlook", "command": "outlook-tool", "description": "mail/cal"}]
    ro_two = [
        {"name": "outlook", "command": "outlook-tool", "description": "mail/cal"},
        {"name": "jira", "command": "jira-tool", "description": "tickets"},
    ]

    # Exactly one declared command: read_only_command: and tool: normalize to outlook:
    mapped1 = _map_locator("read_only_command:cal list --days 14", (), ro_one)
    assert mapped1 == ("tool", "outlook:cal list --days 14", None)

    mapped2 = _map_locator("tool:cal list --days 14", (), ro_one)
    assert mapped2 == ("tool", "outlook:cal list --days 14", None)

    # More than one declared command: read_only_command: and tool: are not normalized
    assert _map_locator("read_only_command:cal list --days 14", (), ro_two) is None
    assert _map_locator("tool:cal list --days 14", (), ro_two) is None


def test_convert_research_json_features(tmp_path: Path) -> None:
    from foxhound.task_research_agent import _convert_research_json
    from foxhound.task_research import render_markdown, validate_draft

    kb_dir = tmp_path / "kb"
    kb_dir.mkdir()
    doc_file = kb_dir / "note.md"
    doc_file.write_text("Hello world")
    k_roots = (("kb", str(kb_dir)),)

    # 1. Recommendation present, ownership reasoning present, blocking open question
    raw = {
        "ownership": {
            "verdict": "other:Alice",
            "reasoning": "Alice owns the student workflow. She agreed during the sync.",
            "evidence": ["kb:note.md"],
        },
        "requested_deliverable": {
            "text": "Review student submissions",
            "evidence": ["kb:note.md"],
        },
        "constraints": [],
        "entities": [],
        "facts": [
            {"text": "Deadline is tomorrow", "status": "confirmed", "evidence": ["kb:note.md"]}
        ],
        "open_questions": [
            {"text": "Is the rubric finalized?", "blocking": True},
            "Non-blocking question",
        ],
        "recommendation": {
            "text": "Follow up with Alice regarding the submissions",
            "evidence": ["kb:note.md"],
        },
    }

    draft, sources = _convert_research_json(raw, k_roots)
    assert draft["research_status"] == "inconclusive"
    assert draft["requested_action"]["text"] == "Follow up with Alice regarding the submissions"
    assert draft["objective"]["text"] == "Review student submissions"
    stakeholders = draft["stakeholders"]
    assert len(stakeholders) == 1
    assert stakeholders[0]["text"] == "Owner: other:Alice — Alice owns the student workflow"
    open_qs = draft["open_questions"]
    assert len(open_qs) == 2
    assert open_qs[0]["text"] == "Is the rubric finalized? (blocking)"
    assert open_qs[1]["text"] == "Non-blocking question (for the reader)"
    assert "recommendation" in draft
    assert len(draft["recommendation"]) == 1
    assert draft["recommendation"][0]["text"] == "Follow up with Alice regarding the submissions"

    # Validate draft
    validate_draft(draft, sources)

    # Render markdown and verify sections
    published_doc = {
        "schema_version": 1,
        "task_identity": {"task_id": 123, "task_version": 1},
        "research_status": draft["research_status"],
        "report": {k: v for k, v in draft.items() if k not in {"research_status", "scheduling_recommendations"}},
        "scheduling_recommendations": draft["scheduling_recommendations"],
        "sources": sources,
    }
    md = render_markdown(published_doc)
    assert "## Recommendation" in md
    assert "- Follow up with Alice regarding the submissions (supported) [src-001]" in md
    assert "- Owner: other:Alice — Alice owns the student workflow (supported) [src-001]" in md
    assert "Open Questions" not in md

    # Check status rules:
    # 2. Undetermined owner -> inconclusive even with no blocking questions
    raw2 = dict(raw)
    raw2["ownership"] = {"verdict": "undetermined", "evidence": []}
    raw2["open_questions"] = [{"text": "Just asking", "blocking": False}]
    draft2, sources2 = _convert_research_json(raw2, k_roots)
    assert draft2["research_status"] == "inconclusive"

    # 3. Non-blocking only + known owner + unresolved entity -> sufficient
    raw3 = dict(raw)
    raw3["ownership"] = {"verdict": "reader", "evidence": []}
    raw3["entities"] = [{"as_written": "Unresolved Thing", "status": "unresolved", "meaning": "unresolved"}]
    raw3["open_questions"] = ["Non-blocking plain string question"]
    del raw3["recommendation"]
    raw3["guide"] = {
        "path": "kb:note.md",
        "reason": "Follow student grading workflow guide",
        "evidence": [],
    }
    draft3, sources3 = _convert_research_json(raw3, k_roots)
    assert draft3["research_status"] == "sufficient"
    # When recommendation is absent, requested_action falls back to objective
    assert draft3["requested_action"]["text"] == "Review student submissions"
    assert "recommendation" not in draft3
    assert "guide" in draft3
    assert draft3["guide"]["text"] == "Follow student grading workflow guide"
    assert draft3["guide"]["status"] == "supported"
    assert len(draft3["guide"]["source_refs"]) == 1
    validate_draft(draft3, sources3)

    # If guide path is unmappable, it is dropped
    raw_unmappable_guide = dict(raw3)
    raw_unmappable_guide["guide"] = {
        "path": "kb:nonexistent_guide.md",
        "reason": "Follow nonexistent guide",
        "evidence": [],
    }
    draft_unmap, sources_unmap = _convert_research_json(raw_unmappable_guide, k_roots)
    assert "guide" not in draft_unmap

    # "No guide applies", citing only the index, is not a guide.
    (kb_dir / "guides.md").write_text("# Guides\n", encoding="utf-8")
    for negative in (
        {"path": "kb:guides.md", "reason": "No KB guide matches", "evidence": []},
        {"reason": "No KB guide matches", "evidence": ["kb:guides.md"]},
    ):
        raw_negative = dict(raw3)
        raw_negative["guide"] = negative
        draft_neg, _ = _convert_research_json(raw_negative, k_roots)
        assert "guide" not in draft_neg

    # deadline and effort conversions
    raw_timing = dict(raw3)
    raw_timing["deadline"] = {
        "date": "2026-12-01",
        "reason": "Due date moved by instructor",
        "evidence": ["kb:note.md"],
    }
    raw_timing["effort"] = {
        "size": "day",
        "reason": "One day needed for review",
        "evidence": ["kb:note.md"],
    }
    draft_timing, sources_timing = _convert_research_json(raw_timing, k_roots)
    assert "deadline" in draft_timing
    assert draft_timing["deadline"]["date"] == "2026-12-01"
    assert draft_timing["deadline"]["text"] == "Due date moved by instructor"
    assert "effort" in draft_timing
    assert draft_timing["effort"]["size"] == "day"
    assert draft_timing["effort"]["text"] == "One day needed for review"
    validate_draft(draft_timing, sources_timing)

    # invalid date / size or unmappable evidence are dropped
    raw_invalid_timing = dict(raw3)
    raw_invalid_timing["deadline"] = {
        "date": "not-a-date",
        "reason": "Invalid date",
        "evidence": ["kb:note.md"],
    }
    raw_invalid_timing["effort"] = {
        "size": "month",
        "reason": "Invalid size",
        "evidence": ["kb:note.md"],
    }
    draft_inv, _ = _convert_research_json(raw_invalid_timing, k_roots)
    assert "deadline" not in draft_inv
    assert "effort" not in draft_inv

    raw_uncited_timing = dict(raw3)
    raw_uncited_timing["deadline"] = {
        "date": "2026-12-01",
        "reason": "Uncited date",
        "evidence": ["kb:nonexistent.md"],
    }
    raw_uncited_timing["effort"] = {
        "size": "day",
        "reason": "Uncited size",
        "evidence": ["kb:nonexistent.md"],
    }
    draft_uncited, _ = _convert_research_json(raw_uncited_timing, k_roots)
    assert "deadline" not in draft_uncited
    assert "effort" not in draft_uncited
    published_doc3 = {
        "schema_version": 1,
        "task_identity": {"task_id": 123, "task_version": 1},
        "research_status": draft3["research_status"],
        "report": {k: v for k, v in draft3.items() if k not in {"research_status", "scheduling_recommendations"}},
        "scheduling_recommendations": draft3["scheduling_recommendations"],
        "sources": sources3,
    }
    md3 = render_markdown(published_doc3)
    assert "## Recommendation" not in md3

    from foxhound.task_research_agent import check_research_output

    run_dir = tmp_path / "run_task_json"
    run_dir.mkdir()

    for item in ("task.json", "./task.json", "task.json#L11", "./task.json#L5"):
        (run_dir / "research.json").write_text(json.dumps({
            "ownership": {"verdict": "reader", "evidence": [item]},
            "requested_deliverable": {"text": "Goal", "evidence": []},
            "constraints": [],
            "entities": [],
            "facts": [],
            "open_questions": [],
        }))
        _, _, problems = check_research_output(run_dir, ())
        assert any(
            "task.json is the task itself, not evidence; cite the origin record (meeting protocol, transcript, email) instead" in p
            for p in problems
        )


def test_structured_citations_and_research_check(tmp_path: Path) -> None:
    from foxhound.research_check import main as check_main
    from foxhound.task_research_agent import (
        _evidence_item_to_locator_string,
        _map_locator,
        check_research_output,
    )

    kb_dir = tmp_path / "sync_kb"
    kb_dir.mkdir()
    doc_file = kb_dir / "doc.md"
    doc_file.write_text("line 1\nline 2\n")

    k_roots = (("kb", str(kb_dir)),)
    ro_cmds = ({"name": "cal", "command": "calendar", "description": ""},)

    # 1. Object citations mapping
    # File object
    loc1, err1 = _evidence_item_to_locator_string({"root": "kb", "path": "doc.md", "lines": "2", "note": "some note"})
    assert err1 is None
    assert loc1 == "kb:doc.md#L2"
    assert _map_locator(loc1, k_roots) == ("kb", "doc.md", "L2")

    # Command object
    loc2, err2 = _evidence_item_to_locator_string({"root": "cal", "command": "list --today"})
    assert err2 is None
    assert loc2 == "cal:list --today"
    assert _map_locator(loc2, k_roots, read_only_commands=ro_cmds) == ("tool", "cal:list --today", None)

    # Web object
    loc3, err3 = _evidence_item_to_locator_string({"url": "https://example.com/spec"})
    assert err3 is None
    assert loc3 == "https://example.com/spec"
    assert _map_locator(loc3, k_roots) == ("web", "https://example.com/spec", None)

    # Legacy strings
    assert _map_locator("kb:doc.md#L2", k_roots) == ("kb", "doc.md", "L2")
    assert _map_locator("https://example.com/spec", k_roots) == ("web", "https://example.com/spec", None)

    # Unknown key -> error
    loc_err, err = _evidence_item_to_locator_string({"root": "kb", "path": "doc.md", "extra": "invalid"})
    assert err is not None
    assert "Unknown key" in err

    # 2. research-check CLI
    run_dir = tmp_path / "check_run"
    run_dir.mkdir()
    (run_dir / "task.json").write_text(json.dumps({
        "knowledge_roots": [{"name": "kb", "path": str(kb_dir)}],
        "read_only_commands": [{"name": "cal", "command": "cal", "description": ""}],
    }))

    # Valid research.json
    valid_obj = {
        "ownership": {"verdict": "reader", "evidence": [{"root": "kb", "path": "doc.md"}]},
        "requested_deliverable": {"text": "Deliverable", "evidence": [{"url": "https://example.com/page"}]},
        "constraints": [],
        "entities": [],
        "facts": [],
        "open_questions": [],
    }
    (run_dir / "research.json").write_text(json.dumps(valid_obj))

    cwd = os.getcwd()
    try:
        os.chdir(run_dir)
        ret = check_main([])
        assert ret == 0

        # Bad path
        bad_path_obj = dict(valid_obj)
        bad_path_obj["ownership"] = {"verdict": "reader", "evidence": [{"root": "kb", "path": "nonexistent.md"}]}
        (run_dir / "research.json").write_text(json.dumps(bad_path_obj))
        ret = check_main([])
        assert ret == 1

        # Unknown root
        bad_root_obj = dict(valid_obj)
        bad_root_obj["ownership"] = {"verdict": "reader", "evidence": [{"root": "unknown_root", "path": "doc.md"}]}
        (run_dir / "research.json").write_text(json.dumps(bad_root_obj))
        ret = check_main([])
        assert ret == 1

        # Bad JSON
        (run_dir / "research.json").write_text("invalid json")
        ret = check_main([])
        assert ret == 1
    finally:
        os.chdir(cwd)


def test_degraded_publication_and_metrics(tmp_path: Path) -> None:
    fake_hermes = tmp_path / "fake_hermes_degraded.py"
    kb_dir = tmp_path / "sync_kb"
    kb_dir.mkdir()
    doc_file = kb_dir / "valid.md"
    doc_file.write_text("hello")

    # One valid citation and two unmappable citations
    research_with_bad_cits = {
        "ownership": {"verdict": "reader", "evidence": [{"root": "kb", "path": "nonexistent.md"}]},
        "requested_deliverable": {"text": "Produce summary", "evidence": [{"root": "kb", "path": "valid.md"}]},
        "constraints": [{"text": "Be fast", "binding": True, "evidence": ["kb:ghost.md"]}],
        "entities": [],
        "facts": [{"text": "A fact", "status": "confirmed", "evidence": []}],
        "open_questions": [],
    }

    _write_fake_hermes(fake_hermes, output_json=research_with_bad_cits)

    config = AgentResearchConfig(
        hermes_command=str(fake_hermes),
        model="test-model",
        knowledge_roots=(("kb", str(kb_dir)),),
    )
    ctx = {
        "task_snapshot": {"task_id": 789, "title": "Degraded Task"},
        "origin": {"kind": "issue", "record_id": "repo/test"},
    }
    run_dir = tmp_path / "run_degraded"
    result = agent_synthesize(ctx, config=config, bound_sources=None, run_dir=run_dir)

    assert result.metrics["degraded"] is True
    assert result.metrics["self_check_ok"] is False
    assert result.metrics["dropped_citations"] == 2
    assert result.metrics["unsourced_claims"] == 2
    assert result.coverage["degraded"] is True
    assert result.coverage["dropped_citations"] == 2
    assert result.coverage["unsourced_claims"] == 2

    # Verify ownership claim became "unsourced" because its only evidence was dropped
    draft_dict: dict[str, Any] = result.draft  # type: ignore
    assert draft_dict["stakeholders"][0]["status"] == "unsourced"
    assert draft_dict["stakeholders"][0]["source_refs"] == []

    # Constraints claim became "unsourced"
    assert draft_dict["constraints"][0]["status"] == "unsourced"
    assert draft_dict["constraints"][0]["source_refs"] == []

    # Deliverable kept its valid source
    assert draft_dict["objective"]["status"] == "supported"
    assert len(draft_dict["objective"]["source_refs"]) == 1

    # Validate draft accepts unsourced status
    validate_draft(result.draft, list(result.sources))


def test_missing_or_unparseable_research_json_fails(tmp_path: Path) -> None:
    fake_hermes = tmp_path / "fake_hermes_fail.py"
    # hermes creates no research.json
    _write_fake_hermes(fake_hermes, output_json=None)

    config = AgentResearchConfig(
        hermes_command=str(fake_hermes),
        model="test-model",
    )
    ctx = {
        "task_snapshot": {"task_id": 999, "title": "Failing Task"},
        "origin": {"kind": "issue", "record_id": "repo/test"},
    }
    run_dir = tmp_path / "run_fail"
    with pytest.raises(SynthesisError) as exc:
        agent_synthesize(ctx, config=config, bound_sources=None, run_dir=run_dir)
    assert exc.value.code == "draft_missing"


def test_note_is_accepted_on_url_and_command_citations():
    import tempfile, json as _json
    from pathlib import Path as _P
    from foxhound.task_research_agent import check_research_output
    with tempfile.TemporaryDirectory() as tmp:
        run = _P(tmp)
        doc = {
            "ownership": {"verdict": "reader", "evidence": [
                {"url": "https://example.org/a", "note": "official page"}]},
            "requested_deliverable": {"text": "Do X.", "evidence": [
                {"root": "mail", "command": "search alpha", "note": "thread"}]},
            "constraints": [], "entities": [], "facts": [], "open_questions": [],
        }
        (run / "research.json").write_text(_json.dumps(doc), encoding="utf-8")
        _, _, problems = check_research_output(
            run, (), [{"name": "mail", "command": "/bin/true", "description": "d"}],
        )
        assert problems == []


def test_repair_locator_rules(tmp_path: Path) -> None:
    from foxhound.task_research_agent import _repair_locator, _map_locator
    kb_dir = tmp_path / "kb"
    kb_dir.mkdir()
    (kb_dir / "notes").mkdir()
    doc1 = kb_dir / "notes" / "alpha.md"
    doc1.write_text("Hello alpha")

    sub_dir = kb_dir / "Meetings" / "2026"
    sub_dir.mkdir(parents=True)
    doc2 = sub_dir / "protocol.md"
    doc2.write_text("Protocol notes")

    roots = (("kb", str(kb_dir)),)

    # Rule 1: strip wrapping quotes, backticks, angle brackets and trailing punctuation
    r1 = _repair_locator('"kb:notes/alpha.md")', roots)
    assert r1 == "kb:notes/alpha.md"
    assert _map_locator(r1, roots) is not None

    r1_ticks = _repair_locator("`<kb:notes/alpha.md>:`", roots)
    assert r1_ticks == "kb:notes/alpha.md"

    # Rule 2: path:LINE or path:LINE-LINE -> path#LLINE fragment form
    r2_single = _repair_locator("kb:notes/alpha.md:14", roots)
    assert r2_single == "kb:notes/alpha.md#L14"
    assert _map_locator(r2_single, roots) is not None

    r2_range = _repair_locator("kb:notes/alpha.md:14-25", roots)
    assert r2_range == "kb:notes/alpha.md#L14"
    assert _map_locator(r2_range, roots) is not None

    # Rule 3: absolute path under a knowledge root
    r3 = _repair_locator(str(doc1), roots)
    assert r3 == "kb:notes/alpha.md"
    assert _map_locator(r3, roots) is not None

    # Rule 4: path relative to run directory through a symlink
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    (run_dir / "kb_link").symlink_to(kb_dir, target_is_directory=True)
    r4 = _repair_locator("kb_link/notes/alpha.md", roots, run_dir=run_dir)
    assert r4 == "kb:notes/alpha.md"
    assert _map_locator(r4, roots) is not None

    # Rule 5: bare file name unique across knowledge roots
    r5 = _repair_locator("protocol.md", roots)
    assert r5 == "kb:Meetings/2026/protocol.md"
    assert _map_locator(r5, roots) is not None

    # Rule 6: path with one wrong leading directory (e.g. Meetings/protocol.md)
    r6 = _repair_locator("kb:Meetings/protocol.md", roots)
    assert r6 == "kb:Meetings/2026/protocol.md"
    assert _map_locator(r6, roots) is not None


def test_repair_locator_ambiguity(tmp_path: Path) -> None:
    from foxhound.task_research_agent import _repair_locator
    kb_dir = tmp_path / "kb"
    (kb_dir / "dir1").mkdir(parents=True)
    (kb_dir / "dir2").mkdir(parents=True)
    (kb_dir / "dir1" / "dup.txt").write_text("1")
    (kb_dir / "dir2" / "dup.txt").write_text("2")

    roots = (("kb", str(kb_dir)),)
    # dup.txt is ambiguous across kb roots
    assert _repair_locator("dup.txt", roots) is None
    assert _repair_locator("kb:dup.txt", roots) is None


def test_agent_synthesize_metrics_dropped_and_repaired(tmp_path: Path) -> None:
    kb_dir = tmp_path / "kb"
    kb_dir.mkdir()
    doc = kb_dir / "file.txt"
    doc.write_text("content")

    # 1 valid, 1 repairable ("<kb:file.txt:10>"), 1 totally unmappable
    research_doc = {
        "ownership": {"verdict": "reader", "evidence": [str(doc)]},
        "requested_deliverable": {"text": "Do task", "evidence": ["<kb:file.txt:10>"]},
        "constraints": [{"text": "constraint", "evidence": ["nonexistent_bogus_xyz.pdf"]}],
        "entities": [],
        "facts": [],
        "open_questions": [],
    }

    run_dir = tmp_path / "run_metrics"
    run_dir.mkdir()
    (run_dir / "research.json").write_text(json.dumps(research_doc), encoding="utf-8")

    def fake_runner(argv, cwd, env, timeout, capture_output, text):
        res = MagicMock()
        res.returncode = 0
        res.stdout = "done"
        res.stderr = ""
        return res

    config = AgentResearchConfig(
        hermes_command="hermes",
        model="test-model",
        knowledge_roots=(("kb", str(kb_dir)),),
    )

    result = agent_synthesize(
        {"task_snapshot": {}},
        config=config,
        bound_sources=None,
        run_dir=run_dir,
        runner=fake_runner,
    )

    assert result.metrics["degraded"] is True
    assert result.metrics["repaired_citations"] == 1
    assert result.metrics["dropped_citations"] == 1
    assert result.metrics["dropped_locators"] == ["nonexistent_bogus_xyz.pdf"]


def test_agent_synthesize_repair_turn_suggests_nearest_path(tmp_path: Path) -> None:
    kb_dir = tmp_path / "kb"
    kb_dir.mkdir()
    (kb_dir / "Meeting_Notes_2026.md").write_text("Notes")

    bad_json = {
        "ownership": {"verdict": "reader", "evidence": ["Meeting_Notes_2025.md"]},
        "requested_deliverable": {"text": "Deliverable", "evidence": []},
        "constraints": [],
        "entities": [],
        "facts": [],
        "open_questions": [],
    }
    fixed_json = {
        "ownership": {"verdict": "reader", "evidence": ["kb:Meeting_Notes_2026.md"]},
        "requested_deliverable": {"text": "Deliverable", "evidence": []},
        "constraints": [],
        "entities": [],
        "facts": [],
        "open_questions": [],
    }

    captured_queries = []

    def fake_runner(argv, cwd, env, timeout, capture_output, text):
        res = MagicMock()
        res.returncode = 0
        res.stderr = ""
        query_idx = argv.index("--query") + 1
        captured_queries.append(argv[query_idx])
        if len(captured_queries) == 1:
            res.stdout = "session_id: sess-nearest-test\nOutput ready."
            (cwd / "research.json").write_text(json.dumps(bad_json))
        else:
            res.stdout = "Repaired."
            (cwd / "research.json").write_text(json.dumps(fixed_json))
        return res

    config = AgentResearchConfig(
        hermes_command="hermes",
        model="test-model",
        knowledge_roots=(("kb", str(kb_dir)),),
    )
    run_dir = tmp_path / "run_repair_suggest"
    result = agent_synthesize(
        {"task_snapshot": {}},
        config=config,
        bound_sources=None,
        run_dir=run_dir,
        runner=fake_runner,
    )

    assert result.metrics["repair_turns"] == 1
    assert len(captured_queries) == 2
    repair_msg = captured_queries[1]
    assert "Meeting_Notes_2025.md" in repair_msg
    assert "kb:Meeting_Notes_2026.md" in repair_msg


def test_convert_research_json_scheduling(tmp_path: Path) -> None:
    from foxhound.task_research import validate_draft
    from foxhound.task_research_agent import _convert_research_json

    kb_dir = tmp_path / "kb"
    kb_dir.mkdir()
    (kb_dir / "dep.md").write_text("Dependency notes\n")
    (kb_dir / "date.md").write_text("Date notes\n")

    k_roots = (("kb", str(kb_dir)),)

    # 1. Valid cited items + 1 uncited item -> only 2 cited kept
    raw = {
        "ownership": {"verdict": "reader", "evidence": []},
        "requested_deliverable": {"text": "Deliverable", "evidence": []},
        "scheduling": [
            {
                "type": "after_task_completed",
                "task": "T42",
                "confidence": 0.85,
                "reason": "Wait for T42 to complete",
                "evidence": ["kb:dep.md"],
            },
            {
                "type": "not_before",
                "not_before": "2026-11-01",
                "confidence": 0.9,
                "reason": "Wait until November 1",
                "evidence": ["kb:date.md"],
            },
            {
                "type": "raise_priority",
                "confidence": 0.7,
                "reason": "Uncited priority raise",
                "evidence": [],
            },
        ],
    }

    draft, sources = _convert_research_json(raw, k_roots)
    recs = draft["scheduling_recommendations"]
    assert len(recs) == 2
    assert recs[0]["type"] == "after_task_completed"
    assert recs[0]["related_task_id"] == 42
    assert recs[0]["confidence"] == 0.85
    assert recs[0]["rationale"]["text"] == "Wait for T42 to complete"
    assert len(recs[0]["rationale"]["source_refs"]) == 1

    assert recs[1]["type"] == "not_before"
    assert recs[1]["not_before"] == "2026-11-01T00:00:00Z"
    assert recs[1]["confidence"] == 0.9
    assert recs[1]["rationale"]["text"] == "Wait until November 1"
    assert len(recs[1]["rationale"]["source_refs"]) == 1

    validate_draft(draft, sources)

    # 2. Self-reference dropped
    run_dir = tmp_path / "run_self_ref"
    run_dir.mkdir()
    (run_dir / "task.json").write_text(json.dumps({"task_id": 42}))

    raw_self = {
        "ownership": {"verdict": "reader", "evidence": []},
        "requested_deliverable": {"text": "Deliverable", "evidence": []},
        "scheduling": [
            {
                "type": "after_task_completed",
                "task": "42",
                "confidence": 0.9,
                "reason": "Wait for self",
                "evidence": ["kb:dep.md"],
            },
            {
                "type": "after_task_completed",
                "task": "T43",
                "confidence": 0.95,
                "reason": "Wait for 43",
                "evidence": ["kb:dep.md"],
            },
        ],
    }
    draft_self, sources_self = _convert_research_json(raw_self, k_roots, run_dir=run_dir)
    recs_self = draft_self["scheduling_recommendations"]
    assert len(recs_self) == 1
    assert recs_self[0]["related_task_id"] == 43
    validate_draft(draft_self, sources_self)

    # 3. Truncation to at most 3 items
    raw_many = {
        "ownership": {"verdict": "reader", "evidence": []},
        "requested_deliverable": {"text": "Deliverable", "evidence": []},
        "scheduling": [
            {
                "type": "after_task_completed",
                "task": f"T{i}",
                "confidence": 0.9,
                "reason": f"Reason {i}",
                "evidence": ["kb:dep.md"],
            }
            for i in range(1, 6)
        ],
    }
    draft_many, sources_many = _convert_research_json(raw_many, k_roots)
    recs_many = draft_many["scheduling_recommendations"]
    assert len(recs_many) == 3
    assert [r["related_task_id"] for r in recs_many] == [1, 2, 3]
    validate_draft(draft_many, sources_many)
