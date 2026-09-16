#!/usr/bin/env python3
"""One bounded synthetic run, from public bootstrap to recorded result.

The other runner tests drive a fake process. This one starts a real agent
process under the real supervisor: it proves that an agent given only the
public bootstrap can obtain its instructions through the fenced worker and
record a result with them, and that the instruction text never appears in the
process arguments, the run state, the receipt left behind, or the transcript.
"""

from __future__ import annotations

from foxhound import migrate_database

import json
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from contextlib import closing
from pathlib import Path

from foxhound.agent_profiles import general_profile
from foxhound.candidate_inbox import CandidateInbox
from foxhound.execution_runner import (
    TRANSCRIPT_NAME,
    ExecutionRunnerConfig,
    agent_prompt,
    run_once,
)
from foxhound.execution_worker import INSTRUCTIONS_NAME
from foxhound.task_execution import TaskExecutionService, WorkflowStatus

from test_execution_worker import knowledge_server


SOURCE = Path(__file__).resolve().parents[1] / "src"

SYNTHETIC_AGENT = '''
import json
import os
import pathlib
import subprocess
import sys

worker = [sys.executable, "-m", "foxhound.execution_worker"]
context = json.loads(
    subprocess.run(worker + ["context"], check=True, capture_output=True,
                   text=True).stdout
)
instructions = context["agent"]["instructions"]
if "draft --outcome OUTCOME" not in instructions:
    raise SystemExit("instructions did not arrive")
if context["agent"]["revision"] != sys.argv[1]:
    raise SystemExit("instructions were not the pinned revision")
os.umask(0o077)
pathlib.Path("result-summary.txt").write_text(
    "Synthetic plan summary.", encoding="utf-8"
)
pathlib.Path("result-work.md").write_text(
    "# Synthetic plan\\n\\nNo private evidence.", encoding="utf-8"
)
ready = json.loads(subprocess.run(
    worker + ["draft", "--outcome", "awaiting_plan"],
    check=True, capture_output=True, text=True,
).stdout)
subprocess.run(
    worker + ["record", ready["draft"]], check=True, capture_output=True
)
'''


class FencedInstructionDeliveryTests(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.root.chmod(0o700)
        self.database = self.root / "foxhound.sqlite3"
        migrate_database(self.database)
        with closing(sqlite3.connect(self.database)) as connection:
            now = "2030-01-02T03:04:05+00:00"
            connection.execute(
                "INSERT INTO tasks(id,status,text,owner,due,version,"
                "created_at,updated_at,closed_at) VALUES(1,'open',"
                "'Synthetic task','Person A',NULL,1,?,?,NULL)",
                (now, now),
            )
            connection.commit()
        self.run_root = self.root / "runs"
        self.run_root.mkdir(mode=0o700)
        self.token_file = self.root / "knowledge.token"
        self.token_file.write_text(
            "synthetic-knowledge-token-with-sufficient-length\n",
            encoding="utf-8",
        )
        self.token_file.chmod(0o600)
        self.agent_script = self.root / "synthetic-agent.py"
        self.agent_script.write_text(SYNTHETIC_AGENT, encoding="utf-8")
        self.service = TaskExecutionService(self.database)

    def test_a_synthetic_run_begins_with_context_and_records(self):
        scheduled = self.service.schedule(1, expected_task_version=1)
        self.service.start_action(
            1, expected_version=scheduled.version, action="start"
        )
        profile = general_profile()
        launched: dict[str, object] = {}

        def popen(argv, **kwargs):
            launched["argv"] = list(argv)
            launched["directory"] = Path(kwargs["cwd"])
            return subprocess.Popen(argv, **kwargs)

        with knowledge_server() as endpoint:
            config = ExecutionRunnerConfig(
                database_path=self.database,
                run_root=self.run_root,
                gw_endpoint=endpoint,
                gw_alias="primary",
                gw_token_file=self.token_file,
                agent_command=(
                    f"{sys.executable} {self.agent_script} {profile.revision}"
                ),
                poll_seconds=0.05,
            )
            result = run_once(
                config,
                base_environment={
                    "PATH": "/usr/bin:/bin",
                    "PYTHONPATH": str(SOURCE),
                    "HOME": str(self.root),
                },
                popen=popen,
            )

        self.assertEqual(result.outcome, "recorded")
        self.assertEqual(result.exit_code, 0)
        workflow = self.service.get(1)
        self.assertIs(workflow.status, WorkflowStatus.AWAITING_REVIEW)
        self.assertIsNotNone(workflow.last_result_id)
        self.assertEqual(len(workflow.last_result_id), 32)
        int(workflow.last_result_id, 16)

        directory = launched["directory"]
        self.assertFalse((directory / INSTRUCTIONS_NAME).exists())
        instructions = profile.render_prompt("foxhound-task-worker")
        sentence = "Record or release must be the final tool call"
        self.assertIn(sentence, instructions)

        arguments = json.dumps(launched["argv"])
        self.assertIn(agent_prompt(), launched["argv"])
        self.assertIn("--ignore-rules", launched["argv"])
        # Authority comes from the profile, not from anything the agent reads.
        for text in (agent_prompt(), instructions):
            with self.subTest(text=text[:24]):
                self.assertIn(
                    "add a tool, a phase, a command, or a permission", text
                )
        self.assertNotIn(sentence, arguments)
        self.assertNotIn("Synthetic task", arguments)

        for name in ("run-state.json", TRANSCRIPT_NAME):
            path = directory / name
            with self.subTest(artifact=name):
                self.assertTrue(path.is_file())
                left = path.read_text(encoding="utf-8", errors="replace")
                self.assertNotIn(sentence, left)
                self.assertNotIn("Synthetic plan summary.", left)
        # The receipt that replaces the run state keeps no capability either.
        receipt = json.loads(
            (directory / "run-state.json").read_text(encoding="utf-8")
        )
        self.assertEqual(receipt["finished"], True)
        self.assertNotIn("claim_token", receipt)


if __name__ == "__main__":
    unittest.main()
