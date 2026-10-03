"""Tests for foxhound.work_first_runner and component_command."""

from __future__ import annotations

import io
import json
import os
from pathlib import Path
import stat
import sys
import tempfile
import unittest
from unittest import mock

from foxhound.deployment_config import (
    DeploymentConfig,
    DeploymentConfigError,
    component_command,
    load_deployment_config,
)
from foxhound.work_first_runner import _parse_last_json_line, main


def _create_dummy_executable(directory: Path, name: str, script_body: str) -> Path:
    script_path = directory / name
    script_path.write_text(f"#!/usr/bin/env python3\n{script_body}\n", encoding="utf-8")
    script_path.chmod(script_path.stat().st_mode | stat.S_IXUSR | stat.S_IRUSR)
    return script_path


class WorkFirstRunnerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.bin_dir = self.root / "bin"
        self.bin_dir.mkdir(parents=True)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_parse_last_json_line(self) -> None:
        text = "some log line\n{\"first\": 1}\nmore logs\n{\"outcome\": \"idle\"}\ntrailing noise"
        self.assertEqual(_parse_last_json_line(text), {"outcome": "idle"})
        self.assertIsNone(_parse_last_json_line("no json here"))
        self.assertIsNone(_parse_last_json_line("{invalid json"))

    def test_component_command_sets_voice_summaries_for_execution_runner_when_disabled(self) -> None:
        config_mock = mock.MagicMock()
        config_mock.argv.return_value = ["foxhound-execution-runner", "--flag"]
        config_mock.workflow.voice_summaries = False

        exec_path = self.bin_dir / "foxhound-execution-runner"
        exec_path.write_text("#!/bin/sh\n", encoding="utf-8")

        argv, extra_env = component_command(
            config_mock,
            "execution-runner:primary",
            script_directory=self.bin_dir,
        )
        self.assertEqual(argv[0], str(exec_path))
        self.assertEqual(argv[1:], ["--flag"])
        self.assertEqual(extra_env, {"FOXHOUND_VOICE_SUMMARIES": "0"})

    def test_component_command_omits_voice_summaries_when_enabled(self) -> None:
        config_mock = mock.MagicMock()
        config_mock.argv.return_value = ["foxhound-execution-runner", "--flag"]
        config_mock.workflow.voice_summaries = True

        exec_path = self.bin_dir / "foxhound-execution-runner"
        exec_path.write_text("#!/bin/sh\n", encoding="utf-8")

        argv, extra_env = component_command(
            config_mock,
            "execution-runner:primary",
            script_directory=self.bin_dir,
        )
        self.assertEqual(extra_env, {})

    def test_component_command_missing_executable_raises_error(self) -> None:
        config_mock = mock.MagicMock()
        config_mock.argv.return_value = ["foxhound-execution-runner"]
        config_mock.workflow.voice_summaries = True

        with self.assertRaises(DeploymentConfigError):
            component_command(
                config_mock,
                "execution-runner:primary",
                script_directory=self.bin_dir,
            )

    def test_config_load_failure_returns_78_and_json_error(self) -> None:
        stdout = io.StringIO()
        stderr = io.StringIO()
        with mock.patch("sys.stdout", stdout), mock.patch("sys.stderr", stderr):
            with mock.patch("foxhound.work_first_runner.load_deployment_config") as load_cfg:
                load_cfg.side_effect = DeploymentConfigError("bad config")
                exit_code = main(["--config", "/path/to/config.json", "--execution-component", "execution-runner:slot-1"])

        self.assertEqual(exit_code, 78)
        lines = stdout.getvalue().splitlines()
        self.assertEqual(len(lines), 1)
        data = json.loads(lines[0])
        self.assertEqual(data, {"error_code": "configuration_unavailable", "ok": False})

    def test_execution_did_work_research_not_run(self) -> None:
        exec_script = _create_dummy_executable(
            self.bin_dir,
            "fake-exec",
            "import json; print('child stdout'); print(json.dumps({'ok': True, 'outcome': 'executed'}))",
        )
        research_script = _create_dummy_executable(
            self.bin_dir,
            "fake-research",
            "import sys; sys.exit(99)",
        )

        stdout = io.StringIO()
        stderr = io.StringIO()
        with (
            mock.patch("sys.stdout", stdout),
            mock.patch("sys.stderr", stderr),
            mock.patch("foxhound.work_first_runner.load_deployment_config"),
            mock.patch(
                "foxhound.work_first_runner.component_command",
                side_effect=[
                    ([str(exec_script)], {}),
                    ([str(research_script)], {}),
                ],
            ),
        ):
            exit_code = main(["--config", "/cfg.json", "--execution-component", "exec:1"])

        self.assertEqual(exit_code, 0)
        lines = stdout.getvalue().splitlines()
        self.assertEqual(len(lines), 1)
        report = json.loads(lines[0])
        self.assertEqual(report, {"child_exit": 0, "ok": True, "ran": "execution"})
        # Child stdout is forwarded to stderr
        self.assertIn("child stdout", stderr.getvalue())

    def test_execution_idle_research_runs_and_did_work(self) -> None:
        exec_script = _create_dummy_executable(
            self.bin_dir,
            "fake-exec",
            "import json; print(json.dumps({'ok': True, 'outcome': 'idle'}))",
        )
        research_script = _create_dummy_executable(
            self.bin_dir,
            "fake-research",
            "import json; print(json.dumps({'accepted': True, 'claimed': True}))",
        )

        stdout = io.StringIO()
        stderr = io.StringIO()
        with (
            mock.patch("sys.stdout", stdout),
            mock.patch("sys.stderr", stderr),
            mock.patch("foxhound.work_first_runner.load_deployment_config"),
            mock.patch(
                "foxhound.work_first_runner.component_command",
                side_effect=[
                    ([str(exec_script)], {}),
                    ([str(research_script)], {}),
                ],
            ),
        ):
            exit_code = main(["--config", "/cfg.json", "--execution-component", "exec:1"])

        self.assertEqual(exit_code, 0)
        lines = stdout.getvalue().splitlines()
        self.assertEqual(len(lines), 1)
        report = json.loads(lines[0])
        self.assertEqual(report, {"child_exit": 0, "ok": True, "ran": "research"})

    def test_both_idle_returns_0_and_ran_idle(self) -> None:
        exec_script = _create_dummy_executable(
            self.bin_dir,
            "fake-exec",
            "import json; print(json.dumps({'ok': True, 'outcome': 'idle'}))",
        )
        research_script = _create_dummy_executable(
            self.bin_dir,
            "fake-research",
            "import json; print(json.dumps({'accepted': True, 'claimed': False}))",
        )

        stdout = io.StringIO()
        stderr = io.StringIO()
        with (
            mock.patch("sys.stdout", stdout),
            mock.patch("sys.stderr", stderr),
            mock.patch("foxhound.work_first_runner.load_deployment_config"),
            mock.patch(
                "foxhound.work_first_runner.component_command",
                side_effect=[
                    ([str(exec_script)], {}),
                    ([str(research_script)], {}),
                ],
            ),
        ):
            exit_code = main(["--config", "/cfg.json", "--execution-component", "exec:1"])

        self.assertEqual(exit_code, 0)
        lines = stdout.getvalue().splitlines()
        self.assertEqual(len(lines), 1)
        report = json.loads(lines[0])
        self.assertEqual(report, {"child_exit": 0, "ok": True, "ran": "idle"})

    def test_execution_failure_propagates_exit_code_and_skips_research(self) -> None:
        exec_script = _create_dummy_executable(
            self.bin_dir,
            "fake-exec",
            "import sys; sys.exit(70)",
        )
        research_script = _create_dummy_executable(
            self.bin_dir,
            "fake-research",
            "import sys; sys.exit(99)",
        )

        stdout = io.StringIO()
        stderr = io.StringIO()
        with (
            mock.patch("sys.stdout", stdout),
            mock.patch("sys.stderr", stderr),
            mock.patch("foxhound.work_first_runner.load_deployment_config"),
            mock.patch(
                "foxhound.work_first_runner.component_command",
                side_effect=[
                    ([str(exec_script)], {}),
                    ([str(research_script)], {}),
                ],
            ),
        ):
            exit_code = main(["--config", "/cfg.json", "--execution-component", "exec:1"])

        self.assertEqual(exit_code, 70)
        lines = stdout.getvalue().splitlines()
        self.assertEqual(len(lines), 1)
        report = json.loads(lines[0])
        self.assertEqual(report, {"child_exit": 70, "ok": False, "ran": "execution"})

    def test_research_failure_propagates_exit_code(self) -> None:
        exec_script = _create_dummy_executable(
            self.bin_dir,
            "fake-exec",
            "import json; print(json.dumps({'ok': True, 'outcome': 'idle'}))",
        )
        research_script = _create_dummy_executable(
            self.bin_dir,
            "fake-research",
            "import sys; sys.exit(70)",
        )

        stdout = io.StringIO()
        stderr = io.StringIO()
        with (
            mock.patch("sys.stdout", stdout),
            mock.patch("sys.stderr", stderr),
            mock.patch("foxhound.work_first_runner.load_deployment_config"),
            mock.patch(
                "foxhound.work_first_runner.component_command",
                side_effect=[
                    ([str(exec_script)], {}),
                    ([str(research_script)], {}),
                ],
            ),
        ):
            exit_code = main(["--config", "/cfg.json", "--execution-component", "exec:1"])

        self.assertEqual(exit_code, 70)
        lines = stdout.getvalue().splitlines()
        self.assertEqual(len(lines), 1)
        report = json.loads(lines[0])
        self.assertEqual(report, {"child_exit": 70, "ok": False, "ran": "research"})


if __name__ == "__main__":
    unittest.main()
