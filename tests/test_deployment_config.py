"""Tests for the private deployment configuration boundary."""

from __future__ import annotations

import contextlib
import io
import json
from pathlib import Path
import socket
import stat
import tempfile
import unittest
from unittest import mock

from foxhound.database_lifecycle import migrate_database
from foxhound.deployment_config import (
    DeploymentConfigError,
    load_deployment_config,
    main,
)
from foxhound.execution_runner import _parser as runner_parser
from foxhound.execution_schedule import _parser as schedule_parser
from foxhound import task_card_server


LOOPBACK = socket.inet_ntoa(bytes.fromhex("7f000001"))


class DeploymentConfigTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.database = self.root / "foxhound.sqlite3"
        migrate_database(self.database)
        self.task_token = self._private_file("task.token", "a" * 32)
        self.execution_token = self._private_file("execution.token", "b" * 32)
        self.gateway_token = self._private_file("gateway.token", "c" * 32)
        self.config_path = self.root / "deployment.json"

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def _private_file(self, name: str, text: str) -> Path:
        path = self.root / name
        path.write_text(text, encoding="utf-8")
        path.chmod(0o600)
        return path

    def _document(self) -> dict[str, object]:
        return {
            "schema": "foxhound.deployment-config",
            "schema_version": 1,
            "database": str(self.database),
            "agent_profile_directory": None,
            "card_service": {
                "enabled": True,
                "bind": LOOPBACK,
                "port": 8790,
                "request_timeout_seconds": 5,
                "task_token_files": {"drip": str(self.task_token)},
                "execution_card_delivery": True,
                "execution_token_files": {"drip": str(self.execution_token)},
            },
            "workflow": {
                "default_agent_profile": "general",
                "plan_without_asking": ["issue"],
                "execution_slot_cap": 2,
                "plan_ready_cap": 10,
                "awaiting_reader_cap": 20,
            },
            "execution_runner": {
                "enabled": True,
                "run_root": str(self.root / "runs"),
                "gw_endpoint": f"http://{LOOPBACK}:8787",
                "gw_alias": "example-operator",
                "gw_token_file": str(self.gateway_token),
                "agent_command": "hermes",
                "worker_command": "foxhound-task-worker",
                "runner_slot": "primary",
                "knowledge_root": None,
                "task_work_root": None,
                "task_kb_root": None,
            },
        }

    def _write_config(self, document: dict[str, object]) -> None:
        self.config_path.write_text(json.dumps(document), encoding="utf-8")
        self.config_path.chmod(0o600)

    def test_loads_valid_configuration_and_renders_known_commands(self) -> None:
        self._write_config(self._document())

        config = load_deployment_config(self.config_path)

        cards = config.argv("task-cards")
        self.assertEqual(cards[0], "foxhound-task-cards")
        self.assertIn("drip=" + str(self.task_token), cards)
        self.assertIn("drip=" + str(self.execution_token), cards)
        schedule = config.argv("execution-schedule")
        self.assertEqual(schedule[0], "foxhound-execution-schedule")
        self.assertNotIn("--execution-slot-cap", schedule)
        runner = config.argv("execution-runner")
        self.assertEqual(runner[0], "foxhound-execution-runner")
        self.assertIn("--execution-slot-cap", runner)
        self.assertIn("--plan-without-asking", runner)

    def test_rendered_arguments_are_accepted_by_the_supported_commands(self) -> None:
        self._write_config(self._document())
        config = load_deployment_config(self.config_path)

        cards = config.argv("task-cards")
        with mock.patch.object(task_card_server, "serve") as serve:
            self.assertEqual(task_card_server.main(cards[1:]), 0)
        serve.assert_called_once()
        schedule_parser().parse_args(config.argv("execution-schedule")[1:])
        runner_parser().parse_args(config.argv("execution-runner")[1:])

    def test_execution_delivery_requires_the_drip_role(self) -> None:
        document = self._document()
        document["card_service"]["execution_token_files"] = {  # type: ignore[index]
            "queue_view": str(self.execution_token)
        }
        self._write_config(document)

        with self.assertRaises(DeploymentConfigError):
            load_deployment_config(self.config_path)

    def test_rejects_an_insecure_token_file(self) -> None:
        self._write_config(self._document())
        self.execution_token.chmod(0o644)

        with self.assertRaises(DeploymentConfigError):
            load_deployment_config(self.config_path)

    def test_rejects_unknown_configuration_fields(self) -> None:
        document = self._document()
        document["workflow"]["extra"] = True  # type: ignore[index]
        self._write_config(document)

        with self.assertRaises(DeploymentConfigError):
            load_deployment_config(self.config_path)

    def test_invalid_cli_output_is_content_free(self) -> None:
        document = self._document()
        document["card_service"]["bind"] = "not-loopback"  # type: ignore[index]
        self._write_config(document)
        output = io.StringIO()
        with contextlib.redirect_stderr(output):
            self.assertEqual(main(["--config", str(self.config_path), "validate"]), 78)

        self.assertEqual(
            output.getvalue(),
            "foxhound deployment configuration: unavailable\n",
        )
        self.assertNotIn(str(self.root), output.getvalue())

    def test_configuration_file_must_be_owner_only(self) -> None:
        self._write_config(self._document())
        self.config_path.chmod(stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP)

        with self.assertRaises(DeploymentConfigError):
            load_deployment_config(self.config_path)


if __name__ == "__main__":
    unittest.main()
