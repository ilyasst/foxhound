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
from foxhound import deployment_config
from foxhound.agent_profiles import AgentProfile, WORKER_COMMAND_TOKEN
from foxhound.deployment_config import (
    DeploymentConfigError,
    execute_component,
    load_deployment_config,
    main,
)
from foxhound.execution_runner import _parser as runner_parser
from foxhound.execution_schedule import _parser as schedule_parser
from foxhound.candidate_feed_import import _parser as candidate_import_parser
from foxhound.execution_card_requeue import _parser as requeue_parser
from foxhound.task_card_requeue import _parser as task_requeue_parser
from foxhound.fused_task_titles import _parser as fused_titles_parser
from foxhound.native_intake import _parser as native_intake_parser
from foxhound.task_duplicate_card_schedule import (
    _parser as duplicate_schedule_parser,
)
from foxhound import task_card_server
from foxhound.task_lifecycle_outcome_export import _parser as lifecycle_export_parser


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
        self.task_work_root = self.root / "task-work"
        self.task_work_root.mkdir(mode=0o700)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def _private_file(self, name: str, text: str) -> Path:
        path = self.root / name
        path.write_text(text, encoding="utf-8")
        path.chmod(0o600)
        return path

    def _drop_runtime_log_keys(self, document: dict[str, object]) -> None:
        """Remove the keys a document written before version 12 never had."""
        for runner in document["execution_runners"]:  # type: ignore[index]
            del runner["runtime_session_database"]
            del runner["runtime_log_retention_bytes"]

    def _drop_deployment_root_keys(self, document: dict[str, object]) -> None:
        """Remove the key a document written before version 13 never had."""
        for runner in document["execution_runners"]:  # type: ignore[index]
            runner.pop("deployment_roots", None)

    def _document(self) -> dict[str, object]:
        return {
            "schema": "foxhound.deployment-config",
            "schema_version": 13,
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
                "task_work_root": str(self.task_work_root),
                "gw_endpoint": f"http://{LOOPBACK}:8787",
                "gw_alias": "example-operator",
                "gw_token_file": str(self.gateway_token),
            },
            "workflow": {
                "default_agent_profile": "general",
                "agent_profile_routes": [],
                "plan_without_asking": ["issue"],
                "execute_without_asking": ["issue"],
                "skip_planning_for": ["issue"],
                "act_without_asking": ["issue"],
                "reader_aliases": [],
                "execution_slot_cap": 2,
                "plan_ready_cap": 10,
                "awaiting_reader_cap": 20,
            },
            "execution_runners": [{
                "enabled": True,
                "run_root": str(self.root / "runs"),
                "gw_endpoint": f"http://{LOOPBACK}:8787",
                "gw_alias": "example-operator",
                "gw_token_file": str(self.gateway_token),
                "agent_command": "hermes",
                "worker_command": "foxhound-task-worker",
                "runner_slot": "primary",
                "knowledge_root": None,
                "deployment_roots": {},
                "task_work_root": None,
                "task_kb_root": None,
                "runtime_session_database": None,
                "runtime_log_retention_bytes": None,
            }, {
                "enabled": True,
                "run_root": str(self.root / "runs-secondary"),
                "gw_endpoint": f"http://{LOOPBACK}:8787",
                "gw_alias": "example-operator",
                "gw_token_file": str(self.gateway_token),
                "agent_command": "hermes",
                "worker_command": "foxhound-task-worker",
                "runner_slot": "secondary",
                "knowledge_root": None,
                "deployment_roots": {},
                "task_work_root": None,
                "task_kb_root": None,
                "runtime_session_database": None,
                "runtime_log_retention_bytes": None,
            }],
            "database_consumers": {
                "candidate_feed_import": {
                    "enabled": True,
                    "outbox": str(self.root / "candidate-outbox"),
                    "stream_id": "example-candidates",
                },
                "native_intake_run": {
                    "enabled": True,
                    "producer": "gw",
                    "stream_id": "example-native",
                    "limit": 100,
                },
                "execution_card_requeue": {"enabled": True, "limit": 100},
                "task_card_requeue": {"enabled": True, "limit": 100},
                "lifecycle_outcome_export": {
                    "enabled": True,
                    "outbox": str(self.root / "lifecycle-outbox"),
                    "stream_id": "example-lifecycle",
                    "max_page_items": 100,
                },
                "fused_task_titles": {
                    "enabled": True,
                    "endpoint": f"http://{LOOPBACK}:8800",
                },
                "duplicate_card_schedule": {"enabled": True, "limit": 100},
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
        self.assertIn("--gw-endpoint", cards)
        self.assertIn("--task-work-root", cards)
        schedule = config.argv("execution-schedule")
        self.assertEqual(schedule[0], "foxhound-execution-schedule")
        self.assertNotIn("--execution-slot-cap", schedule)
        self.assertEqual(
            schedule[schedule.index("--skip-planning-for") + 1], "issue"
        )
        runner = config.argv("execution-runner:primary")
        self.assertEqual(runner[0], "foxhound-execution-runner")
        self.assertIn("--execution-slot-cap", runner)
        self.assertIn("--plan-without-asking", runner)
        self.assertEqual(
            config.argv("candidate-feed-import")[0],
            "foxhound-candidate-feed-import",
        )
        self.assertEqual(
            config.argv("native-intake-run")[0], "foxhound-native-intake"
        )
        self.assertEqual(
            config.argv("execution-card-requeue")[0],
            "foxhound-execution-card-requeue",
        )
        self.assertEqual(
            config.argv("task-card-requeue")[0],
            "foxhound-task-card-requeue",
        )
        self.assertEqual(
            config.argv("lifecycle-outcome-export")[0],
            "foxhound-task-lifecycle-outcome-export",
        )
        self.assertEqual(
            config.argv("fused-task-titles")[0],
            "foxhound-fused-task-titles",
        )
        self.assertEqual(
            config.argv("duplicate-card-schedule")[0],
            "foxhound-task-duplicate-card-schedule",
        )
        for component in (
            "candidate-feed-import", "native-intake-run",
            "execution-card-requeue", "lifecycle-outcome-export",
            "task-card-requeue",
            "fused-task-titles",
            "duplicate-card-schedule",
        ):
            command = config.argv(component)
            self.assertEqual(
                command[command.index("--database") + 1], str(self.database)
            )

    def test_deployment_roots_reach_the_runner_command(self) -> None:
        """A configured root must survive into the launched runner's argv."""
        document = self._document()
        document["execution_runners"][0]["deployment_roots"] = {
            "sync_drive": "/srv/example/drive",
        }
        self._write_config(document)

        config = load_deployment_config(self.config_path)
        runner = next(r for r in config.execution_runners if r.enabled)
        argv = runner.argv(Path("/srv/example/db.sqlite3"), None, config.workflow)

        index = argv.index("--deployment-root")
        self.assertEqual(argv[index + 1], "sync_drive=/srv/example/drive")

    def test_deployment_roots_refuse_an_unusable_name_or_path(self) -> None:
        for roots in (
            {"Sync Drive": "/srv/example/drive"},
            {"sync_drive": "relative/path"},
        ):
            with self.subTest(roots=roots):
                document = self._document()
                document["execution_runners"][0]["deployment_roots"] = roots
                self._write_config(document)
                with self.assertRaises(DeploymentConfigError):
                    load_deployment_config(self.config_path)

    def test_version_twelve_configuration_remains_valid_without_roots(self) -> None:
        document = self._document()
        document["schema_version"] = 12
        self._drop_deployment_root_keys(document)
        self._write_config(document)

        config = load_deployment_config(self.config_path)
        runner = next(r for r in config.execution_runners if r.enabled)
        self.assertEqual(dict(runner.deployment_roots), {})
        argv = runner.argv(Path("/srv/example/db.sqlite3"), None, config.workflow)
        self.assertNotIn("--deployment-root", argv)

    def test_version_one_configuration_remains_valid_without_card_gw_settings(self) -> None:
        document = self._document()
        document["schema_version"] = 1
        self._drop_runtime_log_keys(document)
        self._drop_deployment_root_keys(document)
        del document["workflow"]["agent_profile_routes"]  # type: ignore[index]
        del document["card_service"]["task_work_root"]  # type: ignore[index]
        del document["workflow"]["act_without_asking"]  # type: ignore[index]
        del document["workflow"]["execute_without_asking"]  # type: ignore[index]
        del document["workflow"]["skip_planning_for"]  # type: ignore[index]
        del document["workflow"]["reader_aliases"]  # type: ignore[index]
        document["execution_runner"] = document.pop("execution_runners")[0]
        document.pop("database_consumers")
        for key in ("gw_endpoint", "gw_alias", "gw_token_file"):
            del document["card_service"][key]  # type: ignore[index]
        self._write_config(document)

        config = load_deployment_config(self.config_path)

        self.assertNotIn("--gw-endpoint", config.argv("task-cards"))

    def test_version_two_configuration_remains_valid(self) -> None:
        document = self._document()
        document["schema_version"] = 2
        self._drop_runtime_log_keys(document)
        self._drop_deployment_root_keys(document)
        del document["workflow"]["agent_profile_routes"]  # type: ignore[index]
        del document["card_service"]["task_work_root"]  # type: ignore[index]
        del document["workflow"]["act_without_asking"]  # type: ignore[index]
        del document["workflow"]["execute_without_asking"]  # type: ignore[index]
        del document["workflow"]["skip_planning_for"]  # type: ignore[index]
        del document["workflow"]["reader_aliases"]  # type: ignore[index]
        document["execution_runner"] = document.pop("execution_runners")[0]
        document.pop("database_consumers")
        self._write_config(document)

        config = load_deployment_config(self.config_path)

        self.assertEqual(
            config.argv("execution-runner")[0], "foxhound-execution-runner"
        )

    def test_version_three_configuration_remains_valid_without_title_worker(self) -> None:
        document = self._document()
        document["schema_version"] = 3
        self._drop_runtime_log_keys(document)
        self._drop_deployment_root_keys(document)
        del document["workflow"]["agent_profile_routes"]  # type: ignore[index]
        del document["card_service"]["task_work_root"]  # type: ignore[index]
        del document["workflow"]["act_without_asking"]  # type: ignore[index]
        del document["workflow"]["execute_without_asking"]  # type: ignore[index]
        del document["workflow"]["skip_planning_for"]  # type: ignore[index]
        del document["workflow"]["reader_aliases"]  # type: ignore[index]
        del document["database_consumers"]["fused_task_titles"]  # type: ignore[index]
        del document["database_consumers"]["duplicate_card_schedule"]  # type: ignore[index]
        del document["database_consumers"]["task_card_requeue"]  # type: ignore[index]
        self._write_config(document)

        config = load_deployment_config(self.config_path)

        with self.assertRaises(DeploymentConfigError):
            config.argv("fused-task-titles")

    def test_version_four_configuration_remains_valid_without_duplicate_scheduler(
        self,
    ) -> None:
        document = self._document()
        document["schema_version"] = 4
        self._drop_runtime_log_keys(document)
        self._drop_deployment_root_keys(document)
        del document["workflow"]["agent_profile_routes"]  # type: ignore[index]
        del document["card_service"]["task_work_root"]  # type: ignore[index]
        del document["workflow"]["act_without_asking"]  # type: ignore[index]
        del document["workflow"]["execute_without_asking"]  # type: ignore[index]
        del document["workflow"]["skip_planning_for"]  # type: ignore[index]
        del document["workflow"]["reader_aliases"]  # type: ignore[index]
        del document["database_consumers"]["duplicate_card_schedule"]  # type: ignore[index]
        del document["database_consumers"]["task_card_requeue"]  # type: ignore[index]
        self._write_config(document)

        config = load_deployment_config(self.config_path)

        with self.assertRaises(DeploymentConfigError):
            config.argv("duplicate-card-schedule")

    def test_version_five_configuration_grants_no_execution(self) -> None:
        """A file written before the key existed keeps asking, silently."""
        document = self._document()
        document["schema_version"] = 5
        self._drop_runtime_log_keys(document)
        self._drop_deployment_root_keys(document)
        del document["workflow"]["agent_profile_routes"]  # type: ignore[index]
        del document["card_service"]["task_work_root"]  # type: ignore[index]
        del document["workflow"]["act_without_asking"]  # type: ignore[index]
        del document["workflow"]["execute_without_asking"]  # type: ignore[index]
        del document["workflow"]["skip_planning_for"]  # type: ignore[index]
        del document["workflow"]["reader_aliases"]  # type: ignore[index]
        self._write_config(document)

        config = load_deployment_config(self.config_path)

        self.assertEqual(config.workflow.execute_without_asking, ())
        self.assertNotIn(
            "--execute-without-asking", config.argv("execution-runner:primary")
        )

    def test_the_execution_grant_reaches_the_runner(self) -> None:
        self._write_config(self._document())

        config = load_deployment_config(self.config_path)

        argv = config.argv("execution-runner:primary")

        self.assertEqual(
            argv[argv.index("--execute-without-asking") + 1], "issue"
        )

    def test_a_skip_declaration_reaches_execution_scheduling(self) -> None:
        self._write_config(self._document())

        config = load_deployment_config(self.config_path)

        self.assertEqual(config.workflow.skip_planning_for, ("issue",))
        schedule = config.argv("execution-schedule")
        self.assertEqual(
            schedule[schedule.index("--skip-planning-for") + 1], "issue"
        )

    def test_a_skip_without_execution_authority_names_the_missing_grant(self) -> None:
        document = self._document()
        document["workflow"]["execute_without_asking"] = []  # type: ignore[index]
        self._write_config(document)

        with self.assertRaisesRegex(
            DeploymentConfigError, "execution grants: issue"
        ):
            load_deployment_config(self.config_path)

    def test_a_skip_without_planning_authority_names_the_missing_grant(self) -> None:
        document = self._document()
        document["workflow"]["plan_without_asking"] = []  # type: ignore[index]
        self._write_config(document)

        with self.assertRaisesRegex(
            DeploymentConfigError, "planning grants: issue"
        ):
            load_deployment_config(self.config_path)

    def test_version_nine_configuration_retains_its_plan_phase(self) -> None:
        document = self._document()
        document["schema_version"] = 9
        self._drop_runtime_log_keys(document)
        self._drop_deployment_root_keys(document)
        del document["workflow"]["skip_planning_for"]  # type: ignore[index]
        del document["workflow"]["agent_profile_routes"]  # type: ignore[index]
        self._write_config(document)

        config = load_deployment_config(self.config_path)

        self.assertEqual(config.workflow.skip_planning_for, ())
        self.assertNotIn(
            "--skip-planning-for", config.argv("execution-schedule")
        )

    def test_routes_multiple_source_kinds_to_installed_profiles(self) -> None:
        directory = self.root / "profiles"
        directory.mkdir(mode=0o700)
        profile = AgentProfile(
            profile_id="example-repository-agent",
            display_name="Example Repository Agent",
            runtime="hermes",
            prompt_template=f"First call {WORKER_COMMAND_TOKEN} context.",
            toolsets=("terminal",),
            max_turns=50,
            timeout_seconds=1_800,
            claim_lease_seconds=2_700,
            heartbeat_seconds=60,
            kill_grace_seconds=30,
            allowed_phases=("plan", "execute", "external_action"),
        )
        manifest = directory / f"{profile.profile_id}.json"
        manifest.write_text(json.dumps(profile.document()), encoding="utf-8")
        manifest.chmod(0o600)
        document = self._document()
        document["agent_profile_directory"] = str(directory)
        document["workflow"]["agent_profile_routes"] = [  # type: ignore[index]
            {
                "selector": {"source_kind": "issue"},
                "profile_id": profile.profile_id,
            },
            {
                "selector": {"source_kind": "review_request"},
                "profile_id": profile.profile_id,
            },
        ]
        self._write_config(document)

        config = load_deployment_config(self.config_path)

        self.assertEqual(
            config.workflow.agent_profile_routes,
            (("issue", profile.profile_id), ("review_request", profile.profile_id)),
        )
        argv = config.argv("execution-runner:primary")
        self.assertEqual(argv.count("--profile-route"), 2)
        self.assertIn(f"issue={profile.profile_id}", argv)

    def test_route_to_unavailable_or_phase_incapable_profile_is_refused(self) -> None:
        document = self._document()
        document["workflow"]["agent_profile_routes"] = [  # type: ignore[index]
            {
                "selector": {"source_kind": "issue"},
                "profile_id": "missing-profile",
            },
        ]
        self._write_config(document)

        with self.assertRaises(DeploymentConfigError):
            load_deployment_config(self.config_path)

    def test_the_private_runtime_record_is_routed_to_task_evidence(self) -> None:
        document = self._document()
        runtime_database = self.root / "hermes-state.db"
        runtime_database.touch(mode=0o600)
        for runner in document["execution_runners"]:  # type: ignore[index]
            runner["task_work_root"] = str(self.task_work_root)
            runner["task_kb_root"] = str(self.root / "task-kb")
            runner["runtime_session_database"] = str(runtime_database)
            runner["runtime_log_retention_bytes"] = 31
        self._write_config(document)

        config = load_deployment_config(self.config_path)
        argv = config.argv("execution-runner:primary")

        self.assertEqual(
            argv[argv.index("--runtime-session-database") + 1],
            str(runtime_database),
        )
        self.assertEqual(
            argv[argv.index("--runtime-log-retention-bytes") + 1], "31"
        )

    def test_an_unknown_granted_kind_is_refused(self) -> None:
        document = self._document()
        document["workflow"]["execute_without_asking"] = [  # type: ignore[index]
            "not-a-source-kind"
        ]
        self._write_config(document)

        with self.assertRaises(DeploymentConfigError):
            load_deployment_config(self.config_path)

    def test_a_repeated_granted_kind_is_refused(self) -> None:
        document = self._document()
        document["workflow"]["execute_without_asking"] = [  # type: ignore[index]
            "issue", "issue"
        ]
        self._write_config(document)

        with self.assertRaises(DeploymentConfigError):
            load_deployment_config(self.config_path)

    def test_the_two_grants_are_independent(self) -> None:
        document = self._document()
        document["workflow"]["plan_without_asking"] = []  # type: ignore[index]
        document["workflow"]["skip_planning_for"] = []  # type: ignore[index]
        self._write_config(document)

        config = load_deployment_config(self.config_path)

        self.assertEqual(config.workflow.plan_without_asking, ())
        self.assertEqual(config.workflow.execute_without_asking, ("issue",))

    def test_version_six_configuration_grants_no_action(self) -> None:
        """A file written for the previous key keeps asking about actions."""
        document = self._document()
        document["schema_version"] = 6
        self._drop_runtime_log_keys(document)
        self._drop_deployment_root_keys(document)
        del document["workflow"]["agent_profile_routes"]  # type: ignore[index]
        del document["card_service"]["task_work_root"]  # type: ignore[index]
        del document["workflow"]["act_without_asking"]  # type: ignore[index]
        del document["workflow"]["skip_planning_for"]  # type: ignore[index]
        del document["workflow"]["reader_aliases"]  # type: ignore[index]
        self._write_config(document)

        config = load_deployment_config(self.config_path)

        self.assertEqual(config.workflow.execute_without_asking, ("issue",))
        self.assertEqual(config.workflow.act_without_asking, ())
        self.assertNotIn(
            "--act-without-asking", config.argv("execution-runner:primary")
        )

    def test_the_action_grant_reaches_the_runner(self) -> None:
        self._write_config(self._document())

        config = load_deployment_config(self.config_path)
        argv = config.argv("execution-runner:primary")

        self.assertEqual(
            argv[argv.index("--act-without-asking") + 1], "issue"
        )

    def test_an_unknown_action_kind_is_refused(self) -> None:
        document = self._document()
        document["workflow"]["act_without_asking"] = [  # type: ignore[index]
            "not-a-source-kind"
        ]
        self._write_config(document)

        with self.assertRaises(DeploymentConfigError):
            load_deployment_config(self.config_path)

    def test_acting_may_be_granted_without_executing(self) -> None:
        """Either knob alone, in either direction."""
        document = self._document()
        document["workflow"]["execute_without_asking"] = []  # type: ignore[index]
        document["workflow"]["skip_planning_for"] = []  # type: ignore[index]
        self._write_config(document)

        config = load_deployment_config(self.config_path)

        self.assertEqual(config.workflow.execute_without_asking, ())
        self.assertEqual(config.workflow.act_without_asking, ("issue",))

    def test_partial_card_gw_settings_are_rejected(self) -> None:
        document = self._document()
        document["card_service"]["gw_alias"] = None  # type: ignore[index]
        self._write_config(document)

        with self.assertRaises(DeploymentConfigError):
            load_deployment_config(self.config_path)

    def test_rendered_arguments_are_accepted_by_the_supported_commands(self) -> None:
        document = self._document()
        for key in ("gw_endpoint", "gw_alias", "gw_token_file"):
            document["card_service"][key] = None  # type: ignore[index]
        self._write_config(document)
        config = load_deployment_config(self.config_path)

        cards = config.argv("task-cards")
        with mock.patch.object(task_card_server, "serve") as serve:
            self.assertEqual(task_card_server.main(cards[1:]), 0)
        serve.assert_called_once()
        schedule_parser().parse_args(config.argv("execution-schedule")[1:])
        runner_parser().parse_args(config.argv("execution-runner:primary")[1:])
        candidate_import_parser().parse_args(
            config.argv("candidate-feed-import")[1:]
        )
        native_intake_parser().parse_args(config.argv("native-intake-run")[1:])
        requeue_parser().parse_args(config.argv("execution-card-requeue")[1:])
        task_requeue_parser().parse_args(config.argv("task-card-requeue")[1:])
        lifecycle_export_parser().parse_args(
            config.argv("lifecycle-outcome-export")[1:]
        )
        fused_titles_parser().parse_args(config.argv("fused-task-titles")[1:])
        duplicate_schedule_parser().parse_args(
            config.argv("duplicate-card-schedule")[1:]
        )

    def test_rejects_duplicate_runner_slots(self) -> None:
        document = self._document()
        document["execution_runners"][1]["runner_slot"] = "primary"  # type: ignore[index]
        self._write_config(document)

        with self.assertRaises(DeploymentConfigError):
            load_deployment_config(self.config_path)

    def test_requires_complete_enabled_database_consumers(self) -> None:
        document = self._document()
        del document["database_consumers"]["native_intake_run"]["limit"]  # type: ignore[index]
        self._write_config(document)

        with self.assertRaises(DeploymentConfigError):
            load_deployment_config(self.config_path)

    def test_refuses_a_non_loopback_fused_title_endpoint(self) -> None:
        document = self._document()
        document["database_consumers"]["fused_task_titles"]["endpoint"] = (  # type: ignore[index]
            "https://example.com"
        )
        self._write_config(document)

        with self.assertRaises(DeploymentConfigError):
            load_deployment_config(self.config_path)

    def test_does_not_render_a_disabled_database_consumer(self) -> None:
        document = self._document()
        document["database_consumers"]["execution_card_requeue"] = {  # type: ignore[index]
            "enabled": False
        }
        self._write_config(document)

        config = load_deployment_config(self.config_path)

        with self.assertRaises(DeploymentConfigError):
            config.argv("execution-card-requeue")

    def test_omitted_task_card_requeue_remains_valid_and_refuses_rendering(
        self,
    ) -> None:
        document = self._document()
        del document["database_consumers"]["task_card_requeue"]  # type: ignore[index]
        self._write_config(document)

        config = load_deployment_config(self.config_path)

        with self.assertRaises(DeploymentConfigError):
            config.argv("task-card-requeue")

    def test_task_card_requeue_requires_a_positive_limit(self) -> None:
        document = self._document()
        document["database_consumers"]["task_card_requeue"] = {  # type: ignore[index]
            "enabled": True, "limit": 0,
        }
        self._write_config(document)

        with self.assertRaises(DeploymentConfigError):
            load_deployment_config(self.config_path)

    def test_exec_uses_the_sibling_script_from_the_selected_release(self) -> None:
        self._write_config(self._document())
        config = load_deployment_config(self.config_path)
        release_bin = self.root / "release" / "bin"
        release_bin.mkdir(parents=True)
        executor = release_bin / "foxhound-deployment-config"
        executor.write_text("#!/bin/sh\n", encoding="utf-8")
        component = release_bin / "foxhound-execution-card-requeue"
        component.write_text("#!/bin/sh\n", encoding="utf-8")
        expected = config.argv("execution-card-requeue")

        with mock.patch("foxhound.deployment_config.sys.argv", [str(executor)]):
            with mock.patch("foxhound.deployment_config.os.execv") as execv:
                execute_component(config, "execution-card-requeue")

        execv.assert_called_once_with(str(component), expected)

    def test_exec_refuses_a_missing_selected_release_script(self) -> None:
        self._write_config(self._document())
        config = load_deployment_config(self.config_path)
        executor = self.root / "release" / "bin" / "foxhound-deployment-config"

        with mock.patch("foxhound.deployment_config.sys.argv", [str(executor)]):
            with self.assertRaises(DeploymentConfigError):
                execute_component(config, "execution-card-requeue")

    def test_exec_hides_selected_release_execution_errors(self) -> None:
        self._write_config(self._document())
        config = load_deployment_config(self.config_path)
        release_bin = self.root / "release" / "bin"
        release_bin.mkdir(parents=True)
        executor = release_bin / "foxhound-deployment-config"
        executor.write_text("#!/bin/sh\n", encoding="utf-8")
        component = release_bin / "foxhound-execution-card-requeue"
        component.write_text("#!/bin/sh\n", encoding="utf-8")

        with mock.patch("foxhound.deployment_config.sys.argv", [str(executor)]):
            with mock.patch(
                "foxhound.deployment_config.os.execv", side_effect=OSError
            ):
                with self.assertRaisesRegex(
                    DeploymentConfigError, "deployment executable is unavailable"
                ):
                    execute_component(config, "execution-card-requeue")

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


class ObjectShapeTests(unittest.TestCase):
    """The one shape check every section of the document goes through.

    Exercised directly because the document declares a single optional field
    today, and the behaviour worth pinning is what happens with more than one:
    an optional set is a range, not a second exact shape.
    """

    REQUIRED = {"alpha", "beta"}
    OPTIONAL = {"gamma", "delta"}

    def _check(self, document):
        return deployment_config._object(document, set(self.REQUIRED),
                                         set(self.OPTIONAL))

    def test_required_only_is_valid(self) -> None:
        document = {"alpha": 1, "beta": 2}
        self.assertEqual(self._check(document), document)

    def test_every_optional_field_is_valid(self) -> None:
        document = {"alpha": 1, "beta": 2, "gamma": 3, "delta": 4}
        self.assertEqual(self._check(document), document)

    def test_a_partial_optional_subset_is_valid(self) -> None:
        """All-or-nothing handling refused this while reporting only 'invalid'."""
        for optional in sorted(self.OPTIONAL):
            document = {"alpha": 1, "beta": 2, optional: 3}
            with self.subTest(optional=optional):
                self.assertEqual(self._check(document), document)

    def test_a_missing_required_field_is_refused(self) -> None:
        with self.assertRaises(DeploymentConfigError):
            self._check({"alpha": 1, "gamma": 3})

    def test_an_unknown_field_is_refused(self) -> None:
        with self.assertRaises(DeploymentConfigError):
            self._check({"alpha": 1, "beta": 2, "epsilon": 5})

    def test_an_unknown_field_beside_an_optional_one_is_refused(self) -> None:
        with self.assertRaises(DeploymentConfigError):
            self._check({"alpha": 1, "beta": 2, "gamma": 3, "epsilon": 5})

    def test_a_non_mapping_is_refused(self) -> None:
        for value in ([], "alpha", 7, None):
            with self.subTest(value=value):
                with self.assertRaises(DeploymentConfigError):
                    self._check(value)

    def test_no_optional_set_means_an_exact_shape(self) -> None:
        with self.assertRaises(DeploymentConfigError):
            deployment_config._object({"alpha": 1, "beta": 2, "gamma": 3},
                                      set(self.REQUIRED))


if __name__ == "__main__":
    unittest.main()
