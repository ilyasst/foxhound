"""Private, validated deployment configuration for Foxhound services.

This module deliberately owns only deployment choices, never task data or
credentials.  The configuration file names private files, but validation uses
the existing runtime loaders so that a successful preflight means the same
arguments can start the supported commands.
"""

from __future__ import annotations

import argparse
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
import json
import os
from pathlib import Path
import stat
import sys
from typing import Any

from .agent_profiles import AgentProfileError, load_registry
from .execution_runner import ExecutionRunnerConfig
from .execution_worker import ExecutionWorkerConfigError, load_knowledge_config
from .source_policy import planning_grants
from .task_bootstrap import TaskBootstrapConfigError, _private_database
from .task_card_server import (
    DRIP_ROLE,
    TASK_CARD_CONSUMER_ROLES,
    TaskCardServerConfigError,
    TaskCardServerLimits,
    is_canonical_loopback,
    load_role_tokens,
)
from .task_execution import TaskExecutionService


DEPLOYMENT_SCHEMA = "foxhound.deployment-config"
DEPLOYMENT_SCHEMA_VERSION = 3
MAX_CONFIG_BYTES = 64 * 1024


class DeploymentConfigError(ValueError):
    """A private deployment document is unavailable or unsuitable."""


@dataclass(frozen=True)
class CardServiceConfig:
    enabled: bool
    bind: str | None = None
    port: int | None = None
    request_timeout_seconds: float | None = None
    task_token_files: tuple[tuple[str, Path], ...] = ()
    execution_card_delivery: bool = False
    execution_token_files: tuple[tuple[str, Path], ...] = ()
    gw_endpoint: str | None = None
    gw_alias: str | None = None
    gw_token_file: Path | None = None

    def argv(self, database: Path, profile_directory: Path | None) -> list[str]:
        if not self.enabled:
            raise DeploymentConfigError("task card service is disabled")
        assert self.bind is not None
        assert self.port is not None
        assert self.request_timeout_seconds is not None
        result = [
            "foxhound-task-cards",
            "--database", str(database),
        ]
        for role, path in self.task_token_files:
            result.extend(("--token-file", f"{role}={path}"))
        if self.execution_card_delivery:
            for role, path in self.execution_token_files:
                result.extend(("--execution-token-file", f"{role}={path}"))
        result.extend((
            "--bind", self.bind,
            "--port", str(self.port),
            "--request-timeout", str(self.request_timeout_seconds),
        ))
        if profile_directory is not None:
            result.extend(("--agent-profile-directory", str(profile_directory)))
        if self.gw_endpoint is not None:
            assert self.gw_alias is not None
            assert self.gw_token_file is not None
            result.extend((
                "--gw-endpoint", self.gw_endpoint,
                "--gw-alias", self.gw_alias,
                "--gw-token-file", str(self.gw_token_file),
            ))
        return result


@dataclass(frozen=True)
class WorkflowConfig:
    default_agent_profile: str
    plan_without_asking: tuple[str, ...]
    execution_slot_cap: int
    plan_ready_cap: int
    awaiting_reader_cap: int

    def schedule_argv(
        self, database: Path, profile_directory: Path | None
    ) -> list[str]:
        result = [
            "foxhound-execution-schedule",
            "--database", str(database),
            "--default-agent-profile", self.default_agent_profile,
            "--plan-ready-cap", str(self.plan_ready_cap),
            "--awaiting-reader-cap", str(self.awaiting_reader_cap),
        ]
        if profile_directory is not None:
            result.extend(("--agent-profile-directory", str(profile_directory)))
        for kind in self.plan_without_asking:
            result.extend(("--plan-without-asking", kind))
        return result


@dataclass(frozen=True)
class ExecutionRunnerDeploymentConfig:
    enabled: bool
    run_root: Path | None = None
    gw_endpoint: str | None = None
    gw_alias: str | None = None
    gw_token_file: Path | None = None
    agent_command: str | None = None
    worker_command: str | None = None
    runner_slot: str | None = None
    knowledge_root: Path | None = None
    task_work_root: Path | None = None
    task_kb_root: Path | None = None

    def argv(
        self,
        database: Path,
        profile_directory: Path | None,
        workflow: WorkflowConfig,
    ) -> list[str]:
        if not self.enabled:
            raise DeploymentConfigError("execution runner is disabled")
        assert self.run_root is not None
        assert self.gw_endpoint is not None
        assert self.gw_alias is not None
        assert self.gw_token_file is not None
        assert self.agent_command is not None
        assert self.worker_command is not None
        assert self.runner_slot is not None
        result = [
            "foxhound-execution-runner",
            "--database", str(database),
            "--run-root", str(self.run_root),
            "--gw-endpoint", self.gw_endpoint,
            "--gw-alias", self.gw_alias,
            "--gw-token-file", str(self.gw_token_file),
            "--agent-command", self.agent_command,
            "--default-agent-profile", workflow.default_agent_profile,
            "--worker-command", self.worker_command,
            "--runner-slot", self.runner_slot,
            "--execution-slot-cap", str(workflow.execution_slot_cap),
            "--plan-ready-cap", str(workflow.plan_ready_cap),
            "--awaiting-reader-cap", str(workflow.awaiting_reader_cap),
        ]
        if profile_directory is not None:
            result.extend(("--agent-profile-directory", str(profile_directory)))
        for option, path in (
            ("--knowledge-root", self.knowledge_root),
            ("--task-work-root", self.task_work_root),
            ("--task-kb-root", self.task_kb_root),
        ):
            if path is not None:
                result.extend((option, str(path)))
        for kind in workflow.plan_without_asking:
            result.extend(("--plan-without-asking", kind))
        return result


@dataclass(frozen=True)
class DatabaseConsumersConfig:
    """One-shot database consumers that must share the selected database."""

    candidate_feed_import: tuple[Path, str] | None
    native_intake_run: tuple[str, str, int] | None
    execution_card_requeue: int | None
    lifecycle_outcome_export: tuple[Path, str, int] | None

    def argv(self, component: str, database: Path) -> list[str]:
        if component == "candidate-feed-import":
            if self.candidate_feed_import is None:
                raise DeploymentConfigError("database consumer is disabled")
            outbox, stream_id = self.candidate_feed_import
            return [
                "foxhound-candidate-feed-import",
                "--database", str(database),
                "--outbox", str(outbox),
                "--stream-id", stream_id,
            ]
        if component == "native-intake-run":
            if self.native_intake_run is None:
                raise DeploymentConfigError("database consumer is disabled")
            producer, stream_id, limit = self.native_intake_run
            return [
                "foxhound-native-intake", "run",
                "--database", str(database),
                "--producer", producer,
                "--stream-id", stream_id,
                "--limit", str(limit),
            ]
        if component == "execution-card-requeue":
            if self.execution_card_requeue is None:
                raise DeploymentConfigError("database consumer is disabled")
            return [
                "foxhound-execution-card-requeue",
                "--database", str(database),
                "--limit", str(self.execution_card_requeue),
            ]
        if component == "lifecycle-outcome-export":
            if self.lifecycle_outcome_export is None:
                raise DeploymentConfigError("database consumer is disabled")
            outbox, stream_id, max_page_items = self.lifecycle_outcome_export
            return [
                "foxhound-task-lifecycle-outcome-export",
                "--database", str(database),
                "--outbox", str(outbox),
                "--stream-id", stream_id,
                "--max-page-items", str(max_page_items),
            ]
        raise DeploymentConfigError("deployment component is unknown")


@dataclass(frozen=True)
class DeploymentConfig:
    database: Path
    agent_profile_directory: Path | None
    card_service: CardServiceConfig
    workflow: WorkflowConfig
    execution_runners: tuple[ExecutionRunnerDeploymentConfig, ...]
    database_consumers: DatabaseConsumersConfig | None = None

    def argv(self, component: str) -> list[str]:
        if component == "task-cards":
            return self.card_service.argv(
                self.database, self.agent_profile_directory
            )
        if component == "execution-schedule":
            return self.workflow.schedule_argv(
                self.database, self.agent_profile_directory
            )
        if component == "execution-runner" and len(self.execution_runners) == 1:
            return self.execution_runners[0].argv(
                self.database, self.agent_profile_directory, self.workflow
            )
        if component.startswith("execution-runner:"):
            slot = component.removeprefix("execution-runner:")
            for runner in self.execution_runners:
                if runner.runner_slot == slot:
                    return runner.argv(
                        self.database, self.agent_profile_directory, self.workflow
                    )
            raise DeploymentConfigError("deployment component is unknown")
        if self.database_consumers is not None:
            return self.database_consumers.argv(component, self.database)
        raise DeploymentConfigError("deployment component is unknown")


def load_deployment_config(path: str | os.PathLike[str]) -> DeploymentConfig:
    """Load and validate an owner-only configuration document.

    All errors intentionally collapse to this module's safe exception class.
    Callers must not print exception text: filesystem and token-loader errors
    can disclose private paths.
    """
    document = _load_document(Path(path))
    try:
        config = _parse_document(document)
        _validate_runtime(config)
    except DeploymentConfigError:
        raise
    except (
        AgentProfileError,
        ExecutionWorkerConfigError,
        TaskBootstrapConfigError,
        TaskCardServerConfigError,
        ValueError,
        OSError,
    ) as exc:
        raise DeploymentConfigError("deployment configuration is invalid") from exc
    return config


def _load_document(path: Path) -> object:
    if path.is_symlink():
        raise DeploymentConfigError("deployment configuration is unavailable")
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise DeploymentConfigError("deployment configuration is unavailable") from exc
    try:
        info = os.fstat(descriptor)
        if (
            not stat.S_ISREG(info.st_mode)
            or (os.name == "posix" and (
                info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) != 0o600
            ))
        ):
            raise DeploymentConfigError("deployment configuration is unavailable")
        try:
            with os.fdopen(descriptor, "r", encoding="utf-8") as handle:
                descriptor = -1
                text = handle.read(MAX_CONFIG_BYTES + 1)
        except UnicodeError as exc:
            raise DeploymentConfigError(
                "deployment configuration is invalid"
            ) from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    if len(text.encode("utf-8")) > MAX_CONFIG_BYTES:
        raise DeploymentConfigError("deployment configuration is invalid")
    try:
        return json.loads(text, object_pairs_hook=_strict_object)
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise DeploymentConfigError("deployment configuration is invalid") from exc


def _strict_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate configuration field")
        result[key] = value
    return result


def _parse_document(document: object) -> DeploymentConfig:
    if not isinstance(document, Mapping):
        raise DeploymentConfigError("deployment configuration shape is invalid")
    version = document.get("schema_version")
    if (
        document.get("schema") != DEPLOYMENT_SCHEMA
        or version not in {1, 2, DEPLOYMENT_SCHEMA_VERSION}
        or isinstance(version, bool)
    ):
        raise DeploymentConfigError("deployment configuration version is invalid")
    root = _object(
        document,
        {
            "schema", "schema_version", "database", "agent_profile_directory",
            "card_service", "workflow", "execution_runners", "database_consumers",
        }
        if version == DEPLOYMENT_SCHEMA_VERSION else {
            "schema", "schema_version", "database", "agent_profile_directory",
            "card_service", "workflow", "execution_runner",
        },
    )
    runners = (
        _parse_execution_runners(root["execution_runners"])
        if version == DEPLOYMENT_SCHEMA_VERSION
        else (_parse_execution_runner(root["execution_runner"]),)
    )
    return DeploymentConfig(
        database=_absolute_path(root["database"]),
        agent_profile_directory=_optional_absolute_path(
            root["agent_profile_directory"]
        ),
        card_service=_parse_card_service(
            root["card_service"], version=int(version)
        ),
        workflow=_parse_workflow(root["workflow"]),
        execution_runners=runners,
        database_consumers=(
            _parse_database_consumers(root["database_consumers"])
            if version == DEPLOYMENT_SCHEMA_VERSION else None
        ),
    )


def _parse_card_service(value: object, *, version: int) -> CardServiceConfig:
    if not isinstance(value, Mapping):
        raise DeploymentConfigError("card service configuration is invalid")
    enabled = value.get("enabled")
    if not isinstance(enabled, bool):
        raise DeploymentConfigError("card service configuration is invalid")
    if not enabled:
        _object(value, {"enabled"})
        return CardServiceConfig(enabled=False)
    fields = {
        "enabled", "bind", "port", "request_timeout_seconds",
        "task_token_files", "execution_card_delivery", "execution_token_files",
    }
    if version >= 2:
        fields.update({"gw_endpoint", "gw_alias", "gw_token_file"})
    document = _object(value, fields)
    bind = document["bind"]
    port = document["port"]
    timeout = document["request_timeout_seconds"]
    delivery = document["execution_card_delivery"]
    if (
        not isinstance(bind, str)
        or isinstance(port, bool) or not isinstance(port, int)
        or isinstance(timeout, bool) or not isinstance(timeout, (int, float))
        or not isinstance(delivery, bool)
    ):
        raise DeploymentConfigError("card service configuration is invalid")
    gw_values = (
        document.get("gw_endpoint"),
        document.get("gw_alias"),
        document.get("gw_token_file"),
    )
    if any(value is not None for value in gw_values) and not all(gw_values):
        raise DeploymentConfigError("card service configuration is invalid")
    if all(gw_values) and (
        not isinstance(gw_values[0], str)
        or not isinstance(gw_values[1], str)
    ):
        raise DeploymentConfigError("card service configuration is invalid")
    return CardServiceConfig(
        enabled=True,
        bind=bind,
        port=port,
        request_timeout_seconds=float(timeout),
        task_token_files=_role_paths(document["task_token_files"]),
        execution_card_delivery=delivery,
        execution_token_files=_role_paths(
            document["execution_token_files"], allow_empty=True
        ),
        gw_endpoint=gw_values[0] if isinstance(gw_values[0], str) else None,
        gw_alias=gw_values[1] if isinstance(gw_values[1], str) else None,
        gw_token_file=(
            _absolute_path(gw_values[2]) if gw_values[2] is not None else None
        ),
    )


def _parse_workflow(value: object) -> WorkflowConfig:
    document = _object(value, {
        "default_agent_profile", "plan_without_asking", "execution_slot_cap",
        "plan_ready_cap", "awaiting_reader_cap",
    })
    profile = document["default_agent_profile"]
    grants = document["plan_without_asking"]
    caps = tuple(document[key] for key in (
        "execution_slot_cap", "plan_ready_cap", "awaiting_reader_cap"
    ))
    if (
        not isinstance(profile, str)
        or not isinstance(grants, list)
        or any(not isinstance(kind, str) for kind in grants)
        or len(set(grants)) != len(grants)
        or any(isinstance(cap, bool) or not isinstance(cap, int) for cap in caps)
    ):
        raise DeploymentConfigError("workflow configuration is invalid")
    return WorkflowConfig(profile, tuple(grants), *caps)


def _parse_execution_runner(value: object) -> ExecutionRunnerDeploymentConfig:
    if not isinstance(value, Mapping):
        raise DeploymentConfigError("execution runner configuration is invalid")
    enabled = value.get("enabled")
    if not isinstance(enabled, bool):
        raise DeploymentConfigError("execution runner configuration is invalid")
    if not enabled:
        _object(value, {"enabled"})
        return ExecutionRunnerDeploymentConfig(enabled=False)
    document = _object(value, {
        "enabled", "run_root", "gw_endpoint", "gw_alias", "gw_token_file",
        "agent_command", "worker_command", "runner_slot", "knowledge_root",
        "task_work_root", "task_kb_root",
    })
    strings = tuple(document[key] for key in (
        "gw_endpoint", "gw_alias", "agent_command", "worker_command", "runner_slot"
    ))
    if any(not isinstance(item, str) for item in strings):
        raise DeploymentConfigError("execution runner configuration is invalid")
    return ExecutionRunnerDeploymentConfig(
        enabled=True,
        run_root=_absolute_path(document["run_root"]),
        gw_endpoint=strings[0],
        gw_alias=strings[1],
        gw_token_file=_absolute_path(document["gw_token_file"]),
        agent_command=strings[2],
        worker_command=strings[3],
        runner_slot=strings[4],
        knowledge_root=_optional_absolute_path(document["knowledge_root"]),
        task_work_root=_optional_absolute_path(document["task_work_root"]),
        task_kb_root=_optional_absolute_path(document["task_kb_root"]),
    )


def _parse_execution_runners(
    value: object,
) -> tuple[ExecutionRunnerDeploymentConfig, ...]:
    if not isinstance(value, list) or not value:
        raise DeploymentConfigError("execution runner configuration is invalid")
    runners = tuple(_parse_execution_runner(item) for item in value)
    slots = [runner.runner_slot for runner in runners if runner.enabled]
    if len(slots) != len(set(slots)):
        raise DeploymentConfigError("execution runner configuration is invalid")
    return runners


def _parse_database_consumers(value: object) -> DatabaseConsumersConfig:
    document = _object(value, {
        "candidate_feed_import", "native_intake_run", "execution_card_requeue",
        "lifecycle_outcome_export",
    })
    candidate = _parse_candidate_feed_import(document["candidate_feed_import"])
    intake = _parse_native_intake_run(document["native_intake_run"])
    requeue = _parse_execution_card_requeue(document["execution_card_requeue"])
    lifecycle = _parse_lifecycle_outcome_export(document["lifecycle_outcome_export"])
    return DatabaseConsumersConfig(candidate, intake, requeue, lifecycle)


def _enabled_document(
    value: object, fields: set[str]
) -> Mapping[str, object] | None:
    if not isinstance(value, Mapping) or not isinstance(value.get("enabled"), bool):
        raise DeploymentConfigError("database consumer configuration is invalid")
    if not value["enabled"]:
        _object(value, {"enabled"})
        return None
    return _object(value, {"enabled", *fields})


def _parse_candidate_feed_import(value: object) -> tuple[Path, str] | None:
    document = _enabled_document(value, {"outbox", "stream_id"})
    if document is None:
        return None
    return (
        _absolute_path(document["outbox"]),
        _nonempty_string(document["stream_id"]),
    )


def _parse_native_intake_run(value: object) -> tuple[str, str, int] | None:
    document = _enabled_document(value, {"producer", "stream_id", "limit"})
    if document is None:
        return None
    return (
        _nonempty_string(document["producer"]),
        _nonempty_string(document["stream_id"]),
        _positive_int(document["limit"]),
    )


def _parse_execution_card_requeue(value: object) -> int | None:
    document = _enabled_document(value, {"limit"})
    return None if document is None else _positive_int(document["limit"])


def _parse_lifecycle_outcome_export(
    value: object,
) -> tuple[Path, str, int] | None:
    document = _enabled_document(
        value, {"outbox", "stream_id", "max_page_items"}
    )
    if document is None:
        return None
    max_page_items = _positive_int(document["max_page_items"])
    if max_page_items > 500:
        raise DeploymentConfigError("database consumer configuration is invalid")
    return (
        _absolute_path(document["outbox"]),
        _nonempty_string(document["stream_id"]),
        max_page_items,
    )


def _object(value: object, fields: set[str]) -> Mapping[str, object]:
    if not isinstance(value, Mapping) or set(value) != fields:
        raise DeploymentConfigError("deployment configuration shape is invalid")
    return value


def _absolute_path(value: object) -> Path:
    if not isinstance(value, str):
        raise DeploymentConfigError("deployment path is invalid")
    path = Path(value)
    if not path.is_absolute():
        raise DeploymentConfigError("deployment path is invalid")
    return path


def _nonempty_string(value: object) -> str:
    if not isinstance(value, str) or not value.strip():
        raise DeploymentConfigError("database consumer configuration is invalid")
    return value


def _positive_int(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise DeploymentConfigError("database consumer configuration is invalid")
    return value


def _optional_absolute_path(value: object) -> Path | None:
    return None if value is None else _absolute_path(value)


def _role_paths(
    value: object, *, allow_empty: bool = False
) -> tuple[tuple[str, Path], ...]:
    if not isinstance(value, Mapping) or (not value and not allow_empty):
        raise DeploymentConfigError("token file configuration is invalid")
    result: list[tuple[str, Path]] = []
    for role, path in value.items():
        if role not in TASK_CARD_CONSUMER_ROLES:
            raise DeploymentConfigError("token file configuration is invalid")
        result.append((role, _absolute_path(path)))
    return tuple(sorted(result))


def _validate_runtime(config: DeploymentConfig) -> None:
    _private_database(config.database)
    registry = load_registry(config.agent_profile_directory)
    planning_grants(config.workflow.plan_without_asking)
    TaskExecutionService(
        config.database,
        profile_registry=registry,
        default_profile_id=config.workflow.default_agent_profile,
        planning_grants=config.workflow.plan_without_asking,
        execution_slot_cap=config.workflow.execution_slot_cap,
        plan_ready_cap=config.workflow.plan_ready_cap,
        awaiting_reader_cap=config.workflow.awaiting_reader_cap,
    )
    for runner in config.execution_runners:
        if runner.enabled:
            assert runner.run_root is not None
            assert runner.gw_endpoint is not None
            assert runner.gw_alias is not None
            assert runner.gw_token_file is not None
            assert runner.agent_command is not None
            assert runner.worker_command is not None
            assert runner.runner_slot is not None
            load_knowledge_config(
                runner.gw_endpoint, runner.gw_alias, runner.gw_token_file
            )
            ExecutionRunnerConfig(
                database_path=config.database,
                run_root=runner.run_root,
                gw_endpoint=runner.gw_endpoint,
                gw_alias=runner.gw_alias,
                gw_token_file=runner.gw_token_file,
                agent_command=runner.agent_command,
                profile_registry=registry,
                default_agent_profile=config.workflow.default_agent_profile,
                worker_command=runner.worker_command,
                runner_slot=runner.runner_slot,
                planning_grants=config.workflow.plan_without_asking,
                knowledge_root=runner.knowledge_root,
                task_work_root=runner.task_work_root,
                task_kb_root=runner.task_kb_root,
                execution_slot_cap=config.workflow.execution_slot_cap,
                plan_ready_cap=config.workflow.plan_ready_cap,
                awaiting_reader_cap=config.workflow.awaiting_reader_cap,
            )
    cards = config.card_service
    if not cards.enabled:
        return
    assert cards.bind is not None
    assert cards.port is not None
    assert cards.request_timeout_seconds is not None
    if not is_canonical_loopback(cards.bind) or not 1 <= cards.port <= 65_535:
        raise DeploymentConfigError("card service configuration is invalid")
    TaskCardServerLimits(
        request_timeout_seconds=cards.request_timeout_seconds
    ).validate()
    if cards.gw_endpoint is not None:
        assert cards.gw_alias is not None
        assert cards.gw_token_file is not None
        load_knowledge_config(
            cards.gw_endpoint, cards.gw_alias, cards.gw_token_file
        )
    task_roles = {role for role, _ in cards.task_token_files}
    if DRIP_ROLE not in task_roles:
        raise DeploymentConfigError("task card delivery role is unavailable")
    load_role_tokens(_role_specs(cards.task_token_files))
    if cards.execution_card_delivery:
        execution_roles = {role for role, _ in cards.execution_token_files}
        if DRIP_ROLE not in execution_roles:
            raise DeploymentConfigError("execution card delivery role is unavailable")
        load_role_tokens(
            _role_specs(cards.execution_token_files),
            option_name="--execution-token-file",
        )
    elif cards.execution_token_files:
        raise DeploymentConfigError("execution card configuration is invalid")


def _role_specs(paths: tuple[tuple[str, Path], ...]) -> list[str]:
    return [f"{role}={path}" for role, path in paths]


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="foxhound-deployment-config",
        description="Validate or render a private Foxhound deployment configuration",
    )
    parser.add_argument("--config", type=Path, required=True)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("validate")
    render = commands.add_parser("render")
    render.add_argument("--component", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        config = load_deployment_config(args.config)
    except DeploymentConfigError:
        print("foxhound deployment configuration: unavailable", file=sys.stderr)
        return 78
    if args.command == "validate":
        print(json.dumps({"ok": True}, sort_keys=True))
        return 0
    try:
        print(json.dumps({"argv": config.argv(args.component)}, sort_keys=True))
    except DeploymentConfigError:
        print("foxhound deployment configuration: unavailable", file=sys.stderr)
        return 78
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
