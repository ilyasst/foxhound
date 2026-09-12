"""Explicit one-shot activation of verified GW shadow decisions."""

from __future__ import annotations

import argparse
import json
import stat
import sys
from pathlib import Path
from typing import Sequence

from .execution_worker import (
    ExecutionWorkerConfigError,
    load_knowledge_config,
)
from .knowledge_client import GwKnowledgeClient, KnowledgeClientError
from .task_ledger import (
    BootstrapResult,
    TaskLedger,
    TaskLedgerError,
)


class TaskBootstrapConfigError(RuntimeError):
    """The one-shot bootstrap configuration is unsafe or unavailable."""


def run_bootstrap(
    *,
    database_path: Path,
    gw_endpoint: str,
    gw_alias: str,
    gw_token_file: Path,
) -> BootstrapResult:
    """Materialize the current verified shadow prefix exactly once."""
    database = _private_database(database_path)
    knowledge = load_knowledge_config(gw_endpoint, gw_alias, gw_token_file)
    client = GwKnowledgeClient(knowledge)
    return TaskLedger(database).bootstrap_from_shadow(
        owner_resolver=client.resolve_task_owner
    )


def _private_database(path: Path) -> Path:
    if not isinstance(path, Path) or not path.is_absolute():
        raise TaskBootstrapConfigError("task database path is invalid")
    try:
        if path.resolve(strict=True) != path:
            raise TaskBootstrapConfigError("task database path is invalid")
        parent = path.parent.stat()
        info = path.lstat()
    except OSError:
        raise TaskBootstrapConfigError(
            "task database is unavailable"
        ) from None
    if (
        not stat.S_ISDIR(parent.st_mode)
        or parent.st_mode & 0o077
        or not stat.S_ISREG(info.st_mode)
        or info.st_mode & 0o077
    ):
        raise TaskBootstrapConfigError("task database is not private")
    return path


def _result_document(result: BootstrapResult) -> dict[str, object]:
    return {
        "ok": result.accepted,
        "disposition": result.disposition.value,
        "refusal": None if result.refusal is None else result.refusal.value,
        "counts": {
            "bindings_created": result.bindings_created,
            "bindings_unchanged": result.bindings_unchanged,
            "candidates_divergent": result.candidates_divergent,
            "candidates_owner_equivalent":
                result.candidates_owner_equivalent,
            "candidates_pending": result.candidates_pending,
            "candidates_refused": result.candidates_refused,
            "candidates_unmapped": result.candidates_unmapped,
            "incomplete_groups": result.incomplete_groups,
            "owner_equivalences_created":
                result.owner_equivalences_created,
            "owner_equivalences_unchanged":
                result.owner_equivalences_unchanged,
            "tasks_created": result.tasks_created,
        },
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="foxhound-task-bootstrap",
        description="Activate verified imported GW shadow decisions",
    )
    parser.add_argument("--database", required=True, type=Path)
    parser.add_argument("--gw-endpoint", required=True)
    parser.add_argument("--gw-alias", required=True)
    parser.add_argument("--gw-token-file", required=True, type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        result = run_bootstrap(
            database_path=args.database,
            gw_endpoint=args.gw_endpoint,
            gw_alias=args.gw_alias,
            gw_token_file=args.gw_token_file,
        )
    except (
        ExecutionWorkerConfigError,
        TaskBootstrapConfigError,
    ):
        print(
            "foxhound task bootstrap: configuration unavailable",
            file=sys.stderr,
        )
        return 78
    except (KnowledgeClientError, TaskLedgerError, OSError):
        print("foxhound task bootstrap: bootstrap failed", file=sys.stderr)
        return 70
    except Exception:
        print("foxhound task bootstrap: bootstrap failed", file=sys.stderr)
        return 70
    print(json.dumps(_result_document(result), sort_keys=True))
    return 0 if result.accepted else 1


if __name__ == "__main__":
    raise SystemExit(main())
