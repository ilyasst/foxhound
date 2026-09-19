"""Content-free delivery health for an already-initialized Foxhound database."""

from __future__ import annotations

import argparse
from contextlib import closing
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sys
from typing import Callable, Sequence

from .task_cards import TaskCardService
from .task_bootstrap import TaskBootstrapConfigError, _private_database
from .task_execution import ExecutionReadiness, TaskExecutionService
from .task_ledger import TaskLedgerError


HEALTH_SCHEMA = "foxhound.delivery-health"
# 2 adds `superseded_profiles`. A consumer reading version 1 sees every
# field it did before; the addition is why the version moved rather than
# something a reader has to infer from a missing key.
HEALTH_SCHEMA_VERSION = 2
MAX_THRESHOLD_SECONDS = 7 * 24 * 60 * 60
MAX_RECENT_FAILURES = 10_000


class DeliveryHealthError(RuntimeError):
    """Delivery health could not read a compatible private database."""


@dataclass(frozen=True)
class SupersededProfileHealth:
    """Workflows held to a budget their profile no longer installs.

    A profile's revision fixes its timeout and turn limit, and a workflow
    keeps the revision it was scheduled under for life (ADR 0024). So an
    operator who raises a budget changes nothing for work already queued,
    and no existing report says so: `profile_health` marks a retired
    revision `available`, which is true -- it resolves and runs -- while
    saying nothing about the budget it carries.
    """

    #: Distinct retired revisions still pinned by at least one workflow.
    revisions: int
    #: Workflows pinned to one of them.
    workflows: int
    #: Of those, how many could adopt the installed revision now: not
    #: `running`, so nothing is rebound under a live claim.
    ready: int
    parked: int
    running: int


@dataclass(frozen=True)
class DeliveryCardHealth:
    pending: int
    delivering: int
    delivered: int
    active: int
    oldest_pending_age_seconds: int | None


@dataclass(frozen=True)
class DeliveryHealthPolicy:
    max_pending_age_seconds: int = 900
    max_recent_failures: int = 3
    failure_window_seconds: int = 900
    max_last_delivery_age_seconds: int = 900

    def validate(self) -> None:
        for value in (
            self.max_pending_age_seconds,
            self.failure_window_seconds,
            self.max_last_delivery_age_seconds,
        ):
            if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= MAX_THRESHOLD_SECONDS:
                raise ValueError("delivery health time threshold is invalid")
        if (
            isinstance(self.max_recent_failures, bool)
            or not isinstance(self.max_recent_failures, int)
            or not 1 <= self.max_recent_failures <= MAX_RECENT_FAILURES
        ):
            raise ValueError("delivery health failure threshold is invalid")


@dataclass(frozen=True)
class DeliveryHealth:
    task_cards: DeliveryCardHealth
    execution_cards: DeliveryCardHealth
    recent_failures: int
    failure_window_seconds: int
    last_successful_delivery_age_seconds: int | None
    workflows: ExecutionReadiness
    superseded_profiles: SupersededProfileHealth
    alerts: tuple[str, ...]

    @property
    def ok(self) -> bool:
        return not self.alerts

    def document(self) -> dict[str, object]:
        return {
            "schema": HEALTH_SCHEMA,
            "schema_version": HEALTH_SCHEMA_VERSION,
            "ok": self.ok,
            "alerts": list(self.alerts),
            "task_cards": asdict(self.task_cards),
            "execution_cards": asdict(self.execution_cards),
            "delivery": {
                "recent_failures": self.recent_failures,
                "failure_window_seconds": self.failure_window_seconds,
                "last_successful_delivery_age_seconds": self.last_successful_delivery_age_seconds,
            },
            "workflows": asdict(self.workflows),
            "superseded_profiles": asdict(self.superseded_profiles),
        }


def collect_delivery_health(
    database_path: Path,
    *,
    policy: DeliveryHealthPolicy = DeliveryHealthPolicy(),
    clock: Callable[[], datetime] | None = None,
) -> DeliveryHealth:
    """Read aggregate delivery health without exposing private work records."""
    policy.validate()
    now = _now(clock)
    try:
        database = _private_database(database_path)
        cards = TaskCardService(database, clock=lambda: now)
        with closing(cards._connect()) as connection:
            task_cards = _card_health(
                connection, "task_review_cards", "due_at", now
            )
            execution_cards = _card_health(
                connection, "execution_review_cards", "created_at", now
            )
            recent_failures, last_delivery = _delivery_events(
                connection, now - timedelta(seconds=policy.failure_window_seconds)
            )
        execution = TaskExecutionService(database, clock=lambda: now)
        workflows = execution.readiness()
        superseded = execution.superseded_profile_revisions()
    except (TaskBootstrapConfigError, TaskLedgerError, OSError, ValueError) as exc:
        raise DeliveryHealthError("delivery health is unavailable") from exc

    last_age = None if last_delivery is None else _age(now, last_delivery)
    pending_or_stuck = (
        task_cards.pending + task_cards.delivering
        + execution_cards.pending + execution_cards.delivering
    )
    alerts: list[str] = []
    oldest = max((
        age for age in (
            task_cards.oldest_pending_age_seconds,
            execution_cards.oldest_pending_age_seconds,
        ) if age is not None
    ), default=None)
    if oldest is not None and oldest > policy.max_pending_age_seconds:
        alerts.append("pending_age_exceeded")
    if recent_failures >= policy.max_recent_failures:
        alerts.append("recent_delivery_failures_exceeded")
    if pending_or_stuck and (
        last_age is None or last_age > policy.max_last_delivery_age_seconds
    ):
        alerts.append("delivery_stale")
    return DeliveryHealth(
        task_cards=task_cards,
        execution_cards=execution_cards,
        recent_failures=recent_failures,
        failure_window_seconds=policy.failure_window_seconds,
        last_successful_delivery_age_seconds=last_age,
        workflows=workflows,
        superseded_profiles=SupersededProfileHealth(
            revisions=len(superseded),
            workflows=sum(row.workflows for row in superseded),
            ready=sum(row.workflows - row.running for row in superseded),
            parked=sum(row.parked for row in superseded),
            running=sum(row.running for row in superseded),
        ),
        alerts=tuple(alerts),
    )


def _card_health(
    connection: object, table: str, pending_time_column: str, now: datetime
) -> DeliveryCardHealth:
    row = connection.execute(
        "SELECT "
        "SUM(CASE WHEN status='pending' THEN 1 ELSE 0 END) AS pending,"
        "SUM(CASE WHEN status='delivering' THEN 1 ELSE 0 END) AS delivering,"
        "SUM(CASE WHEN status='delivered' THEN 1 ELSE 0 END) AS delivered,"
        "SUM(CASE WHEN status IN ('pending','delivering','delivered') THEN 1 ELSE 0 END) AS active,"
        f"MIN(CASE WHEN status='pending' AND {pending_time_column}<=? THEN {pending_time_column} END) AS oldest_pending "
        f"FROM {table}",
        (now.isoformat(timespec="seconds"),),
    ).fetchone()
    oldest = row["oldest_pending"]
    return DeliveryCardHealth(
        pending=int(row["pending"] or 0),
        delivering=int(row["delivering"] or 0),
        delivered=int(row["delivered"] or 0),
        active=int(row["active"] or 0),
        oldest_pending_age_seconds=(
            None if oldest is None else _age(now, _timestamp(str(oldest)))
        ),
    )


def _delivery_events(connection: object, since: datetime) -> tuple[int, datetime | None]:
    row = connection.execute(
        "SELECT "
        "SUM(CASE WHEN kind='delivery_failed' AND occurred_at>=? THEN 1 ELSE 0 END) AS recent_failures,"
        "MAX(CASE WHEN kind='delivered' THEN occurred_at END) AS last_delivery "
        "FROM ("
        "SELECT kind,occurred_at FROM task_review_card_events "
        "UNION ALL "
        "SELECT kind,occurred_at FROM execution_review_card_events"
        ")",
        (since.isoformat(timespec="seconds"),),
    ).fetchone()
    last = row["last_delivery"]
    return int(row["recent_failures"] or 0), (
        None if last is None else _timestamp(str(last))
    )


def _now(clock: Callable[[], datetime] | None) -> datetime:
    value = (clock or (lambda: datetime.now(timezone.utc)))()
    if value.tzinfo is None or value.utcoffset() is None:
        raise DeliveryHealthError("delivery health clock is invalid")
    return value.astimezone(timezone.utc)


def _timestamp(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise DeliveryHealthError("delivery health timestamp is invalid") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise DeliveryHealthError("delivery health timestamp is invalid")
    return parsed.astimezone(timezone.utc)


def _age(now: datetime, then: datetime) -> int:
    return max(0, int((now - then).total_seconds()))


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="foxhound-delivery-health",
        description="Report content-free Foxhound card delivery health",
    )
    parser.add_argument("--database", type=Path, required=True)
    parser.add_argument("--max-pending-age-seconds", type=int, default=900)
    parser.add_argument("--max-recent-failures", type=int, default=3)
    parser.add_argument("--failure-window-seconds", type=int, default=900)
    parser.add_argument("--max-last-delivery-age-seconds", type=int, default=900)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        health = collect_delivery_health(
            args.database,
            policy=DeliveryHealthPolicy(
                max_pending_age_seconds=args.max_pending_age_seconds,
                max_recent_failures=args.max_recent_failures,
                failure_window_seconds=args.failure_window_seconds,
                max_last_delivery_age_seconds=args.max_last_delivery_age_seconds,
            ),
        )
    except (DeliveryHealthError, ValueError):
        print("foxhound delivery health: unavailable", file=sys.stderr)
        return 78
    print(json.dumps(health.document(), sort_keys=True))
    return 0 if health.ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
