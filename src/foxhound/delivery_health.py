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
# 2 adds `superseded_profiles`; 3 adds the count of workflows parked because
# their measured context did not fit; 4 adds `admission`. Consumers must not
# infer any of them from a missing aggregate field. 6 adds `admission.preserved_open`.
# 7 adds `execution_cards.review_backpressure` and `delivery.recent_surface_full_releases`.
HEALTH_SCHEMA_VERSION = 7
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
class ExecutionDeliveryCardHealth(DeliveryCardHealth):
    review_backpressure: bool = False


@dataclass(frozen=True)
class AdmissionHealth:
    """Open tasks the scheduler has not given a workflow row yet.

    Zero is the healthy steady state: `schedule_new` admits everything
    eligible on its next pass, so a task is normally unadmitted only for the
    seconds between intake and that pass. A number that stays above zero
    means new work has stopped entering execution, and the reason does not
    matter to a watchdog -- a saturated capacity cap, a stopped timer, a
    scheduler crashing before it commits, and a database it cannot write all
    look the same from here and are all worth waking someone for.
    """

    unadmitted: int
    oldest_unadmitted_age_seconds: int | None
    preserved_open: int


@dataclass(frozen=True)
class DeliveryHealthPolicy:
    max_pending_age_seconds: int = 900
    max_recent_failures: int = 3
    failure_window_seconds: int = 900
    max_last_delivery_age_seconds: int = 900
    #: Generous against the scheduler's own five-minute cadence, so an
    #: ordinary wait between intake and the next pass never alerts.
    max_unadmitted_age_seconds: int = 1800

    def validate(self) -> None:
        for value in (
            self.max_pending_age_seconds,
            self.failure_window_seconds,
            self.max_last_delivery_age_seconds,
            self.max_unadmitted_age_seconds,
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
    execution_cards: ExecutionDeliveryCardHealth
    recent_failures: int
    recent_requeues: int
    recent_surface_full_releases: int
    failure_window_seconds: int
    last_successful_delivery_age_seconds: int | None
    workflows: ExecutionReadiness
    superseded_profiles: SupersededProfileHealth
    admission: AdmissionHealth
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
                "recent_requeues": self.recent_requeues,
                "recent_surface_full_releases": self.recent_surface_full_releases,
                "surface_full": self.recent_surface_full_releases,
                "failure_window_seconds": self.failure_window_seconds,
                "last_successful_delivery_age_seconds": self.last_successful_delivery_age_seconds,
            },
            "workflows": asdict(self.workflows),
            "superseded_profiles": asdict(self.superseded_profiles),
            "admission": asdict(self.admission),
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
            execution_cards_base = _card_health(
                connection, "execution_review_cards", "created_at", now,
                scope="summary_only=0",
            )
            (
                recent_failures,
                recent_requeues,
                recent_surface_full,
                last_delivery,
                last_task_delivery,
                last_execution_delivery,
            ) = _delivery_events(
                connection, now - timedelta(seconds=policy.failure_window_seconds)
            )
            (
                review_backpressure,
                unbackpressured_age_exceeded,
                unbackpressured_delivery_stale,
            ) = _execution_backpressure_and_status(
                connection, now, policy
            )
            admission = _admission_health(connection, now)
        execution = TaskExecutionService(database, clock=lambda: now)
        workflows = execution.readiness()
        superseded = execution.superseded_profile_revisions()
    except (TaskBootstrapConfigError, TaskLedgerError, OSError, ValueError) as exc:
        raise DeliveryHealthError("delivery health is unavailable") from exc

    execution_cards = ExecutionDeliveryCardHealth(
        pending=execution_cards_base.pending,
        delivering=execution_cards_base.delivering,
        delivered=execution_cards_base.delivered,
        active=execution_cards_base.active,
        oldest_pending_age_seconds=execution_cards_base.oldest_pending_age_seconds,
        review_backpressure=review_backpressure,
    )

    last_age = None if last_delivery is None else _age(now, last_delivery)
    alerts: list[str] = []

    task_age_exceeded = (
        task_cards.oldest_pending_age_seconds is not None
        and task_cards.oldest_pending_age_seconds > policy.max_pending_age_seconds
    )
    if task_age_exceeded or unbackpressured_age_exceeded:
        alerts.append("pending_age_exceeded")
    if recent_failures >= policy.max_recent_failures:
        alerts.append("recent_delivery_failures_exceeded")

    task_delivery_stale = (
        (task_cards.pending + task_cards.delivering) > 0
        and (
            last_task_delivery is None
            or _age(now, last_task_delivery) > policy.max_last_delivery_age_seconds
        )
    )
    if task_delivery_stale or unbackpressured_delivery_stale:
        alerts.append("delivery_stale")
    if (
        admission.oldest_unadmitted_age_seconds is not None
        and admission.oldest_unadmitted_age_seconds
        > policy.max_unadmitted_age_seconds
    ):
        alerts.append("admission_stalled")
    return DeliveryHealth(
        task_cards=task_cards,
        execution_cards=execution_cards,
        recent_failures=recent_failures,
        recent_requeues=recent_requeues,
        recent_surface_full_releases=recent_surface_full,
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
        admission=admission,
        alerts=tuple(alerts),
    )


def _admission_health(connection: object, now: datetime) -> AdmissionHealth:
    """Count open tasks with no workflow row, and age the oldest.

    The predicate mirrors the `eligible` count in
    `TaskExecutionService.schedule_new`, including its exclusion of a task
    whose producer candidate was withdrawn but whose task was deliberately
    preserved open -- that one is not waiting for admission and must not
    hold the alert on forever.
    """
    row = connection.execute(
        "SELECT "
        "SUM(CASE WHEN is_preserved = 0 THEN 1 ELSE 0 END) AS unadmitted, "
        "MIN(CASE WHEN is_preserved = 0 THEN created_at END) AS oldest, "
        "SUM(CASE WHEN is_preserved = 1 THEN 1 ELSE 0 END) AS preserved "
        "FROM ("
        " SELECT t.id, t.created_at, "
        " CASE WHEN EXISTS("
        "  SELECT 1 FROM task_candidate_bindings AS b JOIN "
        "  task_candidate_lifecycle AS l ON l.candidate_id=b.candidate_id "
        "  WHERE b.task_id=t.id AND b.relation='accepted' "
        "  AND l.state='withdrawn' AND l.resolution='preserved_open'"
        " ) THEN 1 ELSE 0 END AS is_preserved "
        " FROM tasks AS t "
        " LEFT JOIN task_execution_workflows AS w ON w.task_id=t.id "
        " WHERE t.status='open' AND w.task_id IS NULL"
        ")"
    ).fetchone()
    unadmitted = int(row["unadmitted"] or 0)
    preserved = int(row["preserved"] or 0)
    oldest = row["oldest"]
    return AdmissionHealth(
        unadmitted=unadmitted,
        oldest_unadmitted_age_seconds=(
            None if not unadmitted or oldest is None
            else _age(now, _timestamp(oldest))
        ),
        preserved_open=preserved,
    )


def _card_health(
    connection: object,
    table: str,
    pending_time_column: str,
    now: datetime,
    *,
    scope: str = "1=1",
) -> DeliveryCardHealth:
    """How much is waiting on the reader, and how long the oldest has waited.

    `scope` exists because not every row in a card table is a card the reader
    owes an answer to.  A run summary is delivered when the actionable queue
    is empty or at its ceiling, so on a busy host it legitimately waits --
    counting one here reports ordinary prioritisation as a delivery fault, and
    the watchdog then alerts forever on a system that is working.
    """
    row = connection.execute(
        "SELECT "
        "SUM(CASE WHEN status='pending' THEN 1 ELSE 0 END) AS pending,"
        "SUM(CASE WHEN status='delivering' THEN 1 ELSE 0 END) AS delivering,"
        "SUM(CASE WHEN status='delivered' THEN 1 ELSE 0 END) AS delivered,"
        "SUM(CASE WHEN status IN ('pending','delivering','delivered') THEN 1 ELSE 0 END) AS active,"
        f"MIN(CASE WHEN status='pending' AND {pending_time_column}<=? THEN {pending_time_column} END) AS oldest_pending "
        f"FROM {table} WHERE {scope}",
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


def _delivery_events(
    connection: object, since: datetime
) -> tuple[int, int, int, datetime | None, datetime | None, datetime | None]:
    """Failures, requeues, surface-full releases, and delivery timestamps."""
    row = connection.execute(
        "SELECT "
        "SUM(CASE WHEN kind='delivery_failed' AND occurred_at>=? THEN 1 ELSE 0 END) AS recent_failures,"
        "SUM(CASE WHEN kind='requeued' AND occurred_at>=? THEN 1 ELSE 0 END) AS recent_requeues,"
        "SUM(CASE WHEN kind='claim_released' AND action='surface_full' AND occurred_at>=? THEN 1 ELSE 0 END) AS recent_surface_full,"
        "MAX(CASE WHEN kind='delivered' THEN occurred_at END) AS last_delivery,"
        "MAX(CASE WHEN source='task' AND kind='delivered' THEN occurred_at END) AS last_task_delivery,"
        "MAX(CASE WHEN source='execution' AND kind='delivered' THEN occurred_at END) AS last_execution_delivery "
        "FROM ("
        "SELECT 'task' AS source, kind, action, occurred_at FROM task_review_card_events "
        "UNION ALL "
        "SELECT 'execution' AS source, kind, action, occurred_at FROM execution_review_card_events"
        ")",
        (since.isoformat(timespec="seconds"),) * 3,
    ).fetchone()
    last = row["last_delivery"]
    last_task = row["last_task_delivery"]
    last_exec = row["last_execution_delivery"]
    return (
        int(row["recent_failures"] or 0),
        int(row["recent_requeues"] or 0),
        int(row["recent_surface_full"] or 0),
        None if last is None else _timestamp(str(last)),
        None if last_task is None else _timestamp(str(last_task)),
        None if last_exec is None else _timestamp(str(last_exec)),
    )


def _execution_backpressure_and_status(
    connection: object,
    now: datetime,
    policy: DeliveryHealthPolicy,
) -> tuple[bool, bool, bool]:
    """Calculate review backpressure and check un-backpressured consumers.

    Returns:
        (review_backpressure, unbackpressured_age_exceeded, unbackpressured_delivery_stale)
    """
    rows = connection.execute(
        "SELECT "
        "COALESCE(consumer_digest, '') AS consumer,"
        "SUM(CASE WHEN status='pending' THEN 1 ELSE 0 END) AS pending,"
        "SUM(CASE WHEN status='delivering' THEN 1 ELSE 0 END) AS delivering,"
        "SUM(CASE WHEN status='delivered' THEN 1 ELSE 0 END) AS delivered,"
        "MIN(CASE WHEN status='pending' AND created_at<=? THEN created_at END) AS oldest_pending,"
        "MAX(CASE WHEN status='delivered' THEN delivered_at END) AS last_delivered "
        "FROM execution_review_cards "
        "WHERE summary_only=0 "
        "GROUP BY COALESCE(consumer_digest, '')",
        (now.isoformat(timespec="seconds"),),
    ).fetchall()

    sf_rows = connection.execute(
        "SELECT "
        "COALESCE(c.consumer_digest, '') AS consumer,"
        "MAX(e.occurred_at) AS last_surface_full "
        "FROM execution_review_card_events e "
        "LEFT JOIN execution_review_cards c ON c.id=e.card_id "
        "WHERE e.kind='claim_released' AND e.action='surface_full' "
        "GROUP BY COALESCE(c.consumer_digest, '')"
    ).fetchall()
    sf_by_consumer = {
        row["consumer"]: _timestamp(str(row["last_surface_full"]))
        for row in sf_rows
        if row["last_surface_full"] is not None
    }

    deliv_rows = connection.execute(
        "SELECT "
        "COALESCE(c.consumer_digest, '') AS consumer,"
        "MAX(e.occurred_at) AS last_delivery "
        "FROM execution_review_card_events e "
        "LEFT JOIN execution_review_cards c ON c.id=e.card_id "
        "WHERE e.kind='delivered' "
        "GROUP BY COALESCE(c.consumer_digest, '')"
    ).fetchall()
    deliv_by_consumer = {
        row["consumer"]: _timestamp(str(row["last_delivery"]))
        for row in deliv_rows
        if row["last_delivery"] is not None
    }

    any_backpressure = False
    unbackpressured_age_exceeded = False
    unbackpressured_delivery_stale = False

    for row in rows:
        consumer = row["consumer"]
        pending = int(row["pending"] or 0)
        delivering = int(row["delivering"] or 0)
        delivered = int(row["delivered"] or 0)
        oldest_pending_raw = row["oldest_pending"]
        last_deliv_raw = row["last_delivered"]

        last_delivered_dt = None
        if last_deliv_raw is not None:
            last_delivered_dt = _timestamp(str(last_deliv_raw))
        event_deliv = deliv_by_consumer.get(consumer)
        if event_deliv is not None:
            if last_delivered_dt is None or event_deliv > last_delivered_dt:
                last_delivered_dt = event_deliv

        last_sf_dt = sf_by_consumer.get(consumer)
        if last_sf_dt is None and len(rows) == 1:
            last_sf_dt = sf_by_consumer.get("")

        sf_fresh = (
            last_sf_dt is not None
            and _age(now, last_sf_dt) <= policy.max_last_delivery_age_seconds
        )
        is_backpressured = (pending > 0 and delivered >= 1 and sf_fresh)
        if is_backpressured:
            any_backpressure = True
        else:
            if oldest_pending_raw is not None:
                oldest_age = _age(now, _timestamp(str(oldest_pending_raw)))
                if oldest_age > policy.max_pending_age_seconds:
                    unbackpressured_age_exceeded = True

            if (pending + delivering) > 0:
                if (
                    last_delivered_dt is None
                    or _age(now, last_delivered_dt) > policy.max_last_delivery_age_seconds
                ):
                    unbackpressured_delivery_stale = True

    return any_backpressure, unbackpressured_age_exceeded, unbackpressured_delivery_stale


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
