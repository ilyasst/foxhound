"""Synthetic coverage for the independent delivery-health signal."""

from __future__ import annotations

import contextlib
from contextlib import closing
from datetime import datetime, timedelta, timezone
import io
from pathlib import Path
import sqlite3
import tempfile
import unittest

from foxhound.database_lifecycle import migrate_database
from foxhound.delivery_health import (
    DeliveryHealthPolicy,
    collect_delivery_health,
    main,
)


NOW = datetime(2040, 1, 2, 12, 0, tzinfo=timezone.utc)


class DeliveryHealthTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.database = Path(self.temporary.name) / "foxhound.sqlite3"
        migrate_database(self.database)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def _insert_task_card(self, *, due: datetime) -> int:
        with closing(sqlite3.connect(self.database)) as connection, connection:
            cursor = connection.execute(
                "INSERT INTO task_review_cards("
                "task_id,task_version,status,version,due_at,created_at,updated_at"
                ") VALUES(1,1,'pending',1,?,?,?)",
                (self._time(due), self._time(due), self._time(due)),
            )
        return int(cursor.lastrowid)

    def _task_event(self, card_id: int, *, kind: str, at: datetime) -> None:
        with closing(sqlite3.connect(self.database)) as connection, connection:
            connection.execute(
                "INSERT INTO task_review_card_events("
                "card_id,task_id,kind,card_version,task_version,action,occurred_at"
                ") VALUES(?,1,?,1,1,NULL,?)",
                (card_id, kind, self._time(at)),
            )

    def _execution_event(self, *, kind: str, at: datetime) -> None:
        with closing(sqlite3.connect(self.database)) as connection, connection:
            card = connection.execute(
                "INSERT INTO execution_review_cards("
                "task_id,task_version,workflow_version,kind,phase,status,version,"
                "created_at,updated_at"
                ") VALUES(2,1,1,'start','plan','pending',1,?,?)",
                (self._time(at), self._time(at)),
            )
            connection.execute(
                "INSERT INTO execution_review_card_events("
                "card_id,task_id,kind,card_version,workflow_version,action,occurred_at"
                ") VALUES(?,2,?,1,1,NULL,?)",
                (int(card.lastrowid), kind, self._time(at)),
            )

    def _execution_events_on_one_card(self, *pairs) -> None:
        """Several events against a single card.

        `execution_review_cards` carries a unique index over the active
        statuses per task, so a card per event is not a legal shape.
        """
        with closing(sqlite3.connect(self.database)) as connection, connection:
            first = pairs[0][1]
            card = connection.execute(
                "INSERT INTO execution_review_cards("
                "task_id,task_version,workflow_version,kind,phase,status,version,"
                "created_at,updated_at"
                ") VALUES(2,1,1,'start','plan','pending',1,?,?)",
                (self._time(first), self._time(first)),
            )
            for kind, at in pairs:
                connection.execute(
                    "INSERT INTO execution_review_card_events("
                    "card_id,task_id,kind,card_version,workflow_version,action,"
                    "occurred_at) VALUES(?,2,?,1,1,NULL,?)",
                    (int(card.lastrowid), kind, self._time(at)),
                )

    def _open_task(self, task_id: int, *, created: datetime,
                   workflow: bool = False) -> None:
        with closing(sqlite3.connect(self.database)) as connection, connection:
            connection.execute(
                "INSERT INTO tasks(id,status,text,owner,due,version,"
                "created_at,updated_at,closed_at) "
                "VALUES(?,'open',?,NULL,NULL,1,?,?,NULL)",
                (task_id, f"Synthetic task {task_id}", self._time(created),
                 self._time(created)),
            )
            if workflow:
                connection.execute(
                    "INSERT INTO task_execution_workflows("
                    "task_id,task_version,status,phase,version,due_at,"
                    "failure_count,created_at,updated_at) "
                    "VALUES(?,1,'queued','plan',1,NULL,0,?,?)",
                    (task_id, self._time(created), self._time(created)),
                )

    def _withdraw_preserving_task(self, task_id: int) -> None:
        """Model a producer candidate withdrawn with its task kept open."""
        candidate = f"candidate-{task_id}"
        with closing(sqlite3.connect(self.database)) as connection, connection:
            connection.execute(
                "INSERT INTO task_candidate_bindings("
                "candidate_id,source_revision,task_id,relation,decided_at) "
                "VALUES(?,?,?,'accepted',?)",
                (candidate, "a" * 64, task_id, self._time(NOW)),
            )
            connection.execute(
                "INSERT INTO task_candidate_lifecycle("
                "candidate_id,source_revision,task_version,state,resolution,"
                "changed_at,decided_at) "
                "VALUES(?,?,1,'withdrawn','preserved_open',?,?)",
                (candidate, "a" * 64, self._time(NOW), self._time(NOW)),
            )

    @staticmethod
    def _time(value: datetime) -> str:
        return value.isoformat(timespec="seconds")

    def test_empty_database_is_healthy(self) -> None:
        health = collect_delivery_health(self.database, clock=lambda: NOW)

        self.assertTrue(health.ok)
        self.assertEqual(health.alerts, ())
        self.assertEqual(health.task_cards.active, 0)
        self.assertEqual(health.execution_cards.active, 0)
        self.assertIsNone(health.last_successful_delivery_age_seconds)
        self.assertEqual(health.workflows.ready, 0)
        self.assertEqual(health.workflows.context_exhausted, 0)
        self.assertEqual(health.superseded_profiles.workflows, 0)
        self.assertEqual(health.superseded_profiles.revisions, 0)

    def test_workflows_held_to_a_replaced_budget_are_reported(self) -> None:
        """The count an operator needs after raising a profile's budget.

        Nothing else says it. `profile_health` marks the retired revision
        available, which is true, and readiness counts the workflow as
        ready, which is also true. Neither says it will run to a timeout
        and turn limit that were replaced.
        """
        now = self._time(NOW)
        with closing(sqlite3.connect(self.database)) as connection, connection:
            connection.execute(
                "INSERT INTO tasks(id,status,text,owner,due,version,"
                "created_at,updated_at,closed_at) VALUES(1,'open',"
                "'Synthetic task','Person A',NULL,1,?,?,NULL)",
                (now, now),
            )
            # Pinned to a revision no installed profile carries. The shape
            # of a deployment whose profile budget was raised after this
            # workflow was scheduled.
            connection.execute(
                "INSERT INTO task_execution_workflows(task_id,task_version,"
                "status,phase,version,due_at,failure_count,created_at,"
                "updated_at,agent_profile_id,agent_profile_revision) "
                "VALUES(1,1,'queued','plan',1,NULL,0,?,?,'general',?)",
                (now, now, "b" * 64),
            )

        health = collect_delivery_health(self.database, clock=lambda: NOW)

        self.assertEqual(health.superseded_profiles.revisions, 1)
        self.assertEqual(health.superseded_profiles.workflows, 1)
        # Nothing is running, so nothing would be rebound under a claim.
        self.assertEqual(health.superseded_profiles.ready, 1)
        self.assertEqual(health.superseded_profiles.running, 0)
        document = health.document()
        self.assertEqual(
            document["superseded_profiles"]["workflows"], 1
        )
        # Counts and revisions only, as everywhere else in this report.
        self.assertNotIn("Synthetic task", repr(document))

    def test_old_pending_delivery_is_unhealthy_without_exposing_the_card(self) -> None:
        self._insert_task_card(due=NOW - timedelta(minutes=20))

        health = collect_delivery_health(self.database, clock=lambda: NOW)

        self.assertFalse(health.ok)
        self.assertEqual(
            health.alerts, ("pending_age_exceeded", "delivery_stale")
        )
        self.assertEqual(health.task_cards.oldest_pending_age_seconds, 1_200)
        self.assertNotIn("card_id", health.document())
        self.assertNotIn("task_id", health.document())

    def test_recent_delivery_failures_are_unhealthy(self) -> None:
        card_id = self._insert_task_card(due=NOW - timedelta(seconds=5))
        self._task_event(card_id, kind="delivery_failed", at=NOW - timedelta(seconds=1))
        self._task_event(card_id, kind="delivery_failed", at=NOW)

        health = collect_delivery_health(
            self.database,
            policy=DeliveryHealthPolicy(max_recent_failures=2),
            clock=lambda: NOW,
        )

        self.assertEqual(health.recent_failures, 2)
        self.assertIn("recent_delivery_failures_exceeded", health.alerts)

    def test_execution_card_events_contribute_to_delivery_health(self) -> None:
        self._execution_event(kind="delivery_failed", at=NOW)

        health = collect_delivery_health(
            self.database,
            policy=DeliveryHealthPolicy(max_recent_failures=1),
            clock=lambda: NOW,
        )

        self.assertEqual(health.execution_cards.pending, 1)
        self.assertEqual(health.recent_failures, 1)
        self.assertIn("recent_delivery_failures_exceeded", health.alerts)

    def test_requeued_cards_are_reported_but_do_not_alarm(self) -> None:
        """Re-presenting unanswered cards is not a delivery failure.

        `requeue_unanswered` runs hourly and re-presents every card left
        unanswered for an hour. It used to record that as `delivery_failed`,
        and this check counts those against a threshold of three in fifteen
        minutes -- so any hour with three unanswered cards reported delivery
        as unhealthy on a system that was delivering perfectly well. Observed
        firing at 06:30, 07:30 and 11:30 on one morning.

        The cost was not only noise: a real transport failure became
        indistinguishable from routine re-presentation, so the check that
        exists to catch broken delivery could not. The count is still
        reported, because "three cards have gone unanswered for an hour" is
        worth knowing -- it is simply not a delivery failure.
        """
        self._execution_events_on_one_card(
            ("requeued", NOW - timedelta(seconds=2)),
            ("requeued", NOW - timedelta(seconds=1)),
            ("requeued", NOW),
        )

        health = collect_delivery_health(
            self.database,
            policy=DeliveryHealthPolicy(max_recent_failures=3),
            clock=lambda: NOW,
        )

        self.assertEqual(health.recent_requeues, 3)
        self.assertEqual(health.recent_failures, 0)
        self.assertNotIn("recent_delivery_failures_exceeded", health.alerts)

    def test_a_real_failure_still_alarms_beside_requeues(self) -> None:
        """The separation must not blunt the check it is protecting."""
        self._execution_events_on_one_card(
            ("requeued", NOW - timedelta(seconds=2)),
            ("delivery_failed", NOW),
        )

        health = collect_delivery_health(
            self.database,
            policy=DeliveryHealthPolicy(max_recent_failures=1),
            clock=lambda: NOW,
        )

        self.assertEqual(health.recent_requeues, 1)
        self.assertEqual(health.recent_failures, 1)
        self.assertIn("recent_delivery_failures_exceeded", health.alerts)

    def test_recent_success_keeps_fresh_pending_delivery_healthy(self) -> None:
        card_id = self._insert_task_card(due=NOW - timedelta(seconds=20))
        self._task_event(card_id, kind="delivered", at=NOW - timedelta(seconds=10))

        health = collect_delivery_health(self.database, clock=lambda: NOW)

        self.assertTrue(health.ok)
        self.assertEqual(health.last_successful_delivery_age_seconds, 10)

    def test_a_task_waiting_for_admission_briefly_is_healthy(self) -> None:
        """The gap between intake and the next scheduling pass is normal."""
        self._open_task(1, created=NOW - timedelta(minutes=4))

        health = collect_delivery_health(self.database, clock=lambda: NOW)

        self.assertTrue(health.ok)
        self.assertEqual(health.admission.unadmitted, 1)
        self.assertEqual(health.admission.oldest_unadmitted_age_seconds, 240)

    def test_a_task_never_admitted_is_unhealthy(self) -> None:
        """The alert that was missing when admission silently closed.

        A saturated capacity cap stopped every new task from reaching
        execution for days while the scheduler reported success on every
        pass. Nothing here asks *why* admission stopped: a stopped timer or
        a scheduler that cannot commit look identical from the ledger and
        are equally worth waking someone for.
        """
        self._open_task(1, created=NOW - timedelta(hours=6))
        self._open_task(2, created=NOW - timedelta(minutes=1))
        self._open_task(3, created=NOW - timedelta(days=2), workflow=True)

        health = collect_delivery_health(self.database, clock=lambda: NOW)

        self.assertFalse(health.ok)
        self.assertIn("admission_stalled", health.alerts)
        # The admitted task is not counted, and the oldest unadmitted one
        # sets the age.
        self.assertEqual(health.admission.unadmitted, 2)
        self.assertEqual(
            health.admission.oldest_unadmitted_age_seconds, 6 * 60 * 60
        )

    def test_a_task_kept_open_after_withdrawal_never_alerts(self) -> None:
        """It is not waiting for admission, so it must not hold the alert on.

        `schedule_new` excludes these from its own eligible count. A check
        that disagreed would alert forever on a deployment that has one,
        and an alert that is always on is the same as no alert.
        """
        self._open_task(1, created=NOW - timedelta(days=30))
        self._withdraw_preserving_task(1)

        health = collect_delivery_health(self.database, clock=lambda: NOW)

        self.assertTrue(health.ok)
        self.assertEqual(health.admission.unadmitted, 0)
        self.assertIsNone(health.admission.oldest_unadmitted_age_seconds)
        self.assertEqual(health.admission.preserved_open, 1)

    def test_unavailable_database_has_content_free_cli_output(self) -> None:
        output = io.StringIO()
        missing = self.database.parent / "missing.sqlite3"
        with contextlib.redirect_stderr(output):
            self.assertEqual(main(["--database", str(missing)]), 78)

        self.assertEqual(output.getvalue(), "foxhound delivery health: unavailable\n")
        self.assertNotIn(str(missing), output.getvalue())


class RunSummaryHealthTests(DeliveryHealthTests):
    """A queued run summary is not a delivery fault."""

    def _summary_card(self, *, task_id: int, at: datetime) -> None:
        with closing(sqlite3.connect(self.database)) as connection, connection:
            connection.execute(
                "INSERT INTO task_execution_results("
                "result_id,task_id,workflow_version,task_version,phase,"
                "outcome,content_digest,summary,work_markdown,"
                "questions_json,external_actions_json,deliverables_json,"
                "repository_references_json,repository_impact,"
                "agent_profile_id,agent_profile_revision,created_at"
                ") VALUES(?,?,1,1,'plan','awaiting_plan',?,?,?,"
                "'[]','[]','[]','[]',0,'general',?,?)",
                (f"synthetic-{task_id}", task_id, "d" * 64,
                 "Synthetic summary", "Synthetic work", "e" * 64,
                 self._time(at)),
            )
            connection.execute(
                "INSERT INTO execution_review_cards("
                "task_id,task_version,workflow_version,kind,phase,result_id,"
                "status,version,created_at,updated_at,summary_only"
                ") VALUES(?,1,1,'result_review','plan',?,'pending',1,?,?,1)",
                (task_id, f"synthetic-{task_id}",
                 self._time(at), self._time(at)),
            )

    def test_a_long_queued_summary_does_not_alert(self):
        """The regression this class exists for.

        A summary is delivered when the actionable queue is empty or at its
        ceiling, so on a busy host it waits by design.  Counted as pending
        work, it drags `oldest_pending` past the threshold and the watchdog
        alerts forever on a deployment that is delivering perfectly.
        """
        long_ago = NOW - timedelta(hours=6)
        for task_id in range(2, 8):
            self._summary_card(task_id=task_id, at=long_ago)

        health = collect_delivery_health(self.database, clock=lambda: NOW)

        self.assertEqual(health.execution_cards.pending, 0)
        self.assertIsNone(health.execution_cards.oldest_pending_age_seconds)
        self.assertNotIn("pending_age_exceeded", health.alerts)

    def test_an_actionable_card_behind_summaries_still_alerts(self):
        """Excluding summaries must not mask a real backlog."""
        long_ago = NOW - timedelta(hours=6)
        self._summary_card(task_id=2, at=long_ago)
        with closing(sqlite3.connect(self.database)) as connection, connection:
            connection.execute(
                "INSERT INTO execution_review_cards("
                "task_id,task_version,workflow_version,kind,phase,status,"
                "version,created_at,updated_at"
                ") VALUES(9,1,1,'start','plan','pending',1,?,?)",
                (self._time(long_ago), self._time(long_ago)),
            )

        health = collect_delivery_health(self.database, clock=lambda: NOW)

        self.assertEqual(health.execution_cards.pending, 1)
        self.assertIn("pending_age_exceeded", health.alerts)


if __name__ == "__main__":
    unittest.main()
