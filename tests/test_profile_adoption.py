#!/usr/bin/env python3
"""Synthetic tests for adopting a profile's installed revision (issue #501).

A workflow keeps the profile revision it was scheduled under for life, and a
revision fixes the timeout and turn limit. So raising a budget changes nothing
for queued work unless something rebinds it. These cover that rebinding: what
it may touch, what it must refuse, and what it has to report.
"""

from __future__ import annotations

import sqlite3
import tempfile
import unittest
from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path

from foxhound import migrate_database
from foxhound.agent_profiles import (
    AgentProfileRegistry,
    general_profile,
    parse_profile,
)
from foxhound.task_execution import (
    TaskExecutionService,
    WorkflowDisposition,
    WorkflowRefusal,
    WorkflowStatus,
)
from foxhound.task_ledger import TaskLedgerError

CLAIM_TOKEN = "0" * 43


class MutableClock:
    def __init__(self) -> None:
        self.value = datetime(2030, 2, 1, 12, 0, tzinfo=timezone.utc)

    def __call__(self) -> datetime:
        return self.value

    def advance(self, seconds: int) -> None:
        self.value += timedelta(seconds=seconds)


def _profile(**overrides):
    """A profile differing from the built-in only in the named fields."""
    document = general_profile().document()
    document.update(overrides)
    return parse_profile(document)


class AdoptInstalledRevisionTest(unittest.TestCase):
    """Rebinding a workflow to the installed revision of its own profile."""

    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.root.chmod(0o700)
        self.database = self.root / "foxhound.sqlite3"
        self.clock = MutableClock()
        migrate_database(self.database)
        stamp = self.clock().isoformat(timespec="seconds")
        with closing(sqlite3.connect(self.database)) as connection:
            for task_id in (1, 2, 3):
                connection.execute(
                    "INSERT INTO tasks(id,status,text,owner,due,version,"
                    "created_at,updated_at,closed_at) "
                    "VALUES(?,'open','Synthetic task','Person A',NULL,1,?,?,"
                    "NULL)",
                    (task_id, stamp, stamp),
                )
            connection.commit()

        # The retired revision: a narrower budget, as a deployment would have
        # had before the operator raised it.
        self.retired = _profile(
            max_turns=50, timeout_seconds=1800, claim_lease_seconds=2400
        )
        # The installed revision: what the operator now wants used.
        self.installed = _profile(
            max_turns=120, timeout_seconds=3300, claim_lease_seconds=3600
        )

    def _service(self, *, installed, historical=()):
        return TaskExecutionService(
            self.database,
            clock=self.clock,
            token_factory=lambda: CLAIM_TOKEN,
            profile_registry=AgentProfileRegistry(
                (installed,), historical_profiles=tuple(historical)
            ),
        )

    def _scheduled_on_retired(self, task_id: int = 1):
        """Schedule a workflow under the retired revision, then return a
        service whose registry installs the newer one."""
        old = self._service(installed=self.retired)
        workflow = old.schedule(task_id, expected_task_version=1)
        self.assertEqual(
            workflow.agent_profile_revision, self.retired.revision
        )
        return self._service(
            installed=self.installed, historical=(self.retired,)
        )

    # ------------------------------------------------------------ single

    def test_a_retired_pin_adopts_the_installed_revision(self):
        service = self._scheduled_on_retired()
        before = service.get(1)
        result = service.adopt_installed_revision(
            1, expected_version=before.version
        )
        self.assertIs(result.disposition, WorkflowDisposition.APPLIED)
        self.assertEqual(
            result.agent_profile_revision, self.installed.revision
        )
        after = service.get(1)
        self.assertEqual(
            after.agent_profile_revision, self.installed.revision
        )
        self.assertEqual(
            after.agent_profile_id, before.agent_profile_id,
            "adoption must never change which profile a workflow names",
        )

    def test_adoption_increments_the_version_so_cards_go_stale(self):
        service = self._scheduled_on_retired()
        before = service.get(1)
        service.adopt_installed_revision(1, expected_version=before.version)
        self.assertEqual(service.get(1).version, before.version + 1)

    def test_adoption_emits_an_event(self):
        service = self._scheduled_on_retired()
        before = service.get(1)
        service.adopt_installed_revision(1, expected_version=before.version)
        with closing(sqlite3.connect(self.database)) as connection:
            kinds = [
                row[0] for row in connection.execute(
                    "SELECT kind FROM task_execution_events WHERE task_id=1"
                    " ORDER BY sequence"
                )
            ]
        self.assertIn("agent_selected", kinds)

    def test_a_stale_version_is_refused(self):
        service = self._scheduled_on_retired()
        current = service.get(1).version
        result = service.adopt_installed_revision(
            1, expected_version=current + 5
        )
        self.assertIs(result.disposition, WorkflowDisposition.REFUSED)

    def test_a_workflow_already_installed_is_unchanged(self):
        service = self._service(installed=self.installed)
        service.schedule(1, expected_task_version=1)
        before = service.get(1)
        result = service.adopt_installed_revision(
            1, expected_version=before.version
        )
        self.assertIs(result.disposition, WorkflowDisposition.UNCHANGED)
        self.assertEqual(service.get(1).version, before.version)

    def test_an_absent_profile_is_refused_rather_than_guessed(self):
        """The profile ID itself is gone, so there is nothing to adopt.

        Adoption only ever moves a workflow to the installed revision of the
        profile it already names. When that name is no longer installed there
        is no such revision, and picking a different profile would be
        `select_agent`'s job and the reader's decision.
        """
        retired_other = _profile(
            profile_id="other", max_turns=50, timeout_seconds=1800,
            claim_lease_seconds=2400,
        )
        old = TaskExecutionService(
            self.database,
            clock=self.clock,
            token_factory=lambda: CLAIM_TOKEN,
            profile_registry=AgentProfileRegistry((retired_other,)),
            default_profile_id="other",
        )
        old.schedule(1, expected_task_version=1)

        # "other" is gone from the registry; the default profile remains.
        service = self._service(installed=self.installed)
        result = service.adopt_installed_revision(
            1, expected_version=service.get(1).version
        )
        self.assertIs(result.disposition, WorkflowDisposition.REFUSED)
        self.assertIs(
            result.refusal, WorkflowRefusal.AGENT_PROFILE_UNAVAILABLE
        )
        self.assertEqual(
            service.get(1).agent_profile_id, "other",
            "a refused adoption must leave the binding untouched",
        )

    # ------------------------------------------------------------ batch

    def test_a_dry_run_reports_without_writing(self):
        service = self._scheduled_on_retired()
        before = service.get(1)
        report = service.adopt_installed_revisions(dry_run=True)
        self.assertTrue(report.dry_run)
        self.assertEqual(report.examined, 1)
        self.assertEqual(report.adopted, 1)
        after = service.get(1)
        self.assertEqual(after.version, before.version)
        self.assertEqual(
            after.agent_profile_revision, self.retired.revision,
            "a dry run must leave the pin exactly as it found it",
        )

    def test_an_applied_pass_rebinds_and_counts(self):
        service = self._scheduled_on_retired()
        report = service.adopt_installed_revisions(dry_run=False)
        self.assertFalse(report.dry_run)
        self.assertEqual(report.adopted, 1)
        self.assertEqual(report.remaining, 0)
        self.assertEqual(
            service.get(1).agent_profile_revision,
            self.installed.revision,
        )

    def test_the_pass_is_bounded_and_reports_what_it_left(self):
        old = self._service(installed=self.retired)
        for task_id in (1, 2, 3):
            old.schedule(task_id, expected_task_version=1)
        service = self._service(
            installed=self.installed, historical=(self.retired,)
        )
        report = service.adopt_installed_revisions(limit=2, dry_run=False)
        self.assertEqual(report.examined, 3)
        self.assertEqual(report.adopted, 2)
        self.assertEqual(report.remaining, 1)

    def test_a_narrowed_budget_is_reported_not_silent(self):
        """Adoption may reduce a budget, but never quietly."""
        wide = _profile(
            max_turns=120, timeout_seconds=3300, claim_lease_seconds=3600
        )
        narrow = _profile(
            max_turns=50, timeout_seconds=1800, claim_lease_seconds=2400
        )
        old = self._service(installed=wide)
        old.schedule(1, expected_task_version=1)
        service = self._service(installed=narrow, historical=(wide,))
        report = service.adopt_installed_revisions(dry_run=False)
        self.assertEqual(report.adopted, 1)
        self.assertEqual(
            report.narrowed, 1,
            "a workflow losing budget is a decision the operator must see",
        )

    def test_a_widened_budget_is_not_counted_as_narrowed(self):
        service = self._scheduled_on_retired()
        report = service.adopt_installed_revisions(dry_run=False)
        self.assertEqual(report.adopted, 1)
        self.assertEqual(report.narrowed, 0)

    def test_a_non_positive_limit_is_refused(self):
        service = self._scheduled_on_retired()
        for bad in (0, -1, True):
            with self.assertRaises(TaskLedgerError):
                service.adopt_installed_revisions(limit=bad)

    def test_nothing_to_do_reports_zero(self):
        service = self._service(installed=self.installed)
        service.schedule(1, expected_task_version=1)
        report = service.adopt_installed_revisions(dry_run=False)
        self.assertEqual(report.examined, 0)
        self.assertEqual(report.adopted, 0)


if __name__ == "__main__":
    unittest.main()
