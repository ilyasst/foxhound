#!/usr/bin/env python3
"""Synthetic tests for owner-conditioned admission past the Start gate."""

from __future__ import annotations

import sqlite3
import tempfile
import unittest
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path

from foxhound import migrate_database
from foxhound.deployment_config import (
    DEPLOYMENT_SCHEMA_VERSION,
    DeploymentConfigError,
    _parse_workflow,
)
from foxhound.task_execution import TaskExecutionService, WorkflowStatus
from foxhound.task_owner import (
    confidently_other_owned,
    normalized_aliases,
    reader_owned,
)


READER = "Person A"
OTHER = "Person B"


def _confirmed(owner: str) -> dict[str, object]:
    """A version-one, non-provisional individual owner reference."""
    return {
        "owner": owner,
        "owner_kind": "person",
        "owner_ref_version": 1,
        "owner_provisional": 0,
    }


class ReaderOwnedPredicateTests(unittest.TestCase):
    """The predicate itself, independent of any database."""

    def setUp(self) -> None:
        self.aliases = normalized_aliases([READER])

    def test_a_confirmed_reader_owner_matches(self):
        self.assertTrue(reader_owned(_confirmed(READER), self.aliases))

    def test_another_confirmed_owner_does_not_match(self):
        self.assertFalse(reader_owned(_confirmed(OTHER), self.aliases))

    def test_matching_ignores_case_accents_and_punctuation(self):
        aliases = normalized_aliases(["Renée O'Connor-Smith"])
        self.assertTrue(
            reader_owned(_confirmed("RENEE o connor smith"), aliases)
        )

    def test_without_aliases_nothing_is_reader_owned(self):
        self.assertFalse(reader_owned(_confirmed(READER), frozenset()))

    def test_a_provisional_owner_is_not_the_reader(self):
        row = _confirmed(READER) | {"owner_provisional": 1}
        self.assertFalse(reader_owned(row, self.aliases))

    def test_a_version_zero_reference_is_not_the_reader(self):
        row = _confirmed(READER) | {"owner_ref_version": 0}
        self.assertFalse(reader_owned(row, self.aliases))

    def test_a_group_owner_is_not_the_reader(self):
        row = _confirmed(READER) | {"owner_kind": "group"}
        self.assertFalse(reader_owned(row, self.aliases))

    def test_an_unresolved_owner_is_not_the_reader(self):
        row = _confirmed(READER) | {"owner_kind": "unresolved"}
        self.assertFalse(reader_owned(row, self.aliases))

    def test_a_missing_owner_is_not_the_reader(self):
        row = _confirmed(READER) | {"owner": None}
        self.assertFalse(reader_owned(row, self.aliases))


class ConfidentlyOtherOwnedPredicateTests(unittest.TestCase):
    """The confidently_other_owned predicate."""

    def setUp(self) -> None:
        self.aliases = normalized_aliases([READER])

    def test_another_confirmed_owner_matches(self):
        self.assertTrue(confidently_other_owned(_confirmed(OTHER), self.aliases))

    def test_the_reader_does_not_match(self):
        self.assertFalse(confidently_other_owned(_confirmed(READER), self.aliases))

    def test_without_aliases_returns_false(self):
        self.assertFalse(confidently_other_owned(_confirmed(OTHER), frozenset()))

    def test_provisional_owner_does_not_match(self):
        row = _confirmed(OTHER) | {"owner_provisional": 1}
        self.assertFalse(confidently_other_owned(row, self.aliases))

    def test_version_zero_reference_does_not_match(self):
        row = _confirmed(OTHER) | {"owner_ref_version": 0}
        self.assertFalse(confidently_other_owned(row, self.aliases))

    def test_group_owner_does_not_match(self):
        row = _confirmed(OTHER) | {"owner_kind": "group"}
        self.assertFalse(confidently_other_owned(row, self.aliases))

    def test_unresolved_owner_does_not_match(self):
        row = _confirmed(OTHER) | {"owner_kind": "unresolved"}
        self.assertFalse(confidently_other_owned(row, self.aliases))

    def test_missing_owner_does_not_match(self):
        row = _confirmed(OTHER) | {"owner": None}
        self.assertFalse(confidently_other_owned(row, self.aliases))

    def test_external_owner_matches_when_not_reader(self):
        row = _confirmed(OTHER) | {"owner_kind": "external"}
        self.assertTrue(confidently_other_owned(row, self.aliases))


class OwnerConditionedAdmissionTests(unittest.TestCase):
    """Admission for a source kind this machine has NOT granted."""

    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.root.chmod(0o700)
        self.database = self.root / "foxhound.sqlite3"
        migrate_database(self.database)

    def _now(self) -> str:
        return datetime(2030, 1, 5, tzinfo=timezone.utc).isoformat(
            timespec="seconds")

    def _task(self, task_id: int, owner: str, **owner_columns: object) -> None:
        columns = {
            "owner_kind": "person",
            "owner_ref_version": 1,
            "owner_provisional": 0,
        } | owner_columns
        with closing(sqlite3.connect(self.database)) as connection:
            connection.execute(
                "INSERT INTO tasks(id,status,text,owner,due,version,"
                "created_at,updated_at,closed_at,owner_kind,"
                "owner_ref_version,owner_provisional) "
                "VALUES(?,'open',?,?,NULL,1,?,?,NULL,?,?,?)",
                (
                    task_id, f"Synthetic task {task_id}", owner,
                    self._now(), self._now(), columns["owner_kind"],
                    columns["owner_ref_version"],
                    columns["owner_provisional"],
                ),
            )
            connection.execute(
                "INSERT INTO task_events(task_id,kind,task_version,"
                "candidate_id,source_revision,from_status,to_status,"
                "occurred_at) VALUES(?,'created',1,NULL,NULL,NULL,'open',?)",
                (task_id, self._now()),
            )
            connection.execute(
                "INSERT INTO candidate_inbox(candidate_id,source_system,"
                "source_kind,source_record_id,source_item_id,"
                "source_revision,payload_json,created_at,"
                "first_imported_at,updated_at) "
                "VALUES(?,'gw','meeting','record-synthetic',?,?,'{}',?,?,?)",
                (
                    f"origin-{task_id}", str(task_id), "b" * 64,
                    self._now(), self._now(), self._now(),
                ),
            )
            connection.execute(
                "INSERT INTO task_candidate_bindings(candidate_id,"
                "source_revision,task_id,relation,decided_at) "
                "VALUES(?,?,?,'accepted',?)",
                (f"origin-{task_id}", "b" * 64, task_id, self._now()),
            )
            connection.commit()

    def _service(self, aliases: object = None) -> TaskExecutionService:
        # `meeting` is deliberately absent from the grants: ownership is the
        # only thing that can admit these tasks.
        return TaskExecutionService(
            self.database,
            planning_grants=["issue"],
            reader_aliases=aliases,
        )

    def test_a_reader_owned_task_is_admitted_without_a_grant(self):
        self._task(1, READER)
        service = self._service([READER])

        service.schedule_new(limit=10)

        self.assertEqual(service.get(1).status, WorkflowStatus.QUEUED)

    def test_another_persons_task_still_waits_at_the_gate(self):
        self._task(1, OTHER)
        service = self._service([READER])

        service.schedule_new(limit=10)

        self.assertEqual(service.get(1).status, WorkflowStatus.AWAITING_START)

    def test_without_configured_aliases_every_task_waits(self):
        """A configuration written before this key existed is unchanged."""
        self._task(1, READER)
        service = self._service()

        service.schedule_new(limit=10)

        self.assertEqual(service.get(1).status, WorkflowStatus.AWAITING_START)

    def test_a_provisional_reader_owner_still_waits(self):
        self._task(1, READER, owner_provisional=1)
        service = self._service([READER])

        service.schedule_new(limit=10)

        self.assertEqual(service.get(1).status, WorkflowStatus.AWAITING_START)

    def test_explicit_scheduling_agrees_with_the_bulk_pass(self):
        self._task(1, READER)
        self._task(2, OTHER)
        service = self._service([READER])

        service.schedule(1, expected_task_version=1)
        service.schedule(2, expected_task_version=1)

        self.assertEqual(service.get(1).status, WorkflowStatus.QUEUED)
        self.assertEqual(service.get(2).status, WorkflowStatus.AWAITING_START)

    def test_a_granted_kind_is_admitted_whoever_owns_it(self):
        """Ownership widens admission; it never narrows an existing grant."""
        self._task(1, OTHER)
        service = TaskExecutionService(
            self.database,
            planning_grants=["meeting"],
            reader_aliases=[READER],
        )

        service.schedule_new(limit=10)

        self.assertEqual(service.get(1).status, WorkflowStatus.QUEUED)

    def test_invalid_alias_lists_are_refused(self):
        for value in ("Person A", [1], [None]):
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    self._service(value)


class DeploymentConfigAliasTests(unittest.TestCase):
    """The alias list is operator-supplied configuration, never a default."""

    def _document(self, **extra: object) -> dict[str, object]:
        return {
            "default_agent_profile": "general",
            "plan_without_asking": [],
            "execution_slot_cap": 1,
            "plan_ready_cap": 1,
            "awaiting_reader_cap": 1,
            "execute_without_asking": [],
            "act_without_asking": [],
        } | extra

    def _current(self, **extra: object) -> dict[str, object]:
        """The same document with every key the current version requires."""
        return self._document(
            skip_planning_for=[], agent_profile_routes=[], **extra
        )

    def test_aliases_are_read_at_the_current_version(self):
        workflow = _parse_workflow(
            self._current(reader_aliases=[READER, OTHER]),
            version=DEPLOYMENT_SCHEMA_VERSION,
        )
        self.assertEqual(workflow.reader_aliases, (READER, OTHER))

    def test_omitted_steer_policy_is_an_empty_current_version_declaration(self):
        workflow = _parse_workflow(
            self._current(reader_aliases=[]),
            version=DEPLOYMENT_SCHEMA_VERSION,
        )
        self.assertEqual(workflow.steer_while_running, ())

    def test_an_older_configuration_is_still_accepted_and_empty(self):
        workflow = _parse_workflow(self._document(), version=8)
        self.assertEqual(workflow.reader_aliases, ())

    def test_the_key_is_unknown_before_its_version(self):
        with self.assertRaises(DeploymentConfigError):
            _parse_workflow(self._document(reader_aliases=[READER]), version=8)

    def test_a_duplicated_alias_is_refused(self):
        with self.assertRaises(DeploymentConfigError):
            _parse_workflow(
                self._current(reader_aliases=[READER, READER]),
                version=DEPLOYMENT_SCHEMA_VERSION,
            )

    def test_each_alias_reaches_the_scheduler(self):
        workflow = _parse_workflow(
            self._current(reader_aliases=[READER, OTHER]),
            version=DEPLOYMENT_SCHEMA_VERSION,
        )
        argv = workflow.schedule_argv(Path("/srv/example/db.sqlite3"), None)
        self.assertEqual(
            [argv[index + 1] for index, value in enumerate(argv)
             if value == "--reader-alias"],
            [READER, OTHER],
        )


class AskWhenOwnedByOthersTests(unittest.TestCase):
    """Synthetic tests for workflow.ask_when_owned_by_others."""

    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.root.chmod(0o700)
        self.database = self.root / "foxhound.sqlite3"
        migrate_database(self.database)

    def _now(self) -> str:
        return datetime(2030, 1, 5, tzinfo=timezone.utc).isoformat(
            timespec="seconds")

    def _task(
        self,
        task_id: int,
        owner: str | None,
        origin_kind: str = "meeting",
        **owner_columns: object,
    ) -> None:
        columns = {
            "owner_kind": "person",
            "owner_ref_version": 1,
            "owner_provisional": 0,
        } | owner_columns
        with closing(sqlite3.connect(self.database)) as connection:
            connection.execute(
                "INSERT INTO tasks(id,status,text,owner,due,version,"
                "created_at,updated_at,closed_at,owner_kind,"
                "owner_ref_version,owner_provisional) "
                "VALUES(?,'open',?,?,NULL,1,?,?,NULL,?,?,?)",
                (
                    task_id, f"Synthetic task {task_id}", owner,
                    self._now(), self._now(), columns["owner_kind"],
                    columns["owner_ref_version"],
                    columns["owner_provisional"],
                ),
            )
            connection.execute(
                "INSERT INTO task_events(task_id,kind,task_version,"
                "candidate_id,source_revision,from_status,to_status,"
                "occurred_at) VALUES(?,'created',1,NULL,NULL,NULL,'open',?)",
                (task_id, self._now()),
            )
            connection.execute(
                "INSERT INTO candidate_inbox(candidate_id,source_system,"
                "source_kind,source_record_id,source_item_id,"
                "source_revision,payload_json,created_at,"
                "first_imported_at,updated_at) "
                "VALUES(?,'gw',?,'record-synthetic',?,?,'{}',?,?,?)",
                (
                    f"origin-{task_id}", origin_kind, str(task_id), "b" * 64,
                    self._now(), self._now(), self._now(),
                ),
            )
            connection.execute(
                "INSERT INTO task_candidate_bindings(candidate_id,"
                "source_revision,task_id,relation,decided_at) "
                "VALUES(?,?,?,'accepted',?)",
                (f"origin-{task_id}", "b" * 64, task_id, self._now()),
            )
            connection.commit()

    def _service(
        self,
        *,
        ask_when_owned_by_others: object = ("meeting",),
        planning_grants: object = (),
        reader_aliases: object = [READER],
    ) -> TaskExecutionService:
        return TaskExecutionService(
            self.database,
            planning_grants=planning_grants,
            ask_when_owned_by_others=ask_when_owned_by_others,
            reader_aliases=reader_aliases,
        )

    def test_listed_kind_reader_owned_is_queued(self):
        self._task(1, READER)
        service = self._service()
        service.schedule_new(limit=10)
        workflow = service.get(1)
        self.assertIsNotNone(workflow)
        assert workflow is not None
        self.assertEqual(workflow.status, WorkflowStatus.QUEUED)

    def test_listed_kind_unresolved_owner_is_queued(self):
        self._task(1, "(unassigned)", owner_kind="unresolved")
        service = self._service()
        service.schedule_new(limit=10)
        workflow = service.get(1)
        self.assertIsNotNone(workflow)
        assert workflow is not None
        self.assertEqual(workflow.status, WorkflowStatus.QUEUED)

    def test_listed_kind_provisional_owner_is_queued(self):
        self._task(1, OTHER, owner_provisional=1)
        service = self._service()
        service.schedule_new(limit=10)
        workflow = service.get(1)
        self.assertIsNotNone(workflow)
        assert workflow is not None
        self.assertEqual(workflow.status, WorkflowStatus.QUEUED)

    def test_listed_kind_other_named_owner_waits_at_start(self):
        self._task(1, OTHER)
        service = self._service()
        service.schedule_new(limit=10)
        workflow = service.get(1)
        self.assertIsNotNone(workflow)
        assert workflow is not None
        self.assertEqual(workflow.status, WorkflowStatus.AWAITING_START)

    def test_listed_kind_empty_reader_aliases_is_queued(self):
        self._task(1, OTHER)
        service = self._service(reader_aliases=[])
        service.schedule_new(limit=10)
        workflow = service.get(1)
        self.assertIsNotNone(workflow)
        assert workflow is not None
        self.assertEqual(workflow.status, WorkflowStatus.QUEUED)

    def test_unlisted_kind_unchanged_waits_at_start(self):
        self._task(1, OTHER, origin_kind="email")
        service = self._service(ask_when_owned_by_others=["meeting"])
        service.schedule_new(limit=10)
        workflow = service.get(1)
        self.assertIsNotNone(workflow)
        assert workflow is not None
        self.assertEqual(workflow.status, WorkflowStatus.AWAITING_START)

    def test_overlap_with_planning_grants_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "overlap"):
            self._service(
                planning_grants=["meeting"],
                ask_when_owned_by_others=["meeting"],
            )

    def test_deployment_config_render_includes_the_list(self):
        workflow = _parse_workflow(
            {
                "default_agent_profile": "general",
                "plan_without_asking": ["issue"],
                "ask_when_owned_by_others": ["meeting"],
                "execution_slot_cap": 1,
                "plan_ready_cap": 1,
                "awaiting_reader_cap": 1,
                "execute_without_asking": [],
                "act_without_asking": [],
                "reader_aliases": [READER],
                "skip_planning_for": [],
                "agent_profile_routes": [],
            },
            version=DEPLOYMENT_SCHEMA_VERSION,
        )
        self.assertEqual(workflow.ask_when_owned_by_others, ("meeting",))
        argv = workflow.schedule_argv(Path("/srv/example/db.sqlite3"), None)
        self.assertIn("--ask-when-owned-by-others", argv)
        self.assertEqual(
            argv[argv.index("--ask-when-owned-by-others") + 1], "meeting"
        )

    def test_waiting_other_owned_task_released_when_owner_becomes_unresolved(self):
        self._task(1, OTHER)
        service = self._service()
        service.schedule_new(limit=10)
        workflow = service.get(1)
        self.assertIsNotNone(workflow)
        assert workflow is not None
        self.assertEqual(workflow.status, WorkflowStatus.AWAITING_START)

        # Update owner to unresolved
        with closing(sqlite3.connect(self.database)) as connection:
            connection.execute(
                "UPDATE tasks SET owner='(unassigned)', owner_kind='unresolved' WHERE id=1"
            )
            connection.commit()

        # Run admission pass again
        service.schedule_new(limit=10)
        workflow_after = service.get(1)
        self.assertIsNotNone(workflow_after)
        assert workflow_after is not None
        self.assertEqual(workflow_after.status, WorkflowStatus.QUEUED)


if __name__ == "__main__":
    unittest.main()
