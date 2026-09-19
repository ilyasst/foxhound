import unittest

from foxhound import migrate_database
from foxhound.candidate_inbox import SCHEMA_VERSION
from foxhound.effect_intents import EffectIntent, EffectIntentError, EffectReceipt
from foxhound.forge_action import (
    IssueCommentReceipt,
    PullRequestReceipt,
    ReviewReceipt,
)
from foxhound.persistent_effects import PersistentEffectExecutor

import sqlite3
import tempfile
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path


class EffectIntentTests(unittest.TestCase):
    def test_target_bound_intent_and_receipt_are_strict(self):
        intent = EffectIntent("forge-comment", 1, 2, "forge", "github.com/acme/widget/issues/7", "a" * 64, "b" * 64, True)
        self.assertTrue(intent.freshness_required)
        self.assertEqual(EffectReceipt("forge-comment", "completed", "receipt-1", False).state, "completed")
        with self.assertRaises(EffectIntentError):
            EffectIntent("bad", 0, 2, "forge", "x", "a" * 64, "b" * 64, True)

    def test_receipts_accept_every_forge_adapter_reference(self):
        receipts = (
            PullRequestReceipt(
                "example.com/ExampleOrg/ProjectAlpha", "7", 8,
                "https://example.com/ExampleOrg/ProjectAlpha/pull/8",
                "foxhound/issue-7", "main",
            ),
            ReviewReceipt(
                "example.com/ExampleOrg/ProjectAlpha", 8,
                "https://example.com/ExampleOrg/ProjectAlpha/pull/8",
            ),
            IssueCommentReceipt(
                "example.com/ExampleOrg/ProjectAlpha", 7,
                "https://example.com/ExampleOrg/ProjectAlpha/issues/7",
            ),
        )
        for receipt in receipts:
            with self.subTest(receipt=type(receipt).__name__):
                self.assertEqual(
                    EffectReceipt("forge-comment", "completed", receipt.url, False)
                    .receipt_id,
                    receipt.url,
                )
        with self.assertRaises(EffectIntentError):
            EffectReceipt("forge-comment", "completed", "http://example.com/7", False)

    def test_a_boolean_is_not_a_row_identity(self):
        """bool subclasses int, so True must not pass as work item 1."""
        with self.assertRaises(EffectIntentError):
            EffectIntent(
                "forge-comment", True, 2, "forge", "x", "a" * 64, "b" * 64,
                True,
            )
        with self.assertRaises(EffectIntentError):
            EffectIntent(
                "forge-comment", 1, True, "forge", "x", "a" * 64, "b" * 64,
                True,
            )


class PersistentEffectExecutorTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.database = Path(self.temporary.name) / "effects.sqlite3"
        migrate_database(self.database)
        now = "2030-03-01T12:00:00+00:00"
        with closing(sqlite3.connect(self.database)) as connection, connection:
            task_id = connection.execute(
                "INSERT INTO tasks(status,text,owner,due,version,created_at,"
                "updated_at,closed_at) VALUES('open','Synthetic task',NULL,"
                "NULL,1,?,?,NULL)", (now, now),
            ).lastrowid
            work_item_id = connection.execute(
                "INSERT INTO work_items(task_id,state,created_at,updated_at) "
                "VALUES(?,'active',?,?)", (task_id, now, now),
            ).lastrowid
            self.work_revision_id = connection.execute(
                "INSERT INTO work_revisions(work_item_id,candidate_id,"
                "source_revision,task_version,kind,created_at) "
                "VALUES(?,'synthetic-candidate','source-r1',1,'accepted',?)",
                (work_item_id, now),
            ).lastrowid
            self.work_item_id = work_item_id

    def _intent(self, *, suffix: str = "b") -> EffectIntent:
        return EffectIntent(
            f"forge-{suffix}", self.work_item_id, self.work_revision_id,
            "forge", "example.com/ExampleOrg/ProjectAlpha/issues/7",
            "a" * 64, (suffix * 64), True,
        )

    def test_first_attempt_records_intent_before_calling_adapter(self):
        intent = self._intent()
        adapter = _RecordingExecutor(self.database, "completed")
        executor = PersistentEffectExecutor(
            self.database, adapter, clock=lambda: _now(),
        )

        receipt = executor.execute(intent)

        self.assertEqual(receipt.state, "completed")
        self.assertEqual(adapter.calls, 1)
        self.assertEqual(adapter.intent_rows_when_called, 1)
        with closing(sqlite3.connect(self.database)) as connection:
            stored_intent = connection.execute(
                "SELECT work_item_id,work_revision_id,payload_digest "
                "FROM effect_intents"
            ).fetchone()
            stored_receipt = connection.execute(
                "SELECT work_item_id,work_revision_id,state,receipt_id "
                "FROM effect_receipts"
            ).fetchone()
        self.assertEqual(
            stored_intent,
            (self.work_item_id, self.work_revision_id, "a" * 64),
        )
        self.assertEqual(
            stored_receipt,
            (self.work_item_id, self.work_revision_id, "completed", "receipt-1"),
        )

    def test_completed_receipt_prevents_a_replayed_effect(self):
        intent = self._intent()
        initial_adapter = _RecordingExecutor(self.database, "completed")
        PersistentEffectExecutor(self.database, initial_adapter).execute(intent)
        retry_adapter = _RecordingExecutor(self.database, "completed")

        receipt = PersistentEffectExecutor(self.database, retry_adapter).execute(intent)

        self.assertEqual(receipt.state, "completed")
        self.assertEqual(initial_adapter.calls, 1)
        self.assertEqual(retry_adapter.calls, 0)

    def test_failed_receipt_is_retained_but_a_retry_attempts_again(self):
        intent = self._intent(suffix="c")
        failing_adapter = _RecordingExecutor(self.database, "failed")
        PersistentEffectExecutor(self.database, failing_adapter).execute(intent)
        with closing(sqlite3.connect(self.database)) as connection:
            failed_state = connection.execute(
                "SELECT state FROM effect_receipts WHERE intent_id=?",
                (intent.intent_id,),
            ).fetchone()[0]
        retry_adapter = _RecordingExecutor(self.database, "completed")

        receipt = PersistentEffectExecutor(self.database, retry_adapter).execute(intent)

        self.assertEqual(receipt.state, "completed")
        self.assertEqual(failed_state, "failed")
        self.assertEqual(failing_adapter.calls, 1)
        self.assertEqual(retry_adapter.calls, 1)
        with closing(sqlite3.connect(self.database)) as connection:
            state = connection.execute(
                "SELECT state FROM effect_receipts WHERE intent_id=?",
                (intent.intent_id,),
            ).fetchone()[0]
        self.assertEqual(state, "completed")

    def test_the_effect_migration_replays_when_its_tables_exist(self):
        with closing(sqlite3.connect(self.database)) as connection, connection:
            connection.execute("PRAGMA user_version = 40")

        migrate_database(self.database)

        with closing(sqlite3.connect(self.database)) as connection:
            tables = {
                row[0] for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' "
                    "AND name IN ('effect_intents','effect_receipts')"
                )
            }
            version = connection.execute("PRAGMA user_version").fetchone()[0]
        self.assertEqual(tables, {"effect_intents", "effect_receipts"})
        self.assertEqual(version, SCHEMA_VERSION)

    def test_inspect_confirms_completed_effect_after_crash(self):
        """Crash after the write landed but before the receipt committed.

        The adapter's inspect() reports 'completed', so execute() adopts it
        instead of calling the adapter again and risking a duplicate.
        """
        intent = self._intent(suffix="d")
        # First attempt: adapter executes and completes, but we simulate a
        # crash by deleting the receipt row afterwards.
        initial_adapter = _RecordingExecutor(self.database, "completed")
        PersistentEffectExecutor(self.database, initial_adapter).execute(intent)
        # Simulate the crash scenario: the external write landed but the
        # local receipt commit died.  Delete the receipt row.
        with closing(sqlite3.connect(self.database)) as connection, connection:
            connection.execute(
                "DELETE FROM effect_receipts WHERE intent_id=?",
                (intent.intent_id,),
            )
            # Verify: intent exists, receipt does not
            self.assertTrue(connection.execute(
                "SELECT COUNT(*) FROM effect_intents WHERE intent_id=?",
                (intent.intent_id,),
            ).fetchone()[0] == 1)
            self.assertTrue(connection.execute(
                "SELECT COUNT(*) FROM effect_receipts WHERE intent_id=?",
                (intent.intent_id,),
            ).fetchone()[0] == 0)
        # Retry with an adapter whose inspect() confirms the effect landed.
        retry_adapter = _RecordingExecutor(
            self.database, "completed", inspect_state="completed",
        )
        receipt = PersistentEffectExecutor(self.database, retry_adapter).execute(
            intent,
        )

        self.assertEqual(receipt.state, "completed")
        self.assertEqual(retry_adapter.calls, 0)
        self.assertEqual(retry_adapter.inspect_calls, 1)
        with closing(sqlite3.connect(self.database)) as connection:
            stored = connection.execute(
                "SELECT state FROM effect_receipts WHERE intent_id=?",
                (intent.intent_id,),
            ).fetchone()[0]
        self.assertEqual(stored, "completed")

    def test_inspect_reports_not_done_allows_retry(self):
        """inspect() returns a non-completed receipt -> execute() proceeds."""
        intent = self._intent(suffix="e")
        # First attempt fails, leaving a failed receipt
        failing_adapter = _RecordingExecutor(self.database, "failed")
        PersistentEffectExecutor(self.database, failing_adapter).execute(intent)
        # Retry with inspect() reporting the effect didn't land
        retry_adapter = _RecordingExecutor(
            self.database, "completed", inspect_state="prepared",
        )
        receipt = PersistentEffectExecutor(self.database, retry_adapter).execute(
            intent,
        )

        self.assertEqual(receipt.state, "completed")
        self.assertEqual(retry_adapter.calls, 1)
        self.assertEqual(retry_adapter.inspect_calls, 1)

    def test_inspect_cannot_answer_still_proceeds(self):
        """When inspect() returns None, execute() proceeds to call the adapter.

        The adapter's idempotency key or the external system's own dedup should
        handle this case. The executor cannot prevent the call when the adapter
        literally cannot determine the effect's state.
        """
        intent = self._intent(suffix="f")
        # First attempt fails
        failing_adapter = _RecordingExecutor(self.database, "failed")
        PersistentEffectExecutor(self.database, failing_adapter).execute(intent)
        # Retry with inspect() returning None (cannot answer)
        retry_adapter = _RecordingExecutor(
            self.database, "completed", inspect_state=None,
        )
        receipt = PersistentEffectExecutor(self.database, retry_adapter).execute(
            intent,
        )

        self.assertEqual(receipt.state, "completed")
        self.assertEqual(retry_adapter.calls, 1)
        self.assertEqual(retry_adapter.inspect_calls, 1)

    def test_cancel_records_a_receipt(self):
        """A cancelled effect is recorded so an intent with no receipt no
        longer conflates cancellation with a crash."""
        intent = self._intent(suffix="a")
        executor = PersistentEffectExecutor(self.database, _RecordingExecutor(
            self.database, "completed",
        ))
        # Persist the intent first
        executor._persist_intent(intent)
        receipt = executor.cancel(intent)

        self.assertEqual(receipt.state, "cancelled")
        with closing(sqlite3.connect(self.database)) as connection:
            stored = connection.execute(
                "SELECT state FROM effect_receipts WHERE intent_id=?",
                (intent.intent_id,),
            ).fetchone()[0]
        self.assertEqual(stored, "cancelled")


class _RecordingExecutor:
    """Adapter mock that records calls and returns a configurable state.

    Parameters:
        database: database path for internal checks
        state: state returned by ``execute()``
        inspect_state: state returned by ``inspect()``. Pass ``None`` to make
            ``inspect()`` return ``None`` (cannot answer).  Pass a string from
            ``ATTEMPT_STATES`` to return a matching ``EffectReceipt``.
            Defaults to ``None`` (cannot answer).
    """
    def __init__(
        self,
        database: Path,
        state: str,
        *,
        inspect_state: str | None = None,
    ):
        self.database = database
        self.state = state
        self.inspect_state = inspect_state
        self.calls = 0
        self.inspect_calls = 0
        self.intent_rows_when_called = 0

    def prepare(self, intent: EffectIntent) -> EffectReceipt:
        return EffectReceipt(intent.intent_id, "prepared", None, False)

    def preflight(self, intent: EffectIntent) -> EffectReceipt:
        return EffectReceipt(intent.intent_id, "prepared", None, False)

    def execute(self, intent: EffectIntent) -> EffectReceipt:
        self.calls += 1
        with closing(sqlite3.connect(self.database)) as connection:
            self.intent_rows_when_called = connection.execute(
                "SELECT COUNT(*) FROM effect_intents WHERE intent_id=?",
                (intent.intent_id,),
            ).fetchone()[0]
        return EffectReceipt(
            intent.intent_id, self.state, f"receipt-{self.calls}", False,
        )

    def inspect(self, intent: EffectIntent) -> EffectReceipt | None:
        self.inspect_calls += 1
        if self.inspect_state is None:
            return None
        return EffectReceipt(
            intent.intent_id, self.inspect_state, None, False,
        )

    def cancel(self, intent: EffectIntent) -> EffectReceipt:
        return EffectReceipt(intent.intent_id, "cancelled", None, False)


def _now() -> datetime:
    return datetime(2030, 3, 1, 12, 0, tzinfo=timezone.utc)
