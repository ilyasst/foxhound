"""Keep/Undo cards for tasks closed by their forge source (#844)."""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from foxhound import migrate_database
from foxhound.change_reviews import ChangeReviewService, render_change_review_card
from foxhound.task_ledger import TaskLedger

NOW = datetime(2030, 1, 1, 12, 0, tzinfo=timezone.utc)
NOW_ISO = NOW.isoformat(timespec="seconds")
TOKEN = "t" * 43
CONSUMER = "c" * 64


def _candidate(kind="review_request", number="32", reason=None):
    return SimpleNamespace(
        source=SimpleNamespace(kind=kind, item_id=number),
        lifecycle=SimpleNamespace(reason=reason),
    )


@pytest.fixture
def database(tmp_path: Path) -> Path:
    path = tmp_path / "foxhound.sqlite3"
    migrate_database(path)
    with sqlite3.connect(path) as connection:
        connection.execute(
            "INSERT INTO tasks(id,version,status,text,owner,due,created_at,updated_at) "
            "VALUES(1,1,'open','Review the synthetic change','Person A',NULL,?,?)",
            (NOW_ISO, NOW_ISO),
        )
        connection.execute(
            "INSERT INTO task_execution_workflows(task_id,task_version,version,status,"
            "phase,created_at,updated_at) VALUES(1,1,1,'awaiting_start','plan',?,?)",
            (NOW_ISO, NOW_ISO),
        )
    return path


def _close(database: Path, **candidate) -> None:
    connection = sqlite3.connect(database)
    connection.row_factory = sqlite3.Row
    with connection:
        task = connection.execute("SELECT * FROM tasks WHERE id=1").fetchone()
        TaskLedger._close_for_forge_source(
            connection, candidate=_candidate(**candidate), task=task, now=NOW_ISO)
    connection.close()


def _service(database: Path, now: datetime = NOW) -> ChangeReviewService:
    return ChangeReviewService(database, clock=lambda: now, token_factory=lambda: TOKEN)


def _deliver(service: ChangeReviewService):
    claim = service.claim_next(consumer_digest=CONSUMER, lease_seconds=60)
    assert claim is not None
    result = service.complete_delivery(
        claim.card.card_id, expected_version=claim.card.version,
        claim_token=claim.token, transport="telegram", delivery_ref="m-1")
    assert result.accepted
    return claim


def _task(database: Path) -> tuple[str, int]:
    with sqlite3.connect(database) as connection:
        return connection.execute("SELECT status,version FROM tasks WHERE id=1").fetchone()


def test_forge_close_records_one_pending_review(database):
    _close(database, reason="pr_merged")
    with sqlite3.connect(database) as connection:
        connection.row_factory = sqlite3.Row
        rows = connection.execute("SELECT * FROM automatic_change_reviews").fetchall()
    assert len(rows) == 1
    assert (rows[0]["kind"], rows[0]["status"]) == ("source_closed", "pending")
    assert rows[0]["summary"] == "Pull request #32 was merged"
    assert json.loads(rows[0]["before_state"])["task"]["status"] == "open"
    assert json.loads(rows[0]["after_state"])["task"]["status"] == "done"


@pytest.mark.parametrize("candidate,summary", [
    ({"reason": "pr_closed"}, "Pull request #32 was closed without merging"),
    ({"kind": "issue", "number": "7", "reason": "issue_not_planned"},
     "Issue #7 was closed as not planned"),
    ({"kind": "issue", "number": "7"}, "Issue #7 was closed"),
])
def test_summary_says_how_the_source_ended(database, candidate, summary):
    _close(database, **candidate)
    with sqlite3.connect(database) as connection:
        assert connection.execute(
            "SELECT summary FROM automatic_change_reviews").fetchone()[0] == summary


def test_claim_renders_body_and_fhc_controls(database):
    _close(database, reason="pr_merged")
    claim = _service(database).claim_next(consumer_digest=CONSUMER, lease_seconds=60)
    body, markup = render_change_review_card(claim.card)
    assert "T1 Review the synthetic change" in body
    assert "Pull request #32 was merged" in body
    callbacks = [b["callback_data"] for b in markup["inline_keyboard"][0]]
    assert callbacks == [f"fhc|{claim.card.card_id}|{claim.card.version}|keep",
                         f"fhc|{claim.card.card_id}|{claim.card.version}|undo"]


def test_a_delivered_card_is_not_claimed_again(database):
    _close(database)
    service = _service(database)
    _deliver(service)
    assert service.claim_next(consumer_digest=CONSUMER, lease_seconds=60) is None


def test_failed_delivery_and_expired_lease_return_the_card(database):
    _close(database)
    service = _service(database)
    claim = service.claim_next(consumer_digest=CONSUMER, lease_seconds=60)
    assert service.fail_delivery(claim.card.card_id, expected_version=claim.card.version,
                                 claim_token=claim.token).accepted
    again = service.claim_next(consumer_digest=CONSUMER, lease_seconds=60)
    assert again is not None and again.card.version > claim.card.version
    later = _service(database, NOW + timedelta(minutes=5))
    assert later.claim_next(consumer_digest=CONSUMER, lease_seconds=60) is not None


def test_keep_finalises(database):
    _close(database)
    service = _service(database)
    claim = _deliver(service)
    assert service.act(claim.card.card_id, expected_version=claim.card.version,
                       action="keep").accepted
    assert _task(database)[0] == "done"
    assert not service.act(claim.card.card_id, expected_version=claim.card.version,
                           action="undo").accepted


def test_undo_reopens_the_task_with_a_proper_event(database):
    _close(database)
    closed_version = _task(database)[1]
    service = _service(database)
    claim = _deliver(service)
    assert service.act(claim.card.card_id, expected_version=claim.card.version,
                       action="undo").accepted
    assert _task(database) == ("open", closed_version + 1)
    with sqlite3.connect(database) as connection:
        event = connection.execute(
            "SELECT kind,task_version,from_status,to_status FROM task_events "
            "WHERE task_id=1 ORDER BY sequence DESC LIMIT 1").fetchone()
    assert event == ("status_changed", closed_version + 1, "done", "open")


def test_undo_after_the_task_moved_is_refused(database):
    _close(database)
    service = _service(database)
    claim = _deliver(service)
    with sqlite3.connect(database) as connection:
        connection.execute("UPDATE tasks SET version=version+1 WHERE id=1")
    result = service.act(claim.card.card_id, expected_version=claim.card.version,
                         action="undo")
    assert not result.accepted and result.refusal.value == "undo_conflict"
    assert _task(database)[0] == "done"


def test_stale_card_version_is_refused(database):
    _close(database)
    service = _service(database)
    claim = _deliver(service)
    result = service.act(claim.card.card_id, expected_version=claim.card.version + 1,
                         action="keep")
    assert not result.accepted and result.refusal.value == "stale_card"
