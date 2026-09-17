"""Private, immutable results from a duplicate-detector evaluation.

Assessment rows deliberately contain no task text, prompt, model output, or
free-form explanation.  A reader's duplicate proposal is the label; this
ledger only says which closed relation an experimental detector chose for an
unordered pair and what bounded local resources that choice used.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from enum import StrEnum


MAX_DETECTOR = 64


class SemanticVerdict(StrEnum):
    """Closed relation choices a semantic evaluator may return."""

    REDUNDANT = "redundant"
    INTERSECTING = "intersecting"
    INTERCONNECTED = "interconnected"


class AssessmentDisposition(StrEnum):
    RECORDED = "recorded"
    UNCHANGED = "unchanged"


@dataclass(frozen=True)
class AssessmentResult:
    disposition: AssessmentDisposition


@dataclass(frozen=True)
class AssessmentCounts:
    """Content-free quality and resource counts for one semantic detector."""

    detector: str
    assessed: int
    redundant: int
    intersecting: int
    interconnected: int
    labeled: int
    confirmed: int
    rejected: int
    awaiting: int
    latency_ms: int
    prompt_tokens: int
    completion_tokens: int
    redundant_without_overlap: int
    not_redundant_with_overlap: int


def record(
    connection: sqlite3.Connection,
    *,
    task_id_a: int,
    task_id_b: int,
    detector: str,
    verdict: SemanticVerdict,
    latency_ms: int,
    prompt_tokens: int,
    completion_tokens: int,
    assessed_at: str,
) -> AssessmentResult:
    """Append one immutable content-free assessment, idempotently."""
    task_id_a = _identifier(task_id_a, "first task id")
    task_id_b = _identifier(task_id_b, "second task id")
    if task_id_a == task_id_b:
        raise ValueError("assessment cannot compare one task")
    detector = _text(detector, "detector", MAX_DETECTOR)
    assessed_at = _text(assessed_at, "assessment timestamp", 40)
    if not isinstance(verdict, SemanticVerdict):
        raise ValueError("assessment verdict is invalid")
    latency_ms = _count(latency_ms, "latency")
    prompt_tokens = _count(prompt_tokens, "prompt tokens")
    completion_tokens = _count(completion_tokens, "completion tokens")
    left_task_id, right_task_id = sorted((task_id_a, task_id_b))
    cursor = connection.execute(
        "INSERT OR IGNORE INTO task_duplicate_assessments("
        "left_task_id,right_task_id,detector,verdict,latency_ms,"
        "prompt_tokens,completion_tokens,assessed_at) VALUES(?,?,?,?,?,?,?,?)",
        (left_task_id, right_task_id, detector, verdict.value, latency_ms,
         prompt_tokens, completion_tokens, assessed_at),
    )
    return AssessmentResult(
        AssessmentDisposition.RECORDED
        if cursor.rowcount == 1 else AssessmentDisposition.UNCHANGED
    )


def counts(connection: sqlite3.Connection) -> tuple[AssessmentCounts, ...]:
    """Summarize paired reader labels without selecting private task content."""
    rows = connection.execute(
        "WITH labels AS ("
        " SELECT left_task_id,right_task_id,MAX(state) AS state"
        " FROM task_duplicate_proposals"
        " WHERE state IN ('confirmed','rejected')"
        " GROUP BY left_task_id,right_task_id"
        "), overlap AS ("
        " SELECT left_task_id,right_task_id FROM task_duplicate_proposals"
        " WHERE basis LIKE 'shared task terms across %'"
        ")"
        " SELECT a.detector,COUNT(*) AS assessed,"
        " SUM(a.verdict='redundant') AS redundant,"
        " SUM(a.verdict='intersecting') AS intersecting,"
        " SUM(a.verdict='interconnected') AS interconnected,"
        " SUM(label.state IS NOT NULL) AS labeled,"
        " SUM(a.verdict='redundant' AND label.state='confirmed') AS confirmed,"
        " SUM(a.verdict='redundant' AND label.state='rejected') AS rejected,"
        " SUM(a.verdict='redundant' AND label.state IS NULL) AS awaiting,"
        " SUM(a.latency_ms) AS latency_ms,"
        " SUM(a.prompt_tokens) AS prompt_tokens,"
        " SUM(a.completion_tokens) AS completion_tokens,"
        " SUM(a.verdict='redundant' AND overlap.left_task_id IS NULL)"
        " AS redundant_without_overlap,"
        " SUM(a.verdict<>'redundant' AND overlap.left_task_id IS NOT NULL)"
        " AS not_redundant_with_overlap"
        " FROM task_duplicate_assessments AS a"
        " LEFT JOIN labels AS label ON label.left_task_id=a.left_task_id"
        " AND label.right_task_id=a.right_task_id"
        " LEFT JOIN overlap ON overlap.left_task_id=a.left_task_id"
        " AND overlap.right_task_id=a.right_task_id"
        " GROUP BY a.detector ORDER BY a.detector"
    ).fetchall()
    return tuple(AssessmentCounts(
        detector=str(row["detector"]),
        assessed=int(row["assessed"] or 0),
        redundant=int(row["redundant"] or 0),
        intersecting=int(row["intersecting"] or 0),
        interconnected=int(row["interconnected"] or 0),
        labeled=int(row["labeled"] or 0),
        confirmed=int(row["confirmed"] or 0),
        rejected=int(row["rejected"] or 0),
        awaiting=int(row["awaiting"] or 0),
        latency_ms=int(row["latency_ms"] or 0),
        prompt_tokens=int(row["prompt_tokens"] or 0),
        completion_tokens=int(row["completion_tokens"] or 0),
        redundant_without_overlap=int(
            row["redundant_without_overlap"] or 0
        ),
        not_redundant_with_overlap=int(
            row["not_redundant_with_overlap"] or 0
        ),
    ) for row in rows)


def _identifier(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"{field} is invalid")
    return value


def _text(value: object, field: str, maximum: int) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field} must be text")
    value = value.strip()
    if not 1 <= len(value) <= maximum:
        raise ValueError(f"{field} has invalid length")
    if any(ord(char) < 32 or ord(char) == 127 for char in value):
        raise ValueError(f"{field} contains control characters")
    return value


def _count(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{field} is invalid")
    return value
