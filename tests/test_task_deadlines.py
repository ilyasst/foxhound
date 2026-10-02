from datetime import date
import pytest

from foxhound.task_deadlines import TaskTiming, band, effective_deadlines


def test_own_due_only():
    today = date(2026, 10, 1)
    tasks = {
        "t1": TaskTiming(due=date(2026, 10, 10), effort="day", open=True),
        "t2": TaskTiming(due=None, effort=None, open=True),
    }
    res = effective_deadlines(tasks, [], today=today)
    assert res.deadlines["t1"] == date(2026, 10, 10)
    assert res.deadlines["t2"] is None
    assert res.cycles == []
    assert res.infeasible == []


def test_dependant_pulls_predecessor_forward_minus_effort():
    today = date(2026, 10, 1)
    # t1 predecessor, t2 dependant
    tasks = {
        "t1": TaskTiming(due=date(2026, 10, 20), effort="hour", open=True),
        "t2": TaskTiming(due=date(2026, 10, 15), effort="day", open=True),  # effort day -> 1 day
    }
    # t1 -> t2 (t2 depends on t1)
    res = effective_deadlines(tasks, [("t1", "t2")], today=today)
    # t2 due is 2026-10-15, effort is day (1 day) -> pulls t1 to 2026-10-14
    assert res.deadlines["t1"] == date(2026, 10, 14)
    assert res.deadlines["t2"] == date(2026, 10, 15)


def test_transitive_chain_of_three():
    today = date(2026, 10, 1)
    # chain: t1 -> t2 -> t3
    # t3 due Oct 20, effort week (7 days) -> pulls t2 to Oct 13
    # t2 effort day (1 day) -> pulls t1 to Oct 12
    tasks = {
        "t1": TaskTiming(due=date(2026, 10, 25), effort="hour", open=True),
        "t2": TaskTiming(due=None, effort="day", open=True),
        "t3": TaskTiming(due=date(2026, 10, 20), effort="week", open=True),
    }
    deps = [("t1", "t2"), ("t2", "t3")]
    res = effective_deadlines(tasks, deps, today=today)
    assert res.deadlines["t3"] == date(2026, 10, 20)
    assert res.deadlines["t2"] == date(2026, 10, 13)
    assert res.deadlines["t1"] == date(2026, 10, 12)


def test_closed_dependants_ignored():
    today = date(2026, 10, 1)
    tasks = {
        "t1": TaskTiming(due=date(2026, 10, 20), effort=None, open=True),
        "t2": TaskTiming(due=date(2026, 10, 5), effort="day", open=False),  # closed!
    }
    res = effective_deadlines(tasks, [("t1", "t2")], today=today)
    assert res.deadlines["t1"] == date(2026, 10, 20)
    assert res.deadlines["t2"] == date(2026, 10, 5)


def test_cycle_reported_and_does_not_hang():
    today = date(2026, 10, 1)
    tasks = {
        "c1": TaskTiming(due=date(2026, 10, 10), effort="day", open=True),
        "c2": TaskTiming(due=date(2026, 10, 12), effort="day", open=True),
        "out": TaskTiming(due=date(2026, 10, 30), effort="hour", open=True),
    }
    deps = [("c1", "c2"), ("c2", "c1"), ("out", "c1")]
    res = effective_deadlines(tasks, deps, today=today)
    assert len(res.cycles) == 1
    # Tasks in cycle keep their own due only
    assert res.deadlines["c1"] == date(2026, 10, 10)
    assert res.deadlines["c2"] == date(2026, 10, 12)
    # out's dependant c1 is in cycle, so cyclic node doesn't pull out
    assert res.deadlines["out"] == date(2026, 10, 30)


def test_infeasible_reported():
    today = date(2026, 10, 10)
    tasks = {
        "t1": TaskTiming(due=date(2026, 10, 15), effort=None, open=True),
        "t2": TaskTiming(due=date(2026, 10, 11), effort="week", open=True),  # 7 days -> 10-04 < 10-10
    }
    res = effective_deadlines(tasks, [("t1", "t2")], today=today)
    assert res.deadlines["t1"] == date(2026, 10, 4)
    assert res.infeasible == [("t1", date(2026, 10, 4))]


def test_band_boundaries():
    today = date(2026, 10, 10)
    # None -> 2
    assert band(None, today) == 2
    # overdue (< today) -> 0
    assert band(date(2026, 10, 9), today) == 0
    # today -> 0
    assert band(date(2026, 10, 10), today) == 0
    # within 3 days -> 0
    assert band(date(2026, 10, 13), today) == 0
    # 4 days -> 1
    assert band(date(2026, 10, 14), today) == 1
    # 14 days -> 1
    assert band(date(2026, 10, 24), today) == 1
    # 15 days -> 2
    assert band(date(2026, 10, 25), today) == 2


def test_large_scale_10k_tasks():
    today = date(2026, 1, 1)
    tasks = {
        i: TaskTiming(
            due=date(2026, 6, 1) if i == 9999 else None,
            effort="hour",  # 0 days effort
            open=True,
        )
        for i in range(10000)
    }
    # Chain 0 -> 1 -> 2 ... -> 9999
    deps = [(i, i + 1) for i in range(9999)]
    res = effective_deadlines(tasks, deps, today=today)
    assert res.deadlines[0] == date(2026, 6, 1)
    assert len(res.deadlines) == 10000
    assert res.cycles == []
