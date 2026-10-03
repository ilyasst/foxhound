"""Tests for host-local clock helpers."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from unittest import mock
from zoneinfo import ZoneInfo

from foxhound.local_clock import local_calendar, local_today, runtime_clock


def test_runtime_clock_toronto_winter_est():
    toronto = ZoneInfo("America/Toronto")
    # 2026-01-15 09:42:00 EST (UTC-5)
    now = datetime(2026, 1, 15, 9, 42, 0, tzinfo=toronto)
    with mock.patch.dict("os.environ", {"TZ": "America/Toronto"}):
        clock = runtime_clock(now)

    assert clock["today"] == "2026-01-15"
    assert clock["today_weekday"] == "Thursday"
    assert clock["now"] == "2026-01-15T09:42-05:00"
    assert clock["timezone"] == "America/Toronto"
    assert clock["timezone_abbreviation"] == "EST"
    assert clock["next_week"] == {
        "start": "2026-01-19",
        "start_weekday": "Monday",
        "end": "2026-01-25",
        "end_weekday": "Sunday",
    }


def test_runtime_clock_toronto_summer_edt():
    toronto = ZoneInfo("America/Toronto")
    # 2026-07-20 15:30:00 EDT (UTC-4)
    now = datetime(2026, 7, 20, 15, 30, 0, tzinfo=toronto)
    with mock.patch.dict("os.environ", {"TZ": "America/Toronto"}):
        clock = runtime_clock(now)

    assert clock["today"] == "2026-07-20"
    assert clock["today_weekday"] == "Monday"
    assert clock["now"] == "2026-07-20T15:30-04:00"
    assert clock["timezone"] == "America/Toronto"
    assert clock["timezone_abbreviation"] == "EDT"
    assert clock["next_week"] == {
        "start": "2026-07-27",
        "start_weekday": "Monday",
        "end": "2026-08-02",
        "end_weekday": "Sunday",
    }


def test_local_calendar_next_week_across_month_end():
    toronto = ZoneInfo("America/Toronto")
    # 2026-01-29 Thursday -> next Monday is 2026-02-02, next Sunday is 2026-02-08
    now = datetime(2026, 1, 29, 10, 0, 0, tzinfo=toronto)
    cal = local_calendar(now)
    assert cal["today"] == "2026-01-29"
    assert cal["today_weekday"] == "Thursday"
    assert cal["next_week"] == {
        "start": "2026-02-02",
        "start_weekday": "Monday",
        "end": "2026-02-08",
        "end_weekday": "Sunday",
    }


def test_local_today_default_and_injected():
    toronto = ZoneInfo("America/Toronto")
    now = datetime(2026, 3, 31, 23, 59, 0, tzinfo=toronto)
    assert local_today(now) == "2026-03-31"


def test_runtime_clock_detect_iana_from_etc_localtime(tmp_path: Path):
    toronto = ZoneInfo("America/Toronto")
    now = datetime(2026, 1, 15, 9, 42, 0, tzinfo=toronto)

    # Point Path("/etc/localtime").resolve() to something in zoneinfo
    fake_target = Path("/usr/share/zoneinfo/America/Toronto")
    with mock.patch.dict("os.environ", {}, clear=True):
        with mock.patch("foxhound.local_clock.Path.resolve", return_value=fake_target):
            clock = runtime_clock(now)
            assert clock["timezone"] == "America/Toronto"


def test_runtime_clock_detect_iana_fallback_to_offset_never_raises():
    toronto = ZoneInfo("America/Toronto")
    now = datetime(2026, 1, 15, 9, 42, 0, tzinfo=toronto)

    # When TZ is empty and /etc/localtime resolution fails or contains no zoneinfo
    with mock.patch.dict("os.environ", {}, clear=True):
        with mock.patch("foxhound.local_clock.Path.resolve", side_effect=OSError("no file")):
            clock = runtime_clock(now)
            assert clock["timezone"] == "-05:00"
            assert clock["timezone_abbreviation"] == "EST"


def test_runtime_clock_default_now():
    clock = runtime_clock()
    assert "today" in clock
    assert "today_weekday" in clock
    assert "next_week" in clock
    assert "now" in clock
    assert "timezone" in clock
    assert "timezone_abbreviation" in clock
