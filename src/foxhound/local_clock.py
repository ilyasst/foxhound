"""Host-local clock helpers for task context and research agents."""

from __future__ import annotations

import datetime as _dt
import os
from pathlib import Path
import re


def _detect_iana_timezone(now: _dt.datetime) -> str:
    """Resolve the host IANA timezone name, falling back to UTC offset string."""
    env_tz = os.environ.get("TZ")
    if env_tz:
        clean_tz = env_tz.strip().lstrip(":")
        if "/" in clean_tz and not re.search(r"[^A-Za-z0-9_/-]", clean_tz):
            return clean_tz
    try:
        localtime_target = str(Path("/etc/localtime").resolve())
        if "zoneinfo/" in localtime_target:
            return localtime_target.split("zoneinfo/", 1)[1]
    except Exception:
        pass
    # Fallback to offset string like "-04:00" or "+00:00"
    offset = now.strftime("%z")
    if len(offset) == 5:
        return f"{offset[:3]}:{offset[3:]}"
    return offset or "+00:00"


def local_today(now: _dt.datetime | None = None) -> str:
    """Return the host's authoritative local calendar date (YYYY-MM-DD)."""
    if now is None:
        now = _dt.datetime.now().astimezone()
    return now.date().isoformat()


def local_calendar(
    now: _dt.datetime | None = None,
    *,
    today_str: str | None = None,
) -> dict[str, object]:
    """Return the local calendar structure (today, weekday, next_week range)."""
    if today_str is not None:
        today = _dt.date.fromisoformat(today_str)
    elif now is not None:
        today = now.date()
    else:
        today = _dt.date.fromisoformat(local_today())

    next_week_start = today + _dt.timedelta(days=7 - today.weekday())
    next_week_end = next_week_start + _dt.timedelta(days=6)
    weekdays = (
        "Monday", "Tuesday", "Wednesday", "Thursday", "Friday",
        "Saturday", "Sunday",
    )
    return {
        "today": today.isoformat(),
        "today_weekday": weekdays[today.weekday()],
        "next_week": {
            "start": next_week_start.isoformat(),
            "start_weekday": weekdays[next_week_start.weekday()],
            "end": next_week_end.isoformat(),
            "end_weekday": weekdays[next_week_end.weekday()],
        },
    }


def runtime_clock(now: _dt.datetime | None = None) -> dict[str, object]:
    """Return runtime clock dictionary with calendar, now timestamp, and timezone."""
    if now is None:
        now = _dt.datetime.now().astimezone()
    elif now.tzinfo is None:
        now = now.astimezone()

    tz_name = _detect_iana_timezone(now)
    tz_abbr = now.tzname() or ""

    return {
        **local_calendar(now),
        "now": now.isoformat(timespec="minutes"),
        "timezone": tz_name,
        "timezone_abbreviation": tz_abbr,
    }
