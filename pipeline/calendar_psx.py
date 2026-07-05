"""
pipeline/calendar_psx.py — Generate expected PSX trading days.

Uses a simple Mon–Fri calendar minus a holidays override file.
Optionally tries pandas-market-calendars for XKAR if available.
"""

import logging
from datetime import date, timedelta
from pathlib import Path
from typing import List, Set

import yaml

log = logging.getLogger(__name__)


def _load_holidays(holidays_path: Path) -> Set[date]:
    """Load holiday dates from config/holidays.yaml."""
    if not holidays_path.exists():
        log.warning("holidays.yaml not found at %s — no holidays will be excluded", holidays_path)
        return set()

    with open(holidays_path) as f:
        data = yaml.safe_load(f)

    holidays = set()
    for entry in data.get("holidays", []):
        d = entry["date"]
        if isinstance(d, str):
            d = date.fromisoformat(d)
        holidays.add(d)
        log.debug("Holiday: %s — %s", d, entry.get("name", ""))

    log.info("Loaded %d holidays from %s", len(holidays), holidays_path)
    return holidays


def expected_trading_days(
    start_date: date,
    end_date: date,
    holidays_path: Path,
    session_days: List[str] = None,
) -> List[date]:
    """
    Generate a sorted list of expected PSX trading days between start_date
    and end_date (inclusive), excluding weekends and holidays.

    Parameters
    ----------
    start_date, end_date : date
    holidays_path : Path — path to config/holidays.yaml
    session_days : list of day-name strings, e.g. ['mon','tue','wed','thu','fri']
    """
    if session_days is None:
        session_days = ["mon", "tue", "wed", "thu", "fri"]

    # Map day names to weekday ints (Mon=0 … Sun=6)
    day_map = {"mon": 0, "tue": 1, "wed": 2, "thu": 3, "fri": 4, "sat": 5, "sun": 6}
    allowed_weekdays = {day_map[d.lower()] for d in session_days}

    holidays = _load_holidays(holidays_path)

    days = []
    current = start_date
    while current <= end_date:
        if current.weekday() in allowed_weekdays and current not in holidays:
            days.append(current)
        current += timedelta(days=1)

    log.info(
        "Expected trading days: %d between %s and %s (%d holidays excluded)",
        len(days), start_date, end_date, len(holidays),
    )
    return days


def is_trading_day(
    d: date,
    holidays_path: Path,
    session_days: List[str] = None,
) -> bool:
    """Check if a single date is an expected trading day."""
    if session_days is None:
        session_days = ["mon", "tue", "wed", "thu", "fri"]
    day_map = {"mon": 0, "tue": 1, "wed": 2, "thu": 3, "fri": 4, "sat": 5, "sun": 6}
    allowed_weekdays = {day_map[d_name.lower()] for d_name in session_days}

    if d.weekday() not in allowed_weekdays:
        return False

    holidays = _load_holidays(holidays_path)
    return d not in holidays
