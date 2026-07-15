from __future__ import annotations

from datetime import date, datetime, time
from typing import List

import pandas as pd


def _as_date(value: str | date | datetime | pd.Timestamp) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, pd.Timestamp):
        return value.date()
    if isinstance(value, date):
        return value
    return pd.Timestamp(value).date()


def xnys_trading_days(start_date: str | date, end_date: str | date) -> List[date]:
    """Return XNYS trading days, inclusive, using pandas_market_calendars when available."""
    start = _as_date(start_date)
    end = _as_date(end_date)
    if end < start:
        return []

    try:
        import pandas_market_calendars as mcal

        cal = mcal.get_calendar("XNYS")
        sched = cal.schedule(start_date=start.isoformat(), end_date=end.isoformat())
        return [pd.Timestamp(idx).date() for idx in sched.index]
    except Exception:
        # Conservative fallback: weekdays minus a small static 2026 holiday set.
        # This is only a fallback for local environments without pandas_market_calendars.
        all_days = pd.date_range(start=start, end=end, freq="B")
        known_closed_2026 = {
            date(2026, 1, 1), date(2026, 1, 19), date(2026, 2, 16),
            date(2026, 4, 3), date(2026, 5, 25), date(2026, 6, 19),
            date(2026, 7, 3), date(2026, 9, 7), date(2026, 11, 26),
            date(2026, 12, 25),
        }
        return [d.date() for d in all_days if d.date() not in known_closed_2026]


def latest_completed_eod_date(as_of: str | date | datetime | None = None) -> date:
    """Return latest completed XNYS trading date as of local calendar date.

    This is date-based, not clock-time based. For intraday production use, wire this
    to current Eastern time and market close. For Step 01 inventory this is enough.
    """
    today = _as_date(as_of or pd.Timestamp.today())
    candidates = xnys_trading_days(today - pd.Timedelta(days=10), today)
    if today in candidates:
        # Date-based inventory should not assume today's EOD is complete.
        # Use previous trading day.
        candidates = [d for d in candidates if d < today]
    return max(candidates)
