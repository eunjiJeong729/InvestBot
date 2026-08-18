"""XKRX 캘린더 경계: first/last_session은 요청 start/end와 다를 수 있다."""

from __future__ import annotations

import unittest
from datetime import date

import exchange_calendars as xcals

from src.market.tasks._market_calendar import _CALENDAR_CODE, build_calendar_rows


class XkrxSessionBoundsTest(unittest.TestCase):
    def test_first_last_session_clip_to_trading_days_not_start_end(self) -> None:
        start, end = date(2026, 1, 1), date(2026, 12, 31)
        calendar = xcals.get_calendar(_CALENDAR_CODE, start=start, end=end)

        self.assertNotEqual(calendar.first_session.date(), start)
        self.assertNotEqual(calendar.last_session.date(), end)
        self.assertEqual(calendar.first_session.date(), date(2026, 1, 2))
        self.assertEqual(calendar.last_session.date(), date(2026, 12, 30))

        with self.assertRaises(xcals.errors.DateOutOfBounds):
            calendar.is_session(start)
        with self.assertRaises(xcals.errors.DateOutOfBounds):
            calendar.is_session(end)

        rows = {day: (is_open, open_time) for day, is_open, open_time in build_calendar_rows(start=start, end=end)}
        self.assertEqual(rows[start], (0, None))
        self.assertEqual(rows[end], (0, None))
        self.assertEqual(rows[date(2026, 1, 2)][0], 1)
        self.assertEqual(rows[date(2026, 12, 30)][0], 1)


if __name__ == "__main__":
    unittest.main()
