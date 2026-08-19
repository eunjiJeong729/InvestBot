#!/usr/bin/env python3
"""XKRX 거래일 캘린더를 ``d_market_calendar``에 스냅샷 적재한다.

Airflow DAG가 아니다. 연 1회 또는 임시공휴일 발표 시 수동 실행한다.

    python scripts/load_d_market_calendar.py --config configs/dev/debug.json
"""

from __future__ import annotations

import argparse
import sys
from datetime import date, datetime, time, timedelta
from typing import Any

from infra.db.rdbms.mysql import MySQLClient
from src.common.entity import DMarketCalendar
from src.common.utils.config import init_runtime_config
from src.common.utils.db_snapshot import replace_snapshot

_CALENDAR_CODE = "XKRX"
_OPEN_TZ = "Asia/Seoul"
_SNAPSHOT_COLUMNS = (
    "market_date",
    "is_market_open",
    "market_open_time",
    "created_at",
    "updated_at",
)


def year_range(*, today: date, years_back: int, years_forward: int) -> tuple[date, date]:
    """적재 구간: ``today.year - years_back`` 1/1 ~ ``today.year + years_forward`` 12/31."""
    if years_back < 0 or years_forward < 0:
        raise ValueError("years_back and years_forward must be >= 0")
    start = date(today.year - years_back, 1, 1)
    end = date(today.year + years_forward, 12, 31)
    return start, end


def build_calendar_rows(*, start: date, end: date) -> list[tuple[date, int, time | None]]:
    """XKRX 세션을 ``start``~``end`` 날짜 전부에 대해 row로 만든다."""
    if end < start:
        raise ValueError("end must be >= start")

    import exchange_calendars as xcals

    calendar = xcals.get_calendar(_CALENDAR_CODE, start=start, end=end)
    first_session = calendar.first_session.date()
    last_session = calendar.last_session.date()
    rows: list[tuple[date, int, time | None]] = []
    day = start
    one_day = timedelta(days=1)
    while day <= end:
        is_open = first_session <= day <= last_session and bool(calendar.is_session(day))
        open_time: time | None = None
        if is_open:
            opened_at = calendar.session_open(day).tz_convert(_OPEN_TZ)
            open_time = time(opened_at.hour, opened_at.minute, opened_at.second)
        rows.append((day, 1 if is_open else 0, open_time))
        day += one_day
    return rows


def load_market_calendar(*, start: date, end: date) -> dict[str, Any]:
    """계산한 캘린더를 ``replace_snapshot``으로 적재한다."""
    from src.common.utils.config import load_mysql_config

    db = MySQLClient(load_mysql_config())
    now = datetime.now()
    rows = [
        (market_date, is_open, open_time, now, now)
        for market_date, is_open, open_time in build_calendar_rows(start=start, end=end)
    ]
    replace_snapshot(
        db,
        table=DMarketCalendar.TABLE,
        columns=_SNAPSHOT_COLUMNS,
        rows=rows,
    )
    open_days = sum(flag for _, flag, _, _, _ in rows)
    return {
        "table": DMarketCalendar.TABLE,
        "start": start.isoformat(),
        "end": end.isoformat(),
        "rows": len(rows),
        "open_days": open_days,
        "closed_days": len(rows) - open_days,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Load d_market_calendar from XKRX")
    parser.add_argument(
        "--config",
        default="configs/dev/debug.json",
        help="런타임 설정 파일 경로 (기본값: configs/dev/debug.json)",
    )
    parser.add_argument(
        "--years-back",
        type=int,
        default=1,
        help="올해 기준 과거 연수 (기본 1 → 작년 1/1부터)",
    )
    parser.add_argument(
        "--years-forward",
        type=int,
        default=2,
        help="올해 기준 미래 연수 (기본 2 → 내후년 12/31까지)",
    )
    args = parser.parse_args(argv)

    if args.years_back < 0 or args.years_forward < 0:
        print("error: --years-back and --years-forward must be >= 0", file=sys.stderr)
        return 2

    init_runtime_config(args.config)

    start, end = year_range(
        today=date.today(),
        years_back=args.years_back,
        years_forward=args.years_forward,
    )
    result = load_market_calendar(start=start, end=end)
    print("==> d_market_calendar snapshot loaded")
    print(f"    table:       {result['table']}")
    print(f"    year range:  {result['start']} .. {result['end']}")
    print(f"    rows:        {result['rows']}")
    print(f"    open_days:   {result['open_days']}")
    print(f"    closed_days: {result['closed_days']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
