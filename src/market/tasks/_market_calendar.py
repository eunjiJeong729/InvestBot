"""XKRX 거래일 캘린더 계산/적재 헬퍼.

Airflow task는 아니고, 연 1회 혹은 공휴일 정보 업데이트 시
수기로 작업하는 scripts용 task다. ``scripts/load_d_market_calendar.py`` 가 호출한다.
``dag_market`` 게이트는 적재된 테이블만 조회한다.
"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta
from typing import Any

from infra.db.rdbms.mysql import MySQLClient
from src.common.entity import DMarketCalendar
from src.common.utils.config import load_mysql_config
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
    """XKRX 세션을 ``start``~``end`` 날짜 전부에 대해 row로 만든다.

    휴장일(주말·공휴일·연말 폐장)은 ``is_market_open=0``, ``market_open_time=NULL``.
    거래일은 ``session_open``을 KST wall-clock TIME으로 넣는다 (연초 10:00 등).
    """
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


def fetch_is_market_open(market_date: date) -> bool | None:
    """``d_market_calendar``의 당일 개장 여부. 행이 없으면 ``None``."""
    row = MySQLClient(load_mysql_config()).fetchone(
        f"""
        SELECT is_market_open
        FROM {DMarketCalendar.TABLE}
        WHERE market_date = %s
        """,
        (market_date,),
    )
    if row is None:
        return None
    return bool(row.get("is_market_open"))
