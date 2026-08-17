"""OHLCV 5분 버킷 집계 순수 함수 (I/O 의존성 없음)."""

from __future__ import annotations

from datetime import datetime, timedelta, tzinfo
from typing import Any

from src.common.entity import SMarketOhlcv

from src.market.tasks._kiwoom_api import normalize_market_time, parse_market_time, parse_number


def _bucket_market_times(slot: datetime, *, bucket_minutes: int = 5) -> list[datetime]:
    """슬롯 종료 시각 기준 bucket_minutes 구간의 분 단위 시각 목록 (오름차순).

    예: slot=14:00, bucket_minutes=5 → [13:56, 13:57, 13:58, 13:59, 14:00]
    """
    return [slot - timedelta(minutes=offset) for offset in range(bucket_minutes - 1, -1, -1)]


def _aggregate_bucket_rows(bucket_rows: list[dict[str, Any]]) -> dict[str, float] | None:
    """시간순 정렬된 1분봉 row 리스트를 하나의 OHLCV 값으로 집계.

    구간 내 일부 분봉이 없어도(무거래) 존재하는 row만으로 집계하며 별도 처리하지 않는다.
    - open: 첫 번째 row의 시가
    - high: 전체 row 중 최고가
    - low: 전체 row 중 최저가
    - close: 마지막 row의 종가
    - volume: 전체 row 거래량 합산

    빈 리스트면 None 반환 (해당 슬롯은 missing 처리).
    """
    if not bucket_rows:
        return None
    return {
        "open": parse_number(bucket_rows[0], "open", "open_pric", "opn_prc", "stck_oprc", "cur_prc"),
        "high": max(
            parse_number(r, "high", "high_pric", "hg_prc", "stck_hgpr", "cur_prc")
            for r in bucket_rows
        ),
        "low": min(
            parse_number(r, "low", "low_pric", "lw_prc", "stck_lwpr", "cur_prc")
            for r in bucket_rows
        ),
        "close": parse_number(
            bucket_rows[-1], "close", "close_pric", "cls_prc", "cur_prc", "stck_clpr"
        ),
        "volume": sum(
            parse_number(r, "volume", "trde_qty", "acml_vol", "cntr_qty", "acc_trde_qty")
            for r in bucket_rows
        ),
    }


def _index_rows_by_market_time(
    rows: list[dict[str, Any]], *, market_tz: tzinfo
) -> dict[datetime, dict[str, Any]]:
    indexed: dict[datetime, dict[str, Any]] = {}
    for row in rows:
        market_time = parse_market_time(row)
        if market_time is None:
            continue
        indexed.setdefault(normalize_market_time(market_time, market_tz=market_tz), row)
    return indexed


def _select_aggregated_bars_for_schedule_slots(
    rows: list[dict[str, Any]],
    target_times: list[datetime],
    *,
    market_tz: tzinfo,
) -> tuple[list[tuple[datetime, dict[str, float]]], list[datetime]]:
    """API 1분봉 rows를 target_times(스케줄 슬롯) 기준 5분 버킷으로 집계한다."""
    indexed = _index_rows_by_market_time(rows, market_tz=market_tz)
    selected: list[tuple[datetime, dict[str, float]]] = []
    missing: list[datetime] = []
    for target in target_times:
        bucket = _bucket_market_times(target)
        bucket_rows = [indexed[t] for t in bucket if t in indexed]
        aggregated = _aggregate_bucket_rows(bucket_rows)
        if aggregated is None:
            missing.append(target)
            continue
        selected.append((target, aggregated))
    return selected, missing


def _row_to_entity(
    aggregated: dict[str, float],
    *,
    asset_type: str,
    asset_code: str,
    market_time: datetime,
    now: datetime,
) -> SMarketOhlcv:
    return SMarketOhlcv(
        asset_type=asset_type,
        asset_code=asset_code,
        market_time=market_time,
        open_price=aggregated["open"],
        high_price=aggregated["high"],
        low_price=aggregated["low"],
        close_price=aggregated["close"],
        volume=int(aggregated["volume"]),
        created_at=now,
    )
