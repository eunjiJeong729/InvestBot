"""태스크: fetch_s_market_ohlcv."""

from __future__ import annotations

import logging
import time
from datetime import date, datetime, timedelta, tzinfo
from typing import Any
from zoneinfo import ZoneInfo

import pendulum

from infra.db.rdbms.mysql import MySQLClient
from src.common.kiwoom_api import (
    KiwoomApi,
    KiwoomCredentials,
    _sleep_remaining,
    normalize_market_time,
    parse_market_time,
    parse_number,
)
from src.common.entity import DMarketAssetMaster, SMarketOhlcv
from src.common.utils.config import load_market_settings, load_mysql_config
from src.common.utils.db_snapshot import replace_snapshot
from src.common.utils.task_log import log_task_run
from src.market.tasks import Task

SCHEDULE_OHLCV_BAR_COUNT = 7  # run 슬롯 + 이전 6개 5분봉 (예: 12:30 → 12:30~12:00)


def resolve_schedule_slot_kst(context: object | None = None) -> datetime:
    """DAG run 슬롯을 KST datetime으로 반환한다 (fetch task 수집 anchor).

    Airflow context의 logical_date를 직접 읽는다. 고정 간격 스케줄이므로
    물리 트리거 시각 = logical_date + airflow.dag.market.schedule_step_minutes.
    context 없음 (CLI 테스트 등): 현재 시각을 step 단위 floor.
    """
    market = load_market_settings()
    if isinstance(context, dict):
        raw = context.get("logical_date")
        if isinstance(raw, datetime):
            slot_dt = raw.replace(tzinfo=ZoneInfo("UTC")) if raw.tzinfo is None else raw
            return (
                pendulum.instance(slot_dt).in_timezone(market.timezone)
                + timedelta(minutes=market.schedule_step_minutes)
            ).replace(second=0, microsecond=0)

    now = datetime.now(market.timezone)
    floored = (now.minute // market.schedule_step_minutes) * market.schedule_step_minutes
    return now.replace(minute=floored, second=0, microsecond=0)


def target_schedule_market_times(
    schedule_slot_kst: datetime,
    *,
    bar_count: int = SCHEDULE_OHLCV_BAR_COUNT,
) -> list[datetime]:
    """run 슬롯 기준 수집 대상 분봉 시각 목록 (최신순).

    예: 12:30 run → [12:30, 12:25, 12:20, 12:15, 12:10, 12:05, 12:00]
    """
    market = load_market_settings()
    slot = normalize_market_time(schedule_slot_kst, market_tz=market.timezone)
    step = timedelta(minutes=market.schedule_step_minutes)
    return [slot - step * offset for offset in range(bar_count)]


def _fetch_minute_rows_with_retry(
    api: KiwoomApi, asset_code: str, *, base_dt: date
) -> tuple[list[dict[str, Any]], Exception | None]:
    max_retries = api.max_retries
    last_error: Exception | None = None

    for attempt in range(max_retries):
        attempt_started = time.perf_counter()
        try:
            return api.fetch_minute_ohlcv_rows(asset_code, base_dt=base_dt), None
        except Exception as exc:
            last_error = exc
            if attempt < max_retries - 1:
                _sleep_remaining(min(2**attempt, 10), attempt_started)

    return [], last_error


def _bucket_market_times(slot: datetime, *, bucket_minutes: int = 5) -> list[datetime]:
    """슬롯 종료 시각 기준 bucket_minutes 구간의 분 단위 시각 목록 (오름차순)."""
    return [slot - timedelta(minutes=offset) for offset in range(bucket_minutes - 1, -1, -1)]


def _aggregate_bucket_rows(bucket_rows: list[dict[str, Any]]) -> dict[str, float] | None:
    """시간순 정렬된 1분봉 row 리스트를 하나의 OHLCV 값으로 집계한다."""
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


class FetchSMarketOhlcv(Task):
    def __init__(self) -> None:
        super().__init__("fetch_s_market_ohlcv")

    def run(self, **context: object) -> int:
        """스케줄 슬롯 분봉을 자산별로 조회해 s_market_ohlcv를 덮어쓴다."""
        with log_task_run(self.name) as logger:
            market = load_market_settings()
            schedule_slot = resolve_schedule_slot_kst(context if context else None)
            target_times = target_schedule_market_times(schedule_slot)
            base_dt = schedule_slot.date()
            bar_count = SCHEDULE_OHLCV_BAR_COUNT

            logger.info(
                "schedule slot=%s target_bars=%d times=%s",
                schedule_slot.isoformat(),
                len(target_times),
                [slot.strftime("%H:%M") for slot in target_times],
            )

            db = MySQLClient(load_mysql_config())
            target_assets = db.fetchall(
                f"""
                SELECT asset_type, asset_code
                FROM {DMarketAssetMaster.TABLE}
                WHERE is_tradable = 1 AND is_target = 1
                ORDER BY asset_code ASC
                """
            )

            if not target_assets:
                raise RuntimeError("No target tradable assets found in d_market_asset_master")

            logger.info(
                "target assets=%d filter=is_tradable=1 AND is_target=1", len(target_assets)
            )

            api = KiwoomApi(KiwoomCredentials.from_env())
            now = datetime.now()
            entities: list[SMarketOhlcv] = []
            assets_with_rows = 0
            no_row = 0
            fetch_errors = 0
            fetch_error_samples: list[str] = []
            parse_errors = 0
            parse_error_samples: list[str] = []
            missing_slot_total = 0
            missing_slot_samples: list[str] = []

            for asset in target_assets:
                asset_code = str(asset.get("asset_code") or "").strip()
                asset_type = str(asset.get("asset_type") or "STOCK").strip() or "STOCK"
                if not asset_code:
                    continue

                rows, fetch_error = _fetch_minute_rows_with_retry(
                    api, asset_code, base_dt=base_dt
                )
                if fetch_error is not None:
                    fetch_errors += 1
                    if len(fetch_error_samples) < 5:
                        fetch_error_samples.append(f"{asset_code}: {fetch_error}")
                    if logger.isEnabledFor(logging.DEBUG):
                        logger.debug(
                            "skip fetch_error asset_code=%s error=%s", asset_code, fetch_error
                        )
                    continue

                if not rows:
                    no_row += 1
                    if logger.isEnabledFor(logging.DEBUG):
                        logger.debug("skip no_api_row asset_code=%s", asset_code)
                    continue

                matched_rows, missing_slots = _select_aggregated_bars_for_schedule_slots(
                    rows, target_times, market_tz=market.timezone
                )
                if missing_slots:
                    missing_slot_total += len(missing_slots)
                    if len(missing_slot_samples) < 5:
                        missing_slot_samples.append(
                            f"{asset_code}="
                            + ",".join(slot.strftime("%H:%M") for slot in missing_slots)
                        )

                if not matched_rows:
                    no_row += 1
                    continue

                assets_with_rows += 1
                for market_time, aggregated in matched_rows:
                    try:
                        entities.append(
                            _row_to_entity(
                                aggregated,
                                asset_type=asset_type,
                                asset_code=asset_code,
                                market_time=market_time,
                                now=now,
                            )
                        )
                    except ValueError as exc:
                        parse_errors += 1
                        if len(parse_error_samples) < 5:
                            parse_error_samples.append(
                                f"{asset_code}@{market_time.strftime('%H:%M')}: {exc}"
                            )
                        continue

            logger.info(
                "fetch summary schedule_slot=%s targets=%d assets_with_rows=%d bars=%d "
                "bars_per_asset=%d no_row=%d fetch_errors=%d parse_errors=%d missing_slots=%d",
                schedule_slot.strftime("%Y-%m-%d %H:%M"),
                len(target_assets),
                assets_with_rows,
                len(entities),
                bar_count,
                no_row,
                fetch_errors,
                parse_errors,
                missing_slot_total,
            )
            if fetch_error_samples:
                logger.warning("fetch error samples=%s", fetch_error_samples)
            if parse_error_samples:
                logger.warning("parse error samples=%s", parse_error_samples)
            if missing_slot_samples:
                logger.warning("missing slot samples=%s", missing_slot_samples)

            if not entities:
                raise RuntimeError("No OHLCV rows fetched from Kiwoom API")

            sample = entities[0]
            logger.info(
                "sample ohlcv asset_code=%s market_time=%s close=%s volume=%s",
                sample.asset_code,
                sample.market_time,
                sample.close_price,
                sample.volume,
            )

            replace_snapshot(
                db,
                table=SMarketOhlcv.TABLE,
                columns=(
                    "asset_type",
                    "asset_code",
                    "market_time",
                    "open_price",
                    "high_price",
                    "low_price",
                    "close_price",
                    "volume",
                    "created_at",
                ),
                rows=[
                    (
                        entity.asset_type,
                        entity.asset_code,
                        entity.market_time,
                        entity.open_price,
                        entity.high_price,
                        entity.low_price,
                        entity.close_price,
                        entity.volume,
                        entity.created_at,
                    )
                    for entity in entities
                ],
            )

            logger.info("loaded table=%s rows=%d", SMarketOhlcv.TABLE, len(entities))
            return len(entities)
