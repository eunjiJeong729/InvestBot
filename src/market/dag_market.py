"""[서비스 1] 마켓 데이터 공급 — Airflow DAG."""

from __future__ import annotations

import os
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo

import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator

from infra.db.rdbms.mysql import MySQLClient
from src.common.entity import DMarketAssetMaster
from src.common.utils.config import init_runtime_config, load_market_settings, load_mysql_config
from src.common.utils.task_log import log_gate_decision
from src.market.tasks._market_calendar import fetch_is_market_open
from src.market.tasks.fetch_d_market_asset_master import run as fetch_d_market_asset_master
from src.market.tasks.fetch_s_market_ohlcv import run as fetch_s_market_ohlcv
from src.market.tasks.insert_s_market_ohlcv_history import run as insert_s_market_ohlcv_history

# DAG import 시점에 런타임 config를 env에 반영한다 (INVESTBOT_CONFIG 필수).
_CONFIG_PATH = os.environ.get("INVESTBOT_CONFIG", "").strip()
if not _CONFIG_PATH:
    raise RuntimeError(
        "INVESTBOT_CONFIG is required to load dag_market "
        "(e.g. export INVESTBOT_CONFIG=configs/dev/debug.json)"
    )
init_runtime_config(_CONFIG_PATH)

# --- 스케줄 / 장 운영 시간 상수 ---
# timezone / schedule_step_minutes 는 configs 의 airflow.environment / airflow.dag.market 에서 로드
_MARKET = load_market_settings()
_MARKET_TZ = _MARKET.timezone
_SCHEDULE_STEP_MINUTES = _MARKET.schedule_step_minutes
_DAG_SCHEDULE = "*/5 8-15 * * 1-5"  # 평일 08:00~16:00, 5분 슬롯 (물리 트리거 시각 기준)
_OHLCV_START = time(9, 0)  # OHLCV 수집 시작
_OHLCV_END = time(15, 30)  # OHLCV 수집 종료
_OHLCV_SKIP_START = time(15, 21)  # 장 마감 동호가 구간 — 신규 5분봉 없음
_OHLCV_SKIP_END = time(15, 29)
_ASSET_MASTER_RUN = time(8, 10)  # 종목 마스터 정규 갱신 시각
_ASSET_MASTER_RETRY_UNTIL = time(8, 55)  # 08:10 run 누락 시 09:00 OHLCV 전까지 재시도


def _slot_from_context(context: object) -> tuple[datetime, time] | None:
    """Airflow task context에서 물리적 트리거 시각 기준 슬롯을 추출한다.

    슬롯 = data_interval_end(물리 실행 시각) 기준이다. */5 고정 간격 스케줄이므로
    data_interval_end = logical_date + 스케줄 간격(_SCHEDULE_STEP_MINUTES)과 동일하다.
    Airflow UI의 logical_date 라벨과 실제 슬롯 값은 스케줄 간격만큼 차이가 난다
    (예: UI 13:55 run → slot 14:00).
    """
    if not isinstance(context, dict):
        return None

    raw = context.get("logical_date")
    if not isinstance(raw, datetime):
        return None

    # naive는 UTC로 간주한다. pendulum.DateTime.astimezone(ZoneInfo) 후 timedelta를
    # 더하면 tz가 사라지므로 pendulum.instance로 맞춘 뒤 in_timezone만 사용한다.
    slot_dt = raw.replace(tzinfo=ZoneInfo("UTC")) if raw.tzinfo is None else raw
    slot_kst = (
        pendulum.instance(slot_dt).in_timezone(_MARKET_TZ) + timedelta(minutes=_SCHEDULE_STEP_MINUTES)
    ).replace(second=0, microsecond=0)
    slot_time = slot_kst.time().replace(second=0, microsecond=0)
    return slot_kst, slot_time


def _asset_master_last_updated() -> datetime | None:
    """d_market_asset_master 테이블의 MAX(updated_at). 당일 갱신 여부 판단에 사용."""
    row = MySQLClient(load_mysql_config()).fetchone(
        f"SELECT MAX(updated_at) AS max_updated_at FROM {DMarketAssetMaster.TABLE}"
    )
    if not row:
        return None
    value = row.get("max_updated_at")
    return value if isinstance(value, datetime) else None


def _reject_non_trading_day(gate_name: str, slot_kst: datetime) -> bool:
    """``d_market_calendar`` 기준 비거래일이면 skip 로그를 남기고 True를 반환한다.

    주말·공휴일·연말 폐장을 ``not_a_trading_day``로 통일한다.
    캘린더 행이 없으면 ``missing_calendar_row`` (재적재 필요).
    """
    market_date = slot_kst.date()
    is_open = fetch_is_market_open(market_date)
    if is_open is None:
        log_gate_decision(
            gate_name,
            allowed=False,
            reason="missing_calendar_row",
            logical_date=str(slot_kst),
            market_date=str(market_date),
        )
        return True
    if not is_open:
        log_gate_decision(
            gate_name,
            allowed=False,
            reason="not_a_trading_day",
            logical_date=str(slot_kst),
            market_date=str(market_date),
        )
        return True
    return False


def should_run_asset_master(**context: object) -> bool:
    """gate_fetch_d_market_asset_master — 종목 마스터 수집 여부.

    | 슬롯 (KST) | 동작 |
    | 비거래일 | skip (``d_market_calendar.is_market_open=0``) |
    | 08:10 | 정규 실행 (항상) |
    | 08:15~08:55 | 당일 미갱신 시에만 재시도 |
    | 그 외 | skip |

    ShortCircuitOperator: False 반환 시 downstream(fetch) skip.
    log_gate_decision: 판단 이유(reason)를 Airflow task 로그에 남겨 skip 원인 추적.
    """
    slot = _slot_from_context(context)
    if slot is None:
        # logical_date 없음 → 슬롯을 알 수 없어 skip (로그에 reason 기록)
        log_gate_decision(
            "gate_fetch_d_market_asset_master",
            allowed=False,
            reason="missing_schedule_slot",
            logical_date=str(context.get("logical_date")),
        )
        return False

    slot_kst, slot_time = slot

    if _reject_non_trading_day("gate_fetch_d_market_asset_master", slot_kst):
        return False

    # 당일 00:00 이후 갱신됐는지 확인
    start_of_day = slot_kst.replace(hour=0, minute=0, second=0, microsecond=0).replace(tzinfo=None)
    last_updated = _asset_master_last_updated()
    updated_today = last_updated is not None and last_updated >= start_of_day

    if slot_time == _ASSET_MASTER_RUN:
        allowed, reason = True, "asset_master_schedule_0810"
    elif _ASSET_MASTER_RUN < slot_time <= _ASSET_MASTER_RETRY_UNTIL:
        allowed = not updated_today
        reason = (
            "asset_master_retry_before_0900"
            if allowed
            else "asset_master_already_updated_today"
        )
    else:
        allowed, reason = False, "outside_asset_master_window"

    # 최종 판단(allowed/reason)을 task 로그에 남긴 뒤 return
    log_gate_decision(
        "gate_fetch_d_market_asset_master",
        allowed=allowed,
        reason=reason,
        logical_date=str(slot_kst),
        schedule_slot_time=str(slot_time),
        asset_master_updated_today=updated_today,
    )
    return allowed


def should_run_market_ohlcv(**context: object) -> bool:
    """gate_market_window — OHLCV 수집 시간대 여부.

    | 조건 | 동작 |
    | 비거래일 | skip (``d_market_calendar``) |
    | 거래일 09:00~15:30 | 허용 |
    | 15:21~15:29 | skip (동호가 구간, 신규 5분봉 없음) |
    | 장외 | skip |

    log_gate_decision: skip/허용 이유를 Airflow task 로그에 기록.
    """
    slot = _slot_from_context(context)
    if slot is None:
        log_gate_decision(
            "gate_market_window",
            allowed=False,
            reason="missing_schedule_slot",
            logical_date=str(context.get("logical_date")),
        )
        return False

    slot_kst, slot_time = slot

    if _reject_non_trading_day("gate_market_window", slot_kst):
        return False

    if not (_OHLCV_START <= slot_time <= _OHLCV_END):
        log_gate_decision(
            "gate_market_window",
            allowed=False,
            reason="outside_market_window",
            logical_date=str(slot_kst),
            schedule_slot_time=str(slot_time),
        )
        return False

    if _OHLCV_SKIP_START <= slot_time <= _OHLCV_SKIP_END:
        log_gate_decision(
            "gate_market_window",
            allowed=False,
            reason="closing_duplicate_window_1521_1529",
            logical_date=str(slot_kst),
            schedule_slot_time=str(slot_time),
        )
        return False

    log_gate_decision(
        "gate_market_window",
        allowed=True,
        reason="market_window",
        logical_date=str(slot_kst),
        schedule_slot_time=str(slot_time),
    )
    return True


def is_asset_master_ready_today(**context: object) -> bool:
    """gate_asset_master_ready — OHLCV fetch 전 종목 마스터 당일 갱신 확인.

    09:00 이전 슬롯이거나, d_market_asset_master가 당일 갱신되지 않았으면 skip.
    log_gate_decision: 준비 여부/미갱신 이유를 Airflow task 로그에 기록.
    """
    slot = _slot_from_context(context)
    if slot is None:
        log_gate_decision(
            "gate_asset_master_ready",
            allowed=False,
            reason="missing_schedule_slot",
            logical_date=str(context.get("logical_date")),
        )
        return False

    slot_kst, slot_time = slot

    if slot_time < _OHLCV_START:
        log_gate_decision(
            "gate_asset_master_ready",
            allowed=False,
            reason="before_ohlcv_window_0900",
            logical_date=str(slot_kst),
            schedule_slot_time=str(slot_time),
        )
        return False

    start_of_day = slot_kst.replace(hour=0, minute=0, second=0, microsecond=0).replace(tzinfo=None)
    last_updated = _asset_master_last_updated()
    if last_updated is None:
        log_gate_decision(
            "gate_asset_master_ready",
            allowed=False,
            reason="no_asset_master_rows",
            logical_date=str(slot_kst),
            start_of_day=str(start_of_day),
        )
        return False

    ready = last_updated >= start_of_day
    log_gate_decision(
        "gate_asset_master_ready",
        allowed=ready,
        reason="asset_master_updated_today" if ready else "asset_master_stale",
        logical_date=str(slot_kst),
        max_updated_at=str(last_updated),
        start_of_day=str(start_of_day),
    )
    return ready


default_args = {
    "owner": "market",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

# --- DAG 정의 ---
# asset master: gate → fetch (독립 브랜치, 08:10~08:55)
# OHLCV: gate_market_window → gate_asset_master_ready → fetch → history (09:00~15:30)
with DAG(
    dag_id="dag_market",
    description="종목 마스터 + 장중 5분봉 OHLCV 수집",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=_DAG_SCHEDULE,
    catchup=False,  # 과거 run 일괄 backfill 비활성 — sliding window로 갭 보완
    tags=["market"],
) as dag:
    gate_fetch_d_market_asset_master = ShortCircuitOperator(
        task_id="gate_fetch_d_market_asset_master",
        python_callable=should_run_asset_master,
    )
    fetch_d_market_asset_master_task = PythonOperator(
        task_id="fetch_d_market_asset_master",
        python_callable=fetch_d_market_asset_master,
    )

    gate_market_window = ShortCircuitOperator(
        task_id="gate_market_window",
        python_callable=should_run_market_ohlcv,
    )
    gate_asset_master_ready = ShortCircuitOperator(
        task_id="gate_asset_master_ready",
        python_callable=is_asset_master_ready_today,
    )
    fetch_s_market_ohlcv_task = PythonOperator(
        task_id="fetch_s_market_ohlcv",
        python_callable=fetch_s_market_ohlcv,
    )
    insert_s_market_ohlcv_history_task = PythonOperator(
        task_id="insert_s_market_ohlcv_history",
        python_callable=insert_s_market_ohlcv_history,
    )

    gate_fetch_d_market_asset_master >> fetch_d_market_asset_master_task
    gate_market_window >> gate_asset_master_ready >> fetch_s_market_ohlcv_task
    fetch_s_market_ohlcv_task >> insert_s_market_ohlcv_history_task
