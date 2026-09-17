"""[서비스 3] 데이터 웨어하우스 이관 — Airflow DAG."""

from __future__ import annotations

import os
from datetime import date, datetime, timedelta
from functools import partial

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator

from infra.db.rdbms.mysql import MySQLClient
from src.common.utils.airflow.config import (
    init_runtime_config,
    load_dag_dw_migration_settings,
    load_mysql_config,
)
from src.common.utils.airflow.date import context_logical_date_in_timezone
from src.common.utils.airflow.task_log import log_gate_decision
from src.dw_migration.tasks.convert_s_market_ohlcv_history import ConvertSMarketOhlcvHistory
from src.dw_migration.tasks.extract_s_market_ohlcv_history import ExtractSMarketOhlcvHistory
from src.dw_migration.tasks.gate_dq_s_market_ohlcv_history import should_pass_dq_gate
from src.dw_migration.tasks.purge_s_market_ohlcv_history import PurgeSMarketOhlcvHistory
from src.dw_migration.tasks.register_glue_partition_s_market_ohlcv_history import (
    RegisterGluePartitionSMarketOhlcvHistory,
)
from src.dw_migration.tasks.upload_s_market_ohlcv_history import UploadSMarketOhlcvHistory

_CONFIG_PATH = os.environ.get("INVESTBOT_CONFIG", "").strip()
if not _CONFIG_PATH:
    raise RuntimeError(
        "INVESTBOT_CONFIG is required to load dag_dw_migration "
        "(e.g. export INVESTBOT_CONFIG=configs/dev/debug.json)"
    )
init_runtime_config(_CONFIG_PATH)

_DW = load_dag_dw_migration_settings()
_DAG_SCHEDULE = "40 15 * * 1-5"  # 평일 15:40 KST — 장 마감 직후 DW 이관
_SERVICE = "dw_migration"


def _run_with_partition_date(dw_task: object, **context: object) -> object:
    slot = context_logical_date_in_timezone(context, _DW.timezone)
    partition_date = (slot or datetime.now(_DW.timezone)).date()
    return dw_task.run(partition_date=partition_date, **context)  # type: ignore[attr-defined]


def _run_gate_with_partition_date(gate_fn: object, **context: object) -> object:
    slot = context_logical_date_in_timezone(context, _DW.timezone)
    partition_date = (slot or datetime.now(_DW.timezone)).date()
    return gate_fn(partition_date=partition_date, **context)  # type: ignore[operator]


def _is_market_open(market_date: date) -> bool | None:
    row = MySQLClient(load_mysql_config()).fetchone(
        """
        SELECT is_market_open
        FROM d_market_calendar
        WHERE market_date = %s
        """,
        (market_date,),
    )
    if row is None:
        return None
    return bool(row.get("is_market_open"))


def should_run_dw_migration(**context: object) -> bool:
    """gate_trading_day — 거래일에만 DW 이관 실행."""
    slot_kst = context_logical_date_in_timezone(context, _DW.timezone)
    if slot_kst is None:
        log_gate_decision(
            "gate_trading_day",
            allowed=False,
            reason="missing_logical_date",
            service=_SERVICE,
        )
        return False

    market_date = slot_kst.date()
    is_open = _is_market_open(market_date)
    if is_open is None:
        log_gate_decision(
            "gate_trading_day",
            allowed=False,
            reason="missing_calendar_row",
            service=_SERVICE,
            market_date=str(market_date),
        )
        return False
    if not is_open:
        log_gate_decision(
            "gate_trading_day",
            allowed=False,
            reason="not_a_trading_day",
            service=_SERVICE,
            market_date=str(market_date),
        )
        return False

    log_gate_decision(
        "gate_trading_day",
        allowed=True,
        reason="trading_day",
        service=_SERVICE,
        market_date=str(market_date),
    )
    return True


def should_register_glue_partition(**context: object) -> bool:
    """gate_glue_registration_s_market_ohlcv_history — config로 Glue 등록 단계 on/off."""
    enabled = _DW.run_glue_registration
    log_gate_decision(
        "gate_glue_registration_s_market_ohlcv_history",
        allowed=enabled,
        reason="enabled" if enabled else "glue_registration_disabled_by_config",
        service=_SERVICE,
    )
    return enabled


default_args = {
    "owner": "dw_migration",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dag_dw_migration",
    description="MySQL history → DQ Gate(pandas) → S3 Bronze → Glue 파티션 → MySQL Purge",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=_DAG_SCHEDULE,
    catchup=False,
    tags=["dw_migration"],
) as dag:
    extract_ohlcv = ExtractSMarketOhlcvHistory()
    convert_parquet = ConvertSMarketOhlcvHistory()
    upload_s3 = UploadSMarketOhlcvHistory()
    register_glue_partition = RegisterGluePartitionSMarketOhlcvHistory()
    purge_mysql = PurgeSMarketOhlcvHistory()

    gate_dw_migration = ShortCircuitOperator(
        task_id="gate_trading_day",
        python_callable=should_run_dw_migration,
    )
    extract_ohlcv_task = PythonOperator(
        task_id="extract_s_market_ohlcv_history",
        python_callable=partial(_run_with_partition_date, extract_ohlcv),
    )
    gate_dq = ShortCircuitOperator(
        task_id="gate_dq_s_market_ohlcv_history",
        python_callable=partial(_run_gate_with_partition_date, should_pass_dq_gate),
    )
    convert_parquet_task = PythonOperator(
        task_id="convert_s_market_ohlcv_history",
        python_callable=partial(_run_with_partition_date, convert_parquet),
    )
    upload_s3_task = PythonOperator(
        task_id="upload_s_market_ohlcv_history",
        python_callable=partial(_run_with_partition_date, upload_s3),
    )
    gate_glue_registration = ShortCircuitOperator(
        task_id="gate_glue_registration_s_market_ohlcv_history",
        python_callable=should_register_glue_partition,
    )
    register_glue_partition_task = PythonOperator(
        task_id="register_glue_partition_s_market_ohlcv_history",
        python_callable=partial(_run_with_partition_date, register_glue_partition),
    )
    purge_mysql_task = PythonOperator(
        task_id="purge_s_market_ohlcv_history",
        python_callable=partial(_run_with_partition_date, purge_mysql),
    )

    (
        gate_dw_migration
        >> extract_ohlcv_task
        >> gate_dq
        >> convert_parquet_task
        >> upload_s3_task
    )
    upload_s3_task >> gate_glue_registration >> register_glue_partition_task
    upload_s3_task >> purge_mysql_task
