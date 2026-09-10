#!/usr/bin/env python3
"""E2E용 ``s_market_ohlcv_history_test`` 테스트용 더미 데이터 생성."""

from __future__ import annotations

import os
import sys
from datetime import date, datetime, time, timedelta

from infra.db.rdbms.mysql import MySQLClient
from src.common.entity import SMarketOhlcvHistory
from src.common.utils.config import init_runtime_config, load_dag_dw_migration_settings, load_mysql_config
from src.common.utils.db_partition import (
    existing_partition_names,
    partition_name,
    partition_upper_bound,
)

_CONFIG = os.environ.get("INVESTBOT_CONFIG", "configs/dev/e2e.json")
_TEST_TABLE = "s_market_ohlcv_history_test"
_PROD_TABLE = SMarketOhlcvHistory.TABLE
_ASSETS = (
    ("STOCK", "005930"),
    ("STOCK", "000660"),
)
_SESSION_START = time(9, 0)
_SESSION_END = time(15, 30)
_BAR_MINUTES = 5


def _recreate_test_table(db: MySQLClient, conn: object) -> None:
    """``{_PROD_TABLE}`` 스키마를 복제한 뒤 파티션만 시드용으로 초기화한다."""
    db.execute(f"DROP TABLE IF EXISTS {_TEST_TABLE}", conn=conn)
    db.execute(
        f"CREATE TABLE {_TEST_TABLE} LIKE {_PROD_TABLE}",
        conn=conn,
    )
    db.execute(f"ALTER TABLE {_TEST_TABLE} REMOVE PARTITIONING", conn=conn)
    db.execute(
        f"""
        ALTER TABLE {_TEST_TABLE}
        PARTITION BY RANGE COLUMNS(partition_date) (
            PARTITION p_init VALUES LESS THAN ('2000-01-01')
        )
        """,
        conn=conn,
    )


def _assert_schema_matches_prod(db: MySQLClient, conn: object) -> None:
    """테스트 테이블 컬럼 정의가 ``{_PROD_TABLE}`` 와 동일한지 검증한다."""
    prod_cols = db.fetchall(f"DESCRIBE {_PROD_TABLE}", conn=conn)
    test_cols = db.fetchall(f"DESCRIBE {_TEST_TABLE}", conn=conn)
    if prod_cols != test_cols:
        raise RuntimeError(
            f"Schema drift: {_TEST_TABLE} columns do not match {_PROD_TABLE}. "
            f"prod={prod_cols} test={test_cols}"
        )


def _ensure_partition(db: MySQLClient, conn: object, partition_date: date) -> None:
    existing = existing_partition_names(db, conn, _TEST_TABLE)
    name = partition_name(partition_date)
    if name in existing:
        return
    upper = partition_upper_bound(partition_date)
    db.execute(
        f"""
        ALTER TABLE {_TEST_TABLE}
        ADD PARTITION (
            PARTITION {name} VALUES LESS THAN ('{upper}')
        )
        """,
        conn=conn,
    )


def _session_bar_times(partition_date: date) -> list[datetime]:
    start = datetime.combine(partition_date, _SESSION_START)
    end = datetime.combine(partition_date, _SESSION_END)
    step = timedelta(minutes=_BAR_MINUTES)
    times: list[datetime] = []
    cursor = start
    while cursor <= end:
        times.append(cursor)
        cursor += step
    return times


def _insert_rows(
    db: MySQLClient,
    conn: object,
    *,
    partition_date: date,
    base_price: float,
) -> int:
    # 가격은 실제 시세와 무관한 생성값
    rows = 0
    for asset_type, asset_code in _ASSETS:
        for market_time in _session_bar_times(partition_date):
            price = base_price + (market_time.hour * 10) + (market_time.minute * 0.1)
            db.execute(
                f"""
                INSERT INTO {_TEST_TABLE} (
                    asset_type, asset_code, open_price, high_price, low_price,
                    close_price, volume, market_time, partition_date,
                    partition_hour, created_at
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                """,
                (
                    asset_type,
                    asset_code,
                    price,
                    price + 1,
                    price - 1,
                    price,
                    1000 + market_time.minute,
                    market_time,
                    partition_date,
                    market_time.hour,
                    datetime.now(),
                ),
                conn=conn,
            )
            rows += 1
    return rows


def seed() -> None:
    init_runtime_config(_CONFIG)
    settings = load_dag_dw_migration_settings()
    if settings.source_table != _TEST_TABLE:
        raise RuntimeError(
            f"Refusing to seed: source_table={settings.source_table!r} "
            f"(expected {_TEST_TABLE!r}). Use INVESTBOT_CONFIG=configs/dev/e2e.json"
        )

    now = datetime.now(settings.timezone)
    target_date = now.date()
    untouched_date = target_date - timedelta(days=3)

    db = MySQLClient(load_mysql_config())
    conn = db.connection()
    try:
        _recreate_test_table(db, conn)

        for part_date in sorted({target_date, untouched_date}):
            _ensure_partition(db, conn, part_date)

        target_rows = _insert_rows(
            db, conn, partition_date=target_date, base_price=50_000
        )
        control_rows = _insert_rows(
            db, conn, partition_date=untouched_date, base_price=10_000
        )

        db.commit(conn)
        print(f"[seed] table={_TEST_TABLE} target_date={target_date}")
        print(
            f"[seed] target partition_date={target_date} rows={target_rows} "
            f"(should be dropped after S3 migration)"
        )
        print(
            f"[seed] control partition_date={untouched_date} rows={control_rows} "
            f"(should survive purge — control)"
        )
        print(f"[seed] total inserted={target_rows + control_rows}")
        _assert_schema_matches_prod(db, conn)
        print(f"[seed] schema verified: {_TEST_TABLE} columns match {_PROD_TABLE}")
    except Exception:
        db.rollback(conn)
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    try:
        seed()
    except Exception as exc:
        print(f"[seed] FAILED: {exc}", file=sys.stderr)
        raise
