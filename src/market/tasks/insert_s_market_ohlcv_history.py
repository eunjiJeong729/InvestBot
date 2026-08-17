"""태스크: insert_s_market_ohlcv_history."""

from __future__ import annotations

from datetime import date, datetime

from infra.db.rdbms.mysql import MySQLClient
from src.common.entity import SMarketOhlcv, SMarketOhlcvHistory
from src.common.utils.config import load_mysql_config
from src.common.utils.db_partition import (
    existing_partition_names,
    partition_name,
    partition_upper_bound,
)
from src.common.utils.sql_loader import format_sql
from src.common.utils.task_log import log_task_run

_TASK = "insert_s_market_ohlcv_history"
_SERVICE = "market"
_UPSERT_SQL_NAME = "insert_s_market_ohlcv_history"


def _coerce_date(raw: object) -> date | None:
    if isinstance(raw, date) and not isinstance(raw, datetime):
        return raw
    if isinstance(raw, datetime):
        return raw.date()
    if raw:
        return date.fromisoformat(str(raw)[:10])
    return None


def _snapshot_partition_dates(db: MySQLClient, conn: object) -> list[date]:
    rows = db.fetchall(
        f"""
        SELECT DISTINCT DATE(market_time) AS partition_date
        FROM {SMarketOhlcv.TABLE}
        ORDER BY partition_date
        """,
        conn=conn,
    )
    dates: list[date] = []
    for row in rows:
        parsed = _coerce_date(row.get("partition_date"))
        if parsed is not None:
            dates.append(parsed)
    return dates


def ensure_history_partitions(db: MySQLClient, conn: object, logger: object) -> list[str]:
    """``s_market_ohlcv`` 행에 필요한 일별 RANGE 파티션을 추가한다."""
    table = SMarketOhlcvHistory.TABLE
    existing = existing_partition_names(db, conn, table)
    added: list[str] = []
    for partition_date in _snapshot_partition_dates(db, conn):
        name = partition_name(partition_date)
        if name in existing:
            continue
        upper = partition_upper_bound(partition_date)
        db.execute(
            f"""
            ALTER TABLE {table}
            ADD PARTITION (
                PARTITION {name} VALUES LESS THAN ('{upper}')
            )
            """,
            conn=conn,
        )
        existing.add(name)
        added.append(name)
        logger.info(
            "added partition name=%s partition_date=%s upper_bound=%s",
            name,
            partition_date,
            upper,
        )
    return added


def run() -> int:
    """s_market_ohlcv 스냅샷 행을 s_market_ohlcv_history에 UPSERT한다."""
    with log_task_run(_TASK) as logger:
        db = MySQLClient(load_mysql_config())
        conn = db.connection()
        try:
            snapshot_count = db.fetchone(
                f"SELECT COUNT(*) AS c FROM {SMarketOhlcv.TABLE}",
                conn=conn,
            )
            logger.info("snapshot rows in %s=%s", SMarketOhlcv.TABLE, snapshot_count.get("c"))

            added_partitions = ensure_history_partitions(db, conn, logger)
            if not added_partitions:
                logger.info("partitions already exist for snapshot dates")

            hist = SMarketOhlcvHistory.TABLE
            staging = SMarketOhlcv.TABLE
            unchanged = f"""
                {hist}.open_price <=> VALUES(open_price)
                AND {hist}.high_price <=> VALUES(high_price)
                AND {hist}.low_price <=> VALUES(low_price)
                AND {hist}.close_price <=> VALUES(close_price)
                AND {hist}.volume <=> VALUES(volume)
            """
            rows = db.execute(
                format_sql(
                    _SERVICE,
                    _UPSERT_SQL_NAME,
                    hist=hist,
                    staging=staging,
                    unchanged=unchanged,
                ),
                conn=conn,
            )
            affected = int(rows.rowcount or 0)
            db.commit(conn)
            logger.info(
                "upserted history affected_rows=%d table=%s partitions_added=%d "
                "(rowcount 1=insert, 2=update)",
                affected,
                SMarketOhlcvHistory.TABLE,
                len(added_partitions),
            )
            return affected
        except Exception:
            db.rollback(conn)
            raise
        finally:
            conn.close()
