"""태스크: extract_s_market_ohlcv_history — MySQL history 추출."""

from __future__ import annotations

from datetime import date

import pandas as pd

from infra.db.rdbms.mysql import MySQLClient
from src.common.utils.config import load_dag_dw_migration_settings, load_mysql_config, repo_root
from src.common.utils.sql_loader import format_sql
from src.common.utils.task_log import log_task_run
from src.dw_migration.tasks import Task

_SERVICE = "dw_migration"
_SQL_NAME = "extract_s_market_ohlcv_history"


class ExtractSMarketOhlcvHistory(Task):
    def __init__(self) -> None:
        super().__init__("extract_s_market_ohlcv_history")

    def run(self, *, partition_date: date, **context: object) -> int:
        settings = load_dag_dw_migration_settings()
        source_table = settings.source_table
        with log_task_run(
            self.name, service=_SERVICE, partition_date=str(partition_date), source_table=source_table
        ) as logger:
            run_dir = repo_root() / "data" / "dw_migration" / "runs" / partition_date.isoformat()
            run_dir.mkdir(parents=True, exist_ok=True)
            out = run_dir / "extracted.parquet"

            db = MySQLClient(load_mysql_config())
            conn = db.connection()
            try:
                query = format_sql(_SERVICE, _SQL_NAME, table=source_table)
                df = pd.read_sql(query, conn, params=(partition_date,))
                if df.empty:
                    raise RuntimeError(
                        f"No rows in {source_table} for partition_date={partition_date}"
                    )
                df.to_parquet(out, index=False)
                logger.info("extracted rows=%d path=%s", len(df), out)
                return len(df)
            finally:
                conn.close()
