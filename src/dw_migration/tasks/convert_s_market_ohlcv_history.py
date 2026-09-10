"""태스크: convert_s_market_ohlcv_history — pyarrow 스키마 정규화."""

from __future__ import annotations

import json
from datetime import date

import pandas as pd

from src.common.utils.config import load_dag_dw_migration_settings, repo_root
from src.common.utils.task_log import log_task_run
from src.dw_migration.tasks import Task

_SERVICE = "dw_migration"


class ConvertSMarketOhlcvHistory(Task):
    def __init__(self) -> None:
        super().__init__("convert_s_market_ohlcv_history")

    def run(self, *, partition_date: date, **context: object) -> int:
        settings = load_dag_dw_migration_settings()
        with log_task_run(
            self.name, service=_SERVICE, partition_date=str(partition_date)
        ) as logger:
            run_dir = repo_root() / "data" / "dw_migration" / "runs" / partition_date.isoformat()
            run_dir.mkdir(parents=True, exist_ok=True)
            src = run_dir / "extracted.parquet"
            if not src.is_file():
                raise FileNotFoundError(f"Missing extracted parquet: {src}")

            df = pd.read_parquet(src)
            df["market_time"] = pd.to_datetime(df["market_time"])
            if "created_at" in df.columns:
                df["created_at"] = pd.to_datetime(df["created_at"])
            if "partition_date" not in df.columns:
                df["partition_date"] = partition_date
            else:
                df["partition_date"] = pd.to_datetime(df["partition_date"]).dt.date

            for col in ("open_price", "high_price", "low_price", "close_price"):
                df[col] = df[col].astype("float64")
            df["volume"] = df["volume"].astype("int64")
            df["partition_hour"] = df["partition_hour"].astype("int32")

            dest = run_dir / "data.parquet"
            df.to_parquet(dest, index=False, engine="pyarrow")

            key = (
                f"{settings.table_name}/"
                f"partition_year={partition_date.year:04d}/"
                f"partition_month={partition_date.month:02d}/"
                f"partition_date={partition_date.isoformat()}/"
                f"{settings.s3_object_name}"
            )
            manifest = {
                "partition_date": partition_date.isoformat(),
                "rows": len(df),
                "parquet_path": str(dest),
                "s3_object_key": key,
            }
            (run_dir / "manifest.json").write_text(
                json.dumps(manifest, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            logger.info("converted rows=%d path=%s s3_key=%s", len(df), dest, key)
            return len(df)
