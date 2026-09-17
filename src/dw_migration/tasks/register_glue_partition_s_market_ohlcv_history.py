"""태스크: register_glue_partition_s_market_ohlcv_history — Glue Catalog 파티션 등록."""

from __future__ import annotations

import json
from datetime import date

from infra.glue import GlueClient
from infra.s3 import S3Client, S3Config
from src.common.utils.airflow.config import load_dag_dw_migration_settings, load_glue_config, load_s3_config, repo_root
from src.common.utils.airflow.task_log import log_task_run
from src.dw_migration.tasks import Task

_SERVICE = "dw_migration"

_PARTITION_KEYS = [
    {"Name": "partition_year", "Type": "string"},
    {"Name": "partition_month", "Type": "string"},
    {"Name": "partition_date", "Type": "string"},
]

_GLUE_COLUMNS = [
    {"Name": "asset_type", "Type": "string"},
    {"Name": "asset_code", "Type": "string"},
    {"Name": "market_time", "Type": "timestamp"},
    {"Name": "open_price", "Type": "double"},
    {"Name": "high_price", "Type": "double"},
    {"Name": "low_price", "Type": "double"},
    {"Name": "close_price", "Type": "double"},
    {"Name": "volume", "Type": "bigint"},
    {"Name": "partition_hour", "Type": "int"},
    {"Name": "created_at", "Type": "timestamp"},
]


class RegisterGluePartitionSMarketOhlcvHistory(Task):
    def __init__(self) -> None:
        super().__init__("register_glue_partition_s_market_ohlcv_history")

    def run(self, *, partition_date: date, **context: object) -> int:
        settings = load_dag_dw_migration_settings()
        partition_str = partition_date.isoformat()
        year_str = f"{partition_date.year:04d}"
        month_str = f"{partition_date.month:02d}"
        partition_values = [year_str, month_str, partition_str]
        with log_task_run(
            self.name, service=_SERVICE, partition_date=partition_str
        ) as logger:
            run_dir = repo_root() / "data" / "dw_migration" / "runs" / partition_date.isoformat()
            manifest_file = run_dir / "manifest.json"
            if not manifest_file.is_file():
                raise FileNotFoundError(f"Missing manifest: {manifest_file}")
            manifest = json.loads(manifest_file.read_text(encoding="utf-8"))
            s3_key = str(manifest["s3_object_key"])

            s3_cfg = load_s3_config()
            glue_cfg = load_glue_config()
            bucket = str(s3_cfg.get("bucket") or "")
            if not bucket:
                raise ValueError("S3 bucket is required for Glue registration")

            s3_client = S3Client(S3Config.from_mapping(s3_cfg))
            object_key = s3_client.config.object_key(s3_key)
            location = f"s3://{bucket}/{object_key.rsplit('/', 1)[0]}/"

            table_location = f"{settings.table_name}/"
            table_location_uri = (
                f"s3://{bucket}/{s3_client.config.object_key(table_location)}"
            )

            glue_mapping = {
                **glue_cfg,
                "database": glue_cfg.get("database") or "dw_trading_bronze",
                "table": glue_cfg.get("table") or settings.table_name,
            }
            with GlueClient(glue_mapping) as glue:
                glue.ensure_table(
                    columns=_GLUE_COLUMNS,
                    s3_location=table_location_uri,
                    partition_keys=_PARTITION_KEYS,
                )
                if glue.partition_exists(partition_values):
                    logger.info("glue partition already exists partition_date=%s", partition_str)
                    return 0
                glue.batch_create_partition(partition_values=partition_values, s3_location=location)
            logger.info(
                "registered glue partition database=%s table=%s partition_date=%s location=%s",
                glue_mapping["database"],
                glue_mapping["table"],
                partition_str,
                location,
            )
            return 1
