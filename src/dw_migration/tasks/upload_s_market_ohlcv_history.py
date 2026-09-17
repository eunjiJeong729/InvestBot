"""태스크: upload_s_market_ohlcv_history — Parquet 1일 1 PUT."""

from __future__ import annotations

import json
from datetime import date

from infra.s3 import S3Client, upload_file
from src.common.utils.airflow.config import load_dag_dw_migration_settings, load_s3_config, repo_root
from src.common.utils.airflow.task_log import log_task_run
from src.dw_migration.tasks import Task

_SERVICE = "dw_migration"


class UploadSMarketOhlcvHistory(Task):
    def __init__(self) -> None:
        super().__init__("upload_s_market_ohlcv_history")

    def run(self, *, partition_date: date, **context: object) -> int:
        settings = load_dag_dw_migration_settings()
        with log_task_run(
            self.name, service=_SERVICE, partition_date=str(partition_date)
        ) as logger:
            run_dir = repo_root() / "data" / "dw_migration" / "runs" / partition_date.isoformat()
            parquet = run_dir / "data.parquet"
            if not parquet.is_file():
                raise FileNotFoundError(f"Missing parquet for upload: {parquet}")

            manifest_file = run_dir / "manifest.json"
            if manifest_file.is_file():
                manifest = json.loads(manifest_file.read_text(encoding="utf-8"))
                key = str(manifest["s3_object_key"])
            else:
                key = (
                    f"{settings.table_name}/"
                    f"partition_year={partition_date.year:04d}/"
                    f"partition_month={partition_date.month:02d}/"
                    f"partition_date={partition_date.isoformat()}/"
                    f"{settings.s3_object_name}"
                )

            with S3Client(load_s3_config()) as client:
                object_key = upload_file(
                    client,
                    key,
                    parquet,
                    content_type="application/octet-stream",
                )
            bytes_uploaded = parquet.stat().st_size
            logger.info("uploaded s3_key=%s bytes=%d", object_key, bytes_uploaded)

            for local_parquet in (run_dir / "extracted.parquet", run_dir / "data.parquet"):
                if local_parquet.is_file():
                    local_parquet.unlink()
                    logger.info("removed local parquet path=%s", local_parquet)
            return bytes_uploaded
