#!/usr/bin/env python3
"""devcontainer E2E: moto_server 대상 ``dag_dw_migration`` 태스크 검증.(AWS 이관 전까지 TEST TABLE만 처리)"""

from __future__ import annotations

import os
import sys
from datetime import date, datetime, timedelta, timezone

import pendulum

from infra.db.rdbms.mysql import MySQLClient
from infra.s3 import S3Client, S3Config
from src.common.entity import SMarketOhlcvHistory
from src.common.utils.config import (
    init_runtime_config,
    load_dag_dw_migration_settings,
    load_glue_config,
    load_mysql_config,
    load_s3_config,
)
from src.dw_migration.tasks.convert_s_market_ohlcv_history import ConvertSMarketOhlcvHistory
from src.dw_migration.tasks.extract_s_market_ohlcv_history import ExtractSMarketOhlcvHistory
from src.dw_migration.tasks.gate_dq_s_market_ohlcv_history import should_pass_dq_gate
from src.dw_migration.tasks.purge_s_market_ohlcv_history import PurgeSMarketOhlcvHistory
from src.dw_migration.tasks.register_glue_partition_s_market_ohlcv_history import (
    RegisterGluePartitionSMarketOhlcvHistory,
)
from src.dw_migration.tasks.upload_s_market_ohlcv_history import UploadSMarketOhlcvHistory

_CONFIG = os.environ.get("INVESTBOT_CONFIG", "configs/dev/e2e.json")
_PROD_TABLE = SMarketOhlcvHistory.TABLE


def _assert_e2e_source_table_safe() -> str:
    """AWS 이관 전까지 TEST TABLE만 처리: ``source_table``이 ``*_test``가 아니면 중단."""
    settings = load_dag_dw_migration_settings()
    source_table = settings.source_table
    if not source_table.endswith("_test"):
        print(
            f"[e2e] ABORT: dag_dw_migration.source_table={source_table!r} does not end with '_test'. "
            "Use INVESTBOT_CONFIG=configs/dev/e2e.json to avoid touching production history.",
            file=sys.stderr,
        )
        sys.exit(1)
    return source_table


def _resolve_moto_endpoint() -> str:
    override = os.environ.get("MOTO_ENDPOINT_URL", "").strip()
    if override:
        return override
    for url in ("http://moto:5000", "http://127.0.0.1:5000", "http://localhost:5000"):
        try:
            import urllib.request

            urllib.request.urlopen(url, timeout=1)
            return url
        except Exception:
            continue
    return "http://127.0.0.1:5000"


def _init_config_with_moto() -> None:
    """e2e.json을 복사해 S3/Glue endpoint를 moto로 주입한 임시 설정을 로드한다.

    임시 파일 경로: ``configs/dev/.{stem}.e2e.json`` (예: ``.e2e.e2e.json``).
    """
    import json

    from src.common.utils.config import resolve_repo_path

    path = resolve_repo_path(_CONFIG)
    config = json.loads(path.read_text(encoding="utf-8"))
    endpoint = _resolve_moto_endpoint()
    airflow = config.setdefault("airflow", {})
    dag = airflow.setdefault("dag", {})
    dw = dag.setdefault("dw_migration", {})
    dw.setdefault("s3", {})["endpoint_url"] = endpoint
    dw.setdefault("glue", {})["endpoint_url"] = endpoint
    tmp = path.parent / f".{path.stem}.e2e.json"
    env = config.setdefault("environment", {})
    env["INVESTBOT_CONFIG"] = str(tmp.relative_to(resolve_repo_path(".")))
    tmp.write_text(json.dumps(config, ensure_ascii=False, indent=2), encoding="utf-8")
    os.environ.pop("_INVESTBOT_RUNTIME_APPLIED", None)
    init_runtime_config(tmp)
    print(f"[e2e] moto endpoint={endpoint}")


def _bootstrap_moto() -> None:
    s3_cfg = load_s3_config()
    bucket = str(s3_cfg.get("bucket") or "")
    if not bucket:
        raise RuntimeError("S3 bucket is required in airflow.dag.dw_migration.s3 config")
    with S3Client(S3Config.from_mapping(s3_cfg)) as client:
        try:
            client.client.create_bucket(
                Bucket=bucket,
                CreateBucketConfiguration={"LocationConstraint": client.config.region},
            )
        except client.client.exceptions.BucketAlreadyOwnedByYou:
            pass
        except client.client.exceptions.BucketAlreadyExists:
            pass


def _prod_table_snapshot(db: MySQLClient) -> dict[str, int]:
    """실 테이블(``s_market_ohlcv_history``)의 파티션·행 수 스냅샷 — E2E 중 변경 여부 검증용."""
    partitions = db.fetchall(
        """
        SELECT PARTITION_NAME
        FROM information_schema.PARTITIONS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = %s
          AND PARTITION_NAME IS NOT NULL
        """,
        (_PROD_TABLE,),
    )
    row = db.fetchone(f"SELECT COUNT(*) AS c FROM {_PROD_TABLE}")
    return {
        "partition_count": len(partitions),
        "row_count": int(row.get("c") or 0),
    }


def _test_table_partition_dates(db: MySQLClient, source_table: str) -> list[date]:
    rows = db.fetchall(
        f"""
        SELECT DISTINCT partition_date
        FROM {source_table}
        ORDER BY partition_date
        """
    )
    dates: list[date] = []
    for row in rows:
        raw = row.get("partition_date")
        if isinstance(raw, datetime):
            dates.append(raw.date())
        elif isinstance(raw, date):
            dates.append(raw)
    return dates


def _target_logical_date(settings: object) -> tuple[datetime, date]:
    """시드 ``target_date``(오늘)와 동일한 logical_date."""
    target = datetime.now(settings.timezone).date()  
    slot_kst = pendulum.datetime(
        target.year, target.month, target.day, 15, 40, tz=settings.timezone
    )
    logical = slot_kst.astimezone(timezone.utc).replace(tzinfo=None)
    return logical, target


def _verify_purge_result(
    source_table: str,
    *,
    target_date: date,
    control_date: date,
) -> None:
    db = MySQLClient(load_mysql_config())
    remaining = _test_table_partition_dates(db, source_table)
    if target_date in remaining:
        raise RuntimeError(
            f"Purge verification failed: target partition still present {target_date}"
        )
    if control_date not in remaining:
        raise RuntimeError(
            f"Purge verification failed: control partition missing {control_date}"
        )
    print(
        f"[e2e] purge verified target_date={target_date} dropped "
        f"control_date={control_date} survived remaining_partitions={remaining}"
    )


def _run() -> None:
    # moto endpoint + 런타임 설정 로드
    _init_config_with_moto()
    source_table = _assert_e2e_source_table_safe()

    # 운영 테이블 스냅샷 — 파이프라인 전후 검증
    db = MySQLClient(load_mysql_config())
    prod_before = _prod_table_snapshot(db)
    print(f"[e2e] prod table snapshot before {_PROD_TABLE}: {prod_before}")

    _bootstrap_moto()

    settings = load_dag_dw_migration_settings()
    logical_date, partition_date = _target_logical_date(settings)
    control_date = partition_date - timedelta(days=3)
    context = {"logical_date": logical_date}
    print(
        f"[e2e] source_table={source_table} logical_date={logical_date.isoformat()}Z "
        f"partition_date={partition_date.isoformat()} control_date={control_date.isoformat()}"
    )

    extract = ExtractSMarketOhlcvHistory()
    convert = ConvertSMarketOhlcvHistory()
    upload = UploadSMarketOhlcvHistory()
    register = RegisterGluePartitionSMarketOhlcvHistory()
    purge = PurgeSMarketOhlcvHistory()

    tasks = [
        (
            "extract_s_market_ohlcv_history",
            lambda **ctx: extract.run(partition_date=partition_date, **ctx),
        ),
        (
            "gate_dq_s_market_ohlcv_history",
            lambda **ctx: 1
            if should_pass_dq_gate(partition_date=partition_date, **ctx)
            else 0,
        ),
        (
            "convert_s_market_ohlcv_history",
            lambda **ctx: convert.run(partition_date=partition_date, **ctx),
        ),
        (
            "upload_s_market_ohlcv_history",
            lambda **ctx: upload.run(partition_date=partition_date, **ctx),
        ),
        (
            "register_glue_partition_s_market_ohlcv_history",
            lambda **ctx: register.run(partition_date=partition_date, **ctx),
        ),
        ("purge_s_market_ohlcv_history", lambda **ctx: purge.run(partition_date=partition_date, **ctx)),
    ]
    for name, fn in tasks:
        result = fn(**context)
        print(f"[e2e] {name} ok result={result}")

    _verify_purge_result(source_table, target_date=partition_date, control_date=control_date)

    prod_after = _prod_table_snapshot(db)
    print(f"[e2e] prod table snapshot after {_PROD_TABLE}: {prod_after}")
    if prod_before != prod_after:
        raise RuntimeError(
            f"Production table {_PROD_TABLE} was modified during E2E: "
            f"before={prod_before} after={prod_after}"
        )

    s3_cfg = load_s3_config()
    bucket = str(s3_cfg["bucket"])
    with S3Client(S3Config.from_mapping(s3_cfg)) as s3:
        prefix = str(s3_cfg.get("prefix") or "").strip("/")
        listed = s3.client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        keys = [o["Key"] for o in listed.get("Contents", [])]
        print(f"[e2e] s3 objects under {prefix}/: {keys}")
        if not keys:
            raise RuntimeError("S3 upload verification failed: no objects")

    from infra.glue import GlueClient

    glue_cfg = load_glue_config()
    glue_mapping = {
        **glue_cfg,
        "database": glue_cfg.get("database"),
        "table": glue_cfg.get("table") or settings.table_name,
    }
    part_str = partition_date.isoformat()
    partition_values = [
        f"{partition_date.year:04d}",
        f"{partition_date.month:02d}",
        part_str,
    ]
    with GlueClient(glue_mapping) as glue:
        if not glue.partition_exists(partition_values):
            raise RuntimeError(f"Glue partition missing: partition_values={partition_values}")
    print(f"[e2e] glue partition exists partition_values={partition_values}")
    print("[e2e] dag_dw_migration pipeline OK")


if __name__ == "__main__":
    try:
        _run()
    except Exception as exc:
        print(f"[e2e] FAILED: {exc}", file=sys.stderr)
        raise
