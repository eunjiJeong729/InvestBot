"""태스크: purge_s_market_ohlcv_history — 해당 partition_date 파티션 DROP."""

from __future__ import annotations

import shutil
from datetime import date

from infra.db.rdbms.mysql import MySQLClient
from src.common.utils.config import load_dag_dw_migration_settings, load_mysql_config, repo_root
from src.common.utils.db_partition import existing_partition_names
from src.common.utils.task_log import log_task_run
from src.dw_migration.tasks import Task

_SERVICE = "dw_migration"
_RUNS_ROOT = repo_root() / "data" / "dw_migration" / "runs"


def _partition_date_from_name(name: str) -> date | None:
    if not name.startswith("p_"):
        return None
    try:
        return date.fromisoformat(name[2:].replace("_", "-"))
    except ValueError:
        return None


def _purge_local_run_dirs(*, cutoff: date, logger: object) -> int:
    """``partition_date``(cutoff)보다 엄격히 이전 날짜의 leftover ``runs/{partition_date}/``
    디렉터리만 삭제한다. cutoff 당일 디렉토리는 register_glue_partition_task가 같은 run에서
    병렬로 manifest.json을 읽어야 하므로 절대 삭제하지 않는다 — 다음 run(내일)이 돌 때
    자연스럽게 leftover로 정리된다."""
    if not _RUNS_ROOT.is_dir():
        return 0
    removed = 0
    for child in sorted(_RUNS_ROOT.iterdir()):
        if not child.is_dir():
            continue
        try:
            part_date = date.fromisoformat(child.name)
        except ValueError:
            continue
        if part_date >= cutoff:
            continue
        shutil.rmtree(child)
        removed += 1
        logger.info("removed local run dir partition_date=%s path=%s", part_date, child)
    return removed


class PurgeSMarketOhlcvHistory(Task):
    def __init__(self) -> None:
        super().__init__("purge_s_market_ohlcv_history")

    def run(self, *, partition_date: date, **context: object) -> int:
        settings = load_dag_dw_migration_settings()
        with log_task_run(
            self.name,
            service=_SERVICE,
            partition_date=str(partition_date),
            source_table=settings.source_table,
        ) as logger:
            db = MySQLClient(load_mysql_config())
            conn = db.connection()
            dropped = 0
            try:
                table = settings.source_table
                existing = existing_partition_names(db, conn, table)
                for name in sorted(existing):
                    part_date = _partition_date_from_name(name)
                    # 정확히 partition_date와 일치하는 파티션만 DROP — 이 run이
                    # 방금 S3로 이관 완료한 그 날짜만 지운다. 그 외 날짜는 아직
                    # 이관되지 않았을 수 있으므로 절대 건드리지 않는다.
                    if part_date != partition_date:
                        continue
                    db.execute(f"ALTER TABLE {table} DROP PARTITION {name}", conn=conn)
                    dropped += 1
                    logger.info(
                        "dropped partition name=%s partition_date=%s", name, part_date
                    )
                db.commit(conn)
                local_removed = _purge_local_run_dirs(cutoff=partition_date, logger=logger)
                logger.info(
                    "purge complete dropped_partitions=%d removed_local_dirs=%d partition_date=%s",
                    dropped,
                    local_removed,
                    partition_date,
                )
                return dropped
            except Exception:
                db.rollback(conn)
                raise
            finally:
                conn.close()
