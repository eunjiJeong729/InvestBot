"""MySQL RANGE 파티션 이름/조회 헬퍼 (history ADD · DW DROP 공용)."""

from __future__ import annotations

from datetime import date, timedelta

from infra.db.rdbms.mysql import MySQLClient


def partition_name(partition_date: date) -> str:
    """일별 파티션 식별자. 예: ``p_2026_08_14``."""
    return f"p_{partition_date.strftime('%Y_%m_%d')}"


def partition_upper_bound(partition_date: date) -> str:
    """VALUES LESS THAN 상한 (다음 캘린더 일 ``YYYY-MM-DD``)."""
    return (partition_date + timedelta(days=1)).strftime("%Y-%m-%d")


def partition_date_from_name(name: str) -> date | None:
    """파티션 이름(``p_YYYY_MM_DD``)에서 날짜를 역산한다. 형식이 아니면 ``None``."""
    if not name.startswith("p_"):
        return None
    try:
        return date.fromisoformat(name[2:].replace("_", "-"))
    except ValueError:
        return None


def existing_partition_names(db: MySQLClient, conn: object, table: str) -> set[str]:
    """현재 스키마에서 ``table``의 PARTITION_NAME 집합을 반환한다."""
    rows = db.fetchall(
        """
        SELECT PARTITION_NAME
        FROM information_schema.PARTITIONS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = %s
          AND PARTITION_NAME IS NOT NULL
        """,
        (table,),
        conn=conn,
    )
    return {str(row["PARTITION_NAME"]) for row in rows}
