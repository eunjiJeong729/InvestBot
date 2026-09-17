"""스테이징 테이블용 TRUNCATE + INSERT 스냅샷 헬퍼."""

from __future__ import annotations

from collections.abc import Sequence

from infra.db.rdbms.mysql import MySQLClient


def replace_snapshot(
    db: MySQLClient,
    *,
    table: str,
    columns: Sequence[str],
    rows: Sequence[tuple],
) -> None:
    """``table``을 TRUNCATE한 뒤 ``rows``를 한 트랜잭션으로 INSERT한다.

    연결을 열고 TRUNCATE + executemany INSERT 후 commit한다.
    오류 시 rollback 후 재발생. 연결은 항상 닫는다.
    """
    col_list = ", ".join(columns)
    placeholders = ", ".join(["%s"] * len(columns))
    insert_sql = f"""
        INSERT INTO {table}
        ({col_list})
        VALUES ({placeholders})
    """

    conn = db.connection()
    try:
        db.execute(f"TRUNCATE TABLE {table}", conn=conn)
        db.executemany(insert_sql, list(rows), conn=conn)
        db.commit(conn)
    except Exception:
        db.rollback(conn)
        raise
    finally:
        conn.close()
