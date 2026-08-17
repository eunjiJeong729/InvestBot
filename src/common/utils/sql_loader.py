"""DAG별 정적 SQL 파일 로더 — ``src/{service}/sql/{name}.sql``."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from src.common.utils.config import repo_root


def resolve_sql_path(service: str, sql_name: str) -> Path:
    """``src/{service}/sql/{sql_name}.sql`` 절대 경로.

    예: ``resolve_sql_path("market", "insert_s_market_ohlcv_history")``
    → ``<repo>/src/market/sql/insert_s_market_ohlcv_history.sql``

    ``sql_name``에 ``.sql`` 확장자가 있으면 그대로 쓰고, 없으면 붙인다.
    """
    name = sql_name if sql_name.endswith(".sql") else f"{sql_name}.sql"
    path = (repo_root() / "src" / service / "sql" / name).resolve()
    if not path.is_file():
        raise FileNotFoundError(f"SQL file not found: {path}")
    return path


def load_sql(service: str, sql_name: str) -> str:
    """DAG ``sql/`` 디렉터리의 SQL 파일 내용을 읽는다."""
    return resolve_sql_path(service, sql_name).read_text(encoding="utf-8")


def format_sql(service: str, sql_name: str, **placeholders: Any) -> str:
    """SQL 파일을 읽고 ``str.format(**placeholders)``로 플레이스홀더를 채운다."""
    return load_sql(service, sql_name).format(**placeholders)
