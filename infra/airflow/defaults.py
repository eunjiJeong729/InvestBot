"""공유 Airflow DAG 기본값."""

from __future__ import annotations

from datetime import timedelta
from typing import Any


def default_dag_args(
    *,
    owner: str = "investbot",
    retries: int = 0,
    retry_delay_minutes: int = 5,
    **extra: Any,
) -> dict[str, Any]:
    """프로젝트 DAG용 보수적인 ``default_args`` dict를 반환한다."""
    args: dict[str, Any] = {
        "owner": owner,
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": retries,
    }
    if retries > 0:
        args["retry_delay"] = timedelta(minutes=retry_delay_minutes)
    args.update(extra)
    return args
