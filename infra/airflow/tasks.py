"""Airflow 태스크 헬퍼."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from .compat import require_airflow


def python_task_callable(fn: Callable[..., Any], /, *args: Any, **kwargs: Any) -> Callable[[], Any]:
    """*fn*을 Airflow ``PythonOperator`` / ``@task``가 인자 없이 호출할 수 있게 감싼다."""

    def _call() -> Any:
        return fn(*args, **kwargs)

    _call.__name__ = getattr(fn, "__name__", "python_task_callable")
    _call.__doc__ = getattr(fn, "__doc__", None)
    return _call


def task_failure_message(result: dict[str, Any], *, label: str = "task") -> str:
    """에이전트 스타일 result 페이로드에서 오류 메시지를 만든다."""
    errors = result.get("errors")
    if isinstance(errors, list) and errors:
        return f"{label} failed: {'; '.join(str(e) for e in errors)}"
    return f"{label} failed: {result!r}"


def ensure_success(result: dict[str, Any], *, label: str = "task") -> dict[str, Any]:
    """*result*가 실패한 preparation/agent 페이로드처럼 보이면 raise한다."""
    if result.get("valid") is False:
        raise RuntimeError(task_failure_message(result, label=label))
    if result.get("status") == "failed":
        raise RuntimeError(task_failure_message(result, label=label))
    return result


def python_operator(
    *,
    task_id: str,
    python_callable: Callable[..., Any],
    op_kwargs: dict[str, Any] | None = None,
    **operator_kwargs: Any,
):
    """:class:`PythonOperator`를 생성한다 (Airflow 설치 필요)."""
    require_airflow()
    from airflow.operators.python import PythonOperator

    return PythonOperator(
        task_id=task_id,
        python_callable=python_callable,
        op_kwargs=op_kwargs or {},
        **operator_kwargs,
    )
