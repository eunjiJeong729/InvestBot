"""선택적 Airflow import 헬퍼."""

from __future__ import annotations


def airflow_available() -> bool:
    try:
        import airflow  # noqa: F401

        return True
    except ImportError:
        return False


def require_airflow():
    """``airflow`` 모듈을 반환하거나, 설치 안내와 함께 raise한다."""
    try:
        import airflow

        return airflow
    except ImportError as exc:
        raise ImportError(
            "Apache Airflow is not installed. "
            "Run: pip install -r requirements-airflow.txt"
        ) from exc
