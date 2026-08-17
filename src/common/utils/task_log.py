"""태스크 로깅 헬퍼 — Airflow task log 및 CLI 실행 공용."""

from __future__ import annotations

import logging
import os
import time
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

from src.common.utils.config import ensure_runtime_config, load_runtime_config

_CONFIGURED = False
_LOGGER_ROOT = "investbot"


def _resolve_log_level() -> int:
    raw = os.environ.get("INVESTBOT_LOG_LEVEL", "").strip()
    if not raw:
        config_path = os.environ.get("INVESTBOT_CONFIG", "").strip()
        if config_path:
            run = load_runtime_config(config_path).get("run") or {}
            raw = str(run.get("log_level") or "INFO")
        else:
            raw = "INFO"
    return getattr(logging, raw.upper(), logging.INFO)


def _resolve_timezone_name() -> str:
    tz = os.environ.get("TZ", "").strip()
    if tz:
        return tz
    config_path = os.environ.get("INVESTBOT_CONFIG", "").strip()
    if config_path:
        run = load_runtime_config(config_path).get("run") or {}
        tz = str(run.get("timezone") or "").strip()
        if tz:
            return tz
    return "Asia/Seoul"


class _LocalTimezoneFormatter(logging.Formatter):
    """설정된 로컬 타임존(기본 KST)으로 로그 시각을 포맷한다."""

    def __init__(self, tz_name: str, fmt: str) -> None:
        super().__init__(fmt=fmt)
        self._tz = ZoneInfo(tz_name)

    def formatTime(self, record: logging.LogRecord, datefmt: str | None = None) -> str:
        dt = datetime.fromtimestamp(record.created, tz=self._tz)
        return dt.strftime(datefmt or "%Y-%m-%d %H:%M:%S %Z")


def configure_task_logging() -> None:
    global _CONFIGURED
    if _CONFIGURED:
        return
    ensure_runtime_config()
    level = _resolve_log_level()
    tz_name = _resolve_timezone_name()
    root = logging.getLogger(_LOGGER_ROOT)
    root.setLevel(level)
    root.propagate = True
    if not root.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(
            _LocalTimezoneFormatter(
                tz_name,
                "%(asctime)s %(levelname)s [%(name)s] %(message)s",
            )
        )
        root.addHandler(handler)
    _CONFIGURED = True


def get_task_logger(task_name: str) -> logging.Logger:
    """Airflow task 로그에 기록하는 로거를 반환한다."""
    configure_task_logging()
    return logging.getLogger(f"{_LOGGER_ROOT}.market.{task_name}")


@contextmanager
def log_task_run(task_name: str, **context: Any) -> Iterator[logging.Logger]:
    """태스크 시작/종료와 경과 시간을 로그한다."""
    logger = get_task_logger(task_name)
    started = time.perf_counter()
    if context:
        logger.info("task start context=%s", context)
    else:
        logger.info("task start")
    try:
        yield logger
    except Exception:
        elapsed = time.perf_counter() - started
        logger.exception("task failed elapsed=%.2fs", elapsed)
        raise
    else:
        elapsed = time.perf_counter() - started
        logger.info("task finished elapsed=%.2fs", elapsed)


def log_gate_decision(gate_name: str, *, allowed: bool, reason: str, **details: Any) -> None:
    """ShortCircuit gate 판단 결과를 Airflow task 로그에 남긴다.

    Airflow 기본 로그는 True/False만 보여 주므로, skip/실행 이유(reason)와
    logical_date 등 부가 정보를 구조화해 남겨 운영·디버깅에 쓴다.
    gate 통과 여부 자체는 호출부 return 값으로만 결정된다.
    """
    logger = get_task_logger(gate_name)
    payload = {"allowed": allowed, "reason": reason, **details}
    logger.info("gate decision %s", payload)
