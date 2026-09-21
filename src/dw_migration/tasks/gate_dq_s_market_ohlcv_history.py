"""태스크: gate_dq_s_market_ohlcv_history — pandas DQ Gate."""

from __future__ import annotations

from datetime import date, datetime, time, timedelta
from typing import Any

import pandas as pd

from src.common.utils.config import load_dag_dw_migration_settings, repo_root
from src.common.utils.task_log import log_gate_decision

_SERVICE = "dw_migration"
_GATE_ID = "gate_dq_s_market_ohlcv_history"

_REQUIRED_COLUMNS = (
    "asset_type",
    "asset_code",
    "market_time",
    "open_price",
    "high_price",
    "low_price",
    "close_price",
    "volume",
    "partition_hour",
)

_KEY_COLUMNS = ("asset_type", "asset_code", "market_time")
_SESSION_START = time(9, 0)
_SESSION_END = time(15, 30)
_BAR_MINUTES = 5


def _run_dq_checks(df: pd.DataFrame, *, max_gap_ratio: float = 0.5) -> dict[str, Any]:
    """DQ Gate 4종 검증. 항목별로 끝까지 실행해 통과여부/이상치 요약을 모두 수집한다
    (개별 검증 실패로 즉시 raise하지 않고, 4종을 모두 실행한 뒤 종합 판단한다)."""
    if df.empty:
        raise ValueError("DQ Gate failed: empty extract result")

    missing_cols = [col for col in _REQUIRED_COLUMNS if col not in df.columns]
    if missing_cols:
        raise ValueError(f"DQ Gate failed: missing columns {missing_cols}")

    checks: dict[str, dict[str, Any]] = {}

    null_mask = df[list(_REQUIRED_COLUMNS)].isnull().any(axis=1)
    null_count = int(null_mask.sum())
    checks["null_check"] = {"passed": null_count == 0, "count": null_count}
    if null_count:
        checks["null_check"]["sample"] = df.loc[
            null_mask, list(_REQUIRED_COLUMNS)
        ].head(3).to_dict(orient="records")

    dup_row_mask = df.duplicated(subset=list(_KEY_COLUMNS), keep=False)
    dup_count = int(dup_row_mask.sum())
    checks["duplicate_check"] = {"passed": dup_count == 0, "count": dup_count}
    if dup_count:
        checks["duplicate_check"]["sample"] = df.loc[
            dup_row_mask, list(_KEY_COLUMNS)
        ].head(3).to_dict(orient="records")

    invalid_range = (
        (df["close_price"] <= 0)
        | (df["open_price"] <= 0)
        | (df["high_price"] <= 0)
        | (df["low_price"] <= 0)
        | (df["volume"] < 0)
        | (df["high_price"] < df["low_price"])
    )
    range_count = int(invalid_range.sum())
    checks["range_check"] = {"passed": range_count == 0, "count": range_count}
    if range_count:
        cols = list(_KEY_COLUMNS) + [
            "open_price", "high_price", "low_price", "close_price", "volume",
        ]
        checks["range_check"]["sample"] = df.loc[invalid_range, cols].head(3).to_dict(
            orient="records"
        )

    gap_ratio = _max_gap_ratio(df)
    checks["gap_check"] = {
        "passed": gap_ratio <= max_gap_ratio,
        "gap_ratio": round(gap_ratio, 4),
        "max_gap_ratio": max_gap_ratio,
    }

    failed_checks = [name for name, result in checks.items() if not result["passed"]]
    return {
        "rows": len(df),
        "status": "passed" if not failed_checks else "failed",
        "failed_checks": failed_checks,
        "checks": checks,
    }


def _max_gap_ratio(df: pd.DataFrame) -> float:
    """종목별 5분봉 연속성 갭 비율의 최댓값."""
    work = df.copy()
    work["market_time"] = pd.to_datetime(work["market_time"])
    worst = 0.0
    for _, group in work.groupby(["asset_type", "asset_code"], sort=False):
        times = sorted(
            t for t in group["market_time"]
            if _SESSION_START <= t.time() <= _SESSION_END
        )
        if len(times) < 2:
            continue
        expected = _expected_timestamps(times[0], times[-1])
        if not expected:
            continue
        actual = {t.replace(second=0, microsecond=0) for t in times}
        missing = len(expected - actual)
        ratio = missing / len(expected)
        worst = max(worst, ratio)
    return worst


def _expected_timestamps(start: datetime, end: datetime) -> set[datetime]:
    cursor = start.replace(second=0, microsecond=0)
    end = end.replace(second=0, microsecond=0)
    step = timedelta(minutes=_BAR_MINUTES)
    expected: set[datetime] = set()
    while cursor <= end:
        if _SESSION_START <= cursor.time() <= _SESSION_END:
            expected.add(cursor)
        cursor += step
    return expected


def should_pass_dq_gate(*, partition_date: date, **context: object) -> bool:
    """PythonOperator callable — DQ 실패 시 ValueError로 태스크를 failed 처리해 downstream 차단."""
    run_dir = repo_root() / "data" / "dw_migration" / "runs" / partition_date.isoformat()
    path = run_dir / "extracted.parquet"
    if not path.is_file():
        reason = "missing_extracted_parquet"
        log_gate_decision(
            _GATE_ID,
            allowed=False,
            reason=reason,
            service=_SERVICE,
            partition_date=str(partition_date),
            path=str(path),
        )
        raise ValueError(f"DQ Gate failed: {reason} path={path}")

    settings = load_dag_dw_migration_settings()
    try:
        df = pd.read_parquet(path)
        summary = _run_dq_checks(df, max_gap_ratio=settings.max_gap_ratio)
    except ValueError as exc:
        log_gate_decision(
            _GATE_ID,
            allowed=False,
            reason=str(exc),
            service=_SERVICE,
            partition_date=str(partition_date),
        )
        raise

    passed = summary["status"] == "passed"
    reason = "dq_passed" if passed else f"dq_failed:{','.join(summary['failed_checks'])}"
    log_gate_decision(
        _GATE_ID,
        allowed=passed,
        reason=reason,
        service=_SERVICE,
        partition_date=str(partition_date),
        rows=summary["rows"],
        checks=summary["checks"],
    )
    if not passed:
        raise ValueError(f"DQ Gate failed: {reason}")
    return True
