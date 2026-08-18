#!/usr/bin/env python3
"""XKRX 거래일 캘린더를 ``d_market_calendar``에 스냅샷 적재한다.

Airflow DAG가 아니다. 연 1회 또는 임시공휴일 발표 시 수동 실행한다.

    python scripts/load_d_market_calendar.py --config configs/dev/debug.json
"""

from __future__ import annotations

import argparse
import sys
from datetime import date

from src.common.utils.config import init_runtime_config
from src.market.tasks._market_calendar import load_market_calendar, year_range


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Load d_market_calendar from XKRX")
    parser.add_argument(
        "--config",
        default="configs/dev/debug.json",
        help="런타임 설정 파일 경로 (기본값: configs/dev/debug.json)",
    )
    parser.add_argument(
        "--years-back",
        type=int,
        default=1,
        help="올해 기준 과거 연수 (기본 1 → 작년 1/1부터)",
    )
    parser.add_argument(
        "--years-forward",
        type=int,
        default=2,
        help="올해 기준 미래 연수 (기본 2 → 내후년 12/31까지)",
    )
    args = parser.parse_args(argv)

    if args.years_back < 0 or args.years_forward < 0:
        print("error: --years-back and --years-forward must be >= 0", file=sys.stderr)
        return 2

    init_runtime_config(args.config)

    start, end = year_range(
        today=date.today(),
        years_back=args.years_back,
        years_forward=args.years_forward,
    )
    result = load_market_calendar(start=start, end=end)
    print("==> d_market_calendar snapshot loaded")
    print(f"    table:       {result['table']}")
    print(f"    year range:  {result['start']} .. {result['end']}")
    print(f"    rows:        {result['rows']}")
    print(f"    open_days:   {result['open_days']}")
    print(f"    closed_days: {result['closed_days']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
