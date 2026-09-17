"""Airflow logical_date -> timezone 변환 유틸."""

from __future__ import annotations

from datetime import datetime, tzinfo as TzInfo
from zoneinfo import ZoneInfo

import pendulum


def context_logical_date_in_timezone(context: object, tz: TzInfo) -> datetime | None:
    """Airflow task context의 ``logical_date``를 ``tz`` 기준 datetime으로 변환한다.

    naive datetime은 UTC로 간주해 명시적으로 tzinfo를 붙인 뒤 변환한다
    (pendulum이 naive를 로컬 시스템 타임존으로 오인할 수 있음).
    ``astimezone()`` 체이닝 시 tzinfo가 소실되는 pendulum 결함
    (docs/engineering_decisions.md #6)을 피하기 위해 ``in_timezone()``만 사용한다.
    """
    if not isinstance(context, dict):
        return None
    raw = context.get("logical_date")
    if not isinstance(raw, datetime):
        return None
    slot_dt = raw.replace(tzinfo=ZoneInfo("UTC")) if raw.tzinfo is None else raw
    return pendulum.instance(slot_dt).in_timezone(tz).replace(second=0, microsecond=0)
