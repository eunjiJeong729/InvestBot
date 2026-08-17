"""JSON ↔ entity dataclass helpers."""

from __future__ import annotations

import json
from dataclasses import fields
from datetime import datetime
from pathlib import Path
from typing import Any, TypeVar

from src.common.entity import Account, Asset, Index, Sector, Theme

T = TypeVar("T")

_DATETIME_FIELDS: dict[type[Any], frozenset[str]] = {
    Account: frozenset({"created_datetime", "opened_datetime", "closed_datetime"}),
    Asset: frozenset({"created_datetime", "listed_datetime", "delisted_datetime"}),
}


def entity_from_json(cls: type[T], path: str | Path) -> T:
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"Expected JSON object in {path}")
    return entity_from_dict(cls, data)


def entity_from_dict(cls: type[T], data: dict[str, Any]) -> T:
    names = {field.name for field in fields(cls)}
    dt_fields = _DATETIME_FIELDS.get(cls, frozenset())
    kwargs: dict[str, Any] = {}
    for key, value in data.items():
        if key not in names or key.startswith("_"):
            continue
        if key in dt_fields and value:
            value = datetime.fromisoformat(str(value))
        kwargs[key] = value
    return cls(**kwargs)
