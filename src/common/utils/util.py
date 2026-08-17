"""Shared helpers."""

from __future__ import annotations

ID_DELIMITER = "_"


def hash_id(parts: list[str], *, source: str, obj_type: str) -> str:
    """``{key...}_{source}_{obj_type}`` 형식 ID."""
    if not parts:
        raise ValueError("hash_id requires at least one key part")

    all_parts = [*parts, source, obj_type]
    normalized: list[str] = []
    for part in all_parts:
        value = str(part).strip()
        if not value:
            raise ValueError("hash_id parts must be non-empty strings")
        cleaned = value.replace("|", "").replace("/", "").replace("_", "")
        if not cleaned:
            raise ValueError("hash_id part is empty after normalization")
        normalized.append(cleaned)

    return ID_DELIMITER.join(normalized)


def safe_filename(object_id: str) -> str:
    """파일 경로에 쓸 수 있도록 ``id``를 이스케이프한다."""
    return (
        object_id.replace("/", "_")
        .replace("\\", "_")
        .replace(":", "_")
        .replace("|", "_")
    )
