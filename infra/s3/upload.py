"""S3 upload helpers."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .client import S3Client


def upload_bytes(
    client: S3Client,
    key: str,
    data: bytes,
    *,
    content_type: str | None = None,
    metadata: dict[str, str] | None = None,
) -> str:
    """바이트 데이터를 S3에 업로드한다."""
    return client.put_object(
        key,
        data,
        content_type=content_type,
        metadata=metadata,
    )


def upload_file(
    client: S3Client,
    key: str,
    local_path: str | Path,
    *,
    content_type: str | None = None,
    metadata: dict[str, str] | None = None,
) -> str:
    """로컬 파일을 S3에 업로드한다."""
    path = Path(local_path)
    if not path.is_file():
        raise FileNotFoundError(f"Local file not found: {path}")
    return upload_bytes(
        client,
        key,
        path.read_bytes(),
        content_type=content_type,
        metadata=metadata,
    )


def upload_json(
    client: S3Client,
    key: str,
    payload: Any,
    *,
    metadata: dict[str, str] | None = None,
    indent: int | None = None,
) -> str:
    """JSON 객체를 S3에 업로드한다."""
    body = json.dumps(payload, ensure_ascii=False, default=str, indent=indent).encode(
        "utf-8"
    )
    return upload_bytes(
        client,
        key,
        body,
        content_type="application/json; charset=utf-8",
        metadata=metadata,
    )
