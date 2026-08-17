"""Low-level HTTPS request helpers."""

from __future__ import annotations

import json
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from .errors import HttpResponseError, JsonDecodeError, NetworkError


def request_text(
    method: str,
    url: str,
    *,
    body: bytes | None = None,
    headers: dict[str, str] | None = None,
    timeout: float = 15,
) -> str:
    """Perform an HTTPS request and return the response body as text."""
    req = Request(
        url=url,
        data=body,
        headers=headers or {},
        method=method.upper(),
    )
    try:
        with urlopen(req, timeout=timeout) as response:
            return response.read().decode("utf-8")
    except HTTPError as exc:
        error_body = exc.read().decode("utf-8", errors="replace")
        raise HttpResponseError(
            f"HTTP {exc.code} for {method.upper()} {url}: {error_body}",
            status_code=exc.code,
            body=error_body,
        ) from exc
    except TimeoutError as exc:
        raise NetworkError(
            f"Timeout for {method.upper()} {url} after {timeout}s"
        ) from exc
    except URLError as exc:
        raise NetworkError(f"Network error for {method.upper()} {url}: {exc}") from exc


def request_json(
    method: str,
    url: str,
    *,
    json_body: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
    timeout: float = 15,
) -> dict[str, Any]:
    """Perform an HTTPS JSON request and parse the JSON object response."""
    request_headers = {"Content-Type": "application/json;charset=UTF-8"}
    if headers:
        request_headers.update(headers)

    payload = json.dumps(json_body or {}).encode("utf-8")
    raw = request_text(
        method,
        url,
        body=payload,
        headers=request_headers,
        timeout=timeout,
    )
    try:
        data = json.loads(raw) if raw else {}
    except json.JSONDecodeError as exc:
        raise JsonDecodeError(
            f"Invalid JSON response for {method.upper()} {url}"
        ) from exc
    if not isinstance(data, dict):
        raise JsonDecodeError(
            f"Expected JSON object for {method.upper()} {url}, got {type(data).__name__}"
        )
    return data
