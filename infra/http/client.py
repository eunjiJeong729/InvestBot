"""REST client — base URL, timeout, JSON requests."""

from __future__ import annotations

from typing import Any

from . import request as http_request


class RestClient:
    """Thin wrapper around :mod:`infra.http.request`."""

    def __init__(
        self,
        base_url: str,
        *,
        timeout: float = 15,
        default_headers: dict[str, str] | None = None,
    ) -> None:
        self.base_url = str(base_url).strip().rstrip("/")
        self.timeout = float(timeout)
        self.default_headers = dict(default_headers or {})

    def url(self, path: str) -> str:
        if not path.startswith("/"):
            raise ValueError("path must start with '/'")
        return f"{self.base_url}{path}"

    def request_json(
        self,
        method: str,
        path: str,
        *,
        json_body: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        merged = {**self.default_headers, **(headers or {})}
        return http_request.request_json(
            method,
            self.url(path),
            json_body=json_body,
            headers=merged,
            timeout=self.timeout,
        )
