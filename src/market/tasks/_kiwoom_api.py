"""마켓 적재 태스크용 키움 API 헬퍼."""

from __future__ import annotations

import os
import time
from dataclasses import dataclass
from datetime import date, datetime, tzinfo
from typing import Any

from infra.http import HttpError, NetworkError, RestClient
from src.common.utils.config import ensure_runtime_config

_DEFAULT_MAX_RETRIES = 5
_DEFAULT_MAX_RPS = 5.0

TR_STOCK_LIST = "ka10099"
TR_MINUTE_CHART = "ka10080"
TR_TOKEN_PATH = "/oauth2/token"


def _tr_path(tr_code: str) -> str:
    if tr_code == TR_STOCK_LIST:
        return "/api/dostk/stkinfo"
    if tr_code == TR_MINUTE_CHART:
        return "/api/dostk/chart"
    raise KeyError(f"Unsupported TR code: {tr_code}")


def _sleep_remaining(interval_sec: float, started_at: float) -> None:
    """``started_at`` 이후 ``interval_sec``가 될 때까지만 sleep한다."""
    remaining = interval_sec - (time.perf_counter() - started_at)
    if remaining > 0:
        time.sleep(remaining)


def _parse_max_retries(raw: str) -> int:
    if not raw:
        return _DEFAULT_MAX_RETRIES
    try:
        return max(1, int(raw))
    except ValueError:
        return _DEFAULT_MAX_RETRIES


def _parse_max_rps(raw: str) -> float:
    if not raw:
        return _DEFAULT_MAX_RPS
    try:
        return max(0.1, float(raw))
    except ValueError:
        return _DEFAULT_MAX_RPS


@dataclass
class KiwoomCredentials:
    base_url: str
    app_key: str
    secret_key: str
    timeout: float = 15.0
    max_retries: int = _DEFAULT_MAX_RETRIES
    max_rps: float = _DEFAULT_MAX_RPS

    @classmethod
    def from_env(cls) -> "KiwoomCredentials":
        ensure_runtime_config()
        base_url = os.environ.get("KIWOOM_BASE_URL", "").strip()
        app_key = os.environ.get("KIWOOM_APP_KEY", "").strip()
        secret_key = os.environ.get("KIWOOM_SECRET_KEY", "").strip()
        timeout = float(os.environ.get("KIWOOM_TIMEOUT", "15"))
        max_retries = _parse_max_retries(os.environ.get("KIWOOM_MAX_RETRIES", "").strip())
        max_rps = _parse_max_rps(os.environ.get("KIWOOM_MAX_RPS", "").strip())
        if not base_url or not app_key or not secret_key:
            raise ValueError(
                "Missing Kiwoom credentials: set KIWOOM_BASE_URL, "
                "KIWOOM_APP_KEY, KIWOOM_SECRET_KEY"
            )
        return cls(
            base_url=base_url,
            app_key=app_key,
            secret_key=secret_key,
            timeout=timeout,
            max_retries=max_retries,
            max_rps=max_rps,
        )


class KiwoomApi:
    """마켓 DAG 태스크에서 쓰는 최소 키움 REST API 래퍼."""

    def __init__(self, creds: KiwoomCredentials) -> None:
        self._http = RestClient(base_url=creds.base_url, timeout=creds.timeout)
        self._app_key = creds.app_key
        self._secret_key = creds.secret_key
        self._max_retries = creds.max_retries
        self._max_rps = creds.max_rps
        self._token: str | None = None
        self._last_request_at: float | None = None

    @property
    def max_retries(self) -> int:
        return self._max_retries

    def _min_request_interval(self) -> float:
        return 1.0 / self._max_rps

    def _throttle_request(self) -> None:
        interval = self._min_request_interval()
        if self._last_request_at is not None:
            _sleep_remaining(interval, self._last_request_at)
        self._last_request_at = time.perf_counter()

    def fetch_asset_rows(self) -> list[dict[str, Any]]:
        data = self._call_tr(TR_STOCK_LIST, {"mrkt_tp": "0"})
        rows = data.get("list") or []
        return [row for row in rows if isinstance(row, dict)]

    def fetch_minute_ohlcv_rows(
        self, asset_code: str, *, base_dt: date | None = None
    ) -> list[dict[str, Any]]:
        trade_date = base_dt or datetime.now().date()
        body = {
            "stk_cd": asset_code,
            "base_dt": trade_date.strftime("%Y%m%d"),
            "tic_scope": "1",
            "upd_stkpc_tp": "1",
        }
        data = self._call_tr(TR_MINUTE_CHART, body)
        rows = data.get("stk_min_pole_chart_qry") or data.get("output") or []
        if not isinstance(rows, list):
            return []

        parsed: list[tuple[datetime, int, dict[str, Any]]] = []
        for index, row in enumerate(rows):
            if not isinstance(row, dict):
                continue
            market_time = parse_market_time(row)
            if market_time is None:
                continue
            parsed.append((market_time, index, row))

        parsed.sort(key=lambda item: (item[0], -item[1]), reverse=True)
        selected: list[dict[str, Any]] = []
        seen_times: set[datetime] = set()
        for market_time, _, row in parsed:
            if market_time in seen_times:
                continue
            seen_times.add(market_time)
            selected.append(row)
        return selected

    def _issue_token(self) -> str:
        data = self._request_json(
            "POST",
            TR_TOKEN_PATH,
            body={
                "grant_type": "client_credentials",
                "appkey": self._app_key,
                "secretkey": self._secret_key,
            },
            with_auth=False,
        )
        token = str(data.get("token") or "").strip()
        if not token:
            raise RuntimeError(f"Kiwoom token issuance failed: {data}")
        self._token = token
        return token

    def _call_tr(self, tr_code: str, body: dict[str, Any]) -> dict[str, Any]:
        return self._request_json(
            "POST",
            _tr_path(tr_code),
            body=body,
            with_auth=True,
            headers={"api-id": tr_code},
        )

    @staticmethod
    def _should_retry_http_error(exc: HttpError, *, with_auth: bool) -> bool:
        if isinstance(exc, NetworkError):
            return True
        message = str(exc).lower()
        if "429" in message:
            return True
        return with_auth and "401" in str(exc)

    def _request_json(
        self,
        method: str,
        path: str,
        *,
        body: dict[str, Any],
        with_auth: bool,
        headers: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        req_headers = dict(headers or {})
        max_retries = self._max_retries
        last_exc: Exception | None = None

        for attempt in range(max_retries):
            attempt_started = time.perf_counter()
            if with_auth:
                if self._token is None:
                    self._issue_token()
                req_headers["Authorization"] = f"Bearer {self._token}"

            try:
                self._throttle_request()
                data = self._http.request_json(
                    method, path, json_body=body, headers=req_headers
                )
                if data.get("return_code") not in (None, 0, "0"):
                    raise RuntimeError(f"Kiwoom API error: {data}")
                return data
            except HttpError as exc:
                last_exc = exc
                if "401" in str(exc) and with_auth:
                    self._token = None
                if not self._should_retry_http_error(exc, with_auth=with_auth):
                    raise RuntimeError(str(exc)) from exc
                if attempt >= max_retries - 1:
                    raise RuntimeError(str(exc)) from exc
                _sleep_remaining(min(2**attempt, 10), attempt_started)

        raise RuntimeError(str(last_exc) if last_exc else "Kiwoom API request failed")


def parse_asset_type(row: dict[str, Any]) -> str:
    kind = str(row.get("kind") or "").upper()
    market_name = str(row.get("marketName") or "").upper()
    if "ETF" in market_name:
        return "ETF"
    if kind == "A":
        return "STOCK"
    if kind == "B":
        return "BOND"
    return "STOCK"


def parse_asset_code(row: dict[str, Any]) -> str:
    return str(row.get("code") or row.get("stk_cd") or "").strip()


def parse_asset_name(row: dict[str, Any]) -> str:
    return str(row.get("name") or row.get("stk_nm") or "").strip()


def parse_market_time(row: dict[str, Any]) -> datetime | None:
    raw = str(row.get("cntr_tm") or "").strip()
    if len(raw) >= 14 and raw[:14].isdigit():
        return datetime.strptime(raw[:14], "%Y%m%d%H%M%S")
    return None


def normalize_market_time(value: datetime, *, market_tz: tzinfo) -> datetime:
    """Kiwoom/API 분봉 시각을 market_tz naive datetime(초·마이크로초 0)으로 정규화한다."""
    if value.tzinfo is not None:
        value = value.astimezone(market_tz).replace(tzinfo=None)
    return value.replace(second=0, microsecond=0)


def _coerce_kiwoom_number(raw: Any) -> float | None:
    """키움 숫자 문자열을 파싱한다. 앞의 +/-는 부호가 아니라 등락 방향이다."""
    if raw in (None, ""):
        return None
    text = str(raw).replace(",", "").strip()
    if text in ("", "+", "-"):
        return None
    if text[0] in "+-" and len(text) > 1:
        unsigned = text[1:]
        if unsigned.replace(".", "", 1).isdigit():
            return float(unsigned)
    try:
        return float(text)
    except ValueError:
        return None


def parse_number(row: dict[str, Any], *keys: str) -> float:
    for key in keys:
        if key not in row:
            continue
        value = _coerce_kiwoom_number(row[key])
        if value is not None:
            return value
    raise ValueError(f"Missing numeric keys: {keys}")

