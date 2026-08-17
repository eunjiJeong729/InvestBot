"""JSON 설정 및 시크릿 로더."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

_DEFAULT_SECRETS_DIR = Path(".secrets")
_RUNTIME_APPLIED_FLAG = "_INVESTBOT_RUNTIME_APPLIED"


def _read_json(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise FileNotFoundError(f"Config not found: {path}")
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"Config must be a JSON object: {path}")
    return data


def repo_root() -> Path:
    """프로젝트 루트 (``INVESTBOT_REPO_ROOT`` 또는 현재 작업 디렉터리)."""
    override = os.environ.get("INVESTBOT_REPO_ROOT", "").strip()
    if override:
        return Path(override).resolve()
    return Path.cwd().resolve()


def resolve_repo_path(raw: str | Path) -> Path:
    """repo 루트 기준 상대 경로를 절대 경로로 변환한다."""
    path = Path(raw)
    if path.is_absolute():
        return path.resolve()
    return (repo_root() / path).resolve()


def load_json_config(config_path: str | Path) -> dict[str, Any]:
    """임의 JSON 설정 파일을 로드한다."""
    return _read_json(resolve_repo_path(config_path))


def load_runtime_config(config_path: str | Path) -> dict[str, Any]:
    """``configs/{profile}/...json`` 런타임 프로파일을 로드한다."""
    return _read_json(resolve_repo_path(config_path))


def _apply_env_mapping(values: dict[str, Any]) -> None:
    for key, value in values.items():
        os.environ[str(key)] = str(value)


def apply_runtime_config(config: dict[str, Any]) -> None:
    """런타임 config의 environment / secrets / airflow env를 ``os.environ``에 반영한다."""
    secrets = dict(config.get("secrets") or {})
    _apply_env_mapping(dict(config.get("environment") or {}))

    for secret_name, mapping in dict(config.get("environment_from_secrets") or {}).items():
        if not isinstance(mapping, dict):
            raise ValueError(f"environment_from_secrets.{secret_name} must be an object")
        secret_path = secrets.get(secret_name)
        if not secret_path:
            raise ValueError(
                f"environment_from_secrets.{secret_name} requires secrets.{secret_name}"
            )
        secret_data = load_json_config(secret_path)
        for env_name, field in mapping.items():
            if field not in secret_data:
                raise ValueError(
                    f"Secret {secret_path!r} missing field {field!r} "
                    f"for environment variable {env_name!r}"
                )
            os.environ[str(env_name)] = str(secret_data[field])

    airflow = dict(config.get("airflow") or {})
    _apply_env_mapping(dict(airflow.get("environment") or {}))

    run = dict(config.get("run") or {})
    log_level = str(run.get("log_level") or "").strip()
    if log_level:
        os.environ["INVESTBOT_LOG_LEVEL"] = log_level
    timezone_name = str(run.get("timezone") or os.environ.get("TZ") or "Asia/Seoul").strip()
    if timezone_name:
        os.environ["TZ"] = timezone_name
        os.environ["AIRFLOW__CORE__DEFAULT_TIMEZONE"] = timezone_name
        os.environ["AIRFLOW__WEBSERVER__DEFAULT_UI_TIMEZONE"] = timezone_name


def init_runtime_config(config_path: str | Path) -> dict[str, Any]:
    """config 경로를 받아 1회 로드하고 환경 변수를 적용한다."""
    path = resolve_repo_path(config_path)
    if os.environ.get(_RUNTIME_APPLIED_FLAG) == "1":
        applied = os.environ.get("INVESTBOT_CONFIG", "").strip()
        if applied and resolve_repo_path(applied) == path:
            return load_runtime_config(path)

    config = load_runtime_config(path)
    os.environ["INVESTBOT_CONFIG"] = str(path)
    apply_runtime_config(config)
    os.environ[_RUNTIME_APPLIED_FLAG] = "1"
    return config


def ensure_runtime_config() -> None:
    """``INVESTBOT_CONFIG``가 설정돼 있으면 런타임 env를 보장한다."""
    if os.environ.get(_RUNTIME_APPLIED_FLAG) == "1":
        return
    config_path = os.environ.get("INVESTBOT_CONFIG", "").strip()
    if not config_path:
        raise ValueError(
            "INVESTBOT_CONFIG is required (e.g. configs/dev/debug.json). "
            "Call init_runtime_config(path) or export INVESTBOT_CONFIG before running."
        )
    init_runtime_config(config_path)


def load_broker_config(config_path: str | Path) -> dict[str, Any]:
    """브로커 API 설정 JSON을 로드한다."""
    return load_json_config(config_path)


def secrets_dir() -> Path:
    """크리덴셜 디렉터리 경로 (환경변수 ``INVESTBOT_SECRETS_DIR`` 우선)."""
    override = os.environ.get("INVESTBOT_SECRETS_DIR", "").strip()
    if override:
        return Path(override)
    return _DEFAULT_SECRETS_DIR


def load_secret(name: str) -> dict[str, Any]:
    """``.secrets/{name}.json`` 크리덴셜을 로드한다."""
    path = secrets_dir() / f"{name}.json"
    return _read_json(path)


def load_mysql_config() -> dict[str, Any]:
    ensure_runtime_config()
    override = os.environ.get("INVESTBOT_MYSQL_CONFIG", "").strip()
    if override:
        return load_json_config(override)
    return load_secret("mysql")


def load_aws_config() -> dict[str, Any]:
    return load_secret("aws")


@dataclass(frozen=True)
class MarketSettings:
    """``airflow.dag.market`` 설정 + Airflow 기본 타임존."""

    timezone: ZoneInfo
    schedule_step_minutes: int


def load_market_settings() -> MarketSettings:
    """dag_market 전용 설정.

    - timezone: ``airflow.environment.AIRFLOW__CORE__DEFAULT_TIMEZONE``
    - schedule_step_minutes: ``airflow.dag.market.schedule_step_minutes``
    """
    ensure_runtime_config()
    config_path = os.environ.get("INVESTBOT_CONFIG", "").strip()
    config = load_runtime_config(config_path) if config_path else {}
    airflow = dict(config.get("airflow") or {})
    airflow_env = dict(airflow.get("environment") or {})
    market = dict(dict(airflow.get("dag") or {}).get("market") or {})

    tz_name = str(
        airflow_env.get("AIRFLOW__CORE__DEFAULT_TIMEZONE")
        or os.environ.get("AIRFLOW__CORE__DEFAULT_TIMEZONE")
        or "Asia/Seoul"
    ).strip()
    raw_step = market.get("schedule_step_minutes", 5)
    try:
        step = int(raw_step)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            "airflow.dag.market.schedule_step_minutes must be an integer"
        ) from exc
    if step < 1:
        raise ValueError("airflow.dag.market.schedule_step_minutes must be >= 1")

    return MarketSettings(timezone=ZoneInfo(tz_name), schedule_step_minutes=step)


def load_target_universe() -> frozenset[str]:
    """런타임 config의 ``target_universe`` 종목 코드 집합을 반환한다."""
    ensure_runtime_config()
    config_path = os.environ.get("INVESTBOT_CONFIG", "").strip()
    if not config_path:
        return frozenset()
    config = load_runtime_config(config_path)
    raw = config.get("target_universe") or []
    if not isinstance(raw, list):
        raise ValueError("target_universe must be a list of asset codes")
    return frozenset(str(code).strip() for code in raw if str(code).strip())
