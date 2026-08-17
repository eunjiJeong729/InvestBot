"""Domain entity dataclasses — d_*, s_*, f_* 테이블과 1:1 매핑."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, ClassVar, Literal

EntityType = Literal["account", "asset", "sector", "index", "theme", "ohlcv"]
OrderAction = Literal["long", "short"]


@dataclass
class Account:
    """Dimension: ``d_account``."""

    TABLE: ClassVar[str] = "d_account"

    id: str
    number: str
    source: str
    name: str
    description: str = ""
    created_datetime: datetime | None = None
    opened_datetime: datetime | None = None
    closed_datetime: datetime | None = None


@dataclass
class Asset:
    """Dimension: ``d_asset``."""

    TABLE: ClassVar[str] = "d_asset"

    id: str
    source: str
    name: str
    ticker: str
    type: str
    description: str = ""
    region: str = ""
    created_datetime: datetime | None = None
    listed_datetime: datetime | None = None
    delisted_datetime: datetime | None = None
    details: dict[str, Any] = field(default_factory=dict)


@dataclass
class Sector:
    """Dimension: ``d_sector``."""

    TABLE: ClassVar[str] = "d_sector"

    id: str
    name: str
    source: str
    description: str = ""
    details: dict[str, Any] = field(default_factory=dict)


@dataclass
class Index:
    """Dimension: ``d_index``."""

    TABLE: ClassVar[str] = "d_index"

    id: str
    name: str
    source: str
    description: str = ""
    details: dict[str, Any] = field(default_factory=dict)


@dataclass
class Theme:
    """Dimension: ``d_theme``."""

    TABLE: ClassVar[str] = "d_theme"

    id: str
    name: str
    source: str
    description: str = ""
    details: dict[str, Any] = field(default_factory=dict)


@dataclass
class OHLCV:
    """Fact: ``f_ohlcv`` (자산별 파티션/테이블은 서비스 레이어에서 결정)."""

    TABLE: ClassVar[str] = "f_ohlcv"

    open: float
    high: float
    low: float
    close: float
    volume: float
    datetime: datetime | None = None
    asset_id: str = ""
    modified_datetime: datetime | None = None


@dataclass
class DMarketAssetMaster:
    """Dimension: ``d_market_asset_master``."""

    TABLE: ClassVar[str] = "d_market_asset_master"

    asset_type: str
    asset_code: str
    asset_name: str
    is_tradable: int = 1
    is_target: int = 0
    created_at: datetime | None = None
    updated_at: datetime | None = None


@dataclass
class SMarketOhlcv:
    """Stage: ``s_market_ohlcv``."""

    TABLE: ClassVar[str] = "s_market_ohlcv"

    asset_type: str
    asset_code: str
    market_time: datetime
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    volume: int
    created_at: datetime | None = None


@dataclass
class SMarketOhlcvHistory:
    """Stage history: ``s_market_ohlcv_history``."""

    TABLE: ClassVar[str] = "s_market_ohlcv_history"

    asset_type: str
    asset_code: str
    market_time: datetime
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    volume: int
    partition_date: date
    partition_hour: int
    created_at: datetime | None = None


# Staging 테이블 접두사 — 서비스별 SQL 모듈에서 ``s_{service}_{entity}`` 형태로 사용
STAGING_PREFIX = "s_"
