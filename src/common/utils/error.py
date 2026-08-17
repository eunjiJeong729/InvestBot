"""마켓 클라이언트 예외."""


class MarketClientError(Exception):
    """마켓 클라이언트 기본 예외."""


class InvalidTimePeriodError(MarketClientError):
    """시작/종료 시각 또는 interval이 잘못된 경우."""


class AssetNotFoundError(MarketClientError):
    """요청한 자산이 없거나 조회할 수 없는 경우."""


class DataFetchError(MarketClientError):
    """과거 데이터 조회에 실패한 경우."""


class ApiError(MarketClientError):
    """브로커 API 오류 또는 네트워크 오류."""
