"""HTTP client errors."""


class HttpError(Exception):
    """HTTP layer base error."""


class NetworkError(HttpError):
    """Network or transport failure."""


class HttpResponseError(HttpError):
    """Non-success HTTP response."""

    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        body: str = "",
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.body = body


class JsonDecodeError(HttpError):
    """Response body is not valid JSON."""
