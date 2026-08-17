"""S3 client wrapper."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class S3Config:
    """S3 connection settings."""

    bucket: str
    region: str = "ap-northeast-2"
    prefix: str = ""
    endpoint_url: str | None = None
    aws_access_key_id: str | None = None
    aws_secret_access_key: str | None = None

    @classmethod
    def from_mapping(cls, data: dict[str, Any]) -> S3Config:
        return cls(
            bucket=str(data.get("bucket") or ""),
            region=str(data.get("region") or "ap-northeast-2"),
            prefix=str(data.get("prefix") or "").strip("/"),
            endpoint_url=data.get("endpoint_url"),
            aws_access_key_id=data.get("aws_access_key_id"),
            aws_secret_access_key=data.get("aws_secret_access_key"),
        )

    def object_key(self, key: str) -> str:
        normalized = key.strip("/")
        if not self.prefix:
            return normalized
        return f"{self.prefix}/{normalized}" if normalized else self.prefix


class S3Client:
    """boto3 S3 client에 대한 얇은 래퍼."""

    def __init__(self, config: S3Config | dict[str, Any]) -> None:
        if isinstance(config, dict):
            config = S3Config.from_mapping(config)
        if not config.bucket:
            raise ValueError("S3 bucket is required")
        self.config = config
        self._client: Any = None

    def connect(self) -> S3Client:
        try:
            import boto3
        except ImportError as exc:
            raise ImportError("S3 backend requires boto3") from exc

        cfg = self.config
        session_kwargs: dict[str, Any] = {"region_name": cfg.region}
        if cfg.aws_access_key_id and cfg.aws_secret_access_key:
            session_kwargs["aws_access_key_id"] = cfg.aws_access_key_id
            session_kwargs["aws_secret_access_key"] = cfg.aws_secret_access_key

        session = boto3.session.Session(**session_kwargs)
        client_kwargs: dict[str, Any] = {}
        if cfg.endpoint_url:
            client_kwargs["endpoint_url"] = cfg.endpoint_url

        self._client = session.client("s3", **client_kwargs)
        return self

    @property
    def client(self) -> Any:
        if self._client is None:
            self.connect()
        return self._client

    def put_object(
        self,
        key: str,
        body: bytes,
        *,
        content_type: str | None = None,
        metadata: dict[str, str] | None = None,
    ) -> str:
        object_key = self.config.object_key(key)
        kwargs: dict[str, Any] = {
            "Bucket": self.config.bucket,
            "Key": object_key,
            "Body": body,
        }
        if content_type:
            kwargs["ContentType"] = content_type
        if metadata:
            kwargs["Metadata"] = metadata

        self.client.put_object(**kwargs)
        return object_key

    def head_object(self, key: str) -> dict[str, Any]:
        return self.client.head_object(
            Bucket=self.config.bucket,
            Key=self.config.object_key(key),
        )

    def object_exists(self, key: str) -> bool:
        try:
            self.head_object(key)
            return True
        except self.client.exceptions.ClientError as exc:
            code = exc.response.get("Error", {}).get("Code")
            if code in {"404", "NoSuchKey", "NotFound"}:
                return False
            raise

    def close(self) -> None:
        self._client = None

    def __enter__(self) -> S3Client:
        self.connect()
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()
