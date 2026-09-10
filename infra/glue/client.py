"""AWS Glue Catalog 클라이언트 래퍼."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class GlueConfig:
    """Glue Catalog 연결 설정."""

    database: str
    table: str
    region: str = "ap-northeast-2"
    endpoint_url: str | None = None
    aws_access_key_id: str | None = None
    aws_secret_access_key: str | None = None

    @classmethod
    def from_mapping(cls, data: dict[str, Any]) -> GlueConfig:
        database = str(data.get("database") or data.get("glue_database") or "")
        table = str(data.get("table") or data.get("glue_table") or "")
        return cls(
            database=database,
            table=table,
            region=str(data.get("region") or "ap-northeast-2"),
            endpoint_url=data.get("endpoint_url"),
            aws_access_key_id=data.get("aws_access_key_id"),
            aws_secret_access_key=data.get("aws_secret_access_key"),
        )


class GlueClient:
    """boto3 Glue client에 대한 래퍼."""

    def __init__(self, config: GlueConfig | dict[str, Any]) -> None:
        if isinstance(config, dict):
            config = GlueConfig.from_mapping(config)
        if not config.database or not config.table:
            raise ValueError("Glue database and table are required")
        self.config = config
        self._client: Any = None

    def connect(self) -> GlueClient:
        try:
            import boto3
        except ImportError as exc:
            raise ImportError("Glue backend requires boto3") from exc

        cfg = self.config
        session_kwargs: dict[str, Any] = {"region_name": cfg.region}
        if cfg.aws_access_key_id and cfg.aws_secret_access_key:
            session_kwargs["aws_access_key_id"] = cfg.aws_access_key_id
            session_kwargs["aws_secret_access_key"] = cfg.aws_secret_access_key

        session = boto3.session.Session(**session_kwargs)
        client_kwargs: dict[str, Any] = {}
        if cfg.endpoint_url:
            client_kwargs["endpoint_url"] = cfg.endpoint_url

        self._client = session.client("glue", **client_kwargs)
        return self

    @property
    def client(self) -> Any:
        if self._client is None:
            self.connect()
        return self._client

    def ensure_database(self) -> None:
        try:
            self.client.get_database(Name=self.config.database)
        except self.client.exceptions.EntityNotFoundException:
            self.client.create_database(DatabaseInput={"Name": self.config.database})

    def ensure_table(
        self,
        *,
        columns: list[dict[str, str]],
        s3_location: str,
        partition_keys: list[dict[str, str]],
    ) -> None:
        self.ensure_database()
        try:
            self.client.get_table(DatabaseName=self.config.database, Name=self.config.table)
            return
        except self.client.exceptions.EntityNotFoundException:
            pass

        self.client.create_table(
            DatabaseName=self.config.database,
            TableInput={
                "Name": self.config.table,
                "TableType": "EXTERNAL_TABLE",
                "Parameters": {"classification": "parquet", "EXTERNAL": "TRUE"},
                "PartitionKeys": partition_keys,
                "StorageDescriptor": {
                    "Columns": columns,
                    "Location": s3_location,
                    "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                    "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                    "SerdeInfo": {
                        "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                    },
                },
            },
        )

    def batch_create_partition(self, *, partition_values: list[str], s3_location: str) -> None:
        self.client.batch_create_partition(
            DatabaseName=self.config.database,
            TableName=self.config.table,
            PartitionInputList=[
                {
                    "Values": partition_values,
                    "StorageDescriptor": {
                        "Location": s3_location,
                        "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                        "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                        "SerdeInfo": {
                            "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                        },
                    },
                }
            ],
        )

    def partition_exists(self, partition_values: list[str]) -> bool:
        try:
            self.client.get_partition(
                DatabaseName=self.config.database,
                TableName=self.config.table,
                PartitionValues=partition_values,
            )
            return True
        except self.client.exceptions.EntityNotFoundException:
            return False

    def close(self) -> None:
        self._client = None

    def __enter__(self) -> GlueClient:
        self.connect()
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()
