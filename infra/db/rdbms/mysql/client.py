"""MySQL connection pool wrapper."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Iterator

_IDENT_POOL = "investbot_pool"


@dataclass(frozen=True)
class MySQLPoolConfig:
    """MySQL pool connection settings."""

    host: str = "localhost"
    port: int = 3306
    user: str = ""
    password: str = ""
    database: str = ""
    pool_name: str = _IDENT_POOL
    pool_size: int = 5
    charset: str = "utf8mb4"
    autocommit: bool = False

    @classmethod
    def from_mapping(cls, data: dict[str, Any]) -> MySQLPoolConfig:
        return cls(
            host=str(data.get("host") or "localhost"),
            port=int(data.get("port") or 3306),
            user=str(data.get("user") or ""),
            password=str(data.get("password") or ""),
            database=str(data.get("database") or ""),
            pool_name=str(data.get("pool_name") or _IDENT_POOL),
            pool_size=int(data.get("pool_size") or 5),
            charset=str(data.get("charset") or "utf8mb4"),
            autocommit=bool(data.get("autocommit", False)),
        )


class MySQLClient:
    """MySQL connection pool에 대한 얇은 래퍼."""

    def __init__(self, config: MySQLPoolConfig | dict[str, Any]) -> None:
        if isinstance(config, dict):
            config = MySQLPoolConfig.from_mapping(config)
        self.config = config
        self._pool: Any = None

    def connect(self) -> MySQLClient:
        try:
            from mysql.connector import pooling
        except ImportError as exc:
            raise ImportError(
                "MySQL backend requires mysql-connector-python"
            ) from exc

        cfg = self.config
        pool_kwargs: dict[str, Any] = {
            "pool_name": cfg.pool_name,
            "pool_size": cfg.pool_size,
            "host": cfg.host,
            "port": cfg.port,
            "user": cfg.user,
            "password": cfg.password,
            "charset": cfg.charset,
            "autocommit": cfg.autocommit,
        }
        if cfg.database:
            pool_kwargs["database"] = cfg.database

        self._pool = pooling.MySQLConnectionPool(**pool_kwargs)
        return self

    @property
    def pool(self) -> Any:
        if self._pool is None:
            self.connect()
        return self._pool

    def connection(self) -> Any:
        return self.pool.get_connection()

    def close(self) -> None:
        self._pool = None

    def __enter__(self) -> MySQLClient:
        self.connect()
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def execute(
        self,
        sql: str,
        params: Iterable[Any] = (),
        *,
        conn: Any | None = None,
    ) -> Any:
        own_conn = conn is None
        connection = conn or self.connection()
        cursor = connection.cursor(dictionary=True)
        try:
            cursor.execute(sql, tuple(params))
            return cursor
        finally:
            cursor.close()
            if own_conn:
                connection.close()

    def executemany(
        self,
        sql: str,
        params: Iterable[Iterable[Any]],
        *,
        conn: Any | None = None,
    ) -> Any:
        own_conn = conn is None
        connection = conn or self.connection()
        cursor = connection.cursor()
        try:
            cursor.executemany(sql, [tuple(row) for row in params])
            return cursor
        finally:
            cursor.close()
            if own_conn:
                connection.close()

    def fetchone(
        self,
        sql: str,
        params: Iterable[Any] = (),
        *,
        conn: Any | None = None,
    ) -> dict[str, Any] | None:
        own_conn = conn is None
        connection = conn or self.connection()
        cursor = connection.cursor(dictionary=True)
        try:
            cursor.execute(sql, tuple(params))
            row = cursor.fetchone()
            return dict(row) if row is not None else None
        finally:
            cursor.close()
            if own_conn:
                connection.close()

    def fetchall(
        self,
        sql: str,
        params: Iterable[Any] = (),
        *,
        conn: Any | None = None,
    ) -> list[dict[str, Any]]:
        own_conn = conn is None
        connection = conn or self.connection()
        cursor = connection.cursor(dictionary=True)
        try:
            cursor.execute(sql, tuple(params))
            return [dict(row) for row in cursor.fetchall()]
        finally:
            cursor.close()
            if own_conn:
                connection.close()

    def iterrows(
        self,
        sql: str,
        params: Iterable[Any] = (),
        *,
        conn: Any | None = None,
    ) -> Iterator[dict[str, Any]]:
        own_conn = conn is None
        connection = conn or self.connection()
        cursor = connection.cursor(dictionary=True)
        try:
            cursor.execute(sql, tuple(params))
            for row in cursor:
                yield dict(row)
        finally:
            cursor.close()
            if own_conn:
                connection.close()

    def commit(self, conn: Any) -> None:
        conn.commit()

    def rollback(self, conn: Any) -> None:
        conn.rollback()
