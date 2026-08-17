""":class:`MySQLClient` 기반 테이블 스코프 CRUD."""

from __future__ import annotations

import json
import re
from typing import Any

from .client import MySQLClient

_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class CRUD:
    """단일 테이블에 대한 최소 CRUD."""

    def __init__(
        self,
        client: MySQLClient,
        table: str,
        *,
        pk: str = "id",
    ) -> None:
        self.client = client
        self.table = self._validate_ident(table, "table")
        self.pk = self._validate_ident(pk, "primary key")

    def create(self, data: dict[str, Any], *, conn: Any | None = None) -> dict[str, Any]:
        if not data:
            raise ValueError("data must not be empty")
        if self.pk not in data:
            raise ValueError(f"Missing primary key {self.pk!r}")

        row = self._prepare(data)
        cols = list(row.keys())
        sql = (
            f"INSERT INTO `{self.table}` "
            f"({', '.join(f'`{c}`' for c in cols)}) "
            f"VALUES ({', '.join('%s' for _ in cols)})"
        )
        self.client.execute(sql, [row[c] for c in cols], conn=conn)
        return row

    def get(self, pk_value: Any, *, conn: Any | None = None) -> dict[str, Any] | None:
        sql = f"SELECT * FROM `{self.table}` WHERE `{self.pk}` = %s"
        row = self.client.fetchone(sql, (pk_value,), conn=conn)
        return self._restore(row) if row is not None else None

    def list(
        self,
        *,
        where: str = "",
        params: tuple[Any, ...] = (),
        order_by: str = "",
        limit: int | None = None,
        conn: Any | None = None,
    ) -> list[dict[str, Any]]:
        sql = f"SELECT * FROM `{self.table}`"
        if where:
            sql += f" WHERE {where}"
        if order_by:
            sql += f" ORDER BY {order_by}"
        if limit is not None:
            sql += " LIMIT %s"
            params = (*params, limit)
        return [
            self._restore(row)
            for row in self.client.fetchall(sql, params, conn=conn)
        ]

    def update(
        self,
        pk_value: Any,
        data: dict[str, Any],
        *,
        conn: Any | None = None,
    ) -> bool:
        if not data:
            raise ValueError("data must not be empty")

        row = self._prepare(data)
        sets = [c for c in row if c != self.pk]
        if not sets:
            return False

        sql = (
            f"UPDATE `{self.table}` "
            f"SET {', '.join(f'`{c}` = %s' for c in sets)} "
            f"WHERE `{self.pk}` = %s"
        )
        cur = self.client.execute(
            sql,
            [*[row[c] for c in sets], pk_value],
            conn=conn,
        )
        return cur.rowcount > 0

    def delete(self, pk_value: Any, *, conn: Any | None = None) -> bool:
        cur = self.client.execute(
            f"DELETE FROM `{self.table}` WHERE `{self.pk}` = %s",
            (pk_value,),
            conn=conn,
        )
        return cur.rowcount > 0

    def upsert(self, data: dict[str, Any], *, conn: Any | None = None) -> dict[str, Any]:
        if not data:
            raise ValueError("data must not be empty")
        if self.pk not in data:
            raise ValueError(f"Missing primary key {self.pk!r}")

        row = self._prepare(data)
        cols = list(row.keys())
        updates = [c for c in cols if c != self.pk]
        sql = (
            f"INSERT INTO `{self.table}` ({', '.join(f'`{c}`' for c in cols)}) "
            f"VALUES ({', '.join('%s' for _ in cols)}) "
            f"ON DUPLICATE KEY UPDATE "
            f"{', '.join(f'`{c}` = VALUES(`{c}`)' for c in updates)}"
        )
        self.client.execute(sql, [row[c] for c in cols], conn=conn)
        return row

    def _prepare(self, data: dict[str, Any]) -> dict[str, Any]:
        row: dict[str, Any] = {}
        for key, value in data.items():
            col = self._validate_ident(key, "column")
            if isinstance(value, (dict, list)):
                row[col] = json.dumps(value, ensure_ascii=False, default=str)
            else:
                row[col] = value
        return row

    def _restore(self, row: dict[str, Any]) -> dict[str, Any]:
        restored: dict[str, Any] = {}
        for key, value in row.items():
            if isinstance(value, str) and value and value[0] in "{[":
                try:
                    restored[key] = json.loads(value)
                    continue
                except json.JSONDecodeError:
                    pass
            restored[key] = value
        return restored

    @staticmethod
    def _validate_ident(name: str, kind: str) -> str:
        if not _IDENT.fullmatch(name):
            raise ValueError(f"Invalid SQL {kind} name: {name!r}")
        return name
