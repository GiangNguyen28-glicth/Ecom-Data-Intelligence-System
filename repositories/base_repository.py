import dataclasses
from abc import ABC, abstractmethod
from dataclasses import asdict
from typing import TypeVar, Generic, Any, Optional
import psycopg2.extras

from builder.postgres_builder import PostgresBuilder

T = TypeVar("T")


class BaseRepository(ABC, Generic[T]):
    def __init__(self, conn):
        self.conn = conn

    @property
    @abstractmethod
    def table(self) -> str:
        ...

    @property
    @abstractmethod
    def model(self) -> type[T]:
        ...

    def _from_row(self, row: dict) -> T:
        field_names = {f.name for f in dataclasses.fields(self.model)}
        filtered = {k: v for k, v in row.items() if k in field_names}
        return self.model(**filtered)

    @property
    def primary_key(self) -> str:
        return "id"

    def _qb(self) -> PostgresBuilder:
        return PostgresBuilder(self.table)

    def _build_insert(self, data: T, exclude: set[str] = set()) -> tuple[str, dict]:
        data_dict = {
            k: v for k, v in asdict(data).items()
            if k not in exclude and v is not None
        }
        cols = ", ".join(data_dict.keys())
        placeholders = ", ".join(f"%({k})s" for k in data_dict.keys())
        query = f"INSERT INTO {self.table} ({cols}) VALUES ({placeholders}) RETURNING *"
        return query, data_dict

    def _execute(self, query: str, params: Any = None) -> list[dict]:
        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
            cursor.execute(query, params)
            self.conn.commit()
            try:
                return cursor.fetchall()
            except Exception:
                return []

    def find_by_id(self, record_id: Any) -> Optional[T]:
        query, params = (
            self._qb()
            .where(**{self.primary_key: record_id})
            .limit(1)
            .build_select()
        )
        rows = self._execute(query, params)
        return self._from_row(rows[0]) if rows else None

    def find_all(self, limit: int = 100, offset: int = 0) -> list[T]:
        query, params = (
            self._qb()
            .limit(limit)
            .offset(offset)
            .build_select()
        )
        rows = self._execute(query, params)
        return [self._from_row(row) for row in rows]

    def find_by(self, **filters) -> list[T]:
        query, params = (
            self._qb()
            .where(**filters)
            .build_select()
        )
        rows = self._execute(query, params)
        return [self._from_row(row) for row in rows]

    def create(self, data: T) -> T:
        query, params = (
            self._qb()
            .returning("*")
            .build_insert(data)
        )
        print("query:", query)
        print("params:", params)
        rows = self._execute(query, params)
        return self._from_row(rows[0])

    def update(self, data: T, exclude: set[str] = set()) -> Optional[T]:
        query, params = (
            self._qb()
            .exclude(*exclude)
            .returning("*")
            .build_update(data, self.primary_key)
        )
        print(query)
        rows = self._execute(query, params)
        return self._from_row(rows[0]) if rows else None

    def upsert(self, data: T, conflict_fields: list[str], exclude: set[str] = {"id"}) -> T:
        query, params = (
            self._qb()
            .exclude(*exclude)
            .returning("*")
            .build_upsert(data, conflict_fields)
        )
        rows = self._execute(query, params)
        return self._from_row(rows[0])

    def delete(self, record_id: Any) -> bool:
        query, params = (
            self._qb()
            .where(**{self.primary_key: record_id})
            .returning(self.primary_key)
            .build_delete()
        )
        rows = self._execute(query, params)
        return len(rows) > 0

    def exists(self, **filters) -> bool:
        query, params = (
            self._qb()
            .where(**filters)
            .limit(1)
            .build_select(columns=["1"])
        )
        rows = self._execute(query, params)
        return len(rows) > 0
