from dataclasses import asdict
from typing import Any, Optional

class PostgresBuilder:
    def __init__(self, table: str):
        self.table       = table
        self._exclude    : set[str]  = set()
        self._returning  : list[str] = []
        self._where      : dict      = {}
        self._limit      : Optional[int] = None
        self._offset     : Optional[int] = None
        self._order_by   : Optional[str] = None

    # ------------------------------------------------------------------ #
    #  Chainable options                                                   #
    # ------------------------------------------------------------------ #
    def exclude(self, *fields: str) -> "PostgresBuilder":
        self._exclude = set(fields)
        return self

    def returning(self, *columns: str) -> "PostgresBuilder":
        self._returning = list(columns) if columns else ["*"]
        return self

    def where(self, **filters) -> "PostgresBuilder":
        self._where = filters
        return self

    def limit(self, n: int) -> "PostgresBuilder":
        self._limit = n
        return self

    def offset(self, n: int) -> "PostgresBuilder":
        self._offset = n
        return self

    def order_by(self, column: str, direction: str = "ASC") -> "PostgresBuilder":
        self._order_by = f"{column} {direction.upper()}"
        return self

    def _returning_clause(self) -> str:
        if not self._returning:
            return ""
        return f" RETURNING {', '.join(self._returning)}"

    def _where_clause(self, params: dict) -> tuple[str, dict]:
        if not self._where:
            return "", params
        clause = " AND ".join(f"{k} = %(where_{k})s" for k in self._where)
        params = {**params, **{f"where_{k}": v for k, v in self._where.items()}}
        return f" WHERE {clause}", params

    def _filter_data(self, data: Any) -> dict:
        return {
            k: v for k, v in asdict(data).items()
            if k not in self._exclude and v is not None
        }

    def build_insert(self, data: Any) -> tuple[str, dict]:
        data_dict    = self._filter_data(data)
        cols         = ", ".join(data_dict.keys())
        placeholders = ", ".join(f"%({k})s" for k in data_dict.keys())
        query        = f"INSERT INTO {self.table} ({cols}) VALUES ({placeholders})"
        query       += self._returning_clause()
        return query, data_dict

    def build_update(self, data: Any, where_field: str) -> tuple[str, dict]:
        data_dict  = {
            k: v for k, v in asdict(data).items()
            if k not in self._exclude and k != where_field and v is not None
        }
        set_clause   = ", ".join(f"{k} = %({k})s" for k in data_dict.keys())
        where_value  = getattr(data, where_field)
        params       = {**data_dict, "where_value": where_value}
        query        = f"UPDATE {self.table} SET {set_clause} WHERE {where_field} = %(where_value)s"
        query       += self._returning_clause()
        return query, params

    def build_upsert(self, data: Any, conflict_fields: list[str]) -> tuple[str, dict]:
        data_dict = self._filter_data(data)
        cols = ", ".join(data_dict.keys())
        placeholders = ", ".join(f"%({k})s" for k in data_dict.keys())

        update_cols = [k for k in data_dict if k not in conflict_fields]
        set_clause = ", ".join(f"{k} = EXCLUDED.{k}" for k in update_cols)

        conflict = ", ".join(conflict_fields)

        query = f"INSERT INTO {self.table} ({cols}) VALUES ({placeholders})"
        query += f" ON CONFLICT ({conflict}) DO UPDATE SET {set_clause}"
        query += self._returning_clause()

        return query, data_dict

    def build_select(self, columns: list[str] = None) -> tuple[str, dict]:
        cols   = ", ".join(columns) if columns else "*"
        query  = f"SELECT {cols} FROM {self.table}"
        params = {}

        where_sql, params = self._where_clause(params)
        query += where_sql

        if self._order_by:
            query += f" ORDER BY {self._order_by}"
        if self._limit is not None:
            query += f" LIMIT {self._limit}"
        if self._offset is not None:
            query += f" OFFSET {self._offset}"

        return query, params

    def build_delete(self) -> tuple[str, dict]:
        params    = {}
        where_sql, params = self._where_clause(params)
        query     = f"DELETE FROM {self.table}{where_sql}"
        query    += self._returning_clause()
        return query, params


