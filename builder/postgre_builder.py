from typing import Any
class PostgreBuilder:
    def __init__(self, table: str):
        self.table = table
        self._columns = []
        self._values = []

    def insert(self, **kwargs) -> "PostgreBuilder":
        self._columns = list(kwargs.keys())
        self._values = list(kwargs.values())
        return  self

    def returning(self, *columns) -> "PostgreBuilder":
        self._returning = list(columns)
        return self

    def build(self) -> tuple[str, tuple]:
        placeholders = ", ".join(["%s"] * len(self._columns))
        cols = ", ".join(self._columns)
        query = f"INSERT INTO {self.table} ({cols}) VALUES ({placeholders})"
        if hasattr(self, "_returning"):
            query += f" RETURNING {', '.join(self._returning)}"

        return query, tuple(self._values)
