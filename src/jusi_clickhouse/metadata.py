from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

from .state import read_json, write_json


METADATA_TTL_SECONDS = 60 * 60.0


@dataclass(frozen=True)
class CompletionObject:
    name: str
    schema: str
    kind: str
    detail: str = ""


@dataclass(frozen=True)
class CompletionColumn:
    schema: str
    table: str
    name: str
    data_type: str = ""


@dataclass
class MetadataSnapshot:
    schemas: list[str] = field(default_factory=list)
    objects: list[CompletionObject] = field(default_factory=list)
    columns: list[CompletionColumn] = field(default_factory=list)
    functions: list[CompletionObject] = field(default_factory=list)
    refreshed_at: float = 0.0

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "MetadataSnapshot":
        return cls(
            schemas=[str(item) for item in payload.get("schemas", []) if str(item)],
            objects=[
                CompletionObject(
                    name=str(item.get("name", "")),
                    schema=str(item.get("schema", "")),
                    kind=str(item.get("kind", "")),
                    detail=str(item.get("detail", "")),
                )
                for item in payload.get("objects", [])
                if isinstance(item, dict) and item.get("name")
            ],
            columns=[
                CompletionColumn(
                    schema=str(item.get("schema", "")),
                    table=str(item.get("table", "")),
                    name=str(item.get("name", "")),
                    data_type=str(item.get("data_type", "")),
                )
                for item in payload.get("columns", [])
                if isinstance(item, dict) and item.get("table") and item.get("name")
            ],
            functions=[
                CompletionObject(
                    name=str(item.get("name", "")),
                    schema=str(item.get("schema", "")),
                    kind="function",
                    detail=str(item.get("detail", "")),
                )
                for item in payload.get("functions", [])
                if isinstance(item, dict) and item.get("name")
            ],
            refreshed_at=float(payload.get("refreshed_at", 0.0) or 0.0),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schemas": self.schemas,
            "objects": [item.__dict__ for item in self.objects],
            "columns": [item.__dict__ for item in self.columns],
            "functions": [item.__dict__ for item in self.functions],
            "refreshed_at": self.refreshed_at,
        }


class MetadataCache:
    def __init__(self, cache_dir: Path, loader: Callable[[], MetadataSnapshot]) -> None:
        self.cache_dir = cache_dir
        self.cache_path = cache_dir / "metadata.json"
        self.loader = loader
        self._lock = threading.Lock()
        self._refreshing = False
        self._thread: threading.Thread | None = None
        self._snapshot = MetadataSnapshot.from_dict(read_json(self.cache_path))

    def snapshot(self) -> MetadataSnapshot:
        with self._lock:
            return self._snapshot

    def mark_stale(self) -> None:
        with self._lock:
            self._snapshot.refreshed_at = 0.0

    def ensure_fresh_async(self, *, force: bool = False) -> bool:
        with self._lock:
            if self._refreshing:
                return False
            age = time.time() - self._snapshot.refreshed_at if self._snapshot.refreshed_at else 10**9
            if not force and age < METADATA_TTL_SECONDS:
                return False
            self._refreshing = True
        thread = threading.Thread(target=self._refresh, daemon=True)
        with self._lock:
            self._thread = thread
        thread.start()
        return True

    def close(self, *, timeout: float = 2.0) -> bool:
        with self._lock:
            thread = self._thread
        if thread is None or not thread.is_alive():
            return True
        thread.join(timeout=timeout)
        return not thread.is_alive()

    def _refresh(self) -> None:
        try:
            try:
                snapshot = self.loader()
            except Exception:
                return
            else:
                snapshot.refreshed_at = time.time()
                write_json(self.cache_path, snapshot.to_dict())
                with self._lock:
                    self._snapshot = snapshot
        finally:
            with self._lock:
                self._refreshing = False


def load_clickhouse_metadata(client: Any) -> MetadataSnapshot:
    schemas = [str(row[0]) for row in _query_rows(client, "SELECT name FROM system.databases ORDER BY name")]
    object_rows = _query_rows(
        client,
        """
        SELECT database, name, engine
        FROM system.tables
        WHERE database NOT IN ('INFORMATION_SCHEMA', 'information_schema')
        ORDER BY database, name
        """,
    )
    objects = [
        CompletionObject(schema=str(row[0]), name=str(row[1]), kind=_table_kind(str(row[2])), detail=str(row[2]))
        for row in object_rows
    ]
    column_rows = _query_rows(
        client,
        """
        SELECT database, table, name, type
        FROM system.columns
        WHERE database NOT IN ('INFORMATION_SCHEMA', 'information_schema')
        ORDER BY database, table, position
        """,
    )
    columns = [
        CompletionColumn(schema=str(row[0]), table=str(row[1]), name=str(row[2]), data_type=str(row[3]))
        for row in column_rows
    ]
    function_rows = _query_rows(client, "SELECT name, origin FROM system.functions ORDER BY name")
    functions = [
        CompletionObject(schema="", name=str(row[0]), kind="function", detail=str(row[1]))
        for row in function_rows
    ]
    return MetadataSnapshot(schemas=schemas, objects=objects, columns=columns, functions=functions)


def _query_rows(client: Any, sql: str) -> list[Any]:
    result = client.query(sql)
    return list(getattr(result, "result_rows", []) or [])


def _table_kind(engine: str) -> str:
    normalized = engine.lower()
    if normalized == "view":
        return "view"
    if "dictionary" in normalized:
        return "dictionary"
    return "table"
