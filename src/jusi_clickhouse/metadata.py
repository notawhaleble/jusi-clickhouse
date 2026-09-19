from __future__ import annotations

from typing import Any

from jusi_sql import CompletionColumn, CompletionObject, MetadataSnapshot


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
