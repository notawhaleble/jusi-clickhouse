from __future__ import annotations

import os
import threading
import zipfile
from io import BytesIO

from jusi.domain.models import JUSI_HANDLER_HANDOFF_MIME
from jusi.plugins import DisplayHandlerSpec

from jusi_clickhouse.completion import completion_items, parse_query_relations
from jusi_clickhouse.config import parse_clickhouse_options
from jusi_clickhouse.constants import CLICKHOUSE_BOOTSTRAP_SQL
from jusi_clickhouse.kernel import _parse_sql_line, _sql_blank_body_transformer, configure_sql_session, register_sql_magic
from jusi_clickhouse.metadata import CompletionColumn, CompletionObject, MetadataCache, MetadataSnapshot
from jusi_clickhouse.plugin import ClickHouseHandler, display_handler_specs
from jusi_clickhouse.runner import _iter_stream_rows, _looks_like_result_query, _refetch_cell_as_bytes, _write_raw_value_file
from jusi_clickhouse.state import target_cache_dir


def test_display_handler_spec_registers_sql_magic() -> None:
    specs = display_handler_specs()
    assert len(specs) == 1
    spec = specs[0]
    assert isinstance(spec, DisplayHandlerSpec)
    assert spec.handler_id == "clickhouse"
    assert spec.magic_commands[0].name == "sql"
    assert spec.kernel_extension_modules == ("jusi_clickhouse.kernel",)
    assert spec.presentation["completion"] is True


def test_bootstrap_body_is_safe_empty_result_query() -> None:
    assert ClickHouseHandler.bootstrap_cell_body("analytics") == "SELECT 1 AS jusi_bootstrap WHERE 0"


def test_result_query_detection_matches_clickhouse_result_statements() -> None:
    assert _looks_like_result_query("select 1") is True
    assert _looks_like_result_query("show databases") is True
    assert _looks_like_result_query("describe table events") is True
    assert _looks_like_result_query("insert into t values (1)") is False


def test_parse_clickhouse_options_keeps_driver_options_and_plugin_options() -> None:
    options = parse_clickhouse_options(
        {
            "provider": "clickhouse",
            "host": "db.example",
            "database": "analytics",
            "password": "secret",
            "initial_fetch": "25",
        }
    )
    assert options.initial_fetch == 25
    assert options.connect == {"host": "db.example", "database": "analytics", "password": "secret"}


def test_parse_sql_line_supports_initial_fetch_magic_arg() -> None:
    alias, options = _parse_sql_line("analytics --initial-fetch 7")
    assert alias == "analytics"
    assert options == {"initial_fetch": 7}


def test_kernel_substitutes_clickhouse_blank_body_bootstrap(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    captured: list[dict[str, object]] = []

    class FakeMagicsManager:
        magics = {"cell": {}}

    class FakeIp:
        magics_manager = FakeMagicsManager()

        def register_magic_function(self, func, *, magic_kind: str, magic_name: str) -> None:  # type: ignore[no-untyped-def]
            assert magic_kind == "cell"
            assert magic_name == "sql"
            self.magic = func

    def fake_display(payload, *, raw=False, metadata=None):  # type: ignore[no-untyped-def]
        captured.append({"payload": payload, "raw": raw, "metadata": metadata})

    monkeypatch.setattr("IPython.display.display", fake_display)
    configure_sql_session({"sql": {"local_ch": {"provider": "clickhouse", "host": "127.0.0.1"}}})
    fake_ip = FakeIp()
    register_sql_magic(fake_ip)

    fake_ip.magic("local_ch", "\n")

    handoff = captured[0]["payload"][JUSI_HANDLER_HANDOFF_MIME]  # type: ignore[index]
    assert handoff["content"] == CLICKHOUSE_BOOTSTRAP_SQL


def test_kernel_transformer_adds_body_for_header_only_clickhouse_magic() -> None:
    configure_sql_session({"sql": {"local_ch": {"provider": "clickhouse", "host": "127.0.0.1"}}})
    assert _sql_blank_body_transformer(["%%sql local_ch\n"]) == [
        "%%sql local_ch\n",
        CLICKHOUSE_BOOTSTRAP_SQL + "\n",
    ]


def test_target_cache_dir_redacts_secret_values(monkeypatch, tmp_path) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("JUSI_STATE_HOME", str(tmp_path))
    first = target_cache_dir("analytics", {"host": "db", "password": "one"})
    second = target_cache_dir("analytics", {"host": "db", "password": "two"})
    assert first == second
    assert str(first).startswith(str(tmp_path / "plugins" / "clickhouse" / "analytics"))


def test_parse_query_relations_extracts_aliases() -> None:
    relations = parse_query_relations(
        "select u.id, o.total from analytics.users u join orders as o on o.user_id = u.id"
    )
    assert ("analytics", "users", "u") in [(item.schema, item.table, item.alias) for item in relations]
    assert ("", "orders", "o") in [(item.schema, item.table, item.alias) for item in relations]


def test_completion_items_include_alias_columns() -> None:
    snapshot = MetadataSnapshot(
        schemas=["analytics"],
        objects=[CompletionObject(schema="analytics", name="users", kind="table", detail="MergeTree")],
        columns=[CompletionColumn(schema="analytics", table="users", name="email", data_type="String")],
        functions=[CompletionObject(schema="", name="lower", kind="function", detail="system")],
        refreshed_at=1.0,
    )
    items = completion_items(
        snapshot,
        {
            "current_word": "e",
            "cursor_col": len("select u.e"),
            "line_text": "select u.e",
            "cell_text": "%%sql local_ch\nselect u.e from analytics.users u",
        },
    )
    alias_item = next(item for item in items if item["value"] == "u.email" and item["kind"] == "column")
    assert alias_item["start_col"] == len("select ")
    assert alias_item["end_col"] == len("select u.e")


def test_completion_span_handles_vim_cursor_col_after_word() -> None:
    snapshot = MetadataSnapshot(
        schemas=["analytics"],
        objects=[CompletionObject(schema="analytics", name="items", kind="table")],
        columns=[CompletionColumn(schema="analytics", table="items", name="value", data_type="String")],
        functions=[],
        refreshed_at=1.0,
    )
    items = completion_items(
        snapshot,
        {
            "current_word": "value",
            "cursor_col": 13,
            "line_text": "select value from items",
            "cell_text": "%%sql local_ch\nselect value from items",
        },
    )
    value_item = next(item for item in items if item["value"] == "value" and item["kind"] == "column")
    assert value_item["start_col"] == len("select ")
    assert value_item["end_col"] == len("select value")


def test_completion_after_from_space_uses_empty_cursor_span_and_relation_items() -> None:
    snapshot = MetadataSnapshot(
        schemas=["INFORMATION_SCHEMA", "demo"],
        objects=[CompletionObject(schema="demo", name="accounts", kind="table")],
        columns=[],
        functions=[CompletionObject(schema="", name="FROM_BASE64", kind="function", detail="system")],
        refreshed_at=1.0,
    )
    line_text = "select * FROM "
    items = completion_items(
        snapshot,
        {
            "current_word": "FROM",
            "cursor_col": len(line_text),
            "line_text": line_text,
            "cell_text": f"%%sql local_ch\n{line_text}",
        },
    )

    values = {item["value"] for item in items}
    assert "FROM_BASE64" not in values
    assert "INFORMATION_SCHEMA" in values
    assert "demo.accounts" in values
    for item in items:
        assert item["start_col"] == len(line_text)
        assert item["end_col"] == len(line_text)


def test_handler_forwards_complete_payload_with_cursor_context() -> None:
    handler = ClickHouseHandler()
    calls: list[tuple[str, dict[str, object]]] = []

    class FakeContext:
        def call_backend_action(self, action_name: str, payload: dict[str, object]) -> dict[str, object]:
            calls.append((action_name, payload))
            return {"items": [{"value": "value", "kind": "column"}]}

    payload = {
        "cell_text": "%%sql local_ch\nselect value from demo.items",
        "cursor_row": 1,
        "cursor_col": 12,
        "line_text": "select value",
        "current_word": "value",
    }

    items = handler.complete(FakeContext(), payload)  # type: ignore[arg-type]

    assert calls == [
        (
            "plugin_runtime_request",
            {
                "message_type": "complete",
                "payload": payload,
            },
        )
    ]
    assert items[0]["start_col"] == len("select ")
    assert items[0]["end_col"] == len("select value")


def test_iter_stream_rows_supports_block_and_row_streams() -> None:
    assert list(_iter_stream_rows([[("a", 1), ("b", 2)]])) == [("a", 1), ("b", 2)]
    assert list(_iter_stream_rows([("a", 1), ("b", 2)])) == [("a", 1), ("b", 2)]
    assert list(_iter_stream_rows([["a", 1]])) == [["a", 1]]


def test_write_raw_value_file_preserves_binary_bytes() -> None:
    path = _write_raw_value_file(b"PK\x05\x06" + (b"\x00" * 18), ".zip")
    with open(path, "rb") as handle:
        assert handle.read(4) == b"PK\x05\x06"
    os.unlink(path)


def test_write_raw_value_file_preserves_clickhouse_binary_string_for_zip() -> None:
    buffer = BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("blob-readme.txt", "Jusi ClickHouse blob fixture\n")
    raw_zip = buffer.getvalue()
    value = raw_zip.decode("latin-1")

    path = _write_raw_value_file(value, ".zip")

    with zipfile.ZipFile(path) as archive:
        assert archive.namelist() == ["blob-readme.txt"]
    os.unlink(path)


def test_write_raw_value_file_keeps_text_suffix_as_utf8_text() -> None:
    path = _write_raw_value_file("cafe \N{SNOWMAN}", ".txt")
    with open(path, encoding="utf-8") as handle:
        assert handle.read() == "cafe \N{SNOWMAN}"
    os.unlink(path)


def test_refetch_cell_as_bytes_uses_clickhouse_column_format() -> None:
    class FakeResult:
        result_rows = [(b"PK\x03\x04raw zip bytes",)]

    class FakeClient:
        def __init__(self) -> None:
            self.calls: list[dict[str, object]] = []

        def query(self, sql: str, **kwargs: object) -> FakeResult:
            self.calls.append({"sql": sql, **kwargs})
            return FakeResult()

    class FakeSession:
        def __init__(self) -> None:
            self.lock = threading.RLock()
            self.client = FakeClient()

        def connect(self) -> FakeClient:
            return self.client

    class FakeSheet:
        query = "SELECT payload FROM demo.blob_files;"

        def __init__(self) -> None:
            self.session = FakeSession()

    sheet = FakeSheet()

    value = _refetch_cell_as_bytes(sheet, column_name="payload", row_index=3)

    assert value == b"PK\x03\x04raw zip bytes"
    call = sheet.session.client.calls[0]
    assert "FROM (SELECT payload FROM demo.blob_files)" in str(call["sql"])
    assert "LIMIT 1 OFFSET 3" in str(call["sql"])
    assert call["column_formats"] == {"payload": "bytes"}


def test_metadata_snapshot_does_not_refresh_until_cell_entry(tmp_path) -> None:  # type: ignore[no-untyped-def]
    calls: list[bool] = []

    def load() -> MetadataSnapshot:
        calls.append(True)
        return MetadataSnapshot(schemas=["analytics"])

    cache = MetadataCache(tmp_path, load)
    assert cache.snapshot().schemas == []
    assert calls == []
    assert cache.ensure_fresh_async() is True
    assert cache.close(timeout=2.0) is True
    assert calls == [True]
    assert cache.snapshot().schemas == ["analytics"]


def test_metadata_refresh_deduplicates_concurrent_cell_entries(tmp_path) -> None:  # type: ignore[no-untyped-def]
    started = threading.Event()
    release = threading.Event()
    calls: list[bool] = []

    def load() -> MetadataSnapshot:
        calls.append(True)
        started.set()
        release.wait(timeout=2.0)
        return MetadataSnapshot(schemas=["analytics"])

    cache = MetadataCache(tmp_path, load)
    assert cache.ensure_fresh_async() is True
    assert started.wait(timeout=2.0)
    assert cache.ensure_fresh_async() is False
    release.set()
    assert cache.close(timeout=2.0) is True
    assert calls == [True]
