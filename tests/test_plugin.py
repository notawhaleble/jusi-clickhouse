from __future__ import annotations

import importlib
import os
import sys
import threading
import zipfile
from io import BytesIO
from pathlib import Path
from types import SimpleNamespace

import pytest

from jusi.plugin_api import OperationRejected, WorkerContext, validate_discovered_entry
from jusi.protocol import validate_plugin_kernel_message
from jusi_sql import CompletionColumn, CompletionObject, MetadataSnapshot, SqlCompletionRequest, find_sql_actions
from jusi_sql.kernel import _reset_runtime_for_tests, dispatch_sql

from jusi_clickhouse.catalog import catalog_entry
from jusi_clickhouse.config import parse_clickhouse_options
from jusi_clickhouse.constants import CLICKHOUSE_BOOTSTRAP_SQL
from jusi_clickhouse.ipc import ApplicationController, WorkerApplicationBridge
from jusi_clickhouse.metadata import load_clickhouse_metadata
from jusi_clickhouse.runner import (
    ClickHouseResultSheet,
    _complete_clickhouse_sql,
    _followup_sql,
    _initialize_visidata_application,
    _iter_stream_rows,
    _looks_like_result_query,
    _refetch_cell_as_bytes,
    _write_raw_value_file,
)
from jusi_clickhouse.worker import ClickHouseClientSession, create_worker


def worker_context() -> WorkerContext:
    return WorkerContext("worker_1", "runtime_1", "clickhouse", "sql", "client_1", "execution_1")


def test_catalog_is_an_exact_jusi_1_sql_provider() -> None:
    assert catalog_entry() == {
        "plugin_id": "clickhouse",
        "plugin_version": "0.2.0",
        "distribution": "jusi-clickhouse",
        "families": [{
            "family_id": "sql",
            "magic_name": "sql",
            "capabilities": ["execute", "followup", "complete", "interrupt", "editor_actions"],
            "presentation": {"syntax": "sql", "indent": "sql"},
            "provider_presentation": {"syntax": "clickhouse", "indent": "sql"},
        }],
        "kernel_extensions": ["jusi_clickhouse.kernel"],
        "worker_entry_point": "jusi_clickhouse.worker:create_worker",
        "media_types": ["text/x-ansi"],
        "interaction": "terminal_interactive",
    }


def test_catalog_identity_matches_distribution_metadata() -> None:
    validated = validate_discovered_entry(
        catalog_entry(),
        entry_point_name="clickhouse",
        distribution="jusi-clickhouse",
        distribution_version="0.2.0",
    )
    assert validated["plugin_id"] == "clickhouse"


def test_catalog_import_does_not_load_runtime_dependencies(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    for name in list(sys.modules):
        if name == "jusi_clickhouse.catalog" or name.startswith(("clickhouse_connect", "visidata", "IPython")):
            monkeypatch.delitem(sys.modules, name, raising=False)
    importlib.import_module("jusi_clickhouse.catalog").catalog_entry()
    assert not any(name.startswith(("clickhouse_connect", "visidata", "IPython")) for name in sys.modules)


class FakeIp:
    def __init__(self) -> None:
        self.magics_manager = SimpleNamespace(magics={"cell": {}})
        self.registrations = []

    def register_magic_function(self, function, *, magic_kind, magic_name):  # type: ignore[no-untyped-def]
        self.magics_manager.magics[magic_kind][magic_name] = function
        self.registrations.append((magic_kind, magic_name))


def test_kernel_uses_family_dispatcher_and_exact_handoff() -> None:
    _reset_runtime_for_tests()
    kernel = importlib.reload(importlib.import_module("jusi_clickhouse.kernel"))
    kernel.configure_jusi_runtime_v1({
        "sql": {"targets": {"analytics": {"provider": "clickhouse", "host": "db"}}}
    })
    ipython = FakeIp()
    kernel.load_ipython_extension(ipython)
    assert ipython.registrations == [("cell", "sql")]
    handoff = dispatch_sql("analytics", "")
    validate_plugin_kernel_message(handoff)
    assert handoff["plugin_id"] == "clickhouse"
    assert handoff["payload"] == {
        "alias": "analytics", "sql": CLICKHOUSE_BOOTSTRAP_SQL, "options": {"host": "db"},
    }
    _reset_runtime_for_tests()


def test_parse_clickhouse_options_keeps_only_driver_options() -> None:
    options = parse_clickhouse_options({
        "host": "db.example", "database": "analytics", "password": "secret", "initial_fetch": "25",
    })
    assert options.initial_fetch == 25
    assert options.connect == {"host": "db.example", "database": "analytics", "password": "secret"}


def test_followup_preserves_literal_sql_but_removes_magic_header() -> None:
    assert _followup_sql("select 1") == "select 1"
    assert _followup_sql("%%sql analytics\nselect α") == "select α"


def test_application_loads_visidata_config_before_provider_commands(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    calls = []
    monkeypatch.setattr(
        "jusi.visidata_support.initialize_visidata",
        lambda **kwargs: calls.append(("initialize", kwargs)),
    )
    monkeypatch.setattr(
        "jusi_clickhouse.runner.install_clickhouse_commands",
        lambda: calls.append(("commands", {})),
    )

    _initialize_visidata_application()

    assert calls == [
        ("initialize", {"open_name": "selection.sql", "open_filetype": "sql"}),
        ("commands", {}),
    ]


def test_worker_returns_one_terminal_and_delegates_to_family_router(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setattr("jusi_clickhouse.worker.find_spec", lambda _name: object())
    worker = create_worker(worker_context())
    result = worker.handle("execute", {"alias": "analytics", "sql": "select 1", "options": {"host": "db"}})
    assert result.result == {"accepted": True, "alias": "analytics"}
    assert len(result.core_requests) == 1
    surface = result.core_requests[0]
    assert surface.request_id == "clickhouse_visidata"
    assert surface.argv[1:3] == ("-m", "jusi_clickhouse.runner")
    assert "signal" in surface.capabilities
    with pytest.raises(OperationRejected, match="already started"):
        worker.handle("execute", {"alias": "analytics", "sql": "select 2", "options": {}})
    worker.close()


def test_worker_editor_actions_are_content_based() -> None:
    session = ClickHouseClientSession(worker_context())
    assert session.editor_action("copy", {"text": "select α", "linewise": True}).result == {
        "action": "copy", "text": "select α", "regtype": "V",
    }
    assert session.editor_action("open", {"text": "select 1"}).result["name"] == "selection.sql"
    with pytest.raises(OperationRejected, match="requires text"):
        session.editor_action("copy", {})


def test_private_bridge_round_trip_and_interrupt() -> None:
    socket_path = f"/tmp/jusi-ch-test-{os.getpid()}.sock"
    Path(socket_path).unlink(missing_ok=True)
    bridge = WorkerApplicationBridge(socket_path)
    interrupted = threading.Event()

    def handle(operation: str, payload: dict) -> dict:
        if operation == "interrupt":
            interrupted.set()
        return {"operation": operation, **payload}

    controller = ApplicationController(bridge.socket_path, handle)
    controller.start()
    assert bridge.request("followup", {"body": "select 2"})["result"]["body"] == "select 2"
    bridge.interrupt()
    assert interrupted.wait(1)
    bridge.close()


def test_result_sheet_uses_family_actions() -> None:
    session = SimpleNamespace(alias="analytics", register_sheet=lambda _sheet: None)
    sheet = ClickHouseResultSheet(session=session, query="select 1")
    actions = find_sql_actions(sheet)
    assert actions is not None
    assert actions.fetch_more.__self__ is sheet


def test_clickhouse_completion_uses_family_absolute_ranges_and_preserves_suffix() -> None:
    from jusi_clickhouse.runner import ClickHouseSession

    session = object.__new__(ClickHouseSession)
    session.metadata = SimpleNamespace(snapshot=lambda: MetadataSnapshot(
        columns=[CompletionColumn("events", "email", "demo", "String")],
    ))
    request = SqlCompletionRequest.from_payload({
        "body": "select emaSUFFIX",
        "prefix": "select ema",
        "cursor_pos": 10,
        "cursor_row": 0,
        "cursor_col": 10,
    })
    result = session.complete(request)
    item = next(item for item in result["items"] if item["text"] == "email")
    assert item["start"] == 7
    assert item["end"] == 10


def test_blank_relation_completion_returns_only_schemas() -> None:
    prefix = "select * from "
    request = SqlCompletionRequest(prefix, prefix, len(prefix), 0, len(prefix))
    snapshot = MetadataSnapshot(
        schemas=["demo", "system"],
        objects=[CompletionObject("events", "demo", "table")],
        columns=[CompletionColumn("events", "email", "demo", "String")],
        functions=[CompletionObject("lower", "", "function")],
    )

    result = _complete_clickhouse_sql(snapshot, request)

    assert [(item["text"], item["kind"]) for item in result["items"]] == [
        ("demo", "schema"),
        ("system", "schema"),
    ]
    assert {(item["start"], item["end"]) for item in result["items"]} == {
        (len(prefix), len(prefix)),
    }


def test_typed_relation_completion_keeps_matching_metadata() -> None:
    prefix = "select * from eve"
    request = SqlCompletionRequest(prefix, prefix, len(prefix), 0, len(prefix))
    snapshot = MetadataSnapshot(
        schemas=["demo"],
        objects=[CompletionObject("events", "demo", "table")],
    )

    result = _complete_clickhouse_sql(snapshot, request)

    assert "events" in {item["text"] for item in result["items"]}


def test_metadata_collector_supplies_family_models() -> None:
    result_sets = [
        [("demo",)],
        [("demo", "events", "MergeTree")],
        [("demo", "events", "id", "UInt64")],
        [("lower", "System")],
    ]

    class Client:
        def query(self, _sql):  # type: ignore[no-untyped-def]
            return SimpleNamespace(result_rows=result_sets.pop(0))

    assert load_clickhouse_metadata(Client()) == MetadataSnapshot(
        schemas=["demo"],
        objects=[CompletionObject("events", "demo", "table", "MergeTree")],
        columns=[CompletionColumn("events", "id", "demo", "UInt64")],
        functions=[CompletionObject("lower", "", "function", "System")],
    )


def test_result_query_detection_matches_clickhouse_statements() -> None:
    assert _looks_like_result_query("select 1") is True
    assert _looks_like_result_query("show databases") is True
    assert _looks_like_result_query("insert into t values (1)") is False


def test_iter_stream_rows_supports_block_and_row_streams() -> None:
    assert list(_iter_stream_rows([[("a", 1), ("b", 2)]])) == [("a", 1), ("b", 2)]
    assert list(_iter_stream_rows([("a", 1), ("b", 2)])) == [("a", 1), ("b", 2)]


def test_write_raw_value_file_preserves_binary_bytes() -> None:
    path = _write_raw_value_file(b"PK\x05\x06" + (b"\x00" * 18), ".zip")
    assert Path(path).read_bytes()[:4] == b"PK\x05\x06"
    Path(path).unlink()


def test_write_raw_value_file_preserves_clickhouse_binary_string_for_zip() -> None:
    buffer = BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("blob-readme.txt", "Jusi ClickHouse blob fixture\n")
    path = _write_raw_value_file(buffer.getvalue().decode("latin-1"), ".zip")
    with zipfile.ZipFile(path) as archive:
        assert archive.namelist() == ["blob-readme.txt"]
    Path(path).unlink()


def test_refetch_cell_as_bytes_uses_clickhouse_column_format() -> None:
    class FakeClient:
        def __init__(self) -> None:
            self.calls = []
        def query(self, sql, **kwargs):  # type: ignore[no-untyped-def]
            self.calls.append({"sql": sql, **kwargs})
            return SimpleNamespace(result_rows=[(b"PK\x03\x04raw zip bytes",)])

    client = FakeClient()
    session = SimpleNamespace(lock=threading.RLock(), connect=lambda: client)
    sheet = SimpleNamespace(query="SELECT payload FROM demo.blob_files;", session=session)
    assert _refetch_cell_as_bytes(sheet, column_name="payload", row_index=3) == b"PK\x03\x04raw zip bytes"
    assert client.calls[0]["column_formats"] == {"payload": "bytes"}
