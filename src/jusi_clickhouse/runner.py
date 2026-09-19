from __future__ import annotations

import json
import curses
from collections import deque
from pathlib import Path
import sys
import tempfile
import threading
import uuid
from typing import Any, Iterable, Iterator

import visidata
from visidata import ItemColumn, SequenceSheet, run, vd

from jusi_sql import (
    MetadataCache,
    SqlCompletionRequest,
    SqlSheetActions,
    bind_sql_actions,
    complete_sql,
    install_visidata_commands,
    sql_cache_directory,
)

from .config import parse_clickhouse_options
from .constants import CLICKHOUSE_BOOTSTRAP_SQL
from .ipc import ApplicationController
from .metadata import load_clickhouse_metadata


RESULT_QUERY_PREFIXES = ("select", "with", "show", "describe", "desc", "explain")
CLICKHOUSE_KEYWORDS = (
    "SELECT", "FROM", "PREWHERE", "WHERE", "JOIN", "LEFT", "RIGHT", "FULL",
    "INNER", "OUTER", "ON", "GROUP", "BY", "ORDER", "HAVING", "LIMIT",
    "OFFSET", "INSERT", "INTO", "CREATE", "ALTER", "DROP", "TRUNCATE", "WITH",
    "FORMAT", "SETTINGS", "VALUES", "SHOW", "DESCRIBE", "EXPLAIN",
)
_PENDING_SHEETS: deque[Any] = deque()


class ClickHouseSession:
    def __init__(
        self,
        *,
        alias: str,
        connect_options: dict[str, Any],
        initial_fetch: int = 100,
    ) -> None:
        self.alias = alias
        self.connect_options = _with_session_id(connect_options)
        self.initial_fetch = initial_fetch
        self.client: Any = None
        self.lock = threading.RLock()
        self.sheets: list[ClickHouseResultSheet] = []
        self.metadata = MetadataCache(
            sql_cache_directory("clickhouse", alias, self.connect_options),
            lambda: self.with_client(lambda client: load_clickhouse_metadata(client), blocking=False),
            on_warning=lambda message: vd.warning(message),
        )

    def connect(self) -> Any:
        with self.lock:
            if self.client is None:
                self.client = _connect_clickhouse(self.connect_options)
            return self.client

    def with_client(self, fn, *, blocking: bool = True):  # type: ignore[no-untyped-def]
        if not self.lock.acquire(blocking=blocking):
            raise RuntimeError("ClickHouse connection is busy")
        try:
            client = self.connect()
            return fn(client)
        finally:
            self.lock.release()

    def register_sheet(self, sheet: "ClickHouseResultSheet") -> None:
        self.sheets.append(sheet)

    def complete(self, request: SqlCompletionRequest) -> dict[str, Any]:
        snapshot = self.metadata.snapshot()
        return complete_sql(snapshot, request, keywords=CLICKHOUSE_KEYWORDS, schema_detail="database")

    def enter_cell(self) -> None:
        self.metadata.ensure_fresh_async()

    def followup(self, body: str) -> None:
        sql = _followup_sql(body).strip()
        if not sql:
            return
        self.enter_cell()
        _queue_sheet(ClickHouseResultSheet(session=self, query=sql))

    def interrupt(self) -> None:
        query_ids = [sheet.query_id for sheet in self.sheets if sheet.active]
        for query_id in query_ids:
            try:
                cancel_client = _connect_clickhouse(self.connect_options)
                _command(cancel_client, f"KILL QUERY WHERE query_id = '{_escape_sql_literal(query_id)}' SYNC")
                _close_client(cancel_client)
            except Exception as exc:
                vd.warning(f"ClickHouse cancellation failed: {exc}")
        if query_ids:
            vd.warning("ClickHouse query cancellation requested")

    def close(self) -> None:
        metadata_done = self.metadata.close(timeout=2.0)
        if not metadata_done:
            self.interrupt()
        for sheet in list(self.sheets):
            sheet.close_stream("session closed")
        client = self.client
        self.client = None
        if client is not None:
            _close_client(client)


class ClickHouseResultSheet(SequenceSheet):
    rowtype = "rows"

    def __init__(self, *, session: ClickHouseSession, query: str) -> None:
        super().__init__(name=session.alias, source=query)
        self.session = session
        self.query = query
        self.query_id = f"jusi_ch_{uuid.uuid4().hex}"
        self.exhausted = False
        self.active = False
        self.stream_closed_reason = ""
        self._stream_context: Any = None
        self._row_iter: Iterator[Any] | None = None
        self._buffer: list[Any] = []
        session.register_sheet(self)
        bind_sql_actions(self, SqlSheetActions(fetch_more=self.fetch_more))

    def iterload(self):  # type: ignore[no-untyped-def]
        try:
            if _looks_like_result_query(self.query):
                yield from self._load_result_query()
            else:
                yield from self._load_statement()
        except Exception as exc:
            self.columns = [ItemColumn("error", 0)]
            yield [f"{exc.__class__.__name__}: {exc}"]
        finally:
            self.active = False

    def fetch_more(self, count: int) -> int:
        if self.stream_closed_reason:
            vd.warning(f"ClickHouse stream is closed: {self.stream_closed_reason}")
            return 0
        if self.exhausted or self._row_iter is None:
            vd.status("ClickHouse stream is exhausted")
            return 0
        try:
            with self.session.lock:
                rows = self._fetch_rows(count)
            for row in rows:
                self.addRow(list(row))
            self._notify_more()
            return len(rows)
        except Exception as exc:
            vd.warning(f"ClickHouse fetch failed: {exc}")
            return 0

    def close_stream(self, reason: str = "") -> None:
        if reason:
            self.stream_closed_reason = self.stream_closed_reason or reason
        self._row_iter = None
        context = self._stream_context
        self._stream_context = None
        if context is not None:
            exit_fn = getattr(context, "__exit__", None)
            if callable(exit_fn):
                try:
                    exit_fn(None, None, None)
                except Exception:
                    pass

    def _load_result_query(self):  # type: ignore[no-untyped-def]
        client = self.session.connect()
        self.active = True
        with self.session.lock:
            stream = _open_row_stream(client, self.query, self.query_id)
            if stream is None:
                result = _query(client, self.query, query_id=self.query_id)
                column_names = [str(item) for item in getattr(result, "column_names", [])] or ["result"]
                self.columns = [ItemColumn(name, index) for index, name in enumerate(column_names)]
                self._buffer = list(getattr(result, "result_rows", []) or [])
                self.exhausted = True
            else:
                self._stream_context = stream
                source = _enter_stream(stream)
                column_names = _stream_column_names(source)
                self.columns = [ItemColumn(name, index) for index, name in enumerate(column_names)]
                self._row_iter = iter(_iter_stream_rows(source))
            rows = self._fetch_rows(self.session.initial_fetch)
        if self.columns:
            yield [column.name for column in self.columns]
        for row in rows:
            yield list(row)
        self._notify_more()

    def _load_statement(self):  # type: ignore[no-untyped-def]
        client = self.session.connect()
        self.active = True
        with self.session.lock:
            value = _command(client, self.query, query_id=self.query_id)
        self.columns = [ItemColumn("status", 0), ItemColumn("value", 1)]
        yield ["status", "done"]
        if value is not None:
            yield ["result", value]
        self.session.metadata.mark_stale()

    def _fetch_rows(self, count: int) -> list[Any]:
        if count == 0:
            rows = list(self._buffer)
            self._buffer.clear()
            if self._row_iter is not None:
                rows.extend(list(self._row_iter))
            self.exhausted = True
            self.close_stream()
            return rows
        rows = list(self._buffer[:count])
        self._buffer = self._buffer[count:]
        remaining = count - len(rows)
        if remaining > 0 and self._row_iter is not None:
            for _ in range(remaining + 1):
                try:
                    fetched = next(self._row_iter)
                except StopIteration:
                    self.exhausted = True
                    self.close_stream()
                    break
                if remaining > 0:
                    rows.append(fetched)
                    remaining -= 1
                else:
                    self._buffer.append(fetched)
        return rows

    def _notify_more(self) -> None:
        if self._buffer or not self.exhausted:
            vd.warning("ClickHouse stream has more rows; press 1-9 or gf to fetch more")
        else:
            vd.status("ClickHouse stream exhausted")


def install_clickhouse_commands() -> None:
    install_visidata_commands(visidata)
    if getattr(visidata.BaseSheet, "_jusi_clickhouse_commands_v1", False):
        return

    @visidata.BaseSheet.command("gb", "jusi-clickhouse-open-raw-value", "open raw ClickHouse cell value", replay=False)
    def _open_raw_value(sheet: Any) -> None:
        value = getattr(sheet, "cursorValue", None)
        if callable(value):
            value = value()
        value = value if value is not None else ""
        extension = str(vd.input("extension: ") or "").strip().lstrip(".")
        suffix = f".{extension}" if extension else ""
        if _is_binary_suffix(suffix):
            value = _maybe_refetch_clickhouse_value_as_bytes(sheet, value)
        path = _write_raw_value_file(value, suffix)
        opened = vd.openPath(visidata.Path(path))
        vd.push(opened)

    @visidata.BaseSheet.command("", "jusi-clickhouse-open-pending-sheet", "open pending ClickHouse result", replay=False)
    def _open_pending(_sheet: Any) -> None:
        if not _PENDING_SHEETS:
            return
        next_sheet = _PENDING_SHEETS.popleft()
        vd.push(next_sheet)
        next_sheet.ensureLoaded()

    setattr(visidata.BaseSheet, "_jusi_clickhouse_commands_v1", True)


def _queue_sheet(sheet: ClickHouseResultSheet) -> None:
    _PENDING_SHEETS.append(sheet)
    vd.queueCommand("jusi-clickhouse-open-pending-sheet")
    try:
        curses.ungetch(curses.KEY_RESIZE)
    except Exception:
        pass


def _followup_sql(body: str) -> str:
    first, separator, remainder = body.partition("\n")
    header = first.strip()
    if header == "%%sql" or header.startswith(("%%sql ", "%%sql\t")):
        return remainder if separator else ""
    return body


def _write_raw_value_file(value: Any, suffix: str) -> str:
    if isinstance(value, memoryview):
        value = value.tobytes()
    if isinstance(value, bytearray):
        value = bytes(value)
    if isinstance(value, str) and (_is_binary_suffix(suffix) or _looks_like_binary_string(value)):
        raw_value = _text_to_raw_bytes(value)
        if raw_value is not None:
            value = raw_value
    if isinstance(value, bytes):
        with tempfile.NamedTemporaryFile("wb", prefix="jusi-clickhouse-value-", suffix=suffix, delete=False) as handle:
            handle.write(value)
            return handle.name
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", prefix="jusi-clickhouse-value-", suffix=suffix, delete=False) as handle:
        handle.write(_value_to_text(value))
        return handle.name


def _value_to_text(value: Any) -> str:
    if isinstance(value, (dict, list, tuple)):
        return json.dumps(value, ensure_ascii=False, default=str, indent=2)
    return "" if value is None else str(value)


def _is_binary_suffix(suffix: str) -> bool:
    extension = suffix.lower()
    if extension and not extension.startswith("."):
        extension = f".{extension}"
    return extension in {
        ".7z",
        ".avro",
        ".bin",
        ".bz2",
        ".doc",
        ".docx",
        ".gz",
        ".ico",
        ".jpeg",
        ".jpg",
        ".lz4",
        ".orc",
        ".parquet",
        ".pdf",
        ".png",
        ".snappy",
        ".tar",
        ".tgz",
        ".webp",
        ".xls",
        ".xlsx",
        ".xz",
        ".zip",
        ".zst",
    }


def _text_to_raw_bytes(value: str) -> bytes | None:
    stripped = value.strip()
    if _looks_like_hex_blob(stripped):
        return bytes.fromhex(stripped)
    try:
        return value.encode("latin-1")
    except UnicodeEncodeError:
        return None


def _looks_like_hex_blob(value: str) -> bool:
    if len(value) < 8 or len(value) % 2:
        return False
    return all(char in "0123456789abcdefABCDEF" for char in value)


def _looks_like_binary_string(value: str) -> bool:
    raw_value = _text_to_raw_bytes(value)
    if raw_value is None:
        return False
    if raw_value.startswith((b"PK\x03\x04", b"PK\x05\x06", b"\x1f\x8b", b"\x89PNG\r\n\x1a\n", b"%PDF-")):
        return True
    sample = raw_value[:4096]
    if not sample:
        return False
    control = sum(1 for byte in sample if byte < 32 and byte not in (9, 10, 13))
    return b"\x00" in sample or control / len(sample) > 0.05


def _maybe_refetch_clickhouse_value_as_bytes(sheet: Any, value: Any) -> Any:
    if isinstance(value, (bytes, bytearray, memoryview)):
        return value
    if not isinstance(sheet, ClickHouseResultSheet):
        return value
    column = getattr(sheet, "cursorCol", None)
    column_name = str(getattr(column, "name", "") or "").strip()
    row_index = getattr(sheet, "cursorRowIndex", None)
    if not column_name or not isinstance(row_index, int) or row_index < 0:
        return value
    try:
        return _refetch_cell_as_bytes(sheet, column_name=column_name, row_index=row_index)
    except Exception as exc:
        vd.warning(f"ClickHouse raw byte refetch failed; using visible value: {exc}")
        return value


def _refetch_cell_as_bytes(sheet: Any, *, column_name: str, row_index: int) -> Any:
    query = _strip_query_for_subquery(str(getattr(sheet, "query", "")))
    if not query:
        raise RuntimeError("missing source query")
    session = sheet.session
    client = session.connect()
    sql = (
        f"SELECT {_quote_identifier(column_name)} "
        f"FROM ({query}) AS jusi_raw_value "
        f"LIMIT 1 OFFSET {int(row_index)}"
    )
    query_id = f"jusi_ch_raw_{uuid.uuid4().hex}"
    with session.lock:
        result = _query(client, sql, query_id=query_id, column_formats={column_name: "bytes"})
    rows = list(getattr(result, "result_rows", []) or [])
    if not rows:
        raise RuntimeError("selected row is not available from source query")
    return rows[0][0]


def _quote_identifier(value: str) -> str:
    return "`" + value.replace("`", "``") + "`"


def _strip_query_for_subquery(query: str) -> str:
    return query.strip().rstrip(";").strip()


def _looks_like_result_query(sql: str) -> bool:
    stripped = sql.lstrip().lower()
    return stripped.startswith(RESULT_QUERY_PREFIXES)


def _connect_clickhouse(connect_options: dict[str, Any]) -> Any:
    try:
        import clickhouse_connect
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "Missing dependency 'clickhouse_connect'. Install this plugin with dependencies, "
            "for example: ./.venv/bin/python -m pip install -e ."
        ) from exc
    return clickhouse_connect.get_client(**connect_options)


def _with_session_id(connect_options: dict[str, Any]) -> dict[str, Any]:
    options = dict(connect_options)
    if not options.get("session_id"):
        options["session_id"] = f"jusi_ch_{uuid.uuid4().hex}"
    return options


def _open_row_stream(client: Any, query: str, query_id: str) -> Any:
    for method_name in ("query_row_block_stream", "query_rows_stream"):
        method = getattr(client, method_name, None)
        if callable(method):
            try:
                return method(query, query_id=query_id)
            except TypeError:
                return method(query)
    return None


def _enter_stream(stream: Any) -> Any:
    enter = getattr(stream, "__enter__", None)
    if callable(enter):
        return enter()
    return stream


def _stream_column_names(source: Any) -> list[str]:
    for attr in ("column_names", "columns"):
        value = getattr(source, attr, None)
        if value:
            return [str(item) for item in value]
    source_result = getattr(source, "source", None)
    value = getattr(source_result, "column_names", None)
    if value:
        return [str(item) for item in value]
    return ["result"]


def _iter_stream_rows(source: Any) -> Iterable[Any]:
    for block in source:
        if isinstance(block, list) and (not block or isinstance(block[0], (list, tuple, dict))):
            for row in block:
                yield row
        else:
            yield block


def _query(client: Any, sql: str, *, query_id: str, column_formats: dict[str, Any] | None = None) -> Any:
    try:
        return client.query(sql, query_id=query_id, column_formats=column_formats)
    except TypeError:
        return client.query(sql)


def _command(client: Any, sql: str, *, query_id: str | None = None) -> Any:
    if query_id is not None:
        try:
            return client.command(sql, query_id=query_id)
        except TypeError:
            pass
    return client.command(sql)


def _close_client(client: Any) -> None:
    for method_name in ("close", "close_connections"):
        method = getattr(client, method_name, None)
        if callable(method):
            try:
                method()
            except Exception:
                pass
            return


def _escape_sql_literal(value: str) -> str:
    return value.replace("\\", "\\\\").replace("'", "\\'")


def _read_payload(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    finally:
        path.unlink(missing_ok=True)
    if not isinstance(value, dict):
        raise RuntimeError("invalid ClickHouse application payload")
    return value


def _handle_application_operation(
    session: ClickHouseSession,
    operation: str,
    payload: dict[str, Any],
) -> dict[str, Any]:
    if operation == "followup":
        body = payload.get("body")
        if not isinstance(body, str):
            raise ValueError("SQL followup requires string body")
        session.followup(body)
        return {"accepted": True}
    if operation == "complete":
        return session.complete(SqlCompletionRequest.from_payload(payload))
    if operation == "interrupt":
        session.interrupt()
        return {"accepted": True}
    raise ValueError(f"unsupported ClickHouse application operation: {operation}")


def run_clickhouse_application(payload_path: Path, socket_path: str) -> int:
    session: ClickHouseSession | None = None
    try:
        from jusi.plugins.vd.application import install_editor_actions

        install_clickhouse_commands()
        install_editor_actions()
        visidata.vd.timeouts_before_idle = -1
        payload = _read_payload(payload_path)
        query = str(payload.get("sql", "")).strip() or CLICKHOUSE_BOOTSTRAP_SQL
        alias = str(payload.get("alias", "")).strip()
        if not alias:
            raise RuntimeError("missing SQL target alias")
        raw_options = payload.get("options")
        if not isinstance(raw_options, dict):
            raise RuntimeError("missing ClickHouse target options")
        options = parse_clickhouse_options(raw_options)
        session = ClickHouseSession(
            alias=alias,
            connect_options=options.connect,
            initial_fetch=options.initial_fetch,
        )
        controller = ApplicationController(
            socket_path,
            lambda operation, control_payload: _handle_application_operation(session, operation, control_payload),
        )
        controller.start()
        session.enter_cell()
        sheet = ClickHouseResultSheet(session=session, query=query)
        run(sheet)
        return 0
    except Exception as exc:
        sys.stderr.write(str(exc) + "\n")
        sys.stderr.flush()
        return 2
    finally:
        if session is not None:
            session.close()


if __name__ == "__main__":
    if len(sys.argv) != 4 or sys.argv[1] != "--application":
        raise SystemExit("ClickHouse application requires a private payload path and control socket")
    raise SystemExit(run_clickhouse_application(Path(sys.argv[2]), sys.argv[3]))
