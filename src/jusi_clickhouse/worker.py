"""Jusi 1.0 worker boundary; imports neither IPython nor VisiData."""
from __future__ import annotations

from importlib.util import find_spec
import os
from pathlib import Path
import sys
import tempfile
from typing import Any

from jusi.plugin_api import OperationRejected, WorkerResult, copy_text, open_text, show_diff, terminal_surface
from jusi_sql import SqlCompletionRequest
from jusi_sql.worker import SqlExecuteRequest, SqlWorker, StagedApplicationPayload

from .ipc import WorkerApplicationBridge


class ClickHouseClientSession:
    def __init__(self, context: object) -> None:
        self.context = context
        self.runtime_directory: Path | None = None
        self.payload: StagedApplicationPayload | None = None
        self.bridge: WorkerApplicationBridge | None = None

    def execute(self, request: SqlExecuteRequest) -> WorkerResult:
        if self.runtime_directory is not None:
            raise OperationRejected("ClickHouse client is already initialized", reason="conflict")
        if find_spec("visidata") is None:
            raise OperationRejected("%%sql requires VisiData; install jusi-clickhouse with dependencies", reason="unsupported")
        self.runtime_directory = Path(tempfile.mkdtemp(prefix="jusi-clickhouse-", dir="/tmp"))
        self.runtime_directory.chmod(0o700)
        socket_path = str(self.runtime_directory / "control.sock")
        try:
            self.payload = StagedApplicationPayload(
                {"alias": request.alias, "sql": request.sql, "options": request.options},
                prefix="jusi-clickhouse-payload-",
            )
            self.bridge = WorkerApplicationBridge(socket_path)
            return WorkerResult(
                {"accepted": True, "alias": request.alias},
                (terminal_surface(
                    "clickhouse_visidata",
                    (sys.executable, "-m", "jusi_clickhouse.runner", "--application", str(self.payload.path), socket_path),
                    environment_overrides={"TERM": os.environ.get("JUSI_SQL_TERM", "").strip() or "xterm-256color"},
                    signal=True,
                ),),
            )
        except BaseException:
            self.close()
            raise

    def followup(self, body: str) -> dict[str, Any]:
        return self._request("followup", {"body": body})

    def complete(self, request: SqlCompletionRequest) -> dict[str, Any]:
        return self._request("complete", {
            "body": request.body,
            "prefix": request.prefix,
            "cursor_pos": request.cursor_pos,
            "cursor_row": request.cursor_row,
            "cursor_col": request.cursor_col,
        })

    def interrupt(self) -> None:
        if self.bridge is not None:
            self.bridge.interrupt()

    def editor_action(self, action: str, selection: dict[str, Any]) -> WorkerResult:
        if action == "show_diff":
            before, after = selection.get("before"), selection.get("after")
            if not isinstance(before, str) or not isinstance(after, str):
                raise OperationRejected("ClickHouse diff selection requires before and after text", reason="invalid_request")
            return show_diff(before, after, filetype="sql")
        text = selection.get("text")
        if not isinstance(text, str):
            raise OperationRejected("ClickHouse selection requires text", reason="invalid_request")
        if action == "copy":
            return copy_text(text, linewise=bool(selection.get("linewise", False)))
        if action == "open":
            return open_text(text, name="selection.sql", filetype="sql")
        raise OperationRejected(f"Unsupported ClickHouse editor action: {action}", reason="unsupported")

    def _request(self, operation: str, payload: dict[str, Any]) -> dict[str, Any]:
        if self.bridge is None:
            raise OperationRejected("ClickHouse client is not initialized", reason="conflict")
        response = self.bridge.request(operation, payload)
        if response.get("ok") is not True:
            raise RuntimeError(str(response.get("message", "ClickHouse terminal application failed")))
        result = response.get("result", {})
        return dict(result) if isinstance(result, dict) else {"accepted": True}

    def close(self) -> None:
        if self.bridge is not None:
            self.bridge.close()
            self.bridge = None
        if self.payload is not None:
            self.payload.close()
            self.payload = None
        if self.runtime_directory is not None:
            try:
                self.runtime_directory.rmdir()
            except OSError:
                pass
            self.runtime_directory = None


def create_worker(context: object) -> SqlWorker:
    return SqlWorker(context, ClickHouseClientSession)
