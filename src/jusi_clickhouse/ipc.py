"""Private worker/application control channel for one ClickHouse client."""
from __future__ import annotations

import json
import os
import socket
import threading
from typing import Any, Callable
from uuid import uuid4


class WorkerApplicationBridge:
    def __init__(self, socket_path: str) -> None:
        self.socket_path = socket_path
        self._listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self._listener.bind(socket_path)
        self._listener.listen(1)
        self._connection: socket.socket | None = None
        self._connected = threading.Event()
        self._closed = threading.Event()
        self._write_lock = threading.Lock()
        self._condition = threading.Condition()
        self._responses: dict[str, dict[str, Any]] = {}
        threading.Thread(target=self._accept, name="jusi-clickhouse-bridge", daemon=True).start()

    def _accept(self) -> None:
        try:
            connection, _ = self._listener.accept()
            self._connection = connection
            self._connected.set()
            with connection.makefile("r", encoding="utf-8") as stream:
                for line in stream:
                    response = json.loads(line)
                    request_id = str(response.get("id", ""))
                    if request_id:
                        with self._condition:
                            self._responses[request_id] = response
                            self._condition.notify_all()
        except (OSError, ValueError, json.JSONDecodeError):
            pass
        finally:
            self._closed.set()
            self._connected.set()
            with self._condition:
                self._condition.notify_all()

    def request(self, operation: str, payload: dict[str, Any]) -> dict[str, Any]:
        if not self._connected.wait(10) or self._connection is None:
            raise RuntimeError("ClickHouse terminal application did not connect")
        request_id = uuid4().hex
        self._send({"id": request_id, "operation": operation, "payload": payload})
        with self._condition:
            while request_id not in self._responses and not self._closed.is_set():
                self._condition.wait()
            response = self._responses.pop(request_id, None)
        if response is None:
            raise RuntimeError("ClickHouse terminal application disconnected")
        return response

    def interrupt(self) -> None:
        if self._connection is not None and not self._closed.is_set():
            self._send({"id": uuid4().hex, "operation": "interrupt", "payload": {}})

    def _send(self, message: dict[str, Any]) -> None:
        connection = self._connection
        if connection is None:
            raise RuntimeError("ClickHouse terminal application is unavailable")
        data = (json.dumps(message, ensure_ascii=False) + "\n").encode("utf-8")
        with self._write_lock:
            connection.sendall(data)

    def close(self) -> None:
        self._closed.set()
        try:
            self._listener.close()
        except OSError:
            pass
        if self._connection is not None:
            try:
                self._connection.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            self._connection.close()
        try:
            os.unlink(self.socket_path)
        except FileNotFoundError:
            pass
        with self._condition:
            self._condition.notify_all()


class ApplicationController:
    def __init__(self, socket_path: str, handler: Callable[[str, dict[str, Any]], dict[str, Any]]) -> None:
        self._handler = handler
        self._connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self._connection.connect(socket_path)
        self._write_lock = threading.Lock()

    def start(self) -> None:
        threading.Thread(target=self._read, name="jusi-clickhouse-control", daemon=True).start()

    def _read(self) -> None:
        try:
            with self._connection.makefile("r", encoding="utf-8") as stream:
                for line in stream:
                    message = json.loads(line)
                    operation = str(message.get("operation", ""))
                    if operation == "interrupt":
                        self._handle(message)
                    else:
                        threading.Thread(target=self._handle, args=(message,), daemon=True).start()
        except (OSError, ValueError, json.JSONDecodeError):
            return

    def _handle(self, message: dict[str, Any]) -> None:
        payload = message.get("payload", {})
        if not isinstance(payload, dict):
            payload = {}
        try:
            result = self._handler(str(message.get("operation", "")), payload)
            response = {"ok": True, "result": result}
        except BaseException as exc:
            response = {"ok": False, "error": "fatal", "message": f"{type(exc).__name__}: {exc}"}
        response = {"id": str(message.get("id", "")), **response}
        try:
            with self._write_lock:
                self._connection.sendall((json.dumps(response, ensure_ascii=False) + "\n").encode("utf-8"))
        except OSError:
            return
