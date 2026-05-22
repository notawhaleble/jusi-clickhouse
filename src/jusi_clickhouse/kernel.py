from __future__ import annotations

import argparse
import json
import os
import shlex
from typing import Any

from IPython.core.error import UsageError

from jusi.domain.models import JUSI_HANDLER_HANDOFF_MIME
from jusi.infrastructure.debug_timing import emit_timing
from jusi.infrastructure.runtime import JUSI_SESSION_CONFIG_ENV
from jusi_sql.config import SqlConfigError, resolve_sql_target, set_session_config

from .constants import CLICKHOUSE_BOOTSTRAP_SQL


JUSI_SQL_CONFIG_ENV = JUSI_SESSION_CONFIG_ENV


def register_sql_magic(ipython: Any) -> None:
    cell_magics = getattr(getattr(ipython, "magics_manager", None), "magics", {}).get("cell", {})
    if "sql" in cell_magics:
        return

    def _jusi_sql_magic(line: str, cell: str) -> None:
        from IPython.display import display

        alias, magic_options = _parse_sql_line(line)
        emit_timing("sql.clickhouse.kernel.magic.invoked", alias=alias, line=line, content_len=len(cell))
        if not alias:
            raise UsageError("%%sql requires a target alias, for example %%sql analytics")
        try:
            target = resolve_sql_target(alias)
        except SqlConfigError as exc:
            emit_timing("sql.clickhouse.kernel.magic.resolve_error", alias=alias, error=str(exc))
            raise UsageError(str(exc)) from exc
        target_options = dict(target.options)
        target_options.update(magic_options)
        content = cell
        if target.provider == "clickhouse" and not content.strip():
            content = CLICKHOUSE_BOOTSTRAP_SQL
        emit_timing("sql.clickhouse.kernel.magic.resolved", alias=target.alias, provider=target.provider)
        payload = {
            "handler_id": target.provider,
            "magic_name": "sql",
            "content": content,
            "meta": {
                "alias": target.alias,
                "provider": target.provider,
                "line": line,
                "target_config": target_options,
                "magic_options": magic_options,
            },
        }
        display(
            {JUSI_HANDLER_HANDOFF_MIME: payload},
            raw=True,
            metadata={JUSI_HANDLER_HANDOFF_MIME: {"alias": target.alias, "provider": target.provider}},
        )
        emit_timing("sql.clickhouse.kernel.magic.handoff_emitted", alias=target.alias, provider=target.provider)

    ipython.register_magic_function(_jusi_sql_magic, magic_kind="cell", magic_name="sql")


def configure_sql_session(config: dict[str, Any] | None) -> None:
    set_session_config(config)
    emit_timing("sql.clickhouse.kernel.session_config", has_config=bool(config), keys=sorted(list((config or {}).keys())))


def load_ipython_extension(ipython: Any) -> None:
    raw = os.environ.get(JUSI_SQL_CONFIG_ENV, "").strip()
    if raw:
        try:
            loaded = json.loads(raw)
        except json.JSONDecodeError:
            loaded = {}
        if isinstance(loaded, dict):
            configure_sql_session(loaded)
    install_blank_body_transformer(ipython)
    register_sql_magic(ipython)
    emit_timing("sql.clickhouse.kernel.extension_loaded")


def install_blank_body_transformer(ipython: Any) -> None:
    if bool(getattr(ipython, "_jusi_clickhouse_blank_body_transformer", False)):
        return
    transformers = getattr(ipython, "input_transformers_cleanup", None)
    if not isinstance(transformers, list):
        return
    transformers.append(_sql_blank_body_transformer)
    setattr(ipython, "_jusi_clickhouse_blank_body_transformer", True)


def _sql_blank_body_transformer(lines: list[str]) -> list[str]:
    if len(lines) != 1:
        return lines
    first_line = lines[0]
    stripped = first_line.strip()
    if not stripped.startswith("%%sql"):
        return lines
    try:
        alias, _magic_options = _parse_sql_line(stripped.removeprefix("%%sql").strip())
    except UsageError:
        return lines
    if not alias:
        return lines
    try:
        target = resolve_sql_target(alias)
    except SqlConfigError:
        return lines
    if target.provider != "clickhouse":
        return lines
    return [first_line, CLICKHOUSE_BOOTSTRAP_SQL + "\n"]


def _parse_sql_line(line: str) -> tuple[str, dict[str, object]]:
    try:
        parts = shlex.split(line)
    except ValueError as exc:
        raise UsageError(str(exc)) from exc
    if not parts:
        return "", {}
    parser = argparse.ArgumentParser(prog="%%sql", add_help=False)
    parser.add_argument("alias")
    parser.add_argument("--initial-fetch", type=int, dest="initial_fetch")
    namespace, unknown = parser.parse_known_args(parts)
    if unknown:
        raise UsageError(f"Unknown %%sql option(s): {' '.join(unknown)}")
    options: dict[str, object] = {}
    if namespace.initial_fetch is not None:
        if namespace.initial_fetch < 0:
            raise UsageError("--initial-fetch must be >= 0")
        options["initial_fetch"] = namespace.initial_fetch
    return str(namespace.alias), options
