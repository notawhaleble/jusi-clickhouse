"""Lightweight Jusi 1.0 catalog provider; imports no runtime dependencies."""
from __future__ import annotations

from typing import Any

from jusi_sql import sql_catalog_entry

from . import __version__


def catalog_entry() -> dict[str, Any]:
    return sql_catalog_entry(
        plugin_id="clickhouse",
        plugin_version=__version__,
        distribution="jusi-clickhouse",
        kernel_extension="jusi_clickhouse.kernel",
        worker_entry_point="jusi_clickhouse.worker:create_worker",
        provider_presentation={"syntax": "clickhouse", "indent": "sql"},
    )
