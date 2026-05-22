from __future__ import annotations

from jusi.plugins import DisplayHandlerSpec, MagicCommand
from jusi_sql import BaseSqlHandler

from .constants import CLICKHOUSE_BOOTSTRAP_SQL


class ClickHouseHandler(BaseSqlHandler):
    def handler_id(self) -> str:
        return "clickhouse"

    @staticmethod
    def bootstrap_cell_body(first_line: str) -> str | None:
        _ = first_line
        return CLICKHOUSE_BOOTSTRAP_SQL

    def plugin_runtime_callable(self) -> str:
        return "jusi_clickhouse.runner:run_clickhouse_runner"


def display_handler_specs() -> tuple[DisplayHandlerSpec, ...]:
    return (
        DisplayHandlerSpec(
            handler_id="clickhouse",
            factory=ClickHouseHandler,
            magic_commands=(MagicCommand("sql", bootstrap_body=ClickHouseHandler.bootstrap_cell_body),),
            kernel_extension_modules=("jusi_clickhouse.kernel",),
            family_presentation={"syntax": "sql", "indent": "sql", "followup": True, "completion": True},
            presentation={"syntax": "clickhouse", "indent": "sql", "followup": True, "completion": True},
        ),
    )
