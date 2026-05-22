from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping


PLUGIN_OPTIONS = {"provider", "initial_fetch"}


@dataclass(frozen=True)
class ClickHouseOptions:
    connect: dict[str, Any]
    initial_fetch: int


def parse_clickhouse_options(options: Mapping[str, Any]) -> ClickHouseOptions:
    raw_initial = options.get("initial_fetch", 100)
    try:
        initial_fetch = int(raw_initial)
    except (TypeError, ValueError):
        initial_fetch = 100
    if initial_fetch < 0:
        initial_fetch = 0
    connect = {str(key): value for key, value in options.items() if str(key) not in PLUGIN_OPTIONS}
    return ClickHouseOptions(connect=connect, initial_fetch=initial_fetch)
