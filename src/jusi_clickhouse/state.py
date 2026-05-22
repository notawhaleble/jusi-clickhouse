from __future__ import annotations

import hashlib
import json
import os
import re
from pathlib import Path
from typing import Any


PLUGIN_NAME = "clickhouse"


def default_jusi_state_home() -> Path:
    override = os.environ.get("JUSI_STATE_HOME", "").strip()
    if override:
        return Path(os.path.expanduser(override)).resolve()
    xdg_state = os.environ.get("XDG_STATE_HOME", "").strip()
    if xdg_state:
        return (Path(os.path.expanduser(xdg_state)) / "jusi").resolve()
    return (Path.home() / ".local" / "state" / "jusi").resolve()


def target_cache_dir(alias: str, connect_options: dict[str, Any]) -> Path:
    fingerprint = json.dumps(_redacted_for_fingerprint(connect_options), ensure_ascii=True, sort_keys=True, default=str)
    digest = hashlib.sha256(fingerprint.encode("utf-8")).hexdigest()[:16]
    return default_jusi_state_home() / "plugins" / PLUGIN_NAME / _safe_segment(alias) / digest


def read_json(path: Path) -> dict[str, Any]:
    try:
        loaded = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        return {}
    return loaded if isinstance(loaded, dict) else {}


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".tmp")
    with temp.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=True, indent=2, sort_keys=True, default=str)
        handle.write("\n")
    temp.replace(path)


def _safe_segment(value: str) -> str:
    normalized = re.sub(r"[^A-Za-z0-9_.-]+", "-", value.strip()).strip("-._")
    return normalized[:120] or "default"


def _redacted_for_fingerprint(options: dict[str, Any]) -> dict[str, Any]:
    redacted: dict[str, Any] = {}
    for key, value in options.items():
        lower = str(key).lower()
        if "password" in lower or "secret" in lower or "token" in lower or "key" in lower:
            redacted[str(key)] = "<redacted>"
        else:
            redacted[str(key)] = value
    return redacted
