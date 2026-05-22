from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

import sqlparse
from sqlparse.sql import Identifier, IdentifierList, TokenList
from sqlparse.tokens import Keyword, Name

from .metadata import MetadataSnapshot


CLICKHOUSE_KEYWORDS = [
    "SELECT",
    "FROM",
    "PREWHERE",
    "WHERE",
    "JOIN",
    "LEFT",
    "RIGHT",
    "FULL",
    "INNER",
    "OUTER",
    "ON",
    "GROUP",
    "BY",
    "ORDER",
    "HAVING",
    "LIMIT",
    "OFFSET",
    "INSERT",
    "INTO",
    "CREATE",
    "ALTER",
    "DROP",
    "TRUNCATE",
    "WITH",
    "FORMAT",
    "SETTINGS",
    "VALUES",
    "SHOW",
    "DESCRIBE",
    "EXPLAIN",
]


RELATION_CONTEXT_KEYWORDS = {"FROM", "JOIN", "INTO", "UPDATE", "TABLE", "DESCRIBE", "DESC"}


@dataclass(frozen=True)
class QueryRelation:
    schema: str
    table: str
    alias: str

    @property
    def completion_prefix(self) -> str:
        return self.alias or self.table


@dataclass(frozen=True)
class CompletionToken:
    value: str
    start_col: int | None
    end_col: int | None


def completion_items(snapshot: MetadataSnapshot, payload: dict[str, Any]) -> list[dict[str, Any]]:
    token = _completion_token(payload)
    prefix = token.value
    prefix_lower = prefix.lower()
    line_text = str(payload.get("line_text", ""))
    cell_text = _sql_body(str(payload.get("cell_text", "")) or str(payload.get("content", "")))
    wants_relations = _wants_relations(line_text, token.end_col)
    dotted = prefix.rsplit(".", 1) if "." in prefix else None
    owner_prefix = dotted[0] if dotted else ""
    value_prefix = dotted[1] if dotted else prefix
    value_prefix_lower = value_prefix.lower()
    relations = parse_query_relations(cell_text)
    seen: set[tuple[str, str]] = set()
    items: list[dict[str, Any]] = []

    def add(value: str, kind: str, *, label: str | None = None, detail: str = "", documentation: str | None = None) -> None:
        if dotted:
            if owner_prefix and not value.lower().startswith(owner_prefix.lower() + "."):
                return
            suffix = value.rsplit(".", 1)[-1]
            if value_prefix and not suffix.lower().startswith(value_prefix_lower):
                return
        elif prefix and not value.lower().startswith(prefix_lower):
            return
        key = (value, kind)
        if key in seen:
            return
        seen.add(key)
        items.append(
            {
                "value": value,
                "label": label or value,
                "kind": kind,
                "detail": detail,
                "documentation": documentation,
                "start_col": token.start_col,
                "end_col": token.end_col,
            }
        )

    if not dotted and not wants_relations:
        for keyword in CLICKHOUSE_KEYWORDS:
            add(keyword, "keyword", detail="keyword")

    if not dotted:
        for schema in snapshot.schemas:
            add(schema, "schema", detail="database")

    for item in snapshot.objects:
        add(item.name, item.kind, detail=item.schema, documentation=item.detail or None)
        add(f"{item.schema}.{item.name}", item.kind, label=item.name, detail=item.schema, documentation=item.detail or None)

    if wants_relations:
        return items

    if not dotted:
        for item in snapshot.functions:
            add(item.name, "function", detail=item.detail or "function")

    for column in snapshot.columns:
        if not dotted:
            add(column.name, "column", detail=f"{column.schema}.{column.table}", documentation=column.data_type or None)
        add(f"{column.table}.{column.name}", "column", label=column.name, detail=f"{column.schema}.{column.table}", documentation=column.data_type or None)
        add(f"{column.schema}.{column.table}.{column.name}", "column", label=column.name, detail=f"{column.schema}.{column.table}", documentation=column.data_type or None)

    for relation in relations:
        for column in snapshot.columns:
            if relation.schema and column.schema.lower() != relation.schema.lower():
                continue
            if column.table.lower() != relation.table.lower():
                continue
            add(
                f"{relation.completion_prefix}.{column.name}",
                "column",
                label=column.name,
                detail=f"{column.schema}.{column.table}",
                documentation=column.data_type or None,
            )

    return items


def parse_query_relations(sql: str) -> list[QueryRelation]:
    relations: list[QueryRelation] = []
    for statement in sqlparse.parse(sql):
        _collect_relations(statement, relations)
    unique: list[QueryRelation] = []
    seen: set[tuple[str, str, str]] = set()
    for relation in relations:
        key = (relation.schema.lower(), relation.table.lower(), relation.alias.lower())
        if key not in seen:
            seen.add(key)
            unique.append(relation)
    return unique


def _collect_relations(tokens: TokenList, relations: list[QueryRelation]) -> None:
    flattened = list(tokens.tokens)
    index = 0
    while index < len(flattened):
        token = flattened[index]
        if token.is_group:
            _collect_relations(token, relations)
        normalized = str(getattr(token, "normalized", "") or "").upper()
        relation_keyword = normalized in {"FROM", "UPDATE", "INTO", "TABLE"} or normalized.endswith("JOIN")
        if token.ttype is Keyword and relation_keyword:
            next_token = _next_meaningful(flattened, index + 1)
            if isinstance(next_token, IdentifierList):
                for ident in next_token.get_identifiers():
                    _append_identifier_relation(ident, relations)
            elif isinstance(next_token, Identifier):
                _append_identifier_relation(next_token, relations)
            elif next_token is not None and next_token.ttype in (Name, Keyword):
                value = str(next_token.value)
                relations.append(QueryRelation(schema="", table=value, alias=""))
        index += 1


def _append_identifier_relation(identifier: Identifier, relations: list[QueryRelation]) -> None:
    if any(token.is_group and token.value.strip().startswith("(") for token in identifier.tokens):
        return
    table = identifier.get_real_name() or ""
    if not table:
        return
    relations.append(
        QueryRelation(
            schema=identifier.get_parent_name() or "",
            table=table,
            alias=identifier.get_alias() or "",
        )
    )


def _next_meaningful(tokens: list[Any], start: int) -> Any:
    for token in tokens[start:]:
        if token.is_whitespace:
            continue
        return token
    return None


def _completion_token(payload: dict[str, Any]) -> CompletionToken:
    line_text = str(payload.get("line_text", ""))
    current_word = _completion_prefix(payload)
    cursor_col = _cursor_col(payload, line_text)
    if _cursor_is_after_relation_context(line_text, cursor_col):
        return CompletionToken("", cursor_col, cursor_col)
    for end_col in _candidate_end_cols(payload, line_text):
        candidate = _sql_token_before(line_text, end_col)
        if candidate.value and _token_matches_current_word(candidate.value, current_word):
            return candidate
    fallback = _sql_token_before(line_text, len(line_text))
    if fallback.value:
        return fallback
    current_word = str(payload.get("current_word", "")).strip()
    if not current_word:
        return CompletionToken("", None, None)
    start = max(0, len(line_text) - len(current_word))
    return CompletionToken(current_word.lstrip("([{\"'`").rstrip(",);]\"'`"), start, len(line_text))


def _completion_prefix(payload: dict[str, Any]) -> str:
    current_word = str(payload.get("current_word", "")).strip()
    if not current_word:
        return ""
    return current_word.lstrip("([{\"'`").rstrip(",);]\"'`")


def _candidate_end_cols(payload: dict[str, Any], line_text: str) -> list[int]:
    cursor = _cursor_col(payload, line_text)
    if isinstance(payload.get("cursor_col"), int):
        candidates = [cursor]
        if cursor > 0:
            candidates.append(cursor - 1)
        candidates.append(len(line_text))
    else:
        candidates = [len(line_text)]
    unique: list[int] = []
    for candidate in candidates:
        if candidate not in unique:
            unique.append(candidate)
    return unique


def _cursor_col(payload: dict[str, Any], line_text: str) -> int:
    raw = payload.get("cursor_col")
    if isinstance(raw, int):
        return max(0, min(raw, len(line_text)))
    return len(line_text)


def _cursor_is_after_relation_context(line_text: str, cursor_col: int) -> bool:
    if cursor_col <= 0:
        return False
    prefix = line_text[:cursor_col]
    if not prefix or not prefix[-1].isspace():
        return False
    words = re.findall(r"[A-Za-z_][A-Za-z0-9_]*", prefix)
    if not words:
        return False
    return words[-1].upper() in RELATION_CONTEXT_KEYWORDS


def _sql_token_before(line_text: str, end_col: int) -> CompletionToken:
    prefix = line_text[: max(0, min(end_col, len(line_text)))]
    match = re.search(r'(?:"[^"]*"|`[^`]*`|[A-Za-z_][A-Za-z0-9_$]*)(?:\.(?:"[^"]*"|`[^`]*`|[A-Za-z_][A-Za-z0-9_$]*))*\.?$', prefix)
    if not match:
        return CompletionToken("", end_col, end_col)
    return CompletionToken(match.group(0), match.start(), end_col)


def _token_matches_current_word(token: str, current_word: str) -> bool:
    if not current_word:
        return True
    stripped = token.rstrip(".")
    return stripped == current_word or stripped.endswith(current_word) or stripped.rsplit(".", 1)[-1] == current_word


def _sql_body(cell_text: str) -> str:
    lines = cell_text.splitlines()
    if lines and lines[0].lstrip().startswith("%%sql"):
        return "\n".join(lines[1:]).lstrip("\n")
    return cell_text


def _wants_relations(line_text: str, end_col: int | None) -> bool:
    prefix = line_text[:end_col] if isinstance(end_col, int) else line_text
    words = re.findall(r"[A-Za-z_][A-Za-z0-9_]*", prefix)
    if not words:
        return False
    return words[-1].upper() in RELATION_CONTEXT_KEYWORDS
