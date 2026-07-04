from __future__ import annotations

from collections.abc import Iterable
from typing import Any


def quote_identifier(identifier: str) -> str:
    return '"' + str(identifier).replace('"', '""') + '"'


def quote_relation(database: str, schema: str, identifier: str) -> str:
    return ".".join(
        quote_identifier(part)
        for part in [database, schema, identifier]
        if part is not None and part != ""
    )


def string_literal(value: Any) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def bool_literal(value: bool) -> str:
    return "TRUE" if value else "FALSE"


def csv(values: Iterable[str]) -> str:
    return ", ".join(values)


def relation_from_config(relation: dict[str, Any]) -> str:
    return quote_relation(
        relation["database"],
        relation["schema"],
        relation["identifier"],
    )


def stage_copy_location(named_stage_location: str, run_prefix: str) -> str:
    base = named_stage_location.rstrip("/")
    clean_prefix = run_prefix.strip("/")
    return f"{base}/{clean_prefix}/"

