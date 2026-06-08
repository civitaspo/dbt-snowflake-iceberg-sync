"""Small utility helpers shared by the procedure modules."""

from __future__ import annotations

import hashlib
import json
import re
import uuid
from datetime import UTC, datetime
from typing import Any


def utcnow() -> datetime:
    return datetime.now(tz=UTC)


def new_run_id() -> str:
    return str(uuid.uuid4())


def stable_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def stable_hash(value: Any, length: int = 16) -> str:
    return hashlib.sha256(stable_json(value).encode("utf-8")).hexdigest()[:length]


def quote_identifier(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def normalize_snowflake_object_identifier(identifier: str) -> str:
    return str(identifier).strip().replace('"', "").upper()


def quote_object_identifier(identifier: str) -> str:
    return quote_identifier(normalize_snowflake_object_identifier(identifier))


def quote_fqn(database: str, schema: str, identifier: str) -> str:
    return ".".join(
        quote_object_identifier(part)
        for part in (
            database,
            schema,
            identifier,
        )
    )


def quote_stage_fqn(parts: list[str]) -> str:
    return ".".join(quote_object_identifier(part) for part in parts)


def sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def bool_sql(value: bool) -> str:
    return "TRUE" if value else "FALSE"


def lower_snake(name: str) -> str:
    """Convert a source field name to the unquoted lower-snake view alias."""

    value = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name)
    value = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", value)
    value = re.sub(r"[^0-9A-Za-z]+", "_", value)
    value = re.sub(r"_+", "_", value).strip("_").lower()
    if not value:
        value = "field"
    if value[0].isdigit():
        value = f"_{value}"
    return value


def load_snowflake_secret(alias: str) -> str:
    """Read a Snowflake secret in procedure runtime.

    Snowflake exposes secret helpers through the special `_snowflake` module.
    Keeping the import local lets unit tests import the procedure package outside
    Snowflake.
    """

    try:
        import _snowflake  # type: ignore[import-not-found]
    except ImportError as exc:  # pragma: no cover - only reached outside Snowflake
        raise RuntimeError("Snowflake secret APIs are available only inside Snowflake") from exc
    return _snowflake.get_generic_secret_string(alias)


def parse_json_maybe(value: Any) -> Any:
    if isinstance(value, str):
        return json.loads(value)
    return value
