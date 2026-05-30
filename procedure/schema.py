"""BigQuery to Snowflake Iceberg schema mapping."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

from .errors import SchemaError
from .utils import lower_snake, quote_identifier

SCALAR_TYPE_MAP = {
    "STRING": "VARCHAR",
    "INT64": "BIGINT",
    "INTEGER": "BIGINT",
    "FLOAT64": "DOUBLE",
    "FLOAT": "DOUBLE",
    "DOUBLE": "DOUBLE",
    "BOOL": "BOOLEAN",
    "BOOLEAN": "BOOLEAN",
    "DATE": "DATE",
    "TIMESTAMP": "TIMESTAMP_LTZ(6)",
    "NUMERIC": "NUMBER(38,9)",
    "DECIMAL": "NUMBER(38,9)",
    "BYTES": "BINARY",
}

UNSUPPORTED_TYPES = {
    "BIGNUMERIC",
    "BIGDECIMAL",
    "DATETIME",
    "GEOGRAPHY",
    "JSON",
    "TIME",
}


@dataclass(frozen=True)
class SnowflakeColumn:
    source_name: str
    snowflake_type: str
    nullable: bool = True
    fields: tuple[SnowflakeColumn, ...] = field(default_factory=tuple)

    @property
    def ddl(self) -> str:
        null_sql = "" if self.nullable else " NOT NULL"
        return f"{quote_identifier(self.source_name)} {self.snowflake_type}{null_sql}"


@dataclass(frozen=True)
class ViewColumn:
    source_name: str
    alias: str


def map_bigquery_schema(fields: list[dict[str, Any]]) -> list[SnowflakeColumn]:
    columns = [_map_field(field) for field in fields]
    validate_view_aliases(columns)
    return columns


def view_columns(columns: list[SnowflakeColumn]) -> list[ViewColumn]:
    result = [
        ViewColumn(source_name=column.source_name, alias=lower_snake(column.source_name))
        for column in columns
    ]
    aliases = [column.alias for column in result]
    duplicates = sorted({alias for alias in aliases if aliases.count(alias) > 1})
    if duplicates:
        raise SchemaError("view alias collisions detected: " + ", ".join(duplicates))
    return result


def validate_view_aliases(columns: list[SnowflakeColumn]) -> None:
    view_columns(columns)


def validate_schema_compatibility(
    existing_columns: list[SnowflakeColumn], desired_columns: list[SnowflakeColumn]
) -> None:
    """Allow only safe additive schema evolution.

    The first scope is intentionally conservative: existing columns must keep
    order, names, and exact mapped types. New columns may be appended.
    """

    if len(existing_columns) > len(desired_columns):
        raise SchemaError("source schema removed one or more existing columns")
    for index, existing in enumerate(existing_columns):
        desired = desired_columns[index]
        if existing.source_name != desired.source_name:
            raise SchemaError(
                "source schema reordered or renamed columns; expected "
                f"{existing.source_name!r}, found {desired.source_name!r}"
            )
        _assert_same_column(existing, desired)


def _assert_same_column(existing: SnowflakeColumn, desired: SnowflakeColumn) -> None:
    if _normalized_type(existing.snowflake_type) != _normalized_type(desired.snowflake_type):
        raise SchemaError(
            f"incompatible type change for {existing.source_name}: "
            f"{existing.snowflake_type} -> {desired.snowflake_type}"
        )
    if len(existing.fields) > len(desired.fields):
        raise SchemaError(f"nested fields were removed from {existing.source_name}")
    for index, existing_nested in enumerate(existing.fields):
        desired_nested = desired.fields[index]
        if existing_nested.source_name != desired_nested.source_name:
            raise SchemaError(f"nested field order changed under {existing.source_name}")
        _assert_same_column(existing_nested, desired_nested)


def _normalized_type(snowflake_type: str) -> str:
    result = snowflake_type.upper()
    result = re.sub(r"\b(VARCHAR|TEXT|STRING)\(\d+\)", "VARCHAR", result)
    result = re.sub(r"\bNUMBER\(19,0\)", "BIGINT", result)
    result = re.sub(r"\bFLOAT\b", "DOUBLE", result)
    result = result.replace("TEXT", "VARCHAR").replace("STRING", "VARCHAR")
    result = result.replace('"', "")
    return result


def _map_field(field: dict[str, Any]) -> SnowflakeColumn:
    name = str(field.get("name") or "")
    if not name:
        raise SchemaError("BigQuery schema field is missing a name")
    mode = str(field.get("mode") or "NULLABLE").upper()
    nullable = mode != "REQUIRED"

    if mode == "REPEATED":
        inner_type, nested_fields = _map_field_type(field, repeated_element=True)
        return SnowflakeColumn(
            source_name=name,
            snowflake_type=f"ARRAY({inner_type})",
            nullable=True,
            fields=nested_fields,
        )

    snowflake_type, nested_fields = _map_field_type(field, repeated_element=False)
    return SnowflakeColumn(
        source_name=name,
        snowflake_type=snowflake_type,
        nullable=nullable,
        fields=nested_fields,
    )


def _map_field_type(
    field: dict[str, Any], *, repeated_element: bool
) -> tuple[str, tuple[SnowflakeColumn, ...]]:
    field_type = str(field.get("type") or "").upper()
    if field_type in UNSUPPORTED_TYPES:
        raise SchemaError(f"BigQuery type {field_type} is not supported")
    if field_type in {"RECORD", "STRUCT"}:
        nested_fields = tuple(_map_field(child) for child in field.get("fields") or ())
        if not nested_fields:
            raise SchemaError(f"RECORD field {field.get('name')} must contain nested fields")
        inner = ", ".join(
            f"{quote_identifier(child.source_name)} {child.snowflake_type}"
            for child in nested_fields
        )
        return f"OBJECT({inner})", nested_fields
    if field_type not in SCALAR_TYPE_MAP:
        raise SchemaError(f"BigQuery type {field_type or '<missing>'} is not supported")
    if repeated_element and field_type == "BYTES":
        raise SchemaError("repeated BYTES fields are not supported")
    return SCALAR_TYPE_MAP[field_type], ()


def columns_from_snowflake_describe(rows: list[Any]) -> list[SnowflakeColumn]:
    """Build comparable column objects from DESCRIBE TABLE output rows."""

    columns: list[SnowflakeColumn] = []
    for row in rows:
        data = _row_to_mapping(row)
        name = data.get("name") or data.get("NAME")
        type_name = data.get("type") or data.get("TYPE")
        null_value = data.get("null?") or data.get("NULL?")
        if not name or not type_name:
            continue
        columns.append(
            SnowflakeColumn(
                source_name=str(name),
                snowflake_type=str(type_name).upper(),
                nullable=str(null_value).upper() != "N",
            )
        )
    return columns


def _row_to_mapping(row: Any) -> dict[str, Any]:
    if isinstance(row, dict):
        return row
    if hasattr(row, "as_dict"):
        return row.as_dict()
    if hasattr(row, "asDict"):
        return row.asDict()
    return {
        key: getattr(row, key)
        for key in dir(row)
        if not key.startswith("_") and not callable(getattr(row, key))
    }
