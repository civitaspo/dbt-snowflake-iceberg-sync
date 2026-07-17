"""Source schema mapping helpers for Snowflake Iceberg sync."""

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
    "DATETIME": "TIMESTAMP_NTZ(6)",
    "TIMESTAMP": "TIMESTAMP_LTZ(6)",
    "NUMERIC": "NUMBER(38,9)",
    "DECIMAL": "NUMBER(38,9)",
    "BYTES": "BINARY",
}

UNSUPPORTED_TYPES = {
    "BIGNUMERIC",
    "BIGDECIMAL",
    "GEOGRAPHY",
    "JSON",
    "TIME",
}

UNSUPPORTED_PARQUET_TYPE_MARKERS = {
    "GEOGRAPHY",
    "GEOMETRY",
    "VECTOR",
}


@dataclass(frozen=True)
class SnowflakeColumn:
    source_name: str
    snowflake_type: str
    nullable: bool = True
    fields: tuple[SnowflakeColumn, ...] = field(default_factory=tuple)
    alias: str | None = None
    expression: str | None = None

    @property
    def ddl(self) -> str:
        null_sql = "" if self.nullable else " NOT NULL"
        return f"{quote_identifier(self.source_name)} {self.snowflake_type}{null_sql}"


@dataclass(frozen=True)
class ViewColumn:
    source_name: str
    alias: str
    expression: str | None = None


def map_bigquery_schema(fields: list[dict[str, Any]]) -> list[SnowflakeColumn]:
    columns = [_map_field(field) for field in fields]
    validate_view_aliases(columns)
    return columns


def map_parquet_infer_schema(fields: list[dict[str, Any]]) -> list[SnowflakeColumn]:
    """Map Snowflake INFER_SCHEMA rows into Iceberg DDL column objects."""

    ordered = sorted(fields, key=_infer_schema_order)
    columns = [_map_infer_schema_field(field) for field in ordered]
    validate_view_aliases(columns)
    return columns


def map_parquet_declared_schema(fields: list[dict[str, Any]]) -> list[SnowflakeColumn]:
    """Map user-declared s3_parquet column definitions into Iceberg DDL columns."""

    if not fields:
        raise SchemaError("s3_parquet_columns must not be empty when set")
    columns = [_map_declared_schema_field(field, index) for index, field in enumerate(fields)]
    names = [column.source_name for column in columns]
    duplicate_names = sorted({name for name in names if names.count(name) > 1})
    if duplicate_names:
        raise SchemaError(
            "s3_parquet_columns contains duplicate column names: " + ", ".join(duplicate_names)
        )
    validate_view_aliases(columns)
    return columns


def view_columns(columns: list[SnowflakeColumn]) -> list[ViewColumn]:
    result = [
        ViewColumn(
            source_name=column.source_name,
            alias=column.alias or lower_snake(column.source_name),
            expression=column.expression,
        )
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
    result = re.sub(r"\bTEXT\b", "VARCHAR", result)
    result = re.sub(r"\bSTRING\b", "VARCHAR", result)
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


def _infer_schema_order(field: dict[str, Any]) -> int:
    order = field.get("ORDER_ID")
    if order is None:
        order = field.get("order_id")
    try:
        return int(order)
    except (TypeError, ValueError):
        return 0


def _map_infer_schema_field(field: dict[str, Any]) -> SnowflakeColumn:
    name = (
        field.get("COLUMN_NAME")
        or field.get("column_name")
        or field.get("name")
        or field.get("NAME")
    )
    if not name:
        raise SchemaError("INFER_SCHEMA field is missing COLUMN_NAME")
    type_name = (
        field.get("TYPE") or field.get("type") or field.get("EXPRESSION") or field.get("expression")
    )
    if not type_name:
        raise SchemaError(f"INFER_SCHEMA field {name!r} is missing TYPE")
    snowflake_type = _normalize_infer_schema_type(str(type_name))
    nullable_value = field.get("NULLABLE")
    if nullable_value is None:
        nullable_value = field.get("nullable")
    if nullable_value is None:
        nullable = True
    elif isinstance(nullable_value, bool):
        nullable = nullable_value
    else:
        nullable = str(nullable_value).strip().upper() in {"TRUE", "Y", "YES", "1"}
    return SnowflakeColumn(
        source_name=str(name),
        snowflake_type=snowflake_type,
        nullable=nullable,
    )


def _map_declared_schema_field(field: dict[str, Any], index: int) -> SnowflakeColumn:
    if not isinstance(field, dict):
        raise SchemaError(f"s3_parquet_columns[{index}] must be an object")
    name = field.get("name") or field.get("NAME") or field.get("COLUMN_NAME")
    if name is None or str(name).strip() == "":
        raise SchemaError(f"s3_parquet_columns[{index}].name is required")
    type_name = field.get("type") or field.get("TYPE")
    if type_name is None or str(type_name).strip() == "":
        raise SchemaError(f"s3_parquet_columns[{index}].type is required")
    snowflake_type = _normalize_infer_schema_type(str(type_name).strip())
    nullable_value = field.get("nullable")
    if nullable_value is None:
        nullable_value = field.get("NULLABLE")
    if nullable_value is None:
        nullable = True
    elif isinstance(nullable_value, bool):
        nullable = nullable_value
    else:
        nullable = str(nullable_value).strip().upper() in {"TRUE", "Y", "YES", "1"}
    alias_value = field.get("alias")
    if alias_value is not None and str(alias_value).strip() == "":
        raise SchemaError(f"s3_parquet_columns[{index}].alias must not be empty when set")
    alias = str(alias_value).strip() if alias_value is not None else None
    expression_value = field.get("expression")
    if expression_value is not None and str(expression_value).strip() == "":
        raise SchemaError(f"s3_parquet_columns[{index}].expression must not be empty when set")
    expression = str(expression_value).strip() if expression_value is not None else None
    return SnowflakeColumn(
        source_name=str(name),
        snowflake_type=snowflake_type,
        nullable=nullable,
        alias=alias,
        expression=expression,
    )


def _normalize_infer_schema_type(type_name: str) -> str:
    result = type_name.strip()
    upper = result.upper()
    for marker in UNSUPPORTED_PARQUET_TYPE_MARKERS:
        if marker in upper:
            raise SchemaError(f"Parquet/INFER_SCHEMA type {type_name} is not supported")
    result = re.sub(r"\bTEXT\b", "VARCHAR", result, flags=re.IGNORECASE)
    result = re.sub(r"\bSTRING\b", "VARCHAR", result, flags=re.IGNORECASE)
    result = re.sub(r"\bFLOAT\b", "DOUBLE", result, flags=re.IGNORECASE)
    result = re.sub(r"\bNUMBER\(19,\s*0\)", "BIGINT", result, flags=re.IGNORECASE)
    return result


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
