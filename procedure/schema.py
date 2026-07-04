from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .errors import SchemaError
from .sql import quote_identifier
from .utils import lower_snake


UNSUPPORTED_TYPES = {
    "BIGNUMERIC",
    "BIGDECIMAL",
    "DATETIME",
    "GEOGRAPHY",
    "JSON",
    "TIME",
}


SCALAR_TYPE_MAP = {
    "STRING": "VARCHAR",
    "INT64": "NUMBER(38,0)",
    "INTEGER": "NUMBER(38,0)",
    "FLOAT64": "FLOAT",
    "FLOAT": "FLOAT",
    "DOUBLE": "FLOAT",
    "BOOL": "BOOLEAN",
    "BOOLEAN": "BOOLEAN",
    "DATE": "DATE",
    "TIMESTAMP": "TIMESTAMP_LTZ(6)",
    "NUMERIC": "NUMBER(38,9)",
    "DECIMAL": "NUMBER(38,9)",
    "BYTES": "BINARY",
}


@dataclass(frozen=True)
class BigQueryField:
    name: str
    type: str
    mode: str = "NULLABLE"
    fields: list["BigQueryField"] = field(default_factory=list)

    @classmethod
    def from_api(cls, value: dict[str, Any]) -> "BigQueryField":
        return cls(
            name=value["name"],
            type=value["type"].upper(),
            mode=value.get("mode", "NULLABLE").upper(),
            fields=[cls.from_api(child) for child in value.get("fields", [])],
        )


@dataclass(frozen=True)
class SnowflakeColumn:
    storage_name: str
    snowflake_type: str
    alias: str
    children: list["SnowflakeColumn"] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {
            "storage_name": self.storage_name,
            "snowflake_type": self.snowflake_type,
            "alias": self.alias,
            "children": [child.as_dict() for child in self.children],
        }


def fields_from_bigquery_schema(schema: dict[str, Any] | list[dict[str, Any]]) -> list[BigQueryField]:
    if isinstance(schema, dict):
        raw_fields = schema.get("fields", [])
    else:
        raw_fields = schema
    return [BigQueryField.from_api(field) for field in raw_fields]


def snowflake_type_for_field(field: BigQueryField) -> str:
    base_type = field.type.upper()
    if base_type in UNSUPPORTED_TYPES:
        raise SchemaError(f"Unsupported BigQuery type for Iceberg sync: {field.name} {base_type}")

    if base_type in {"RECORD", "STRUCT"}:
        if not field.fields:
            raise SchemaError(f"RECORD field {field.name} must contain nested fields.")
        inner = ", ".join(
            f"{quote_identifier(child.name)} {snowflake_type_for_field(child)}"
            for child in field.fields
        )
        mapped = f"OBJECT({inner})"
    else:
        try:
            mapped = SCALAR_TYPE_MAP[base_type]
        except KeyError as exc:
            raise SchemaError(f"Unsupported BigQuery type for Iceberg sync: {field.name} {base_type}") from exc

    if field.mode == "REPEATED":
        return f"ARRAY({mapped})"
    if field.mode not in {"NULLABLE", "REQUIRED"}:
        raise SchemaError(f"Unsupported BigQuery mode for {field.name}: {field.mode}")
    return mapped


def columns_from_bigquery_schema(schema: dict[str, Any] | list[dict[str, Any]]) -> list[SnowflakeColumn]:
    fields = fields_from_bigquery_schema(schema)
    columns = [
        SnowflakeColumn(
            storage_name=field.name,
            snowflake_type=snowflake_type_for_field(field),
            alias=lower_snake(field.name),
            children=[
                SnowflakeColumn(
                    storage_name=child.name,
                    snowflake_type=snowflake_type_for_field(child),
                    alias=lower_snake(child.name),
                )
                for child in field.fields
            ],
        )
        for field in fields
    ]
    validate_aliases(columns)
    return columns


def validate_aliases(columns: list[SnowflakeColumn]) -> None:
    seen: dict[str, str] = {}
    for column in columns:
        if column.alias in seen:
            raise SchemaError(
                "Lower-snake view alias collision: "
                f"{seen[column.alias]!r} and {column.storage_name!r} both map to {column.alias!r}"
            )
        seen[column.alias] = column.storage_name


def desired_column_type_map(columns: list[SnowflakeColumn]) -> dict[str, str]:
    return {column.storage_name: normalize_type(column.snowflake_type) for column in columns}


def normalize_type(value: str) -> str:
    return " ".join(str(value).upper().split())


def validate_schema_compatibility(
    existing_columns: dict[str, str],
    desired_columns: list[SnowflakeColumn],
) -> list[SnowflakeColumn]:
    """Return additive top-level columns, or raise for unsafe changes."""

    desired = desired_column_type_map(desired_columns)
    normalized_existing = {name: normalize_type(data_type) for name, data_type in existing_columns.items()}

    removed = [name for name in normalized_existing if name not in desired]
    if removed:
        raise SchemaError("Source schema removed existing target column(s): " + ", ".join(sorted(removed)))

    changed = [
        name
        for name, data_type in normalized_existing.items()
        if name in desired and desired[name] != data_type
    ]
    if changed:
        details = ", ".join(
            f"{name}: existing {normalized_existing[name]} vs desired {desired[name]}"
            for name in sorted(changed)
        )
        raise SchemaError("Source schema changed existing target column type(s): " + details)

    return [column for column in desired_columns if column.storage_name not in normalized_existing]
