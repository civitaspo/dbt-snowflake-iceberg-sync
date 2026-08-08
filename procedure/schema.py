"""Source schema mapping helpers for Snowflake Iceberg sync."""

from __future__ import annotations

import re
from dataclasses import dataclass, field, replace
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
    # BigQuery EXTRACT to Parquet writes TIMESTAMP with isAdjustedToUTC=false.
    # Snowflake's vectorized Parquet scanner / Iceberg ADD_FILES_COPY requires
    # TIMESTAMP_NTZ for that metadata (TIMESTAMP_LTZ expects isAdjustedToUTC=true).
    "TIMESTAMP": "TIMESTAMP_NTZ(6)",
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

_DECIMAL_TYPE_PATTERN = re.compile(
    r"^NUMBER\((\d+),\s*(\d+)\)$",
    re.IGNORECASE,
)


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


@dataclass(frozen=True)
class SchemaEvolutionPlan:
    add_columns: tuple[SnowflakeColumn, ...] = ()
    alter_columns: tuple[SnowflakeColumn, ...] = ()
    warnings: tuple[str, ...] = ()

    @property
    def altered(self) -> bool:
        return bool(self.add_columns or self.alter_columns)


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


def map_declared_columns(fields: list[dict[str, Any]]) -> list[SnowflakeColumn]:
    """Map user-declared column definitions into Iceberg DDL columns."""

    if not fields:
        raise SchemaError("columns must not be empty when set")
    columns = [_map_declared_schema_field(field, index) for index, field in enumerate(fields)]
    names = [column.source_name for column in columns]
    duplicate_names = sorted({name for name in names if names.count(name) > 1})
    if duplicate_names:
        raise SchemaError("columns contains duplicate column names: " + ", ".join(duplicate_names))
    validate_view_aliases(columns)
    return columns


def view_columns(
    columns: list[SnowflakeColumn],
    *,
    expressions_applied_at_load: bool = False,
) -> list[ViewColumn]:
    result = [
        ViewColumn(
            source_name=column.source_name,
            alias=column.alias or lower_snake(column.source_name),
            expression=None if expressions_applied_at_load else column.expression,
        )
        for column in columns
    ]
    aliases = [column.alias for column in result]
    duplicates = sorted({alias for alias in aliases if aliases.count(alias) > 1})
    if duplicates:
        raise SchemaError("view alias collisions detected: " + ", ".join(duplicates))
    return result


def load_uses_column_expressions(
    *,
    source_type: str,
    load_mode: str | None,
    columns: list[SnowflakeColumn] | tuple[SnowflakeColumn, ...],
) -> bool:
    """Return True when FULL_INGEST should apply columns.expression in COPY SELECT."""

    if source_type != "s3_parquet":
        return False
    if (load_mode or "add_files_copy").strip().lower() != "full_ingest":
        return False
    return any(column.expression for column in columns)


def validate_view_aliases(columns: list[SnowflakeColumn]) -> None:
    view_columns(columns)


def validate_schema_compatibility(
    existing_columns: list[SnowflakeColumn], desired_columns: list[SnowflakeColumn]
) -> None:
    """Validate schema evolution without applying DDL."""

    plan_schema_evolution(existing_columns, desired_columns)


def plan_schema_evolution(
    existing_columns: list[SnowflakeColumn], desired_columns: list[SnowflakeColumn]
) -> SchemaEvolutionPlan:
    """Plan top-level additive columns and nested structured-type evolution.

    Top-level policy remains conservative: order/names must match and types must
    stay equivalent after normalization (no top-level widen/remove/reorder).

    Nested OBJECT / ARRAY(OBJECT) fields support add, reorder, and Iceberg type
    widening. Existing nested fields missing from the desired schema are kept
    (never dropped) and reported as warnings. Nested rename is not inferred.
    """

    if len(existing_columns) > len(desired_columns):
        raise SchemaError("source schema removed one or more existing columns")

    alter_columns: list[SnowflakeColumn] = []
    warnings: list[str] = []
    for index, existing in enumerate(existing_columns):
        desired = desired_columns[index]
        if _field_key(existing.source_name) != _field_key(desired.source_name):
            raise SchemaError(
                "source schema reordered or renamed columns; expected "
                f"{existing.source_name!r}, found {desired.source_name!r}"
            )
        altered, column_warnings = _plan_top_level_column(existing, desired)
        warnings.extend(column_warnings)
        if altered is not None:
            alter_columns.append(altered)

    add_columns = tuple(desired_columns[len(existing_columns) :])
    return SchemaEvolutionPlan(
        add_columns=add_columns,
        alter_columns=tuple(alter_columns),
        warnings=tuple(warnings),
    )


def columns_from_payload(
    columns: list[dict[str, Any]] | tuple[dict[str, Any], ...],
) -> list[SnowflakeColumn]:
    """Rebuild SnowflakeColumn objects from procedure/materialization payloads."""

    result: list[SnowflakeColumn] = []
    for index, raw in enumerate(columns):
        if not isinstance(raw, dict):
            raise SchemaError(f"columns[{index}] must be an object")
        result.append(_column_from_payload(raw, index))
    return result


def enrich_column_fields(column: SnowflakeColumn) -> SnowflakeColumn:
    """Populate nested fields by parsing structured Snowflake type strings."""

    if column.fields:
        enriched_children = tuple(enrich_column_fields(child) for child in column.fields)
        if enriched_children != column.fields:
            return replace(column, fields=enriched_children)
        return column
    parsed_fields = parse_structured_fields(column.snowflake_type)
    if not parsed_fields:
        return column
    return replace(column, fields=parsed_fields)


def parse_structured_fields(snowflake_type: str) -> tuple[SnowflakeColumn, ...]:
    """Parse OBJECT(...) / ARRAY(OBJECT(...)) nested fields from a type string."""

    text = snowflake_type.strip()
    # TODO: Support structured MAP(...) storage and evolution when a source needs it.
    if _normalized_type(text).startswith("MAP("):
        return ()
    kind = _structured_kind(text)
    if kind == "array_object":
        inner = _paren_content(text, prefix="ARRAY")
        return _parse_object_fields(inner)
    if kind == "object":
        return _parse_object_fields(text)
    return ()


def _plan_top_level_column(
    existing: SnowflakeColumn, desired: SnowflakeColumn
) -> tuple[SnowflakeColumn | None, list[str]]:
    existing = enrich_column_fields(existing)
    desired = enrich_column_fields(desired)
    existing_kind = _structured_kind(existing.snowflake_type)
    desired_kind = _structured_kind(desired.snowflake_type)

    if existing_kind in {"object", "array_object"} or desired_kind in {"object", "array_object"}:
        if existing_kind != desired_kind:
            raise SchemaError(
                f"incompatible type change for {existing.source_name}: "
                f"{existing.snowflake_type} -> {desired.snowflake_type}"
            )
        merged, warnings, changed = _merge_structured_fields(
            existing.fields,
            desired.fields,
            path=existing.source_name,
            kind=existing_kind or desired_kind or "object",
        )
        if not changed:
            return None, warnings
        snowflake_type = _render_structured_type(existing_kind or "object", merged)
        return (
            replace(
                desired,
                snowflake_type=snowflake_type,
                fields=merged,
            ),
            warnings,
        )

    if _normalized_type(existing.snowflake_type) != _normalized_type(desired.snowflake_type):
        raise SchemaError(
            f"incompatible type change for {existing.source_name}: "
            f"{existing.snowflake_type} -> {desired.snowflake_type}"
        )
    return None, []


def _merge_structured_fields(
    existing_fields: tuple[SnowflakeColumn, ...],
    desired_fields: tuple[SnowflakeColumn, ...],
    *,
    path: str,
    kind: str,
) -> tuple[tuple[SnowflakeColumn, ...], list[str], bool]:
    existing_fields = tuple(enrich_column_fields(field) for field in existing_fields)
    desired_fields = tuple(enrich_column_fields(field) for field in desired_fields)
    existing_by_key = {_field_key(field.source_name): field for field in existing_fields}
    desired_by_key = {_field_key(field.source_name): field for field in desired_fields}
    if len(existing_by_key) != len(existing_fields):
        raise SchemaError(f"duplicate nested field names under {path}")
    if len(desired_by_key) != len(desired_fields):
        raise SchemaError(f"duplicate nested field names under {path}")

    merged: list[SnowflakeColumn] = []
    warnings: list[str] = []
    changed = False

    for desired in desired_fields:
        key = _field_key(desired.source_name)
        existing = existing_by_key.get(key)
        if existing is None:
            merged.append(desired)
            changed = True
            continue
        child, child_warnings, child_changed = _merge_nested_column(
            existing, desired, path=f"{path}.{desired.source_name}"
        )
        warnings.extend(child_warnings)
        merged.append(child)
        # Case-only identifier spelling differences are the same field; do not
        # force SET DATA TYPE solely to rewrite quotes/case from DESCRIBE.
        if child_changed:
            changed = True

    for existing in existing_fields:
        key = _field_key(existing.source_name)
        if key in desired_by_key:
            continue
        warnings.append(
            f"keeping nested field {path}.{existing.source_name} absent from source schema"
        )
        merged.append(existing)

    matched_existing_order = [
        _field_key(field.source_name)
        for field in existing_fields
        if _field_key(field.source_name) in desired_by_key
    ]
    matched_desired_order = [_field_key(field.source_name) for field in desired_fields]
    if matched_existing_order != matched_desired_order:
        changed = True

    _ = kind  # kind is selected by the caller when rendering the merged type.
    return tuple(merged), warnings, changed


def _merge_nested_column(
    existing: SnowflakeColumn, desired: SnowflakeColumn, *, path: str
) -> tuple[SnowflakeColumn, list[str], bool]:
    existing = enrich_column_fields(existing)
    desired = enrich_column_fields(desired)
    existing_kind = _structured_kind(existing.snowflake_type)
    desired_kind = _structured_kind(desired.snowflake_type)

    if existing_kind in {"object", "array_object"} or desired_kind in {"object", "array_object"}:
        if existing_kind != desired_kind:
            raise SchemaError(
                f"incompatible type change for {path}: "
                f"{existing.snowflake_type} -> {desired.snowflake_type}"
            )
        merged_fields, warnings, changed = _merge_structured_fields(
            existing.fields,
            desired.fields,
            path=path,
            kind=existing_kind or desired_kind or "object",
        )
        snowflake_type = _render_structured_type(existing_kind or "object", merged_fields)
        if not changed and _normalized_type(existing.snowflake_type) == _normalized_type(
            snowflake_type
        ):
            return (
                replace(desired, snowflake_type=snowflake_type, fields=merged_fields),
                warnings,
                False,
            )
        return (
            replace(desired, snowflake_type=snowflake_type, fields=merged_fields),
            warnings,
            True,
        )

    if _types_compatible(existing.snowflake_type, desired.snowflake_type):
        type_changed = _normalized_type(existing.snowflake_type) != _normalized_type(
            desired.snowflake_type
        )
        if type_changed:
            return replace(desired), [], True
        # Prefer desired spelling in the in-memory merge result, but skip DDL
        # when only identifier case/quoting differs from DESCRIBE output.
        return replace(desired), [], False

    if _can_widen_type(existing.snowflake_type, desired.snowflake_type):
        return replace(desired), [], True

    raise SchemaError(
        f"incompatible type change for {path}: "
        f"{existing.snowflake_type} -> {desired.snowflake_type}"
    )


def _types_compatible(existing: str, desired: str) -> bool:
    return _normalized_type(existing) == _normalized_type(desired)


def _can_widen_type(existing: str, desired: str) -> bool:
    """Return True when desired is an Iceberg-compatible widening of existing."""

    existing_norm = _normalized_type(existing)
    desired_norm = _normalized_type(desired)
    if existing_norm == desired_norm:
        return True

    # Iceberg int -> long. FLOAT is normalized to DOUBLE already.
    if _is_iceberg_int_type(existing_norm) and desired_norm in {"BIGINT", "LONG"}:
        return True

    existing_decimal = _DECIMAL_TYPE_PATTERN.fullmatch(existing_norm)
    desired_decimal = _DECIMAL_TYPE_PATTERN.fullmatch(desired_norm)
    if existing_decimal and desired_decimal:
        existing_precision = int(existing_decimal.group(1))
        existing_scale = int(existing_decimal.group(2))
        desired_precision = int(desired_decimal.group(1))
        desired_scale = int(desired_decimal.group(2))
        return existing_scale == desired_scale and desired_precision >= existing_precision
    return False


def _is_iceberg_int_type(normalized_type: str) -> bool:
    if normalized_type in {"INTEGER", "INT"}:
        return True
    match = _DECIMAL_TYPE_PATTERN.fullmatch(normalized_type)
    return bool(match and int(match.group(2)) == 0 and int(match.group(1)) <= 10)


def _structured_kind(snowflake_type: str) -> str | None:
    normalized = _normalized_type(snowflake_type)
    if normalized.startswith("ARRAY(OBJECT("):
        return "array_object"
    if normalized.startswith("OBJECT("):
        return "object"
    if normalized.startswith("MAP("):
        return "map"
    return None


def _render_structured_type(kind: str, fields: tuple[SnowflakeColumn, ...]) -> str:
    inner = ", ".join(
        f"{quote_identifier(child.source_name)} {child.snowflake_type}" for child in fields
    )
    if kind == "array_object":
        return f"ARRAY(OBJECT({inner}))"
    return f"OBJECT({inner})"


def _parse_object_fields(object_type: str) -> tuple[SnowflakeColumn, ...]:
    text = object_type.strip()
    if not _normalized_type(text).startswith("OBJECT("):
        raise SchemaError(f"expected OBJECT(...) type, found {object_type!r}")
    inner = _paren_content(text, prefix="OBJECT")
    fields: list[SnowflakeColumn] = []
    for name, type_str in _split_object_field_defs(inner):
        cleaned_type = _strip_null_constraint(type_str)
        nested_fields = parse_structured_fields(cleaned_type)
        fields.append(
            SnowflakeColumn(
                source_name=name,
                snowflake_type=cleaned_type,
                nullable=True,
                fields=nested_fields,
            )
        )
    return tuple(fields)


def _split_object_field_defs(inner: str) -> list[tuple[str, str]]:
    results: list[tuple[str, str]] = []
    index = 0
    length = len(inner)
    while index < length:
        while index < length and inner[index].isspace():
            index += 1
        if index >= length:
            break
        if inner[index] == '"':
            index += 1
            start = index
            while index < length and inner[index] != '"':
                if inner[index] == '"' and index + 1 < length and inner[index + 1] == '"':
                    index += 2
                    continue
                index += 1
            name = inner[start:index].replace('""', '"')
            if index >= length or inner[index] != '"':
                raise SchemaError(f"malformed structured type field name in {inner!r}")
            index += 1
        else:
            start = index
            while index < length and (inner[index].isalnum() or inner[index] == "_"):
                index += 1
            name = inner[start:index]
            if not name:
                raise SchemaError(f"malformed structured type field list in {inner!r}")
        while index < length and inner[index].isspace():
            index += 1
        type_start = index
        depth = 0
        while index < length:
            char = inner[index]
            if char == "(":
                depth += 1
            elif char == ")":
                depth -= 1
            elif char == "," and depth == 0:
                break
            index += 1
        type_str = inner[type_start:index].strip()
        if not type_str:
            raise SchemaError(f"malformed structured type field type in {inner!r}")
        results.append((name, type_str))
        if index < length and inner[index] == ",":
            index += 1
    return results


def _paren_content(type_text: str, *, prefix: str) -> str:
    text = type_text.strip()
    if not text.upper().startswith(prefix.upper() + "("):
        raise SchemaError(f"expected {prefix}(...) type, found {type_text!r}")
    start = len(prefix)
    while start < len(text) and text[start].isspace():
        start += 1
    if start >= len(text) or text[start] != "(":
        raise SchemaError(f"expected {prefix}(...) type, found {type_text!r}")
    depth = 0
    for index in range(start, len(text)):
        char = text[index]
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
            if depth == 0:
                return text[start + 1 : index]
    raise SchemaError(f"unbalanced parentheses in type {type_text!r}")


def _strip_null_constraint(type_str: str) -> str:
    return re.sub(r"\s+NOT\s+NULL\s*$", "", type_str.strip(), flags=re.IGNORECASE)


def _field_key(name: str) -> str:
    return name.upper()


def _normalized_type(snowflake_type: str) -> str:
    result = snowflake_type.upper()
    result = re.sub(r"\b(VARCHAR|TEXT|STRING)\(\d+\)", "VARCHAR", result)
    result = re.sub(r"\bNUMBER\(19,0\)", "BIGINT", result)
    result = re.sub(r"\bFLOAT\b", "DOUBLE", result)
    result = re.sub(r"\bTEXT\b", "VARCHAR", result)
    result = re.sub(r"\bSTRING\b", "VARCHAR", result)
    result = result.replace('"', "")
    result = re.sub(r"\s+", " ", result).strip()
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
        column = SnowflakeColumn(
            source_name=str(name),
            snowflake_type=str(type_name).upper(),
            nullable=str(null_value).upper() != "N",
        )
        columns.append(enrich_column_fields(column))
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
    # TODO: Support structured MAP(...) when INFER_SCHEMA returns MAP types.
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
    column = SnowflakeColumn(
        source_name=str(name),
        snowflake_type=snowflake_type,
        nullable=nullable,
    )
    return enrich_column_fields(column)


def _map_declared_schema_field(field: dict[str, Any], index: int) -> SnowflakeColumn:
    if not isinstance(field, dict):
        raise SchemaError(f"columns[{index}] must be an object")
    name = field.get("name") or field.get("NAME") or field.get("COLUMN_NAME")
    if name is None or str(name).strip() == "":
        raise SchemaError(f"columns[{index}].name is required")
    type_name = field.get("type") or field.get("TYPE")
    if type_name is None or str(type_name).strip() == "":
        raise SchemaError(f"columns[{index}].type is required")
    # TODO: Support declared MAP(...) column types when callers need them.
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
        raise SchemaError(f"columns[{index}].alias must not be empty when set")
    alias = str(alias_value).strip() if alias_value is not None else None
    expression_value = field.get("expression")
    if expression_value is not None and str(expression_value).strip() == "":
        raise SchemaError(f"columns[{index}].expression must not be empty when set")
    expression = str(expression_value).strip() if expression_value is not None else None
    column = SnowflakeColumn(
        source_name=str(name),
        snowflake_type=snowflake_type,
        nullable=nullable,
        alias=alias,
        expression=expression,
    )
    return enrich_column_fields(column)


def _column_from_payload(raw: dict[str, Any], index: int) -> SnowflakeColumn:
    name = raw.get("source_name") or raw.get("name") or raw.get("NAME")
    if name is None or str(name).strip() == "":
        raise SchemaError(f"columns[{index}].source_name is required")
    type_name = raw.get("snowflake_type") or raw.get("type") or raw.get("TYPE")
    if type_name is None or str(type_name).strip() == "":
        raise SchemaError(f"columns[{index}].snowflake_type is required")
    nullable_value = raw.get("nullable")
    if nullable_value is None:
        nullable = True
    elif isinstance(nullable_value, bool):
        nullable = nullable_value
    else:
        nullable = str(nullable_value).strip().upper() in {"TRUE", "Y", "YES", "1"}
    nested_raw = raw.get("fields") or ()
    nested_fields = tuple(
        _column_from_payload(child, index)
        for child in nested_raw
        if isinstance(child, dict)
    )
    alias_value = raw.get("alias")
    alias = str(alias_value).strip() if alias_value is not None else None
    expression_value = raw.get("expression")
    expression = str(expression_value).strip() if expression_value is not None else None
    column = SnowflakeColumn(
        source_name=str(name),
        snowflake_type=str(type_name),
        nullable=nullable,
        fields=nested_fields,
        alias=alias or None,
        expression=expression or None,
    )
    return enrich_column_fields(column)


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
