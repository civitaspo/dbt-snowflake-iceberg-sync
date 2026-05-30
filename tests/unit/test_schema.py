from __future__ import annotations

import pytest

from procedure.errors import SchemaError
from procedure.schema import (
    SnowflakeColumn,
    map_bigquery_schema,
    validate_schema_compatibility,
    view_columns,
)


def test_maps_supported_bigquery_scalars():
    columns = map_bigquery_schema(
        [
            {"name": "OrderID", "type": "INT64", "mode": "REQUIRED"},
            {"name": "CustomerName", "type": "STRING"},
            {"name": "CreatedAt", "type": "TIMESTAMP"},
            {"name": "Amount", "type": "NUMERIC"},
        ]
    )

    assert [column.snowflake_type for column in columns] == [
        "BIGINT",
        "VARCHAR",
        "TIMESTAMP_LTZ(6)",
        "NUMBER(38,9)",
    ]
    assert columns[0].nullable is False


def test_maps_nested_record_to_structured_object():
    columns = map_bigquery_schema(
        [
            {
                "name": "ShippingAddress",
                "type": "RECORD",
                "fields": [
                    {"name": "City", "type": "STRING"},
                    {"name": "PostalCode", "type": "STRING"},
                ],
            }
        ]
    )

    assert columns[0].snowflake_type.startswith("OBJECT(")
    assert '"City" VARCHAR' in columns[0].snowflake_type


def test_rejects_unsupported_type():
    with pytest.raises(SchemaError, match="BIGNUMERIC"):
        map_bigquery_schema([{"name": "TooWide", "type": "BIGNUMERIC"}])


def test_detects_view_alias_collisions():
    with pytest.raises(SchemaError, match="view alias collisions"):
        map_bigquery_schema(
            [
                {"name": "OrderID", "type": "STRING"},
                {"name": "order_id", "type": "STRING"},
            ]
        )


def test_view_columns_are_lower_snake():
    columns = [SnowflakeColumn("HTTPStatusCode", "BIGINT")]

    assert view_columns(columns)[0].alias == "http_status_code"


def test_schema_compatibility_allows_additive_columns():
    existing = [SnowflakeColumn("OrderID", "BIGINT")]
    desired = [
        SnowflakeColumn("OrderID", "BIGINT"),
        SnowflakeColumn("CustomerName", "VARCHAR"),
    ]

    validate_schema_compatibility(existing, desired)


def test_schema_compatibility_treats_default_varchar_width_as_equivalent():
    validate_schema_compatibility(
        [SnowflakeColumn("CustomerName", "VARCHAR(134217728)")],
        [SnowflakeColumn("CustomerName", "VARCHAR")],
    )


def test_schema_compatibility_treats_bigint_describe_type_as_equivalent():
    validate_schema_compatibility(
        [SnowflakeColumn("EventTimestamp", "NUMBER(19,0)")],
        [SnowflakeColumn("EventTimestamp", "BIGINT")],
    )


def test_schema_compatibility_treats_double_describe_type_as_equivalent():
    validate_schema_compatibility(
        [SnowflakeColumn("DoubleValue", "FLOAT")],
        [SnowflakeColumn("DoubleValue", "DOUBLE")],
    )


def test_schema_compatibility_normalizes_structured_type_field_names():
    validate_schema_compatibility(
        [
            SnowflakeColumn(
                "event_params",
                "ARRAY(OBJECT(KEY VARCHAR(134217728), VALUE OBJECT(DOUBLE_VALUE DOUBLE)))",
            )
        ],
        [
            SnowflakeColumn(
                "event_params",
                'ARRAY(OBJECT("key" VARCHAR, "value" OBJECT("double_value" DOUBLE)))',
            )
        ],
    )


def test_schema_compatibility_rejects_type_change():
    existing = [SnowflakeColumn("OrderID", "BIGINT")]
    desired = [SnowflakeColumn("OrderID", "VARCHAR")]

    with pytest.raises(SchemaError, match="incompatible type change"):
        validate_schema_compatibility(existing, desired)
