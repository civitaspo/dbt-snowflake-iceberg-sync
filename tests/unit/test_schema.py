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
            {"name": "OrderDateTime", "type": "DATETIME"},
            {"name": "CreatedAt", "type": "TIMESTAMP"},
            {"name": "Amount", "type": "NUMERIC"},
        ]
    )

    assert [column.snowflake_type for column in columns] == [
        "BIGINT",
        "VARCHAR",
        "TIMESTAMP_NTZ(6)",
        "TIMESTAMP_LTZ(6)",
        "NUMBER(38,9)",
    ]
    assert columns[0].nullable is False


def test_maps_required_bigquery_datetime_to_not_null_ddl():
    columns = map_bigquery_schema(
        [{"name": "some_datetime", "type": "DATETIME", "mode": "REQUIRED"}]
    )

    assert columns[0].snowflake_type == "TIMESTAMP_NTZ(6)"
    assert columns[0].ddl == '"some_datetime" TIMESTAMP_NTZ(6) NOT NULL'


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


def test_maps_repeated_record_to_structured_array():
    columns = map_bigquery_schema(
        [
            {
                "name": "Items",
                "type": "RECORD",
                "mode": "REPEATED",
                "fields": [
                    {"name": "Sku", "type": "STRING"},
                    {"name": "Quantity", "type": "INT64"},
                ],
            }
        ]
    )

    assert columns[0].snowflake_type.startswith("ARRAY(OBJECT(")
    assert '"Sku" VARCHAR' in columns[0].snowflake_type


@pytest.mark.parametrize(
    "field_type", ["BIGNUMERIC", "BIGDECIMAL", "JSON", "GEOGRAPHY", "TIME"]
)
def test_rejects_unsupported_type(field_type):
    with pytest.raises(SchemaError, match=field_type):
        map_bigquery_schema([{"name": "UnsupportedField", "type": field_type}])


@pytest.mark.parametrize(
    ("fields", "message"),
    [
        ([{"type": "STRING"}], "missing a name"),
        ([{"name": "Mystery", "type": "INTERVAL"}], "INTERVAL"),
        ([{"name": "EmptyRecord", "type": "RECORD", "fields": []}], "must contain"),
        ([{"name": "BytesArray", "type": "BYTES", "mode": "REPEATED"}], "repeated BYTES"),
    ],
)
def test_rejects_malformed_or_unsupported_fields(fields, message):
    with pytest.raises(SchemaError, match=message):
        map_bigquery_schema(fields)


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


def test_schema_compatibility_accepts_unchanged_datetime_mapping():
    validate_schema_compatibility(
        [SnowflakeColumn("OccurredDateTime", "TIMESTAMP_NTZ(6)")],
        [SnowflakeColumn("OccurredDateTime", "TIMESTAMP_NTZ(6)")],
    )


def test_schema_compatibility_normalizes_structured_type_field_names():
    validate_schema_compatibility(
        [
            SnowflakeColumn(
                "event_params",
                "ARRAY(OBJECT(KEY VARCHAR(134217728), "
                "VALUE OBJECT(STRING_VALUE VARCHAR(134217728), "
                "INT_VALUE NUMBER(19,0), FLOAT_VALUE FLOAT, DOUBLE_VALUE FLOAT)))",
            )
        ],
        [
            SnowflakeColumn(
                "event_params",
                'ARRAY(OBJECT("key" VARCHAR, '
                '"value" OBJECT("string_value" VARCHAR, "int_value" BIGINT, '
                '"float_value" DOUBLE, "double_value" DOUBLE)))',
            )
        ],
    )


def test_schema_compatibility_rejects_type_change():
    existing = [SnowflakeColumn("OrderID", "BIGINT")]
    desired = [SnowflakeColumn("OrderID", "VARCHAR")]

    with pytest.raises(SchemaError, match="incompatible type change"):
        validate_schema_compatibility(existing, desired)


def test_schema_compatibility_rejects_change_to_datetime_mapping():
    existing = [SnowflakeColumn("OccurredDateTime", "TIMESTAMP_LTZ(6)")]
    desired = map_bigquery_schema([{"name": "OccurredDateTime", "type": "DATETIME"}])

    with pytest.raises(SchemaError, match="incompatible type change"):
        validate_schema_compatibility(existing, desired)


def test_schema_compatibility_rejects_removed_columns():
    existing = [
        SnowflakeColumn("OrderID", "BIGINT"),
        SnowflakeColumn("CustomerName", "VARCHAR"),
    ]
    desired = [SnowflakeColumn("OrderID", "BIGINT")]

    with pytest.raises(SchemaError, match="removed"):
        validate_schema_compatibility(existing, desired)


def test_schema_compatibility_rejects_reordered_columns():
    existing = [
        SnowflakeColumn("OrderID", "BIGINT"),
        SnowflakeColumn("CustomerName", "VARCHAR"),
    ]
    desired = [
        SnowflakeColumn("CustomerName", "VARCHAR"),
        SnowflakeColumn("OrderID", "BIGINT"),
    ]

    with pytest.raises(SchemaError, match="reordered or renamed"):
        validate_schema_compatibility(existing, desired)


def test_schema_compatibility_rejects_nested_field_removal():
    existing = [
        SnowflakeColumn(
            "payload",
            "OBJECT",
            fields=(
                SnowflakeColumn("a", "VARCHAR"),
                SnowflakeColumn("b", "VARCHAR"),
            ),
        )
    ]
    desired = [
        SnowflakeColumn(
            "payload",
            "OBJECT",
            fields=(SnowflakeColumn("a", "VARCHAR"),),
        )
    ]

    with pytest.raises(SchemaError, match="nested fields were removed"):
        validate_schema_compatibility(existing, desired)


def test_schema_compatibility_rejects_nested_field_reorder():
    existing = [
        SnowflakeColumn(
            "payload",
            "OBJECT",
            fields=(
                SnowflakeColumn("a", "VARCHAR"),
                SnowflakeColumn("b", "VARCHAR"),
            ),
        )
    ]
    desired = [
        SnowflakeColumn(
            "payload",
            "OBJECT",
            fields=(
                SnowflakeColumn("b", "VARCHAR"),
                SnowflakeColumn("a", "VARCHAR"),
            ),
        )
    ]

    with pytest.raises(SchemaError, match="nested field order changed"):
        validate_schema_compatibility(existing, desired)
