from __future__ import annotations

import pytest

from procedure.errors import SchemaError
from procedure.schema import (
    SnowflakeColumn,
    enrich_column_fields,
    map_bigquery_schema,
    map_declared_columns,
    parse_structured_fields,
    plan_schema_evolution,
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
        "TIMESTAMP_NTZ(6)",
        "NUMBER(38,9)",
    ]
    assert columns[0].nullable is False


def test_maps_bigquery_timestamp_to_timestamp_ntz():
    """BQ extract Parquet uses isAdjustedToUTC=false; map to NTZ for ADD_FILES_COPY."""
    columns = map_bigquery_schema(
        [
            {"name": "usage_start_time", "type": "TIMESTAMP"},
            {"name": "usage_end_time", "type": "TIMESTAMP"},
            {"name": "export_time", "type": "TIMESTAMP"},
        ]
    )

    assert [column.snowflake_type for column in columns] == [
        "TIMESTAMP_NTZ(6)",
        "TIMESTAMP_NTZ(6)",
        "TIMESTAMP_NTZ(6)",
    ]
    assert columns[0].ddl == '"usage_start_time" TIMESTAMP_NTZ(6)'


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


@pytest.mark.parametrize("field_type", ["BIGNUMERIC", "BIGDECIMAL", "JSON", "GEOGRAPHY", "TIME"])
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


def test_view_columns_honor_declared_alias_and_expression():
    columns = [
        SnowflakeColumn(
            "AmountText",
            "VARCHAR",
            alias="amount",
            expression='TRY_TO_NUMBER("AmountText")',
        )
    ]

    view = view_columns(columns)[0]
    assert view.alias == "amount"
    assert view.expression == 'TRY_TO_NUMBER("AmountText")'


def test_view_columns_skip_expression_when_applied_at_load():
    columns = [
        SnowflakeColumn(
            "AmountText",
            "VARCHAR",
            alias="amount",
            expression='$1:"AmountText"::NUMBER',
        )
    ]

    view = view_columns(columns, expressions_applied_at_load=True)[0]
    assert view.alias == "amount"
    assert view.expression is None


def test_map_declared_columns():
    columns = map_declared_columns(
        [
            {
                "name": "OrderID",
                "type": "NUMBER(19,0)",
                "nullable": False,
            },
            {
                "name": "AmountText",
                "type": "TEXT",
                "alias": "amount",
                "expression": 'TRY_TO_NUMBER("AmountText")',
            },
        ]
    )

    assert columns[0].snowflake_type == "BIGINT"
    assert columns[0].nullable is False
    assert columns[1].snowflake_type == "VARCHAR"
    assert columns[1].alias == "amount"
    assert columns[1].expression == 'TRY_TO_NUMBER("AmountText")'


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


def test_schema_compatibility_rejects_legacy_timestamp_ltz_mapping():
    """Callers who previously loaded TIMESTAMP as LTZ must recreate the table."""
    existing = [SnowflakeColumn("usage_start_time", "TIMESTAMP_LTZ(6)")]
    desired = map_bigquery_schema([{"name": "usage_start_time", "type": "TIMESTAMP"}])

    assert desired[0].snowflake_type == "TIMESTAMP_NTZ(6)"
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


def test_schema_evolution_keeps_missing_nested_fields_with_warning():
    existing = [
        SnowflakeColumn(
            "payload",
            'OBJECT("a" VARCHAR, "b" VARCHAR)',
            fields=(
                SnowflakeColumn("a", "VARCHAR"),
                SnowflakeColumn("b", "VARCHAR"),
            ),
        )
    ]
    desired = [
        SnowflakeColumn(
            "payload",
            'OBJECT("a" VARCHAR)',
            fields=(SnowflakeColumn("a", "VARCHAR"),),
        )
    ]

    plan = plan_schema_evolution(existing, desired)
    assert plan.alter_columns == ()
    assert any("keeping nested field payload.b" in warning for warning in plan.warnings)


def test_schema_evolution_keep_missing_ignores_describe_case_and_varchar_length():
    """DESCRIBE often uppercases unquoted nested names and expands VARCHAR(n)."""

    existing = [
        enrich_column_fields(
            SnowflakeColumn(
                "PAYLOAD",
                "OBJECT(A VARCHAR(134217728), B VARCHAR(134217728))",
            )
        )
    ]
    desired = [
        SnowflakeColumn(
            "payload",
            'OBJECT("a" VARCHAR)',
            fields=(SnowflakeColumn("a", "VARCHAR"),),
        )
    ]

    plan = plan_schema_evolution(existing, desired)
    assert plan.alter_columns == ()
    assert any(
        "keeping nested field" in warning
        and warning.upper().endswith(".B ABSENT FROM SOURCE SCHEMA")
        for warning in plan.warnings
    )


def test_schema_evolution_allows_nested_field_reorder():
    existing = [
        SnowflakeColumn(
            "payload",
            'OBJECT("a" VARCHAR, "b" VARCHAR)',
            fields=(
                SnowflakeColumn("a", "VARCHAR"),
                SnowflakeColumn("b", "VARCHAR"),
            ),
        )
    ]
    desired = [
        SnowflakeColumn(
            "payload",
            'OBJECT("b" VARCHAR, "a" VARCHAR)',
            fields=(
                SnowflakeColumn("b", "VARCHAR"),
                SnowflakeColumn("a", "VARCHAR"),
            ),
        )
    ]

    plan = plan_schema_evolution(existing, desired)
    assert len(plan.alter_columns) == 1
    assert plan.alter_columns[0].snowflake_type == 'OBJECT("b" VARCHAR, "a" VARCHAR)'


def test_parse_structured_fields_from_describe_spelling():
    fields = parse_structured_fields(
        "OBJECT(ID VARCHAR(134217728), DESCRIPTION VARCHAR(134217728))"
    )
    assert [field.source_name for field in fields] == ["ID", "DESCRIPTION"]
    assert fields[0].snowflake_type == "VARCHAR(134217728)"


def test_schema_evolution_allows_nested_field_add_like_consumption_model():
    existing = [
        SnowflakeColumn(
            "consumption_model",
            "OBJECT(ID VARCHAR(134217728), DESCRIPTION VARCHAR(134217728))",
        )
    ]
    desired = [
        SnowflakeColumn(
            "consumption_model",
            'OBJECT("id" VARCHAR, "description" VARCHAR, '
            '"applied_subscription_instance_id" VARCHAR)',
            fields=(
                SnowflakeColumn("id", "VARCHAR"),
                SnowflakeColumn("description", "VARCHAR"),
                SnowflakeColumn("applied_subscription_instance_id", "VARCHAR"),
            ),
        )
    ]

    plan = plan_schema_evolution(existing, desired)
    assert len(plan.alter_columns) == 1
    altered = plan.alter_columns[0]
    assert '"applied_subscription_instance_id" VARCHAR' in altered.snowflake_type
    assert '"id" VARCHAR' in altered.snowflake_type


def test_schema_evolution_allows_nested_type_widen():
    existing = [
        SnowflakeColumn(
            "payload",
            'OBJECT("count" INTEGER)',
            fields=(SnowflakeColumn("count", "INTEGER"),),
        )
    ]
    desired = [
        SnowflakeColumn(
            "payload",
            'OBJECT("count" BIGINT)',
            fields=(SnowflakeColumn("count", "BIGINT"),),
        )
    ]

    plan = plan_schema_evolution(existing, desired)
    assert len(plan.alter_columns) == 1
    assert '"count" BIGINT' in plan.alter_columns[0].snowflake_type


def test_schema_evolution_keeps_old_and_adds_different_nested_name():
    existing = [
        SnowflakeColumn(
            "payload",
            'OBJECT("old_name" VARCHAR)',
            fields=(SnowflakeColumn("old_name", "VARCHAR"),),
        )
    ]
    desired = [
        SnowflakeColumn(
            "payload",
            'OBJECT("new_name" VARCHAR)',
            fields=(SnowflakeColumn("new_name", "VARCHAR"),),
        )
    ]

    plan = plan_schema_evolution(existing, desired)
    assert len(plan.alter_columns) == 1
    assert '"new_name" VARCHAR' in plan.alter_columns[0].snowflake_type
    assert '"old_name" VARCHAR' in plan.alter_columns[0].snowflake_type
    assert any("old_name" in warning for warning in plan.warnings)


def test_schema_evolution_allows_array_object_nested_add():
    existing = [
        SnowflakeColumn(
            "items",
            'ARRAY(OBJECT("sku" VARCHAR))',
        )
    ]
    desired = [
        SnowflakeColumn(
            "items",
            'ARRAY(OBJECT("sku" VARCHAR, "qty" BIGINT))',
            fields=(
                SnowflakeColumn("sku", "VARCHAR"),
                SnowflakeColumn("qty", "BIGINT"),
            ),
        )
    ]

    plan = plan_schema_evolution(existing, desired)
    assert len(plan.alter_columns) == 1
    assert plan.alter_columns[0].snowflake_type.startswith("ARRAY(OBJECT(")
    assert '"qty" BIGINT' in plan.alter_columns[0].snowflake_type


def test_map_parquet_infer_schema_orders_and_normalizes_types():
    from procedure.schema import map_parquet_infer_schema

    columns = map_parquet_infer_schema(
        [
            {"COLUMN_NAME": "CustomerName", "TYPE": "TEXT", "NULLABLE": True, "ORDER_ID": 2},
            {"COLUMN_NAME": "OrderID", "TYPE": "NUMBER(19,0)", "NULLABLE": False, "ORDER_ID": 1},
        ]
    )

    assert [column.source_name for column in columns] == ["OrderID", "CustomerName"]
    assert columns[0].snowflake_type == "BIGINT"
    assert columns[0].nullable is False
    assert columns[1].snowflake_type == "VARCHAR"


def test_map_parquet_infer_schema_rejects_unsupported_types():
    from procedure.schema import map_parquet_infer_schema

    with pytest.raises(SchemaError, match="not supported"):
        map_parquet_infer_schema(
            [{"COLUMN_NAME": "geo", "TYPE": "GEOGRAPHY", "NULLABLE": True, "ORDER_ID": 1}]
        )
