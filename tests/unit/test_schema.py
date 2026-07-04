import pytest

from procedure.errors import SchemaError
from procedure.schema import (
    SnowflakeColumn,
    columns_from_bigquery_schema,
    validate_schema_compatibility,
)


def test_maps_supported_scalar_types_and_aliases():
    columns = columns_from_bigquery_schema(
        {
            "fields": [
                {"name": "CustomerID", "type": "INT64"},
                {"name": "eventDate", "type": "DATE"},
                {"name": "payload_bytes", "type": "BYTES"},
            ]
        }
    )

    assert [column.alias for column in columns] == ["customer_id", "event_date", "payload_bytes"]
    assert [column.snowflake_type for column in columns] == ["NUMBER(38,0)", "DATE", "BINARY"]


def test_maps_nested_record_to_structured_object():
    columns = columns_from_bigquery_schema(
        {
            "fields": [
                {
                    "name": "Address",
                    "type": "RECORD",
                    "fields": [
                        {"name": "City", "type": "STRING"},
                        {"name": "Zip", "type": "INTEGER"},
                    ],
                }
            ]
        }
    )

    assert columns[0].snowflake_type == 'OBJECT("City" VARCHAR, "Zip" NUMBER(38,0))'


def test_alias_collision_is_rejected():
    with pytest.raises(SchemaError, match="alias collision"):
        columns_from_bigquery_schema(
            {
                "fields": [
                    {"name": "CustomerID", "type": "STRING"},
                    {"name": "customer_id", "type": "STRING"},
                ]
            }
        )


def test_unsupported_type_is_rejected():
    with pytest.raises(SchemaError, match="BIGNUMERIC"):
        columns_from_bigquery_schema({"fields": [{"name": "amount", "type": "BIGNUMERIC"}]})


def test_schema_compatibility_allows_additive_columns():
    additive = validate_schema_compatibility(
        {"id": "NUMBER(38,0)"},
        [
            SnowflakeColumn("id", "NUMBER(38,0)", "id"),
            SnowflakeColumn("name", "VARCHAR", "name"),
        ],
    )

    assert [column.storage_name for column in additive] == ["name"]


def test_schema_compatibility_rejects_removed_or_changed_columns():
    with pytest.raises(SchemaError, match="removed"):
        validate_schema_compatibility(
            {"id": "NUMBER(38,0)", "deleted": "VARCHAR"},
            [SnowflakeColumn("id", "NUMBER(38,0)", "id")],
        )

    with pytest.raises(SchemaError, match="changed"):
        validate_schema_compatibility(
            {"id": "VARCHAR"},
            [SnowflakeColumn("id", "NUMBER(38,0)", "id")],
        )
