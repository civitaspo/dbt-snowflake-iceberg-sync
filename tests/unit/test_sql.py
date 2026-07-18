from __future__ import annotations

from procedure.config import parse_config
from procedure.schema import SnowflakeColumn, ViewColumn
from procedure.sql import (
    alter_table_add_columns_sql,
    copy_into_sql,
    create_iceberg_table_sql,
    create_or_alter_run_log_table_sql,
    create_or_replace_view_sql,
    drop_iceberg_table_sql,
    quote_view_alias,
)


def test_copy_into_uses_required_iceberg_options(base_payload):
    config = parse_config(base_payload)

    sql = copy_into_sql(config.internal_relation, '@"ANALYTICS"."PUBLIC"."STAGE"/run')

    assert "LOAD_MODE = ADD_FILES_COPY" in sql
    assert "MATCH_BY_COLUMN_NAME = CASE_SENSITIVE" in sql
    assert "PURGE = FALSE" in sql


def test_copy_into_full_ingest_without_transforms(base_payload):
    config = parse_config(base_payload)

    sql = copy_into_sql(
        config.internal_relation,
        '@"ANALYTICS"."PUBLIC"."STAGE"/run',
        load_mode="full_ingest",
        force=True,
    )

    assert "LOAD_MODE = FULL_INGEST" in sql
    assert "MATCH_BY_COLUMN_NAME = CASE_SENSITIVE" in sql
    assert "FORCE = TRUE" in sql
    assert "FROM (" not in sql


def test_copy_into_full_ingest_applies_column_expressions(base_payload):
    config = parse_config(base_payload)
    columns = [
        SnowflakeColumn("OrderID", "BIGINT", nullable=False),
        SnowflakeColumn(
            "UsageStart",
            "TIMESTAMP_LTZ(6)",
            expression='$1:"UsageStart"',
        ),
    ]

    sql = copy_into_sql(
        config.internal_relation,
        '@"ANALYTICS"."PUBLIC"."STAGE"/cur',
        files=["part.parquet"],
        force=True,
        load_mode="full_ingest",
        transform_columns=columns,
    )

    assert 'COPY INTO "ANALYTICS"."PUBLIC"."__ORDERS" ("OrderID", "UsageStart")' in sql
    assert "LOAD_MODE = FULL_INGEST" in sql
    assert '$1:"OrderID"' in sql
    assert '$1:"UsageStart"' in sql
    assert "MATCH_BY_COLUMN_NAME" not in sql
    assert "FILES = ('part.parquet')" in sql
    assert "FORCE = TRUE" in sql


def test_create_iceberg_table_renders_managed_iceberg_options(base_payload):
    config = parse_config(base_payload)

    sql = create_iceberg_table_sql(
        config,
        [SnowflakeColumn("OrderID", "BIGINT", nullable=False)],
    )

    assert 'CREATE ICEBERG TABLE IF NOT EXISTS "ANALYTICS"."PUBLIC"."__ORDERS"' in sql
    assert "EXTERNAL_VOLUME = 'ICEBERG_EXTERNAL_VOLUME'" in sql
    assert "CATALOG = 'SNOWFLAKE'" in sql
    assert "STORAGE_SERIALIZATION_POLICY = COMPATIBLE" in sql
    assert "ERROR_LOGGING = FALSE" in sql
    assert "ENABLE_DATA_COMPACTION = TRUE" in sql
    assert '"OrderID" BIGINT NOT NULL' in sql


def test_create_iceberg_table_renders_datetime_column(base_payload):
    config = parse_config(base_payload)

    sql = create_iceberg_table_sql(
        config,
        [SnowflakeColumn("some_datetime", "TIMESTAMP_NTZ(6)")],
    )

    assert '"some_datetime" TIMESTAMP_NTZ(6)' in sql


def test_add_columns_uses_alter_iceberg_table(base_payload):
    config = parse_config(base_payload)

    statements = alter_table_add_columns_sql(
        config.internal_relation,
        [SnowflakeColumn("CustomerName", "VARCHAR")],
    )

    assert statements == [
        'ALTER ICEBERG TABLE "ANALYTICS"."PUBLIC"."__ORDERS" ADD COLUMN "CustomerName" VARCHAR'
    ]
    assert not statements[0].startswith("ALTER TABLE ")


def test_create_view_preserves_source_case_and_aliases_lower_snake(base_payload):
    config = parse_config(base_payload)

    sql = create_or_replace_view_sql(
        config.target_relation,
        config.internal_relation,
        [ViewColumn("OrderID", "order_id")],
    )

    assert 'SELECT\n  "OrderID" AS "ORDER_ID"' in sql
    assert 'FROM "ANALYTICS"."PUBLIC"."__ORDERS"' in sql


def test_create_view_uses_custom_expression_when_provided(base_payload):
    config = parse_config(base_payload)

    sql = create_or_replace_view_sql(
        config.target_relation,
        config.internal_relation,
        [ViewColumn("AmountText", "amount", expression='TRY_TO_NUMBER("AmountText")')],
    )

    assert 'SELECT\n  TRY_TO_NUMBER("AmountText") AS "AMOUNT"' in sql

def test_drop_iceberg_table_uses_if_exists(base_payload):
    config = parse_config(base_payload)

    sql = drop_iceberg_table_sql(config.internal_relation)

    assert sql == 'DROP ICEBERG TABLE IF EXISTS "ANALYTICS"."PUBLIC"."__ORDERS"'


def test_run_log_sql_uses_create_or_alter_and_includes_all_columns(base_payload):
    config = parse_config(base_payload)
    run_log = config.deployment.run_log_table
    assert run_log is not None

    create_sql = create_or_alter_run_log_table_sql(run_log)

    assert 'CREATE OR ALTER TABLE "ANALYTICS"."UTIL"."ICEBERG_SYNC_RUN_LOG"' in create_sql
    assert "CREATE TABLE IF NOT EXISTS" not in create_sql
    assert "ADD COLUMN IF NOT EXISTS" not in create_sql
    for expected_column in [
        "run_id VARCHAR",
        "invocation_id VARCHAR",
        "model_unique_id VARCHAR",
        "target_view VARCHAR",
        "internal_iceberg_table VARCHAR",
        "source_type VARCHAR",
        "effective_mode VARCHAR",
        "predicate_json VARIANT",
        "export_segments VARIANT",
        "source_job_references VARIANT",
        "staging_table_reference VARCHAR",
        "snowflake_query_ids VARIANT",
        "retry VARIANT",
        "cleanup VARIANT",
        "status VARCHAR",
        "error_message VARCHAR",
        "started_at TIMESTAMP_LTZ",
        "finished_at TIMESTAMP_LTZ",
    ]:
        assert expected_column in create_sql


def test_quote_view_alias_preserves_snowflake_unquoted_folding():
    assert quote_view_alias("select") == '"SELECT"'
