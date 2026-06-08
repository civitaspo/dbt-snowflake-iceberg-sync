from __future__ import annotations

from procedure.config import parse_config
from procedure.schema import SnowflakeColumn, ViewColumn
from procedure.sql import (
    alter_run_log_table_sql,
    copy_into_sql,
    create_iceberg_table_sql,
    create_or_replace_view_sql,
    create_run_log_table_sql,
    drop_iceberg_table_sql,
    quote_view_alias,
)


def test_copy_into_uses_required_iceberg_options(base_payload):
    config = parse_config(base_payload)

    sql = copy_into_sql(config.internal_relation, '@"ANALYTICS"."PUBLIC"."STAGE"/run')

    assert "LOAD_MODE = ADD_FILES_COPY" in sql
    assert "MATCH_BY_COLUMN_NAME = CASE_SENSITIVE" in sql
    assert "PURGE = FALSE" in sql


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


def test_create_view_preserves_source_case_and_aliases_lower_snake(base_payload):
    config = parse_config(base_payload)

    sql = create_or_replace_view_sql(
        config.target_relation,
        config.internal_relation,
        [ViewColumn("OrderID", "order_id")],
    )

    assert 'SELECT\n  "OrderID" AS "ORDER_ID"' in sql
    assert 'FROM "ANALYTICS"."PUBLIC"."__ORDERS"' in sql


def test_drop_iceberg_table_uses_if_exists(base_payload):
    config = parse_config(base_payload)

    sql = drop_iceberg_table_sql(config.internal_relation)

    assert sql == 'DROP ICEBERG TABLE IF EXISTS "ANALYTICS"."PUBLIC"."__ORDERS"'


def test_run_log_sql_includes_retry_and_cleanup_columns(base_payload):
    config = parse_config(base_payload)
    run_log = config.deployment.run_log_table
    assert run_log is not None

    create_sql = create_run_log_table_sql(run_log)
    alter_sql = alter_run_log_table_sql(run_log)

    assert "retry VARIANT" in create_sql
    assert "cleanup VARIANT" in create_sql
    assert any("ADD COLUMN IF NOT EXISTS retry VARIANT" in sql for sql in alter_sql)
    assert any("ADD COLUMN IF NOT EXISTS cleanup VARIANT" in sql for sql in alter_sql)


def test_quote_view_alias_preserves_snowflake_unquoted_folding():
    assert quote_view_alias("select") == '"SELECT"'
