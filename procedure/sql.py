"""Snowflake SQL rendering for the procedure and dbt materialization."""

from __future__ import annotations

import json
from typing import Any

from .config import IcebergSyncConfig, RelationConfig
from .schema import SnowflakeColumn, ViewColumn
from .utils import bool_sql, quote_fqn, quote_identifier, sql_string


def relation_sql(relation: RelationConfig) -> str:
    return quote_fqn(relation.database, relation.schema, relation.identifier)


def create_iceberg_table_sql(config: IcebergSyncConfig, columns: list[SnowflakeColumn]) -> str:
    column_sql = ",\n  ".join(column.ddl for column in columns)
    table = config.iceberg_table
    base_location = table.base_location or (
        f"{config.target_relation.database}/{config.target_relation.schema}/"
        f"{config.target_relation.identifier}"
    )
    parts = [
        f"CREATE ICEBERG TABLE IF NOT EXISTS {relation_sql(config.internal_relation)} (",
        f"  {column_sql}",
        ")",
        f"EXTERNAL_VOLUME = {sql_string(table.external_volume)}",
        "CATALOG = 'SNOWFLAKE'",
        f"BASE_LOCATION = {sql_string(base_location)}",
        f"TARGET_FILE_SIZE = {sql_string(table.target_file_size)}",
        f"STORAGE_SERIALIZATION_POLICY = {table.storage_serialization_policy}",
        f"DATA_RETENTION_TIME_IN_DAYS = {table.data_retention_time_in_days}",
        f"CHANGE_TRACKING = {bool_sql(table.change_tracking)}",
        f"ERROR_LOGGING = {bool_sql(table.error_logging)}",
        f"ICEBERG_VERSION = {table.iceberg_version}",
        f"ENABLE_ICEBERG_MERGE_ON_READ = {bool_sql(table.enable_iceberg_merge_on_read)}",
        f"ENABLE_DATA_COMPACTION = {bool_sql(table.enable_data_compaction)}",
    ]
    if table.max_data_extension_time_in_days is not None:
        parts.append(f"MAX_DATA_EXTENSION_TIME_IN_DAYS = {table.max_data_extension_time_in_days}")
    if table.copy_grants:
        parts.append("COPY GRANTS")
    return "\n".join(parts)


def alter_table_add_columns_sql(
    relation: RelationConfig, columns: list[SnowflakeColumn]
) -> list[str]:
    return [
        f"ALTER ICEBERG TABLE {relation_sql(relation)} ADD COLUMN {column.ddl}"
        for column in columns
    ]


def delete_sql(relation: RelationConfig, predicate: str | None) -> str:
    if predicate:
        return f"DELETE FROM {relation_sql(relation)} WHERE {predicate}"
    return f"DELETE FROM {relation_sql(relation)}"


def copy_into_sql(
    relation: RelationConfig,
    stage_run_location: str,
    *,
    pattern: str | None = None,
    force: bool = False,
) -> str:
    lines = [
        f"COPY INTO {relation_sql(relation)}",
        f"FROM {stage_run_location.rstrip('/')}/",
        "FILE_FORMAT = (TYPE = PARQUET USE_VECTORIZED_SCANNER = TRUE)",
        "LOAD_MODE = ADD_FILES_COPY",
        "MATCH_BY_COLUMN_NAME = CASE_SENSITIVE",
        "PURGE = FALSE",
    ]
    if pattern:
        lines.append(f"PATTERN = {sql_string(pattern)}")
    if force:
        lines.append("FORCE = TRUE")
    return "\n".join(lines)


def list_files_sql(stage_location: str) -> str:
    return f"LIST {stage_location.rstrip('/')}"


def infer_schema_sql(
    *,
    location: str,
    file_format: str,
    files: list[str] | None = None,
    kind: str = "ICEBERG",
) -> str:
    parts = [
        f"LOCATION => {sql_string(location)}",
        f"FILE_FORMAT => {sql_string(file_format)}",
        f"KIND => {sql_string(kind)}",
    ]
    if files:
        file_list = ", ".join(sql_string(name) for name in files)
        parts.append(f"FILES => ({file_list})")
    joined = ",\n    ".join(parts)
    return f"SELECT *\nFROM TABLE(\n  INFER_SCHEMA(\n    {joined}\n  )\n)\nORDER BY ORDER_ID"


def create_parquet_file_format_sql(file_format_fqn: str) -> str:
    return (
        f"CREATE FILE FORMAT IF NOT EXISTS {file_format_fqn}\n"
        "TYPE = PARQUET\n"
        "USE_VECTORIZED_SCANNER = TRUE"
    )


def create_or_replace_view_sql(
    target: RelationConfig, internal: RelationConfig, columns: list[ViewColumn]
) -> str:
    select_list = ",\n  ".join(
        f"{quote_identifier(column.source_name)} AS {quote_view_alias(column.alias)}"
        for column in columns
    )
    return f"""CREATE OR REPLACE VIEW {relation_sql(target)} AS
SELECT
  {select_list}
FROM {relation_sql(internal)}"""


def drop_iceberg_table_sql(relation: RelationConfig) -> str:
    return f"DROP ICEBERG TABLE IF EXISTS {relation_sql(relation)}"


def quote_view_alias(alias: str) -> str:
    return quote_identifier(alias.upper())


def desc_stage_sql(stage_fqn: str) -> str:
    return f"DESC STAGE {stage_fqn}"


def create_or_alter_run_log_table_sql(relation: RelationConfig) -> str:
    return f"""CREATE OR ALTER TABLE {relation_sql(relation)} (
  run_id VARCHAR,
  invocation_id VARCHAR,
  model_unique_id VARCHAR,
  target_view VARCHAR,
  internal_iceberg_table VARCHAR,
  source_type VARCHAR,
  effective_mode VARCHAR,
  predicate_json VARIANT,
  export_segments VARIANT,
  source_job_references VARIANT,
  staging_table_reference VARCHAR,
  snowflake_query_ids VARIANT,
  retry VARIANT,
  cleanup VARIANT,
  status VARCHAR,
  error_message VARCHAR,
  started_at TIMESTAMP_LTZ,
  finished_at TIMESTAMP_LTZ
)"""


def insert_run_log_sql(relation: RelationConfig, payload: dict[str, Any]) -> str:
    def value(key: str) -> str:
        item = payload.get(key)
        if item is None:
            return "NULL"
        return sql_string(str(item))

    def json_value(key: str) -> str:
        item = payload.get(key)
        if item is None:
            item = None
        return f"PARSE_JSON({sql_string(json.dumps(item, sort_keys=True, default=str))})"

    return f"""INSERT INTO {relation_sql(relation)} (
  run_id,
  invocation_id,
  model_unique_id,
  target_view,
  internal_iceberg_table,
  source_type,
  effective_mode,
  predicate_json,
  export_segments,
  source_job_references,
  staging_table_reference,
  snowflake_query_ids,
  retry,
  cleanup,
  status,
  error_message,
  started_at,
  finished_at
)
SELECT
  {value("run_id")},
  {value("invocation_id")},
  {value("model_unique_id")},
  {value("target_view")},
  {value("internal_iceberg_table")},
  {value("source_type")},
  {value("effective_mode")},
  {json_value("predicate_json")},
  {json_value("export_segments")},
  {json_value("source_job_references")},
  {value("staging_table_reference")},
  {json_value("snowflake_query_ids")},
  {json_value("retry")},
  {json_value("cleanup")},
  {value("status")},
  {value("error_message")},
  TO_TIMESTAMP_LTZ({value("started_at")}),
  TO_TIMESTAMP_LTZ({value("finished_at")})"""
