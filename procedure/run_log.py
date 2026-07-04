from __future__ import annotations

import json
from typing import Any

from .config import IcebergSyncConfig
from .sql import quote_relation, string_literal
from .utils import utc_now_iso


def run_log_relation(config: IcebergSyncConfig) -> str | None:
    if not config.deployment.run_log_enabled:
        return None
    database = config.deployment.procedure_database
    schema = config.deployment.procedure_schema
    table = config.deployment.run_log_table
    if not database or not schema or not table:
        return None
    return quote_relation(database, schema, table)


def ensure_run_log_table(snowflake: Any, config: IcebergSyncConfig) -> None:
    relation = run_log_relation(config)
    if relation is None:
        return
    snowflake.execute(
        f"""
create table if not exists {relation} (
  run_id varchar,
  invocation_id varchar,
  model_unique_id varchar,
  target_view varchar,
  internal_iceberg_table varchar,
  source_type varchar,
  effective_mode varchar,
  predicate_json variant,
  export_segments variant,
  bigquery_job_references variant,
  staging_table variant,
  snowflake_query_ids variant,
  status varchar,
  error_message varchar,
  started_at timestamp_ltz,
  finished_at timestamp_ltz
)
""".strip()
    )


def write_run_log(
    snowflake: Any,
    config: IcebergSyncConfig,
    *,
    run_id: str,
    effective_mode: str | None,
    export_result: Any | None,
    snowflake_query_ids: list[str] | None,
    status: str,
    error_message: str | None,
    started_at: str,
) -> None:
    relation = run_log_relation(config)
    if relation is None:
        return

    export_payload = export_result.as_dict() if export_result is not None else {}
    target = config.target_relation
    internal = config.internal_relation
    values = {
        "predicate_json": {
            "type": export_payload.get("predicate_type"),
            "predicates": export_payload.get("predicates", []),
        },
        "export_segments": export_payload.get("segments", []),
        "bigquery_job_references": export_payload.get("job_references", []),
        "staging_table": export_payload.get("staging_table"),
        "snowflake_query_ids": snowflake_query_ids or [],
    }
    snowflake.execute(
        f"""
insert into {relation} (
  run_id,
  invocation_id,
  model_unique_id,
  target_view,
  internal_iceberg_table,
  source_type,
  effective_mode,
  predicate_json,
  export_segments,
  bigquery_job_references,
  staging_table,
  snowflake_query_ids,
  status,
  error_message,
  started_at,
  finished_at
)
select
  {string_literal(run_id)},
  {string_literal(config.invocation_id or '')},
  {string_literal(config.model_unique_id or '')},
  {string_literal(target.rendered or target.identifier)},
  {string_literal(internal.rendered or internal.identifier)},
  {string_literal(config.source_type)},
  {string_literal(effective_mode or '')},
  parse_json({string_literal(json.dumps(values["predicate_json"]))}),
  parse_json({string_literal(json.dumps(values["export_segments"]))}),
  parse_json({string_literal(json.dumps(values["bigquery_job_references"]))}),
  parse_json({string_literal(json.dumps(values["staging_table"]))}),
  parse_json({string_literal(json.dumps(values["snowflake_query_ids"]))}),
  {string_literal(status)},
  {string_literal(error_message or '')},
  {string_literal(started_at)}::timestamp_ltz,
  {string_literal(utc_now_iso())}::timestamp_ltz
""".strip()
    )
