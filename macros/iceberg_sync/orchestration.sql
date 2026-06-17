{% macro iceberg_sync_bool_sql(value) -%}
  {{ return('TRUE' if value else 'FALSE') }}
{%- endmacro %}

{% macro iceberg_sync_run_id(payload) -%}
  {%- set raw = (payload['model']['invocation_id'] or invocation_id) ~ '_' ~ payload['model']['name'] -%}
  {{ return(
    raw
    | replace('-', '_')
    | replace('.', '_')
    | replace('/', '_')
    | replace(' ', '_')
    | lower
  ) }}
{%- endmacro %}

{% macro iceberg_sync_effective_mode(payload, internal_table_exists, target_view_exists) -%}
  {%- if payload['dbt_full_refresh'] or payload['materialization_strategy'] == 'full_refresh' -%}
    {{ return('full_refresh') }}
  {%- elif not internal_table_exists -%}
    {{ return('full_refresh') }}
  {%- elif not target_view_exists -%}
    {{ return('full_refresh') }}
  {%- else -%}
    {{ return('incremental') }}
  {%- endif -%}
{%- endmacro %}

{% macro iceberg_sync_predicates_for_mode(payload, effective_mode) -%}
  {%- if effective_mode == 'full_refresh' -%}
    {{ return(payload['bigquery']['full_refresh_predicates']) }}
  {%- endif -%}
  {{ return(payload['bigquery']['incremental_predicates']) }}
{%- endmacro %}

{% macro iceberg_sync_parse_stage_location(export_location) -%}
  {%- if not export_location.startswith('@') -%}
    {%- do dbt_snowflake_iceberg_sync.iceberg_sync_raise(
      'bigquery_export_location must start with @'
    ) -%}
  {%- endif -%}
  {%- set raw = export_location[1:] -%}
  {%- if raw == '' or raw.startswith('~') or raw.startswith('%') -%}
    {%- do dbt_snowflake_iceberg_sync.iceberg_sync_raise(
      'bigquery_export_location must be a named Snowflake stage, not a user or table stage'
    ) -%}
  {%- endif -%}
  {%- if '/' in raw -%}
    {%- set stage_raw = raw.split('/', 1)[0] -%}
    {%- set stage_path = raw.split('/', 1)[1].strip('/') -%}
  {%- else -%}
    {%- set stage_raw = raw -%}
    {%- set stage_path = '' -%}
  {%- endif -%}
  {{ return({
    'stage_fqn': dbt_snowflake_iceberg_sync.iceberg_sync_object_fqn(
      stage_raw, 'bigquery_export_location stage', 1, 3
    ),
    'stage_path': stage_path
  }) }}
{%- endmacro %}

{% macro iceberg_sync_resolve_stage_location(export_location, run_id) -%}
  {%- set parsed = dbt_snowflake_iceberg_sync.iceberg_sync_parse_stage_location(export_location) -%}
  {%- set stage_fqn = parsed['stage_fqn'] -%}
  {%- set stage_path = parsed['stage_path'] -%}
  {%- set stage_table = run_query('DESC STAGE ' ~ stage_fqn) -%}
  {%- set ns = namespace(url=none) -%}
  {%- for row in stage_table.rows -%}
    {%- set key = row['property'] or row['PROPERTY'] or row[1] -%}
    {%- if (key | string | upper) == 'URL' -%}
      {%- set ns.url = row['property_value'] or row['PROPERTY_VALUE'] or row[3] -%}
    {%- endif -%}
  {%- endfor -%}
  {%- if ns.url is none or ns.url == '' -%}
    {%- do dbt_snowflake_iceberg_sync.iceberg_sync_raise(
      'DESC STAGE did not return a URL property'
    ) -%}
  {%- endif -%}
  {%- set url_text = (ns.url | string).strip().rstrip('/') -%}
  {%- if url_text.startswith('[') -%}
    {%- set urls = fromjson(url_text) -%}
    {%- if urls | length > 0 -%}
      {%- set url_text = (urls[0] | string).strip().rstrip('/') -%}
    {%- endif -%}
  {%- endif -%}
  {%- if not url_text.startswith('gcs://') -%}
    {%- do dbt_snowflake_iceberg_sync.iceberg_sync_raise(
      'bigquery_export_location must reference a Snowflake stage backed by GCS'
    ) -%}
  {%- endif -%}
  {%- set run_path = (stage_path ~ '/' ~ run_id).strip('/') -%}
  {%- set run_stage_location = '@' ~ stage_fqn ~ '/' ~ run_path -%}
  {%- set gcs_base = 'gs://' ~ url_text.removeprefix('gcs://') -%}
  {{ return({
    'stage_fqn': stage_fqn,
    'stage_path': stage_path,
    'run_stage_location': run_stage_location,
    'gcs_run_uri': (gcs_base.rstrip('/') ~ '/' ~ run_path).strip()
  }) }}
{%- endmacro %}

{% macro iceberg_sync_call_export_action(action_payload, statement_name) -%}
  {%- set procedure_fqn = dbt_snowflake_iceberg_sync.iceberg_sync_procedure_fqn() -%}
  {%- call statement(statement_name, fetch_result=True, auto_begin=False) -%}
    CALL {{ procedure_fqn }}(
      PARSE_JSON({{ dbt_snowflake_iceberg_sync.iceberg_sync_json_sql_literal(action_payload) }})
    )
  {%- endcall -%}
  {%- set result_table = load_result(statement_name)['table'] -%}
  {%- set result_values = [] -%}
  {%- if result_table is not none and (result_table.columns | length) > 0 -%}
    {%- set result_values = result_table.columns[0].values() -%}
  {%- endif -%}
  {%- if (result_values | length) == 0 or result_values[0] is none or result_values[0] == '' -%}
    {%- do dbt_snowflake_iceberg_sync.iceberg_sync_raise(
      'procedure returned no result'
    ) -%}
  {%- endif -%}
  {{ return(dbt_snowflake_iceberg_sync.iceberg_sync_parse_procedure_result(result_values[0])) }}
{%- endmacro %}

{% macro iceberg_sync_wait_for_export(payload, effective_mode, destination_uri) -%}
  {%- set start_payload = {
    'action': 'start_export',
    'config': payload,
    'effective_mode': effective_mode,
    'destination_uri': destination_uri
  } -%}
  {%- set ns = namespace(result=dbt_snowflake_iceberg_sync.iceberg_sync_call_export_action(
    start_payload, 'iceberg_sync_start_export'
  )) -%}
  {%- set interval_seconds = payload['bigquery']['export_poll_interval_seconds'] -%}
  {%- set timeout_seconds = payload['bigquery']['export_poll_timeout_seconds'] -%}
  {%- set max_polls = ((timeout_seconds / interval_seconds) | int) + 1 -%}
  {%- for attempt in range(max_polls) -%}
    {%- if ns.result.get('status') == 'success' -%}
      {{ return(ns.result['export_result']) }}
    {%- elif ns.result.get('status') != 'running' -%}
      {%- do dbt_snowflake_iceberg_sync.iceberg_sync_raise(
        ns.result.get('error_message', 'BigQuery export failed')
      ) -%}
    {%- endif -%}
    {%- if attempt + 1 < max_polls -%}
      {%- set wait_milliseconds = ((interval_seconds * 1000) | int) -%}
      {%- if wait_milliseconds > 0 -%}
        {%- call statement('iceberg_sync_wait_for_bigquery_' ~ attempt, auto_begin=False) -%}
          CALL SYSTEM$WAIT({{ wait_milliseconds }}, 'MILLISECONDS')
        {%- endcall -%}
      {%- endif -%}
      {%- set ns.result = dbt_snowflake_iceberg_sync.iceberg_sync_call_export_action({
        'action': 'poll_export',
        'config': payload,
        'export_state': ns.result['export_state']
      }, 'iceberg_sync_poll_export_' ~ attempt) -%}
    {%- endif -%}
  {%- endfor -%}
  {%- do dbt_snowflake_iceberg_sync.iceberg_sync_raise(
    'BigQuery export did not finish before bigquery_export_poll_timeout_seconds'
  ) -%}
{%- endmacro %}

{% macro iceberg_sync_create_iceberg_table_sql(payload, columns) -%}
  {%- set relation = dbt_snowflake_iceberg_sync.iceberg_sync_relation_from_payload(
    payload['internal_relation']
  ) -%}
  {%- set table = payload['iceberg_table'] -%}
  {%- set base_location = table['base_location'] -%}
  {%- if base_location is none or base_location == '' -%}
    {%- set target = payload['target_relation'] -%}
    {%- set base_location = target['database'] ~ '/' ~ target['schema'] ~ '/' ~ target['identifier'] -%}
  {%- endif -%}
CREATE ICEBERG TABLE IF NOT EXISTS {{ relation }} (
  {%- for column in columns %}
  {{ column['ddl'] }}{{ "," if not loop.last }}
  {%- endfor %}
)
EXTERNAL_VOLUME = {{ dbt_snowflake_iceberg_sync.iceberg_sync_sql_string_literal(table['external_volume']) }}
CATALOG = 'SNOWFLAKE'
BASE_LOCATION = {{ dbt_snowflake_iceberg_sync.iceberg_sync_sql_string_literal(base_location) }}
TARGET_FILE_SIZE = {{ dbt_snowflake_iceberg_sync.iceberg_sync_sql_string_literal(table['target_file_size']) }}
STORAGE_SERIALIZATION_POLICY = {{ table['storage_serialization_policy'] }}
DATA_RETENTION_TIME_IN_DAYS = {{ table['data_retention_time_in_days'] }}
CHANGE_TRACKING = {{ dbt_snowflake_iceberg_sync.iceberg_sync_bool_sql(table['change_tracking']) }}
ERROR_LOGGING = {{ dbt_snowflake_iceberg_sync.iceberg_sync_bool_sql(table['error_logging']) }}
ICEBERG_VERSION = {{ table['iceberg_version'] }}
ENABLE_ICEBERG_MERGE_ON_READ = {{ dbt_snowflake_iceberg_sync.iceberg_sync_bool_sql(table['enable_iceberg_merge_on_read']) }}
ENABLE_DATA_COMPACTION = {{ dbt_snowflake_iceberg_sync.iceberg_sync_bool_sql(table['enable_data_compaction']) }}
{%- if table['max_data_extension_time_in_days'] is not none %}
MAX_DATA_EXTENSION_TIME_IN_DAYS = {{ table['max_data_extension_time_in_days'] }}
{%- endif %}
{%- if table['copy_grants'] %}
COPY GRANTS
{%- endif %}
{%- endmacro %}

{% macro iceberg_sync_describe_table_columns(relation) -%}
  {%- set describe_table = run_query('DESCRIBE TABLE ' ~ relation) -%}
  {%- set columns = [] -%}
  {%- for row in describe_table.rows -%}
    {%- set name = row['name'] or row['NAME'] or row[0] -%}
    {%- set type_name = row['type'] or row['TYPE'] or row[1] -%}
    {%- set null_value = row['null?'] or row['NULL?'] or row[3] -%}
    {%- if name and type_name -%}
      {%- do columns.append({
        'source_name': name | string,
        'snowflake_type': (type_name | string | upper),
        'nullable': (null_value | string | upper) != 'N'
      }) -%}
    {%- endif -%}
  {%- endfor -%}
  {{ return(columns) }}
{%- endmacro %}

{% macro iceberg_sync_normalized_snowflake_type(value) -%}
  {%- set result = value | string | upper | replace('"', '') -%}
  {%- set result = result | replace('TEXT', 'VARCHAR') | replace('STRING', 'VARCHAR') -%}
  {%- if result.startswith('VARCHAR(') -%}
    {{ return('VARCHAR') }}
  {%- elif result == 'NUMBER(19,0)' -%}
    {{ return('BIGINT') }}
  {%- elif result == 'FLOAT' -%}
    {{ return('DOUBLE') }}
  {%- endif -%}
  {{ return(result) }}
{%- endmacro %}

{% macro iceberg_sync_validate_or_add_columns(internal_relation, desired_columns) -%}
  {%- set existing_columns = dbt_snowflake_iceberg_sync.iceberg_sync_describe_table_columns(
    internal_relation
  ) -%}
  {%- if existing_columns | length > desired_columns | length -%}
    {%- do dbt_snowflake_iceberg_sync.iceberg_sync_raise(
      'source schema removed one or more existing columns'
    ) -%}
  {%- endif -%}
  {%- for existing in existing_columns -%}
    {%- set desired = desired_columns[loop.index0] -%}
    {%- if existing['source_name'] != desired['source_name'] -%}
      {%- do dbt_snowflake_iceberg_sync.iceberg_sync_raise(
        "source schema reordered or renamed columns; expected '" ~ existing['source_name'] ~
        "', found '" ~ desired['source_name'] ~ "'"
      ) -%}
    {%- endif -%}
    {%- if dbt_snowflake_iceberg_sync.iceberg_sync_normalized_snowflake_type(existing['snowflake_type']) !=
      dbt_snowflake_iceberg_sync.iceberg_sync_normalized_snowflake_type(desired['snowflake_type']) -%}
      {%- do dbt_snowflake_iceberg_sync.iceberg_sync_raise(
        'incompatible type change for ' ~ existing['source_name'] ~ ': ' ~
        existing['snowflake_type'] ~ ' -> ' ~ desired['snowflake_type']
      ) -%}
    {%- endif -%}
  {%- endfor -%}
  {%- for column in desired_columns[existing_columns | length:] -%}
    {%- call statement('iceberg_sync_add_column_' ~ loop.index, auto_begin=False) -%}
      ALTER TABLE {{ internal_relation }} ADD COLUMN {{ column['ddl'] }}
    {%- endcall -%}
  {%- endfor -%}
{%- endmacro %}

{% macro iceberg_sync_delete_sql(payload, effective_mode) -%}
  {%- set internal_relation = dbt_snowflake_iceberg_sync.iceberg_sync_relation_from_payload(
    payload['internal_relation']
  ) -%}
  {%- if effective_mode == 'full_refresh' or not payload['incremental_predicate'] -%}
    {{ return('DELETE FROM ' ~ internal_relation) }}
  {%- endif -%}
  {{ return('DELETE FROM ' ~ internal_relation ~ ' WHERE ' ~ payload['incremental_predicate']) }}
{%- endmacro %}

{% macro iceberg_sync_copy_sql(payload, stage_run_location) -%}
  {%- set internal_relation = dbt_snowflake_iceberg_sync.iceberg_sync_relation_from_payload(
    payload['internal_relation']
  ) -%}
  {{ return(
    'COPY INTO ' ~ internal_relation ~ '\n'
    ~ 'FROM ' ~ stage_run_location.rstrip('/') ~ '/\n'
    ~ 'FILE_FORMAT = (TYPE = PARQUET USE_VECTORIZED_SCANNER = TRUE)\n'
    ~ 'LOAD_MODE = ADD_FILES_COPY\n'
    ~ 'MATCH_BY_COLUMN_NAME = CASE_SENSITIVE\n'
    ~ 'PURGE = FALSE'
  ) }}
{%- endmacro %}

{% macro iceberg_sync_run_load(payload, effective_mode, stage_run_location) -%}
  {%- call statement('main', auto_begin=False) -%}
    BEGIN;
    {{ dbt_snowflake_iceberg_sync.iceberg_sync_delete_sql(payload, effective_mode) }};
    {{ dbt_snowflake_iceberg_sync.iceberg_sync_copy_sql(payload, stage_run_location) }};
    COMMIT;
  {%- endcall -%}
  {{ return({
    'status': 'success',
    'retry': {
      'max_attempts': 1,
      'configured_max_attempts': payload['retry']['max_attempts'],
      'attempts': 1,
      'retryable_errors': [],
      'run_log_errors': []
    }
  }) }}
{%- endmacro %}

{% macro iceberg_sync_create_view(target_relation, internal_relation, view_columns) -%}
  {%- call statement('iceberg_sync_create_target_view', auto_begin=False) -%}
    {{ dbt_snowflake_iceberg_sync.iceberg_sync_create_view_sql(
      target_relation, internal_relation, view_columns
    ) }}
  {%- endcall -%}
{%- endmacro %}

{% macro iceberg_sync_insert_run_log_sql(payload, run_id, effective_mode, predicates, export_result, retry, cleanup, status, error_message) -%}
  {%- set run_log_table = payload['deployment']['run_log_table'] -%}
  {%- if run_log_table is none -%}
    {{ return('') }}
  {%- endif -%}
  {%- set relation = dbt_snowflake_iceberg_sync.iceberg_sync_relation_from_payload(run_log_table) -%}
  {%- set target_relation = dbt_snowflake_iceberg_sync.iceberg_sync_relation_from_payload(
    payload['target_relation'], 'view'
  ) -%}
  {%- set internal_relation = dbt_snowflake_iceberg_sync.iceberg_sync_relation_from_payload(
    payload['internal_relation']
  ) -%}
INSERT INTO {{ relation }} (
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
  {{ dbt_snowflake_iceberg_sync.iceberg_sync_sql_string_literal(run_id) }},
  {{ dbt_snowflake_iceberg_sync.iceberg_sync_sql_string_literal(payload['model']['invocation_id']) }},
  {{ dbt_snowflake_iceberg_sync.iceberg_sync_sql_string_literal(payload['model']['unique_id']) }},
  {{ dbt_snowflake_iceberg_sync.iceberg_sync_sql_string_literal(target_relation) }},
  {{ dbt_snowflake_iceberg_sync.iceberg_sync_sql_string_literal(internal_relation) }},
  {{ dbt_snowflake_iceberg_sync.iceberg_sync_sql_string_literal(payload['source_type']) }},
  {{ dbt_snowflake_iceberg_sync.iceberg_sync_sql_string_literal(effective_mode) }},
  PARSE_JSON({{ dbt_snowflake_iceberg_sync.iceberg_sync_json_sql_literal(predicates) }}),
  PARSE_JSON({{ dbt_snowflake_iceberg_sync.iceberg_sync_json_sql_literal(export_result['segments']) }}),
  PARSE_JSON({{ dbt_snowflake_iceberg_sync.iceberg_sync_json_sql_literal(export_result['job_references']) }}),
  {{ dbt_snowflake_iceberg_sync.iceberg_sync_sql_string_literal(export_result.get('staging_table_reference')) if export_result.get('staging_table_reference') else 'NULL' }},
  PARSE_JSON('[]'),
  PARSE_JSON({{ dbt_snowflake_iceberg_sync.iceberg_sync_json_sql_literal(retry) }}),
  PARSE_JSON({{ dbt_snowflake_iceberg_sync.iceberg_sync_json_sql_literal(cleanup) }}),
  {{ dbt_snowflake_iceberg_sync.iceberg_sync_sql_string_literal(status) }},
  {{ dbt_snowflake_iceberg_sync.iceberg_sync_sql_string_literal(error_message) if error_message else 'NULL' }},
  CURRENT_TIMESTAMP(),
  CURRENT_TIMESTAMP()
{%- endmacro %}

{% macro iceberg_sync_write_success_log(payload, run_id, effective_mode, predicates, export_result, retry, cleanup) -%}
  {%- if payload['deployment']['run_log_table'] is not none -%}
    {%- call statement('iceberg_sync_write_run_log', auto_begin=False) -%}
      {{ dbt_snowflake_iceberg_sync.iceberg_sync_insert_run_log_sql(
        payload, run_id, effective_mode, predicates, export_result, retry, cleanup, 'success', none
      ) }}
    {%- endcall -%}
  {%- endif -%}
{%- endmacro %}
