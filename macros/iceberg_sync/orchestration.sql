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
  {%- if payload['source_type'] == 's3_parquet' -%}
    {%- if effective_mode == 'full_refresh' -%}
      {{ return(payload['s3_parquet']['full_refresh_paths']) }}
    {%- endif -%}
    {{ return(payload['s3_parquet']['incremental_paths']) }}
  {%- endif -%}
  {%- if effective_mode == 'full_refresh' -%}
    {{ return(payload['bigquery']['full_refresh_predicates']) }}
  {%- endif -%}
  {{ return(payload['bigquery']['incremental_predicates']) }}
{%- endmacro %}

{% macro iceberg_sync_parse_stage_location(export_location, field_name='bigquery_export_location') -%}
  {%- if not export_location.startswith('@') -%}
    {%- do dbt_snowflake_iceberg_sync.iceberg_sync_raise(
      field_name ~ ' must start with @'
    ) -%}
  {%- endif -%}
  {%- set raw = export_location[1:] -%}
  {%- if raw == '' or raw.startswith('~') or raw.startswith('%') -%}
    {%- do dbt_snowflake_iceberg_sync.iceberg_sync_raise(
      field_name ~ ' must be a named Snowflake stage, not a user or table stage'
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
      stage_raw, field_name ~ ' stage', 1, 3
    ),
    'stage_path': stage_path
  }) }}
{%- endmacro %}

{% macro iceberg_sync_resolve_stage_location(
  export_location,
  run_id=none,
  allowed_schemes=['gcs://'],
  field_name='bigquery_export_location',
  cloud_label='GCS'
) -%}
  {%- set parsed = dbt_snowflake_iceberg_sync.iceberg_sync_parse_stage_location(
    export_location,
    field_name
  ) -%}
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
  {%- set scheme_check = namespace(ok=false) -%}
  {%- for scheme in allowed_schemes -%}
    {%- if url_text.startswith(scheme) -%}
      {%- set scheme_check.ok = true -%}
    {%- endif -%}
  {%- endfor -%}
  {%- if not scheme_check.ok -%}
    {%- do dbt_snowflake_iceberg_sync.iceberg_sync_raise(
      field_name ~ ' must reference a Snowflake stage backed by ' ~ cloud_label
    ) -%}
  {%- endif -%}
  {%- set run_path_parts = [] -%}
  {%- if stage_path -%}
    {%- do run_path_parts.append(stage_path) -%}
  {%- endif -%}
  {%- if run_id -%}
    {%- do run_path_parts.append(run_id) -%}
  {%- endif -%}
  {%- set run_path = run_path_parts | join('/') -%}
  {%- if run_path -%}
    {%- set run_stage_location = '@' ~ stage_fqn ~ '/' ~ run_path -%}
  {%- else -%}
    {%- set run_stage_location = '@' ~ stage_fqn -%}
  {%- endif -%}
  {%- if url_text.startswith('gcs://') -%}
    {%- set remote_base = 'gs://' ~ url_text.removeprefix('gcs://') -%}
  {%- else -%}
    {%- set remote_base = url_text -%}
  {%- endif -%}
  {%- if run_path -%}
    {%- set remote_run_uri = (remote_base.rstrip('/') ~ '/' ~ run_path).strip() -%}
  {%- else -%}
    {%- set remote_run_uri = remote_base.rstrip('/') -%}
  {%- endif -%}
  {{ return({
    'stage_fqn': stage_fqn,
    'stage_path': stage_path,
    'stage_url': url_text,
    'run_stage_location': run_stage_location,
    'remote_run_uri': remote_run_uri,
    'gcs_run_uri': remote_run_uri
  }) }}
{%- endmacro %}

{% macro iceberg_sync_resolve_s3_parquet_locations(payload, effective_mode) -%}
  {%- set s3 = payload['s3_parquet'] -%}
  {%- set stage = dbt_snowflake_iceberg_sync.iceberg_sync_resolve_stage_location(
    s3['location'],
    none,
    ['s3://', 's3gov://', 's3china://'],
    's3_parquet_location',
    'S3'
  ) -%}
  {%- set paths = dbt_snowflake_iceberg_sync.iceberg_sync_predicates_for_mode(
    payload,
    effective_mode
  ) -%}
  {%- set locations = [] -%}
  {%- for path_suffix in paths -%}
    {%- if path_suffix -%}
      {%- set stage_location = stage['run_stage_location'].rstrip('/') ~ '/' ~ path_suffix -%}
      {%- set remote_uri = stage['remote_run_uri'].rstrip('/') ~ '/' ~ path_suffix -%}
    {%- else -%}
      {%- set stage_location = stage['run_stage_location'] -%}
      {%- set remote_uri = stage['remote_run_uri'] -%}
    {%- endif -%}
    {%- do locations.append({
      'stage_location': stage_location,
      'remote_uri': remote_uri,
      'pattern': s3['file_pattern'],
      'force': true,
      'path_suffix': path_suffix
    }) -%}
  {%- endfor -%}
  {{ return({
    'stage': stage,
    'locations': locations,
    'destination_uri': stage['remote_run_uri']
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
  {%- if payload['source_type'] == 's3_parquet' -%}
    {%- set interval_seconds = 1 -%}
    {%- set timeout_seconds = 1 -%}
  {%- else -%}
    {%- set interval_seconds = payload['bigquery']['export_poll_interval_seconds'] -%}
    {%- set timeout_seconds = payload['bigquery']['export_poll_timeout_seconds'] -%}
  {%- endif -%}
  {%- set max_polls = ((timeout_seconds / interval_seconds) | int) + 1 -%}
  {%- for attempt in range(max_polls) -%}
    {%- if ns.result.get('status') == 'success' -%}
      {{ return(ns.result['export_result']) }}
    {%- elif ns.result.get('status') == 'skipped' -%}
      {%- set skipped_result = ns.result.get('export_result', {}) -%}
      {%- do skipped_result.update({
        'skipped': true,
        'skip_reason': ns.result.get('skip_reason')
      }) -%}
      {{ return(skipped_result) }}
    {%- elif ns.result.get('status') != 'running' -%}
      {%- do dbt_snowflake_iceberg_sync.iceberg_sync_raise(
        ns.result.get('error_message', 'source export failed')
      ) -%}
    {%- endif -%}
    {%- if attempt + 1 < max_polls -%}
      {%- set wait_milliseconds = ((interval_seconds * 1000) | int) -%}
      {%- if wait_milliseconds > 0 -%}
        {%- call statement('iceberg_sync_wait_for_export_' ~ attempt, auto_begin=False) -%}
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
    'source export did not finish before the configured poll timeout'
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
  {%- set result = modules.re.sub('\\b(VARCHAR|TEXT|STRING)\\(\\d+\\)', 'VARCHAR', result) -%}
  {%- set result = modules.re.sub('\\bNUMBER\\(19,0\\)', 'BIGINT', result) -%}
  {%- set result = modules.re.sub('\\bFLOAT\\b', 'DOUBLE', result) -%}
  {%- set result = modules.re.sub('\\bTEXT\\b', 'VARCHAR', result) -%}
  {%- set result = modules.re.sub('\\bSTRING\\b', 'VARCHAR', result) -%}
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
      ALTER ICEBERG TABLE {{ internal_relation }} ADD COLUMN {{ column['ddl'] }}
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

{% macro iceberg_sync_copy_sql(payload, stage_run_location, pattern=none, files=none, force=false, columns=none) -%}
  {%- set internal_relation = dbt_snowflake_iceberg_sync.iceberg_sync_relation_from_payload(
    payload['internal_relation']
  ) -%}
  {%- set load_mode = 'add_files_copy' -%}
  {%- if payload['source_type'] == 's3_parquet' and payload.get('s3_parquet') -%}
    {%- set load_mode = payload['s3_parquet'].get('load_mode', 'add_files_copy') | string | lower | trim -%}
  {%- endif -%}
  {%- set sql_load_mode = 'FULL_INGEST' if load_mode == 'full_ingest' else 'ADD_FILES_COPY' -%}
  {%- set stage_location = stage_run_location.rstrip('/') ~ '/' -%}
  {%- set expressed_columns = [] -%}
  {%- if sql_load_mode == 'FULL_INGEST' and columns is not none and (columns | length) > 0 -%}
    {%- for column in columns -%}
      {%- if column.get('expression') -%}
        {%- do expressed_columns.append(column) -%}
      {%- endif -%}
    {%- endfor -%}
  {%- endif -%}
  {%- set use_transforms = (expressed_columns | length) > 0 -%}
  {%- if use_transforms -%}
    {%- set target_columns = [] -%}
    {%- set select_items = [] -%}
    {%- for column in columns -%}
      {%- set source_name = column.get('source_name') or column.get('name') -%}
      {%- do target_columns.append(adapter.quote(source_name)) -%}
      {%- if column.get('expression') -%}
        {%- do select_items.append(column['expression']) -%}
      {%- else -%}
        {%- do select_items.append('$1:' ~ adapter.quote(source_name)) -%}
      {%- endif -%}
    {%- endfor -%}
    {%- set parts = [
      'COPY INTO ' ~ internal_relation ~ ' (' ~ (target_columns | join(', ')) ~ ')',
      'FROM (',
      '  SELECT',
      '    ' ~ (select_items | join(',\n    ')),
      '  FROM ' ~ stage_location,
      ')',
      'FILE_FORMAT = (TYPE = PARQUET USE_VECTORIZED_SCANNER = TRUE)',
      'LOAD_MODE = ' ~ sql_load_mode,
      'PURGE = FALSE'
    ] -%}
  {%- else -%}
    {%- set parts = [
      'COPY INTO ' ~ internal_relation,
      'FROM ' ~ stage_location,
      'FILE_FORMAT = (TYPE = PARQUET USE_VECTORIZED_SCANNER = TRUE)',
      'LOAD_MODE = ' ~ sql_load_mode,
      'MATCH_BY_COLUMN_NAME = CASE_SENSITIVE',
      'PURGE = FALSE'
    ] -%}
  {%- endif -%}
  {%- if files is not none and (files | length) > 0 -%}
    {%- set file_literals = [] -%}
    {%- for file_name in files -%}
      {%- do file_literals.append(
        dbt_snowflake_iceberg_sync.iceberg_sync_sql_string_literal(file_name)
      ) -%}
    {%- endfor -%}
    {%- do parts.append('FILES = (' ~ (file_literals | join(', ')) ~ ')') -%}
  {%- elif pattern -%}
    {%- do parts.append(
      'PATTERN = ' ~ dbt_snowflake_iceberg_sync.iceberg_sync_sql_string_literal(pattern)
    ) -%}
  {%- endif -%}
  {%- if force -%}
    {%- do parts.append('FORCE = TRUE') -%}
  {%- endif -%}
  {{ return(parts | join('\n')) }}
{%- endmacro %}

{% macro iceberg_sync_run_load(payload, effective_mode, load_locations, columns=none) -%}
  {%- call statement('main', auto_begin=False) -%}
    BEGIN;
    {{ dbt_snowflake_iceberg_sync.iceberg_sync_delete_sql(payload, effective_mode) }};
    {%- for location in load_locations %}
    {{ dbt_snowflake_iceberg_sync.iceberg_sync_copy_sql(
      payload,
      location['stage_location'],
      location.get('pattern'),
      location.get('files'),
      location.get('force', false),
      columns
    ) }};
    {%- endfor %}
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

{% macro iceberg_sync_write_skipped_log(payload, run_id, effective_mode, predicates, export_result, retry, cleanup) -%}
  {%- if payload['deployment']['run_log_table'] is not none -%}
    {%- call statement('iceberg_sync_write_skipped_run_log', auto_begin=False) -%}
      {{ dbt_snowflake_iceberg_sync.iceberg_sync_insert_run_log_sql(
        payload,
        run_id,
        effective_mode,
        predicates,
        export_result,
        retry,
        cleanup,
        'skipped',
        export_result.get('skip_reason')
      ) }}
    {%- endcall -%}
  {%- endif -%}
{%- endmacro %}
