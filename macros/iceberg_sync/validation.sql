{% macro iceberg_sync_raise(message) -%}
  {%- do exceptions.raise_compiler_error("iceberg_sync: " ~ message) -%}
{%- endmacro %}

{% macro iceberg_sync_model_meta(model_node) -%}
  {%- set model_meta = {} -%}
  {%- if model_node.config is defined and model_node.config.meta is defined and model_node.config.meta is mapping -%}
    {%- set model_meta = model_node.config.meta -%}
  {%- endif -%}
  {%- if model_meta.get('iceberg_sync', none) is mapping -%}
    {{ return(model_meta.get('iceberg_sync')) }}
  {%- endif -%}
  {{ return({}) }}
{%- endmacro %}

{% macro iceberg_sync_model_config(model_node, config_name, default=none) -%}
  {%- set model_meta = dbt_snowflake_iceberg_sync.iceberg_sync_model_meta(model_node) -%}
  {%- if model_meta.get(config_name, none) is not none -%}
    {{ return(model_meta.get(config_name)) }}
  {%- endif -%}
  {{ return(config.get(config_name, default)) }}
{%- endmacro %}

{% macro iceberg_sync_required_model_config(model_node, config_name) -%}
  {%- set value = dbt_snowflake_iceberg_sync.iceberg_sync_model_config(model_node, config_name, none) -%}
  {%- if value is none or value == "" -%}
    {%- do dbt_snowflake_iceberg_sync.iceberg_sync_raise(config_name ~ " is required") -%}
  {%- endif -%}
  {{ return(value) }}
{%- endmacro %}

{% macro iceberg_sync_number_model_config(model_node, config_name, default, integer=false) -%}
  {%- set value = dbt_snowflake_iceberg_sync.iceberg_sync_model_config(
    model_node, config_name, default
  ) -%}
  {{ return(dbt_snowflake_iceberg_sync.iceberg_sync_number_config(
    value, config_name, integer
  )) }}
{%- endmacro %}

{% macro iceberg_sync_number_config(value, config_name, integer=false) -%}
  {%- if value is sameas true or value is sameas false -%}
    {%- do dbt_snowflake_iceberg_sync.iceberg_sync_raise(
      config_name ~ " must be " ~ ("an integer" if integer else "a number")
    ) -%}
  {%- elif value is number -%}
    {%- if integer and value != (value | int) -%}
      {%- do dbt_snowflake_iceberg_sync.iceberg_sync_raise(
        config_name ~ " must be an integer"
      ) -%}
    {%- endif -%}
    {{ return((value | int) if integer else value) }}
  {%- elif value is string -%}
    {%- set text = value | trim -%}
    {%- set unsigned = text -%}
    {%- if unsigned.startswith("+") or unsigned.startswith("-") -%}
      {%- set unsigned = unsigned[1:] -%}
    {%- endif -%}
    {%- set parts = unsigned.split(".") -%}
    {%- set validation = namespace(invalid=(
      text == ""
      or unsigned == ""
      or parts | length > 2
      or "" in parts
      or (integer and parts | length != 1)
    )) -%}
    {%- for part in parts -%}
      {%- if not part.isdigit() -%}
        {%- set validation.invalid = true -%}
      {%- endif -%}
    {%- endfor -%}
    {%- if validation.invalid -%}
      {%- do dbt_snowflake_iceberg_sync.iceberg_sync_raise(
        config_name ~ " must be " ~ ("an integer" if integer else "a number")
      ) -%}
    {%- endif -%}
    {{ return((text | int) if integer else (text | float)) }}
  {%- else -%}
    {%- do dbt_snowflake_iceberg_sync.iceberg_sync_raise(
      config_name ~ " must be " ~ ("an integer" if integer else "a number")
    ) -%}
  {%- endif -%}
{%- endmacro %}

{% macro iceberg_sync_as_list(value) -%}
  {%- if value is none -%}
    {{ return([]) }}
  {%- elif value is string -%}
    {{ return([value]) }}
  {%- else -%}
    {{ return(value) }}
  {%- endif -%}
{%- endmacro %}

{% macro iceberg_sync_validate_forbidden_model_configs(model_node) -%}
  {%- set forbidden = [
    'credentials',
    'credential',
    'password',
    'private_key',
    'service_account',
    'service_account_json',
    'google_cloud_service_account_json',
    'google_cloud_service_account_secret_fqdn',
    'google_cloud_service_account_secret_alias',
    'google_application_credentials'
  ] -%}
  {%- set model_meta = dbt_snowflake_iceberg_sync.iceberg_sync_model_meta(model_node) -%}
  {%- for key in forbidden -%}
    {%- if (
      config.get(key, none) is not none
      or model_meta.get(key, none) is not none
    ) -%}
      {%- do dbt_snowflake_iceberg_sync.iceberg_sync_raise(
        "credential material must not be set in model config: " ~ key
      ) -%}
    {%- endif -%}
  {%- endfor -%}
{%- endmacro %}

{% macro iceberg_sync_validate_payload(payload) -%}
  {%- if payload['source_type'] != 'bigquery' -%}
    {%- do dbt_snowflake_iceberg_sync.iceberg_sync_raise("source_type must be 'bigquery'") -%}
  {%- endif -%}

  {%- if payload['materialization_strategy'] not in ['full_refresh', 'incremental'] -%}
    {%- do dbt_snowflake_iceberg_sync.iceberg_sync_raise(
      "materialization_strategy must be 'full_refresh' or 'incremental'"
    ) -%}
  {%- endif -%}

  {%- if payload['incremental_strategy'] != 'delete+copy' -%}
    {%- do dbt_snowflake_iceberg_sync.iceberg_sync_raise(
      "incremental_strategy must be 'delete+copy'"
    ) -%}
  {%- endif -%}

  {%- set retry = payload['retry'] -%}
  {%- if retry['max_attempts'] < 1 -%}
    {%- do dbt_snowflake_iceberg_sync.iceberg_sync_raise(
      "iceberg_sync_retry_max_attempts must be at least 1"
    ) -%}
  {%- endif -%}
  {%- if retry['initial_delay_seconds'] < 0 -%}
    {%- do dbt_snowflake_iceberg_sync.iceberg_sync_raise(
      "iceberg_sync_retry_initial_delay_seconds must be non-negative"
    ) -%}
  {%- endif -%}
  {%- if retry['max_delay_seconds'] < 0 -%}
    {%- do dbt_snowflake_iceberg_sync.iceberg_sync_raise(
      "iceberg_sync_retry_max_delay_seconds must be non-negative"
    ) -%}
  {%- endif -%}
  {%- if retry['backoff_multiplier'] < 1.0 -%}
    {%- do dbt_snowflake_iceberg_sync.iceberg_sync_raise(
      "iceberg_sync_retry_backoff_multiplier must be at least 1.0"
    ) -%}
  {%- endif -%}
  {%- if retry['jitter_seconds'] < 0 -%}
    {%- do dbt_snowflake_iceberg_sync.iceberg_sync_raise(
      "iceberg_sync_retry_jitter_seconds must be non-negative"
    ) -%}
  {%- endif -%}

  {%- set iceberg_table = payload['iceberg_table'] -%}
  {%- if iceberg_table['storage_serialization_policy'] not in ['COMPATIBLE', 'OPTIMIZED'] -%}
    {%- do dbt_snowflake_iceberg_sync.iceberg_sync_raise(
      "iceberg_table_storage_serialization_policy must be COMPATIBLE or OPTIMIZED"
    ) -%}
  {%- endif -%}
  {%- if iceberg_table['iceberg_version'] not in [2, 3] -%}
    {%- do dbt_snowflake_iceberg_sync.iceberg_sync_raise(
      "iceberg_table_iceberg_version must be 2 or 3"
    ) -%}
  {%- endif -%}
  {%- if iceberg_table['iceberg_version'] == 3 and not iceberg_table['change_tracking'] -%}
    {%- do dbt_snowflake_iceberg_sync.iceberg_sync_raise(
      "iceberg_table_change_tracking must be true for Iceberg V3 tables"
    ) -%}
  {%- endif -%}
  {%- if iceberg_table['error_logging'] -%}
    {%- do dbt_snowflake_iceberg_sync.iceberg_sync_raise(
      "iceberg_table_error_logging is not supported for Iceberg COPY INTO"
    ) -%}
  {%- endif -%}

  {%- set bq = payload['bigquery'] -%}
  {%- if bq['export_strategy'] not in ['extract', 'select'] -%}
    {%- do dbt_snowflake_iceberg_sync.iceberg_sync_raise(
      "bigquery_export_strategy must be 'extract' or 'select'"
    ) -%}
  {%- endif -%}
  {%- if bq['export_compression'] not in ['GZIP', 'NONE', 'SNAPPY', 'ZSTD'] -%}
    {%- do dbt_snowflake_iceberg_sync.iceberg_sync_raise(
      "bigquery_export_compression must be one of GZIP, NONE, SNAPPY, or ZSTD"
    ) -%}
  {%- endif -%}
  {%- if bq['export_predicate_type'] not in ['auto', 'none', 'partition_decorator', 'table_suffix', 'where'] -%}
    {%- do dbt_snowflake_iceberg_sync.iceberg_sync_raise(
      "bigquery_export_predicate_type is invalid"
    ) -%}
  {%- endif -%}
  {%- if bq['export_poll_interval_seconds'] <= 0 -%}
    {%- do dbt_snowflake_iceberg_sync.iceberg_sync_raise(
      "bigquery_export_poll_interval_seconds must be positive"
    ) -%}
  {%- endif -%}
  {%- if bq['export_poll_timeout_seconds'] <= 0 -%}
    {%- do dbt_snowflake_iceberg_sync.iceberg_sync_raise(
      "bigquery_export_poll_timeout_seconds must be positive"
    ) -%}
  {%- endif -%}
  {%- if bq['export_poll_interval_seconds'] > bq['export_poll_timeout_seconds'] -%}
    {%- do dbt_snowflake_iceberg_sync.iceberg_sync_raise(
      "bigquery_export_poll_interval_seconds must not exceed bigquery_export_poll_timeout_seconds"
    ) -%}
  {%- endif -%}
  {%- if not bq['export_location'] or not bq['export_location'].startswith('@') -%}
    {%- do dbt_snowflake_iceberg_sync.iceberg_sync_raise(
      "bigquery_export_location must be a named Snowflake stage location"
    ) -%}
  {%- endif -%}
  {%- if bq['export_location'].startswith('@~') or bq['export_location'].startswith('@%') -%}
    {%- do dbt_snowflake_iceberg_sync.iceberg_sync_raise(
      "bigquery_export_location must be a named Snowflake stage, not a user or table stage"
    ) -%}
  {%- endif -%}
  {%- if bq['export_strategy'] == 'select' and not bq['staging_dataset_id'] -%}
    {%- do dbt_snowflake_iceberg_sync.iceberg_sync_raise(
      "bigquery_staging_dataset_id is required when bigquery_export_strategy='select'"
    ) -%}
  {%- endif -%}
  {%- if bq['export_strategy'] == 'select' and not payload['model']['sql'] | trim -%}
    {%- do dbt_snowflake_iceberg_sync.iceberg_sync_raise(
      "model SQL is required when bigquery_export_strategy='select'"
    ) -%}
  {%- endif -%}
  {%- if bq['export_strategy'] == 'select' and bq['export_predicate_type'] not in ['auto', 'none', 'where'] -%}
    {%- do dbt_snowflake_iceberg_sync.iceberg_sync_raise(
      "select export strategy allows only auto, none, or where predicates"
    ) -%}
  {%- endif -%}
  {%- if bq['export_strategy'] == 'extract' and bq['export_predicate_type'] == 'where' -%}
    {%- do dbt_snowflake_iceberg_sync.iceberg_sync_raise(
      "extract export strategy does not support where predicates"
    ) -%}
  {%- endif -%}

  {%- if payload['partition_by'] | length > 0 -%}
    {%- do dbt_snowflake_iceberg_sync.iceberg_sync_raise(
      "partition_by is not supported by iceberg_sync in the first scope"
    ) -%}
  {%- endif -%}
  {%- if payload['cluster_by'] | length > 0 -%}
    {%- do dbt_snowflake_iceberg_sync.iceberg_sync_raise(
      "cluster_by is not supported by iceberg_sync in the first scope"
    ) -%}
  {%- endif -%}

  {%- set has_bq_incremental = bq['incremental_predicates'] | length > 0 -%}
  {%- set has_snowflake_incremental = payload['incremental_predicate'] is not none and payload['incremental_predicate'] != "" -%}
  {%- if has_bq_incremental != has_snowflake_incremental -%}
    {%- do dbt_snowflake_iceberg_sync.iceberg_sync_raise(
      "incremental BigQuery predicates and incremental_predicate must be both present or both absent"
    ) -%}
  {%- endif -%}
{%- endmacro %}
