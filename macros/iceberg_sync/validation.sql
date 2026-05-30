{% macro iceberg_sync_raise(message) -%}
  {%- do exceptions.raise_compiler_error("iceberg_sync: " ~ message) -%}
{%- endmacro %}

{% macro iceberg_sync_required_model_config(config_name) -%}
  {%- set value = config.get(config_name, none) -%}
  {%- if value is none or value == "" -%}
    {%- do dbt_snowflake_iceberg_sync.iceberg_sync_raise(config_name ~ " is required") -%}
  {%- endif -%}
  {{ return(value) }}
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

{% macro iceberg_sync_validate_forbidden_model_configs() -%}
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
  {%- for key in forbidden -%}
    {%- if config.get(key, none) is not none -%}
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
  {%- if bq['export_predicate_type'] not in ['auto', 'none', 'partition_decorator', 'table_suffix', 'where'] -%}
    {%- do dbt_snowflake_iceberg_sync.iceberg_sync_raise(
      "bigquery_export_predicate_type is invalid"
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
