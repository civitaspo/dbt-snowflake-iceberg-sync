{% macro install_iceberg_sync_procedure() -%}
  {%- set deployment = dbt_snowflake_iceberg_sync.iceberg_sync_deployment_config() -%}
  {%- set procedure_fqn = dbt_snowflake_iceberg_sync.iceberg_sync_procedure_fqn() -%}
  {%- set handler_stage = deployment['handler_stage'] -%}
  {%- set handler_stage_path = deployment['handler_stage_path'] -%}
  {%- set handler_import_name = deployment['handler_import_name'] -%}
  {%- set handler_name = deployment['handler_name'] -%}
  {%- set handler_local_path = deployment['handler_local_path'] -%}
  {%- set external_access_integrations = deployment['external_access_integrations'] -%}
  {%- set run_log_table = deployment['run_log_table'] -%}
  {%- set parquet_file_format = deployment['parquet_file_format'] -%}
  {%- set google_cloud_auth_method = deployment['google_cloud_auth_method'] -%}
  {%- set google_cloud_service_account_secret_fqdn = deployment['google_cloud_service_account_secret_fqdn'] -%}
  {%- set google_cloud_service_account_secret_alias = deployment['google_cloud_service_account_secret_alias'] -%}
  {%- set procedure_files = [
    '__init__.py',
    'handler.py',
    'config.py',
    'google_cloud_auth.py',
    'snowflake.py',
    'schema.py',
    'run_log.py',
    'sql.py',
    'errors.py',
    'utils.py',
    'sources/base.py',
    'sources/registry.py',
    'sources/__init__.py',
    'sources/bigquery.py',
    'sources/s3_parquet.py'
  ] -%}

  {% call statement('iceberg_sync_create_handler_stage') -%}
    CREATE STAGE IF NOT EXISTS {{ handler_stage }}
  {%- endcall %}

  {%- if parquet_file_format is not none -%}
    {% call statement('iceberg_sync_create_parquet_file_format') -%}
      CREATE FILE FORMAT IF NOT EXISTS {{ parquet_file_format }}
      TYPE = PARQUET
      USE_VECTORIZED_SCANNER = TRUE
    {%- endcall %}
  {%- endif -%}

  {%- if run_log_table is not none -%}
    {%- set run_log_relation = dbt_snowflake_iceberg_sync.iceberg_sync_relation_from_payload(run_log_table) -%}
    {% call statement('iceberg_sync_create_or_alter_run_log_table') -%}
      CREATE OR ALTER TABLE {{ run_log_relation }} (
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
      )
    {%- endcall %}
  {%- endif -%}

  {%- for procedure_file in procedure_files -%}
    {%- set destination_dir = handler_stage_path -%}
    {%- if '/' in procedure_file -%}
      {%- set path_parts = procedure_file.split('/') -%}
      {%- set destination_dir = handler_stage_path ~ '/' ~ (path_parts[:-1] | join('/')) -%}
    {%- endif -%}
    {% call statement('iceberg_sync_put_' ~ loop.index) -%}
      PUT file://{{ handler_local_path.rstrip('/') }}/{{ procedure_file }}
      @{{ handler_stage }}/{{ destination_dir }}
      AUTO_COMPRESS = FALSE
      OVERWRITE = TRUE
    {%- endcall %}
  {%- endfor %}

  {% call statement('iceberg_sync_create_procedure') -%}
    CREATE OR ALTER PROCEDURE {{ procedure_fqn }}(config VARIANT)
    RETURNS VARIANT
    LANGUAGE PYTHON
    RUNTIME_VERSION = '3.12'
    PACKAGES = ('snowflake-snowpark-python', 'requests', 'google-auth')
    IMPORTS = ('@{{ handler_stage }}/{{ handler_stage_path }}/={{ handler_import_name }}/')
    HANDLER = '{{ handler_name }}'
    {%- if external_access_integrations | length > 0 %}
    EXTERNAL_ACCESS_INTEGRATIONS = ({{ external_access_integrations | join(', ') }})
    {%- endif %}
    {%- if google_cloud_auth_method == 'service_account_credentials_json'
      and google_cloud_service_account_secret_fqdn is not none %}
    SECRETS = ('{{ google_cloud_service_account_secret_alias }}' = {{ google_cloud_service_account_secret_fqdn }})
    {%- endif %}
    EXECUTE AS CALLER
  {%- endcall %}
{%- endmacro %}
