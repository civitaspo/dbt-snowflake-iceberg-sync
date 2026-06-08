{% macro install_iceberg_sync_procedure() -%}
  {%- set deployment = dbt_snowflake_iceberg_sync.iceberg_sync_deployment_config() -%}
  {%- set procedure_relation = dbt_snowflake_iceberg_sync.iceberg_sync_procedure_relation() -%}
  {%- set handler_stage = deployment['handler_stage'] -%}
  {%- set handler_stage_path = deployment['handler_stage_path'] -%}
  {%- set handler_import_name = deployment['handler_import_name'] -%}
  {%- set handler_name = deployment['handler_name'] -%}
  {%- set handler_local_path = deployment['handler_local_path'] -%}
  {%- set external_access_integrations = deployment['external_access_integrations'] -%}
  {%- set google_cloud_service_account_secret_fqdn = deployment['google_cloud_service_account_secret_fqdn'] -%}
  {%- set google_cloud_service_account_secret_alias = deployment['google_cloud_service_account_secret_alias'] -%}
  {%- set procedure_files = [
    '__init__.py',
    'handler.py',
    'config.py',
    'snowflake.py',
    'schema.py',
    'run_log.py',
    'sql.py',
    'errors.py',
    'utils.py',
    'sources/base.py',
    'sources/registry.py',
    'sources/__init__.py',
    'sources/bigquery.py'
  ] -%}

  {% call statement('iceberg_sync_create_handler_stage') -%}
    CREATE STAGE IF NOT EXISTS {{ handler_stage }}
  {%- endcall %}

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
    CREATE OR REPLACE PROCEDURE {{ procedure_relation }}(config VARIANT)
    RETURNS VARIANT
    LANGUAGE PYTHON
    RUNTIME_VERSION = '3.12'
    PACKAGES = ('snowflake-snowpark-python', 'requests', 'google-auth')
    IMPORTS = ('@{{ handler_stage }}/{{ handler_stage_path }}/={{ handler_import_name }}/')
    HANDLER = '{{ handler_name }}'
    {%- if external_access_integrations | length > 0 %}
    EXTERNAL_ACCESS_INTEGRATIONS = ({{ external_access_integrations | join(', ') }})
    {%- endif %}
    SECRETS = ('{{ google_cloud_service_account_secret_alias }}' = {{ google_cloud_service_account_secret_fqdn }})
    EXECUTE AS CALLER
  {%- endcall %}
{%- endmacro %}
