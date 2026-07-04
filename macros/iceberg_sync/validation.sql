{% macro iceberg_sync_raise_if_secret_config(config_values) %}
  {% set forbidden = [
    'gcp_sa_secret_fqdn',
    'gcp_sa_secret_alias',
    'gcp_auth_method',
    'gcp_wif_secret_fqdn',
    'gcp_wif_audience',
    'gcp_service_account_impersonation',
    'google_application_credentials',
    'google_credentials',
    'gcp_service_account_json',
    'service_account_json',
    'private_key',
    'password',
    'secret'
  ] %}

  {% for key in forbidden %}
    {% set value = config_values.get(key) %}
    {% if value is not none %}
      {{ exceptions.raise_compiler_error(
        "Do not put credential material or Snowflake secret bindings in model config. Move " ~ key ~ " to vars.iceberg_sync or Snowflake secrets."
      ) }}
    {% endif %}
  {% endfor %}
{% endmacro %}

{% macro iceberg_sync_validate_config(config_values) %}
  {% do dbt_snowflake_iceberg_sync.iceberg_sync_raise_if_secret_config(config_values) %}

  {% if config_values.get('source_type') != 'bigquery' %}
    {{ exceptions.raise_compiler_error("iceberg_sync currently supports source_type='bigquery' only.") }}
  {% endif %}

  {% if config_values.get('materialization_strategy') not in ['full_refresh', 'incremental'] %}
    {{ exceptions.raise_compiler_error("materialization_strategy must be 'full_refresh' or 'incremental'.") }}
  {% endif %}

  {% if config_values.get('bigquery_export_strategy') not in ['extract', 'select'] %}
    {{ exceptions.raise_compiler_error("bigquery_export_strategy must be 'extract' or 'select'.") }}
  {% endif %}

  {% set predicate_type = config_values.get('bigquery_export_predicate_type') %}
  {% if predicate_type not in ['auto', 'none', 'partition_decorator', 'table_suffix', 'where'] %}
    {{ exceptions.raise_compiler_error("bigquery_export_predicate_type must be auto, none, partition_decorator, table_suffix, or where.") }}
  {% endif %}

  {% if config_values.get('bigquery_export_strategy') == 'extract' and predicate_type == 'where' %}
    {{ exceptions.raise_compiler_error("bigquery_export_strategy='extract' does not support bigquery_export_predicate_type='where'.") }}
  {% endif %}

  {% if config_values.get('bigquery_export_strategy') == 'select' and predicate_type not in ['auto', 'none', 'where'] %}
    {{ exceptions.raise_compiler_error("bigquery_export_strategy='select' supports only auto, none, or where predicate types.") }}
  {% endif %}

  {% for key in [
    'google_cloud_project_id',
    'bigquery_dataset_id',
    'bigquery_table_id',
    'bigquery_location',
    'bigquery_export_location',
    'iceberg_table_external_volume'
  ] %}
    {% if config_values.get(key) is none or config_values.get(key) == '' %}
      {{ exceptions.raise_compiler_error("Missing required iceberg_sync model config: " ~ key) }}
    {% endif %}
  {% endfor %}

  {% if config_values.get('bigquery_export_strategy') == 'select' %}
    {% if config_values.get('bigquery_staging_dataset_id') is none or config_values.get('bigquery_staging_dataset_id') == '' %}
      {{ exceptions.raise_compiler_error("bigquery_staging_dataset_id is required when bigquery_export_strategy='select'.") }}
    {% endif %}
  {% endif %}

  {% if config_values.get('partition_by') | length > 0 %}
    {{ exceptions.raise_compiler_error("partition_by is not supported by iceberg_sync in the first scope.") }}
  {% endif %}

  {% if config_values.get('cluster_by') | length > 0 %}
    {{ exceptions.raise_compiler_error("cluster_by is not supported by iceberg_sync in the first scope.") }}
  {% endif %}

  {% if config_values.get('incremental_strategy') != 'delete+copy' %}
    {{ exceptions.raise_compiler_error("iceberg_sync currently supports incremental_strategy='delete+copy' only.") }}
  {% endif %}
{% endmacro %}

{% macro iceberg_sync_validate_view_columns(view_columns) %}
  {% set seen = [] %}
  {% for column in view_columns %}
    {% if column.get('storage_name') is none or column.get('alias') is none %}
      {{ exceptions.raise_compiler_error("Procedure result view_columns entries must include storage_name and alias.") }}
    {% endif %}
    {% if column.get('alias') in seen %}
      {{ exceptions.raise_compiler_error("Procedure returned duplicate view alias: " ~ column.get('alias')) }}
    {% endif %}
    {% do seen.append(column.get('alias')) %}
  {% endfor %}
{% endmacro %}
