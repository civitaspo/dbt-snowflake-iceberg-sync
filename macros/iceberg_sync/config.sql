{% macro iceberg_sync_config(model, target_relation, internal_relation) %}
  {% set target = dbt_snowflake_iceberg_sync.iceberg_sync_relation_dict(target_relation) %}
  {% set internal = dbt_snowflake_iceberg_sync.iceberg_sync_relation_dict(internal_relation) %}
  {% set deployment = var('iceberg_sync', {}) %}

  {% set model_sql = model.get('compiled_code') %}
  {% if model_sql is none %}
    {% set model_sql = model.get('compiled_sql', '') %}
  {% endif %}

  {% set values = {
    'source_type': config.get('source_type', 'bigquery'),
    'materialization_strategy': config.get('materialization_strategy', 'incremental'),
    'bigquery_export_strategy': config.get('bigquery_export_strategy', 'extract'),
    'google_cloud_project_id': config.get('google_cloud_project_id'),
    'bigquery_dataset_id': config.get('bigquery_dataset_id'),
    'bigquery_table_id': config.get('bigquery_table_id'),
    'bigquery_location': config.get('bigquery_location'),
    'bigquery_export_location': config.get('bigquery_export_location'),
    'bigquery_export_predicate_type': config.get('bigquery_export_predicate_type', 'auto'),
    'bigquery_export_full_refresh_predicates': config.get('bigquery_export_full_refresh_predicates', []),
    'bigquery_export_incremental_predicates': config.get('bigquery_export_incremental_predicates', []),
    'bigquery_staging_dataset_id': config.get('bigquery_staging_dataset_id'),
    'bigquery_staging_table_expiration_hours': config.get('bigquery_staging_table_expiration_hours', 24),
    'bigquery_staging_table_reuse': config.get('bigquery_staging_table_reuse', true),
    'force_rebuild_staging_table': config.get('force_rebuild_staging_table', false),
    'incremental_strategy': config.get('incremental_strategy', 'delete+copy'),
    'incremental_predicate': config.get('incremental_predicate'),
    'iceberg_table_external_volume': config.get('iceberg_table_external_volume'),
    'iceberg_table_base_location': config.get('iceberg_table_base_location'),
    'iceberg_table_target_file_size': config.get('iceberg_table_target_file_size', 'AUTO'),
    'iceberg_table_storage_serialization_policy': config.get('iceberg_table_storage_serialization_policy', 'COMPATIBLE'),
    'iceberg_table_data_retention_time_in_days': config.get('iceberg_table_data_retention_time_in_days', 7),
    'iceberg_table_max_data_extension_time_in_days': config.get('iceberg_table_max_data_extension_time_in_days'),
    'iceberg_table_change_tracking': config.get('iceberg_table_change_tracking', false),
    'iceberg_table_copy_grants': config.get('iceberg_table_copy_grants', false),
    'iceberg_table_error_logging': config.get('iceberg_table_error_logging', true),
    'iceberg_table_iceberg_version': config.get('iceberg_table_iceberg_version', 3),
    'iceberg_table_enable_iceberg_merge_on_read': config.get('iceberg_table_enable_iceberg_merge_on_read', true),
    'iceberg_table_enable_data_compaction': config.get('iceberg_table_enable_data_compaction', true),
    'partition_by': config.get('partition_by', []),
    'cluster_by': config.get('cluster_by', []),
    'gcp_sa_secret_fqdn': config.get('gcp_sa_secret_fqdn'),
    'gcp_sa_secret_alias': config.get('gcp_sa_secret_alias'),
    'gcp_auth_method': config.get('gcp_auth_method'),
    'gcp_wif_secret_fqdn': config.get('gcp_wif_secret_fqdn'),
    'gcp_wif_audience': config.get('gcp_wif_audience'),
    'gcp_service_account_impersonation': config.get('gcp_service_account_impersonation'),
    'google_application_credentials': config.get('google_application_credentials'),
    'google_credentials': config.get('google_credentials'),
    'gcp_service_account_json': config.get('gcp_service_account_json'),
    'service_account_json': config.get('service_account_json'),
    'private_key': config.get('private_key'),
    'password': config.get('password'),
    'secret': config.get('secret'),
    'target_relation': target,
    'internal_relation': internal,
    'model_sql': model_sql,
    'model_unique_id': model.get('unique_id'),
    'invocation_id': invocation_id,
    'dbt_full_refresh': flags.FULL_REFRESH,
    'deployment': {
      'procedure_database': deployment.get('procedure_database'),
      'procedure_schema': deployment.get('procedure_schema'),
      'procedure_name': deployment.get('procedure_name'),
      'run_log_table': deployment.get('run_log_table', 'ICEBERG_SYNC_RUN_LOG'),
      'run_log_enabled': deployment.get('run_log_enabled', true),
      'gcp_auth_method': dbt_snowflake_iceberg_sync.iceberg_sync_deployment_var(deployment, 'gcp_auth_method', 'service_account_key'),
      'gcp_sa_secret_alias': deployment.get('gcp_sa_secret_alias', 'gcp_sa_credentials_json'),
      'gcp_wif_secret_fqdn': dbt_snowflake_iceberg_sync.iceberg_sync_deployment_var(deployment, 'gcp_wif_secret_fqdn'),
      'gcp_wif_audience': dbt_snowflake_iceberg_sync.iceberg_sync_deployment_var(deployment, 'gcp_wif_audience'),
      'gcp_service_account_impersonation': dbt_snowflake_iceberg_sync.iceberg_sync_deployment_var(deployment, 'gcp_service_account_impersonation')
    }
  } %}

  {% if values.get('iceberg_table_base_location') is none %}
    {% do values.update({
      'iceberg_table_base_location': target_relation.database ~ '/' ~ target_relation.schema ~ '/' ~ target_relation.identifier
    }) %}
  {% endif %}

  {{ return(values) }}
{% endmacro %}
