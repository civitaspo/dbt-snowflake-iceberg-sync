{% macro iceberg_sync_required_var(vars_dict, key) -%}
  {%- if vars_dict.get(key, none) is none or vars_dict.get(key) == "" -%}
    {%- do dbt_snowflake_iceberg_sync.iceberg_sync_raise(
      "vars.iceberg_sync." ~ key ~ " is required"
    ) -%}
  {%- endif -%}
  {{ return(vars_dict.get(key)) }}
{%- endmacro %}

{% macro iceberg_sync_defaulted_var(vars_dict, key, default) -%}
  {%- if vars_dict.get(key, none) is none or vars_dict.get(key) == "" -%}
    {{ return(default) }}
  {%- endif -%}
  {{ return(vars_dict.get(key)) }}
{%- endmacro %}

{% macro iceberg_sync_deployment_config() -%}
  {%- set vars_dict = var('iceberg_sync', {}) -%}
  {%- set procedure_database = dbt_snowflake_iceberg_sync.iceberg_sync_normalize_object_identifier(
    dbt_snowflake_iceberg_sync.iceberg_sync_defaulted_var(
      vars_dict,
      'procedure_database',
      target.database
    )
  ) -%}
  {%- set procedure_schema = dbt_snowflake_iceberg_sync.iceberg_sync_normalize_object_identifier(
    dbt_snowflake_iceberg_sync.iceberg_sync_defaulted_var(
      vars_dict,
      'procedure_schema',
      target.schema
    )
  ) -%}
  {%- set procedure_name = dbt_snowflake_iceberg_sync.iceberg_sync_normalize_object_identifier(
    dbt_snowflake_iceberg_sync.iceberg_sync_defaulted_var(
      vars_dict,
      'procedure_name',
      'ICEBERG_SYNC'
    )
  ) -%}
  {%- set procedure_relation = {
    'database': procedure_database,
    'schema': procedure_schema,
    'identifier': procedure_name
  } -%}
  {%- set handler_stage_default = (
    procedure_database ~ '.' ~ procedure_schema ~ '.ICEBERG_SYNC_HANDLER_STAGE'
  ) -%}
  {%- set handler_stage = dbt_snowflake_iceberg_sync.iceberg_sync_object_fqn(
    dbt_snowflake_iceberg_sync.iceberg_sync_defaulted_var(
      vars_dict,
      'handler_stage',
      handler_stage_default
    ),
    'vars.iceberg_sync.handler_stage'
  ) -%}
  {%- set handler_stage_path = dbt_snowflake_iceberg_sync.iceberg_sync_defaulted_var(
    vars_dict,
    'handler_stage_path',
    'procedure'
  ) -%}
  {%- set handler_import_name = dbt_snowflake_iceberg_sync.iceberg_sync_defaulted_var(
    vars_dict,
    'handler_import_name',
    'iceberg_sync_procedure'
  ) -%}
  {%- set handler_name = dbt_snowflake_iceberg_sync.iceberg_sync_defaulted_var(
    vars_dict,
    'handler_name',
    handler_import_name ~ '.handler.main'
  ) -%}
  {%- set handler_local_path = dbt_snowflake_iceberg_sync.iceberg_sync_required_var(
    vars_dict,
    'handler_local_path'
  ) -%}
  {%- set google_cloud_service_account_secret_fqdn = (
    dbt_snowflake_iceberg_sync.iceberg_sync_object_fqn(
      dbt_snowflake_iceberg_sync.iceberg_sync_required_var(
        vars_dict,
        'google_cloud_service_account_secret_fqdn'
      ),
      'vars.iceberg_sync.google_cloud_service_account_secret_fqdn',
      3,
      3
    )
  ) -%}
  {%- set google_cloud_service_account_secret_alias = vars_dict.get(
    'google_cloud_service_account_secret_alias',
    'google_cloud_service_account_credentials_json'
  ) -%}
  {%- set external_access_integrations = [] -%}
  {%- for integration in dbt_snowflake_iceberg_sync.iceberg_sync_as_list(
    vars_dict.get('external_access_integrations', [])
  ) -%}
    {%- do external_access_integrations.append(
      dbt_snowflake_iceberg_sync.iceberg_sync_quote_object_identifier(integration)
    ) -%}
  {%- endfor -%}
  {%- if vars_dict.get('run_log_table', none) is not none -%}
    {%- set run_log_table = dbt_snowflake_iceberg_sync.iceberg_sync_relation_from_fqn(
      vars_dict.get('run_log_table'), 'vars.iceberg_sync.run_log_table'
    ) -%}
  {%- else -%}
    {%- set run_log_table = {
      'database': procedure_database,
      'schema': procedure_schema,
      'identifier': 'ICEBERG_SYNC_RUN_LOG'
    } -%}
  {%- endif -%}
  {{ return({
    'procedure_database': procedure_database,
    'procedure_schema': procedure_schema,
    'procedure_name': procedure_name,
    'procedure_relation': procedure_relation,
    'handler_stage': handler_stage,
    'handler_stage_path': handler_stage_path,
    'handler_import_name': handler_import_name,
    'handler_name': handler_name,
    'handler_local_path': handler_local_path,
    'external_access_integrations': external_access_integrations,
    'run_log_table': run_log_table,
    'google_cloud_service_account_secret_fqdn': google_cloud_service_account_secret_fqdn,
    'google_cloud_service_account_secret_alias': google_cloud_service_account_secret_alias
  }) }}
{%- endmacro %}

{% macro iceberg_sync_procedure_fqn() -%}
  {%- set deployment = dbt_snowflake_iceberg_sync.iceberg_sync_deployment_config() -%}
  {%- set procedure = deployment['procedure_relation'] -%}
  {{ return(
    dbt_snowflake_iceberg_sync.iceberg_sync_quote_object_identifier(
      procedure['database']
    ) ~ '.' ~
    dbt_snowflake_iceberg_sync.iceberg_sync_quote_object_identifier(
      procedure['schema']
    ) ~ '.' ~
    dbt_snowflake_iceberg_sync.iceberg_sync_quote_object_identifier(
      procedure['identifier']
    )
  ) }}
{%- endmacro %}

{% macro iceberg_sync_collect_config(model_sql, target_relation, model_node, dbt_full_refresh=False) -%}
  {%- do dbt_snowflake_iceberg_sync.iceberg_sync_validate_forbidden_model_configs() -%}

  {%- set partition_by = dbt_snowflake_iceberg_sync.iceberg_sync_as_list(config.get('partition_by', [])) -%}
  {%- set cluster_by = dbt_snowflake_iceberg_sync.iceberg_sync_as_list(config.get('cluster_by', [])) -%}
  {%- set deployment = dbt_snowflake_iceberg_sync.iceberg_sync_deployment_config() -%}
  {%- set target_payload = dbt_snowflake_iceberg_sync.iceberg_sync_relation_payload(target_relation) -%}
  {%- set internal_payload = {
    'database': target_payload['database'],
    'schema': target_payload['schema'],
    'identifier': dbt_snowflake_iceberg_sync.iceberg_sync_internal_identifier(target_relation)
  } -%}
  {%- set model_config = {} -%}
  {%- if model_node.config is defined and model_node.config.extra is defined -%}
    {%- set model_config = model_node.config.extra -%}
  {%- endif -%}

  {%- set payload = {
    'source_type': config.get('source_type', none) or 'bigquery',
    'materialization_strategy': config.get('materialization_strategy', none) or 'incremental',
    'incremental_strategy': config.get('incremental_strategy', none) or 'delete+copy',
    'incremental_predicate': config.get('incremental_predicate', none),
    'dbt_full_refresh': dbt_full_refresh,
    'partition_by': partition_by,
    'cluster_by': cluster_by,
    'target_relation': target_payload,
    'internal_relation': internal_payload,
    'model': {
      'unique_id': model_node.unique_id,
      'name': model_node.name,
      'sql': model_sql,
      'invocation_id': invocation_id
    },
    'model_config': model_config,
    'deployment': deployment,
    'bigquery': {
      'export_strategy': config.get('bigquery_export_strategy', none) or 'extract',
      'project_id': dbt_snowflake_iceberg_sync.iceberg_sync_required_model_config('google_cloud_project_id'),
      'dataset_id': dbt_snowflake_iceberg_sync.iceberg_sync_required_model_config('bigquery_dataset_id'),
      'table_id': dbt_snowflake_iceberg_sync.iceberg_sync_required_model_config('bigquery_table_id'),
      'location': dbt_snowflake_iceberg_sync.iceberg_sync_required_model_config('bigquery_location'),
      'export_location': dbt_snowflake_iceberg_sync.iceberg_sync_required_model_config('bigquery_export_location'),
      'export_predicate_type': config.get('bigquery_export_predicate_type', none) or 'auto',
      'full_refresh_predicates': dbt_snowflake_iceberg_sync.iceberg_sync_as_list(config.get('bigquery_export_full_refresh_predicates', [])),
      'incremental_predicates': dbt_snowflake_iceberg_sync.iceberg_sync_as_list(config.get('bigquery_export_incremental_predicates', [])),
      'staging_dataset_id': config.get('bigquery_staging_dataset_id', none),
      'staging_table_expiration_hours': config.get('bigquery_staging_table_expiration_hours', 24),
      'staging_table_reuse': config.get('bigquery_staging_table_reuse', true),
      'force_rebuild_staging_table': config.get('force_rebuild_staging_table', false)
    },
    'iceberg_table': {
      'external_volume': dbt_snowflake_iceberg_sync.iceberg_sync_required_model_config('iceberg_table_external_volume'),
      'base_location': config.get('iceberg_table_base_location', none),
      'target_file_size': config.get('iceberg_table_target_file_size', none) or 'AUTO',
      'storage_serialization_policy': config.get('iceberg_table_storage_serialization_policy', none) or 'COMPATIBLE',
      'data_retention_time_in_days': config.get('iceberg_table_data_retention_time_in_days', 7),
      'max_data_extension_time_in_days': config.get('iceberg_table_max_data_extension_time_in_days', none),
      'change_tracking': config.get('iceberg_table_change_tracking', true),
      'copy_grants': config.get('iceberg_table_copy_grants', false),
      'error_logging': config.get('iceberg_table_error_logging', false),
      'iceberg_version': config.get('iceberg_table_iceberg_version', 3),
      'enable_iceberg_merge_on_read': config.get('iceberg_table_enable_iceberg_merge_on_read', true),
      'enable_data_compaction': config.get('iceberg_table_enable_data_compaction', true)
    }
  } -%}
  {%- do dbt_snowflake_iceberg_sync.iceberg_sync_validate_payload(payload) -%}
  {{ return(payload) }}
{%- endmacro %}
