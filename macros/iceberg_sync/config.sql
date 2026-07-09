{% macro iceberg_sync_required_var(vars_dict, key) -%}
  {%- if vars_dict.get(key, none) is none or vars_dict.get(key) == "" -%}
    {%- do dbt_snowflake_iceberg_sync.iceberg_sync_raise(
      "vars.iceberg_sync." ~ key ~ " is required"
    ) -%}
  {%- endif -%}
  {{ return(vars_dict.get(key)) }}
{%- endmacro %}

{% macro iceberg_sync_absolute_local_path(path) -%}
  {#- Absolute-ize relative handler paths for Snowflake PUT.
      dbt Fusion (ADBC) does not resolve relative file:// paths against CWD. -#}
  {%- set local_path = path | string -%}
  {%- if local_path.startswith('/') -%}
    {{ return(local_path) }}
  {%- endif -%}
  {%- if modules is defined and modules.os is defined -%}
    {{ return(modules.os.path.abspath(local_path)) }}
  {%- endif -%}
  {%- set project_dir = env_var('DBT_PROJECT_DIR', '') | string -%}
  {%- if project_dir != '' -%}
    {{ return(project_dir.rstrip('/') ~ '/' ~ local_path.lstrip('/')) }}
  {%- endif -%}
  {%- do exceptions.warn(
    "iceberg_sync: relative handler_local_path='" ~ local_path ~ "' could not be "
    ~ "resolved to an absolute path. Export DBT_PROJECT_DIR or pass an absolute "
    ~ "path so Snowflake PUT works under dbt Fusion."
  ) -%}
  {{ return(local_path) }}
{%- endmacro %}

{% macro iceberg_sync_defaulted_var(vars_dict, key, default) -%}
  {%- if vars_dict.get(key, none) is none or vars_dict.get(key) == "" -%}
    {{ return(default) }}
  {%- endif -%}
  {{ return(vars_dict.get(key)) }}
{%- endmacro %}

{% macro iceberg_sync_deployment_var(vars_dict, key, default=none) -%}
  {%- set override = var('iceberg_sync_' ~ key, none) -%}
  {%- if override is not none and override != "" -%}
    {{ return(override) }}
  {%- endif -%}
  {{ return(dbt_snowflake_iceberg_sync.iceberg_sync_defaulted_var(
    vars_dict,
    key,
    default
  )) }}
{%- endmacro %}

{% macro iceberg_sync_workload_identity_federation_config_hint(vars_dict, key) -%}
  {%- set by_dbt_target = vars_dict.get('google_cloud_workload_identity_federation_by_dbt_target', {}) -%}
  {%- set map_keys = [] -%}
  {%- if by_dbt_target is mapping -%}
    {%- for map_key in by_dbt_target.keys() | list | sort -%}
      {%- do map_keys.append("'" ~ map_key ~ "'") -%}
    {%- endfor -%}
  {%- endif -%}
  {%- set map_keys_text = map_keys | join(', ') if map_keys | length > 0 else '(none)' -%}
  {%- set has_default = by_dbt_target is mapping and by_dbt_target.get('default', none) is not none -%}
  {{ return(
    "Configure vars.iceberg_sync." ~ key
    ~ " (or top-level var iceberg_sync_" ~ key
    ~ "), vars.iceberg_sync.google_cloud_workload_identity_federation_by_dbt_target['"
    ~ target.name ~ "']"
    ~ (" or ['default']" if has_default else " (no 'default' entry)")
    ~ ". Available by_dbt_target keys: "
    ~ map_keys_text
    ~ " when google_cloud_auth_method='workload_identity_federation'"
  ) }}
{%- endmacro %}

{% macro iceberg_sync_workload_identity_federation_by_dbt_target_entry_var(entry_settings, entry_label, key) -%}
  {%- if entry_settings is none -%}
    {{ return(none) }}
  {%- endif -%}
  {%- if entry_settings is not mapping -%}
    {%- do dbt_snowflake_iceberg_sync.iceberg_sync_raise(
      "vars.iceberg_sync.google_cloud_workload_identity_federation_by_dbt_target['"
      ~ entry_label
      ~ "'] must be a mapping"
    ) -%}
  {%- endif -%}
  {%- set entry_value = entry_settings.get(key, none) -%}
  {%- if entry_value is not none and entry_value != "" -%}
    {{ return(entry_value) }}
  {%- endif -%}
  {{ return(none) }}
{%- endmacro %}

{% macro iceberg_sync_workload_identity_federation_deployment_var(vars_dict, key, default=none) -%}
  {%- set override = var('iceberg_sync_' ~ key, none) -%}
  {%- if override is not none and override != "" -%}
    {{ return(override) }}
  {%- endif -%}
  {%- set by_dbt_target = vars_dict.get('google_cloud_workload_identity_federation_by_dbt_target', none) -%}
  {%- if by_dbt_target is not none and by_dbt_target is not mapping -%}
    {%- do dbt_snowflake_iceberg_sync.iceberg_sync_raise(
      "vars.iceberg_sync.google_cloud_workload_identity_federation_by_dbt_target must be a mapping"
    ) -%}
  {%- endif -%}
  {%- if by_dbt_target is mapping -%}
    {%- for entry_label, entry_settings in [
      (target.name, by_dbt_target.get(target.name, none)),
      ('default', by_dbt_target.get('default', none))
    ] -%}
      {%- set entry_value = dbt_snowflake_iceberg_sync.iceberg_sync_workload_identity_federation_by_dbt_target_entry_var(
        entry_settings,
        entry_label,
        key
      ) -%}
      {%- if entry_value is not none -%}
        {{ return(entry_value) }}
      {%- endif -%}
    {%- endfor -%}
  {%- endif -%}
  {{ return(dbt_snowflake_iceberg_sync.iceberg_sync_defaulted_var(
    vars_dict,
    key,
    default
  )) }}
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
  {%- set handler_local_path = dbt_snowflake_iceberg_sync.iceberg_sync_absolute_local_path(
    dbt_snowflake_iceberg_sync.iceberg_sync_required_var(
      vars_dict,
      'handler_local_path'
    )
  ) -%}
  {%- set google_cloud_auth_method = dbt_snowflake_iceberg_sync.iceberg_sync_deployment_var(
    vars_dict,
    'google_cloud_auth_method',
    'service_account_credentials_json'
  ) -%}
  {%- if google_cloud_auth_method not in ['service_account_credentials_json', 'workload_identity_federation'] -%}
    {%- do dbt_snowflake_iceberg_sync.iceberg_sync_raise(
      "vars.iceberg_sync.google_cloud_auth_method (or top-level var iceberg_sync_google_cloud_auth_method) must be "
      ~ "'service_account_credentials_json' or 'workload_identity_federation'"
    ) -%}
  {%- endif -%}
  {%- set google_cloud_service_account_secret_fqdn = none -%}
  {%- set google_cloud_service_account_secret_alias = vars_dict.get(
    'google_cloud_service_account_secret_alias',
    'google_cloud_service_account_credentials_json'
  ) -%}
  {%- set google_cloud_workload_identity_federation_secret_fqdn = none -%}
  {%- set google_cloud_workload_identity_federation_audience = dbt_snowflake_iceberg_sync.iceberg_sync_workload_identity_federation_deployment_var(
    vars_dict,
    'google_cloud_workload_identity_federation_audience',
    none
  ) -%}
  {%- set google_cloud_service_account_impersonation = dbt_snowflake_iceberg_sync.iceberg_sync_workload_identity_federation_deployment_var(
    vars_dict,
    'google_cloud_service_account_impersonation',
    none
  ) -%}
  {%- if google_cloud_auth_method == 'service_account_credentials_json' -%}
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
  {%- else -%}
    {%- set raw_google_cloud_workload_identity_federation_secret_fqdn = dbt_snowflake_iceberg_sync.iceberg_sync_workload_identity_federation_deployment_var(
      vars_dict,
      'google_cloud_workload_identity_federation_secret_fqdn',
      none
    ) -%}
    {%- if raw_google_cloud_workload_identity_federation_secret_fqdn is none or raw_google_cloud_workload_identity_federation_secret_fqdn == "" -%}
      {%- do dbt_snowflake_iceberg_sync.iceberg_sync_raise(
        dbt_snowflake_iceberg_sync.iceberg_sync_workload_identity_federation_config_hint(
          vars_dict,
          'google_cloud_workload_identity_federation_secret_fqdn'
        )
      ) -%}
    {%- endif -%}
    {%- if google_cloud_workload_identity_federation_audience is none or google_cloud_workload_identity_federation_audience == "" -%}
      {%- do dbt_snowflake_iceberg_sync.iceberg_sync_raise(
        dbt_snowflake_iceberg_sync.iceberg_sync_workload_identity_federation_config_hint(
          vars_dict,
          'google_cloud_workload_identity_federation_audience'
        )
      ) -%}
    {%- endif -%}
    {%- set workload_identity_federation_secret_relation = dbt_snowflake_iceberg_sync.iceberg_sync_relation_from_fqn(
      raw_google_cloud_workload_identity_federation_secret_fqdn,
      'google_cloud_workload_identity_federation_secret_fqdn'
    ) -%}
    {%- set google_cloud_workload_identity_federation_secret_fqdn = (
      workload_identity_federation_secret_relation['database']
      ~ '.'
      ~ workload_identity_federation_secret_relation['schema']
      ~ '.'
      ~ workload_identity_federation_secret_relation['identifier']
    ) -%}
  {%- endif -%}
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
    'google_cloud_service_account_secret_alias': google_cloud_service_account_secret_alias,
    'google_cloud_auth_method': google_cloud_auth_method,
    'google_cloud_workload_identity_federation_secret_fqdn': google_cloud_workload_identity_federation_secret_fqdn,
    'google_cloud_workload_identity_federation_audience': google_cloud_workload_identity_federation_audience,
    'google_cloud_service_account_impersonation': google_cloud_service_account_impersonation
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
  {%- do dbt_snowflake_iceberg_sync.iceberg_sync_validate_forbidden_model_configs(model_node) -%}

  {%- set partition_by = dbt_snowflake_iceberg_sync.iceberg_sync_as_list(
    dbt_snowflake_iceberg_sync.iceberg_sync_model_config(model_node, 'partition_by', [])
  ) -%}
  {%- set cluster_by = dbt_snowflake_iceberg_sync.iceberg_sync_as_list(
    dbt_snowflake_iceberg_sync.iceberg_sync_model_config(model_node, 'cluster_by', [])
  ) -%}
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
  {%- set source_type = dbt_snowflake_iceberg_sync.iceberg_sync_model_config(model_node, 'source_type', none) -%}
  {%- set materialization_strategy = dbt_snowflake_iceberg_sync.iceberg_sync_model_config(model_node, 'materialization_strategy', none) -%}
  {%- set incremental_strategy = dbt_snowflake_iceberg_sync.iceberg_sync_model_config(model_node, 'incremental_strategy', none) -%}
  {%- set incremental_predicate = dbt_snowflake_iceberg_sync.iceberg_sync_model_config(model_node, 'incremental_predicate', none) -%}

  {%- set payload = {
    'source_type': source_type or 'bigquery',
    'materialization_strategy': materialization_strategy or 'incremental',
    'incremental_strategy': incremental_strategy or 'delete+copy',
    'incremental_predicate': incremental_predicate,
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
    'retry': {
      'max_attempts': dbt_snowflake_iceberg_sync.iceberg_sync_number_model_config(model_node, 'iceberg_sync_retry_max_attempts', 3, true),
      'initial_delay_seconds': dbt_snowflake_iceberg_sync.iceberg_sync_number_model_config(model_node, 'iceberg_sync_retry_initial_delay_seconds', 5),
      'max_delay_seconds': dbt_snowflake_iceberg_sync.iceberg_sync_number_model_config(model_node, 'iceberg_sync_retry_max_delay_seconds', 60),
      'backoff_multiplier': dbt_snowflake_iceberg_sync.iceberg_sync_number_model_config(model_node, 'iceberg_sync_retry_backoff_multiplier', 2.0),
      'jitter_seconds': dbt_snowflake_iceberg_sync.iceberg_sync_number_model_config(model_node, 'iceberg_sync_retry_jitter_seconds', 3)
    },
    'cleanup': {
      'created_table_on_failure': dbt_snowflake_iceberg_sync.iceberg_sync_model_config(model_node, 'iceberg_sync_cleanup_created_table_on_failure', true)
    },
    'run_log': {
      'fail_on_error': dbt_snowflake_iceberg_sync.iceberg_sync_model_config(model_node, 'iceberg_sync_run_log_fail_on_error', false)
    },
    'bigquery': {
      'export_strategy': dbt_snowflake_iceberg_sync.iceberg_sync_model_config(model_node, 'bigquery_export_strategy', none) or 'extract',
      'project_id': dbt_snowflake_iceberg_sync.iceberg_sync_required_model_config(model_node, 'google_cloud_project_id'),
      'dataset_id': dbt_snowflake_iceberg_sync.iceberg_sync_required_model_config(model_node, 'bigquery_dataset_id'),
      'table_id': dbt_snowflake_iceberg_sync.iceberg_sync_required_model_config(model_node, 'bigquery_table_id'),
      'location': dbt_snowflake_iceberg_sync.iceberg_sync_required_model_config(model_node, 'bigquery_location'),
      'export_location': dbt_snowflake_iceberg_sync.iceberg_sync_required_model_config(model_node, 'bigquery_export_location'),
      'export_compression': (dbt_snowflake_iceberg_sync.iceberg_sync_model_config(model_node, 'bigquery_export_compression', none) or 'ZSTD') | upper,
      'export_predicate_type': dbt_snowflake_iceberg_sync.iceberg_sync_model_config(model_node, 'bigquery_export_predicate_type', none) or 'auto',
      'full_refresh_predicates': dbt_snowflake_iceberg_sync.iceberg_sync_as_list(dbt_snowflake_iceberg_sync.iceberg_sync_model_config(model_node, 'bigquery_export_full_refresh_predicates', [])),
      'incremental_predicates': dbt_snowflake_iceberg_sync.iceberg_sync_as_list(dbt_snowflake_iceberg_sync.iceberg_sync_model_config(model_node, 'bigquery_export_incremental_predicates', [])),
      'staging_dataset_id': dbt_snowflake_iceberg_sync.iceberg_sync_model_config(model_node, 'bigquery_staging_dataset_id', none),
      'staging_table_expiration_hours': dbt_snowflake_iceberg_sync.iceberg_sync_model_config(model_node, 'bigquery_staging_table_expiration_hours', 24),
      'staging_table_reuse': dbt_snowflake_iceberg_sync.iceberg_sync_model_config(model_node, 'bigquery_staging_table_reuse', true),
      'force_rebuild_staging_table': dbt_snowflake_iceberg_sync.iceberg_sync_model_config(model_node, 'force_rebuild_staging_table', false),
      'skip_missing_tables': dbt_snowflake_iceberg_sync.iceberg_sync_model_config(model_node, 'bigquery_extract_skip_missing_tables', false),
      'export_poll_interval_seconds': dbt_snowflake_iceberg_sync.iceberg_sync_number_model_config(model_node, 'bigquery_export_poll_interval_seconds', 30),
      'export_poll_timeout_seconds': dbt_snowflake_iceberg_sync.iceberg_sync_number_model_config(model_node, 'bigquery_export_poll_timeout_seconds', 3600)
    },
    'iceberg_table': {
      'external_volume': dbt_snowflake_iceberg_sync.iceberg_sync_required_model_config(model_node, 'iceberg_table_external_volume'),
      'base_location': dbt_snowflake_iceberg_sync.iceberg_sync_model_config(model_node, 'iceberg_table_base_location', none),
      'target_file_size': dbt_snowflake_iceberg_sync.iceberg_sync_model_config(model_node, 'iceberg_table_target_file_size', none) or 'AUTO',
      'storage_serialization_policy': dbt_snowflake_iceberg_sync.iceberg_sync_model_config(model_node, 'iceberg_table_storage_serialization_policy', none) or 'COMPATIBLE',
      'data_retention_time_in_days': dbt_snowflake_iceberg_sync.iceberg_sync_model_config(model_node, 'iceberg_table_data_retention_time_in_days', 7),
      'max_data_extension_time_in_days': dbt_snowflake_iceberg_sync.iceberg_sync_model_config(model_node, 'iceberg_table_max_data_extension_time_in_days', none),
      'change_tracking': dbt_snowflake_iceberg_sync.iceberg_sync_model_config(model_node, 'iceberg_table_change_tracking', true),
      'copy_grants': dbt_snowflake_iceberg_sync.iceberg_sync_model_config(model_node, 'iceberg_table_copy_grants', false),
      'error_logging': dbt_snowflake_iceberg_sync.iceberg_sync_model_config(model_node, 'iceberg_table_error_logging', false),
      'iceberg_version': dbt_snowflake_iceberg_sync.iceberg_sync_model_config(model_node, 'iceberg_table_iceberg_version', 3),
      'enable_iceberg_merge_on_read': dbt_snowflake_iceberg_sync.iceberg_sync_model_config(model_node, 'iceberg_table_enable_iceberg_merge_on_read', true),
      'enable_data_compaction': dbt_snowflake_iceberg_sync.iceberg_sync_model_config(model_node, 'iceberg_table_enable_data_compaction', true)
    }
  } -%}
  {%- do dbt_snowflake_iceberg_sync.iceberg_sync_validate_payload(payload) -%}
  {{ return(payload) }}
{%- endmacro %}
