{% materialization iceberg_sync, adapter='snowflake' %}
  {%- set payload = dbt_snowflake_iceberg_sync.iceberg_sync_collect_config(
    sql, this, model, flags.FULL_REFRESH
  ) -%}
  {%- set target_relation = dbt_snowflake_iceberg_sync.iceberg_sync_relation_from_payload(
    payload['target_relation'], 'view'
  ) -%}
  {%- set internal_relation = dbt_snowflake_iceberg_sync.iceberg_sync_relation_from_payload(
    payload['internal_relation']
  ) -%}

  {%- if execute -%}
    {%- set existing_internal_relation = adapter.get_relation(
      database=internal_relation.database,
      schema=internal_relation.schema,
      identifier=internal_relation.identifier
    ) -%}
    {%- set existing_target_relation = adapter.get_relation(
      database=target_relation.database,
      schema=target_relation.schema,
      identifier=target_relation.identifier
    ) -%}
    {%- set internal_table_exists = existing_internal_relation is not none -%}
    {%- set target_view_exists = (
      existing_target_relation is not none
      and (existing_target_relation.type | upper) == 'VIEW'
    ) -%}
    {%- set effective_mode = dbt_snowflake_iceberg_sync.iceberg_sync_effective_mode(
      payload, internal_table_exists, target_view_exists
    ) -%}
    {%- set predicates = dbt_snowflake_iceberg_sync.iceberg_sync_predicates_for_mode(
      payload, effective_mode
    ) -%}
    {%- set run_id = dbt_snowflake_iceberg_sync.iceberg_sync_run_id(payload) -%}
    {%- if payload['source_type'] == 's3_parquet' -%}
      {%- set resolved = dbt_snowflake_iceberg_sync.iceberg_sync_resolve_s3_parquet_locations(
        payload, effective_mode
      ) -%}
      {%- set destination_uri = resolved['destination_uri'] -%}
      {%- set load_locations = resolved['locations'] -%}
    {%- else -%}
      {%- set stage = dbt_snowflake_iceberg_sync.iceberg_sync_resolve_stage_location(
        payload['bigquery']['export_location'], run_id
      ) -%}
      {%- set destination_uri = stage['remote_run_uri'] -%}
      {%- set load_locations = [{
        'stage_location': stage['run_stage_location'],
        'pattern': none,
        'force': false
      }] -%}
    {%- endif -%}
    {%- set export_result = dbt_snowflake_iceberg_sync.iceberg_sync_wait_for_export(
      payload, effective_mode, destination_uri
    ) -%}
    {%- set cleanup = {
      'created_internal_table': false,
      'altered_internal_table_schema': false,
      'dropped_created_internal_table': false,
      'cleanup_error_message': none
    } -%}
    {%- if export_result.get('skipped') -%}
      {%- call statement('main', auto_begin=False) -%}
        SELECT 1
      {%- endcall -%}
      {%- do dbt_snowflake_iceberg_sync.iceberg_sync_write_skipped_log(
        payload,
        run_id,
        effective_mode,
        predicates,
        export_result,
        {},
        cleanup
      ) -%}
    {%- else -%}
      {%- set desired_columns = export_result['columns'] -%}
      {%- if export_result.get('load_locations') -%}
        {%- set load_locations = export_result['load_locations'] -%}
      {%- endif -%}

      {%- call statement('iceberg_sync_create_internal_table', auto_begin=False) -%}
        {{ dbt_snowflake_iceberg_sync.iceberg_sync_create_iceberg_table_sql(
          payload, desired_columns
        ) }}
      {%- endcall -%}

      {%- set before_add_columns = dbt_snowflake_iceberg_sync.iceberg_sync_describe_table_columns(
        internal_relation
      ) -%}
      {%- do dbt_snowflake_iceberg_sync.iceberg_sync_validate_or_add_columns(
        internal_relation, desired_columns
      ) -%}
      {%- if desired_columns | length > before_add_columns | length -%}
        {%- do cleanup.update({'altered_internal_table_schema': true}) -%}
      {%- endif -%}

      {%- set load_result = dbt_snowflake_iceberg_sync.iceberg_sync_run_load(
        payload, effective_mode, load_locations, desired_columns
      ) -%}
      {%- if load_result.get('status') != 'success' -%}
        {%- do dbt_snowflake_iceberg_sync.iceberg_sync_raise(
          load_result.get('error_message', 'Iceberg load failed')
        ) -%}
      {%- endif -%}

      {%- do dbt_snowflake_iceberg_sync.iceberg_sync_create_view(
        target_relation, internal_relation, export_result['view_columns']
      ) -%}

      {%- do dbt_snowflake_iceberg_sync.iceberg_sync_write_success_log(
        payload,
        run_id,
        effective_mode,
        predicates,
        export_result,
        load_result.get('retry', {}),
        cleanup
      ) -%}
    {%- endif -%}
  {%- endif -%}

  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
