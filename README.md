# dbt-snowflake-iceberg-sync

`dbt-snowflake-iceberg-sync` is a dbt package that syncs data exported from an
external source into a Snowflake-managed Iceberg table and exposes the dbt model
as a Snowflake view.

The first supported source type is BigQuery. The dbt materialization is
Snowflake-only and is named `iceberg_sync`.

## Supported Versions

- dbt Core: `>=1.10.0,<2.0.0`
- dbt Fusion Engine: `>=2.0.0,<3.0.0`
- dbt adapter: `dbt-snowflake`
- Snowflake Python procedure runtime: Python `3.12`
- Local package tests: Python `>=3.11`

## Installation

Add the package to `packages.yml`:

```yaml
packages:
  - git: https://github.com/civitaspo/dbt-snowflake-iceberg-sync.git
    revision: v0.2.0
```

Then run:

```bash
dbt deps
```

## Required Snowflake Setup

You must create and manage the Snowflake resources that grant access to GCS and
BigQuery:

- A Snowflake-managed Iceberg external volume.
- A GCS-backed Snowflake stage used as the BigQuery export destination.
- A network rule and external access integration for BigQuery API calls.
- A Snowflake secret containing the GCP service account JSON.
- A database/schema where the package procedure can be installed.

Example deployment vars:

```yaml
vars:
  iceberg_sync:
    handler_local_path: /absolute/path/to/dbt_packages/dbt_snowflake_iceberg_sync/procedure

    external_access_integrations: [BIGQUERY_API]
    google_cloud_service_account_secret_fqdn: ANALYTICS.SECRETS.GOOGLE_CLOUD_SERVICE_ACCOUNT_JSON
```

`procedure_database` defaults to the active dbt `target.database`, and
`procedure_schema` defaults to the active dbt `target.schema`.
`procedure_name` defaults to `ICEBERG_SYNC`, and `handler_stage` defaults to
`<procedure_database>.<procedure_schema>.ICEBERG_SYNC_HANDLER_STAGE`. Override
these only when the package helper objects should live outside the active target
database/schema. This lets clone and CI targets install helper objects in their
own target database and schema without requiring privileges on a production
database/schema.

The package creates a run log table named
`<procedure_database>.<procedure_schema>.ICEBERG_SYNC_RUN_LOG` by default. Set
`vars.iceberg_sync.run_log_table` to a three-part relation name to override it.

Deployment vars:

| Var | Required | Default | Description |
| --- | --- | --- | --- |
| `handler_local_path` | Yes | None | Local path to the package `procedure/` directory uploaded by the installer. Use an absolute path with dbt Fusion. |
| `google_cloud_service_account_secret_fqdn` | Yes | None | Fully qualified Snowflake secret containing the GCP service account JSON. |
| `procedure_database` | No | `target.database` | Database where the package procedure and default helper objects are installed. |
| `procedure_schema` | No | `target.schema` | Schema where the package procedure and default helper objects are installed. |
| `procedure_name` | No | `ICEBERG_SYNC` | Name of the Snowflake stored procedure. |
| `handler_stage` | No | `<procedure_database>.<procedure_schema>.ICEBERG_SYNC_HANDLER_STAGE` | Internal Snowflake stage used for the Python handler files. |
| `handler_stage_path` | No | `procedure` | Directory prefix inside `handler_stage` for uploaded handler files. |
| `handler_import_name` | No | `iceberg_sync_procedure` | Import directory name mounted into the Snowflake Python runtime. |
| `handler_name` | No | `<handler_import_name>.handler.main` | Python procedure entry point. |
| `external_access_integrations` | No | `[]` | External access integrations granted to the procedure. |
| `google_cloud_service_account_secret_alias` | No | `google_cloud_service_account_credentials_json` | Secret alias read by the Python handler. |
| `run_log_table` | No | `<procedure_database>.<procedure_schema>.ICEBERG_SYNC_RUN_LOG` | Three-part relation used for procedure run logs. |

## Required GCP IAM Setup

The GCP service account stored in the Snowflake secret needs permissions to:

- Read BigQuery table metadata.
- Run BigQuery query jobs when `bigquery_export_strategy='select'`.
- Run BigQuery extract jobs.
- Write Parquet exports to the GCS bucket behind `bigquery_export_location`.
- Read or create staging tables when select/staging mode is used.

Exact IAM bindings depend on your project layout. Keep the permissions scoped to
the datasets and bucket prefixes used by the package.

## Procedure Installation

Install or update the Snowflake procedure from `on-run-start`:

```yaml
on-run-start:
  - "{{ dbt_snowflake_iceberg_sync.install_iceberg_sync_procedure() }}"
```

The installer uploads the Python `procedure/` package to the configured internal
stage using Snowflake directory imports, then creates:

```sql
CREATE OR ALTER PROCEDURE <procedure>(config VARIANT)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.12'
PACKAGES = ('snowflake-snowpark-python', 'requests', 'google-auth')
IMPORTS = ('@<handler_stage>/<handler_stage_path>/=<handler_import_name>/')
HANDLER = '<handler_import_name>.handler.main'
EXTERNAL_ACCESS_INTEGRATIONS = (...)
SECRETS = ('<alias>' = <secret_fqdn>)
EXECUTE AS CALLER
```

## BigQuery Extract Model

Use `extract` when the source is a concrete BigQuery table, a wildcard table, or
native partition decorators.

```sql
{{
  config(
    materialized='iceberg_sync',
    meta={
      'iceberg_sync': {
        'source_type': 'bigquery',
        'materialization_strategy': 'incremental',

        'bigquery_export_strategy': 'extract',
        'google_cloud_project_id': 'my-gcp-project',
        'bigquery_dataset_id': 'analytics',
        'bigquery_table_id': 'orders',
        'bigquery_location': 'US',
        'bigquery_export_location': '@ANALYTICS.PUBLIC.BQ_EXPORT_STAGE/orders',
        'bigquery_export_predicate_type': 'auto',
        'bigquery_export_incremental_predicates': ["20260530"],

        'incremental_strategy': 'delete+copy',
        'incremental_predicate': "\"order_date\" = '2026-05-30'",

        'iceberg_table_external_volume': 'ICEBERG_EXTERNAL_VOLUME'
      }
    }
  )
}}
```

The model body is ignored for `extract` mode and should be empty.

## BigQuery Select/Staging Model

Use `select` when the source is arbitrary BigQuery SQL. The SQL in the model body
is BigQuery SQL, not Snowflake SQL.

```sql
{{
  config(
    materialized='iceberg_sync',
    meta={
      'iceberg_sync': {
        'source_type': 'bigquery',

        'bigquery_export_strategy': 'select',
        'google_cloud_project_id': 'my-gcp-project',
        'bigquery_dataset_id': 'analytics',
        'bigquery_table_id': 'orders',
        'bigquery_location': 'US',
        'bigquery_export_location': '@ANALYTICS.PUBLIC.BQ_EXPORT_STAGE/orders_select',
        'bigquery_export_predicate_type': 'where',
        'bigquery_export_incremental_predicates': ["order_date = '2026-05-30'"],

        'bigquery_staging_dataset_id': 'dbt_staging',
        'bigquery_staging_table_expiration_hours': 24,
        'bigquery_staging_table_reuse': true,

        'incremental_strategy': 'delete+copy',
        'incremental_predicate': "\"order_date\" = '2026-05-30'",

        'iceberg_table_external_volume': 'ICEBERG_EXTERNAL_VOLUME'
      }
    }
  )
}}

select
  *
from `my-gcp-project.analytics.orders`
```

The procedure creates or reuses a deterministic staging table, exports it to GCS
as Parquet, and then loads those files into the Snowflake-managed Iceberg table.

## Materialization Options

All options in this section are dbt model configs under `meta.iceberg_sync`.
Keeping package-specific keys under `meta` is required by dbt Fusion. dbt Core
also accepts the same `meta.iceberg_sync` shape, and this package still reads
legacy top-level config keys for existing dbt Core projects. The materialization
separates common options from source-specific options so future source types can
define their own required fields. In the current release, `source_type='bigquery'`
is the only supported source type, so every working model uses the BigQuery
option group below.

Credential material is not a materialization option. Keep service account JSON
and secret names in Snowflake resources and `vars.iceberg_sync`; model configs
that contain credential-like keys are rejected.

Snowflake object identifiers managed by the package, including relation
database, schema, table, view, procedure, run-log, and named stage identifiers,
are normalized to uppercase so they are compatible with Snowflake unquoted
identifier folding. Source column names are different: BigQuery and Parquet loads
use `MATCH_BY_COLUMN_NAME = CASE_SENSITIVE`, so source column case is preserved
and quoted in internal table DDL and exposed view SQL.

### Common Options

| Option | Required | Default | Description |
| --- | --- | --- | --- |
| `materialized` | Yes | None | Must be `iceberg_sync`. |
| `source_type` | No | `bigquery` | Source adapter to use. Only `bigquery` is supported in this release. |
| `materialization_strategy` | No | `incremental` | `incremental` or `full_refresh`. `full_refresh` always reloads the target table. |
| `incremental_strategy` | No | `delete+copy` | Incremental load strategy. Only `delete+copy` is supported. |
| `incremental_predicate` | Conditional | None | Snowflake SQL predicate used to delete rows from the internal Iceberg table during incremental runs. Required when `bigquery_export_incremental_predicates` is non-empty, and must be absent when that list is empty. |
| `partition_by` | No | `[]` | Not supported yet. Any non-empty value fails validation. |
| `cluster_by` | No | `[]` | Not supported yet. Any non-empty value fails validation. |
| `iceberg_sync_retry_max_attempts` | No | `3` | Maximum attempts for retryable Snowflake load transaction failures. Must be at least `1`. |
| `iceberg_sync_retry_initial_delay_seconds` | No | `5` | Initial delay before retrying a retryable Snowflake load transaction failure. |
| `iceberg_sync_retry_max_delay_seconds` | No | `60` | Maximum retry delay after applying exponential backoff and jitter. |
| `iceberg_sync_retry_backoff_multiplier` | No | `2.0` | Retry delay multiplier. Must be at least `1.0`. |
| `iceberg_sync_retry_jitter_seconds` | No | `3` | Maximum random jitter added to retry delays. |
| `iceberg_sync_cleanup_created_table_on_failure` | No | `true` | Drop a newly-created internal Iceberg table after failed initial creation when no target view existed before the run. |
| `iceberg_sync_run_log_fail_on_error` | No | `false` | Fail the model when writing the shared run log table fails. The default keeps run-log writes best-effort for high-concurrency runs. |

The effective mode becomes full refresh when dbt is invoked with
`--full-refresh`, when `materialization_strategy='full_refresh'`, or when the
internal Iceberg table or exposed target view does not yet exist.

### Iceberg Table Options

These options apply to the Snowflake-managed Iceberg table created behind the
exposed dbt view. The table is created as `__<model_identifier>` in the model's
target database and schema.

| Option | Required | Default | Description |
| --- | --- | --- | --- |
| `iceberg_table_external_volume` | Yes | None | Snowflake external volume used by `CREATE ICEBERG TABLE`. |
| `iceberg_table_base_location` | No | `<database>/<schema>/<identifier>` | Iceberg base location. When omitted, the package derives a stable location from the target relation. |
| `iceberg_table_target_file_size` | No | `AUTO` | Passed to `TARGET_FILE_SIZE`. |
| `iceberg_table_storage_serialization_policy` | No | `COMPATIBLE` | `COMPATIBLE` or `OPTIMIZED`. |
| `iceberg_table_data_retention_time_in_days` | No | `7` | Passed to `DATA_RETENTION_TIME_IN_DAYS`. |
| `iceberg_table_max_data_extension_time_in_days` | No | None | Optional `MAX_DATA_EXTENSION_TIME_IN_DAYS`. |
| `iceberg_table_change_tracking` | No | `true` | Passed to `CHANGE_TRACKING`. Must stay `true` when `iceberg_table_iceberg_version=3`. |
| `iceberg_table_copy_grants` | No | `false` | Adds `COPY GRANTS` when the table is created. |
| `iceberg_table_error_logging` | No | `false` | Must stay `false`; Snowflake does not support error logging for this Iceberg `COPY INTO` path. |
| `iceberg_table_iceberg_version` | No | `3` | `2` or `3`. |
| `iceberg_table_enable_iceberg_merge_on_read` | No | `true` | Passed to `ENABLE_ICEBERG_MERGE_ON_READ`. |
| `iceberg_table_enable_data_compaction` | No | `true` | Passed to `ENABLE_DATA_COMPACTION`. |

### BigQuery Source Options

These options apply when `source_type='bigquery'`.

| Option | Required | Default | Description |
| --- | --- | --- | --- |
| `google_cloud_project_id` | Yes | None | BigQuery project for metadata, query, and extract jobs. |
| `bigquery_dataset_id` | Yes | None | BigQuery dataset containing the source table or wildcard shard set. |
| `bigquery_table_id` | Yes | None | BigQuery source table id. For `extract`, this is the table to export and may end with `_*` for sharded tables. For `select`, this still identifies the source for deterministic staging-table hashing even though the model SQL is the exported query. |
| `bigquery_location` | Yes | None | BigQuery job location, for example `US` or a regional location. |
| `bigquery_export_location` | Yes | None | Named Snowflake stage location that resolves to the GCS export prefix, for example `@DB.SCHEMA.STAGE/path`. User stages (`@~`) and table stages (`@%`) are rejected. |
| `bigquery_export_strategy` | No | `extract` | `extract` exports BigQuery tables directly. `select` runs model SQL into a BigQuery staging table, then exports that table. |
| `bigquery_export_predicate_type` | No | `auto` | Predicate planning mode. Supported values are `auto`, `none`, `partition_decorator`, `table_suffix`, and `where`; valid values depend on `bigquery_export_strategy`. |
| `bigquery_export_full_refresh_predicates` | No | `[]` | Source predicates used only when the effective mode is full refresh. A string is treated as a single-item list. |
| `bigquery_export_incremental_predicates` | No | `[]` | Source predicates used only when the effective mode is incremental. Must be paired with `incremental_predicate`. A string is treated as a single-item list. |

BigQuery source predicates are interpreted differently by predicate type:

| Predicate type | Valid with | Predicate value meaning |
| --- | --- | --- |
| `auto` | `extract`, `select` | Chooses `none` when no source predicates are configured. For `extract`, chooses `table_suffix` for `bigquery_table_id` values ending in `_*`, or `partition_decorator` for native time- or integer-range-partitioned tables. For `select`, chooses `where` when predicates exist. |
| `none` | `extract`, `select` | No source predicates are allowed. `extract` exports the concrete table, or every table matching the wildcard prefix when `bigquery_table_id` ends in `_*`. |
| `partition_decorator` | `extract` only | Each predicate is appended as a BigQuery partition decorator, such as `20260530` or an integer-range partition id. Requires a concrete native partitioned table. |
| `table_suffix` | `extract` only | Each predicate is appended to the wildcard prefix. Requires `bigquery_table_id` to end with `_*`. |
| `where` | `select` only | Each predicate is BigQuery SQL. Predicates are combined with `OR` and applied outside the model SQL subquery. |

### BigQuery Extract Requirements

Use `bigquery_export_strategy='extract'` for concrete tables, native partitioned
tables, and sharded tables. The dbt model body must be empty in this mode; model
SQL is rejected so it is not silently ignored.

`extract` does not use BigQuery staging table options. Its required fields are
the common BigQuery source fields plus `iceberg_table_external_volume`.

### BigQuery Select/Staging Requirements

Use `bigquery_export_strategy='select'` for arbitrary BigQuery SQL. The dbt
model body is required and must be BigQuery SQL. `select` allows only `auto`,
`none`, or `where` predicate types.

| Option | Required | Default | Description |
| --- | --- | --- | --- |
| `bigquery_staging_dataset_id` | Yes for `select` | None | BigQuery dataset where deterministic staging tables are created. |
| `bigquery_staging_table_expiration_hours` | No | `24` | Expiration applied to generated staging tables. |
| `bigquery_staging_table_reuse` | No | `true` | Reuse an existing non-expired staging table when its stored hash matches the model SQL, predicates, source identity, target relation, and export settings. |
| `force_rebuild_staging_table` | No | `false` | Rebuild the staging table even if a reusable table exists. This option is currently unprefixed in dbt model config. |

When `where` predicates are configured for `select`, the package renders:

```sql
SELECT *
FROM (
<model SQL>
) AS __dbt_iceberg_sync_src
WHERE (<predicate 1>) OR (<predicate 2>)
```

## Full Refresh Behavior

The effective mode is full refresh when:

- dbt runs with `--full-refresh`;
- `materialization_strategy='full_refresh'`;
- the internal Iceberg table or exposed target view does not exist.

Full refresh deletes all rows from the internal Iceberg table before copying the
new Parquet files.

## Incremental Delete+Copy Behavior

Incremental mode uses `incremental_strategy='delete+copy'`. BigQuery incremental
predicates and Snowflake `incremental_predicate` must be both present or both
absent.

- Both present: export the BigQuery predicate window, delete the Snowflake
  predicate window, then copy files.
- Both absent: copy a complete export after a full-table delete.

The Snowflake transaction begins after export and table DDL. If `DELETE` or
`COPY INTO` fails, the procedure rolls back the transaction and preserves the
previous committed table data.

## Retry And Cleanup Behavior

The materialization retries transient Snowflake failures at two layers. The dbt
materialization wraps the outer stored procedure `CALL` in a Snowflake Scripting
retry block so Snowflake internal errors raised by the `CALL` itself can be
retried. The procedure also retries transient Snowflake failures raised by the
load transaction:

```sql
BEGIN;
DELETE FROM <internal_iceberg_table> [WHERE ...];
COPY INTO <internal_iceberg_table> ... LOAD_MODE = ADD_FILES_COPY;
COMMIT;
```

Procedure-level retries reuse the same BigQuery export files and do not rerun
the BigQuery export job. Outer `CALL` retries may rerun the procedure and create
a new export prefix. This remains idempotent for full-refresh and `delete+copy`
incremental runs because each load attempt applies the configured Snowflake
delete predicate before copying files. Retry classifiers apply only to stable
Snowflake execution-error text that looks transient, including messages
containing `SQL execution internal error`, `incident`, or the scoped-transaction
error text `Scoped transaction started in stored procedure is incomplete`.

The procedure does not retry configuration, schema, predicate validation,
BigQuery source, permission, or relation-conflict errors.

The stored procedure creates or replaces the exposed dbt view after a successful
load commit. During initial creation, if the procedure created the internal
Iceberg table and the run fails before the target view is successfully created,
the procedure drops that newly-created internal table when
`iceberg_sync_cleanup_created_table_on_failure=true`. Pre-existing internal
tables are never dropped by this cleanup path.

The run log and returned procedure result include `retry` and `cleanup` objects
with attempt counts, retryable error diagnostics, and best-effort cleanup
outcomes.

The run log table is shared by all concurrent models. Run-log writes are
best-effort by default, and lock-contention failures such as `000625`, `locked
table`, or `number of waiters` are retried before being ignored. When a
successful sync cannot write the run log, the procedure result includes
`run_log_error`. Set `iceberg_sync_run_log_fail_on_error=true` to restore strict
run-log behavior.

The `incremental_predicate` is evaluated against the internal Iceberg table, not
the exposed view. Top-level source field names are preserved exactly in that
table for `MATCH_BY_COLUMN_NAME = CASE_SENSITIVE`, so quote lowercase or mixed
case source names in Snowflake predicates, for example
`incremental_predicate="\"event_date\" = '20240111'"`.

## Schema Support

Top-level source field names are preserved exactly in the internal Iceberg table
for `MATCH_BY_COLUMN_NAME = CASE_SENSITIVE`.

The exposed view aliases top-level fields to lower-snake names. Alias collisions
fail before loading.

| BigQuery type | Snowflake type |
| --- | --- |
| `STRING` | `VARCHAR` |
| `INT64`, `INTEGER` | `BIGINT` |
| `FLOAT64`, `FLOAT`, `DOUBLE` | `DOUBLE` |
| `BOOL`, `BOOLEAN` | `BOOLEAN` |
| `DATE` | `DATE` |
| `DATETIME` | `TIMESTAMP_NTZ(6)` |
| `TIMESTAMP` | `TIMESTAMP_LTZ(6)` |
| `NUMERIC`, `DECIMAL` | `NUMBER(38,9)` |
| `BYTES` | `BINARY` |
| `RECORD`, `STRUCT` | structured `OBJECT(...)` |
| repeated compatible fields | structured `ARRAY(...)` |

Unsupported in the first scope:

- `BIGNUMERIC`, `BIGDECIMAL`
- `GEOGRAPHY`
- `JSON`
- `TIME`
- unsupported nested or repeated combinations

Schema evolution is conservative. Existing column order, names, and mapped types
must remain compatible. Safe additive columns may be added.

## Unsupported First-Scope Features

The package rejects:

- Non-empty `partition_by`.
- Non-empty `cluster_by`.
- Arbitrary `COPY INTO` transformations.
- Non-Parquet file formats.
- Unstaged cloud URI loads.
- Generic `VARIANT` sinks for unsupported nested data.
- `iceberg_table_error_logging=true`; Snowflake does not support error logging
  for this `COPY INTO` path.
- `iceberg_table_change_tracking=false` with Iceberg V3 tables.

The Iceberg load uses:

```sql
COPY INTO <iceberg_table>
FROM @<named_stage>/<run_prefix>/
FILE_FORMAT = (TYPE = PARQUET USE_VECTORIZED_SCANNER = TRUE)
LOAD_MODE = ADD_FILES_COPY
MATCH_BY_COLUMN_NAME = CASE_SENSITIVE
PURGE = FALSE
```

## Local Tests

Unit tests do not require Snowflake or BigQuery credentials:

```bash
mise install --locked
uv sync --frozen
uv run pytest tests/unit
uv run ruff check procedure tests
uv run dbt parse --profiles-dir tests/ci_profiles --no-version-check
```

For dbt Fusion validation, run the same package parse with the Fusion CLI and
do not pass partial-parse flags:

```bash
dbtf parse --profiles-dir tests/ci_profiles --no-version-check
```

## Local Integration Test Setup

Integration tests are opt-in:

```bash
DBT_SNOWFLAKE_ICEBERG_SYNC_RUN_INTEGRATION=1 uv run pytest -m integration
```

Configure these environment variables for your own resources:

```text
SNOWFLAKE_ACCOUNT
SNOWFLAKE_USER
SNOWFLAKE_AUTHENTICATOR
SNOWFLAKE_PASSWORD
SNOWFLAKE_PRIVATE_KEY_PATH
SNOWFLAKE_ROLE
SNOWFLAKE_WAREHOUSE
SNOWFLAKE_DATABASE
SNOWFLAKE_SCHEMA
DBT_SNOWFLAKE_ICEBERG_SYNC_HANDLER_STAGE
DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_EXPORT_STAGE
DBT_SNOWFLAKE_ICEBERG_SYNC_EXTERNAL_VOLUME
DBT_SNOWFLAKE_ICEBERG_SYNC_PROCEDURE_DATABASE
DBT_SNOWFLAKE_ICEBERG_SYNC_PROCEDURE_SCHEMA
DBT_SNOWFLAKE_ICEBERG_SYNC_SECRET_FQDN
DBT_SNOWFLAKE_ICEBERG_SYNC_SECRET_ALIAS
DBT_SNOWFLAKE_ICEBERG_SYNC_EXTERNAL_ACCESS_INTEGRATION
DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_PROJECT_ID
DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_LOCATION
DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_DATASET_ID
DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_TABLE_ID
DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_TABLE_EXPECTED_ROWS
DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_DATETIME_TABLE_ID
DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_DATETIME_EXPECTED_ROWS
DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_DATETIME_EXPECTED_VALUES

DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_NON_PARTITIONED_TABLE_ID
DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_NON_PARTITIONED_EXPECTED_ROWS
DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_TIME_PARTITIONED_TABLE_ID
DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_TIME_PARTITION_DECORATOR
DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_TIME_PARTITION_EXPECTED_ROWS
DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_RANGE_PARTITIONED_TABLE_ID
DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_RANGE_PARTITION_DECORATOR
DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_RANGE_PARTITION_EXPECTED_ROWS
DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_SHARDED_TABLE_ID
DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_SHARDED_SUFFIX
DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_SHARDED_SUFFIX_EXPECTED_ROWS
DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_SHARDED_ALL_EXPECTED_ROWS

DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_STAGING_DATASET_ID
DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_SELECT_SQL
DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_SELECT_PREDICATE
DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_SELECT_TABLE_ID
DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_SELECT_EXPECTED_ROWS
DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_SELECT_ALL_EXPECTED_ROWS

DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_INCREMENTAL_TABLE_ID
DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_INCREMENTAL_EXPORT_PREDICATE_TYPE
DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_INCREMENTAL_FULL_REFRESH_PREDICATES
DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_INCREMENTAL_PREDICATES
DBT_SNOWFLAKE_ICEBERG_SYNC_INCREMENTAL_PREDICATE
DBT_SNOWFLAKE_ICEBERG_SYNC_INCREMENTAL_EXPECTED_ROWS
```

`SNOWFLAKE_AUTHENTICATOR` defaults to `externalbrowser` when unset.
`DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_TABLE_EXPECTED_ROWS` is optional. The
incremental predicate list variables accept either JSON arrays or comma-separated
strings.

Integration tests may create temporary Snowflake procedures, views, Iceberg
tables, run log tables, BigQuery extract jobs, and GCS objects under generated
test prefixes. The tests use caller-provided BigQuery fixture tables and do not
create or delete those fixture tables. Cleanup is best-effort and must not delete
user-specified non-test resources.

### Integration CI Approval

Pull request CI includes an approval-only `Integration Approval` check. It does
not run live integration tests and does not receive Snowflake, BigQuery, GCS, or
fixture credentials.

The check passes when the current PR head has a fresh approving review from an
`OWNER`, `MEMBER`, or `COLLABORATOR`, excluding the PR author. New commits require
a new approval. The check is bypassed when the PR author is a repository owner.
Run live integration tests outside GitHub with company-managed credentials after
that approval.

## Security Notes

- No credential material belongs in dbt model config.
- GCP service account JSON should live in a Snowflake secret.
- External access integrations, network rules, stages, and IAM permissions are
  managed by the user.
- BigQuery credentials are not included in the procedure call payload.
- Exported GCS files are not cleaned up by first-scope code. Use a GCS lifecycle
  policy or add explicit cleanup in a later release.
