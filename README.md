# dbt-snowflake-iceberg-sync

`dbt-snowflake-iceberg-sync` is a dbt package that syncs data exported from an
external source into a Snowflake-managed Iceberg table and exposes the dbt model
as a Snowflake view.

The first supported source type is BigQuery. The dbt materialization is
Snowflake-only and is named `iceberg_sync`.

## Supported Versions

- dbt Core: `>=1.8.0`
- dbt adapter: `dbt-snowflake`
- Snowflake Python procedure runtime: Python `3.12`
- Local package tests: Python `>=3.11`

## Installation

Add the package to `packages.yml`:

```yaml
packages:
  - git: https://github.com/civitaspo/dbt-snowflake-iceberg-sync.git
    revision: v0.1.0
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
    procedure_database: ANALYTICS
    procedure_schema: UTIL
    procedure_name: ICEBERG_SYNC

    handler_stage: ANALYTICS.UTIL.ICEBERG_SYNC_HANDLER_STAGE
    handler_stage_path: procedure
    handler_import_name: iceberg_sync_procedure
    handler_name: iceberg_sync_procedure.handler.main
    handler_local_path: dbt_packages/dbt_snowflake_iceberg_sync/procedure

    external_access_integrations: [BIGQUERY_API]
    google_cloud_service_account_secret_fqdn: ANALYTICS.SECRETS.GOOGLE_CLOUD_SERVICE_ACCOUNT_JSON
    google_cloud_service_account_secret_alias: google_cloud_service_account_credentials_json
```

The package creates a run log table named
`<procedure_database>.<procedure_schema>.ICEBERG_SYNC_RUN_LOG` by default. Set
`vars.iceberg_sync.run_log_table` to a three-part relation name to override it.

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
CREATE OR REPLACE PROCEDURE <procedure>(config VARIANT)
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
    source_type='bigquery',
    materialization_strategy='incremental',

    bigquery_export_strategy='extract',
    google_cloud_project_id='my-gcp-project',
    bigquery_dataset_id='analytics',
    bigquery_table_id='orders',
    bigquery_location='US',
    bigquery_export_location='@ANALYTICS.PUBLIC.BQ_EXPORT_STAGE/orders',
    bigquery_export_predicate_type='auto',
    bigquery_export_incremental_predicates=["20260530"],

    incremental_strategy='delete+copy',
    incremental_predicate="\"order_date\" = '2026-05-30'",

    iceberg_table_external_volume='ICEBERG_EXTERNAL_VOLUME'
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
    source_type='bigquery',

    bigquery_export_strategy='select',
    google_cloud_project_id='my-gcp-project',
    bigquery_dataset_id='analytics',
    bigquery_table_id='orders',
    bigquery_location='US',
    bigquery_export_location='@ANALYTICS.PUBLIC.BQ_EXPORT_STAGE/orders_select',
    bigquery_export_predicate_type='where',
    bigquery_export_incremental_predicates=["order_date = '2026-05-30'"],

    bigquery_staging_dataset_id='dbt_staging',
    bigquery_staging_table_expiration_hours=24,
    bigquery_staging_table_reuse=true,

    incremental_strategy='delete+copy',
    incremental_predicate="\"order_date\" = '2026-05-30'",

    iceberg_table_external_volume='ICEBERG_EXTERNAL_VOLUME'
  )
}}

select
  *
from `my-gcp-project.analytics.orders`
```

The procedure creates or reuses a deterministic staging table, exports it to GCS
as Parquet, and then loads those files into the Snowflake-managed Iceberg table.

## Full Refresh Behavior

The effective mode is full refresh when:

- dbt runs with `--full-refresh`;
- `materialization_strategy='full_refresh'`;
- the internal Iceberg table does not exist.

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
| `TIMESTAMP` | `TIMESTAMP_LTZ(6)` |
| `NUMERIC`, `DECIMAL` | `NUMBER(38,9)` |
| `BYTES` | `BINARY` |
| `RECORD`, `STRUCT` | structured `OBJECT(...)` |
| repeated compatible fields | structured `ARRAY(...)` |

Unsupported in the first scope:

- `BIGNUMERIC`, `BIGDECIMAL`
- `DATETIME`
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
uv run dbt parse --profiles-dir tests/ci_profiles --no-version-check --no-partial-parse
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

DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_NON_PARTITIONED_TABLE_ID
DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_NON_PARTITIONED_EXPECTED_ROWS
DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_TIME_PARTITIONED_TABLE_ID
DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_TIME_PARTITION_DECORATOR
DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_TIME_PARTITION_EXPECTED_ROWS
DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_RANGE_PARTITIONED_TABLE_ID
DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_RANGE_PARTITION_DECORATOR
DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_RANGE_PARTITION_EXPECTED_ROWS

DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_STAGING_DATASET_ID
DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_SELECT_SQL
DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_SELECT_PREDICATE
DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_SELECT_TABLE_ID
DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_SELECT_EXPECTED_ROWS

DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_INCREMENTAL_TABLE_ID
DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_INCREMENTAL_EXPORT_PREDICATE_TYPE
DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_INCREMENTAL_FULL_REFRESH_PREDICATES
DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_INCREMENTAL_PREDICATES
DBT_SNOWFLAKE_ICEBERG_SYNC_INCREMENTAL_PREDICATE
DBT_SNOWFLAKE_ICEBERG_SYNC_INCREMENTAL_EXPECTED_ROWS
```

`SNOWFLAKE_AUTHENTICATOR` defaults to `externalbrowser` when unset.
`DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_TABLE_EXPECTED_ROWS` and
`DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_SELECT_TABLE_ID` are optional. The
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
a new approval. Run live integration tests outside GitHub with company-managed
credentials after that approval.

## Security Notes

- No credential material belongs in dbt model config.
- GCP service account JSON should live in a Snowflake secret.
- External access integrations, network rules, stages, and IAM permissions are
  managed by the user.
- BigQuery credentials are not included in the procedure call payload.
- Exported GCS files are not cleaned up by first-scope code. Use a GCS lifecycle
  policy or add explicit cleanup in a later release.
