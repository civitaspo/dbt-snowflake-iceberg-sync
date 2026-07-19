# dbt-snowflake-iceberg-sync

`dbt-snowflake-iceberg-sync` is a dbt package that syncs data exported from an
external source into a Snowflake-managed Iceberg table and exposes the dbt model
as a Snowflake view.

Supported source types:

- `bigquery` — export BigQuery tables/queries to a GCS-backed stage as Parquet
- `s3_parquet` — load pre-existing Iceberg-compatible Parquet files from an
  S3-backed Snowflake stage (Storage Integration managed by the user)

The dbt materialization is Snowflake-only and is named `iceberg_sync`.

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
    revision: v0.4.0
```

Then run:

```bash
dbt deps
```

## Required Snowflake Setup

You must create and manage the Snowflake resources used by the chosen source
type(s).

Common to all source types:

- A Snowflake-managed Iceberg external volume.
- A database/schema where the package procedure can be installed.

For BigQuery sources:

- A GCS-backed Snowflake stage used as the BigQuery export destination.
- A network rule and external access integration for BigQuery API calls.
- A Snowflake secret for Google Cloud auth:
  - a generic secret containing the Google Cloud service account JSON when
    `google_cloud_auth_method=service_account_credentials_json` (default), or
  - a workload identity federation secret when
    `google_cloud_auth_method=workload_identity_federation`.

For S3 Parquet sources:

- An S3-backed Snowflake stage with a Storage Integration that can `LIST` and
  `COPY` the source Parquet prefixes.
- The Iceberg external volume may live on any cloud supported by Snowflake;
  `LOAD_MODE = ADD_FILES_COPY` performs a server-side copy into the volume.

Example BigQuery deployment vars:

```yaml
vars:
  iceberg_sync:
    handler_local_path: dbt_packages/dbt_snowflake_iceberg_sync/procedure

    external_access_integrations: [BIGQUERY_API]
    google_cloud_service_account_secret_fqdn: ANALYTICS.SECRETS.GOOGLE_CLOUD_SERVICE_ACCOUNT_JSON
```

Example S3-only deployment vars (no Google Cloud secret required):

```yaml
vars:
  iceberg_sync:
    handler_local_path: dbt_packages/dbt_snowflake_iceberg_sync/procedure
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
| `handler_local_path` | Yes | None | Local path to the package `procedure/` directory uploaded by the installer. Relative paths (for example `dbt_packages/dbt_snowflake_iceberg_sync/procedure`) are absolute-ized before Snowflake `PUT`, preferring `DBT_PROJECT_DIR` when set and otherwise resolving against the process working directory. Absolute paths are accepted unchanged. |
| `google_cloud_service_account_secret_fqdn` | Yes for BigQuery + `service_account_credentials_json` | None | Fully qualified Snowflake secret containing the Google Cloud service account JSON. Optional for S3-only installs. |
| `procedure_database` | No | `target.database` | Database where the package procedure and default helper objects are installed. |
| `procedure_schema` | No | `target.schema` | Schema where the package procedure and default helper objects are installed. |
| `procedure_name` | No | `ICEBERG_SYNC` | Name of the Snowflake stored procedure. |
| `handler_stage` | No | `<procedure_database>.<procedure_schema>.ICEBERG_SYNC_HANDLER_STAGE` | Internal Snowflake stage used for the Python handler files. |
| `handler_stage_path` | No | `procedure` | Directory prefix inside `handler_stage` for uploaded handler files. |
| `handler_import_name` | No | `iceberg_sync_procedure` | Import directory name mounted into the Snowflake Python runtime. |
| `handler_name` | No | `<handler_import_name>.handler.main` | Python procedure entry point. |
| `external_access_integrations` | No | `[]` | External access integrations granted to the procedure. Required for BigQuery API calls. |
| `google_cloud_auth_method` | No | `service_account_credentials_json` | Google Cloud auth mode. Supported values are `service_account_credentials_json` and `workload_identity_federation`. |
| `google_cloud_service_account_secret_alias` | No | `google_cloud_service_account_credentials_json` | Secret alias read by the Python handler for `service_account_credentials_json`. |
| `google_cloud_workload_identity_federation_secret_fqdn` | Yes for `workload_identity_federation` | None | Three-part name of the Snowflake workload identity federation secret used by `SYSTEM$ISSUE_WORKLOAD_IDENTITY_FEDERATION_TOKEN`. |
| `google_cloud_workload_identity_federation_audience` | Yes for `workload_identity_federation` | None | Google Cloud workload identity provider resource name, for example `//iam.googleapis.com/projects/<project_number>/locations/global/workloadIdentityPools/<pool_id>/providers/<provider_id>`. |
| `google_cloud_service_account_impersonation` | No | None | Optional Google Cloud service account email to impersonate after the STS token exchange. |
| `google_cloud_workload_identity_federation_by_dbt_target` | No | None | Map of `target.name` to per-target workload identity federation settings. Each entry uses the same keys as the flat vars above. An optional `default` entry is used when `target.name` is not present in the map. |
| `parquet_file_format` | No | `<procedure_database>.<procedure_schema>.ICEBERG_SYNC_PARQUET_FILE_FORMAT` | Named Parquet file format created by the installer and used by `s3_parquet` `INFER_SCHEMA`. |
| `run_log_table` | No | `<procedure_database>.<procedure_schema>.ICEBERG_SYNC_RUN_LOG` | Three-part relation used for procedure run logs. |

## Required Google Cloud IAM Setup

The Google Cloud service account stored in the Snowflake secret needs permissions to:

- Read BigQuery table metadata.
- Run BigQuery query jobs when `bigquery_export_strategy='select'`.
- Run BigQuery extract jobs.
- Write ZSTD-compressed Parquet exports to the GCS bucket behind
  `bigquery_export_location` by default.
- Read or create staging tables when select/staging mode is used.

Exact IAM bindings depend on your project layout. Keep the permissions scoped to
the datasets and bucket prefixes used by the package.

When `bigquery_job_project_id` differs from `google_cloud_project_id`, split
permissions across projects:

| Project / resource | Typical roles |
| --- | --- |
| Source project (`google_cloud_project_id`) | `roles/bigquery.dataViewer` on source datasets |
| Job project (`bigquery_job_project_id`) | `roles/bigquery.jobUser` (includes `bigquery.jobs.create`) |
| Staging dataset in the job project (`bigquery_staging_dataset_id`, `select` only) | `roles/bigquery.dataEditor` on the staging dataset (create destination tables; patch labels / expiration) |
| GCS export bucket | object create/write for extract destinations |

`roles/bigquery.dataViewer` alone on the source project is not enough if jobs are
submitted to that same project. Use a separate job project when the Snowflake
service account should stay read-only on source datasets.

`bigquery_job_project_id` is the BigQuery Jobs API project
(`jobs.insert` / `jobs.get`). It is not the same as a Google auth
`quota_project_id` / `x-goog-user-project` header.

## Workload Identity Federation

To use Snowflake outbound workload identity federation instead of a static
service account key, set:

```yaml
vars:
  iceberg_sync:
    handler_local_path: dbt_packages/dbt_snowflake_iceberg_sync/procedure
    external_access_integrations: [BIGQUERY_API]
    google_cloud_auth_method: workload_identity_federation
    google_cloud_workload_identity_federation_secret_fqdn: ANALYTICS.SECRETS.WORKLOAD_IDENTITY_FEDERATION_DEFAULT
    google_cloud_workload_identity_federation_audience: //iam.googleapis.com/projects/000000000000/locations/global/workloadIdentityPools/example-pool/providers/example-provider
    google_cloud_service_account_impersonation: sync@example-project.iam.gserviceaccount.com
```

In this mode the procedure issues a short-lived JWT with
`SYSTEM$ISSUE_WORKLOAD_IDENTITY_FEDERATION_TOKEN`, exchanges it through
`google-auth` identity-pool credentials, and optionally impersonates a service
account. The installer does not bind a `SECRETS = (...)` clause for workload identity federation auth.

When each dbt target needs a different workload identity provider, secret, or
impersonation service account, use
`google_cloud_workload_identity_federation_by_dbt_target`:

```yaml
vars:
  iceberg_sync:
    google_cloud_auth_method: workload_identity_federation
    google_cloud_workload_identity_federation_by_dbt_target:
      dev:
        google_cloud_workload_identity_federation_secret_fqdn: ANALYTICS.SECRETS.WORKLOAD_IDENTITY_FEDERATION_DEV
        google_cloud_workload_identity_federation_audience: //iam.googleapis.com/projects/111111111111/locations/global/workloadIdentityPools/dev-pool/providers/dev-provider
        google_cloud_service_account_impersonation: sync-dev@example-project.iam.gserviceaccount.com
      stg:
        google_cloud_workload_identity_federation_secret_fqdn: ANALYTICS.SECRETS.WORKLOAD_IDENTITY_FEDERATION_STG
        google_cloud_workload_identity_federation_audience: //iam.googleapis.com/projects/222222222222/locations/global/workloadIdentityPools/stg-pool/providers/stg-provider
        google_cloud_service_account_impersonation: sync-stg@example-project.iam.gserviceaccount.com
      default:
        google_cloud_workload_identity_federation_secret_fqdn: ANALYTICS.SECRETS.WORKLOAD_IDENTITY_FEDERATION_DEFAULT
        google_cloud_workload_identity_federation_audience: //iam.googleapis.com/projects/000000000000/locations/global/workloadIdentityPools/example-pool/providers/example-provider
```

dbt does not expose keys nested under `vars.<target.name>` to `var('foo')`.
The package therefore resolves workload identity federation settings from this
map using `target.name` explicitly.

For each workload identity federation field, resolution order is:

1. Top-level dbt vars such as `iceberg_sync_google_cloud_workload_identity_federation_audience` or CLI `--vars` overrides
2. `google_cloud_workload_identity_federation_by_dbt_target[target.name]`
3. `google_cloud_workload_identity_federation_by_dbt_target['default']`
4. Flat `vars.iceberg_sync.google_cloud_workload_identity_federation_*` keys

Per-target overrides can also be passed as top-level dbt vars such as
`iceberg_sync_google_cloud_auth_method`, `iceberg_sync_google_cloud_workload_identity_federation_secret_fqdn`, and
`iceberg_sync_google_cloud_workload_identity_federation_audience`. This is useful when a shared project file keeps
common package settings under `vars.iceberg_sync` but each target needs a
different workload identity provider or secret at runtime.

The workload identity federation transfer integration test
(`test_dbt_select_smoke_workload_identity_federation`) uses the `select` export
strategy to materialize one generated row into a writable BigQuery staging
dataset. It does not read from or write to caller-owned production fixture
tables. Configure a Snowflake-controlled Google Cloud project and staging dataset
where the impersonated service account can create staging tables and run extract
jobs.

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

When `google_cloud_auth_method='workload_identity_federation'`, the procedure omits the
`SECRETS = (...)` clause and issues the Snowflake federation token at runtime.

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

## S3 Parquet Model

Use `source_type: s3_parquet` when Iceberg-compatible Parquet files already exist
on an S3-backed Snowflake stage. The package lists matching files, resolves
schema via `INFER_SCHEMA` or optional shared `columns`, and loads with
`COPY INTO ... ADD_FILES_COPY`. S3 access comes from the stage Storage
Integration; keep AWS credentials out of dbt model config.

```sql
{{
  config(
    materialized='iceberg_sync',
    meta={
      'iceberg_sync': {
        'source_type': 's3_parquet',
        'materialization_strategy': 'incremental',

        's3_parquet_location': '@ANALYTICS.PUBLIC.S3_PARQUET_STAGE/orders',
        's3_parquet_file_pattern': '.*[.]parquet',
        's3_parquet_full_refresh_paths': ['dt=2026-05-29', 'dt=2026-05-30'],
        's3_parquet_incremental_paths': ['dt=2026-05-30'],

        'incremental_strategy': 'delete+copy',
        'incremental_predicate': "\"order_date\" = '2026-05-30'",

        'iceberg_table_external_volume': 'ICEBERG_EXTERNAL_VOLUME'
      }
    }
  )
}}
```

Declare columns explicitly when you want stable DDL and view-side casts without
`INFER_SCHEMA`:

```sql
{{
  config(
    materialized='iceberg_sync',
    meta={
      'iceberg_sync': {
        'source_type': 's3_parquet',
        's3_parquet_location': '@ANALYTICS.PUBLIC.S3_PARQUET_STAGE/orders',
        'columns': [
          {'name': 'OrderID', 'type': 'BIGINT', 'nullable': false, 'alias': 'order_id'},
          {
            'name': 'AmountText',
            'type': 'VARCHAR',
            'alias': 'amount',
            'expression': 'TRY_TO_NUMBER("AmountText")'
          }
        ],
        'iceberg_table_external_volume': 'ICEBERG_EXTERNAL_VOLUME'
      }
    }
  )
}}
```

For non-Iceberg-compatible Parquet (for example AWS CUR with
`TIMESTAMP_MILLIS`), set `s3_parquet_load_mode: full_ingest`. Snowflake rewrites
files into Iceberg-compatible Parquet. Optional `columns[].expression` values
are applied during COPY (`$1:"ColName"`), not on the view:

```sql
{{
  config(
    materialized='iceberg_sync',
    meta={
      'iceberg_sync': {
        'source_type': 's3_parquet',
        's3_parquet_location': '@ANALYTICS.PUBLIC.CUR_STAGE/cur',
        's3_parquet_load_mode': 'full_ingest',
        'columns': [
          {
            'name': 'line_item_usage_start_date',
            'type': 'TIMESTAMP_LTZ(6)',
            'alias': 'line_item_usage_start_date'
          },
          {
            'name': 'line_item_unblended_cost',
            'type': 'DOUBLE',
            'expression': 'TRY_TO_DOUBLE($1:"line_item_unblended_cost")'
          }
        ],
        'iceberg_table_external_volume': 'ICEBERG_EXTERNAL_VOLUME'
      }
    }
  )
}}
```

The model body must be empty. See `docs/design/s3_parquet_source.md` for load
semantics (`FORCE = TRUE`, `PURGE = FALSE`) and schema-evolution limits.

## Materialization Options

All options in this section are dbt model configs under `meta.iceberg_sync`.
Keeping package-specific keys under `meta` is required by dbt Fusion. dbt Core
also accepts the same `meta.iceberg_sync` shape, and this package still reads
legacy top-level config keys for existing dbt Core projects. The materialization
separates common options from source-specific options so future source types can
define their own required fields. Supported source types are `bigquery` and
`s3_parquet`.

Credential material is not a materialization option. Keep service account JSON,
AWS keys, and secret names in Snowflake resources and `vars.iceberg_sync`; model
configs that contain credential-like keys are rejected.

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
| `source_type` | No | `bigquery` | Source adapter to use. Supported values: `bigquery`, `s3_parquet`. |
| `materialization_strategy` | No | `incremental` | `incremental` or `full_refresh`. `full_refresh` always reloads the target table. |
| `incremental_strategy` | No | `delete+copy` | Incremental load strategy. Only `delete+copy` is supported. |
| `incremental_predicate` | Conditional | None | Snowflake SQL predicate used to delete rows from the internal Iceberg table during incremental runs. For BigQuery, required when `bigquery_export_incremental_predicates` is non-empty. For S3 Parquet, required when `s3_parquet_incremental_paths` is customized away from the default `['']`. Must be absent when the matching source incremental list is empty/default. |
| `columns` | No | None | Declared Iceberg column list under `meta.iceberg_sync` (not dbt `config.columns`). When set, overrides source schema inference for every source type. Each object needs `name` and `type`; optional `nullable`, `alias`, and view `expression` (for casts/transforms). |
| `partition_by` | No | `[]` | Not supported yet. Any non-empty value fails validation. Read from `meta.iceberg_sync` first, with legacy top-level config fallback. |
| `cluster_by` | No | `[]` | Not supported yet. Any non-empty value fails validation. Read from `meta.iceberg_sync` first, with legacy top-level config fallback. |
| `iceberg_sync_retry_max_attempts` | No | `3` | Compatibility retry setting for the legacy full-run procedure path. The dbt-side materialization issues Snowflake load statements directly and does not retry failed `COPY INTO` statements inside Snowflake Scripting. |
| `iceberg_sync_retry_initial_delay_seconds` | No | `5` | Compatibility retry delay for the legacy full-run procedure path. |
| `iceberg_sync_retry_max_delay_seconds` | No | `60` | Compatibility maximum retry delay for the legacy full-run procedure path. |
| `iceberg_sync_retry_backoff_multiplier` | No | `2.0` | Compatibility retry delay multiplier for the legacy full-run procedure path. Must be at least `1.0`. |
| `iceberg_sync_retry_jitter_seconds` | No | `3` | Compatibility random retry jitter for the legacy full-run procedure path. |
| `iceberg_sync_cleanup_created_table_on_failure` | No | `true` | Compatibility cleanup setting for the legacy full-run procedure path. In the dbt-side path, uncaught Snowflake statement failures abort the materialization before Jinja can run cleanup SQL, and the materialization does not drop tables after `CREATE ICEBERG TABLE IF NOT EXISTS` because ownership cannot be proven under concurrent runs. |
| `iceberg_sync_run_log_fail_on_error` | No | `false` | Fail the model when writing the shared run log table fails. The default keeps run-log writes best-effort for high-concurrency runs. |

The effective mode becomes full refresh when dbt is invoked with
`--full-refresh`, when `materialization_strategy='full_refresh'`, or when the
internal Iceberg table or exposed target view does not yet exist.

The materialization orchestrates Snowflake work from dbt. BigQuery REST API
calls still run through the package-managed Snowflake procedure because they
need the configured external access integration and secret, but dbt now controls
the wait loop between BigQuery job polls and directly issues Snowflake
`CREATE ICEBERG TABLE`, additive `ALTER ICEBERG TABLE`, `DELETE`, `COPY INTO`,
target view creation, and run-log `INSERT` statements.

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
| `google_cloud_project_id` | Yes | None | BigQuery project that contains the source table/dataset (`project.dataset.table`). Used for metadata reads, extract `sourceTable`, and wildcard/shard discovery. |
| `bigquery_job_project_id` | No | `google_cloud_project_id` | Project where BigQuery jobs are created and polled (`jobs.insert` / `jobs.get`). For `select`, staging tables are created in this project and `bigquery_staging_dataset_id` is interpreted there. Not a Google auth quota project. |
| `bigquery_dataset_id` | Yes | None | BigQuery dataset containing the source table or wildcard shard set. |
| `bigquery_table_id` | Yes | None | BigQuery source table id. For `extract`, this is the table to export and may end with `_*` for sharded tables. For `select`, this still identifies the source for deterministic staging-table hashing even though the model SQL is the exported query. |
| `bigquery_location` | Yes | None | BigQuery job location, for example `US` or a regional location. |
| `bigquery_export_location` | Yes | None | Named Snowflake stage location that resolves to the GCS export prefix, for example `@DB.SCHEMA.STAGE/path`. User stages (`@~`) and table stages (`@%`) are rejected. |
| `bigquery_export_compression` | No | `ZSTD` | BigQuery Parquet extract compression. Supported values are `NONE`, `SNAPPY`, `GZIP`, and `ZSTD`. Applies to both direct `extract` exports and `select` exports from generated staging tables. |
| `bigquery_export_strategy` | No | `extract` | `extract` exports BigQuery tables directly. `select` runs model SQL into a BigQuery staging table, then exports that table. |
| `bigquery_export_predicate_type` | No | `auto` | Predicate planning mode. Supported values are `auto`, `none`, `partition_decorator`, `table_suffix`, and `where`; valid values depend on `bigquery_export_strategy`. |
| `bigquery_export_full_refresh_predicates` | No | `[]` | Source predicates used only when the effective mode is full refresh. A string is treated as a single-item list. |
| `bigquery_export_incremental_predicates` | No | `[]` | Source predicates used only when the effective mode is incremental. Must be paired with `incremental_predicate`. A string is treated as a single-item list. |
| `bigquery_extract_skip_missing_tables` | No | `false` | For `extract` only. When `true`, missing planned BigQuery tables are skipped instead of failing the model. If every planned table is missing, dbt reports the model as successful without creating, loading, or replacing Snowflake objects. |
| `bigquery_export_poll_interval_seconds` | No | `30` | Seconds dbt waits between BigQuery export job polls. Must be positive. |
| `bigquery_export_poll_timeout_seconds` | No | `3600` | Maximum dbt-side wait window for BigQuery export completion. Must be positive and at least `bigquery_export_poll_interval_seconds`. |

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

Extract jobs write Parquet with `bigquery_export_compression='ZSTD'` by default.
Use `NONE`, `SNAPPY`, or `GZIP` only when downstream compatibility or performance
testing calls for a different codec.

`extract` does not use BigQuery staging table options. Its required fields are
the common BigQuery source fields plus `iceberg_table_external_volume`.

By default, missing BigQuery source tables fail the model. Set
`bigquery_extract_skip_missing_tables=true` when a scheduled shard or partition
may not exist yet and the model should leave existing Snowflake objects unchanged
instead of failing. If some planned tables exist and others are missing, only the
existing tables are exported.

### BigQuery Select/Staging Requirements

Use `bigquery_export_strategy='select'` for arbitrary BigQuery SQL. The dbt
model body is required and must be BigQuery SQL. `select` allows only `auto`,
`none`, or `where` predicate types.

| Option | Required | Default | Description |
| --- | --- | --- | --- |
| `bigquery_staging_dataset_id` | Yes for `select` | None | BigQuery dataset where deterministic staging tables are created. When `bigquery_job_project_id` is set, this dataset is resolved in the job project. |
| `bigquery_staging_table_expiration_hours` | No | `24` | Expiration applied to generated staging tables. |
| `bigquery_staging_table_reuse` | No | `true` | Reuse an existing non-expired staging table when its stored hash matches the model SQL, predicates, source identity, and target relation. The final Parquet extract still runs with the current `bigquery_export_compression`. |
| `force_rebuild_staging_table` | No | `false` | Rebuild the staging table even if a reusable table exists. This option is currently unprefixed in dbt model config. |

When `where` predicates are configured for `select`, the package renders:

```sql
SELECT *
FROM (
<model SQL>
) AS __dbt_iceberg_sync_src
WHERE (<predicate 1>) OR (<predicate 2>)
```

### S3 Parquet Source Options

These options apply when `source_type='s3_parquet'`.

| Option | Required | Default | Description |
| --- | --- | --- | --- |
| `s3_parquet_location` | Yes | None | Named Snowflake stage location that resolves to an S3 prefix, for example `@DB.SCHEMA.STAGE/path`. User stages (`@~`) and table stages (`@%`) are rejected. |
| `s3_parquet_file_pattern` | No | None | Regex relative to each load location. Applied during LIST planning with Python `re.search`; matched files are passed to COPY via `FILES` (not Snowflake `PATTERN`) so planning and load stay aligned. |
| `s3_parquet_full_refresh_paths` | No | `['']` | Path suffixes under `s3_parquet_location` used for full refresh. `['']` means the location itself. |
| `s3_parquet_incremental_paths` | No | `['']` | Path suffixes used for incremental runs. Custom values must be paired with `incremental_predicate`. |
| `s3_parquet_skip_missing_location` | No | `false` | When `true`, a location with zero matching files skips the run instead of failing. |
| `s3_parquet_infer_schema_max_file_count` | No | `16` | Maximum number of files passed to `INFER_SCHEMA` (newest by `last_modified` when capped). Ignored when shared `columns` is set. |
| `s3_parquet_load_mode` | No | `add_files_copy` | `add_files_copy` for Iceberg-compatible Parquet; `full_ingest` to scan/rewrite files (for example AWS CUR `TIMESTAMP_MILLIS`). With `full_ingest`, `columns[].expression` runs in the COPY `SELECT` (`$1:"Col"`) instead of only on the view. |

S3 Parquet loads always use `FORCE = TRUE` and `PURGE = FALSE`. When shared
`columns` is omitted, schema is detected with
`INFER_SCHEMA(..., KIND => 'ICEBERG')` against the installer-managed Parquet
file format (`vars.iceberg_sync.parquet_file_format`). Declared `columns` make that
file format optional for S3 models.

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

The Snowflake transaction begins after export and table DDL. dbt issues plain
SQL statements for `BEGIN`, the delete statement, `COPY INTO`, and `COMMIT`.
The commit is reached only after both the delete and copy statements succeed.

## Retry And Cleanup Behavior

The dbt-side materialization does not wrap Snowflake load work in anonymous
Snowflake Scripting such as `EXECUTE IMMEDIATE 'DECLARE ...'`. Instead, dbt
issues the load sequence as individual statements:

```sql
BEGIN;
DELETE FROM <internal_iceberg_table> [WHERE ...];
COPY INTO <internal_iceberg_table> ... LOAD_MODE = ADD_FILES_COPY;
COMMIT;
```

dbt/Jinja cannot catch a failed Snowflake statement and then continue the same
materialization run with a rollback or retry. For that reason, failed Snowflake
load statements should be retried by rerunning the dbt model or by using an
external orchestrator retry policy. This avoids keeping retry sleeps inside a
long-running Snowflake Scripting block.

The BigQuery export wait is controlled by dbt through `start_export` and
`poll_export` procedure actions. These actions keep Google API calls inside
Snowflake external access, while dbt controls the poll cadence.

The dbt materialization creates or replaces the exposed dbt view after a
successful load commit. Because dbt/Jinja cannot catch failed Snowflake
statements, the dbt-side path cannot guarantee cleanup SQL after an uncaught
`COPY INTO` or view-creation failure. It also avoids dropping an internal table
after `CREATE ICEBERG TABLE IF NOT EXISTS`, because a concurrent run may have
created that table between the initial existence check and the create statement.
The legacy full-run procedure path keeps the prior
`iceberg_sync_cleanup_created_table_on_failure` behavior.

The run log includes `retry` and `cleanup` objects for compatibility with prior
versions. In the dbt-side load path, `retry.attempts` is `1` because Snowflake
load retries are not performed inside the materialization.

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
| `TIMESTAMP` | `TIMESTAMP_NTZ(6)` |
| `NUMERIC`, `DECIMAL` | `NUMBER(38,9)` |
| `BYTES` | `BINARY` |
| `RECORD`, `STRUCT` | structured `OBJECT(...)` |
| repeated compatible fields | structured `ARRAY(...)` |

BigQuery `TIMESTAMP` and `DATETIME` both map to Snowflake `TIMESTAMP_NTZ(6)`.
BigQuery EXTRACT to Parquet writes these fields with `isAdjustedToUTC = false`.
Snowflake's vectorized Parquet scanner used by Iceberg
`COPY INTO ... LOAD_MODE = ADD_FILES_COPY` accepts that metadata only for
`TIMESTAMP_NTZ` (not `TIMESTAMP_LTZ`). Values are stored as UTC wall-clock
timestamps without a session time zone; treat them as UTC or use
`CONVERT_TIMEZONE` in downstream models.

This mapping is a breaking change from earlier package versions that mapped
`TIMESTAMP` to `TIMESTAMP_LTZ(6)`. Existing Iceberg tables created with the old
mapping will fail additive schema-evolution checks and need a full refresh /
recreate. The `select` strategy with `DATETIME(column)` casts remains valid but
is no longer required to avoid UTC-adjustment COPY failures on TIMESTAMP-only
sources.

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
FROM @<named_stage>/<load_prefix>/
FILE_FORMAT = (TYPE = PARQUET USE_VECTORIZED_SCANNER = TRUE)
LOAD_MODE = ADD_FILES_COPY
MATCH_BY_COLUMN_NAME = CASE_SENSITIVE
PURGE = FALSE
-- s3_parquet also sets FORCE = TRUE and may set FILES = (...)
```

BigQuery exports use a per-run stage prefix. S3 Parquet loads read the
configured stage location (and optional path suffixes) directly.
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
DBT_SNOWFLAKE_ICEBERG_SYNC_WORKLOAD_IDENTITY_FEDERATION_SECRET_FQDN
DBT_SNOWFLAKE_ICEBERG_SYNC_WORKLOAD_IDENTITY_FEDERATION_AUDIENCE
DBT_SNOWFLAKE_ICEBERG_SYNC_WORKLOAD_IDENTITY_FEDERATION_SERVICE_ACCOUNT
DBT_SNOWFLAKE_ICEBERG_SYNC_WIF_TRANSFER_BIGQUERY_PROJECT_ID
DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_STAGING_DATASET_ID
DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_PROJECT_ID
DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_LOCATION
DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_DATASET_ID
DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_TABLE_ID
DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_TABLE_EXPECTED_ROWS
DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_DATETIME_TABLE_ID
DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_DATETIME_EXPECTED_ROWS
DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_DATETIME_EXPECTED_VALUES
# Optional TIMESTAMP extract fixture (test skips when TABLE_ID is unset):
DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_TIMESTAMP_TABLE_ID
DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_TIMESTAMP_EXPECTED_ROWS
DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_TIMESTAMP_EXPECTED_VALUES
DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_TIMESTAMP_COLUMN

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

DBT_SNOWFLAKE_ICEBERG_SYNC_S3_PARQUET_STAGE
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
- Google Cloud service account JSON should live in a Snowflake secret.
- External access integrations, network rules, stages, and IAM permissions are
  managed by the user.
- BigQuery credentials are not included in the procedure call payload.
- Exported GCS files are not cleaned up by first-scope code. Use a GCS lifecycle
  policy or add explicit cleanup in a later release.
