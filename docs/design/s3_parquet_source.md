# Design: S3 Parquet Source (`source_type: s3_parquet`)

## Motivation

`iceberg_sync` originally synced BigQuery tables by exporting Parquet to a
GCS-backed Snowflake stage and loading into a Snowflake-managed Iceberg table.
Many pipelines already land Iceberg-compatible Parquet files on Amazon S3.
Users should be able to point `iceberg_sync` at those files without a BigQuery
export step.

S3 access is granted through a user-managed Snowflake Storage Integration
attached to the named stage (and separately to the Iceberg external volume).
This package does not store AWS credentials and does not call the AWS API.

## High-level flow

```text
dbt model (source_type='s3_parquet')
  -> collect/validate s3_parquet config
  -> DESC STAGE (must be s3:// / s3gov:// / s3china://)
  -> CALL procedure start_export:
       LIST matching files
       INFER_SCHEMA(..., KIND => 'ICEBERG') via named Parquet file format
  -> CREATE ICEBERG TABLE IF NOT EXISTS / additive ALTER
  -> BEGIN
       DELETE (full refresh or incremental_predicate)
       COPY INTO ... LOAD_MODE=ADD_FILES_COPY|FULL_INGEST FORCE=TRUE [FILES=...]
         (FULL_INGEST may use SELECT transforms from columns.expression)
     COMMIT
  -> CREATE OR REPLACE VIEW
  -> INSERT run log
```

There is no asynchronous export. `start_export` completes in one call and
`poll_export` is a no-op pass-through so the existing dbt wait loop exits
immediately.

## Configuration

Model options under `meta.iceberg_sync`:

| Option | Required | Default | Description |
| --- | --- | --- | --- |
| `source_type` | Yes | — | `s3_parquet` |
| `s3_parquet_location` | Yes | — | `@DB.SCHEMA.STAGE[/prefix]` named stage |
| `s3_parquet_file_pattern` | No | none | Regex relative to each load location; matched files are loaded via COPY `FILES` |
| `s3_parquet_full_refresh_paths` | No | `['']` | Subpaths under the location for full refresh |
| `s3_parquet_incremental_paths` | No | `['']` | Subpaths for incremental runs |
| `s3_parquet_skip_missing_location` | No | `false` | Skip the run when no files match |
| `s3_parquet_infer_schema_max_file_count` | No | `16` | Cap on files passed to `INFER_SCHEMA` |
| `s3_parquet_load_mode` | No | `add_files_copy` | `add_files_copy` binary-copies Iceberg-compatible Parquet; `full_ingest` scans and rewrites files (needed for non-Iceberg Parquet such as AWS CUR `TIMESTAMP_MILLIS`) |
| `columns` | No | infer | Shared declared Iceberg columns; skips `INFER_SCHEMA` when set |

Deployment var:

| Var | Required | Default | Description |
| --- | --- | --- | --- |
| `vars.iceberg_sync.parquet_file_format` | No | `<procedure_database>.<procedure_schema>.ICEBERG_SYNC_PARQUET_FILE_FORMAT` | Named Parquet file format created by the installer |

`FORCE` is always `TRUE` and `PURGE` is always `FALSE` for this source. They
are not model-configurable.

## Schema detection

By default, schema comes from Snowflake `INFER_SCHEMA` with `KIND => 'ICEBERG'`,
ordered by `ORDER_ID`. Column names stay case-sensitive for Iceberg DDL and
`MATCH_BY_COLUMN_NAME = CASE_SENSITIVE`. View aliases continue to use
lower_snake conversions. `CREATE FILE FORMAT` still uses the deployment quoted
identifier FQN; `INFER_SCHEMA(..., FILE_FORMAT => ...)` must pass that same
object as a string literal (for example `'DB.SCHEMA.FMT'`), not as a bare
identifier.

Alternatively, set shared `columns` under `meta.iceberg_sync` to declare the
Iceberg table columns explicitly and skip `INFER_SCHEMA`. The same option
overrides BigQuery schema mapping for `source_type: bigquery`. Each entry needs
`name` (Parquet / Iceberg column name) and `type` (Snowflake DDL type). Optional
fields:

- `nullable` (default `true`)
- `alias` (view alias; default `lower_snake(name)`)
- `expression` (view SELECT expression; default quoted `name`)

`expression` is applied only on the exposed target view when
`s3_parquet_load_mode='add_files_copy'` (the default), so authors can cast or
transform values without changing the internal Iceberg table DDL.

With `s3_parquet_load_mode='full_ingest'`, any non-empty `expression` is instead
applied in the COPY `SELECT` list so Snowflake can rewrite Parquet into
Iceberg-compatible files (for example AWS CUR timestamps). Expressions should
reference staged Parquet fields as `$1:"ColumnName"`. Columns without an
expression default to `$1:"ColumnName"`. The target view then exposes the loaded
Iceberg columns without re-applying those expressions.

Example for non-Iceberg Parquet (FULL_INGEST):

```yaml
source_type: s3_parquet
s3_parquet_location: '@ANALYTICS.PUBLIC.CUR_STAGE/cur'
s3_parquet_load_mode: full_ingest
columns:
  - name: line_item_usage_start_date
    type: TIMESTAMP_LTZ(6)
    alias: line_item_usage_start_date
  - name: line_item_unblended_cost
    type: DOUBLE
    expression: 'TRY_TO_DOUBLE($1:"line_item_unblended_cost")'
```

When `full_ingest` is set and no column has `expression`, COPY still uses
`MATCH_BY_COLUMN_NAME = CASE_SENSITIVE` and lets Snowflake convert compatible
physical/logical types during rewrite.

`columns` is read only from `meta.iceberg_sync` so it does not collide with dbt
`config.columns` / schema.yml documentation. When `columns` is set for an S3
model, `vars.iceberg_sync.parquet_file_format` is not required. When it is
omitted, schema inference still requires the installer-managed Parquet file
format.

Limits:

- Schema evolution remains additive-only (same conservative rule as BigQuery).
- Files under a load must share a compatible Parquet schema when relying on
  `INFER_SCHEMA`.
- Nested / structured type spellings depend on live `INFER_SCHEMA` output and
  are normalized where needed. Declared `type` values are passed through to
  Iceberg DDL after light normalization.

## Load semantics and FORCE

Snowflake COPY load history would otherwise skip previously loaded files.
Combined with `delete+copy`, that would delete Iceberg rows and then load
nothing on a repeated run over the same prefix. `FORCE = TRUE` reloads the
matched files every run. `PURGE = FALSE` keeps user-owned source objects.

`ADD_FILES_COPY` supports cross-cloud and cross-region server-side copies, so
an S3 stage can feed an Iceberg external volume on another cloud.

## Incremental pairing

If `s3_parquet_incremental_paths` is customized away from the default `['']`,
`incremental_predicate` must also be set, and vice versa. This mirrors the
BigQuery predicate pairing rule.

## Out of scope

- Non-Parquet formats
- Event-driven / Snowpipe ingestion
- Package-managed Storage Integration or IAM role creation
- GCS-native Parquet source (`gcs_parquet`) — left for a later source type
- Load-time row transforms with `LOAD_MODE = ADD_FILES_COPY` (use
  `s3_parquet_load_mode='full_ingest'` plus `columns[].expression`, or view
  `expression` for post-load casts)

## Compatibility notes

- Existing BigQuery models are unchanged.
- Google Cloud deployment secrets are optional at install time and required only
  when compiling/running `source_type: bigquery` models with
  `google_cloud_auth_method='service_account_credentials_json'`.
- Re-run `install_iceberg_sync_procedure()` (typically via `on-run-start`) so
  Snowflake picks up `sources/s3_parquet.py` and the Parquet file format.
