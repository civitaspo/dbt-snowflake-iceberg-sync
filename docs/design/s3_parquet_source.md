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
       COPY INTO ... LOAD_MODE=ADD_FILES_COPY FORCE=TRUE [PATTERN=...]
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
| `s3_parquet_file_pattern` | No | none | Regex relative to each load location |
| `s3_parquet_full_refresh_paths` | No | `['']` | Subpaths under the location for full refresh |
| `s3_parquet_incremental_paths` | No | `['']` | Subpaths for incremental runs |
| `s3_parquet_skip_missing_location` | No | `false` | Skip the run when no files match |
| `s3_parquet_infer_schema_max_file_count` | No | `16` | Cap on files passed to `INFER_SCHEMA` |

Deployment var:

| Var | Required | Default | Description |
| --- | --- | --- | --- |
| `vars.iceberg_sync.parquet_file_format` | No | `<procedure_database>.<procedure_schema>.ICEBERG_SYNC_PARQUET_FILE_FORMAT` | Named Parquet file format created by the installer |

`FORCE` is always `TRUE` and `PURGE` is always `FALSE` for this source. They
are not model-configurable.

## Schema detection

Schema comes from Snowflake `INFER_SCHEMA` with `KIND => 'ICEBERG'`, ordered by
`ORDER_ID`. Column names stay case-sensitive for Iceberg DDL and
`MATCH_BY_COLUMN_NAME = CASE_SENSITIVE`. View aliases continue to use
lower_snake conversions.

Limits:

- Schema evolution remains additive-only (same conservative rule as BigQuery).
- Files under a load must share a compatible Parquet schema.
- Nested / structured type spellings depend on live `INFER_SCHEMA` output and
  are normalized where needed.

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

- User-declared column maps instead of `INFER_SCHEMA`
- Non-Parquet formats
- Event-driven / Snowpipe ingestion
- Package-managed Storage Integration or IAM role creation
- GCS-native Parquet source (`gcs_parquet`) — left for a later source type

## Compatibility notes

- Existing BigQuery models are unchanged.
- Google Cloud deployment secrets are optional at install time and required only
  when compiling/running `source_type: bigquery` models with
  `google_cloud_auth_method='service_account_credentials_json'`.
- Re-run `install_iceberg_sync_procedure()` (typically via `on-run-start`) so
  Snowflake picks up `sources/s3_parquet.py` and the Parquet file format.
