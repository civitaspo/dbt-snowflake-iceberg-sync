# Changelog

## 0.3.0 - 2026-06-17

### Changed

- Moved Snowflake relation state checks, Iceberg table DDL, additive schema
  changes, load transaction SQL, target view creation, BigQuery export polling,
  and success run-log writes to the dbt materialization side.
- Split the package-managed procedure path so dbt can start and poll BigQuery
  exports while keeping Google API calls inside Snowflake external access.
- Removed the dbt-side `EXECUTE IMMEDIATE 'DECLARE ...'` load retry block. The
  materialization now issues plain `BEGIN`, `DELETE`, `COPY INTO`, and `COMMIT`
  SQL in the dbt-controlled load path and leaves failed Snowflake load retries
  to the dbt job/orchestrator layer.
- Documented that uncaught Snowflake statement failures in the dbt-side path
  cannot run Jinja-side rollback, retry, or cleanup SQL.
- Avoided dbt-side failure cleanup that could drop an internal Iceberg table
  when ownership cannot be proven under concurrent initial runs.

## 0.2.3 - 2026-06-09

### Added

- Added configurable BigQuery Parquet export compression with `ZSTD` as the
  default and support for `NONE`, `SNAPPY`, `GZIP`, and `ZSTD`.

## 0.2.2 - 2026-06-09

### Changed

- Switched package-managed run-log table setup to `CREATE OR ALTER TABLE`
  instead of separate create and alter statements.
- Disabled dbt's outer transaction wrapper for the materialization procedure
  call so run-log writes remain standalone procedure statements.

## 0.2.1 - 2026-06-09

### Changed

- Made shared run-log writes best-effort by default with retry for Snowflake
  lock-contention failures, while keeping strict behavior available through
  `iceberg_sync_run_log_fail_on_error`.
- Moved run-log table setup to install-time deployment instead of each
  materialization run.
- Added materialization-level retry around the outer stored procedure call for
  stable transient Snowflake error messages.
- Hardened load transaction cleanup so rollback failures do not mask the
  original load error.

## 0.2.0 - 2026-06-09

### Added

- Added dbt Fusion package compatibility metadata and CI parse coverage with a
  pinned Fusion CLI.
- Added Fusion-backed release validation and opt-in integration test support via
  `DBT_SNOWFLAKE_ICEBERG_SYNC_DBT_EXECUTABLE`.

### Changed

- Read materialization options from `meta.iceberg_sync` first so package-specific
  model configs are accepted by dbt Fusion.
- Kept legacy top-level materialization config keys readable for existing dbt
  Core projects.
- Documented the Fusion-safe model config shape and absolute
  `handler_local_path` guidance for Snowflake procedure uploads.

## 0.1.4 - 2026-06-09

### Added

- Added configurable retry handling for retryable Snowflake load transaction
  failures.
- Added failed-initial-run cleanup for newly created internal Iceberg tables.
- Persisted retry and cleanup metadata in procedure results and run logs.

### Changed

- Moved exposed target view creation into the Snowflake procedure after a
  successful load commit.
- Treated missing internal Iceberg tables or exposed target views as full
  refresh runs.
- Narrowed Snowflake retry classification to messages containing
  `SQL execution internal error` or `incident`.

## 0.1.3 - 2026-06-08

### Changed

- Added first-class BigQuery `DATETIME` schema support, mapping it to Snowflake
  `TIMESTAMP_NTZ(6)`.
- Kept `TIME`, `GEOGRAPHY`, `JSON`, `BIGNUMERIC`, and `BIGDECIMAL`
  unsupported.
- Added unit and opt-in integration coverage for `DATETIME` extract behavior.

## 0.1.2 - 2026-06-08

### Changed

- Defaulted package-managed Snowflake procedure and helper objects to the active
  dbt target database and schema unless explicitly configured.
- Rendered procedure references as quoted Snowflake fully qualified names while
  avoiding unsupported dbt relation types.
- Used `CREATE OR ALTER PROCEDURE` for procedure deployment.
- Quoted the Google Cloud service account secret fully qualified name and
  normalized external access integration identifiers.
- Enabled mise caching in GitHub Actions workflows to reduce repeated tool
  downloads.

## 0.1.1 - 2026-06-08

### Changed

- Documented materialization option requirements by common, Iceberg table, and
  BigQuery source option groups.
- Normalized package-managed Snowflake object identifiers to uppercase for
  unquoted-identifier compatibility.
- Preserved BigQuery and Parquet source column case for
  `MATCH_BY_COLUMN_NAME = CASE_SENSITIVE` loads.

## 0.1.0 - 2026-06-07

### Added

- Initial `iceberg_sync` dbt materialization for loading BigQuery exports into
  Snowflake-managed Iceberg tables.
- Snowflake Python procedure installer and runtime for BigQuery extract and
  query-export workflows.
- Support for non-partitioned, partition-decorator, table-suffix, and query
  predicate export plans.
- Incremental `delete+copy` loading with separate source export predicates and
  Snowflake delete predicates.
- BigQuery staging table reuse and forced rebuild controls for query exports.
- BigQuery-to-Snowflake schema mapping, additive schema evolution, view alias
  generation, and run-log recording.
- Validation guardrails for unsupported materialization settings, unsafe stage
  locations, and credential-like model configuration.
- Opt-in live integration test coverage, default mocked unit tests, pinned CI,
  workflow linting, release workflow, and GitHub Sponsors funding metadata.
