# Changelog

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
