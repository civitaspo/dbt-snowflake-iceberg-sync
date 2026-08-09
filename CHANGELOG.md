# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

## [0.6.0] - 2026-08-09


### Documentation

- point releasing guide at shared client-releases spec (#58)


### Features

- evolve nested OBJECT schema before ADD_FILES_COPY (#55)


### Maintenance

- bump pyasn1 from 0.6.3 to 0.6.4 (#53)
- Bump cryptography from 48.0.0 to 50.0.0 (#54)
- remove Integration Approval workflow (#59)
- bump securefix-server reusables for job summary links (#60)
- use securefix-server release workflow reusables (#57)
- add auto-approve and CSM release workflows (#56)

## [0.5.6] - 2026-07-19


### Bug Fixes

- map BigQuery TIMESTAMP → TIMESTAMP_NTZ for Parquet extract (v0.5.6) (#52)

## [0.5.5] - 2026-07-19


### Features

- separate BigQuery job project (`bigquery_job_project_id`) — v0.5.5 (#50)

## [0.5.4] - 2026-07-18


### Bug Fixes

- avoid hive month=1 LIST prefix collision (#47)


### Maintenance

- sync uv.lock for v0.5.4 release (#48)

## [0.5.3] - 2026-07-18


### Bug Fixes

- INFER_SCHEMA FILE_FORMAT string literal (v0.5.3) (#46)

## [0.5.2] - 2026-07-18


### Features

- s3_parquet full_ingest load mode with COPY expressions (#45)

## [0.5.1] - 2026-07-17


### Maintenance

- prepare v0.5.1 release (#44)


### Miscellaneous

- Add s3_parquet source type for staged Parquet loads (#43)

## [0.4.6] - 2026-07-09


### Bug Fixes

- absolute-ize relative handler_local_path for Fusion PUT (#42)

## [0.4.5] - 2026-07-09


### Bug Fixes

- read partition_by and cluster_by from meta.iceberg_sync (#40)


### Documentation

- remove skipped v0.4.1-v0.4.3 changelog entries (#39)


### Maintenance

- prepare v0.4.5 release (#41)

## [0.4.4] - 2026-07-05


### Features

- add workload identity federation by-dbt-target map (v0.4.1) (#36)


### Maintenance

- prepare v0.4.4 release (#38)
- prepare v0.4.2 release (#37)

## [0.4.0] - 2026-07-05


### Features

- add workload identity federation auth (#34)


### Miscellaneous

- Prepare v0.4.0 release (#35)

## [0.3.3] - 2026-06-24


### Miscellaneous

- Prepare v0.3.3 release
- Use ALTER ICEBERG TABLE for schema changes

## [0.3.2] - 2026-06-18


### Miscellaneous

- Document v0.3.2 BigQuery 404 handling
- Prepare v0.3.2 release
- Skip missing BigQuery extract tables

## [0.3.1] - 2026-06-18


### Miscellaneous

- Prepare v0.3.1 release
- Fix structured Snowflake type normalization

## [0.3.0] - 2026-06-17


### Miscellaneous

- Prepare v0.3.0 release
- Move Iceberg sync orchestration to dbt

## [0.2.3] - 2026-06-09


### Miscellaneous

- Prepare v0.2.3 release
- Support BigQuery Parquet export compression

## [0.2.2] - 2026-06-09


### Miscellaneous

- Simplify run-log setup and avoid dbt transaction wrapper

## [0.2.1] - 2026-06-09


### Miscellaneous

- Harden iceberg_sync concurrency retry

## [0.2.0] - 2026-06-09


### Miscellaneous

- Prepare v0.2.0 release
- Support dbt Fusion
- Fix iceberg_sync main statement execution (#13)

## [0.1.4] - 2026-06-08


### Miscellaneous

- Prepare v0.1.4 release
- Add procedure retry and cleanup handling

## [0.1.3] - 2026-06-08


### Miscellaneous

- Prepare v0.1.3 release
- Support BigQuery DATETIME schema mapping

## [0.1.2] - 2026-06-08


### Miscellaneous

- Prepare v0.1.2 release
- Default deployment objects to dbt target

## [0.1.1] - 2026-06-08


### Miscellaneous

- Prepare v0.1.1 release
- Normalize Snowflake object identifiers
- Document materialization options (#4)

## [0.1.0] - 2026-06-06


### Miscellaneous

- Update changelog for v0.1.0
- Add GitHub Sponsors funding config
- Implement Snowflake Iceberg sync materialization
- Initial commit


