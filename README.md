# dbt-snowflake-iceberg-sync

`dbt-snowflake-iceberg-sync` is a dbt package for syncing source data exported from BigQuery into Snowflake-managed Iceberg tables.

The package exposes a Snowflake-only materialization named `iceberg_sync`. The dbt model relation is a Snowflake view, while the physical storage table is an internal Snowflake-managed Iceberg table in the same database and schema with the model identifier prefixed by `__`.

## Status

This package is early OSS implementation work. The first supported source type is BigQuery, and the first supported Snowflake load mode is Parquet `COPY INTO ... LOAD_MODE = ADD_FILES_COPY`.

## Requirements

- dbt Core 1.10 or later, below 2.0.
- Snowflake with managed Iceberg table support.
- A Snowflake external volume for the managed Iceberg table.
- A Snowflake stage backed by GCS for BigQuery export files.
- A Snowflake external access integration and GCP credentials that allow the Python procedure to call BigQuery APIs, either a service account key JSON in a Snowflake secret or Snowflake outbound workload identity federation.
- BigQuery tables or SQL queries that can be exported to Parquet.

## Installation

Add the package to your dbt project's `packages.yml`:

```yaml
packages:
  - git: "https://github.com/civitaspo/dbt-snowflake-iceberg-sync.git"
    revision: v0.1.0
```

Install dependencies:

```bash
dbt deps
```

## Snowflake Procedure Setup

The materialization delegates source export, schema mapping, Iceberg DDL, `DELETE`, `COPY`, and run logging to a Snowflake Python procedure. Configure deployment-level values through dbt vars, not model config:

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

    # GCP authentication. gcp_auth_method defaults to service_account_key.
    gcp_auth_method: service_account_key
    gcp_sa_secret_fqdn: ANALYTICS.SECRETS.GCP_SA_CREDENTIALS_JSON
    gcp_sa_secret_alias: gcp_sa_credentials_json
```

Install or replace the procedure from `on-run-start` or a one-off `dbt run-operation`:

```yaml
on-run-start:
  - "{{ dbt_snowflake_iceberg_sync.install_iceberg_sync_procedure() }}"
```

The installer uploads the package's `procedure/` directory to an internal Snowflake stage and creates a Python procedure with directory imports.

## GCP IAM Setup

The GCP identity used by the procedure (the service account stored in the Snowflake secret, or the workload identity federation principal) needs BigQuery permissions to inspect tables, create query jobs, create or update staging tables for `select` exports, and run extract jobs. It also needs permission to write exported Parquet files to the GCS bucket behind the Snowflake stage.

Do not put service account JSON, private keys, passwords, or Snowflake secret FQDNs in model config.

## GCP Workload Identity Federation

As an alternative to a static service account key, the procedure can authenticate to GCP with Snowflake outbound workload identity federation (WIF). Snowflake issues a short-lived JWT with `SYSTEM$ISSUE_WORKLOAD_IDENTITY_FEDERATION_TOKEN`, the procedure exchanges it at Google STS through `google.auth.identity_pool.Credentials`, and optionally impersonates a GCP service account. No key material is stored anywhere.

dbt configuration:

```yaml
vars:
  iceberg_sync:
    # ... procedure and stage settings as above ...
    external_access_integrations: [BIGQUERY_API]

    gcp_auth_method: workload_identity_federation
    gcp_wif_secret_fqdn: ANALYTICS.SECRETS.GCP_WIF
    gcp_wif_audience: //iam.googleapis.com/projects/<project_number>/locations/global/workloadIdentityPools/<pool_id>/providers/<provider_id>
    # Optional. Omit to use the federated token directly, which requires IAM
    # roles granted to the workload identity pool principal itself.
    gcp_service_account_impersonation: <service_account_email>
```

### Per-target WIF configuration

dbt renders jinja only in top-level string vars; values nested inside the `iceberg_sync` map reach the package unrendered. When WIF values differ per environment (for example a per-target GCP project number in the audience), set them through dedicated top-level vars instead. Each of the four auth keys can be overridden by a top-level var named `iceberg_sync_<key>`; a set and non-empty top-level var takes precedence over the nested `vars.iceberg_sync` entry:

```yaml
vars:
  iceberg_sync:
    # ... static deployment settings ...
    gcp_auth_method: workload_identity_federation
    gcp_wif_secret_fqdn: ANALYTICS.SECRETS.GCP_WIF

  # Top-level vars are jinja-rendered with target context.
  iceberg_sync_gcp_wif_audience: "{{ '//iam.googleapis.com/projects/<dev_project_number>/locations/global/workloadIdentityPools/<pool_id>/providers/<provider_id>' if target.name == 'dev' else '//iam.googleapis.com/projects/<prd_project_number>/locations/global/workloadIdentityPools/<pool_id>/providers/<provider_id>' }}"
  iceberg_sync_gcp_service_account_impersonation: "sync-{{ target.name }}@<project_id>.iam.gserviceaccount.com"
```

The overrides also apply to `iceberg_sync_gcp_auth_method` and `iceberg_sync_gcp_wif_secret_fqdn`, and they can equally be passed on the command line with `--vars`.

`gcp_sa_secret_fqdn` is not required with WIF, and the installer does not bind a `SECRETS = (...)` clause to the procedure. WIF secrets cannot be read with `_snowflake.get_generic_secret_string`; the token is issued per run through the Snowpark session, so the calling role needs USAGE on the WIF secret (the procedure runs `EXECUTE AS CALLER`).

### Snowflake setup

Create a WIF secret and read its issuer and subject:

```sql
CREATE SECRET ANALYTICS.SECRETS.GCP_WIF TYPE = WORKLOAD_IDENTITY_FEDERATION;
DESC SECRET ANALYTICS.SECRETS.GCP_WIF;
-- Note workload_identity_federation_issuer and workload_identity_federation_subject.
GRANT USAGE ON SECRET ANALYTICS.SECRETS.GCP_WIF TO ROLE <dbt_role>;
```

The external access integration used by the procedure must allow egress to `sts.googleapis.com:443` (always) and `iamcredentials.googleapis.com:443` (only when `gcp_service_account_impersonation` is set), in addition to the existing BigQuery endpoints, and the WIF secret must be listed in the integration's `ALLOWED_AUTHENTICATION_SECRETS`:

```sql
CREATE OR REPLACE NETWORK RULE GCP_APIS
  MODE = EGRESS
  TYPE = HOST_PORT
  VALUE_LIST = (
    'bigquery.googleapis.com:443',
    'sts.googleapis.com:443',
    'iamcredentials.googleapis.com:443'
  );

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION BIGQUERY_API
  ALLOWED_NETWORK_RULES = (GCP_APIS)
  ALLOWED_AUTHENTICATION_SECRETS = (ANALYTICS.SECRETS.GCP_WIF)
  ENABLED = TRUE;
```

### GCP setup

Create a workload identity pool and an OIDC provider that trusts the Snowflake secret, using the issuer and subject from `DESC SECRET`:

```bash
gcloud iam workload-identity-pools create <pool_id> \
  --project=<project_id> --location=global

gcloud iam workload-identity-pools providers create-oidc <provider_id> \
  --project=<project_id> --location=global \
  --workload-identity-pool=<pool_id> \
  --issuer-uri="<workload_identity_federation_issuer from DESC SECRET>" \
  --attribute-mapping="google.subject=assertion.sub"
```

When the provider defines no allowed audiences, GCP expects the JWT audience `https://iam.googleapis.com/projects/<project_number>/locations/global/workloadIdentityPools/<pool_id>/providers/<provider_id>`. The package derives it from `gcp_wif_audience` automatically (both the `//iam.googleapis.com/...` and `https://iam.googleapis.com/...` spellings are accepted).

Grant access to the federated principal, where the subject is the `workload_identity_federation_subject` from `DESC SECRET`:

```bash
# With impersonation: let the principal mint tokens for the service account.
gcloud iam service-accounts add-iam-policy-binding <service_account_email> \
  --project=<project_id> \
  --role="roles/iam.workloadIdentityUser" \
  --member="principal://iam.googleapis.com/projects/<project_number>/locations/global/workloadIdentityPools/<pool_id>/subject/<subject>"

# Without impersonation: grant BigQuery and GCS roles to the principal directly.
```

### Runtime requirements

The Snowflake procedure resolves `google-auth` from the Snowflake Anaconda channel; WIF requires google-auth 2.29.0 or later for custom subject token suppliers. Snowflake WIF tokens expire after 15 minutes; the package re-issues and re-exchanges tokens automatically when credentials refresh.

## BigQuery Extract Model

Use `bigquery_export_strategy='extract'` for concrete BigQuery tables, native partition decorators, or wildcard tables expanded into concrete tables.

```sql
{{
  config(
    materialized='iceberg_sync',
    source_type='bigquery',
    materialization_strategy='incremental',

    bigquery_export_strategy='extract',
    google_cloud_project_id='example-project',
    bigquery_dataset_id='analytics',
    bigquery_table_id='events',
    bigquery_location='US',
    bigquery_export_location='@ANALYTICS.UTIL.BQ_EXPORT_STAGE/events',

    bigquery_export_predicate_type='partition_decorator',
    bigquery_export_incremental_predicates=['20260529'],
    incremental_predicate="event_date = DATE '2026-05-29'",

    iceberg_table_external_volume='ICEBERG_VOLUME'
  )
}}

select 1 as placeholder
```

The SQL body is ignored for `extract` exports.

## BigQuery Select Model

Use `bigquery_export_strategy='select'` when the model body should be treated as BigQuery SQL. The procedure writes the query result into a deterministic expiring BigQuery staging table, then exports that table to Parquet.

```sql
{{
  config(
    materialized='iceberg_sync',
    source_type='bigquery',
    materialization_strategy='incremental',

    bigquery_export_strategy='select',
    google_cloud_project_id='example-project',
    bigquery_dataset_id='analytics',
    bigquery_table_id='events',
    bigquery_location='US',
    bigquery_export_location='@ANALYTICS.UTIL.BQ_EXPORT_STAGE/events',
    bigquery_export_predicate_type='where',
    bigquery_export_incremental_predicates=["event_date = DATE '2026-05-29'"],

    bigquery_staging_dataset_id='dbt_staging',
    bigquery_staging_table_expiration_hours=24,
    bigquery_staging_table_reuse=true,

    incremental_predicate="event_date = DATE '2026-05-29'",
    iceberg_table_external_volume='ICEBERG_VOLUME'
  )
}}

select
  *
from `example-project.analytics.events`
```

## Refresh Behavior

The procedure runs in full-refresh mode when dbt `--full-refresh` is set, when `materialization_strategy='full_refresh'`, or when the internal Iceberg table does not yet exist. Otherwise it runs incrementally.

In full-refresh mode the procedure deletes the full internal table before copying exported files. In incremental mode, BigQuery predicates and the Snowflake `incremental_predicate` must be both present or both absent. If both are absent, the procedure performs a full-table delete plus copy.

The exposed dbt relation is recreated as a view after a successful procedure run.

## Schema Support

Initial scalar mapping:

| BigQuery type | Snowflake type |
| --- | --- |
| `STRING` | `VARCHAR` |
| `INT64`, `INTEGER` | `NUMBER(38,0)` |
| `FLOAT64`, `FLOAT`, `DOUBLE` | `FLOAT` |
| `BOOL`, `BOOLEAN` | `BOOLEAN` |
| `DATE` | `DATE` |
| `TIMESTAMP` | `TIMESTAMP_LTZ(6)` |
| `NUMERIC`, `DECIMAL` | `NUMBER(38,9)` |
| `BYTES` | `BINARY` |
| `RECORD`, `STRUCT` | structured `OBJECT(...)` |
| repeated fields | structured `ARRAY(...)` |

Unsupported first-scope types include `BIGNUMERIC`, `DATETIME`, `GEOGRAPHY`, `JSON`, and `TIME`.

Storage columns preserve source field names exactly for `MATCH_BY_COLUMN_NAME = CASE_SENSITIVE`. The exposed view aliases top-level fields to lower-snake unquoted identifiers. Alias collisions fail before loading.

## Unsupported First-Scope Features

- `partition_by`
- `cluster_by`
- arbitrary `COPY INTO` transformations
- non-Parquet export files
- unstaged cloud URI loads
- generic `VARIANT` sinks for unsupported nested data

## Run Logs

When `vars.iceberg_sync.run_log_enabled` is true, the procedure creates a run log table in the configured procedure schema. The log captures run identifiers, target relations, effective mode, predicate payloads, export segments, BigQuery job references, Snowflake query ids, status, errors, and timestamps.

## Local Tests

Unit tests do not require Snowflake or BigQuery credentials:

```bash
uv run pytest
```

Live integration tests are opt-in:

```bash
DBT_SNOWFLAKE_ICEBERG_SYNC_RUN_INTEGRATION=1 uv run pytest -m integration
```

Integration tests use generic environment variables such as `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`, `SNOWFLAKE_PRIVATE_KEY_PATH`, `SNOWFLAKE_ROLE`, `SNOWFLAKE_WAREHOUSE`, `SNOWFLAKE_DATABASE`, `SNOWFLAKE_SCHEMA`, `DBT_SNOWFLAKE_ICEBERG_SYNC_STAGE`, `DBT_SNOWFLAKE_ICEBERG_SYNC_EXTERNAL_VOLUME`, `DBT_SNOWFLAKE_ICEBERG_SYNC_SECRET_FQDN`, `DBT_SNOWFLAKE_ICEBERG_SYNC_EXTERNAL_ACCESS_INTEGRATION`, `GOOGLE_APPLICATION_CREDENTIALS`, `DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_PROJECT_ID`, `DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_LOCATION`, `DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_DATASET_ID`, `DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_TABLE_ID`, `DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_PARTITIONED_TABLE_ID`, `DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_WILDCARD_TABLE_ID`, `DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_STAGING_DATASET_ID`, `DBT_SNOWFLAKE_ICEBERG_SYNC_GCS_BUCKET`, and `DBT_SNOWFLAKE_ICEBERG_SYNC_GCS_PREFIX`.

The workload identity federation integration test additionally uses `DBT_SNOWFLAKE_ICEBERG_SYNC_WIF_SECRET_FQDN`, `DBT_SNOWFLAKE_ICEBERG_SYNC_WIF_AUDIENCE`, and optionally `DBT_SNOWFLAKE_ICEBERG_SYNC_WIF_SERVICE_ACCOUNT`. It opens a real Snowpark session, which requires the opt-in dependency group:

```bash
uv sync --group integration
```

## Security Notes

- Credential material does not belong in dbt model config.
- GCP service account JSON should live in a Snowflake secret, or be avoided entirely with workload identity federation.
- Workload identity federation tokens are short-lived (15 minutes), only ever exist in procedure memory, and are never written to run logs or error messages.
- External access integrations and network rules are managed by the user.
- Exported GCS files are not cleaned up by first-scope code. Use a GCS lifecycle policy or implement cleanup in a later extension.
