from __future__ import annotations

import os

import pytest

pytestmark = pytest.mark.integration

_REQUIRED_ENV_VARS = [
    "SNOWFLAKE_ACCOUNT",
    "SNOWFLAKE_USER",
    "DBT_SNOWFLAKE_ICEBERG_SYNC_WIF_SECRET_FQDN",
    "DBT_SNOWFLAKE_ICEBERG_SYNC_WIF_AUDIENCE",
    "DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_PROJECT_ID",
    "DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_DATASET_ID",
    "DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_TABLE_ID",
]

_OPTIONAL_SESSION_CONFIGS = [
    ("password", "SNOWFLAKE_PASSWORD"),
    ("private_key_file", "SNOWFLAKE_PRIVATE_KEY_PATH"),
    ("authenticator", "SNOWFLAKE_AUTHENTICATOR"),
    ("role", "SNOWFLAKE_ROLE"),
    ("warehouse", "SNOWFLAKE_WAREHOUSE"),
    ("database", "SNOWFLAKE_DATABASE"),
    ("schema", "SNOWFLAKE_SCHEMA"),
]


def _skip_unless_configured() -> None:
    if os.getenv("DBT_SNOWFLAKE_ICEBERG_SYNC_RUN_INTEGRATION") != "1":
        pytest.skip("Live Snowflake, BigQuery, and GCS integration tests are opt-in.")
    missing = [name for name in _REQUIRED_ENV_VARS if not os.getenv(name)]
    if missing:
        pytest.skip(
            "Missing workload identity federation integration environment variables: "
            + ", ".join(missing)
        )


def _open_snowflake_session():
    snowpark = pytest.importorskip(
        "snowflake.snowpark",
        reason="snowflake-snowpark-python is required: uv sync --group integration",
    )
    configs = {
        "account": os.environ["SNOWFLAKE_ACCOUNT"],
        "user": os.environ["SNOWFLAKE_USER"],
    }
    for key, env_name in _OPTIONAL_SESSION_CONFIGS:
        value = os.getenv(env_name)
        if value:
            configs[key] = value
    return snowpark.Session.builder.configs(configs).create()


def test_workload_identity_federation_credentials_authorize_bigquery_calls():
    _skip_unless_configured()

    from google.auth.transport.requests import Request

    from procedure.config import DeploymentConfig
    from procedure.gcp_auth import build_gcp_credentials
    from procedure.sources.bigquery import BigQueryRestClient

    deployment = DeploymentConfig(
        gcp_auth_method="workload_identity_federation",
        gcp_wif_secret_fqdn=os.environ["DBT_SNOWFLAKE_ICEBERG_SYNC_WIF_SECRET_FQDN"],
        gcp_wif_audience=os.environ["DBT_SNOWFLAKE_ICEBERG_SYNC_WIF_AUDIENCE"],
        gcp_service_account_impersonation=os.getenv(
            "DBT_SNOWFLAKE_ICEBERG_SYNC_WIF_SERVICE_ACCOUNT_IMPERSONATION"
        ),
    )

    session = _open_snowflake_session()
    try:
        credentials = build_gcp_credentials(
            session,
            deployment,
            secret_reader=lambda alias: pytest.fail(
                f"workload identity federation must not read generic secrets: {alias}"
            ),
        )

        credentials.refresh(Request())
        assert credentials.valid
        assert credentials.token

        client = BigQueryRestClient(credentials)
        table_id = os.environ["DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_TABLE_ID"]
        table = client.get_table(
            os.environ["DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_PROJECT_ID"],
            os.environ["DBT_SNOWFLAKE_ICEBERG_SYNC_BIGQUERY_DATASET_ID"],
            table_id,
        )
        assert table.get("tableReference", {}).get("tableId") == table_id
    finally:
        session.close()
