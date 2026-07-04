import pytest

from procedure.config import ConfigError, DeploymentConfig, IcebergSyncConfig


def base_config(**overrides):
    value = {
        "source_type": "bigquery",
        "materialization_strategy": "incremental",
        "bigquery_export_strategy": "extract",
        "google_cloud_project_id": "project",
        "bigquery_dataset_id": "dataset",
        "bigquery_table_id": "events",
        "bigquery_location": "US",
        "bigquery_export_location": "@DB.SCHEMA.STAGE/prefix",
        "iceberg_table_external_volume": "ICEBERG_VOLUME",
        "target_relation": {
            "database": "DB",
            "schema": "MART",
            "identifier": "EVENTS",
            "rendered": '"DB"."MART"."EVENTS"',
        },
        "internal_relation": {
            "database": "DB",
            "schema": "MART",
            "identifier": "__EVENTS",
            "rendered": '"DB"."MART"."__EVENTS"',
        },
        "deployment": {
            "procedure_database": "DB",
            "procedure_schema": "UTIL",
            "procedure_name": "ICEBERG_SYNC",
        },
    }
    value.update(overrides)
    return value


def test_effective_mode_prefers_full_refresh_flag():
    config = IcebergSyncConfig.from_dict(base_config(dbt_full_refresh=True))

    assert config.effective_mode(internal_table_exists=True) == "full_refresh"


def test_effective_mode_full_refresh_when_internal_table_missing():
    config = IcebergSyncConfig.from_dict(base_config())

    assert config.effective_mode(internal_table_exists=False) == "full_refresh"


def test_incremental_pairing_requires_both_predicates():
    config = IcebergSyncConfig.from_dict(
        base_config(bigquery_export_incremental_predicates=["_PARTITIONDATE = DATE '2026-01-01'"])
    )

    with pytest.raises(ConfigError, match="both present or both absent"):
        config.validate_incremental_pairing("incremental")


def test_secret_like_model_config_is_rejected():
    with pytest.raises(ConfigError, match="Credential material"):
        IcebergSyncConfig.from_dict(base_config(gcp_sa_secret_fqdn="DB.SECRET.SA"))


def test_wif_model_config_is_rejected():
    for key in ("gcp_auth_method", "gcp_wif_secret_fqdn", "gcp_wif_audience"):
        with pytest.raises(ConfigError, match="Credential material"):
            IcebergSyncConfig.from_dict(base_config(**{key: "anything"}))


def test_unknown_gcp_auth_method_is_rejected():
    with pytest.raises(ConfigError, match="gcp_auth_method"):
        DeploymentConfig.from_dict({"gcp_auth_method": "oidc"})


def test_wif_requires_secret_and_audience():
    with pytest.raises(ConfigError, match="gcp_wif_secret_fqdn, gcp_wif_audience"):
        DeploymentConfig.from_dict({"gcp_auth_method": "workload_identity_federation"})

    with pytest.raises(ConfigError, match="gcp_wif_audience"):
        DeploymentConfig.from_dict(
            {
                "gcp_auth_method": "workload_identity_federation",
                "gcp_wif_secret_fqdn": "DB.AUTH.GCP_WIF",
            }
        )


def test_wif_keys_require_wif_auth_method():
    with pytest.raises(ConfigError, match="only valid with"):
        DeploymentConfig.from_dict({"gcp_wif_audience": "//iam.googleapis.com/projects/1/x"})

    with pytest.raises(ConfigError, match="only valid with"):
        DeploymentConfig.from_dict(
            {"gcp_service_account_impersonation": "sync@example-project.iam.gserviceaccount.com"}
        )


def test_valid_wif_deployment_parses():
    deployment = DeploymentConfig.from_dict(
        {
            "gcp_auth_method": "workload_identity_federation",
            "gcp_wif_secret_fqdn": "DB.AUTH.GCP_WIF",
            "gcp_wif_audience": "//iam.googleapis.com/projects/000000000000/locations/global/workloadIdentityPools/pool/providers/provider",
            "gcp_service_account_impersonation": "sync@example-project.iam.gserviceaccount.com",
        }
    )

    assert deployment.gcp_auth_method == "workload_identity_federation"
    assert deployment.gcp_wif_secret_fqdn == "DB.AUTH.GCP_WIF"


def test_partition_and_cluster_are_rejected():
    with pytest.raises(ConfigError, match="partition_by"):
        IcebergSyncConfig.from_dict(base_config(partition_by=["event_date"]))

    with pytest.raises(ConfigError, match="cluster_by"):
        IcebergSyncConfig.from_dict(base_config(cluster_by=["customer_id"]))


def test_select_strategy_requires_staging_dataset_and_sql():
    with pytest.raises(ConfigError, match="bigquery_staging_dataset_id"):
        IcebergSyncConfig.from_dict(base_config(bigquery_export_strategy="select", model_sql="select 1"))

    with pytest.raises(ConfigError, match="Model SQL"):
        IcebergSyncConfig.from_dict(
            base_config(
                bigquery_export_strategy="select",
                bigquery_staging_dataset_id="staging",
                model_sql="",
            )
        )
