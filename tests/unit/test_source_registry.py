from __future__ import annotations

import pytest

from procedure.config import parse_config
from procedure.errors import IcebergSyncError
from procedure.sources import registry


class FakeBigQueryRestClient:
    def __init__(self, credentials):
        self.credentials = credentials


def test_bigquery_adapter_builds_client_with_google_credentials(base_payload, monkeypatch):
    captured = {}
    credentials = object()

    def fake_build_credentials(session, deployment, secret_reader):
        captured["session"] = session
        captured["deployment"] = deployment
        captured["secret_reader"] = secret_reader
        return credentials

    monkeypatch.setattr(registry, "build_google_cloud_credentials", fake_build_credentials)
    monkeypatch.setattr(registry, "BigQueryRestClient", FakeBigQueryRestClient)

    adapter = registry.create_source_adapter(parse_config(base_payload))

    assert adapter.source_type == "bigquery"
    assert adapter.client.credentials is credentials
    assert captured["session"] is None
    assert captured["deployment"].google_cloud_service_account_secret_alias == (
        "google_cloud_service_account_credentials_json"
    )
    assert captured["secret_reader"] is registry.load_snowflake_secret


def test_bigquery_adapter_passes_session_for_workload_identity_federation(
    payload_factory, monkeypatch
):
    config = parse_config(
        payload_factory(
            deployment__google_cloud_auth_method="workload_identity_federation",
            deployment__google_cloud_workload_identity_federation_secret_fqdn="DB.AUTH.WORKLOAD_IDENTITY_FEDERATION_SECRET",
            deployment__google_cloud_workload_identity_federation_audience=(
                "//iam.googleapis.com/projects/000000000000/locations/global/"
                "workloadIdentityPools/example-pool/providers/example-provider"
            ),
        )
    )
    session = object()
    captured = {}
    credentials = object()

    def fake_build_credentials(passed_session, deployment, secret_reader):
        captured["session"] = passed_session
        captured["deployment"] = deployment
        captured["secret_reader"] = secret_reader
        return credentials

    monkeypatch.setattr(registry, "build_google_cloud_credentials", fake_build_credentials)
    monkeypatch.setattr(registry, "BigQueryRestClient", FakeBigQueryRestClient)

    adapter = registry.create_source_adapter(config, session=session)

    assert adapter.client.credentials is credentials
    assert captured["session"] is session
    assert captured["deployment"].google_cloud_auth_method == "workload_identity_federation"
    assert captured["secret_reader"] is registry.load_snowflake_secret


def test_bigquery_adapter_surfaces_auth_errors(payload_factory, monkeypatch):
    config = parse_config(
        payload_factory(deployment__google_cloud_service_account_secret_alias=None)
    )

    monkeypatch.setattr(
        registry,
        "build_google_cloud_credentials",
        lambda session, deployment, secret_reader: (_ for _ in ()).throw(
            IcebergSyncError("auth failed")
        ),
    )

    with pytest.raises(IcebergSyncError, match="auth failed"):
        registry.create_source_adapter(config)


def test_create_source_adapter_honors_explicit_empty_factory_map(base_payload):
    with pytest.raises(IcebergSyncError, match="unsupported source_type"):
        registry.create_source_adapter(parse_config(base_payload), factories={})


def test_s3_parquet_adapter_requires_session(s3_parquet_payload):
    with pytest.raises(IcebergSyncError, match="requires a Snowflake session"):
        registry.create_source_adapter(parse_config(s3_parquet_payload))


def test_s3_parquet_adapter_builds_from_session(s3_parquet_payload):
    class Session:
        pass

    adapter = registry.create_source_adapter(parse_config(s3_parquet_payload), session=Session())

    assert adapter.source_type == "s3_parquet"
